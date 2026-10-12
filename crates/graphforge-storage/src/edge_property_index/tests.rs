//! The edge property index builder: shard cutting, sealing, poisoning and
//! determinism. Lookups need a published generation and are covered through the
//! facade in `graphforge-api/tests/edge_property_index.rs`.

use super::*;
use arrow::array::{BooleanArray, FixedSizeBinaryBuilder, Float64Array, Int64Array, StringArray};

fn uuid(index: u128) -> [u8; 16] {
    (0x2000_0000_0000_0000_0000_0000_0000_0000_u128 | (index + 1)).to_be_bytes()
}

/// A fragment of `rows` edges starting at index `first`, ascending by UUID.
fn fragment(first: u128, rows: usize, memo_bytes: usize) -> RecordBatch {
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for row in 0..rows {
        uuids.append_value(uuid(first + row as u128)).unwrap();
    }
    let memo = "m".repeat(memo_bytes);
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(UUID_COLUMN, DataType::FixedSizeBinary(16), false),
            Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
            Field::new("timestamp", DataType::Int64, true),
            Field::new("memo", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(uuids.finish()),
            Arc::new(BooleanArray::from(vec![false; rows])),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| (first as i64 + row as i64) * 3),
            )),
            Arc::new(StringArray::from(vec![memo.as_str(); rows])),
        ],
    )
    .unwrap()
}

type Files = Vec<(String, Vec<u8>)>;

fn build(fragments: &[RecordBatch]) -> Option<(Files, EdgePropertyIndexFile)> {
    let mut builder = EdgeRouteIndexBuilder::new("route.shards".to_owned());
    let mut files = Vec::new();
    for fragment in fragments {
        builder
            .observe(fragment, &mut |path, bytes| {
                files.push((path, bytes));
                Ok(())
            })
            .unwrap();
    }
    builder
        .finish("_exploratory", "digest")
        .unwrap()
        .map(|manifest| (files, manifest))
}

fn manifest_of(file: &EdgePropertyIndexFile) -> Manifest {
    decode_manifest(&file.bytes, Path::new(&file.relative_path)).unwrap()
}

#[test]
fn fragments_are_cut_into_row_bounded_shards_each_sealed_in_the_manifest() {
    let (files, manifest_file) = build(&[fragment(0, 10_000, 4)]).expect("indexed");
    let manifest = manifest_of(&manifest_file);
    assert_eq!(manifest.rows, 10_000);
    assert_eq!(manifest.columns, ["timestamp", "memo"]);
    assert_eq!(manifest.shard_dir, "route.shards");
    let rows = manifest
        .shards
        .iter()
        .map(|shard| shard.rows)
        .collect::<Vec<_>>();
    assert_eq!(rows, [4096, 4096, 1808]);
    // One UUID file and one file per stored column, per shard.
    assert_eq!(files.len(), 3 * 3);
    for shard in &manifest.shards {
        let uuid_file = files
            .iter()
            .find(|(path, _)| path.ends_with(&format!("{}.{UUID_EXTENSION}", shard.stem)))
            .expect("uuid file");
        assert_eq!(FileSeal::of(&uuid_file.1).xxh64, shard.uuids.xxh64);
        assert_eq!(uuid_file.1.len() as u64, shard.uuids.byte_length);
        for column in &shard.columns {
            let name = format!("{}.c{}.{COLUMN_EXTENSION}", shard.stem, column.column);
            let bytes = &files
                .iter()
                .find(|(path, _)| path.ends_with(&name))
                .expect("column file")
                .1;
            assert_eq!(FileSeal::of(bytes).xxh64, column.seal.xxh64);
        }
    }
    // Ranges are the shard's first and last UUID.
    assert_eq!(manifest.shards[0].first, hex(&uuid(0)));
    assert_eq!(manifest.shards[0].last, hex(&uuid(4095)));
    assert_eq!(manifest.shards[2].last, hex(&uuid(9_999)));
}

#[test]
fn a_shard_file_is_read_back_exactly() {
    let (files, manifest_file) = build(&[fragment(7, 100, 4)]).expect("indexed");
    let manifest = manifest_of(&manifest_file);
    let dir = tempfile::tempdir().unwrap();
    for (path, bytes) in &files {
        let name = path.rsplit('/').next().unwrap();
        std::fs::write(dir.path().join(name), bytes).unwrap();
    }
    let shard = &manifest.shards[0];
    let uuids = read_column(
        dir.path(),
        &format!("{}.{UUID_EXTENSION}", shard.stem),
        shard.uuids,
        shard.rows,
    )
    .unwrap();
    let uuids = uuids
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(uuids.value(0), uuid(7));
    assert_eq!(uuids.value(99), uuid(106));
    let timestamps = shard
        .columns
        .iter()
        .find(|column| manifest.columns[column.column] == "timestamp")
        .unwrap();
    let column = read_column(
        dir.path(),
        &format!("{}.c{}.{COLUMN_EXTENSION}", shard.stem, timestamps.column),
        timestamps.seal,
        shard.rows,
    )
    .unwrap();
    let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(column.value(0), 21);
    assert_eq!(column.value(99), 106 * 3);

    // A flipped byte fails the seal; a changed length does too.
    let name = format!("{}.{UUID_EXTENSION}", shard.stem);
    let path = dir.path().join(&name);
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    let error = read_column(dir.path(), &name, shard.uuids, shard.rows).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    bytes.push(0);
    std::fs::write(&path, &bytes).unwrap();
    assert!(read_column(dir.path(), &name, shard.uuids, shard.rows).is_err());
}

#[test]
fn wide_values_shrink_the_shard_until_a_single_value_passes_the_bound() {
    // 600 KiB per memo: two rows pass the 1 MiB column bound, so one per shard.
    let (_, manifest_file) = build(&[fragment(0, 4, 600 << 10)]).expect("indexed");
    let rows = manifest_of(&manifest_file)
        .shards
        .iter()
        .map(|shard| shard.rows)
        .collect::<Vec<_>>();
    assert_eq!(rows, [1, 1, 1, 1]);
    // A single value over the bound cannot be sharded: the route is left to
    // the property overlay.
    assert!(build(&[fragment(0, 2, 2 << 20)]).is_none());
}

#[test]
fn routes_that_break_the_build_assumptions_are_not_indexed() {
    // A tombstone.
    let live = fragment(0, 8, 4);
    let mut columns = live.columns().to_vec();
    columns[1] = Arc::new(BooleanArray::from(vec![
        false, false, true, false, false, false, false, false,
    ]));
    let tombstoned = RecordBatch::try_new(live.schema(), columns).unwrap();
    assert!(build(&[tombstoned]).is_none());

    // Rows that are not strictly ascending by UUID.
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(2, 16);
    uuids.append_value(uuid(5)).unwrap();
    uuids.append_value(uuid(4)).unwrap();
    let unsorted = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(UUID_COLUMN, DataType::FixedSizeBinary(16), false),
            Field::new("timestamp", DataType::Int64, true),
        ])),
        vec![
            Arc::new(uuids.finish()),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    assert!(build(&[unsorted]).is_none());

    // One column stored under two types by two fragments of the route.
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(1, 16);
    uuids.append_value(uuid(1_000)).unwrap();
    let retyped = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(UUID_COLUMN, DataType::FixedSizeBinary(16), false),
            Field::new("timestamp", DataType::Float64, true),
        ])),
        vec![
            Arc::new(uuids.finish()),
            Arc::new(Float64Array::from(vec![1.5])),
        ],
    )
    .unwrap();
    assert!(build(&[fragment(0, 8, 4), retyped]).is_none());
}

#[test]
fn fragments_with_different_columns_share_a_route() {
    // The first fragment stores `timestamp` and `memo`; the second only `amount`.
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(2, 16);
    uuids.append_value(uuid(100)).unwrap();
    uuids.append_value(uuid(101)).unwrap();
    let second = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(UUID_COLUMN, DataType::FixedSizeBinary(16), false),
            Field::new("amount", DataType::Float64, true),
        ])),
        vec![
            Arc::new(uuids.finish()),
            Arc::new(Float64Array::from(vec![0.5, 1.5])),
        ],
    )
    .unwrap();
    let (_, manifest_file) = build(&[fragment(0, 8, 4), second]).expect("indexed");
    let manifest = manifest_of(&manifest_file);
    assert_eq!(manifest.columns, ["timestamp", "memo", "amount"]);
    let stored = manifest
        .shards
        .iter()
        .map(|shard| shard.columns.iter().map(|c| c.column).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(stored, [vec![0, 1], vec![2]]);
}

#[test]
fn the_bytes_are_a_function_of_the_content() {
    let fragments = [fragment(0, 5_000, 16), fragment(5_000, 100, 16)];
    let (first_files, first) = build(&fragments).expect("indexed");
    let (second_files, second) = build(&fragments).expect("indexed");
    assert_eq!(first, second);
    assert_eq!(first_files, second_files);
}

#[test]
fn a_manifest_that_fails_its_checksum_is_refused() {
    let (_, manifest_file) = build(&[fragment(0, 8, 4)]).expect("indexed");
    let path = Path::new("indexes/edge-properties/x.epx");
    assert!(decode_manifest(&manifest_file.bytes, path).is_ok());
    let mut corrupt = manifest_file.bytes.clone();
    let last = corrupt.len() - 2;
    corrupt[last] ^= 1;
    let error = decode_manifest(&corrupt, path).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    assert!(decode_manifest(&manifest_file.bytes[..10], path).is_err());
}
