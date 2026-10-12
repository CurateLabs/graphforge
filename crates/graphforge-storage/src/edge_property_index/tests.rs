//! The edge property index builder: shard cutting, packing, sealing, poisoning
//! and determinism, and reading a sealed blob back. Lookups need a published
//! generation and are covered through the facade in
//! `graphforge-api/tests/edge_property_index.rs`.

use super::*;
use arrow::array::{BooleanArray, FixedSizeBinaryBuilder, Float64Array, Int64Array, StringArray};

fn uuid(index: u128) -> [u8; 16] {
    (0x2000_0000_0000_0000_0000_0000_0000_0000_u128 | (index + 1)).to_be_bytes()
}

/// A fragment of one edge per memo, starting at index `first`, ascending by UUID.
fn fragment_of(first: u128, memos: &[String]) -> RecordBatch {
    let rows = memos.len();
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for row in 0..rows {
        uuids.append_value(uuid(first + row as u128)).unwrap();
    }
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
            Arc::new(StringArray::from_iter_values(memos)),
        ],
    )
    .unwrap()
}

/// A fragment of `rows` edges starting at index `first`, ascending by UUID.
fn fragment(first: u128, rows: usize, memo_bytes: usize) -> RecordBatch {
    fragment_of(first, &vec!["m".repeat(memo_bytes); rows])
}

/// `bytes` of pseudo-random hex text, which Zstd cannot shrink below half.
fn noise(seed: u64, bytes: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut text = String::with_capacity(bytes + 16);
    while text.len() < bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        text.push_str(&format!("{state:016x}"));
    }
    text.truncate(bytes);
    text
}

type Files = Vec<(String, Vec<u8>)>;

/// A finished index: every pack the builder wrote, in order, and the manifest.
struct Built {
    packs: Files,
    manifest_file: EdgePropertyIndexFile,
    manifest: Manifest,
}

fn build(fragments: &[RecordBatch]) -> Option<Built> {
    let mut builder = EdgeRouteIndexBuilder::new("route.packs".to_owned());
    let mut packs = Vec::new();
    for fragment in fragments {
        builder
            .observe(fragment, &mut |path, bytes| {
                packs.push((path, bytes));
                Ok(())
            })
            .unwrap();
    }
    let mut files = builder.finish("_exploratory", "digest").unwrap();
    let manifest_file = files.pop()?;
    packs.extend(
        files
            .into_iter()
            .map(|file| (file.relative_path, file.bytes)),
    );
    let manifest = decode_manifest(
        &manifest_file.bytes,
        Path::new(&manifest_file.relative_path),
    )
    .unwrap();
    Some(Built {
        packs,
        manifest_file,
        manifest,
    })
}

/// The bytes the seal names inside the packs of a built index.
fn blob(packs: &Files, seal: BlobSeal) -> &[u8] {
    let (path, bytes) = &packs[seal.pack as usize];
    assert_eq!(
        path,
        &format!("indexes/edge-properties/route.packs/{:08}.epk", seal.pack)
    );
    let start = usize::try_from(seal.offset).unwrap();
    &bytes[start..start + usize::try_from(seal.byte_length).unwrap()]
}

/// Every blob a manifest names, in the order the builder appended them.
fn seals(manifest: &Manifest) -> Vec<BlobSeal> {
    manifest
        .shards
        .iter()
        .flat_map(|shard| {
            std::iter::once(shard.uuids).chain(shard.columns.iter().map(|column| column.blob))
        })
        .collect()
}

/// Lay the packs of a built index out as a lookup finds them, below `root`.
fn write_packs(root: &Path, packs: &Files) -> PathBuf {
    let directory = root.join("route.packs");
    std::fs::create_dir_all(&directory).unwrap();
    for (path, bytes) in packs {
        std::fs::write(directory.join(path.rsplit('/').next().unwrap()), bytes).unwrap();
    }
    directory
}

#[test]
fn fragments_are_cut_into_row_bounded_shards_each_blob_sealed_in_the_manifest() {
    let built = build(&[fragment(0, 10_000, 4)]).expect("indexed");
    let manifest = &built.manifest;
    assert_eq!(manifest.rows, 10_000);
    assert_eq!(manifest.columns, ["timestamp", "memo"]);
    assert_eq!(manifest.pack_dir, "route.packs");
    let rows = manifest
        .shards
        .iter()
        .map(|shard| shard.rows)
        .collect::<Vec<_>>();
    assert_eq!(rows, [4096, 4096, 1808]);
    // Small values: the whole route is one pack, holding a UUID blob and a blob
    // per stored column, per shard.
    assert_eq!(built.packs.len(), 1);
    assert_eq!(seals(manifest).len(), 3 * 3);
    for seal in seals(manifest) {
        assert_eq!(
            crate::corruption_checksum::checksum(blob(&built.packs, seal)),
            seal.xxh64
        );
    }
    // Ranges are the shard's first and last UUID.
    assert_eq!(manifest.shards[0].first, hex(&uuid(0)));
    assert_eq!(manifest.shards[0].last, hex(&uuid(4095)));
    assert_eq!(manifest.shards[2].last, hex(&uuid(9_999)));
}

#[test]
fn a_route_larger_than_a_pack_fills_consecutive_packs_with_contiguous_blobs() {
    // 500 KiB of incompressible text per edge: two edges per shard, about 6 MiB
    // of compressed blobs, which is more than one 4 MiB pack.
    let memos = (0..24)
        .map(|row| noise(row + 1, 500 << 10))
        .collect::<Vec<_>>();
    let built = build(&[fragment_of(0, &memos)]).expect("indexed");
    assert!(
        built.packs.len() >= 2,
        "expected more than one pack, got {}",
        built.packs.len()
    );
    for (index, (path, bytes)) in built.packs.iter().enumerate() {
        assert_eq!(
            path,
            &format!("indexes/edge-properties/route.packs/{index:08}.epk")
        );
        assert!(!bytes.is_empty());
        assert!(bytes.len() <= PACK_BYTES, "pack {index} is {}", bytes.len());
    }
    // Blobs are laid end to end, a blob never straddles two packs, and the
    // packs fill in order.
    let mut expected_pack = 0_u32;
    let mut expected_offset = 0_u64;
    for seal in seals(&built.manifest) {
        if seal.pack != expected_pack {
            assert_eq!(seal.pack, expected_pack + 1, "packs fill in order");
            assert_eq!(
                expected_offset,
                built.packs[expected_pack as usize].1.len() as u64,
                "a pack holds exactly its blobs"
            );
            expected_pack = seal.pack;
            expected_offset = 0;
        }
        assert_eq!(seal.offset, expected_offset);
        assert_eq!(
            crate::corruption_checksum::checksum(blob(&built.packs, seal)),
            seal.xxh64
        );
        expected_offset += seal.byte_length;
    }
    assert_eq!(expected_pack as usize, built.packs.len() - 1);
    assert_eq!(
        expected_offset,
        built.packs.last().unwrap().1.len() as u64,
        "the last pack holds exactly its blobs"
    );
}

#[test]
fn a_blob_is_read_back_exactly_and_each_blob_is_sealed_alone() {
    let built = build(&[fragment(7, 100, 4)]).expect("indexed");
    let shard = &built.manifest.shards[0];
    let timestamps = shard
        .columns
        .iter()
        .find(|column| built.manifest.columns[column.column] == "timestamp")
        .unwrap()
        .blob;
    let memos = shard
        .columns
        .iter()
        .find(|column| built.manifest.columns[column.column] == "memo")
        .unwrap()
        .blob;
    let dir = tempfile::tempdir().unwrap();
    let directory = write_packs(dir.path(), &built.packs);

    let mut packs = PackHandles::new(directory.clone());
    let uuids = packs.read_column(shard.uuids, shard.rows).unwrap();
    let uuids = uuids
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(uuids.value(0), uuid(7));
    assert_eq!(uuids.value(99), uuid(106));
    let column = packs.read_column(timestamps, shard.rows).unwrap();
    let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(column.value(0), 21);
    assert_eq!(column.value(99), 106 * 3);
    // A blob read with the wrong row count is refused.
    assert!(packs.read_column(timestamps, shard.rows + 1).is_err());

    // A flipped byte in one blob fails that blob and no other.
    let pack_path = directory.join("00000000.epk");
    let original = std::fs::read(&pack_path).unwrap();
    let mut flipped = original.clone();
    let last = usize::try_from(memos.offset + memos.byte_length).unwrap() - 1;
    flipped[last] ^= 1;
    std::fs::write(&pack_path, &flipped).unwrap();
    let mut packs = PackHandles::new(directory.clone());
    let error = packs.read_column(memos, shard.rows).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    packs.read_column(timestamps, shard.rows).unwrap();
    packs.read_column(shard.uuids, shard.rows).unwrap();

    // A pack cut short inside a blob fails that blob; the blobs before it
    // still read.
    let cut = usize::try_from(memos.offset).unwrap() + 1;
    std::fs::write(&pack_path, &original[..cut]).unwrap();
    let mut packs = PackHandles::new(directory.clone());
    let error = packs.read_column(memos, shard.rows).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    packs.read_column(timestamps, shard.rows).unwrap();

    // A seal that names bytes past the end of the pack, or past the pack bound,
    // is refused rather than read.
    let past_end = BlobSeal {
        offset: original.len() as u64,
        ..memos
    };
    std::fs::write(&pack_path, &original).unwrap();
    let mut packs = PackHandles::new(directory.clone());
    let error = packs.read_column(past_end, shard.rows).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    let oversize = BlobSeal {
        byte_length: PACK_BYTES as u64 + 1,
        ..memos
    };
    let error = packs.read_column(oversize, shard.rows).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");

    // A pack that is gone is an error, not an empty answer.
    std::fs::remove_file(&pack_path).unwrap();
    let mut packs = PackHandles::new(directory);
    assert!(packs.read_column(timestamps, shard.rows).is_err());
}

#[test]
fn wide_values_shrink_the_shard_until_a_single_value_passes_the_bound() {
    // 600 KiB per memo: two rows pass the 1 MiB column bound, so one per shard.
    let built = build(&[fragment(0, 4, 600 << 10)]).expect("indexed");
    let rows = built
        .manifest
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
fn a_blob_that_cannot_fit_a_pack_leaves_the_route_to_the_property_overlay() {
    let mut builder = EdgeRouteIndexBuilder::new("route.packs".to_owned());
    let mut written = 0_usize;
    let mut write = |_: String, _: Vec<u8>| {
        written += 1;
        Ok(())
    };
    let sealed = builder
        .append(&vec![0_u8; PACK_BYTES + 1], &mut write)
        .unwrap();
    assert!(sealed.is_none());
    // A poisoned builder indexes nothing, and writes no manifest.
    builder.observe(&fragment(0, 8, 4), &mut write).unwrap();
    assert!(builder.finish("_exploratory", "digest").unwrap().is_empty());
    assert_eq!(written, 0);
}

#[test]
fn a_blob_that_would_overflow_a_pack_starts_the_next_one() {
    let mut builder = EdgeRouteIndexBuilder::new("route.packs".to_owned());
    let mut flushed: Files = Vec::new();
    let mut write = |path: String, bytes: Vec<u8>| {
        flushed.push((path, bytes));
        Ok(())
    };
    let first = builder
        .append(&vec![1_u8; PACK_BYTES - 10], &mut write)
        .unwrap()
        .unwrap();
    // Exactly the room left: it still fits.
    let second = builder.append(&[2_u8; 10], &mut write).unwrap().unwrap();
    // One byte more than the room left: the pack is flushed first.
    let third = builder.append(&[3_u8; 1], &mut write).unwrap().unwrap();
    assert_eq!((first.pack, first.offset), (0, 0));
    assert_eq!((second.pack, second.offset), (0, PACK_BYTES as u64 - 10));
    assert_eq!((third.pack, third.offset), (1, 0));
    assert_eq!(flushed.len(), 1);
    assert_eq!(
        flushed[0].0,
        "indexes/edge-properties/route.packs/00000000.epk"
    );
    assert_eq!(flushed[0].1.len(), PACK_BYTES);
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
    let manifest = build(&[fragment(0, 8, 4), second])
        .expect("indexed")
        .manifest;
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
    let first = build(&fragments).expect("indexed");
    let second = build(&fragments).expect("indexed");
    assert_eq!(first.manifest_file, second.manifest_file);
    assert_eq!(first.packs, second.packs);
}

#[test]
fn a_manifest_that_fails_its_checksum_is_refused() {
    let built = build(&[fragment(0, 8, 4)]).expect("indexed");
    let path = Path::new("indexes/edge-properties/x.epx");
    assert!(decode_manifest(&built.manifest_file.bytes, path).is_ok());
    let mut corrupt = built.manifest_file.bytes.clone();
    let last = corrupt.len() - 2;
    corrupt[last] ^= 1;
    let error = decode_manifest(&corrupt, path).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    assert!(decode_manifest(&built.manifest_file.bytes[..10], path).is_err());
}
