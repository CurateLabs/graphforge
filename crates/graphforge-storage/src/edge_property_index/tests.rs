//! The edge property index builder: shard cutting, packing, sealing, poisoning
//! and determinism, and reading a sealed blob back. Lookups need a published
//! generation and are covered through the facade in
//! `graphforge-api/tests/edge_property_index.rs`.

use super::*;
use arrow::array::{BooleanArray, FixedSizeBinaryBuilder, Float64Array, Int64Array, StringArray};
use arrow::datatypes::Field;

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
    let ids = vec![None; fragments.len()];
    build_with(fragments, &ids, None)
}

/// As [`build`], with the topology ids of each fragment's rows (`None` for a
/// fragment the join could not resolve) and the digest of the topology they
/// were joined from.
fn build_with(
    fragments: &[RecordBatch],
    ids: &[Option<Vec<u64>>],
    topology_digest: Option<&str>,
) -> Option<Built> {
    let mut builder = EdgeRouteIndexBuilder::new("route.packs".to_owned());
    let mut packs = Vec::new();
    for (fragment, ids) in fragments.iter().zip(ids) {
        builder
            .observe(fragment, ids.as_deref(), &mut |path, bytes| {
                packs.push((path, bytes));
                Ok(())
            })
            .unwrap();
    }
    let mut files = builder
        .finish("_exploratory", "digest", topology_digest)
        .unwrap();
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

/// Every blob a manifest names, in the order the builder appended them.
fn seals(manifest: &Manifest) -> Vec<BlobSeal> {
    manifest
        .shards
        .iter()
        .flat_map(|shard| {
            std::iter::once(shard.uuids)
                .chain(shard.ids)
                .chain(shard.columns.iter().map(|column| column.blob))
        })
        .collect()
}

/// Topology ids for the rows `first..first + rows` of a fragment that run
/// against the UUID order, as ids do when topology was not written in UUID order.
fn descending_ids(first: u128, rows: usize) -> Vec<u64> {
    (0..rows as u64)
        .map(|row| 10_000_000 - (first as u64 + row) * 7)
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

/// A whole column read through the page path: its directory opened against the
/// seal, then every page read and authenticated.
fn read_column(
    packs: &mut PackHandles,
    seal: BlobSeal,
    rows: u64,
    keys: Keys,
) -> Result<ArrayRef, GfError> {
    let blob = packs.open_blob(seal, rows, keys)?;
    let count = usize::try_from(rows).unwrap().div_ceil(blob.page_rows());
    let wanted: Vec<usize> = (0..count).collect();
    let pages = packs.read_pages(&blob, &wanted)?;
    let pages: Vec<&dyn Array> = pages.iter().map(AsRef::as_ref).collect();
    arrow::compute::concat(&pages).map_err(|e| validation(e.to_string()))
}

/// Every blob of a built index, read whole from packs written to disk, which
/// authenticates each directory against its seal and each page against the
/// directory.
fn read_every_blob(built: &Built) {
    let dir = tempfile::tempdir().unwrap();
    let directory = write_packs(dir.path(), &built.packs);
    let mut packs = PackHandles::new(directory, built.manifest.page_rows);
    for shard in &built.manifest.shards {
        read_column(&mut packs, shard.uuids, shard.rows, Keys::Uuid).unwrap();
        if let Some(ids) = shard.ids {
            read_column(&mut packs, ids, shard.rows, Keys::Id).unwrap();
        }
        for column in &shard.columns {
            read_column(&mut packs, column.blob, shard.rows, Keys::Values).unwrap();
        }
    }
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
    read_every_blob(&built);
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
        expected_offset += seal.byte_length;
    }
    read_every_blob(&built);
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

    let mut packs = PackHandles::new(directory.clone(), built.manifest.page_rows);
    let uuids = read_column(&mut packs, shard.uuids, shard.rows, Keys::Uuid).unwrap();
    let uuids = uuids
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(uuids.value(0), uuid(7));
    assert_eq!(uuids.value(99), uuid(106));
    let column = read_column(&mut packs, timestamps, shard.rows, Keys::Values).unwrap();
    let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(column.value(0), 21);
    assert_eq!(column.value(99), 106 * 3);
    // A blob read with the wrong row count is refused.
    assert!(read_column(&mut packs, timestamps, shard.rows + 1, Keys::Values).is_err());

    // A flipped byte in one blob fails that blob and no other.
    let pack_path = directory.join("00000000.epk");
    let original = std::fs::read(&pack_path).unwrap();
    let mut flipped = original.clone();
    let last = usize::try_from(memos.offset + memos.byte_length).unwrap() - 1;
    flipped[last] ^= 1;
    std::fs::write(&pack_path, &flipped).unwrap();
    let mut packs = PackHandles::new(directory.clone(), built.manifest.page_rows);
    let error = read_column(&mut packs, memos, shard.rows, Keys::Values).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    read_column(&mut packs, timestamps, shard.rows, Keys::Values).unwrap();
    read_column(&mut packs, shard.uuids, shard.rows, Keys::Uuid).unwrap();

    // A pack cut short inside a blob fails that blob; the blobs before it
    // still read.
    let cut = usize::try_from(memos.offset).unwrap() + 1;
    std::fs::write(&pack_path, &original[..cut]).unwrap();
    let mut packs = PackHandles::new(directory.clone(), built.manifest.page_rows);
    let error = read_column(&mut packs, memos, shard.rows, Keys::Values).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    read_column(&mut packs, timestamps, shard.rows, Keys::Values).unwrap();

    // A seal that names bytes past the end of the pack, or past the pack bound,
    // is refused rather than read.
    let past_end = BlobSeal {
        offset: original.len() as u64,
        ..memos
    };
    std::fs::write(&pack_path, &original).unwrap();
    let mut packs = PackHandles::new(directory.clone(), built.manifest.page_rows);
    let error = read_column(&mut packs, past_end, shard.rows, Keys::Values).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    let oversize = BlobSeal {
        byte_length: PACK_BYTES as u64 + 1,
        ..memos
    };
    let error = read_column(&mut packs, oversize, shard.rows, Keys::Values).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");

    // A pack that is gone is an error, not an empty answer.
    std::fs::remove_file(&pack_path).unwrap();
    let mut packs = PackHandles::new(directory, built.manifest.page_rows);
    assert!(read_column(&mut packs, timestamps, shard.rows, Keys::Values).is_err());
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
        .append(&vec![0_u8; PACK_BYTES + 1], 0, &mut write)
        .unwrap();
    assert!(sealed.is_none());
    // A poisoned builder indexes nothing, and writes no manifest.
    builder
        .observe(&fragment(0, 8, 4), None, &mut write)
        .unwrap();
    assert!(
        builder
            .finish("_exploratory", "digest", None)
            .unwrap()
            .is_empty()
    );
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
        .append(&vec![1_u8; PACK_BYTES - 10], 0, &mut write)
        .unwrap()
        .unwrap();
    // Exactly the room left: it still fits.
    let second = builder.append(&[2_u8; 10], 0, &mut write).unwrap().unwrap();
    // One byte more than the room left: the pack is flushed first.
    let third = builder.append(&[3_u8; 1], 0, &mut write).unwrap().unwrap();
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

#[test]
fn ids_are_sealed_beside_the_uuids_with_the_range_they_span() {
    let ids = descending_ids(0, 10_000);
    let built = build_with(
        &[fragment(0, 10_000, 4)],
        &[Some(ids.clone())],
        Some("topology"),
    )
    .expect("indexed");
    let manifest = &built.manifest;
    assert_eq!(manifest.topology_digest.as_deref(), Some("topology"));
    // A UUID blob, an id blob and two column blobs per shard.
    assert_eq!(seals(manifest).len(), 3 * 4);
    read_every_blob(&built);
    let dir = tempfile::tempdir().unwrap();
    let directory = write_packs(dir.path(), &built.packs);
    let mut packs = PackHandles::new(directory, manifest.page_rows);
    let mut start = 0;
    for shard in &manifest.shards {
        let window = &ids[start..start + shard.rows as usize];
        assert_eq!(shard.id_min, window.iter().copied().min());
        assert_eq!(shard.id_max, window.iter().copied().max());
        let column = read_column(&mut packs, shard.ids.unwrap(), shard.rows, Keys::Id).unwrap();
        let column = column.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(column.values().as_ref(), window, "ids stay in UUID order");
        start += shard.rows as usize;
    }
}

#[test]
fn a_shard_is_read_back_by_id_with_the_rows_of_those_ids() {
    let ids = descending_ids(0, 100);
    let built = build_with(
        &[fragment(0, 100, 4)],
        &[Some(ids.clone())],
        Some("topology"),
    )
    .expect("indexed");
    let shard = &built.manifest.shards[0];
    let dir = tempfile::tempdir().unwrap();
    let directory = write_packs(dir.path(), &built.packs);
    let mut packs = PackHandles::new(directory, built.manifest.page_rows);
    let properties = ["timestamp".to_owned()];
    let demanded = demanded_columns(&built.manifest, &properties);
    let range = (shard.id_min.unwrap(), shard.id_max.unwrap());
    // Rows 99, 50 and 3 by id, and an id inside the shard's range that no row
    // holds: the shard answers only what it holds, in id order.
    let mut targets = vec![ids[99], ids[50], ids[3], ids[99] + 1];
    targets.sort_unstable();
    let hit = read_hit_shard_by_id(
        &mut packs,
        shard,
        shard.ids.unwrap(),
        range,
        &targets,
        &demanded,
    )
    .unwrap()
    .expect("the shard holds three of the targets");
    assert_eq!(hit.keys, [ids[99], ids[50], ids[3]]);
    let timestamps = hit.columns.get(&0).expect("the demanded column is read");
    let timestamps = timestamps.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(timestamps.values().as_ref(), [99 * 3, 50 * 3, 3 * 3]);
    // The column that was not demanded stays unread.
    assert_eq!(hit.columns.len(), 1);
    // A shard holding none of the ids answers nothing.
    let none = read_hit_shard_by_id(
        &mut packs,
        shard,
        shard.ids.unwrap(),
        range,
        &[ids[99] + 1],
        &demanded,
    )
    .unwrap();
    assert!(none.is_none());
}

#[test]
fn a_corrupt_id_blob_is_refused_not_answered() {
    let dir = tempfile::tempdir().unwrap();
    let properties = ["timestamp".to_owned()];

    // An id outside the range the manifest promises.
    let ids = descending_ids(0, 100);
    let built = build_with(
        &[fragment(0, 100, 4)],
        &[Some(ids.clone())],
        Some("topology"),
    )
    .expect("indexed");
    let shard = &built.manifest.shards[0];
    let mut packs = PackHandles::new(
        write_packs(&dir.path().join("range"), &built.packs),
        built.manifest.page_rows,
    );
    let demanded = demanded_columns(&built.manifest, &properties);
    let narrowed = (shard.id_min.unwrap() + 1, shard.id_max.unwrap());
    let error = read_hit_shard_by_id(
        &mut packs,
        shard,
        shard.ids.unwrap(),
        narrowed,
        &[ids[3]],
        &demanded,
    )
    .err()
    .expect("an id outside the manifest range is refused");
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");

    // One id held by two rows is two owners of one edge.
    let mut duplicated = descending_ids(0, 100);
    duplicated[40] = duplicated[10];
    let built = build_with(
        &[fragment(0, 100, 4)],
        &[Some(duplicated.clone())],
        Some("topology"),
    )
    .expect("indexed");
    let shard = &built.manifest.shards[0];
    let mut packs = PackHandles::new(
        write_packs(&dir.path().join("duplicate"), &built.packs),
        built.manifest.page_rows,
    );
    let error = read_hit_shard_by_id(
        &mut packs,
        shard,
        shard.ids.unwrap(),
        (shard.id_min.unwrap(), shard.id_max.unwrap()),
        &[duplicated[10]],
        &demanded,
    )
    .err()
    .expect("a duplicated id is refused");
    assert!(matches!(error, GfError::Project { .. }), "{error}");
}

#[test]
fn ids_are_published_all_or_none_and_only_with_their_topology_digest() {
    let fragments = [fragment(0, 5_000, 4), fragment(5_000, 100, 4)];
    let ids = [
        Some(descending_ids(0, 5_000)),
        Some(descending_ids(5_000, 100)),
    ];
    let published = |built: &Built| {
        built
            .manifest
            .shards
            .iter()
            .map(|shard| {
                (
                    shard.ids.is_some(),
                    shard.id_min.is_some(),
                    shard.id_max.is_some(),
                )
            })
            .collect::<Vec<_>>()
    };

    let joined = build_with(&fragments, &ids, Some("topology")).expect("indexed");
    assert!(joined.manifest.topology_digest.is_some());
    assert!(
        published(&joined)
            .iter()
            .all(|shard| *shard == (true, true, true))
    );

    // Ids joined without a topology digest cannot be shown current: withdrawn.
    let undated = build_with(&fragments, &ids, None).expect("indexed");
    assert_eq!(undated.manifest.topology_digest, None);
    assert!(
        published(&undated)
            .iter()
            .all(|shard| *shard == (false, false, false))
    );

    // One fragment the join could not resolve withdraws the whole route's ids,
    // whichever side of it the resolved fragments are on.
    for partial in [[None, ids[1].clone()], [ids[0].clone(), None], [None, None]] {
        let built = build_with(&fragments, &partial, Some("topology")).expect("indexed");
        assert_eq!(built.manifest.topology_digest, None);
        assert!(
            published(&built)
                .iter()
                .all(|shard| *shard == (false, false, false))
        );
        // The UUID-keyed index is intact.
        assert_eq!(built.manifest.rows, 5_100);
    }
}

#[test]
fn an_index_without_ids_has_the_bytes_it_had_before_ids_existed() {
    let built = build(&[fragment(0, 100, 4)]).expect("indexed");
    let text = String::from_utf8_lossy(&built.manifest_file.bytes);
    for key in ["topology_digest", "\"ids\"", "id_min", "id_max"] {
        assert!(!text.contains(key), "{key} in {text}");
    }
    assert_eq!(built.manifest.topology_digest, None);
    assert!(
        built
            .manifest
            .shards
            .iter()
            .all(|shard| shard.ids.is_none())
    );
    // The keys appear only when the ids do.
    let joined = build_with(
        &[fragment(0, 100, 4)],
        &[Some(descending_ids(0, 100))],
        Some("t"),
    )
    .expect("indexed");
    let text = String::from_utf8_lossy(&joined.manifest_file.bytes);
    for key in ["topology_digest", "\"ids\"", "id_min", "id_max"] {
        assert!(text.contains(key), "{key} missing from {text}");
    }
}

#[test]
fn ids_that_do_not_cover_the_batch_are_refused() {
    let mut builder = EdgeRouteIndexBuilder::new("route.packs".to_owned());
    let error = builder
        .observe(&fragment(0, 8, 4), Some(&[1, 2, 3]), &mut |_, _| Ok(()))
        .unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
}

/// Where each page of a paged blob lies in it, read from the directory the
/// blob ends with. This restates the layout of `pages.rs` on purpose: a change
/// to it must change the tests that tamper with a page.
fn page_extents(blob: &[u8], rows: usize, key_width: usize) -> Vec<std::ops::Range<usize>> {
    let count = rows.div_ceil(PAGE_ROWS);
    let entry = 4 + 8 + 2 * key_width;
    let schema_length = u32::from_le_bytes(blob[blob.len() - 4..].try_into().unwrap()) as usize;
    let entries = blob.len() - 4 - count * entry;
    let mut start = 0;
    let extents: Vec<_> = (0..count)
        .map(|page| {
            let at = entries + page * entry;
            let length = u32::from_le_bytes(blob[at..at + 4].try_into().unwrap()) as usize;
            let extent = start..start + length;
            start += length;
            extent
        })
        .collect();
    assert_eq!(start + schema_length, entries, "the pages fill the blob");
    extents
}

/// Flip a byte in the middle of `page` of the blob `seal` names, in the pack
/// file below `directory`.
fn flip_page(directory: &Path, seal: BlobSeal, rows: usize, key_width: usize, page: usize) {
    let path = directory.join(format!("{:08}.epk", seal.pack));
    let mut bytes = std::fs::read(&path).unwrap();
    let start = usize::try_from(seal.offset).unwrap();
    let end = start + usize::try_from(seal.byte_length).unwrap();
    let extent = page_extents(&bytes[start..end], rows, key_width)[page].clone();
    bytes[start + extent.start + extent.len() / 2] ^= 1;
    std::fs::write(path, bytes).unwrap();
}

fn column_named(manifest: &Manifest, shard: &ShardEntry, name: &str) -> BlobSeal {
    shard
        .columns
        .iter()
        .find(|column| manifest.columns[column.column] == name)
        .unwrap()
        .blob
}

fn int64s(array: &ArrayRef) -> Vec<i64> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn a_lookup_decodes_only_the_pages_that_hold_its_targets() {
    let built = build(&[fragment(0, 2_000, 4)]).expect("indexed");
    assert_eq!(built.manifest.page_rows, PAGE_ROWS as u32);
    assert_eq!(built.manifest.shards.len(), 1, "one shard of 16 pages");
    let shard = &built.manifest.shards[0];
    let timestamps = column_named(&built.manifest, shard, "timestamp");
    let rows = shard.rows as usize;
    assert_eq!(rows.div_ceil(PAGE_ROWS), 16);

    let dir = tempfile::tempdir().unwrap();
    let directory = write_packs(dir.path(), &built.packs);
    // A page of the UUID blob and a page of the timestamp blob that hold none of
    // the targets below are damaged.
    flip_page(&directory, shard.uuids, rows, 16, 5);
    flip_page(&directory, timestamps, rows, 0, 7);

    let properties = ["timestamp".to_owned()];
    let demanded = demanded_columns(&built.manifest, &properties);
    let mut packs = PackHandles::new(directory.clone(), built.manifest.page_rows);
    // Rows 3 and 1500 lie in pages 0 and 11: the damaged pages are never read.
    let targets = [uuid(3), uuid(1_500)];
    let hit = read_hit_shard(&mut packs, shard, &targets, &demanded)
        .unwrap()
        .expect("the shard holds both");
    assert_eq!(hit.keys, targets);
    assert_eq!(int64s(&hit.columns[&0]), [9, 4_500]);

    // A target in the damaged UUID page is refused, not answered or skipped.
    let error = read_hit_shard(&mut packs, shard, &[uuid(5 * 128 + 1)], &demanded)
        .err()
        .expect("a damaged UUID page is refused");
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    // A target whose column page is damaged is refused too.
    let error = read_hit_shard(&mut packs, shard, &[uuid(7 * 128 + 2)], &demanded)
        .err()
        .expect("a damaged column page is refused");
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    // Reading either blob whole meets the damage.
    for (seal, keys) in [(shard.uuids, Keys::Uuid), (timestamps, Keys::Values)] {
        let error = read_column(&mut packs, seal, shard.rows, keys).unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    }
    // The other column of the shard is untouched.
    let memos = column_named(&built.manifest, shard, "memo");
    read_column(&mut packs, memos, shard.rows, Keys::Values).unwrap();
}

#[test]
fn a_directory_that_disagrees_with_its_pages_is_refused() {
    let built = build(&[fragment(0, 400, 4)]).expect("indexed");
    let shard = &built.manifest.shards[0];
    let rows = shard.rows as usize;
    let dir = tempfile::tempdir().unwrap();
    let directory = write_packs(dir.path(), &built.packs);
    let path = directory.join("00000000.epk");
    let original = std::fs::read(&path).unwrap();
    let properties = ["timestamp".to_owned()];
    let demanded = demanded_columns(&built.manifest, &properties);

    // Edit the UUID directory and seal the edit, as a writer that computed the
    // directory wrongly would have: the seal is then valid and only the
    // directory's claims about the pages can catch it.
    let reseal = |edit: &dyn Fn(&mut [u8], usize)| {
        let seal = shard.uuids;
        let start = usize::try_from(seal.offset).unwrap();
        let end = start + usize::try_from(seal.byte_length).unwrap();
        let mut bytes = original.clone();
        let whole = &mut bytes[start..end];
        let pages_end = page_extents(whole, rows, 16).last().unwrap().end;
        let schema_length =
            u32::from_le_bytes(whole[whole.len() - 4..].try_into().unwrap()) as usize;
        let tail = &mut whole[pages_end..];
        edit(tail, schema_length);
        let xxh64 = crate::corruption_checksum::checksum(tail);
        std::fs::write(&path, bytes).unwrap();
        BlobSeal { xxh64, ..seal }
    };
    let lookup = |seal: BlobSeal| {
        let forged = ShardEntry {
            uuids: seal,
            ..shard.clone()
        };
        let mut packs = PackHandles::new(directory.clone(), built.manifest.page_rows);
        read_hit_shard(&mut packs, &forged, &[uuid(3)], &demanded).err()
    };

    // Unedited, the same reseal changes nothing.
    assert!(lookup(reseal(&|_, _| {})).is_none());
    // A least key the first page does not start with.
    let error = lookup(reseal(&|tail, schema| tail[schema + 12 + 15] ^= 1))
        .expect("the page does not hold the key its entry names");
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    // A length that leaves bytes of the blob unaccounted for.
    let error = lookup(reseal(&|tail, schema| tail[schema] ^= 1))
        .expect("the directory must account for every byte");
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    // A page checksum that is not the page's.
    let error =
        lookup(reseal(&|tail, schema| tail[schema + 4] ^= 1)).expect("a page must match its entry");
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
}

#[test]
fn ids_out_of_uuid_order_are_found_through_page_ranges_that_overlap() {
    // A multiplicative permutation: every page of 128 ids spans nearly the
    // whole id range, so no page can be ruled out by its neighbours.
    let ids: Vec<u64> = (0..2_000_u64)
        .map(|row| (row * 7_919 + 13) % 1_000_003)
        .collect();
    let built = build_with(
        &[fragment(0, 2_000, 4)],
        &[Some(ids.clone())],
        Some("topology"),
    )
    .expect("indexed");
    let shard = &built.manifest.shards[0];
    let range = (shard.id_min.unwrap(), shard.id_max.unwrap());
    let dir = tempfile::tempdir().unwrap();
    let directory = write_packs(dir.path(), &built.packs);
    let mut packs = PackHandles::new(directory, built.manifest.page_rows);
    let blob = packs
        .open_blob(shard.ids.unwrap(), shard.rows, Keys::Id)
        .unwrap();
    assert!(
        blob.id_pages(&[ids[3]]).len() > 1,
        "the ranges of several pages hold the id"
    );

    let properties = ["timestamp".to_owned()];
    let demanded = demanded_columns(&built.manifest, &properties);
    let mut targets: Vec<u64> = [3_usize, 777, 1_500, 1_999]
        .iter()
        .map(|row| ids[*row])
        .collect();
    targets.sort_unstable();
    let hit = read_hit_shard_by_id(
        &mut packs,
        shard,
        shard.ids.unwrap(),
        range,
        &targets,
        &demanded,
    )
    .unwrap()
    .expect("the shard holds every target");
    assert_eq!(hit.keys, targets);
    let expected: Vec<i64> = targets
        .iter()
        .map(|id| ids.iter().position(|held| held == id).unwrap() as i64 * 3)
        .collect();
    assert_eq!(int64s(&hit.columns[&0]), expected);

    // An id inside the range that no row holds is found in no page.
    let absent = (range.0 + 1..)
        .find(|id| !ids.contains(id))
        .expect("the range has gaps");
    assert!(absent < range.1);
    assert!(
        read_hit_shard_by_id(
            &mut packs,
            shard,
            shard.ids.unwrap(),
            range,
            &[absent],
            &demanded,
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn a_manifest_names_a_usable_page_size() {
    let built = build(&[fragment(0, 8, 4)]).expect("indexed");
    let path = Path::new("indexes/edge-properties/x.epx");
    let mut manifest = decode_manifest(&built.manifest_file.bytes, path).unwrap();
    for rows in [0, SHARD_ROWS as u32 + 1] {
        manifest.page_rows = rows;
        let error = decode_manifest(&encode_manifest(&manifest).unwrap(), path).unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION", "{error}");
        assert!(error.to_string().contains("rebuild_property_indexes"));
    }
    manifest.page_rows = PAGE_ROWS as u32;
    assert!(decode_manifest(&encode_manifest(&manifest).unwrap(), path).is_ok());
}

/// The manifest of an index built before pages: the current document without
/// `page_rows`, sealed by its own checksum so only its content is at issue.
fn manifest_written_before_pages(built: &Built) -> Vec<u8> {
    let mut document: serde_json::Value =
        serde_json::from_slice(&built.manifest_file.bytes[17..]).unwrap();
    document
        .as_object_mut()
        .unwrap()
        .remove("page_rows")
        .expect("the current manifest names its page size");
    let body = serde_json::to_vec(&document).unwrap();
    let mut bytes = format!("{:016x}\n", crate::corruption_checksum::checksum(&body)).into_bytes();
    bytes.extend_from_slice(&body);
    bytes
}

#[test]
fn a_manifest_written_before_pages_is_refused_until_the_index_is_rebuilt() {
    let built = build(&[fragment(0, 8, 4)]).expect("indexed");
    let old = manifest_written_before_pages(&built);
    let path = Path::new("indexes/edge-properties/x.epx");

    // The same refusal as any manifest the reader cannot use: a typed error
    // that names the explicit rebuild, never a repair or a silent bypass.
    let error = decode_manifest(&old, path).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    let message = error.to_string();
    assert!(message.contains("does not decode"), "{message}");
    assert!(message.contains("page_rows"), "{message}");
    assert!(message.contains("rebuild_property_indexes"), "{message}");
    let mut corrupt = built.manifest_file.bytes.clone();
    let last = corrupt.len() - 2;
    corrupt[last] ^= 1;
    let damaged = decode_manifest(&corrupt, path).unwrap_err().to_string();
    assert!(damaged.contains("rebuild_property_indexes"), "{damaged}");

    // The refusal comes before the staleness check that sends a read to the
    // property overlay: a current-format manifest of other source objects is
    // not indexed, the old layout is an error.
    let project = tempfile::tempdir().unwrap();
    let inventory =
        AuthenticatedPropertyInventory::from_entries_at_root(project.path(), Vec::new()).unwrap();
    let route = built.manifest.route.clone();
    let manifest_path = project.path().join(manifest_relative_path(&route));
    std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
    std::fs::write(&manifest_path, &built.manifest_file.bytes).unwrap();
    assert!(!is_current(project.path(), &inventory, &route).unwrap());
    std::fs::write(&manifest_path, &old).unwrap();
    let error = is_current(project.path(), &inventory, &route).unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION", "{error}");
    assert!(error.to_string().contains("rebuild_property_indexes"));
}
