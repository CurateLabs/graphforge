use super::super::BULK_IO_BYTES;
use super::super::IDENTITY_RECORD_BYTES;
use super::super::IDENTITY_RECORD_WIDTH;
use super::super::INDEX_DIR;
use super::super::NODE_LOOKUP_RECORD_BYTES;
use super::super::UuidMembershipIndex;
use super::super::append_uuid_membership_delta;
use super::super::identity_codec;
use super::super::ordinal_artifacts::publish_v4_construction_artifacts;
use super::super::ordinal_artifacts::stage_v4_ordinal_bundle;
use super::super::rebuild::build_identity_run;
use super::super::rebuild::read_exact_record;
use super::super::topology_delta::hex_sha256;
use super::super::topology_delta::plan_uuid_membership_delta;
use super::super::topology_delta::write_identity_records;
use super::cleanup_private_construction_index;
use super::encode_construction_index;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use uuid::Uuid;

#[test]
fn cleanup_authenticates_complete_unpublished_v4_facet() {
    let encoded_dir = tempfile::tempdir().unwrap();
    let encoded = graphforge_filesystem::StableDirectory::open(encoded_dir.path()).unwrap();
    let graph = encoded
        .create_child_directory(std::ffi::OsStr::new("graph"))
        .unwrap();
    let topology = graph
        .create_child_directory(std::ffi::OsStr::new("topology"))
        .unwrap();
    let index = topology
        .create_child_directory(std::ffi::OsStr::new("uuid-membership"))
        .unwrap();
    let mappings = [(Uuid::from_u128(1), 1_u64), (Uuid::from_u128(2), 3_u64)];
    let bundle = stage_v4_ordinal_bundle(mappings, 1, &index, &mut || false).unwrap();
    publish_v4_construction_artifacts(
        &encoded,
        bundle,
        1,
        &hex_sha256(b"delta"),
        None,
        &mut || false,
        None,
    )
    .unwrap();

    cleanup_private_construction_index(&encoded).unwrap();
    assert!(index.child_names().unwrap().is_empty());
}

#[test]
fn packed_construction_index_preserves_full_width_surrogates_and_refuses_invalid_records() {
    let uuid = Uuid::from_u128(u128::MAX);
    let surrogate = u64::MAX;
    let mut golden = uuid.as_bytes().to_vec();
    golden.extend_from_slice(&[0, 0]);
    golden.extend_from_slice(&surrogate.to_be_bytes());
    assert_eq!(golden.len(), 26);
    let mut cases = vec![(golden.clone(), true)];
    for length in 1..26 {
        cases.push((golden[..length].to_vec(), false));
    }
    for (offset, value) in [(16, 2), (17, 1), (17, 255)] {
        let mut invalid = golden.clone();
        invalid[offset] = value;
        cases.push((invalid, false));
    }
    let mut zero = golden.clone();
    zero[18..26].fill(0);
    cases.push((zero, false));
    for (bytes, valid) in cases {
        let source_dir = tempfile::tempdir().unwrap();
        let encoded_dir = tempfile::tempdir().unwrap();
        fs::write(source_dir.path().join("identities.run"), &bytes).unwrap();
        let source = graphforge_filesystem::StableDirectory::open(source_dir.path()).unwrap();
        let encoded = graphforge_filesystem::StableDirectory::open(encoded_dir.path()).unwrap();
        let result = encode_construction_index(
            &source,
            "identities.run",
            &hex_sha256(&bytes),
            &encoded,
            1,
            0,
            None,
            1,
            0,
            &mut || false,
            None,
        );
        if valid {
            let result = result.unwrap();
            let identity = result
                .artifacts
                .iter()
                .find(|artifact| artifact.name.ends_with(".uuidx"))
                .unwrap();
            let mut expected = uuid.as_bytes().to_vec();
            expected.push(0);
            expected.extend_from_slice(&surrogate.to_be_bytes());
            assert_eq!(expected.len(), 25);
            assert_eq!(
                fs::read(
                    encoded_dir
                        .path()
                        .join("graph/topology/uuid-membership")
                        .join(&identity.name)
                )
                .unwrap(),
                expected
            );
            let mut index =
                UuidMembershipIndex::open_at_generation(&encoded_dir.path().join("graph"), 1)
                    .unwrap();
            assert_eq!(
                index.lookup_node_surrogates(&[uuid]).unwrap().0,
                vec![Some(surrogate)]
            );
        } else {
            assert!(result.is_err());
            assert_eq!(
                fs::read(source_dir.path().join("identities.run")).unwrap(),
                bytes
            );
        }
    }
}

#[test]
fn construction_encoder_io_geometry_is_block_bounded() {
    for records in [32_768_u64, 65_536, 131_072] {
        let source_dir = tempfile::tempdir().unwrap();
        let encoded_dir = tempfile::tempdir().unwrap();
        let mut input = BufWriter::with_capacity(
            BULK_IO_BYTES,
            File::create(source_dir.path().join("identities.run")).unwrap(),
        );
        for value in 1..=records {
            input.write_all(&u128::from(value).to_be_bytes()).unwrap();
            input.write_all(&[0]).unwrap();
            input.write_all(&[0]).unwrap();
            input.write_all(&value.to_be_bytes()).unwrap();
        }
        input.flush().unwrap();
        drop(input);
        let source = graphforge_filesystem::StableDirectory::open(source_dir.path()).unwrap();
        let encoded = graphforge_filesystem::StableDirectory::open(encoded_dir.path()).unwrap();
        let source_sha256 =
            hex_sha256(&fs::read(source_dir.path().join("identities.run")).unwrap());
        let result = encode_construction_index(
            &source,
            "identities.run",
            &source_sha256,
            &encoded,
            1,
            0,
            None,
            records,
            0,
            &mut || false,
            None,
        )
        .unwrap();
        let identity_blocks = (records * IDENTITY_RECORD_BYTES).div_ceil(BULK_IO_BYTES as u64);
        let surrogate_blocks = (records * NODE_LOOKUP_RECORD_BYTES).div_ceil(BULK_IO_BYTES as u64);
        assert!(
            result.read_operations <= 2 * identity_blocks + surrogate_blocks + 4,
            "{records}: {} reads",
            result.read_operations
        );
        assert!(
            result.write_operations <= identity_blocks + surrogate_blocks + 4,
            "{records}: {} writes",
            result.write_operations
        );
        assert!(result.peak_buffer_bytes <= 3 * BULK_IO_BYTES as u64);
    }
}

#[test]
fn packed_identity_write_count_tracks_whole_record_flush_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let records = (1..=123_361_u128)
        .map(|id| (Uuid::from_u128(id), 1, 0))
        .collect::<Vec<_>>();
    let path = dir.path().join("edges.uuidx");
    assert_eq!(write_identity_records(&path, &records).unwrap(), 3);
    assert_eq!(fs::metadata(path).unwrap().len(), 2_097_137);
    let edges = records.iter().map(|record| record.0).collect::<Vec<_>>();
    let root = dir.path().join(INDEX_DIR);
    fs::create_dir_all(&root).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let (_, _, _, planned) =
        plan_uuid_membership_delta(&root, 0, 1, None, scratch.path(), &[], &edges, &[], &[])
            .unwrap();
    assert_eq!(planned.write_blocks, 3);
    assert_eq!(planned.write_bytes, 2_097_137);
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    let appended = append_uuid_membership_delta(dir.path(), 1, &[], &edges).unwrap();
    assert_eq!(appended.write_blocks, 3);
    assert_eq!(appended.write_bytes, 2_097_137);
}

#[test]
fn fixed_width_codecs_distinguish_clean_eof_from_partial_tail() {
    let mut clean = std::io::Cursor::new(Vec::<u8>::new());
    assert_eq!(identity_codec::read(&mut clean).unwrap(), None);
    for length in 1..IDENTITY_RECORD_WIDTH {
        let mut partial = std::io::Cursor::new(vec![0_u8; length]);
        assert!(identity_codec::read(&mut partial).is_err());
    }
    for length in 1..24 {
        let mut partial = std::io::Cursor::new(vec![0_u8; length]);
        assert!(read_exact_record::<24>(&mut partial).is_err());
    }
}

#[test]
fn unified_identity_merge_rejects_cross_kind_uuid() {
    let scratch = tempfile::tempdir().unwrap();
    let uuid = [7_u8; 16];
    let node = scratch.path().join("node.run");
    let edge = scratch.path().join("edge.run");
    for path in [&node, &edge] {
        let mut file = File::create(path).unwrap();
        file.write_all(&uuid).unwrap();
        file.write_all(&1_u64.to_le_bytes()).unwrap();
    }
    assert!(build_identity_run(&node, &edge, &scratch.path().join("out.run")).is_err());
}
