use super::super::identity;
use super::super::planning::{inspect, portable_id, portable_participant_id};
use super::*;
use crate::project_portable_v2::pax::USTAR_MAX_ENTRY_BYTES;

#[test]
fn pax_and_structural_budgets_are_canonical_without_payload_allocation() {
    assert_eq!(portable_id("graph-files"), "graph-files");
    assert_ne!(portable_id("graph-files"), "graph-tree");
    assert_ne!(
        portable_participant_id("a-b", "c"),
        portable_participant_id("a", "b-c")
    );
    let long = format!(
        "data/components/graph-data/graph-files/{}/nodes.parquet",
        "segment".repeat(40)
    );
    let record = pax::record("path", &long);
    let declared: usize = record.split_once(' ').unwrap().0.parse().unwrap();
    assert_eq!(declared, record.len());
    assert_eq!(
        PortableV2ExportLimits::default().max_entry_bytes,
        16 * 1024 * 1024 * 1024 * 1024
    );
    assert!(PortableV2ExportLimits::default().copy_buffer_bytes <= 8 * 1024 * 1024);
    let out = tempfile::NamedTempFile::new().unwrap();
    let mut file = out.reopen().unwrap();
    let mut digest = TransportHash::new();
    header(&mut file, &mut digest, &long, 1_073_741_824).unwrap();
    let bytes = fs::read(out.path()).unwrap();
    assert_eq!(&bytes[..11], b"PaxHeaders/");
    assert_eq!(bytes[156], b'x');
    assert_eq!(bytes[1024..1033].as_ref(), b"PaxFiles/");
    assert_eq!(bytes[1024 + 156], b'0');
    assert!(oct(&mut [0; 12], USTAR_MAX_ENTRY_BYTES).is_ok());
    assert!(oct(&mut [0; 12], USTAR_MAX_ENTRY_BYTES + 1).is_err());
}

/// Header bytes the writer emits for one entry, without any payload.
fn header_bytes(path: &str, size: u64) -> Vec<u8> {
    let out = tempfile::NamedTempFile::new().unwrap();
    let mut file = out.reopen().unwrap();
    header(&mut file, &mut TransportHash::new(), path, size).unwrap();
    fs::read(out.path()).unwrap()
}

#[test]
fn entries_over_the_ustar_size_field_carry_a_pax_size_record_and_a_zero_field() {
    let path = "data/components/graph-data/graph-tree/graph-objects/sha256/aaaa";
    let at_limit = header_bytes(path, USTAR_MAX_ENTRY_BYTES);
    assert_eq!(
        at_limit.len(),
        512,
        "an entry that fits keeps one ustar header"
    );
    assert_eq!(at_limit[156], b'0');
    assert_eq!(&at_limit[124..136], b"77777777777\0");

    let size = USTAR_MAX_ENTRY_BYTES + 1;
    let bytes = header_bytes(path, size);
    assert_eq!(bytes.len(), 3 * 512);
    assert_eq!(&bytes[..11], b"PaxHeaders/");
    assert_eq!(bytes[156], b'x');
    let records = pax::encode(path, size);
    assert_eq!(
        records,
        format!("{}19 size={size}\n", pax::record("path", path))
    );
    assert_eq!(&bytes[512..512 + records.len()], records.as_bytes());
    assert!(bytes[512 + records.len()..1024].iter().all(|b| *b == 0));
    assert_eq!(&bytes[1024..1033], b"PaxFiles/");
    assert_eq!(bytes[1024 + 156], b'0');
    assert_eq!(&bytes[1024 + 124..1024 + 136], b"00000000000\0");

    // One independently derived vector pins the writer's bytes to the
    // portable-v2 contract (`scripts/ci/portable-v2-contract.py`).
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/portable-v2/bundle-byte-vectors.json"
    ))
    .unwrap();
    let vector = vectors["header_vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|vector| vector["name"] == "canonical-local-pax-size-record")
        .unwrap();
    let bytes = header_bytes(
        vector["path"].as_str().unwrap(),
        vector["declared_size"].as_u64().unwrap(),
    );
    assert_eq!(
        bytes.len() as u64,
        vector["header_length"].as_u64().unwrap()
    );
    assert_eq!(
        hex(sha2::Sha256::digest(&bytes).into()),
        vector["header_sha256"].as_str().unwrap()
    );
}

/// A one-entry archive built through the writer's own primitives: headers,
/// payload, zero padding and the two terminal zero blocks.
fn single_entry_archive(path: &str, payload: &[u8]) -> Vec<u8> {
    let out = tempfile::NamedTempFile::new().unwrap();
    let mut file = out.reopen().unwrap();
    let mut transport = TransportHash::new();
    header(&mut file, &mut transport, path, payload.len() as u64).unwrap();
    emit(&mut file, &mut transport, payload).unwrap();
    pad(&mut file, &mut transport, payload.len() as u64).unwrap();
    emit(&mut file, &mut transport, &[0u8; 1024]).unwrap();
    fs::read(out.path()).unwrap()
}

#[test]
fn entries_within_the_ustar_fields_keep_the_contract_archive_bytes() {
    // ADR 0038: bundles of normal-size entries stay byte-stable. The vectors
    // are derived independently by `scripts/ci/portable-v2-contract.py`.
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/portable-v2/bundle-byte-vectors.json"
    ))
    .unwrap();
    let vectors = vectors["vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 3);
    for vector in vectors {
        let name = vector["name"].as_str().unwrap();
        let payload = (0..vector["payload_hex"].as_str().unwrap().len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&vector["payload_hex"].as_str().unwrap()[i..i + 2], 16).unwrap()
            })
            .collect::<Vec<_>>();
        let archive = single_entry_archive(vector["path"].as_str().unwrap(), &payload);
        assert_eq!(
            archive.len() as u64,
            vector["archive_length"].as_u64().unwrap(),
            "{name}"
        );
        assert_eq!(
            hex(sha2::Sha256::digest(&archive).into()),
            vector["archive_sha256"].as_str().unwrap(),
            "{name}"
        );
    }
}

#[test]
fn an_oversized_entry_with_a_long_path_carries_both_records() {
    let path = format!(
        "data/components/graph-data/core/{}",
        "b".repeat(crate::project_portable_v2::pax::PAX_RECORD_OVERHEAD_BYTES + 100)
    );
    assert!(
        split(&path).is_err(),
        "the path must not fit the ustar split"
    );
    let _limit = pax::test_seam::lower_ustar_size_limit(1);
    let payload = b"{}";
    let archive = single_entry_archive(&path, payload);
    let records = format!("{}{}", pax::record("path", &path), pax::record("size", "2"));
    assert_eq!(archive[156], b'x');
    assert_eq!(&archive[512..512 + records.len()], records.as_bytes());
    assert_eq!(
        pax::parse(&records).unwrap(),
        pax::PaxHeader {
            path: path.clone(),
            size: Some(2)
        }
    );
    let regular = 1024;
    assert_eq!(&archive[regular..regular + 9], b"PaxFiles/");
    assert_eq!(&archive[regular + 124..regular + 136], b"00000000000\0");
    assert_eq!(&archive[regular + 512..regular + 514], payload);
    assert_eq!(archive.len(), 4 * 512 + 1024);
}

#[test]
fn large_sparse_source_streams_densely_with_a_tiny_buffer() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("large.parquet");
    let file = File::create(&source).unwrap();
    file.set_len(32 * 1024 * 1024).unwrap();
    drop(file);
    let limits = PortableV2ExportLimits {
        copy_buffer_bytes: 11,
        ..Default::default()
    };
    let mut total = 0;
    let planned = inspect(
        &source,
        "data/components/graph-data/graph-files/large.parquet",
        limits,
        &mut total,
    )
    .unwrap();
    assert_eq!(total, 32 * 1024 * 1024);
    let destination = root.path().join("dense.parquet");
    let mut observed = 0;
    let mut allocation = ExportAllocationObserver::default();
    copy(
        &planned,
        &destination,
        limits.copy_buffer_bytes,
        &|| false,
        &mut allocation,
        |bytes| {
            observed += bytes;
        },
    )
    .unwrap();
    assert_eq!(observed, total);
    assert_eq!(fs::metadata(destination).unwrap().len(), total);
}

#[test]
fn portable_member_copy_counts_crypto_and_refuses_same_identity_content_mutation() {
    use graphforge_core::hash_observation::operation::Capture;

    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("member.parquet");
    let bytes = b"portable member payload";
    fs::write(&source, bytes).unwrap();
    let limits = PortableV2ExportLimits::default();
    let capture = Capture::start();
    let mut total = 0;
    let planned = inspect(
        &source,
        "data/components/graph-data/graph-files/member.parquet",
        limits,
        &mut total,
    )
    .unwrap();
    let healthy = root.path().join("healthy.parquet");
    let mut allocation = ExportAllocationObserver::default();
    copy(
        &planned,
        &healthy,
        limits.copy_buffer_bytes,
        &|| false,
        &mut allocation,
        |_| {},
    )
    .unwrap();
    let observed = capture.snapshot();
    drop(capture);
    assert_eq!(
        observed.portable_authentication_sha256_bytes,
        bytes.len() as u64
    );
    assert_eq!(observed.artifact_payload_sha256_bytes, 0);
    assert_eq!(observed.unclassified_sha256_bytes, 0);
    assert_eq!(fs::read(&healthy).unwrap(), bytes);
    assert_eq!(
        planned.digest,
        <[u8; 32]>::from(sha2::Sha256::digest(bytes))
    );

    let before = identity(&fs::metadata(&source).unwrap()).unwrap();
    let mut changed = bytes.to_vec();
    changed[0] ^= 1;
    fs::write(&source, &changed).unwrap();
    // Keep the metadata identity exactly as planned, so this refusal proves
    // captured-byte checksum refusal rather than only an mtime observation.
    OpenOptions::new()
        .write(true)
        .open(&source)
        .unwrap()
        .set_modified(before.modified.unwrap())
        .unwrap();
    assert_eq!(identity(&fs::metadata(&source).unwrap()).unwrap(), before);
    let capture = Capture::start();
    let error = copy(
        &planned,
        &root.path().join("refused.parquet"),
        limits.copy_buffer_bytes,
        &|| false,
        &mut allocation,
        |_| {},
    )
    .unwrap_err();
    let refused = capture.snapshot();
    drop(capture);
    assert_eq!(error.code, crate::PortableV2ErrorCode::ConcurrentMutation);
    assert_eq!(refused.portable_authentication_sha256_bytes, 0);
    assert!(refused.checksum_bytes >= bytes.len() as u64);
    assert_eq!(refused.artifact_payload_sha256_bytes, 0);
    assert_eq!(refused.unclassified_sha256_bytes, 0);
}
