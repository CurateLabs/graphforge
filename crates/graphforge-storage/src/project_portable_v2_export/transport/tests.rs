use super::super::USTAR_MAX_ENTRY_BYTES;
use super::super::identity;
use super::super::planning::{inspect, portable_id, portable_participant_id};
use super::*;

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
    let record = pax_path_record(&long);
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
    // The >16 GiB structural case is represented by multiple bounded
    // shards; no individual ustar size field requires a base-256 escape.
    header(&mut file, &mut digest, &long, 1_073_741_824).unwrap();
    let bytes = fs::read(out.path()).unwrap();
    assert_eq!(&bytes[..11], b"PaxHeaders/");
    assert_eq!(bytes[156], b'x');
    assert_eq!(bytes[1024..1033].as_ref(), b"PaxFiles/");
    assert_eq!(bytes[1024 + 156], b'0');
    assert!(oct(&mut [0; 12], USTAR_MAX_ENTRY_BYTES).is_ok());
    assert!(oct(&mut [0; 12], USTAR_MAX_ENTRY_BYTES + 1).is_err());
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
