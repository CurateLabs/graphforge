//! Portable export and import hash and copy each payload byte a bounded number of times (#1405).
//!
//! Hashing is measured by the operation-scoped SHA-256 counters; copying by the
//! process's read/write syscall byte counters (`/proc/self/io`), so this test
//! runs one operation at a time.
#![cfg(target_os = "linux")]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{FixedSizeBinaryBuilder, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    BulkInputKind, GraphForge, ImportSessionLimits, OperationId, PortableSelection,
    PortableV2ExportRequest, PortableV2ImportRequest, PortableV2Limits, PortableV2Output,
    PortableV2SelectionProfile, bulk_edge_input_schema, bulk_node_input_schema,
};
use graphforge_storage::payload_digest::{PayloadDigestCapture, PayloadDigestSnapshot};

const NODES: u128 = 2_000;

fn v7(counter: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000 | counter)
}

fn uuid_column(values: impl Iterator<Item = u128>) -> arrow::array::ArrayRef {
    let values = values.collect::<Vec<_>>();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        builder.append_value(v7(value).as_bytes()).unwrap();
    }
    Arc::new(builder.finish())
}

fn bulk_load(graph: &GraphForge, nodes: u128) {
    let node_batch = RecordBatch::try_new(
        bulk_node_input_schema(Vec::new()).unwrap(),
        vec![
            uuid_column(1..=nodes),
            Arc::new(StringArray::from(vec!["Person"; nodes as usize])),
        ],
    )
    .unwrap();
    let edges = nodes - 1;
    let edge_batch = RecordBatch::try_new(
        bulk_edge_input_schema(Vec::new()).unwrap(),
        vec![
            uuid_column((0..edges).map(|i| 1_000_000 + i)),
            Arc::new(StringArray::from(vec!["KNOWS"; edges as usize])),
            uuid_column((1..=edges).map(|i| i)),
            uuid_column((2..=nodes).map(|i| i)),
        ],
    )
    .unwrap();
    let mut session = graph
        .begin_import_session(
            OperationId(uuid::Uuid::now_v7()),
            ImportSessionLimits::default(),
        )
        .unwrap();
    session
        .append_arrow(BulkInputKind::Node, &[node_batch])
        .unwrap();
    session
        .append_arrow(BulkInputKind::Edge, &[edge_batch])
        .unwrap();
    session.validate(graph).unwrap();
    session.commit(graph, None).unwrap();
}

/// Bytes passed through read and write syscalls by this process so far.
fn syscall_bytes() -> (u64, u64) {
    let io = std::fs::read_to_string("/proc/self/io").unwrap();
    let field = |name: &str| {
        io.lines()
            .find_map(|line| line.strip_prefix(name))
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap()
    };
    (field("rchar:"), field("wchar:"))
}

struct Measured {
    work: PayloadDigestSnapshot,
    read: u64,
    written: u64,
}

fn measure<T>(operation: impl FnOnce() -> T) -> (T, Measured) {
    let (read_before, written_before) = syscall_bytes();
    let capture = PayloadDigestCapture::start();
    let value = operation();
    let work = capture.snapshot();
    drop(capture);
    let (read_after, written_after) = syscall_bytes();
    (
        value,
        Measured {
            work,
            read: read_after - read_before,
            written: written_after - written_before,
        },
    )
}

fn report(label: &str, package: u64, measured: &Measured) {
    let work = &measured.work;
    eprintln!(
        "{label}: package={package} read={} written={} artifact_sha={} portable_sha={} \
         control_sha={} contract_sha={} unclassified_sha={} checksum={}",
        measured.read,
        measured.written,
        work.artifact_payload_sha256_bytes,
        work.portable_authentication_sha256_bytes,
        work.control_authentication_sha256_bytes,
        work.contract_identity_sha256_bytes,
        work.unclassified_sha256_bytes,
        work.checksum_bytes,
    );
}

fn file_bytes(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

#[test]
fn portable_export_and_import_hash_and_copy_the_package_a_bounded_number_of_times() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let graph = GraphForge::new(source.to_str()).unwrap();
    bulk_load(&graph, NODES);
    let package = root.path().join("package.gfpb");
    let limits = PortableV2Limits::default();
    let (exported, export) = measure(|| {
        graph
            .export_portable_v2(
                &PortableV2ExportRequest {
                    selection: PortableSelection::Current,
                    output_path: package.clone(),
                    representation: PortableV2Output::Bundle,
                    profile: PortableV2SelectionProfile::Complete,
                    subset: None,
                    limits,
                },
                None,
                |_| {},
            )
            .unwrap()
    });
    let package_bytes = file_bytes(&package);
    report("export", package_bytes, &export);
    drop(graph);

    let target = root.path().join("target");
    let (_, import) = measure(|| {
        GraphForge::import_portable_v2(
            &target,
            &PortableV2ImportRequest {
                input: package.clone(),
                operation_id: OperationId(uuid::Uuid::now_v7()),
                limits,
            },
            None,
        )
        .unwrap()
    });
    report("import", package_bytes, &import);

    let imported = GraphForge::new(target.to_str()).unwrap();
    assert_eq!(
        imported.node_count("Person").unwrap(),
        u64::try_from(NODES).unwrap()
    );
    assert!(!exported.package_digest.is_empty());
    for (label, measured) in [("export", &export), ("import", &import)] {
        assert_eq!(measured.work.unclassified_sha256_bytes, 0, "{label}");
        // One pass computes the transport digest and each member's digest
        // over the same bytes; nothing re-hashes the package afterwards.
        assert!(
            measured.work.portable_authentication_sha256_bytes <= 2 * package_bytes,
            "{label}: portable SHA {} exceeds one authenticated pass over {package_bytes}",
            measured.work.portable_authentication_sha256_bytes
        );
    }
    // Export writes the package once; there is no staging copy to re-verify.
    assert!(
        export.written <= package_bytes,
        "export wrote {} for a {package_bytes}-byte package",
        export.written
    );
    // Import writes each component once into its authenticated private stage
    // and once into the graph object store, plus bounded control records.
    assert!(
        import.written <= 2 * package_bytes + 64 * 1024,
        "import wrote {} for a {package_bytes}-byte package",
        import.written
    );
}
