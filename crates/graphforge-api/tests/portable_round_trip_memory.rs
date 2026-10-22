//! Peak live heap of a portable export and import of one project whose payload
//! is many times any fixed buffer (#1769), through the public facade.
//!
//! A counting global allocator measures every live byte the process holds while
//! one export (or import) runs. Both operations keep the payload on disk, so
//! the only way the peak can scale with the payload is the library buffering
//! it. The assertion is the fixed bound below, never a ratio between runs or a
//! pinned measured number.
//!
//! No public API attaches one opaque artifact blob to a project, so the
//! payload is dominated by graph data: `Doc` nodes whose pseudo-random `body`
//! string properties carry the bytes, bulk-loaded through the public import
//! session.
#![allow(
    unsafe_code,
    reason = "a counting global allocator wraps System and only observes sizes"
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use arrow::array::{FixedSizeBinaryArray, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    BulkInputKind, GRAPHFORGE_VERSION, GraphForge, ImportSessionLimits, OperationId,
    PortableExportRequest, PortableImportRequest, PortableMode, PortableSelection,
    PortableVerifyRequest, bulk_node_input_schema, verify_portable,
};
use graphforge_core::portable::{PortableLimits, PortableOutput, PortableSelectionProfile};
use parquet::arrow::ArrowWriter;
use std::sync::Arc;
use uuid::Uuid;

struct Counting;
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let now = CURRENT.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

// SAFETY: every method defers to `System`, which upholds the `GlobalAlloc`
// contract; the counters only observe sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new) };
        if !moved.is_null() {
            // A reallocation may copy, so the old and the new block are both
            // live at once; count it that way instead of assuming it is in place.
            grow(new);
            CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The counters are process-wide, so the tests here do not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Run `operation`; return its result and the most bytes it held live at once
/// above where it started.
fn peak_live_bytes<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    let baseline = CURRENT.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let result = operation();
    (
        result,
        PEAK.load(Ordering::Relaxed).saturating_sub(baseline),
    )
}

/// `length` deterministic letters, the same generator the OCI peak test uses.
/// The stored payload stays one order of magnitude above the bound below.
fn blob(length: usize) -> String {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

/// A durable project whose payload is `blob_bytes` of graph data: no public
/// API attaches one opaque artifact blob, so the bytes are spread over `Doc`
/// nodes whose pseudo-random `body` properties carry them, bulk-loaded in one
/// committed import session.
fn project_with_blob(root: &Path, blob_bytes: usize) {
    let nodes = 32;
    let per_node = blob_bytes / nodes;
    let ids: Vec<Uuid> = (0..nodes).map(|_| Uuid::now_v7()).collect();
    let batch = RecordBatch::try_new(
        bulk_node_input_schema(vec![arrow::datatypes::Field::new(
            "body",
            arrow::datatypes::DataType::Utf8,
            true,
        )])
        .unwrap(),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_bytes().as_slice()))
                    .unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Doc"; nodes])),
            Arc::new(StringArray::from(
                (0..nodes).map(|_| blob(per_node)).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let source = root.join("nodes.parquet");
    let mut writer =
        ArrowWriter::try_new(fs::File::create(&source).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let graph = GraphForge::new(root.join("source").to_str()).unwrap();
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    session
        .register_parquet(BulkInputKind::Node, &source)
        .unwrap();
    session.validate(&graph).unwrap();
    session.commit(&graph, None).unwrap();
}

/// Export the current selection as a bundle and import it into a fresh
/// durable project, reporting each operation's peak live heap separately.
fn round_trip(root: &Path, operation_uuid: u128) -> (u64, usize, usize, String) {
    let limits = PortableLimits::default();
    let graph = GraphForge::new(root.join("source").to_str()).unwrap();
    let package = root.join("project.gfpb");
    let (exported, export_peak) = peak_live_bytes(|| {
        graph
            .export_portable(
                &PortableExportRequest {
                    selection: PortableSelection::Current,
                    output_path: package.clone(),
                    representation: PortableOutput::Bundle,
                    profile: PortableSelectionProfile::Complete,
                    subset: None,
                    limits,
                },
                None,
                |_| {},
            )
            .unwrap()
    });
    drop(graph);

    // Round trip and producer identity, outside the measured windows.
    let verified = verify_portable(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: PortableMode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert_eq!(verified.package_digest, exported.package_digest);
    assert_eq!(verified.producing_core_version, GRAPHFORGE_VERSION);

    let (imported, import_peak) = peak_live_bytes(|| {
        GraphForge::import_portable(
            &root.join("destination"),
            &PortableImportRequest {
                input: package.clone(),
                operation_id: OperationId(Uuid::from_u128(operation_uuid)),
                limits,
            },
            None,
        )
        .unwrap()
    });
    assert_eq!(imported.package_digest, exported.package_digest);
    assert!(!imported.idempotent_replay);
    assert!(
        imported.warnings.is_empty(),
        "a package from the running Core warns of nothing: {:?}",
        imported.warnings
    );
    let reopened = GraphForge::new(root.join("destination").to_str()).unwrap();
    assert_eq!(reopened.node_count("Doc").unwrap(), NODES as u64);

    (
        exported.payload_bytes,
        export_peak,
        import_peak,
        exported.package_digest,
    )
}

#[test]
fn export_and_import_peak_heap_stays_bounded_as_the_payload_grows() {
    let _serial = serial();
    let mut small = None;
    let mut large = None;
    for (blob_bytes, operation_uuid) in [
        (8 * 1024 * 1024, 0x1769_0000_0000_0001_u128),
        (256 * 1024 * 1024, 0x1769_0000_0000_0002_u128),
    ] {
        let root = tempfile::tempdir().unwrap();
        project_with_blob(root.path(), blob_bytes);
        let (payload_bytes, export_peak, import_peak, package_digest) =
            round_trip(root.path(), operation_uuid);
        eprintln!(
            "blob_bytes={blob_bytes} payload_bytes={payload_bytes} \
             export_peak={export_peak} import_peak={import_peak} \
             package_digest={package_digest}"
        );
        if blob_bytes == LARGE_BLOB_BYTES {
            large = Some((payload_bytes, export_peak, import_peak));
        } else {
            small = Some((payload_bytes, export_peak, import_peak));
        }
    }
    let (small_payload, small_export_peak, small_import_peak) = small.unwrap();
    let (large_payload, large_export_peak, large_import_peak) = large.unwrap();

    // The invariant: at the large size, each operation's peak live heap stays
    // below a fixed bound far under the payload it streams through.
    assert!(
        large_export_peak < PEAK_BOUND_BYTES,
        "export held {large_export_peak} bytes"
    );
    assert!(
        large_import_peak < PEAK_BOUND_BYTES,
        "import held {large_import_peak} bytes"
    );
    // The bound only means something while the payload dwarfs it.
    assert!(large_payload > u64::try_from(2 * PEAK_BOUND_BYTES).unwrap());
    // The same bound holds at the small size, where it is trivially true.
    assert!(small_export_peak < PEAK_BOUND_BYTES);
    assert!(small_import_peak < PEAK_BOUND_BYTES);
    // Both operations stream the payload rather than scaling with it, so the
    // small run already meets the bound the large run is held to.
    assert!(small_payload < large_payload / 4);
}

const NODES: usize = 32;
const LARGE_BLOB_BYTES: usize = 256 * 1024 * 1024;
const PEAK_BOUND_BYTES: usize = 64 * 1024 * 1024;
