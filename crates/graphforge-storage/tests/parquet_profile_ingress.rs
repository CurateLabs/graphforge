//! A correctly hashed package carrying a Parquet page outside the funded
//! encoding profile is refused at every ingress, before any decode.
//!
//! Parquet 58.4's `DELTA_LENGTH_BYTE_ARRAY` decoder resizes a vector from the
//! delta count embedded in the page before it reads any length, so the control
//! below shows the hostile bytes really do allocate hundreds of MiB when
//! decoded, while verification and import allocate nothing of the kind.
#![allow(unsafe_code)]

#[path = "support/hostile_parquet.rs"]
mod hostile;

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use graphforge_storage::{
    PortableErrorCode, PortableLimits, PortableMode, PortableOutput, ProjectCapability,
    ProjectGenerationRequest, ProjectStageOutcome, ResolvedProjectGeneration, capture_graph_files,
    empty_workspace_participants, export_complete_portable, import_complete_portable,
    open_or_initialize_project, plan_complete_portable, resolve_project_generation,
    stage_project_generation_with_graph_tree, verify_portable,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;
use sha2::Digest;
use uuid::Uuid;

static TRACK: AtomicBool = AtomicBool::new(false);
static LARGEST: AtomicUsize = AtomicUsize::new(0);

struct Counting;

fn record(size: usize) {
    if TRACK.load(Ordering::Relaxed) {
        LARGEST.fetch_max(size, Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }
}

/// Run `work` and return its result with the largest single allocation it made.
fn tracked<T>(work: impl FnOnce() -> T) -> (T, usize) {
    LARGEST.store(0, Ordering::Relaxed);
    TRACK.store(true, Ordering::Relaxed);
    let result = work();
    TRACK.store(false, Ordering::Relaxed);
    (result, LARGEST.load(Ordering::Relaxed))
}

/// A committed generation whose graph tree holds `a.parquet` with `bytes`.
fn generation_with_graph_file(bytes: &[u8]) -> (tempfile::TempDir, ResolvedProjectGeneration) {
    let project = tempfile::tempdir().unwrap();
    let parent = open_or_initialize_project(project.path()).unwrap();
    let tree = tempfile::tempdir().unwrap();
    std::fs::write(tree.path().join("a.parquet"), bytes).unwrap();
    let (_, inventory) = capture_graph_files(tree.path()).unwrap();
    let mut participants = empty_workspace_participants().unwrap();
    participants.insert(0, inventory);
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::new_v4(),
        generation_uuid: Uuid::new_v4(),
        capabilities: vec![
            ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(project.path(), &request, Some(tree.path()))
            .unwrap()
    else {
        panic!("fresh graph generation replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    drop(parent);
    let generation = resolve_project_generation(project.path()).unwrap();
    (project, generation)
}

const GRAPH_FILE: &str = "data/components/graph-data/graph-tree/a.parquet";

fn hex(bytes: impl AsRef<[u8]>) -> String {
    use std::fmt::Write;
    sha2::Sha256::digest(bytes.as_ref())
        .iter()
        .fold(String::new(), |mut text, byte| {
            write!(text, "{byte:02x}").unwrap();
            text
        })
}

fn replace_text(value: &mut serde_json::Value, from: &str, to: &str) {
    match value {
        serde_json::Value::String(text) if text == from => *text = to.to_owned(),
        serde_json::Value::Array(items) => items.iter_mut().for_each(|v| replace_text(v, from, to)),
        serde_json::Value::Object(map) => map.values_mut().for_each(|v| replace_text(v, from, to)),
        _ => {}
    }
}

/// An exported expanded package whose `a.parquet` is replaced by `hostile`, with
/// the entry digest, manifest, package digest, inventory and tag manifest all
/// re-signed so that only the encoding profile can refuse it. GraphForge's own
/// export verifies what it writes, so it will not produce such a package.
fn hostile_package(
    hostile: &[u8],
    directory: &Path,
) -> (PathBuf, ResolvedProjectGeneration, tempfile::TempDir) {
    // Same length keeps every size field of the package valid.
    let placeholder = vec![b'x'; hostile.len()];
    let (project, generation) = generation_with_graph_file(&placeholder);
    let plan = plan_complete_portable(&generation, Default::default()).unwrap();
    let root = directory.join("hostile.gfproject");
    export_complete_portable(
        &plan,
        &root,
        PortableOutput::Expanded,
        Default::default(),
        &AtomicBool::new(false),
        |_| {},
    )
    .unwrap();
    let old_entry = hex(&placeholder);
    std::fs::write(root.join(GRAPH_FILE), hostile).unwrap();

    let manifest_path = root.join("data/graphforge-project.json");
    let old_manifest_bytes = std::fs::read(&manifest_path).unwrap();
    let mut manifest: serde_json::Value = serde_json::from_slice(&old_manifest_bytes).unwrap();
    assert_eq!(
        serde_json::to_vec(&manifest).unwrap(),
        old_manifest_bytes,
        "the manifest must be reproducible to be re-signed"
    );
    replace_text(&mut manifest, &old_entry, &hex(hostile));
    manifest.as_object_mut().unwrap().remove("package_digest");
    let semantic = serde_json::to_vec(&manifest).unwrap();
    let digest = hex([b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat());
    manifest
        .as_object_mut()
        .unwrap()
        .insert("package_digest".into(), format!("sha256:{digest}").into());
    let new_manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    std::fs::write(&manifest_path, &new_manifest_bytes).unwrap();

    let inventory_path = root.join("manifest-sha256.txt");
    let inventory = std::fs::read_to_string(&inventory_path)
        .unwrap()
        .replace(&old_entry, &hex(hostile))
        .replace(&hex(&old_manifest_bytes), &hex(&new_manifest_bytes));
    std::fs::write(&inventory_path, &inventory).unwrap();
    let tags = ["bag-info.txt", "bagit.txt", "manifest-sha256.txt"]
        .into_iter()
        .map(|tag| format!("{}  {tag}\n", hex(std::fs::read(root.join(tag)).unwrap())))
        .collect::<String>();
    std::fs::write(root.join("tagmanifest-sha256.txt"), tags).unwrap();
    (root, generation, project)
}

fn verify(package: &Path) -> Result<(), graphforge_storage::PortableError> {
    verify_portable(
        package,
        PortableMode::Full,
        PortableLimits::default(),
        None,
    )
    .map(|_| ())
}

fn import(
    package: &Path,
    target: &Path,
    generation: &ResolvedProjectGeneration,
) -> Result<(), graphforge_storage::PortableError> {
    let supported = generation
        .capabilities()
        .into_iter()
        .map(|capability| ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect::<Vec<_>>();
    import_complete_portable(
        package,
        target,
        Uuid::new_v4(),
        Uuid::new_v4(),
        &supported,
        PortableLimits::default(),
        None,
    )
    .map(|_| ())
}

/// Largest allocation any ingress may make; the hostile count is 256 MiB.
const INGRESS_ALLOCATION_BOUND: usize = 64 << 20;
const HOSTILE_COUNT: u64 = 1 << 26;

#[test]
fn unfunded_parquet_pages_are_refused_at_verify_and_import_before_any_decode() {
    let delta = hostile::string_file(
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        WriterVersion::PARQUET_1_0,
        64,
    );
    let mut hostile_count = hostile::single_page_delta_length(2_100_000);
    hostile::inflate_embedded_count(&mut hostile_count, HOSTILE_COUNT);
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("delta length", delta.clone()),
        (
            "delta byte array",
            hostile::string_file(Encoding::DELTA_BYTE_ARRAY, WriterVersion::PARQUET_1_0, 64),
        ),
        (
            "delta length v2 pages",
            hostile::string_file(
                Encoding::DELTA_LENGTH_BYTE_ARRAY,
                WriterVersion::PARQUET_2_0,
                64,
            ),
        ),
        (
            "footer declares plain",
            hostile::declaring(&delta, &[Encoding::PLAIN, Encoding::RLE]),
        ),
        (
            "footer declares plain over an inflated embedded count",
            hostile::declaring(&hostile_count, &[Encoding::PLAIN, Encoding::RLE]),
        ),
        ("inflated embedded count", hostile_count.clone()),
    ];

    // Known positive: decoding the hostile bytes really does allocate the count.
    let (_, control) = tracked(|| {
        // Only the first batch: after the allocation it fails, and a failed
        // reader may keep yielding errors forever.
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(hostile_count.clone()))
            .unwrap()
            .build()
            .unwrap()
            .next()
    });
    assert!(
        control >= (HOSTILE_COUNT as usize) * 4,
        "the control decode must allocate the embedded count, saw {control} bytes"
    );

    for (name, bytes) in cases {
        let directory = tempfile::tempdir().unwrap();
        let (package, generation, _project) = hostile_package(&bytes, directory.path());
        let (verified, largest) = tracked(|| verify(&package));
        let error = verified.expect_err(name);
        assert_eq!(
            error.code,
            PortableErrorCode::LimitExceeded,
            "{name}: {error}"
        );
        assert!(
            error.to_string().contains("funded decoder profile"),
            "{name}: {error}"
        );
        assert_eq!(error.entry.as_deref(), Some(GRAPH_FILE), "{name}");
        assert!(
            largest < INGRESS_ALLOCATION_BOUND,
            "{name} verify allocated {largest} bytes"
        );

        let target = directory.path().join("imported");
        let (imported, largest) = tracked(|| import(&package, &target, &generation));
        let error = imported.expect_err(name);
        assert_eq!(
            error.code,
            PortableErrorCode::LimitExceeded,
            "{name}: {error}"
        );
        assert!(
            largest < INGRESS_ALLOCATION_BOUND,
            "{name} import allocated {largest} bytes"
        );
        assert!(
            !target.exists(),
            "{name}: a refused import must leave no target"
        );
    }
}

#[test]
fn a_funded_graph_parquet_still_verifies_after_the_same_re_signing() {
    let funded = hostile::string_file(Encoding::PLAIN, WriterVersion::PARQUET_1_0, 64);
    let directory = tempfile::tempdir().unwrap();
    let (package, _generation, _project) = hostile_package(&funded, directory.path());
    verify(&package).unwrap();
}
