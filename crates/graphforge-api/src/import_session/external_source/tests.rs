//! In-place Parquet sources: nothing is copied, and every read refuses a source
//! that no longer matches its registration (#1898).

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use graphforge_core::{ApiErrorCode, GfError};
use parquet::arrow::ArrowWriter;
use sha2::Digest as _;
use uuid::Uuid;

use super::{SourceDigest, hex};
use crate::import_session::test_fixtures::{edges, fixture, nodes, seeded_fixture};
use crate::import_session::{
    BuildRoute, GraphImportSession, ImportPhase, ImportSessionLimits, ImportSourceKind,
};
use crate::{BulkInputKind, GraphForge, OperationId};

const BATCH_ROWS: usize = 2;
const NODE_ROWS: usize = 6;

/// An initial import builds on the bulk builder; an append stages chunk by chunk.
/// Every refusal below must hold on both construction paths.
#[derive(Clone, Copy, Debug)]
enum Route {
    Staged,
    Bulk,
}

const ROUTES: [Route; 2] = [Route::Staged, Route::Bulk];

fn fixture_for(route: Route) -> (tempfile::TempDir, PathBuf, GraphForge) {
    match route {
        Route::Staged => seeded_fixture(),
        Route::Bulk => fixture(),
    }
}

fn assert_route(session: &GraphImportSession, route: Route) {
    assert_eq!(
        session.manifest.build_route,
        Some(match route {
            Route::Staged => BuildRoute::Staged,
            Route::Bulk => BuildRoute::Bulk,
        })
    );
}

fn sha256(bytes: &[u8]) -> String {
    hex(&sha2::Sha256::digest(bytes))
}

fn write_nodes(path: &Path, ids: &[Uuid]) {
    let batch = nodes(ids);
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn limits() -> ImportSessionLimits {
    ImportSessionLimits {
        batch_rows: BATCH_ROWS,
        ..ImportSessionLimits::default()
    }
}

struct Source {
    _directory: tempfile::TempDir,
    path: PathBuf,
    ids: Vec<Uuid>,
}

fn source() -> Source {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nodes.parquet");
    let ids = (0..NODE_ROWS).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
    write_nodes(&path, &ids);
    Source {
        _directory: directory,
        path,
        ids,
    }
}

fn begin(graph: &GraphForge) -> GraphImportSession {
    graph
        .begin_import_session(OperationId(Uuid::now_v7()), limits())
        .unwrap()
}

fn api_error(error: &GfError) -> (ApiErrorCode, String) {
    match error {
        GfError::Api { code, message } => (*code, message.clone()),
        other => panic!("expected a typed API error, found {other:?}"),
    }
}

/// A way to change a registered file, and the refusal it must produce.
struct Change {
    name: &'static str,
    apply: fn(&Path),
    code: ApiErrorCode,
    reason: &'static str,
}

fn set_mtime(path: &Path, delta: i64) {
    let file = OpenOptions::new().write(true).open(path).unwrap();
    let current = fs::metadata(path).unwrap().modified().unwrap();
    let moved = if delta >= 0 {
        current + Duration::from_secs(delta.unsigned_abs())
    } else {
        current - Duration::from_secs(delta.unsigned_abs())
    };
    file.set_modified(moved).unwrap();
}

const CHANGES: [Change; 5] = [
    Change {
        name: "deleted",
        apply: |path| fs::remove_file(path).unwrap(),
        code: ApiErrorCode::NotFound,
        reason: "is missing",
    },
    Change {
        name: "replaced",
        apply: |path| {
            let bytes = fs::read(path).unwrap();
            let replacement = path.with_extension("replacement");
            fs::write(&replacement, bytes).unwrap();
            fs::rename(replacement, path).unwrap();
        },
        code: ApiErrorCode::IdentityConflict,
        reason: "was replaced",
    },
    Change {
        name: "truncated",
        apply: |path| {
            let length = fs::metadata(path).unwrap().len();
            OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_len(length - 4)
                .unwrap();
        },
        code: ApiErrorCode::IdentityConflict,
        reason: "was resized",
    },
    Change {
        name: "appended",
        apply: |path| {
            OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap()
                .write_all(b"tail")
                .unwrap();
        },
        code: ApiErrorCode::IdentityConflict,
        reason: "was resized",
    },
    Change {
        name: "touched",
        apply: |path| set_mtime(path, 7),
        code: ApiErrorCode::IdentityConflict,
        reason: "was modified",
    },
];

fn assert_refusal(error: &GfError, change: &Change, context: &str) {
    let (code, message) = api_error(error);
    assert_eq!(code, change.code, "{} {context}: {message}", change.name);
    assert!(
        message.contains(change.reason),
        "{} {context}: {message}",
        change.name
    );
}

#[test]
fn digest_equals_sha256_for_any_read_order() {
    let bytes = (0..(3 << 20))
        .map(|index: u32| index.wrapping_mul(2_654_435_761).to_le_bytes()[2])
        .collect::<Vec<u8>>();
    let expected = sha256(&bytes);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blob");
    fs::write(&path, &bytes).unwrap();
    let file = File::open(&path).unwrap();
    let length = bytes.len() as u64;
    let identity = fake_source(&path, length);

    let ranges = |size: usize| -> Vec<(usize, usize)> {
        (0..bytes.len())
            .step_by(size)
            .map(|start| (start, (start + size).min(bytes.len())))
            .collect()
    };
    let digest_of = |observe: &dyn Fn(&SourceDigest)| {
        let digest = SourceDigest::new(length);
        observe(&digest);
        digest.finish(&identity, &file).unwrap()
    };
    // In order, as a sequential decode reads.
    assert_eq!(
        digest_of(&|d| ranges(100_003)
            .iter()
            .for_each(|&(a, b)| d.observe(a as u64, &bytes[a..b]))),
        expected
    );
    // Footer first, then the body in order: the shape of a Parquet decode.
    assert_eq!(
        digest_of(&|d| {
            let tail = bytes.len() - 4_000;
            d.observe(tail as u64, &bytes[tail..]);
            for &(a, b) in ranges(65_536).iter().filter(|&&(a, _)| a > 4 && a < tail) {
                d.observe(a as u64, &bytes[a..b.min(tail)]);
            }
        }),
        expected
    );
    // Reversed, overlapping, repeated and entirely unobserved: all still exact.
    assert_eq!(
        digest_of(&|d| ranges(70_001).iter().rev().for_each(|&(a, b)| {
            let from = a.saturating_sub(10);
            d.observe(from as u64, &bytes[from..b]);
        })),
        expected
    );
    assert_eq!(digest_of(&|_| ()), expected);
    assert_eq!(
        digest_of(&|d| {
            d.observe(0, &bytes[..10]);
            d.observe(0, &bytes[..10]);
            d.observe(5, &bytes[5..2_000]);
        }),
        expected
    );
}

#[test]
fn digest_rereads_ranges_dropped_beyond_the_pending_bound() {
    let bytes = (0..(1 << 20))
        .map(|index: u32| index as u8)
        .collect::<Vec<u8>>();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blob");
    fs::write(&path, &bytes).unwrap();
    let file = File::open(&path).unwrap();
    let identity = fake_source(&path, bytes.len() as u64);
    // Room for two 64 KiB ranges only; the rest of a reversed read is dropped.
    let digest = SourceDigest::with_pending_limit(bytes.len() as u64, 128 << 10);
    for start in (0..bytes.len()).step_by(65_536).rev() {
        digest.observe(start as u64, &bytes[start..start + 65_536]);
    }
    assert!(digest.state().pending_bytes <= 128 << 10);
    assert_eq!(digest.finish(&identity, &file).unwrap(), sha256(&bytes));
    // Ranges kept near the prefix were hashed in order; only the dropped ones
    // were read again, and nothing more.
    let reread = digest.reread_bytes();
    assert!(
        reread > 0 && reread < bytes.len() as u64,
        "re-read {reread} of {} bytes",
        bytes.len()
    );
}

#[test]
fn a_streamed_column_chunk_is_held_as_one_range() {
    let bytes = (0..(1 << 20))
        .map(|index: u32| (index >> 3) as u8)
        .collect::<Vec<u8>>();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blob");
    fs::write(&path, &bytes).unwrap();
    let file = File::open(&path).unwrap();
    let identity = fake_source(&path, bytes.len() as u64);
    let digest = SourceDigest::new(bytes.len() as u64);
    // A later column chunk streams in page-sized reads while the first is not
    // done: one held run, not one entry per read.
    for start in (600_000..900_000).step_by(1_000) {
        digest.observe(start as u64, &bytes[start..start + 1_000]);
    }
    {
        let state = digest.state();
        assert_eq!(state.pending.len(), 1);
        assert_eq!(state.pending_bytes, 300_000);
        assert_eq!(state.hashed, 0);
    }
    // The first chunk then arrives and the run is hashed in order behind it.
    for start in (0..600_000).step_by(4_096) {
        digest.observe(start as u64, &bytes[start..(start + 4_096).min(600_000)]);
    }
    assert_eq!(digest.state().hashed, 900_000);
    assert_eq!(digest.state().pending_bytes, 0);
    assert_eq!(digest.finish(&identity, &file).unwrap(), sha256(&bytes));
    // Only the tail nothing observed was read again.
    assert_eq!(digest.reread_bytes(), (bytes.len() - 900_000) as u64);
}

#[test]
fn the_bound_on_held_bytes_follows_the_workers_and_the_largest_task() {
    use super::pending_limit;
    let floor = 64 << 20;
    assert_eq!(pending_limit(16, 1 << 20), floor);
    assert_eq!(pending_limit(16, 10 << 20), 160 << 20);
    assert_eq!(pending_limit(64, 100 << 20), 1 << 30);
    assert_eq!(pending_limit(1, u64::MAX), 1 << 30);
    assert_eq!(pending_limit(0, u64::MAX), floor);
}

#[test]
fn digest_refuses_a_read_past_the_registered_size() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blob");
    fs::write(&path, [1_u8; 100]).unwrap();
    let file = File::open(&path).unwrap();
    let identity = fake_source(&path, 100);
    let digest = SourceDigest::new(100);
    digest.observe(90, &[1_u8; 20]);
    let error = digest.finish(&identity, &file).unwrap_err();
    assert_eq!(api_error(&error).0, ApiErrorCode::IdentityConflict);

    // A file shorter than registered is a resize too, not an I/O mystery.
    let digest = SourceDigest::new(200);
    let error = digest.finish(&fake_source(&path, 200), &file).unwrap_err();
    assert!(api_error(&error).1.contains("was resized"));
}

fn fake_source(path: &Path, size: u64) -> super::ExternalSource {
    super::ExternalSource {
        path: path.to_path_buf(),
        volume_serial: 0,
        file_id: String::new(),
        size,
        mtime_secs: 0,
        mtime_nanos: 0,
        footer_len: 0,
        footer_sha256: String::new(),
    }
}

fn registration_copies_and_writes_no_source_bytes_on(route: Route) {
    let (_directory, _project, graph) = fixture_for(route);
    // Large enough that a copy would dwarf the manifest and journal writes.
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nodes.parquet");
    let ids = (0..40_000).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
    write_nodes(&path, &ids);
    let size = fs::metadata(&path).unwrap().len();
    assert!(
        size > 512 << 10,
        "fixture is too small to distinguish a copy"
    );

    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    let capture = graphforge_storage::concurrency_attribution::RegionCapture::start("test");
    session
        .register_parquet(BulkInputKind::Node, &path)
        .unwrap();
    let snapshot = capture.finish();
    let row = &snapshot.regions["test/register_parquet"];
    assert_eq!(row.work["bytes"], size);
    if let Some(written) = row.inclusive.written_bytes {
        assert!(
            written < 64 << 10,
            "registration wrote {written} bytes for a {size}-byte source"
        );
    }

    let sources = session.root.join("sources");
    assert_eq!(fs::read_dir(&sources).unwrap().count(), 0);
    session.validate(&graph).unwrap();
    assert_eq!(fs::read_dir(&sources).unwrap().count(), 0);
    let session_bytes = walk_bytes(&session.root);
    assert!(
        session_bytes < size / 4,
        "the session holds {session_bytes} bytes for a {size}-byte source"
    );
    session.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 40_000);
}

#[test]
fn registration_copies_and_writes_no_source_bytes() {
    for route in ROUTES {
        registration_copies_and_writes_no_source_bytes_on(route);
    }
}

fn walk_bytes(root: &Path) -> u64 {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                walk_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}

fn registration_still_refuses_unsafe_and_oversized_sources_on(route: Route) {
    let (_directory, _project, graph) = fixture_for(route);
    let source = source();
    let mut session = begin(&graph);

    assert!(
        session
            .register_parquet(BulkInputKind::Node, Path::new("../escape.parquet"))
            .is_err()
    );
    assert!(
        session
            .register_parquet(
                BulkInputKind::Node,
                &source.path.parent().unwrap().join("..").join("x.parquet")
            )
            .is_err()
    );
    assert!(
        session
            .register_parquet(BulkInputKind::Node, source.path.parent().unwrap())
            .is_err(),
        "a directory is not a regular file"
    );
    #[cfg(unix)]
    {
        let link = source.path.with_extension("link");
        std::os::unix::fs::symlink(&source.path, &link).unwrap();
        assert!(
            session
                .register_parquet(BulkInputKind::Node, &link)
                .is_err(),
            "a symlink is refused"
        );
    }
    let not_parquet = source.path.with_extension("txt");
    fs::write(
        &not_parquet,
        b"not parquet at all, but long enough to have a trailer",
    )
    .unwrap();
    assert!(
        session
            .register_parquet(BulkInputKind::Node, &not_parquet)
            .is_err(),
        "registration reads the footer, so a non-Parquet file is refused up front"
    );
    assert_eq!(session.status().1.files_accepted, 0);

    let mut small = graph
        .begin_import_session(
            OperationId(Uuid::now_v7()),
            ImportSessionLimits {
                max_source_bytes: 8,
                ..limits()
            },
        )
        .unwrap();
    assert!(
        small
            .register_parquet(BulkInputKind::Node, &source.path)
            .is_err()
    );
    let mut single = graph
        .begin_import_session(
            OperationId(Uuid::now_v7()),
            ImportSessionLimits {
                max_files: 1,
                ..limits()
            },
        )
        .unwrap();
    single
        .register_parquet(BulkInputKind::Node, &source.path)
        .unwrap();
    assert!(
        single
            .register_parquet(BulkInputKind::Node, &source.path)
            .is_err()
    );
}

#[test]
fn registration_still_refuses_unsafe_and_oversized_sources() {
    for route in ROUTES {
        registration_still_refuses_unsafe_and_oversized_sources_on(route);
    }
}

fn manifest_and_receipt_carry_the_digest_of_the_bytes_that_were_read_on(route: Route) {
    let (directory, _project, graph) = fixture_for(route);
    let source = source();
    let mut session = begin(&graph);
    let session_uuid = session.session_uuid();
    session
        .register_parquet(BulkInputKind::Node, &source.path)
        .unwrap();
    let external = session.manifest.sources[0].external.clone().unwrap();
    assert_eq!(external.path, fs::canonicalize(&source.path).unwrap());
    assert_eq!(external.size, fs::metadata(&source.path).unwrap().len());
    assert!(
        session.manifest.sources[0].sha256.is_none(),
        "the digest comes from the read pass"
    );

    let progress = session.validate(&graph).unwrap();
    assert_route(&session, route);
    let expected = sha256(&fs::read(&source.path).unwrap());
    assert_eq!(
        session.manifest.sources[0].sha256.as_deref(),
        Some(expected.as_str())
    );
    let provenance = &progress.construction.unwrap().source_provenance;
    assert_eq!(provenance.len(), 1);
    assert_eq!(provenance[0].kind, ImportSourceKind::ParquetNodes);
    assert_eq!(provenance[0].sha256.as_deref(), Some(expected.as_str()));
    assert_eq!(
        provenance[0].footer_sha256.as_deref(),
        Some(external.footer_sha256.as_str())
    );

    // Provenance is durable: a fresh process sees the same receipt.
    session.commit(&graph, None).unwrap();
    drop(session);
    drop(graph);
    let graph = GraphForge::new(directory.path().join("project").to_str()).unwrap();
    let (phase, progress) = graph.import_session_status(session_uuid).unwrap();
    assert_eq!(phase, ImportPhase::Committed);
    let provenance = progress.construction.unwrap().source_provenance;
    assert_eq!(provenance[0].sha256.as_deref(), Some(expected.as_str()));
    assert_eq!(graph.node_count("Person").unwrap(), NODE_ROWS as u64);
}

#[test]
fn manifest_and_receipt_carry_the_digest_of_the_bytes_that_were_read() {
    for route in ROUTES {
        manifest_and_receipt_carry_the_digest_of_the_bytes_that_were_read_on(route);
    }
}

fn each_change_between_registration_and_validate_is_refused_on(route: Route) {
    for change in &CHANGES {
        let (_directory, _project, graph) = fixture_for(route);
        let source = source();
        let mut session = begin(&graph);
        session
            .register_parquet(BulkInputKind::Node, &source.path)
            .unwrap();
        let session_uuid = session.session_uuid();
        drop(session);
        (change.apply)(&source.path);

        let mut session = graph.resume_import_session(session_uuid).unwrap();
        let error = session.validate(&graph).unwrap_err();
        assert_refusal(&error, change, "before validate");
        assert_eq!(session.status().1.rows_accepted, 0, "{}", change.name);
        assert_eq!(graph.node_count("Person").unwrap(), 0);
    }
}

#[test]
fn each_change_between_registration_and_validate_is_refused() {
    for route in ROUTES {
        each_change_between_registration_and_validate_is_refused_on(route);
    }
}

fn each_change_during_the_build_is_refused_on(route: Route) {
    for change in &CHANGES {
        let (_directory, _project, graph) = fixture_for(route);
        let source = source();
        let mut session = begin(&graph);
        session
            .register_parquet(BulkInputKind::Node, &source.path)
            .unwrap();
        let path = source.path.clone();
        let apply = change.apply;
        super::set_pass_hook(&source.path, move |stage, index| {
            if stage == "batch" && index == 1 {
                apply(&path);
            }
        });
        let error = session.validate(&graph).unwrap_err();
        super::clear_pass_hook(&source.path);
        assert_refusal(&error, change, "during the build");
        assert!(
            session.manifest.sources[0].sha256.is_none(),
            "{}",
            change.name
        );
        assert!(!session.manifest.sources[0].staged);
        assert_eq!(graph.node_count("Person").unwrap(), 0);
    }
}

#[test]
fn each_change_during_the_build_is_refused() {
    for route in ROUTES {
        each_change_during_the_build_is_refused_on(route);
    }
}

fn a_touched_source_is_readable_again_once_it_matches_its_registration_on(route: Route) {
    let (_directory, _project, graph) = fixture_for(route);
    let source = source();
    let mut session = begin(&graph);
    session
        .register_parquet(BulkInputKind::Node, &source.path)
        .unwrap();
    let session_uuid = session.session_uuid();
    let original = fs::metadata(&source.path).unwrap().modified().unwrap();
    set_mtime(&source.path, 30);
    assert!(session.validate(&graph).is_err());
    drop(session);

    OpenOptions::new()
        .write(true)
        .open(&source.path)
        .unwrap()
        .set_modified(original)
        .unwrap();
    let mut session = graph.resume_import_session(session_uuid).unwrap();
    assert_eq!(
        session.validate(&graph).unwrap().rows_accepted,
        NODE_ROWS as u64
    );
    session.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), NODE_ROWS as u64);
    assert_eq!(source.ids.len(), NODE_ROWS);
}

#[test]
fn a_touched_source_is_readable_again_once_it_matches_its_registration() {
    for route in ROUTES {
        a_touched_source_is_readable_again_once_it_matches_its_registration_on(route);
    }
}

#[cfg(unix)]
/// Rewrite one label byte without changing the size or the modification time:
/// only a content digest can tell.
fn rewrite_label_in_place(path: &Path) {
    let original = fs::metadata(path).unwrap().modified().unwrap();
    let mut bytes = fs::read(path).unwrap();
    let at = bytes
        .windows(6)
        .position(|window| window == b"Person")
        .expect("the label is stored as plain text");
    bytes[at] = b'Q';
    let file = OpenOptions::new().write(true).open(path).unwrap();
    {
        use std::os::unix::fs::FileExt as _;
        file.write_all_at(&bytes[at..=at], at as u64).unwrap();
    }
    file.set_modified(original).unwrap();
}

#[cfg(unix)]
fn a_digest_that_differs_from_the_recorded_one_is_refused_on(route: Route) {
    for during_build in [false, true] {
        let (_directory, _project, graph) = fixture_for(route);
        let source = source();
        let mut session = begin(&graph);
        session
            .register_parquet(BulkInputKind::Node, &source.path)
            .unwrap();
        // The digest an earlier complete read of the unchanged file recorded.
        let recorded = sha256(&fs::read(&source.path).unwrap());
        session.manifest.sources[0].sha256 = Some(recorded.clone());
        if during_build {
            let path = source.path.clone();
            super::set_pass_hook(&source.path, move |stage, _| {
                if stage == "opened" {
                    rewrite_label_in_place(&path);
                }
            });
        } else {
            rewrite_label_in_place(&source.path);
        }
        let error = session.validate(&graph).unwrap_err();
        super::clear_pass_hook(&source.path);
        let (code, message) = api_error(&error);
        assert_eq!(code, ApiErrorCode::IdentityConflict, "{message}");
        assert!(message.contains("digest changed"), "{message}");
        assert!(message.contains(&recorded), "{message}");
        assert!(!session.manifest.sources[0].staged);
    }
}

#[cfg(unix)]
#[test]
fn a_digest_that_differs_from_the_recorded_one_is_refused() {
    for route in ROUTES {
        a_digest_that_differs_from_the_recorded_one_is_refused_on(route);
    }
}

#[cfg(unix)]
fn a_same_size_same_mtime_rewrite_is_digested_not_trusted_on(route: Route) {
    let (_directory, _project, graph) = fixture_for(route);
    let source = source();
    let mut session = begin(&graph);
    session
        .register_parquet(BulkInputKind::Node, &source.path)
        .unwrap();
    rewrite_label_in_place(&source.path);
    // No size, mtime or identity change: the read proceeds and the receipt
    // attests to the bytes that were actually read, not the ones registered.
    let expected = sha256(&fs::read(&source.path).unwrap());
    session.validate(&graph).unwrap();
    assert_eq!(
        session.manifest.sources[0].sha256.as_deref(),
        Some(expected.as_str())
    );
}

#[cfg(unix)]
#[test]
fn a_same_size_same_mtime_rewrite_is_digested_not_trusted() {
    for route in ROUTES {
        a_same_size_same_mtime_rewrite_is_digested_not_trusted_on(route);
    }
}

fn abort_removes_the_session_and_leaves_the_source_alone_on(route: Route) {
    let (_directory, _project, graph) = fixture_for(route);
    let source = source();
    let before = fs::read(&source.path).unwrap();
    let mut session = begin(&graph);
    session
        .register_parquet(BulkInputKind::Node, &source.path)
        .unwrap();
    session.validate(&graph).unwrap();
    let root = session.root.clone();
    session.abort(&graph).unwrap();
    assert!(!root.join("sources").exists());
    assert_eq!(fs::read(&source.path).unwrap(), before);
}

#[test]
fn abort_removes_the_session_and_leaves_the_source_alone() {
    for route in ROUTES {
        abort_removes_the_session_and_leaves_the_source_alone_on(route);
    }
}

fn edge_sources_are_read_in_place_too_on(route: Route) {
    let (_directory, _project, graph) = fixture_for(route);
    let directory = tempfile::tempdir().unwrap();
    let node_path = directory.path().join("nodes.parquet");
    let edge_path = directory.path().join("edges.parquet");
    let (a, b) = (Uuid::now_v7(), Uuid::now_v7());
    write_nodes(&node_path, &[a, b]);
    let batch = edges(Uuid::now_v7(), a, b);
    let mut writer =
        ArrowWriter::try_new(File::create(&edge_path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut session = begin(&graph);
    session
        .register_parquet(BulkInputKind::Node, &node_path)
        .unwrap();
    session
        .register_parquet(BulkInputKind::Edge, &edge_path)
        .unwrap();
    assert_eq!(
        fs::read_dir(session.root.join("sources")).unwrap().count(),
        0
    );
    let progress = session.validate(&graph).unwrap();
    assert_route(&session, route);
    let provenance = progress.construction.unwrap().source_provenance;
    assert_eq!(provenance.len(), 2);
    assert!(provenance.iter().all(|source| source.sha256.is_some()));
    session.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
}

#[test]
fn edge_sources_are_read_in_place_too() {
    for route in ROUTES {
        edge_sources_are_read_in_place_too_on(route);
    }
}

#[test]
fn a_staged_read_hashes_what_it_reads_and_rereads_almost_nothing() {
    use parquet::file::properties::WriterProperties;

    // Several row groups, so the decode reads footer-first and then each column
    // chunk in file order, as every Parquet read does.
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nodes.parquet");
    let ids = (0..600_000).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
    let batch = nodes(&ids);
    let properties = WriterProperties::builder()
        .set_max_row_group_size(100_000)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        batch.schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let size = fs::metadata(&path).unwrap().len();

    let (_directory, _project, graph) = fixture();
    let mut session = begin(&graph);
    session
        .register_parquet(BulkInputKind::Node, &path)
        .unwrap();
    let record = session.manifest.sources[0].clone();

    let capture = graphforge_storage::concurrency_attribution::RegionCapture::start("test");
    let digest =
        crate::import_session::for_each_source_batch(&session.root, &record, 65_536, |_| Ok(()))
            .unwrap();
    let snapshot = capture.finish();
    assert_eq!(
        digest.as_deref(),
        Some(sha256(&fs::read(&path).unwrap()).as_str())
    );
    let work = &snapshot.regions["test/source_read"].work;
    // The decode consumed the file once, footer, page index and all, and the
    // digest read nothing of its own.
    assert_eq!(work["reread_bytes"], 0, "{work:?}");
    assert!(
        work["observed_bytes"] >= size && work["observed_bytes"] * 100 <= size * 101,
        "the decode read {} of {size} bytes",
        work["observed_bytes"]
    );
}

/// A session an earlier version began copied each Parquet source into the session
/// and recorded no identity for it. It keeps resuming, validating and appending.
fn historical_session(graph: &GraphForge, kept: &Path) -> (Uuid, PathBuf) {
    let mut session = begin(graph);
    session.register_parquet(BulkInputKind::Node, kept).unwrap();
    // The shape an earlier version wrote: version 2, a copy under `sources/`, and
    // neither an external identity nor a digest.
    let name = session.manifest.sources[0].name.clone();
    let copy = session.root.join("sources").join(&name);
    fs::create_dir_all(copy.parent().unwrap()).unwrap();
    fs::copy(kept, &copy).unwrap();
    session.manifest.format_version = 2;
    session.manifest.sources[0].external = None;
    session.checkpoint().unwrap();
    assert_eq!(session.manifest.sources[0].sha256, None);
    let id = session.session_uuid();
    let root = session.root.clone();
    drop(session);
    (id, root)
}

fn a_historical_copied_session_resumes_validates_and_appends_on(route: Route) {
    let (_directory, _project, graph) = fixture_for(route);
    let historical = source();
    let (id, root) = historical_session(&graph, &historical.path);
    // Only the copy remains: nothing may be read from where the file was.
    fs::remove_file(&historical.path).unwrap();

    let mut session = graph.resume_import_session(id).unwrap();
    assert_eq!(session.manifest.format_version, 2);
    let (phase, _) = graph.import_session_status(id).unwrap();
    assert_eq!(phase, ImportPhase::Open);

    // Append to it: a new in-place file and an Arrow batch join the copied one.
    let extra = source();
    session
        .register_parquet(BulkInputKind::Node, &extra.path)
        .unwrap();
    assert_eq!(
        session.manifest.format_version, 3,
        "a session holding an in-place source is no longer readable by the version that copied"
    );
    let arrow_ids = (0..BATCH_ROWS).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
    session
        .append_arrow(BulkInputKind::Node, &[nodes(&arrow_ids)])
        .unwrap();

    let progress = session.validate(&graph).unwrap();
    assert_route(&session, route);
    assert_eq!(progress.rows_accepted, (2 * NODE_ROWS + BATCH_ROWS) as u64);
    // Only the in-place source has a digest to attest; the copy and the Arrow
    // batch are session-owned.
    let provenance = progress.construction.unwrap().source_provenance;
    assert_eq!(provenance.len(), 3);
    assert_eq!(provenance[0].sha256, None);
    assert_eq!(provenance[0].footer_sha256, None);
    assert_eq!(
        provenance[1].sha256.as_deref(),
        Some(sha256(&fs::read(&extra.path).unwrap()).as_str())
    );
    assert_eq!(provenance[2].sha256, None);
    assert!(
        root.join("sources")
            .join(&session.manifest.sources[0].name)
            .exists()
    );

    // It survives a fresh process, commits, and the project takes a further append.
    let session_uuid = session.session_uuid();
    session.commit(&graph, None).unwrap();
    drop(session);
    assert_eq!(
        graph.node_count("Person").unwrap(),
        (2 * NODE_ROWS + BATCH_ROWS) as u64
    );
    let (phase, _) = graph.import_session_status(session_uuid).unwrap();
    assert_eq!(phase, ImportPhase::Committed);
    let mut next = begin(&graph);
    next.append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
        .unwrap();
    next.validate(&graph).unwrap();
    next.commit(&graph, None).unwrap();
    assert_eq!(
        graph.node_count("Person").unwrap(),
        (2 * NODE_ROWS + BATCH_ROWS + 1) as u64
    );
}

#[test]
fn a_historical_copied_session_resumes_validates_and_appends() {
    for route in ROUTES {
        a_historical_copied_session_resumes_validates_and_appends_on(route);
    }
}

/// A copied source is the session's own: nothing outside it can refuse the build,
/// and an abort removes it with the session.
#[test]
fn a_historical_copied_source_ignores_the_original_and_is_removed_by_abort() {
    for route in ROUTES {
        let (_directory, _project, graph) = fixture_for(route);
        let historical = source();
        let (id, root) = historical_session(&graph, &historical.path);
        // Changing the original in any way cannot matter to a copied source.
        for change in CHANGES.iter().rev() {
            (change.apply)(&historical.path);
        }
        assert!(!historical.path.exists());
        let mut session = graph.resume_import_session(id).unwrap();
        assert_eq!(
            session.validate(&graph).unwrap().rows_accepted,
            NODE_ROWS as u64
        );
        session.abort(&graph).unwrap();
        assert!(!root.join("sources").exists());
    }
}

// ---------------------------------------------------------------------------
// Resume, coverage and classification (review of #1923)
// ---------------------------------------------------------------------------

/// Leave a bulk session where a crash between pinning the encoded inventory and
/// recording the source digests would: the inventory is pinned, the manifest has
/// no digest, and the session is closed.
fn crash_after_inventory_pin(graph: &GraphForge, path: &Path) -> Uuid {
    let mut session = begin(graph);
    session.register_parquet(BulkInputKind::Node, path).unwrap();
    session.manifest.build_route = Some(BuildRoute::Bulk);
    session.persist_manifest().unwrap();
    let mut construction = session.open_construction(graph).unwrap();
    let refusals = crate::import_session::bulk_source::Refusals::default();
    let digests = crate::import_session::bulk_source::Digests::default();
    let plan = session
        .plan_bulk_build(graph, None, &refusals, &digests)
        .unwrap();
    construction.build_initial(&plan, None).unwrap();
    assert_eq!(session.manifest.sources[0].sha256, None);
    session.session_uuid()
}

/// A staged session that has durably staged its first batch and then stopped.
fn stop_after_first_staged_batch(graph: &GraphForge, path: &Path) -> Uuid {
    let mut session = begin(graph);
    session.register_parquet(BulkInputKind::Node, path).unwrap();
    // The first batch is accepted and its progress appended; the process then
    // stops before the journal is flushed or any digest exists.
    crate::import_session::journal::inject("completed_before_fsync");
    assert!(session.validate(graph).is_err());
    assert!(session.manifest.sources[0].batches_staged >= 1);
    assert_eq!(session.manifest.sources[0].sha256, None);
    session.session_uuid()
}

#[test]
fn a_normal_edit_between_a_crash_and_resume_is_refused_before_progress_is_reused() {
    for (route, stop) in [
        (
            Route::Bulk,
            crash_after_inventory_pin as fn(&GraphForge, &Path) -> Uuid,
        ),
        (Route::Staged, stop_after_first_staged_batch),
    ] {
        for change in &CHANGES[1..] {
            let (_directory, _project, graph) = fixture_for(route);
            let source = source();
            let id = stop(&graph, &source.path);
            (change.apply)(&source.path);
            let mut session = graph.resume_import_session(id).unwrap();
            let staged = session.manifest.sources[0].batches_staged;
            let error = session.validate(&graph).unwrap_err();
            assert_refusal(&error, change, &format!("{route:?} resume"));
            assert_eq!(session.manifest.sources[0].batches_staged, staged);
            assert_eq!(session.manifest.sources[0].sha256, None);
        }
    }
}

/// Rewrite one label byte keeping device, inode, size and modification time.
#[cfg(unix)]
fn graph_label_counts(graph: &GraphForge) -> (u64, u64) {
    (
        graph.node_count("Person").unwrap(),
        graph.node_count("Qerson").unwrap(),
    )
}

#[cfg(unix)]
#[test]
fn a_bulk_inventory_pinned_before_its_digest_is_never_published_with_another_files_digest() {
    let (_directory, _project, graph) = fixture_for(Route::Bulk);
    let source = source();
    let id = crash_after_inventory_pin(&graph, &source.path);
    rewrite_label_in_place(&source.path);

    let mut session = graph.resume_import_session(id).unwrap();
    let progress = session.validate(&graph).unwrap();
    session.commit(&graph, None).unwrap();
    let recorded = progress.construction.unwrap().source_provenance[0]
        .sha256
        .clone()
        .unwrap();
    // The digest names the file that was read, and the published graph is the
    // graph of that same file.
    assert_eq!(recorded, sha256(&fs::read(&source.path).unwrap()));
    assert_eq!(
        graph_label_counts(&graph),
        (0, NODE_ROWS as u64),
        "the graph is the one the digest describes"
    );
}

#[cfg(unix)]
#[test]
fn a_bulk_build_whose_digest_is_recorded_reuses_its_inventory_and_keeps_that_digest() {
    let (_directory, _project, graph) = fixture_for(Route::Bulk);
    let source = source();
    let mut session = begin(&graph);
    session
        .register_parquet(BulkInputKind::Node, &source.path)
        .unwrap();
    session.validate(&graph).unwrap();
    let recorded = session.manifest.sources[0].sha256.clone().unwrap();
    let id = session.session_uuid();
    drop(session);

    // The same pinned file, validated again after a reopen: nothing is decoded
    // again and the recorded digest stands.
    let mut session = graph.resume_import_session(id).unwrap();
    let _region = graphforge_storage::concurrency_attribution::RegionCapture::start("again");
    session.validate(&graph).unwrap();
    assert_eq!(
        session.manifest.sources[0].sha256.as_deref(),
        Some(recorded.as_str())
    );
    session.commit(&graph, None).unwrap();
    assert_eq!(graph_label_counts(&graph), (NODE_ROWS as u64, 0));
}

/// A rewrite that keeps device, inode, size and modification time defeats the
/// pin. It is not detected between a crash and a resume of the staged route: the
/// receipt names the file's content as read under the pin, which is the whole
/// guarantee (see `ImportSourceProvenance::sha256`).
#[cfg(unix)]
#[test]
fn a_rewrite_that_preserves_the_whole_pin_is_digested_not_detected_when_staged_work_resumes() {
    let (_directory, _project, graph) = fixture_for(Route::Staged);
    let source = source();
    let id = stop_after_first_staged_batch(&graph, &source.path);
    rewrite_label_in_place(&source.path);
    let mut session = graph.resume_import_session(id).unwrap();
    let progress = session.validate(&graph).unwrap();
    let provenance = progress.construction.unwrap().source_provenance;
    assert_eq!(
        provenance[0].sha256.as_deref(),
        Some(sha256(&fs::read(&source.path).unwrap()).as_str())
    );
}

#[test]
fn a_range_the_bound_dropped_is_hashed_from_the_file_not_from_what_was_decoded() {
    let original = (0..(256 << 10))
        .map(|index: u32| index as u8)
        .collect::<Vec<u8>>();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blob");
    fs::write(&path, &original).unwrap();
    let identity = fake_source(&path, original.len() as u64);
    // Room for one 64 KiB range: of the three read ahead, the nearest is kept and
    // the others are dropped to be read again.
    let digest = SourceDigest::with_pending_limit(original.len() as u64, 64 << 10);
    for start in [64 << 10, 128 << 10, 192 << 10] {
        digest.observe(start as u64, &original[start..start + (64 << 10)]);
    }
    digest.observe(0, &original[..64 << 10]);
    // The file changes after the decode and before the digest completes.
    let mut rewritten = original.clone();
    rewritten[200 << 10] ^= 0xff;
    fs::write(&path, &rewritten).unwrap();
    let file = File::open(&path).unwrap();
    let hashed = digest.finish(&identity, &file).unwrap();
    assert_ne!(hashed, sha256(&original));
    assert_eq!(hashed, sha256(&rewritten));
    assert_eq!(digest.reread_bytes(), 128 << 10);
}

fn truncate_to_half(path: &Path) {
    let length = fs::metadata(path).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(length / 2)
        .unwrap();
}

#[test]
fn a_truncation_during_the_read_keeps_its_typed_classification() {
    for route in ROUTES {
        let (_directory, _project, graph) = fixture_for(route);
        let source = source();
        let mut session = begin(&graph);
        session
            .register_parquet(BulkInputKind::Node, &source.path)
            .unwrap();
        let path = source.path.clone();
        super::set_pass_hook(&source.path, move |stage, _| {
            if stage == "opened" {
                truncate_to_half(&path);
            }
        });
        let error = session.validate(&graph).unwrap_err();
        super::clear_pass_hook(&source.path);
        let (code, message) = api_error(&error);
        assert_eq!(code, ApiErrorCode::IdentityConflict, "{route:?}: {message}");
        assert!(message.contains("was resized"), "{route:?}: {message}");
    }
}

#[cfg(unix)]
#[test]
fn capture_refuses_a_symlink_itself_so_a_swap_after_any_precheck_cannot_register_its_target() {
    let source = source();
    let link = source.path.with_file_name("link.parquet");
    std::os::unix::fs::symlink(&source.path, &link).unwrap();
    assert!(super::ExternalSource::capture(&link).is_err());
    let directory = source.path.parent().unwrap();
    assert!(super::ExternalSource::capture(directory).is_err());
    assert!(super::ExternalSource::capture(&source.path).is_ok());
}
