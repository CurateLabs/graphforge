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
        digest_of(&|d| ranges(70_001)
            .iter()
            .rev()
            .for_each(|&(a, b)| d.observe(a as u64, &bytes[a.saturating_sub(10)..b]))),
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

    let mut session = begin(&graph);
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
        super::set_pass_hook(move |stage, index| {
            if stage == "batch" && index == 1 {
                apply(&path);
            }
        });
        let error = session.validate(&graph).unwrap_err();
        super::clear_pass_hook();
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
            super::set_pass_hook(move |stage, _| {
                if stage == "opened" {
                    rewrite_label_in_place(&path);
                }
            });
        } else {
            rewrite_label_in_place(&source.path);
        }
        let error = session.validate(&graph).unwrap_err();
        super::clear_pass_hook();
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
