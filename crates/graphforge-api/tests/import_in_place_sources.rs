//! Registered Parquet sources are read where they are (#1898), through the
//! public facade.
//!
//! `register_parquet` records a source's identity and copies nothing; the build
//! reads it in place, computes its SHA-256 from the bytes that read decodes, and
//! refuses a file that is no longer the one registered. An initial import builds
//! on the bulk builder; an import into a project that already has a generation
//! stages chunk by chunk. Each claim is checked on both.

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{ArrayRef, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use graphforge_api::{
    ApiErrorCode, BulkInputKind, GfError, GraphForge, GraphImportSession, ImportPhase,
    ImportProgress, ImportSessionLimits, OperationId, bulk_edge_input_schema,
    bulk_node_input_schema,
};
use graphforge_storage::concurrency_attribution::{RegionCapture, RegionSnapshot};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// Rows per construction batch; a task of the bulk builder is sixteen batches.
const BATCH_ROWS: usize = 1_024;
const ROW_GROUP_ROWS: usize = 16 * BATCH_ROWS;

/// The measurements below difference process-wide counters, so tests that take
/// them do not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// An initial import builds on the bulk builder; an import into a project that
/// already has a generation stages chunk by chunk.
#[derive(Clone, Copy, Debug)]
enum Route {
    Bulk,
    Staged,
}

const ROUTES: [Route; 2] = [Route::Bulk, Route::Staged];

fn uuid_array(values: &[Uuid]) -> ArrayRef {
    Arc::new(
        FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_bytes().as_slice()))
            .unwrap(),
    )
}

fn node_batch(ids: &[Uuid]) -> RecordBatch {
    let ranks = (0..ids.len() as i64).collect::<Vec<_>>();
    RecordBatch::try_new(
        bulk_node_input_schema(vec![Field::new("rank", DataType::Int64, true)]).unwrap(),
        vec![
            uuid_array(ids),
            Arc::new(StringArray::from(vec!["Person"; ids.len()])),
            Arc::new(Int64Array::from(ranks)),
        ],
    )
    .unwrap()
}

fn edge_batch(ids: &[Uuid], nodes: &[Uuid]) -> RecordBatch {
    let source = (0..ids.len())
        .map(|index| nodes[(index * 7 + 1) % nodes.len()])
        .collect::<Vec<_>>();
    let target = (0..ids.len())
        .map(|index| nodes[(index * 13 + 5) % nodes.len()])
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        bulk_edge_input_schema(Vec::new()).unwrap(),
        vec![
            uuid_array(ids),
            Arc::new(StringArray::from(vec!["KNOWS"; ids.len()])),
            uuid_array(&source),
            uuid_array(&target),
        ],
    )
    .unwrap()
}

/// Several row groups of whole tasks, with no page index, so every byte outside
/// the footer and the magic is a column chunk a decode reads.
fn write_parquet(path: &Path, batch: &RecordBatch) {
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        batch.schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

struct Input {
    directory: tempfile::TempDir,
    node_ids: Vec<Uuid>,
    edge_ids: Vec<Uuid>,
}

impl Input {
    fn new(nodes: usize, edges: usize) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let node_ids = (0..nodes).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
        let edge_ids = (0..edges).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
        write_parquet(
            &directory.path().join("nodes.parquet"),
            &node_batch(&node_ids),
        );
        write_parquet(
            &directory.path().join("edges.parquet"),
            &edge_batch(&edge_ids, &node_ids),
        );
        Self {
            directory,
            node_ids,
            edge_ids,
        }
    }

    fn nodes(&self) -> PathBuf {
        self.directory.path().join("nodes.parquet")
    }

    fn edges(&self) -> PathBuf {
        self.directory.path().join("edges.parquet")
    }

    fn bytes(&self) -> u64 {
        fs::metadata(self.nodes()).unwrap().len() + fs::metadata(self.edges()).unwrap().len()
    }
}

fn limits() -> ImportSessionLimits {
    ImportSessionLimits {
        batch_rows: BATCH_ROWS,
        ..ImportSessionLimits::default()
    }
}

fn begin(graph: &GraphForge) -> GraphImportSession {
    graph
        .begin_import_session(OperationId(Uuid::now_v7()), limits())
        .unwrap()
}

/// A project to import into: empty for the bulk route, one committed generation
/// for the staged route.
fn project(route: Route) -> (tempfile::TempDir, PathBuf, GraphForge) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("project");
    fs::create_dir(&path).unwrap();
    let graph = GraphForge::new(path.to_str()).unwrap();
    if matches!(route, Route::Staged) {
        let mut seed = begin(&graph);
        seed.append_arrow(BulkInputKind::Node, &[node_batch(&[Uuid::now_v7()])])
            .unwrap();
        seed.validate(&graph).unwrap();
        seed.commit(&graph, None).unwrap();
    }
    (directory, path, graph)
}

fn sha256(path: &Path) -> String {
    Sha256::digest(fs::read(path).unwrap())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A table of the project's answers, rendered for comparison.
fn answers(graph: &GraphForge) -> String {
    [
        "MATCH (n:Person) RETURN count(n) AS nodes, sum(n.rank) AS ranks, min(n.rank) AS low, \
         max(n.rank) AS high",
        "MATCH (a)-[r:KNOWS]->(b) RETURN count(r) AS edges, count(DISTINCT a) AS sources, \
         count(DISTINCT b) AS targets",
    ]
    .iter()
    .map(|query| {
        let result = graph.execute(query).unwrap();
        pretty_format_batches(&result.batches).unwrap().to_string()
    })
    .collect::<Vec<_>>()
    .join("\n")
}

fn row<'a>(
    snapshot: &'a RegionSnapshot,
    region: &str,
) -> &'a graphforge_storage::concurrency_attribution::RegionRow {
    let suffix = format!("/{region}");
    snapshot
        .regions
        .iter()
        .find(|(path, _)| path.ends_with(&suffix))
        .unwrap_or_else(|| {
            panic!(
                "no region {region} in {:?}",
                snapshot.regions.keys().collect::<Vec<_>>()
            )
        })
        .1
}

fn tree_bytes(root: &Path) -> u64 {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                tree_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}

fn session_root(project: &Path, session: &GraphImportSession) -> PathBuf {
    project
        .join("import-sessions")
        .join(session.session_uuid().to_string())
}

/// Register both files, validate, and return the final progress.
fn import(graph: &GraphForge, input: &Input) -> (GraphImportSession, ImportProgress) {
    let mut session = begin(graph);
    session
        .register_parquet(BulkInputKind::Node, &input.nodes())
        .unwrap();
    session
        .register_parquet(BulkInputKind::Edge, &input.edges())
        .unwrap();
    let progress = session.validate(graph).unwrap();
    (session, progress)
}

#[test]
fn an_in_place_import_publishes_the_answers_of_an_arrow_import() {
    let _serial = serial();
    let input = Input::new(12_000, 8_000);
    for route in ROUTES {
        let (_directory, _path, graph) = project(route);
        let (mut session, progress) = import(&graph, &input);
        session.commit(&graph, None).unwrap();

        // The same rows, appended as Arrow batches, into a project of the same shape.
        let (_other_directory, _other_path, other) = project(route);
        let mut control = begin(&other);
        for (kind, whole) in [
            (BulkInputKind::Node, node_batch(&input.node_ids)),
            (
                BulkInputKind::Edge,
                edge_batch(&input.edge_ids, &input.node_ids),
            ),
        ] {
            let batches = (0..whole.num_rows())
                .step_by(BATCH_ROWS)
                .map(|start| whole.slice(start, BATCH_ROWS.min(whole.num_rows() - start)))
                .collect::<Vec<_>>();
            control.append_arrow(kind, &batches).unwrap();
        }
        control.validate(&other).unwrap();
        control.commit(&other, None).unwrap();

        let (nodes, edges) = (input.node_ids.len() as u64, input.edge_ids.len() as u64);
        assert_eq!(progress.rows_accepted, nodes + edges, "{route:?}");
        // A staged import adds to the seed node; a bulk import is the whole graph.
        let seed = u64::from(matches!(route, Route::Staged));
        let expected = answers(&other);
        assert!(expected.contains(&(nodes + seed).to_string()), "{expected}");
        assert_eq!(answers(&graph), expected, "{route:?}");
        assert_eq!(graph.node_count("Person").unwrap(), nodes + seed);
    }
}

#[test]
fn the_receipt_carries_the_sha256_of_each_source_that_was_read() {
    let _serial = serial();
    let input = Input::new(12_000, 8_000);
    for route in ROUTES {
        let (_directory, path, graph) = project(route);
        let (mut session, progress) = import(&graph, &input);
        let id = session.session_uuid();
        let provenance = progress.construction.unwrap().source_provenance;
        assert_eq!(provenance.len(), 2, "{route:?}");
        for (entry, source) in provenance.iter().zip([input.nodes(), input.edges()]) {
            assert_eq!(entry.sha256.as_deref(), Some(sha256(&source).as_str()));
            assert_eq!(entry.bytes, fs::metadata(&source).unwrap().len());
            assert!(entry.footer_sha256.is_some());
        }
        session.commit(&graph, None).unwrap();
        // Durable: a fresh process sees the same receipt.
        drop(session);
        drop(graph);
        let graph = GraphForge::new(path.to_str()).unwrap();
        let (phase, progress) = graph.import_session_status(id).unwrap();
        assert_eq!(phase, ImportPhase::Committed);
        assert_eq!(progress.construction.unwrap().source_provenance, provenance);
    }
}

/// Fail if any file under `root` is a copy of one of `sources`.
fn assert_no_copy_under(root: &Path, sources: &[PathBuf], context: &str) {
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            assert_no_copy_under(&path, sources, context);
            continue;
        }
        for source in sources {
            if fs::metadata(&path).unwrap().len() == fs::metadata(source).unwrap().len() {
                assert_ne!(
                    fs::read(&path).unwrap(),
                    fs::read(source).unwrap(),
                    "{context}: {} is a copy of {}",
                    path.display(),
                    source.display()
                );
            }
        }
    }
}

fn arrow_control(other: &GraphForge, input: &Input) -> GraphImportSession {
    let mut control = begin(other);
    for (kind, whole) in [
        (BulkInputKind::Node, node_batch(&input.node_ids)),
        (
            BulkInputKind::Edge,
            edge_batch(&input.edge_ids, &input.node_ids),
        ),
    ] {
        let batches = (0..whole.num_rows())
            .step_by(BATCH_ROWS)
            .map(|start| whole.slice(start, BATCH_ROWS.min(whole.num_rows() - start)))
            .collect::<Vec<_>>();
        control.append_arrow(kind, &batches).unwrap();
    }
    control
}

#[test]
fn registering_and_building_write_no_source_bytes() {
    let _serial = serial();
    for route in ROUTES {
        // Staging a chunk at a time is slower than the bulk builder.
        let input = match route {
            Route::Bulk => Input::new(100_000, 60_000),
            Route::Staged => Input::new(40_000, 20_000),
        };
        let size = input.bytes();
        assert!(size > 1 << 20, "the sources are too small to show a copy");
        let sources = [input.nodes(), input.edges()];
        let before = sources.clone().map(|path| {
            (
                sha256(&path),
                fs::metadata(&path).unwrap().modified().unwrap(),
            )
        });

        let (_directory, path, graph) = project(route);
        let mut session = begin(&graph);
        let capture = RegionCapture::start("import");
        session
            .register_parquet(BulkInputKind::Node, &input.nodes())
            .unwrap();
        session
            .register_parquet(BulkInputKind::Edge, &input.edges())
            .unwrap();
        let registered = capture.finish();
        let register = row(&registered, "register_parquet");
        assert_eq!(register.calls, 2);
        // The work counter is the size of what was registered; the write
        // counter, what registration put on disk. A copy would make it `size`.
        assert_eq!(register.work["bytes"], size, "{route:?}");
        let written = register.inclusive.written_bytes.expect("write counter");
        assert!(
            written < 128 << 10,
            "{route:?}: registering {size} bytes wrote {written}"
        );
        let root = session_root(&path, &session);
        assert!(
            fs::read_dir(root.join("sources")).map_or(true, |mut entries| entries.next().is_none()),
            "{route:?}: the session holds a copy of a source"
        );

        let capture = RegionCapture::start("import");
        session.validate(&graph).unwrap();
        let validated = capture.finish();
        let written_in_place = written
            + row(&validated, "stage+seal")
                .inclusive
                .written_bytes
                .expect("write counter");
        assert_no_copy_under(&root, &sources, &format!("{route:?}"));
        session.commit(&graph, None).unwrap();

        // The same rows handed over as Arrow are encoded into the session, which
        // is what registering a Parquet file used to do: that import writes the
        // input on top of the graph.
        let (_other_directory, _other_path, other) = project(route);
        let capture = RegionCapture::start("control");
        let mut control = arrow_control(&other, &input);
        control.validate(&other).unwrap();
        let control_snapshot = capture.finish();
        let written_control = ["register_arrow", "stage+seal"]
            .into_iter()
            .map(|region| {
                row(&control_snapshot, region)
                    .inclusive
                    .written_bytes
                    .expect("write counter")
            })
            .sum::<u64>();
        eprintln!(
            "{route:?}: size={size} in_place_written={written_in_place} \
             arrow_control_written={written_control}"
        );
        assert!(
            written_in_place + size / 4 <= written_control,
            "{route:?}: writes {written_in_place} in place, {written_control} through the session"
        );
        let after = sources.map(|path| {
            (
                sha256(&path),
                fs::metadata(&path).unwrap().modified().unwrap(),
            )
        });
        assert_eq!(before, after, "an import never touches its sources");
    }
}

/// Bytes of a Parquet file that no footer-driven decode reads: page and offset
/// indexes and bloom filters.
fn unread_bytes(path: &Path) -> u64 {
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).unwrap(),
    )
    .unwrap();
    builder
        .metadata()
        .row_groups()
        .iter()
        .flat_map(|group| group.columns())
        .map(|column| {
            [
                column.offset_index_length(),
                column.column_index_length(),
                column.bloom_filter_length(),
            ]
            .into_iter()
            .flatten()
            .map(|length| u64::try_from(length).unwrap())
            .sum::<u64>()
        })
        .sum()
}

/// Each byte of a source is read once, for the decode and the digest together.
///
/// The region counters say how many bytes the decode read (`observed_bytes`,
/// every range its `ChunkReader` served, counting repeats) and how many the digest
/// had to read itself afterwards (`reread_bytes`). Every read of a source goes
/// through one or the other, so together they are the import's whole read of it.
#[test]
fn each_source_byte_is_read_once_for_hashing_and_decoding() {
    let _serial = serial();
    for route in ROUTES {
        let input = match route {
            Route::Bulk => Input::new(200_000, 100_000),
            Route::Staged => Input::new(60_000, 30_000),
        };
        let size = input.bytes();
        assert!(size > 2 << 20, "{size}");
        let (_directory, _path, graph) = project(route);
        let mut session = begin(&graph);
        session
            .register_parquet(BulkInputKind::Node, &input.nodes())
            .unwrap();
        session
            .register_parquet(BulkInputKind::Edge, &input.edges())
            .unwrap();

        let capture = RegionCapture::start("import");
        let progress = session.validate(&graph).unwrap();
        let snapshot = capture.finish();

        let source_read = row(&snapshot, "source_read");
        let observed = source_read.work["observed_bytes"];
        let reread = source_read.work["reread_bytes"];
        eprintln!("{route:?}: size={size} observed={observed} digest_reread={reread}");
        // The digest was folded from the decode's own reads. What it read itself
        // is exactly the bytes no decode asks for: the offset index the writer
        // appends, which a footer-driven decode skips.
        let unread = unread_bytes(&input.nodes()) + unread_bytes(&input.edges());
        assert!(unread > 0, "the fixture should carry an index nobody reads");
        assert_eq!(
            reread, unread,
            "{route:?}: the digest read {reread} bytes itself, the index is {unread}"
        );
        // The decode consumed every other byte of both files, and not much more:
        // it reads the start of each page twice, header then body.
        assert!(
            observed >= size - unread,
            "{route:?}: observed {observed} of {size}"
        );
        assert!(
            observed <= size + size / 50,
            "{route:?}: the decode read {observed} bytes of {size}"
        );
        let digests = progress
            .construction
            .unwrap()
            .source_provenance
            .into_iter()
            .map(|entry| entry.sha256.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(digests, [sha256(&input.nodes()), sha256(&input.edges())]);
    }
}

/// A way to change a registered file, and the refusal it must produce.
struct Change {
    name: &'static str,
    apply: fn(&Path),
    code: ApiErrorCode,
    reason: &'static str,
}

fn set_modified(path: &Path, seconds: u64) {
    let file = OpenOptions::new().write(true).open(path).unwrap();
    let modified = fs::metadata(path).unwrap().modified().unwrap();
    file.set_modified(modified + std::time::Duration::from_secs(seconds))
        .unwrap();
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
            let replacement = path.with_extension("replacement");
            fs::write(&replacement, fs::read(path).unwrap()).unwrap();
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
        apply: |path| set_modified(path, 7),
        code: ApiErrorCode::IdentityConflict,
        reason: "was modified",
    },
];

fn assert_refusal(error: &GfError, change: &Change, route: Route) {
    let GfError::Api { code, message } = error else {
        panic!(
            "{} on {route:?}: expected a typed API error, found {error:?}",
            change.name
        );
    };
    assert_eq!(
        *code, change.code,
        "{} on {route:?}: {message}",
        change.name
    );
    assert!(
        message.contains(change.reason),
        "{} on {route:?}: {message}",
        change.name
    );
}

#[test]
fn a_source_changed_after_registration_is_refused_before_anything_is_built() {
    let _serial = serial();
    for route in ROUTES {
        for change in &CHANGES {
            let (_directory, _path, graph) = project(route);
            let input = Input::new(3_000, 1_000);
            let mut session = begin(&graph);
            session
                .register_parquet(BulkInputKind::Node, &input.nodes())
                .unwrap();
            session
                .register_parquet(BulkInputKind::Edge, &input.edges())
                .unwrap();
            let id = session.session_uuid();
            drop(session);

            // The edges change; the nodes, read first, are still good.
            (change.apply)(&input.edges());
            let mut session = graph.resume_import_session(id).unwrap();
            let before = graph.node_count("Person").unwrap();
            let error = session.validate(&graph).unwrap_err();
            assert_refusal(&error, change, route);
            assert_eq!(graph.node_count("Person").unwrap(), before);
            assert_ne!(session.status().0, ImportPhase::Validated);
            assert!(session.commit(&graph, None).is_err(), "{}", change.name);
        }
    }
}

#[test]
fn a_touched_source_is_read_again_once_it_matches_its_registration() {
    let _serial = serial();
    for route in ROUTES {
        let (_directory, _path, graph) = project(route);
        let input = Input::new(3_000, 1_000);
        let mut session = begin(&graph);
        session
            .register_parquet(BulkInputKind::Node, &input.nodes())
            .unwrap();
        let id = session.session_uuid();
        let original = fs::metadata(input.nodes()).unwrap().modified().unwrap();
        set_modified(&input.nodes(), 30);
        assert!(session.validate(&graph).is_err());
        drop(session);

        OpenOptions::new()
            .write(true)
            .open(input.nodes())
            .unwrap()
            .set_modified(original)
            .unwrap();
        let mut session = graph.resume_import_session(id).unwrap();
        assert_eq!(session.validate(&graph).unwrap().rows_accepted, 3_000);
        session.commit(&graph, None).unwrap();
    }
}

#[test]
fn aborting_removes_the_session_and_leaves_the_sources() {
    let _serial = serial();
    let input = Input::new(3_000, 1_000);
    let before = [sha256(&input.nodes()), sha256(&input.edges())];
    for route in ROUTES {
        let (_directory, path, graph) = project(route);
        let (mut session, _) = import(&graph, &input);
        let root = session_root(&path, &session);
        assert!(root.exists());
        let id = session.session_uuid();
        session.abort(&graph).unwrap();
        assert_eq!(
            graph.import_session_status(id).unwrap().0,
            ImportPhase::Aborted
        );
        assert!(!root.join("sources").exists());
        assert!(input.nodes().exists() && input.edges().exists());
    }
    assert_eq!(before, [sha256(&input.nodes()), sha256(&input.edges())]);
}
