//! Registered Parquet and Arrow sources decode inside the build's memory budget
//! (#1918), through the public facade.
//!
//! The initial-build reader sizes every batch from the file's page headers (and,
//! where the headers cannot say, from its dictionary indices or value lengths)
//! before it decodes it, reserves what the decode will hold from the build's
//! workspace pool, and refuses a batch that cannot fit with a typed resource
//! limit before the offending allocation. These tests build inputs whose stored
//! bytes understate their decoded size on purpose: dictionaries, delta
//! encodings, repeated values, wide schemas and compressed Arrow buffers.
//!
//! Heap use is measured by a counting global allocator: every live byte this
//! process allocates, which is what a resident-set limit bounds, without the
//! noise of the page cache and of the allocator's free lists.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Int64Array, Int64Builder, LargeStringArray,
    ListBuilder, StringArray, StringDictionaryBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    BulkInputKind, GfError, GraphForge, GraphImportSession, ImportProgress, ImportSessionLimits,
    OperationId, bulk_node_input_schema,
};
use graphforge_core::ProjectErrorCode;
use graphforge_storage::BulkBuildReport;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
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

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new) };
        if !moved.is_null() {
            if new >= layout.size() {
                grow(new - layout.size());
            } else {
                CURRENT.fetch_sub(layout.size() - new, Ordering::Relaxed);
            }
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const MIB: u64 = 1 << 20;
const BUDGET_ENV: &str = "GF_BULK_BUILD_MEMORY_BUDGET_BYTES";

/// The budget and the heap counters are process-wide, so tests that set the
/// first or read the second do not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn set_budget(budget: Option<u64>) {
    // SAFETY: tests that read or write the environment hold `SERIAL`.
    unsafe {
        match budget {
            Some(bytes) => std::env::set_var(BUDGET_ENV, bytes.to_string()),
            None => std::env::remove_var(BUDGET_ENV),
        }
    }
}

/// A budget the property route fits and these inputs overflow, so the build
/// goes through scratch.
const SCRATCH_BUDGET: u64 = 1_100 * MIB;

/// Every route a build can take, as the budget that selects it.
const ROUTES: [Option<u64>; 2] = [None, Some(SCRATCH_BUDGET)];

fn v7(value: u128) -> Uuid {
    Uuid::from_u128((value << 80) | (0x7 << 76) | (0x2 << 62) | value)
}

fn uuid_array(values: &[Option<Uuid>]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        match value {
            Some(value) => builder.append_value(value.as_bytes()).unwrap(),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

/// Node rows with the given property columns, which must already be in the
/// lexicographic order the canonical schema keeps.
fn node_batch(first: u128, rows: usize, properties: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let ids = (0..rows)
        .map(|row| Some(v7(first + row as u128)))
        .collect::<Vec<_>>();
    node_batch_with(&ids, properties)
}

fn node_batch_with(ids: &[Option<Uuid>], properties: Vec<(&str, ArrayRef)>) -> RecordBatch {
    // A Parquet file carries no bulk-contract metadata and its columns need not
    // be Arrow types the contract accepts (a dictionary is stored as a dictionary
    // page and read back as strings), so the schema is built here.
    let mut fields = vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
    ];
    fields.extend(
        properties
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true)),
    );
    let mut columns = vec![
        uuid_array(ids),
        Arc::new(StringArray::from(vec!["Thing"; ids.len()])) as ArrayRef,
    ];
    columns.extend(properties.into_iter().map(|(_, array)| array));
    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns).unwrap()
}

/// A Parquet writer that does not embed the Arrow schema, so dictionary arrays
/// are stored as dictionary pages and read back as plain strings.
fn writer(
    path: &Path,
    schema: arrow::datatypes::SchemaRef,
    properties: WriterProperties,
) -> ArrowWriter<File> {
    ArrowWriter::try_new_with_options(
        File::create(path).unwrap(),
        schema,
        ArrowWriterOptions::new()
            .with_properties(properties)
            .with_skip_arrow_metadata(true),
    )
    .unwrap()
}

/// Writes with the Arrow schema kept in the file's metadata, as ArrowWriter
/// does by default: the reader then decodes to the hinted types.
fn write_parquet_with_arrow_schema(path: &Path, batch: &RecordBatch) {
    let mut out = ArrowWriter::try_new(
        File::create(path).unwrap(),
        batch.schema(),
        Some(WriterProperties::builder().build()),
    )
    .unwrap();
    out.write(batch).unwrap();
    out.close().unwrap();
}

fn write_parquet(path: &Path, batches: &[RecordBatch], properties: WriterProperties) {
    let mut out = writer(path, batches[0].schema(), properties);
    for batch in batches {
        out.write(batch).unwrap();
    }
    out.close().unwrap();
}

struct Imported {
    _directory: tempfile::TempDir,
    graph: GraphForge,
    session: GraphImportSession,
    result: Result<ImportProgress, GfError>,
    /// Peak live heap bytes above the baseline while `validate` ran.
    peak: u64,
}

impl Imported {
    fn bulk_build(&self) -> BulkBuildReport {
        self.result
            .as_ref()
            .unwrap()
            .construction
            .as_ref()
            .unwrap()
            .bulk_build
            .clone()
            .expect("an initial import builds on the bulk builder")
    }

    fn scratch(&self) -> bool {
        let report = self.bulk_build();
        report.property_scratch_write_bytes > 0 || report.scratch_write_bytes > 0
    }

    /// Whether the import was staged chunk by chunk instead of built in bulk.
    fn staged(&self) -> bool {
        self.result
            .as_ref()
            .unwrap()
            .construction
            .as_ref()
            .is_some_and(|construction| construction.bulk_build.is_none())
    }

    fn error(&self) -> &GfError {
        self.result.as_ref().unwrap_err()
    }

    fn commit(&mut self) {
        self.session.commit(&self.graph, None).unwrap();
    }

    fn rejected_rows(&self) -> u64 {
        self.graph
            .import_session_status(self.session.session_uuid())
            .unwrap()
            .1
            .rows_rejected
    }
}

fn begin(batch_rows: usize) -> (tempfile::TempDir, PathBuf, GraphForge, GraphImportSession) {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    fs::create_dir(&project).unwrap();
    let graph = GraphForge::new(project.to_str()).unwrap();
    let session = graph
        .begin_import_session(
            // Identities of rows without one derive from the operation, so every
            // import of the same rows names it the same.
            OperationId(v7(7)),
            ImportSessionLimits {
                batch_rows,
                ..ImportSessionLimits::default()
            },
        )
        .unwrap();
    (directory, project, graph, session)
}

/// Validate the session with `budget` pinned, measuring the heap it needs.
fn validate(
    directory: tempfile::TempDir,
    graph: GraphForge,
    mut session: GraphImportSession,
    budget: Option<u64>,
) -> Imported {
    set_budget(budget);
    let baseline = CURRENT.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let result = session.validate(&graph);
    let peak = (PEAK.load(Ordering::Relaxed) - baseline) as u64;
    set_budget(None);
    Imported {
        _directory: directory,
        graph,
        session,
        result,
        peak,
    }
}

fn import(path: &Path, batch_rows: usize, budget: Option<u64>) -> Imported {
    import_sources(&[(BulkInputKind::Node, path)], batch_rows, budget)
}

fn import_sources(
    sources: &[(BulkInputKind, &Path)],
    batch_rows: usize,
    budget: Option<u64>,
) -> Imported {
    let (directory, _project, graph, mut session) = begin(batch_rows);
    for (kind, path) in sources {
        session.register_parquet(*kind, path).unwrap();
    }
    validate(directory, graph, session, budget)
}

fn is_resource_limit(error: &GfError) -> bool {
    matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    )
}

/// Answers a project gives, rendered for comparison across routes.
fn answers(graph: &GraphForge, queries: &[&str]) -> String {
    queries
        .iter()
        .map(|query| {
            let result = graph.execute(query).unwrap();
            arrow::util::pretty::pretty_format_batches(&result.batches)
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Import `path` on every route, commit each, and return their one answer.
///
/// The resident route keeps every decoded batch by design, so only the route
/// through scratch is held to `scratch_heap`.
fn same_answers_on_every_route(
    path: &Path,
    batch_rows: usize,
    queries: &[&str],
    scratch_heap: u64,
) -> String {
    let mut expected: Option<String> = None;
    for budget in ROUTES {
        let mut run = import(path, batch_rows, budget);
        run.result
            .as_ref()
            .unwrap_or_else(|error| panic!("{budget:?}: {error}"));
        assert_eq!(run.scratch(), budget.is_some(), "{budget:?}");
        if budget.is_some() {
            // The scratch route decodes and forms property runs on concurrent
            // workers (#1960); their live buffers are the source workspace and
            // property-run budget the build reports. The heap may hold those
            // reservations beside a fixed allowance for the builder's own
            // tables, and no more: a container retained per row or per cell
            // would exceed it.
            let report = run.bulk_build();
            let limit = scratch_heap.max(
                48 * MIB
                    + report.source_workspace_peak_bytes
                    + report.property_retained_budget_bytes,
            );
            assert!(
                run.peak < limit,
                "{budget:?}: peak heap {} MiB, limit {} MiB",
                run.peak / MIB,
                limit / MIB
            );
        }
        run.commit();
        let answer = answers(&run.graph, queries);
        match &expected {
            Some(expected) => assert_eq!(&answer, expected, "{budget:?}"),
            None => expected = Some(answer),
        }
    }
    expected.unwrap()
}

/// A column of one 100 KiB string, stored once and decoded for every row: enough
/// decoded bytes that the planner routes a small input through scratch.
fn pad(rows: usize) -> (&'static str, ArrayRef) {
    pad_of(rows, 100 << 10)
}

fn pad_of(rows: usize, bytes: usize) -> (&'static str, ArrayRef) {
    ("zz_pad", dictionary_strings(rows, |_| "z".repeat(bytes)))
}

/// Writer properties that keep a dictionary page however large its entries are: a
/// dictionary page over the default limit is abandoned for plain pages, which
/// store every row's copy and defeat the point of a dictionary.
fn dictionary_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_dictionary_page_size_limit(64 << 20)
        .build()
}

/// A value wider than the 64 MiB window: no piece, even of one row, holds it.
const OVERSIZED_VALUE_BYTES: usize = 65 << 20;

/// One `OVERSIZED_VALUE_BYTES` value in `rows` rows, stored once in a
/// compressed dictionary page: a small file no piece can decode.
fn write_oversized_value(path: &Path, rows: usize) {
    write_parquet(
        path,
        &[node_batch(
            1,
            rows,
            vec![(
                "text",
                dictionary_strings(rows, |_| "x".repeat(OVERSIZED_VALUE_BYTES)),
            )],
        )],
        WriterProperties::builder()
            .set_dictionary_page_size_limit(128 << 20)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build(),
    );
    assert!(fs::metadata(path).unwrap().len() < MIB);
}

fn dictionary_strings(rows: usize, value: impl Fn(usize) -> String) -> ArrayRef {
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    for row in 0..rows {
        builder.append_value(value(row));
    }
    Arc::new(builder.finish())
}

#[test]
fn wide_schemas_do_not_retain_a_row_container_per_cell() {
    let _serial = serial();
    let rows = 4_096;
    let names = (0..700)
        .map(|column| format!("p{column:04}"))
        .collect::<Vec<_>>();
    let properties = names
        .iter()
        .enumerate()
        .map(|(column, name)| {
            let array: ArrayRef = Arc::new(Int64Array::from(
                (0..rows as i64)
                    .map(|row| row * 31 + column as i64)
                    .collect::<Vec<_>>(),
            ));
            (name.as_str(), array)
        })
        .collect();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nodes.parquet");
    write_parquet(
        &path,
        &[node_batch(1, rows, properties)],
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(2_048))
            .build(),
    );
    let queries = [
        "MATCH (n:Thing) RETURN count(n) AS n, sum(n.p0000) AS a, sum(n.p0699) AS b",
        "MATCH (n:Thing) WHERE n.p0350 = 31 * 5 + 350 RETURN n.p0001 AS x",
    ];
    // A 1,024-row batch holds 5.7 MB of Arrow. Normalizing it used to keep a map
    // of 700 values for every one of its rows, 215 MiB of them; now it keeps one
    // row's. The bound sits between the two.
    let expected = same_answers_on_every_route(&path, 1_024, &queries, 96 * MIB);
    assert!(expected.contains("4096"), "{expected}");
}

#[test]
fn a_dictionary_that_expands_past_the_window_is_refused_before_it_is_decoded() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nodes.parquet");
    // One 100 KiB value, stored once; 1,024 rows of it decode to 100 MiB, past
    // the 64 MiB window. The batch is decoded in pieces that fit the window,
    // and no piece outlives its turn.
    let rows = 2_048;
    let batch = node_batch(
        1,
        rows,
        vec![("text", dictionary_strings(rows, |_| "x".repeat(100 << 10)))],
    );
    write_parquet(&path, &[batch], dictionary_properties());
    assert!(fs::metadata(&path).unwrap().len() < MIB);
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n, sum(size(n.text)) AS bytes"];
    let answer = same_answers_on_every_route(&path, 1_024, &queries, 192 * MIB);
    assert!(
        answer.contains(&(rows * (100 << 10)).to_string()),
        "{answer}"
    );

    // One value wider than the window fits no piece: it is refused before a
    // byte of it is decoded, and its batch is counted as rejected.
    let wide = directory.path().join("wide.parquet");
    let rows = 2;
    write_oversized_value(&wide, rows);
    for budget in ROUTES {
        let run = import(&wide, 1_024, budget);
        let error = run.error();
        assert!(is_resource_limit(error), "{budget:?}: {error}");
        assert!(
            error.to_string().contains("refused before it was decoded"),
            "{error}"
        );
        // Sizing the batch holds its stored dictionary page, admitted against
        // the inventory budget; decoding the batch would have held 130 MiB and
        // more, and refusing it adds nothing.
        assert!(
            run.peak < (rows * OVERSIZED_VALUE_BYTES) as u64,
            "{budget:?}: {} MiB",
            run.peak / MIB
        );
        assert_eq!(
            run.rejected_rows(),
            rows as u64,
            "the refused batch is counted"
        );
    }

    // The same value in batches that fit the window imports.
    let fits = directory.path().join("fits.parquet");
    let rows = 256;
    write_parquet(
        &fits,
        &[node_batch(
            1,
            rows,
            vec![("text", dictionary_strings(rows, |_| "x".repeat(100 << 10)))],
        )],
        dictionary_properties(),
    );
    let answer = same_answers_on_every_route(&fits, 16, &queries, 400 * MIB);
    assert!(
        answer.contains(&(rows * (100 << 10)).to_string()),
        "{answer}"
    );
}

#[test]
fn plain_values_that_share_a_page_import_in_pieces_whose_buffers_fit() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    // Six 10 MiB values ahead of ninety 100 KiB ones, stored plain in one
    // page. The reader reserves a piece's values buffer at the page's average
    // per row, under a megabyte here, and grows it by doubling as the wide
    // values arrive: a piece of six wide rows would reach 80 MiB of capacity
    // for 60 MiB of values, over the window. Pieces are sized for that growth,
    // so the wide rows decode three at a time and every piece fits.
    let rows = 96;
    let path = directory.path().join("plain.parquet");
    write_parquet(
        &path,
        &[node_batch(
            1,
            rows,
            vec![(
                "text",
                dictionary_strings(rows, |row| {
                    if row < 6 {
                        char::from(b'a' + row as u8).to_string().repeat(10 << 20)
                    } else {
                        format!("{row:08}{}", "s".repeat((100 << 10) - 8))
                    }
                }),
            )],
        )],
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(1 << 30)
            .set_data_page_row_count_limit(usize::MAX)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build(),
    );
    // The builder's window is the same on every route; the resident route
    // reaches it without the scratch route's fixed floor, which with these
    // reservations is more than the facade's scratch budget. A query
    // projecting 69 MiB of one property is bounded separately by the property
    // overlay's read admission, so the count is what is asked.
    let mut run = import(&path, rows, None);
    run.result
        .as_ref()
        .unwrap_or_else(|error| panic!("{error}"));
    run.commit();
    let answer = answers(&run.graph, &["MATCH (n:Thing) RETURN count(n) AS n"]);
    assert!(answer.contains(&rows.to_string()), "{answer}");
}

#[test]
fn columns_reserved_apart_are_refused_as_one_row_before_decoding() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    // Two plain columns in one logical batch. A page reader reserves a
    // piece's buffer at its own page's average per row, whatever the row holds
    // in that column: row 0 asks column A for 30 MiB on a 20 MiB reservation,
    // which doubles to 40 MiB, and column B for nothing on a 25 MiB
    // reservation. Together they pass the window although the row's values
    // are 30 MiB, so the row is refused before it is decoded.
    let rows = 12;
    let wide = |row: usize| char::from(b'a' + row as u8).to_string().repeat(30 << 20);
    let a = dictionary_strings(rows, |row| {
        if row == 0 || row == 2 {
            wide(row)
        } else {
            String::new()
        }
    });
    let b = dictionary_strings(rows, |row| {
        if row == 0 || row == 2 {
            String::new()
        } else {
            wide(row)
        }
    });
    let path = directory.path().join("apart.parquet");
    write_parquet(
        &path,
        &[node_batch(1, rows, vec![("a", a), ("b", b)])],
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(1 << 30)
            .set_data_page_row_count_limit(usize::MAX)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build(),
    );
    assert!(fs::metadata(&path).unwrap().len() < 4 * MIB);
    for budget in ROUTES {
        let run = import(&path, rows, budget);
        let error = run.error();
        assert!(is_resource_limit(error), "{budget:?}: {error}");
        assert!(
            error.to_string().contains("refused before it was decoded"),
            "{error}"
        );
        assert_eq!(run.rejected_rows(), rows as u64, "{budget:?}");
    }
}

#[test]
fn arrow_schema_hints_decode_to_the_admitted_types() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    // A file that keeps its Arrow schema asks for large offsets; the reader
    // decodes to that type, which the admitted schema already names, instead
    // of the plain strings the Parquet types alone would suggest.
    let rows = 1_024;
    let large: ArrayRef = Arc::new(LargeStringArray::from(
        (0..rows)
            .map(|row| format!("large-{row}"))
            .collect::<Vec<_>>(),
    ));
    let path = directory.path().join("hinted.parquet");
    write_parquet_with_arrow_schema(&path, &node_batch(1, rows, vec![("large", large)]));
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n, sum(size(n.large)) AS large"];
    let bytes: usize = (0..rows).map(|row| format!("large-{row}").len()).sum();
    for budget in ROUTES {
        let mut run = import(&path, 128, budget);
        run.result
            .as_ref()
            .unwrap_or_else(|error| panic!("{budget:?}: {error}"));
        run.commit();
        let answer = answers(&run.graph, &queries);
        assert!(
            answer.contains("1024") && answer.contains(&bytes.to_string()),
            "{budget:?}: {answer}"
        );
    }
}

#[test]
fn a_source_with_no_row_groups_imports_beside_a_full_one() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    // A converter writes an empty table as a Parquet file of schema and footer
    // alone: no row group, so no column chunk to size a row by.
    let rows = 512;
    let full = directory.path().join("full.parquet");
    write_parquet(
        &full,
        &[node_batch(
            1,
            rows,
            vec![("text", dictionary_strings(rows, |row| format!("v{row}")))],
        )],
        WriterProperties::builder().build(),
    );
    let empty = directory.path().join("empty.parquet");
    write_parquet(
        &empty,
        &[node_batch(
            1,
            0,
            vec![("text", dictionary_strings(0, |row| format!("v{row}")))],
        )],
        WriterProperties::builder().build(),
    );
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n"];
    for budget in ROUTES {
        let mut run = import_sources(
            &[(BulkInputKind::Node, &full), (BulkInputKind::Node, &empty)],
            128,
            budget,
        );
        let progress = run
            .result
            .as_ref()
            .unwrap_or_else(|error| panic!("{budget:?}: {error}"));
        assert_eq!(progress.rows_accepted, rows as u64, "{budget:?}");
        run.commit();
        assert!(answers(&run.graph, &queries).contains("512"), "{budget:?}");
    }
}

#[test]
fn compressed_pages_are_admitted_with_their_codec_state() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    // Small pages, each the whole of what its task's workspace admits: the
    // codec's own state has to be admitted beside them, or every compressed
    // page is refused for want of it.
    let rows = 2_048;
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n, sum(size(n.text)) AS bytes"];
    for (name, compression) in [
        ("zstd", Compression::ZSTD(ZstdLevel::default())),
        ("gzip", Compression::GZIP(Default::default())),
        ("brotli", Compression::BROTLI(Default::default())),
    ] {
        let path = directory.path().join(format!("{name}.parquet"));
        write_parquet(
            &path,
            &[node_batch(
                1,
                rows,
                vec![(
                    "text",
                    dictionary_strings(rows, |row| format!("value-{row:04}")),
                )],
            )],
            WriterProperties::builder()
                .set_compression(compression)
                .build(),
        );
        for budget in ROUTES {
            let mut run = import(&path, 256, budget);
            run.result
                .as_ref()
                .unwrap_or_else(|error| panic!("{name} {budget:?}: {error}"));
            run.commit();
            let answer = answers(&run.graph, &queries);
            assert!(
                answer.contains(&(rows * 10).to_string()),
                "{name} {budget:?}: {answer}"
            );
        }
    }
}

#[test]
fn a_large_dictionary_with_small_selected_values_is_not_refused() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nodes.parquet");
    // One 6 MiB entry is referenced by one row of 4,096: sizing a batch by its
    // dictionary's largest entry would charge every row for it (24 GiB).
    let rows = 4_096;
    let batch = node_batch(
        1,
        rows,
        vec![
            (
                "text",
                dictionary_strings(rows, |row| {
                    if row == 7 {
                        "y".repeat(6 << 20)
                    } else {
                        format!("v{}", row % 9)
                    }
                }),
            ),
            pad_of(rows, 8 << 10),
        ],
    );
    write_parquet(&path, &[batch], dictionary_properties());
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n, max(size(n.text)) AS widest"];
    let answer = same_answers_on_every_route(&path, 4_096, &queries, 200 * MIB);
    assert!(answer.contains(&(6 << 20).to_string()), "{answer}");
}

#[test]
fn repeated_large_values_import_within_the_window_and_are_refused_beyond_it() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    let value = |row: usize| {
        char::from(b'a' + (row % 3) as u8)
            .to_string()
            .repeat(1 << 20)
    };
    let accepted = directory.path().join("accepted.parquet");
    write_parquet(
        &accepted,
        &[node_batch(
            1,
            24,
            vec![("text", dictionary_strings(24, value))],
        )],
        dictionary_properties(),
    );
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n, sum(size(n.text)) AS bytes"];
    // 24 MiB of three distinct values: inside the window.
    let answer = same_answers_on_every_route(&accepted, 24, &queries, 400 * MIB);
    assert!(answer.contains(&(24 << 20).to_string()), "{answer}");

    // 160 MiB in one batch is decoded in pieces; a value wider than the window
    // is past it, wherever the budget is.
    let split = directory.path().join("split.parquet");
    write_parquet(
        &split,
        &[node_batch(
            1,
            160,
            vec![("text", dictionary_strings(160, value))],
        )],
        dictionary_properties(),
    );
    let answer = same_answers_on_every_route(&split, 160, &queries, 192 * MIB);
    assert!(answer.contains(&(160 << 20).to_string()), "{answer}");
    let refused = directory.path().join("refused.parquet");
    write_oversized_value(&refused, 2);
    for budget in ROUTES {
        let run = import(&refused, 160, budget);
        assert!(
            is_resource_limit(run.error()),
            "{budget:?}: {}",
            run.error()
        );
        // The stored page, held to size the batch; not the decoded batch.
        assert!(
            run.peak < (2 * OVERSIZED_VALUE_BYTES) as u64,
            "{budget:?}: {} MiB",
            run.peak / MIB
        );
    }
}

/// Strings of `width` bytes that share all but their last digits with the one
/// before: a page of them stores a few bytes per value.
fn prefix_sharing(rows: usize, width: usize) -> Vec<String> {
    (0..rows)
        .map(|row| format!("{}{row:08}", "p".repeat(width - 8)))
        .collect()
}

fn write_delta_strings(path: &Path, rows: usize, width: usize) {
    let values = prefix_sharing(rows, width);
    // One page per row group: a page ends when it holds a megabyte, and each
    // page starts over with a whole value.
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_column_encoding("text".into(), Encoding::DELTA_BYTE_ARRAY)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_data_page_size_limit(1 << 30)
        .set_data_page_row_count_limit(usize::MAX)
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let first = node_batch(
        1,
        1,
        vec![(
            "text",
            Arc::new(StringArray::from(vec![values[0].as_str()])),
        )],
    );
    let mut out = writer(path, first.schema(), properties);
    for start in (0..rows).step_by(8) {
        let end = (start + 8).min(rows);
        let chunk = node_batch(
            1 + start as u128,
            end - start,
            vec![(
                "text",
                Arc::new(StringArray::from(
                    values[start..end]
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                )),
            )],
        );
        out.write(&chunk).unwrap();
    }
    out.close().unwrap();
}

#[test]
fn delta_encoded_values_are_sized_by_what_they_decode_to() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    // A megabyte per value, a few bytes of it stored per value: 512 MiB in one
    // batch, decoded in pieces that fit the window.
    let split = directory.path().join("split.parquet");
    write_delta_strings(&split, 512, 1 << 20);
    assert!(
        fs::metadata(&split).unwrap().len() < 4 * MIB,
        "{} bytes stored",
        fs::metadata(&split).unwrap().len()
    );
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n, sum(size(n.text)) AS bytes"];
    let answer = same_answers_on_every_route(&split, 512, &queries, 192 * MIB);
    assert!(answer.contains(&(512 << 20).to_string()), "{answer}");

    // A value wider than the window fits no piece, however few bytes of it the
    // page stores.
    let refused = directory.path().join("refused.parquet");
    write_delta_strings(&refused, 2, OVERSIZED_VALUE_BYTES);
    assert!(
        fs::metadata(&refused).unwrap().len() < 4 * MIB,
        "{} bytes stored",
        fs::metadata(&refused).unwrap().len()
    );
    for budget in ROUTES {
        let run = import(&refused, 512, budget);
        assert!(
            is_resource_limit(run.error()),
            "{budget:?}: {}",
            run.error()
        );
        // The stored page, held to size the batch; not the decoded batch.
        assert!(
            run.peak < (2 * OVERSIZED_VALUE_BYTES) as u64,
            "{budget:?}: {} MiB",
            run.peak / MIB
        );
    }

    let accepted = directory.path().join("accepted.parquet");
    write_delta_strings(&accepted, 24, 1 << 20);
    let answer = same_answers_on_every_route(&accepted, 24, &queries, 400 * MIB);
    assert!(answer.contains(&(24 << 20).to_string()), "{answer}");
}

#[test]
fn repeated_values_are_bounded_by_their_children() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();

    // Lists of every length, empty and null ones among them, in row groups that
    // straddle batches: every route reads the same children.
    let mut small = ListBuilder::new(Int64Builder::new());
    for row in 0..200_i64 {
        for child in 0..(row % 13) * 50 {
            small.values().append_value(row * 1_000 + child);
        }
        small.append(row % 17 != 0);
    }
    let path = directory.path().join("small.parquet");
    write_parquet(
        &path,
        &[node_batch(
            1,
            200,
            vec![("items", Arc::new(small.finish())), pad(200)],
        )],
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(30))
            .build(),
    );
    let queries = [
        "MATCH (n:Thing) RETURN count(n) AS n, count(n.items) AS lists, sum(size(n.items)) AS children",
    ];
    let answer = same_answers_on_every_route(&path, 16, &queries, 256 * MIB);
    assert!(answer.contains("200"), "{answer}");

    // One row holds a million and a half children: 12 MB of Arrow, and 168 MB
    // if every child were converted to a value at once.
    let mut numbers = ListBuilder::new(Int64Builder::new());
    for row in 0..64_i64 {
        let children = if row == 3 { 1_500_000 } else { row % 4 };
        for child in 0..children {
            numbers.values().append_value(child);
        }
        numbers.append(true);
    }
    let wide = directory.path().join("wide.parquet");
    write_parquet(
        &wide,
        &[node_batch(
            1,
            64,
            vec![("items", Arc::new(numbers.finish()))],
        )],
        WriterProperties::builder().build(),
    );
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n"];
    let answer = same_answers_on_every_route(&wide, 64, &queries, 128 * MIB);
    assert!(answer.contains("64"), "{answer}");

    // Twenty million booleans in one cell take 2.5 MB of Arrow, but a pair of
    // levels and an index each (160 MB), and 2.2 GB converted to values at once.
    // No piece of the window holds the row: it is refused while the source is
    // planned, before anything decodes it.
    let mut flags = ListBuilder::new(BooleanBuilder::new());
    for row in 0..8_usize {
        let children = if row == 5 { 20_000_000 } else { 3 };
        for child in 0..children {
            flags.values().append_value(child % 3 == 0);
        }
        flags.append(true);
    }
    let long = directory.path().join("flags.parquet");
    write_parquet(
        &long,
        &[node_batch(1, 8, vec![("flags", Arc::new(flags.finish()))])],
        WriterProperties::builder().build(),
    );
    for budget in ROUTES {
        let run = import(&long, 8, budget);
        assert!(
            is_resource_limit(run.error()),
            "{budget:?}: {}",
            run.error()
        );
        assert!(
            run.error()
                .to_string()
                .contains("refused before it was decoded"),
            "{}",
            run.error()
        );
        assert!(run.peak < 48 * MIB, "{budget:?}: {} MiB", run.peak / MIB);
    }
}

/// A node batch in the canonical bulk schema, for `append_arrow`.
fn canonical_batch(first: u128, rows: usize, text: &str) -> RecordBatch {
    let ids = (0..rows)
        .map(|row| v7(first + row as u128))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        bulk_node_input_schema(vec![Field::new("text", DataType::Utf8, true)]).unwrap(),
        vec![
            uuid_array(&ids.iter().copied().map(Some).collect::<Vec<_>>()),
            Arc::new(StringArray::from(vec!["Thing"; rows])) as ArrayRef,
            Arc::new(StringArray::from(vec![text; rows])) as ArrayRef,
        ],
    )
    .unwrap()
}

/// The one file an Arrow import keeps its rows in.
fn arrow_source(project: &Path, session: &GraphImportSession) -> PathBuf {
    let sources = project
        .join("import-sessions")
        .join(session.session_uuid().to_string())
        .join("sources");
    let mut files = fs::read_dir(sources)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1, "{files:?}");
    files.pop().unwrap()
}

fn write_ipc(
    path: &Path,
    batches: &[RecordBatch],
    compression: Option<arrow::ipc::CompressionType>,
) {
    let options = arrow::ipc::writer::IpcWriteOptions::default()
        .try_with_compression(compression)
        .unwrap();
    let mut out = arrow::ipc::writer::FileWriter::try_new_with_options(
        File::create(path).unwrap(),
        &batches[0].schema(),
        options,
    )
    .unwrap();
    for batch in batches {
        out.write(batch).unwrap();
    }
    out.finish().unwrap();
}

/// An Arrow import whose session file is replaced by `replacement`.
fn arrow_import(
    batches: &[RecordBatch],
    replacement: impl FnOnce(&Path, &[RecordBatch]),
    budget: Option<u64>,
) -> Imported {
    let (directory, project, graph, mut session) = begin(1_000);
    session.append_arrow(BulkInputKind::Node, batches).unwrap();
    let source = arrow_source(&project, &session);
    replacement(&source, batches);
    validate(directory, graph, session, budget)
}

/// An Arrow file's reader slices every column of a batch out of one message body.
/// Charged per column, 200 integer columns of 1,000 rows (1.6 MB) read as 320 MB,
/// and the import was refused for exceeding the 64 MiB intake window.
#[test]
fn a_wide_arrow_batch_is_charged_its_body_once_not_once_per_column() {
    let _serial = serial();
    let rows = 1_000;
    let names = (0..200)
        .map(|column| format!("p{column:03}"))
        .collect::<Vec<_>>();
    let fields = names
        .iter()
        .map(|name| Field::new(name, DataType::Int64, true))
        .collect::<Vec<_>>();
    let mut columns: Vec<ArrayRef> = vec![
        uuid_array(
            &(0..rows)
                .map(|row| Some(v7(1 + row as u128)))
                .collect::<Vec<_>>(),
        ),
        Arc::new(StringArray::from(vec!["Thing"; rows])),
    ];
    for column in 0..names.len() {
        columns.push(Arc::new(Int64Array::from(
            (0..rows as i64)
                .map(|row| row * 3 + column as i64)
                .collect::<Vec<_>>(),
        )));
    }
    let batch = RecordBatch::try_new(bulk_node_input_schema(fields).unwrap(), columns).unwrap();
    for budget in ROUTES {
        let mut run = arrow_import(std::slice::from_ref(&batch), |_, _| {}, budget);
        run.result
            .as_ref()
            .unwrap_or_else(|error| panic!("{budget:?}: {error}"));
        run.commit();
        let answer = answers(
            &run.graph,
            &["MATCH (n:Thing) RETURN count(n) AS n, sum(n.p199) AS last"],
        );
        assert!(
            answer.contains("1000") && answer.contains("1697500"),
            "{answer}"
        );
    }
}

#[test]
fn compressed_arrow_buffers_import_and_a_buffer_that_advertises_a_huge_size_is_refused() {
    let _serial = serial();
    // Four megabytes of one repeated string per batch: it compresses to a few
    // hundred bytes and decodes to all four.
    let batches = (0..3)
        .map(|batch| canonical_batch(1 + batch * 1_000, 1_000, &"x".repeat(4_096)))
        .collect::<Vec<_>>();
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n, sum(size(n.text)) AS bytes"];

    let mut plain = arrow_import(&batches, |_, _| {}, Some(SCRATCH_BUDGET));
    plain.result.as_ref().unwrap();
    plain.commit();
    let expected = answers(&plain.graph, &queries);
    assert!(
        expected.contains(&(3_000 * 4_096).to_string()),
        "{expected}"
    );

    let compress = |path: &Path, batches: &[RecordBatch]| {
        write_ipc(path, batches, Some(arrow::ipc::CompressionType::ZSTD));
    };
    let mut compressed = arrow_import(&batches, compress, Some(SCRATCH_BUDGET));
    compressed.result.as_ref().unwrap();
    compressed.commit();
    assert_eq!(answers(&compressed.graph, &queries), expected);

    // The same file with one buffer's advertised expansion raised to 8 GiB: the
    // footer and the message are consistent, only the length that sizes the
    // decompressed buffer lies.
    let lie = |path: &Path, batches: &[RecordBatch]| {
        write_ipc(path, batches, Some(arrow::ipc::CompressionType::ZSTD));
        let mut bytes = fs::read(path).unwrap();
        let honest = (1_000_u64 * 4_096).to_le_bytes();
        // The expansion a compressed buffer states precedes the frame's magic.
        let zstd_magic = [0x28, 0xB5, 0x2F, 0xFD];
        let at = bytes
            .windows(12)
            .position(|window| window[..8] == honest && window[8..] == zstd_magic)
            .expect("a compressed values buffer states its expansion");
        bytes[at..at + 8].copy_from_slice(&(8_u64 << 30).to_le_bytes());
        fs::write(path, bytes).unwrap();
    };
    let lied = arrow_import(&batches, lie, Some(SCRATCH_BUDGET));
    assert!(is_resource_limit(lied.error()), "{}", lied.error());
    assert!(
        lied.error().to_string().contains("Arrow"),
        "{}",
        lied.error()
    );
    assert!(lied.peak < 48 * MIB, "{} MiB", lied.peak / MIB);
}

/// The expansion a compressed values buffer states before the frame magic of
/// each codec, for a batch of 1,000 rows of `width`-byte strings.
fn lz4_prefix(width: u64) -> [u8; 12] {
    let mut window = (1_000 * width).to_le_bytes().to_vec();
    window.extend_from_slice(&[0x04, 0x22, 0x4D, 0x18]);
    window.try_into().unwrap()
}

/// An IPC buffer's eight-byte prefix states the length its frame expands to,
/// and Arrow's own LZ4 path expanded into a buffer of that size before checking
/// it - so a prefix that lies *downward* passed every plan-time bound and then
/// allocated without one. The reader now expands every frame itself, in
/// fixed-size chunks that stop at the advertised length: this file's batches
/// hold 64 MiB each while advertising 1 KiB, and the import is refused with a
/// heap that never saw either.
#[test]
fn an_lz4_buffer_that_advertises_less_than_it_expands_is_refused_before_it_expands() {
    let _serial = serial();
    let batches = (0..2)
        .map(|batch| canonical_batch(1 + batch * 1_000, 1_000, &"x".repeat(64 << 10)))
        .collect::<Vec<_>>();
    let lie = |path: &Path, batches: &[RecordBatch]| {
        write_ipc(path, batches, Some(arrow::ipc::CompressionType::LZ4_FRAME));
        let mut bytes = fs::read(path).unwrap();
        let at = bytes
            .windows(12)
            .position(|window| window == lz4_prefix(64 << 10))
            .expect("a compressed values buffer states its expansion");
        bytes[at..at + 8].copy_from_slice(&1_024_u64.to_le_bytes());
        fs::write(path, bytes).unwrap();
    };
    for budget in ROUTES {
        let lied = arrow_import(&batches, lie, budget);
        assert!(
            is_resource_limit(lied.error()),
            "{budget:?}: {}",
            lied.error()
        );
        assert!(
            lied.error().to_string().contains("advertised"),
            "{budget:?}: {}",
            lied.error()
        );
        // Expanding honestly would have held 64 MiB; refusing it held none.
        assert!(lied.peak < 24 * MIB, "{budget:?}: {} MiB", lied.peak / MIB);
    }

    // The same rows with honest prefixes still import, on both routes, and
    // publish exactly the rows the file holds. Their text is narrow enough
    // that the committed properties fit the fixed property-live-byte budget.
    let honest_batches = (0..2)
        .map(|batch| canonical_batch(1 + batch * 1_000, 1_000, &"x".repeat(8 << 10)))
        .collect::<Vec<_>>();
    let queries = ["MATCH (n:Thing) RETURN count(n) AS n, sum(size(n.text)) AS bytes"];
    let compress = |path: &Path, batches: &[RecordBatch]| {
        write_ipc(path, batches, Some(arrow::ipc::CompressionType::LZ4_FRAME));
    };
    let mut expected: Option<String> = None;
    for budget in ROUTES {
        let mut run = arrow_import(&honest_batches, compress, budget);
        run.result
            .as_ref()
            .unwrap_or_else(|error| panic!("{budget:?}: {error}"));
        run.commit();
        let answer = answers(&run.graph, &queries);
        match &expected {
            Some(expected) => assert_eq!(&answer, expected, "{budget:?}"),
            None => expected = Some(answer),
        }
    }
    assert!(
        expected.unwrap().contains(&(2_000 * (8 << 10)).to_string()),
        "the honest LZ4 file publishes its rows"
    );
}

/// Every block is checked as it is read, not the file once: two batches decode
/// and emit exactly as before, and the third - whose values buffer advertises
/// 1 KiB but expands to 64 MiB - is refused when the task reaches it.
#[test]
fn a_downward_lie_in_a_later_block_is_refused_when_that_block_is_read() {
    let _serial = serial();
    let batches = (0_u128..3)
        .map(|batch| {
            let width = [4 << 10, 8 << 10, 64 << 10][batch as usize];
            canonical_batch(1 + batch * 1_000, 1_000, &"x".repeat(width))
        })
        .collect::<Vec<_>>();
    let lie_last = |path: &Path, batches: &[RecordBatch]| {
        write_ipc(path, batches, Some(arrow::ipc::CompressionType::LZ4_FRAME));
        let mut bytes = fs::read(path).unwrap();
        let at = bytes
            .windows(12)
            .position(|window| window == lz4_prefix(64 << 10))
            .expect("the last batch's values buffer states its expansion");
        bytes[at..at + 8].copy_from_slice(&1_024_u64.to_le_bytes());
        fs::write(path, bytes).unwrap();
    };
    let lied = arrow_import(&batches, lie_last, Some(SCRATCH_BUDGET));
    assert!(is_resource_limit(lied.error()), "{}", lied.error());
    assert!(
        lied.error().to_string().contains("advertised"),
        "{}",
        lied.error()
    );
    // Only the first two batches' honest 4 and 8 MiB decodes ran; the third's
    // 64 MiB never existed.
    assert!(lied.peak < 24 * MIB, "{} MiB", lied.peak / MIB);
}

#[test]
fn an_unsupported_arrow_column_is_refused_before_the_file_decodes_its_dictionaries() {
    let _serial = serial();
    let batches = [canonical_batch(1, 1_000, "x")];
    let replace = |path: &Path, _: &[RecordBatch]| {
        // A dictionary-typed property is not a property type; its dictionary is
        // forty megabytes, which Arrow's file reader decodes the moment it opens.
        let values = StringArray::from(
            (0..40)
                .map(|entry| char::from(b'a' + entry as u8).to_string().repeat(1 << 20))
                .collect::<Vec<_>>(),
        );
        let keys =
            arrow::array::Int32Array::from((0..1_000).map(|row| row % 40).collect::<Vec<_>>());
        let dictionary: ArrayRef = Arc::new(
            arrow::array::DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap(),
        );
        let ids = (0..1_000).map(|row| Some(v7(1 + row))).collect::<Vec<_>>();
        let schema =
            bulk_node_input_schema(vec![Field::new("text", DataType::Utf8, true)]).unwrap();
        let mut fields = schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[2] = Field::new("text", dictionary.data_type().clone(), true);
        let batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new_with_metadata(
                fields,
                schema.metadata().clone(),
            )),
            vec![
                uuid_array(&ids),
                Arc::new(StringArray::from(vec!["Thing"; 1_000])) as ArrayRef,
                dictionary,
            ],
        )
        .unwrap();
        write_ipc(path, &[batch], None);
    };
    let refused = arrow_import(&batches, replace, Some(SCRATCH_BUDGET));
    assert!(
        refused
            .error()
            .to_string()
            .contains("unsupported Arrow property type"),
        "{}",
        refused.error()
    );
    // Decoding the dictionary first would have held all forty megabytes.
    assert!(refused.peak < 24 * MIB, "{} MiB", refused.peak / MIB);
}

/// Where the first page of column `column` starts in `bytes`, and the positions
/// and widths of its header's two sizes (Thrift compact: a field header byte,
/// then a zig-zag varint).
fn first_page_sizes(path: &Path, column: usize) -> [(usize, usize); 2] {
    use parquet::file::reader::FileReader as _;
    let reader =
        parquet::file::serialized_reader::SerializedFileReader::new(File::open(path).unwrap())
            .unwrap();
    let start = reader.metadata().row_group(0).column(column).byte_range().0 as usize;
    let bytes = fs::read(path).unwrap();
    let varint = |at: usize| {
        bytes[at..]
            .iter()
            .take_while(|byte| *byte & 0x80 != 0)
            .count()
            + 1
    };
    // type, uncompressed size, compressed size, each behind a one-byte field header.
    let mut at = start + 1;
    at += varint(at);
    at += 1;
    let uncompressed = (at, varint(at));
    at += uncompressed.1 + 1;
    let compressed = (at, varint(at));
    [uncompressed, compressed]
}

/// The largest positive value a zig-zag varint of `width` bytes holds.
fn widest(width: usize) -> Vec<u8> {
    let zigzag = (1_u64 << (7 * width)) - 2;
    (0..width)
        .map(|index| {
            let byte = ((zigzag >> (7 * index)) & 0x7f) as u8;
            if index + 1 < width { byte | 0x80 } else { byte }
        })
        .collect()
}

#[test]
fn page_lengths_that_the_file_cannot_hold_are_refused_before_they_are_allocated() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    // One 1.8 MB page: its sizes are four-byte varints that can claim 134 MB.
    let rows = 3_000;
    let values = (0..rows)
        .map(|row| format!("{row:0600}"))
        .collect::<Vec<_>>();
    let batch = node_batch(
        1,
        rows,
        vec![(
            "text",
            Arc::new(StringArray::from(
                values.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        )],
    );
    let clean = directory.path().join("clean.parquet");
    write_parquet(
        &clean,
        &[batch],
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(8 << 20)
            .set_data_page_row_count_limit(usize::MAX)
            .set_max_row_group_row_count(Some(1 << 20))
            .build(),
    );
    let text_column = 2;
    let [uncompressed, compressed] = first_page_sizes(&clean, text_column);
    assert_eq!((uncompressed.1, compressed.1), (4, 4));
    import(&clean, 1_000, Some(SCRATCH_BUDGET))
        .result
        .as_ref()
        .unwrap();

    for (name, (at, width), expected) in [
        ("compressed", compressed, "extends beyond its column chunk"),
        (
            "uncompressed",
            uncompressed,
            "claim more uncompressed bytes than the footer states",
        ),
    ] {
        let corrupt = directory.path().join(format!("{name}.parquet"));
        let mut bytes = fs::read(&clean).unwrap();
        bytes[at..at + width].copy_from_slice(&widest(width));
        fs::write(&corrupt, bytes).unwrap();
        let run = import(&corrupt, 1_000, Some(SCRATCH_BUDGET));
        assert!(
            run.error().to_string().contains(expected),
            "{name}: {}",
            run.error()
        );
        assert!(run.peak < 48 * MIB, "{name}: {} MiB", run.peak / MIB);
    }

    // A footer that claims to be two gigabytes is refused when it is registered.
    let corrupt = directory.path().join("footer.parquet");
    let mut bytes = fs::read(&clean).unwrap();
    let end = bytes.len();
    bytes[end - 8..end - 4].copy_from_slice(&0x7fff_fff0_u32.to_le_bytes());
    fs::write(&corrupt, bytes).unwrap();
    let (_directory, _project, _graph, mut session) = begin(1_000);
    let error = session
        .register_parquet(BulkInputKind::Node, &corrupt)
        .unwrap_err();
    assert!(error.to_string().contains("footer length"), "{error}");
}

/// Everything the three routes must agree on: identities (derived ones
/// included), every encoding of value, and a list.
#[test]
fn every_route_publishes_the_same_graph_for_every_encoding() {
    let _serial = serial();
    let rows = 300;
    let ids = (0..rows)
        .map(|row| (row % 5 == 0).then(|| v7(10_000 + row as u128)))
        .collect::<Vec<_>>();
    let mut lists = ListBuilder::new(Int64Builder::new());
    for row in 0..rows {
        for child in 0..(row % 4) {
            lists.values().append_value((row * 10 + child) as i64);
        }
        lists.append(row % 11 != 0);
    }
    let properties: Vec<(&str, ArrayRef)> = vec![
        (
            "a_dict",
            dictionary_strings(rows, |row| format!("entry-{}", row % 7)),
        ),
        (
            "b_delta",
            Arc::new(StringArray::from(prefix_sharing(rows, 40))),
        ),
        (
            "c_plain",
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|row| {
                        (row % 6 != 0).then(|| format!("plain-{row:05}-{}", "z".repeat(row % 17)))
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "d_int",
            Arc::new(Int64Array::from(
                (0..rows)
                    .map(|row| (row % 9 != 0).then_some(row as i64 * 3))
                    .collect::<Vec<_>>(),
            )),
        ),
        ("e_list", Arc::new(lists.finish())),
    ];
    let directory = tempfile::tempdir().unwrap();
    // Seven-row groups and four-row batches: every task of sixteen batches
    // starts inside a row group. The padded file adds decoded bytes (stored once)
    // so the planner routes it through scratch; the plain one is the staged
    // path's, which fsyncs every batch and is slow to feed megabytes.
    let write_nodes = |name: &str, padded: bool| {
        let mut columns = properties.clone();
        if padded {
            columns.push(pad(rows));
        }
        let path = directory.path().join(name);
        write_parquet(
            &path,
            &[node_batch_with(&ids, columns)],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(7))
                .set_dictionary_page_size_limit(64 << 20)
                .set_column_dictionary_enabled("b_delta".into(), false)
                .set_column_encoding("b_delta".into(), Encoding::DELTA_BYTE_ARRAY)
                .set_column_dictionary_enabled("c_plain".into(), false)
                .build(),
        );
        path
    };
    let padded = write_nodes("padded.parquet", true);
    let plain = write_nodes("plain.parquet", false);
    // Edges between the nodes that name themselves, with identities of their own
    // for two rows in three, in the same small row groups.
    let anchors = ids.iter().flatten().copied().collect::<Vec<_>>();
    let edge_rows = 240;
    let edge_ids = (0..edge_rows)
        .map(|row| (row % 3 != 0).then(|| v7(50_000 + row as u128)))
        .collect::<Vec<_>>();
    let sources = (0..edge_rows)
        .map(|row| Some(anchors[row * 7 % anchors.len()]))
        .collect::<Vec<_>>();
    let targets = (0..edge_rows)
        .map(|row| Some(anchors[(row * 11 + 3) % anchors.len()]))
        .collect::<Vec<_>>();
    let edge_fields = vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("rel_type", DataType::Utf8, false),
        Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
        Field::new(
            "a_kind",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("b_weight", DataType::Int64, true),
    ];
    let edge_batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(edge_fields)),
        vec![
            uuid_array(&edge_ids),
            Arc::new(StringArray::from(vec!["LINKS"; edge_rows])) as ArrayRef,
            uuid_array(&sources),
            uuid_array(&targets),
            dictionary_strings(edge_rows, |row| format!("kind-{}", row % 4)),
            Arc::new(Int64Array::from(
                (0..edge_rows as i64).map(|row| row * 5).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    let edges = directory.path().join("edges.parquet");
    write_parquet(
        &edges,
        &[edge_batch],
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(7))
            .build(),
    );
    let queries = [
        "MATCH (n:Thing) RETURN count(n) AS n, count(n.c_plain) AS plain, sum(n.d_int) AS ints",
        "MATCH (n:Thing) RETURN n.node_uuid AS id, n.a_dict AS a, n.b_delta AS b, n.c_plain AS c, \
         n.d_int AS d, n.e_list AS e ORDER BY id",
        "MATCH ()-[r:LINKS]->() RETURN count(r) AS edges, sum(r.b_weight) AS weight",
        "MATCH (a)-[r:LINKS]->(b) RETURN r.edge_uuid AS id, a.node_uuid AS source, \
         b.node_uuid AS target, r.a_kind AS kind, r.b_weight AS weight ORDER BY id",
    ];
    // Resident, through scratch, and the plain unpadded file again: every bulk
    // route publishes one graph, so every run answers alike. A budget under the
    // scratch route's fixed floor is refused by naming what it needs before a
    // byte is decoded; a durable bulk route cannot fall back to staging (ADR
    // 0058).
    let mut expected: Option<String> = None;
    for (name, nodes, budget) in [
        ("resident", &padded, None),
        ("scratch", &padded, Some(SCRATCH_BUDGET)),
        ("resident unpadded", &plain, None),
    ] {
        let mut run = import_sources(
            &[(BulkInputKind::Node, nodes), (BulkInputKind::Edge, &edges)],
            4,
            budget,
        );
        run.result
            .as_ref()
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(!run.staged(), "{name}");
        if name == "scratch" {
            assert!(run.scratch(), "{:?}", run.bulk_build());
        }
        run.commit();
        let answer = answers(&run.graph, &queries);
        match &expected {
            Some(expected) => assert_eq!(&answer, expected, "{name}"),
            None => expected = Some(answer),
        }
    }
    let under = import_sources(
        &[(BulkInputKind::Node, &plain), (BulkInputKind::Edge, &edges)],
        4,
        Some(256 * MIB),
    );
    let error = under.error();
    assert!(is_resource_limit(error), "{error}");
    assert!(error.to_string().contains("before decoding"), "{error}");
    assert!(under.peak < 24 * MIB, "{} MiB", under.peak / MIB);
    let expected = expected.unwrap();
    assert!(expected.contains("entry-3") && expected.contains("kind-2"));
    assert!(expected.contains("240"), "{expected}");
}

/// The resident bytes the build says it needs before it decodes anything.
fn required_before_decoding(error: &GfError) -> u64 {
    let message = error.to_string();
    let (_, after) = message
        .split_once("requires ")
        .unwrap_or_else(|| panic!("{message}"));
    after
        .split_whitespace()
        .next()
        .and_then(|number| number.parse().ok())
        .unwrap_or_else(|| panic!("{message}"))
}

#[test]
fn the_pages_a_decode_holds_are_reserved_before_the_first_is_read() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    let rows = 1 << 18;
    let names = (0..8)
        .map(|column| format!("p{column}"))
        .collect::<Vec<_>>();
    let properties = names
        .iter()
        .enumerate()
        .map(|(column, name)| {
            let values: ArrayRef = Arc::new(Int64Array::from(
                (0..rows as i64)
                    .map(|row| row * 7 + column as i64)
                    .collect::<Vec<_>>(),
            ));
            (name.as_str(), values)
        })
        .collect::<Vec<_>>();
    let batch = node_batch(1, rows, properties);
    // The same values in one 2 MiB page per column, and in the writer's default
    // pages of twenty thousand rows.
    let fat = directory.path().join("fat.parquet");
    write_parquet(
        &fat,
        std::slice::from_ref(&batch),
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(2 << 20)
            .set_data_page_row_count_limit(usize::MAX)
            .set_max_row_group_row_count(Some(rows))
            .build(),
    );
    let thin = directory.path().join("thin.parquet");
    write_parquet(
        &thin,
        std::slice::from_ref(&batch),
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_max_row_group_row_count(Some(rows))
            .build(),
    );

    // A budget no property build fits is refused by naming what it needs, before
    // a byte is decoded. The need depends on the budget a little (the digest's
    // held reads are a share of it), so files are compared at the same budget.
    let needs = |path: &Path| {
        let run = import(path, 65_536, Some(600 * MIB));
        assert!(is_resource_limit(run.error()), "{}", run.error());
        assert!(run.peak < 24 * MIB, "{} MiB", run.peak / MIB);
        required_before_decoding(run.error())
    };
    let (fat_needs, thin_needs) = (needs(&fat), needs(&thin));
    assert!(
        fat_needs >= thin_needs + 14 * MIB,
        "eight 2 MiB pages and one being decompressed are 18 MiB beyond the thin file's \
         need: {fat_needs} against {thin_needs}"
    );

    // Following the need, the build is refused until the budget is the need, and
    // then it imports: a larger budget keeps the input.
    let mut budget = 600 * MIB;
    let mut fits = loop {
        let run = import(&fat, 65_536, Some(budget));
        match &run.result {
            Err(error) if is_resource_limit(error) && error.to_string().contains("requires") => {
                let need = required_before_decoding(error);
                assert!(need > budget, "{need} against {budget}");
                assert!(error.to_string().contains("before decoding"), "{error}");
                budget = need;
            }
            _ => break run,
        }
    };
    fits.result
        .as_ref()
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(fits.scratch());
    let short = import(&fat, 65_536, Some(budget - 1));
    assert!(is_resource_limit(short.error()), "{}", short.error());
    let report = fits.bulk_build();
    assert!(
        report.source_workspace_peak_bytes >= 16 * MIB,
        "{} bytes reserved",
        report.source_workspace_peak_bytes
    );
    assert!(report.source_workspace_peak_bytes <= report.source_workspace_capacity_bytes);
    fits.commit();
    assert!(answers(&fits.graph, &["MATCH (n:Thing) RETURN count(n) AS n"]).contains("262144"));
}
