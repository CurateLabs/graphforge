//! Registered sources import inside a fixed memory budget, measured as the
//! resident set of a separate process (#1918).
//!
//! `bounded_source_decoding.rs` counts heap bytes in one process. This file asks
//! the operating system: each case runs its import in a child process (this test
//! binary, re-executed on the one `child` test) and reads the child's own
//! `VmHWM`, the high-water resident set, from `/proc/self/status` when the import
//! ends. Inputs are written by the parent, so the child's peak is the import's.
//!
//! The accepted cases hold the budget fixed and grow the input: the resident set
//! must not follow the payload. The refused cases are inputs with a value no
//! piece of the window can hold: the child must refuse them with a typed
//! resource limit while staying small. Every case reports the bytes it read and the scratch it
//! wrote; `GF_RSS_REPORT=<file>` appends them as JSON lines for the pull request.

use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, StringArray, StringDictionaryBuilder};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{BulkInputKind, GraphForge, ImportSessionLimits, OperationId};
use graphforge_core::{GfError, ProjectErrorCode};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

const MIB: u64 = 1 << 20;
const BUDGET: u64 = 1_100 * MIB;
const CHILD_ENV: &str = "GF_RSS_CHILD";

fn v7(value: u128) -> Uuid {
    Uuid::from_u128((value << 80) | (0x7 << 76) | (0x2 << 62) | value)
}

/// `rows` nodes with one dictionary-encoded string property of `distinct` values
/// of `bytes` each: stored once each, decoded for every row.
fn write_dictionary_nodes(
    path: &Path,
    rows: usize,
    distinct: usize,
    bytes: usize,
    compression: Compression,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
        Field::new(
            "text",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));
    let mut out = ArrowWriter::try_new_with_options(
        File::create(path).unwrap(),
        schema.clone(),
        ArrowWriterOptions::new()
            // A dictionary page larger than this limit is abandoned for plain
            // pages, which would store every row's copy.
            .with_properties(
                WriterProperties::builder()
                    .set_dictionary_page_size_limit(64 << 20)
                    .set_compression(compression)
                    .build(),
            )
            .with_skip_arrow_metadata(true),
    )
    .unwrap();
    let values = (0..distinct)
        .map(|entry| {
            char::from(b'a' + (entry % 26) as u8)
                .to_string()
                .repeat(bytes)
        })
        .collect::<Vec<_>>();
    // A few thousand rows at a time, so writing the input never holds it whole.
    for start in (0..rows).step_by(2_048) {
        let end = (start + 2_048).min(rows);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut text = StringDictionaryBuilder::<Int32Type>::new();
        for row in start..end {
            ids.append_value(v7(1 + row as u128).as_bytes()).unwrap();
            text.append_value(&values[row % distinct]);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Thing"; end - start])),
                Arc::new(text.finish()),
            ],
        )
        .unwrap();
        out.write(&batch).unwrap();
    }
    out.close().unwrap();
}

fn status_kib(field: &str) -> u64 {
    fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

fn io_bytes(field: &str) -> u64 {
    fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.trim().parse().ok())
        .unwrap_or(0)
}

/// The child: import `$GF_RSS_CHILD` (`path|batch_rows`) under `$GF_BULK_BUILD_MEMORY_BUDGET_BYTES`
/// and print one `RSS {json}` line. It does nothing outside a child process.
#[test]
fn child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let (path, batch_rows) = spec.split_once('|').unwrap();
    let directory =
        tempfile::tempdir_in(std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".into())).unwrap();
    let project = directory.path().join("project");
    fs::create_dir(&project).unwrap();
    let graph = GraphForge::new(project.to_str()).unwrap();
    let mut session = graph
        .begin_import_session(
            OperationId(v7(7)),
            ImportSessionLimits {
                batch_rows: batch_rows.parse().unwrap(),
                ..ImportSessionLimits::default()
            },
        )
        .unwrap();
    session
        .register_parquet(BulkInputKind::Node, Path::new(path))
        .unwrap();
    let before = (status_kib("VmRSS:"), io_bytes("rchar:"), io_bytes("wchar:"));
    let result = session.validate(&graph);
    let (outcome, report) = match &result {
        Ok(progress) => (
            "imported".to_owned(),
            progress
                .construction
                .as_ref()
                .and_then(|construction| construction.bulk_build.clone()),
        ),
        Err(GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }) => ("resource_limit".to_owned(), None),
        Err(error) => (format!("error: {error}"), None),
    };
    let line = serde_json::json!({
        "outcome": outcome,
        "peak_rss_bytes": status_kib("VmHWM:") * 1024,
        "start_rss_bytes": before.0 * 1024,
        "read_bytes": io_bytes("rchar:") - before.1,
        "written_bytes": io_bytes("wchar:") - before.2,
        "scratch_write_bytes": report.as_ref().map_or(0, |report| report.scratch_write_bytes),
        "property_scratch_write_bytes": report
            .as_ref()
            .map_or(0, |report| report.property_scratch_write_bytes),
        "property_scratch_read_bytes": report
            .as_ref()
            .map_or(0, |report| report.property_scratch_read_bytes),
        "source_workspace_capacity_bytes": report
            .as_ref()
            .map_or(0, |report| report.source_workspace_capacity_bytes),
        "source_workspace_peak_bytes": report
            .as_ref()
            .map_or(0, |report| report.source_workspace_peak_bytes),
    });
    println!("RSS {line}");
}

struct Measured {
    outcome: String,
    peak: u64,
    json: serde_json::Value,
}

/// Run the `child` test in a fresh process importing `path` under `budget`, with
/// the address space capped well above the budget so a decode that ignores it
/// aborts instead of taking the host with it.
fn measure(name: &str, path: &Path, batch_rows: usize, input_bytes: u64) -> Measured {
    let executable = std::env::current_exe().unwrap();
    let output = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "ulimit -v {}; exec \"$0\" --exact child --nocapture --test-threads 1",
            12 * 1024 * 1024
        ))
        .arg(executable)
        .env(CHILD_ENV, format!("{}|{batch_rows}", path.display()))
        .env("GF_BULK_BUILD_MEMORY_BUDGET_BYTES", BUDGET.to_string())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find_map(|line| line.split_once("RSS {").map(|(_, rest)| rest))
        .unwrap_or_else(|| {
            panic!(
                "{name}: the child printed no result ({:?}):\n{stdout}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
        });
    let mut json: serde_json::Value = serde_json::from_str(&format!("{{{line}")).unwrap();
    json["case"] = name.into();
    json["budget_bytes"] = BUDGET.into();
    json["input_bytes"] = input_bytes.into();
    println!("{json}");
    if let Ok(report) = std::env::var("GF_RSS_REPORT") {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(report)
            .unwrap();
        writeln!(file, "{json}").unwrap();
    }
    Measured {
        outcome: json["outcome"].as_str().unwrap().to_owned(),
        peak: json["peak_rss_bytes"].as_u64().unwrap(),
        json,
    }
}

fn input(
    directory: &Path,
    name: &str,
    rows: usize,
    distinct: usize,
    bytes: usize,
    compression: Compression,
) -> (PathBuf, u64) {
    let path = directory.join(name);
    write_dictionary_nodes(&path, rows, distinct, bytes, compression);
    let size = fs::metadata(&path).unwrap().len();
    (path, size)
}

/// Growing the decoded payload fourfold, at a fixed budget, grows the resident
/// set by no more than a small constant: nothing a batch decodes to outlives it.
#[test]
fn the_resident_set_does_not_follow_the_payload() {
    let directory =
        tempfile::tempdir_in(std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".into())).unwrap();
    // 8 KiB values, 1,024-row batches (8 MiB each): 32, 64 and 128 MB decoded
    // from files of a few hundred kilobytes.
    let mut peaks = Vec::new();
    for rows in [4_096, 8_192, 16_384] {
        let (path, size) = input(
            directory.path(),
            &format!("{rows}.parquet"),
            rows,
            4,
            8 << 10,
            Compression::UNCOMPRESSED,
        );
        let run = measure(&format!("dictionary_{rows}_rows"), &path, 1_024, size);
        assert_eq!(run.outcome, "imported", "{}", run.json);
        assert!(run.peak <= BUDGET, "{}", run.json);
        assert!(
            run.json["property_scratch_write_bytes"].as_u64().unwrap() > 0,
            "the build must have gone through scratch: {}",
            run.json
        );
        peaks.push(run.peak);
    }
    assert!(
        peaks[2] <= peaks[0] + 64 * MIB,
        "the resident set followed the payload: {peaks:?}"
    );
}

/// Inputs no piece of the window can hold are refused, and the process that
/// refuses them stays small.
#[test]
fn inputs_that_would_decode_past_the_window_are_refused_while_small() {
    let directory =
        tempfile::tempdir_in(std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".into())).unwrap();
    // One 40 MiB value, stored once and compressed to kilobytes: its decoded
    // buffers outgrow the 64 MiB window, so no row of it is admitted.
    let (path, size) = input(
        directory.path(),
        "expansion.parquet",
        2,
        1,
        40 << 20,
        Compression::GZIP(Default::default()),
    );
    assert!(size < 4 * MIB, "{size} bytes stored");
    let run = measure("dictionary_expansion_40_mib_value", &path, 2, size);
    assert_eq!(run.outcome, "resource_limit", "{}", run.json);
    assert!(
        run.peak <= BUDGET / 5,
        "refusing it took {} MiB: {}",
        run.peak / MIB,
        run.json
    );
}
