//! What a task reserves covers what its decode holds (#1918).
//!
//! Each case decodes every task of a source in a fresh process. The size series
//! holds the source workspace cap constant while encoded and decoded input grows.
//! It reports the absolute high-water mark and its pre-decode baseline: subtracting
//! two high-water marks can hide a later peak when planning already set an earlier one.

use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_storage::SourceWorkspace;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::super::test_fixtures::fixture;
use super::super::*;

const CHILD: &str = "GF_RESERVATION_CHILD";
const TEST: &str =
    "import_session::bulk_source::reservation_tests::a_task_reserves_what_its_decode_holds";
/// What the pool may grant: a task's pieces at the reader's capacity beside
/// the exact copy the builder is handed, with the task's page and
/// normalization workspace.
const WORKSPACE_BUDGET: u64 = 384 << 20;
/// What the process may actually grow by decoding every task.
const PROCESS_GROWTH_BOUND: u64 = 256 << 20;
const PROCESS_OVERHEAD_SLACK: u64 = 64 << 20;

fn v7(value: u128) -> Uuid {
    Uuid::from_u128((value << 80) | (0x7 << 76) | (0x2 << 62) | value)
}

fn nodes(rows: usize, properties: Vec<(String, ArrayRef)>) -> RecordBatch {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for row in 0..rows {
        ids.append_value(v7(1 + row as u128).as_bytes()).unwrap();
    }
    let mut fields = vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(ids.finish()),
        Arc::new(StringArray::from(vec!["Thing"; rows])),
    ];
    for (name, array) in properties {
        fields.push(Field::new(name, array.data_type().clone(), true));
        columns.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// 96 integer columns in 2 MiB pages: the pages are what a decode holds.
fn write_wide(path: &Path, rows: usize) {
    let properties = (0..96)
        .map(|column| {
            let values = (0..rows as i64)
                .map(|row| row * 7 + i64::from(column))
                .collect::<Vec<_>>();
            (
                format!("p{column:03}"),
                Arc::new(Int64Array::from(values)) as ArrayRef,
            )
        })
        .collect();
    let batch = nodes(rows, properties);
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        batch.schema(),
        Some(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_data_page_size_limit(2 << 20)
                .set_data_page_row_count_limit(usize::MAX)
                .set_max_row_group_row_count(Some(rows))
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Four strings columns of 256 distinct 16 KiB values: 4 MiB dictionaries, and 8 MiB
/// of strings per 512 rows each.
fn write_dictionaries(path: &Path, rows: usize) {
    let values = (0..256)
        .map(|entry| {
            char::from(b'a' + (entry % 26) as u8)
                .to_string()
                .repeat(16 << 10)
                + &entry.to_string()
        })
        .collect::<Vec<_>>();
    let properties = (0..4)
        .map(|column| {
            let strings = (0..rows)
                .map(|row| values[(row * 7 + column) % 256].as_str())
                .collect::<Vec<_>>();
            (
                format!("t{column}"),
                Arc::new(StringArray::from(strings)) as ArrayRef,
            )
        })
        .collect();
    let batch = nodes(rows, properties);
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        batch.schema(),
        Some(
            WriterProperties::builder()
                .set_dictionary_page_size_limit(64 << 20)
                .set_max_row_group_row_count(Some(rows))
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn decoded_dictionary_payload_bytes(rows: usize) -> u64 {
    let bytes = (0..rows)
        .flat_map(|row| (0..4).map(move |column| (row * 7 + column) % 256))
        .map(|entry| (16 << 10) + entry.to_string().len())
        .sum::<usize>();
    u64::try_from(bytes).unwrap()
}

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
        .unwrap()
}

fn io_counters() -> (u64, u64, u64) {
    let io = std::fs::read_to_string("/proc/self/io").unwrap();
    let value = |name: &str| {
        io.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| rest.trim().parse().ok())
            .unwrap()
    };
    (value("rchar:"), value("read_bytes:"), value("write_bytes:"))
}

/// In the child: decode every task and report absolute process and reservation
/// measurements. Scratch I/O is not exercised by this direct source-reader test.
fn child(spec: &str) {
    let mut parts = spec.split('|');
    let path = parts.next().unwrap();
    let project_path = parts.next().unwrap();
    let batch_rows: usize = parts.next().unwrap().parse().unwrap();
    let decoded_payload_bytes: u64 = parts.next().unwrap().parse().unwrap();
    assert!(parts.next().is_none());
    let source_bytes = std::fs::metadata(path).unwrap().len();
    let graph = GraphForge::new(Some(project_path)).unwrap();
    let baseline_hwm = status_kib("VmHWM:") * 1024;
    let before_rss = status_kib("VmRSS:") * 1024;
    // Reading /proc/self/io contributes a small amount to rchar; keep both raw
    // endpoints so the measurement remains transparent.
    let io_before = io_counters();
    let mut session = graph
        .begin_import_session(
            OperationId(Uuid::now_v7()),
            ImportSessionLimits {
                batch_rows,
                ..ImportSessionLimits::default()
            },
        )
        .unwrap();
    session
        .register_parquet(BulkInputKind::Node, Path::new(path))
        .unwrap();
    bulk_source::TEST_BUDGET.with(|cell| cell.set(Some(4 << 30)));
    let refusals = bulk_source::Refusals::default();
    let digests = bulk_source::Digests::default();
    let plan = session
        .plan_bulk_build(&graph, None, &refusals, &digests)
        .unwrap();
    let source = &plan.nodes[0];
    let pool = SourceWorkspace::new(WORKSPACE_BUDGET);
    source.reader.bind_workspace(&pool).unwrap();
    let pre_decode_hwm = status_kib("VmHWM:") * 1024;
    let pre_decode_rss = status_kib("VmRSS:") * 1024;
    let io_pre_decode = io_counters();
    let mut rows = 0_usize;
    for task in 0..source.tasks {
        source
            .reader
            .read_task(task, &mut |batch| {
                rows += batch.num_rows();
                Ok(())
            })
            .unwrap();
    }
    let after_hwm = status_kib("VmHWM:") * 1024;
    let after_rss = status_kib("VmRSS:") * 1024;
    let io_after = io_counters();
    println!(
        "RESERVATION {}",
        serde_json::json!({
            "rows": rows,
            "source_bytes": source_bytes,
            "decoded_payload_bytes": decoded_payload_bytes,
            "workspace_capacity_bytes": pool.capacity(),
            "reserved": pool.peak_bytes(),
            "baseline_hwm_bytes": baseline_hwm,
            "before_rss_bytes": before_rss,
            "pre_decode_hwm_bytes": pre_decode_hwm,
            "pre_decode_rss_bytes": pre_decode_rss,
            "after_hwm_bytes": after_hwm,
            "after_rss_bytes": after_rss,
            "process_io_before": {"rchar": io_before.0, "read_bytes": io_before.1, "write_bytes": io_before.2},
            "process_io_pre_decode": {"rchar": io_pre_decode.0, "read_bytes": io_pre_decode.1, "write_bytes": io_pre_decode.2},
            "process_io_after": {"rchar": io_after.0, "read_bytes": io_after.1, "write_bytes": io_after.2},
            "planned": source.reader.decoded_workspace_bytes(),
            "source_io": "rchar/read_bytes/write_bytes are process-wide, not source-attributed; /proc reads are included",
            "scratch_io": "not exercised by direct registered-source reads",
        })
    );
}

fn measure(
    path: &Path,
    project_path: &Path,
    batch_rows: usize,
    decoded_payload_bytes: u64,
) -> serde_json::Value {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture", "--test-threads", "1"])
        .env(
            CHILD,
            format!(
                "{}|{}|{batch_rows}|{decoded_payload_bytes}",
                path.display(),
                project_path.display()
            ),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child measurement failed ({:?}):\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find_map(|line| line.split_once("RESERVATION ").map(|(_, rest)| rest))
        .unwrap_or_else(|| {
            panic!(
                "no result ({:?}): {stdout}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
        });
    serde_json::from_str(line).unwrap()
}

#[test]
fn a_task_reserves_what_its_decode_holds() {
    if let Ok(spec) = std::env::var(CHILD) {
        child(&spec);
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    // For each family, source bytes and total decoded payload grow while batch
    // geometry, the admission cap and the process growth bound remain fixed.
    let mut previous_source_bytes = 0;
    for rows in [1 << 13, 1 << 15, 1 << 17] {
        let path = directory.path().join(format!("wide-{rows}.parquet"));
        write_wide(&path, rows);
        let (_project_directory, project_path, graph) = fixture();
        drop(graph);
        let decoded = u64::try_from(rows).unwrap() * 96 * 8;
        let measured = measure(&path, &project_path, 8_192, decoded);
        println!("wide pages rows={rows}: {measured}");
        assert_eq!(measured["rows"].as_u64().unwrap(), rows as u64);
        let source_bytes = measured["source_bytes"].as_u64().unwrap();
        assert!(source_bytes > previous_source_bytes);
        previous_source_bytes = source_bytes;
        assert_eq!(measured["workspace_capacity_bytes"], WORKSPACE_BUDGET);
        assert!(measured["reserved"].as_u64().unwrap() <= WORKSPACE_BUDGET);
        assert_eq!(measured["decoded_payload_bytes"], decoded);
        assert!(
            measured["after_hwm_bytes"].as_u64().unwrap()
                <= measured["baseline_hwm_bytes"].as_u64().unwrap()
                    + PROCESS_GROWTH_BOUND
                    + PROCESS_OVERHEAD_SLACK,
            "wide pages rows={rows}: absolute process VmHWM exceeded baseline + fixed process growth bound + fixed process overhead"
        );
    }

    previous_source_bytes = 0;
    for rows in [512, 2_048, 8_192] {
        let path = directory
            .path()
            .join(format!("dictionaries-{rows}.parquet"));
        write_dictionaries(&path, rows);
        let (_project_directory, project_path, graph) = fixture();
        drop(graph);
        // Count the exact UTF-8 payload bytes, including each value's suffix.
        let decoded = decoded_dictionary_payload_bytes(rows);
        let measured = measure(&path, &project_path, 512, decoded);
        println!("fat dictionaries rows={rows}: {measured}");
        assert_eq!(measured["rows"].as_u64().unwrap(), rows as u64);
        let source_bytes = measured["source_bytes"].as_u64().unwrap();
        assert!(source_bytes > previous_source_bytes);
        previous_source_bytes = source_bytes;
        assert_eq!(measured["workspace_capacity_bytes"], WORKSPACE_BUDGET);
        assert!(measured["reserved"].as_u64().unwrap() <= WORKSPACE_BUDGET);
        assert_eq!(measured["decoded_payload_bytes"], decoded);
        assert!(
            measured["after_hwm_bytes"].as_u64().unwrap()
                <= measured["baseline_hwm_bytes"].as_u64().unwrap()
                    + PROCESS_GROWTH_BOUND
                    + PROCESS_OVERHEAD_SLACK,
            "fat dictionaries rows={rows}: absolute process VmHWM exceeded baseline + fixed process growth bound + fixed process overhead"
        );
    }
}
