//! What a task reserves covers what its decode holds (#1918).
//!
//! Each case decodes every task of a source in a fresh process, reading the
//! process's own high-water resident set around the reads, and compares the growth
//! with the most the task reservations asked of the pool. A bound that the decode
//! outgrew would be a promise broken; one far above it would refuse inputs that fit.

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
fn write_wide(path: &Path) {
    let rows = 1 << 18;
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
fn write_dictionaries(path: &Path) {
    let rows = 8_192;
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

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
        .unwrap()
}

/// In the child: decode every task of the source and print what it reserved and
/// how far the resident set rose.
fn child(spec: &str) {
    let (path, batch_rows) = spec.split_once('|').unwrap();
    let (_directory, _project, graph) = fixture();
    let mut session = graph
        .begin_import_session(
            OperationId(Uuid::now_v7()),
            ImportSessionLimits {
                batch_rows: batch_rows.parse().unwrap(),
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
    let pool = SourceWorkspace::new(u64::MAX / 2);
    source.reader.bind_workspace(&pool);
    let before = status_kib("VmHWM:") * 1024;
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
    let after = status_kib("VmHWM:") * 1024;
    println!(
        "RESERVATION {}",
        serde_json::json!({
            "rows": rows,
            "reserved": pool.peak_bytes(),
            "rise": after - before,
            "planned": source.reader.decoded_workspace_bytes(),
        })
    );
}

fn measure(path: &Path, batch_rows: usize) -> serde_json::Value {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture", "--test-threads", "1"])
        .env(CHILD, format!("{}|{batch_rows}", path.display()))
        .output()
        .unwrap();
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
    let wide = directory.path().join("wide.parquet");
    write_wide(&wide);
    let dictionaries = directory.path().join("dictionaries.parquet");
    write_dictionaries(&dictionaries);
    for (name, path, batch_rows) in [
        ("wide pages", &wide, 65_536),
        ("fat dictionaries", &dictionaries, 512),
    ] {
        let measured = measure(path, batch_rows);
        let reserved = measured["reserved"].as_u64().unwrap();
        let rise = measured["rise"].as_u64().unwrap();
        println!("{name}: {measured}");
        // The reads the digest holds ahead of its hashed prefix (a sixty-fourth of
        // the 4 GiB budget, planned apart from the task) and the process's own
        // allocator arenas and thread stacks are not the task's.
        let slack = (4_u64 << 30) / 64 + (24 << 20);
        assert!(
            rise <= reserved + slack,
            "{name}: the decode's resident set rose {rise} bytes; the task reserved {reserved}"
        );
        assert!(
            rise * 2 >= reserved,
            "{name}: the task reserved {reserved} bytes for a decode that held {rise}"
        );
    }
}
