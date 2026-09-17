//! CLI conformance for `gf query`, the streaming Cypher result sink (#360),
//! and the one-process-per-open read-phase surfaces built on it.
//!
//! The underlying `execute_to_result_sink_with_evidence` contract is covered at
//! the API level. These tests cover the command wrapper: sink option defaults,
//! format selection, the JSON and human receipt shapes, repeated statement
//! pairs, the failure paths the wrapper owns rather than delegates, and the
//! `storage-attribution --recovery` receipt pairing the ladder's reopen phase
//! relies on.

use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

fn gf(project: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(project)
        .args(args)
        .output()
        .expect("run same-build gf binary")
}

fn initialized_project() -> TempDir {
    let project = TempDir::new().unwrap();
    graphforge_storage::open_or_initialize_project(project.path()).unwrap();
    project
}

fn stdout_json(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "expected success, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("deterministic JSON receipt")
}

/// The JSON receipt reports the sink contract, destination and exact progress.
#[test]
fn json_receipt_reports_the_sink_contract_and_progress() {
    let project = initialized_project();
    let sink = project.path().join("rows.parquet");
    let output = gf(
        project.path(),
        &[
            "--json",
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            sink.to_str().unwrap(),
        ],
    );

    let receipt = stdout_json(&output);
    assert_eq!(receipt["contract"], "graphforge-result-sink/2");
    assert_eq!(receipt["format"], "Parquet");
    assert_eq!(receipt["complete"], Value::Bool(true));
    assert_eq!(receipt["rows"], 1);
    assert!(
        receipt["bytes"].as_u64().unwrap() > 0,
        "a written sink reports nonzero bytes"
    );
    assert!(
        receipt["result_sha256"].is_string(),
        "the receipt fingerprints its result"
    );
    assert!(sink.is_file(), "the sink file is written to --output");
}

/// Without `--json` the command reports the same outcome in the human form.
#[test]
fn human_receipt_reports_destination_and_counts() {
    let project = initialized_project();
    let sink = project.path().join("human.parquet");
    let output = gf(
        project.path(),
        &[
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            sink.to_str().unwrap(),
        ],
    );

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).expect("UTF-8 receipt");
    assert!(text.starts_with("wrote "), "unexpected receipt: {text}");
    assert!(text.contains("rows=1"), "unexpected receipt: {text}");
    assert!(text.contains("bytes="), "unexpected receipt: {text}");
    assert!(sink.is_file());
}

/// `--format arrow-ipc` selects the Arrow IPC sink rather than the Parquet default.
#[test]
fn arrow_ipc_format_is_selected_by_flag() {
    let project = initialized_project();
    let sink = project.path().join("rows.arrow");
    let output = gf(
        project.path(),
        &[
            "--json",
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            sink.to_str().unwrap(),
            "--format",
            "arrow-ipc",
        ],
    );

    let receipt = stdout_json(&output);
    assert_eq!(receipt["format"], "ArrowIpc");
    assert_eq!(receipt["complete"], Value::Bool(true));
    assert!(sink.is_file());
}

/// Explicit sink bounds are accepted and do not change the reported result.
#[test]
fn explicit_sink_bounds_do_not_change_the_result() {
    let project = initialized_project();
    let default_sink = project.path().join("default.parquet");
    let bounded_sink = project.path().join("bounded.parquet");

    let default = stdout_json(&gf(
        project.path(),
        &[
            "--json",
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            default_sink.to_str().unwrap(),
        ],
    ));
    let bounded = stdout_json(&gf(
        project.path(),
        &[
            "--json",
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            bounded_sink.to_str().unwrap(),
            "--max-batch-rows",
            "1",
            "--max-row-group-rows",
            "1",
        ],
    ));

    assert_eq!(bounded["rows"], default["rows"]);
    assert_eq!(bounded["result_sha256"], default["result_sha256"]);
    assert_eq!(bounded["complete"], Value::Bool(true));
}

/// A query the binder rejects fails the command instead of writing a sink.
#[test]
fn a_rejected_query_fails_without_writing_a_sink() {
    let project = initialized_project();
    let sink = project.path().join("never.parquet");
    let output = gf(
        project.path(),
        &[
            "--json",
            "query",
            "--cypher",
            "RETURN undeclaredThing",
            "--output",
            sink.to_str().unwrap(),
        ],
    );

    assert!(
        !output.status.success(),
        "a rejected query must not report success"
    );
    assert!(
        !sink.is_file(),
        "a rejected query must not leave a partial sink behind"
    );
    assert!(
        !String::from_utf8_lossy(&output.stderr).is_empty(),
        "a rejected query reports a diagnostic"
    );
}

/// Repeated `--cypher`/`--output` pairs run against one project open and emit
/// one receipt line per statement, in statement order.
#[test]
fn repeated_pairs_emit_one_receipt_per_statement_in_order() {
    let project = initialized_project();
    let first = project.path().join("first.parquet");
    let second = project.path().join("second.parquet");
    let output = gf(
        project.path(),
        &[
            "--json",
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            first.to_str().unwrap(),
            "--cypher",
            "RETURN 2 AS n, 3 AS m",
            "--output",
            second.to_str().unwrap(),
        ],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).expect("UTF-8 receipts");
    let receipts: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("one JSON receipt per line"))
        .collect();
    assert_eq!(receipts.len(), 2);
    for (receipt, sink) in receipts.iter().zip([&first, &second]) {
        assert_eq!(receipt["contract"], "graphforge-result-sink/2");
        assert_eq!(receipt["complete"], Value::Bool(true));
        assert_eq!(receipt["rows"], 1);
        assert_eq!(receipt["destination"], sink.to_str().unwrap());
        assert!(sink.is_file());
        // Every receipt carries a closed, reconciled attribution of its own.
        let io = receipt["application_io"].as_object().unwrap();
        for field in ["read_bytes", "read_calls", "write_bytes", "write_calls"] {
            let phases: u64 = io["phases"]
                .as_object()
                .unwrap()
                .values()
                .map(|row| row[field].as_u64().unwrap())
                .sum();
            assert_eq!(io["totals"][field].as_u64().unwrap(), phases);
        }
    }
    assert_ne!(receipts[0]["result_sha256"], receipts[1]["result_sha256"]);
    // The first receipt owns the project open; later receipts attribute only
    // the I/O since the previous receipt, so the lines sum to the process.
    let read_calls = |receipt: &Value| {
        receipt["application_io"]["totals"]["read_calls"]
            .as_u64()
            .unwrap()
    };
    assert!(read_calls(&receipts[0]) > 0);
    assert!(read_calls(&receipts[1]) <= read_calls(&receipts[0]));
}

/// Without `--json`, each statement reports its own human line.
#[test]
fn repeated_pairs_report_one_human_line_per_statement() {
    let project = initialized_project();
    let first = project.path().join("first.parquet");
    let second = project.path().join("second.parquet");
    let output = gf(
        project.path(),
        &[
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            first.to_str().unwrap(),
            "--cypher",
            "RETURN 2 AS n",
            "--output",
            second.to_str().unwrap(),
        ],
    );
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("UTF-8 receipt");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2, "unexpected receipt: {text}");
    assert!(lines.iter().all(|line| line.starts_with("wrote ")));
    assert!(lines[0].contains("first.parquet") && lines[1].contains("second.parquet"));
}

/// An unpaired `--cypher` or a repeated destination is refused before any
/// statement runs, so no sink is written.
#[test]
fn unpaired_or_duplicate_destinations_are_rejected_before_any_sink_is_written() {
    let project = initialized_project();
    let sink = project.path().join("never.parquet");
    let unpaired = gf(
        project.path(),
        &[
            "--json",
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--cypher",
            "RETURN 2 AS n",
            "--output",
            sink.to_str().unwrap(),
        ],
    );
    assert!(!unpaired.status.success());
    assert!(
        !sink.is_file(),
        "an unpaired statement must not write a sink"
    );
    let duplicate = gf(
        project.path(),
        &[
            "--json",
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            sink.to_str().unwrap(),
            "--cypher",
            "RETURN 2 AS n",
            "--output",
            sink.to_str().unwrap(),
        ],
    );
    assert!(!duplicate.status.success());
    assert!(
        !sink.is_file(),
        "a duplicate destination must not write a sink"
    );
    let missing = gf(
        project.path(),
        &["--json", "query", "--output", sink.to_str().unwrap()],
    );
    assert!(!missing.status.success());
}

fn json_lines(output: &Output) -> Vec<Value> {
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout.clone())
        .expect("UTF-8 receipts")
        .lines()
        .map(|line| serde_json::from_str(line).expect("one JSON receipt per line"))
        .collect()
}

/// `storage-attribution --recovery` emits the recovery receipt of its first
/// open, then the attribution receipt, so one process carries both receipts
/// of the ladder's reopen phase with the same content as two processes.
#[test]
fn storage_attribution_recovery_flag_emits_the_recovery_receipt_first() {
    let project = initialized_project();
    let standalone = json_lines(&gf(project.path(), &["--json", "recovery"]));
    assert_eq!(standalone.len(), 1);
    let combined = json_lines(&gf(
        project.path(),
        &["--json", "storage-attribution", "--recovery"],
    ));
    assert_eq!(
        combined.len(),
        2,
        "recovery receipt then attribution receipt"
    );
    let (recovery, attribution) = (&combined[0], &combined[1]);
    assert_eq!(recovery["kind"], "project_open");
    assert!(
        recovery["selected_generation_class"]
            .as_str()
            .is_some_and(|class| !class.is_empty()),
        "the receipt names the selected generation class"
    );
    let stable = |value: &Value| {
        let mut value = value.clone();
        value.as_object_mut().unwrap().remove("elapsed_ms");
        value
    };
    assert_eq!(stable(recovery), stable(&standalone[0]));
    assert_eq!(
        attribution["contract"],
        "graphforge-storage-attribution-command/1"
    );
    assert_eq!(attribution["reopen_agrees"], Value::Bool(true));
    assert_eq!(
        json_lines(&gf(project.path(), &["--json", "storage-attribution"])).len(),
        1
    );
}
