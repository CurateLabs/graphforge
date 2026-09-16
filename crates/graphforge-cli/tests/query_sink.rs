//! CLI conformance for `gf query`, the streaming Cypher result sink (#360).
//!
//! The underlying `execute_to_result_sink_with_evidence` contract is covered at
//! the API level. These tests cover the command wrapper: sink option defaults,
//! format selection, the JSON and human receipt shapes, and the failure paths
//! the wrapper owns rather than delegates.

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
