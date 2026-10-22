//! `gf` warns on standard error, and in `--json`, when data was written by
//! another Core version (#1936).
//!
//! "Core X" data is made by the real writer and then relabelled: a generation
//! by changing its one serialized `producing_core_version` field and recomputing
//! the digest `CURRENT` records; a package by changing the declared version and
//! recomputing `package_digest` and the BagIt checksums.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

#[path = "support/core_version.rs"]
mod core_version;
use core_version::{RUNNING, relabel_generation, relabel_package};

const CORE_X: &str = "0.0.1";

fn gf(args: &[&str]) -> Output {
    Command::new(fs::canonicalize(env!("CARGO_BIN_EXE_gf")).expect("same-build gf binary"))
        .args(args)
        .output()
        .expect("run gf")
}

fn stdout_json(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "gf failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stable JSON result")
}

/// Export the project's current state as an expanded package. The first call
/// also creates the project through the real writer.
fn export(project: &Path, package: &Path, json: bool) -> Output {
    let mut args = vec!["--project", project.to_str().unwrap()];
    if json {
        args.push("--json");
    }
    args.extend([
        "export",
        "--current",
        "--format",
        "expanded",
        "--profile",
        "complete",
        "--output",
        package.to_str().unwrap(),
    ]);
    gf(&args)
}

fn import(project: &Path, package: &Path, key: &str, json: bool) -> Output {
    let mut args = vec!["--project", project.to_str().unwrap()];
    if json {
        args.push("--json");
    }
    args.extend([
        "import",
        "--input",
        package.to_str().unwrap(),
        "--idempotency-key",
        key,
    ]);
    gf(&args)
}

fn assert_names_both(warning: &Value) {
    assert_eq!(warning["code"], "GF_CORE_VERSION_MISMATCH");
    assert_eq!(warning["producing_core_version"], CORE_X);
    assert_eq!(warning["running_core_version"], RUNNING);
    let message = warning["message"].as_str().unwrap();
    assert!(
        message.contains(CORE_X) && message.contains(RUNNING),
        "{message}"
    );
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn opening_data_from_another_core_warns_on_stderr_in_text() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    // The first command creates the project; the running Core wrote it.
    assert!(
        export(&project, &root.path().join("package"), true)
            .status
            .success()
    );
    relabel_generation(&project, CORE_X);

    let text = export(&project, &root.path().join("text"), false);
    assert!(text.status.success(), "{}", stderr_text(&text));
    let stderr = stderr_text(&text);
    assert!(
        stderr.contains("warning GF_CORE_VERSION_MISMATCH:"),
        "{stderr}"
    );
    assert!(
        stderr.contains(CORE_X) && stderr.contains(RUNNING),
        "{stderr}"
    );
}

#[test]
fn under_json_a_mismatch_with_success_is_one_stdout_document_and_silent_stderr() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    assert!(
        export(&project, &root.path().join("package"), true)
            .status
            .success()
    );
    relabel_generation(&project, CORE_X);

    let as_json = export(&project, &root.path().join("json"), true);
    let result = stdout_json(&as_json);
    let warnings = result["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1, "{result}");
    assert_names_both(&warnings[0]);
    // The command's own result is still there.
    assert_eq!(result["contract"], "graphforge-portable-export/1");
    assert_eq!(stderr_text(&as_json), "");
}

#[test]
fn under_json_a_mismatch_with_failure_is_one_stderr_document_with_error_and_warnings() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    let package = root.path().join("package");
    assert!(export(&project, &package, true).status.success());
    relabel_generation(&project, CORE_X);

    // Exporting onto the package that already exists fails after the open.
    let failed = export(&project, &package, true);
    assert!(!failed.status.success());
    let stderr = stderr_text(&failed);
    let mut documents = serde_json::Deserializer::from_str(&stderr).into_iter::<Value>();
    let document = documents.next().expect("one document").expect("valid JSON");
    assert!(
        documents.next().is_none(),
        "more than one document: {stderr}"
    );
    assert!(document["error"]["code"].is_string(), "{document}");
    assert_eq!(
        document["warnings"].as_array().unwrap().len(),
        1,
        "{document}"
    );
    assert_names_both(&document["warnings"][0]);
    assert!(failed.stdout.is_empty() || serde_json::from_slice::<Value>(&failed.stdout).is_ok());
}

#[test]
fn a_failure_without_a_mismatch_has_no_warnings_key() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    let package = root.path().join("package");
    assert!(export(&project, &package, true).status.success());

    let failed = export(&project, &package, true);
    assert!(!failed.status.success());
    let document: Value = serde_json::from_str(stderr_text(&failed).trim()).unwrap();
    assert!(document.get("warnings").is_none(), "{document}");
}

#[test]
fn opening_data_from_the_running_core_prints_no_warning() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    assert!(
        export(&project, &root.path().join("one"), true)
            .status
            .success()
    );

    for json in [false, true] {
        let again = export(&project, &root.path().join(format!("again-{json}")), json);
        assert!(again.status.success());
        assert_eq!(stderr_text(&again), "", "json={json}");
    }
}

#[test]
fn importing_a_package_from_another_core_warns_and_still_imports() {
    let root = TempDir::new().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    let package = root.path().join("package");
    assert!(export(&source, &package, true).status.success());
    relabel_package(&package, CORE_X);

    let as_json = import(
        &root.path().join("into-json"),
        &package,
        "00000000-0000-0000-0000-000000000001",
        true,
    );
    let result = stdout_json(&as_json);
    let warnings = result["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1, "{result}");
    assert_names_both(&warnings[0]);
    assert_eq!(stderr_text(&as_json), "");

    let text = import(
        &root.path().join("into-text"),
        &package,
        "00000000-0000-0000-0000-000000000002",
        false,
    );
    assert!(text.status.success(), "{}", stderr_text(&text));
    assert!(String::from_utf8_lossy(&text.stdout).starts_with("imported generation"));
    let stderr = stderr_text(&text);
    assert!(
        stderr.contains("warning GF_CORE_VERSION_MISMATCH:"),
        "{stderr}"
    );
    assert!(
        stderr.contains(CORE_X) && stderr.contains(RUNNING),
        "{stderr}"
    );
}

#[test]
fn importing_a_package_from_the_running_core_prints_no_warning() {
    let root = TempDir::new().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    let package = root.path().join("package");
    assert!(export(&source, &package, true).status.success());

    let as_json = import(
        &root.path().join("into-json"),
        &package,
        "00000000-0000-0000-0000-000000000003",
        true,
    );
    let result = stdout_json(&as_json);
    assert!(
        result["warnings"].as_array().unwrap().is_empty(),
        "{result}"
    );
    assert_eq!(stderr_text(&as_json), "");

    let text = import(
        &root.path().join("into-text"),
        &package,
        "00000000-0000-0000-0000-000000000004",
        false,
    );
    assert!(text.status.success());
    assert_eq!(stderr_text(&text), "");
}

fn gf_in(project: &Path, json: bool, args: &[&str]) -> Output {
    let mut all = vec!["--project", project.to_str().unwrap()];
    if json {
        all.push("--json");
    }
    all.extend_from_slice(args);
    gf(&all)
}

/// A project that has a checkpoint and a later commit, then relabelled as
/// written by another Core (the checkpoint pins an earlier generation).
fn project_with_checkpoint(root: &Path) -> std::path::PathBuf {
    let project = root.join("project");
    fs::create_dir(&project).unwrap();
    let created = gf_in(
        &project,
        false,
        &[
            "checkpoint",
            "create",
            "before",
            "--idempotency-key",
            "00000000-0000-0000-0000-0000000000c1",
        ],
    );
    assert!(created.status.success(), "{}", stderr_text(&created));
    let graph = graphforge_api::GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Item {score: 1})").unwrap();
    drop(graph);
    relabel_generation(&project, CORE_X);
    project
}

#[test]
fn a_revert_preview_of_another_cores_project_warns_in_text_and_json() {
    let root = TempDir::new().unwrap();
    let project = project_with_checkpoint(root.path());

    let text = gf_in(&project, false, &["revert", "before", "--preview"]);
    assert!(text.status.success(), "{}", stderr_text(&text));
    let stderr = stderr_text(&text);
    assert!(
        stderr.contains("warning GF_CORE_VERSION_MISMATCH:"),
        "{stderr}"
    );
    assert!(
        stderr.contains(CORE_X) && stderr.contains(RUNNING),
        "{stderr}"
    );

    let as_json = gf_in(&project, true, &["revert", "before", "--preview"]);
    let document = stdout_json(&as_json);
    assert_eq!(document["contract"], "graphforge-revert-preview/1");
    assert_eq!(
        document["warnings"].as_array().unwrap().len(),
        1,
        "{document}"
    );
    assert_names_both(&document["warnings"][0]);
    assert_eq!(stderr_text(&as_json), "");
}

#[test]
fn a_revert_preview_of_a_matching_project_has_no_warnings() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    let created = gf_in(
        &project,
        false,
        &[
            "checkpoint",
            "create",
            "before",
            "--idempotency-key",
            "00000000-0000-0000-0000-0000000000c2",
        ],
    );
    assert!(created.status.success());

    let as_json = gf_in(&project, true, &["revert", "before", "--preview"]);
    let document = stdout_json(&as_json);
    assert!(document.get("warnings").is_none(), "{document}");
    assert_eq!(stderr_text(&as_json), "");
    let text = gf_in(&project, false, &["revert", "before", "--preview"]);
    assert!(text.status.success());
    assert_eq!(stderr_text(&text), "");
}

fn written_project(root: &Path) -> std::path::PathBuf {
    let project = root.join("project");
    fs::create_dir(&project).unwrap();
    assert!(export(&project, &root.join("seed"), true).status.success());
    project
}

#[test]
fn a_multi_output_json_query_stamps_only_its_first_document() {
    let root = TempDir::new().unwrap();
    let project = written_project(root.path());
    relabel_generation(&project, CORE_X);
    let (a, b) = (root.path().join("a.parquet"), root.path().join("b.parquet"));

    let both = gf_in(
        &project,
        true,
        &[
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            a.to_str().unwrap(),
            "--cypher",
            "RETURN 2 AS n",
            "--output",
            b.to_str().unwrap(),
        ],
    );
    assert!(both.status.success(), "{}", stderr_text(&both));
    let documents: Vec<Value> = String::from_utf8(both.stdout.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).expect("each stdout line is JSON"))
        .collect();
    assert_eq!(documents.len(), 2);
    assert_eq!(documents[0]["warnings"].as_array().unwrap().len(), 1);
    assert_names_both(&documents[0]["warnings"][0]);
    assert!(documents[1].get("warnings").is_none(), "{}", documents[1]);
    assert_eq!(stderr_text(&both), "");
}

#[test]
fn a_failing_second_query_gives_one_stderr_document_carrying_the_warnings() {
    let root = TempDir::new().unwrap();
    let project = written_project(root.path());
    relabel_generation(&project, CORE_X);
    let (a, b) = (root.path().join("a.parquet"), root.path().join("b.parquet"));

    let failed = gf_in(
        &project,
        true,
        &[
            "query",
            "--cypher",
            "RETURN 1 AS n",
            "--output",
            a.to_str().unwrap(),
            "--cypher",
            "THIS IS NOT CYPHER",
            "--output",
            b.to_str().unwrap(),
        ],
    );
    assert!(!failed.status.success());
    let stderr = stderr_text(&failed);
    let mut documents = serde_json::Deserializer::from_str(&stderr).into_iter::<Value>();
    let document = documents.next().expect("one document").expect("valid JSON");
    assert!(
        documents.next().is_none(),
        "more than one document: {stderr}"
    );
    assert!(document["error"]["code"].is_string(), "{document}");
    assert_names_both(&document["warnings"][0]);
    for line in String::from_utf8(failed.stdout).unwrap().lines() {
        serde_json::from_str::<Value>(line).expect("each stdout line is JSON");
    }
}

fn decision_file(root: &Path) -> std::path::PathBuf {
    let question = uuid::Uuid::now_v7();
    let item = uuid::Uuid::now_v7();
    let request = serde_json::json!({
        "input": {
            "generation_uuid": uuid::Uuid::now_v7(),
            "version_uuid": null,
            "projection_sha256": vec![7; 32],
            "selection_sha256": vec![9; 32],
            "selected_item_uuids": [item],
        },
        "producer": { "name": "offline CLI fixture" },
        "questions": [{
            "question_uuid": question,
            "text": "Where should this item go?",
            "item_uuids": [item],
            "kind": { "kind": "choice", "allowed_choices": ["research", "human_review"] },
        }],
        "results": [{
            "question_uuid": question,
            "item_uuid": item,
            "status": "answered",
            "value": { "kind": "choice", "value": "research" },
        }],
    });
    let path = root.join("decision.json");
    fs::write(&path, serde_json::to_vec(&request).unwrap()).unwrap();
    path
}

fn validate_decisions(project: &Path, input: &Path, output: &Path, json: bool) -> Output {
    gf_in(
        project,
        json,
        &[
            "research",
            "decision",
            "validate",
            "--file",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
        ],
    )
}

#[test]
fn decision_validate_emits_one_json_document_under_json_and_text_otherwise() {
    let root = TempDir::new().unwrap();
    let project = written_project(root.path());
    let input = decision_file(root.path());
    let arrow = root.path().join("decision.arrow");

    let text = validate_decisions(&project, &input, &arrow, false);
    assert!(text.status.success(), "{}", stderr_text(&text));
    assert_eq!(
        String::from_utf8_lossy(&text.stdout),
        "validated 1 decision rows\n"
    );

    let as_json = validate_decisions(&project, &input, &arrow, true);
    let document = stdout_json(&as_json);
    assert_eq!(document["validated_rows"], 1);
    assert_eq!(document["output"], arrow.to_str().unwrap());
    assert!(document.get("warnings").is_none());
    assert_eq!(stderr_text(&as_json), "");
}

#[test]
fn decision_validate_on_another_cores_project_carries_the_warnings_in_its_document() {
    let root = TempDir::new().unwrap();
    let project = written_project(root.path());
    relabel_generation(&project, CORE_X);
    let input = decision_file(root.path());
    let arrow = root.path().join("decision.arrow");

    let as_json = validate_decisions(&project, &input, &arrow, true);
    let document = stdout_json(&as_json);
    assert_eq!(document["validated_rows"], 1);
    assert_eq!(
        document["warnings"].as_array().unwrap().len(),
        1,
        "{document}"
    );
    assert_names_both(&document["warnings"][0]);
    assert_eq!(stderr_text(&as_json), "");

    let text = validate_decisions(&project, &input, &arrow, false);
    assert!(stderr_text(&text).contains("warning GF_CORE_VERSION_MISMATCH:"));
    assert_eq!(
        String::from_utf8_lossy(&text.stdout),
        "validated 1 decision rows\n"
    );
}
