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
fn opening_data_from_another_core_warns_on_stderr_in_text_and_json() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    let package = root.path().join("package");
    // The first command creates the project; the running Core wrote it.
    assert!(export(&project, &package, true).status.success());
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

    let as_json = export(&project, &root.path().join("json"), true);
    assert!(as_json.status.success(), "{}", stderr_text(&as_json));
    let line: Value = serde_json::from_str(stderr_text(&as_json).trim()).unwrap();
    assert_names_both(&line["warning"]);
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
    let line: Value = serde_json::from_str(stderr_text(&as_json).trim()).unwrap();
    assert_names_both(&line["warning"]);

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
