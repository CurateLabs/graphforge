//! Structured and text warnings from the same-build CLI on tmpfs.
#![cfg(target_os = "linux")]
use serde_json::Value;
use std::{
    path::Path,
    process::{Command, Output},
};
fn gf(project: &Path, config: &Path, args: &[&str], json: bool) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_gf"));
    cmd.arg("--project")
        .arg(project)
        .args(args)
        .env("XDG_CONFIG_HOME", config)
        .env_remove("GF_AUTHOR")
        .env_remove("GF_COMMITTER")
        .env_remove("GRAPHFORGE_FILESYSTEM_DURABILITY");
    if json {
        cmd.arg("--json");
    }
    cmd.output().unwrap()
}
fn success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn status_export_and_verify_report_unverified_writer_and_strict_refuses() {
    let root = tempfile::tempdir_in("/dev/shm").unwrap();
    let persistent = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let config = persistent.path().join("config");
    let graph = graphforge_api::GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Item {value: 1})").unwrap();
    drop(graph);
    // Complete export precedes operational Branch history, which requires an
    // explicit research interchange selection to be exported as a complete package.
    let package = persistent.path().join("package.gfpb");
    let exported = gf(
        &project,
        &config,
        &[
            "export",
            "--current",
            "--format",
            "bundle",
            "--profile",
            "complete",
            "--output",
            package.to_str().unwrap(),
        ],
        true,
    );
    success(&exported);
    let exported: Value = serde_json::from_slice(&exported.stdout).unwrap();
    assert_eq!(
        exported["source_unverified_durability"]["filesystem_type"],
        "tmpfs"
    );
    let verified = gf(
        &project,
        &config,
        &["portable", "verify", "--input", package.to_str().unwrap()],
        true,
    );
    success(&verified);
    let verified: Value = serde_json::from_slice(&verified.stdout).unwrap();
    assert_eq!(
        verified["source_unverified_durability"]["filesystem_type"],
        "tmpfs"
    );
    success(&gf(
        &project,
        &config,
        &[
            "config",
            "user",
            "--name",
            "Example",
            "--email",
            "example@example.org",
        ],
        true,
    ));
    success(&gf(&project, &config, &["branch", "main"], true));
    success(&gf(&project, &config, &["switch", "main"], true));
    let text = gf(&project, &config, &["status"], false);
    success(&text);
    assert_eq!(
        String::from_utf8_lossy(&text.stderr)
            .matches("warning GF_UNVERIFIED_FILESYSTEM:")
            .count(),
        1
    );
    assert!(String::from_utf8_lossy(&text.stdout).contains("Durability: unverified"));
    let json = gf(&project, &config, &["status"], true);
    success(&json);
    let json: Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(json["unverified_durability"]["filesystem_type"], "tmpfs");
    assert!(
        json["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["code"] == "GF_UNVERIFIED_FILESYSTEM")
    );
    let strict = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(&project)
        .args(["--json", "status"])
        .env("GRAPHFORGE_FILESYSTEM_DURABILITY", "strict")
        .output()
        .unwrap();
    assert_eq!(strict.status.code(), Some(3));
    let error: Value = serde_json::from_slice(&strict.stderr).unwrap();
    assert_eq!(error["error"]["code"], "GF_UNSUPPORTED_FILESYSTEM");
    for args in [
        vec!["status"],
        vec![
            "import",
            "--input",
            package.to_str().unwrap(),
            "--idempotency-key",
            "11111111-1111-4111-8111-111111111111",
        ],
    ] {
        let invalid = Command::new(env!("CARGO_BIN_EXE_gf"))
            .arg("--project")
            .arg(&project)
            .arg("--json")
            .args(args)
            .env("GRAPHFORGE_FILESYSTEM_DURABILITY", "invalid")
            .output()
            .unwrap();
        assert_eq!(invalid.status.code(), Some(2));
        let error: Value = serde_json::from_slice(&invalid.stderr).unwrap();
        assert_eq!(error["error"]["code"], "GF_VALIDATION");
    }
}
