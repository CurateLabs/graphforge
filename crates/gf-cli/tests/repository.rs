use std::fs;
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

fn gf() -> Command {
    Command::new(env!("CARGO_BIN_EXE_gf"))
}

#[test]
fn repository_lifecycle_emits_stable_json_and_keeps_data_out_of_git() {
    let root = tempdir().unwrap();
    assert!(
        Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(root.path())
            .status()
            .unwrap()
            .success()
    );
    let init = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "init",
        ])
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let value: Value = serde_json::from_slice(&init.stdout).unwrap();
    assert_eq!(value["created_config"], true);

    assert!(
        Command::new("git")
            .arg("-C")
            .arg(root.path())
            .args(["add", "."])
            .status()
            .unwrap()
            .success()
    );
    let staged = Command::new("git")
        .arg("-C")
        .arg(root.path())
        .args(["diff", "--cached", "--name-only"])
        .output()
        .unwrap();
    let staged = String::from_utf8(staged.stdout).unwrap();
    assert!(staged.contains(".graphforge/graphforge.yaml"));
    assert!(!staged.contains(".graphforge/state/"));

    let sync = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "sync",
        ])
        .output()
        .unwrap();
    assert!(sync.status.success());
    let value: Value = serde_json::from_slice(&sync.stdout).unwrap();
    assert!(
        value["definition_digests"]
            .as_object()
            .is_some_and(|value| value.len() == 4)
    );

    fs::write(root.path().join(".graphforge/ontology/keep.yaml"), "keep").unwrap();
    let remove = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "remove",
            "--yes",
        ])
        .output()
        .unwrap();
    assert!(remove.status.success());
    assert!(root.path().join(".graphforge/ontology/keep.yaml").exists());
    assert!(!root.path().join(".graphforge/state").exists());
}

#[test]
fn json_validation_error_has_stable_envelope_and_exit_code() {
    let root = tempdir().unwrap();
    fs::create_dir(root.path().join(".graphforge")).unwrap();
    fs::write(
        root.path().join(".graphforge/graphforge.yaml"),
        "schema_version: 999\n",
    )
    .unwrap();
    let output = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "config",
            "validate",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let value: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(value["error"]["code"], "GF_VALIDATION");
}
