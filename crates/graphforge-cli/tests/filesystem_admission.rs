//! CLI parity for durable project filesystem admission.

use std::collections::BTreeSet;
use std::fs;
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

fn gf(project: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(project)
        .args(args)
        .output()
        .expect("run same-build gf binary")
}

fn names(path: &std::path::Path) -> BTreeSet<std::ffi::OsString> {
    fs::read_dir(path)
        .expect("read directory snapshot")
        .map(|entry| entry.expect("read directory entry").file_name())
        .collect()
}

#[test]
fn traversal_is_typed_in_text_and_json_without_project_mutation() {
    let fixture = TempDir::new().expect("temporary admission parent");
    let parent = fixture
        .path()
        .canonicalize()
        .expect("canonical fixture parent");
    let project = parent.join("project");
    let initialized = gf(&project, &["checkpoint", "list"]);
    assert!(
        initialized.status.success(),
        "initial project open failed: {}",
        String::from_utf8_lossy(&initialized.stderr)
    );

    fs::create_dir(parent.join("hop")).expect("create traversal hop");
    let traversal = parent.join("hop").join("..").join("project");
    let current_before = fs::read(project.join("CURRENT")).expect("read CURRENT");
    let generations_before = names(&project.join("generations"));
    let parent_before = names(&parent);

    let text = gf(&traversal, &["checkpoint", "list"]);
    assert_eq!(text.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&text.stderr).starts_with("GF_UNSUPPORTED_FILESYSTEM:"),
        "unexpected text error: {}",
        String::from_utf8_lossy(&text.stderr)
    );

    let json = gf(&traversal, &["--json", "checkpoint", "list"]);
    assert_eq!(json.status.code(), Some(1));
    let error: Value = serde_json::from_slice(&json.stderr).expect("JSON error payload");
    assert_eq!(error["error"]["code"], "GF_UNSUPPORTED_FILESYSTEM");
    assert_eq!(error["error"]["details"]["kind"], "project");

    assert_eq!(fs::read(project.join("CURRENT")).unwrap(), current_before);
    assert_eq!(names(&project.join("generations")), generations_before);
    assert_eq!(names(&parent), parent_before);
}
