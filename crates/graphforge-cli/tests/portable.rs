//! Same-binary integration coverage for portable project interchange.

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

fn gf_repo(repository: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project-dir")
        .arg(repository)
        .args(args)
        .output()
        .expect("run same-build gf binary")
}

fn json(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "gf failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stable JSON result")
}

#[test]
fn current_export_and_import_round_trip_with_stable_json() {
    let source = TempDir::new().expect("source project parent");
    let source_project = source.path().join("source");
    fs::create_dir(&source_project).expect("source project directory");
    let envelope = source.path().join("project.gfportable");

    let exported = json(&gf(
        &source_project,
        &[
            "--json",
            "export",
            "--current",
            "--output",
            envelope.to_str().unwrap(),
        ],
    ));
    assert_eq!(exported["contract"], "graphforge-portable-export/1");
    assert_eq!(exported["source"], "current");
    assert!(exported["checkpoint"].is_null());
    assert_eq!(exported["output"], envelope.to_str().unwrap());
    assert_eq!(exported["envelope_sha256"].as_str().unwrap().len(), 64);
    assert!(exported["participant_count"].as_u64().unwrap() > 0);

    let destination_project = source.path().join("destination");
    let imported = json(&gf(
        &destination_project,
        &[
            "--json",
            "import",
            "--input",
            envelope.to_str().unwrap(),
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000229",
        ],
    ));
    assert_eq!(imported["contract"], "graphforge-portable-import/1");
    assert_eq!(
        imported["source_generation_uuid"],
        exported["generation_uuid"]
    );
    assert_eq!(imported["envelope_sha256"], exported["envelope_sha256"]);
    assert_eq!(imported["idempotent_replay"], false);

    // Normal runtime access after import proves the published CURRENT is readable.
    let listed = gf(&destination_project, &["checkpoint", "list"]);
    assert!(
        listed.status.success(),
        "reopen failed: {}",
        String::from_utf8_lossy(&listed.stderr)
    );
}

#[test]
fn checkpoint_export_reports_the_selected_checkpoint() {
    let project = TempDir::new().expect("project parent");
    let source_project = project.path().join("source");
    fs::create_dir(&source_project).expect("source project directory");
    let created = gf(
        &source_project,
        &[
            "checkpoint",
            "create",
            "before-change",
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000001",
        ],
    );
    assert!(created.status.success());

    let envelope = project.path().join("checkpoint.gfportable");
    let exported = json(&gf(
        &source_project,
        &[
            "--json",
            "export",
            "--checkpoint",
            "before-change",
            "--output",
            envelope.to_str().unwrap(),
        ],
    ));
    assert_eq!(exported["source"], "checkpoint");
    assert_eq!(exported["checkpoint"], "before-change");
}

#[test]
fn import_rejects_noncanonical_idempotency_key_as_stable_json() {
    let project = TempDir::new().expect("project parent");
    let output = gf(
        &project.path().join("destination"),
        &[
            "--json",
            "import",
            "--input",
            project.path().join("missing.gfportable").to_str().unwrap(),
            "--idempotency-key",
            "NOT-A-UUID",
        ],
    );
    assert_eq!(output.status.code(), Some(2));
    let value: Value = serde_json::from_slice(&output.stderr).expect("JSON error");
    assert_eq!(value["error"]["code"], "GF_VALIDATION");
    assert_eq!(
        value["error"]["message"],
        "validation error: expected canonical UUID"
    );
}

#[test]
fn initialized_repository_can_import_into_its_pristine_state() {
    let repository = TempDir::new().expect("repository");
    let initialized_git = Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(repository.path())
        .status()
        .expect("initialize Git fixture");
    assert!(initialized_git.success());
    let initialized = gf_repo(repository.path(), &["init"]);
    assert!(
        initialized.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    let envelope = repository
        .path()
        .join(".graphforge/exports/pristine.gfportable");
    let exported = gf_repo(
        repository.path(),
        &[
            "export",
            "--current",
            "--output",
            envelope.to_str().unwrap(),
        ],
    );
    assert!(exported.status.success());
    let imported = gf_repo(
        repository.path(),
        &[
            "import",
            "--input",
            envelope.to_str().unwrap(),
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000230",
        ],
    );
    assert!(
        imported.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&imported.stderr)
    );
}

#[test]
fn portable_verify_skips_repository_discovery() {
    let outside = TempDir::new().expect("outside repository");
    let missing = outside.path().join("missing.gfpb");
    let output = Command::new(env!("CARGO_BIN_EXE_gf"))
        .current_dir(outside.path())
        .args([
            "--json",
            "portable",
            "verify",
            "--mode",
            "full",
            "--input",
            missing.to_str().unwrap(),
        ])
        .output()
        .expect("run verify outside repository");
    assert_ne!(output.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.to_ascii_lowercase().contains("repository"),
        "verify must not require repository discovery: {stderr}"
    );
    assert!(
        !stderr.contains("GF_REPOSITORY"),
        "verify must not fail as repository lookup: {stderr}"
    );
}

#[test]
fn portable_v2_export_verify_and_import_round_trip() {
    let root = TempDir::new().expect("temp root");
    let source_project = root.path().join("source");
    fs::create_dir(&source_project).expect("source project");
    let bundle = root.path().join("complete.gfpb");

    let exported = json(&gf(
        &source_project,
        &[
            "--json",
            "portable",
            "export",
            "--current",
            "--format",
            "bundle",
            "--profile",
            "complete",
            "--output",
            bundle.to_str().unwrap(),
        ],
    ));
    assert_eq!(exported["contract"], "graphforge-portable-export/2");
    assert_eq!(exported["representation"], "bundle");
    let package_digest = exported["package_digest"].as_str().unwrap().to_owned();
    assert!(package_digest.starts_with("sha256:"));

    let verified = json(&gf(
        &source_project,
        &[
            "--json",
            "portable",
            "verify",
            "--mode",
            "full",
            "--input",
            bundle.to_str().unwrap(),
        ],
    ));
    assert_eq!(verified["package_digest"], package_digest);

    let destination = root.path().join("destination");
    let imported = json(&gf(
        &destination,
        &[
            "--json",
            "portable",
            "import",
            "--input",
            bundle.to_str().unwrap(),
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000744",
        ],
    ));
    assert_eq!(imported["contract"], "graphforge-portable-import/2");
    assert_eq!(imported["package_digest"], package_digest);
    assert_eq!(imported["idempotent_replay"], false);

    let listed = gf(&destination, &["checkpoint", "list"]);
    assert!(
        listed.status.success(),
        "reopen failed: {}",
        String::from_utf8_lossy(&listed.stderr)
    );

    // Verify is repository-independent: it must not require --project or a discovered repo.
    let outside = TempDir::new().expect("outside repository");
    let verified_outside = Command::new(env!("CARGO_BIN_EXE_gf"))
        .current_dir(outside.path())
        .args([
            "--json",
            "portable",
            "verify",
            "--mode",
            "full",
            "--input",
            bundle.to_str().unwrap(),
        ])
        .output()
        .expect("run verify outside repository");
    let verified_outside = json(&verified_outside);
    assert_eq!(verified_outside["package_digest"], package_digest);
}
