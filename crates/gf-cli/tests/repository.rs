//! Repository and project-local skill lifecycle integration tests.

use std::fs;
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

fn gf() -> Command {
    Command::new(env!("CARGO_BIN_EXE_gf"))
}

fn assert_sync_check_publish_and_replay(root: &std::path::Path) {
    let current_before_check = fs::read_to_string(root.join(".graphforge/state/CURRENT")).unwrap();
    let plain_check = gf()
        .args(["--project-dir", root.to_str().unwrap(), "sync", "--check"])
        .output()
        .unwrap();
    assert_eq!(plain_check.status.code(), Some(4));
    assert_eq!(plain_check.stdout, b"drift\n");
    assert!(plain_check.stderr.is_empty());
    assert_eq!(
        fs::read_to_string(root.join(".graphforge/state/CURRENT")).unwrap(),
        current_before_check
    );

    let check = gf()
        .args([
            "--project-dir",
            root.to_str().unwrap(),
            "--json",
            "sync",
            "--check",
        ])
        .output()
        .unwrap();
    assert_eq!(check.status.code(), Some(4));
    let value: Value = serde_json::from_slice(&check.stdout).unwrap();
    assert_eq!(value["status"], "drift");
    assert_eq!(value["requested_operation_uuid"], Value::Null);
    assert!(check.stderr.is_empty());

    let operation = "41414141-4141-4141-4141-414141414141";
    let actor = "42424242-4242-4242-4242-424242424242";
    let sync = gf()
        .args([
            "--project-dir",
            root.to_str().unwrap(),
            "--json",
            "sync",
            "--idempotency-key",
            operation,
            "--actor-uuid",
            actor,
        ])
        .output()
        .unwrap();
    assert!(
        sync.status.success(),
        "{}",
        String::from_utf8_lossy(&sync.stderr)
    );
    let value: Value = serde_json::from_slice(&sync.stdout).unwrap();
    assert_eq!(value["status"], "published");
    assert_eq!(value["requested_operation_uuid"], operation);
    assert_eq!(value["snapshot_operation_uuid"], operation);
    assert_eq!(value["snapshot_actor_uuid"], actor);

    let replay = gf()
        .args([
            "--project-dir",
            root.to_str().unwrap(),
            "--json",
            "sync",
            "--idempotency-key",
            operation,
            "--actor-uuid",
            actor,
        ])
        .output()
        .unwrap();
    assert!(replay.status.success());
    let value: Value = serde_json::from_slice(&replay.stdout).unwrap();
    assert_eq!(value["status"], "in_sync");
    assert_eq!(value["idempotent_replay"], true);

    let in_sync = gf()
        .args([
            "--project-dir",
            root.to_str().unwrap(),
            "--json",
            "sync",
            "--check",
        ])
        .output()
        .unwrap();
    assert!(in_sync.status.success());
    let value: Value = serde_json::from_slice(&in_sync.stdout).unwrap();
    assert_eq!(value["status"], "in_sync");
    assert_eq!(value["requested_operation_uuid"], Value::Null);
}

fn assert_sync_detects_definition_drift(root: &std::path::Path) {
    fs::write(root.join(".graphforge/ontology/keep.yaml"), "version: 1\n").unwrap();
    let drift = gf()
        .args([
            "--project-dir",
            root.to_str().unwrap(),
            "--json",
            "sync",
            "--check",
        ])
        .output()
        .unwrap();
    assert_eq!(drift.status.code(), Some(4));
    assert_eq!(
        serde_json::from_slice::<Value>(&drift.stdout).unwrap()["status"],
        "drift"
    );
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
    assert_eq!(value["root"], ".");
    assert_eq!(value["state"], ".graphforge/state");
    assert!(!String::from_utf8_lossy(&init.stdout).contains(root.path().to_str().unwrap()));
    assert_eq!(value["created_config"], true);
    assert_eq!(value["skills"]["changed"], true);
    assert!(
        root.path()
            .join(".agents/skills/graphforge-bootstrap/SKILL.md")
            .is_file()
    );
    assert!(
        root.path()
            .join(".agents/skills/graphforge-build-knowledge/SKILL.md")
            .is_file()
    );

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
    assert!(staged.contains(".agents/skills/.graphforge-managed.json"));
    assert!(staged.contains(".agents/skills/graphforge-bootstrap/SKILL.md"));
    assert!(!staged.contains(".agents/skills/.graphforge-lock"));
    assert!(!staged.contains(".graphforge/state/"));
    let ignore = fs::read_to_string(root.path().join(".gitignore")).unwrap();
    assert!(!ignore.contains(".agents/"));

    assert_sync_check_publish_and_replay(root.path());
    assert_sync_detects_definition_drift(root.path());
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
    let value: Value = serde_json::from_slice(&remove.stdout).unwrap();
    assert_eq!(value["target"], ".graphforge/state");
    assert!(!String::from_utf8_lossy(&remove.stdout).contains(root.path().to_str().unwrap()));
    assert!(root.path().join(".graphforge/ontology/keep.yaml").exists());
    assert!(!root.path().join(".graphforge/state").exists());
}

#[test]
fn repository_receipts_are_byte_identical_across_private_absolute_roots() {
    let roots = [tempdir().unwrap(), tempdir().unwrap()];
    let mut receipts = Vec::new();
    for root in &roots {
        let output = gf()
            .args([
                "--project-dir",
                root.path().to_str().unwrap(),
                "--json",
                "init",
                "--no-skills",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains(root.path().to_str().unwrap()));
        let _: Value = serde_json::from_slice(&output.stdout).unwrap();
        receipts.push(output.stdout);
    }
    assert_eq!(receipts[0], receipts[1]);
}

#[test]
fn project_skills_are_idempotent_and_protect_user_edits() {
    let root = tempdir().unwrap();
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

    let install = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "skills",
            "install",
        ])
        .output()
        .unwrap();
    assert!(install.status.success());
    let value: Value = serde_json::from_slice(&install.stdout).unwrap();
    assert_eq!(value["changed"], false);

    let managed = root
        .path()
        .join(".agents/skills/graphforge-bootstrap/SKILL.md");
    fs::write(&managed, "user edit\n").unwrap();
    let status = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "skills",
            "status",
        ])
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(value["status"], "conflict");
    assert_eq!(
        value["edited_files"],
        serde_json::json!(["graphforge-bootstrap/SKILL.md"])
    );

    for command in ["update", "remove"] {
        let rejected = gf()
            .args([
                "--project-dir",
                root.path().to_str().unwrap(),
                "--json",
                "skills",
                command,
            ])
            .output()
            .unwrap();
        assert_eq!(rejected.status.code(), Some(2));
        assert_eq!(fs::read_to_string(&managed).unwrap(), "user edit\n");
    }

    fs::create_dir_all(root.path().join(".agents/skills/unrelated")).unwrap();
    fs::write(
        root.path().join(".agents/skills/unrelated/SKILL.md"),
        "unrelated\n",
    )
    .unwrap();
    let removed = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "skills",
            "remove",
            "--force",
        ])
        .output()
        .unwrap();
    assert!(removed.status.success());
    assert!(
        root.path()
            .join(".agents/skills/unrelated/SKILL.md")
            .is_file()
    );
    assert!(!managed.exists());
}

#[test]
fn init_can_skip_skills_and_managed_paths_reject_symlinks() {
    let root = tempdir().unwrap();
    let init = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "init",
            "--no-skills",
        ])
        .output()
        .unwrap();
    assert!(init.status.success());
    assert!(!root.path().join(".agents").exists());

    #[cfg(unix)]
    {
        fs::create_dir_all(root.path().join(".agents")).unwrap();
        let outside = tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join(".agents/skills")).unwrap();
        let rejected = gf()
            .args([
                "--project-dir",
                root.path().to_str().unwrap(),
                "--json",
                "skills",
                "install",
            ])
            .output()
            .unwrap();
        assert_eq!(rejected.status.code(), Some(2));
        assert!(fs::read_dir(outside.path()).unwrap().next().is_none());
    }
}

#[test]
fn interrupted_skill_publication_rolls_back_before_the_next_mutation() {
    let root = tempdir().unwrap();
    assert!(
        gf().args(["--project-dir", root.path().to_str().unwrap(), "init",])
            .status()
            .unwrap()
            .success()
    );
    let skills = root.path().join(".agents/skills");
    let managed = skills.join("graphforge-bootstrap");
    let lifecycle = root.path().join(".graphforge/imports/skills-lifecycle");
    let backup = lifecycle.join("backup");
    fs::create_dir(&backup).unwrap();
    fs::rename(&managed, backup.join("graphforge-bootstrap")).unwrap();
    fs::rename(
        skills.join("graphforge-build-knowledge"),
        backup.join("graphforge-build-knowledge"),
    )
    .unwrap();
    fs::create_dir(&managed).unwrap();
    fs::write(managed.join("SKILL.md"), "partial publication\n").unwrap();
    fs::write(lifecycle.join("transaction"), "graphforge-skills/1\n").unwrap();

    let recovered = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "skills",
            "install",
        ])
        .output()
        .unwrap();
    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    assert!(!lifecycle.join("transaction").exists());
    assert!(!backup.exists());
    assert_ne!(
        fs::read_to_string(managed.join("SKILL.md")).unwrap(),
        "partial publication\n"
    );
    assert!(skills.join("graphforge-build-knowledge/SKILL.md").is_file());
}

#[test]
fn status_recovers_an_interrupted_remove_before_reporting() {
    let root = tempdir().unwrap();
    assert!(
        gf().args(["--project-dir", root.path().to_str().unwrap(), "init"])
            .status()
            .unwrap()
            .success()
    );
    let skills = root.path().join(".agents/skills");
    let lifecycle = root.path().join(".graphforge/imports/skills-lifecycle");
    let backup = lifecycle.join("backup");
    fs::create_dir(&backup).unwrap();
    for name in ["graphforge-bootstrap", "graphforge-build-knowledge"] {
        fs::rename(skills.join(name), backup.join(name)).unwrap();
    }
    fs::rename(
        skills.join(".graphforge-managed.json"),
        backup.join(".graphforge-managed.json"),
    )
    .unwrap();
    fs::write(lifecycle.join("transaction"), "graphforge-skills/1\n").unwrap();

    let status = gf()
        .args([
            "--project-dir",
            root.path().to_str().unwrap(),
            "--json",
            "skills",
            "status",
        ])
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    let value: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(value["status"], "current");
    for name in ["graphforge-bootstrap", "graphforge-build-knowledge"] {
        assert!(skills.join(name).join("SKILL.md").is_file());
    }
    assert!(skills.join(".graphforge-managed.json").is_file());
    assert!(!backup.exists());
    assert!(!lifecycle.join("transaction").exists());
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
