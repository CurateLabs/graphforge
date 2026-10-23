//! Installed Hub guidance uses the commands and flags the shipped binary offers.

use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::tempdir;

fn skills(root: &Path, action: &str) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project-dir")
        .arg(root)
        .args(["--json", "skills", action])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn installed_hub_skill_examples_match_binary_help() {
    let root = tempdir().unwrap();
    let installed = skills(root.path(), "install");
    assert_eq!(installed["changed"], true);
    assert_eq!(installed["bundle_version"], 2);
    assert_eq!(installed["installed_files"], 3);
    let text =
        fs::read_to_string(root.path().join(".agents/skills/graphforge-hub/SKILL.md")).unwrap();
    assert_eq!(
        text,
        include_str!("../../../project-skills/graphforge-hub/SKILL.md")
    );

    let mut commands = BTreeSet::new();
    // Examples are single-line commands with unquoted, single-token arguments.
    // Ask the real binary for each example's help, then independently check every
    // option against that help: clap's --help can short-circuit other validation.
    for line in text.lines().filter(|line| line.starts_with("gf ")) {
        let arguments: Vec<_> = line.split_whitespace().skip(1).collect();
        let output = Command::new(env!("CARGO_BIN_EXE_gf"))
            .args(&arguments)
            .arg("--help")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{line}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let help = String::from_utf8(output.stdout).unwrap();
        let usage = help
            .lines()
            .find(|line| line.starts_with("Usage: gf "))
            .unwrap();
        commands.insert(usage.split_whitespace().nth(2).unwrap().to_owned());
        for option in arguments
            .iter()
            .filter(|argument| argument.starts_with('-'))
        {
            assert!(
                help.split_whitespace()
                    .any(|word| word.trim_end_matches(',') == *option),
                "{line}: option {option} missing from implemented help:\n{help}"
            );
        }
    }
    assert_eq!(
        commands,
        [
            "clone", "status", "log", "branch", "tag", "commit", "switch", "push", "pull"
        ]
        .map(str::to_owned)
        .into_iter()
        .collect()
    );
}

#[test]
fn previous_bundle_updates_and_hub_edits_remain_protected() {
    let root = tempdir().unwrap();
    skills(root.path(), "install");
    let managed = root.path().join(".agents/skills");
    let manifest_path = managed.join(".graphforge-managed.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    // Recreate the installed v1 bundle: its other two payloads are unchanged.
    manifest["bundle_version"] = 1.into();
    manifest["files"].as_array_mut().unwrap().retain(|entry| {
        !entry["path"]
            .as_str()
            .unwrap()
            .starts_with("graphforge-hub/")
    });
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    fs::remove_dir_all(managed.join("graphforge-hub")).unwrap();
    assert_eq!(skills(root.path(), "status")["status"], "outdated");
    assert_eq!(skills(root.path(), "update")["changed"], true);
    assert_eq!(skills(root.path(), "status")["status"], "current");

    let hub = managed.join("graphforge-hub/SKILL.md");
    fs::write(&hub, "researcher edits\n").unwrap();
    assert_eq!(skills(root.path(), "status")["status"], "conflict");
    for action in ["install", "update", "remove"] {
        let output = Command::new(env!("CARGO_BIN_EXE_gf"))
            .arg("--project-dir")
            .arg(root.path())
            .args(["--json", "skills", action])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(fs::read_to_string(&hub).unwrap(), "researcher edits\n");
    }
}

#[cfg(unix)]
#[test]
fn hub_namespace_symlinks_are_rejected_before_publication() {
    let root = tempdir().unwrap();
    let outside = tempdir().unwrap();
    let managed = root.path().join(".agents/skills");
    fs::create_dir_all(&managed).unwrap();
    std::os::unix::fs::symlink(outside.path(), managed.join("graphforge-hub")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project-dir")
        .arg(root.path())
        .args(["--json", "skills", "install"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(fs::read_dir(outside.path()).unwrap().next().is_none());
}

#[test]
fn agent_guide_examples_match_public_results() {
    let guide = include_str!("../../../docs/guide/work-with-an-agent.md");
    let root = tempdir().unwrap();
    let receipt = skills(root.path(), "install");
    assert!(guide.contains(&serde_json::to_string(&receipt).unwrap()));
    let identity = Command::new(env!("CARGO_BIN_EXE_gf"))
        .args(["--json", "config", "user", "--name", "Research-agent"])
        .env("XDG_CONFIG_HOME", root.path())
        .env("APPDATA", root.path())
        .env_remove("GF_AUTHOR")
        .env_remove("GF_COMMITTER")
        .output()
        .unwrap();
    assert!(identity.status.success());
    assert!(guide.contains(String::from_utf8(identity.stdout).unwrap().trim()));
    let refusal = Command::new(env!("CARGO_BIN_EXE_gf"))
        .args(["--json", "clone", "Not An Identity"])
        .output()
        .unwrap();
    assert_eq!(refusal.status.code(), Some(2));
    assert!(refusal.stdout.is_empty());
    assert!(guide.contains(String::from_utf8(refusal.stderr).unwrap().trim()));
}
