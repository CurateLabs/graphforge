use super::super::*;
use super::*;
use std::sync::mpsc;
use tempfile::tempdir;

fn test_skill_manifest() -> (Vec<u8>, [SkillBundleFile<'static>; 2]) {
    let files = [
        SkillBundleFile {
            path: "graphforge-bootstrap/SKILL.md",
            bytes: b"bootstrap",
        },
        SkillBundleFile {
            path: "graphforge-build-knowledge/SKILL.md",
            bytes: b"knowledge",
        },
    ];
    let manifest = serde_json::to_vec(&json!({
        "schema_version": 1,
        "bundle_version": 1,
        "graphforge_compatibility": ">=0.5.0 <0.6.0",
        "skills": MANAGED_SKILL_NAMES,
        "files": files.iter().map(|file| json!({
            "path": file.path,
            "sha256": encode_hex(&Sha256::digest(file.bytes))
        })).collect::<Vec<_>>()
    }))
    .unwrap();
    (manifest, files)
}

#[test]
fn skill_recovery_removes_orphan_staging_without_a_transaction_marker() {
    let dir = tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let context = RepositoryContext {
        config_path: root.join("graphforge.yaml"),
        state_path: root.join(".graphforge/state"),
        root: root.clone(),
        git: false,
    };
    for relative in [SKILLS_STAGE, SKILLS_BACKUP] {
        let path = root.join(relative);
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("orphan"), b"partial").unwrap();
    }
    context.recover_skill_transaction().unwrap();
    assert!(!root.join(SKILLS_STAGE).exists());
    assert!(!root.join(SKILLS_BACKUP).exists());
}

#[test]
fn skill_rollback_removes_a_target_that_was_absent_before_transaction() {
    let dir = tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let context = RepositoryContext {
        config_path: root.join("graphforge.yaml"),
        state_path: root.join(".graphforge/state"),
        root: root.clone(),
        git: false,
    };
    let name = MANAGED_SKILL_NAMES[0];
    fs::create_dir_all(root.join(SKILLS_ROOT).join(name)).unwrap();
    fs::create_dir_all(root.join(SKILLS_BACKUP)).unwrap();
    fs::write(
        root.join(SKILLS_BACKUP).join(format!(".missing-{name}")),
        b"",
    )
    .unwrap();
    fs::write(root.join(SKILLS_TRANSACTION), b"graphforge-skills/1\n").unwrap();
    context.recover_skill_transaction().unwrap();
    assert!(!root.join(SKILLS_ROOT).join(name).exists());
    assert!(!root.join(SKILLS_TRANSACTION).exists());
}

#[test]
fn packaged_skill_manifest_rejects_each_contract_mismatch() {
    let (manifest_bytes, files) = test_skill_manifest();
    let manifest: Value = serde_json::from_slice(&manifest_bytes).unwrap();
    let error_for = |manifest: Value, files: &[SkillBundleFile<'_>]| {
        let bytes = serde_json::to_vec(&manifest).unwrap();
        validate_skill_bundle(&SkillBundle {
            manifest: &bytes,
            files,
        })
        .unwrap_err()
        .to_string()
    };

    assert!(
        validate_skill_bundle(&SkillBundle {
            manifest: &manifest_bytes,
            files: &files,
        })
        .is_ok()
    );
    assert!(
        validate_skill_bundle(&SkillBundle {
            manifest: b"not-json",
            files: &files,
        })
        .unwrap_err()
        .to_string()
        .starts_with("validation error: invalid project skill manifest:")
    );

    let mut changed = manifest.clone();
    changed["bundle_version"] = json!(0);
    assert_eq!(
        error_for(changed, &files),
        "validation error: unsupported project skill manifest version"
    );
    let mut changed = manifest.clone();
    changed["graphforge_compatibility"] = json!(">=9.0.0");
    assert_eq!(
        error_for(changed, &files),
        "validation error: project skill bundle is incompatible with this GraphForge release"
    );
    let mut changed = manifest.clone();
    changed["skills"] = json!(["graphforge-bootstrap"]);
    assert_eq!(
        error_for(changed, &files),
        "validation error: project skill manifest must name the supported managed directories"
    );
    let mut changed = manifest.clone();
    changed["files"] = json!([]);
    assert_eq!(
        error_for(changed, &[]),
        "validation error: project skill manifest does not match packaged files"
    );

    let mut changed = manifest.clone();
    let duplicate_path = changed["files"][0]["path"].clone();
    changed["files"][1]["path"] = duplicate_path;
    assert_eq!(
        error_for(changed, &files),
        "validation error: project skill manifest files must be unique and sorted"
    );
    let mut changed = manifest.clone();
    changed["files"][0]["path"] = json!("graphforge-bootstrap/OTHER.md");
    assert_eq!(
        error_for(changed, &files),
        "validation error: project skill manifest does not match packaged file paths"
    );
    let mut changed = manifest.clone();
    changed["files"][0]["sha256"] = json!("invalid");
    assert_eq!(
        error_for(changed, &files),
        "validation error: invalid sha256 digest"
    );
    let mut changed = manifest.clone();
    changed["files"][0]["sha256"] = json!("00".repeat(32));
    assert_eq!(
        error_for(changed, &files),
        "validation error: packaged project skill digest mismatch: graphforge-bootstrap/SKILL.md"
    );

    let only_bootstrap = [files[0]];
    let mut changed = manifest;
    let bootstrap_entry = changed["files"][0].clone();
    changed["files"] = json!([bootstrap_entry]);
    assert_eq!(
        error_for(changed, &only_bootstrap),
        "validation error: project skill bundle is missing graphforge-build-knowledge/SKILL.md"
    );

    assert_eq!(
        validate_skill_path("").unwrap_err().to_string(),
        "validation error: project skill file path is empty"
    );
    for invalid in [
        "/graphforge-bootstrap/SKILL.md",
        "other/SKILL.md",
        "graphforge-bootstrap",
        "graphforge-bootstrap/../SKILL.md",
        "graphforge-bootstrap\\SKILL.md",
    ] {
        assert_eq!(
            validate_skill_path(invalid).unwrap_err().to_string(),
            "validation error: project skill file path is not contained",
            "{invalid}"
        );
    }
}

#[test]
fn installed_skill_manifest_rejects_invalid_versions_provenance_and_files() {
    let root = tempdir().unwrap();
    let path = root.path().join("manifest.json");
    let valid = json!({
        "schema_version": 1,
        "bundle_version": 1,
        "graphforge_compatibility": ">=0.5.0 <0.6.0",
        "source": "graphforge-packaged-bundle",
        "files": [{
            "path": "graphforge-bootstrap/SKILL.md",
            "sha256": "00".repeat(32),
        }],
    });
    let error_for = |value: &Value| {
        fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        read_installed_manifest(&path).unwrap_err().to_string()
    };

    fs::write(&path, b"not-json").unwrap();
    assert!(
        read_installed_manifest(&path)
            .unwrap_err()
            .to_string()
            .starts_with("validation error: invalid managed skill manifest:")
    );
    fs::write(&path, serde_json::to_vec(&valid).unwrap()).unwrap();
    assert_eq!(read_installed_manifest(&path).unwrap().files.len(), 1);

    let mut changed = valid.clone();
    changed["schema_version"] = json!(2);
    assert_eq!(
        error_for(&changed),
        "validation error: unsupported managed skill manifest version"
    );
    let mut changed = valid.clone();
    changed["source"] = json!("unknown");
    assert_eq!(
        error_for(&changed),
        "validation error: invalid managed skill provenance"
    );
    let mut changed = valid.clone();
    changed["files"] = json!([]);
    assert_eq!(
        error_for(&changed),
        "validation error: managed skill manifest files are required"
    );
    let mut changed = valid.clone();
    changed["files"][0]["sha256"] = json!("invalid");
    assert_eq!(
        error_for(&changed),
        "validation error: invalid sha256 digest"
    );
    let mut changed = valid;
    let first = changed["files"][0].clone();
    changed["files"] = json!([first.clone(), first]);
    assert_eq!(
        error_for(&changed),
        "validation error: managed skill manifest files must be unique and sorted"
    );
}

#[test]
fn status_waits_for_the_writer_lock_and_recovers_before_reading() {
    let root = tempdir().unwrap();
    let context = RepositoryContext::discover(root.path()).unwrap();
    let (manifest, files) = test_skill_manifest();
    let bundle = SkillBundle {
        manifest: &manifest,
        files: &files,
    };
    context.skills_install(&bundle, false).unwrap();
    let writer = context.lock_skills().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let thread_root = root.path().to_path_buf();
    let reader = std::thread::spawn(move || {
        let context = RepositoryContext::discover(thread_root).unwrap();
        let (manifest, files) = test_skill_manifest();
        let bundle = SkillBundle {
            manifest: &manifest,
            files: &files,
        };
        started_tx.send(()).unwrap();
        let result = context.skills_status(&bundle);
        finished_tx.send(result).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(matches!(
        finished_rx.recv_timeout(std::time::Duration::from_millis(250)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    drop(writer);
    assert_eq!(
        finished_rx.recv().unwrap().unwrap().status,
        SkillStatus::Current
    );
    reader.join().unwrap();
}

#[test]
fn malformed_managed_manifest_is_a_force_resolvable_conflict() {
    let root = tempdir().unwrap();
    let context = RepositoryContext::discover(root.path()).unwrap();
    let (manifest, files) = test_skill_manifest();
    let bundle = SkillBundle {
        manifest: &manifest,
        files: &files,
    };
    context.skills_install(&bundle, false).unwrap();
    fs::write(context.contained_path(SKILLS_MANIFEST).unwrap(), b"{}").unwrap();
    assert_eq!(
        context.skills_status(&bundle).unwrap().status,
        SkillStatus::Conflict
    );
    assert!(context.skills_install(&bundle, false).is_err());
    assert!(context.skills_install(&bundle, true).unwrap().changed);
    fs::write(context.contained_path(SKILLS_MANIFEST).unwrap(), b"{}").unwrap();
    assert!(context.skills_remove(false).is_err());
    assert!(context.skills_remove(true).unwrap().changed);
}

#[test]
fn managed_skill_status_detects_missing_extra_nested_and_symlinked_files() {
    let root = tempdir().unwrap();
    let context = RepositoryContext::discover(root.path()).unwrap();
    let (manifest, files) = test_skill_manifest();
    let bundle = SkillBundle {
        manifest: &manifest,
        files: &files,
    };
    context.skills_install(&bundle, false).unwrap();

    let bootstrap = context
        .contained_path(".agents/skills/graphforge-bootstrap/SKILL.md")
        .unwrap();
    fs::remove_file(&bootstrap).unwrap();
    assert_eq!(
        context.skills_status(&bundle).unwrap().status,
        SkillStatus::Conflict
    );
    context.skills_install(&bundle, true).unwrap();

    let extra = context
        .contained_path(".agents/skills/graphforge-bootstrap/nested/extra.md")
        .unwrap();
    fs::create_dir_all(extra.parent().unwrap()).unwrap();
    fs::write(&extra, "extra").unwrap();
    assert_eq!(
        context.skills_status(&bundle).unwrap().status,
        SkillStatus::Conflict
    );
    context.skills_install(&bundle, true).unwrap();

    #[cfg(unix)]
    {
        let link = context
            .contained_path(".agents/skills/graphforge-bootstrap/link")
            .unwrap();
        std::os::unix::fs::symlink(root.path(), link).unwrap();
        assert_eq!(
            context.skills_status(&bundle).unwrap_err().code(),
            "GF_VALIDATION"
        );
    }
}

#[test]
fn bare_managed_directory_is_not_a_valid_manifest_file() {
    assert!(validate_skill_path("graphforge-bootstrap").is_err());
    assert!(validate_skill_path("graphforge-bootstrap/SKILL.md").is_ok());
}
