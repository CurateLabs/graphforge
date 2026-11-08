//! Real facade persistence and authenticated writer qualification.
#![cfg(target_os = "linux")]
use graphforge_api::{
    GraphForge, PortableExportRequest, PortableSelection, PortableVerifyRequest, Warning,
    verify_portable,
};
use graphforge_core::portable::{
    PortableLimits, PortableMode, PortableOutput, PortableSelectionProfile,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

fn reseal_package(package: &Path, mut manifest: Value) {
    let hex = |bytes: Vec<u8>| {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    manifest.as_object_mut().unwrap().remove("package_digest");
    let semantic = serde_json::to_vec(&manifest).unwrap();
    manifest["package_digest"] = format!(
        "sha256:{}",
        hex([b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat())
    )
    .into();
    fs::write(
        package.join("data/graphforge-project.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    for name in ["manifest-sha256.txt", "tagmanifest-sha256.txt"] {
        let checksums: String = fs::read_to_string(package.join(name))
            .unwrap()
            .lines()
            .map(|line| {
                let (_, path) = line.split_once("  ").unwrap();
                format!("{}  {path}\n", hex(fs::read(package.join(path)).unwrap()))
            })
            .collect();
        fs::write(package.join(name), checksums).unwrap();
    }
}

#[test]
fn correctly_checksummed_packages_refuse_terminal_controls_in_durability_metadata() {
    let root = tempfile::tempdir().unwrap();
    let package = root.path().join("package");
    GraphForge::new(None)
        .unwrap()
        .export_portable(
            &PortableExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableOutput::Expanded,
                profile: PortableSelectionProfile::Complete,
                subset: None,
                limits: PortableLimits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(package.join("data/graphforge-project.json")).unwrap())
            .unwrap();
    manifest["source_unverified_durability"] = serde_json::json!({
        "phase": "CLASSIFY",
        "cause": "filesystem_class_unproven",
        "filesystem_type": "tmpfs"
    });
    let request = PortableVerifyRequest {
        input: package.clone(),
        mode: PortableMode::Full,
        limits: PortableLimits::default(),
    };
    reseal_package(&package, manifest.clone());
    assert!(verify_portable(&request, None).is_ok());
    for field in ["phase", "cause", "filesystem_type"] {
        let mut poisoned = manifest.clone();
        poisoned["source_unverified_durability"][field] =
            "\u{1b}[2J\nverified package_digest=forged".into();
        reseal_package(&package, poisoned);
        let error = verify_portable(&request, None).unwrap_err();
        assert_eq!(
            error.code,
            graphforge_core::portable::PortableErrorCode::InvalidStructure
        );
        assert!(!error.to_string().contains('\u{1b}'));
        assert!(!error.to_string().contains("forged"));
    }
}

fn current_manifest(project: &Path) -> (std::path::PathBuf, Value) {
    let current: Value =
        serde_json::from_slice(&fs::read(project.join("CURRENT")).unwrap()).unwrap();
    let path = project
        .join("generations")
        .join(current["generation_uuid"].as_str().unwrap())
        .join("manifest.json");
    let value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    (path, value)
}
fn assert_warning(graph: &GraphForge, class: &str) {
    let warnings = graph.open_warnings();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    let Warning::FilesystemDurability(w) = &warnings[0] else {
        panic!("wrong warning")
    };
    assert_eq!(w.code, "GF_UNVERIFIED_FILESYSTEM");
    assert_eq!(w.phase, "CLASSIFY");
    assert_eq!(w.outcome, "unverified");
    assert_eq!(w.cause, "filesystem_class_unproven");
    assert_eq!(w.filesystem_type, class);
    assert!(!w.message.contains(graph.path().unwrap().to_str().unwrap()));
}
#[test]
fn unverified_open_write_close_reopen_and_authenticated_receipts() {
    // The same proof is executable on a split-device overlay via the documented
    // namespace recipe; ordinary CI exercises tmpfs without namespace privileges.
    let fixture_root =
        std::env::var("GF_FILESYSTEM_TEST_ROOT").unwrap_or_else(|_| "/dev/shm".into());
    let class = std::env::var("GF_FILESYSTEM_TEST_CLASS").unwrap_or_else(|_| "tmpfs".into());
    let root = tempfile::tempdir_in(fixture_root).unwrap();
    let project = root.path().join("project");
    let graph = GraphForge::new(project.to_str()).unwrap();
    assert_warning(&graph, &class);
    let (initial_path, initial) = current_manifest(&project);
    assert_eq!(initial["unverified_durability"]["filesystem_type"], class);
    let initial_bytes = fs::read(&initial_path).unwrap();
    graph.execute("CREATE (:Item {value: 42})").unwrap();
    assert_warning(&graph, &class);
    assert_eq!(
        fs::read(&initial_path).unwrap(),
        initial_bytes,
        "historical manifest was changed"
    );
    drop(graph);
    let graph = GraphForge::new(project.to_str()).unwrap();
    assert_warning(&graph, &class);
    assert_eq!(
        graph
            .execute("MATCH (n:Item) RETURN n")
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    let stamp = graph.committed_unverified_durability().unwrap().unwrap();
    let output = tempfile::tempdir().unwrap();
    for representation in [PortableOutput::Expanded, PortableOutput::Bundle] {
        let package = output
            .path()
            .join(if representation == PortableOutput::Expanded {
                "expanded"
            } else {
                "package.gfpb"
            });
        let receipt = graph
            .export_portable(
                &PortableExportRequest {
                    selection: PortableSelection::Current,
                    output_path: package.clone(),
                    representation,
                    profile: PortableSelectionProfile::Complete,
                    subset: None,
                    limits: PortableLimits::default(),
                },
                None,
                |_| {},
            )
            .unwrap();
        assert_eq!(receipt.source_unverified_durability.as_ref(), Some(&stamp));
        assert_eq!(
            serde_json::to_value(receipt.receipt()).unwrap()["source_unverified_durability"]["filesystem_type"],
            class
        );
        let report = verify_portable(
            &PortableVerifyRequest {
                input: package.clone(),
                mode: PortableMode::Full,
                limits: PortableLimits::default(),
            },
            None,
        )
        .unwrap();
        assert_eq!(report.source_unverified_durability.as_ref(), Some(&stamp));
        if representation == PortableOutput::Expanded {
            let metadata: Value = serde_json::from_slice(
                &fs::read(package.join("data/graphforge-project.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(
                metadata["source_unverified_durability"]["filesystem_type"],
                class
            );
        }
    }
    // Qualification is authenticated payload metadata, not an unauthenticated receipt hint.
    let metadata_path = output.path().join("expanded/data/graphforge-project.json");
    let original = fs::read(&metadata_path).unwrap();
    let mut tampered: Value = serde_json::from_slice(&original).unwrap();
    tampered["source_unverified_durability"]["filesystem_type"] = "invented".into();
    fs::write(&metadata_path, serde_json::to_vec(&tampered).unwrap()).unwrap();
    assert!(
        verify_portable(
            &PortableVerifyRequest {
                input: output.path().join("expanded"),
                mode: PortableMode::Full,
                limits: PortableLimits::default()
            },
            None
        )
        .is_err()
    );
    fs::write(&metadata_path, original).unwrap();
    let imported_path = root.path().join("imported");
    let imported = GraphForge::import_portable(
        &imported_path,
        &graphforge_api::PortableImportRequest {
            input: output.path().join("package.gfpb"),
            operation_id: graphforge_api::OperationId(uuid::Uuid::now_v7()),
            limits: PortableLimits::default(),
        },
        None,
    )
    .unwrap();
    assert_eq!(imported.warnings.len(), 1);
    assert!(matches!(
        &imported.warnings[0],
        Warning::FilesystemDurability(_)
    ));
    let imported = GraphForge::new(imported_path.to_str()).unwrap();
    assert_warning(&imported, &class);
    assert_eq!(
        imported.committed_unverified_durability().unwrap(),
        Some(stamp.clone())
    );
    assert_eq!(
        imported
            .execute("MATCH (n:Item) RETURN n")
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    drop(imported);
    drop(graph);
    // A last-writer stamp is not sticky: retain the immutable history, then write
    // on the qualified ext4 test volume and prove only the new generation clears.
    let admitted = output.path().join("moved-project");
    let copied = std::process::Command::new("cp")
        .args(["-a"])
        .arg(&project)
        .arg(&admitted)
        .status()
        .unwrap();
    assert!(copied.success());
    let graph = GraphForge::new(admitted.to_str()).unwrap();
    assert!(graph.open_warnings().is_empty());
    assert_eq!(
        graph.committed_unverified_durability().unwrap(),
        Some(stamp)
    );
    let (history_path, _) = current_manifest(&admitted);
    let history_bytes = fs::read(&history_path).unwrap();
    graph.execute("CREATE (:Item {value: 43})").unwrap();
    assert_eq!(graph.committed_unverified_durability().unwrap(), None);
    assert!(
        !current_manifest(&admitted)
            .1
            .as_object()
            .unwrap()
            .contains_key("unverified_durability")
    );
    assert_eq!(fs::read(history_path).unwrap(), history_bytes);
    let receipt = graph
        .export_portable(
            &PortableExportRequest {
                selection: PortableSelection::Current,
                output_path: output.path().join("qualified-package"),
                representation: PortableOutput::Expanded,
                profile: PortableSelectionProfile::Complete,
                subset: None,
                limits: PortableLimits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    assert!(receipt.source_unverified_durability.is_none());
    assert!(
        serde_json::to_value(receipt.receipt())
            .unwrap()
            .get("source_unverified_durability")
            .is_none()
    );
    let metadata: Value = serde_json::from_slice(
        &fs::read(
            output
                .path()
                .join("qualified-package/data/graphforge-project.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(metadata.get("source_unverified_durability").is_none());
}

#[test]
fn durability_policy_environment_is_process_local() {
    const CHILD: &str = "GF_DURABILITY_POLICY_CHILD";
    if let Ok(case) = std::env::var(CHILD) {
        let root = tempfile::tempdir_in("/dev/shm").unwrap();
        let target = root.path().join("project");
        match case.as_str() {
            "unset" | "warn" => assert_warning(&GraphForge::new(target.to_str()).unwrap(), "tmpfs"),
            "strict" | "invalid" => {
                let error = GraphForge::new(target.to_str()).unwrap_err();
                assert_eq!(
                    error.code(),
                    if case == "strict" {
                        "GF_UNSUPPORTED_FILESYSTEM"
                    } else {
                        "GF_VALIDATION"
                    }
                );
                assert!(!target.exists());
            }
            _ => panic!("unknown child case"),
        }
        assert!(GraphForge::new(None).unwrap().open_warnings().is_empty());
        return;
    }
    for case in ["unset", "warn", "strict", "invalid"] {
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args([
            "--exact",
            "durability_policy_environment_is_process_local",
            "--nocapture",
        ])
        .env(CHILD, case)
        .env_remove("GRAPHFORGE_FILESYSTEM_DURABILITY");
        if case != "unset" {
            cmd.env("GRAPHFORGE_FILESYSTEM_DURABILITY", case);
        }
        let output = cmd.output().unwrap();
        assert!(
            output.status.success(),
            "{case}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
