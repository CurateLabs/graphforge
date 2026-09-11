//! Same-binary integration coverage for portable project interchange.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

fn gf_bin() -> std::path::PathBuf {
    // Bazel/cargo may provide a relative binary path; canonicalize before changing cwd.
    fs::canonicalize(env!("CARGO_BIN_EXE_gf")).expect("resolve same-build gf binary")
}

fn gf(project: &Path, args: &[&str]) -> Output {
    Command::new(gf_bin())
        .arg("--project")
        .arg(project)
        .args(args)
        .output()
        .expect("run same-build gf binary")
}

fn gf_repo(repository: &Path, args: &[&str]) -> Output {
    Command::new(gf_bin())
        .arg("--project-dir")
        .arg(repository)
        .args(args)
        .output()
        .expect("run same-build gf binary")
}

fn gf_cwd(cwd: &Path, args: &[&str]) -> Output {
    // Poison GF_REPOSITORY so accidental discovery fails deterministically.
    let decoy = cwd.join("no-such-gf-repository");
    Command::new(gf_bin())
        .current_dir(cwd)
        .env("GF_REPOSITORY", &decoy)
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
    assert!(
        exported.get("output").is_none(),
        "JSON must not expose host paths"
    );
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
    let output = gf_cwd(
        outside.path(),
        &[
            "--json",
            "portable",
            "verify",
            "--mode",
            "full",
            "--input",
            missing.to_str().unwrap(),
        ],
    );
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
    assert!(
        exported.get("output").is_none(),
        "JSON must not expose host paths"
    );
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
    let verified_outside = json(&gf_cwd(
        outside.path(),
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
    assert_eq!(verified_outside["package_digest"], package_digest);
}

#[test]
fn portable_facade_and_same_binary_preserve_complete_receipts() {
    use graphforge_api::{
        GraphForge, PortableSelection, PortableV2ExportRequest, PortableV2Limits, PortableV2Mode,
        PortableV2Output, PortableV2SelectionPreviewRequest, PortableV2SelectionProfile,
        PortableV2SelectionRequest, PortableVerifyRequest,
    };
    let root = TempDir::new().unwrap();
    let project = root.path().join("source");
    let graph = GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Person {name: 'Ada'})").unwrap();
    drop(graph);
    let checkpoint = gf(
        &project,
        &[
            "checkpoint",
            "create",
            "pinned",
            "--idempotency-key",
            "00000000-0000-0000-0000-000000001015",
        ],
    );
    assert!(
        checkpoint.status.success(),
        "{}",
        String::from_utf8_lossy(&checkpoint.stderr)
    );
    for (selector, selection) in [
        (vec!["--current"], PortableSelection::Current),
        (
            vec!["--checkpoint", "pinned"],
            PortableSelection::Checkpoint("pinned".into()),
        ),
    ] {
        let mut args = vec!["--json", "portable", "preview"];
        args.extend(selector.iter().copied());
        args.extend(["--profile", "complete"]);
        let cli_preview = json(&gf(&project, &args));
        let graph = GraphForge::new(project.to_str()).unwrap();
        let preview = graph
            .preview_portable_v2_selection(&PortableV2SelectionPreviewRequest {
                selection: selection.clone(),
                request: PortableV2SelectionRequest {
                    profile: PortableV2SelectionProfile::Complete,
                    strict: false,
                },
                limits: PortableV2Limits::default(),
            })
            .unwrap();
        assert_eq!(cli_preview, serde_json::to_value(&preview).unwrap());
        assert_eq!(cli_preview["include_graph_tree"], true);
        drop(graph);
        for (format, representation) in [
            ("bundle", PortableV2Output::Bundle),
            ("expanded", PortableV2Output::Expanded),
        ] {
            let package = root.path().join("package");
            let mut args = vec!["--json", "portable", "export"];
            args.extend(selector.iter().copied());
            args.extend([
                "--format",
                format,
                "--profile",
                "complete",
                "--output",
                package.to_str().unwrap(),
            ]);
            let cli_export = json(&gf(&project, &args));
            for (mode_flag, mode) in [
                ("full", PortableV2Mode::Full),
                ("inspect", PortableV2Mode::StructureOnly),
            ] {
                let cli_verify = json(&gf(
                    &project,
                    &[
                        "--json",
                        "portable",
                        "verify",
                        "--mode",
                        mode_flag,
                        "--input",
                        package.to_str().unwrap(),
                    ],
                ));
                let report = graphforge_api::verify_portable_v2(
                    &PortableVerifyRequest {
                        input: package.clone(),
                        mode,
                        limits: PortableV2Limits::default(),
                    },
                    None,
                )
                .unwrap();
                assert_eq!(cli_verify, serde_json::to_value(report).unwrap());
            }
            if package.is_dir() {
                fs::remove_dir_all(&package).unwrap();
            } else {
                fs::remove_file(&package).unwrap();
            }
            let graph = GraphForge::new(project.to_str()).unwrap();
            let request = PortableV2ExportRequest {
                selection: selection.clone(),
                output_path: package.clone(),
                representation,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            };
            let exported = graph.export_portable_v2(&request, None, |_| {}).unwrap();
            assert_eq!(
                cli_export,
                serde_json::to_value(exported.receipt()).unwrap()
            );
            let denied = graph
                .export_portable_v2(&request, None, |_| {})
                .unwrap_err();
            assert_eq!(denied.code, graphforge_api::PortableV2ErrorCode::Io);
            drop(graph);
            if package.is_dir() {
                fs::remove_dir_all(&package).unwrap();
            } else {
                fs::remove_file(&package).unwrap();
            }
        }
    }
}

#[test]
fn shared_verification_golden_matches_real_facade_and_cli() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    // Bazel runfiles are links; verifier inputs must be ordinary owned files.
    let fixture = root.join("tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb");
    let materialized = tempfile::tempdir().unwrap();
    let package = materialized.path().join("openalex-openalex.gfpb");
    fs::copy(&fixture, &package).unwrap();
    assert_eq!(fs::read(&package).unwrap(), fs::read(&fixture).unwrap());
    let expected: Value = serde_json::from_slice(
        &fs::read(root.join("tests/fixtures/portable-v2/facade-verification-receipts.json"))
            .unwrap(),
    )
    .unwrap();
    for (key, flag, mode) in [
        ("full", "full", graphforge_api::PortableV2Mode::Full),
        (
            "structure_only",
            "inspect",
            graphforge_api::PortableV2Mode::StructureOnly,
        ),
    ] {
        let report = graphforge_api::verify_portable_v2(
            &graphforge_api::PortableVerifyRequest {
                input: package.clone(),
                mode,
                limits: graphforge_api::PortableV2Limits::default(),
            },
            None,
        )
        .unwrap();
        assert_eq!(serde_json::to_value(report).unwrap(), expected[key]);
        let cli = gf_cwd(
            &std::env::current_dir().unwrap(),
            &[
                "--json",
                "portable",
                "verify",
                "--mode",
                flag,
                "--input",
                package.to_str().unwrap(),
            ],
        );
        assert_eq!(json(&cli), expected[key]);
    }
}

#[test]
fn import_operation_timings_survive_separate_cli_processes() {
    use arrow::array::{FixedSizeBinaryArray, StringArray};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    let input = root.path().join("nodes.parquet");
    let ids = [uuid::Uuid::now_v7(), uuid::Uuid::now_v7()];
    let batch = RecordBatch::try_new(
        graphforge_api::bulk_node_input_schema(Vec::new()).unwrap(),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_bytes().as_slice()))
                    .unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Person", "Person"])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(fs::File::create(&input).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let operation = uuid::Uuid::now_v7().to_string();
    let begun = json(&gf(
        &project,
        &[
            "--json",
            "import-session",
            "begin",
            "--operation-uuid",
            &operation,
        ],
    ));
    let session = begun["session_uuid"].as_str().unwrap();
    json(&gf(
        &project,
        &[
            "--json",
            "import-session",
            "register-parquet",
            "--session-uuid",
            session,
            "--kind",
            "nodes",
            "--path",
            input.to_str().unwrap(),
        ],
    ));
    let validated = json(&gf(
        &project,
        &[
            "--json",
            "import-session",
            "validate",
            "--session-uuid",
            session,
        ],
    ));
    let timing = &validated["operation_timings"];
    assert_eq!(timing["begin"]["calls"], 1);
    assert_eq!(timing["resume"]["calls"], 0);
    assert_eq!(timing["append"]["calls"], 1);
    assert_eq!(timing["seal"]["calls"], 1);
    assert_eq!(timing["publish"]["calls"], 0);
    for phase in ["begin", "append", "seal"] {
        assert!(timing[phase]["elapsed_ns"].as_u64().is_some());
        assert_eq!(timing[phase]["errors"], 0);
    }
    let committed = json(&gf(
        &project,
        &[
            "--json",
            "import-session",
            "commit",
            "--session-uuid",
            session,
        ],
    ));
    let timing = &committed["operation_timings"];
    assert_eq!(timing["begin"]["calls"], 0);
    assert_eq!(timing["resume"]["calls"], 1);
    assert_eq!(timing["append"]["calls"], 0);
    assert_eq!(timing["seal"]["calls"], 0);
    assert_eq!(timing["publish"]["calls"], 1);
    assert!(timing["resume"]["elapsed_ns"].as_u64().is_some());
    assert!(timing["publish"]["elapsed_ns"].as_u64().is_some());
    assert_eq!(committed["construction"]["input_rows"], 2);
    assert_eq!(committed["construction"]["publication_committed"], true);
    let status = json(&gf(
        &project,
        &[
            "--json",
            "import-session",
            "status",
            "--session-uuid",
            session,
        ],
    ));
    assert!(
        status.get("operation_timings").is_none(),
        "durable status must not replay timings"
    );
    let graph = graphforge_api::GraphForge::new(project.to_str()).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
}
