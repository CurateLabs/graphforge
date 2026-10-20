//! Same-binary integration coverage for portable project interchange.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

fn gf_bin() -> std::path::PathBuf {
    // The binary path may be relative; canonicalize before changing cwd.
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
    let bundle = source.path().join("project.gfpb");

    let exported = json(&gf(
        &source_project,
        &[
            "--json",
            "export",
            "--current",
            "--output",
            bundle.to_str().unwrap(),
        ],
    ));
    assert_eq!(exported["contract"], "graphforge-portable-export/2");
    assert_eq!(exported["source"], "current");
    assert!(exported["checkpoint"].is_null());
    assert!(
        exported.get("output").is_none(),
        "JSON must not expose host paths"
    );
    assert_eq!(exported["package_digest"].as_str().unwrap().len(), 71);
    assert!(exported["entry_count"].as_u64().unwrap() > 0);

    let destination_project = source.path().join("destination");
    let imported = json(&gf(
        &destination_project,
        &[
            "--json",
            "import",
            "--input",
            bundle.to_str().unwrap(),
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000229",
        ],
    ));
    assert_eq!(imported["contract"], "graphforge-portable-import/2");
    assert_eq!(imported["package_digest"], exported["package_digest"]);
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

    let bundle = project.path().join("checkpoint.gfpb");
    let exported = json(&gf(
        &source_project,
        &[
            "--json",
            "export",
            "--checkpoint",
            "before-change",
            "--output",
            bundle.to_str().unwrap(),
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
            project.path().join("missing.gfpb").to_str().unwrap(),
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
    let bundle = repository.path().join(".graphforge/exports/pristine.gfpb");
    let exported = gf_repo(
        repository.path(),
        &["export", "--current", "--output", bundle.to_str().unwrap()],
    );
    assert!(exported.status.success());
    let imported = gf_repo(
        repository.path(),
        &[
            "import",
            "--input",
            bundle.to_str().unwrap(),
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
fn portable_export_verify_and_import_round_trip() {
    let root = TempDir::new().expect("temp root");
    let source_project = root.path().join("source");
    fs::create_dir(&source_project).expect("source project");
    let bundle = root.path().join("complete.gfpb");

    let exported = json(&gf(
        &source_project,
        &[
            "--json",
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
fn verify_reports_graph_rows_from_footers_alike_from_a_file_and_a_stream() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("source");
    let graph = graphforge_api::GraphForge::new(project.to_str()).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person {name: 'Bob'}), \
             (b)-[:KNOWS]->(c:Person {name: 'Cy'})",
        )
        .unwrap();
    drop(graph);
    let bundle = root.path().join("project.gfpb");
    json(&gf(
        &project,
        &[
            "--json",
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
    let mut from_file = json(&gf(
        &project,
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
    let mut reader = std::io::BufReader::new(fs::File::open(&bundle).unwrap());
    let streamed = graphforge_api::verify_portable_stream(
        &mut reader,
        None,
        graphforge_api::PortableLimits::default(),
        None,
    )
    .unwrap();
    let mut from_stream = serde_json::to_value(streamed).unwrap();
    assert_eq!(
        from_file.as_object_mut().unwrap().remove("source").unwrap(),
        "file"
    );
    assert_eq!(
        from_stream
            .as_object_mut()
            .unwrap()
            .remove("source")
            .unwrap(),
        "stream"
    );
    assert_eq!(from_file, from_stream);

    let parquet = &from_file["plausibility"]["parquet"];
    assert_eq!(parquet["node_rows"], 3);
    assert_eq!(parquet["edge_rows"], 2);
    assert!(parquet["files"].as_u64().unwrap() > 0);
    assert_eq!(parquet["files_with_footer"], parquet["files"]);
    let kinds = from_file["bytes_by_kind"].as_object().unwrap();
    assert!(kinds["graph-data"].as_u64().unwrap() > 0);
    assert_eq!(
        kinds
            .values()
            .map(|bytes| bytes.as_u64().unwrap())
            .sum::<u64>(),
        from_file["payload_bytes"].as_u64().unwrap()
    );
}

#[test]
fn portable_facade_and_same_binary_preserve_complete_receipts() {
    use graphforge_api::{
        GraphForge, PortableExportRequest, PortableLimits, PortableMode, PortableOutput,
        PortableSelection, PortableSelectionPreviewRequest, PortableSelectionProfile,
        PortableSelectionRequest, PortableVerifyRequest,
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
            .preview_portable_selection(&PortableSelectionPreviewRequest {
                selection: selection.clone(),
                request: PortableSelectionRequest {
                    profile: PortableSelectionProfile::Complete,
                    strict: false,
                },
                limits: PortableLimits::default(),
            })
            .unwrap();
        assert_eq!(cli_preview, serde_json::to_value(&preview).unwrap());
        assert_eq!(cli_preview["include_graph_tree"], true);
        drop(graph);
        for (format, representation) in [
            ("bundle", PortableOutput::Bundle),
            ("expanded", PortableOutput::Expanded),
        ] {
            let package = root.path().join("package");
            let mut args = vec!["--json", "--diagnostics", "export"];
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
                ("full", PortableMode::Full),
                ("inspect", PortableMode::StructureOnly),
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
                let report = graphforge_api::verify_portable(
                    &PortableVerifyRequest {
                        input: package.clone(),
                        mode,
                        limits: PortableLimits::default(),
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
            let request = PortableExportRequest {
                selection: selection.clone(),
                output_path: package.clone(),
                representation,
                profile: PortableSelectionProfile::Complete,
                subset: None,
                limits: PortableLimits::default(),
            };
            let exported = graph.export_portable(&request, None, |_| {}).unwrap();
            // #1389 adds process-scoped per-phase I/O attribution to the CLI
            // receipt. It is observability the package receipt does not model,
            // so it is asserted separately and the package contract below stays
            // exactly as strict: no package field invented, dropped or altered.
            let mut cli_package = cli_export.clone();
            let application_io = cli_package
                .as_object_mut()
                .unwrap()
                .remove("application_io")
                .expect("export receipt carries per-phase application I/O");
            assert_eq!(
                application_io["phases"].as_object().unwrap().len(),
                graphforge_api::StorageIoPhase::LIFECYCLE.len()
            );
            assert!(application_io["totals"]["read_bytes"].as_u64().unwrap() > 0);
            assert_eq!(
                cli_package,
                serde_json::to_value(exported.receipt()).unwrap()
            );
            let denied = graph.export_portable(&request, None, |_| {}).unwrap_err();
            assert_eq!(denied.code, graphforge_api::PortableErrorCode::Io);
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
    // Verifier inputs must be ordinary owned files, never links into the tree.
    let fixture = root.join("tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb");
    let materialized = tempfile::tempdir().unwrap();
    let package = materialized.path().join("openalex-openalex.gfpb");
    fs::copy(&fixture, &package).unwrap();
    assert_eq!(fs::read(&package).unwrap(), fs::read(&fixture).unwrap());
    let expected: Value = serde_json::from_slice(
        &fs::read(root.join("tests/fixtures/portable/facade-verification-receipts.json")).unwrap(),
    )
    .unwrap();
    for (key, flag, mode) in [
        ("full", "full", graphforge_api::PortableMode::Full),
        (
            "structure_only",
            "inspect",
            graphforge_api::PortableMode::StructureOnly,
        ),
    ] {
        let report = graphforge_api::verify_portable(
            &graphforge_api::PortableVerifyRequest {
                input: package.clone(),
                mode,
                limits: graphforge_api::PortableLimits::default(),
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

/// Write `ids` as a Parquet node source labelled `label`.
fn write_nodes(path: &Path, ids: &[uuid::Uuid], label: &str) {
    use arrow::array::{FixedSizeBinaryArray, StringArray};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let batch = RecordBatch::try_new(
        graphforge_api::bulk_node_input_schema(Vec::new()).unwrap(),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_bytes().as_slice()))
                    .unwrap(),
            ),
            Arc::new(StringArray::from(vec![label; ids.len()])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(fs::File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Commit one `Seed` node through the five import commands.
fn seed_project(scratch: &Path, project: &Path) {
    let source = scratch.join("seed.parquet");
    write_nodes(&source, &[uuid::Uuid::now_v7()], "Seed");
    let begun = json(&gf(
        project,
        &[
            "--json",
            "import-session",
            "begin",
            "--operation-uuid",
            &uuid::Uuid::now_v7().to_string(),
        ],
    ));
    let session = begun["session_uuid"].as_str().unwrap().to_owned();
    for args in [
        vec![
            "register-parquet",
            "--kind",
            "nodes",
            "--path",
            source.to_str().unwrap(),
        ],
        vec!["validate"],
        vec!["commit"],
    ] {
        let mut command = vec![
            "--json",
            "import-session",
            args[0],
            "--session-uuid",
            &session,
        ];
        command.extend_from_slice(&args[1..]);
        json(&gf(project, &command));
    }
}

#[test]
fn an_initial_import_reports_its_bulk_build_across_cli_processes() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    let input = root.path().join("nodes.parquet");
    write_nodes(
        &input,
        &[uuid::Uuid::now_v7(), uuid::Uuid::now_v7()],
        "Person",
    );
    let begun = json(&gf(
        &project,
        &[
            "--json",
            "import-session",
            "begin",
            "--operation-uuid",
            &uuid::Uuid::now_v7().to_string(),
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
            "--diagnostics",
            "import-session",
            "validate",
            "--session-uuid",
            session,
        ],
    ));
    assert_eq!(validated["outcome"], "stage+seal");
    assert_eq!(validated["region_diagnostics"]["complete"], true);
    let regions = &validated["region_diagnostics"]["regions"];
    assert!(regions["import_command/stage+seal/bulk_build"].is_object());
    assert!(regions["import_command/stage+seal/append_nodes"].is_null());
    let built = &validated["construction"]["bulk_build"];
    assert_eq!(
        (built["nodes"].as_u64(), built["edges"].as_u64()),
        (Some(2), Some(0))
    );
    assert_eq!(validated["construction"]["accepted_chunks"], 0);
    for pass in ["plan", "nodes", "edges", "tables", "ordinal", "adjacency"] {
        assert!(built["passes"][pass]["wall_ms"].is_u64(), "{pass}");
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
    assert_eq!(committed["outcome"], "committed");
    assert_eq!(
        committed["construction"]["bulk_build"]["nodes"],
        built["nodes"]
    );
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
    // An initial import runs on the bulk builder and stages nothing. This test
    // measures the staged path, which appends use, so import on a generation.
    seed_project(root.path(), &project);
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
            "--diagnostics",
            "import-session",
            "begin",
            "--operation-uuid",
            &operation,
        ],
    ));
    assert!(begun["region_diagnostics"]["regions"]["import_command/begin_import"].is_object());
    let session = begun["session_uuid"].as_str().unwrap();
    let registered = json(&gf(
        &project,
        &[
            "--json",
            "--diagnostics",
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
    assert!(
        registered["region_diagnostics"]["regions"]["import_command/resume_import"].is_object()
    );
    assert_eq!(
        registered["region_diagnostics"]["regions"]["import_command/register_parquet"]["work"]["bytes"],
        fs::metadata(&input).unwrap().len()
    );
    let validated = json(&gf(
        &project,
        &[
            "--json",
            "--diagnostics",
            "import-session",
            "validate",
            "--session-uuid",
            session,
        ],
    ));
    let regions = &validated["region_diagnostics"]["regions"];
    assert_eq!(validated["region_diagnostics"]["complete"], true);
    assert_eq!(validated["outcome"], "stage+seal");
    assert_eq!(
        validated["region_diagnostics"]["contract"],
        "graphforge-region-diagnostics/2"
    );
    assert!(regions["import_command/stage+seal/append_nodes"].is_object());
    assert!(regions["import_command/stage+seal/source_read"].is_object());
    assert!(regions["import_command/stage+seal/manifest_persistence"].is_object());
    assert!(
        regions["import_command/stage+seal/append_nodes"]["work"]["hashed_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        regions["import_command/stage+seal/append_nodes"]["work"]["written_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    // Reconcile the successful XXH64 authentication population from independent
    // retained chunk receipts, not attempted SHA measurements or source size.
    let mut parquet_bytes = 0_u64;
    let mut chunks = 0_u64;
    for operation in fs::read_dir(project.join(".graphforge-construction")).unwrap() {
        for entry in fs::read_dir(operation.unwrap().path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_str().unwrap().starts_with("receipt-") {
                let receipt: serde_json::Value =
                    serde_json::from_slice(&fs::read(entry.path()).unwrap()).unwrap();
                parquet_bytes += receipt["parquet"]["bytes"].as_u64().unwrap();
                chunks += 1;
            }
        }
    }
    assert!(chunks > 0);
    let routing_auth =
        &regions["import_command/stage+seal/seal/shaping/shape_routing/artifact_authentication"];
    assert_eq!(routing_auth["work"]["hashed_bytes"], parquet_bytes);
    assert_eq!(routing_auth["calls"], chunks);

    for field in [
        "hashed_bytes",
        "hash_elapsed_ns",
        "fsync_calls",
        "fsync_elapsed_ns",
    ] {
        assert!(
            regions["import_command/stage+seal"]["inclusive"][field]
                .as_u64()
                .unwrap()
                > 0,
            "missing {field}"
        );
        let measured = regions["import_command"]["inclusive"][field]
            .as_u64()
            .unwrap();
        let residual: u64 = regions
            .as_object()
            .unwrap()
            .values()
            .map(|row| row["residual"][field].as_u64().unwrap())
            .sum();
        assert_eq!(measured, residual, "{field} does not reconcile");
    }
    #[cfg(target_os = "linux")]
    assert!(
        regions["import_command/stage+seal"]["inclusive"]["written_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(regions["import_command/stage+seal/seal/shaping"].is_object());
    let root = regions["import_command"]["inclusive"]["wall_ns"]
        .as_u64()
        .unwrap();
    let residuals: u64 = regions
        .as_object()
        .unwrap()
        .values()
        .map(|row| row["residual"]["wall_ns"].as_u64().unwrap())
        .sum();
    assert_eq!(root, residuals);
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
            "--diagnostics",
            "import-session",
            "commit",
            "--session-uuid",
            session,
        ],
    ));
    assert_eq!(committed["region_diagnostics"]["complete"], true);
    let publish_regions = &committed["region_diagnostics"]["regions"];
    assert!(publish_regions["import_command/commit/publish"].is_object());
    // Reader preparation for a fresh publication runs against the durable
    // candidate before CURRENT, so hydration and read_authority nest under
    // generation_commit; the remaining regions stay sequential siblings.
    for child in [
        "prepare_encoding",
        "publication_authentication",
        "cas_install",
        "publication_intent",
        "generation_commit",
        "publication_receipt",
    ] {
        let path = format!("import_command/commit/publish/{child}");
        assert!(publish_regions[&path].is_object(), "missing {path}");
        assert_eq!(publish_regions[&path]["calls"], 1, "{path}");
    }
    for prepared in ["hydration", "read_authority"] {
        let path = format!("import_command/commit/publish/generation_commit/{prepared}");
        assert!(publish_regions[&path].is_object(), "missing {path}");
        assert_eq!(publish_regions[&path]["calls"], 1, "{path}");
    }
    let publish_root = publish_regions["import_command/commit/publish"]["inclusive"]["wall_ns"]
        .as_u64()
        .unwrap();
    let publish_children: u64 = publish_regions
        .as_object()
        .unwrap()
        .iter()
        .filter(|(path, _)| {
            path.starts_with("import_command/commit/publish/") && path.matches('/').count() == 3
        })
        .map(|(_, row)| row["inclusive"]["wall_ns"].as_u64().unwrap())
        .sum();
    assert!(publish_children <= publish_root);
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
            "--diagnostics",
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

#[test]
fn ordinary_json_import_does_not_request_region_diagnostics() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
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
    assert!(begun.get("region_diagnostics").is_none());
    assert!(
        begun
            .get("application_io")
            .is_none_or(serde_json::Value::is_null)
    );
}

#[test]
fn retired_format_and_duplicate_verbs_are_refused_without_destination_writes() {
    let root = TempDir::new().unwrap();
    let source = root.path().join("unsupported.bin");
    fs::write(&source, b"graphforge-exp\0\n").unwrap();
    let target = root.path().join("untouched");
    let output = gf(
        &target,
        &[
            "--json",
            "import",
            "--input",
            source.to_str().unwrap(),
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000999",
        ],
    );
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "GF_UNSUPPORTED_PROJECT_FORMAT");
    assert!(!target.exists());
    for verb in ["export", "import"] {
        let output = gf(&target, &["portable", verb]);
        assert!(!output.status.success());
        assert!(!target.exists());
    }
}

#[test]
fn primary_streaming_current_and_checkpoint_reopen_with_exact_query_results() {
    use arrow::array::Int64Array;
    use graphforge_api::{CheckpointRequest, GraphForge, OperationId};
    let root = TempDir::new().unwrap();
    let project = root.path().join("source");
    let graph = GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Person {name: 'Ada'})").unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "one".into(),
            description: None,
            idempotency_key: OperationId(uuid::Uuid::new_v4()),
            actor_uuid: None,
        })
        .unwrap();
    graph.execute("CREATE (:Person {name: 'Grace'})").unwrap();
    drop(graph);
    for (name, selector, expected) in [
        ("current", vec!["--current"], 2),
        ("checkpoint", vec!["--checkpoint", "one"], 1),
    ] {
        let bundle = root.path().join(format!("{name}.gfpb"));
        let mut args = vec!["--json", "export"];
        args.extend(selector);
        args.extend(["--format", "bundle", "--output", bundle.to_str().unwrap()]);
        let exported = json(&gf(&project, &args));
        assert_eq!(exported["contract"], "graphforge-portable-export/2");
        let target = root.path().join(format!("{name}-imported"));
        let operation = uuid::Uuid::new_v4().to_string();
        let imported = json(&gf(
            &target,
            &[
                "--json",
                "import",
                "--input",
                bundle.to_str().unwrap(),
                "--idempotency-key",
                &operation,
            ],
        ));
        assert_eq!(imported["package_digest"], exported["package_digest"]);
        let reopened = GraphForge::new(target.to_str()).unwrap();
        assert_eq!(reopened.node_count("Person").unwrap(), expected);
        let result = reopened
            .execute("MATCH (p:Person) RETURN count(p) AS people")
            .unwrap();
        assert_eq!(
            result.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected as i64
        );
    }
}

/// JCS-style canonical JSON: sorted keys, no whitespace, as the package
/// manifest and its digest preimage are written.
fn canonical_json(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            out.push('{');
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            for (index, key) in keys.into_iter().enumerate() {
                if index != 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).unwrap());
                out.push(':');
                canonical_json(&map[key], out);
            }
            out.push('}');
        }
        Value::Array(values) => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    out.push(',');
                }
                canonical_json(value, out);
            }
            out.push(']');
        }
        other => out.push_str(&serde_json::to_string(other).unwrap()),
    }
}

/// An expanded complete package whose manifest declares eight tebibytes of payload (under the per-file bound)
/// and is resealed, so its controls are admitted and only the disk can refuse
/// it. Nothing about the files on disk is inflated.
fn package_too_large_for_any_disk(root: &Path) -> std::path::PathBuf {
    use sha2::{Digest as _, Sha256};
    let source_project = root.join("source");
    fs::create_dir(&source_project).unwrap();
    let package = root.join("too-large.gfproject");
    json(&gf(
        &source_project,
        &[
            "--json",
            "export",
            "--current",
            "--format",
            "expanded",
            "--profile",
            "complete",
            "--output",
            package.to_str().unwrap(),
        ],
    ));
    let manifest_path = package.join("data/graphforge-project.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest.as_object_mut().unwrap().remove("package_digest");
    let length = &mut manifest["components"][0]["files"][0]["length"];
    *length = (length.as_u64().unwrap() + (1_u64 << 43)).into();
    let mut preimage = String::new();
    canonical_json(&manifest, &mut preimage);
    let digest =
        Sha256::digest([b"graphforge-project/2\0".as_slice(), preimage.as_bytes()].concat());
    manifest["package_digest"] = format!(
        "sha256:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
    .into();
    let mut sealed = String::new();
    canonical_json(&manifest, &mut sealed);
    fs::write(&manifest_path, sealed).unwrap();
    package
}

fn refused_import(destination: &Path, package: &Path) -> (Option<i32>, Value) {
    let output = gf(
        destination,
        &[
            "--json",
            "import",
            "--input",
            package.to_str().unwrap(),
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000770",
        ],
    );
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap_or_else(|_| {
        panic!(
            "JSON error, got {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code(), error)
}

fn assert_nothing_written(parent: &Path, destination: &Path) {
    assert!(!destination.exists());
    let staged: Vec<_> = fs::read_dir(parent)
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("portable-v2"))
        .collect();
    assert!(staged.is_empty(), "left staging behind: {staged:?}");
}

#[test]
fn an_import_the_disk_cannot_hold_is_refused_as_a_storage_error_before_extraction() {
    let root = TempDir::new().unwrap();
    let package = package_too_large_for_any_disk(root.path());
    let destination = root.path().join("destination");
    let (code, error) = refused_import(&destination, &package);
    assert_eq!(code, Some(3), "{error}");
    assert_eq!(error["error"]["code"], "GF_STORAGE", "{error}");
    assert_eq!(
        error["error"]["diagnostics"][0]["code"], "storage.insufficient_space",
        "{error}"
    );
    let message = error["error"]["diagnostics"][0]["message"]
        .as_str()
        .unwrap();
    assert!(
        message.contains("bytes available beside the target"),
        "{message}"
    );
    assert_nothing_written(root.path(), &destination);
}

#[cfg(target_os = "linux")]
#[test]
fn an_import_onto_an_unsupported_filesystem_keeps_its_code_and_exit_class() {
    let root = TempDir::new().unwrap();
    let package = package_too_large_for_any_disk(root.path());
    // `/dev/shm` is tmpfs, which the durable-publication contract does not admit.
    let shm = TempDir::new_in("/dev/shm").unwrap();
    let destination = shm.path().join("destination");
    let (code, error) = refused_import(&destination, &package);
    assert_eq!(code, Some(3), "{error}");
    assert_eq!(
        error["error"]["code"], "GF_UNSUPPORTED_FILESYSTEM",
        "{error}"
    );
    assert_eq!(
        error["error"]["diagnostics"][0]["code"], "storage.unsupported_filesystem",
        "{error}"
    );
    let message = error["error"]["diagnostics"][0]["message"]
        .as_str()
        .unwrap();
    assert!(message.contains("GF_UNSUPPORTED_FILESYSTEM"), "{message}");
    assert_nothing_written(shm.path(), &destination);
}
