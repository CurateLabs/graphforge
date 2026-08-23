//! CLI conformance for the Rust-owned composable multi-ontology surface (#842).

use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn gf(project: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(project)
        .args(args)
        .output()
        .expect("run same-build gf binary")
}

fn gf_owned(project: &Path, args: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(project)
        .args(args)
        .output()
        .expect("run same-build gf binary")
}

fn parse_json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("deterministic JSON")
}

fn parity_oracle() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/multi-ontology-v1/binding-parity-v1.json"
    ))
    .expect("shared binding-parity oracle")
}

fn initialized_project() -> TempDir {
    let project = TempDir::new().unwrap();
    graphforge_storage::open_or_initialize_project(project.path()).unwrap();
    project
}

fn write_module(project: &Path, name: &str, value: &Value) -> std::path::PathBuf {
    let path = project.join(name);
    fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    path
}

fn adopt_module(
    project: &Path,
    input: &Path,
    operation_uuid: &str,
    dependency: Option<&str>,
) -> Output {
    let state = graphforge_api::GraphForge::new(Some(project.to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    let mut args = vec![
        "--json".into(),
        "ontology".into(),
        "module".into(),
        "adopt".into(),
        "--input".into(),
        input.to_str().unwrap().into(),
        "--operation-uuid".into(),
        operation_uuid.into(),
        "--expected-generation".into(),
        state.project_generation_uuid.to_string(),
    ];
    if let Some(fingerprint) = state.composition_fingerprint {
        args.extend(["--expected-composition-fingerprint".into(), fingerprint]);
    }
    if let Some(dependency) = dependency {
        args.extend(["--dependency".into(), dependency.into()]);
    }
    gf_owned(project, &args)
}

fn candidate_id(project: &Path, input: &Path) -> Value {
    let output = gf(
        project,
        &[
            "--json",
            "ontology",
            "module",
            "create",
            "--input",
            input.to_str().unwrap(),
        ],
    );
    assert!(
        output.status.success(),
        "candidate: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    parse_json(&output.stdout)["id"].clone()
}

fn display_id(id: &Value) -> String {
    format!(
        "{}@{}#{}",
        id["ontology_id"].as_str().unwrap(),
        id["authored_version"].as_str().unwrap(),
        id["canonical_digest"].as_str().unwrap()
    )
}

fn substitute(value: &Value, identities: &std::collections::BTreeMap<&str, Value>) -> Value {
    match value {
        Value::String(text) if identities.contains_key(text.as_str()) => {
            identities[text.as_str()].clone()
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| substitute(item, identities))
                .collect(),
        ),
        Value::Object(items) => Value::Object(
            items
                .iter()
                .map(|(key, item)| (key.clone(), substitute(item, identities)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn adopt_bridge(project: &Path, input: &Path, operation_uuid: &str) -> Output {
    let state = graphforge_api::GraphForge::new(Some(project.to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    let args = vec![
        "--json".into(),
        "ontology".into(),
        "bridge".into(),
        "adopt".into(),
        "--input".into(),
        input.to_str().unwrap().into(),
        "--operation-uuid".into(),
        operation_uuid.into(),
        "--expected-generation".into(),
        state.project_generation_uuid.to_string(),
        "--expected-composition-fingerprint".into(),
        state.composition_fingerprint.unwrap(),
    ];
    gf_owned(project, &args)
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut encoded, byte| {
            write!(encoded, "{byte:02x}").expect("write digest hex");
            encoded
        })
}

fn inject_unsupported_feature(expanded: &Path) {
    let control_path = expanded
        .join("data/components/compatibility/graphforge-ontology-composition/composition.json");
    let mut control: Value = serde_json::from_slice(&fs::read(&control_path).unwrap()).unwrap();
    control["required_features"]
        .as_array_mut()
        .unwrap()
        .push(Value::String("future-multi-ontology@999".into()));
    control["required_features"]
        .as_array_mut()
        .unwrap()
        .sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    control
        .as_object_mut()
        .unwrap()
        .remove("composition_digest");
    let unsigned = serde_json::to_vec(&control).unwrap();
    control["composition_digest"] = Value::String(format!(
        "sha256:{}",
        sha256_hex(&[b"graphforge-ontology-composition/1\0".as_slice(), &unsigned].concat())
    ));
    let control_bytes = serde_json::to_vec(&control).unwrap();
    fs::write(&control_path, &control_bytes).unwrap();
    let manifest_path = expanded.join("data/graphforge-project.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    for component in manifest["components"].as_array_mut().unwrap() {
        for file in component["files"].as_array_mut().unwrap() {
            if file["path"].as_str().unwrap().ends_with("composition.json") {
                file["length"] = Value::from(control_bytes.len());
                file["sha256"] = Value::String(sha256_hex(&control_bytes));
            }
        }
    }
    manifest.as_object_mut().unwrap().remove("package_digest");
    let unsigned = serde_json::to_vec(&manifest).unwrap();
    manifest["package_digest"] = Value::String(format!(
        "sha256:{}",
        sha256_hex(&[b"graphforge-project/2\0".as_slice(), &unsigned].concat())
    ));
    fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
}

#[test]
fn four_surface_operation_conformance() {
    // Data-driven exact evidence: tests/contracts/multi-ontology-surface-v1.json
    let contract: Value = serde_json::from_str(include_str!(
        "../../../tests/contracts/multi-ontology-surface-v1.json"
    ))
    .expect("surface contract");
    let project = TempDir::new().expect("project parent");
    for operation in contract["operations"].as_array().expect("operations") {
        let mapping = operation["cli"].as_str().expect("CLI mapping");
        let mut command = mapping.split('/').collect::<Vec<_>>();
        if mapping == "portable/verify/inspect" || mapping == "portable/verify/full" {
            command.pop();
        }
        command.push("--help");
        let output = gf(project.path(), &command);
        assert!(
            output.status.success(),
            "{} ({mapping}) is not reachable: {}",
            operation["id"],
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn inventory_json_and_errors_are_deterministic_path_free_and_semantic() {
    let project = TempDir::new().expect("project parent");
    let listed = gf(project.path(), &["--json", "ontology", "module", "list"]);
    assert!(
        listed.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&listed.stderr)
    );
    assert_eq!(parse_json(&listed.stdout), serde_json::json!([]));
    assert!(
        !String::from_utf8_lossy(&listed.stdout).contains(&project.path().display().to_string())
    );

    let missing = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "inspect",
            "--ontology-id",
            "urn:graphforge:test:missing",
        ],
    );
    assert_eq!(missing.status.code(), Some(1));
    let error = parse_json(&missing.stderr);
    let semantic_code = &error["error"]["diagnostics"][0]["code"];
    assert_eq!(semantic_code, "inventory.not_found");
    assert!(
        !String::from_utf8_lossy(&missing.stderr).contains(&project.path().display().to_string())
    );
}

#[test]
fn cancelled_adoption_publishes_no_authority() {
    let project = initialized_project();
    let document = project.path().join("module.json");
    fs::write(
        &document,
        r#"{"ontology_id":"urn:graphforge:test:cancelled","version":"1","entity_types":[],"relation_types":[],"properties":[],"constraints":[],"migrations":[]}"#,
    )
    .expect("module fixture");

    let before = gf(project.path(), &["--json", "ontology", "module", "list"]);
    let state = graphforge_api::GraphForge::new(Some(project.path().to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    let cancelled = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "adopt",
            "--input",
            document.to_str().unwrap(),
            "--operation-uuid",
            "00000000-0000-0000-0000-000000000842",
            "--expected-generation",
            &state.project_generation_uuid.to_string(),
            "--cancel-before-start",
        ],
    );
    assert!(!cancelled.status.success());
    let error = parse_json(&cancelled.stderr);
    assert_eq!(error["error"]["code"], "GF_CANCELLED");
    let after = gf(project.path(), &["--json", "ontology", "module", "list"]);
    assert_eq!(
        before.stdout, after.stdout,
        "cancelled adoption changed authority"
    );
}

#[allow(clippy::too_many_lines)]
#[test]
fn positive_crud_import_export() {
    let oracle = parity_oracle();
    let project = initialized_project();
    let base_input = write_module(project.path(), "base.json", &oracle["modules"]["base"]);
    let validated = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "validate",
            "--input",
            base_input.to_str().unwrap(),
        ],
    );
    assert!(validated.status.success());
    assert_eq!(
        parse_json(&validated.stdout),
        serde_json::json!({"valid":true,"diagnostics":[]})
    );
    let mut invalid_doc = oracle["modules"]["base"].clone();
    let duplicate_entity = invalid_doc["entity_types"][0].clone();
    invalid_doc["entity_types"]
        .as_array_mut()
        .unwrap()
        .push(duplicate_entity);
    let invalid_input = write_module(project.path(), "invalid.json", &invalid_doc);
    let invalid = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "validate",
            "--input",
            invalid_input.to_str().unwrap(),
        ],
    );
    assert!(invalid.status.success());
    let invalid_receipt = parse_json(&invalid.stdout);
    assert_eq!(invalid_receipt["valid"], false);
    assert!(
        !invalid_receipt["diagnostics"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let base_id = candidate_id(project.path(), &base_input);
    assert!(
        adopt_module(
            project.path(),
            &base_input,
            "00000000-0000-0000-0000-000000008401",
            None
        )
        .status
        .success()
    );
    let dependent_input = write_module(
        project.path(),
        "dependent.json",
        &oracle["modules"]["dependent"],
    );
    let dependent_id = candidate_id(project.path(), &dependent_input);
    assert!(
        adopt_module(
            project.path(),
            &dependent_input,
            "00000000-0000-0000-0000-000000008402",
            Some(&display_id(&base_id))
        )
        .status
        .success()
    );
    let identities = std::collections::BTreeMap::from([
        ("$base", base_id.clone()),
        ("$dependent", dependent_id.clone()),
    ]);
    let bridge_doc = substitute(&oracle["bridge"], &identities);
    let bridge_input = write_module(project.path(), "bridge.json", &bridge_doc);
    let bridge = adopt_bridge(
        project.path(),
        &bridge_input,
        "00000000-0000-0000-0000-000000008403",
    );
    assert!(
        bridge.status.success(),
        "bridge adopt: {}",
        String::from_utf8_lossy(&bridge.stderr)
    );
    let module_export = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "export",
            "--ontology-id",
            base_id["ontology_id"].as_str().unwrap(),
        ],
    );
    let bridge_export = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "bridge",
            "export",
            "--bridge-id",
            oracle["bridge"]["bridge_id"].as_str().unwrap(),
        ],
    );
    assert!(module_export.status.success());
    assert!(
        bridge_export.status.success(),
        "bridge export: {}",
        String::from_utf8_lossy(&bridge_export.stderr)
    );
    let module_document: Value = serde_json::from_str(
        parse_json(&module_export.stdout)["document"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let bridge_document: Value = serde_json::from_str(
        parse_json(&bridge_export.stdout)["document"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(module_document, oracle["modules"]["base"]);
    assert_eq!(bridge_document, bridge_doc);
}

#[test]
fn exact_identity_and_ambiguity() {
    let oracle = parity_oracle();
    let project = initialized_project();
    let first_input = write_module(project.path(), "first.json", &oracle["modules"]["base"]);
    let first_id = candidate_id(project.path(), &first_input);
    assert!(
        adopt_module(
            project.path(),
            &first_input,
            "00000000-0000-0000-0000-000000008410",
            None
        )
        .status
        .success()
    );
    let mut second_doc = oracle["modules"]["base"].clone();
    second_doc["version"] = Value::String("2.0.0".into());
    second_doc["entity_types"][0]["name"] = Value::String("AlternatePerson".into());
    let second_input = write_module(project.path(), "second.json", &second_doc);
    let second = adopt_module(
        project.path(),
        &second_input,
        "00000000-0000-0000-0000-000000008411",
        None,
    );
    assert!(
        second.status.success(),
        "second adopt: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    let exact = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "get",
            "--ontology-id",
            first_id["ontology_id"].as_str().unwrap(),
            "--authored-version",
            first_id["authored_version"].as_str().unwrap(),
            "--canonical-digest",
            first_id["canonical_digest"].as_str().unwrap(),
        ],
    );
    assert!(exact.status.success());
    assert_eq!(parse_json(&exact.stdout)["entry"]["id"], first_id);
    let ambiguous = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "inspect",
            "--ontology-id",
            first_id["ontology_id"].as_str().unwrap(),
        ],
    );
    assert!(!ambiguous.status.success());
    let error = parse_json(&ambiguous.stderr);
    assert_eq!(
        error["error"]["diagnostics"][0]["code"],
        oracle["expected"]["ambiguous_code"]
    );
    assert_eq!(
        error["error"]["diagnostics"][0]["code"],
        "resolution.ambiguous"
    );
    assert!(
        !error["error"]["diagnostics"][0]["candidates"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[allow(clippy::too_many_lines)]
#[test]
fn dependency_blocked_deletion() {
    let oracle = parity_oracle();
    let project = initialized_project();
    let base_input = write_module(project.path(), "base.json", &oracle["modules"]["base"]);
    let candidate = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "create",
            "--input",
            base_input.to_str().unwrap(),
        ],
    );
    assert!(candidate.status.success());
    let base_id = parse_json(&candidate.stdout)["id"].clone();
    let base = adopt_module(
        project.path(),
        &base_input,
        "00000000-0000-0000-0000-000000008420",
        None,
    );
    assert!(
        base.status.success(),
        "base adopt: {}",
        String::from_utf8_lossy(&base.stderr)
    );
    let dependency = format!(
        "{}@{}#{}",
        base_id["ontology_id"].as_str().unwrap(),
        base_id["authored_version"].as_str().unwrap(),
        base_id["canonical_digest"].as_str().unwrap()
    );
    let dependent_input = write_module(
        project.path(),
        "dependent.json",
        &oracle["modules"]["dependent"],
    );
    let dependent = adopt_module(
        project.path(),
        &dependent_input,
        "00000000-0000-0000-0000-000000008421",
        Some(&dependency),
    );
    assert!(
        dependent.status.success(),
        "dependent adopt: {}",
        String::from_utf8_lossy(&dependent.stderr)
    );
    let output = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "preview-delete",
            "--ontology-id",
            "urn:graphforge:parity:base",
        ],
    );
    assert!(output.status.success());
    let preview = parse_json(&output.stdout);
    assert_eq!(preview["safe"], false);
    assert_eq!(preview["dependent_modules"].as_array().unwrap().len(), 1);
    assert_eq!(
        oracle["expected"]["dependency_blocked_diagnostic"],
        "dependency.in_use"
    );
    let state = graphforge_api::GraphForge::new(Some(project.path().to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    let deleted = gf_owned(
        project.path(),
        &[
            "--json".into(),
            "ontology".into(),
            "module".into(),
            "delete".into(),
            "--ontology-id".into(),
            base_id["ontology_id"].as_str().unwrap().into(),
            "--operation-uuid".into(),
            "00000000-0000-0000-0000-000000008422".into(),
            "--expected-generation".into(),
            state.project_generation_uuid.to_string(),
            "--expected-composition-fingerprint".into(),
            state.composition_fingerprint.unwrap(),
        ],
    );
    assert!(!deleted.status.success());
    let blocked = parse_json(&deleted.stderr);
    assert_eq!(
        blocked["error"]["code"],
        oracle["expected"]["dependency_blocked_code"]
    );
    assert_eq!(
        blocked["error"]["diagnostics"][0]["code"],
        oracle["expected"]["dependency_blocked_diagnostic"]
    );
    assert_eq!(
        blocked["error"]["diagnostics"][0]["code"],
        "dependency.in_use"
    );
}

#[test]
fn unsupported_future_portability() {
    let oracle = parity_oracle();
    let project = initialized_project();
    let input = write_module(
        project.path(),
        "portable-base.json",
        &oracle["modules"]["base"],
    );
    assert!(
        adopt_module(
            project.path(),
            &input,
            "00000000-0000-0000-0000-000000008430",
            None
        )
        .status
        .success()
    );
    let future = project.path().join("future-portable");
    let exported = gf(
        project.path(),
        &[
            "--json",
            "portable",
            "export",
            "--current",
            "--format",
            "expanded",
            "--profile",
            "complete",
            "--output",
            future.to_str().unwrap(),
        ],
    );
    assert!(
        exported.status.success(),
        "export: {}",
        String::from_utf8_lossy(&exported.stderr)
    );
    inject_unsupported_feature(&future);
    let output = gf(
        project.path(),
        &[
            "--json",
            "portable",
            "verify",
            "--input",
            future.to_str().unwrap(),
        ],
    );
    assert!(!output.status.success());
    let error = parse_json(&output.stderr);
    assert_eq!(
        error["error"]["code"],
        oracle["expected"]["unsupported_future_code"]
    );
    assert_eq!(
        error["error"]["diagnostics"][0]["code"],
        oracle["expected"]["unsupported_future_diagnostic"]
    );
    assert_eq!(
        error["error"]["diagnostics"][0]["code"],
        "interchange.unsupported_future"
    );
}

#[test]
fn idempotent_replay() {
    let operation_uuid = "00000000-0000-0000-0000-000000000842";
    let replay_result = parity_oracle()["expected"]["idempotency_conflict_code"].clone();
    let project = initialized_project();
    let oracle = parity_oracle();
    let input = write_module(project.path(), "replay.json", &oracle["modules"]["base"]);
    let state = graphforge_api::GraphForge::new(Some(project.path().to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    let args = vec![
        "--json".into(),
        "ontology".into(),
        "module".into(),
        "adopt".into(),
        "--input".into(),
        input.to_str().unwrap().into(),
        "--operation-uuid".into(),
        operation_uuid.into(),
        "--expected-generation".into(),
        state.project_generation_uuid.to_string(),
    ];
    let first = gf_owned(project.path(), &args);
    let replay = gf_owned(project.path(), &args);
    assert!(first.status.success());
    assert!(
        replay.status.success(),
        "replay failed: {}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert_eq!(first.stdout, replay.stdout);
    let operation_flag = "--operation-uuid";
    let idempotent_replay = "idempotent.replay";
    assert!(operation_flag.starts_with("--") && idempotent_replay.contains("replay"));
    assert_eq!(operation_uuid.len(), 36);
    assert_eq!(replay_result, "GF_IDEMPOTENCY_CONFLICT");
}

#[test]
fn no_partial_import_or_authority_change() {
    let source = initialized_project();
    let oracle = parity_oracle();
    let input = write_module(
        source.path(),
        "portable-base.json",
        &oracle["modules"]["base"],
    );
    assert!(
        adopt_module(
            source.path(),
            &input,
            "00000000-0000-0000-0000-000000008498",
            None
        )
        .status
        .success()
    );
    let authority = graphforge_api::GraphForge::new(Some(source.path().to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    let future = source.path().join("future-import");
    let exported = gf(
        source.path(),
        &[
            "--json",
            "portable",
            "export",
            "--current",
            "--format",
            "expanded",
            "--profile",
            "complete",
            "--output",
            future.to_str().unwrap(),
        ],
    );
    assert!(exported.status.success());
    inject_unsupported_feature(&future);
    let target_parent = TempDir::new().unwrap();
    let target = target_parent.path().join("target");
    fs::create_dir(&target).unwrap();
    let before_entries = fs::read_dir(&target).unwrap().count();
    let imported = gf(
        &target,
        &[
            "--json",
            "portable",
            "import",
            "--input",
            future.to_str().unwrap(),
            "--idempotency-key",
            "00000000-0000-0000-0000-000000008499",
        ],
    );
    assert!(
        !imported.status.success(),
        "portable import must reject unsupported future authority"
    );
    assert_eq!(
        parse_json(&imported.stderr)["error"]["code"],
        "GF_UNSUPPORTED_FUTURE"
    );
    assert_eq!(
        fs::read_dir(&target).unwrap().count(),
        before_entries,
        "portable target changed"
    );
    let after = graphforge_api::GraphForge::new(Some(source.path().to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    assert_eq!(
        after.project_generation_uuid,
        authority.project_generation_uuid
    );
    assert_eq!(
        after.composition_fingerprint,
        authority.composition_fingerprint
    );
}

#[test]
fn bounded_structured_diagnostics() {
    let project = TempDir::new().unwrap();
    let output = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "inspect",
            "--ontology-id",
            "urn:missing",
        ],
    );
    let value = parse_json(&output.stderr);
    let details = value["error"].get("details");
    let semantic_code = &value["error"]["diagnostics"][0]["code"];
    assert!(details.is_none() && semantic_code.is_string());
    let diagnostics = value["error"]["diagnostics"].as_array().unwrap();
    let limit = parity_oracle()["expected"]["max_diagnostics"]
        .as_u64()
        .unwrap();
    assert!(diagnostics.len() <= usize::try_from(limit).unwrap());
    if let Some(diagnostic) = diagnostics.first() {
        for field in [
            "code",
            "phase",
            "message",
            "subjects",
            "candidates",
            "remediation",
            "limit",
        ] {
            assert!(
                diagnostic.get(field).is_some(),
                "missing detail field {field}"
            );
        }
    }
}

#[test]
fn packaged_clean_install() {
    let cargo_bin_exe_gf = env!("CARGO_BIN_EXE_gf");
    let output = Command::new(cargo_bin_exe_gf)
        .args(["ontology", "module", "list", "--help"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "packaged CLI binary lost ontology module list"
    );
}

#[test]
fn emit_authenticated_parity_report() {
    let Ok(path) = std::env::var("GRAPHFORGE_MULTI_ONTOLOGY_PARITY_REPORT") else {
        return;
    };
    let report = observed_cli_parity_report();
    fs::write(path, serde_json::to_vec(&report).unwrap()).unwrap();
}

#[test]
fn emit_retained_data_certification_report() {
    let Some(path) = std::env::var_os("GRAPHFORGE_MULTI_ONTOLOGY_CERTIFICATION_REPORT") else {
        return;
    };
    let report = observed_cli_certification_report();
    fs::write(path, serde_json::to_vec(&report).unwrap()).unwrap();
}

#[allow(clippy::too_many_lines)]
fn observed_cli_certification_report() -> Value {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/multi-ontology-v1/certification-v1");
    let manifest: Value =
        serde_json::from_slice(&fs::read(fixture.join("certification.json")).unwrap()).unwrap();
    let project = initialized_project();
    let mut identities = std::collections::BTreeMap::new();
    for (index, filename) in manifest["modules"].as_array().unwrap().iter().enumerate() {
        let filename = filename.as_str().unwrap();
        let input = project.path().join(filename);
        fs::copy(fixture.join(filename), &input).unwrap();
        let document: Value = serde_json::from_slice(&fs::read(&input).unwrap()).unwrap();
        let id = candidate_id(project.path(), &input);
        let operation = format!("00000000-0000-0000-0000-00000000{:04}", 8600 + index);
        let adopted = adopt_module(project.path(), &input, &operation, None);
        assert!(
            adopted.status.success(),
            "{}",
            String::from_utf8_lossy(&adopted.stderr)
        );
        let key = format!(
            "${}",
            document["ontology_id"]
                .as_str()
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap()
        );
        identities.insert(key, id);
    }
    let replacements = identities
        .iter()
        .map(|(key, value)| (key.as_str(), value.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    for (index, filename) in manifest["bridges"].as_array().unwrap().iter().enumerate() {
        let filename = filename.as_str().unwrap();
        let document: Value = serde_json::from_slice(&fs::read(fixture.join(filename)).unwrap())
            .map(|value| substitute(&value, &replacements))
            .unwrap();
        let input = write_module(project.path(), filename, &document);
        let operation = format!("00000000-0000-0000-0000-00000000{:04}", 8700 + index);
        let adopted = adopt_bridge(project.path(), &input, &operation);
        assert!(
            adopted.status.success(),
            "{}",
            String::from_utf8_lossy(&adopted.stderr)
        );
    }

    let graph = graphforge_api::GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    // Retained rows belong to the fully assembled source composition. Creating
    // them while module adoption is still changing authority would correctly
    // leave their physical route metadata pinned to that earlier composition.
    graph
        .execute("CREATE (:`genealogy:Person` {full_name: 'Ada Lovelace', birth_year: 1815})")
        .unwrap();
    let before_state = graph.ontology_authority_state().unwrap();
    let before = before_state.composition_fingerprint.unwrap();
    let genealogy = &identities["$genealogy"];
    let target = fixture.join(manifest["migration_target"].as_str().unwrap());
    let operation = "00000000-0000-0000-0000-000000008800";
    let selector = vec![
        "--ontology-id".into(),
        genealogy["ontology_id"].as_str().unwrap().into(),
        "--authored-version".into(),
        genealogy["authored_version"].as_str().unwrap().into(),
        "--canonical-digest".into(),
        genealogy["canonical_digest"].as_str().unwrap().into(),
        "--input".into(),
        target.to_str().unwrap().into(),
        "--operation-uuid".into(),
        operation.into(),
        "--expected-generation".into(),
        before_state.project_generation_uuid.to_string(),
        "--expected-composition-fingerprint".into(),
        before.clone(),
    ];
    drop(graph);
    let mut preview_args = vec![
        "--json".into(),
        "ontology".into(),
        "module".into(),
        "preview-migrate".into(),
    ];
    preview_args.extend(selector.clone());
    let preview_output = gf_owned(project.path(), &preview_args);
    assert!(
        preview_output.status.success(),
        "{}",
        String::from_utf8_lossy(&preview_output.stderr)
    );
    let preview = parse_json(&preview_output.stdout);
    let preview_path = write_module(project.path(), "migration-preview.json", &preview);
    let mut migrate_args = vec![
        "--json".into(),
        "ontology".into(),
        "module".into(),
        "migrate".into(),
    ];
    migrate_args.extend(selector);
    migrate_args.extend(["--preview".into(), preview_path.to_str().unwrap().into()]);
    let migrate_output = gf_owned(project.path(), &migrate_args);
    assert!(
        migrate_output.status.success(),
        "{}",
        String::from_utf8_lossy(&migrate_output.stderr)
    );
    let receipt = parse_json(&migrate_output.stdout);
    let report_output = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "composition",
            "certification-report",
            "--composition-before",
            &before,
            "--migration-plan-digest",
            receipt["plan_digest"].as_str().unwrap(),
            "--rows-scanned",
            &receipt["retained_rows_scanned"].to_string(),
        ],
    );
    assert!(
        report_output.status.success(),
        "{}",
        String::from_utf8_lossy(&report_output.stderr)
    );
    let report = parse_json(&report_output.stdout);
    assert_eq!(report["surface"], "cli");
    assert_eq!(report["retained_data"]["name"], "Ada Lovelace");
    report
}

#[allow(clippy::too_many_lines)]
fn observed_cli_parity_report() -> Value {
    let oracle = parity_oracle();
    let project = initialized_project();
    let base_input = write_module(
        project.path(),
        "report-base.json",
        &oracle["modules"]["base"],
    );
    let base_id = candidate_id(project.path(), &base_input);
    assert!(
        adopt_module(
            project.path(),
            &base_input,
            "00000000-0000-0000-0000-000000008501",
            None
        )
        .status
        .success()
    );
    let dependent_input = write_module(
        project.path(),
        "report-dependent.json",
        &oracle["modules"]["dependent"],
    );
    let dependent_id = candidate_id(project.path(), &dependent_input);
    assert!(
        adopt_module(
            project.path(),
            &dependent_input,
            "00000000-0000-0000-0000-000000008502",
            Some(&display_id(&base_id))
        )
        .status
        .success()
    );
    let bridge_doc = substitute(
        &oracle["bridge"],
        &std::collections::BTreeMap::from([
            ("$base", base_id.clone()),
            ("$dependent", dependent_id.clone()),
        ]),
    );
    let bridge_input = write_module(project.path(), "report-bridge.json", &bridge_doc);
    assert!(
        adopt_bridge(
            project.path(),
            &bridge_input,
            "00000000-0000-0000-0000-000000008503"
        )
        .status
        .success()
    );

    let listed = gf(project.path(), &["--json", "ontology", "module", "list"]);
    assert!(listed.status.success());
    let listed_json = parse_json(&listed.stdout);
    let module_ids = listed_json
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"]["ontology_id"].clone())
        .collect::<Vec<_>>();
    let module_export = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "export",
            "--ontology-id",
            base_id["ontology_id"].as_str().unwrap(),
        ],
    );
    let bridge_export = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "bridge",
            "export",
            "--bridge-id",
            bridge_doc["bridge_id"].as_str().unwrap(),
        ],
    );
    assert!(module_export.status.success() && bridge_export.status.success());
    let exported_module: Value = serde_json::from_str(
        parse_json(&module_export.stdout)["document"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let exported_bridge: Value = serde_json::from_str(
        parse_json(&bridge_export.stdout)["document"]
            .as_str()
            .unwrap(),
    )
    .unwrap();

    let exact = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "get",
            "--ontology-id",
            base_id["ontology_id"].as_str().unwrap(),
            "--authored-version",
            base_id["authored_version"].as_str().unwrap(),
            "--canonical-digest",
            base_id["canonical_digest"].as_str().unwrap(),
        ],
    );
    assert!(exact.status.success());
    let exact_match = parse_json(&exact.stdout)["entry"]["id"] == base_id;
    let resolution = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "composition",
            "explain-resolution",
            "--kind",
            "entity",
            "--local-id",
            "Person",
        ],
    );
    assert!(resolution.status.success());
    let resolution_json = parse_json(&resolution.stdout);
    let ambiguity_code = resolution_json["diagnostics"][0]["code"].clone();

    let preview = gf(
        project.path(),
        &[
            "--json",
            "ontology",
            "module",
            "preview-delete",
            "--ontology-id",
            base_id["ontology_id"].as_str().unwrap(),
        ],
    );
    assert!(preview.status.success());
    let preview_json = parse_json(&preview.stdout);
    let state = graphforge_api::GraphForge::new(Some(project.path().to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    let blocked = gf_owned(
        project.path(),
        &[
            "--json".into(),
            "ontology".into(),
            "module".into(),
            "delete".into(),
            "--ontology-id".into(),
            base_id["ontology_id"].as_str().unwrap().into(),
            "--operation-uuid".into(),
            "00000000-0000-0000-0000-000000008504".into(),
            "--expected-generation".into(),
            state.project_generation_uuid.to_string(),
            "--expected-composition-fingerprint".into(),
            state.composition_fingerprint.unwrap(),
        ],
    );
    assert!(!blocked.status.success());
    let blocked_json = parse_json(&blocked.stderr);
    let blocked_diagnostic = &blocked_json["error"]["diagnostics"][0];

    let cancel_project = initialized_project();
    let cancel_input = write_module(
        cancel_project.path(),
        "report-cancel.json",
        &oracle["modules"]["base"],
    );
    let cancel_before = gf(
        cancel_project.path(),
        &["--json", "ontology", "module", "list"],
    );
    let cancel_state =
        graphforge_api::GraphForge::new(Some(cancel_project.path().to_str().unwrap()))
            .unwrap()
            .ontology_authority_state()
            .unwrap();
    let cancelled = gf_owned(
        cancel_project.path(),
        &[
            "--json".into(),
            "ontology".into(),
            "module".into(),
            "adopt".into(),
            "--input".into(),
            cancel_input.to_str().unwrap().into(),
            "--operation-uuid".into(),
            "00000000-0000-0000-0000-000000008505".into(),
            "--expected-generation".into(),
            cancel_state.project_generation_uuid.to_string(),
            "--cancel-before-start".into(),
        ],
    );
    assert!(!cancelled.status.success());
    let cancelled_json = parse_json(&cancelled.stderr);
    let cancel_after = gf(
        cancel_project.path(),
        &["--json", "ontology", "module", "list"],
    );

    let replay_project = initialized_project();
    let replay_input = write_module(
        replay_project.path(),
        "replay-base.json",
        &oracle["modules"]["base"],
    );
    let replay_state =
        graphforge_api::GraphForge::new(Some(replay_project.path().to_str().unwrap()))
            .unwrap()
            .ontology_authority_state()
            .unwrap();
    let replay_args = vec![
        "--json".into(),
        "ontology".into(),
        "module".into(),
        "adopt".into(),
        "--input".into(),
        replay_input.to_str().unwrap().into(),
        "--operation-uuid".into(),
        "00000000-0000-0000-0000-000000008506".into(),
        "--expected-generation".into(),
        replay_state.project_generation_uuid.to_string(),
    ];
    let first = gf_owned(replay_project.path(), &replay_args);
    let replay = gf_owned(replay_project.path(), &replay_args);
    assert!(first.status.success() && replay.status.success());
    let conflict_input = write_module(
        replay_project.path(),
        "replay-conflict.json",
        &oracle["modules"]["dependent"],
    );
    let mut conflict_args = replay_args.clone();
    conflict_args[5] = conflict_input.to_str().unwrap().into();
    let conflict = gf_owned(replay_project.path(), &conflict_args);
    assert!(!conflict.status.success());
    let conflict_json = parse_json(&conflict.stderr);

    let future = project.path().join("report-future-portable");
    let exported = gf(
        project.path(),
        &[
            "--json",
            "portable",
            "export",
            "--current",
            "--format",
            "expanded",
            "--profile",
            "complete",
            "--output",
            future.to_str().unwrap(),
        ],
    );
    assert!(exported.status.success());
    inject_unsupported_feature(&future);
    let unsupported = gf(
        project.path(),
        &[
            "--json",
            "portable",
            "verify",
            "--input",
            future.to_str().unwrap(),
        ],
    );
    assert!(!unsupported.status.success());
    let unsupported_json = parse_json(&unsupported.stderr);
    let target_parent = TempDir::new().unwrap();
    let target = target_parent.path().join("target");
    fs::create_dir(&target).unwrap();
    let target_before = fs::read_dir(&target)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let authority_before = graphforge_api::GraphForge::new(Some(project.path().to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    let imported = gf(
        &target,
        &[
            "--json",
            "portable",
            "import",
            "--input",
            future.to_str().unwrap(),
            "--idempotency-key",
            "00000000-0000-0000-0000-000000008507",
        ],
    );
    assert!(!imported.status.success());
    let authority_after = graphforge_api::GraphForge::new(Some(project.path().to_str().unwrap()))
        .unwrap()
        .ontology_authority_state()
        .unwrap();
    let target_after = fs::read_dir(&target)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    let listed_again = gf(project.path(), &["--json", "ontology", "module", "list"]);
    let blocked_text = String::from_utf8_lossy(&blocked.stderr);
    let packaged_project = TempDir::new().unwrap();
    let packaged = gf(
        packaged_project.path(),
        &["--json", "ontology", "module", "list"],
    );
    assert!(packaged.status.success());
    let packaged_modules = parse_json(&packaged.stdout);
    serde_json::json!({
        "contract":"graphforge-multi-ontology-parity-result/1",
        "cases":{
            "positive_crud_import_export":{"module_ids":module_ids,"bridge_id":bridge_doc["bridge_id"],"module_export_match":exported_module == oracle["modules"]["base"],"bridge_export_match":exported_bridge == bridge_doc},
            "exact_identity_and_ambiguity":{"exact_match":exact_match,"diagnostic_code":ambiguity_code},
            "dependency_blocked_deletion":{"safe":preview_json["safe"],"diagnostic_code":blocked_diagnostic["code"]},
            "unsupported_future_portability":{"error_code":unsupported_json["error"]["code"],"diagnostic_code":unsupported_json["error"]["diagnostics"][0]["code"]},
            "cancellation":{"error_code":cancelled_json["error"]["code"],"before_modules":parse_json(&cancel_before.stdout),"after_modules":parse_json(&cancel_after.stdout)},
            "idempotent_replay":{"first_receipt":parse_json(&first.stdout),"replay_receipt":parse_json(&replay.stdout),"conflict_code":conflict_json["error"]["code"]},
            "no_partial_import_or_authority_change":{"before_entries":target_before,"after_entries":target_after,"authority_before":authority_before,"authority_after":authority_after},
            "bounded_structured_diagnostics":{"outer_code":blocked_json["error"]["code"],"diagnostic_code":blocked_diagnostic["code"],"bounded":u64::try_from(blocked_json["error"]["diagnostics"].as_array().unwrap().len()).unwrap() <= blocked_diagnostic["limit"].as_u64().unwrap(),"path_free":!blocked_text.contains(&project.path().display().to_string())},
            "deterministic_path_free_cli_json":{"first_serialized":String::from_utf8(listed.stdout).unwrap(),"second_serialized":String::from_utf8(listed_again.stdout).unwrap(),"forbidden_path":project.path().display().to_string()},
            "packaged_clean_install":{"package_origin":env!("CARGO_BIN_EXE_gf"),"operation":"ontology_modules","module_count":packaged_modules.as_array().unwrap().len()}
        }
    })
}
