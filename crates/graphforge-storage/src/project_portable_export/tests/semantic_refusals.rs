use super::*;
use serde_json::{Value, json};

fn assert_refusal_in_both_forms(
    plan: &PortableExportPlan,
    code: PortableErrorCode,
    detail: &str,
    entry: Option<&str>,
) {
    let outputs = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(plan, outputs.path());
    for path in [&expanded, &bundle] {
        let error =
            verify_portable(path, PortableMode::Full, PortableLimits::default(), None).unwrap_err();
        assert_eq!(error.code, code, "{detail}: {error}");
        assert_eq!(error.to_string(), format!("portable {code:?}: {detail}"));
        assert_eq!(error.entry.as_deref(), entry);
    }
}

#[test]
fn authenticated_manifest_refuses_invalid_authority_and_selection() {
    let (_project, generation) = graph_generation_with_bridge();
    let original = plan_complete_portable(&generation, PortableLimits::default()).unwrap();
    for case in [
        "dependency-rule",
        "capability",
        "dangling",
        "cycle",
        "selection",
        "generation",
        "state",
        "complete-omission",
        "subset",
        "component-order",
        "component-identity",
        "descriptor",
    ] {
        let mut plan = original.clone();
        let mut manifest: Value = serde_json::from_slice(&plan.manifest).unwrap();
        let (code, detail, entry) = match case {
            "dependency-rule" => {
                manifest["requirements"]["dependency_rule"] =
                    json!("required-transitive-closure/2");
                (
                    PortableErrorCode::UnsupportedFuture,
                    "dependency rule",
                    None,
                )
            }
            "capability" => {
                manifest["requirements"]["capabilities"] = json!(["future-capability@1"]);
                (PortableErrorCode::UnsupportedFuture, "capability", None)
            }
            "dangling" => {
                manifest["components"][0]["required_dependencies"] = json!(["missing-component"]);
                (PortableErrorCode::Incompatible, "dependency closure", None)
            }
            "cycle" => {
                let first = manifest["components"][0]["participant_id"].clone();
                let second = manifest["components"][1]["participant_id"].clone();
                manifest["components"][0]["required_dependencies"] = json!([second]);
                manifest["components"][1]["required_dependencies"] = json!([first]);
                (PortableErrorCode::Incompatible, "dependency cycle", None)
            }
            "selection" => {
                manifest["selection"]["roots"] = json!(["missing-root"]);
                (
                    PortableErrorCode::Incompatible,
                    "unknown selection root",
                    None,
                )
            }
            "generation" => {
                manifest["source_generation"]["generation_uuid"] = json!(Uuid::nil().to_string());
                (
                    PortableErrorCode::Incompatible,
                    "source generation identity",
                    None,
                )
            }
            "state" => {
                manifest["states"]["integrity"] = json!("unverified");
                (
                    PortableErrorCode::Incompatible,
                    "manifest state declaration",
                    None,
                )
            }
            "complete-omission" => {
                manifest["selection"]["omissions"] = json!(["omitted-component"]);
                (
                    PortableErrorCode::Incompatible,
                    "complete package has omissions/redactions",
                    None,
                )
            }
            "subset" => {
                manifest["package_class"] = json!("graph-data-subset");
                (
                    PortableErrorCode::Incompatible,
                    "graph subset/class mismatch",
                    None,
                )
            }
            "component-order" => {
                manifest["components"].as_array_mut().unwrap().reverse();
                (PortableErrorCode::Incompatible, "component order", None)
            }
            "component-identity" => {
                manifest["components"][0]["participant_id"] = json!("UPPERCASE");
                (PortableErrorCode::Incompatible, "component identity", None)
            }
            "descriptor" => {
                let file = &mut manifest["components"][0]["files"][0];
                file["media_type"] = json!("INVALID");
                (
                    PortableErrorCode::Incompatible,
                    "file descriptor",
                    Some(file["path"].as_str().unwrap().to_owned()),
                )
            }
            _ => unreachable!(),
        };
        plan.manifest = canonical_json(&manifest).unwrap();
        resign_test_manifest(&mut plan);
        assert_refusal_in_both_forms(&plan, code, detail, entry.as_deref());
    }
}

#[test]
fn authenticated_runtime_map_refuses_dangling_or_invalid_placement() {
    const PATH: &str =
        "data/components/compatibility/graphforge-runtime-map/runtime-generation.json";
    let (_project, generation) = compact_graph_generation();
    let original = plan_complete_portable(&generation, PortableLimits::default()).unwrap();
    let PlannedSource::Control(bytes) = &original
        .files
        .iter()
        .find(|file| file.path == PATH)
        .unwrap()
        .source
    else {
        panic!("runtime map must be inline")
    };
    let runtime: Value = serde_json::from_slice(bytes).unwrap();
    for case in ["contract", "participant", "capability", "placement"] {
        let mut plan = original.clone();
        let mut control = runtime.clone();
        let detail = match case {
            "contract" => {
                control["contract"] = json!("graphforge-runtime-generation-map/2");
                "runtime map contract"
            }
            "participant" => {
                control["participants"][0]["participant_id"] = json!("missing-participant");
                "runtime participant mapping"
            }
            "capability" => {
                control["capabilities"][0]["capability_version"] = json!(0);
                "runtime capability mapping"
            }
            "placement" => {
                control["graph_tree"]["inventory_participant_id"] = json!("missing-inventory");
                "runtime graph placement"
            }
            _ => unreachable!(),
        };
        replace_test_control(&mut plan, PATH, canonical_json(&control).unwrap());
        assert_refusal_in_both_forms(&plan, PortableErrorCode::Incompatible, detail, Some(PATH));
    }
}

#[test]
fn authenticated_composition_refuses_invalid_activation_and_identity() {
    const PATH: &str = crate::project_portable::ONTOLOGY_COMPOSITION_PATH;
    let (_project, generation) = graph_generation_with_bridge_activation(true);
    let original = plan_complete_portable(&generation, PortableLimits::default()).unwrap();
    let PlannedSource::Control(bytes) = &original
        .files
        .iter()
        .find(|file| file.path == PATH)
        .unwrap()
        .source
    else {
        panic!("composition must be inline")
    };
    let original_control: Value = serde_json::from_slice(bytes).unwrap();
    for case in [
        "activation-subject",
        "activation-duplicate",
        "dependencies",
        "digest",
        "empty-endpoint",
        "identity",
    ] {
        let mut plan = original.clone();
        let mut control = original_control.clone();
        let (code, detail, entry) = match case {
            "activation-subject" => {
                control["activation_profile"]["overrides"][0]["subject"]["id"] =
                    json!("https://graphforge.dev/bridge/missing");
                (PortableErrorCode::Incompatible, "activation closure", None)
            }
            "activation-duplicate" => {
                let overrides = control["activation_profile"]["overrides"]
                    .as_array_mut()
                    .unwrap();
                overrides.insert(1, overrides[0].clone());
                (PortableErrorCode::Incompatible, "activation order", None)
            }
            "dependencies" => {
                let mut manifest: Value = serde_json::from_slice(&plan.manifest).unwrap();
                let component = manifest["components"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|component| {
                        component["participant_id"] == "graphforge-ontology-composition"
                    })
                    .unwrap();
                component["required_dependencies"]
                    .as_array_mut()
                    .unwrap()
                    .pop()
                    .unwrap();
                plan.manifest = canonical_json(&manifest).unwrap();
                (
                    PortableErrorCode::Incompatible,
                    "ontology composition dependency closure",
                    None,
                )
            }
            "digest" => (
                PortableErrorCode::DigestMismatch,
                "composition digest",
                Some(PATH),
            ),
            "empty-endpoint" => {
                control["bridge_sets"][0]["source_modules"] = json!([]);
                (
                    PortableErrorCode::Incompatible,
                    "empty bridge endpoint",
                    None,
                )
            }
            "identity" => {
                control["modules"][0]["ontology_id"] = json!("not-an-absolute-identity");
                (
                    PortableErrorCode::Incompatible,
                    "exact ontology identity",
                    None,
                )
            }
            _ => unreachable!(),
        };
        control
            .as_object_mut()
            .unwrap()
            .remove("composition_digest");
        let mut digest = Sha256::new();
        digest.update(b"graphforge-ontology-composition/1\0");
        digest.update(canonical_json(&control).unwrap());
        control["composition_digest"] = json!(if case == "digest" {
            format!("sha256:{}", "0".repeat(64))
        } else {
            format!("sha256:{}", hex(digest.finalize().into()))
        });
        replace_test_control(&mut plan, PATH, canonical_json(&control).unwrap());
        assert_refusal_in_both_forms(&plan, code, detail, entry);
    }
}
