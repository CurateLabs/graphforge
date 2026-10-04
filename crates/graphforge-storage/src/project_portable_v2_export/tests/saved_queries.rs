use super::*;
use crate::{SavedQuery, SavedQueryParameterType, WorkspaceSavedQueries};
use serde_json::{Value, json};

fn queries() -> WorkspaceSavedQueries {
    let query = SavedQuery {
        query_uuid: Uuid::new_v4(),
        name: "Unpublished analysis".into(),
        description: None,
        query: "MATCH (n:UnpublishedLabel) WHERE n.name = $name RETURN n.name".into(),
        parameters: BTreeMap::from([("name".into(), SavedQueryParameterType::String)]),
    };
    WorkspaceSavedQueries {
        queries: BTreeMap::from([(query.query_uuid, query)]),
        ..WorkspaceSavedQueries::default()
    }
}

#[test]
fn saved_query_definitions_round_trip_complete_and_require_explicit_selective_selection() {
    let definitions = queries();
    let (_project, generation) =
        graph_generation_with_metadata(false, Some(definitions.to_project_participant().unwrap()));
    let limits = PortableV2Limits::default();
    for (profile, includes) in [
        (PortableV2SelectionProfile::Complete, true),
        (PortableV2SelectionProfile::DataComponents, false),
        (PortableV2SelectionProfile::OntologyOnly, false),
        (
            PortableV2SelectionProfile::Custom(vec![crate::PortableV2ParticipantId {
                capability_id: crate::WORKSPACE_CAPABILITY_ID.into(),
                record_family_id: crate::WORKSPACE_SAVED_QUERIES_FAMILY.into(),
            }]),
            true,
        ),
    ] {
        let selection = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile,
                strict: false,
            },
            limits,
        )
        .unwrap();
        assert_eq!(
            selection.includes(
                crate::WORKSPACE_CAPABILITY_ID,
                crate::WORKSPACE_SAVED_QUERIES_FAMILY
            ),
            includes
        );
        if includes {
            assert!(
                selection
                    .included
                    .iter()
                    .any(|entry| entry.identity.record_family_id
                        == crate::WORKSPACE_SAVED_QUERIES_FAMILY
                        && entry.kind == "settings")
            );
        }
    }
    let plan = plan_complete_portable_v2(&generation, limits).unwrap();
    let outputs = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(&plan, outputs.path());
    for source in [&expanded, &bundle] {
        verify_portable_v2(source, PortableV2Mode::Full, limits, None).unwrap();
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("imported");
        crate::import_complete_portable_v2(
            source,
            &target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &[
                crate::ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            limits,
            None,
        )
        .unwrap();
        let reopened = crate::resolve_project_generation(&target).unwrap();
        assert_eq!(
            crate::read_workspace_saved_queries(&reopened).unwrap(),
            definitions
        );
    }
}

#[test]
fn malformed_native_definition_is_refused_by_selected_export_but_can_be_omitted() {
    let mut participant = queries().to_project_participant().unwrap();
    let mut record: Value = serde_json::from_slice(&participant.bytes).unwrap();
    let definition = record["queries"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .unwrap();
    definition["query"] = json!("CREATE (n)");
    definition["parameters"] = json!({});
    participant.bytes = serde_json::to_vec(&record).unwrap();
    participant.bytes.push(b'\n');
    let (_root, generation) = graph_generation_with_metadata(false, Some(participant));
    assert!(plan_complete_portable_v2(&generation, PortableV2Limits::default()).is_err());
    let selection = preview_portable_v2_selection(
        &generation,
        &PortableV2SelectionRequest {
            profile: PortableV2SelectionProfile::DataComponents,
            strict: false,
        },
        PortableV2Limits::default(),
    )
    .unwrap();
    assert!(
        plan_selected_portable_v2(&generation, &selection, PortableV2Limits::default()).is_ok()
    );
}

#[test]
fn authenticated_packages_refuse_invalid_saved_queries_before_destination_admission() {
    let (_project, generation) =
        graph_generation_with_metadata(false, Some(queries().to_project_participant().unwrap()));
    let original = plan_complete_portable_v2(&generation, PortableV2Limits::default()).unwrap();
    let manifest: Value = serde_json::from_slice(&original.manifest).unwrap();
    let path = manifest["components"]
        .as_array()
        .unwrap()
        .iter()
        .find(|component| {
            component["participant_id"]
                .as_str()
                .unwrap()
                .starts_with("workspace-saved_queries-")
        })
        .unwrap()["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let native_bytes = crate::read_workspace_saved_queries(&generation)
        .unwrap()
        .to_canonical_json()
        .unwrap();
    for case in [
        "mutation",
        "call",
        "missing",
        "extra",
        "future",
        "values",
        "header-version",
        "header-count",
        "header-schema",
    ] {
        let mut plan = original.clone();
        if case.starts_with("header") {
            let runtime_path = crate::project_portable_v2::RUNTIME_MAP_PATH;
            let runtime_file = plan
                .files
                .iter()
                .find(|file| file.path == runtime_path)
                .unwrap();
            let PlannedSource::Control(bytes) = &runtime_file.source else {
                panic!("runtime map must be control");
            };
            let mut runtime: Value = serde_json::from_slice(bytes).unwrap();
            let participant = runtime["participants"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|participant| {
                    participant["record_family_id"] == crate::WORKSPACE_SAVED_QUERIES_FAMILY
                })
                .unwrap();
            match case {
                "header-version" => participant["record_version"] = 2.into(),
                "header-count" => participant["row_count"] = 2.into(),
                _ => participant["schema_fingerprint"] = "a".repeat(64).into(),
            }
            replace_test_control(&mut plan, runtime_path, canonical_json(&runtime).unwrap());
        } else {
            let mut record: Value = serde_json::from_slice(&native_bytes).unwrap();
            if case == "future" {
                record["contract_version"] = 2.into();
            } else {
                let definition = record["queries"]
                    .as_object_mut()
                    .unwrap()
                    .values_mut()
                    .next()
                    .unwrap();
                match case {
                    "mutation" => {
                        definition["query"] = json!("CREATE (n)");
                        definition["parameters"] = json!({});
                    }
                    "call" => {
                        definition["query"] = json!("CALL unsafe.procedure() RETURN 1");
                        definition["parameters"] = json!({});
                    }
                    "missing" => definition["parameters"] = json!({}),
                    "extra" => definition["parameters"]["unused"] = json!("integer"),
                    "values" => definition["parameter_values"] = json!({"name": "private"}),
                    _ => unreachable!(),
                }
            }
            let mut bytes = serde_json::to_vec(&record).unwrap();
            bytes.push(b'\n');
            replace_test_control(&mut plan, &path, bytes);
        }
        let outputs = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&plan, outputs.path());
        for source in [&expanded, &bundle] {
            assert!(
                verify_portable_v2(
                    source,
                    PortableV2Mode::Full,
                    PortableV2Limits::default(),
                    None
                )
                .is_err(),
                "{case}"
            );
            let root = tempfile::tempdir().unwrap();
            let target = root.path().join("refused");
            assert!(
                crate::import_complete_portable_v2(
                    source,
                    &target,
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    &[
                        crate::ProjectCapability {
                            capability_id: "graph".into(),
                            capability_version: 1
                        },
                        crate::ProjectCapability {
                            capability_id: "workspace".into(),
                            capability_version: 1
                        },
                    ],
                    PortableV2Limits::default(),
                    None
                )
                .is_err(),
                "{case}"
            );
            assert!(!target.exists(), "{case} must not admit a destination");
        }
    }
}
