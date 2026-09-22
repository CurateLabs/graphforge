//! Real facade evidence for immutable research, restoration and retained closure.
use arrow::array::{BinaryArray, Int64Array};
use graphforge_api::*;
use std::collections::BTreeSet;
use uuid::Uuid;

fn context() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}
fn capture(graph: &mut GraphForge, owner: Uuid) -> ResearchOperation {
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            context_uuid: owner,
            label: Some("Frozen research".into()),
            description: Some("Original citation".into()),
            created_at: 1,
            required_versions: BTreeSet::new(),
        })
        .unwrap();
    graph
        .commit_research_version_operation(operation.clone(), &CancellationToken::new())
        .unwrap();
    operation
}
fn mutation(graph: &mut GraphForge, mutation: ResearchMutation) -> ResearchOperationReceipt {
    let operation = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        mutation,
    };
    graph
        .commit_research_version_operation(operation, &CancellationToken::new())
        .unwrap()
}
fn count(result: ExecutionResult) -> i64 {
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
fn version_id(operation: &ResearchOperation) -> Uuid {
    match &operation.mutation {
        ResearchMutation::Register(spec) => spec.version_uuid,
        _ => panic!("capture"),
    }
}

#[test]
fn historical_graph_ontology_and_artifact_survive_cleanup_restore_and_replay() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Person)").unwrap();
    let ontology = directory.path().join("ontology.yaml");
    std::fs::write(&ontology, "ontology_id: historical\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types: []\n").unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(),
            path: ontology,
            mode: OntologyMode::Advisory,
        })
        .unwrap();
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let source = Uuid::now_v7();
    let artifact = Uuid::now_v7();
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid: source,
            label: "Original source".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            artifact_uuid: artifact,
            source_uuid: source,
            artifact_kind: ArtifactKind::RawScan,
            media_type: "application/octet-stream".into(),
            payload: ArtifactPayloadRequest::LocalBytes(b"historical bytes".to_vec()),
            derivation_inputs: vec![],
            run_uuid: None,
        })
        .unwrap();
    let frozen_ontology = graph.workspace_ontology().unwrap();
    let owner = Uuid::now_v7();
    let operation = capture(&mut graph, owner);
    let version = version_id(&operation);
    let frozen_record = graph.research_version(version).unwrap();
    assert_eq!(
        graph.list_research_versions().unwrap().batches[0].num_rows(),
        1
    );
    graph
        .checkpoint(CheckpointRequest {
            name: "independent".into(),
            description: None,
            idempotency_key: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        })
        .unwrap();
    graph.execute("CREATE (:Person)").unwrap();
    let other = capture(&mut graph, Uuid::now_v7());
    graph
        .delete_checkpoint(DeleteCheckpointRequest {
            name: "independent".into(),
            idempotency_key: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        })
        .unwrap();
    mutation(
        &mut graph,
        ResearchMutation::Compact {
            versions: BTreeSet::from([version, version_id(&other)]),
        },
    );
    drop(graph);
    graphforge_storage::execute_project_cleanup(
        &root,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        Default::default(),
    )
    .unwrap();
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    assert!(
        graphforge_storage::resolve_generation_by_uuid(
            &root,
            frozen_record.content.generation_uuid
        )
        .is_err(),
        "compacted source generation must actually be reclaimed"
    );
    let historical = graph.open_research_version(version).unwrap();
    assert_eq!(historical.version(), &frozen_record);
    assert_eq!(historical.workspace_ontology().unwrap(), frozen_ontology);
    historical.research_project_metadata().unwrap();
    assert_eq!(
        historical.artifact(artifact).unwrap().batches[0].num_rows(),
        1
    );
    assert_eq!(
        count(
            historical
                .execute("MATCH (n:Person) RETURN count(n)")
                .unwrap()
        ),
        1
    );
    assert!(historical.execute("CREATE (:Person)").is_err());
    let payload = historical.artifact_payload(artifact).unwrap();
    assert_eq!(
        payload.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"historical bytes"
    );
    let before = graph.research_version_retention().unwrap();
    let restore = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        mutation: ResearchMutation::RestoreProject {
            context_uuid: owner,
            source_version: version,
            version_uuid: Uuid::now_v7(),
            created_at: 2,
        },
    };
    let restored = graph
        .commit_research_version_operation(restore.clone(), &CancellationToken::new())
        .unwrap();
    assert_ne!(restored.version_uuid, Some(version));
    assert_eq!(
        count(graph.execute("MATCH (n:Person) RETURN count(n)").unwrap()),
        1
    );
    assert_eq!(graph.workspace_ontology().unwrap(), frozen_ontology);
    for (id, receipt) in before.receipts {
        assert_eq!(
            graph.research_version_retention().unwrap().receipts[&id],
            receipt
        );
    }
    for (id, head) in before.heads {
        if id != owner {
            assert_eq!(graph.research_version_retention().unwrap().heads[&id], head);
        }
    }
    graph.execute("CREATE (:Person)").unwrap();
    assert_eq!(
        graph
            .commit_research_version_operation(restore, &CancellationToken::new())
            .unwrap(),
        restored
    );
    assert_eq!(
        count(graph.execute("MATCH (n:Person) RETURN count(n)").unwrap()),
        2
    );
    graph
        .commit_research_version_operation(operation, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        count(graph.execute("MATCH (n:Person) RETURN count(n)").unwrap()),
        2
    );
    drop(graph);
    let reopened = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        count(
            reopened
                .execute("MATCH (n:Person) RETURN count(n)")
                .unwrap()
        ),
        2
    );
    assert_eq!(reopened.research_version(version).unwrap(), frozen_record);
}

#[test]
fn in_memory_capture_empty_graph_and_cancelled_commit_are_real_native_operations() {
    let mut graph = GraphForge::new(None).unwrap();
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 1,
            required_versions: BTreeSet::new(),
        })
        .unwrap();
    graph
        .commit_research_version_operation(operation.clone(), &CancellationToken::new())
        .unwrap();
    assert_eq!(
        count(
            graph
                .open_research_version(version_id(&operation))
                .unwrap()
                .execute("MATCH (n) RETURN count(n)")
                .unwrap()
        ),
        0
    );
    let mut conflict = operation.clone();
    if let ResearchMutation::Register(spec) = &mut conflict.mutation {
        spec.label = Some("changed".into());
    }
    assert!(
        graph
            .commit_research_version_operation(conflict, &CancellationToken::new())
            .is_err()
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(
        graph
            .commit_research_version_operation(operation, &cancellation)
            .is_err()
    );
}

#[test]
fn restore_initial_snapshot_removes_later_file_backed_graph() {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().to_str()).unwrap();
    let owner = Uuid::now_v7();
    let capture = capture(&mut graph, owner);
    graph.execute("CREATE (:Person)").unwrap();
    mutation(
        &mut graph,
        ResearchMutation::RestoreProject {
            context_uuid: owner,
            source_version: version_id(&capture),
            version_uuid: Uuid::now_v7(),
            created_at: 2,
        },
    );
    assert_eq!(
        count(graph.execute("MATCH (n) RETURN count(n)").unwrap()),
        0
    );
    drop(graph);
    let reopened = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(
        count(reopened.execute("MATCH (n) RETURN count(n)").unwrap()),
        0
    );
}

#[test]
fn restoration_fault_child_keeps_one_authoritative_facade() {
    let Ok(root) = std::env::var("GF_RESEARCH_FACADE_FAULT_ROOT") else {
        return;
    };
    let operation: ResearchOperation =
        serde_json::from_str(&std::env::var("GF_RESEARCH_FACADE_FAULT_REQUEST").unwrap()).unwrap();
    let mut graph = GraphForge::new(Some(&root)).unwrap();
    let error = graph
        .commit_research_version_operation(operation.clone(), &CancellationToken::new())
        .unwrap_err();
    let committed = std::env::var("GF_RESEARCH_FACADE_FAULT_COMMITTED").unwrap() == "true";
    assert!(
        error
            .to_string()
            .contains(&format!("committed={committed}")),
        "{error}"
    );
    assert_eq!(
        count(graph.execute("MATCH (n) RETURN count(n)").unwrap()),
        if committed { 1 } else { 2 }
    );
    if committed {
        graph
            .commit_research_version_operation(operation, &CancellationToken::new())
            .unwrap();
        assert_eq!(
            count(graph.execute("MATCH (n) RETURN count(n)").unwrap()),
            1
        );
    }
    graph.execute("CREATE (:Person)").unwrap();
    assert_eq!(
        count(graph.execute("MATCH (n) RETURN count(n)").unwrap()),
        if committed { 2 } else { 3 }
    );
}

#[test]
fn restoration_errors_before_and_after_current_preserve_same_facade_and_retry() {
    for committed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.execute("CREATE (:Person)").unwrap();
        let owner = Uuid::now_v7();
        let capture = capture(&mut graph, owner);
        graph.execute("CREATE (:Person)").unwrap();
        let operation = ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: graph
                .research_project_summary()
                .unwrap()
                .identity
                .generation_uuid,
            mutation: ResearchMutation::RestoreProject {
                context_uuid: owner,
                source_version: version_id(&capture),
                version_uuid: Uuid::now_v7(),
                created_at: 2,
            },
        };
        drop(graph);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "restoration_fault_child_keeps_one_authoritative_facade",
                "--nocapture",
            ])
            .env("GF_RESEARCH_FACADE_FAULT_ROOT", root.path())
            .env(
                "GF_RESEARCH_FACADE_FAULT_REQUEST",
                serde_json::to_string(&operation).unwrap(),
            )
            .env("GF_RESEARCH_FACADE_FAULT_COMMITTED", committed.to_string())
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                operation.operation_uuid.to_string(),
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                if committed {
                    "project.after_current_replace.error"
                } else {
                    "project.before_current_replace.error"
                },
            )
            .status()
            .unwrap();
        assert!(status.success());
    }
}

#[test]
fn exact_restore_retry_on_stale_facade_refreshes_graph_authority() {
    let root = tempfile::tempdir().unwrap();
    let mut first = GraphForge::new(root.path().to_str()).unwrap();
    first.execute("CREATE (:Person)").unwrap();
    let owner = Uuid::now_v7();
    let capture = capture(&mut first, owner);
    first.execute("CREATE (:Person)").unwrap();
    let mut stale = GraphForge::new(root.path().to_str()).unwrap();
    let operation = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: first
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        mutation: ResearchMutation::RestoreProject {
            context_uuid: owner,
            source_version: version_id(&capture),
            version_uuid: Uuid::now_v7(),
            created_at: 2,
        },
    };
    let receipt = first
        .commit_research_version_operation(operation.clone(), &CancellationToken::new())
        .unwrap();
    assert_eq!(
        stale
            .commit_research_version_operation(operation, &CancellationToken::new())
            .unwrap(),
        receipt
    );
    assert_eq!(
        count(stale.execute("MATCH (n) RETURN count(n)").unwrap()),
        1
    );
    stale.execute("CREATE (:Person)").unwrap();
    assert_eq!(
        count(stale.execute("MATCH (n) RETURN count(n)").unwrap()),
        2
    );
}

#[test]
fn released_payload_preserves_exact_replay_and_required_root_blocks_deletion() {
    let mut graph = GraphForge::new(None).unwrap();
    let owner = Uuid::now_v7();
    let first = capture(&mut graph, owner);
    let version = version_id(&first);
    let root = Uuid::now_v7();
    mutation(
        &mut graph,
        ResearchMutation::RetainRoot {
            root: ResearchRetentionRoot {
                root_uuid: root,
                kind: ResearchRootKind::RetainedVersion,
                versions: BTreeSet::from([version]),
            },
        },
    );
    let second = capture(&mut graph, owner);
    let deletion = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        mutation: ResearchMutation::DeleteVersion {
            version_uuid: version,
        },
    };
    assert!(
        graph
            .commit_research_version_operation(deletion, &CancellationToken::new())
            .unwrap_err()
            .to_string()
            .contains("blocked")
    );
    mutation(
        &mut graph,
        ResearchMutation::ReleaseRoot { root_uuid: root },
    );
    mutation(
        &mut graph,
        ResearchMutation::DeleteVersion {
            version_uuid: version,
        },
    );
    assert!(graph.research_version(version).is_err());
    graph
        .commit_research_version_operation(first, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        graph.research_version_retention().unwrap().heads[&owner],
        version_id(&second)
    );
}

#[test]
fn storage_projection_fixture_refuses_unretained_graph_and_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Person)").unwrap();
    let owner = Uuid::now_v7();
    let original = capture(&mut graph, owner);
    let original_id = version_id(&original);
    let ResearchMutation::Register(mut spec) = original.mutation else {
        unreachable!()
    };
    spec.version_uuid = Uuid::now_v7();
    spec.context_uuid = Uuid::now_v7();
    spec.source_version = Some(original_id);
    spec.selection = Some(
        graph
            .research_version(original_id)
            .unwrap()
            .content
            .participants
            .into_iter()
            .filter(|p| p.key.capability == "workspace" && p.key.family != "research_metadata")
            .map(|p| p.key)
            .collect(),
    );
    let projection_id = spec.version_uuid;
    let operation = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        mutation: ResearchMutation::Register(spec),
    };
    // Future domain owners select projections; the public facade cannot accept raw closure claims.
    assert!(
        graph
            .commit_research_version_operation(operation.clone(), &CancellationToken::new())
            .is_err()
    );
    graphforge_storage::research_versions::publish_research_operation(
        &root,
        &operation,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    let view = graph.open_research_version(projection_id).unwrap();
    for error in [
        view.execute("MATCH (n) RETURN count(n)").unwrap_err(),
        view.research_project_metadata().unwrap_err(),
    ] {
        assert!(matches!(
            error,
            GfError::Api {
                code: ApiErrorCode::ResultNotRetained,
                ..
            }
        ));
    }
}

#[test]
fn registered_public_contract_matches_native_schema_and_storage_limits() {
    use graphforge_storage::research_versions::{
        MAX_CONTEXTS, MAX_RECEIPTS, MAX_REGISTRY_BYTES, MAX_ROOTS, MAX_VERSIONS,
    };
    let contract: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/contracts/research-version-api-v1.json"
    ))
    .unwrap();
    for (name, actual) in [
        ("retained_versions", MAX_VERSIONS),
        ("receipts_and_identities", MAX_RECEIPTS),
        ("contexts", MAX_CONTEXTS),
        ("roots", MAX_ROOTS),
        ("registry_bytes", MAX_REGISTRY_BYTES),
    ] {
        assert_eq!(contract["limits"][name].as_u64().unwrap(), actual as u64);
    }
    let graph = GraphForge::new(None).unwrap();
    let result = graph.list_research_versions().unwrap();
    let expected = contract["arrow"]["versions"].as_object().unwrap();
    assert_eq!(result.schema.fields().len(), expected.len());
    for field in result.schema.fields() {
        let kind = match field.data_type() {
            arrow::datatypes::DataType::Utf8 => "utf8",
            arrow::datatypes::DataType::Boolean => "bool",
            other => panic!("unregistered type {other:?}"),
        };
        let actual = if field.is_nullable() {
            format!("nullable {kind}")
        } else {
            kind.to_owned()
        };
        assert_eq!(expected[field.name()], actual);
    }
    let invalid = serde_json::json!({"operation_uuid": Uuid::now_v7(), "version_uuid": Uuid::now_v7(), "context_uuid": Uuid::now_v7(), "created_at": 1, "required_versions": [], "unknown": true});
    assert!(serde_json::from_value::<PrepareResearchVersionRequest>(invalid).is_err());
}

#[test]
fn selected_object_root_version_executes_after_ancestor_release_and_cleanup() {
    use graphforge_storage::research_versions::{
        RegisterResearchVersion, ResearchGraphSelection, publish_research_operation,
    };
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Person {name: 'selected'}), (:Person {name: 'outside'})")
        .unwrap();
    let result = graph
        .execute("MATCH (n {name: 'selected'}) RETURN n.node_uuid")
        .unwrap();
    let selected = Uuid::from_slice(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let owner = Uuid::now_v7();
    let original = version_id(&capture(&mut graph, owner));
    let version = graph.research_version(original).unwrap();
    let projection = Uuid::now_v7();
    let operation = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        mutation: ResearchMutation::RegisterGraphProjection {
            spec: RegisterResearchVersion {
                version_uuid: projection,
                context_uuid: owner,
                source_generation_uuid: version.content.generation_uuid,
                selection: Some(
                    version
                        .content
                        .participants
                        .iter()
                        .map(|p| p.key.clone())
                        .collect(),
                ),
                source_version: Some(original),
                required_versions: BTreeSet::new(),
                label: None,
                description: None,
                created_at: 2,
                evidence: vec![],
            },
            selection: ResearchGraphSelection {
                nodes: BTreeSet::from([selected]),
                edges: BTreeSet::new(),
                induced_edges: false,
                exclude_properties: BTreeSet::new(),
            },
        },
    };
    publish_research_operation(
        &root,
        &operation,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    drop(graph);
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    mutation(
        &mut graph,
        ResearchMutation::DeleteVersion {
            version_uuid: original,
        },
    );
    drop(graph);
    graphforge_storage::execute_project_cleanup(
        &root,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        graphforge_storage::ProjectRetentionLimits::default(),
    )
    .unwrap();
    assert!(
        graphforge_storage::resolve_generation_by_uuid(&root, version.content.generation_uuid)
            .is_err()
    );
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        count(
            graph
                .open_research_version(projection)
                .unwrap()
                .execute("MATCH (n) RETURN count(n)")
                .unwrap()
        ),
        1
    );
    assert_eq!(
        count(graph.execute("MATCH (n) RETURN count(n)").unwrap()),
        2
    );
}
