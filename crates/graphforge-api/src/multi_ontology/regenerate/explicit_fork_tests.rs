//! Internal history and cancellation invariants of the shared native engine.
use super::*;
use crate::{BranchSource, CreateResearchBranchRequest, ModuleAdoptionRequest};

const OLD: &str = "ont_aaaaaaaaaaaaaaaa";
fn project() -> (tempfile::TempDir, GraphForge, Uuid, Uuid) {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    let candidate = graph.create_ontology_module(serde_json::from_value(serde_json::json!({
        "ontology_id":"https://graphforge.sh/ont_aaaaaaaaaaaaaaaa","version":"v1",
        "entity_types":[{"name":"Thing"}],"properties":[{"owner":"Thing","name":"value","type":"utf8"}]
    })).unwrap(), vec![], None).unwrap();
    let state = graph.ontology_authority_state().unwrap();
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: OntologyAuthorityExpectation {
                    context: WriteContext {
                        operation_uuid: OperationId(Uuid::now_v7()),
                        actor_uuid: None,
                    },
                    expected_project_generation_uuid: state.project_generation_uuid,
                    expected_composition_fingerprint: state.composition_fingerprint,
                },
                candidate,
            },
            None,
        )
        .unwrap();
    graph.execute("CREATE (:Thing {value:'retained'})").unwrap();
    let branch = Uuid::now_v7();
    let base = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph
                    .committed_generation_identity()
                    .unwrap()
                    .generation_uuid,
                branch_uuid: branch,
                version_uuid: base,
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "main".into(),
                description: None,
                author: None,
                committer: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    (root, graph, branch, base)
}

#[test]
fn explicit_fork_preserves_field_history_and_historical_authority_is_read_only() {
    let (_root, mut graph, branch, base) = project();
    let version = graph.research_version(base).unwrap();
    let mut historical = crate::research_versions::materialize_version(&graph, &version).unwrap();
    let before = historical.committed_generation_identity().unwrap();
    assert!(
        historical
            .fork_ontology_id(OLD, Uuid::now_v7(), None)
            .is_err()
    );
    assert_eq!(historical.committed_generation_identity().unwrap(), before);
    let fields =
        crate::branches::baseline::read(graph.open_research_branch(branch).unwrap().graph())
            .unwrap();
    assert!(!fields.is_empty());
    let operation = Uuid::now_v7();
    let result = graph.fork_ontology_id(OLD, operation, None).unwrap();
    let view = graph.open_research_branch(branch).unwrap();
    let rewritten = crate::branches::baseline::read(view.graph()).unwrap();
    assert_eq!(fields.len(), rewritten.len());
    for original in fields.values() {
        let next = rewritten
            .values()
            .find(|row| row.contribution == original.contribution)
            .unwrap();
        assert_eq!(next.origin, original.origin);
        assert_eq!(next.incorporated, original.incorporated);
        assert_eq!(next.original, original.original);
        assert_eq!(next.baseline, original.baseline);
    }
    assert_eq!(graph.research_version(base).unwrap(), version);
    assert_eq!(
        graph.research_version(result.version_uuid).unwrap().parents,
        vec![base]
    );
    let first = graph
        .workspace_ontology_composition()
        .unwrap()
        .unwrap()
        .modules[0]
        .id
        .clone();
    let second = graph
        .fork_ontology_id(&result.id, Uuid::now_v7(), None)
        .unwrap();
    let composition = graph.workspace_ontology_composition().unwrap().unwrap();
    assert_eq!(
        composition.modules[0].document.forked_from,
        Some((&first).into())
    );
    assert_ne!(second.id, result.id);
    // The immediate source digest commits the earlier fork chain, while the
    // first operation remains replayable without reinstalling its content.
    let after = graph.committed_generation_identity().unwrap();
    assert_eq!(
        graph.fork_ontology_id(OLD, operation, None).unwrap(),
        result
    );
    assert_eq!(graph.committed_generation_identity().unwrap(), after);
}

#[test]
fn explicit_cancellation_helper() {
    let Ok(path) = std::env::var("GF_EXPLICIT_FORK_CANCEL_ROOT") else {
        return;
    };
    let operation = Uuid::parse_str(&std::env::var("GF_ITEM_ID_OPERATION").unwrap()).unwrap();
    let mut graph = GraphForge::new(Some(&path)).unwrap();
    let result = graph.fork_ontology_id(OLD, operation, None);
    match std::env::var("GF_ITEM_ID_FAULT").unwrap().as_str() {
        "cancel_after" => {
            result.unwrap();
        }
        "private_ack_crash" => unreachable!("process exits before acknowledgement"),
        _ => {
            assert!(result.is_err());
        }
    }
}
#[test]
fn explicit_cancellation_and_unacknowledged_native_success_replay_without_refusals() {
    for phase in [
        "cancel_before",
        "cancel_after",
        "private_ack_error",
        "private_ack_crash",
        "race",
    ] {
        let (_root, graph, _, _) = project();
        let path = graph.path().unwrap().to_path_buf();
        let before = graph
            .committed_generation_identity()
            .unwrap()
            .generation_uuid;
        let operation = Uuid::now_v7();
        drop(graph);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "multi_ontology::regenerate::explicit_fork_tests::explicit_cancellation_helper",
                "--nocapture",
            ])
            .env("GF_EXPLICIT_FORK_CANCEL_ROOT", &path)
            .env("GF_ITEM_ID_OPERATION", operation.to_string())
            .env("GF_ITEM_ID_FAULT", phase)
            .status()
            .unwrap();
        assert_eq!(status.success(), phase != "private_ack_crash", "{phase}");
        let document: serde_json::Value = serde_json::from_slice(
            &std::fs::read(path.join(".publish-refusals/id-taken.json")).unwrap(),
        )
        .unwrap();
        let expected: ItemIdRegeneration = serde_json::from_value(
            document["regenerations"][operation.to_string()]["replacement"].clone(),
        )
        .unwrap();
        let mut graph = GraphForge::new(path.to_str()).unwrap();
        let registry = graph.research_version_retention().unwrap();
        let committed = matches!(
            phase,
            "cancel_after" | "private_ack_error" | "private_ack_crash"
        );
        assert_eq!(registry.receipts.contains_key(&operation), committed);
        if phase == "race" {
            assert!(
                graph
                    .fork_ontology_id(OLD, operation, None)
                    .unwrap_err()
                    .to_string()
                    .contains("cannot be rebased")
            );
        } else {
            if !committed {
                assert_eq!(
                    graph
                        .committed_generation_identity()
                        .unwrap()
                        .generation_uuid,
                    before
                );
            }
            assert_eq!(
                graph.fork_ontology_id(OLD, operation, None).unwrap(),
                expected
            );
            let generation = graph.committed_generation_identity().unwrap();
            assert_eq!(
                graph.fork_ontology_id(OLD, operation, None).unwrap(),
                expected
            );
            assert_eq!(graph.committed_generation_identity().unwrap(), generation);
        }
        assert!(graph.id_taken_receipt(OLD).unwrap().is_none());
    }
}
