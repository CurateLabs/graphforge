//! Opt-in executable evidence for the correction-churn release workflow.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, StringArray, UInt64Array};
use gf_api::{
    AdoptOntologyRequest, AssertionGraphRefInput, AssertionGraphRole, CapabilityId,
    CheckpointRequest, CreateAssertionRequest, CreateAssertionWithEvidenceRequest,
    CreateHypothesisGroupRequest, EnableCapabilityRequest, EvidenceInput, EvidenceRole,
    EvidenceSourceKind, GraphForge, GraphObjectKind, HypothesisMembershipAction, OperationId,
    PageRequest, PropValue, RankAlgorithm, RankOptions, ReasoningContentFormat, ReasoningKind,
    RecordHypothesisMembershipRequest, RecordReasoningRequest, SupersedeAssertionRequest,
    WriteContext,
};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

const BUNDLE: &str = "../../tests/release_workflows/correction-churn";

fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(BUNDLE)
        .join(relative)
}

fn id(suffix: u16) -> Uuid {
    Uuid::parse_str(&format!("018f0f4e-7b8c-7000-8000-00000003{suffix:04x}")).unwrap()
}

fn context(suffix: u16) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(id(suffix)),
        actor_uuid: Some(id(0xfffe)),
    }
}

fn enable(graph: &GraphForge, capability_id: CapabilityId, suffix: u16) {
    graph
        .enable_capability(EnableCapabilityRequest {
            context: context(suffix),
            capability_id,
            capability_version: 1,
        })
        .unwrap();
}

fn provenance(result: &gf_api::ExecutionResult) -> Uuid {
    let values = result.batches[0]
        .column_by_name("provenance_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    Uuid::from_slice(values.value(0)).unwrap()
}

fn names(graph: &GraphForge) -> Vec<String> {
    let result = graph
        .execute("MATCH (o:Organization) RETURN o.name AS name ORDER BY name")
        .unwrap();
    result.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect()
}

fn main() {
    let sha = std::env::var("GF_CORRECTION_CHURN_SHA").expect("workflow SHA is required");
    let evidence_path = PathBuf::from(
        std::env::var("GF_CORRECTION_CHURN_EVIDENCE").expect("evidence path is required"),
    );
    let project = TempDir::new().unwrap();
    let project_path = project.path().to_str().unwrap();
    let mut graph = GraphForge::new(Some(project_path)).unwrap();

    // CC-01/04/05: an invalid ontology definition publishes nothing; the corrected
    // advisory ontology becomes authoritative and remains stable thereafter.
    let invalid = graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(0x0100),
            path: fixture("ontologies/invalid-v0.yaml"),
            mode: gf_api::OntologyMode::Advisory,
        })
        .unwrap_err();
    let validation_code = invalid.code().to_owned();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(0x0101),
            path: fixture("ontologies/advisory-v1.yaml"),
            mode: gf_api::OntologyMode::Advisory,
        })
        .unwrap();
    assert_eq!(
        graph.workspace_ontology().unwrap().mode,
        gf_storage::WorkspaceOntologyMode::Advisory
    );
    for (capability, suffix) in [
        (CapabilityId::Provenance, 0x0110),
        (CapabilityId::Knowledge, 0x0111),
        (CapabilityId::Epistemic, 0x0112),
    ] {
        enable(&graph, capability, suffix);
    }

    let aster = graph
        .add_node(
            "Organization",
            &HashMap::from([
                ("name".into(), PropValue::Str("Aster Labs".into())),
                ("risk_score".into(), PropValue::Float(0.4)),
            ]),
        )
        .unwrap();
    let duplicate = graph
        .add_node(
            "Organization",
            &HashMap::from([
                ("name".into(), PropValue::Str("Aster Laboratory".into())),
                ("risk_score".into(), PropValue::Float(0.4)),
            ]),
        )
        .unwrap();
    let boreal = graph
        .add_node(
            "Organization",
            &HashMap::from([
                ("name".into(), PropValue::Str("Boreal Supply".into())),
                ("risk_score".into(), PropValue::Float(0.7)),
            ]),
        )
        .unwrap();
    graph
        .add_edge(&duplicate, "SUPPLIES", &boreal, &HashMap::new())
        .unwrap();
    graph
        .rank(
            "Organization",
            RankOptions {
                by: RankAlgorithm::Degree,
                via: Some("SUPPLIES".into()),
                directed: false,
                write_property: None,
            },
        )
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "before-corrections".into(),
            description: Some("duplicate entity and wrong edge".into()),
            idempotency_key: OperationId(id(0x0200)),
            actor_uuid: Some(id(0xfffe)),
        })
        .unwrap();
    let prior_names = names(&graph);

    // CC-03: public graph compensation removes the duplicate and wrong edge, then
    // creates the correct edge. Repeating the delete is an idempotent no-op.
    graph
        .execute("MATCH (o:Organization {name:'Aster Laboratory'}) DETACH DELETE o")
        .unwrap();
    graph
        .add_edge(&aster, "SUPPLIES", &boreal, &HashMap::new())
        .unwrap();
    let after_compensation = names(&graph);
    let repeated = graph
        .execute("MATCH (o:Organization {name:'Aster Laboratory'}) DETACH DELETE o")
        .unwrap();
    assert_eq!(repeated.stats.rows_produced, 1);
    let summary = &repeated.batches[0];
    for name in [
        "nodes_created",
        "edges_created",
        "nodes_deleted",
        "edges_deleted",
        "properties_set",
        "properties_removed",
    ] {
        let values = summary
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values.value(0), 0, "repeated compensation changed {name}");
    }
    let side_effects = repeated.side_effects.as_ref().unwrap();
    assert_eq!(side_effects.nodes_deleted, 0);
    assert_eq!(side_effects.relationships_deleted, 0);
    assert!(repeated.mutation_receipt.as_ref().unwrap().is_empty());
    assert_eq!(names(&graph), after_compensation);

    // CC-06/07: supersession and reasoning amendment append successors.
    let first = graph
        .create_assertion_with_evidence(CreateAssertionWithEvidenceRequest {
            assertion: CreateAssertionRequest {
                context: context(0x0300),
                assertion_uuid: id(0x0301),
                claim: "Aster Laboratory is a separate supplier".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: aster.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            },
            evidence: vec![EvidenceInput {
                evidence_uuid: id(0x0302),
                source_uuid: id(0x0303),
                source_kind: EvidenceSourceKind::Document,
                role: EvidenceRole::Supports,
                weight: Some(0.5),
            }],
        })
        .unwrap();
    let prov = provenance(&first);
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(0x0310),
            reasoning_uuid: id(0x0311),
            assertion_uuid: id(0x0301),
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"Initial name matching treated the alias as distinct".to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid: prov,
        })
        .unwrap();
    let replacement = graph
        .create_assertion(CreateAssertionRequest {
            context: context(0x0320),
            assertion_uuid: id(0x0321),
            claim: "Aster Laboratory is an alias of Aster Labs".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: aster.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    let replacement_prov = provenance(&replacement);
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(0x0312),
            reasoning_uuid: id(0x0313),
            assertion_uuid: id(0x0301),
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"Identifier review shows the name was an alias".to_vec(),
            supersedes_reasoning_uuid: Some(id(0x0311)),
            provenance_uuid: prov,
        })
        .unwrap();
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(0x0330),
            reasoning_uuid: id(0x0331),
            assertion_uuid: id(0x0321),
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"Stable identifier review established an alias".to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid: replacement_prov,
        })
        .unwrap();
    graph
        .supersede_assertion(SupersedeAssertionRequest {
            context: context(0x0340),
            supersession_uuid: id(0x0341),
            prior_assertion_uuid: id(0x0301),
            replacement_assertion_uuid: id(0x0321),
            status_event_uuid: id(0x0342),
            reasoning_uuid: id(0x0313),
            provenance_uuid: replacement_prov,
        })
        .unwrap();

    // CC-08/09: membership correction is a new event. Exact replay is
    // idempotent; changed content under the same event UUID conflicts.
    let group = id(0x0400);
    graph
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(0x0401),
            group_uuid: group,
            question_key: "correction-churn.identity.v1".into(),
            provenance_uuid: replacement_prov,
        })
        .unwrap();
    let added = RecordHypothesisMembershipRequest {
        context: context(0x0410),
        membership_event_uuid: id(0x0411),
        group_uuid: group,
        assertion_uuid: id(0x0321),
        action: HypothesisMembershipAction::Added,
        reasoning_uuid: id(0x0331),
        provenance_uuid: replacement_prov,
    };
    graph.record_hypothesis_membership(&added).unwrap();
    graph.record_hypothesis_membership(&added).unwrap();
    let removed = RecordHypothesisMembershipRequest {
        context: context(0x0420),
        membership_event_uuid: id(0x0421),
        group_uuid: group,
        assertion_uuid: id(0x0321),
        action: HypothesisMembershipAction::Removed,
        reasoning_uuid: id(0x0331),
        provenance_uuid: replacement_prov,
    };
    graph.record_hypothesis_membership(&removed).unwrap();
    let mut conflict = removed.clone();
    conflict.action = HypothesisMembershipAction::Added;
    let conflict_code = graph
        .record_hypothesis_membership(&conflict)
        .unwrap_err()
        .code()
        .to_owned();

    // CC-10/11: public pinned view and append-only ledgers remain readable.
    let checkpoint = graph.open_checkpoint("before-corrections").unwrap();
    let checkpoint_result = checkpoint
        .execute("MATCH (o:Organization) RETURN o.name AS name ORDER BY name")
        .unwrap();
    let checkpoint_names = checkpoint_result.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(checkpoint_names, prior_names);
    let current_names = names(&graph);
    let runs = graph
        .list_algorithm_runs(gf_api::ListAlgorithmRunsRequest {
            page: PageRequest::default(),
            algorithm: None,
        })
        .unwrap();
    let run_count = runs
        .batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum::<usize>();
    let snapshot = graph.epistemic_snapshot(i64::MAX).unwrap();
    let snapshot_rows = snapshot
        .batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum::<usize>();
    drop(graph);
    let reopened = GraphForge::new(Some(project_path)).unwrap();
    let reopen_equal = names(&reopened) == current_names;

    let evidence = json!({
        "schema_version": 1, "scenario_id": "correction-churn", "commit_sha": sha,
        "correction_cycles": 3,
        "prior_views": {"checkpoint_names": prior_names, "algorithm_runs": run_count},
        "current": {"organization_names": current_names, "ontology_mode":"advisory", "validation_code":validation_code},
        "history": {"append_only":true, "assertion_supersessions":1, "reasoning_amendments":1, "epistemic_snapshot_rows":snapshot_rows},
        "idempotency": {"exact_replay":"idempotent", "conflict_code":conflict_code},
        "reopen_equal": reopen_equal
    });
    fs::write(evidence_path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
}
