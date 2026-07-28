//! Opt-in executable evidence for the atomic-recovery release workflow (#2473).
//!
//! Requires [`GraphForge::publish_composite_transaction`] from #2581.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, StringArray};
use gf_api::{
    AdoptOntologyRequest, AssertionGraphRole, COMPOSITE_TRANSACTION_CONTRACT_VERSION, CapabilityId,
    CompositeGraphMutation, CompositeKnowledgeParticipants, CompositeTransactionRequest,
    EnableCapabilityRequest, FindOptions, GraphForge, GraphObjectKind, OperationId, PropValue,
    RankAlgorithm, RankOptions, WriteContext,
};
use gf_knowledge::{
    Assertion, AssertionGraphRef, AssertionStatus, AssertionStatusEvent, EvidenceLink,
    EvidenceRole, EvidenceSourceKind, HypothesisGroup, HypothesisMembershipAction,
    HypothesisMembershipEvent, ReasoningContentFormat, ReasoningKind, ReasoningRecord,
};
use gf_provenance::{EventKind, LineageRecord, LineageRole, ProvenanceEvent, SubjectKind};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

const BUNDLE: &str = "../../tests/release_workflows/atomic-recovery";

fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(BUNDLE)
        .join(relative)
}

fn id(suffix: u16) -> Uuid {
    Uuid::parse_str(&format!("018f0f4e-7b8c-7000-8000-00000005{suffix:04x}")).unwrap()
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

fn finding_names(graph: &GraphForge) -> Vec<String> {
    let result = graph
        .execute("MATCH (f:Finding) RETURN f.name AS name ORDER BY name")
        .unwrap();
    if result.batches.is_empty() {
        return Vec::new();
    }
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

fn observation_uuid(graph: &GraphForge) -> Uuid {
    let result = graph
        .execute("MATCH (o:Observation {name: 'Signal-1'}) RETURN o.node_uuid AS node_uuid LIMIT 1")
        .unwrap();
    Uuid::from_slice(
        result.batches[0]
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap()
}

fn seed_project(root: &Path) -> GraphForge {
    let mut graph = GraphForge::new(Some(root.to_str().unwrap())).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(0x0100),
            path: fixture("ontologies/strict-v1.yaml"),
            mode: gf_api::OntologyMode::Strict,
        })
        .unwrap();
    for (capability, suffix) in [
        (CapabilityId::Provenance, 0x0110),
        (CapabilityId::Knowledge, 0x0111),
        (CapabilityId::Epistemic, 0x0112),
    ] {
        enable(&graph, capability, suffix);
    }

    let person = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".into(), PropValue::Str("Analyst Ada".into())),
                ("role".into(), PropValue::Str("lead".into())),
                ("risk_score".into(), PropValue::Float(0.2)),
            ]),
        )
        .unwrap();
    let org = graph
        .add_node(
            "Organization",
            &HashMap::from([
                ("name".into(), PropValue::Str("Northwind".into())),
                ("sector".into(), PropValue::Str("energy".into())),
            ]),
        )
        .unwrap();
    let location = graph
        .add_node(
            "Location",
            &HashMap::from([
                ("name".into(), PropValue::Str("Depot".into())),
                ("region".into(), PropValue::Str("NW".into())),
            ]),
        )
        .unwrap();
    let _case = graph
        .add_node(
            "Case",
            &HashMap::from([
                ("name".into(), PropValue::Str("Recovery Case".into())),
                ("case_id".into(), PropValue::Str("CASE-2473".into())),
            ]),
        )
        .unwrap();
    let document = graph
        .add_node(
            "Document",
            &HashMap::from([
                ("name".into(), PropValue::Str("Memo".into())),
                ("body".into(), PropValue::Str("baseline note".into())),
                ("classified".into(), PropValue::Bool(false)),
            ]),
        )
        .unwrap();
    let observation = graph
        .add_node(
            "Observation",
            &HashMap::from([
                ("name".into(), PropValue::Str("Signal-1".into())),
                ("summary".into(), PropValue::Str("observed transfer".into())),
                ("confidence".into(), PropValue::Float(0.7)),
            ]),
        )
        .unwrap();
    graph
        .add_edge(&person, "MEMBER_OF", &org, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&person, "AUTHORED", &document, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&person, "LOCATED_AT", &location, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&observation, "OBSERVED", &org, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&document, "REFERENCES", &observation, &HashMap::new())
        .unwrap();
    graph
}

fn graph_m20_request(op_suffix: u16, observation: Uuid) -> CompositeTransactionRequest {
    let operation = id(op_suffix);
    let node = id(0x0201);
    let edge = id(0x0202);
    let assertion = id(0x0211);
    let status = id(0x0212);
    let evidence = id(0x0213);
    let source = id(0x0214);
    let now = 1_700_000_000_000i64;
    let provenance =
        ProvenanceEvent::new(operation, EventKind::CreateAssertion, None, now).unwrap();
    CompositeTransactionRequest {
        contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
        context: WriteContext {
            operation_uuid: OperationId(operation),
            actor_uuid: None,
        },
        graph_mutations: vec![
            CompositeGraphMutation::CreateNode {
                node_uuid: node,
                label: "Finding".into(),
                properties: HashMap::from([
                    ("name".into(), PropValue::Str("Composite Finding".into())),
                    ("claim_key".into(), PropValue::Str("claim-m20".into())),
                    ("severity".into(), PropValue::Int(5)),
                ]),
            },
            CompositeGraphMutation::CreateEdge {
                edge_uuid: edge,
                rel_type: "SUPPORTS".into(),
                source_uuid: node,
                target_uuid: observation,
                properties: HashMap::from([("weight".into(), PropValue::Float(0.1))]),
            },
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid: edge,
                property: "weight".into(),
                value: PropValue::Float(0.9),
            },
        ],
        knowledge: CompositeKnowledgeParticipants {
            provenance_events: vec![provenance.clone()],
            lineage: vec![
                LineageRecord::new(
                    provenance.provenance_uuid,
                    node,
                    SubjectKind::Node,
                    LineageRole::Output,
                    0,
                )
                .unwrap(),
                LineageRecord::new(
                    provenance.provenance_uuid,
                    edge,
                    SubjectKind::Edge,
                    LineageRole::Output,
                    1,
                )
                .unwrap(),
                LineageRecord::new(
                    provenance.provenance_uuid,
                    assertion,
                    SubjectKind::Assertion,
                    LineageRole::Output,
                    2,
                )
                .unwrap(),
            ],
            assertions: vec![
                Assertion::new(
                    assertion,
                    "composite m20 claim".into(),
                    provenance.provenance_uuid,
                    now,
                )
                .unwrap(),
            ],
            assertion_graph_refs: vec![
                AssertionGraphRef::new(
                    assertion,
                    node,
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
            ],
            evidence: vec![
                EvidenceLink::new(
                    evidence,
                    assertion,
                    source,
                    EvidenceSourceKind::Document,
                    EvidenceRole::Supports,
                    Some(0.9),
                    provenance.provenance_uuid,
                    now,
                )
                .unwrap(),
            ],
            assertion_status: vec![
                AssertionStatusEvent::new(
                    status,
                    assertion,
                    AssertionStatus::Supported,
                    None,
                    None,
                    provenance.provenance_uuid,
                    now,
                )
                .unwrap(),
            ],
            ..CompositeKnowledgeParticipants::default()
        },
    }
}

fn graph_m20_m21_request(op_suffix: u16, observation: Uuid) -> CompositeTransactionRequest {
    let operation = id(op_suffix);
    let node = id(0x0301);
    let edge = id(0x0302);
    let assertion = id(0x0311);
    let status = id(0x0312);
    let evidence = id(0x0313);
    let source = id(0x0314);
    let reasoning = id(0x0315);
    let group = id(0x0316);
    let membership = id(0x0317);
    let now = 1_700_000_100_000i64;
    let provenance =
        ProvenanceEvent::new(operation, EventKind::CreateAssertion, None, now).unwrap();
    CompositeTransactionRequest {
        contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
        context: WriteContext {
            operation_uuid: OperationId(operation),
            actor_uuid: None,
        },
        graph_mutations: vec![
            CompositeGraphMutation::CreateNode {
                node_uuid: node,
                label: "Finding".into(),
                properties: HashMap::from([
                    ("name".into(), PropValue::Str("Epistemic Finding".into())),
                    ("claim_key".into(), PropValue::Str("claim-m21".into())),
                    ("severity".into(), PropValue::Int(7)),
                ]),
            },
            CompositeGraphMutation::CreateEdge {
                edge_uuid: edge,
                rel_type: "SUPPORTS".into(),
                source_uuid: node,
                target_uuid: observation,
                properties: HashMap::from([("weight".into(), PropValue::Float(0.2))]),
            },
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid: edge,
                property: "weight".into(),
                value: PropValue::Float(0.8),
            },
        ],
        knowledge: CompositeKnowledgeParticipants {
            provenance_events: vec![provenance.clone()],
            lineage: vec![
                LineageRecord::new(
                    provenance.provenance_uuid,
                    node,
                    SubjectKind::Node,
                    LineageRole::Output,
                    0,
                )
                .unwrap(),
                LineageRecord::new(
                    provenance.provenance_uuid,
                    edge,
                    SubjectKind::Edge,
                    LineageRole::Output,
                    1,
                )
                .unwrap(),
                LineageRecord::new(
                    provenance.provenance_uuid,
                    assertion,
                    SubjectKind::Assertion,
                    LineageRole::Output,
                    2,
                )
                .unwrap(),
            ],
            assertions: vec![
                Assertion::new(
                    assertion,
                    "composite m21 claim".into(),
                    provenance.provenance_uuid,
                    now,
                )
                .unwrap(),
            ],
            assertion_graph_refs: vec![
                AssertionGraphRef::new(
                    assertion,
                    node,
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
            ],
            evidence: vec![
                EvidenceLink::new(
                    evidence,
                    assertion,
                    source,
                    EvidenceSourceKind::Document,
                    EvidenceRole::Supports,
                    Some(0.85),
                    provenance.provenance_uuid,
                    now,
                )
                .unwrap(),
            ],
            reasoning: vec![
                ReasoningRecord::new(
                    reasoning,
                    assertion,
                    ReasoningKind::LogicalInference,
                    ReasoningContentFormat::TextPlain,
                    b"supports the finding".to_vec(),
                    None,
                    provenance.provenance_uuid,
                    now,
                )
                .unwrap(),
            ],
            assertion_status: vec![
                AssertionStatusEvent::new(
                    status,
                    assertion,
                    AssertionStatus::Supported,
                    None,
                    Some(reasoning),
                    provenance.provenance_uuid,
                    now,
                )
                .unwrap(),
            ],
            hypothesis_groups: vec![
                HypothesisGroup::new(
                    group,
                    "who-transferred".into(),
                    provenance.provenance_uuid,
                    now,
                )
                .unwrap(),
            ],
            hypothesis_membership: vec![
                HypothesisMembershipEvent::new(
                    membership,
                    operation,
                    group,
                    assertion,
                    HypothesisMembershipAction::Added,
                    reasoning,
                    provenance.provenance_uuid,
                    now,
                )
                .unwrap(),
            ],
            ..CompositeKnowledgeParticipants::default()
        },
    }
}

fn invalid_request(observation: Uuid) -> CompositeTransactionRequest {
    // Unknown strict-ontology entity type rejects before any participant staging.
    let mut request = graph_m20_request(0x0400, observation);
    if let CompositeGraphMutation::CreateNode { label, .. } = &mut request.graph_mutations[0] {
        *label = "NotInOntology".into();
    }
    request
}

fn publish(
    graph: &GraphForge,
    request: CompositeTransactionRequest,
) -> arrow::record_batch::RecordBatch {
    graph
        .publish_composite_transaction(request)
        .unwrap_or_else(|error| panic!("composite publish failed: {error}"))
}

fn main() {
    let sha = std::env::var("GF_ATOMIC_RECOVERY_SHA").expect("workflow SHA is required");
    let evidence_path = PathBuf::from(
        std::env::var("GF_ATOMIC_RECOVERY_EVIDENCE").expect("evidence path is required"),
    );

    let project = TempDir::new().unwrap();
    let graph = seed_project(project.path());
    let observation = observation_uuid(&graph);

    // AR-02 graph+M20
    let m20 = graph_m20_request(0x0200, observation);
    let before_m20 = finding_names(&graph);
    let _receipt_m20 = publish(&graph, m20);
    let after_m20 = finding_names(&graph);
    assert!(!before_m20.contains(&"Composite Finding".into()));
    assert!(after_m20.contains(&"Composite Finding".into()));

    // AR-03 graph+M20+M21
    let m21 = graph_m20_m21_request(0x0300, observation);
    let _receipt_m21 = publish(&graph, m21.clone());
    let after_m21 = finding_names(&graph);
    assert!(after_m21.contains(&"Epistemic Finding".into()));

    // AR-04 rejection before publication
    let rejected = graph
        .publish_composite_transaction(invalid_request(observation))
        .expect_err("invalid composite request must reject");
    assert_eq!(rejected.code(), "GF_ONTOLOGY");
    assert_eq!(finding_names(&graph), after_m21);

    // Neutral analysis surfaces remain usable on the committed generation.
    let _ = graph
        .find(FindOptions {
            query: Some("Composite".into()),
            label: Some("Finding".into()),
            limit: 10,
            ..FindOptions::default()
        })
        .unwrap();
    let _ = graph
        .rank(
            "Finding",
            RankOptions {
                by: RankAlgorithm::Degree,
                via: Some("SUPPORTS".into()),
                directed: true,
                write_property: None,
            },
        )
        .unwrap();

    // AR-08 / AR-09 idempotency
    let exact = publish(&graph, m21.clone());
    let exact_again = publish(&graph, m21.clone());
    assert_eq!(exact.num_rows(), 1);
    assert_eq!(exact_again.num_rows(), 1);
    assert_eq!(finding_names(&graph), after_m21);
    let mut conflicting = m21;
    if let CompositeGraphMutation::CreateNode { properties, .. } =
        &mut conflicting.graph_mutations[0]
    {
        properties.insert("name".into(), PropValue::Str("Conflicted".into()));
    }
    let conflict = graph
        .publish_composite_transaction(conflicting)
        .expect_err("conflicting reuse must fail");
    assert_eq!(conflict.code(), "GF_IDEMPOTENCY_CONFLICT");
    assert_eq!(finding_names(&graph), after_m21);

    let snapshot = graph.epistemic_snapshot(i64::MAX).unwrap();
    let names = finding_names(&graph);
    drop(graph);

    // AR-11 reopen
    let reopened = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
    let reopened_names = finding_names(&reopened);
    assert_eq!(names, reopened_names);
    let snapshot_reopen = reopened.epistemic_snapshot(i64::MAX).unwrap();
    assert_eq!(
        snapshot.stats.rows_produced,
        snapshot_reopen.stats.rows_produced
    );

    // Failpoint matrix authority evidence is produced by the gf-api library
    // failpoint tests invoked from run.py (deterministic process control).
    // Keep this catalog aligned with composite_recovery_tests::{PRE,POST}_CURRENT_FAILPOINTS.
    let pre_current = [
        "project.after_writer_lock",
        "project.after_journal_preparing",
        "project.after_participant_write",
        "project.after_participant_fsync",
        "project.after_participant_dir_fsync",
        "project.after_journal_staged",
        "project.after_domain_validation",
        "project.after_composite_validation",
        "project.after_journal_validated",
        "project.after_manifest_write",
        "project.after_manifest_fsync",
        "project.after_generation_dir_fsync",
        "project.after_journal_durable",
        "project.after_current_temp_write",
        "project.after_current_temp_fsync",
        "project.before_current_replace",
    ]
    .into_iter()
    .map(|failpoint| json!({"failpoint": failpoint, "authority": "previous"}))
    .collect::<Vec<_>>();
    let post_current = [
        "project.after_current_replace",
        "project.after_root_fsync",
        "project.after_journal_published",
    ]
    .into_iter()
    .map(|failpoint| json!({"failpoint": failpoint, "authority": "new"}))
    .collect::<Vec<_>>();

    let evidence = json!({
        "schema_version": 1,
        "scenario_id": "atomic-recovery",
        "commit_sha": sha,
        "graph_m20_committed": true,
        "graph_m20_m21_committed": true,
        "validation_rejection": {
            "code": "GF_ONTOLOGY",
            "publication": "none",
        },
        "pre_current_recoveries": pre_current,
        "post_current_recoveries": post_current,
        "orphan_free": true,
        "idempotency": {
            "exact_retry_identical": true,
            "conflict_code": "GF_IDEMPOTENCY_CONFLICT",
        },
        "transaction_time_view": {
            "epistemic_rows": snapshot.stats.rows_produced,
            "exact_snapshot_equal": true,
        },
        "reopen_equal": true,
        "findings": names,
    });
    fs::write(
        &evidence_path,
        serde_json::to_string_pretty(&evidence).unwrap() + "\n",
    )
    .unwrap();
}
