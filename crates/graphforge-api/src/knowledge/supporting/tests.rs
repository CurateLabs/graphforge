use super::super::tests::assertion_fixture;
use super::super::tests::enable;
use super::super::tests::uuid7;
use super::super::*;

use crate::CapabilityId;
use std::collections::HashMap;

#[test]
fn reasoning_is_append_only_exact_idempotent_and_reopenable() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(10));
    enable(&graph, CapabilityId::Provenance, 1);
    enable(&graph, CapabilityId::Knowledge, 2);
    enable(&graph, CapabilityId::Epistemic, 3);
    let assertion_uuid = uuid7(4);
    let provenance_uuid = assertion_fixture(&graph, assertion_uuid, 5);
    let first_uuid = uuid7(6);
    let first = RecordReasoningRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(7)),
            actor_uuid: None,
        },
        reasoning_uuid: first_uuid,
        assertion_uuid,
        kind: ReasoningKind::EvidenceInterpretation,
        content_format: ReasoningContentFormat::TextMarkdown,
        content: b"exact **reasoning**".to_vec(),
        supersedes_reasoning_uuid: None,
        provenance_uuid,
    };
    let created = graph.record_reasoning(first.clone()).unwrap();
    assert_eq!(
        created.batches[0].schema(),
        Arc::clone(&graphforge_knowledge::REASONING_SCHEMA)
    );
    assert_eq!(
        graph.record_reasoning(first).unwrap().batches[0],
        created.batches[0]
    );

    graph.set_clock_for_test(|| Ok(20));
    let amendment_uuid = uuid7(8);
    graph
        .record_reasoning(RecordReasoningRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(9)),
                actor_uuid: None,
            },
            reasoning_uuid: amendment_uuid,
            assertion_uuid,
            kind: ReasoningKind::LogicalInference,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"explicit amendment".to_vec(),
            supersedes_reasoning_uuid: Some(first_uuid),
            provenance_uuid,
        })
        .unwrap();
    let history = graph
        .list_reasoning(ListReasoningRequest {
            assertion_uuid: Some(assertion_uuid),
            page: PageRequest::default(),
        })
        .unwrap();
    assert_eq!(history.stats.rows_produced, 2);

    drop(graph);
    let reopened = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(
        reopened.reasoning(amendment_uuid, None).unwrap().batches[0].schema(),
        Arc::clone(&graphforge_knowledge::REASONING_SCHEMA)
    );
    assert_eq!(
        reopened
            .list_reasoning(ListReasoningRequest {
                assertion_uuid: Some(assertion_uuid),
                page: PageRequest::default(),
            })
            .unwrap()
            .stats
            .rows_produced,
        2
    );
}

#[test]
fn reasoning_rejects_dangling_cross_assertion_and_conflicting_replay() {
    let graph = GraphForge::new(None).unwrap();
    graph.set_clock_for_test(|| Ok(10));
    enable(&graph, CapabilityId::Provenance, 20);
    enable(&graph, CapabilityId::Knowledge, 21);
    enable(&graph, CapabilityId::Epistemic, 22);
    let assertion_uuid = uuid7(23);
    let provenance_uuid = assertion_fixture(&graph, assertion_uuid, 24);
    let request = RecordReasoningRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(25)),
            actor_uuid: None,
        },
        reasoning_uuid: uuid7(26),
        assertion_uuid,
        kind: ReasoningKind::MethodologicalNote,
        content_format: ReasoningContentFormat::TextPlain,
        content: b"method".to_vec(),
        supersedes_reasoning_uuid: None,
        provenance_uuid,
    };
    graph.record_reasoning(request.clone()).unwrap();
    let mut conflict = request;
    conflict.content = b"changed".to_vec();
    assert_eq!(
        graph.record_reasoning(conflict).unwrap_err().code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    let dangling = graph
        .record_reasoning(RecordReasoningRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(27)),
                actor_uuid: None,
            },
            reasoning_uuid: uuid7(28),
            assertion_uuid,
            kind: ReasoningKind::DecisionRationale,
            content_format: ReasoningContentFormat::ApplicationJson,
            content: br#"{"decision":"no"}"#.to_vec(),
            supersedes_reasoning_uuid: Some(uuid7(99)),
            provenance_uuid,
        })
        .unwrap_err();
    assert_eq!(dangling.code(), "GF_NOT_FOUND");
}

#[test]
fn confidence_publication_is_atomic_idempotent_deterministic_and_reopenable() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(3_000));
    enable(&graph, CapabilityId::Provenance, 50);
    let node = graph.add_node("Person", &HashMap::new()).unwrap();
    enable(&graph, CapabilityId::Knowledge, 51);
    let assertion_uuid = uuid7(52);
    graph
        .create_assertion(CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(53)),
                actor_uuid: None,
            },
            assertion_uuid,
            claim: "confidence target".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: node.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    let explicit_uuid = uuid7(54);
    let explicit = AssessConfidenceRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(55)),
            actor_uuid: None,
        },
        confidence_uuid: explicit_uuid,
        assertion_uuid,
        policy: ConfidencePolicyRequest::Explicit { value: 0.8 },
    };
    let created = graph.assess_confidence(explicit.clone()).unwrap();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        graph
            .confidence_assessment(explicit_uuid, Some(cancelled))
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    let generation = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    assert_eq!(
        graph.assess_confidence(explicit.clone()).unwrap().batches[0],
        created.batches[0]
    );
    let mut conflict = explicit;
    conflict.policy = ConfidencePolicyRequest::Explicit { value: 0.7 };
    assert_eq!(
        graph.assess_confidence(conflict).unwrap_err().code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        generation
    );

    let derived_uuid = uuid7(56);
    graph
        .assess_confidence(AssessConfidenceRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(57)),
                actor_uuid: None,
            },
            confidence_uuid: derived_uuid,
            assertion_uuid,
            policy: ConfidencePolicyRequest::ConservativeMin {
                input_confidence_uuids: vec![uuid7(99), explicit_uuid],
            },
        })
        .unwrap();
    let inputs = graph
        .confidence_inputs(derived_uuid, PageRequest::default())
        .unwrap();
    assert_eq!(inputs.stats.rows_produced, 2);
    let values = inputs.batches[0]
        .column_by_name("input_value")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!(values.is_valid(0));
    assert!(values.is_null(1));
    let list = graph
        .list_confidence_assessments(ListConfidenceAssessmentsRequest {
            assertion_uuid: Some(assertion_uuid),
            page: PageRequest::default(),
        })
        .unwrap();
    assert_eq!(list.stats.rows_produced, 2);

    drop(graph);
    let reopened = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(
        reopened
            .confidence_assessment(explicit_uuid, None)
            .unwrap()
            .batches[0],
        created.batches[0]
    );
    let history = reopened
        .list_provenance_history(crate::ProvenanceHistoryRequest {
            subject_uuid: Some(derived_uuid),
            operation_uuid: None,
            page: PageRequest::default(),
        })
        .unwrap();
    assert_eq!(history.stats.rows_produced, 1);
}

#[test]
fn invalid_confidence_writes_publish_nothing() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(4_000));
    enable(&graph, CapabilityId::Provenance, 60);
    enable(&graph, CapabilityId::Knowledge, 61);
    let before = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let missing = graph
        .assess_confidence(AssessConfidenceRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(62)),
                actor_uuid: None,
            },
            confidence_uuid: uuid7(63),
            assertion_uuid: uuid7(64),
            policy: ConfidencePolicyRequest::Explicit { value: 0.5 },
        })
        .unwrap_err();
    assert_eq!(missing.code(), "GF_NOT_FOUND");
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        before
    );
}

#[test]
fn evidence_publication_is_atomic_idempotent_filterable_and_reopenable() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(5_000));
    enable(&graph, CapabilityId::Provenance, 70);
    let node = graph.add_node("Person", &HashMap::new()).unwrap();
    enable(&graph, CapabilityId::Knowledge, 71);
    let assertion_uuid = uuid7(72);
    graph
        .create_assertion(CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(73)),
                actor_uuid: None,
            },
            assertion_uuid,
            claim: "evidence target".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: node.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    let evidence_uuid = uuid7(74);
    let request = AttachEvidenceRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(75)),
            actor_uuid: None,
        },
        evidence_uuid,
        assertion_uuid,
        source_uuid: node.uuid,
        source_kind: EvidenceSourceKind::GraphNode,
        role: EvidenceRole::Supports,
        weight: Some(0.9),
    };
    let created = graph.attach_evidence(request.clone()).unwrap();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        graph
            .evidence_link(evidence_uuid, Some(cancelled))
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    let generation = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    assert_eq!(
        graph.attach_evidence(request.clone()).unwrap().batches[0],
        created.batches[0]
    );
    let mut conflict = request;
    conflict.role = EvidenceRole::Contradicts;
    assert_eq!(
        graph.attach_evidence(conflict).unwrap_err().code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        generation
    );
    assert_eq!(
        graph
            .list_evidence_links(ListEvidenceLinksRequest {
                assertion_uuid: Some(assertion_uuid),
                source_uuid: Some(node.uuid),
                page: PageRequest::default(),
            })
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    drop(graph);
    let reopened = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(
        reopened.evidence_link(evidence_uuid, None).unwrap().batches[0],
        created.batches[0]
    );
    assert_eq!(
        reopened
            .list_provenance_history(crate::ProvenanceHistoryRequest {
                subject_uuid: Some(evidence_uuid),
                operation_uuid: None,
                page: PageRequest::default(),
            })
            .unwrap()
            .stats
            .rows_produced,
        1
    );
}

#[test]
fn invalid_evidence_companion_publishes_nothing() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(6_000));
    enable(&graph, CapabilityId::Provenance, 80);
    enable(&graph, CapabilityId::Knowledge, 81);
    let before = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let error = graph
        .attach_evidence(AttachEvidenceRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(82)),
                actor_uuid: None,
            },
            evidence_uuid: uuid7(83),
            assertion_uuid: uuid7(84),
            source_uuid: uuid7(85),
            source_kind: EvidenceSourceKind::GraphNode,
            role: EvidenceRole::Supports,
            weight: None,
        })
        .unwrap_err();
    assert_eq!(error.code(), "GF_NOT_FOUND");
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        before
    );
}

#[test]
fn assertion_with_evidence_is_one_atomic_idempotent_generation() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(7_000));
    enable(&graph, CapabilityId::Provenance, 90);
    let node = graph.add_node("Person", &HashMap::new()).unwrap();
    enable(&graph, CapabilityId::Knowledge, 91);
    let assertion_uuid = uuid7(92);
    let evidence_uuid = uuid7(93);
    let request = CreateAssertionWithEvidenceRequest {
        assertion: CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(94)),
                actor_uuid: None,
            },
            assertion_uuid,
            claim: "atomic bundle".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: node.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        },
        evidence: vec![EvidenceInput {
            evidence_uuid,
            source_uuid: uuid7(95),
            source_kind: EvidenceSourceKind::Document,
            role: EvidenceRole::Context,
            weight: None,
        }],
    };
    let created = graph
        .create_assertion_with_evidence(request.clone())
        .unwrap();
    let generation = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    assert_eq!(
        graph
            .create_assertion_with_evidence(request.clone())
            .unwrap()
            .batches[0],
        created.batches[0]
    );
    let mut conflict = request;
    conflict.assertion.claim = "changed bundle".into();
    assert_eq!(
        graph
            .create_assertion_with_evidence(conflict)
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        generation
    );
    assert_eq!(
        graph
            .evidence_link(evidence_uuid, None)
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    assert_eq!(
        graph
            .list_provenance_history(crate::ProvenanceHistoryRequest {
                subject_uuid: Some(assertion_uuid),
                operation_uuid: None,
                page: PageRequest::default(),
            })
            .unwrap()
            .stats
            .rows_produced,
        1
    );
}
