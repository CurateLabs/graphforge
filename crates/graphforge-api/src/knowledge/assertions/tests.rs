use super::super::tests::assertion_fixture;
use super::super::tests::enable;
use super::super::tests::reasoning_fixture;
use super::super::tests::uuid7;
use super::super::*;

use crate::CapabilityId;
use std::collections::HashMap;

#[test]
fn assertion_publication_is_atomic_idempotent_and_reopenable() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(1_234_567));
    enable(&graph, CapabilityId::Provenance, 1);
    let node = graph.add_node("Person", &HashMap::new()).unwrap();
    enable(&graph, CapabilityId::Knowledge, 2);
    let assertion_uuid = uuid7(3);
    let request = CreateAssertionRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(4)),
            actor_uuid: Some(uuid7(5)),
        },
        assertion_uuid,
        claim: "e\u{301} is not é".into(),
        graph_refs: vec![AssertionGraphRefInput {
            graph_uuid: node.uuid,
            graph_kind: GraphObjectKind::Node,
            role: AssertionGraphRole::Subject,
            ordinal: 0,
        }],
    };

    let created = graph.create_assertion(request.clone()).unwrap();
    assert_eq!(created.stats.rows_produced, 1);
    crate::permanent_parquet_test_support::assert_participants(root.path(), "knowledge");
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        graph
            .assertion(assertion_uuid, Some(cancelled))
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    let generation = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let replay = graph.create_assertion(request).unwrap();
    assert_eq!(replay.batches[0], created.batches[0]);
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        generation
    );
    let refs = graph
        .assertion_graph_refs(assertion_uuid, PageRequest::default())
        .unwrap();
    assert_eq!(refs.stats.rows_produced, 1);

    drop(graph);
    let reopened = GraphForge::new(root.path().to_str()).unwrap();
    let fetched = reopened.assertion(assertion_uuid, None).unwrap();
    let claim = fetched.batches[0]
        .column_by_name("claim")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(claim.value(0), "e\u{301} is not é");
    let history = reopened
        .list_provenance_history(crate::ProvenanceHistoryRequest {
            subject_uuid: Some(assertion_uuid),
            operation_uuid: None,
            page: PageRequest::default(),
        })
        .unwrap();
    assert_eq!(history.stats.rows_produced, 1);
}

#[test]
fn invalid_graph_reference_and_conflicting_replay_publish_nothing() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(999));
    enable(&graph, CapabilityId::Provenance, 10);
    let node = graph.add_node("Person", &HashMap::new()).unwrap();
    let edge = graph
        .add_edge(&node, "SELF", &node, &HashMap::new())
        .unwrap();
    enable(&graph, CapabilityId::Knowledge, 11);
    let before = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let missing = graph
        .create_assertion(CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(12)),
                actor_uuid: None,
            },
            assertion_uuid: uuid7(13),
            claim: "missing".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: uuid7(14),
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap_err();
    assert_eq!(missing.code(), "GF_NOT_FOUND");
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        before
    );

    let empty = graph
        .create_assertion(CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(17)),
                actor_uuid: None,
            },
            assertion_uuid: uuid7(18),
            claim: "unanchored".into(),
            graph_refs: vec![],
        })
        .unwrap_err();
    assert_eq!(
        empty.to_string(),
        "validation error: assertion requires at least one graph reference"
    );

    let edge_assertion_uuid = uuid7(19);
    graph
        .create_assertion(CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(20)),
                actor_uuid: None,
            },
            assertion_uuid: edge_assertion_uuid,
            claim: "edge-backed".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: edge.uuid,
                graph_kind: GraphObjectKind::Edge,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    graph
        .attach_evidence(AttachEvidenceRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(21)),
                actor_uuid: None,
            },
            evidence_uuid: uuid7(22),
            assertion_uuid: edge_assertion_uuid,
            source_uuid: edge.uuid,
            source_kind: EvidenceSourceKind::GraphEdge,
            role: EvidenceRole::Supports,
            weight: None,
        })
        .unwrap();

    let assertion_uuid = uuid7(15);
    let mut request = CreateAssertionRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(16)),
            actor_uuid: None,
        },
        assertion_uuid,
        claim: "first".into(),
        graph_refs: vec![AssertionGraphRefInput {
            graph_uuid: node.uuid,
            graph_kind: GraphObjectKind::Node,
            role: AssertionGraphRole::Subject,
            ordinal: 0,
        }],
    };
    graph.create_assertion(request.clone()).unwrap();
    let committed = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    request.claim = "different".into();
    let conflict = graph.create_assertion(request).unwrap_err();
    assert_eq!(conflict.code(), "GF_IDEMPOTENCY_CONFLICT");
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        committed
    );
}

#[test]
fn assertion_lists_filter_and_page_with_generation_bound_tokens() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(2_000));
    enable(&graph, CapabilityId::Provenance, 20);
    let first_node = graph.add_node("Person", &HashMap::new()).unwrap();
    let second_node = graph.add_node("Person", &HashMap::new()).unwrap();
    enable(&graph, CapabilityId::Knowledge, 21);

    for (seed, claim, refs) in [
        (
            30,
            "first",
            vec![
                AssertionGraphRefInput {
                    graph_uuid: first_node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                },
                AssertionGraphRefInput {
                    graph_uuid: second_node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Context,
                    ordinal: 0,
                },
            ],
        ),
        (
            31,
            "second",
            vec![AssertionGraphRefInput {
                graph_uuid: second_node.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        ),
    ] {
        graph
            .create_assertion(CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid7(seed + 10)),
                    actor_uuid: None,
                },
                assertion_uuid: uuid7(seed),
                claim: claim.into(),
                graph_refs: refs,
            })
            .unwrap();
    }

    let first_page = graph
        .list_assertions(ListAssertionsRequest {
            graph_uuid: None,
            page: PageRequest {
                limit: 1,
                after: None,
                cancellation: None,
            },
        })
        .unwrap();
    let token = first_page.batches[0]
        .schema()
        .metadata()
        .get("graphforge.next_page_token")
        .cloned()
        .expect("first assertion page must continue");
    let second_page = graph
        .list_assertions(ListAssertionsRequest {
            graph_uuid: None,
            page: PageRequest {
                limit: 1,
                after: Some(PageToken::parse(&token).unwrap()),
                cancellation: None,
            },
        })
        .unwrap();
    assert_eq!(first_page.stats.rows_produced, 1);
    assert_eq!(second_page.stats.rows_produced, 1);
    assert_ne!(first_page.batches[0], second_page.batches[0]);

    let excluded = graph
        .list_assertions(ListAssertionsRequest {
            graph_uuid: Some(uuid7(99)),
            page: PageRequest::default(),
        })
        .unwrap();
    assert_eq!(excluded.stats.rows_produced, 0);

    let first_refs = graph
        .assertion_graph_refs(
            uuid7(30),
            PageRequest {
                limit: 1,
                after: None,
                cancellation: None,
            },
        )
        .unwrap();
    let ref_token = first_refs.batches[0]
        .schema()
        .metadata()
        .get("graphforge.next_page_token")
        .cloned()
        .expect("first graph-ref page must continue");
    let second_refs = graph
        .assertion_graph_refs(
            uuid7(30),
            PageRequest {
                limit: 1,
                after: Some(PageToken::parse(&ref_token).unwrap()),
                cancellation: None,
            },
        )
        .unwrap();
    assert_eq!(first_refs.stats.rows_produced, 1);
    assert_eq!(second_refs.stats.rows_produced, 1);
    assert_ne!(first_refs.batches[0], second_refs.batches[0]);
}

#[test]
fn assertion_status_is_explicit_append_only_idempotent_and_reopenable() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(10));
    enable(&graph, CapabilityId::Provenance, 110);
    enable(&graph, CapabilityId::Knowledge, 111);
    enable(&graph, CapabilityId::Epistemic, 112);
    let assertion_uuid = uuid7(113);
    let provenance_uuid = assertion_fixture(&graph, assertion_uuid, 114);
    assert_eq!(
        graph
            .assertion_status(assertion_uuid)
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let first = RecordAssertionStatusRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(115)),
            actor_uuid: None,
        },
        status_event_uuid: uuid7(116),
        assertion_uuid,
        status: AssertionStatus::Hypothesis,
        confidence_uuid: None,
        reasoning_uuid: None,
        provenance_uuid,
    };
    let created = graph.record_assertion_status(first.clone()).unwrap();
    assert_eq!(
        graph.record_assertion_status(first).unwrap().batches[0],
        created.batches[0]
    );
    let confidence_uuid = uuid7(117);
    graph
        .assess_confidence(AssessConfidenceRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(118)),
                actor_uuid: None,
            },
            confidence_uuid,
            assertion_uuid,
            policy: ConfidencePolicyRequest::Explicit { value: 0.75 },
        })
        .unwrap();
    let reasoning_uuid = uuid7(119);
    graph
        .record_reasoning(RecordReasoningRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(120)),
                actor_uuid: None,
            },
            reasoning_uuid,
            assertion_uuid,
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"supports status".to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid,
        })
        .unwrap();
    graph
        .record_assertion_status(RecordAssertionStatusRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(121)),
                actor_uuid: None,
            },
            status_event_uuid: uuid7(122),
            assertion_uuid,
            status: AssertionStatus::Supported,
            confidence_uuid: Some(confidence_uuid),
            reasoning_uuid: Some(reasoning_uuid),
            provenance_uuid,
        })
        .unwrap();
    assert_eq!(
        graph.assertion_status(assertion_uuid).unwrap().batches[0]
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "supported"
    );
    let tied_lower_uuid = RecordAssertionStatusRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(123)),
            actor_uuid: None,
        },
        status_event_uuid: uuid7(100),
        assertion_uuid,
        status: AssertionStatus::Disputed,
        confidence_uuid: None,
        reasoning_uuid: None,
        provenance_uuid,
    };
    let appended = graph
        .record_assertion_status(tied_lower_uuid.clone())
        .unwrap();
    assert_eq!(
        appended.batches[0]
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "disputed"
    );
    assert_eq!(
        graph
            .record_assertion_status(tied_lower_uuid)
            .unwrap()
            .batches[0],
        appended.batches[0]
    );
    assert_eq!(
        graph.assertion_status(assertion_uuid).unwrap().batches[0]
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "supported"
    );
    let reopened = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(
        reopened
            .list_assertion_status(ListAssertionStatusRequest {
                assertion_uuid: Some(assertion_uuid),
                page: PageRequest::default(),
            })
            .unwrap()
            .stats
            .rows_produced,
        3
    );
}

#[test]
fn assertion_status_rejects_conflicts_missing_refs_and_direct_supersession() {
    let graph = GraphForge::new(None).unwrap();
    graph.set_clock_for_test(|| Ok(10));
    enable(&graph, CapabilityId::Provenance, 120);
    enable(&graph, CapabilityId::Knowledge, 121);
    enable(&graph, CapabilityId::Epistemic, 122);
    let assertion_uuid = uuid7(123);
    let provenance_uuid = assertion_fixture(&graph, assertion_uuid, 124);
    let request = RecordAssertionStatusRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(125)),
            actor_uuid: None,
        },
        status_event_uuid: uuid7(126),
        assertion_uuid,
        status: AssertionStatus::Disputed,
        confidence_uuid: None,
        reasoning_uuid: None,
        provenance_uuid,
    };
    graph.record_assertion_status(request.clone()).unwrap();
    let mut conflict = request.clone();
    conflict.status = AssertionStatus::Refuted;
    assert_eq!(
        graph.record_assertion_status(conflict).unwrap_err().code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    let mut missing = request.clone();
    missing.status_event_uuid = uuid7(127);
    missing.context.operation_uuid = OperationId(uuid7(128));
    missing.confidence_uuid = Some(uuid7(129));
    assert_eq!(
        graph.record_assertion_status(missing).unwrap_err().code(),
        "GF_NOT_FOUND"
    );
    let mut superseded = request;
    superseded.status_event_uuid = uuid7(130);
    superseded.context.operation_uuid = OperationId(uuid7(131));
    superseded.status = AssertionStatus::Superseded;
    assert_eq!(
        graph
            .record_assertion_status(superseded)
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );
}

#[test]
fn assertion_and_first_status_publish_as_one_idempotent_generation() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(20));
    enable(&graph, CapabilityId::Provenance, 140);
    enable(&graph, CapabilityId::Knowledge, 141);
    enable(&graph, CapabilityId::Epistemic, 142);
    let node = graph.add_node("StatusSubject", &HashMap::new()).unwrap();
    let assertion_uuid = uuid7(143);
    let request = CreateAssertionWithStatusRequest {
        assertion: CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(144)),
                actor_uuid: None,
            },
            assertion_uuid,
            claim: "explicit first status".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: node.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        },
        first_status: FirstAssertionStatusInput {
            status_event_uuid: uuid7(145),
            status: AssertionStatus::Hypothesis,
        },
    };
    let created = graph.create_assertion_with_status(request.clone()).unwrap();
    let generation = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    graph.set_clock_for_test(|| Ok(999));
    assert_eq!(
        graph.create_assertion_with_status(request).unwrap().batches[0],
        created.batches[0]
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        generation
    );
    assert_eq!(
        graph
            .assertion(assertion_uuid, None)
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    assert_eq!(
        graph
            .assertion_status(assertion_uuid)
            .unwrap()
            .stats
            .rows_produced,
        1
    );
}

#[test]
fn supersession_is_atomic_branch_preserving_idempotent_and_reopenable() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.set_clock_for_test(|| Ok(50));
    enable(&graph, CapabilityId::Provenance, 200);
    enable(&graph, CapabilityId::Knowledge, 201);
    enable(&graph, CapabilityId::Epistemic, 202);
    let prior = uuid7(203);
    let first_replacement = uuid7(204);
    let second_replacement = uuid7(205);
    let provenance = assertion_fixture(&graph, prior, 206);
    assertion_fixture(&graph, first_replacement, 207);
    assertion_fixture(&graph, second_replacement, 208);
    let reasoning = reasoning_fixture(&graph, prior, provenance, 209, 210);
    let first = SupersedeAssertionRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(211)),
            actor_uuid: None,
        },
        supersession_uuid: uuid7(212),
        prior_assertion_uuid: prior,
        replacement_assertion_uuid: first_replacement,
        status_event_uuid: uuid7(213),
        reasoning_uuid: reasoning,
        provenance_uuid: provenance,
    };
    let created = graph.supersede_assertion(first.clone()).unwrap();
    let generation = graphforge_storage::resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    assert_eq!(
        graph.supersede_assertion(first).unwrap().batches[0],
        created.batches[0]
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        generation
    );
    graph
        .supersede_assertion(SupersedeAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(uuid7(214)),
                actor_uuid: None,
            },
            supersession_uuid: uuid7(215),
            prior_assertion_uuid: prior,
            replacement_assertion_uuid: second_replacement,
            status_event_uuid: uuid7(216),
            reasoning_uuid: reasoning,
            provenance_uuid: provenance,
        })
        .unwrap();
    let reopened = GraphForge::new(root.path().to_str()).unwrap();
    let first_page = reopened
        .list_assertion_supersessions(ListAssertionSupersessionsRequest {
            prior_assertion_uuid: Some(prior),
            replacement_assertion_uuid: None,
            page: PageRequest {
                limit: 1,
                after: None,
                cancellation: None,
            },
        })
        .unwrap();
    let token = first_page.batches[0]
        .schema()
        .metadata()
        .get("graphforge.next_page_token")
        .cloned()
        .expect("first supersession page must continue");
    let second_page = reopened
        .list_assertion_supersessions(ListAssertionSupersessionsRequest {
            prior_assertion_uuid: Some(prior),
            replacement_assertion_uuid: None,
            page: PageRequest {
                limit: 1,
                after: Some(PageToken::parse(&token).unwrap()),
                cancellation: None,
            },
        })
        .unwrap();
    assert_eq!(first_page.stats.rows_produced, 1);
    assert_eq!(second_page.stats.rows_produced, 1);
    assert_ne!(first_page.batches[0], second_page.batches[0]);
    assert_eq!(
        reopened
            .list_assertion_supersessions(ListAssertionSupersessionsRequest {
                prior_assertion_uuid: None,
                replacement_assertion_uuid: Some(second_replacement),
                page: PageRequest::default(),
            })
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    assert_eq!(
        reopened
            .list_assertion_status(ListAssertionStatusRequest {
                assertion_uuid: Some(prior),
                page: PageRequest::default(),
            })
            .unwrap()
            .stats
            .rows_produced,
        2
    );
    for assertion_uuid in [prior, first_replacement, second_replacement] {
        assert_eq!(
            reopened
                .assertion(assertion_uuid, None)
                .unwrap()
                .stats
                .rows_produced,
            1
        );
    }
}

#[test]
fn supersession_rejects_self_links_cycles_dangling_refs_and_conflicts() {
    let graph = GraphForge::new(None).unwrap();
    graph.set_clock_for_test(|| Ok(60));
    enable(&graph, CapabilityId::Provenance, 220);
    enable(&graph, CapabilityId::Knowledge, 221);
    enable(&graph, CapabilityId::Epistemic, 222);
    let first = uuid7(223);
    let second = uuid7(224);
    let first_provenance = assertion_fixture(&graph, first, 225);
    let second_provenance = assertion_fixture(&graph, second, 226);
    let first_reasoning = reasoning_fixture(&graph, first, first_provenance, 227, 228);
    let second_reasoning = reasoning_fixture(&graph, second, second_provenance, 229, 230);
    let request = SupersedeAssertionRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(231)),
            actor_uuid: None,
        },
        supersession_uuid: uuid7(232),
        prior_assertion_uuid: first,
        replacement_assertion_uuid: second,
        status_event_uuid: uuid7(233),
        reasoning_uuid: first_reasoning,
        provenance_uuid: first_provenance,
    };
    graph.supersede_assertion(request.clone()).unwrap();
    let mut conflict = request.clone();
    conflict.status_event_uuid = uuid7(234);
    assert_eq!(
        graph.supersede_assertion(conflict).unwrap_err().code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    let self_link = SupersedeAssertionRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(235)),
            actor_uuid: None,
        },
        supersession_uuid: uuid7(236),
        prior_assertion_uuid: second,
        replacement_assertion_uuid: second,
        status_event_uuid: uuid7(237),
        reasoning_uuid: second_reasoning,
        provenance_uuid: second_provenance,
    };
    assert_eq!(
        graph.supersede_assertion(self_link).unwrap_err().code(),
        "GF_VALIDATION"
    );
    let cycle = SupersedeAssertionRequest {
        context: WriteContext {
            operation_uuid: OperationId(uuid7(238)),
            actor_uuid: None,
        },
        supersession_uuid: uuid7(239),
        prior_assertion_uuid: second,
        replacement_assertion_uuid: first,
        status_event_uuid: uuid7(240),
        reasoning_uuid: second_reasoning,
        provenance_uuid: second_provenance,
    };
    assert_eq!(
        graph.supersede_assertion(cycle).unwrap_err().code(),
        "GF_VALIDATION"
    );
    let mut wrong_reasoning = request;
    wrong_reasoning.context.operation_uuid = OperationId(uuid7(241));
    wrong_reasoning.supersession_uuid = uuid7(242);
    wrong_reasoning.status_event_uuid = uuid7(243);
    wrong_reasoning.reasoning_uuid = second_reasoning;
    assert_eq!(
        graph
            .supersede_assertion(wrong_reasoning)
            .unwrap_err()
            .code(),
        "GF_NOT_FOUND"
    );
}
