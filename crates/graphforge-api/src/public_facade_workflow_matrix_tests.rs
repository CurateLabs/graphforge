//! Public-facade workflows measured independently at two unrelated-data sizes.
//!
//! Cancellation/reopen is covered by
//! `durability_certification_tests::production_history_certifies_cancel_drop_and_idempotent_retry`;
//! injected fault recovery/reopen by
//! `composite_recovery_tests::composite_kill_reopen_matrix_never_exposes_mixed_state`;
//! large-participant retention/GC and portable reuse by
//! `graph_publication::tests::graph_mutation_reuses_large_unchanged_knowledge_participant`.
//! Counters are emitted per operation. A counter absent from a region is
//! printed as `None` (n/a), never interpreted as zero work.

use std::collections::HashMap;

use arrow::array::Array;

use graphforge_storage::{
    concurrency_attribution, lifecycle_io, payload_digest::PayloadDigestCapture,
};

use crate::{
    ArtifactKind, ArtifactPayloadRequest, AssertionGraphRefInput, AssertionGraphRole, CapabilityId,
    CreateAssertionRequest, DerivationInput, DerivationSubjectKind, EnableCapabilityRequest,
    GraphForge, GraphObjectKind, ListArtifactsRequest, ListAssertionsRequest, ListSourcesRequest,
    OperationId, RegisterArtifactRequest, RegisterSourceRequest, SourceKind, WriteContext,
};

fn context() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(uuid::Uuid::now_v7()),
        actor_uuid: None,
    }
}

fn work_total(snapshot: &concurrency_attribution::RegionSnapshot, name: &str) -> Option<u64> {
    let values = snapshot
        .regions
        .values()
        .filter_map(|region| region.work.get(name).copied())
        .collect::<Vec<_>>();
    (!values.is_empty()).then(|| values.into_iter().sum())
}

fn metric(value: Option<u64>) -> String {
    value.map_or_else(|| "n/a".to_owned(), |count| count.to_string())
}

fn varied_claim(length: usize) -> String {
    let mut state = 0x5eed_u64;
    (0..length)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            char::from(b'a' + u8::try_from((state >> 32) % 26).unwrap())
        })
        .collect()
}

fn measure<T>(unrelated_nodes: usize, action: &'static str, run: impl FnOnce() -> T) -> T {
    let before = lifecycle_io::snapshot().unwrap();
    let capture = concurrency_attribution::RegionCapture::start("public_facade_operation");
    let digest_capture = PayloadDigestCapture::start();
    let result = run();
    let checksum_bytes = digest_capture.snapshot().checksum_bytes;
    drop(digest_capture);
    let regions = capture.finish();
    assert!(
        regions.complete,
        "{action} region capture should be complete"
    );
    let after = lifecycle_io::snapshot().unwrap();
    let io = after.since(&before).unwrap();
    io.validate_for_qualification().unwrap();
    let decoded_rows = work_total(&regions, "rows_decoded");
    let inspection_rows = work_total(&regions, "rows");
    let sealed_written_bytes = work_total(&regions, "written_bytes");
    let write_bytes = io.totals.write_bytes;
    let read_bytes = io.totals.read_bytes;
    eprintln!(
        "public_facade_matrix unrelated_nodes={unrelated_nodes} action={action} bytes_read={read_bytes} lifecycle_write_bytes={write_bytes} sealed_written_bytes={} checksum_bytes={checksum_bytes} rows_decoded={} instrumented_rows={}",
        metric(sealed_written_bytes),
        metric(decoded_rows),
        metric(inspection_rows)
    );
    result
}

fn participant_bytes(project: &std::path::Path, participant: &str) -> Vec<u8> {
    graphforge_storage::resolve_project_generation(project)
        .unwrap()
        .participant_snapshot("knowledge", participant)
        .unwrap()
        .unwrap()
        .bytes
}

fn run_scenario(unrelated_nodes: usize, assertion_claim_bytes: usize) {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let graph = GraphForge::new(project.to_str()).unwrap();
    let _lifecycle = lifecycle_io::CaptureScope::install();
    graph
        .enable_capability(EnableCapabilityRequest {
            context: context(),
            capability_id: CapabilityId::Provenance,
            capability_version: 1,
        })
        .unwrap();
    graph
        .enable_capability(EnableCapabilityRequest {
            context: context(),
            capability_id: CapabilityId::Knowledge,
            capability_version: 1,
        })
        .unwrap();

    let seed = (0..unrelated_nodes)
        .map(|index| format!("CREATE (:Unrelated {{slot: {index}}})"))
        .collect::<Vec<_>>()
        .join(" ");
    graph.execute(&seed).unwrap();

    let mut properties = HashMap::new();
    properties.insert("name".to_owned(), crate::PropValue::Str("left".into()));
    let left = measure(unrelated_nodes, "add_node", || {
        graph.add_node("Anchor", &properties).unwrap()
    });
    let mut properties = HashMap::new();
    properties.insert("name".to_owned(), crate::PropValue::Str("right".into()));
    let right = measure(unrelated_nodes, "add_node", || {
        graph.add_node("Anchor", &properties).unwrap()
    });
    measure(unrelated_nodes, "add_edge", || {
        graph
            .add_edge(&left, "LINKS", &right, &HashMap::new())
            .unwrap()
    });

    measure(unrelated_nodes, "property_set", || {
        graph
            .execute("MATCH (n:Anchor {name: 'left'}) SET n.transient = 7")
            .unwrap()
    });
    let set_value = graph
        .execute("MATCH (n:Anchor {name: 'left'}) RETURN n.transient")
        .unwrap();
    let set_value = set_value.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(set_value.value(0), 7);
    measure(unrelated_nodes, "property_remove", || {
        graph
            .execute("MATCH (n:Anchor {name: 'left'}) REMOVE n.transient")
            .unwrap()
    });
    let removed_value = graph
        .execute("MATCH (n:Anchor {name: 'left'}) RETURN n.transient IS NULL AS removed")
        .unwrap();
    let removed = removed_value.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(removed.value(0));
    measure(unrelated_nodes, "relationship_merge", || {
        graph
            .execute("MATCH (a:Anchor {name: 'left'}), (b:Anchor {name: 'right'}) MERGE (a)-[:MERGED_LINK]->(b)")
            .unwrap()
    });
    measure(unrelated_nodes, "label_change", || {
        graph
            .execute("MATCH (n:Anchor {name: 'left'}) SET n:Renamed REMOVE n:Anchor")
            .unwrap()
    });

    let committed = graph.begin_transaction(context()).unwrap();
    measure(unrelated_nodes, "multi_statement_commit", || {
        committed
            .stage_cypher("CREATE (:Committed {part: 1})", HashMap::new())
            .unwrap();
        committed
            .stage_cypher("CREATE (:Committed {part: 2})", HashMap::new())
            .unwrap();
        committed.validate(&graph).unwrap();
        committed.commit(&graph).unwrap()
    });
    let rolled_back = graph.begin_transaction(context()).unwrap();
    measure(unrelated_nodes, "multi_statement_abort", || {
        rolled_back
            .stage_cypher("CREATE (:Aborted {part: 1})", HashMap::new())
            .unwrap();
        rolled_back
            .stage_cypher("CREATE (:Aborted {part: 2})", HashMap::new())
            .unwrap();
        rolled_back.rollback().unwrap()
    });

    let ledger_subject = measure(unrelated_nodes, "ledger_subject_node", || {
        graph.add_node("LedgerSubject", &HashMap::new()).unwrap()
    });
    let assertion_uuid = uuid::Uuid::now_v7();
    measure(unrelated_nodes, "ledger_append_assertion", || {
        graph
            .create_assertion(CreateAssertionRequest {
                context: context(),
                assertion_uuid,
                claim: varied_claim(assertion_claim_bytes),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: ledger_subject.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap()
    });
    let initial_assertions = participant_bytes(&project, "assertions");
    assert!(initial_assertions.len() > assertion_claim_bytes / 2);
    measure(
        unrelated_nodes,
        "assertion_ledger_append_with_existing_row",
        || {
            graph
                .create_assertion(CreateAssertionRequest {
                    context: context(),
                    assertion_uuid: uuid::Uuid::now_v7(),
                    claim: "second assertion appended to the existing ledger".into(),
                    graph_refs: vec![AssertionGraphRefInput {
                        graph_uuid: ledger_subject.uuid,
                        graph_kind: GraphObjectKind::Node,
                        role: AssertionGraphRole::Subject,
                        ordinal: 0,
                    }],
                })
                .unwrap()
        },
    );
    let assertion_rows = graph
        .list_assertions(ListAssertionsRequest::default())
        .unwrap();
    assert_eq!(assertion_rows.stats.rows_produced, 2);
    let claims = assertion_rows
        .batches
        .iter()
        .flat_map(|batch| {
            let claims = batch
                .column_by_name("claim")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..claims.len())
                .map(|row| claims.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 2);
    assert!(
        claims
            .iter()
            .any(|claim| claim == "second assertion appended to the existing ledger")
    );
    assert!(
        claims
            .iter()
            .any(|claim| claim == &varied_claim(assertion_claim_bytes))
    );
    let assertions_before = participant_bytes(&project, "assertions");
    eprintln!(
        "public_facade_matrix unrelated_nodes={unrelated_nodes} assertion_ledger_bytes={}",
        assertions_before.len()
    );

    // This graph publication carries forward the large, unchanged assertion
    // participant. The exact bytes must remain unchanged through publication.
    measure(
        unrelated_nodes,
        "graph_publish_with_large_unchanged_ledger",
        || graph.add_node("AfterLedger", &HashMap::new()).unwrap(),
    );
    assert_eq!(participant_bytes(&project, "assertions"), assertions_before);

    let source_uuid = uuid::Uuid::now_v7();
    measure(unrelated_nodes, "source_ledger_append_merge", || {
        graph
            .register_source(RegisterSourceRequest {
                context: context(),
                source_uuid,
                label: "matrix source".into(),
                source_kind: SourceKind::Manuscript,
                identity_uri: None,
            })
            .unwrap()
    });
    let second_source_uuid = uuid::Uuid::now_v7();
    measure(unrelated_nodes, "source_ledger_second_append", || {
        graph
            .register_source(RegisterSourceRequest {
                context: context(),
                source_uuid: second_source_uuid,
                label: "second matrix source".into(),
                source_kind: SourceKind::Manuscript,
                identity_uri: None,
            })
            .unwrap()
    });
    let artifact_uuid = uuid::Uuid::now_v7();
    measure(
        unrelated_nodes,
        "artifact_append_and_derivation_validation",
        || {
            graph
                .register_artifact(RegisterArtifactRequest {
                    context: context(),
                    artifact_uuid,
                    source_uuid,
                    artifact_kind: ArtifactKind::RawScan,
                    media_type: "application/octet-stream".into(),
                    payload: ArtifactPayloadRequest::Absent,
                    derivation_inputs: vec![DerivationInput {
                        input_uuid: source_uuid,
                        input_kind: DerivationSubjectKind::Source,
                    }],
                    run_uuid: None,
                })
                .unwrap()
        },
    );
    measure(
        unrelated_nodes,
        "artifact_ledger_append_with_existing_row",
        || {
            graph
                .register_artifact(RegisterArtifactRequest {
                    context: context(),
                    artifact_uuid: uuid::Uuid::now_v7(),
                    source_uuid,
                    artifact_kind: ArtifactKind::RawScan,
                    media_type: "application/octet-stream".into(),
                    payload: ArtifactPayloadRequest::Absent,
                    derivation_inputs: vec![DerivationInput {
                        input_uuid: artifact_uuid,
                        input_kind: DerivationSubjectKind::Artifact,
                    }],
                    run_uuid: None,
                })
                .unwrap()
        },
    );

    let unchanged_ledgers = [
        ("assertions", participant_bytes(&project, "assertions")),
        ("sources", participant_bytes(&project, "sources")),
        ("artifacts", participant_bytes(&project, "artifacts")),
        (
            "artifact_derivations",
            participant_bytes(&project, "artifact_derivations"),
        ),
    ];
    measure(
        unrelated_nodes,
        "graph_publish_with_large_knowledge_siblings",
        || graph.add_node("AfterKnowledge", &HashMap::new()).unwrap(),
    );
    for (participant, bytes) in unchanged_ledgers {
        assert_eq!(
            participant_bytes(&project, participant),
            bytes,
            "{participant}"
        );
    }

    measure(unrelated_nodes, "inspection_helpers", || {
        assert_eq!(
            graph.node_count("Unrelated").unwrap(),
            unrelated_nodes as u64
        );
        assert_eq!(graph.node_count("Committed").unwrap(), 2);
        assert_eq!(graph.node_count("Aborted").unwrap(), 0);
        assert!(graph.labels().unwrap().contains(&"Renamed".to_owned()));
        assert!(
            graph
                .relationship_types()
                .unwrap()
                .contains(&"MERGED_LINK".to_owned())
        );
        assert!(graph.schema().unwrap().num_rows() >= 4);
    });
    measure(unrelated_nodes, "source_and_artifact_inspection", || {
        assert_eq!(
            graph
                .list_sources(ListSourcesRequest::default())
                .unwrap()
                .stats
                .rows_produced,
            2
        );
        assert_eq!(
            graph
                .list_artifacts(ListArtifactsRequest::default())
                .unwrap()
                .stats
                .rows_produced,
            2
        );
    });

    drop(graph);
    let reopened = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(
        reopened.node_count("Unrelated").unwrap(),
        unrelated_nodes as u64
    );
    assert_eq!(reopened.node_count("Committed").unwrap(), 2);
    assert_eq!(reopened.node_count("Aborted").unwrap(), 0);
    assert_eq!(reopened.node_count("Renamed").unwrap(), 1);
    assert_eq!(participant_bytes(&project, "assertions"), assertions_before);
    assert_eq!(
        reopened
            .list_sources(ListSourcesRequest::default())
            .unwrap()
            .stats
            .rows_produced,
        2
    );
    assert_eq!(
        reopened
            .list_artifacts(ListArtifactsRequest::default())
            .unwrap()
            .stats
            .rows_produced,
        2
    );
    assert_eq!(
        reopened
            .execute("MATCH ()-[r:LINKS]->() RETURN count(r) AS n")
            .unwrap()
            .batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        reopened
            .execute("MATCH ()-[r:MERGED_LINK]->() RETURN count(r) AS n")
            .unwrap()
            .batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[test]
fn public_facade_workflow_matrix_runs_at_two_unrelated_data_sizes() {
    run_scenario(4, 64 * 1024);
    run_scenario(32, 128 * 1024);
}
