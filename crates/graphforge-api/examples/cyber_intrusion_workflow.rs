//! Opt-in executable release evidence for issue #2467.
//!
//! The bundle-local runner invokes this example on a developer or release-
//! candidate machine. Ordinary workspace tests and the aggregate CI gate do
//! not execute this full workflow.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
use graphforge_api::{
    AdoptOntologyRequest, AssertionGraphRefInput, AssertionGraphRole, AssessConfidenceRequest,
    AttachEvidenceRequest, CallerEmbeddingBatchRequest, CallerEmbeddingBatchRow,
    CallerEmbeddingDistance, CallerEmbeddingNormalization, CancellationToken, CapabilityId,
    CheckpointRequest, ClusterAlgorithm, ClusterOptions, ConfidencePolicyRequest,
    CreateAssertionRequest, CreateHypothesisGroupRequest, EnableCapabilityRequest, EvidenceRole,
    EvidenceSourceKind, FindOptions, GraphForge, GraphObjectKind, HypothesisMembershipAction,
    ListAssertionsRequest, NodeHandle, NodeSelector, OntologyMode, OperationId, PageRequest,
    PathAlgorithm, PathsOptions, PropValue, RankAlgorithm, RankOptions, ReasoningContentFormat,
    ReasoningKind, RecordHypothesisMembershipRequest, RecordReasoningRequest,
    RevertCheckpointRequest, SearchIndexOptions, SimilarAlgorithm, SimilarOptions, WriteContext,
};
use graphforge_ontology::{OntologyCompiler, OntologyLoader};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

const BUNDLE: &str = "../../tests/release_workflows/cyber-intrusion";

fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(BUNDLE)
        .join(relative)
}

fn id(seed: u16) -> Uuid {
    Uuid::parse_str(&format!("018f0f4e-7b8c-7000-8000-00000003{seed:04x}")).unwrap()
}

fn context(seed: u16) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(id(seed)),
        actor_uuid: Some(id(0xff00)),
    }
}

fn enable(graph: &GraphForge, capability_id: CapabilityId, seed: u16) {
    graph
        .enable_capability(EnableCapabilityRequest {
            context: context(seed),
            capability_id,
            capability_version: 1,
        })
        .unwrap();
}

fn props(values: &[(&str, PropValue)]) -> HashMap<String, PropValue> {
    values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect()
}

fn provenance(result: &graphforge_api::ExecutionResult) -> Uuid {
    let values = result.batches[0]
        .column_by_name("provenance_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    Uuid::from_slice(values.value(0)).unwrap()
}

fn assertion(
    graph: &GraphForge,
    seed: u16,
    claim: &str,
    subject: &NodeHandle,
) -> (Uuid, Uuid, Uuid) {
    let assertion_uuid = id(seed);
    let result = graph
        .create_assertion(CreateAssertionRequest {
            context: context(seed + 1),
            assertion_uuid,
            claim: claim.to_owned(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: subject.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    let provenance_uuid = provenance(&result);
    graph
        .attach_evidence(AttachEvidenceRequest {
            context: context(seed + 2),
            evidence_uuid: id(seed + 3),
            assertion_uuid,
            source_uuid: subject.uuid,
            source_kind: EvidenceSourceKind::GraphNode,
            role: EvidenceRole::Supports,
            weight: Some(0.7),
        })
        .unwrap();
    graph
        .assess_confidence(AssessConfidenceRequest {
            context: context(seed + 4),
            confidence_uuid: id(seed + 5),
            assertion_uuid,
            policy: ConfidencePolicyRequest::Explicit { value: 0.7 },
        })
        .unwrap();
    let reasoning_uuid = id(seed + 6);
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(seed + 7),
            reasoning_uuid,
            assertion_uuid,
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: claim.as_bytes().to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid,
        })
        .unwrap();
    (assertion_uuid, reasoning_uuid, provenance_uuid)
}

fn publish_embeddings(graph: &GraphForge, alerts: &[Uuid], replace_alias: bool) {
    graph
        .publish_caller_embeddings(CallerEmbeddingBatchRequest {
            display_name: "alert-semantic-v1".into(),
            contract_version: "cyber-intrusion-v1".into(),
            dimensions: 3,
            normalization: CallerEmbeddingNormalization::None,
            distance: CallerEmbeddingDistance::Cosine,
            source_projection_recipe: BTreeMap::from([
                ("label".into(), "Alert".into()),
                ("property".into(), "summary".into()),
            ]),
            rows: vec![
                CallerEmbeddingBatchRow {
                    node: NodeSelector::Uuid(alerts[0]),
                    vector: vec![1.0, 0.1, 0.0],
                },
                CallerEmbeddingBatchRow {
                    node: NodeSelector::Uuid(alerts[1]),
                    vector: vec![0.9, 0.2, 0.1],
                },
                CallerEmbeddingBatchRow {
                    node: NodeSelector::Uuid(alerts[2]),
                    vector: vec![0.0, 0.1, 1.0],
                },
            ],
            replace_alias,
        })
        .unwrap();
}

fn hybrid_options() -> FindOptions {
    FindOptions {
        query: Some("encoded powershell".into()),
        label: Some("Alert".into()),
        vector: Some(vec![1.0, 0.0, 0.0]),
        limit: 3,
        space: Some("alert-semantic-v1".into()),
        ..FindOptions::default()
    }
}

fn schema_fingerprint(batch: &arrow::record_batch::RecordBatch) -> String {
    let schema = batch
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{}:{:?}", field.name(), field.data_type()))
        .collect::<Vec<_>>()
        .join("|");
    hex_sha256(schema.as_bytes())
}

#[allow(clippy::too_many_lines, reason = "one auditable release workflow")]

fn hex_sha256(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn main() {
    let project = TempDir::new().unwrap();
    let ontology_path = fixture("ontologies/strict-v1.yaml");
    let document = OntologyLoader::load_file(&ontology_path).unwrap();
    let compiled = OntologyCompiler::compile(&document).unwrap();
    assert_eq!(compiled.entity_types.num_rows(), 12);
    assert_eq!(compiled.relation_types.num_rows(), 8);
    assert_eq!(compiled.property_types.num_rows(), 18);
    assert_eq!(compiled.type_constraints.num_rows(), 4);
    assert_eq!(compiled.semantic_flags.num_rows(), 8);

    let mut graph = GraphForge::new(project.path().to_str()).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(1),
            path: ontology_path,
            mode: OntologyMode::Strict,
        })
        .unwrap();
    assert_eq!(graph.ontology_mode(), OntologyMode::Strict);
    enable(&graph, CapabilityId::Provenance, 2);
    enable(&graph, CapabilityId::Knowledge, 3);
    enable(&graph, CapabilityId::Epistemic, 4);

    let host1 = graph
        .add_node(
            "Host",
            &props(&[
                ("name", PropValue::Str("HOST-01".into())),
                ("hostname", PropValue::Str("host-01.example".into())),
                ("criticality", PropValue::Int(5)),
                ("exposed", PropValue::Bool(true)),
                ("risk", PropValue::Float(8.2)),
            ]),
        )
        .unwrap();
    let host2 = graph
        .add_node(
            "Host",
            &props(&[
                ("name", PropValue::Str("HOST-02".into())),
                ("hostname", PropValue::Str("host-02.example".into())),
                ("criticality", PropValue::Int(4)),
                ("exposed", PropValue::Bool(false)),
            ]),
        )
        .unwrap();
    let host3 = graph
        .add_node(
            "Host",
            &props(&[
                ("name", PropValue::Str("HOST-03".into())),
                ("hostname", PropValue::Str("host-03.example".into())),
                ("criticality", PropValue::Int(3)),
                ("exposed", PropValue::Bool(false)),
            ]),
        )
        .unwrap();
    let host4 = graph
        .add_node(
            "Host",
            &props(&[
                ("name", PropValue::Str("HOST-04".into())),
                ("hostname", PropValue::Str("host-04.example".into())),
                ("criticality", PropValue::Int(2)),
                ("exposed", PropValue::Bool(false)),
            ]),
        )
        .unwrap();
    let identity = graph
        .add_node(
            "Identity",
            &props(&[
                ("name", PropValue::Str("svc-backup".into())),
                ("account", PropValue::Str("svc-backup".into())),
            ]),
        )
        .unwrap();
    let identity2 = graph
        .add_node(
            "Identity",
            &props(&[
                ("name", PropValue::Str("analyst".into())),
                ("account", PropValue::Str("analyst".into())),
            ]),
        )
        .unwrap();
    let process1 = graph
        .add_node(
            "Process",
            &props(&[
                ("name", PropValue::Str("powershell".into())),
                (
                    "command",
                    PropValue::Str("powershell -enc SYNTHETIC".into()),
                ),
            ]),
        )
        .unwrap();
    let process2 = graph
        .add_node(
            "Process",
            &props(&[
                ("name", PropValue::Str("rundll32".into())),
                ("command", PropValue::Str("rundll32 synthetic.dll".into())),
            ]),
        )
        .unwrap();
    let process3 = graph
        .add_node(
            "Process",
            &props(&[
                ("name", PropValue::Str("backup".into())),
                ("command", PropValue::Str("backup --verify".into())),
            ]),
        )
        .unwrap();
    let alert1 = graph
        .add_node(
            "Alert",
            &props(&[
                ("name", PropValue::Str("ALERT-01".into())),
                (
                    "summary",
                    PropValue::Str("encoded powershell launch".into()),
                ),
                ("severity", PropValue::Int(9)),
            ]),
        )
        .unwrap();
    let alert2 = graph
        .add_node(
            "Alert",
            &props(&[
                ("name", PropValue::Str("ALERT-02".into())),
                (
                    "summary",
                    PropValue::Str("powershell lateral movement".into()),
                ),
                ("severity", PropValue::Int(8)),
            ]),
        )
        .unwrap();
    let alert3 = graph
        .add_node(
            "Alert",
            &props(&[
                ("name", PropValue::Str("ALERT-03".into())),
                (
                    "summary",
                    PropValue::Str("routine backup verification".into()),
                ),
                ("severity", PropValue::Int(2)),
            ]),
        )
        .unwrap();
    let vulnerability1 = graph
        .add_node(
            "Vulnerability",
            &props(&[
                ("name", PropValue::Str("VULN-01".into())),
                ("cve", PropValue::Str("CVE-2099-0001".into())),
                ("cvss", PropValue::Float(8.8)),
            ]),
        )
        .unwrap();
    let vulnerability2 = graph
        .add_node(
            "Vulnerability",
            &props(&[
                ("name", PropValue::Str("VULN-02".into())),
                ("cve", PropValue::Str("CVE-2099-0002".into())),
                ("cvss", PropValue::Float(6.4)),
            ]),
        )
        .unwrap();
    let indicator1 = graph
        .add_node(
            "Indicator",
            &props(&[
                ("name", PropValue::Str("IOC-01".into())),
                ("value", PropValue::Str("198.51.100.10".into())),
            ]),
        )
        .unwrap();
    let indicator2 = graph
        .add_node(
            "Indicator",
            &props(&[
                ("name", PropValue::Str("IOC-02".into())),
                ("value", PropValue::Str("synthetic.example".into())),
            ]),
        )
        .unwrap();
    let indicator3 = graph
        .add_node(
            "Indicator",
            &props(&[
                ("name", PropValue::Str("IOC-03".into())),
                ("value", PropValue::Str("backup-tool".into())),
            ]),
        )
        .unwrap();

    graph
        .add_edge(&identity, "AUTHENTICATED_TO", &host1, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&identity2, "AUTHENTICATED_TO", &host4, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&process1, "SPAWNED", &process2, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&process1, "EXECUTED_ON", &host1, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&process2, "EXECUTED_ON", &host2, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&process3, "EXECUTED_ON", &host4, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&alert1, "TRIGGERED_ON", &host1, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&alert2, "TRIGGERED_ON", &host2, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&alert3, "TRIGGERED_ON", &host4, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&vulnerability1, "AFFECTS", &host1, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&vulnerability2, "AFFECTS", &host2, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&indicator1, "OBSERVED_IN", &alert1, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&indicator2, "OBSERVED_IN", &alert2, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&indicator3, "OBSERVED_IN", &alert3, &HashMap::new())
        .unwrap();
    for (left, right) in [(&host1, &host2), (&host2, &host3), (&host3, &host4)] {
        graph
            .add_edge(left, "COMMUNICATED_WITH", right, &HashMap::new())
            .unwrap();
    }
    graph
        .add_edge(&host1, "COMMUNICATED_WITH", &host3, &HashMap::new())
        .unwrap();

    let before_invalid = graph.execute("MATCH (n) RETURN count(n) AS total").unwrap();
    let before_invalid_count = before_invalid.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let unknown = graph.add_node("UnknownAsset", &HashMap::new()).unwrap_err();
    assert_eq!(unknown.code(), "GF_PARSE");
    let invalid_property = graph
        .add_node(
            "Host",
            &props(&[("unknown_field", PropValue::Str("must fail".into()))]),
        )
        .unwrap_err();
    assert_eq!(invalid_property.code(), "GF_VALIDATION");
    let invalid_find = graph.find(FindOptions::default()).unwrap_err();
    assert_eq!(invalid_find.code(), "GF_VALIDATION");
    let after_invalid = graph.execute("MATCH (n) RETURN count(n) AS total").unwrap();
    let after_invalid_count = after_invalid.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(after_invalid_count, before_invalid_count);

    graph
        .index_search(
            "Alert",
            SearchIndexOptions::Text {
                properties: Some(vec!["summary".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    publish_embeddings(&graph, &[alert1.uuid, alert2.uuid, alert3.uuid], false);
    let text = graph
        .find(FindOptions {
            query: Some("backup".into()),
            label: Some("Alert".into()),
            limit: 3,
            ..FindOptions::default()
        })
        .unwrap();
    let vector = graph
        .find(FindOptions {
            label: Some("Alert".into()),
            vector: Some(vec![1.0, 0.0, 0.0]),
            space: Some("alert-semantic-v1".into()),
            limit: 3,
            ..FindOptions::default()
        })
        .unwrap();
    let hybrid = graph.find(hybrid_options()).unwrap();
    assert_eq!(text.num_rows(), 1);
    assert_eq!(vector.num_rows(), 3);
    assert_eq!(hybrid.num_rows(), 3);
    assert_eq!(
        hybrid
            .column_by_name("matched_on")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "text+vector"
    );

    let path = graph
        .paths(
            Some(&NodeSelector::Handle(host1.clone())),
            Some(&NodeSelector::Handle(host4.clone())),
            PathsOptions {
                by: PathAlgorithm::Bfs,
                via: Some("COMMUNICATED_WITH".into()),
                directed: true,
                ..PathsOptions::default()
            },
        )
        .unwrap();
    let rank = graph
        .rank(
            "Host",
            RankOptions {
                by: RankAlgorithm::Degree,
                via: Some("COMMUNICATED_WITH".into()),
                directed: true,
                write_property: None,
            },
        )
        .unwrap();
    let cluster = graph
        .cluster(
            "Host",
            ClusterOptions {
                by: ClusterAlgorithm::Components,
                via: Some("COMMUNICATED_WITH".into()),
                directed: false,
                ..ClusterOptions::default()
            },
        )
        .unwrap();
    let similar = graph
        .similar(
            "Host",
            SimilarOptions {
                by: SimilarAlgorithm::NodeSimilarity,
                k: 3,
                vector_property: None,
                via: Some("COMMUNICATED_WITH".into()),
            },
        )
        .unwrap();
    assert_eq!(path.num_rows(), 1);
    assert_eq!(rank.num_rows(), 4);
    assert_eq!(cluster.num_rows(), 4);
    assert_eq!(similar.num_rows(), 2);

    let first = assertion(
        &graph,
        0x100,
        "The alert chain is compatible with credential-led lateral movement",
        &alert1,
    );
    let second = assertion(
        &graph,
        0x120,
        "The observed chain may combine routine administration and unrelated alerts",
        &alert3,
    );
    let group_uuid = id(0x180);
    graph
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(0x181),
            group_uuid,
            question_key: "cyber.synthetic.intrusion-path.v1".into(),
            provenance_uuid: first.2,
        })
        .unwrap();
    for (seed, item) in [(0x190, first), (0x1a0, second)] {
        graph
            .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
                context: context(seed),
                membership_event_uuid: id(seed + 1),
                group_uuid,
                assertion_uuid: item.0,
                action: HypothesisMembershipAction::Added,
                reasoning_uuid: item.1,
                provenance_uuid: item.2,
            })
            .unwrap();
    }
    assert_eq!(
        graph.hypothesis_members(group_uuid).unwrap().batches[0].num_rows(),
        2
    );
    assert_eq!(
        graph.hypothesis_selection(group_uuid).unwrap().batches[0].num_rows(),
        0
    );

    graph
        .checkpoint(CheckpointRequest {
            name: "correct-association".into(),
            description: Some("known-good graph and derived state".into()),
            idempotency_key: OperationId(id(0x200)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();
    graph
        .add_edge(&alert1, "TRIGGERED_ON", &host4, &HashMap::new())
        .unwrap();
    graph
        .add_node(
            "Alert",
            &props(&[
                ("name", PropValue::Str("ALERT-FALSE".into())),
                (
                    "summary",
                    PropValue::Str("false association synthetic alert".into()),
                ),
                ("severity", PropValue::Int(1)),
            ]),
        )
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "false-association".into(),
            description: Some("preserved mistaken association".into()),
            idempotency_key: OperationId(id(0x201)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();
    let stale_error = graph.find(hybrid_options()).unwrap_err();
    assert_ne!(stale_error.code(), "GF_INTERNAL");

    graph
        .revert_to_checkpoint(RevertCheckpointRequest {
            name: "correct-association".into(),
            reason: "remove false host association and restore complete workspace".into(),
            idempotency_key: OperationId(id(0x202)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();
    graph
        .index_search(
            "Alert",
            SearchIndexOptions::Text {
                properties: Some(vec!["summary".into()]),
                rebuild: true,
            },
        )
        .unwrap();
    publish_embeddings(&graph, &[alert1.uuid, alert2.uuid, alert3.uuid], true);
    let corrected_hybrid = graph.find(hybrid_options()).unwrap();
    assert_eq!(corrected_hybrid, hybrid);
    let false_view = graph.open_checkpoint("false-association").unwrap();
    let false_edges = false_view
        .execute("MATCH (:Alert)-[:TRIGGERED_ON]->(:Host) RETURN count(*) AS total")
        .unwrap();
    let false_edge_count = false_edges.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let corrected_edges = graph
        .execute("MATCH (:Alert)-[:TRIGGERED_ON]->(:Host) RETURN count(*) AS total")
        .unwrap();
    let corrected_edge_count = corrected_edges.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(false_edge_count, 4);
    assert_eq!(corrected_edge_count, 3);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = graph
        .list_assertions(ListAssertionsRequest {
            graph_uuid: None,
            page: PageRequest {
                limit: 10,
                after: None,
                cancellation: Some(cancellation),
            },
        })
        .unwrap_err();
    assert_eq!(cancelled.code(), "GF_CANCELLED");
    assert_eq!(
        graph
            .list_assertions(ListAssertionsRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        2
    );

    let host_uuids = [host1.uuid, host2.uuid, host3.uuid, host4.uuid];
    let corrected_schema = schema_fingerprint(&corrected_hybrid);
    drop(graph);
    let reopened = GraphForge::new(project.path().to_str()).unwrap();
    assert_eq!(reopened.ontology_mode(), OntologyMode::Strict);
    let reopened_hybrid = reopened.find(hybrid_options()).unwrap();
    assert_eq!(reopened_hybrid, corrected_hybrid);
    assert_eq!(
        reopened
            .rank(
                "Host",
                RankOptions {
                    by: RankAlgorithm::Degree,
                    via: Some("COMMUNICATED_WITH".into()),
                    directed: true,
                    write_property: None,
                }
            )
            .unwrap(),
        rank
    );
    assert_eq!(
        reopened.hypothesis_members(group_uuid).unwrap().batches[0].num_rows(),
        2
    );
    assert_eq!(
        reopened.hypothesis_selection(group_uuid).unwrap().batches[0].num_rows(),
        0
    );

    let evidence_path = std::env::var("GF_CYBER_EVIDENCE_PATH").unwrap();
    let commit_sha = std::env::var("GF_RELEASE_WORKFLOW_SHA").unwrap();
    let evidence = json!({
        "schema_version": 1,
        "scenario_id": "cyber-intrusion",
        "commit_sha": commit_sha,
        "strict_errors": {"unknown_label": unknown.code(), "unknown_property": invalid_property.code(), "invalid_find": invalid_find.code()},
        "stale_search_error": stale_error.code(),
        "cancelled_error": cancelled.code(),
        "ontology_metrics": {"entities": 12, "relations": 8, "properties": 18, "constraints": 4, "semantic_rows": 8},
        "operation_rows": {"text": text.num_rows(), "vector": vector.num_rows(), "hybrid": hybrid.num_rows(), "paths": path.num_rows(), "rank": rank.num_rows(), "cluster": cluster.num_rows(), "similar": similar.num_rows()},
        "host_uuids": host_uuids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
        "hybrid_schema_sha256": corrected_schema,
        "hypotheses": {"members": 2, "selection": null, "confidence_selected_implicitly": false},
        "correction": {"false_view_edges": false_edge_count, "corrected_view_edges": corrected_edge_count, "derived_state_refreshed": true},
        "reopen_equal": true
    });
    fs::write(evidence_path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
}
