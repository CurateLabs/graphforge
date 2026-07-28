//! Public topology and text freshness transitions for release workflow #2470.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use gf_api::{
    AdoptOntologyRequest, AssertionGraphRefInput, AssertionGraphRole, CallerEmbeddingBatchRequest,
    CallerEmbeddingBatchRow, CallerEmbeddingDistance, CallerEmbeddingNormalization,
    CancellationToken, CapabilityId, CreateAssertionRequest, CreateHypothesisGroupRequest,
    EnableCapabilityRequest, FindOptions, GraphForge, GraphObjectKind, HypothesisMembershipAction,
    IrLiteral, NodeHandle, NodeSelector, OperationId, PropValue, RankAlgorithm, RankOptions,
    ReasoningContentFormat, ReasoningKind, RecordHypothesisMembershipRequest,
    RecordReasoningRequest, SearchIndexOptions, WriteContext,
};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

const BUNDLE: &str = "../../tests/release_workflows/derived-state-freshness";

fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(BUNDLE)
        .join(relative)
}

fn context(suffix: u16) -> WriteContext {
    let uuid = id(suffix);
    WriteContext {
        operation_uuid: OperationId(uuid),
        actor_uuid: None,
    }
}

fn id(suffix: u16) -> Uuid {
    Uuid::parse_str(&format!("018f0f4e-7b8c-7000-8000-00000004{suffix:04x}")).unwrap()
}

fn state(value: impl std::fmt::Debug) -> String {
    format!("{value:?}").to_ascii_lowercase()
}

struct Seed {
    people: Vec<NodeHandle>,
    removable_edge: Uuid,
}

fn seed_graph(graph: &GraphForge) -> Seed {
    let mut people = Vec::new();
    for index in 0..11 {
        people.push(
            graph
                .add_node(
                    "Person",
                    &HashMap::from([
                        ("name".into(), PropValue::Str(format!("Person {index:02}"))),
                        (
                            "summary".into(),
                            PropValue::Str(if index == 0 {
                                "quasar".into()
                            } else {
                                format!("baseline topic {index:02}")
                            }),
                        ),
                        (
                            "risk_score".into(),
                            PropValue::Float(f64::from(index) / 12.0),
                        ),
                        (
                            "external_id".into(),
                            PropValue::Str(format!("P-{index:02}")),
                        ),
                    ]),
                )
                .unwrap(),
        );
    }
    let mut removable_edge = None;
    for (source, target) in [
        (0, 1),
        (1, 2),
        (2, 0),
        (0, 2),
        (3, 4),
        (4, 5),
        (5, 3),
        (3, 5),
        (2, 3),
        (5, 6),
        (6, 7),
        (7, 8),
        (8, 9),
        (9, 10),
    ] {
        let edge = graph
            .add_edge(&people[source], "KNOWS", &people[target], &HashMap::new())
            .unwrap();
        if (source, target) == (0, 1) {
            removable_edge = Some(edge.uuid);
        }
    }
    Seed {
        people,
        removable_edge: removable_edge.unwrap(),
    }
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

fn record_hypothesis(graph: &GraphForge, subject: Uuid) -> gf_api::ExecutionResult {
    let assertion = graph
        .create_assertion(CreateAssertionRequest {
            context: context(0x0200),
            assertion_uuid: id(0x0201),
            claim: "The dense local community is operationally significant".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: subject,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    let provenance_uuid = provenance(&assertion);
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(0x0210),
            reasoning_uuid: id(0x0211),
            assertion_uuid: id(0x0201),
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"Derived-state refresh must not alter hypothesis membership".to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid,
        })
        .unwrap();
    graph
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(0x0220),
            group_uuid: id(0x0221),
            question_key: "derived-state-freshness.community.v1".into(),
            provenance_uuid,
        })
        .unwrap();
    graph
        .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
            context: context(0x0230),
            membership_event_uuid: id(0x0231),
            group_uuid: id(0x0221),
            assertion_uuid: id(0x0201),
            action: HypothesisMembershipAction::Added,
            reasoning_uuid: id(0x0211),
            provenance_uuid,
        })
        .unwrap();
    graph.epistemic_snapshot(i64::MAX).unwrap()
}

fn embedding_request(
    people: &[NodeHandle],
    version: &str,
    swapped: bool,
    replace_alias: bool,
) -> CallerEmbeddingBatchRequest {
    CallerEmbeddingBatchRequest {
        display_name: "semantic".into(),
        contract_version: version.into(),
        dimensions: 2,
        normalization: CallerEmbeddingNormalization::None,
        distance: CallerEmbeddingDistance::Cosine,
        source_projection_recipe: std::collections::BTreeMap::from([
            ("label".into(), "Person".into()),
            ("recipe".into(), version.into()),
        ]),
        rows: people
            .iter()
            .enumerate()
            .map(|(index, node)| CallerEmbeddingBatchRow {
                node: NodeSelector::Handle(node.clone()),
                vector: if (index == 0) ^ swapped {
                    vec![1.0, 0.0]
                } else {
                    vec![0.0, 1.0]
                },
            })
            .collect(),
        replace_alias,
    }
}

fn enable_knowledge(graph: &GraphForge) {
    for (capability, suffix) in [
        (CapabilityId::Provenance, 0x0110),
        (CapabilityId::Knowledge, 0x0111),
        (CapabilityId::Epistemic, 0x0112),
    ] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(suffix),
                capability_id: capability,
                capability_version: 1,
            })
            .unwrap();
    }
}

fn strict_graph(project_path: &str) -> GraphForge {
    let mut graph = GraphForge::new(Some(project_path)).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(0x0100),
            path: fixture("ontologies/strict-v1.yaml"),
            mode: gf_api::OntologyMode::Strict,
        })
        .unwrap();
    drop(graph);
    let graph = GraphForge::new(Some(project_path)).unwrap();
    assert_eq!(graph.ontology_mode(), gf_api::OntologyMode::Strict);
    enable_knowledge(&graph);
    graph
}

struct ExtensionEvidence {
    analysis: serde_json::Value,
    embeddings: serde_json::Value,
    hypothesis: serde_json::Value,
}

fn rank_values(batch: &arrow::record_batch::RecordBatch, value: &str) -> Vec<f64> {
    let column = batch
        .column_by_name(value)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows()).map(|row| column.value(row)).collect()
}

fn node_uuids(batch: &arrow::record_batch::RecordBatch) -> Vec<Uuid> {
    let values = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| Uuid::from_slice(values.value(row)).unwrap())
        .collect()
}

fn text_result(batch: &RecordBatch) -> serde_json::Value {
    let names = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let matched = batch
        .column_by_name("matched_on")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    json!({
        "schema": batch
            .schema()
            .fields()
            .iter()
            .map(|field| format!("{}:{:?}", field.name(), field.data_type()))
            .collect::<Vec<_>>(),
        "rows": (0..batch.num_rows())
            .map(|row| json!({"name": names.value(row), "matched_on": matched.value(row)}))
            .collect::<Vec<_>>(),
    })
}

fn correction_evidence(graph: &GraphForge, seed: &Seed) -> ExtensionEvidence {
    let rank_options = |write_property| RankOptions {
        by: RankAlgorithm::Degree,
        via: Some("KNOWS".into()),
        directed: false,
        write_property,
    };
    let rank_corrected = graph
        .rank("Person", rank_options(Some("risk_score".into())))
        .unwrap();
    let rank_reanalyzed = graph.rank("Person", rank_options(None)).unwrap();
    let corrected_scores = rank_values(&rank_corrected, "score");
    assert_eq!(corrected_scores, rank_values(&rank_reanalyzed, "score"));
    assert_eq!(
        corrected_scores,
        rank_values(&rank_reanalyzed, "risk_score")
    );

    let hypothesis_before = record_hypothesis(graph, seed.people[0].uuid);
    let initial = embedding_request(&seed.people, "derived-state-v1", false, false);
    let initial_generation = graph
        .publish_caller_embeddings(initial)
        .unwrap()
        .compatibility_id;
    let embedding_initial = graph
        .inspect_embedding_space_freshness(Some("semantic"), false)
        .unwrap();
    assert_eq!(state(&embedding_initial.state), "fresh");
    let replacement = embedding_request(&seed.people, "derived-state-v2", true, true);
    let replacement_generation = graph.publish_caller_embeddings(replacement).unwrap();
    let embedding_replaced = graph
        .inspect_embedding_space_freshness(Some("semantic"), false)
        .unwrap();
    assert_eq!(state(&embedding_replaced.state), "fresh");
    let replay = embedding_request(&seed.people, "derived-state-v2", true, false);
    let replay_generation = graph.publish_caller_embeddings(replay).unwrap();
    assert_eq!(replacement_generation, replay_generation);
    let exact_replay = replacement_generation == replay_generation;

    let mut incompatible = embedding_request(&seed.people, "derived-state-v3", true, true);
    incompatible.dimensions = 3;
    let incompatible_code = graph
        .publish_caller_embeddings(incompatible)
        .unwrap_err()
        .code()
        .to_owned();
    assert_eq!(incompatible_code, "GF_VALIDATION");
    assert_eq!(
        graph
            .inspect_embedding_space_freshness(Some("semantic"), false)
            .unwrap(),
        embedding_replaced
    );
    let vector_results = graph
        .find(FindOptions {
            label: Some("Person".into()),
            vector: Some(vec![1.0, 0.0]),
            space: Some("semantic".into()),
            limit: 3,
            ..FindOptions::default()
        })
        .unwrap();
    let mut expected = seed.people[1..]
        .iter()
        .map(|node| node.uuid)
        .collect::<Vec<_>>();
    expected.sort_unstable();
    expected.truncate(3);
    assert_eq!(node_uuids(&vector_results), expected);
    let hypothesis_after = graph.epistemic_snapshot(i64::MAX).unwrap();
    assert_eq!(hypothesis_before.batches, hypothesis_after.batches);

    ExtensionEvidence {
        analysis: json!({
            "property_correction_reanalyzed": true,
            "authoritative_vector_uuids": expected,
        }),
        embeddings: json!({
            "compatibility_ids": [initial_generation, replacement_generation.compatibility_id],
            "states": [state(&embedding_initial.state), state(&embedding_replaced.state)],
            "exact_replay": exact_replay,
            "incompatible_code": incompatible_code,
            "prior_authority_preserved": true,
        }),
        hypothesis: json!({ "exact_snapshot_equal": true }),
    }
}

fn reopen_equal(graph: GraphForge, project_path: &str) -> bool {
    let expected_text = graph
        .inspect_text_index("Person", Some(&["name".into(), "summary".into()]))
        .unwrap();
    let expected_adjacency = graph.inspect_adjacency().unwrap();
    let expected_embedding = graph
        .inspect_embedding_space_freshness(Some("semantic"), false)
        .unwrap();
    drop(graph);
    let reopened = GraphForge::new(Some(project_path)).unwrap();
    assert_eq!(reopened.ontology_mode(), gf_api::OntologyMode::Strict);
    reopened
        .inspect_text_index("Person", Some(&["name".into(), "summary".into()]))
        .unwrap()
        == expected_text
        && reopened.inspect_adjacency().unwrap() == expected_adjacency
        && reopened
            .inspect_embedding_space_freshness(Some("semantic"), false)
            .unwrap()
            == expected_embedding
}

fn main() {
    let project = TempDir::new().unwrap();
    let project_path = project.path().to_str().unwrap();
    let graph = strict_graph(project_path);

    let mut seed = seed_graph(&graph);
    let text_initial = graph
        .index_search(
            "Person",
            SearchIndexOptions::Text {
                properties: Some(vec!["name".into(), "summary".into()]),
                rebuild: true,
            },
        )
        .unwrap()
        .unwrap();
    let adjacency_initial = graph.index_adjacency().unwrap();
    assert_eq!(state(text_initial.state), "current");
    assert_eq!(state(adjacency_initial.state), "current");
    let baseline_text = graph
        .find(FindOptions {
            query: Some("quasar".into()),
            label: Some("Person".into()),
            limit: 3,
            ..FindOptions::default()
        })
        .unwrap();
    assert_eq!(baseline_text.num_rows(), 1);

    graph
        .execute_with_params(
            "MATCH ()-[r:KNOWS]->() WHERE r.edge_uuid = $edge_uuid DELETE r",
            &HashMap::from([(
                "edge_uuid".into(),
                IrLiteral::Uuid(*seed.removable_edge.as_bytes()),
            )]),
        )
        .unwrap();
    let adjacency_stale = graph.inspect_adjacency().unwrap();
    assert_eq!(state(adjacency_stale.state), "stale");
    assert!(adjacency_stale.reason.is_some());

    let prior_authority = adjacency_stale.clone();
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    let cancellation_code = graph
        .rebuild_adjacency(Some(cancellation))
        .unwrap_err()
        .code()
        .to_owned();
    assert_eq!(cancellation_code, "GF_CANCELLED");
    assert_eq!(graph.inspect_adjacency().unwrap(), prior_authority);

    let adjacency_rebuilt = graph.rebuild_adjacency(None).unwrap();
    assert_eq!(state(adjacency_rebuilt.state), "current");
    let final_person = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".into(), PropValue::Str("Person 11".into())),
                ("summary".into(), PropValue::Str("fresh gamma".into())),
                ("risk_score".into(), PropValue::Float(11.0 / 12.0)),
                ("external_id".into(), PropValue::Str("P-11".into())),
            ]),
        )
        .unwrap();
    seed.people.push(final_person);
    let text_stale = graph
        .inspect_text_index("Person", Some(&["name".into(), "summary".into()]))
        .unwrap();
    assert_eq!(state(text_stale.state), "stale");
    assert!(text_stale.reason.is_some());

    let text_rebuilt = graph
        .index_search(
            "Person",
            SearchIndexOptions::Text {
                properties: Some(vec!["name".into(), "summary".into()]),
                rebuild: true,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(state(text_rebuilt.state), "current");
    let refreshed_text = graph
        .find(FindOptions {
            query: Some("fresh gamma".into()),
            label: Some("Person".into()),
            limit: 3,
            ..FindOptions::default()
        })
        .unwrap();
    assert_eq!(refreshed_text.num_rows(), 1);

    let extension = correction_evidence(&graph, &seed);
    let reopen_equal = reopen_equal(graph, project_path);
    assert!(reopen_equal);

    println!(
        "{}",
        json!({
            "scenario_id": "derived-state-freshness",
            "slice": "authoritative-rust",
            "text": [state(text_initial.state), state(text_stale.state), state(text_rebuilt.state)],
            "text_results": {
                "baseline": text_result(&baseline_text),
                "refreshed": text_result(&refreshed_text),
            },
            "adjacency": [state(adjacency_initial.state), state(adjacency_stale.state), state(adjacency_rebuilt.state)],
            "cancellation_code": cancellation_code,
            "prior_authority_preserved": true,
            "analysis": extension.analysis,
            "embeddings": extension.embeddings,
            "hypothesis": extension.hypothesis,
            "transaction_time_view": {"cutoff": i64::MAX, "exact_snapshot_equal": true},
            "ontology_constant": true,
            "reopen_equal": reopen_equal,
        })
    );
}
