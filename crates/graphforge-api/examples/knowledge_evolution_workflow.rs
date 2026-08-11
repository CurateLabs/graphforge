//! Opt-in Rust-owned evidence for knowledge evolution over a stable graph.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    AdoptOntologyRequest, AssertionGraphRefInput, AssertionGraphRole, AssertionStatus,
    AssessConfidenceRequest, BeliefProjectionPolicyV1, CapabilityId, CheckpointDiffDetail,
    CheckpointDiffScope, CheckpointRequest, CheckpointSelector, ConfidencePolicyRequest,
    CreateAssertionRequest, CreateAssertionWithEvidenceRequest, CreateHypothesisGroupRequest,
    DiffCheckpointsRequest, EnableCapabilityRequest, EvidenceInput, EvidenceRole,
    EvidenceSourceKind, FindOptions, GraphForge, GraphObjectKind, HypothesisMembershipAction,
    HypothesisSelectionPolicyV1, OperationId, PageRequest, PathAlgorithm, PathsOptions, PropValue,
    RankAlgorithm, RankOptions, ReasoningContentFormat, ReasoningKind,
    RecordAssertionStatusRequest, RecordAssertionValidityRequest,
    RecordHypothesisMembershipRequest, RecordHypothesisSelectionRequest, RecordReasoningRequest,
    ResolveBeliefProjectionRequest, SearchIndexOptions, StatuslessPolicyV1,
    SupersessionBranchPolicyV1, WriteContext,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

const BUNDLE: &str = "../../tests/release_workflows/knowledge-evolution";

fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(BUNDLE)
        .join(relative)
}

fn id(suffix: u16) -> Uuid {
    Uuid::parse_str(&format!("018f0f4e-7b8c-7000-8000-00000004{suffix:04x}")).unwrap()
}

fn context(suffix: u16) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(id(suffix)),
        actor_uuid: Some(id(0xfffe)),
    }
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

fn recorded_at(result: &graphforge_api::ExecutionResult) -> i64 {
    result.batches[0]
        .column_by_name("recorded_at")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
        .value(0)
}

fn hex_bytes(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn batch_fingerprint(batches: &[RecordBatch]) -> String {
    let batches = batches
        .iter()
        .map(|batch| {
            let schema = batch.schema();
            let fields = schema
                .fields()
                .iter()
                .map(|field| {
                    json!({
                        "name": field.name(),
                        "data_type": format!("{:?}", field.data_type()),
                        "nullable": field.is_nullable(),
                        "metadata": field.metadata().iter().collect::<BTreeMap<_, _>>(),
                    })
                })
                .collect::<Vec<_>>();
            let mut rows = Vec::new();
            {
                let mut writer = arrow::json::WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, arrow::json::writer::JsonArray>(&mut rows);
                writer.write(batch).unwrap();
                writer.finish().unwrap();
            }
            json!({
                "schema": {
                    "fields": fields,
                    "metadata": schema
                        .metadata()
                        .iter()
                        .filter(|(key, _)| key.as_str() != "graphforge.query_id")
                        .collect::<BTreeMap<_, _>>(),
                },
                "rows": serde_json::from_slice::<serde_json::Value>(&rows).unwrap(),
            })
        })
        .collect::<Vec<_>>();
    let canonical = json!({
        "contract": "canonical-json-from-arrow-values/1",
        "batches": batches,
    });
    hex_bytes(Sha256::digest(serde_json::to_vec(&canonical).unwrap()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{FixedSizeBinaryBuilder, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(value: i64, query_id: &str, stable_metadata: &str) -> RecordBatch {
        let mut uuids = FixedSizeBinaryBuilder::with_capacity(2, 16);
        uuids.append_value([1; 16]).unwrap();
        uuids.append_null();
        let metadata = HashMap::from([
            ("graphforge.query_id".into(), query_id.into()),
            ("graphforge.ir_version".into(), stable_metadata.into()),
        ]);
        RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
                    Field::new("score", DataType::Float64, false),
                    Field::new("value", DataType::Int64, true),
                ],
                metadata,
            )),
            vec![
                Arc::new(uuids.finish()),
                Arc::new(Float64Array::from(vec![1.25, -0.0])),
                Arc::new(Int64Array::from(vec![Some(value), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn fingerprint_uses_logical_arrow_values_not_allocations() {
        assert_eq!(
            batch_fingerprint(&[batch(7, "query-a", "1")]),
            batch_fingerprint(&[batch(7, "query-b", "1")])
        );
        assert_ne!(
            batch_fingerprint(&[batch(7, "query-a", "1")]),
            batch_fingerprint(&[batch(7, "query-a", "2")])
        );
        assert_ne!(
            batch_fingerprint(&[batch(7, "query-a", "1")]),
            batch_fingerprint(&[batch(8, "query-a", "1")])
        );
    }
}

fn main() {
    let sha = std::env::var("GF_KNOWLEDGE_EVOLUTION_SHA").expect("workflow SHA required");
    let evidence_path = PathBuf::from(
        std::env::var("GF_KNOWLEDGE_EVOLUTION_EVIDENCE").expect("evidence path required"),
    );
    let project = TempDir::new().unwrap();
    let root = project.path().to_str().unwrap();
    let mut graph = GraphForge::new(Some(root)).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(1),
            path: fixture("ontologies/strict-v1.yaml"),
            mode: graphforge_api::OntologyMode::Strict,
        })
        .unwrap();
    for (capability, suffix) in [
        (CapabilityId::Provenance, 2),
        (CapabilityId::Knowledge, 3),
        (CapabilityId::Epistemic, 4),
        (CapabilityId::ValidTime, 5),
    ] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(suffix),
                capability_id: capability,
                capability_version: 1,
            })
            .unwrap();
    }
    let alpha = graph
        .add_node(
            "Observation",
            &HashMap::from([
                ("name".into(), PropValue::Str("Observation Alpha".into())),
                ("summary".into(), PropValue::Str("Stable graph".into())),
            ]),
        )
        .unwrap();
    let beta = graph
        .add_node(
            "Observation",
            &HashMap::from([
                ("name".into(), PropValue::Str("Observation Beta".into())),
                ("summary".into(), PropValue::Str("Stable graph".into())),
            ]),
        )
        .unwrap();
    let source = graph
        .add_node(
            "Document",
            &HashMap::from([
                ("name".into(), PropValue::Str("Source Record".into())),
                ("body".into(), PropValue::Str("alpha beta evidence".into())),
            ]),
        )
        .unwrap();
    graph
        .add_edge(&alpha, "SUPPORTED_BY", &source, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&beta, "SUPPORTED_BY", &source, &HashMap::new())
        .unwrap();
    graph
        .index_search(
            "Document",
            SearchIndexOptions::Text {
                properties: Some(vec!["body".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "stable-graph".into(),
            description: Some("graph and ontology baseline".into()),
            idempotency_key: OperationId(id(10)),
            actor_uuid: Some(id(0xfffe)),
        })
        .unwrap();

    let neutral_query = graph
        .execute("MATCH (o:Observation) RETURN o.node_uuid AS node_uuid ORDER BY node_uuid")
        .unwrap();
    let neutral_find = graph
        .find(FindOptions {
            query: Some("evidence".into()),
            label: Some("Document".into()),
            limit: 5,
            ..FindOptions::default()
        })
        .unwrap();
    let neutral_rank = graph
        .rank(
            "Observation",
            RankOptions {
                by: RankAlgorithm::Degree,
                via: Some("SUPPORTED_BY".into()),
                directed: false,
                write_property: None,
            },
        )
        .unwrap();
    let neutral_paths = graph
        .paths(
            Some(&graphforge_api::NodeSelector::Handle(alpha.clone())),
            Some(&graphforge_api::NodeSelector::Handle(source.clone())),
            PathsOptions {
                by: PathAlgorithm::Bfs,
                via: Some("SUPPORTED_BY".into()),
                directed: true,
                ..PathsOptions::default()
            },
        )
        .unwrap();
    let neutral_before = [
        batch_fingerprint(&neutral_query.batches),
        batch_fingerprint(std::slice::from_ref(&neutral_find)),
        batch_fingerprint(std::slice::from_ref(&neutral_rank)),
        batch_fingerprint(std::slice::from_ref(&neutral_paths)),
    ];

    let mut records = Vec::new();
    for (base, claim, node) in [
        (0x100, "Alpha explains the observation", &alpha),
        (0x110, "Beta explains the observation", &beta),
    ] {
        let assertion_uuid = id(base);
        let created = graph
            .create_assertion_with_evidence(CreateAssertionWithEvidenceRequest {
                assertion: CreateAssertionRequest {
                    context: context(base + 1),
                    assertion_uuid,
                    claim: claim.into(),
                    graph_refs: vec![AssertionGraphRefInput {
                        graph_uuid: node.uuid,
                        graph_kind: GraphObjectKind::Node,
                        role: AssertionGraphRole::Subject,
                        ordinal: 0,
                    }],
                },
                evidence: vec![EvidenceInput {
                    evidence_uuid: id(base + 2),
                    source_uuid: source.uuid,
                    source_kind: EvidenceSourceKind::GraphNode,
                    role: EvidenceRole::Supports,
                    weight: Some(0.5),
                }],
            })
            .unwrap();
        let prov = provenance(&created);
        graph
            .assess_confidence(AssessConfidenceRequest {
                context: context(base + 3),
                confidence_uuid: id(base + 4),
                assertion_uuid,
                policy: ConfidencePolicyRequest::Explicit {
                    value: if base == 0x100 { 0.9 } else { 0.4 },
                },
            })
            .unwrap();
        graph
            .record_reasoning(RecordReasoningRequest {
                context: context(base + 5),
                reasoning_uuid: id(base + 6),
                assertion_uuid,
                kind: ReasoningKind::EvidenceInterpretation,
                content_format: ReasoningContentFormat::TextPlain,
                content: claim.as_bytes().to_vec(),
                supersedes_reasoning_uuid: None,
                provenance_uuid: prov,
            })
            .unwrap();
        let status = graph
            .record_assertion_status(RecordAssertionStatusRequest {
                context: context(base + 7),
                status_event_uuid: id(base + 8),
                assertion_uuid,
                status: AssertionStatus::Hypothesis,
                confidence_uuid: Some(id(base + 4)),
                reasoning_uuid: Some(id(base + 6)),
                provenance_uuid: prov,
            })
            .unwrap();
        graph
            .record_assertion_validity(RecordAssertionValidityRequest {
                context: context(base + 9),
                validity_event_uuid: id(base + 10),
                assertion_uuid,
                valid_from_micros: Some(100),
                valid_to_micros: Some(200),
                reasoning_uuid: Some(id(base + 6)),
                provenance_uuid: prov,
            })
            .unwrap();
        records.push((assertion_uuid, id(base + 6), prov, recorded_at(&status)));
    }
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(0x180),
            reasoning_uuid: id(0x181),
            assertion_uuid: records[0].0,
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"Alpha remains plausible, but the evidence is not dispositive.".to_vec(),
            supersedes_reasoning_uuid: Some(records[0].1),
            provenance_uuid: records[0].2,
        })
        .unwrap();
    records[0].1 = id(0x181);
    // A higher confidence score must not select either unresolved alternative.
    let group = id(0x200);
    graph
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(0x201),
            group_uuid: group,
            question_key: "knowledge-evolution.explanation.v1".into(),
            provenance_uuid: records[0].2,
        })
        .unwrap();
    let mut cutoff_unselected = i64::MIN;
    for (index, (assertion, reasoning, prov, _)) in records.iter().enumerate() {
        let membership = graph
            .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
                context: context(0x210 + index as u16),
                membership_event_uuid: id(0x220 + index as u16),
                group_uuid: group,
                assertion_uuid: *assertion,
                action: HypothesisMembershipAction::Added,
                reasoning_uuid: *reasoning,
                provenance_uuid: *prov,
            })
            .unwrap();
        cutoff_unselected = cutoff_unselected.max(recorded_at(&membership));
    }
    assert_eq!(
        graph.hypothesis_selection(group).unwrap().batches[0].num_rows(),
        0
    );
    let unselected = graph.epistemic_snapshot(cutoff_unselected).unwrap();
    for (index, selected) in [Some(records[0].0), Some(records[1].0), None]
        .into_iter()
        .enumerate()
    {
        let supporting = &records[index.min(1)];
        graph
            .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
                context: context(0x230 + index as u16),
                selection_event_uuid: id(0x240 + index as u16),
                group_uuid: group,
                selected_assertion_uuid: selected,
                reasoning_uuid: supporting.1,
                provenance_uuid: supporting.2,
            })
            .unwrap();
    }
    assert_eq!(
        graph.hypothesis_selection(group).unwrap().batches[0].num_rows(),
        1
    );
    let current_selection = graph.hypothesis_selection(group).unwrap();
    assert!(
        current_selection.batches[0]
            .column_by_name("selected_assertion_uuid")
            .unwrap()
            .is_null(0)
    );

    let policy = BeliefProjectionPolicyV1 {
        included_statuses: vec![AssertionStatus::Hypothesis],
        statusless: StatuslessPolicyV1::Exclude,
        supersession_branches: SupersessionBranchPolicyV1::IncludeAllLeaves,
        hypotheses: HypothesisSelectionPolicyV1::IncludeAllCurrentMembers,
    };
    let projection = graph
        .resolve_belief_projection(ResolveBeliefProjectionRequest {
            transaction_cutoff_micros: cutoff_unselected,
            valid_time_micros: Some(150),
            policy,
        })
        .unwrap();
    let projection_fingerprint = hex_bytes(projection.graph_content_fingerprint());
    assert_eq!(
        projection.source_record_uuids(),
        &[
            id(0x100),
            id(0x104),
            id(0x106),
            id(0x108),
            id(0x110),
            id(0x114),
            id(0x116),
            id(0x118),
            id(0x181),
            id(0x200),
            id(0x220),
            id(0x221),
        ],
        "belief projection must cite every decision-relevant epistemic record at the cutoff"
    );
    let valid = graph
        .apply_valid_time(graphforge_api::ApplyValidTimeRequest {
            transaction_cutoff_micros: cutoff_unselected,
            valid_time_micros: 150,
        })
        .unwrap();
    assert_eq!(valid.batches[0].num_rows(), 2);

    let after_query = graph
        .execute("MATCH (o:Observation) RETURN o.node_uuid AS node_uuid ORDER BY node_uuid")
        .unwrap();
    let after_find = graph
        .find(FindOptions {
            query: Some("evidence".into()),
            label: Some("Document".into()),
            limit: 5,
            ..FindOptions::default()
        })
        .unwrap();
    let after_rank = graph
        .rank(
            "Observation",
            RankOptions {
                by: RankAlgorithm::Degree,
                via: Some("SUPPORTED_BY".into()),
                directed: false,
                write_property: None,
            },
        )
        .unwrap();
    let after_paths = graph
        .paths(
            Some(&graphforge_api::NodeSelector::Handle(alpha)),
            Some(&graphforge_api::NodeSelector::Handle(source)),
            PathsOptions {
                by: PathAlgorithm::Bfs,
                via: Some("SUPPORTED_BY".into()),
                directed: true,
                ..PathsOptions::default()
            },
        )
        .unwrap();
    let neutral_after = [
        batch_fingerprint(&after_query.batches),
        batch_fingerprint(std::slice::from_ref(&after_find)),
        batch_fingerprint(std::slice::from_ref(&after_rank)),
        batch_fingerprint(std::slice::from_ref(&after_paths)),
    ];
    assert_eq!(neutral_before, neutral_after);
    for scope in [CheckpointDiffScope::Graph, CheckpointDiffScope::Ontology] {
        let diff = graph
            .diff_checkpoints(DiffCheckpointsRequest {
                from: CheckpointSelector::Named("stable-graph".into()),
                to: CheckpointSelector::Current,
                scope,
                detail: CheckpointDiffDetail::Records,
                page: PageRequest::default(),
            })
            .unwrap();
        assert_eq!(diff.stats.rows_produced, 0);
    }
    let ontology_fingerprint = graph
        .workspace_ontology()
        .unwrap()
        .canonical_ontology_sha256
        .unwrap();
    let graph_fingerprint = hex_bytes(Sha256::digest(neutral_before.concat()));
    drop(graph);
    let reopened = GraphForge::new(Some(root)).unwrap();
    let reopen_equal = reopened
        .epistemic_snapshot(cutoff_unselected)
        .unwrap()
        .batches
        == unselected.batches
        && reopened.hypothesis_selection(group).unwrap().batches == current_selection.batches;
    let reopened_projection = reopened
        .resolve_belief_projection(ResolveBeliefProjectionRequest {
            transaction_cutoff_micros: cutoff_unselected,
            valid_time_micros: Some(150),
            policy: BeliefProjectionPolicyV1 {
                included_statuses: vec![AssertionStatus::Hypothesis],
                statusless: StatuslessPolicyV1::Exclude,
                supersession_branches: SupersessionBranchPolicyV1::IncludeAllLeaves,
                hypotheses: HypothesisSelectionPolicyV1::IncludeAllCurrentMembers,
            },
        })
        .unwrap();
    assert_eq!(
        hex_bytes(reopened_projection.graph_content_fingerprint()),
        projection_fingerprint
    );
    let evidence = json!({
        "schema_version":1,"scenario_id":"knowledge-evolution","commit_sha":sha,
        "graph_fingerprint":graph_fingerprint,"ontology_fingerprint":ontology_fingerprint,
        "neutral":{"before":neutral_before,"after":neutral_after,"identical":true},
        "knowledge":{"confidence_selected_implicitly":false,"selection_events":3,"current_selection":null,"projection_fingerprint":projection_fingerprint},
        "cutoffs":{"unselected":cutoff_unselected,"valid_time":150,"snapshot_rows":unselected.stats.rows_produced},
        "reopen_equal":reopen_equal
    });
    fs::write(evidence_path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
}
