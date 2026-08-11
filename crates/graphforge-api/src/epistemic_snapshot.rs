//! Deterministic transaction-time composition of append-only epistemic records.

use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    FixedSizeBinaryBuilder, ListBuilder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_knowledge::{
    AssertionLedger, AssertionStatusLedger, AssertionSupersessionLedger, ConfidenceLedger,
    HypothesisLedger, ReasoningLedger,
};
use uuid::Uuid;

use crate::GraphForge;

/// Frozen transaction-time snapshot resolution policy.
pub const EPISTEMIC_SNAPSHOT_POLICY_VERSION: u32 = 1;
const POLICY: &str = "graphforge-epistemic-snapshot/1";

static SNAPSHOT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let uuid_list = || {
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::FixedSizeBinary(16),
            false,
        )))
    };
    Arc::new(Schema::new(vec![
        Field::new("entity_kind", DataType::Utf8, false),
        Field::new("assertion_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("group_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("question_key", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("status_event_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("reasoning_history_uuids", uuid_list(), false),
        Field::new("reasoning_leaf_uuids", uuid_list(), false),
        Field::new("superseded_by_assertion_uuids", uuid_list(), false),
        Field::new("current_member_assertion_uuids", uuid_list(), false),
        Field::new(
            "selected_assertion_uuid",
            DataType::FixedSizeBinary(16),
            true,
        ),
        Field::new("source_record_uuids", uuid_list(), false),
        Field::new(
            "transaction_cutoff",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("resolution_policy", DataType::Utf8, false),
        Field::new("snapshot_fingerprint", DataType::FixedSizeBinary(32), false),
    ]))
});

#[derive(Debug)]
struct SnapshotRow {
    entity_kind: &'static str,
    assertion_uuid: Option<Uuid>,
    group_uuid: Option<Uuid>,
    question_key: Option<String>,
    status: Option<&'static str>,
    status_event_uuid: Option<Uuid>,
    reasoning_history: Vec<Uuid>,
    reasoning_leaves: Vec<Uuid>,
    superseded_by: Vec<Uuid>,
    current_members: Vec<Uuid>,
    selected_assertion_uuid: Option<Uuid>,
    sources: Vec<Uuid>,
}

impl GraphForge {
    /// Reconstruct one deterministic epistemic view at transaction-time `cutoff_micros`.
    ///
    /// The result contains one row per visible assertion followed by one row per
    /// visible hypothesis group. Statusless assertions and unselected/empty
    /// groups remain explicit. No current-state cache participates.
    pub fn epistemic_snapshot(
        &self,
        cutoff_micros: i64,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        let generation = self.generation_for_read()?;
        let assertions = crate::knowledge::read_ledger(&generation)?;
        let statuses = crate::knowledge::read_status_ledger(&generation)?;
        let reasoning = crate::knowledge::read_reasoning_ledger(&generation)?;
        let supersessions = crate::knowledge::read_supersession_ledger(&generation)?;
        let hypotheses = crate::hypotheses::read_ledger(&generation)?;
        let confidence = crate::knowledge::read_confidence_ledger(&generation)?;
        let rows = compose_rows(
            cutoff_micros,
            assertions,
            statuses,
            reasoning,
            &supersessions,
            &hypotheses,
            &confidence,
        )?;
        let preliminary = build_batch(&rows, cutoff_micros, [0; 32], false)?;
        let content_columns = preliminary.num_columns() - 1;
        let content_schema = Arc::new(Schema::new(
            preliminary.schema().fields()[..content_columns].to_vec(),
        ));
        let content = RecordBatch::try_new(
            content_schema,
            preliminary.columns()[..content_columns].to_vec(),
        )
        .map_err(|error| GfError::Execution(error.to_string()))?;
        let fingerprint = crate::canonical_arrow::result_fingerprint(&[content])
            .map_err(|error| GfError::Execution(error.to_string()))?;
        let batch = build_batch(&rows, cutoff_micros, fingerprint, true)?;
        Ok(crate::knowledge::assertion_result(batch))
    }
}

#[allow(clippy::too_many_lines)]
fn compose_rows(
    cutoff: i64,
    assertions: AssertionLedger,
    statuses: AssertionStatusLedger,
    reasoning: ReasoningLedger,
    supersessions: &AssertionSupersessionLedger,
    hypotheses: &HypothesisLedger,
    confidence: &ConfidenceLedger,
) -> Result<Vec<SnapshotRow>, GfError> {
    let assertion_ids_at_cutoff = assertions
        .assertions
        .iter()
        .filter(|row| row.recorded_at_micros <= cutoff)
        .map(|row| row.assertion_uuid)
        .collect::<HashSet<_>>();
    let assertions = AssertionLedger::new(
        assertions
            .assertions
            .into_iter()
            .filter(|row| row.recorded_at_micros <= cutoff)
            .collect(),
        assertions
            .graph_refs
            .into_iter()
            .filter(|row| assertion_ids_at_cutoff.contains(&row.assertion_uuid))
            .collect(),
    )
    .map_err(crate::knowledge::knowledge_error)?;
    let visible_assertions = assertions
        .assertions
        .iter()
        .map(|row| row.assertion_uuid)
        .collect::<HashSet<_>>();
    let statuses = AssertionStatusLedger::new(
        statuses
            .events
            .into_iter()
            .filter(|row| row.recorded_at_micros <= cutoff)
            .collect(),
    )
    .map_err(crate::knowledge::knowledge_error)?;
    let reasoning = ReasoningLedger::new(
        reasoning
            .records
            .into_iter()
            .filter(|row| row.recorded_at_micros <= cutoff)
            .collect(),
    )
    .map_err(crate::knowledge::knowledge_error)?;
    let supersessions = AssertionSupersessionLedger::new(
        supersessions
            .relations()
            .iter()
            .filter(|row| row.recorded_at_micros <= cutoff)
            .cloned()
            .collect(),
    )
    .map_err(crate::knowledge::knowledge_error)?;
    let hypotheses = HypothesisLedger::new(
        hypotheses
            .groups()
            .iter()
            .filter(|row| row.recorded_at_micros <= cutoff)
            .cloned()
            .collect(),
        hypotheses
            .membership_events()
            .iter()
            .filter(|row| row.recorded_at_micros <= cutoff)
            .cloned()
            .collect(),
        hypotheses
            .selection_events()
            .iter()
            .filter(|row| row.recorded_at_micros <= cutoff)
            .cloned()
            .collect(),
    )
    .map_err(crate::knowledge::knowledge_error)?;

    let visible_reasoning = reasoning
        .records
        .iter()
        .map(|row| row.reasoning_uuid)
        .collect::<HashSet<_>>();
    let visible_statuses = statuses
        .events
        .iter()
        .map(|row| row.status_event_uuid)
        .collect::<HashSet<_>>();
    let visible_confidence = confidence
        .assessments
        .iter()
        .filter(|row| row.recorded_at_micros <= cutoff)
        .map(|row| row.confidence_uuid)
        .collect::<HashSet<_>>();
    for source in statuses
        .events
        .iter()
        .map(|row| row.assertion_uuid)
        .chain(reasoning.records.iter().map(|row| row.assertion_uuid))
    {
        if !visible_assertions.contains(&source) {
            return Err(GfError::Validation(
                "epistemic event references an assertion not visible at the cutoff".into(),
            ));
        }
    }
    for event in &statuses.events {
        if event
            .confidence_uuid
            .is_some_and(|uuid| !visible_confidence.contains(&uuid))
        {
            return Err(dangling_at_cutoff("status confidence"));
        }
        if event
            .reasoning_uuid
            .is_some_and(|uuid| !visible_reasoning.contains(&uuid))
        {
            return Err(dangling_at_cutoff("status reasoning"));
        }
    }
    for relation in supersessions.relations() {
        if !visible_assertions.contains(&relation.prior_assertion_uuid)
            || !visible_assertions.contains(&relation.replacement_assertion_uuid)
        {
            return Err(dangling_at_cutoff("supersession assertion"));
        }
        if !visible_statuses.contains(&relation.status_event_uuid) {
            return Err(dangling_at_cutoff("supersession status"));
        }
        if !visible_reasoning.contains(&relation.reasoning_uuid) {
            return Err(dangling_at_cutoff("supersession reasoning"));
        }
    }
    for event in hypotheses.membership_events() {
        if !visible_assertions.contains(&event.assertion_uuid) {
            return Err(dangling_at_cutoff("hypothesis membership assertion"));
        }
        if !visible_reasoning.contains(&event.reasoning_uuid) {
            return Err(dangling_at_cutoff("hypothesis membership reasoning"));
        }
    }
    for event in hypotheses.selection_events() {
        if event
            .selected_assertion_uuid
            .is_some_and(|uuid| !visible_assertions.contains(&uuid))
        {
            return Err(dangling_at_cutoff("hypothesis selection assertion"));
        }
        if !visible_reasoning.contains(&event.reasoning_uuid) {
            return Err(dangling_at_cutoff("hypothesis selection reasoning"));
        }
    }

    let mut rows = Vec::with_capacity(assertions.assertions.len() + hypotheses.groups().len());
    for assertion in &assertions.assertions {
        let status = statuses.current_for(assertion.assertion_uuid);
        let history = reasoning
            .records
            .iter()
            .filter(|row| row.assertion_uuid == assertion.assertion_uuid)
            .map(|row| row.reasoning_uuid)
            .collect::<Vec<_>>();
        let superseded_reasoning = reasoning
            .records
            .iter()
            .filter(|row| row.assertion_uuid == assertion.assertion_uuid)
            .filter_map(|row| row.supersedes_reasoning_uuid)
            .collect::<HashSet<_>>();
        let leaves = history
            .iter()
            .filter(|uuid| !superseded_reasoning.contains(uuid))
            .copied()
            .collect::<Vec<_>>();
        let superseded_by = supersessions
            .relations()
            .iter()
            .filter(|row| row.prior_assertion_uuid == assertion.assertion_uuid)
            .map(|row| row.replacement_assertion_uuid)
            .collect::<Vec<_>>();
        let mut sources = BTreeSet::from([assertion.assertion_uuid]);
        sources.extend(
            statuses
                .events
                .iter()
                .filter(|row| row.assertion_uuid == assertion.assertion_uuid)
                .map(|row| row.status_event_uuid),
        );
        for event in statuses
            .events
            .iter()
            .filter(|row| row.assertion_uuid == assertion.assertion_uuid)
        {
            sources.extend(event.confidence_uuid);
            sources.extend(event.reasoning_uuid);
        }
        sources.extend(history.iter().copied());
        sources.extend(
            supersessions
                .relations()
                .iter()
                .filter(|row| {
                    row.prior_assertion_uuid == assertion.assertion_uuid
                        || row.replacement_assertion_uuid == assertion.assertion_uuid
                })
                .map(|row| row.supersession_uuid),
        );
        rows.push(SnapshotRow {
            entity_kind: "assertion",
            assertion_uuid: Some(assertion.assertion_uuid),
            group_uuid: None,
            question_key: None,
            status: status.map(|row| row.status.as_str()),
            status_event_uuid: status.map(|row| row.status_event_uuid),
            reasoning_history: history,
            reasoning_leaves: leaves,
            superseded_by,
            current_members: Vec::new(),
            selected_assertion_uuid: None,
            sources: sources.into_iter().collect(),
        });
    }
    for group in hypotheses.groups() {
        let members = hypotheses.current_members(group.group_uuid);
        let selected = hypotheses.current_selection(group.group_uuid);
        let mut sources = BTreeSet::from([group.group_uuid]);
        sources.extend(
            hypotheses
                .membership_events()
                .iter()
                .filter(|row| row.group_uuid == group.group_uuid)
                .map(|row| row.membership_event_uuid),
        );
        sources.extend(
            hypotheses
                .selection_events()
                .iter()
                .filter(|row| row.group_uuid == group.group_uuid)
                .map(|row| row.selection_event_uuid),
        );
        rows.push(SnapshotRow {
            entity_kind: "hypothesis_group",
            assertion_uuid: None,
            group_uuid: Some(group.group_uuid),
            question_key: Some(group.question_key.clone()),
            status: None,
            status_event_uuid: None,
            reasoning_history: Vec::new(),
            reasoning_leaves: Vec::new(),
            superseded_by: Vec::new(),
            current_members: members,
            selected_assertion_uuid: selected,
            sources: sources.into_iter().collect(),
        });
    }
    Ok(rows)
}

fn build_batch(
    rows: &[SnapshotRow],
    cutoff: i64,
    fingerprint: [u8; 32],
    include_metadata: bool,
) -> Result<RecordBatch, GfError> {
    let mut entity_kinds = StringBuilder::new();
    let mut assertion_ids = FixedSizeBinaryBuilder::new(16);
    let mut group_ids = FixedSizeBinaryBuilder::new(16);
    let mut question_keys = StringBuilder::new();
    let mut statuses = StringBuilder::new();
    let mut status_ids = FixedSizeBinaryBuilder::new(16);
    let mut reasoning_history = uuid_list_builder();
    let mut reasoning_leaves = uuid_list_builder();
    let mut superseded_by = uuid_list_builder();
    let mut current_members = uuid_list_builder();
    let mut selected_ids = FixedSizeBinaryBuilder::new(16);
    let mut sources = uuid_list_builder();
    let mut cutoffs = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut policies = StringBuilder::new();
    let mut fingerprints = FixedSizeBinaryBuilder::new(32);
    for row in rows {
        entity_kinds.append_value(row.entity_kind);
        append_optional_uuid(&mut assertion_ids, row.assertion_uuid)?;
        append_optional_uuid(&mut group_ids, row.group_uuid)?;
        match &row.question_key {
            Some(value) => question_keys.append_value(value),
            None => question_keys.append_null(),
        }
        match row.status {
            Some(value) => statuses.append_value(value),
            None => statuses.append_null(),
        }
        append_optional_uuid(&mut status_ids, row.status_event_uuid)?;
        append_uuid_list(&mut reasoning_history, &row.reasoning_history)?;
        append_uuid_list(&mut reasoning_leaves, &row.reasoning_leaves)?;
        append_uuid_list(&mut superseded_by, &row.superseded_by)?;
        append_uuid_list(&mut current_members, &row.current_members)?;
        append_optional_uuid(&mut selected_ids, row.selected_assertion_uuid)?;
        append_uuid_list(&mut sources, &row.sources)?;
        cutoffs.append_value(cutoff);
        policies.append_value(POLICY);
        fingerprints
            .append_value(fingerprint)
            .map_err(|error| GfError::Execution(error.to_string()))?;
    }
    let schema = if include_metadata {
        Arc::new(Schema::new_with_metadata(
            SNAPSHOT_SCHEMA.fields().to_vec(),
            [
                ("graphforge.snapshot_policy".into(), POLICY.into()),
                (
                    "graphforge.snapshot_fingerprint".into(),
                    encode_hex(fingerprint),
                ),
            ]
            .into_iter()
            .collect(),
        ))
    } else {
        Arc::clone(&SNAPSHOT_SCHEMA)
    };
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(entity_kinds.finish()),
            Arc::new(assertion_ids.finish()),
            Arc::new(group_ids.finish()),
            Arc::new(question_keys.finish()),
            Arc::new(statuses.finish()),
            Arc::new(status_ids.finish()),
            Arc::new(reasoning_history.finish()),
            Arc::new(reasoning_leaves.finish()),
            Arc::new(superseded_by.finish()),
            Arc::new(current_members.finish()),
            Arc::new(selected_ids.finish()),
            Arc::new(sources.finish()),
            Arc::new(cutoffs.finish()),
            Arc::new(policies.finish()),
            Arc::new(fingerprints.finish()),
        ],
    )
    .map_err(|error| GfError::Execution(error.to_string()))
}

fn encode_hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write;

    bytes
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

fn dangling_at_cutoff(kind: &'static str) -> GfError {
    GfError::Validation(format!(
        "epistemic snapshot has a dangling or future {kind} reference"
    ))
}

fn uuid_list_builder() -> ListBuilder<FixedSizeBinaryBuilder> {
    ListBuilder::new(FixedSizeBinaryBuilder::new(16)).with_field(Arc::new(Field::new(
        "item",
        DataType::FixedSizeBinary(16),
        false,
    )))
}

fn append_uuid_list(
    builder: &mut ListBuilder<FixedSizeBinaryBuilder>,
    values: &[Uuid],
) -> Result<(), GfError> {
    for value in values {
        builder
            .values()
            .append_value(value.as_bytes())
            .map_err(|error| GfError::Execution(error.to_string()))?;
    }
    builder.append(true);
    Ok(())
}

fn append_optional_uuid(
    builder: &mut FixedSizeBinaryBuilder,
    value: Option<Uuid>,
) -> Result<(), GfError> {
    if let Some(value) = value {
        builder
            .append_value(value.as_bytes())
            .map_err(|error| GfError::Execution(error.to_string()))
    } else {
        builder.append_null();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::{Array, FixedSizeBinaryArray, ListArray, StringArray};

    use super::*;
    use crate::{
        AssertionGraphRefInput, AssessConfidenceRequest, CapabilityId, ConfidencePolicyRequest,
        CreateAssertionRequest, CreateHypothesisGroupRequest, EnableCapabilityRequest, OperationId,
        RecordAssertionStatusRequest, RecordHypothesisMembershipRequest,
        RecordHypothesisSelectionRequest, RecordReasoningRequest, WriteContext,
    };
    use graphforge_knowledge::{
        AssertionGraphRole, AssertionStatus, GraphObjectKind, HypothesisMembershipAction,
        ReasoningContentFormat, ReasoningKind,
    };

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn context(seed: u8) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(uuid7(seed)),
            actor_uuid: None,
        }
    }

    fn enable(graph: &GraphForge, capability_id: CapabilityId, seed: u8) {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(seed),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }

    #[test]
    fn public_snapshot_is_cutoff_stable_branch_preserving_and_reopen_deterministic() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(10));
        enable(&graph, CapabilityId::Provenance, 1);
        enable(&graph, CapabilityId::Knowledge, 2);
        enable(&graph, CapabilityId::Epistemic, 3);

        let node = graph.add_node("Subject", &HashMap::new()).unwrap();
        let assertion_uuid = uuid7(10);
        let assertion = graph
            .create_assertion(CreateAssertionRequest {
                context: context(11),
                assertion_uuid,
                claim: "statusless until explicitly interpreted".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap();
        let provenance = assertion.batches[0]
            .column_by_name("provenance_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let provenance_uuid = Uuid::from_slice(provenance.value(0)).unwrap();
        let statusless_node = graph.add_node("Subject", &HashMap::new()).unwrap();
        let statusless_assertion_uuid = uuid7(30);
        graph
            .create_assertion(CreateAssertionRequest {
                context: context(31),
                assertion_uuid: statusless_assertion_uuid,
                claim: "intentionally statusless".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: statusless_node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap();
        let confidence_uuid = uuid7(35);
        graph
            .assess_confidence(AssessConfidenceRequest {
                context: context(34),
                confidence_uuid,
                assertion_uuid,
                policy: ConfidencePolicyRequest::Explicit { value: 0.5 },
            })
            .unwrap();
        graph
            .record_assertion_status(RecordAssertionStatusRequest {
                context: context(12),
                status_event_uuid: uuid7(13),
                assertion_uuid,
                status: AssertionStatus::Hypothesis,
                confidence_uuid: Some(confidence_uuid),
                reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap();
        let base_reasoning = uuid7(14);
        for (operation, reasoning_uuid, predecessor, content) in [
            (15, base_reasoning, None, b"base".as_slice()),
            (16, uuid7(17), Some(base_reasoning), b"branch a".as_slice()),
            (18, uuid7(19), Some(base_reasoning), b"branch b".as_slice()),
        ] {
            graph
                .record_reasoning(RecordReasoningRequest {
                    context: context(operation),
                    reasoning_uuid,
                    assertion_uuid,
                    kind: ReasoningKind::DecisionRationale,
                    content_format: ReasoningContentFormat::TextPlain,
                    content: content.to_vec(),
                    supersedes_reasoning_uuid: predecessor,
                    provenance_uuid,
                })
                .unwrap();
        }
        let group_uuid = uuid7(20);
        graph
            .create_hypothesis_group(CreateHypothesisGroupRequest {
                context: context(21),
                group_uuid,
                question_key: "snapshot.primary.v1".into(),
                provenance_uuid,
            })
            .unwrap();
        graph
            .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
                context: context(22),
                membership_event_uuid: uuid7(23),
                group_uuid,
                assertion_uuid,
                action: HypothesisMembershipAction::Added,
                reasoning_uuid: base_reasoning,
                provenance_uuid,
            })
            .unwrap();
        graph
            .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
                context: context(24),
                selection_event_uuid: uuid7(25),
                group_uuid,
                selected_assertion_uuid: Some(assertion_uuid),
                reasoning_uuid: base_reasoning,
                provenance_uuid,
            })
            .unwrap();
        graph
            .create_hypothesis_group(CreateHypothesisGroupRequest {
                context: context(32),
                group_uuid: uuid7(33),
                question_key: "snapshot.unselected.v1".into(),
                provenance_uuid,
            })
            .unwrap();

        let before_late_arrival = graph.epistemic_snapshot(10).unwrap();
        assert_eq!(before_late_arrival.batches[0].num_rows(), 4);
        let kinds = before_late_arrival.batches[0]
            .column_by_name("entity_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(kinds.value(0), "assertion");
        assert_eq!(kinds.value(1), "assertion");
        assert_eq!(kinds.value(2), "hypothesis_group");
        assert_eq!(kinds.value(3), "hypothesis_group");
        assert!(
            before_late_arrival.batches[0]
                .column_by_name("status")
                .unwrap()
                .is_null(1),
            "statusless assertions remain explicit"
        );
        let leaves = before_late_arrival.batches[0]
            .column_by_name("reasoning_leaf_uuids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            leaves.value(0).len(),
            2,
            "reasoning branches remain explicit"
        );
        let members = before_late_arrival.batches[0]
            .column_by_name("current_member_assertion_uuids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(members.value(3).len(), 0, "empty groups remain explicit");
        assert!(
            before_late_arrival.batches[0]
                .column_by_name("selected_assertion_uuid")
                .unwrap()
                .is_null(3),
            "unselected groups remain explicit"
        );
        let first_fingerprint = before_late_arrival
            .schema
            .metadata()
            .get("graphforge.snapshot_fingerprint")
            .unwrap()
            .clone();

        graph.set_clock_for_test(|| Ok(20));
        graph
            .record_assertion_status(RecordAssertionStatusRequest {
                context: context(26),
                status_event_uuid: uuid7(27),
                assertion_uuid,
                status: AssertionStatus::Disputed,
                confidence_uuid: None,
                reasoning_uuid: Some(uuid7(19)),
                provenance_uuid,
            })
            .unwrap();
        let after_late_arrival = graph.epistemic_snapshot(10).unwrap();
        assert_eq!(
            after_late_arrival
                .schema
                .metadata()
                .get("graphforge.snapshot_fingerprint")
                .unwrap(),
            &first_fingerprint,
            "a later event must not rewrite an earlier cutoff"
        );
        assert_eq!(
            graph.epistemic_snapshot(9).unwrap().batches[0].num_rows(),
            0
        );

        let current = graph.epistemic_snapshot(i64::MAX).unwrap();
        drop(graph);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        let reopened_current = reopened.epistemic_snapshot(i64::MAX).unwrap();
        assert_eq!(
            current
                .schema
                .metadata()
                .get("graphforge.snapshot_fingerprint"),
            reopened_current
                .schema
                .metadata()
                .get("graphforge.snapshot_fingerprint")
        );
        assert_eq!(current.batches[0], reopened_current.batches[0]);
    }
}
