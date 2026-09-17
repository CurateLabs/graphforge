//! Deterministic transaction-time composition of append-only epistemic records.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    FixedSizeBinaryBuilder, ListBuilder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_knowledge::{
    AssertionLedger, AssertionStatusLedger, AssertionSupersessionLedger, ConfidenceLedger,
    HypothesisLedger, HypothesisMembershipAction, ReasoningLedger,
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

/// Decoded epistemic ledgers cached for one immutable read generation.
///
/// # What invalidates this cache
///
/// The key is `(generation_uuid, manifest_sha256)` from the
/// [`graphforge_storage::ResolvedProjectGeneration`] the read was made against.
/// A `ResolvedProjectGeneration` denotes one immutable, already-committed
/// generation (see `graphforge_storage::project_generation`, "Resolution of
/// the one committed immutable project generation") — its participant bytes
/// never change after the generation is published, and every publish mints a
/// fresh `generation_uuid` (`Uuid::now_v7()`). So:
///
/// - Two calls against the *same* generation are guaranteed byte-identical:
///   a cache hit returns exactly what a fresh read would have returned.
/// - A write that publishes a new generation between calls changes
///   `current_generation_uuid`, so the next `generation_for_read()` resolves
///   a different `ResolvedProjectGeneration` with a different
///   `generation_uuid`. The lookup keys no longer match, it is a cache miss,
///   and the six ledgers are re-read and re-decoded from the new generation.
///
/// There is no time-based or manual invalidation because none is needed: the
/// key itself cannot alias two different sets of ledger bytes.
pub(crate) struct EpistemicLedgerCache {
    generation_uuid: Uuid,
    manifest_sha256: [u8; 32],
    assertions: AssertionLedger,
    statuses: AssertionStatusLedger,
    reasoning: ReasoningLedger,
    supersessions: AssertionSupersessionLedger,
    hypotheses: HypothesisLedger,
    confidence: ConfidenceLedger,
}

#[allow(clippy::type_complexity)]
fn read_ledgers_cached(
    graph: &GraphForge,
    generation: &graphforge_storage::ResolvedProjectGeneration,
) -> Result<
    (
        AssertionLedger,
        AssertionStatusLedger,
        ReasoningLedger,
        AssertionSupersessionLedger,
        HypothesisLedger,
        ConfidenceLedger,
    ),
    GfError,
> {
    let generation_uuid = generation.generation_uuid();
    let manifest_sha256 = generation.manifest_sha256();
    {
        let cached = graph
            .epistemic_ledger_cache
            .lock()
            .expect("epistemic ledger cache lock poisoned");
        if let Some(entry) = cached.as_ref()
            && entry.generation_uuid == generation_uuid
            && entry.manifest_sha256 == manifest_sha256
        {
            return Ok((
                entry.assertions.clone(),
                entry.statuses.clone(),
                entry.reasoning.clone(),
                entry.supersessions.clone(),
                entry.hypotheses.clone(),
                entry.confidence.clone(),
            ));
        }
    }
    let assertions = crate::knowledge::read_ledger(generation)?;
    let statuses = crate::knowledge::read_status_ledger(generation)?;
    let reasoning = crate::knowledge::read_reasoning_ledger(generation)?;
    let supersessions = crate::knowledge::read_supersession_ledger(generation)?;
    let hypotheses = crate::hypotheses::read_ledger(generation)?;
    let confidence = crate::knowledge::read_confidence_ledger(generation)?;
    let mut cached = graph
        .epistemic_ledger_cache
        .lock()
        .expect("epistemic ledger cache lock poisoned");
    *cached = Some(EpistemicLedgerCache {
        generation_uuid,
        manifest_sha256,
        assertions: assertions.clone(),
        statuses: statuses.clone(),
        reasoning: reasoning.clone(),
        supersessions: supersessions.clone(),
        hypotheses: hypotheses.clone(),
        confidence: confidence.clone(),
    });
    Ok((
        assertions,
        statuses,
        reasoning,
        supersessions,
        hypotheses,
        confidence,
    ))
}

impl GraphForge {
    /// Reconstruct one deterministic epistemic view at transaction-time `cutoff_micros`.
    ///
    /// The result contains one row per visible assertion followed by one row per
    /// visible hypothesis group. Statusless assertions and unselected/empty
    /// groups remain explicit. No current-*state* cache participates: the
    /// cutoff-filtered composition below is always recomputed. Only the raw,
    /// unfiltered ledger reads are cached (see [`EpistemicLedgerCache`]),
    /// since every cutoff over one generation reads the same underlying bytes.
    pub fn epistemic_snapshot(
        &self,
        cutoff_micros: i64,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        let generation = self.generation_for_read()?;
        let (assertions, statuses, reasoning, supersessions, hypotheses, confidence) =
            read_ledgers_cached(self, &generation)?;
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

    // Every quantity below is grouped by its owning assertion/group in one
    // linear pass over its source ledger, instead of being recomputed with a
    // fresh `O(ledger size)` scan for every assertion/group (which made the
    // composition below quadratic: `O(assertions * (reasoning + statuses +
    // supersessions))`). Each map lookup in the two loops further down is
    // `O(1)` amortised, so the whole function is now linear in the total
    // number of rows read across every ledger.
    let mut history_by_assertion: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    let mut superseded_reasoning_by_assertion: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    for row in &reasoning.records {
        history_by_assertion
            .entry(row.assertion_uuid)
            .or_default()
            .push(row.reasoning_uuid);
        if let Some(predecessor) = row.supersedes_reasoning_uuid {
            superseded_reasoning_by_assertion
                .entry(row.assertion_uuid)
                .or_default()
                .insert(predecessor);
        }
    }

    let mut current_status_by_assertion: HashMap<Uuid, (i64, Uuid, &'static str)> = HashMap::new();
    let mut status_event_uuids_by_assertion: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    let mut status_extra_sources_by_assertion: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for event in &statuses.events {
        status_event_uuids_by_assertion
            .entry(event.assertion_uuid)
            .or_default()
            .push(event.status_event_uuid);
        let extra = status_extra_sources_by_assertion
            .entry(event.assertion_uuid)
            .or_default();
        extra.extend(event.confidence_uuid);
        extra.extend(event.reasoning_uuid);
        let candidate = (
            event.recorded_at_micros,
            event.status_event_uuid,
            event.status.as_str(),
        );
        current_status_by_assertion
            .entry(event.assertion_uuid)
            .and_modify(|existing| {
                if (candidate.0, candidate.1) >= (existing.0, existing.1) {
                    *existing = candidate;
                }
            })
            .or_insert(candidate);
    }

    let mut superseded_by_prior: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    let mut supersession_uuids_by_assertion: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for relation in supersessions.relations() {
        superseded_by_prior
            .entry(relation.prior_assertion_uuid)
            .or_default()
            .push(relation.replacement_assertion_uuid);
        supersession_uuids_by_assertion
            .entry(relation.prior_assertion_uuid)
            .or_default()
            .push(relation.supersession_uuid);
        if relation.replacement_assertion_uuid != relation.prior_assertion_uuid {
            supersession_uuids_by_assertion
                .entry(relation.replacement_assertion_uuid)
                .or_default()
                .push(relation.supersession_uuid);
        }
    }

    let mut membership_state_by_group: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    let mut membership_event_uuids_by_group: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for event in hypotheses.membership_events() {
        membership_event_uuids_by_group
            .entry(event.group_uuid)
            .or_default()
            .push(event.membership_event_uuid);
        let state = membership_state_by_group
            .entry(event.group_uuid)
            .or_default();
        match event.action {
            HypothesisMembershipAction::Added => {
                state.insert(event.assertion_uuid);
            }
            HypothesisMembershipAction::Removed => {
                state.remove(&event.assertion_uuid);
            }
        }
    }

    let mut current_selection_by_group: HashMap<Uuid, Option<Uuid>> = HashMap::new();
    let mut selection_event_uuids_by_group: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for event in hypotheses.selection_events() {
        selection_event_uuids_by_group
            .entry(event.group_uuid)
            .or_default()
            .push(event.selection_event_uuid);
        // Forward iteration + overwrite is exactly `Vec::rfind` from the end:
        // the last match in original ledger order wins either way.
        current_selection_by_group.insert(event.group_uuid, event.selected_assertion_uuid);
    }

    let mut rows = Vec::with_capacity(assertions.assertions.len() + hypotheses.groups().len());
    for assertion in &assertions.assertions {
        let status = current_status_by_assertion.get(&assertion.assertion_uuid);
        let history = history_by_assertion
            .get(&assertion.assertion_uuid)
            .cloned()
            .unwrap_or_default();
        let superseded_reasoning = superseded_reasoning_by_assertion.get(&assertion.assertion_uuid);
        let leaves = history
            .iter()
            .filter(|uuid| !superseded_reasoning.is_some_and(|set| set.contains(uuid)))
            .copied()
            .collect::<Vec<_>>();
        let superseded_by = superseded_by_prior
            .get(&assertion.assertion_uuid)
            .cloned()
            .unwrap_or_default();
        let mut sources = BTreeSet::from([assertion.assertion_uuid]);
        if let Some(status_event_uuids) =
            status_event_uuids_by_assertion.get(&assertion.assertion_uuid)
        {
            sources.extend(status_event_uuids.iter().copied());
        }
        if let Some(extra) = status_extra_sources_by_assertion.get(&assertion.assertion_uuid) {
            sources.extend(extra.iter().copied());
        }
        sources.extend(history.iter().copied());
        if let Some(supersession_uuids) =
            supersession_uuids_by_assertion.get(&assertion.assertion_uuid)
        {
            sources.extend(supersession_uuids.iter().copied());
        }
        rows.push(SnapshotRow {
            entity_kind: "assertion",
            assertion_uuid: Some(assertion.assertion_uuid),
            group_uuid: None,
            question_key: None,
            status: status.map(|(_, _, status_str)| *status_str),
            status_event_uuid: status.map(|(_, status_event_uuid, _)| *status_event_uuid),
            reasoning_history: history,
            reasoning_leaves: leaves,
            superseded_by,
            current_members: Vec::new(),
            selected_assertion_uuid: None,
            sources: sources.into_iter().collect(),
        });
    }
    for group in hypotheses.groups() {
        let mut members = membership_state_by_group
            .get(&group.group_uuid)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        members.sort_unstable();
        let selected = current_selection_by_group
            .get(&group.group_uuid)
            .copied()
            .flatten();
        let mut sources = BTreeSet::from([group.group_uuid]);
        if let Some(membership_event_uuids) = membership_event_uuids_by_group.get(&group.group_uuid)
        {
            sources.extend(membership_event_uuids.iter().copied());
        }
        if let Some(selection_event_uuids) = selection_event_uuids_by_group.get(&group.group_uuid) {
            sources.extend(selection_event_uuids.iter().copied());
        }
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
    use arrow::array::{Array, FixedSizeBinaryArray, ListArray, StringArray};

    use super::*;
    use crate::{
        AssertionGraphRefInput, AssessConfidenceRequest, CapabilityId, ConfidencePolicyRequest,
        CreateAssertionRequest, CreateHypothesisGroupRequest, EnableCapabilityRequest, OperationId,
        RecordAssertionStatusRequest, RecordHypothesisMembershipRequest,
        RecordHypothesisSelectionRequest, RecordReasoningRequest, WriteContext,
    };
    use graphforge_knowledge::{
        AssertionGraphRole, AssertionStatus, GraphObjectKind, ReasoningContentFormat, ReasoningKind,
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

    fn uuid7_from_u32(seed: u32) -> Uuid {
        let word = seed.to_be_bytes();
        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(&word);
        bytes[4..8].copy_from_slice(&word);
        bytes[8..12].copy_from_slice(&word);
        bytes[12..16].copy_from_slice(&word);
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn context_u32(seed: u32) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(uuid7_from_u32(seed)),
            actor_uuid: None,
        }
    }

    /// Manual perf measurement for issue #1410. Not part of CI. Run with:
    /// `cargo test -p graphforge-api --lib epistemic_snapshot::tests::measure_snapshot_cost --release -- --ignored --nocapture`
    ///
    /// Builds two epistemic graphs an order of magnitude apart in assertion
    /// count. Every assertion carries one reasoning record and one status
    /// event, so every ledger `epistemic_snapshot` reads grows with `n` —
    /// the shape the issue describes as quadratic. For each size this times:
    ///   - a "cold" call: the first `epistemic_snapshot` in the process
    ///     against that generation, which must do real ledger reads;
    ///   - four subsequent "warm" calls against the same, unchanged
    ///     generation (reporting the minimum), which a per-generation ledger
    ///     cache can serve without re-reading or re-decoding anything.
    /// It also reports the exact on-disk byte size of every participant file
    /// a full ledger read touches, using the same public
    /// `participant_snapshot` the production read path calls, so the byte
    /// count is measured, not estimated.
    #[test]
    #[ignore = "manual perf measurement, not part of CI"]
    fn measure_snapshot_cost() {
        const PARTICIPANTS: [(&str, &str); 10] = [
            ("knowledge", "assertions"),
            ("knowledge", "assertion_graph_refs"),
            ("knowledge", "confidence_assessments"),
            ("knowledge", "confidence_inputs"),
            ("epistemic", "reasoning"),
            ("epistemic", "assertion_status_events"),
            ("epistemic", "assertion_supersessions"),
            ("epistemic", "hypothesis_groups"),
            ("epistemic", "hypothesis_membership_events"),
            ("epistemic", "hypothesis_selection_events"),
        ];

        for &n in &[40u32, 800u32] {
            let root = tempfile::tempdir().unwrap();
            let graph = GraphForge::new(root.path().to_str()).unwrap();
            graph.set_clock_for_test(|| Ok(10));
            enable(&graph, CapabilityId::Provenance, 1);
            enable(&graph, CapabilityId::Knowledge, 2);
            enable(&graph, CapabilityId::Epistemic, 3);

            let build_started = std::time::Instant::now();
            for i in 0..n {
                let base = i * 4;
                let node = graph.add_node("Subject", &HashMap::new()).unwrap();
                let assertion_uuid = uuid7_from_u32(base + 1);
                let assertion = graph
                    .create_assertion(CreateAssertionRequest {
                        context: context_u32(base),
                        assertion_uuid,
                        claim: format!("perf claim {i}"),
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
                let reasoning_uuid = uuid7_from_u32(base + 2);
                graph
                    .record_reasoning(RecordReasoningRequest {
                        context: context_u32(base + 2),
                        reasoning_uuid,
                        assertion_uuid,
                        kind: ReasoningKind::DecisionRationale,
                        content_format: ReasoningContentFormat::TextPlain,
                        content: b"perf".to_vec(),
                        supersedes_reasoning_uuid: None,
                        provenance_uuid,
                    })
                    .unwrap();
                graph
                    .record_assertion_status(RecordAssertionStatusRequest {
                        context: context_u32(base + 3),
                        status_event_uuid: uuid7_from_u32(base + 3),
                        assertion_uuid,
                        status: AssertionStatus::Hypothesis,
                        confidence_uuid: None,
                        reasoning_uuid: Some(reasoning_uuid),
                        provenance_uuid,
                    })
                    .unwrap();
            }
            let build_elapsed = build_started.elapsed();

            let generation = graph.generation_for_read().unwrap();
            let mut total_bytes = 0usize;
            for (capability, family) in PARTICIPANTS {
                if let Some(snapshot) = generation.participant_snapshot(capability, family).unwrap()
                {
                    total_bytes += snapshot.bytes.len();
                }
            }

            let cold_started = std::time::Instant::now();
            let cold = graph.epistemic_snapshot(i64::MAX).unwrap();
            let cold_elapsed = cold_started.elapsed();

            let mut warm_min = std::time::Duration::MAX;
            let mut warm_result = None;
            for _ in 0..4 {
                let warm_started = std::time::Instant::now();
                let warm = graph.epistemic_snapshot(i64::MAX).unwrap();
                let warm_elapsed = warm_started.elapsed();
                warm_min = warm_min.min(warm_elapsed);
                warm_result = Some(warm);
            }
            let warm = warm_result.unwrap();

            assert_eq!(
                cold.schema
                    .metadata()
                    .get("graphforge.snapshot_fingerprint"),
                warm.schema
                    .metadata()
                    .get("graphforge.snapshot_fingerprint"),
                "a cached read must fingerprint identically to the cold read"
            );
            assert_eq!(
                cold.batches[0], warm.batches[0],
                "a cached read must be byte-identical to the cold read"
            );
            assert_eq!(cold.batches[0].num_rows(), n as usize);

            println!(
                "n={n:>5} build={build_elapsed:>10.3?} participant_bytes={total_bytes:>10} cold_snapshot={cold_elapsed:>10.3?} warm_snapshot_min={warm_min:>10.3?}"
            );
        }
    }
}
