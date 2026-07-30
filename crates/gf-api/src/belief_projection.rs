//! Explicit M21 policy resolution before knowledge-neutral M18 dispatch.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, ListArray,
    StringArray, TimestampMicrosecondBuilder,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use gf_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use gf_core::{ApiErrorCode, GfError};
use gf_knowledge::{
    ALGORITHM_INTERPRETATION_ATTACHMENT_SCHEMA, AssertionStatus, BeliefProjectionAttachment,
    BeliefProjectionAttachmentLedger, GraphObjectKind, schema_registry,
};
use gf_provenance::{
    EventKind, LineageRecord, LineageRole, ProvenanceEvent, ProvenanceLedger, SubjectKind,
};
use gf_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectStageOutcome,
    ResolvedProjectGeneration,
};
use uuid::Uuid;

use crate::{
    AnalyzeOptions, ApplyValidTimeRequest, ClusterOptions, GraphForge, InvocationDescriptor,
    InvocationError, NodeSelector, PathsOptions, RankOptions, RecordedAlgorithmRequest,
    RecordedAlgorithmResult, SimilarOptions, WriteContext,
};

/// Frozen resolved-belief projection policy version.
pub const BELIEF_PROJECTION_POLICY_VERSION: u32 = 1;

/// Explicit handling for assertions with no status at the selected cutoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatuslessPolicyV1 {
    /// Reject the resolution as ambiguous.
    Reject,
    /// Exclude statusless assertions.
    Exclude,
    /// Include statusless assertions.
    Include,
}

/// Explicit handling for supersession branches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupersessionBranchPolicyV1 {
    /// Reject a prior assertion with multiple replacement leaves.
    Reject,
    /// Keep every eligible leaf.
    IncludeAllLeaves,
}

/// Explicit hypothesis-selection behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HypothesisSelectionPolicyV1 {
    /// Require every non-empty group to have one current selection.
    RequireSelected,
    /// Exclude every member when its group has no current selection.
    ExcludeUnselectedGroup,
    /// Include every current member independently of selection.
    IncludeAllCurrentMembers,
}

/// Complete, versioned policy used to resolve one M21 view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeliefProjectionPolicyV1 {
    /// Status values eligible for projection.
    pub included_statuses: Vec<AssertionStatus>,
    /// Statusless behavior.
    pub statusless: StatuslessPolicyV1,
    /// Supersession branch behavior; superseded non-leaves are always excluded.
    pub supersession_branches: SupersessionBranchPolicyV1,
    /// Hypothesis-selection behavior.
    pub hypotheses: HypothesisSelectionPolicyV1,
}

impl BeliefProjectionPolicyV1 {
    fn canonical_bytes(&self) -> Result<Vec<u8>, GfError> {
        let mut statuses = self
            .included_statuses
            .iter()
            .map(|status| status.as_str())
            .collect::<Vec<_>>();
        statuses.sort_unstable();
        if statuses.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(GfError::Validation(
                "belief projection status policy contains a duplicate".into(),
            ));
        }
        let mut writer = CanonicalWriter::new();
        writer
            .raw(b"GFBP")
            .and_then(|()| writer.u32(BELIEF_PROJECTION_POLICY_VERSION))
            .and_then(|()| writer.u32(u32::try_from(statuses.len()).unwrap_or(u32::MAX)))
            .map_err(canonical_error)?;
        for status in statuses {
            writer.text(status).map_err(canonical_error)?;
        }
        writer
            .u8(match self.statusless {
                StatuslessPolicyV1::Reject => 0,
                StatuslessPolicyV1::Exclude => 1,
                StatuslessPolicyV1::Include => 2,
            })
            .and_then(|()| {
                writer.u8(match self.supersession_branches {
                    SupersessionBranchPolicyV1::Reject => 0,
                    SupersessionBranchPolicyV1::IncludeAllLeaves => 1,
                })
            })
            .and_then(|()| {
                writer.u8(match self.hypotheses {
                    HypothesisSelectionPolicyV1::RequireSelected => 0,
                    HypothesisSelectionPolicyV1::ExcludeUnselectedGroup => 1,
                    HypothesisSelectionPolicyV1::IncludeAllCurrentMembers => 2,
                })
            })
            .map_err(canonical_error)?;
        Ok(writer.finish())
    }
}

/// Resolve one transaction snapshot and optional valid-time intersection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveBeliefProjectionRequest {
    /// Mandatory M21 transaction-time cutoff.
    pub transaction_cutoff_micros: i64,
    /// Optional valid time. When present, only explicitly valid assertions remain.
    pub valid_time_micros: Option<i64>,
    /// Mandatory explicit resolution policy.
    pub policy: BeliefProjectionPolicyV1,
}

/// One explicitly addressed belief subject.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BeliefSubjectV1 {
    /// One immutable assertion identity.
    Assertion(Uuid),
    /// One canonical hypothesis question key.
    HypothesisQuestionKey(String),
}

/// Resolve one subject and its graph projection from the same pinned generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveBeliefSubjectRequest {
    /// Exactly one explicit subject.
    pub subject: BeliefSubjectV1,
    /// Complete temporal projection request.
    pub projection: ResolveBeliefProjectionRequest,
}

/// Same-generation subject evidence and opaque graph-only projection.
#[derive(Debug)]
pub struct ResolvedBeliefSubject {
    /// Rust-owned graph projection and canonical fingerprints.
    pub projection: ResolvedBeliefProjection,
    /// Canonical transaction-snapshot rows relevant to the explicit subject.
    pub evidence: gf_exec::ExecutionResult,
}

/// Execute a resolved neutral descriptor and append its M21 attachment.
#[derive(Clone, Debug)]
pub struct ResolvedRecordedAlgorithmRequest {
    /// Existing M20 durable-run request.
    pub recorded: RecordedAlgorithmRequest,
    /// Caller-supplied UUIDv7 attachment identity and retry key.
    pub attachment_uuid: Uuid,
}

/// Retry only the M21 attachment for an already-completed M20 run.
#[derive(Clone, Debug)]
pub struct AttachResolvedRunRequest {
    /// Idempotency context; no algorithm is dispatched by this operation.
    pub context: WriteContext,
    /// Stable attachment UUID from the original attempt.
    pub attachment_uuid: Uuid,
    /// Existing completed M20 run.
    pub run_uuid: Uuid,
    /// Exact neutral descriptor used by the completed run.
    pub descriptor: InvocationDescriptor,
}

/// M21 attachment outcome after a successful M20 execution.
#[derive(Debug)]
pub enum ResolvedAttachmentOutcome {
    /// Attachment is durably present; exact retries return the same row.
    Attached(gf_exec::ExecutionResult),
    /// M20 completed, but the later M21 publication failed.
    Failed {
        /// Stable attachment retry identity.
        attachment_uuid: Uuid,
        /// Stable completed M20 run identity.
        run_uuid: Uuid,
        /// Stable public failure code.
        error_code: String,
    },
}

/// Successful resolved execution, independent of later attachment publication.
#[derive(Debug)]
pub struct ResolvedRecordedAlgorithmResult {
    /// Completed M20 run and canonical Arrow result.
    pub recorded: RecordedAlgorithmResult,
    /// Separate M21 attachment outcome.
    pub attachment: ResolvedAttachmentOutcome,
}

/// Opaque graph-only projection and its deterministic M21 evidence.
#[derive(Debug)]
pub struct ResolvedBeliefProjection {
    pub(crate) graph: Box<GraphForge>,
    source_generation_uuid: Uuid,
    transaction_cutoff_micros: i64,
    valid_time_micros: Option<i64>,
    policy_bytes: Vec<u8>,
    policy_fingerprint: [u8; 32],
    snapshot_fingerprint: [u8; 32],
    valid_time_fingerprint: Option<[u8; 32]>,
    graph_content_fingerprint: [u8; 32],
    source_record_uuids: Vec<Uuid>,
}

impl ResolvedBeliefProjection {
    /// Source generation pinned before resolution began.
    #[must_use]
    pub const fn source_generation_uuid(&self) -> Uuid {
        self.source_generation_uuid
    }

    /// Universal graph-content fingerprint, independent of algorithm family.
    #[must_use]
    pub const fn graph_content_fingerprint(&self) -> [u8; 32] {
        self.graph_content_fingerprint
    }

    /// Canonical resolution-policy bytes.
    #[must_use]
    pub fn policy_bytes(&self) -> &[u8] {
        &self.policy_bytes
    }

    /// Canonical resolution-policy fingerprint.
    #[must_use]
    pub const fn policy_fingerprint(&self) -> [u8; 32] {
        self.policy_fingerprint
    }

    /// Transaction snapshot fingerprint.
    #[must_use]
    pub const fn snapshot_fingerprint(&self) -> [u8; 32] {
        self.snapshot_fingerprint
    }

    /// Transaction cutoff used for resolution.
    #[must_use]
    pub const fn transaction_cutoff_micros(&self) -> i64 {
        self.transaction_cutoff_micros
    }

    /// Optional valid time used for resolution.
    #[must_use]
    pub const fn valid_time_micros(&self) -> Option<i64> {
        self.valid_time_micros
    }

    /// Optional valid-time result fingerprint.
    #[must_use]
    pub const fn valid_time_fingerprint(&self) -> Option<[u8; 32]> {
        self.valid_time_fingerprint
    }

    /// Sorted, deduplicated M21 records that participated in resolution.
    #[must_use]
    pub fn source_record_uuids(&self) -> &[Uuid] {
        &self.source_record_uuids
    }

    /// Prepare rank through the unchanged neutral descriptor path.
    pub fn prepare_rank_invocation(
        &self,
        label: &str,
        options: &RankOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        self.graph.prepare_rank_invocation(label, options)
    }

    /// Prepare clustering through the unchanged neutral descriptor path.
    pub fn prepare_cluster_invocation(
        &self,
        label: &str,
        options: &ClusterOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        self.graph.prepare_cluster_invocation(label, options)
    }

    /// Prepare paths through the unchanged neutral descriptor path.
    pub fn prepare_paths_invocation(
        &self,
        source: Option<&NodeSelector>,
        target: Option<&NodeSelector>,
        options: &PathsOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        self.graph.prepare_paths_invocation(source, target, options)
    }

    /// Prepare analysis through the unchanged neutral descriptor path.
    pub fn prepare_analyze_invocation(
        &self,
        label: Option<&str>,
        options: &AnalyzeOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        self.graph.prepare_analyze_invocation(label, options)
    }

    /// Prepare similarity through the unchanged neutral descriptor path.
    pub fn prepare_similar_invocation(
        &self,
        label: &str,
        options: &SimilarOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        self.graph.prepare_similar_invocation(label, options)
    }
}

impl GraphForge {
    /// Resolve M21 interpretation into an isolated graph-only projection.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-belief-projection/1 freezes an owned request"
    )]
    pub fn resolve_belief_projection(
        &self,
        request: ResolveBeliefProjectionRequest,
    ) -> Result<ResolvedBeliefProjection, GfError> {
        let _visibility = self.graph_visibility.read()?;
        self.resolve_belief_projection_locked(&request, None)
            .map(|(projection, _)| projection)
    }

    /// Resolve one explicit subject and projection under one generation guard.
    pub fn resolve_belief_subject(
        &self,
        request: &ResolveBeliefSubjectRequest,
    ) -> Result<ResolvedBeliefSubject, GfError> {
        let _visibility = self.graph_visibility.read()?;
        let (projection, evidence) =
            self.resolve_belief_projection_locked(&request.projection, Some(&request.subject))?;
        let evidence = subject_evidence_envelope(
            &evidence.expect("subject request produces evidence"),
            &projection,
        )?;
        Ok(ResolvedBeliefSubject {
            projection,
            evidence: crate::knowledge::assertion_result(evidence),
        })
    }

    fn resolve_belief_projection_locked(
        &self,
        request: &ResolveBeliefProjectionRequest,
        subject: Option<&BeliefSubjectV1>,
    ) -> Result<(ResolvedBeliefProjection, Option<RecordBatch>), GfError> {
        let source_generation = self.generation_for_read()?;
        source_generation.require_capability("epistemic", 1)?;
        let source_generation_uuid = source_generation.generation_uuid();
        let expected_generation = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if source_generation_uuid != expected_generation {
            return Err(transaction_conflict(
                "project generation changed before belief resolution",
            ));
        }
        let (policy_bytes, policy_fingerprint) = policy_fingerprint(&request.policy)?;
        let snapshot = self.epistemic_snapshot(request.transaction_cutoff_micros)?;
        let snapshot_batch = one_batch(&snapshot, "epistemic snapshot")?;
        let subject_evidence = subject
            .map(|subject| resolve_subject_evidence(snapshot_batch, subject))
            .transpose()?;
        #[cfg(test)]
        subject_resolution_barrier_for_test()?;
        let valid_time = request
            .valid_time_micros
            .map(|valid_time_micros| {
                self.apply_valid_time(ApplyValidTimeRequest {
                    transaction_cutoff_micros: request.transaction_cutoff_micros,
                    valid_time_micros,
                })
            })
            .transpose()?;
        let validity_batch = valid_time
            .as_ref()
            .map(|result| one_batch(result, "valid-time result"))
            .transpose()?;
        let assertion_ledger = crate::knowledge::read_ledger(&source_generation)?;
        let graph_refs = assertion_ledger
            .graph_refs
            .iter()
            .map(|row| (row.assertion_uuid, row.graph_uuid, row.graph_kind))
            .collect::<Vec<_>>();
        let selection =
            resolve_selection(snapshot_batch, validity_batch, &request.policy, &graph_refs)?;
        let mut projected = GraphForge::new(None)?;
        let storage_selection = gf_storage::GraphProjectionSelection {
            node_uuids: selection
                .node_uuids
                .iter()
                .map(|uuid| *uuid.as_bytes())
                .collect(),
            edge_uuids: selection
                .edge_uuids
                .iter()
                .map(|uuid| *uuid.as_bytes())
                .collect(),
        };
        let materialized = gf_storage::materialize_graph_projection(
            &self.dir,
            &projected.dir,
            &storage_selection,
        )?;
        projected.ontology.clone_from(&self.ontology);
        projected.ontology_mode = self.ontology_mode;
        projected.runtime_catalog = Arc::new(Mutex::new(
            self.runtime_catalog
                .lock()
                .expect("runtime catalog poisoned")
                .clone(),
        ));
        projected.adjacency_provider = Arc::new(gf_exec::PersistentAdjacencyProvider::new(
            projected.dir.clone(),
            projected.ontology_mode,
        ));
        let current = self.generation_for_read()?;
        if current.generation_uuid() != source_generation_uuid {
            return Err(transaction_conflict(
                "project generation changed during belief resolution",
            ));
        }
        Ok((
            ResolvedBeliefProjection {
                graph: Box::new(projected),
                source_generation_uuid,
                transaction_cutoff_micros: request.transaction_cutoff_micros,
                valid_time_micros: request.valid_time_micros,
                policy_bytes,
                policy_fingerprint,
                snapshot_fingerprint: selection.snapshot_fingerprint,
                valid_time_fingerprint: selection.valid_time_fingerprint,
                graph_content_fingerprint: materialized.graph_content_fingerprint,
                source_record_uuids: selection.source_record_uuids,
            },
            subject_evidence,
        ))
    }

    /// Complete one M20 run on a graph-only projection, then append M21 context.
    pub fn invoke_resolved_recorded(
        &self,
        projection: &ResolvedBeliefProjection,
        request: ResolvedRecordedAlgorithmRequest,
    ) -> Result<ResolvedRecordedAlgorithmResult, GfError> {
        let recorded = self.invoke_recorded_on(&projection.graph, &request.recorded)?;
        let attachment_request = AttachResolvedRunRequest {
            context: request.recorded.context,
            attachment_uuid: request.attachment_uuid,
            run_uuid: request.recorded.run_uuid,
            descriptor: request.recorded.descriptor,
        };
        let attachment = match self.attach_resolved_run(projection, attachment_request) {
            Ok(result) => ResolvedAttachmentOutcome::Attached(result),
            Err(error) => ResolvedAttachmentOutcome::Failed {
                attachment_uuid: request.attachment_uuid,
                run_uuid: recorded.run_uuid,
                error_code: error.code().to_owned(),
            },
        };
        Ok(ResolvedRecordedAlgorithmResult {
            recorded,
            attachment,
        })
    }

    /// Append or replay only the M21 attachment; never redispatch an algorithm.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-belief-projection/1 freezes an owned request"
    )]
    pub fn attach_resolved_run(
        &self,
        projection: &ResolvedBeliefProjection,
        request: AttachResolvedRunRequest,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        validate_attachment_request(&request)?;
        fail_attachment_for_test()?;
        let _visibility = crate::knowledge::lock_graph_visibility(self)?;
        let parent =
            gf_storage::resolve_project_generation(self.resolved_generation.container_root())?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("epistemic", 1)?;
        parent.require_capability("provenance", 1)?;
        let runs = crate::algorithm_runs::read_ledger(&parent)?;
        let run = runs
            .run(request.run_uuid)
            .ok_or_else(|| not_found("algorithm run"))?;
        let terminal = runs
            .terminal_event(request.run_uuid)
            .ok_or_else(|| ambiguous("algorithm run is not terminal"))?;
        if terminal.state != gf_knowledge::AlgorithmRunState::Completed {
            return Err(ambiguous("only a completed algorithm run can be attached"));
        }
        if run.descriptor != request.descriptor.canonical_bytes()
            || request.descriptor.fingerprint() != &descriptor_fingerprint(&run.descriptor)?
        {
            return Err(transaction_conflict(
                "attachment descriptor does not match the completed run",
            ));
        }
        let existing = read_attachment_ledger(&parent)?;
        if let Some((index, row)) = existing
            .attachments
            .iter()
            .enumerate()
            .find(|(_, row)| row.attachment_uuid == request.attachment_uuid)
        {
            verify_existing_attachment(row, projection, &request)?;
            let provenance = crate::provenance::read_ledger(&parent)?;
            let event = provenance
                .events
                .iter()
                .find(|event| event.provenance_uuid == row.provenance_uuid)
                .ok_or_else(|| not_found("attachment provenance"))?;
            if event.operation_uuid != request.attachment_uuid
                || event.actor_uuid != request.context.actor_uuid
                || event.event_kind != EventKind::RecordBeliefProjectionAttachment
            {
                return Err(transaction_conflict(
                    "attachment retry provenance does not match the original publication",
                ));
            }
            return attachment_row(&existing, index);
        }
        let recorded_at = (self.clock.lock().expect("clock lock poisoned"))()?;
        let event = ProvenanceEvent::new(
            request.attachment_uuid,
            EventKind::RecordBeliefProjectionAttachment,
            request.context.actor_uuid,
            recorded_at,
        )
        .map_err(crate::knowledge::provenance_error)?;
        let attachment = BeliefProjectionAttachment::new(
            request.attachment_uuid,
            request.run_uuid,
            projection.source_generation_uuid,
            projection.transaction_cutoff_micros,
            projection.valid_time_micros,
            BELIEF_PROJECTION_POLICY_VERSION,
            projection.policy_bytes.clone(),
            projection.snapshot_fingerprint,
            projection.valid_time_fingerprint,
            projection.graph_content_fingerprint,
            *request.descriptor.fingerprint(),
            projection.source_record_uuids.clone(),
            event.provenance_uuid,
            recorded_at,
        )
        .map_err(crate::knowledge::knowledge_error)?;
        let staged = BeliefProjectionAttachmentLedger::new(vec![attachment])
            .map_err(crate::knowledge::knowledge_error)?;
        let updated = existing
            .merge(&staged)
            .map_err(crate::knowledge::knowledge_error)?;
        let provenance = crate::provenance::read_ledger(&parent)?
            .merge(&attachment_provenance(
                event,
                request.run_uuid,
                request.attachment_uuid,
            )?)
            .map_err(crate::knowledge::provenance_error)?;
        publish_attachment(
            self,
            &parent,
            request.attachment_uuid,
            &updated,
            &provenance,
        )?;
        let index = updated
            .attachments
            .iter()
            .position(|row| row.attachment_uuid == request.attachment_uuid)
            .expect("published attachment exists");
        attachment_row(&updated, index)
    }
}

#[derive(Debug)]
struct AssertionView {
    status: Option<String>,
    superseded_by: Vec<Uuid>,
    sources: Vec<Uuid>,
}

#[derive(Debug)]
struct GroupView {
    members: Vec<Uuid>,
    selected: Option<Uuid>,
    sources: Vec<Uuid>,
}

#[derive(Debug)]
pub(crate) struct BeliefSelection {
    pub(crate) node_uuids: BTreeSet<Uuid>,
    pub(crate) edge_uuids: BTreeSet<Uuid>,
    pub(crate) source_record_uuids: Vec<Uuid>,
    pub(crate) snapshot_fingerprint: [u8; 32],
    pub(crate) valid_time_fingerprint: Option<[u8; 32]>,
}

#[allow(
    clippy::too_many_lines,
    reason = "the complete explicit policy matrix stays together for auditability"
)]
pub(crate) fn resolve_selection(
    snapshot: &RecordBatch,
    validity: Option<&RecordBatch>,
    policy: &BeliefProjectionPolicyV1,
    graph_refs: &[(Uuid, Uuid, GraphObjectKind)],
) -> Result<BeliefSelection, GfError> {
    let (assertions, groups, snapshot_fingerprint) = parse_snapshot(snapshot)?;
    let (validity, valid_time_fingerprint) = validity.map(parse_validity).transpose()?.unzip();
    let included_statuses = policy
        .included_statuses
        .iter()
        .map(|status| status.as_str())
        .collect::<BTreeSet<_>>();
    let mut eligible = BTreeSet::new();
    let mut sources = BTreeSet::new();
    for (assertion_uuid, view) in &assertions {
        sources.extend(view.sources.iter().copied());
        let status_eligible = match view.status.as_deref() {
            Some(status) => included_statuses.contains(status),
            None => match policy.statusless {
                StatuslessPolicyV1::Reject => {
                    return Err(ambiguous(
                        "statusless assertion requires explicit include or exclude policy",
                    ));
                }
                StatuslessPolicyV1::Exclude => false,
                StatuslessPolicyV1::Include => true,
            },
        };
        if view.superseded_by.len() > 1
            && policy.supersession_branches == SupersessionBranchPolicyV1::Reject
        {
            return Err(ambiguous(
                "supersession branch requires include_all_leaves policy",
            ));
        }
        let is_leaf = view.superseded_by.is_empty();
        let valid = validity
            .as_ref()
            .is_none_or(|values| values.get(assertion_uuid) == Some(&true));
        if status_eligible && is_leaf && valid {
            eligible.insert(*assertion_uuid);
        }
    }
    let mut hypothesis_votes = BTreeMap::<Uuid, bool>::new();
    for group in groups {
        sources.extend(group.sources);
        let members = group.members.into_iter().collect::<BTreeSet<_>>();
        let allowed = match policy.hypotheses {
            HypothesisSelectionPolicyV1::RequireSelected => {
                if members.is_empty() {
                    BTreeSet::new()
                } else {
                    let selected = group.selected.ok_or_else(|| {
                        ambiguous("hypothesis group has no current selected assertion")
                    })?;
                    if !members.contains(&selected) {
                        return Err(ambiguous(
                            "hypothesis selection is not a current group member",
                        ));
                    }
                    BTreeSet::from([selected])
                }
            }
            HypothesisSelectionPolicyV1::ExcludeUnselectedGroup => {
                group.selected.into_iter().collect()
            }
            HypothesisSelectionPolicyV1::IncludeAllCurrentMembers => members.clone(),
        };
        for member in members {
            let vote = allowed.contains(&member);
            if hypothesis_votes
                .insert(member, vote)
                .is_some_and(|prior| prior != vote)
            {
                return Err(ambiguous(
                    "assertion has contradictory selections across hypothesis groups",
                ));
            }
        }
    }
    for (member, included) in hypothesis_votes {
        if !included {
            eligible.remove(&member);
        }
    }
    let mut node_uuids = BTreeSet::new();
    let mut edge_uuids = BTreeSet::new();
    for (assertion_uuid, graph_uuid, kind) in graph_refs {
        if !eligible.contains(assertion_uuid) {
            continue;
        }
        match kind {
            GraphObjectKind::Node => {
                node_uuids.insert(*graph_uuid);
            }
            GraphObjectKind::Edge => {
                edge_uuids.insert(*graph_uuid);
            }
        }
    }
    Ok(BeliefSelection {
        node_uuids,
        edge_uuids,
        source_record_uuids: sources.into_iter().collect(),
        snapshot_fingerprint,
        valid_time_fingerprint,
    })
}

type SnapshotParts = (BTreeMap<Uuid, AssertionView>, Vec<GroupView>, [u8; 32]);

fn parse_snapshot(batch: &RecordBatch) -> Result<SnapshotParts, GfError> {
    let kinds = strings(batch, "entity_kind")?;
    let assertion_ids = fixed(batch, "assertion_uuid")?;
    let statuses = strings(batch, "status")?;
    let superseded = lists(batch, "superseded_by_assertion_uuids")?;
    let members = lists(batch, "current_member_assertion_uuids")?;
    let selected = fixed(batch, "selected_assertion_uuid")?;
    let sources = lists(batch, "source_record_uuids")?;
    let fingerprint_values = fixed(batch, "snapshot_fingerprint")?;
    let fingerprint = exact_fingerprint(batch, fingerprint_values)?;
    let mut assertions = BTreeMap::new();
    let mut groups = Vec::new();
    for row in 0..batch.num_rows() {
        match kinds.value(row) {
            "assertion" => {
                assertions.insert(
                    uuid_at(assertion_ids, row, "assertion_uuid")?,
                    AssertionView {
                        status: (!statuses.is_null(row)).then(|| statuses.value(row).to_owned()),
                        superseded_by: uuid_list_at(superseded, row)?,
                        sources: uuid_list_at(sources, row)?,
                    },
                );
            }
            "hypothesis_group" => groups.push(GroupView {
                members: uuid_list_at(members, row)?,
                selected: optional_uuid_at(selected, row)?,
                sources: uuid_list_at(sources, row)?,
            }),
            _ => return Err(schema("unknown epistemic snapshot entity kind")),
        }
    }
    Ok((assertions, groups, fingerprint))
}

fn resolve_subject_evidence(
    batch: &RecordBatch,
    subject: &BeliefSubjectV1,
) -> Result<RecordBatch, GfError> {
    let kinds = strings(batch, "entity_kind")?;
    let assertion_ids = fixed(batch, "assertion_uuid")?;
    let group_ids = fixed(batch, "group_uuid")?;
    let question_keys = strings(batch, "question_key")?;
    let superseded = lists(batch, "superseded_by_assertion_uuids")?;
    let members = lists(batch, "current_member_assertion_uuids")?;
    let selected = fixed(batch, "selected_assertion_uuid")?;
    let mut assertions = BTreeMap::<Uuid, (usize, Vec<Uuid>)>::new();
    let mut groups = Vec::<(usize, Uuid, Option<&str>, Vec<Uuid>, Option<Uuid>)>::new();
    for row in 0..batch.num_rows() {
        match kinds.value(row) {
            "assertion" => {
                assertions.insert(
                    uuid_at(assertion_ids, row, "assertion_uuid")?,
                    (row, uuid_list_at(superseded, row)?),
                );
            }
            "hypothesis_group" => groups.push((
                row,
                uuid_at(group_ids, row, "group_uuid")?,
                (!question_keys.is_null(row)).then(|| question_keys.value(row)),
                uuid_list_at(members, row)?,
                optional_uuid_at(selected, row)?,
            )),
            _ => return Err(schema("unknown epistemic snapshot entity kind")),
        }
    }
    let mut relevant_assertions = BTreeSet::new();
    let mut relevant_groups = BTreeSet::new();
    match subject {
        BeliefSubjectV1::Assertion(uuid) => {
            if !assertions.contains_key(uuid) {
                return Err(not_found("belief assertion subject"));
            }
            relevant_assertions.insert(*uuid);
        }
        BeliefSubjectV1::HypothesisQuestionKey(question_key) => {
            let matches = groups
                .iter()
                .filter(|(_, _, candidate, _, _)| *candidate == Some(question_key.as_str()))
                .collect::<Vec<_>>();
            if matches.is_empty() {
                return Err(not_found("belief hypothesis subject"));
            }
            if matches.len() > 1 {
                return Err(ambiguous(
                    "hypothesis question key resolved to multiple groups",
                ));
            }
            let (_, group_uuid, _, members, selected) = matches[0];
            relevant_groups.insert(*group_uuid);
            relevant_assertions.extend(members.iter().copied());
            relevant_assertions.extend(selected.iter().copied());
        }
    }
    loop {
        let before = (relevant_assertions.len(), relevant_groups.len());
        for (assertion_uuid, (_, replacements)) in &assertions {
            if relevant_assertions.contains(assertion_uuid)
                || replacements
                    .iter()
                    .any(|uuid| relevant_assertions.contains(uuid))
            {
                relevant_assertions.insert(*assertion_uuid);
                relevant_assertions.extend(replacements.iter().copied());
            }
        }
        for (_, group_uuid, _, members, selected) in &groups {
            if members
                .iter()
                .chain(selected.iter())
                .any(|uuid| relevant_assertions.contains(uuid))
            {
                relevant_groups.insert(*group_uuid);
                relevant_assertions.extend(members.iter().copied());
                relevant_assertions.extend(selected.iter().copied());
            }
        }
        if before == (relevant_assertions.len(), relevant_groups.len()) {
            break;
        }
    }
    let relevant_rows = assertions
        .iter()
        .filter(|(uuid, _)| relevant_assertions.contains(uuid))
        .map(|(_, (row, _))| *row)
        .chain(
            groups
                .iter()
                .filter(|(_, uuid, _, _, _)| relevant_groups.contains(uuid))
                .map(|(row, _, _, _, _)| *row),
        )
        .collect::<BTreeSet<_>>();
    let mask = BooleanArray::from(
        (0..batch.num_rows())
            .map(|row| relevant_rows.contains(&row))
            .collect::<Vec<_>>(),
    );
    filter_record_batch(batch, &mask)
        .map_err(|_| schema("belief subject evidence could not be filtered"))
}

fn subject_evidence_envelope(
    evidence: &RecordBatch,
    projection: &ResolvedBeliefProjection,
) -> Result<RecordBatch, GfError> {
    let row_count = evidence.num_rows();
    let mut fields = evidence.schema().fields().to_vec();
    let mut columns = evidence.columns().to_vec();
    let (field, column) = fixed_metadata_column(
        "source_generation_uuid",
        16,
        Some(projection.source_generation_uuid.as_bytes()),
        row_count,
        false,
    )?;
    fields.push(field);
    columns.push(column);
    let mut cutoff = TimestampMicrosecondBuilder::with_capacity(row_count).with_timezone("UTC");
    let mut valid_time = TimestampMicrosecondBuilder::with_capacity(row_count).with_timezone("UTC");
    for _ in 0..row_count {
        cutoff.append_value(projection.transaction_cutoff_micros);
        if let Some(value) = projection.valid_time_micros {
            valid_time.append_value(value);
        } else {
            valid_time.append_null();
        }
    }
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    fields.push(Arc::new(Field::new(
        "transaction_cutoff_micros",
        timestamp.clone(),
        false,
    )));
    columns.push(Arc::new(cutoff.finish()));
    fields.push(Arc::new(Field::new("valid_time_micros", timestamp, true)));
    columns.push(Arc::new(valid_time.finish()));
    let (field, column) = fixed_metadata_column(
        "policy_fingerprint",
        32,
        Some(&projection.policy_fingerprint),
        row_count,
        false,
    )?;
    fields.push(field);
    columns.push(column);
    let (field, column) = fixed_metadata_column(
        "valid_time_fingerprint",
        32,
        projection
            .valid_time_fingerprint
            .as_ref()
            .map(<[u8; 32]>::as_slice),
        row_count,
        true,
    )?;
    fields.push(field);
    columns.push(column);
    let (field, column) = fixed_metadata_column(
        "graph_content_fingerprint",
        32,
        Some(&projection.graph_content_fingerprint),
        row_count,
        false,
    )?;
    fields.push(field);
    columns.push(column);
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            evidence.schema().metadata().clone(),
        )),
        columns,
    )
    .map_err(|_| schema("belief subject evidence envelope could not be built"))
}

fn fixed_metadata_column(
    name: &'static str,
    width: i32,
    value: Option<&[u8]>,
    row_count: usize,
    nullable: bool,
) -> Result<(Arc<Field>, ArrayRef), GfError> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(row_count, width);
    for _ in 0..row_count {
        if let Some(value) = value {
            builder
                .append_value(value)
                .map_err(|_| schema("belief subject evidence metadata is invalid"))?;
        } else {
            builder.append_null();
        }
    }
    Ok((
        Arc::new(Field::new(name, DataType::FixedSizeBinary(width), nullable)),
        Arc::new(builder.finish()),
    ))
}

#[cfg(test)]
thread_local! {
    static SUBJECT_RESOLUTION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce() -> Result<(), GfError>>>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
fn subject_resolution_barrier_for_test() -> Result<(), GfError> {
    SUBJECT_RESOLUTION_HOOK.with(|hook| hook.borrow_mut().take().map_or(Ok(()), |hook| hook()))
}

fn parse_validity(batch: &RecordBatch) -> Result<(BTreeMap<Uuid, bool>, [u8; 32]), GfError> {
    let assertion_ids = fixed(batch, "assertion_uuid")?;
    let values = batch
        .column_by_name("is_valid")
        .and_then(|column| column.as_any().downcast_ref::<BooleanArray>())
        .ok_or_else(|| schema("valid-time is_valid column is absent"))?;
    let fingerprints = fixed(batch, "result_fingerprint")?;
    let fingerprint = exact_fingerprint(batch, fingerprints)?;
    let mut validity = BTreeMap::new();
    for row in 0..batch.num_rows() {
        if !values.is_null(row) {
            validity.insert(
                uuid_at(assertion_ids, row, "assertion_uuid")?,
                values.value(row),
            );
        }
    }
    Ok((validity, fingerprint))
}

fn exact_fingerprint(
    batch: &RecordBatch,
    values: &FixedSizeBinaryArray,
) -> Result<[u8; 32], GfError> {
    if batch.num_rows() == 0 {
        let content_columns = batch
            .num_columns()
            .checked_sub(1)
            .ok_or_else(|| schema("resolved snapshot has no fingerprint-bearing schema"))?;
        let content_schema = Arc::new(Schema::new(
            batch.schema().fields()[..content_columns].to_vec(),
        ));
        let content =
            RecordBatch::try_new(content_schema, batch.columns()[..content_columns].to_vec())
                .map_err(|_| schema("resolved snapshot content is invalid"))?;
        return crate::canonical_arrow::result_fingerprint(&[content])
            .map_err(|error| schema(&error.to_string()));
    }
    let fingerprint: [u8; 32] = values
        .value(0)
        .try_into()
        .map_err(|_| schema("resolved snapshot fingerprint has invalid width"))?;
    if (1..batch.num_rows()).any(|row| values.value(row) != fingerprint) {
        return Err(schema(
            "resolved snapshot rows disagree on their content fingerprint",
        ));
    }
    Ok(fingerprint)
}

fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| schema("resolved snapshot string column is absent"))
}

fn fixed<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| schema("resolved snapshot UUID column is absent"))
}

fn lists<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ListArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| schema("resolved snapshot UUID-list column is absent"))
}

fn uuid_at(values: &FixedSizeBinaryArray, row: usize, name: &str) -> Result<Uuid, GfError> {
    if values.is_null(row) {
        return Err(schema(&format!("resolved snapshot {name} is null")));
    }
    Uuid::from_slice(values.value(row)).map_err(|_| schema("resolved snapshot UUID is invalid"))
}

fn optional_uuid_at(values: &FixedSizeBinaryArray, row: usize) -> Result<Option<Uuid>, GfError> {
    (!values.is_null(row))
        .then(|| {
            Uuid::from_slice(values.value(row))
                .map_err(|_| schema("resolved snapshot UUID is invalid"))
        })
        .transpose()
}

fn uuid_list_at(values: &ListArray, row: usize) -> Result<Vec<Uuid>, GfError> {
    let value = values.value(row);
    let uuids = value
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| schema("resolved snapshot UUID-list item type is invalid"))?;
    (0..uuids.len())
        .map(|index| {
            Uuid::from_slice(uuids.value(index))
                .map_err(|_| schema("resolved snapshot UUID-list value is invalid"))
        })
        .collect()
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the domain error at the API boundary"
)]
fn canonical_error(error: gf_core::canonical::CanonicalError) -> GfError {
    GfError::Validation(error.to_string())
}

fn schema(message: &str) -> GfError {
    GfError::Api {
        code: ApiErrorCode::SchemaMismatch,
        message: message.into(),
    }
}

fn ambiguous(message: &str) -> GfError {
    GfError::Api {
        code: ApiErrorCode::AmbiguousProjection,
        message: message.into(),
    }
}

fn transaction_conflict(message: &str) -> GfError {
    GfError::Project {
        code: gf_core::ProjectErrorCode::TransactionConflict,
        message: message.into(),
    }
}

fn one_batch<'a>(
    result: &'a gf_exec::ExecutionResult,
    name: &str,
) -> Result<&'a RecordBatch, GfError> {
    if result.batches.len() != 1 {
        return Err(schema(&format!("{name} must contain exactly one batch")));
    }
    Ok(&result.batches[0])
}

fn validate_attachment_request(request: &AttachResolvedRunRequest) -> Result<(), GfError> {
    for (uuid, name) in [
        (request.context.operation_uuid.0, "operation_uuid"),
        (request.attachment_uuid, "attachment_uuid"),
        (request.run_uuid, "run_uuid"),
    ] {
        if uuid.is_nil() {
            return Err(GfError::Validation(format!("{name} must not be nil")));
        }
    }
    if request.context.actor_uuid.is_some_and(|uuid| uuid.is_nil()) {
        return Err(GfError::Validation("actor_uuid must not be nil".into()));
    }
    if request.attachment_uuid.get_version() != Some(uuid::Version::SortRand) {
        return Err(GfError::Validation("attachment_uuid must be UUIDv7".into()));
    }
    Ok(())
}

fn descriptor_fingerprint(bytes: &[u8]) -> Result<[u8; 32], GfError> {
    crate::InvocationDescriptor::from_canonical_bytes(bytes)
        .map(|descriptor| *descriptor.fingerprint())
        .map_err(|error| schema(&error.to_string()))
}

fn verify_existing_attachment(
    row: &BeliefProjectionAttachment,
    projection: &ResolvedBeliefProjection,
    request: &AttachResolvedRunRequest,
) -> Result<(), GfError> {
    if row.run_uuid == request.run_uuid
        && row.source_generation_uuid == projection.source_generation_uuid
        && row.transaction_cutoff_micros == projection.transaction_cutoff_micros
        && row.valid_time_micros == projection.valid_time_micros
        && row.policy_version == BELIEF_PROJECTION_POLICY_VERSION
        && row.policy_bytes == projection.policy_bytes
        && row.policy_fingerprint == projection.policy_fingerprint
        && row.snapshot_fingerprint == projection.snapshot_fingerprint
        && row.valid_time_fingerprint == projection.valid_time_fingerprint
        && row.graph_content_fingerprint == projection.graph_content_fingerprint
        && row.descriptor_fingerprint == *request.descriptor.fingerprint()
        && row.source_record_uuids == projection.source_record_uuids
    {
        Ok(())
    } else {
        Err(transaction_conflict(
            "attachment UUID was reused for different content",
        ))
    }
}

fn read_attachment_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<BeliefProjectionAttachmentLedger, GfError> {
    generation.require_capability("epistemic", 1)?;
    match generation.participant_snapshot("epistemic", "algorithm_interpretation_attachments")? {
        None => Ok(BeliefProjectionAttachmentLedger::default()),
        Some(snapshot) => {
            crate::knowledge::require_participant_contract(
                &snapshot,
                "algorithm_interpretation_attachments",
            )?;
            let batches = if snapshot.row_count == 0 {
                vec![RecordBatch::new_empty(Arc::clone(
                    &ALGORITHM_INTERPRETATION_ATTACHMENT_SCHEMA,
                ))]
            } else {
                crate::knowledge::read_parquet(&snapshot.bytes)?
            };
            BeliefProjectionAttachmentLedger::from_batches(&batches)
                .map_err(crate::knowledge::knowledge_error)
        }
    }
}

pub(crate) fn empty_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry()
        .into_iter()
        .find(|entry| entry.record_family == "algorithm_interpretation_attachments")
        .expect("registered interpretation attachment family");
    Ok(vec![crate::knowledge::participant(
        &registry,
        &BeliefProjectionAttachmentLedger::default()
            .batch()
            .map_err(crate::knowledge::knowledge_error)?,
    )?])
}

fn attachment_row(
    ledger: &BeliefProjectionAttachmentLedger,
    index: usize,
) -> Result<gf_exec::ExecutionResult, GfError> {
    Ok(crate::knowledge::assertion_result(
        ledger
            .batch()
            .map_err(crate::knowledge::knowledge_error)?
            .slice(index, 1),
    ))
}

fn attachment_provenance(
    event: ProvenanceEvent,
    run_uuid: Uuid,
    attachment_uuid: Uuid,
) -> Result<ProvenanceLedger, GfError> {
    let input = LineageRecord::new(
        event.provenance_uuid,
        run_uuid,
        SubjectKind::AlgorithmRun,
        LineageRole::Input,
        0,
    )
    .map_err(crate::knowledge::provenance_error)?;
    let output = LineageRecord::new(
        event.provenance_uuid,
        attachment_uuid,
        SubjectKind::BeliefProjectionAttachment,
        LineageRole::Output,
        0,
    )
    .map_err(crate::knowledge::provenance_error)?;
    ProvenanceLedger::new(vec![event], vec![input, output])
        .map_err(crate::knowledge::provenance_error)
}

fn publish_attachment(
    graph: &GraphForge,
    parent: &ResolvedProjectGeneration,
    transaction_uuid: Uuid,
    ledger: &BeliefProjectionAttachmentLedger,
    provenance: &ProvenanceLedger,
) -> Result<(), GfError> {
    let expected_parent = parent.generation_uuid();
    if *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned")
        != expected_parent
    {
        return Err(transaction_conflict(
            "project generation changed before attachment publication",
        ));
    }
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "epistemic"
                && snapshot.record_family_id == "algorithm_interpretation_attachments"
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(crate::knowledge::snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    let registry = schema_registry()
        .into_iter()
        .find(|entry| entry.record_family == "algorithm_interpretation_attachments")
        .expect("registered interpretation attachment family");
    participants.push(crate::knowledge::participant(
        &registry,
        &ledger.batch().map_err(crate::knowledge::knowledge_error)?,
    )?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    let request = ProjectGenerationRequest {
        transaction_uuid,
        generation_uuid: crate::knowledge::knowledge_generation_uuid(
            b"belief-projection-attachment",
            crate::OperationId(transaction_uuid),
            &participants,
        ),
        capabilities: parent
            .capabilities()
            .into_iter()
            .map(|entry| ProjectCapability {
                capability_id: entry.capability_id,
                capability_version: entry.capability_version,
            })
            .collect(),
        participants,
    };
    let receipt = match gf_storage::stage_project_generation(
        graph.resolved_generation.container_root(),
        &request,
    )? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before attachment publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish()?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    Ok(())
}

fn not_found(kind: &str) -> GfError {
    GfError::Api {
        code: ApiErrorCode::NotFound,
        message: format!("{kind} was not found"),
    }
}

#[cfg(test)]
thread_local! {
    static INJECT_ATTACHMENT_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn fail_attachment_for_test() -> Result<(), GfError> {
    INJECT_ATTACHMENT_FAILURE.with(|enabled| {
        if enabled.get() {
            Err(GfError::Project {
                code: gf_core::ProjectErrorCode::PublicationFailed,
                message: "injected attachment publication failure".into(),
            })
        } else {
            Ok(())
        }
    })
}

#[cfg(not(test))]
#[allow(
    clippy::unnecessary_wraps,
    reason = "production no-op preserves the injected test hook signature"
)]
const fn fail_attachment_for_test() -> Result<(), GfError> {
    Ok(())
}

pub(crate) fn policy_fingerprint(
    policy: &BeliefProjectionPolicyV1,
) -> Result<(Vec<u8>, [u8; 32]), GfError> {
    let bytes = policy.canonical_bytes()?;
    let digest = fingerprint(
        CanonicalDomain::BeliefProjectionPolicy,
        CANONICAL_CONTRACT_VERSION,
        &bytes,
    )
    .map_err(canonical_error)?;
    Ok((bytes, digest))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use arrow::array::{FixedSizeBinaryBuilder, ListBuilder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::{
        AssertionGraphRefInput, AssertionGraphRole, CapabilityId, CreateAssertionRequest,
        EnableCapabilityRequest, OperationId, RecordAssertionStatusRequest,
        RecordAssertionValidityRequest,
    };
    use gf_core::algorithms::ClusterAlgorithm;

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
    fn statusless_include_materializes_the_same_neutral_rank_projection() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        enable(&graph, CapabilityId::Provenance, 1);
        enable(&graph, CapabilityId::Knowledge, 2);
        enable(&graph, CapabilityId::Epistemic, 3);
        enable(&graph, CapabilityId::ValidTime, 30);
        let node = graph.add_node("Person", &HashMap::new()).unwrap();
        graph.set_clock_for_test(|| Ok(10));
        let assertion_result = graph
            .create_assertion(CreateAssertionRequest {
                context: context(4),
                assertion_uuid: uuid7(5),
                claim: "the selected graph contains this person".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap();
        let provenance_uuid = Uuid::from_slice(
            assertion_result.batches[0]
                .column_by_name("provenance_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
        )
        .unwrap();
        let policy = BeliefProjectionPolicyV1 {
            included_statuses: Vec::new(),
            statusless: StatuslessPolicyV1::Include,
            supersession_branches: SupersessionBranchPolicyV1::IncludeAllLeaves,
            hypotheses: HypothesisSelectionPolicyV1::IncludeAllCurrentMembers,
        };
        let rejected = graph
            .resolve_belief_projection(ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: i64::MAX,
                valid_time_micros: None,
                policy: BeliefProjectionPolicyV1 {
                    statusless: StatuslessPolicyV1::Reject,
                    ..policy.clone()
                },
            })
            .unwrap_err();
        assert_eq!(rejected.code(), "GF_AMBIGUOUS_PROJECTION");
        let projection = graph
            .resolve_belief_projection(ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: i64::MAX,
                valid_time_micros: None,
                policy: policy.clone(),
            })
            .unwrap();
        assert_eq!(
            gf_storage::read_nodes(&projection.graph.dir)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        for capability in ["knowledge", "epistemic", "provenance", "valid_time"] {
            assert!(
                projection
                    .graph
                    .resolved_generation
                    .capability(capability)
                    .unwrap()
                    .is_none(),
                "resolved execution graph must not expose {capability}"
            );
        }
        let projected_types = projection
            .graph
            .runtime_catalog
            .lock()
            .unwrap()
            .entity_type_names_with_ids()
            .map(|(id, name)| (id.0, name.to_owned()))
            .collect::<Vec<_>>();
        assert_eq!(projected_types, vec![(0, "Person".into())]);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        let reopened_projection = reopened
            .resolve_belief_projection(ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: i64::MAX,
                valid_time_micros: None,
                policy: policy.clone(),
            })
            .unwrap();
        assert_eq!(
            projection.graph_content_fingerprint(),
            reopened_projection.graph_content_fingerprint()
        );
        assert_eq!(
            projection.source_record_uuids(),
            reopened_projection.source_record_uuids()
        );
        let options = RankOptions::default();
        let direct = graph.rank("Person", options.clone()).unwrap();
        let descriptor = projection
            .prepare_rank_invocation("Person", &options)
            .unwrap();
        let resolved = projection.graph.invoke_descriptor(&descriptor).unwrap();
        assert_eq!(direct, resolved);
        assert_eq!(projection.source_record_uuids(), &[uuid7(5)]);
        let cluster_options = ClusterOptions {
            by: ClusterAlgorithm::Louvain,
            vector_property: None,
            via: None,
            directed: true,
            write_property: None,
        };
        let direct_cluster = graph.cluster("Person", cluster_options.clone()).unwrap();
        let cluster = projection
            .prepare_cluster_invocation("Person", &cluster_options)
            .unwrap();
        assert_eq!(
            direct_cluster,
            projection.graph.invoke_descriptor(&cluster).unwrap()
        );
        let selector = NodeSelector::Uuid(node.uuid);
        let direct_paths = graph
            .paths(Some(&selector), Some(&selector), PathsOptions::default())
            .unwrap();
        let paths = projection
            .prepare_paths_invocation(Some(&selector), Some(&selector), &PathsOptions::default())
            .unwrap();
        assert_eq!(
            direct_paths,
            projection.graph.invoke_descriptor(&paths).unwrap()
        );
        let direct_analyze = graph.analyze(None, AnalyzeOptions::default()).unwrap();
        let analyze = projection
            .prepare_analyze_invocation(None, &AnalyzeOptions::default())
            .unwrap();
        assert_eq!(
            direct_analyze,
            projection.graph.invoke_descriptor(&analyze).unwrap()
        );
        let direct_similar = graph.similar("Person", SimilarOptions::default()).unwrap();
        let similar = projection
            .prepare_similar_invocation("Person", &SimilarOptions::default())
            .unwrap();
        assert_eq!(
            direct_similar,
            projection.graph.invoke_descriptor(&similar).unwrap()
        );
        INJECT_ATTACHMENT_FAILURE.with(|enabled| enabled.set(true));
        let outcome = graph
            .invoke_resolved_recorded(
                &projection,
                ResolvedRecordedAlgorithmRequest {
                    recorded: RecordedAlgorithmRequest {
                        context: context(6),
                        run_uuid: uuid7(7),
                        descriptor: descriptor.clone(),
                        cancellation: None,
                    },
                    attachment_uuid: uuid7(8),
                },
            )
            .unwrap();
        let ResolvedAttachmentOutcome::Failed {
            attachment_uuid,
            run_uuid,
            error_code,
        } = outcome.attachment
        else {
            panic!("the injected attachment failure must not erase the completed run");
        };
        assert_eq!(attachment_uuid, uuid7(8));
        assert_eq!(run_uuid, uuid7(7));
        assert_eq!(error_code, "GF_PUBLICATION_FAILED");
        assert_eq!(outcome.recorded.result.batches[0], resolved);
        graph.algorithm_run(uuid7(7), None).unwrap();
        INJECT_ATTACHMENT_FAILURE.with(|enabled| enabled.set(false));
        let replay = graph
            .attach_resolved_run(
                &projection,
                AttachResolvedRunRequest {
                    context: context(6),
                    attachment_uuid: uuid7(8),
                    run_uuid: uuid7(7),
                    descriptor,
                },
            )
            .unwrap();
        let exact_replay = graph
            .attach_resolved_run(
                &projection,
                AttachResolvedRunRequest {
                    context: context(6),
                    attachment_uuid: uuid7(8),
                    run_uuid: uuid7(7),
                    descriptor: projection
                        .prepare_rank_invocation("Person", &options)
                        .unwrap(),
                },
            )
            .unwrap();
        assert_eq!(replay.batches, exact_replay.batches);
        graph.set_clock_for_test(|| Ok(20));
        graph
            .record_assertion_status(RecordAssertionStatusRequest {
                context: context(9),
                status_event_uuid: uuid7(10),
                assertion_uuid: uuid7(5),
                status: AssertionStatus::Retracted,
                confidence_uuid: None,
                reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap();
        let current_snapshot = graph.epistemic_snapshot(i64::MAX).unwrap();
        let current_status = current_snapshot.batches[0]
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(current_status.value(0), "retracted");
        let current_generation =
            gf_storage::resolve_project_generation(graph.resolved_generation.container_root())
                .unwrap();
        let current_refs = crate::knowledge::read_ledger(&current_generation)
            .unwrap()
            .graph_refs
            .into_iter()
            .map(|row| (row.assertion_uuid, row.graph_uuid, row.graph_kind))
            .collect::<Vec<_>>();
        let current_selection =
            resolve_selection(&current_snapshot.batches[0], None, &policy, &current_refs).unwrap();
        assert!(current_selection.node_uuids.is_empty());
        let empty_target = tempfile::tempdir().unwrap();
        let empty_summary = gf_storage::materialize_graph_projection(
            &graph.dir,
            empty_target.path(),
            &gf_storage::GraphProjectionSelection::default(),
        )
        .unwrap();
        assert!(empty_summary.node_uuids.is_empty());
        assert_eq!(
            gf_storage::read_nodes(empty_target.path())
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        let before_retraction = graph
            .resolve_belief_projection(ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: 15,
                valid_time_micros: None,
                policy: policy.clone(),
            })
            .unwrap();
        assert_eq!(
            gf_storage::read_nodes(&before_retraction.graph.dir)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        let after_retraction = graph
            .resolve_belief_projection(ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: i64::MAX,
                valid_time_micros: None,
                policy: policy.clone(),
            })
            .unwrap();
        assert!(
            gf_storage::read_nodes(&after_retraction.graph.dir)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>()
                == 0
        );
        graph.set_clock_for_test(|| Ok(30));
        graph
            .record_assertion_validity(RecordAssertionValidityRequest {
                context: context(31),
                validity_event_uuid: uuid7(32),
                assertion_uuid: uuid7(5),
                valid_from_micros: Some(100),
                valid_to_micros: Some(200),
                reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap();
        let valid_policy = BeliefProjectionPolicyV1 {
            included_statuses: vec![AssertionStatus::Retracted],
            ..policy.clone()
        };
        let inside_validity = graph
            .resolve_belief_projection(ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: i64::MAX,
                valid_time_micros: Some(199),
                policy: valid_policy.clone(),
            })
            .unwrap();
        assert_eq!(
            gf_storage::read_nodes(&inside_validity.graph.dir)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        let exclusive_upper = graph
            .resolve_belief_projection(ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: i64::MAX,
                valid_time_micros: Some(200),
                policy: valid_policy,
            })
            .unwrap();
        assert_eq!(
            gf_storage::read_nodes(&exclusive_upper.graph.dir)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
    }

    #[test]
    fn statusless_reject_is_structured_and_policy_order_is_canonical() {
        let first = BeliefProjectionPolicyV1 {
            included_statuses: vec![AssertionStatus::Supported, AssertionStatus::Disputed],
            statusless: StatuslessPolicyV1::Reject,
            supersession_branches: SupersessionBranchPolicyV1::Reject,
            hypotheses: HypothesisSelectionPolicyV1::RequireSelected,
        };
        let second = BeliefProjectionPolicyV1 {
            included_statuses: vec![AssertionStatus::Disputed, AssertionStatus::Supported],
            ..first.clone()
        };
        assert_eq!(
            policy_fingerprint(&first).unwrap(),
            policy_fingerprint(&second).unwrap()
        );
    }

    #[test]
    fn empty_question_key_does_not_match_null_group_key() {
        let snapshot = policy_snapshot(false, false);
        let mut columns = snapshot.columns().to_vec();
        columns[3] = Arc::new(StringArray::from(vec![None::<&str>; snapshot.num_rows()]));
        let null_key_snapshot = RecordBatch::try_new(snapshot.schema(), columns).unwrap();

        assert_eq!(
            resolve_subject_evidence(
                &null_key_snapshot,
                &BeliefSubjectV1::HypothesisQuestionKey(String::new()),
            )
            .unwrap_err()
            .code(),
            "GF_NOT_FOUND"
        );
    }

    #[test]
    fn subject_snapshot_and_projection_hold_one_generation_guard() {
        let root = tempfile::tempdir().unwrap();
        let graph = Arc::new(GraphForge::new(root.path().to_str()).unwrap());
        enable(&graph, CapabilityId::Provenance, 70);
        enable(&graph, CapabilityId::Knowledge, 71);
        enable(&graph, CapabilityId::Epistemic, 72);
        let node = graph.add_node("Person", &HashMap::new()).unwrap();
        graph
            .create_assertion(CreateAssertionRequest {
                context: context(73),
                assertion_uuid: uuid7(74),
                claim: "pinned subject".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap();
        let request = ResolveBeliefSubjectRequest {
            subject: BeliefSubjectV1::Assertion(uuid7(74)),
            projection: ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: i64::MAX,
                valid_time_micros: None,
                policy: BeliefProjectionPolicyV1 {
                    included_statuses: Vec::new(),
                    statusless: StatuslessPolicyV1::Include,
                    supersession_branches: SupersessionBranchPolicyV1::IncludeAllLeaves,
                    hypotheses: HypothesisSelectionPolicyV1::IncludeAllCurrentMembers,
                },
            },
        };
        let (phase_tx, phase_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let resolver_graph = Arc::clone(&graph);
        let resolver = std::thread::spawn(move || {
            SUBJECT_RESOLUTION_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    phase_tx.try_send(()).map_err(|_| {
                        GfError::Storage("belief subject test barrier disconnected".into())
                    })?;
                    release_rx
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(|_| {
                            GfError::Storage("belief subject test barrier timed out".into())
                        })
                }));
            });
            resolver_graph.resolve_belief_subject(&request)
        });
        phase_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(graph.graph_visibility.try_lock().is_err());
        let (writer_started_tx, writer_started_rx) = mpsc::sync_channel(1);
        let writer_graph = Arc::clone(&graph);
        let writer = std::thread::spawn(move || {
            writer_started_tx.try_send(()).unwrap();
            writer_graph.add_node("Person", &HashMap::new())
        });
        writer_started_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert!(graph.graph_visibility.try_lock().is_err());
        release_tx.try_send(()).unwrap();
        let resolved = resolver.join().unwrap().unwrap();
        writer.join().unwrap().unwrap();
        assert_eq!(resolved.evidence.batches.len(), 1);
        let evidence = &resolved.evidence.batches[0];
        assert_eq!(evidence.num_rows(), 1);
        assert!(
            strings(evidence, "status").unwrap().is_null(0),
            "a concurrent publication must not invent assertion status"
        );
        let generations = fixed(evidence, "source_generation_uuid").unwrap();
        assert_eq!(
            generations.value(0),
            resolved.projection.source_generation_uuid().as_bytes(),
            "evidence and projection must identify one pinned generation"
        );
        let fingerprints = fixed(evidence, "snapshot_fingerprint").unwrap();
        assert_eq!(
            fingerprints.value(0),
            resolved.projection.snapshot_fingerprint(),
            "evidence and projection must share one pinned snapshot"
        );
        assert_eq!(
            fixed(evidence, "policy_fingerprint").unwrap().value(0),
            resolved.projection.policy_fingerprint(),
            "evidence and projection must share one pinned policy"
        );
        assert_eq!(
            fixed(evidence, "graph_content_fingerprint")
                .unwrap()
                .value(0),
            resolved.projection.graph_content_fingerprint(),
            "evidence and projection must share one pinned graph result"
        );
        assert_ne!(
            resolved.projection.source_generation_uuid(),
            graph.generation_for_read().unwrap().generation_uuid(),
            "the concurrent writer must publish only after the pinned read completes"
        );
    }

    #[test]
    fn supersession_branches_and_multi_group_conflicts_require_explicit_resolution() {
        let branch = policy_snapshot(true, false);
        let reject_branch = BeliefProjectionPolicyV1 {
            included_statuses: vec![AssertionStatus::Supported],
            statusless: StatuslessPolicyV1::Exclude,
            supersession_branches: SupersessionBranchPolicyV1::Reject,
            hypotheses: HypothesisSelectionPolicyV1::RequireSelected,
        };
        let refs = vec![
            (uuid7(40), uuid7(50), GraphObjectKind::Node),
            (uuid7(41), uuid7(51), GraphObjectKind::Node),
            (uuid7(42), uuid7(52), GraphObjectKind::Node),
        ];
        assert_eq!(
            resolve_selection(&branch, None, &reject_branch, &refs)
                .unwrap_err()
                .code(),
            "GF_AMBIGUOUS_PROJECTION"
        );
        let conflict = policy_snapshot(false, true);
        let include_leaves = BeliefProjectionPolicyV1 {
            supersession_branches: SupersessionBranchPolicyV1::IncludeAllLeaves,
            ..reject_branch
        };
        assert_eq!(
            resolve_selection(&conflict, None, &include_leaves, &refs)
                .unwrap_err()
                .code(),
            "GF_AMBIGUOUS_PROJECTION"
        );
        let resolved =
            resolve_selection(&policy_snapshot(false, false), None, &include_leaves, &refs)
                .unwrap();
        assert_eq!(resolved.node_uuids, BTreeSet::from([uuid7(51)]));
    }

    fn policy_snapshot(branch: bool, contradictory_group: bool) -> RecordBatch {
        let rows = 3 + usize::from(contradictory_group) + 1;
        let mut kinds = StringBuilder::new();
        let mut assertions = FixedSizeBinaryBuilder::new(16);
        let mut group_ids = FixedSizeBinaryBuilder::new(16);
        let mut question_keys = StringBuilder::new();
        let mut statuses = StringBuilder::new();
        let mut superseded = uuid_lists();
        let mut members = uuid_lists();
        let mut selected = FixedSizeBinaryBuilder::new(16);
        let mut sources = uuid_lists();
        let mut fingerprints = FixedSizeBinaryBuilder::new(32);
        for index in 0..3 {
            kinds.append_value("assertion");
            assertions
                .append_value(uuid7(40 + index).as_bytes())
                .unwrap();
            group_ids.append_null();
            question_keys.append_null();
            statuses.append_value("supported");
            if index == 0 && branch {
                append_test_list(&mut superseded, &[uuid7(41), uuid7(42)]);
            } else if index == 0 {
                append_test_list(&mut superseded, &[uuid7(41)]);
            } else {
                append_test_list(&mut superseded, &[]);
            }
            append_test_list(&mut members, &[]);
            selected.append_null();
            append_test_list(&mut sources, &[uuid7(40 + index)]);
            fingerprints.append_value([9; 32]).unwrap();
        }
        for (group_index, selected_uuid) in std::iter::once(uuid7(41))
            .chain(contradictory_group.then_some(uuid7(42)).into_iter())
            .enumerate()
        {
            kinds.append_value("hypothesis_group");
            assertions.append_null();
            group_ids
                .append_value(uuid7(60 + u8::try_from(group_index).unwrap()).as_bytes())
                .unwrap();
            question_keys.append_value("question.0");
            statuses.append_null();
            append_test_list(&mut superseded, &[]);
            append_test_list(&mut members, &[uuid7(41), uuid7(42)]);
            selected.append_value(selected_uuid.as_bytes()).unwrap();
            append_test_list(&mut sources, &[selected_uuid]);
            fingerprints.append_value([9; 32]).unwrap();
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity_kind", DataType::Utf8, false),
            Field::new("assertion_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("group_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("question_key", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
            uuid_list_field("superseded_by_assertion_uuids"),
            uuid_list_field("current_member_assertion_uuids"),
            Field::new(
                "selected_assertion_uuid",
                DataType::FixedSizeBinary(16),
                true,
            ),
            uuid_list_field("source_record_uuids"),
            Field::new("snapshot_fingerprint", DataType::FixedSizeBinary(32), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(kinds.finish()),
                Arc::new(assertions.finish()),
                Arc::new(group_ids.finish()),
                Arc::new(question_keys.finish()),
                Arc::new(statuses.finish()),
                Arc::new(superseded.finish()),
                Arc::new(members.finish()),
                Arc::new(selected.finish()),
                Arc::new(sources.finish()),
                Arc::new(fingerprints.finish()),
            ],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), rows);
        batch
    }

    fn uuid_lists() -> ListBuilder<FixedSizeBinaryBuilder> {
        ListBuilder::new(FixedSizeBinaryBuilder::new(16)).with_field(Arc::new(Field::new(
            "item",
            DataType::FixedSizeBinary(16),
            false,
        )))
    }

    fn uuid_list_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::FixedSizeBinary(16),
                false,
            ))),
            false,
        )
    }

    fn append_test_list(builder: &mut ListBuilder<FixedSizeBinaryBuilder>, values: &[Uuid]) {
        for value in values {
            builder.values().append_value(value.as_bytes()).unwrap();
        }
        builder.append(true);
    }
}
