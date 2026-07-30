//! Public orchestration for append-only hypothesis membership and selection.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use gf_core::{ApiErrorCode, GfError, ProjectErrorCode};
use gf_knowledge::{
    EPISTEMIC_CAPABILITY_VERSION, HYPOTHESIS_GROUP_SCHEMA, HYPOTHESIS_MEMBERSHIP_SCHEMA,
    HYPOTHESIS_SELECTION_SCHEMA, HypothesisGroup, HypothesisLedger, HypothesisMembershipAction,
    HypothesisMembershipEvent, HypothesisSelectionEvent, schema_registry,
};
use gf_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectStageOutcome,
    ResolvedProjectGeneration,
};
use uuid::Uuid;

use crate::{GraphForge, PageRequest, PageToken, WriteContext};

/// Frozen request for one immutable hypothesis group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateHypothesisGroupRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 group identity.
    pub group_uuid: Uuid,
    /// Canonical versioned question key.
    pub question_key: String,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
}

/// Frozen request for one append-only membership event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordHypothesisMembershipRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 event identity.
    pub membership_event_uuid: Uuid,
    /// Existing group.
    pub group_uuid: Uuid,
    /// Existing immutable assertion.
    pub assertion_uuid: Uuid,
    /// Explicit add/remove action.
    pub action: HypothesisMembershipAction,
    /// Existing reasoning record.
    pub reasoning_uuid: Uuid,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
}

/// Frozen request for one explicit selection or clear event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordHypothesisSelectionRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 event identity.
    pub selection_event_uuid: Uuid,
    /// Existing group.
    pub group_uuid: Uuid,
    /// Existing current member, or `None` to clear.
    pub selected_assertion_uuid: Option<Uuid>,
    /// Existing reasoning record.
    pub reasoning_uuid: Uuid,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
}

/// Frozen atomic selected-member removal plus explicit selection change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveHypothesisMemberRequest {
    /// Idempotency identity and optional actor shared by both events.
    pub context: WriteContext,
    /// Caller-supplied removal-event UUID.
    pub membership_event_uuid: Uuid,
    /// Caller-supplied selection-event UUID.
    pub selection_event_uuid: Uuid,
    /// Existing group.
    pub group_uuid: Uuid,
    /// Current member to remove.
    pub assertion_uuid: Uuid,
    /// Replacement current member, or `None` to clear.
    pub selected_assertion_uuid: Option<Uuid>,
    /// Existing decision reasoning.
    pub reasoning_uuid: Uuid,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
}

/// Frozen group-history filter and page.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListHypothesisGroupsRequest {
    /// Optional exact canonical question-key filter.
    pub question_key: Option<String>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Frozen membership-history filter and page.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListHypothesisMembershipRequest {
    /// Optional group UUID filter.
    pub group_uuid: Option<Uuid>,
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Frozen selection-history filter and page.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListHypothesisSelectionRequest {
    /// Optional group UUID filter.
    pub group_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

impl GraphForge {
    /// Create one immutable hypothesis group.
    pub fn create_hypothesis_group(
        &self,
        request: CreateHypothesisGroupRequest,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        let CreateHypothesisGroupRequest {
            context,
            group_uuid,
            question_key,
            provenance_uuid,
        } = request;
        validate_context(&context)?;
        validate_references(self, None, None, provenance_uuid)?;
        self.publish_hypothesis_change(&context, |existing, recorded_at| {
            if let Some(index) = existing
                .groups()
                .iter()
                .position(|row| row.group_uuid == group_uuid)
            {
                let row = &existing.groups()[index];
                if row.question_key == question_key && row.provenance_uuid == provenance_uuid {
                    return Ok(Change::Replay(Family::Groups, index));
                }
                return Err(conflict("group UUID was reused for different content"));
            }
            if existing
                .groups()
                .iter()
                .any(|row| row.question_key == question_key)
            {
                return Err(conflict(
                    "canonical question key already belongs to a group",
                ));
            }
            let mut groups = existing.groups().to_vec();
            groups.push(
                HypothesisGroup::new(group_uuid, question_key, provenance_uuid, recorded_at)
                    .map_err(crate::knowledge::knowledge_error)?,
            );
            Ok(Change::Publish(
                HypothesisLedger::new(
                    groups,
                    existing.membership_events().to_vec(),
                    existing.selection_events().to_vec(),
                )
                .map_err(crate::knowledge::knowledge_error)?,
                Family::Groups,
                group_uuid,
            ))
        })
    }

    /// Append one explicit membership transition.
    pub fn record_hypothesis_membership(
        &self,
        request: &RecordHypothesisMembershipRequest,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        validate_context(&request.context)?;
        validate_references(
            self,
            Some(request.assertion_uuid),
            Some(request.reasoning_uuid),
            request.provenance_uuid,
        )?;
        self.publish_hypothesis_change(&request.context, |existing, recorded_at| {
            if let Some(index) = existing
                .membership_events()
                .iter()
                .position(|row| row.membership_event_uuid == request.membership_event_uuid)
            {
                let row = &existing.membership_events()[index];
                if row.operation_uuid == request.context.operation_uuid.0
                    && row.group_uuid == request.group_uuid
                    && row.assertion_uuid == request.assertion_uuid
                    && row.action == request.action
                    && row.reasoning_uuid == request.reasoning_uuid
                    && row.provenance_uuid == request.provenance_uuid
                {
                    return Ok(Change::Replay(Family::Membership, index));
                }
                return Err(conflict(
                    "membership event UUID was reused for different content",
                ));
            }
            let mut membership = existing.membership_events().to_vec();
            membership.push(
                HypothesisMembershipEvent::new(
                    request.membership_event_uuid,
                    request.context.operation_uuid.0,
                    request.group_uuid,
                    request.assertion_uuid,
                    request.action,
                    request.reasoning_uuid,
                    request.provenance_uuid,
                    recorded_at,
                )
                .map_err(crate::knowledge::knowledge_error)?,
            );
            Ok(Change::Publish(
                HypothesisLedger::new(
                    existing.groups().to_vec(),
                    membership,
                    existing.selection_events().to_vec(),
                )
                .map_err(crate::knowledge::knowledge_error)?,
                Family::Membership,
                request.membership_event_uuid,
            ))
        })
    }

    /// Append one explicit selection or clear event.
    pub fn record_hypothesis_selection(
        &self,
        request: &RecordHypothesisSelectionRequest,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        validate_context(&request.context)?;
        if let Some(assertion_uuid) = request.selected_assertion_uuid {
            require_uuid(assertion_uuid, "selected_assertion_uuid")?;
        }
        validate_references(
            self,
            None,
            Some(request.reasoning_uuid),
            request.provenance_uuid,
        )?;
        self.publish_hypothesis_change(&request.context, |existing, recorded_at| {
            if let Some(index) = existing
                .selection_events()
                .iter()
                .position(|row| row.selection_event_uuid == request.selection_event_uuid)
            {
                let row = &existing.selection_events()[index];
                if row.operation_uuid == request.context.operation_uuid.0
                    && row.group_uuid == request.group_uuid
                    && row.selected_assertion_uuid == request.selected_assertion_uuid
                    && row.reasoning_uuid == request.reasoning_uuid
                    && row.provenance_uuid == request.provenance_uuid
                {
                    return Ok(Change::Replay(Family::Selection, index));
                }
                return Err(conflict(
                    "selection event UUID was reused for different content",
                ));
            }
            let mut selection = existing.selection_events().to_vec();
            selection.push(
                HypothesisSelectionEvent::new(
                    request.selection_event_uuid,
                    request.context.operation_uuid.0,
                    request.group_uuid,
                    request.selected_assertion_uuid,
                    request.reasoning_uuid,
                    request.provenance_uuid,
                    recorded_at,
                )
                .map_err(crate::knowledge::knowledge_error)?,
            );
            Ok(Change::Publish(
                HypothesisLedger::new(
                    existing.groups().to_vec(),
                    existing.membership_events().to_vec(),
                    selection,
                )
                .map_err(crate::knowledge::knowledge_error)?,
                Family::Selection,
                request.selection_event_uuid,
            ))
        })
    }

    /// Atomically remove one member and explicitly clear/change selection.
    pub fn remove_hypothesis_member(
        &self,
        request: &RemoveHypothesisMemberRequest,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        validate_context(&request.context)?;
        validate_references(
            self,
            Some(request.assertion_uuid),
            Some(request.reasoning_uuid),
            request.provenance_uuid,
        )?;
        self.publish_hypothesis_change(&request.context, |existing, recorded_at| {
            let mut membership = existing.membership_events().to_vec();
            let mut selection = existing.selection_events().to_vec();
            let existing_membership = membership
                .iter()
                .position(|row| row.membership_event_uuid == request.membership_event_uuid);
            let existing_selection = selection
                .iter()
                .position(|row| row.selection_event_uuid == request.selection_event_uuid);
            match (existing_membership, existing_selection) {
                (Some(member_index), Some(selection_index)) => {
                    let member = &membership[member_index];
                    let selected = &selection[selection_index];
                    if member.operation_uuid == request.context.operation_uuid.0
                        && member.group_uuid == request.group_uuid
                        && member.assertion_uuid == request.assertion_uuid
                        && member.action == HypothesisMembershipAction::Removed
                        && member.reasoning_uuid == request.reasoning_uuid
                        && member.provenance_uuid == request.provenance_uuid
                        && selected.operation_uuid == request.context.operation_uuid.0
                        && selected.group_uuid == request.group_uuid
                        && selected.selected_assertion_uuid == request.selected_assertion_uuid
                        && selected.reasoning_uuid == request.reasoning_uuid
                        && selected.provenance_uuid == request.provenance_uuid
                    {
                        return Ok(Change::Replay(Family::Membership, member_index));
                    }
                    return Err(conflict(
                        "member-removal bundle identity was reused for different content",
                    ));
                }
                (None, None) => {}
                _ => {
                    return Err(conflict("member-removal bundle is only partially replayed"));
                }
            }
            membership.push(
                HypothesisMembershipEvent::new(
                    request.membership_event_uuid,
                    request.context.operation_uuid.0,
                    request.group_uuid,
                    request.assertion_uuid,
                    HypothesisMembershipAction::Removed,
                    request.reasoning_uuid,
                    request.provenance_uuid,
                    recorded_at,
                )
                .map_err(crate::knowledge::knowledge_error)?,
            );
            selection.push(
                HypothesisSelectionEvent::new(
                    request.selection_event_uuid,
                    request.context.operation_uuid.0,
                    request.group_uuid,
                    request.selected_assertion_uuid,
                    request.reasoning_uuid,
                    request.provenance_uuid,
                    recorded_at,
                )
                .map_err(crate::knowledge::knowledge_error)?,
            );
            Ok(Change::Publish(
                HypothesisLedger::new(existing.groups().to_vec(), membership, selection)
                    .map_err(crate::knowledge::knowledge_error)?,
                Family::Membership,
                request.membership_event_uuid,
            ))
        })
    }

    /// Return deterministic hypothesis-group history.
    pub fn list_hypothesis_groups(
        &self,
        request: &ListHypothesisGroupsRequest,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        let generation = resolve(self)?;
        let ledger = read_ledger(&generation)?;
        let batch = ledger
            .group_batch()
            .map_err(crate::knowledge::knowledge_error)?;
        let selected = ledger
            .groups()
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .question_key
                    .as_deref()
                    .is_none_or(|key| row.question_key == key)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        page_rows(
            &generation,
            &batch,
            &selected,
            &request.page,
            &HYPOTHESIS_GROUP_SCHEMA,
        )
    }

    /// Return deterministic membership history.
    pub fn list_hypothesis_membership(
        &self,
        request: &ListHypothesisMembershipRequest,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        let generation = resolve(self)?;
        let ledger = read_ledger(&generation)?;
        let batch = ledger
            .membership_batch()
            .map_err(crate::knowledge::knowledge_error)?;
        let selected = ledger
            .membership_events()
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request.group_uuid.is_none_or(|id| row.group_uuid == id)
                    && request
                        .assertion_uuid
                        .is_none_or(|id| row.assertion_uuid == id)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        page_rows(
            &generation,
            &batch,
            &selected,
            &request.page,
            &HYPOTHESIS_MEMBERSHIP_SCHEMA,
        )
    }

    /// Return deterministic selection history.
    pub fn list_hypothesis_selection(
        &self,
        request: &ListHypothesisSelectionRequest,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        let generation = resolve(self)?;
        let ledger = read_ledger(&generation)?;
        let batch = ledger
            .selection_batch()
            .map_err(crate::knowledge::knowledge_error)?;
        let selected = ledger
            .selection_events()
            .iter()
            .enumerate()
            .filter(|(_, row)| request.group_uuid.is_none_or(|id| row.group_uuid == id))
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        page_rows(
            &generation,
            &batch,
            &selected,
            &request.page,
            &HYPOTHESIS_SELECTION_SCHEMA,
        )
    }

    /// Return current members as their latest visible membership events.
    pub fn hypothesis_members(
        &self,
        group_uuid: Uuid,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        require_uuid(group_uuid, "group_uuid")?;
        let generation = resolve(self)?;
        let ledger = read_ledger(&generation)?;
        let members = ledger.current_members(group_uuid);
        let batch = ledger
            .membership_batch()
            .map_err(crate::knowledge::knowledge_error)?;
        let rows = members
            .iter()
            .filter_map(|assertion_uuid| {
                ledger
                    .membership_events()
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, row)| {
                        row.group_uuid == group_uuid
                            && row.assertion_uuid == *assertion_uuid
                            && row.action == HypothesisMembershipAction::Added
                    })
                    .map(|(index, _)| batch.slice(index, 1))
            })
            .collect::<Vec<_>>();
        Ok(crate::knowledge::assertion_result(
            crate::knowledge::concat_or_empty(&rows, &HYPOTHESIS_MEMBERSHIP_SCHEMA)?,
        ))
    }

    /// Return the current explicit selection event, or an empty Arrow table.
    pub fn hypothesis_selection(
        &self,
        group_uuid: Uuid,
    ) -> Result<gf_exec::ExecutionResult, GfError> {
        require_uuid(group_uuid, "group_uuid")?;
        let generation = resolve(self)?;
        let ledger = read_ledger(&generation)?;
        let source = ledger
            .selection_batch()
            .map_err(crate::knowledge::knowledge_error)?;
        let rows = ledger
            .selection_events()
            .iter()
            .enumerate()
            .rev()
            .find(|(_, row)| row.group_uuid == group_uuid)
            .map_or_else(Vec::new, |(index, _)| vec![source.slice(index, 1)]);
        Ok(crate::knowledge::assertion_result(
            crate::knowledge::concat_or_empty(&rows, &HYPOTHESIS_SELECTION_SCHEMA)?,
        ))
    }

    fn publish_hypothesis_change<F>(
        &self,
        context: &WriteContext,
        build: F,
    ) -> Result<gf_exec::ExecutionResult, GfError>
    where
        F: FnOnce(&HypothesisLedger, i64) -> Result<Change, GfError>,
    {
        let _visibility = crate::knowledge::lock_graph_visibility(self)?;
        let parent = resolve_for_write(self)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(conflict(
                "project generation changed before hypothesis publication",
            ));
        }
        let existing = read_ledger(&parent)?;
        let recorded_at = (self.clock.lock().expect("clock lock poisoned"))()?;
        match build(&existing, recorded_at)? {
            Change::Replay(family, index) => result_row(&existing, family, index),
            Change::Publish(ledger, family, id) => {
                publish(self, context, &parent, expected_parent, &ledger)?;
                let committed = read_ledger(&resolve_for_write(self)?)?;
                let index = match family {
                    Family::Groups => committed
                        .groups()
                        .iter()
                        .position(|row| row.group_uuid == id),
                    Family::Membership => committed
                        .membership_events()
                        .iter()
                        .position(|row| row.membership_event_uuid == id),
                    Family::Selection => committed
                        .selection_events()
                        .iter()
                        .position(|row| row.selection_event_uuid == id),
                }
                .ok_or_else(|| GfError::Validation("committed hypothesis row is absent".into()))?;
                result_row(&committed, family, index)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Family {
    Groups,
    Membership,
    Selection,
}

enum Change {
    Replay(Family, usize),
    Publish(HypothesisLedger, Family, Uuid),
}

fn resolve(graph: &GraphForge) -> Result<ResolvedProjectGeneration, GfError> {
    graph.generation_for_read()
}

fn resolve_for_write(graph: &GraphForge) -> Result<ResolvedProjectGeneration, GfError> {
    gf_storage::resolve_project_generation(graph.resolved_generation.container_root())
}

pub(crate) fn read_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<HypothesisLedger, GfError> {
    generation.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
    let group_snapshot = generation.participant_snapshot("epistemic", "hypothesis_groups")?;
    let membership_snapshot =
        generation.participant_snapshot("epistemic", "hypothesis_membership_events")?;
    let selection_snapshot =
        generation.participant_snapshot("epistemic", "hypothesis_selection_events")?;
    let present = [
        group_snapshot.is_some(),
        membership_snapshot.is_some(),
        selection_snapshot.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if present != 0 && present != 3 {
        return Err(GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: "epistemic hypothesis participant set is incomplete".into(),
        });
    }
    let groups = read_family(
        group_snapshot,
        "hypothesis_groups",
        &HYPOTHESIS_GROUP_SCHEMA,
    )?;
    let membership = read_family(
        membership_snapshot,
        "hypothesis_membership_events",
        &HYPOTHESIS_MEMBERSHIP_SCHEMA,
    )?;
    let selection = read_family(
        selection_snapshot,
        "hypothesis_selection_events",
        &HYPOTHESIS_SELECTION_SCHEMA,
    )?;
    HypothesisLedger::from_batches(&groups, &membership, &selection)
        .map_err(crate::knowledge::knowledge_error)
}

fn read_family(
    snapshot: Option<gf_storage::ProjectParticipantSnapshot>,
    family: &str,
    schema: &Arc<arrow::datatypes::Schema>,
) -> Result<Vec<RecordBatch>, GfError> {
    match snapshot {
        None => Ok(vec![RecordBatch::new_empty(Arc::clone(schema))]),
        Some(snapshot) => {
            crate::knowledge::require_participant_contract(&snapshot, family)?;
            if snapshot.row_count == 0 {
                Ok(vec![RecordBatch::new_empty(Arc::clone(schema))])
            } else {
                crate::knowledge::read_parquet(&snapshot.bytes)
            }
        }
    }
}

pub(crate) fn empty_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    publication_participants_from_ledger(&HypothesisLedger::default())
}

fn publication_participants_from_ledger(
    ledger: &HypothesisLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = Vec::new();
    append_ledger_participants(&mut participants, ledger)?;
    Ok(participants)
}

fn publish(
    graph: &GraphForge,
    context: &WriteContext,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    ledger: &HypothesisLedger,
) -> Result<(), GfError> {
    let participants = publication_participants(parent, ledger)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let request = ProjectGenerationRequest {
        transaction_uuid: context.operation_uuid.0,
        generation_uuid: crate::knowledge::knowledge_generation_uuid(
            b"hypothesis",
            context.operation_uuid,
            &participants,
        ),
        capabilities,
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
                        return Err(conflict(
                            "project generation changed before hypothesis publication",
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

fn publication_participants(
    parent: &ResolvedProjectGeneration,
    ledger: &HypothesisLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "epistemic"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "hypothesis_groups"
                        | "hypothesis_membership_events"
                        | "hypothesis_selection_events"
                ))
        })
        .map(crate::knowledge::snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    append_ledger_participants(&mut participants, ledger)?;
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(crate) fn append_ledger_participants(
    participants: &mut Vec<ProjectParticipant>,
    ledger: &HypothesisLedger,
) -> Result<(), GfError> {
    for (family, batch) in [
        (
            "hypothesis_groups",
            ledger
                .group_batch()
                .map_err(crate::knowledge::knowledge_error)?,
        ),
        (
            "hypothesis_membership_events",
            ledger
                .membership_batch()
                .map_err(crate::knowledge::knowledge_error)?,
        ),
        (
            "hypothesis_selection_events",
            ledger
                .selection_batch()
                .map_err(crate::knowledge::knowledge_error)?,
        ),
    ] {
        let registry = schema_registry()
            .into_iter()
            .find(|entry| entry.record_family == family)
            .expect("registered hypothesis family");
        participants.push(crate::knowledge::participant(&registry, &batch)?);
    }
    Ok(())
}

fn result_row(
    ledger: &HypothesisLedger,
    family: Family,
    index: usize,
) -> Result<gf_exec::ExecutionResult, GfError> {
    let batch = match family {
        Family::Groups => ledger
            .group_batch()
            .map_err(crate::knowledge::knowledge_error)?,
        Family::Membership => ledger
            .membership_batch()
            .map_err(crate::knowledge::knowledge_error)?,
        Family::Selection => ledger
            .selection_batch()
            .map_err(crate::knowledge::knowledge_error)?,
    };
    Ok(crate::knowledge::assertion_result(batch.slice(index, 1)))
}

fn page_rows(
    generation: &ResolvedProjectGeneration,
    source: &RecordBatch,
    selected: &[usize],
    page: &PageRequest,
    schema: &Arc<arrow::datatypes::Schema>,
) -> Result<gf_exec::ExecutionResult, GfError> {
    let (start, end) =
        crate::paging::validate_page(page, generation.generation_uuid(), selected.len())?;
    let rows = selected[start..end]
        .iter()
        .map(|index| source.slice(*index, 1))
        .collect::<Vec<_>>();
    let batch = crate::knowledge::concat_or_empty(&rows, schema)?;
    let next = (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
    Ok(crate::knowledge::assertion_result(
        crate::knowledge::with_next_token(&batch, next.as_ref())?,
    ))
}

fn validate_references(
    graph: &GraphForge,
    assertion_uuid: Option<Uuid>,
    reasoning_uuid: Option<Uuid>,
    provenance_uuid: Uuid,
) -> Result<(), GfError> {
    require_uuid(provenance_uuid, "provenance_uuid")?;
    let generation = resolve(graph)?;
    if let Some(assertion_uuid) = assertion_uuid
        && !crate::knowledge::read_ledger(&generation)?
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == assertion_uuid)
    {
        return Err(not_found("assertion"));
    }
    if let Some(reasoning_uuid) = reasoning_uuid
        && !crate::knowledge::read_reasoning_ledger(&generation)?
            .records
            .iter()
            .any(|row| {
                row.reasoning_uuid == reasoning_uuid
                    && assertion_uuid.is_none_or(|id| row.assertion_uuid == id)
            })
    {
        return Err(not_found("reasoning"));
    }
    if !crate::provenance::read_ledger(&generation)?
        .events
        .iter()
        .any(|row| row.provenance_uuid == provenance_uuid)
    {
        return Err(not_found("provenance"));
    }
    Ok(())
}

fn validate_context(context: &WriteContext) -> Result<(), GfError> {
    require_uuid(context.operation_uuid.0, "operation_uuid")?;
    if let Some(actor_uuid) = context.actor_uuid {
        require_uuid(actor_uuid, "actor_uuid")?;
    }
    Ok(())
}

fn require_uuid(uuid: Uuid, field: &'static str) -> Result<(), GfError> {
    if uuid.is_nil() {
        Err(GfError::Validation(format!("{field} must not be nil")))
    } else {
        Ok(())
    }
}

fn conflict(message: &'static str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::TransactionConflict,
        message: message.into(),
    }
}

fn not_found(kind: &'static str) -> GfError {
    GfError::Api {
        code: ApiErrorCode::NotFound,
        message: format!("{kind} was not found"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::FixedSizeBinaryArray;

    use super::*;
    use crate::{
        AssertionGraphRefInput, AssessConfidenceRequest, CapabilityId, ConfidencePolicyRequest,
        CreateAssertionRequest, EnableCapabilityRequest, OperationId, RecordReasoningRequest,
    };
    use gf_knowledge::{
        AssertionGraphRole, GraphObjectKind, ReasoningContentFormat, ReasoningKind,
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

    fn assertion(graph: &GraphForge, assertion_uuid: Uuid, operation_seed: u8) -> Uuid {
        let node = graph
            .add_node("HypothesisSubject", &HashMap::new())
            .unwrap();
        let result = graph
            .create_assertion(CreateAssertionRequest {
                context: context(operation_seed),
                assertion_uuid,
                claim: format!("claim {assertion_uuid}"),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap();
        let values = result.batches[0]
            .column_by_name("provenance_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        Uuid::from_slice(values.value(0)).unwrap()
    }

    fn reasoning(
        graph: &GraphForge,
        assertion_uuid: Uuid,
        provenance_uuid: Uuid,
        reasoning_seed: u8,
        operation_seed: u8,
    ) -> Uuid {
        let reasoning_uuid = uuid7(reasoning_seed);
        graph
            .record_reasoning(RecordReasoningRequest {
                context: context(operation_seed),
                reasoning_uuid,
                assertion_uuid,
                kind: ReasoningKind::DecisionRationale,
                content_format: ReasoningContentFormat::TextPlain,
                content: b"explicit hypothesis decision".to_vec(),
                supersedes_reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap();
        reasoning_uuid
    }

    #[test]
    fn explicit_selection_and_atomic_selected_member_removal_survive_reopen() {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        graph.set_clock_for_test(|| Ok(10));
        enable(&graph, CapabilityId::Provenance, 1);
        enable(&graph, CapabilityId::Knowledge, 2);
        enable(&graph, CapabilityId::Epistemic, 3);

        let first = uuid7(10);
        let second = uuid7(11);
        let first_provenance = assertion(&graph, first, 12);
        let second_provenance = assertion(&graph, second, 13);
        let first_reasoning = reasoning(&graph, first, first_provenance, 14, 15);
        let second_reasoning = reasoning(&graph, second, second_provenance, 16, 17);
        let group_uuid = uuid7(18);
        graph
            .create_hypothesis_group(CreateHypothesisGroupRequest {
                context: context(19),
                group_uuid,
                question_key: "risk.primary-cause.v1".into(),
                provenance_uuid: first_provenance,
            })
            .unwrap();
        for (event_seed, operation_seed, assertion_uuid, reasoning_uuid, provenance_uuid) in [
            (20, 90, first, first_reasoning, first_provenance),
            (22, 80, second, second_reasoning, second_provenance),
        ] {
            graph
                .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
                    context: context(operation_seed),
                    membership_event_uuid: uuid7(event_seed),
                    group_uuid,
                    assertion_uuid,
                    action: HypothesisMembershipAction::Added,
                    reasoning_uuid,
                    provenance_uuid,
                })
                .unwrap();
        }
        graph
            .assess_confidence(AssessConfidenceRequest {
                context: context(69),
                confidence_uuid: uuid7(68),
                assertion_uuid: second,
                policy: ConfidencePolicyRequest::Explicit { value: 1.0 },
            })
            .unwrap();
        assert_eq!(
            graph.hypothesis_selection(group_uuid).unwrap().batches[0].num_rows(),
            0,
            "even the highest confidence must not select implicitly"
        );
        graph
            .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
                context: context(70),
                selection_event_uuid: uuid7(25),
                group_uuid,
                selected_assertion_uuid: Some(first),
                reasoning_uuid: first_reasoning,
                provenance_uuid: first_provenance,
            })
            .unwrap();

        let generation_before = resolve(&graph).unwrap().generation_uuid();
        let error = graph
            .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
                context: context(26),
                membership_event_uuid: uuid7(27),
                group_uuid,
                assertion_uuid: first,
                action: HypothesisMembershipAction::Removed,
                reasoning_uuid: first_reasoning,
                provenance_uuid: first_provenance,
            })
            .unwrap_err();
        assert!(matches!(error, GfError::Validation(_)));
        assert_eq!(
            resolve(&graph).unwrap().generation_uuid(),
            generation_before,
            "rejected selected-member removal must not publish"
        );

        graph
            .remove_hypothesis_member(&RemoveHypothesisMemberRequest {
                context: context(60),
                membership_event_uuid: uuid7(29),
                selection_event_uuid: uuid7(30),
                group_uuid,
                assertion_uuid: first,
                selected_assertion_uuid: Some(second),
                reasoning_uuid: first_reasoning,
                provenance_uuid: first_provenance,
            })
            .unwrap();
        assert_eq!(
            graph.hypothesis_members(group_uuid).unwrap().batches[0].num_rows(),
            1
        );

        drop(graph);
        let reopened = GraphForge::new(root.path().to_str()).unwrap();
        let selected = reopened.hypothesis_selection(group_uuid).unwrap();
        assert_eq!(selected.batches[0].num_rows(), 1);
        let values = selected.batches[0]
            .column_by_name("selected_assertion_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(Uuid::from_slice(values.value(0)).unwrap(), second);
        assert_eq!(
            reopened
                .list_hypothesis_membership(&ListHypothesisMembershipRequest {
                    group_uuid: Some(group_uuid),
                    assertion_uuid: None,
                    page: PageRequest::default(),
                })
                .unwrap()
                .batches[0]
                .num_rows(),
            3
        );
    }
}
