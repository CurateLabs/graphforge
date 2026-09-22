//! Knowledge assertions operations.

use super::{
    ASSERTION_STATUS_SCHEMA, ASSERTION_SUPERSESSION_SCHEMA, Arc, AssertionLedger, AssertionStatus,
    AssertionStatusEvent, AssertionStatusLedger, AssertionSupersession,
    AssertionSupersessionLedger, CancellationToken, CreateAssertionRequest,
    CreateAssertionWithStatusRequest, Digest, EPISTEMIC_CAPABILITY_VERSION, GfError, GraphForge,
    ListAssertionStatusRequest, ListAssertionSupersessionsRequest, ListAssertionsRequest,
    OperationId, PageRequest, PageToken, Path, ProjectCapability, ProjectErrorCode,
    ProjectGenerationRequest, ProjectParticipant, ProjectStageOutcome, ProvenanceLedger,
    RecordAssertionStatusRequest, RecordBatch, ResolvedProjectGeneration, Sha256,
    SupersedeAssertionRequest, Uuid, assertion_publication_participants, assertion_result,
    assertion_status_bundle_participants, concat_or_empty, knowledge_error,
    knowledge_generation_uuid, lock_graph_visibility, merged_provenance, not_found, not_found_kind,
    read_confidence_ledger, read_ledger, read_reasoning_ledger, read_status_ledger,
    read_supersession_ledger, require_uuid, staged_assertion, status_publication_participants,
    supersession_publication_participants, transaction_conflict, validate_graph_refs,
    validate_write_context, with_next_token,
};

fn publish_status(
    graph: &GraphForge,
    request: &RecordAssertionStatusRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    status: &AssertionStatusLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = status_publication_participants(parent, status)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"assertion-status",
            request.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let graph_objects = graphforge_storage::begin_graph_object_publication(
        graph.resolved_generation.container_root(),
    )?;
    let receipt = match graph.stage_project_generation(&publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before assertion-status publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish_with_graph_objects(&graph_objects)?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    committed_status_event(root, request.status_event_uuid)
}

fn publish_supersession(
    graph: &GraphForge,
    request: &SupersedeAssertionRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    relations: &AssertionSupersessionLedger,
    statuses: &AssertionStatusLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = supersession_publication_participants(parent, relations, statuses)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"assertion-supersession",
            request.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let graph_objects = graphforge_storage::begin_graph_object_publication(
        graph.resolved_generation.container_root(),
    )?;
    let receipt = match graph.stage_project_generation(&publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before supersession publication",
                        ));
                    }
                    Ok(())
                },
            )?
            .publish_with_graph_objects(&graph_objects)?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    let committed = graphforge_storage::resolve_project_generation(root)?;
    let ledger = read_supersession_ledger(&committed)?;
    let index = ledger
        .relations()
        .iter()
        .position(|row| row.supersession_uuid == request.supersession_uuid)
        .ok_or_else(|| GfError::Validation("committed supersession is absent".into()))?;
    Ok(assertion_result(
        ledger.batch().map_err(knowledge_error)?.slice(index, 1),
    ))
}

fn publish_assertion_status_bundle(
    graph: &GraphForge,
    request: &CreateAssertionWithStatusRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    assertions: &AssertionLedger,
    status: &AssertionStatusLedger,
    provenance: &ProvenanceLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants =
        assertion_status_bundle_participants(parent, assertions, status, provenance)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.assertion.context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"assertion-status-bundle",
            request.assertion.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graph.stage_project_generation(&publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before assertion-status bundle publication",
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
    committed_status_event(root, request.first_status.status_event_uuid)
}

fn committed_status_event(
    root: &Path,
    status_event_uuid: Uuid,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let committed = graphforge_storage::resolve_project_generation(root)?;
    let ledger = read_status_ledger(&committed)?;
    let index = ledger
        .events
        .iter()
        .position(|row| row.status_event_uuid == status_event_uuid)
        .ok_or_else(|| GfError::Validation("committed status event is absent".into()))?;
    Ok(assertion_result(
        ledger.batch().map_err(knowledge_error)?.slice(index, 1),
    ))
}

fn validate_status_request(request: &RecordAssertionStatusRequest) -> Result<(), GfError> {
    require_uuid(request.status_event_uuid, "status_event_uuid")?;
    require_uuid(request.assertion_uuid, "assertion_uuid")?;
    if let Some(value) = request.confidence_uuid {
        require_uuid(value, "confidence_uuid")?;
    }
    if let Some(value) = request.reasoning_uuid {
        require_uuid(value, "reasoning_uuid")?;
    }
    require_uuid(request.provenance_uuid, "provenance_uuid")
}

fn assertion_refs_match(ledger: &AssertionLedger, request: &CreateAssertionRequest) -> bool {
    let mut existing = ledger
        .graph_refs
        .iter()
        .filter(|row| row.assertion_uuid == request.assertion_uuid)
        .map(|row| (row.graph_uuid, row.graph_kind, row.role, row.ordinal))
        .collect::<Vec<_>>();
    let mut requested = request
        .graph_refs
        .iter()
        .map(|row| (row.graph_uuid, row.graph_kind, row.role, row.ordinal))
        .collect::<Vec<_>>();
    existing.sort_by_key(|row| (row.2.as_str(), row.3, row.0));
    requested.sort_by_key(|row| (row.2.as_str(), row.3, row.0));
    existing == requested
}

fn validate_status_references(
    generation: &ResolvedProjectGeneration,
    assertion_uuid: Uuid,
    confidence_uuid: Option<Uuid>,
    reasoning_uuid: Option<Uuid>,
    provenance_uuid: Uuid,
) -> Result<(), GfError> {
    if !read_ledger(generation)?
        .assertions
        .iter()
        .any(|row| row.assertion_uuid == assertion_uuid)
    {
        return Err(not_found_kind("assertion"));
    }
    if let Some(confidence_uuid) = confidence_uuid
        && !read_confidence_ledger(generation)?
            .assessments
            .iter()
            .any(|row| {
                row.confidence_uuid == confidence_uuid && row.assertion_uuid == assertion_uuid
            })
    {
        return Err(not_found_kind("confidence assessment for assertion"));
    }
    if let Some(reasoning_uuid) = reasoning_uuid
        && !read_reasoning_ledger(generation)?
            .records
            .iter()
            .any(|row| row.reasoning_uuid == reasoning_uuid && row.assertion_uuid == assertion_uuid)
    {
        return Err(not_found_kind("reasoning record for assertion"));
    }
    if !crate::provenance::read_ledger(generation)?
        .events
        .iter()
        .any(|row| row.provenance_uuid == provenance_uuid)
    {
        return Err(not_found_kind("provenance event"));
    }
    Ok(())
}

fn assertion_generation_uuid(
    operation_uuid: OperationId,
    participants: &[ProjectParticipant],
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-assertion-generation/1");
    hasher.update(operation_uuid.0.as_bytes());
    for participant in participants {
        hasher.update(participant.capability_id.as_bytes());
        hasher.update([0]);
        hasher.update(participant.record_family_id.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&participant.bytes));
    }
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

impl GraphForge {
    /// Atomically create one assertion, its graph references, and provenance.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn create_assertion(
        &self,
        request: CreateAssertionRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        request.validate_context()?;
        let _graph_visibility = lock_graph_visibility(self)?;
        validate_graph_refs(self, &request.graph_refs)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("knowledge", 1)?;
        parent.require_capability("provenance", 1)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "project generation changed before assertion publication".into(),
            });
        }

        let existing = read_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged = staged_assertion(&request, recorded_at_micros)?;
        if let Some(index) = existing
            .assertions
            .iter()
            .position(|row| row.assertion_uuid == request.assertion_uuid)
        {
            if existing
                .assertion_fingerprint(request.assertion_uuid)
                .map_err(knowledge_error)?
                == staged
                    .assertion_fingerprint(request.assertion_uuid)
                    .map_err(knowledge_error)?
            {
                return Ok(assertion_result(
                    existing
                        .assertion_batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "assertion UUID was reused for different canonical content".into(),
            });
        }

        let knowledge = existing.merge(&staged).map_err(knowledge_error)?;
        let provenance = merged_provenance(&parent, &request, &staged, recorded_at_micros)?;
        let participants = assertion_publication_participants(&parent, &knowledge, &provenance)?;
        let capabilities = parent
            .capabilities()
            .into_iter()
            .map(|entry| ProjectCapability {
                capability_id: entry.capability_id,
                capability_version: entry.capability_version,
            })
            .collect();
        let generation_uuid =
            assertion_generation_uuid(request.context.operation_uuid, &participants);
        let publication = ProjectGenerationRequest {
            transaction_uuid: request.context.operation_uuid.0,
            generation_uuid,
            capabilities,
            participants,
        };
        let receipt = match self.stage_project_generation(&publication)? {
            ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
            ProjectStageOutcome::Staged(staged_generation) => staged_generation
                .validate(
                    |_| Ok(()),
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(GfError::Project {
                                code: ProjectErrorCode::TransactionConflict,
                                message: "project generation changed before assertion publication"
                                    .into(),
                            });
                        }
                        Ok(())
                    },
                )?
                .publish()?,
        };
        *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = receipt.generation_uuid;
        let committed = graphforge_storage::resolve_project_generation(root)?;
        let ledger = read_ledger(&committed)?;
        let index = ledger
            .assertions
            .iter()
            .position(|row| row.assertion_uuid == request.assertion_uuid)
            .ok_or_else(|| GfError::Validation("committed assertion is absent".into()))?;
        Ok(assertion_result(
            ledger
                .assertion_batch()
                .map_err(knowledge_error)?
                .slice(index, 1),
        ))
    }

    /// Atomically create a Bazel-migration0 assertion and its first explicit epistemic status.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-epistemic-api/1 freezes owned request structs"
    )]
    pub fn create_assertion_with_status(
        &self,
        request: CreateAssertionWithStatusRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        request.assertion.validate_context()?;
        require_uuid(request.first_status.status_event_uuid, "status_event_uuid")?;
        if request.first_status.status == AssertionStatus::Superseded {
            return Err(GfError::Validation(
                "superseded status requires the atomic supersession API".into(),
            ));
        }
        let _graph_visibility = lock_graph_visibility(self)?;
        validate_graph_refs(self, &request.assertion.graph_refs)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("knowledge", 1)?;
        parent.require_capability("provenance", 1)?;
        parent.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before assertion-status bundle publication",
            ));
        }
        let assertions = read_ledger(&parent)?;
        let statuses = read_status_ledger(&parent)?;
        let existing_assertion = assertions
            .assertions
            .iter()
            .find(|row| row.assertion_uuid == request.assertion.assertion_uuid);
        let existing_status = statuses
            .events
            .iter()
            .find(|row| row.status_event_uuid == request.first_status.status_event_uuid);
        match (existing_assertion, existing_status) {
            (Some(assertion), Some(status))
                if assertion.claim == request.assertion.claim
                    && assertion_refs_match(&assertions, &request.assertion)
                    && status.assertion_uuid == request.assertion.assertion_uuid
                    && status.status == request.first_status.status
                    && status.confidence_uuid.is_none()
                    && status.reasoning_uuid.is_none()
                    && status.provenance_uuid == assertion.provenance_uuid =>
            {
                let index = statuses
                    .events
                    .iter()
                    .position(|row| row.status_event_uuid == request.first_status.status_event_uuid)
                    .expect("matched status belongs to ledger");
                return Ok(assertion_result(
                    statuses.batch().map_err(knowledge_error)?.slice(index, 1),
                ));
            }
            (None, None) => {}
            _ => {
                return Err(transaction_conflict(
                    "assertion-status bundle identity was reused for different canonical content",
                ));
            }
        }
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged_assertions = staged_assertion(&request.assertion, recorded_at_micros)?;
        let provenance_uuid = staged_assertions.assertions[0].provenance_uuid;
        let staged_status = AssertionStatusLedger::new(vec![
            AssertionStatusEvent::new(
                request.first_status.status_event_uuid,
                request.assertion.assertion_uuid,
                request.first_status.status,
                None,
                None,
                provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        let merged_assertions = assertions
            .merge(&staged_assertions)
            .map_err(knowledge_error)?;
        let merged_status = statuses.merge(&staged_status).map_err(knowledge_error)?;
        let provenance = merged_provenance(
            &parent,
            &request.assertion,
            &staged_assertions,
            recorded_at_micros,
        )?;
        publish_assertion_status_bundle(
            self,
            &request,
            &parent,
            expected_parent,
            &merged_assertions,
            &merged_status,
            &provenance,
        )
    }

    /// Return one exact `assertion@1` row.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes an owned optional cancellation token"
    )]
    pub fn assertion(
        &self,
        assertion_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(assertion_uuid, "assertion_uuid")?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        let index = ledger
            .assertions
            .iter()
            .position(|row| row.assertion_uuid == assertion_uuid)
            .ok_or_else(not_found)?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        Ok(assertion_result(
            ledger
                .assertion_batch()
                .map_err(knowledge_error)?
                .slice(index, 1),
        ))
    }

    /// Return one deterministic assertion page.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn list_assertions(
        &self,
        request: ListAssertionsRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(graph_uuid) = request.graph_uuid {
            require_uuid(graph_uuid, "graph_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        let selected = ledger
            .assertions
            .iter()
            .enumerate()
            .filter(|(_, assertion)| {
                request.graph_uuid.is_none_or(|graph_uuid| {
                    ledger.graph_refs.iter().any(|reference| {
                        reference.assertion_uuid == assertion.assertion_uuid
                            && reference.graph_uuid == graph_uuid
                    })
                })
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.assertion_batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::ASSERTION_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Return one assertion's graph references in canonical order.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned page requests"
    )]
    pub fn assertion_graph_refs(
        &self,
        assertion_uuid: Uuid,
        page: PageRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(assertion_uuid, "assertion_uuid")?;
        let generation = self.generation_for_read()?;
        let ledger = read_ledger(&generation)?;
        if !ledger
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == assertion_uuid)
        {
            return Err(not_found());
        }
        let source = ledger.graph_ref_batch().map_err(knowledge_error)?;
        let selected = ledger
            .graph_refs
            .iter()
            .enumerate()
            .filter(|(_, row)| row.assertion_uuid == assertion_uuid)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) =
            crate::paging::validate_page(&page, generation.generation_uuid(), selected.len())?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::ASSERTION_GRAPH_REF_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Atomically append one explicit assertion-status event.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-epistemic-api/1 freezes owned request structs"
    )]
    pub fn record_assertion_status(
        &self,
        request: RecordAssertionStatusRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        validate_status_request(&request)?;
        if request.status == AssertionStatus::Superseded {
            return Err(GfError::Validation(
                "superseded status requires the atomic supersession API".into(),
            ));
        }
        let _graph_visibility = lock_graph_visibility(self)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before assertion-status publication",
            ));
        }
        validate_status_references(
            &parent,
            request.assertion_uuid,
            request.confidence_uuid,
            request.reasoning_uuid,
            request.provenance_uuid,
        )?;
        let existing = read_status_ledger(&parent)?;
        if let Some(index) = existing
            .events
            .iter()
            .position(|row| row.status_event_uuid == request.status_event_uuid)
        {
            let row = &existing.events[index];
            if row.assertion_uuid == request.assertion_uuid
                && row.status == request.status
                && row.confidence_uuid == request.confidence_uuid
                && row.reasoning_uuid == request.reasoning_uuid
                && row.provenance_uuid == request.provenance_uuid
            {
                return Ok(assertion_result(
                    existing.batch().map_err(knowledge_error)?.slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "status event UUID was reused for different canonical content",
            ));
        }
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged = AssertionStatusLedger::new(vec![
            AssertionStatusEvent::new(
                request.status_event_uuid,
                request.assertion_uuid,
                request.status,
                request.confidence_uuid,
                request.reasoning_uuid,
                request.provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        let merged = existing.merge(&staged).map_err(knowledge_error)?;
        publish_status(self, &request, &parent, expected_parent, &merged)
    }

    /// Return the deterministic current status, or an empty Arrow table when statusless.
    pub fn assertion_status(
        &self,
        assertion_uuid: Uuid,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(assertion_uuid, "assertion_uuid")?;
        let generation = self.generation_for_read()?;
        if !read_ledger(&generation)?
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == assertion_uuid)
        {
            return Err(not_found_kind("assertion"));
        }
        let ledger = read_status_ledger(&generation)?;
        let batch = ledger.batch().map_err(knowledge_error)?;
        let current = ledger.current_for(assertion_uuid).map_or_else(
            || RecordBatch::new_empty(Arc::clone(&ASSERTION_STATUS_SCHEMA)),
            |event| {
                let index = ledger
                    .events
                    .iter()
                    .position(|row| row.status_event_uuid == event.status_event_uuid)
                    .expect("current status belongs to ledger");
                batch.slice(index, 1)
            },
        );
        Ok(assertion_result(current))
    }

    /// Return deterministic append-only assertion-status history.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-epistemic-api/1 freezes owned request structs"
    )]
    pub fn list_assertion_status(
        &self,
        request: ListAssertionStatusRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(assertion_uuid) = request.assertion_uuid {
            require_uuid(assertion_uuid, "assertion_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_status_ledger(&generation)?;
        let selected = ledger
            .events
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .assertion_uuid
                    .is_none_or(|id| row.assertion_uuid == id)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &ASSERTION_STATUS_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Atomically append a supersession relation and its exact terminal status event.
    #[allow(
        clippy::needless_pass_by_value,
        clippy::too_many_lines,
        reason = "graphforge-epistemic-api/1 freezes one explicit atomic validation transaction"
    )]
    pub fn supersede_assertion(
        &self,
        request: SupersedeAssertionRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        for (uuid, name) in [
            (request.supersession_uuid, "supersession_uuid"),
            (request.prior_assertion_uuid, "prior_assertion_uuid"),
            (
                request.replacement_assertion_uuid,
                "replacement_assertion_uuid",
            ),
            (request.status_event_uuid, "status_event_uuid"),
            (request.reasoning_uuid, "reasoning_uuid"),
            (request.provenance_uuid, "provenance_uuid"),
        ] {
            require_uuid(uuid, name)?;
        }
        let _graph_visibility = lock_graph_visibility(self)?;
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        parent.require_capability("knowledge", 1)?;
        parent.require_capability("provenance", 1)?;
        parent.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(transaction_conflict(
                "project generation changed before assertion-supersession publication",
            ));
        }

        let assertions = read_ledger(&parent)?;
        for assertion_uuid in [
            request.prior_assertion_uuid,
            request.replacement_assertion_uuid,
        ] {
            if !assertions
                .assertions
                .iter()
                .any(|row| row.assertion_uuid == assertion_uuid)
            {
                return Err(not_found_kind("assertion"));
            }
        }
        let reasoning = read_reasoning_ledger(&parent)?;
        if !reasoning.records.iter().any(|row| {
            row.reasoning_uuid == request.reasoning_uuid
                && row.assertion_uuid == request.prior_assertion_uuid
        }) {
            return Err(not_found_kind("reasoning for prior assertion"));
        }
        if !crate::provenance::read_ledger(&parent)?
            .events
            .iter()
            .any(|row| row.provenance_uuid == request.provenance_uuid)
        {
            return Err(not_found_kind("provenance"));
        }

        let existing_relations = read_supersession_ledger(&parent)?;
        let existing_statuses = read_status_ledger(&parent)?;
        if let Some(index) = existing_relations
            .relations()
            .iter()
            .position(|row| row.supersession_uuid == request.supersession_uuid)
        {
            let row = &existing_relations.relations()[index];
            if row.prior_assertion_uuid == request.prior_assertion_uuid
                && row.replacement_assertion_uuid == request.replacement_assertion_uuid
                && row.status_event_uuid == request.status_event_uuid
                && row.reasoning_uuid == request.reasoning_uuid
                && row.provenance_uuid == request.provenance_uuid
                && existing_statuses.events.iter().any(|status| {
                    status.status_event_uuid == request.status_event_uuid
                        && status.assertion_uuid == request.prior_assertion_uuid
                        && status.status == AssertionStatus::Superseded
                        && status.reasoning_uuid == Some(request.reasoning_uuid)
                        && status.provenance_uuid == request.provenance_uuid
                })
            {
                return Ok(assertion_result(
                    existing_relations
                        .batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "supersession identity was reused for different canonical content",
            ));
        }
        if existing_statuses
            .events
            .iter()
            .any(|row| row.status_event_uuid == request.status_event_uuid)
        {
            return Err(transaction_conflict(
                "status event UUID was reused outside the supersession relation",
            ));
        }

        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged_relations = AssertionSupersessionLedger::new(vec![
            AssertionSupersession::new(
                request.supersession_uuid,
                request.prior_assertion_uuid,
                request.replacement_assertion_uuid,
                request.status_event_uuid,
                request.reasoning_uuid,
                request.provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        let staged_statuses = AssertionStatusLedger::new(vec![
            AssertionStatusEvent::new(
                request.status_event_uuid,
                request.prior_assertion_uuid,
                AssertionStatus::Superseded,
                None,
                Some(request.reasoning_uuid),
                request.provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        let relations = existing_relations
            .merge(&staged_relations)
            .map_err(knowledge_error)?;
        let statuses = existing_statuses
            .merge(&staged_statuses)
            .map_err(knowledge_error)?;
        publish_supersession(
            self,
            &request,
            &parent,
            expected_parent,
            &relations,
            &statuses,
        )
    }

    /// Return deterministic branch-preserving supersession history.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-epistemic-api/1 freezes owned request structs"
    )]
    pub fn list_assertion_supersessions(
        &self,
        request: ListAssertionSupersessionsRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        for (uuid, name) in [
            (request.prior_assertion_uuid, "prior_assertion_uuid"),
            (
                request.replacement_assertion_uuid,
                "replacement_assertion_uuid",
            ),
        ] {
            if let Some(uuid) = uuid {
                require_uuid(uuid, name)?;
            }
        }
        let generation = self.generation_for_read()?;
        let ledger = read_supersession_ledger(&generation)?;
        let selected = ledger
            .relations()
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .prior_assertion_uuid
                    .is_none_or(|id| row.prior_assertion_uuid == id)
                    && request
                        .replacement_assertion_uuid
                        .is_none_or(|id| row.replacement_assertion_uuid == id)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &ASSERTION_SUPERSESSION_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }
}

#[cfg(test)]
mod tests;
