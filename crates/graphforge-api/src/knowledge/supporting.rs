//! Knowledge supporting operations.

use super::{
    ApiErrorCode, AssertionLedger, AssessConfidenceRequest, AttachEvidenceRequest,
    CancellationToken, ConfidenceLedger, ConfidencePolicyRequest,
    CreateAssertionWithEvidenceRequest, EPISTEMIC_CAPABILITY_VERSION, EventKind, EvidenceLedger,
    EvidenceLink, EvidenceSourceKind, GfError, GraphForge, HashSet,
    ListConfidenceAssessmentsRequest, ListEvidenceLinksRequest, ListReasoningRequest, PageRequest,
    PageToken, ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome, ProvenanceEvent,
    ProvenanceLedger, ReasoningLedger, ReasoningRecord, RecordReasoningRequest,
    ResolvedProjectGeneration, Uuid, assertion_evidence_publication_participants, assertion_result,
    concat_or_empty, confidence_publication_participants, evidence_publication_participants,
    knowledge_error, knowledge_generation_uuid, lock_graph_visibility, match_requested_edge_uuids,
    match_requested_node_uuids, merged_assertion_evidence_provenance, merged_confidence_provenance,
    merged_evidence_provenance, not_found_kind, provenance_error, read_artifact_ledger,
    read_confidence_ledger, read_evidence_ledger, read_ledger, read_reasoning_ledger,
    read_source_ledger, reasoning_publication_participants, require_uuid, staged_assertion,
    transaction_conflict, validate_graph_refs, validate_write_context, with_next_token,
};

fn publish_reasoning(
    graph: &GraphForge,
    request: &RecordReasoningRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    reasoning: &ReasoningLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let participants = reasoning_publication_participants(parent, reasoning)?;
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
            b"reasoning",
            request.context.operation_uuid,
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
                            "project generation changed before reasoning publication",
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
    graph.reasoning(request.reasoning_uuid, None)
}

fn publish_confidence(
    graph: &GraphForge,
    request: &AssessConfidenceRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    knowledge: &ConfidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = confidence_publication_participants(parent, knowledge, provenance)?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let generation_uuid =
        knowledge_generation_uuid(b"confidence", request.context.operation_uuid, &participants);
    let publication = ProjectGenerationRequest {
        transaction_uuid: request.context.operation_uuid.0,
        generation_uuid,
        capabilities,
        participants,
    };
    let receipt = match graph.stage_project_generation(&publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged_generation) => staged_generation
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before confidence publication",
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
    let ledger = read_confidence_ledger(&graphforge_storage::resolve_project_generation(root)?)?;
    let index = ledger
        .assessments
        .iter()
        .position(|row| row.confidence_uuid == request.confidence_uuid)
        .ok_or_else(|| GfError::Validation("committed confidence is absent".into()))?;
    Ok(assertion_result(
        ledger
            .assessment_batch()
            .map_err(knowledge_error)?
            .slice(index, 1),
    ))
}

fn publish_evidence(
    graph: &GraphForge,
    request: &AttachEvidenceRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    knowledge: &EvidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = evidence_publication_participants(parent, knowledge, provenance)?;
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
            b"evidence",
            request.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graph.stage_project_generation(&publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged_generation) => staged_generation
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before evidence publication",
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
    let ledger = read_evidence_ledger(&graphforge_storage::resolve_project_generation(root)?)?;
    let index = ledger
        .links
        .iter()
        .position(|row| row.evidence_uuid == request.evidence_uuid)
        .ok_or_else(|| GfError::Validation("committed evidence is absent".into()))?;
    Ok(assertion_result(
        ledger.batch().map_err(knowledge_error)?.slice(index, 1),
    ))
}

fn publish_assertion_evidence(
    graph: &GraphForge,
    request: &CreateAssertionWithEvidenceRequest,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    assertions: &AssertionLedger,
    evidence: &EvidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants =
        assertion_evidence_publication_participants(parent, assertions, evidence, provenance)?;
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
            b"assertion-evidence",
            request.assertion.context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graph.stage_project_generation(&publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged_generation) => staged_generation
            .validate(
                |_| Ok(()),
                |actual_parent, _| {
                    if actual_parent.generation_uuid() != expected_parent {
                        return Err(transaction_conflict(
                            "project generation changed before assertion evidence publication",
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
    let ledger = read_ledger(&graphforge_storage::resolve_project_generation(root)?)?;
    let index = ledger
        .assertions
        .iter()
        .position(|row| row.assertion_uuid == request.assertion.assertion_uuid)
        .ok_or_else(|| GfError::Validation("committed assertion is absent".into()))?;
    Ok(assertion_result(
        ledger
            .assertion_batch()
            .map_err(knowledge_error)?
            .slice(index, 1),
    ))
}

fn staged_evidence_bundle(
    request: &CreateAssertionWithEvidenceRequest,
    provenance_uuid: Uuid,
    recorded_at_micros: i64,
) -> Result<EvidenceLedger, GfError> {
    let links = request
        .evidence
        .iter()
        .map(|input| {
            EvidenceLink::new(
                input.evidence_uuid,
                request.assertion.assertion_uuid,
                input.source_uuid,
                input.source_kind,
                input.role,
                input.weight,
                provenance_uuid,
                recorded_at_micros,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(knowledge_error)?;
    EvidenceLedger::new(links).map_err(knowledge_error)
}

fn validate_evidence_source(
    graph: &GraphForge,
    source_uuid: Uuid,
    source_kind: EvidenceSourceKind,
) -> Result<(), GfError> {
    let mut pending = HashSet::from([source_uuid]);
    match source_kind {
        EvidenceSourceKind::Document | EvidenceSourceKind::Observation => return Ok(()),
        EvidenceSourceKind::Source => {
            let generation = graphforge_storage::resolve_project_generation(
                graph.resolved_generation.container_root(),
            )?;
            if read_source_ledger(&generation)?
                .sources
                .iter()
                .any(|row| row.source_uuid == source_uuid)
            {
                return Ok(());
            }
            return Err(not_found_kind("source"));
        }
        EvidenceSourceKind::Artifact => {
            let generation = graphforge_storage::resolve_project_generation(
                graph.resolved_generation.container_root(),
            )?;
            if read_artifact_ledger(&generation)?
                .artifacts
                .iter()
                .any(|row| row.artifact_uuid == source_uuid)
            {
                return Ok(());
            }
            return Err(not_found_kind("artifact"));
        }
        EvidenceSourceKind::GraphNode => match_requested_node_uuids(graph, &mut pending)?,
        EvidenceSourceKind::GraphEdge => match_requested_edge_uuids(graph, &mut pending)?,
    }
    if pending.is_empty() {
        Ok(())
    } else {
        Err(GfError::Api {
            code: ApiErrorCode::NotFound,
            message: "evidence graph source UUID was not found".into(),
        })
    }
}

impl GraphForge {
    /// Atomically record one confidence assessment, its input snapshot, and provenance.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn assess_confidence(
        &self,
        request: AssessConfidenceRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        require_uuid(request.confidence_uuid, "confidence_uuid")?;
        require_uuid(request.assertion_uuid, "assertion_uuid")?;
        let _graph_visibility = lock_graph_visibility(self)?;
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
            return Err(transaction_conflict(
                "project generation changed before confidence publication",
            ));
        }
        let assertions = read_ledger(&parent)?;
        if !assertions
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == request.assertion_uuid)
        {
            return Err(not_found_kind("assertion"));
        }
        let existing = read_confidence_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let event = ProvenanceEvent::new(
            request.context.operation_uuid.0,
            EventKind::AssessConfidence,
            request.context.actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        let staged = match &request.policy {
            ConfidencePolicyRequest::Explicit { value } => ConfidenceLedger::explicit(
                request.confidence_uuid,
                request.assertion_uuid,
                *value,
                event.provenance_uuid,
                recorded_at_micros,
            ),
            ConfidencePolicyRequest::ConservativeMin {
                input_confidence_uuids,
            } => existing.conservative_min(
                request.confidence_uuid,
                request.assertion_uuid,
                input_confidence_uuids.clone(),
                event.provenance_uuid,
                recorded_at_micros,
            ),
        }
        .map_err(knowledge_error)?;
        if let Some(index) = existing
            .assessments
            .iter()
            .position(|row| row.confidence_uuid == request.confidence_uuid)
        {
            if existing
                .assessment_fingerprint(request.confidence_uuid)
                .map_err(knowledge_error)?
                == staged
                    .assessment_fingerprint(request.confidence_uuid)
                    .map_err(knowledge_error)?
            {
                return Ok(assertion_result(
                    existing
                        .assessment_batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "confidence UUID was reused for different canonical content",
            ));
        }

        let knowledge = existing.merge(&staged).map_err(knowledge_error)?;
        let provenance = merged_confidence_provenance(&parent, &request, &staged, &event)?;
        publish_confidence(
            self,
            &request,
            &parent,
            expected_parent,
            &knowledge,
            &provenance,
        )
    }

    /// Return one exact `confidence_assessment@1` row.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes an owned optional cancellation token"
    )]
    pub fn confidence_assessment(
        &self,
        confidence_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(confidence_uuid, "confidence_uuid")?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_confidence_ledger(&generation)?;
        let index = ledger
            .assessments
            .iter()
            .position(|row| row.confidence_uuid == confidence_uuid)
            .ok_or_else(|| not_found_kind("confidence assessment"))?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        Ok(assertion_result(
            ledger
                .assessment_batch()
                .map_err(knowledge_error)?
                .slice(index, 1),
        ))
    }

    /// Return a deterministic page of confidence assessments.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn list_confidence_assessments(
        &self,
        request: ListConfidenceAssessmentsRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(assertion_uuid) = request.assertion_uuid {
            require_uuid(assertion_uuid, "assertion_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_confidence_ledger(&generation)?;
        let selected = ledger
            .assessments
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .assertion_uuid
                    .is_none_or(|assertion_uuid| row.assertion_uuid == assertion_uuid)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) = crate::paging::validate_page(
            &request.page,
            generation.generation_uuid(),
            selected.len(),
        )?;
        let source = ledger.assessment_batch().map_err(knowledge_error)?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::CONFIDENCE_ASSESSMENT_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Return one assessment's immutable normalized input snapshot.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned page requests"
    )]
    pub fn confidence_inputs(
        &self,
        confidence_uuid: Uuid,
        page: PageRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(confidence_uuid, "confidence_uuid")?;
        let generation = self.generation_for_read()?;
        let ledger = read_confidence_ledger(&generation)?;
        if !ledger
            .assessments
            .iter()
            .any(|row| row.confidence_uuid == confidence_uuid)
        {
            return Err(not_found_kind("confidence assessment"));
        }
        let source = ledger.input_batch().map_err(knowledge_error)?;
        let selected = ledger
            .inputs
            .iter()
            .enumerate()
            .filter(|(_, row)| row.confidence_uuid == confidence_uuid)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let (start, end) =
            crate::paging::validate_page(&page, generation.generation_uuid(), selected.len())?;
        let rows = selected[start..end]
            .iter()
            .map(|index| source.slice(*index, 1))
            .collect::<Vec<_>>();
        let batch = concat_or_empty(&rows, &graphforge_knowledge::CONFIDENCE_INPUT_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Atomically create one assertion together with a non-empty evidence bundle.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn create_assertion_with_evidence(
        &self,
        request: CreateAssertionWithEvidenceRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        request.assertion.validate_context()?;
        if request.evidence.is_empty() {
            return Err(GfError::Validation(
                "assertion evidence bundle must not be empty".into(),
            ));
        }
        let _graph_visibility = lock_graph_visibility(self)?;
        validate_graph_refs(self, &request.assertion.graph_refs)?;
        for input in &request.evidence {
            require_uuid(input.evidence_uuid, "evidence_uuid")?;
            require_uuid(input.source_uuid, "source_uuid")?;
            validate_evidence_source(self, input.source_uuid, input.source_kind)?;
        }
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
            return Err(transaction_conflict(
                "project generation changed before assertion evidence publication",
            ));
        }
        let assertions = read_ledger(&parent)?;
        let evidence = read_evidence_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let staged_assertions = staged_assertion(&request.assertion, recorded_at_micros)?;
        let event = ProvenanceEvent::new(
            request.assertion.context.operation_uuid.0,
            EventKind::CreateAssertion,
            request.assertion.context.actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        let staged_evidence =
            staged_evidence_bundle(&request, event.provenance_uuid, recorded_at_micros)?;
        if let Some(index) = assertions
            .assertions
            .iter()
            .position(|row| row.assertion_uuid == request.assertion.assertion_uuid)
        {
            let assertion_same = assertions
                .assertion_fingerprint(request.assertion.assertion_uuid)
                .map_err(knowledge_error)?
                == staged_assertions
                    .assertion_fingerprint(request.assertion.assertion_uuid)
                    .map_err(knowledge_error)?;
            let evidence_same = staged_evidence.links.iter().all(|row| {
                evidence
                    .evidence_fingerprint(row.evidence_uuid)
                    .and_then(|existing| {
                        staged_evidence
                            .evidence_fingerprint(row.evidence_uuid)
                            .map(|staged| existing == staged)
                    })
                    .unwrap_or(false)
            });
            if assertion_same && evidence_same {
                return Ok(assertion_result(
                    assertions
                        .assertion_batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "assertion evidence bundle identity was reused for different canonical content",
            ));
        }
        let merged_assertions = assertions
            .merge(&staged_assertions)
            .map_err(knowledge_error)?;
        let merged_evidence = evidence.merge(&staged_evidence).map_err(knowledge_error)?;
        let provenance =
            merged_assertion_evidence_provenance(&parent, &request, &staged_assertions, &event)?;
        publish_assertion_evidence(
            self,
            &request,
            &parent,
            expected_parent,
            &merged_assertions,
            &merged_evidence,
            &provenance,
        )
    }

    /// Atomically attach one immutable evidence link and its provenance.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn attach_evidence(
        &self,
        request: AttachEvidenceRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        require_uuid(request.evidence_uuid, "evidence_uuid")?;
        require_uuid(request.assertion_uuid, "assertion_uuid")?;
        require_uuid(request.source_uuid, "source_uuid")?;
        let _graph_visibility = lock_graph_visibility(self)?;
        validate_evidence_source(self, request.source_uuid, request.source_kind)?;
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
            return Err(transaction_conflict(
                "project generation changed before evidence publication",
            ));
        }
        if !read_ledger(&parent)?
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == request.assertion_uuid)
        {
            return Err(not_found_kind("assertion"));
        }
        let existing = read_evidence_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let event = ProvenanceEvent::new(
            request.context.operation_uuid.0,
            EventKind::RecordEvidence,
            request.context.actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        let staged = EvidenceLedger::new(vec![
            EvidenceLink::new(
                request.evidence_uuid,
                request.assertion_uuid,
                request.source_uuid,
                request.source_kind,
                request.role,
                request.weight,
                event.provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        if let Some(index) = existing
            .links
            .iter()
            .position(|row| row.evidence_uuid == request.evidence_uuid)
        {
            if existing
                .evidence_fingerprint(request.evidence_uuid)
                .map_err(knowledge_error)?
                == staged
                    .evidence_fingerprint(request.evidence_uuid)
                    .map_err(knowledge_error)?
            {
                return Ok(assertion_result(
                    existing.batch().map_err(knowledge_error)?.slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "evidence UUID was reused for different canonical content",
            ));
        }
        let knowledge = existing.merge(&staged).map_err(knowledge_error)?;
        let provenance = merged_evidence_provenance(&parent, &request, &event)?;
        publish_evidence(
            self,
            &request,
            &parent,
            expected_parent,
            &knowledge,
            &provenance,
        )
    }

    /// Return one exact `evidence_link@1` row.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes an owned optional cancellation token"
    )]
    pub fn evidence_link(
        &self,
        evidence_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(evidence_uuid, "evidence_uuid")?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_evidence_ledger(&generation)?;
        let index = ledger
            .links
            .iter()
            .position(|row| row.evidence_uuid == evidence_uuid)
            .ok_or_else(|| not_found_kind("evidence link"))?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        Ok(assertion_result(
            ledger.batch().map_err(knowledge_error)?.slice(index, 1),
        ))
    }

    /// Return a deterministic page of immutable evidence links.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn list_evidence_links(
        &self,
        request: ListEvidenceLinksRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(assertion_uuid) = request.assertion_uuid {
            require_uuid(assertion_uuid, "assertion_uuid")?;
        }
        if let Some(source_uuid) = request.source_uuid {
            require_uuid(source_uuid, "source_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_evidence_ledger(&generation)?;
        let selected = ledger
            .links
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .assertion_uuid
                    .is_none_or(|id| row.assertion_uuid == id)
                    && request.source_uuid.is_none_or(|id| row.source_uuid == id)
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
        let batch = concat_or_empty(&rows, &graphforge_knowledge::EVIDENCE_LINK_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }

    /// Atomically append one immutable reasoning record.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-epistemic-api/1 freezes owned request structs"
    )]
    pub fn record_reasoning(
        &self,
        request: RecordReasoningRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        require_uuid(request.reasoning_uuid, "reasoning_uuid")?;
        require_uuid(request.assertion_uuid, "assertion_uuid")?;
        require_uuid(request.provenance_uuid, "provenance_uuid")?;
        if let Some(previous) = request.supersedes_reasoning_uuid {
            require_uuid(previous, "supersedes_reasoning_uuid")?;
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
                "project generation changed before reasoning publication",
            ));
        }
        if !read_ledger(&parent)?
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == request.assertion_uuid)
        {
            return Err(not_found_kind("assertion"));
        }
        if !crate::provenance::read_ledger(&parent)?
            .events
            .iter()
            .any(|row| row.provenance_uuid == request.provenance_uuid)
        {
            return Err(not_found_kind("provenance event"));
        }
        let existing = read_reasoning_ledger(&parent)?;
        if let Some(index) = existing
            .records
            .iter()
            .position(|row| row.reasoning_uuid == request.reasoning_uuid)
        {
            let row = &existing.records[index];
            if row.assertion_uuid == request.assertion_uuid
                && row.kind == request.kind
                && row.content_format == request.content_format
                && row.content == request.content
                && row.supersedes_reasoning_uuid == request.supersedes_reasoning_uuid
                && row.provenance_uuid == request.provenance_uuid
            {
                return Ok(assertion_result(
                    existing.batch().map_err(knowledge_error)?.slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "reasoning UUID was reused for different canonical content",
            ));
        }
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let record = ReasoningRecord::new(
            request.reasoning_uuid,
            request.assertion_uuid,
            request.kind,
            request.content_format,
            request.content.clone(),
            request.supersedes_reasoning_uuid,
            request.provenance_uuid,
            recorded_at_micros,
        )
        .map_err(knowledge_error)?;
        let mut records = existing.records;
        records.push(record);
        let merged = ReasoningLedger::new(records).map_err(knowledge_error)?;
        publish_reasoning(self, &request, &parent, expected_parent, &merged)
    }

    /// Return one exact immutable reasoning record.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-epistemic-api/1 freezes an owned optional cancellation token"
    )]
    pub fn reasoning(
        &self,
        reasoning_uuid: Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(reasoning_uuid, "reasoning_uuid")?;
        if let Some(token) = &cancellation {
            token.checkpoint()?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_reasoning_ledger(&generation)?;
        let index = ledger
            .records
            .iter()
            .position(|row| row.reasoning_uuid == reasoning_uuid)
            .ok_or_else(|| not_found_kind("reasoning record"))?;
        Ok(assertion_result(
            ledger.batch().map_err(knowledge_error)?.slice(index, 1),
        ))
    }

    /// Return deterministic immutable reasoning history.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-epistemic-api/1 freezes owned request structs"
    )]
    pub fn list_reasoning(
        &self,
        request: ListReasoningRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        if let Some(assertion_uuid) = request.assertion_uuid {
            require_uuid(assertion_uuid, "assertion_uuid")?;
        }
        let generation = self.generation_for_read()?;
        let ledger = read_reasoning_ledger(&generation)?;
        let selected = ledger
            .records
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
        let batch = concat_or_empty(&rows, &graphforge_knowledge::REASONING_SCHEMA)?;
        let next =
            (end < selected.len()).then(|| PageToken::new(generation.generation_uuid(), end));
        Ok(assertion_result(with_next_token(&batch, next.as_ref())?))
    }
}

#[cfg(test)]
mod tests;
