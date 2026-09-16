//! Knowledge ledger operations.

use super::{
    ApiErrorCode, Arc, ArrowWriter, AssertionLedger, AssertionStatusLedger,
    AssertionSupersessionLedger, AssessConfidenceRequest, AttachEvidenceRequest, ConfidenceLedger,
    CreateAssertionRequest, CreateAssertionWithEvidenceRequest, Digest,
    EPISTEMIC_CAPABILITY_VERSION, EventKind, EvidenceLedger, EvidenceSourceKind, GfError,
    GraphObjectKind, LineageRecord, LineageRole, OperationId, PageToken,
    ParquetRecordBatchReaderBuilder, ProjectParticipant, ProjectParticipantEncoding,
    ProvenanceEvent, ProvenanceLedger, ReasoningLedger, RecordBatch, ResolvedProjectGeneration,
    Schema, SchemaRef, Sha256, SubjectKind, Uuid, fs, knowledge_error, provenance_error,
    schema_registry,
};

pub(crate) fn empty_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = encode_ledger(&AssertionLedger::default())?;
    participants.extend(encode_confidence_ledger(&ConfidenceLedger::default())?);
    participants.extend(encode_evidence_ledger(&EvidenceLedger::default())?);
    participants.extend(crate::algorithm_runs::empty_participants()?);
    Ok(participants)
}

pub(crate) fn empty_epistemic_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = encode_reasoning_ledger(&ReasoningLedger::default())?;
    participants.extend(encode_status_ledger(&AssertionStatusLedger::default())?);
    participants.extend(encode_supersession_ledger(
        &AssertionSupersessionLedger::default(),
    )?);
    participants.extend(crate::hypotheses::empty_participants()?);
    participants.extend(crate::belief_projection::empty_participants()?);
    Ok(participants)
}

pub(crate) fn read_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<AssertionLedger, GfError> {
    generation.require_capability("knowledge", 1)?;
    let assertions = generation.participant_snapshot("knowledge", "assertions")?;
    let refs = generation.participant_snapshot("knowledge", "assertion_graph_refs")?;
    match (assertions, refs) {
        (None, None) => AssertionLedger::new(Vec::new(), Vec::new()).map_err(knowledge_error),
        (Some(assertions), Some(refs)) => {
            require_participant_contract(&assertions, "assertions")?;
            require_participant_contract(&refs, "assertion_graph_refs")?;
            let assertion_batches = read_or_empty(&assertions, true)?;
            let ref_batches = read_or_empty(&refs, false)?;
            AssertionLedger::from_batches(&assertion_batches, &ref_batches).map_err(knowledge_error)
        }
        _ => Err(GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: "knowledge assertion participant set is incomplete".into(),
        }),
    }
}

pub(crate) fn read_confidence_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<ConfidenceLedger, GfError> {
    generation.require_capability("knowledge", 1)?;
    let assessments = generation.participant_snapshot("knowledge", "confidence_assessments")?;
    let inputs = generation.participant_snapshot("knowledge", "confidence_inputs")?;
    match (assessments, inputs) {
        (None, None) => ConfidenceLedger::new(Vec::new(), Vec::new()).map_err(knowledge_error),
        (Some(assessments), Some(inputs)) => {
            require_participant_contract(&assessments, "confidence_assessments")?;
            require_participant_contract(&inputs, "confidence_inputs")?;
            ConfidenceLedger::from_batches(
                &read_confidence_or_empty(&assessments, true)?,
                &read_confidence_or_empty(&inputs, false)?,
            )
            .map_err(knowledge_error)
        }
        _ => Err(GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: "knowledge confidence participant set is incomplete".into(),
        }),
    }
}

pub(crate) fn read_evidence_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<EvidenceLedger, GfError> {
    generation.require_capability("knowledge", 1)?;
    match generation.participant_snapshot("knowledge", "evidence")? {
        None => EvidenceLedger::new(Vec::new()).map_err(knowledge_error),
        Some(snapshot) => {
            require_participant_contract(&snapshot, "evidence")?;
            EvidenceLedger::from_batches(&read_evidence_or_empty(&snapshot)?)
                .map_err(knowledge_error)
        }
    }
}

pub(crate) fn read_reasoning_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<ReasoningLedger, GfError> {
    generation.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
    match generation.participant_snapshot("epistemic", "reasoning")? {
        None => ReasoningLedger::new(Vec::new()).map_err(knowledge_error),
        Some(snapshot) => {
            require_participant_contract(&snapshot, "reasoning")?;
            let batches = if snapshot.row_count == 0 {
                vec![
                    ReasoningLedger::default()
                        .batch()
                        .map_err(knowledge_error)?,
                ]
            } else {
                read_parquet(&snapshot.bytes)?
            };
            ReasoningLedger::from_batches(&batches).map_err(knowledge_error)
        }
    }
}

pub(crate) fn read_status_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<AssertionStatusLedger, GfError> {
    generation.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
    match generation.participant_snapshot("epistemic", "assertion_status_events")? {
        None => AssertionStatusLedger::new(Vec::new()).map_err(knowledge_error),
        Some(snapshot) => {
            require_participant_contract(&snapshot, "assertion_status_events")?;
            let batches = if snapshot.row_count == 0 {
                vec![
                    AssertionStatusLedger::default()
                        .batch()
                        .map_err(knowledge_error)?,
                ]
            } else {
                read_parquet(&snapshot.bytes)?
            };
            AssertionStatusLedger::from_batches(&batches).map_err(knowledge_error)
        }
    }
}

pub(crate) fn read_supersession_ledger(
    generation: &ResolvedProjectGeneration,
) -> Result<AssertionSupersessionLedger, GfError> {
    generation.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
    match generation.participant_snapshot("epistemic", "assertion_supersessions")? {
        None => AssertionSupersessionLedger::new(Vec::new()).map_err(knowledge_error),
        Some(snapshot) => {
            require_participant_contract(&snapshot, "assertion_supersessions")?;
            let batches = if snapshot.row_count == 0 {
                vec![
                    AssertionSupersessionLedger::default()
                        .batch()
                        .map_err(knowledge_error)?,
                ]
            } else {
                read_parquet(&snapshot.bytes)?
            };
            AssertionSupersessionLedger::from_batches(&batches).map_err(knowledge_error)
        }
    }
}

pub(super) fn merged_provenance(
    parent: &ResolvedProjectGeneration,
    request: &CreateAssertionRequest,
    staged: &AssertionLedger,
    recorded_at_micros: i64,
) -> Result<ProvenanceLedger, GfError> {
    let existing = crate::provenance::read_ledger(parent)?;
    let event = ProvenanceEvent::new(
        request.context.operation_uuid.0,
        EventKind::CreateAssertion,
        request.context.actor_uuid,
        recorded_at_micros,
    )
    .map_err(provenance_error)?;
    let mut lineage = Vec::with_capacity(request.graph_refs.len() + 1);
    for (ordinal, reference) in staged.graph_refs.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                reference.graph_uuid,
                match reference.graph_kind {
                    GraphObjectKind::Node => SubjectKind::Node,
                    GraphObjectKind::Edge => SubjectKind::Edge,
                },
                LineageRole::Input,
                u32::try_from(ordinal)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    lineage.push(
        LineageRecord::new(
            event.provenance_uuid,
            request.assertion_uuid,
            SubjectKind::Assertion,
            LineageRole::Output,
            0,
        )
        .map_err(provenance_error)?,
    );
    existing
        .merge(&ProvenanceLedger::new(vec![event], lineage).map_err(provenance_error)?)
        .map_err(provenance_error)
}

pub(super) fn merged_confidence_provenance(
    parent: &ResolvedProjectGeneration,
    request: &AssessConfidenceRequest,
    staged: &ConfidenceLedger,
    event: &ProvenanceEvent,
) -> Result<ProvenanceLedger, GfError> {
    let existing = crate::provenance::read_ledger(parent)?;
    let mut lineage = Vec::with_capacity(staged.inputs.len() + 2);
    lineage.push(
        LineageRecord::new(
            event.provenance_uuid,
            request.assertion_uuid,
            SubjectKind::Assertion,
            LineageRole::Input,
            0,
        )
        .map_err(provenance_error)?,
    );
    for (ordinal, input) in staged.inputs.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                input.input_confidence_uuid,
                SubjectKind::ConfidenceAssessment,
                LineageRole::Input,
                u32::try_from(ordinal + 1)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    lineage.push(
        LineageRecord::new(
            event.provenance_uuid,
            request.confidence_uuid,
            SubjectKind::ConfidenceAssessment,
            LineageRole::Output,
            0,
        )
        .map_err(provenance_error)?,
    );
    existing
        .merge(&ProvenanceLedger::new(vec![event.clone()], lineage).map_err(provenance_error)?)
        .map_err(provenance_error)
}

pub(super) fn merged_evidence_provenance(
    parent: &ResolvedProjectGeneration,
    request: &AttachEvidenceRequest,
    event: &ProvenanceEvent,
) -> Result<ProvenanceLedger, GfError> {
    let existing = crate::provenance::read_ledger(parent)?;
    let source_kind = match request.source_kind {
        EvidenceSourceKind::GraphNode => SubjectKind::Node,
        EvidenceSourceKind::GraphEdge => SubjectKind::Edge,
        EvidenceSourceKind::Document | EvidenceSourceKind::Observation => SubjectKind::EvidenceLink,
    };
    let lineage = vec![
        LineageRecord::new(
            event.provenance_uuid,
            request.assertion_uuid,
            SubjectKind::Assertion,
            LineageRole::Input,
            0,
        )
        .map_err(provenance_error)?,
        LineageRecord::new(
            event.provenance_uuid,
            request.source_uuid,
            source_kind,
            LineageRole::Input,
            1,
        )
        .map_err(provenance_error)?,
        LineageRecord::new(
            event.provenance_uuid,
            request.evidence_uuid,
            SubjectKind::EvidenceLink,
            LineageRole::Output,
            0,
        )
        .map_err(provenance_error)?,
    ];
    existing
        .merge(&ProvenanceLedger::new(vec![event.clone()], lineage).map_err(provenance_error)?)
        .map_err(provenance_error)
}

pub(super) fn merged_assertion_evidence_provenance(
    parent: &ResolvedProjectGeneration,
    request: &CreateAssertionWithEvidenceRequest,
    staged: &AssertionLedger,
    event: &ProvenanceEvent,
) -> Result<ProvenanceLedger, GfError> {
    let existing = crate::provenance::read_ledger(parent)?;
    let mut lineage = Vec::new();
    for (ordinal, reference) in staged.graph_refs.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                reference.graph_uuid,
                match reference.graph_kind {
                    GraphObjectKind::Node => SubjectKind::Node,
                    GraphObjectKind::Edge => SubjectKind::Edge,
                },
                LineageRole::Input,
                u32::try_from(ordinal)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    let evidence_offset = staged.graph_refs.len();
    for (ordinal, input) in request.evidence.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                input.source_uuid,
                match input.source_kind {
                    EvidenceSourceKind::GraphNode => SubjectKind::Node,
                    EvidenceSourceKind::GraphEdge => SubjectKind::Edge,
                    EvidenceSourceKind::Document | EvidenceSourceKind::Observation => {
                        SubjectKind::EvidenceLink
                    }
                },
                LineageRole::Input,
                u32::try_from(evidence_offset + ordinal)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    lineage.push(
        LineageRecord::new(
            event.provenance_uuid,
            request.assertion.assertion_uuid,
            SubjectKind::Assertion,
            LineageRole::Output,
            0,
        )
        .map_err(provenance_error)?,
    );
    for (ordinal, input) in request.evidence.iter().enumerate() {
        lineage.push(
            LineageRecord::new(
                event.provenance_uuid,
                input.evidence_uuid,
                SubjectKind::EvidenceLink,
                LineageRole::Output,
                u32::try_from(ordinal + 1)
                    .map_err(|_| GfError::Execution("lineage ordinal exceeds u32".into()))?,
            )
            .map_err(provenance_error)?,
        );
    }
    existing
        .merge(&ProvenanceLedger::new(vec![event.clone()], lineage).map_err(provenance_error)?)
        .map_err(provenance_error)
}

pub(super) fn assertion_publication_participants(
    parent: &ResolvedProjectGeneration,
    knowledge: &AssertionLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "assertions" | "assertion_graph_refs"
                )
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_ledger(knowledge)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(super) fn confidence_publication_participants(
    parent: &ResolvedProjectGeneration,
    knowledge: &ConfidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "confidence_assessments" | "confidence_inputs"
                )
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_confidence_ledger(knowledge)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(super) fn evidence_publication_participants(
    parent: &ResolvedProjectGeneration,
    knowledge: &EvidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge" && snapshot.record_family_id == "evidence"
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_evidence_ledger(knowledge)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(super) fn reasoning_publication_participants(
    parent: &ResolvedProjectGeneration,
    reasoning: &ReasoningLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "epistemic" && snapshot.record_family_id == "reasoning")
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_reasoning_ledger(reasoning)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(super) fn status_publication_participants(
    parent: &ResolvedProjectGeneration,
    status: &AssertionStatusLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "epistemic"
                && snapshot.record_family_id == "assertion_status_events")
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_status_ledger(status)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(super) fn supersession_publication_participants(
    parent: &ResolvedProjectGeneration,
    relations: &AssertionSupersessionLedger,
    status: &AssertionStatusLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "epistemic"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "assertion_supersessions" | "assertion_status_events"
                ))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_supersession_ledger(relations)?);
    participants.extend(encode_status_ledger(status)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(super) fn assertion_status_bundle_participants(
    parent: &ResolvedProjectGeneration,
    assertions: &AssertionLedger,
    status: &AssertionStatusLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "assertions" | "assertion_graph_refs"
                )
                || snapshot.capability_id == "epistemic"
                    && snapshot.record_family_id == "assertion_status_events"
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_ledger(assertions)?);
    participants.extend(encode_status_ledger(status)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(super) fn assertion_evidence_publication_participants(
    parent: &ResolvedProjectGeneration,
    assertions: &AssertionLedger,
    evidence: &EvidenceLedger,
    provenance: &ProvenanceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "knowledge"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "assertions" | "assertion_graph_refs" | "evidence"
                )
                || snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.extend(encode_ledger(assertions)?);
    participants.extend(encode_evidence_ledger(evidence)?);
    participants.extend(crate::provenance::encode_ledger(provenance)?);
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

pub(crate) fn encode_ledger(ledger: &AssertionLedger) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let assertions = registry
        .iter()
        .find(|entry| entry.record_family == "assertions")
        .expect("assertion registry");
    let refs = registry
        .iter()
        .find(|entry| entry.record_family == "assertion_graph_refs")
        .expect("assertion graph-ref registry");
    Ok(vec![
        participant(
            assertions,
            &ledger.assertion_batch().map_err(knowledge_error)?,
        )?,
        participant(refs, &ledger.graph_ref_batch().map_err(knowledge_error)?)?,
    ])
}

pub(crate) fn encode_confidence_ledger(
    ledger: &ConfidenceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let assessments = registry
        .iter()
        .find(|entry| entry.record_family == "confidence_assessments")
        .expect("confidence assessment registry");
    let inputs = registry
        .iter()
        .find(|entry| entry.record_family == "confidence_inputs")
        .expect("confidence input registry");
    Ok(vec![
        participant(
            assessments,
            &ledger.assessment_batch().map_err(knowledge_error)?,
        )?,
        participant(inputs, &ledger.input_batch().map_err(knowledge_error)?)?,
    ])
}

pub(crate) fn encode_evidence_ledger(
    ledger: &EvidenceLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let evidence = registry
        .iter()
        .find(|entry| entry.record_family == "evidence")
        .expect("evidence registry");
    Ok(vec![participant(
        evidence,
        &ledger.batch().map_err(knowledge_error)?,
    )?])
}

pub(crate) fn encode_reasoning_ledger(
    ledger: &ReasoningLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let reasoning = registry
        .iter()
        .find(|entry| entry.record_family == "reasoning")
        .expect("reasoning registry");
    Ok(vec![participant(
        reasoning,
        &ledger.batch().map_err(knowledge_error)?,
    )?])
}

pub(crate) fn encode_status_ledger(
    ledger: &AssertionStatusLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let status = registry
        .iter()
        .find(|entry| entry.record_family == "assertion_status_events")
        .expect("assertion-status registry");
    Ok(vec![participant(
        status,
        &ledger.batch().map_err(knowledge_error)?,
    )?])
}

pub(crate) fn encode_supersession_ledger(
    ledger: &AssertionSupersessionLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let registry = schema_registry();
    let relation = registry
        .iter()
        .find(|entry| entry.record_family == "assertion_supersessions")
        .expect("assertion-supersession registry");
    Ok(vec![participant(
        relation,
        &ledger.batch().map_err(knowledge_error)?,
    )?])
}

pub(crate) fn participant(
    registry: &graphforge_knowledge::SchemaRegistryEntry,
    batch: &RecordBatch,
) -> Result<ProjectParticipant, GfError> {
    Ok(ProjectParticipant {
        capability_id: registry.capability_id.into(),
        capability_version: registry.capability_version,
        record_family_id: registry.record_family.into(),
        record_version: registry.record_version,
        encoding: ProjectParticipantEncoding::Parquet,
        schema_fingerprint: registry.schema_fingerprint,
        row_count: u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
        bytes: write_parquet(batch, &registry.schema)?,
    })
}

pub(crate) fn knowledge_generation_uuid(
    operation: &[u8],
    operation_uuid: OperationId,
    participants: &[ProjectParticipant],
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-knowledge-generation/1");
    hasher.update(operation);
    hasher.update([0]);
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

pub(crate) fn snapshot_to_participant(
    snapshot: graphforge_storage::ProjectParticipantSnapshot,
) -> Result<ProjectParticipant, GfError> {
    Ok(ProjectParticipant {
        capability_id: snapshot.capability_id,
        capability_version: snapshot.capability_version,
        record_family_id: snapshot.record_family_id,
        record_version: snapshot.record_version,
        encoding: match snapshot.encoding.as_str() {
            "parquet" => ProjectParticipantEncoding::Parquet,
            "arrow" => ProjectParticipantEncoding::Arrow,
            "json" => ProjectParticipantEncoding::Json,
            _ => {
                return Err(GfError::Validation(
                    "committed participant has unsupported encoding".into(),
                ));
            }
        },
        schema_fingerprint: snapshot.schema_fingerprint,
        row_count: snapshot.row_count,
        bytes: snapshot.bytes,
    })
}

fn write_parquet(batch: &RecordBatch, schema: &SchemaRef) -> Result<Vec<u8>, GfError> {
    let mut writer = ArrowWriter::try_new(
        Vec::new(),
        Arc::clone(schema),
        Some(graphforge_storage::permanent_parquet::writer_properties().build()),
    )
    .map_err(|error| GfError::Storage(error.to_string()))?;
    writer
        .write(batch)
        .map_err(|error| GfError::Storage(error.to_string()))?;
    writer
        .into_inner()
        .map_err(|error| GfError::Storage(error.to_string()))
}

pub(crate) fn read_parquet(bytes: &[u8]) -> Result<Vec<RecordBatch>, GfError> {
    let file =
        tempfile::NamedTempFile::new().map_err(|error| GfError::Storage(error.to_string()))?;
    fs::write(file.path(), bytes).map_err(|error| GfError::Storage(error.to_string()))?;
    ParquetRecordBatchReaderBuilder::try_new(
        file.reopen()
            .map_err(|error| GfError::Storage(error.to_string()))?,
    )
    .map_err(|error| GfError::Validation(format!("invalid knowledge parquet: {error}")))?
    .build()
    .map_err(|error| GfError::Validation(format!("invalid knowledge parquet: {error}")))?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|error| GfError::Validation(format!("invalid knowledge parquet: {error}")))
}

fn read_or_empty(
    snapshot: &graphforge_storage::ProjectParticipantSnapshot,
    assertions: bool,
) -> Result<Vec<RecordBatch>, GfError> {
    if snapshot.row_count == 0 {
        let ledger = AssertionLedger::default();
        Ok(vec![if assertions {
            ledger.assertion_batch().map_err(knowledge_error)?
        } else {
            ledger.graph_ref_batch().map_err(knowledge_error)?
        }])
    } else {
        read_parquet(&snapshot.bytes)
    }
}

fn read_confidence_or_empty(
    snapshot: &graphforge_storage::ProjectParticipantSnapshot,
    assessments: bool,
) -> Result<Vec<RecordBatch>, GfError> {
    if snapshot.row_count == 0 {
        let ledger = ConfidenceLedger::default();
        Ok(vec![if assessments {
            ledger.assessment_batch().map_err(knowledge_error)?
        } else {
            ledger.input_batch().map_err(knowledge_error)?
        }])
    } else {
        read_parquet(&snapshot.bytes)
    }
}

fn read_evidence_or_empty(
    snapshot: &graphforge_storage::ProjectParticipantSnapshot,
) -> Result<Vec<RecordBatch>, GfError> {
    if snapshot.row_count == 0 {
        Ok(vec![
            EvidenceLedger::default().batch().map_err(knowledge_error)?,
        ])
    } else {
        read_parquet(&snapshot.bytes)
    }
}

pub(crate) fn require_participant_contract(
    snapshot: &graphforge_storage::ProjectParticipantSnapshot,
    family: &str,
) -> Result<(), GfError> {
    let registry = schema_registry();
    let expected = registry
        .iter()
        .find(|entry| entry.record_family == family)
        .expect("registered knowledge family");
    if snapshot.capability_version != expected.capability_version
        || snapshot.record_version != expected.record_version
        || snapshot.encoding != "parquet"
        || snapshot.schema_fingerprint != expected.schema_fingerprint
    {
        return Err(GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: "unsupported knowledge participant contract".into(),
        });
    }
    Ok(())
}

pub(crate) fn concat_or_empty(
    rows: &[RecordBatch],
    schema: &SchemaRef,
) -> Result<RecordBatch, GfError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    arrow::compute::concat_batches(schema, rows)
        .map_err(|error| GfError::Execution(error.to_string()))
}

pub(crate) fn with_next_token(
    batch: &RecordBatch,
    next: Option<&PageToken>,
) -> Result<RecordBatch, GfError> {
    let mut metadata = batch.schema().metadata().clone();
    if let Some(next) = next {
        metadata.insert(
            "graphforge.next_page_token".into(),
            next.as_str().to_owned(),
        );
    }
    let schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().to_vec(),
        metadata,
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec())
        .map_err(|error| GfError::Execution(error.to_string()))
}

pub(crate) fn assertion_result(batch: RecordBatch) -> graphforge_exec::ExecutionResult {
    let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
    graphforge_exec::ExecutionResult {
        schema: batch.schema(),
        batches: vec![batch],
        stats: graphforge_exec::ExecutionStats {
            rows_produced: rows,
            execution_time_ms: 0,
        },
        side_effects: None,
        mutation_receipt: None,
    }
}

#[cfg(test)]
mod tests;
