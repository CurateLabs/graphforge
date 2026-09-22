//! Research Source and Artifact registration (#1349).

use std::collections::HashSet;

use graphforge_knowledge::{
    ARTIFACT_SCHEMA, Artifact, ArtifactAvailability, ArtifactDerivation, ArtifactDerivationLedger,
    ArtifactKind, ArtifactLedger, ArtifactPayloadKind, ArtifactPreferenceEvent,
    ArtifactPreferenceLedger, DerivationRole, DerivationSubjectKind, RETENTION_DEPENDENCY_SCHEMA,
    SOURCE_SCHEMA, Source, SourceKind, SourceLedger,
};
use sha2::{Digest, Sha256};

use super::ledger::{
    merged_artifact_provenance, merged_preference_provenance, merged_source_provenance,
    read_preference_ledger, read_retention_ledger, source_artifact_publication_participants,
};
use super::{
    ApiErrorCode, EventKind, GfError, GraphForge, PageRequest, ProjectCapability,
    ProjectGenerationRequest, ProjectStageOutcome, ProvenanceEvent, ResolvedProjectGeneration,
    Uuid, WriteContext, assertion_result, concat_or_empty, knowledge_error,
    knowledge_generation_uuid, lock_graph_visibility, match_requested_edge_uuids,
    match_requested_node_uuids, not_found_kind, provenance_error, read_artifact_ledger,
    read_derivation_ledger, read_evidence_ledger, read_ledger, read_source_ledger, require_uuid,
    transaction_conflict, validate_write_context, with_next_token,
};
use crate::PageToken;
use crate::algorithm_runs::read_ledger as read_algorithm_run_ledger;

/// Payload reference for one Artifact registration.
#[derive(Clone, Debug, PartialEq)]
pub enum ArtifactPayloadRequest {
    /// Locally retained bytes installed into project CAS during publication.
    LocalBytes(Vec<u8>),
    /// Historical external reference; never fetched by Core.
    ExternalReference {
        /// Stable external URI.
        uri: String,
        /// Optional integrity fingerprint captured at registration time.
        fingerprint: Option<[u8; 32]>,
    },
    /// Explicitly absent bytes.
    Absent,
}

/// One ordered derivation input for Artifact registration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivationInput {
    /// Input subject UUID.
    pub input_uuid: Uuid,
    /// Closed input subject kind.
    pub input_kind: DerivationSubjectKind,
}

/// Frozen request for one immutable research Source.
#[derive(Clone, Debug, PartialEq)]
pub struct RegisterSourceRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 Source identity.
    pub source_uuid: Uuid,
    /// Bounded human-readable label.
    pub label: String,
    /// Closed source kind.
    pub source_kind: SourceKind,
    /// Optional stable external identity URI.
    pub identity_uri: Option<String>,
}

/// Frozen request for one immutable research Artifact.
#[derive(Clone, Debug, PartialEq)]
pub struct RegisterArtifactRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 Artifact identity.
    pub artifact_uuid: Uuid,
    /// Parent Source identity.
    pub source_uuid: Uuid,
    /// Closed artifact kind.
    pub artifact_kind: ArtifactKind,
    /// MIME-like media type label.
    pub media_type: String,
    /// Closed payload reference.
    pub payload: ArtifactPayloadRequest,
    /// Ordered derivation inputs.
    pub derivation_inputs: Vec<DerivationInput>,
    /// Optional algorithm-run identity for OCR/extraction.
    pub run_uuid: Option<Uuid>,
}

/// Frozen filter and page request for Sources.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListSourcesRequest {
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Frozen filter and page request for Artifacts.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListArtifactsRequest {
    /// Optional Source UUID filter.
    pub source_uuid: Option<Uuid>,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

/// Frozen request to set the preferred Artifact for one Source.
#[derive(Clone, Debug, PartialEq)]
pub struct SetPreferredArtifactRequest {
    /// Idempotency identity and optional actor.
    pub context: WriteContext,
    /// Caller-supplied UUIDv7 preference-event identity.
    pub preference_event_uuid: Uuid,
    /// Parent Source identity.
    pub source_uuid: Uuid,
    /// Newly preferred Artifact identity.
    pub artifact_uuid: Uuid,
    /// Bounded human-readable reason.
    pub reason: String,
}

/// Frozen read-only replacement-impact request for one Source preference change.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReplacementImpactRequest {
    /// Parent Source identity.
    pub source_uuid: Uuid,
    /// Proposed preferred Artifact identity.
    pub artifact_uuid: Uuid,
}

/// Frozen read-only retention-closure request for one scope.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RetentionDependencyClosureRequest {
    /// Selection or retention root UUID.
    pub scope_uuid: Uuid,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

impl GraphForge {
    /// Atomically register one immutable research Source and its provenance.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn register_source(
        &self,
        request: RegisterSourceRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        require_uuid(request.source_uuid, "source_uuid")?;
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
                "project generation changed before source publication",
            ));
        }
        let existing = read_source_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let event = ProvenanceEvent::new(
            request.context.operation_uuid.0,
            EventKind::RegisterSource,
            request.context.actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        let staged = SourceLedger::new(vec![
            Source::new(
                request.source_uuid,
                request.label,
                request.source_kind,
                request.identity_uri,
                event.provenance_uuid,
                recorded_at_micros,
            )
            .map_err(knowledge_error)?,
        ])
        .map_err(knowledge_error)?;
        if let Some(index) = existing
            .sources
            .iter()
            .position(|row| row.source_uuid == request.source_uuid)
        {
            if existing
                .source_fingerprint(request.source_uuid)
                .map_err(knowledge_error)?
                == staged
                    .source_fingerprint(request.source_uuid)
                    .map_err(knowledge_error)?
            {
                return Ok(assertion_result(
                    existing.batch().map_err(knowledge_error)?.slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "source UUID was reused for different canonical content",
            ));
        }
        let sources = existing.merge(&staged).map_err(knowledge_error)?;
        let artifacts = read_artifact_ledger(&parent)?;
        let derivations = read_derivation_ledger(&parent)?;
        let preferences = read_preference_ledger(&parent)?;
        let retention = read_retention_ledger(&parent)?;
        let provenance = merged_source_provenance(&parent, request.source_uuid, &event)?;
        publish_source_artifact(
            self,
            &request.context,
            &parent,
            expected_parent,
            &sources,
            &artifacts,
            &derivations,
            &preferences,
            &retention,
            &provenance,
            request.source_uuid,
            None,
        )
    }

    /// Atomically register one immutable research Artifact, derivations, and provenance.
    #[allow(
        clippy::needless_pass_by_value,
        clippy::too_many_lines,
        reason = "graphforge-knowledge-api/1 freezes owned request structs; publication validates every participant"
    )]
    pub fn register_artifact(
        &self,
        request: RegisterArtifactRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        require_uuid(request.artifact_uuid, "artifact_uuid")?;
        require_uuid(request.source_uuid, "source_uuid")?;
        if let Some(run_uuid) = request.run_uuid {
            require_uuid(run_uuid, "run_uuid")?;
        }
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
                "project generation changed before artifact publication",
            ));
        }
        if !read_source_ledger(&parent)?
            .sources
            .iter()
            .any(|row| row.source_uuid == request.source_uuid)
        {
            return Err(not_found_kind("source"));
        }
        validate_derivation_inputs(self, &parent, &request.derivation_inputs)?;
        let (
            payload_kind,
            content_sha256,
            content_length,
            external_uri,
            external_fingerprint,
            availability,
            local_bytes,
        ) = resolve_payload(&request.payload)?;
        let existing_artifacts = read_artifact_ledger(&parent)?;
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        let event = ProvenanceEvent::new(
            request.context.operation_uuid.0,
            EventKind::RegisterArtifact,
            request.context.actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        let artifact_row = Artifact::new(
            request.artifact_uuid,
            request.source_uuid,
            request.artifact_kind,
            request.media_type,
            payload_kind,
            content_sha256,
            content_length,
            external_uri,
            external_fingerprint,
            availability,
            request.run_uuid,
            event.provenance_uuid,
            recorded_at_micros,
        )
        .map_err(knowledge_error)?;
        let staged_artifacts =
            ArtifactLedger::new(vec![artifact_row.clone()]).map_err(knowledge_error)?;
        if let Some(index) = existing_artifacts
            .artifacts
            .iter()
            .position(|row| row.artifact_uuid == request.artifact_uuid)
        {
            if existing_artifacts
                .artifact_fingerprint(request.artifact_uuid)
                .map_err(knowledge_error)?
                == staged_artifacts
                    .artifact_fingerprint(request.artifact_uuid)
                    .map_err(knowledge_error)?
            {
                return Ok(assertion_result(
                    existing_artifacts
                        .batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "artifact UUID was reused for different canonical content",
            ));
        }
        let artifacts = existing_artifacts
            .merge(&staged_artifacts)
            .map_err(knowledge_error)?;
        let existing_derivations = read_derivation_ledger(&parent)?;
        let mut staged_derivation_rows = Vec::new();
        for (ordinal, input) in request.derivation_inputs.iter().enumerate() {
            staged_derivation_rows.push(
                ArtifactDerivation::new(
                    request.artifact_uuid,
                    DerivationSubjectKind::Artifact,
                    input.input_uuid,
                    input.input_kind,
                    DerivationRole::Input,
                    u32::try_from(ordinal)
                        .map_err(|_| GfError::Execution("derivation ordinal exceeds u32".into()))?,
                    event.provenance_uuid,
                    recorded_at_micros,
                )
                .map_err(knowledge_error)?,
            );
        }
        let staged_derivations =
            ArtifactDerivationLedger::new(staged_derivation_rows).map_err(knowledge_error)?;
        let derivations = existing_derivations
            .merge(&staged_derivations)
            .map_err(knowledge_error)?;
        let sources = read_source_ledger(&parent)?;
        let preferences = read_preference_ledger(&parent)?;
        let retention = read_retention_ledger(&parent)?;
        let derivation_lineage = request
            .derivation_inputs
            .iter()
            .map(|input| (input.input_uuid, input.input_kind))
            .collect::<Vec<_>>();
        let provenance = merged_artifact_provenance(
            &parent,
            request.source_uuid,
            request.artifact_uuid,
            &derivation_lineage,
            &event,
        )?;
        publish_source_artifact(
            self,
            &request.context,
            &parent,
            expected_parent,
            &sources,
            &artifacts,
            &derivations,
            &preferences,
            &retention,
            &provenance,
            request.artifact_uuid,
            local_bytes.as_deref(),
        )
    }

    /// Return one immutable Source row.
    pub fn source(&self, source_uuid: Uuid) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(source_uuid, "source_uuid")?;
        let generation = self.generation_for_read()?;
        let ledger = read_source_ledger(&generation)?;
        let index = ledger
            .sources
            .iter()
            .position(|row| row.source_uuid == source_uuid)
            .ok_or_else(|| not_found_kind("source"))?;
        Ok(assertion_result(
            ledger.batch().map_err(knowledge_error)?.slice(index, 1),
        ))
    }

    /// List Sources in `(recorded_at, source_uuid)` order.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn list_sources(
        &self,
        request: ListSourcesRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        let generation = self.generation_for_read()?;
        let ledger = read_source_ledger(&generation)?;
        page_ledger_rows(
            &ledger.batch().map_err(knowledge_error)?,
            &SOURCE_SCHEMA,
            generation.generation_uuid(),
            &request.page,
        )
    }

    /// Return one immutable Artifact row.
    pub fn artifact(
        &self,
        artifact_uuid: Uuid,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(artifact_uuid, "artifact_uuid")?;
        let generation = self.generation_for_read()?;
        let ledger = read_artifact_ledger(&generation)?;
        let index = ledger
            .artifacts
            .iter()
            .position(|row| row.artifact_uuid == artifact_uuid)
            .ok_or_else(|| not_found_kind("artifact"))?;
        Ok(assertion_result(
            ledger.batch().map_err(knowledge_error)?.slice(index, 1),
        ))
    }

    /// List Artifacts in `(recorded_at, artifact_uuid)` order.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn list_artifacts(
        &self,
        request: ListArtifactsRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        let generation = self.generation_for_read()?;
        let ledger = read_artifact_ledger(&generation)?;
        let batch = ledger.batch().map_err(knowledge_error)?;
        let rows = ledger
            .artifacts
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                request
                    .source_uuid
                    .is_none_or(|source_uuid| row.source_uuid == source_uuid)
            })
            .map(|(index, _)| batch.slice(index, 1))
            .collect::<Vec<_>>();
        page_record_batches(
            &rows,
            &ARTIFACT_SCHEMA,
            generation.generation_uuid(),
            &request.page,
        )
    }

    /// Atomically record one preferred-representation change for a Source.
    #[allow(
        clippy::needless_pass_by_value,
        clippy::too_many_lines,
        reason = "graphforge-knowledge-api/1 freezes owned request structs; preference publication validates every participant"
    )]
    pub fn set_preferred_artifact(
        &self,
        request: SetPreferredArtifactRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        validate_write_context(&request.context)?;
        require_uuid(request.preference_event_uuid, "preference_event_uuid")?;
        require_uuid(request.source_uuid, "source_uuid")?;
        require_uuid(request.artifact_uuid, "artifact_uuid")?;
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
                "project generation changed before preference publication",
            ));
        }
        let sources = read_source_ledger(&parent)?;
        if !sources
            .sources
            .iter()
            .any(|row| row.source_uuid == request.source_uuid)
        {
            return Err(not_found_kind("source"));
        }
        let artifacts = read_artifact_ledger(&parent)?;
        if !artifacts.artifacts.iter().any(|row| {
            row.artifact_uuid == request.artifact_uuid && row.source_uuid == request.source_uuid
        }) {
            return Err(not_found_kind("artifact"));
        }
        let existing_preferences = read_preference_ledger(&parent)?;
        // Check for an already-committed row BEFORE constructing the staged row:
        // prior_artifact_uuid is ledger-derived and may equal artifact_uuid on retry (the
        // committed first preference becomes the current preferred), causing construction to
        // fail validation. Compare against the request's caller-supplied fields instead.
        if let Some(index) = existing_preferences
            .events
            .iter()
            .position(|row| row.preference_event_uuid == request.preference_event_uuid)
        {
            let existing_fp = existing_preferences
                .preference_fingerprint(request.preference_event_uuid)
                .map_err(knowledge_error)?;
            let request_fp = ArtifactPreferenceLedger::preference_request_fingerprint(
                request.preference_event_uuid,
                request.source_uuid,
                request.artifact_uuid,
                &request.reason,
            )
            .map_err(knowledge_error)?;
            if existing_fp == request_fp {
                return Ok(assertion_result(
                    existing_preferences
                        .batch()
                        .map_err(knowledge_error)?
                        .slice(index, 1),
                ));
            }
            return Err(transaction_conflict(
                "preference event UUID was reused for different canonical content",
            ));
        }
        let prior_artifact_uuid =
            existing_preferences.current_preferred_artifact(request.source_uuid);
        let now = (self.clock.lock().expect("clock lock poisoned"))()?;
        let recorded_at_micros = existing_preferences
            .events
            .iter()
            .filter(|event| event.source_uuid == request.source_uuid)
            .map(|event| event.recorded_at_micros)
            .max()
            .map(|prior| {
                prior
                    .checked_add(1)
                    .map(|next| next.max(now))
                    .ok_or_else(|| {
                        GfError::Validation("preferred Artifact event order is exhausted".into())
                    })
            })
            .transpose()?
            .unwrap_or(now);
        let event = ProvenanceEvent::new(
            request.context.operation_uuid.0,
            EventKind::SetArtifactPreference,
            request.context.actor_uuid,
            recorded_at_micros,
        )
        .map_err(provenance_error)?;
        let preference_row = ArtifactPreferenceEvent::new(
            request.preference_event_uuid,
            request.source_uuid,
            request.artifact_uuid,
            prior_artifact_uuid,
            request.reason,
            event.provenance_uuid,
            recorded_at_micros,
        )
        .map_err(knowledge_error)?;
        let staged_preferences =
            ArtifactPreferenceLedger::new(vec![preference_row.clone()]).map_err(knowledge_error)?;
        let preferences = existing_preferences
            .merge(&staged_preferences)
            .map_err(knowledge_error)?;
        let derivations = read_derivation_ledger(&parent)?;
        let retention = read_retention_ledger(&parent)?;
        let provenance = merged_preference_provenance(
            &parent,
            request.source_uuid,
            request.artifact_uuid,
            &event,
        )?;
        publish_source_artifact(
            self,
            &request.context,
            &parent,
            expected_parent,
            &sources,
            &artifacts,
            &derivations,
            &preferences,
            &retention,
            &provenance,
            request.artifact_uuid,
            None,
        )?;
        let generation = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        let committed = read_preference_ledger(&generation)?;
        let index = committed
            .events
            .iter()
            .position(|row| row.preference_event_uuid == request.preference_event_uuid)
            .ok_or_else(|| GfError::Validation("committed preference event is absent".into()))?;
        Ok(assertion_result(
            committed.batch().map_err(knowledge_error)?.slice(index, 1),
        ))
    }

    /// Report Artifacts directly or transitively affected by a preference replacement.
    pub fn replacement_impact(
        &self,
        request: ReplacementImpactRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(request.source_uuid, "source_uuid")?;
        require_uuid(request.artifact_uuid, "artifact_uuid")?;
        let generation = self.generation_for_read()?;
        let artifacts = read_artifact_ledger(&generation)?;
        let preferences = read_preference_ledger(&generation)?;
        let derivations = read_derivation_ledger(&generation)?;
        if !artifacts.artifacts.iter().any(|row| {
            row.artifact_uuid == request.artifact_uuid && row.source_uuid == request.source_uuid
        }) {
            return Err(not_found_kind("artifact"));
        }
        let prior = preferences.current_preferred_artifact(request.source_uuid);
        let mut affected = HashSet::new();
        if let Some(prior_uuid) = prior
            && prior_uuid != request.artifact_uuid
        {
            affected.insert(prior_uuid);
            for index in super::lineage::collect_derivation_indices_internal(
                &derivations,
                prior_uuid,
                DerivationSubjectKind::Artifact,
                super::lineage::LineageDirection::Forward,
                32,
            ) {
                let row = &derivations.derivations[index];
                if row.output_kind == DerivationSubjectKind::Artifact {
                    affected.insert(row.output_uuid);
                }
            }
        }
        let batch = artifacts.batch().map_err(knowledge_error)?;
        let rows = artifacts
            .artifacts
            .iter()
            .enumerate()
            .filter(|(_, row)| affected.contains(&row.artifact_uuid))
            .map(|(index, _)| batch.slice(index, 1))
            .collect::<Vec<_>>();
        Ok(assertion_result(concat_or_empty(&rows, &ARTIFACT_SCHEMA)?))
    }

    /// Return explicit retention dependencies pinned to one scope.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn retention_dependency_closure(
        &self,
        request: RetentionDependencyClosureRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(request.scope_uuid, "scope_uuid")?;
        let generation = self.generation_for_read()?;
        let ledger = read_retention_ledger(&generation)?;
        let batch = ledger.batch().map_err(knowledge_error)?;
        let rows = ledger
            .dependencies
            .iter()
            .enumerate()
            .filter(|(_, row)| row.scope_uuid == request.scope_uuid)
            .map(|(index, _)| batch.slice(index, 1))
            .collect::<Vec<_>>();
        page_record_batches(
            &rows,
            &RETENTION_DEPENDENCY_SCHEMA,
            generation.generation_uuid(),
            &request.page,
        )
    }
}

type ResolvedArtifactPayload = (
    ArtifactPayloadKind,
    Option<[u8; 32]>,
    Option<u64>,
    Option<String>,
    Option<[u8; 32]>,
    ArtifactAvailability,
    Option<Vec<u8>>,
);

fn resolve_payload(payload: &ArtifactPayloadRequest) -> Result<ResolvedArtifactPayload, GfError> {
    match payload {
        ArtifactPayloadRequest::LocalBytes(bytes) => {
            let digest = Sha256::digest(bytes.as_slice()).into();
            let length = u64::try_from(bytes.len())
                .map_err(|_| GfError::Validation("artifact bytes exceed u64".into()))?;
            Ok((
                ArtifactPayloadKind::LocalSha256,
                Some(digest),
                Some(length),
                None,
                None,
                ArtifactAvailability::LocalVerified,
                Some(bytes.clone()),
            ))
        }
        ArtifactPayloadRequest::ExternalReference { uri, fingerprint } => Ok((
            ArtifactPayloadKind::ExternalReference,
            None,
            None,
            Some(uri.clone()),
            *fingerprint,
            ArtifactAvailability::ExternalOnly,
            None,
        )),
        ArtifactPayloadRequest::Absent => Ok((
            ArtifactPayloadKind::Absent,
            None,
            None,
            None,
            None,
            ArtifactAvailability::MissingLocal,
            None,
        )),
    }
}

#[allow(clippy::too_many_arguments)] // publication bundles every coupled ledger participant
fn publish_source_artifact(
    graph: &GraphForge,
    context: &WriteContext,
    parent: &ResolvedProjectGeneration,
    expected_parent: Uuid,
    sources: &SourceLedger,
    artifacts: &ArtifactLedger,
    derivations: &ArtifactDerivationLedger,
    preferences: &graphforge_knowledge::ArtifactPreferenceLedger,
    retention: &graphforge_knowledge::RetentionDependencyLedger,
    provenance: &graphforge_provenance::ProvenanceLedger,
    result_uuid: Uuid,
    local_bytes: Option<&[u8]>,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let root = graph.resolved_generation.container_root();
    let participants = source_artifact_publication_participants(
        parent,
        sources,
        artifacts,
        derivations,
        preferences,
        retention,
        provenance,
    )?;
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid: context.operation_uuid.0,
        generation_uuid: knowledge_generation_uuid(
            b"source_artifact",
            context.operation_uuid,
            &participants,
        ),
        capabilities,
        participants,
    };
    let receipt = match graph.stage_project_generation(&publication)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => {
            let mut prepare = |_: &ResolvedProjectGeneration| -> Result<(), GfError> {
                if let Some(bytes) = local_bytes {
                    graphforge_storage::install_project_object_bytes(root, bytes)?;
                }
                Ok(())
            };
            if local_bytes.is_some() {
                let lease = graphforge_storage::begin_graph_object_publication(root)?;
                staged
                    .validate(
                        |_| Ok(()),
                        |actual_parent, _| {
                            if actual_parent.generation_uuid() != expected_parent {
                                return Err(transaction_conflict(
                                    "project generation changed before artifact publication",
                                ));
                            }
                            Ok(())
                        },
                    )?
                    .publish_with_reader_preparation(Some(&lease), &mut prepare)?
            } else {
                staged
                    .validate(
                        |_| Ok(()),
                        |actual_parent, _| {
                            if actual_parent.generation_uuid() != expected_parent {
                                return Err(transaction_conflict(
                                    "project generation changed before source publication",
                                ));
                            }
                            Ok(())
                        },
                    )?
                    .publish()?
            }
        }
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    let generation = graphforge_storage::resolve_project_generation(root)?;
    if sources
        .sources
        .iter()
        .any(|row| row.source_uuid == result_uuid)
    {
        let ledger = read_source_ledger(&generation)?;
        let index = ledger
            .sources
            .iter()
            .position(|row| row.source_uuid == result_uuid)
            .ok_or_else(|| GfError::Validation("committed source is absent".into()))?;
        return Ok(assertion_result(
            ledger.batch().map_err(knowledge_error)?.slice(index, 1),
        ));
    }
    let ledger = read_artifact_ledger(&generation)?;
    let index = ledger
        .artifacts
        .iter()
        .position(|row| row.artifact_uuid == result_uuid)
        .ok_or_else(|| GfError::Validation("committed artifact is absent".into()))?;
    Ok(assertion_result(
        ledger.batch().map_err(knowledge_error)?.slice(index, 1),
    ))
}

fn validate_derivation_inputs(
    graph: &GraphForge,
    parent: &ResolvedProjectGeneration,
    inputs: &[DerivationInput],
) -> Result<(), GfError> {
    for input in inputs {
        let found = match input.input_kind {
            DerivationSubjectKind::Source => read_source_ledger(parent)?
                .sources
                .iter()
                .any(|row| row.source_uuid == input.input_uuid),
            DerivationSubjectKind::Artifact => read_artifact_ledger(parent)?
                .artifacts
                .iter()
                .any(|row| row.artifact_uuid == input.input_uuid),
            DerivationSubjectKind::Node => {
                let mut pending = HashSet::from([input.input_uuid]);
                match_requested_node_uuids(graph, &mut pending)?;
                pending.is_empty()
            }
            DerivationSubjectKind::Edge => {
                let mut pending = HashSet::from([input.input_uuid]);
                match_requested_edge_uuids(graph, &mut pending)?;
                pending.is_empty()
            }
            DerivationSubjectKind::Assertion => read_ledger(parent)?
                .assertions
                .iter()
                .any(|row| row.assertion_uuid == input.input_uuid),
            DerivationSubjectKind::EvidenceLink => read_evidence_ledger(parent)?
                .links
                .iter()
                .any(|row| row.evidence_uuid == input.input_uuid),
            DerivationSubjectKind::AlgorithmRun => read_algorithm_run_ledger(parent)?
                .runs
                .iter()
                .any(|row| row.run_uuid == input.input_uuid),
        };
        if !found {
            return Err(GfError::Api {
                code: ApiErrorCode::NotFound,
                message: "derivation input subject was not found".into(),
            });
        }
    }
    Ok(())
}

fn page_ledger_rows(
    batch: &arrow::record_batch::RecordBatch,
    schema: &std::sync::Arc<arrow::datatypes::Schema>,
    generation_uuid: Uuid,
    page: &PageRequest,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let selected = (0..batch.num_rows()).collect::<Vec<_>>();
    let (start, end) = crate::paging::validate_page(page, generation_uuid, selected.len())?;
    let rows = selected[start..end]
        .iter()
        .map(|index| batch.slice(*index, 1))
        .collect::<Vec<_>>();
    let output = concat_or_empty(&rows, schema)?;
    let next = (end < selected.len()).then(|| PageToken::new(generation_uuid, end));
    Ok(assertion_result(with_next_token(&output, next.as_ref())?))
}

fn page_record_batches(
    rows: &[arrow::record_batch::RecordBatch],
    schema: &std::sync::Arc<arrow::datatypes::Schema>,
    generation_uuid: Uuid,
    page: &PageRequest,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let selected = (0..rows.len()).collect::<Vec<_>>();
    let (start, end) = crate::paging::validate_page(page, generation_uuid, selected.len())?;
    let slice = selected[start..end]
        .iter()
        .map(|index| rows[*index].clone())
        .collect::<Vec<_>>();
    let output = concat_or_empty(&slice, schema)?;
    let next = (end < selected.len()).then(|| PageToken::new(generation_uuid, end));
    Ok(assertion_result(with_next_token(&output, next.as_ref())?))
}
