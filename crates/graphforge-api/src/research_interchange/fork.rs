//! Explicit independent Project creation, committed by the portable import owner.
use crate::{CancellationToken, GraphForge};
use graphforge_core::portable::{PortableV2Error, PortableV2ErrorCode};
use graphforge_storage::research_versions::ResearchForkRecord;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// Independent local Project governance; no hosted access enforcement is implied.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkResearchRequest {
    /// Caller-owned durable replay identity.
    pub operation_uuid: Uuid,
    /// Explicit fresh Project identity, different from the origin Project.
    pub project_uuid: Uuid,
    /// Exact retained source Version.
    pub version_uuid: Uuid,
    /// Optional distinct selected projection.
    pub projection: Option<super::ResearchExportProjection>,
    /// New or pristine durable Project directory.
    pub target: PathBuf,
    /// Recorded derivative author, not authentication.
    pub actor_uuid: Uuid,
    /// Explicit bounded governance policy for this independent Project.
    pub governance: String,
    /// Explicit adoption of the selected native ontology as local authority.
    pub adopt_selected_ontology: bool,
    /// Complete independent metadata, including explicit access-policy metadata.
    pub metadata: graphforge_storage::WorkspaceResearchMetadata,
}

/// Committed native Fork outcome; original graph and research citations are preserved.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkResearchResult {
    /// Independent authority created by this request.
    pub project_uuid: Uuid,
    /// Original source research citation.
    pub source_version_uuid: Uuid,
    /// Durable imported generation.
    pub generation_uuid: Uuid,
    /// True only for the same durable supported operation replay.
    pub idempotent_replay: bool,
}

impl GraphForge {
    /// Create an independent Project from explicit selected research and governance.
    pub fn fork_research(
        &self,
        request: &ForkResearchRequest,
        cancel: &CancellationToken,
    ) -> Result<ForkResearchResult, PortableV2Error> {
        if request.operation_uuid.is_nil()
            || request.project_uuid.is_nil()
            || request.actor_uuid.is_nil()
            || request.governance.trim().is_empty()
            || request.governance.len() > 4096
            || !request.adopt_selected_ontology
        {
            return Err(invalid());
        }
        request
            .metadata
            .to_canonical_json()
            .map_err(|_| invalid())?;
        cancel
            .checkpoint()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Cancelled, "Fork cancelled"))?;
        let intent = intent(request)?;
        if let Some(result) = replay(request, intent)? {
            return Ok(result);
        }
        let source = self.generation_for_read().map_err(|_| invalid())?;
        if crate::research_claims::authority::project_uuid(&source).map_err(|_| invalid())?
            == request.project_uuid
        {
            return Err(invalid());
        }
        let directory = tempfile::tempdir()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "cannot prepare Fork"))?;
        let output = directory.path().join("research");
        super::export::export(
            self,
            &super::ExportResearchRequest {
                version_uuid: request.version_uuid,
                output: output.clone(),
                bundled: false,
                projection: request.projection.clone(),
            },
            Some(request),
            cancel,
        )?;
        let result = GraphForge::import_portable_v2(
            &request.target,
            &crate::PortableV2ImportRequest {
                input: output,
                operation_id: crate::OperationId(request.operation_uuid),
                limits: graphforge_core::portable::PortableV2Limits::default(),
            },
            Some(cancel.flag()),
        )?;
        Ok(ForkResearchResult {
            project_uuid: request.project_uuid,
            source_version_uuid: request.version_uuid,
            generation_uuid: result.generation_uuid,
            idempotent_replay: result.idempotent_replay,
        })
    }
}

pub(super) fn record(request: &ForkResearchRequest) -> Result<ResearchForkRecord, PortableV2Error> {
    Ok(ResearchForkRecord {
        intent_sha256: intent(request)?,
        project_uuid: request.project_uuid,
        operation_uuid: request.operation_uuid,
        actor_uuid: request.actor_uuid,
        governance: request.governance.clone(),
        adopt_selected_ontology: request.adopt_selected_ontology,
    })
}

pub(super) fn configure(
    generation: &graphforge_storage::ResolvedProjectGeneration,
    request: &ForkResearchRequest,
) -> Result<graphforge_storage::ResolvedProjectGeneration, PortableV2Error> {
    use graphforge_storage::{
        ProjectCapability, ProjectGenerationRequest, ProjectParticipantEncoding,
        ProjectStageOutcome,
    };
    let lease = graphforge_storage::begin_graph_object_publication(generation.container_root())
        .map_err(|_| invalid())?;
    let mut participants = Vec::new();
    for snapshot in generation.participant_snapshots().map_err(|_| invalid())? {
        if snapshot.capability_id == "workspace" && snapshot.record_family_id == "research_metadata"
        {
            continue;
        }
        // The source Version already excludes history participants. Be explicit at
        // the Fork boundary: old promotions cannot become decisions of the new owner.
        if snapshot.capability_id == "research"
            && snapshot.record_family_id == "canonical_decisions"
        {
            continue;
        }
        participants.push(graphforge_storage::ProjectParticipant {
            capability_id: snapshot.capability_id,
            capability_version: snapshot.capability_version,
            record_family_id: snapshot.record_family_id,
            record_version: snapshot.record_version,
            encoding: match snapshot.encoding.as_str() {
                "json" => ProjectParticipantEncoding::Json,
                "arrow" => ProjectParticipantEncoding::Arrow,
                "parquet" => ProjectParticipantEncoding::Parquet,
                _ => return Err(invalid()),
            },
            schema_fingerprint: snapshot.schema_fingerprint,
            row_count: snapshot.row_count,
            bytes: snapshot.bytes,
        });
    }
    participants.push(
        request
            .metadata
            .to_project_participant()
            .map_err(|_| invalid())?,
    );
    let publication = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities: generation
            .capabilities()
            .into_iter()
            .map(|c| ProjectCapability {
                capability_id: c.capability_id,
                capability_version: c.capability_version,
            })
            .collect(),
        participants,
    };
    let staged = graphforge_storage::stage_project_generation_with_graph_tree_mode(
        generation.container_root(),
        &publication,
        None,
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral,
    )
    .map_err(|_| invalid())?;
    if let ProjectStageOutcome::Staged(staged) = staged {
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .map_err(|_| invalid())?
            .publish_with_graph_objects(&lease)
            .map_err(|_| invalid())?;
    }
    graphforge_storage::resolve_project_generation(generation.container_root())
        .map_err(|_| invalid())
}
fn invalid() -> PortableV2Error {
    PortableV2Error::new(
        PortableV2ErrorCode::Incompatible,
        "Fork requires valid independent identity, metadata, governance and explicit ontology adoption",
    )
}

fn replay(
    request: &ForkResearchRequest,
    intent: [u8; 32],
) -> Result<Option<ForkResearchResult>, PortableV2Error> {
    let Some(receipt) =
        graphforge_storage::published_project_transaction(&request.target, request.operation_uuid)
            .map_err(|_| invalid())?
    else {
        return Ok(None);
    };
    let current =
        graphforge_storage::resolve_project_generation(&request.target).map_err(|_| invalid())?;
    let registry = graphforge_storage::research_versions::read_research_registry(&current)
        .map_err(|_| invalid())?;
    let fork = registry
        .interchange
        .values()
        .filter_map(|archive| archive.fork.as_ref())
        .find(|fork| fork.operation_uuid == request.operation_uuid);
    if fork.is_none_or(|fork| {
        fork.intent_sha256 != intent || fork.project_uuid != request.project_uuid
    }) || receipt.generation_uuid
        != graphforge_core::uuid::portable_v2_import_generation(&request.operation_uuid)
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::ConcurrentMutation,
            "Fork operation identity has different immutable inputs",
        ));
    }
    Ok(Some(ForkResearchResult {
        project_uuid: request.project_uuid,
        source_version_uuid: request.version_uuid,
        generation_uuid: receipt.generation_uuid,
        idempotent_replay: true,
    }))
}

fn intent(request: &ForkResearchRequest) -> Result<[u8; 32], PortableV2Error> {
    use sha2::{Digest, Sha256};
    struct HashWriter {
        digest: Sha256,
        remaining: usize,
    }
    impl std::io::Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.remaining = self
                .remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("Fork intent exceeds bound"))?;
            self.digest.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = HashWriter {
        digest: Sha256::new(),
        remaining: 256 * 1024 * 1024,
    };
    writer.digest.update(b"graphforge-fork-intent/1");
    serde_json::to_writer(&mut writer, request).map_err(|_| invalid())?;
    Ok(writer.digest.finalize().into())
}

#[cfg(test)]
mod tests;
