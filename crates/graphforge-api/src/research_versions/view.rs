//! Native read-only execution over a private authenticated historical Project.
use super::{
    ApiErrorCode, Arc, DataType, ExecutionResult, Field, GfError, GraphForge, RecordBatch,
    ResearchEvidenceReference, ResearchVersionRecord, Schema, Uuid, not_retained,
};
use std::fmt::Write;

/// Immutable citation and native historical graph. No mutable facade is exposed.
#[derive(Debug)]
pub struct ResearchVersionView {
    pub(super) version: ResearchVersionRecord,
    pub(super) graph: GraphForge,
}

impl ResearchVersionView {
    /// Frozen citation and content identity; never replaced with current metadata.
    pub fn version(&self) -> &ResearchVersionRecord {
        &self.version
    }

    /// Execute read-only Cypher against exact retained graph content.
    pub fn execute(&self, query: &str) -> Result<ExecutionResult, GfError> {
        self.require_retained("graph", &["files", "snapshot"])?;
        self.graph.execute_read_only(query)
    }

    /// Exact frozen workspace ontology.
    pub fn workspace_ontology(&self) -> Result<graphforge_storage::WorkspaceOntology, GfError> {
        self.graph.workspace_ontology()
    }

    /// Exact frozen research metadata.
    pub fn research_project_metadata(
        &self,
    ) -> Result<graphforge_storage::WorkspaceResearchMetadata, GfError> {
        self.require_retained("workspace", &["research_metadata"])?;
        self.graph.research_project_metadata()
    }

    /// Read immutable Artifact metadata from the frozen native ledger.
    pub fn artifact(&self, artifact_uuid: Uuid) -> Result<ExecutionResult, GfError> {
        self.graph.artifact(artifact_uuid)
    }

    fn require_retained(&self, capability: &str, families: &[&str]) -> Result<(), GfError> {
        if self.version.content.source_version.is_some()
            && !self.version.content.participants.iter().any(|participant| {
                participant.key.capability == capability
                    && families.contains(&participant.key.family.as_str())
            })
        {
            return Err(GfError::Api {
                code: ApiErrorCode::ResultNotRetained,
                message:
                    "historical domain is outside the retained projection; expansion is unavailable"
                        .into(),
            });
        }
        Ok(())
    }

    /// Return exact locally retained Artifact bytes as an Arrow binary value.
    /// External-only or missing evidence is reported explicitly without fetching.
    pub fn artifact_payload(&self, artifact_uuid: Uuid) -> Result<ExecutionResult, GfError> {
        let evidence = self
            .version
            .content
            .evidence
            .iter()
            .find(|reference| match reference {
                ResearchEvidenceReference::Local {
                    artifact_uuid: id, ..
                }
                | ResearchEvidenceReference::ExternalOnly {
                    artifact_uuid: id, ..
                }
                | ResearchEvidenceReference::Unverifiable { artifact_uuid: id } => {
                    *id == artifact_uuid
                }
            })
            .ok_or_else(not_retained)?;
        let ResearchEvidenceReference::Local {
            sha256,
            byte_length,
            ..
        } = evidence
        else {
            return Err(GfError::Api { code: ApiErrorCode::ResultNotRetained,
                message: "historical Artifact bytes are external-only, missing, or unverifiable; Core does not fetch them".into() });
        };
        if *byte_length > 256 * 1024 * 1024 {
            return Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                message: "historical Artifact payload exceeds the 256 MiB read bound".into(),
            });
        }
        let mut digest = String::with_capacity(64);
        for byte in sha256 {
            write!(digest, "{byte:02x}").expect("writing to a String cannot fail");
        }
        let mut object = graphforge_storage::open_graph_object_by_digest(
            self.graph.resolved_generation.container_root(),
            &digest,
            *byte_length,
        )?;
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut object, &mut bytes)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::BinaryArray::from_vec(vec![
                bytes.as_slice(),
            ]))],
        )
        .map_err(|error| GfError::Validation(error.to_string()))?;
        Ok(ExecutionResult {
            schema,
            batches: vec![batch],
            stats: crate::ExecutionStats::default(),
            side_effects: None,
            mutation_receipt: None,
        })
    }
}

pub(super) fn materialize(
    owner: &GraphForge,
    version: &ResearchVersionRecord,
) -> Result<GraphForge, GfError> {
    let directory =
        Arc::new(tempfile::tempdir().map_err(|error| GfError::Storage(error.to_string()))?);
    let generation = graphforge_storage::research_versions::materialize_research_project(
        owner.resolved_generation.container_root(),
        version,
        directory.path(),
    )?;
    crate::checkpoints::validate_research_source(&generation)?;
    let mut graph = GraphForge::open_resolved_with_options(
        directory.path().to_path_buf(),
        generation.clone(),
        true,
        owner.write_options.clone(),
        owner.resource_policy.clone(),
        graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
            generation.generation_uuid(),
        ),
    )?;
    graph.lifecycle_mode =
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral;
    graph.research_materialization = Some(Arc::clone(&directory));
    graph.tempdir = Some(directory);
    Ok(graph)
}
