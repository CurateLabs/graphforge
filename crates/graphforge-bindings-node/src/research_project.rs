//! Research Project metadata and bounded local discovery bindings.

use crate::canonical_operation_id;
use crate::napi;
use crate::optional_uuid;
use crate::result_to_ipc;
use crate::to_napi_err;
use crate::{Buffer, GraphForge, Result};
use graphforge_api::{
    DiscoverResearchProjectsRequest, ResearchProjectDiscoveryLimits, ResearchProjectDiscoveryQuery,
    UpdateResearchMetadataRequest, WorkspaceResearchMetadata, WriteContext,
};

#[napi(object)]
/// Structured discovery filters over caller-supplied local Projects.
pub struct ResearchProjectDiscoveryQueryInput {
    /// Optional case-insensitive substring across title, description, tags, and subjects.
    pub free_text: Option<String>,
    /// Require every listed language label.
    pub languages: Option<Vec<String>>,
    /// Require every listed subject label.
    pub subjects: Option<Vec<String>>,
    /// Require every listed ontology label.
    pub ontologies: Option<Vec<String>>,
    /// Require every listed source-type label.
    pub source_types: Option<Vec<String>>,
    /// Optional temporal label substring match.
    pub temporal_label: Option<String>,
}

#[napi(object)]
/// Resource bounds for bounded local discovery.
pub struct ResearchProjectDiscoveryLimitsInput {
    /// Maximum summaries returned.
    pub max_projects: Option<u32>,
    /// Maximum candidate roots inspected.
    pub max_candidates: Option<u32>,
}

#[napi(object)]
/// Bounded discovery over caller-supplied local Project roots.
pub struct DiscoverResearchProjectsInput {
    /// Local durable Project roots to inspect.
    pub project_roots: Vec<String>,
    /// Structured and free-text filters.
    pub query: Option<ResearchProjectDiscoveryQueryInput>,
    /// Resource bounds.
    pub limits: Option<ResearchProjectDiscoveryLimitsInput>,
}

#[napi(object)]
/// Replace authoritative research metadata for the open Project.
pub struct UpdateResearchMetadataInput {
    /// Complete replacement metadata record.
    pub metadata: serde_json::Value,
    /// Required idempotency UUID.
    pub operation_uuid: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

#[napi(object)]
/// Stable durable Project identity separate from one graph snapshot.
pub struct ResearchProjectIdentityOutput {
    /// Native volume serial for the durable container root.
    pub volume_serial: String,
    /// Canonical hexadecimal file identity.
    pub file_id_hex: String,
    /// Committed generation UUID at summary time.
    pub generation_uuid: String,
}

#[napi(object)]
/// One metadata-only Project summary.
pub struct ResearchProjectSummaryOutput {
    /// Caller-supplied local path to the durable container root.
    pub project_path: String,
    /// Stable Project identity and current generation.
    pub identity: ResearchProjectIdentityOutput,
    /// Authoritative metadata record.
    pub metadata: serde_json::Value,
}

#[napi]
impl GraphForge {
    /// Inspect authoritative research metadata for the open Project.
    #[napi]
    pub fn research_project_metadata(&self) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        let metadata = graph
            .research_project_metadata()
            .map_err(|error| to_napi_err(&error))?;
        serde_json::to_value(metadata)
            .map_err(|error| to_napi_err(&graphforge_api::GfError::Validation(error.to_string())))
    }

    /// Return one metadata-only summary for the open Project.
    #[napi]
    pub fn research_project_summary(&self) -> Result<ResearchProjectSummaryOutput> {
        let graph = self.open_guard()?;
        let summary = graph
            .research_project_summary()
            .map_err(|error| to_napi_err(&error))?;
        Ok(ResearchProjectSummaryOutput {
            project_path: summary.project_path.display().to_string(),
            identity: ResearchProjectIdentityOutput {
                volume_serial: summary.identity.volume_serial.to_string(),
                file_id_hex: summary.identity.file_id_hex,
                generation_uuid: summary.identity.generation_uuid.hyphenated().to_string(),
            },
            metadata: serde_json::to_value(summary.metadata).map_err(|error| {
                to_napi_err(&graphforge_api::GfError::Validation(error.to_string()))
            })?,
        })
    }

    /// Replace authoritative research metadata atomically.
    #[napi]
    pub fn update_research_metadata(&self, request: UpdateResearchMetadataInput) -> Result<()> {
        let mut graph = self.open_write_guard()?;
        let metadata: WorkspaceResearchMetadata = serde_json::from_value(request.metadata)
            .map_err(|error| {
                to_napi_err(&graphforge_api::GfError::Validation(error.to_string()))
            })?;
        graph
            .update_research_metadata(UpdateResearchMetadataRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                },
                metadata,
            })
            .map_err(|error| to_napi_err(&error))
    }

    /// Discover caller-supplied local Projects without opening graph payloads.
    #[napi]
    pub fn discover_research_projects(request: DiscoverResearchProjectsInput) -> Result<Buffer> {
        let query = request.query.unwrap_or(ResearchProjectDiscoveryQueryInput {
            free_text: None,
            languages: None,
            subjects: None,
            ontologies: None,
            source_types: None,
            temporal_label: None,
        });
        let limits = request
            .limits
            .unwrap_or(ResearchProjectDiscoveryLimitsInput {
                max_projects: None,
                max_candidates: None,
            });
        let api_request = DiscoverResearchProjectsRequest {
            project_roots: request
                .project_roots
                .into_iter()
                .map(std::path::PathBuf::from)
                .collect(),
            query: ResearchProjectDiscoveryQuery {
                free_text: query.free_text,
                languages: query.languages.unwrap_or_default(),
                subjects: query.subjects.unwrap_or_default(),
                ontologies: query.ontologies.unwrap_or_default(),
                source_types: query.source_types.unwrap_or_default(),
                temporal_label: query.temporal_label,
            },
            limits: ResearchProjectDiscoveryLimits {
                max_projects: limits.max_projects.unwrap_or(1_024) as usize,
                max_candidates: limits.max_candidates.unwrap_or(1_024) as usize,
            },
        };
        let result = graphforge_api::GraphForge::discover_research_projects(&api_request)
            .map_err(|error| to_napi_err(&error))?;
        result_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }
}
