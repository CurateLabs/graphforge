//! Research Project metadata inspection, mutation, and bounded local discovery.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_exec::{ExecutionResult, ExecutionStats};
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectStageOutcome,
    ResearchProjectDiscoveryLimits, ResearchProjectDiscoveryQuery, ResearchProjectSummary,
    WORKSPACE_CAPABILITY_ID, WORKSPACE_RESEARCH_METADATA_FAMILY, WorkspaceResearchMetadata,
    discover_research_projects, read_workspace_research_metadata, summarize_research_project,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{GraphForge, WriteContext};

/// Replace the authoritative research metadata for the open Project.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateResearchMetadataRequest {
    /// Idempotency and optional actor identity.
    pub context: WriteContext,
    /// Complete replacement metadata record.
    pub metadata: WorkspaceResearchMetadata,
}

/// Bounded discovery over caller-supplied local Project roots.
#[derive(Debug, Clone)]
pub struct DiscoverResearchProjectsRequest {
    /// Local durable Project roots to inspect.
    pub project_roots: Vec<PathBuf>,
    /// Structured and free-text filters.
    pub query: ResearchProjectDiscoveryQuery,
    /// Resource bounds.
    pub limits: ResearchProjectDiscoveryLimits,
}

impl GraphForge {
    /// Inspect authoritative research metadata for the open Project.
    ///
    /// # Errors
    /// Returns structured project errors for missing or corrupt metadata.
    pub fn research_project_metadata(&self) -> Result<WorkspaceResearchMetadata, GfError> {
        let generation = self.generation_for_read()?;
        read_workspace_research_metadata(&generation)
    }

    /// Return one metadata-only summary for the open Project without graph hydration.
    ///
    /// # Errors
    /// Returns structured project errors when the Project cannot be summarized.
    pub fn research_project_summary(&self) -> Result<ResearchProjectSummary, GfError> {
        summarize_research_project(self.generation_for_read()?.container_root())
    }

    /// Replace the authoritative research metadata record atomically.
    ///
    /// # Errors
    /// Returns validation, idempotency, or publication errors without partial mutation.
    pub fn update_research_metadata(
        &mut self,
        request: UpdateResearchMetadataRequest,
    ) -> Result<(), GfError> {
        let root = self.resolved_generation.container_root().to_path_buf();
        let parent = graphforge_storage::resolve_project_generation(&root)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(GfError::Validation(
                "project generation changed before research metadata publication".into(),
            ));
        }
        let metadata = request.metadata;
        metadata.to_canonical_json()?;
        let mut participants = parent
            .participant_snapshots()?
            .into_iter()
            .filter(|snapshot| {
                !(snapshot.capability_id == WORKSPACE_CAPABILITY_ID
                    && snapshot.record_family_id == WORKSPACE_RESEARCH_METADATA_FAMILY)
            })
            .map(snapshot_to_participant)
            .collect::<Result<Vec<_>, _>>()?;
        participants.push(metadata.to_project_participant()?);
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let operation_uuid = request.context.operation_uuid.0;
        let generation_uuid = research_metadata_generation_uuid(
            operation_uuid,
            request.context.actor_uuid,
            &participants,
        );
        let publication = ProjectGenerationRequest {
            transaction_uuid: operation_uuid,
            generation_uuid,
            capabilities: parent
                .capabilities()
                .into_iter()
                .map(|capability| ProjectCapability {
                    capability_id: capability.capability_id,
                    capability_version: capability.capability_version,
                })
                .collect(),
            participants,
        };
        let receipt = match graphforge_storage::stage_project_generation(&root, &publication)? {
            ProjectStageOutcome::AlreadyPublished(receipt) => {
                if receipt.generation_uuid != expected_parent {
                    return Err(GfError::Validation(
                        "research metadata operation identifies a generation that is no longer authoritative"
                            .into(),
                    ));
                }
                receipt
            }
            ProjectStageOutcome::Staged(staged) => staged
                .validate(
                    |_| Ok(()),
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(GfError::Validation(
                                "project generation changed before research metadata publication"
                                    .into(),
                            ));
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
        self.resolved_generation = graphforge_storage::resolve_project_generation(&root)?;
        Ok(())
    }

    /// Discover caller-supplied local Projects and return metadata-only summaries as Arrow.
    ///
    /// # Errors
    /// Returns structured validation errors for invalid discovery bounds.
    pub fn discover_research_projects(
        request: &DiscoverResearchProjectsRequest,
    ) -> Result<ExecutionResult, GfError> {
        let summaries =
            discover_research_projects(&request.project_roots, &request.query, request.limits)?;
        summaries_to_arrow(&summaries)
    }
}

#[allow(clippy::too_many_lines)]
fn summaries_to_arrow(summaries: &[ResearchProjectSummary]) -> Result<ExecutionResult, GfError> {
    let row_count = summaries.len();
    let project_paths = StringArray::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.project_path.display().to_string()),
    );
    let project_identity = StringArray::from_iter_values(summaries.iter().map(|summary| {
        format!(
            "{:016x}:{}",
            summary.identity.volume_serial, summary.identity.file_id_hex
        )
    }));
    let generation_uuid = StringArray::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.identity.generation_uuid.hyphenated().to_string()),
    );
    let titles = StringArray::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.title.as_deref().unwrap_or("")),
    );
    let descriptions = StringArray::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.description.as_deref().unwrap_or("")),
    );
    let languages = StringArray::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.languages.join(",")),
    );
    let subjects = StringArray::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.subjects.join(",")),
    );
    let ontologies = StringArray::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.ontologies.join(",")),
    );
    let temporal_labels = StringArray::from_iter_values(summaries.iter().map(|summary| {
        summary
            .metadata
            .temporal_coverage
            .as_ref()
            .and_then(|coverage| coverage.label.as_deref())
            .unwrap_or("")
    }));
    let text_entry_points = UInt64Array::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.discovery_facets.text_entry_points),
    );
    let source_entry_points = UInt64Array::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.discovery_facets.source_entry_points),
    );
    let entity_entry_points = UInt64Array::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.discovery_facets.entity_entry_points),
    );
    let relationship_entry_points = UInt64Array::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.discovery_facets.relationship_entry_points),
    );
    let story_document_entry_points =
        UInt64Array::from_iter_values(summaries.iter().map(|summary| {
            summary
                .metadata
                .discovery_facets
                .story_document_entry_points
        }));
    let event_location_entry_points =
        UInt64Array::from_iter_values(summaries.iter().map(|summary| {
            summary
                .metadata
                .discovery_facets
                .event_location_entry_points
        }));
    let ontology_type_entry_points = UInt64Array::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.discovery_facets.ontology_type_entry_points),
    );
    let linguistic_property_entry_points =
        UInt64Array::from_iter_values(summaries.iter().map(|summary| {
            summary
                .metadata
                .discovery_facets
                .linguistic_property_entry_points
        }));
    let traversal_query_entry_points =
        UInt64Array::from_iter_values(summaries.iter().map(|summary| {
            summary
                .metadata
                .discovery_facets
                .traversal_query_entry_points
        }));
    let branch_entry_points = UInt64Array::from_iter_values(
        summaries
            .iter()
            .map(|summary| summary.metadata.discovery_facets.branch_entry_points),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("project_path", DataType::Utf8, false),
        Field::new("project_identity", DataType::Utf8, false),
        Field::new("generation_uuid", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("languages", DataType::Utf8, false),
        Field::new("subjects", DataType::Utf8, false),
        Field::new("ontologies", DataType::Utf8, false),
        Field::new("temporal_label", DataType::Utf8, false),
        Field::new("text_entry_points", DataType::UInt64, false),
        Field::new("source_entry_points", DataType::UInt64, false),
        Field::new("entity_entry_points", DataType::UInt64, false),
        Field::new("relationship_entry_points", DataType::UInt64, false),
        Field::new("story_document_entry_points", DataType::UInt64, false),
        Field::new("event_location_entry_points", DataType::UInt64, false),
        Field::new("ontology_type_entry_points", DataType::UInt64, false),
        Field::new("linguistic_property_entry_points", DataType::UInt64, false),
        Field::new("traversal_query_entry_points", DataType::UInt64, false),
        Field::new("branch_entry_points", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(project_paths) as ArrayRef,
            Arc::new(project_identity),
            Arc::new(generation_uuid),
            Arc::new(titles),
            Arc::new(descriptions),
            Arc::new(languages),
            Arc::new(subjects),
            Arc::new(ontologies),
            Arc::new(temporal_labels),
            Arc::new(text_entry_points),
            Arc::new(source_entry_points),
            Arc::new(entity_entry_points),
            Arc::new(relationship_entry_points),
            Arc::new(story_document_entry_points),
            Arc::new(event_location_entry_points),
            Arc::new(ontology_type_entry_points),
            Arc::new(linguistic_property_entry_points),
            Arc::new(traversal_query_entry_points),
            Arc::new(branch_entry_points),
        ],
    )
    .map_err(|error| GfError::Execution(error.to_string()))?;
    Ok(ExecutionResult {
        schema,
        batches: vec![batch],
        stats: ExecutionStats {
            rows_produced: u64::try_from(row_count).unwrap_or(u64::MAX),
            execution_time_ms: 0,
        },
        side_effects: None,
        mutation_receipt: None,
    })
}

fn research_metadata_generation_uuid(
    operation_uuid: Uuid,
    actor_uuid: Option<Uuid>,
    participants: &[ProjectParticipant],
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-research-metadata-generation/1");
    hasher.update(operation_uuid.as_bytes());
    if let Some(actor_uuid) = actor_uuid {
        hasher.update([1]);
        hasher.update(actor_uuid.as_bytes());
    } else {
        hasher.update([0]);
    }
    for participant in participants {
        hasher.update(participant.capability_id.as_bytes());
        hasher.update([0]);
        hasher.update(participant.record_family_id.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&participant.bytes));
    }
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn snapshot_to_participant(
    snapshot: graphforge_storage::ProjectParticipantSnapshot,
) -> Result<ProjectParticipant, GfError> {
    let encoding = match snapshot.encoding.as_str() {
        "parquet" => graphforge_storage::ProjectParticipantEncoding::Parquet,
        "arrow" => graphforge_storage::ProjectParticipantEncoding::Arrow,
        "json" => graphforge_storage::ProjectParticipantEncoding::Json,
        _ => {
            return Err(GfError::Validation(
                "committed participant has unsupported encoding".into(),
            ));
        }
    };
    Ok(ProjectParticipant {
        capability_id: snapshot.capability_id,
        capability_version: snapshot.capability_version,
        record_family_id: snapshot.record_family_id,
        record_version: snapshot.record_version,
        encoding,
        schema_fingerprint: snapshot.schema_fingerprint,
        row_count: snapshot.row_count,
        bytes: snapshot.bytes,
    })
}
