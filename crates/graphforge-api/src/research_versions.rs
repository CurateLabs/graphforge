//! Immutable research Versions, owner-derived capture and exact historical views.
mod view;
pub use view::ResearchVersionView;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::{ApiErrorCode, GfError};
use graphforge_knowledge::ArtifactAvailability;
use graphforge_storage::research_versions::{
    RegisterResearchVersion, ResearchEvidenceReference, ResearchMutation, ResearchOperation,
    ResearchOperationReceipt, ResearchRegistry, ResearchVersionRecord, read_research_registry,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{CancellationToken, ExecutionResult, GraphForge};

/// Prepare an exact complete-Project capture without publishing it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareResearchVersionRequest {
    /// Stable operation identity; retain the prepared request for retries.
    pub operation_uuid: Uuid,
    /// Fresh immutable research identity.
    pub version_uuid: Uuid,
    /// Explicit owner context; not a storage generation or checkpoint name.
    pub context_uuid: Uuid,
    /// Immutable citation label.
    pub label: Option<String>,
    /// Immutable analyst description.
    pub description: Option<String>,
    /// UTC creation time in microseconds.
    pub created_at: i64,
    /// Explicit required retained Versions, distinct from genealogy.
    pub required_versions: BTreeSet<Uuid>,
}

impl GraphForge {
    /// Freeze an exact complete-Project capture request with owner-derived evidence.
    /// Keep this returned request unchanged for durable retries.
    pub fn prepare_research_version(
        &self,
        request: PrepareResearchVersionRequest,
    ) -> Result<ResearchOperation, GfError> {
        let generation = self.generation_for_read()?;
        crate::checkpoints::validate_research_source(&generation)?;
        let evidence = complete_evidence(&generation)?;
        Ok(ResearchOperation {
            operation_uuid: request.operation_uuid,
            expected_generation_uuid: generation.generation_uuid(),
            mutation: ResearchMutation::Register(RegisterResearchVersion {
                version_uuid: request.version_uuid,
                context_uuid: request.context_uuid,
                source_generation_uuid: generation.generation_uuid(),
                selection: None,
                source_version: None,
                required_versions: request.required_versions,
                label: request.label,
                description: request.description,
                created_at: request.created_at,
                evidence,
            }),
        })
    }

    /// Inspect immutable citation metadata without following current context heads.
    pub fn research_version(&self, version_uuid: Uuid) -> Result<ResearchVersionRecord, GfError> {
        registry(self)?
            .versions
            .remove(&version_uuid)
            .ok_or_else(not_retained)
    }

    /// Return the authenticated current retention graph and durable receipts.
    /// These are current lifecycle metadata, separate from frozen Version content.
    pub fn research_version_retention(&self) -> Result<ResearchRegistry, GfError> {
        registry(self)
    }

    /// List immutable Version identities and citation labels as Arrow.
    pub fn list_research_versions(&self) -> Result<ExecutionResult, GfError> {
        let state = registry(self)?;
        let versions: Vec<_> = state.versions.values().collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("version_uuid", DataType::Utf8, false),
            Field::new("context_uuid", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("complete", DataType::Boolean, false),
            Field::new("source_version", DataType::Utf8, true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(
                versions.iter().map(|v| v.version_uuid.to_string()),
            )),
            Arc::new(StringArray::from_iter_values(
                versions.iter().map(|v| v.context_uuid.to_string()),
            )),
            Arc::new(
                versions
                    .iter()
                    .map(|v| v.label.as_deref())
                    .collect::<StringArray>(),
            ),
            Arc::new(
                versions
                    .iter()
                    .map(|v| v.description.as_deref())
                    .collect::<StringArray>(),
            ),
            Arc::new(BooleanArray::from(
                versions
                    .iter()
                    .map(|v| v.content.source_version.is_none())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                versions
                    .iter()
                    .map(|v| v.content.source_version.map(|id| id.to_string()))
                    .collect::<StringArray>(),
            ),
        ];
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|error| GfError::Validation(error.to_string()))?;
        Ok(ExecutionResult {
            schema: batch.schema(),
            batches: vec![batch],
            stats: crate::ExecutionStats::default(),
            side_effects: None,
            mutation_receipt: None,
        })
    }

    /// Publish one exact prepared capture, retention operation or explicit restoration.
    /// Exact replay returns its historical receipt without reinstalling historical state.
    // Mutation commands own their prepared request; callers explicitly retain retry copies.
    #[allow(clippy::needless_pass_by_value)]
    pub fn commit_research_version_operation(
        &mut self,
        operation: ResearchOperation,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        if self.read_only {
            return Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ReadOnlyView,
                message: "historical views cannot mutate research".into(),
            });
        }
        cancellation.checkpoint()?;
        let root = self.resolved_generation.container_root().to_path_buf();
        let state = registry(self)?;
        let replacement = if state.receipts.contains_key(&operation.operation_uuid) {
            None
        } else {
            match &operation.mutation {
                ResearchMutation::Register(spec) => {
                    if spec.selection.is_some() || spec.source_version.is_some() {
                        return Err(GfError::Validation(
                            "complete Project capture cannot use raw participant selection".into(),
                        ));
                    }
                    let source = graphforge_storage::resolve_generation_by_uuid(
                        &root,
                        spec.source_generation_uuid,
                    )?;
                    crate::checkpoints::validate_research_source(&source)?;
                    if complete_evidence(&source)? != spec.evidence {
                        return Err(GfError::Validation(
                            "prepared capture omits or changes owner-derived Artifact evidence"
                                .into(),
                        ));
                    }
                    None
                }
                ResearchMutation::RegisterGraphProjection { .. } => {
                    return Err(GfError::Validation("raw graph projection requires domain-owner closure; use a complete Project capture".into()));
                }
                ResearchMutation::RestoreProject { source_version, .. } => {
                    let source = self.research_version(*source_version)?;
                    if source.content.source_version.is_some() {
                        return Err(GfError::Validation(
                            "Project restore requires complete research".into(),
                        ));
                    }
                    Some(view::materialize(self, &source)?)
                }
                _ => None,
            }
        };
        let outcome = graphforge_storage::research_versions::publish_research_operation_with_mode(
            &root,
            &operation,
            cancellation.flag(),
            self.lifecycle_mode,
        );
        // Publication may report an error after CURRENT linearization. Reconcile
        // the live facade even then; keep the original truthful result for callers.
        if let Err(refresh_error) = self.refresh_research_authority(
            &root,
            &outcome,
            replacement,
            operation.expected_generation_uuid,
            !matches!(operation.mutation, ResearchMutation::RestoreProject { .. }),
        ) {
            self.graph_visibility.health.fail(&refresh_error);
            return Err(outcome.err().unwrap_or(refresh_error));
        }
        outcome
    }

    fn refresh_research_authority(
        &mut self,
        root: &std::path::Path,
        outcome: &Result<ResearchOperationReceipt, GfError>,
        replacement: Option<GraphForge>,
        expected_parent: Uuid,
        metadata_only: bool,
    ) -> Result<(), GfError> {
        let resolved = graphforge_storage::resolve_project_generation(root)?;
        let current = resolved.generation_uuid();
        let cached = *self
            .current_generation_uuid
            .lock()
            .expect("generation lock poisoned");
        if current == cached {
            return Ok(());
        }
        // A concurrently advanced CURRENT must be opened as its own complete
        // authority, never attached to a graph prepared from another generation.
        let exact_prepared = outcome
            .as_ref()
            .ok()
            .is_some_and(|receipt| receipt.generation_uuid == current);
        if exact_prepared && metadata_only && cached == expected_parent {
            // Metadata-only publication leaves the already-bound graph untouched.
            self.resolved_generation = resolved;
            *self
                .current_generation_uuid
                .lock()
                .expect("generation lock poisoned") = current;
            return Ok(());
        }
        let mut prepared = if exact_prepared { replacement } else { None }.map_or_else(
            || {
                GraphForge::open_resolved_with_options(
                    root.to_path_buf(),
                    resolved.clone(),
                    false,
                    self.write_options.clone(),
                    self.resource_policy.clone(),
                    graphforge_storage::ProjectOpenRecoveryEvidence::clean_open(current),
                )
            },
            Ok,
        )?;
        prepared.lifecycle_mode = self.lifecycle_mode;
        prepared.resolved_generation = resolved;
        prepared.read_only = false;
        prepared.path.clone_from(&self.path);
        prepared.tempdir.clone_from(&self.tempdir);
        prepared.clock =
            std::sync::Mutex::new(Arc::clone(&self.clock.lock().expect("clock lock poisoned")));
        prepared.procedures = Arc::clone(&self.procedures);
        prepared.provider_refresh_runtimes = Arc::clone(&self.provider_refresh_runtimes);
        prepared.provider_find_runtimes = Arc::clone(&self.provider_find_runtimes);
        *prepared
            .current_generation_uuid
            .lock()
            .expect("generation lock poisoned") = current;
        *self = prepared;
        Ok(())
    }

    /// Open real read-only native execution over exact retained content.
    pub fn open_research_version(
        &self,
        version_uuid: Uuid,
    ) -> Result<ResearchVersionView, GfError> {
        let version = self.research_version(version_uuid)?;
        let graph = view::materialize(self, &version)?;
        Ok(ResearchVersionView { version, graph })
    }
}

fn registry(graph: &GraphForge) -> Result<ResearchRegistry, GfError> {
    read_research_registry(&graph.generation_for_read()?)
}

fn not_retained() -> GfError {
    GfError::Api {
        code: ApiErrorCode::ResultNotRetained,
        message: "research Version is not retained".into(),
    }
}

fn complete_evidence(
    generation: &graphforge_storage::ResolvedProjectGeneration,
) -> Result<Vec<ResearchEvidenceReference>, GfError> {
    if generation.capability("knowledge")?.is_none() {
        return Ok(Vec::new());
    }
    let ledger = crate::knowledge::read_artifact_ledger(generation)?;
    ledger
        .artifacts
        .into_iter()
        .map(|artifact| {
            Ok(match artifact.availability {
                ArtifactAvailability::LocalVerified => ResearchEvidenceReference::Local {
                    artifact_uuid: artifact.artifact_uuid,
                    sha256: artifact.content_sha256.ok_or_else(|| {
                        GfError::Validation("local Artifact has no digest".into())
                    })?,
                    byte_length: artifact.content_length.ok_or_else(|| {
                        GfError::Validation("local Artifact has no length".into())
                    })?,
                },
                ArtifactAvailability::ExternalOnly => ResearchEvidenceReference::ExternalOnly {
                    artifact_uuid: artifact.artifact_uuid,
                    fingerprint: artifact.external_fingerprint,
                },
                ArtifactAvailability::MissingLocal | ArtifactAvailability::Unverifiable => {
                    ResearchEvidenceReference::Unverifiable {
                        artifact_uuid: artifact.artifact_uuid,
                    }
                }
            })
        })
        .collect()
}
