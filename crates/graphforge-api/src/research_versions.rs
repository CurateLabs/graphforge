//! Immutable research Versions, owner-derived capture and exact historical views.
mod view;
pub use view::ResearchVersionView;
pub(crate) use view::materialize as materialize_version;
pub(crate) use view::materialize_writable as materialize_version_writable;

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
    /// Credited author of the new Version; recorded, not authenticated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<graphforge_storage::research_versions::ResearchSignature>,
    /// Credited committer of the new Version; recorded, not authenticated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committer: Option<graphforge_storage::research_versions::ResearchSignature>,
}

impl GraphForge {
    /// Freeze an exact complete-Project capture request with owner-derived evidence.
    /// Keep this returned request unchanged for durable retries.
    pub fn prepare_research_version(
        &self,
        request: PrepareResearchVersionRequest,
    ) -> Result<ResearchOperation, GfError> {
        for signature in request.author.iter().chain(&request.committer) {
            signature.validate()?;
        }
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
                author: request.author,
                committer: request.committer,
            }),
        })
    }

    /// Inspect immutable citation metadata without following current context heads.
    pub fn research_version(&self, version_uuid: Uuid) -> Result<ResearchVersionRecord, GfError> {
        registry(self)?
            .historical_version(version_uuid)
            .cloned()
            .ok_or_else(not_retained)
    }

    /// Continue one verified imported Branch in its original Project.
    /// Historical/archive readers never call this durable transition.
    pub fn continue_imported_research_branch(
        &mut self,
        expected: &graphforge_discovery::LineageBranch,
        lineage: &graphforge_discovery::ResearchLineage,
        repository_url: &str,
        imported_generation: Uuid,
    ) -> Result<ResearchOperationReceipt, GfError> {
        let branch_uuid = Uuid::parse_str(&expected.branch_uuid)
            .map_err(|_| GfError::Validation("invalid imported Branch".into()))?;
        let head = Uuid::parse_str(&expected.head_version_uuid)
            .map_err(|_| GfError::Validation("invalid imported Branch head".into()))?;
        let generation = self.generation_for_read()?;
        let project_uuid = crate::research_claims::authority::project_uuid(&generation)?;
        let state = read_research_registry(&generation)?;
        let branch = state
            .historical_branch(branch_uuid)
            .ok_or_else(not_retained)?;
        if expected.project_uuid != project_uuid.to_string()
            || expected.parent_branch_uuid != branch.parent_branch_uuid.map(|id| id.to_string())
            || expected.origin_version_uuid != branch.origin_version_uuid.to_string()
            || expected.base_version_uuid != branch.base_version_uuid.to_string()
            || expected.label != branch.label
            || expected.selection_sha256.0
                != format!(
                    "sha256:{}",
                    branch
                        .selection_sha256
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                )
        {
            return Err(GfError::Validation(
                "discovery Branch differs from imported genealogy".into(),
            ));
        }
        let mut hash = sha2::Sha256::new();
        use sha2::Digest as _;
        hash.update(b"graphforge-clone-branch-continuation/1");
        hash.update(branch_uuid.as_bytes());
        hash.update(head.as_bytes());
        lineage
            .validate(graphforge_discovery::DiscoveryLimits::default())
            .map_err(|e| GfError::Validation(e.to_string()))?;
        let repository = url::Url::parse(repository_url)
            .map_err(|_| GfError::Validation("invalid clone repository".into()))?;
        if repository.path() != format!("/{}", lineage.repository.canonical_name())
            || lineage.project_uuid != project_uuid.to_string()
            || !lineage.branches.contains(expected)
        {
            return Err(GfError::Validation(
                "clone repository does not bind native Branch".into(),
            ));
        }
        let remote_branches = lineage
            .branches
            .iter()
            .map(|reference| {
                let version = lineage
                    .version(&reference.head_version_uuid)
                    .ok_or_else(not_retained)?;
                let digest = version
                    .identity_digest
                    .0
                    .strip_prefix("sha256:")
                    .ok_or_else(not_retained)?;
                let mut identity_sha256 = [0_u8; 32];
                for (index, byte) in identity_sha256.iter_mut().enumerate() {
                    *byte = u8::from_str_radix(&digest[index * 2..index * 2 + 2], 16)
                        .map_err(|_| not_retained())?;
                }
                Ok(
                    graphforge_storage::research_versions::ResearchRemoteBranch {
                        repository_url: repository_url.into(),
                        project_uuid,
                        ref_name: reference.ref_name.clone(),
                        branch_uuid: Uuid::parse_str(&reference.branch_uuid)
                            .map_err(|_| not_retained())?,
                        head_version_uuid: Uuid::parse_str(&reference.head_version_uuid)
                            .map_err(|_| not_retained())?,
                        identity_sha256,
                    },
                )
            })
            .collect::<Result<Vec<_>, GfError>>()?;
        let operation = ResearchOperation {
            operation_uuid: graphforge_core::canonical::uuid_v8(hash.finalize().into()),
            expected_generation_uuid: imported_generation,
            mutation: ResearchMutation::ActivateImportedBranch {
                branch_uuid,
                head_version_uuid: head,
                project_uuid,
                remote_branches,
            },
        };
        let receipt =
            self.commit_research_version_operation(operation, &CancellationToken::new())?;
        if self.committed_generation_identity()?.generation_uuid != receipt.generation_uuid {
            return Err(GfError::Validation(
                "imported Branch activation receipt is not the current native generation".into(),
            ));
        }
        Ok(receipt)
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
        if matches!(
            &operation.mutation,
            ResearchMutation::SubmitProposal { .. }
                | ResearchMutation::ReviewProposal { .. }
                | ResearchMutation::ReleaseProposal { .. }
        ) {
            return Err(GfError::Validation(
                "Proposal publication requires the native review and selected-content owner".into(),
            ));
        }
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
                ResearchMutation::ActivateImportedBranch { project_uuid, .. } => {
                    if *project_uuid
                        != crate::research_claims::authority::project_uuid(
                            &self.generation_for_read()?,
                        )?
                    {
                        return Err(GfError::Validation(
                            "imported Branch crosses Project authority".into(),
                        ));
                    }
                    None
                }
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
                ResearchMutation::RegisterGraphProjection { .. }
                | ResearchMutation::PublishBranch { .. }
                | ResearchMutation::UpdateBranch { .. } => {
                    return Err(GfError::Validation("raw graph projection requires domain-owner closure; use a complete Project capture".into()));
                }
                ResearchMutation::RestoreProject { source_version, .. } => {
                    let source = self.research_version(*source_version)?;
                    if source.content.source_version.is_some() {
                        return Err(GfError::Validation(
                            "Project restore requires complete research".into(),
                        ));
                    }
                    Some(view::materialize_writable(
                        self,
                        &source,
                        self.resource_policy.clone(),
                    )?)
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

    pub(crate) fn refresh_research_authority(
        &mut self,
        root: &std::path::Path,
        outcome: &Result<ResearchOperationReceipt, GfError>,
        replacement: Option<GraphForge>,
        expected_parent: Uuid,
        metadata_only: bool,
    ) -> Result<(), GfError> {
        self.refresh_current_authority(
            root,
            outcome.as_ref().ok().map(|r| r.generation_uuid),
            replacement,
            expected_parent,
            metadata_only,
        )
    }

    pub(crate) fn refresh_current_authority(
        &mut self,
        root: &std::path::Path,
        published_generation: Option<Uuid>,
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
        let exact_prepared = published_generation == Some(current);
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
        prepared.path.clone_from(&self.path);
        prepared.tempdir.clone_from(&self.tempdir);
        prepared.clock =
            std::sync::Mutex::new(Arc::clone(&self.clock.lock().expect("clock lock poisoned")));
        prepared.procedures = Arc::clone(&self.procedures);
        #[cfg(feature = "search")]
        {
            prepared.provider_refresh_runtimes = Arc::clone(&self.provider_refresh_runtimes);
            prepared.provider_find_runtimes = Arc::clone(&self.provider_find_runtimes);
        }
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
        self.open_research_version_with_resources(version_uuid, self.resource_policy.clone())
    }

    pub(crate) fn open_research_version_with_resources(
        &self,
        version_uuid: Uuid,
        resource_policy: crate::NormalizedResourcePolicy,
    ) -> Result<ResearchVersionView, GfError> {
        let version = registry(self)?
            .versions
            .remove(&version_uuid)
            .ok_or_else(not_retained)?;
        let graph = view::materialize_with_resources(self, &version, true, resource_policy)?;
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

pub(crate) fn complete_evidence(
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
