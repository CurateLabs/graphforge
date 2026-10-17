//! Same-Branch fetch inputs; discovery verification precedes native publication.
use crate::{DiscoveryResearchVersionRequest, ResearchOperationReceipt};
pub use graphforge_storage::research_versions::ResearchRemoteFetchBinding;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Verify one advertised remote Branch head without replacing local content or head.
/// This request contains no provider credentials and performs no network access itself.
pub struct FetchResearchBranchRequest<'a> {
    /// Caller-owned durable native operation identity.
    pub operation_uuid: Uuid,
    /// Owning Project CURRENT before a new fetch.
    pub expected_generation_uuid: Uuid,
    /// Existing local Branch whose exact creation identity must match the remote.
    pub branch_uuid: Uuid,
    /// Canonical HTTPS repository URL whose identity must match discovery.
    pub repository_url: &'a str,
    /// Full native research-package verification; the Version must be its Branch head.
    pub discovery: DiscoveryResearchVersionRequest<'a>,
}

/// Durable fetch control result; existing native receipts own exact replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FetchedResearchBranch {
    /// Exact source commitment to supply to an explicit remote upstream preview.
    pub binding: ResearchRemoteFetchBinding,
    /// Existing native operation receipt; fetch never manufactures a Branch head.
    pub receipt: ResearchOperationReceipt,
}

mod admission;

impl crate::GraphForge {
    /// Admit a verified remote head of the same Branch without replacing local state.
    /// Exact replay revalidates source bytes and returns its original native receipt.
    pub fn fetch_research_branch(
        &mut self,
        request: &FetchResearchBranchRequest<'_>,
        cancel: &crate::CancellationToken,
    ) -> Result<FetchedResearchBranch, crate::GfError> {
        use graphforge_storage::research_versions::{
            read_research_registry, stage_remote_branch_fetch,
        };
        crate::research_claims::authority::require_owner(self)?;
        cancel.checkpoint()?;
        let current = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        let root = current.container_root().to_path_buf();
        let registry = read_research_registry(&current)?;
        if request.operation_uuid.is_nil() || request.expected_generation_uuid.is_nil() {
            return Err(invalid(
                "fetch operation and generation identities must be non-nil",
            ));
        }
        if !registry.receipts.contains_key(&request.operation_uuid)
            && current.generation_uuid() != request.expected_generation_uuid
        {
            return Err(crate::GfError::Project {
                code: graphforge_core::ProjectErrorCode::WriteConflict,
                message: "fetch CURRENT precondition changed".into(),
            });
        }
        let scratch = tempfile::Builder::new()
            .prefix(".graphforge-remote-fetch-")
            .tempdir_in(&root)
            .map_err(|error| crate::GfError::Storage(error.to_string()))?;
        let (source, fetch) = admission::prepare(request, scratch.path(), cancel)?;
        fetch.validate_identity(
            &registry,
            crate::research_claims::authority::project_uuid(&current)?,
        )?;
        if registry.receipts.contains_key(&request.operation_uuid) {
            let receipt = fetch.authenticate_receipt(&registry)?;
            self.refresh_research_authority(
                &root,
                &Ok(receipt.clone()),
                None,
                request.expected_generation_uuid,
                true,
            )?;
            return Ok(FetchedResearchBranch {
                binding: fetch.binding,
                receipt,
            });
        }
        let lease = stage_remote_branch_fetch(&source.generation_for_read()?, &root, &fetch)?;
        cancel.checkpoint()?;
        let operation = fetch.operation();
        let outcome = graphforge_storage::research_versions::publish_research_operation_with_mode(
            &root,
            &operation,
            cancel.flag(),
            self.lifecycle_mode,
        );
        if let Err(error) = self.refresh_research_authority(
            &root,
            &outcome,
            None,
            request.expected_generation_uuid,
            true,
        ) {
            self.graph_visibility.health.fail(&error);
            return Err(outcome.err().unwrap_or(error));
        }
        drop(lease);
        let receipt = outcome?;
        Ok(FetchedResearchBranch {
            binding: fetch.binding,
            receipt,
        })
    }
}

fn invalid(message: &str) -> crate::GfError {
    crate::GfError::Validation(message.into())
}
