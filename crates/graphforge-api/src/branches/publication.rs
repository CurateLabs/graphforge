//! Intent replay, optimistic CURRENT preconditions and post-publication reconciliation.
use crate::{CancellationToken, GfError, GraphForge};
use graphforge_core::ProjectErrorCode;
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperation, ResearchOperationReceipt, ResearchRegistry,
    read_research_registry,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use uuid::Uuid;

pub(crate) struct Command {
    pub root: PathBuf,
    pub registry: ResearchRegistry,
    pub intent: [u8; 32],
    replay: Option<ResearchOperationReceipt>,
    operation_uuid: Uuid,
    expected: Uuid,
}
pub(crate) fn begin<T: Serialize>(
    owner: &GraphForge,
    operation_uuid: Uuid,
    expected: Uuid,
    request: &T,
    cancellation: &CancellationToken,
) -> Result<Command, GfError> {
    owner.graph_visibility.health.check()?;
    cancellation.checkpoint()?;
    if owner.read_only {
        return Err(GfError::Project {
            code: ProjectErrorCode::ReadOnlyView,
            message: "read-only research cannot publish Branch state".into(),
        });
    }
    if operation_uuid.is_nil() || expected.is_nil() {
        return Err(GfError::Validation(
            "Branch operation and CURRENT identities must be non-nil".into(),
        ));
    }
    let bytes = serde_json::to_vec(request)
        .map_err(|_| GfError::Validation("invalid Branch request".into()))?;
    if bytes.len() > 1024 * 1024 {
        return Err(GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            message: "Branch request byte limit exceeded".into(),
        });
    }
    let mut digest = Sha256::new();
    digest.update(b"graphforge-branch-intent/1");
    digest.update(bytes);
    let intent = digest.finalize().into();
    let root = owner.resolved_generation.container_root().to_path_buf();
    let current = graphforge_storage::resolve_project_generation(&root)?;
    let registry = read_research_registry(&current)?;
    let replay = registry.receipts.get(&operation_uuid).cloned();
    if let Some(receipt) = &replay {
        if receipt.intent_sha256 != Some(intent) {
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "Branch operation identity has conflicting request content".into(),
            });
        }
    } else if current.generation_uuid() != expected {
        return Err(GfError::Project {
            code: ProjectErrorCode::WriteConflict,
            message: "Branch publication CURRENT precondition changed".into(),
        });
    }
    Ok(Command {
        root,
        registry,
        intent,
        replay,
        operation_uuid,
        expected,
    })
}
impl Command {
    pub(crate) fn replay(
        &self,
        owner: &mut GraphForge,
    ) -> Result<Option<ResearchOperationReceipt>, GfError> {
        let Some(receipt) = &self.replay else {
            return Ok(None);
        };
        let outcome = Ok(receipt.clone());
        if let Err(error) =
            owner.refresh_research_authority(&self.root, &outcome, None, self.expected, true)
        {
            owner.graph_visibility.health.fail(&error);
            return Err(error);
        }
        Ok(Some(receipt.clone()))
    }

    pub(crate) fn publish(
        self,
        owner: &mut GraphForge,
        mutation: ResearchMutation,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        let operation = ResearchOperation {
            operation_uuid: self.operation_uuid,
            expected_generation_uuid: self.expected,
            mutation,
        };
        let outcome = graphforge_storage::research_versions::publish_research_operation_with_mode(
            &self.root,
            &operation,
            cancellation.flag(),
            owner.lifecycle_mode,
        );
        if let Err(error) =
            owner.refresh_research_authority(&self.root, &outcome, None, self.expected, true)
        {
            owner.graph_visibility.health.fail(&error);
            return Err(outcome.err().unwrap_or(error));
        }
        outcome
    }
}
