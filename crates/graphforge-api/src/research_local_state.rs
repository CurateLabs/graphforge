//! Tags, the current Branch and the publication a Hub confirmed: local tracking
//! state published through the ordinary research publication (ADR 0061).
//!
//! None of it is shared history. A tag travels in the lineage only when a publish
//! carries it; the current Branch and the recorded upstream never leave the Project.
use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_hub_publish::PublishReceipt;
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperation, ResearchOperationReceipt, ResearchRemoteBranch,
};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use uuid::Uuid;

use crate::{CancellationToken, GraphForge};

/// Create or delete one tag on an immutable Version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetResearchTagRequest {
    /// Stable operation identity. A rerun with the same request replays the original
    /// receipt; the same identity with another request is a conflict.
    pub operation_uuid: Uuid,
    /// Previewed Project CURRENT; never silently refreshed for a mutation.
    pub expected_generation_uuid: Uuid,
    /// Tag name; a valid discovery ref.
    pub name: String,
    /// Version the tag names, or `None` to delete the tag.
    pub version_uuid: Option<Uuid>,
}

/// Local tracking of a publication a Hub confirmed: the repository and ref a
/// Branch now tracks, and the head the Hub's receipt says it serves.
///
/// This is bookkeeping about a remote, not shared history and not authentication
/// of the Hub: the receipt is the Hub's own statement, checked here only against
/// the Branch it claims to publish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordResearchPublicationRequest {
    /// Stable operation identity for exact replay.
    pub operation_uuid: Uuid,
    /// Previewed Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Local Branch that was published; the ref is its label.
    pub branch_uuid: Uuid,
    /// The Version the publication named as the ref's head: the Branch head at
    /// that time, which is the Branch head now or one of its ancestors.
    pub published_version_uuid: Uuid,
    /// Canonical HTTPS repository URL the receipt was issued for.
    pub repository_url: String,
    /// The Hub's receipt for the publication.
    pub receipt: PublishReceipt,
}

/// Select the Branch the porcelain verbs act on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectResearchBranchRequest {
    /// Stable operation identity. A rerun with the same request replays the
    /// original receipt; the same identity with another request is a conflict.
    pub operation_uuid: Uuid,
    /// Previewed Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Existing Branch with a head.
    pub branch_uuid: Uuid,
}

fn intent(kind: &str, parts: &[&[u8]]) -> [u8; 32] {
    let mut digest = sha2::Sha256::new();
    digest.update(b"graphforge-local-state-intent/1");
    digest.update(kind.as_bytes());
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    digest.finalize().into()
}

fn conflict() -> GfError {
    GfError::Project {
        code: ProjectErrorCode::TransactionConflict,
        message: "operation identity has conflicting request content".into(),
    }
}

impl GraphForge {
    /// The receipt of an earlier run of `operation_uuid` when `intent` is exactly
    /// what it recorded; a conflict when the identity was used for anything else.
    /// The request's CURRENT precondition is not part of the intent, so a rerun
    /// after the operation moved CURRENT is still a replay.
    fn replay_local_state(
        &mut self,
        operation_uuid: Uuid,
        expected_generation_uuid: Uuid,
        intent: [u8; 32],
    ) -> Result<Option<ResearchOperationReceipt>, GfError> {
        let Some(receipt) = self
            .research_version_retention()?
            .receipts
            .get(&operation_uuid)
            .cloned()
        else {
            return Ok(None);
        };
        if receipt.intent_sha256 != Some(intent) {
            return Err(conflict());
        }
        let root = self.resolved_generation.container_root().to_path_buf();
        self.refresh_research_authority(
            &root,
            &Ok(receipt.clone()),
            None,
            expected_generation_uuid,
            true,
        )?;
        Ok(Some(receipt))
    }

    /// Create a tag on a Version, or delete one. A tag never moves: creating an
    /// existing tag with another target is refused with `GF_RESEARCH_TAG_EXISTS`.
    pub fn set_research_tag(
        &mut self,
        request: &SetResearchTagRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        graphforge_discovery::check_ref_name(&request.name)
            .map_err(|_| GfError::Validation("a tag name must be a valid ref name".into()))?;
        let target = request.version_uuid.map(|uuid| uuid.to_string());
        let intent = intent(
            "set-tag",
            &[
                request.name.as_bytes(),
                target.as_deref().unwrap_or("").as_bytes(),
                &[u8::from(target.is_some())],
            ],
        );
        if let Some(receipt) = self.replay_local_state(
            request.operation_uuid,
            request.expected_generation_uuid,
            intent,
        )? {
            return Ok(receipt);
        }
        self.commit_research_version_operation(
            ResearchOperation {
                operation_uuid: request.operation_uuid,
                expected_generation_uuid: request.expected_generation_uuid,
                mutation: ResearchMutation::SetTag {
                    name: request.name.clone(),
                    version_uuid: request.version_uuid,
                    intent_sha256: intent,
                },
            },
            cancellation,
        )
    }

    /// Record a publication the Hub's receipt confirmed as the remote citation of
    /// its Branch, the record clone and fetch maintain. The receipt must be for the
    /// named repository and carry the Branch's ref, and the published Version must
    /// be the Branch head or an ancestor of it. The citation is built from native
    /// authority; only the repository and the receipt come from the caller.
    pub fn record_research_publication(
        &mut self,
        request: &RecordResearchPublicationRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        let invalid = |message: &str| GfError::Validation(message.into());
        let registry = self.research_version_retention()?;
        let branch = registry
            .branches
            .get(&request.branch_uuid)
            .ok_or_else(|| invalid("the published Branch does not exist"))?;
        let identity = registry
            .identities
            .get(&request.published_version_uuid)
            .ok_or_else(|| invalid("the published head is not a known Version"))?;
        graphforge_hub_publish::check_format(&request.receipt.format)
            .map_err(|_| invalid("the receipt is not a Hub publish receipt"))?;
        let url = url::Url::parse(&request.repository_url)
            .map_err(|_| invalid("the repository URL is invalid"))?;
        if url.path() != format!("/{}", request.receipt.repository.canonical_name()) {
            return Err(invalid("the receipt is for another repository"));
        }
        if !request.receipt.refs.contains(&branch.label) {
            return Err(invalid("the receipt does not name the Branch's ref"));
        }
        self.publish_research_operation(
            &ResearchOperation {
                operation_uuid: request.operation_uuid,
                expected_generation_uuid: request.expected_generation_uuid,
                mutation: ResearchMutation::RecordPublishedBranch {
                    reference: ResearchRemoteBranch {
                        repository_url: request.repository_url.clone(),
                        project_uuid: branch.project_uuid,
                        ref_name: branch.label.clone(),
                        branch_uuid: request.branch_uuid,
                        head_version_uuid: request.published_version_uuid,
                        identity_sha256: *identity,
                    },
                },
            },
            cancellation,
        )
    }

    /// Digest of what a publication of this Project is made of: every participant
    /// except the research registry, and the registry without the tracking state a
    /// publication itself writes (remote citations and operation receipts).
    ///
    /// A publication's intent binds this, so recording a confirmed publication does
    /// not turn a rerun of that publication into a conflict, while any change to the
    /// Project, a Version, a tag or a Branch still does.
    pub fn publication_content_identity(&self) -> Result<[u8; 32], GfError> {
        let generation = self.generation_for_read()?;
        let mut registry =
            graphforge_storage::research_versions::read_research_registry(&generation)?;
        registry.remote_branches.clear();
        registry.receipts.clear();
        let mut digest = sha2::Sha256::new();
        digest.update(b"graphforge-publication-content/1");
        for participant in generation.participant_descriptors()? {
            if participant.capability_id
                == graphforge_storage::research_versions::RESEARCH_CAPABILITY
            {
                continue;
            }
            for text in [
                &participant.capability_id,
                &participant.record_family_id,
                &participant.encoding,
            ] {
                digest.update((text.len() as u64).to_be_bytes());
                digest.update(text.as_bytes());
            }
            digest.update(participant.record_version.to_be_bytes());
            digest.update(participant.schema_fingerprint);
            digest.update(participant.row_count.to_be_bytes());
            digest.update(participant.byte_length.to_be_bytes());
            digest.update(participant.content_sha256);
        }
        digest.update(
            serde_json::to_vec(&registry)
                .map_err(|_| GfError::Validation("research registry cannot be encoded".into()))?,
        );
        Ok(digest.finalize().into())
    }

    /// Select the Branch the porcelain verbs act on.
    pub fn select_research_branch(
        &mut self,
        request: &SelectResearchBranchRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        let intent = intent("select-branch", &[request.branch_uuid.as_bytes()]);
        if let Some(receipt) = self.replay_local_state(
            request.operation_uuid,
            request.expected_generation_uuid,
            intent,
        )? {
            return Ok(receipt);
        }
        self.commit_research_version_operation(
            ResearchOperation {
                operation_uuid: request.operation_uuid,
                expected_generation_uuid: request.expected_generation_uuid,
                mutation: ResearchMutation::SelectBranch {
                    branch_uuid: request.branch_uuid,
                    intent_sha256: intent,
                },
            },
            cancellation,
        )
    }
}
