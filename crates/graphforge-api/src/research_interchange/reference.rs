//! Resolve a live Branch or immutable Version without following a host URL.
use crate::{CancellationToken, GfError, GraphForge};
use graphforge_storage::research_versions::{ResearchBranchRecord, ResearchVersionRecord};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Explicit citation target; a Branch follows its head only when resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchReferenceTarget {
    /// Resolve the current Version of this native Branch.
    Branch {
        /// Stable Branch context identity.
        branch_uuid: Uuid,
    },
    /// Resolve exactly this immutable Version, regardless of later head changes.
    Version {
        /// Immutable research identity, not a storage or package identity.
        version_uuid: Uuid,
    },
}

/// Native, host-independent citation metadata pinned to one Project generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchReference {
    /// Closed citation contract version.
    pub contract_version: u32,
    /// Original live or immutable target selected by the caller.
    pub target: ResearchReferenceTarget,
    /// Project authority in which this reference was resolved.
    pub project_uuid: Uuid,
    /// Exact owning CURRENT used to resolve a live head.
    pub resolved_generation_uuid: Uuid,
    /// Frozen research identity and content commitments.
    pub version: ResearchVersionRecord,
    /// Original immutable identity commitment from the native registry.
    pub identity_sha256: [u8; 32],
    /// Selected Branch followed by its immediate ancestors. Creator identities,
    /// original bases, and origins are provenance, not remote authorization.
    pub genealogy: Vec<ResearchBranchRecord>,
}

impl GraphForge {
    /// Resolve a consumer-neutral live Branch or immutable Version reference.
    /// Released historical content is explicitly unavailable; no fallback to a
    /// newer Branch head or a similarly named object is permitted.
    pub fn research_reference(
        &self,
        target: &ResearchReferenceTarget,
        cancellation: &CancellationToken,
    ) -> Result<ResearchReference, GfError> {
        cancellation.checkpoint()?;
        let current = self.generation_for_read()?;
        let registry = graphforge_storage::research_versions::read_research_registry(&current)?;
        let version_uuid = match target {
            ResearchReferenceTarget::Branch { branch_uuid } => {
                if !registry.branches.contains_key(branch_uuid) {
                    return Err(unavailable());
                }
                *registry.heads.get(branch_uuid).ok_or_else(unavailable)?
            }
            ResearchReferenceTarget::Version { version_uuid } => *version_uuid,
        };
        let version = registry
            .versions
            .get(&version_uuid)
            .ok_or_else(unavailable)?;
        let identity_sha256 = *registry
            .identities
            .get(&version_uuid)
            .ok_or_else(unavailable)?;
        let mut genealogy = Vec::new();
        let mut branch = registry.historical_branch(version.context_uuid);
        let mut seen = std::collections::BTreeSet::new();
        while let Some(record) = branch {
            cancellation.checkpoint()?;
            if !seen.insert(record.branch_uuid) {
                return Err(GfError::Validation("cyclic research genealogy".into()));
            }
            genealogy.push(record.clone());
            branch = record
                .parent_branch_uuid
                .map(|parent| registry.historical_branch(parent).ok_or_else(unavailable))
                .transpose()?;
        }
        Ok(ResearchReference {
            contract_version: 1,
            target: target.clone(),
            project_uuid: crate::research_claims::authority::project_uuid(&current)?,
            resolved_generation_uuid: current.generation_uuid(),
            version: version.clone(),
            identity_sha256,
            genealogy,
        })
    }
}

fn unavailable() -> GfError {
    GfError::Api {
        code: graphforge_core::ApiErrorCode::ResultNotRetained,
        message: "research reference content or Branch is unavailable; choose a retained Version"
            .into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BranchSource, CreateResearchBranchRequest, ExecuteResearchBranchRequest};

    #[test]
    fn live_and_immutable_references_preserve_base_origin_and_authorship() {
        let mut graph = GraphForge::new(None).unwrap();
        let cancellation = CancellationToken::new();
        graph.execute("CREATE (:Item {x:0})").unwrap();
        let branch_uuid = Uuid::now_v7();
        let base = Uuid::now_v7();
        let origin = Uuid::now_v7();
        let author = Uuid::now_v7();
        graph
            .create_research_branch(
                &CreateResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: graph
                        .generation_for_read()
                        .unwrap()
                        .generation_uuid(),
                    branch_uuid,
                    version_uuid: base,
                    source: BranchSource::Current {
                        origin_version_uuid: origin,
                        context_uuid: Uuid::now_v7(),
                    },
                    creator_uuid: author,
                    created_at: 1,
                    label: "Citable study".into(),
                },
                &cancellation,
            )
            .unwrap();
        let before = graph.generation_for_read().unwrap().generation_uuid();
        let frozen_target = ResearchReferenceTarget::Version { version_uuid: base };
        let frozen = graph
            .research_reference(&frozen_target, &cancellation)
            .unwrap();
        assert_eq!(
            graph.generation_for_read().unwrap().generation_uuid(),
            before
        );
        let next = Uuid::now_v7();
        graph
            .execute_research_branch(
                &ExecuteResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: before,
                    branch_uuid,
                    version_uuid: next,
                    created_at: 2,
                    query: "MATCH (n:Item) SET n.x=1".into(),
                },
                &cancellation,
            )
            .unwrap();
        let live = graph
            .research_reference(
                &ResearchReferenceTarget::Branch { branch_uuid },
                &cancellation,
            )
            .unwrap();
        let still_frozen = graph
            .research_reference(&frozen_target, &cancellation)
            .unwrap();
        assert_eq!(live.version.version_uuid, next);
        assert_eq!(still_frozen.version, frozen.version);
        assert_eq!(still_frozen.identity_sha256, frozen.identity_sha256);
        assert_eq!(live.genealogy, frozen.genealogy);
        assert_eq!(live.genealogy[0].base_version_uuid, base);
        assert_eq!(live.genealogy[0].origin_version_uuid, origin);
        assert_eq!(live.genealogy[0].creator_uuid, author);
        assert_eq!(live.project_uuid, frozen.project_uuid);
        // A released origin is a citation in genealogy, never a different live Version.
        assert!(
            graph
                .research_reference(
                    &ResearchReferenceTarget::Version {
                        version_uuid: origin
                    },
                    &cancellation
                )
                .is_err()
        );
        cancellation.cancel();
        assert!(
            graph
                .research_reference(&frozen_target, &cancellation)
                .is_err()
        );
    }
}
