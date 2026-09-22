//! Native Branch controls; graph-bearing state remains in authenticated participants.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Exact source for independent Branch creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BranchSource {
    /// Freeze the expected current Project without implicitly retaining whole history.
    Current {
        /// Fresh immutable genealogy identity for the source snapshot.
        origin_version_uuid: Uuid,
        /// Project research context, distinct from the new Branch.
        context_uuid: Uuid,
    },
    /// Exact frozen Slice capsule; source history must remain available.
    Slice {
        /// Canonical bounded Arrow IPC from `freeze_slice`.
        frozen_ipc: Vec<u8>,
    },
    /// Use an explicitly retained historical Version.
    Version {
        /// Exact retained immutable source.
        version_uuid: Uuid,
    },
    /// Freeze this Branch's exact current Version as the immediate parent.
    Branch {
        /// Existing parent Branch context.
        branch_uuid: Uuid,
    },
}

/// Create one independently evolving Branch in the same Project CURRENT.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateResearchBranchRequest {
    /// Stable operation identity for exact replay.
    pub operation_uuid: Uuid,
    /// Previewed Project CURRENT; never silently refreshed for a mutation.
    pub expected_generation_uuid: Uuid,
    /// New Branch context identity.
    pub branch_uuid: Uuid,
    /// New immutable selected-base Version.
    pub version_uuid: Uuid,
    /// Explicit immediate source.
    pub source: BranchSource,
    /// Creator metadata, not remote authentication.
    pub creator_uuid: Uuid,
    /// Creation time in UTC microseconds.
    pub created_at: i64,
    /// Immutable creation label.
    pub label: String,
}

/// Restore only one Branch's frozen research, preserving other heads and history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreResearchBranchRequest {
    /// Stable operation identity for exact replay.
    pub operation_uuid: Uuid,
    /// Previewed Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Owning Branch, required to match the source Version context.
    pub branch_uuid: Uuid,
    /// Exact retained Branch Version to restore.
    pub source_version_uuid: Uuid,
    /// Fresh immutable Version for the restored current state.
    pub version_uuid: Uuid,
    /// Restore time in UTC microseconds.
    pub created_at: i64,
}

/// Apply native Cypher only to one Branch's effective research graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteResearchBranchRequest {
    /// Stable operation identity for exact replay.
    pub operation_uuid: Uuid,
    /// Previewed Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Owning Branch context.
    pub branch_uuid: Uuid,
    /// Fresh immutable Version for the resulting Branch state.
    pub version_uuid: Uuid,
    /// Native mutation query; the parent graph is never its execution target.
    pub query: String,
    /// New Version time in UTC microseconds.
    pub created_at: i64,
}

/// Exact Branch-local ontology composition replacement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeResearchBranchOntologyRequest {
    /// Stable operation identity.
    pub operation_uuid: Uuid,
    /// Previewed owning Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Target Branch.
    pub branch_uuid: Uuid,
    /// Fresh Version for the resulting research.
    pub version_uuid: Uuid,
    /// Exact previous Branch composition fingerprint.
    pub expected_composition_fingerprint: Option<String>,
    /// Complete exact candidate authority.
    pub candidate: graphforge_storage::WorkspaceOntologyComposition,
    /// Existing native stored-data validation contract.
    pub data_disposition: crate::CompositionDataDisposition,
    /// Version time in UTC microseconds.
    pub created_at: i64,
}

/// Cite historical research without incorporating its objects or retaining its payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceResearchBranchRequest {
    /// Stable operation identity.
    pub operation_uuid: Uuid,
    /// Previewed owning Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Destination Branch.
    pub branch_uuid: Uuid,
    /// Fresh immutable destination Version.
    pub version_uuid: Uuid,
    /// Stable reference identity.
    pub reference_uuid: Uuid,
    /// Exact source Version; never implicit CURRENT.
    pub source_version_uuid: Uuid,
    /// Bounded reference label.
    pub label: String,
    /// Version time in UTC microseconds.
    pub created_at: i64,
}

/// Incorporate exact frozen membership into one Branch, preserving public UUIDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BringResearchBranchRequest {
    /// Stable operation identity.
    pub operation_uuid: Uuid,
    /// Previewed owning Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Destination Branch.
    pub branch_uuid: Uuid,
    /// Fresh destination Version.
    pub version_uuid: Uuid,
    /// Exact bounded frozen Slice from explicitly retained history.
    pub frozen_ipc: Vec<u8>,
    /// Version time in UTC microseconds.
    pub created_at: i64,
}

/// Suppress one assertion in the Branch knowledge view without deleting graph objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuppressResearchBranchAssertionRequest {
    /// Stable operation identity.
    pub operation_uuid: Uuid,
    /// Previewed owning Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Destination Branch.
    pub branch_uuid: Uuid,
    /// Fresh resulting Version.
    pub version_uuid: Uuid,
    /// Assertion removed from this Branch's active knowledge view.
    pub assertion_uuid: Uuid,
    /// Version time in UTC microseconds.
    pub created_at: i64,
}
