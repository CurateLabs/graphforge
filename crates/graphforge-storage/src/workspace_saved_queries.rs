//! Optional generation-managed saved-query definitions; parameter values are never persisted.

use std::collections::{BTreeMap, BTreeSet};

use graphforge_core::hash_observation::ContractSha256;
use graphforge_core::{GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use uuid::Uuid;

use crate::{
    ProjectParticipant, ProjectParticipantEncoding, ResolvedProjectGeneration,
    WORKSPACE_CAPABILITY_ID, WORKSPACE_CAPABILITY_VERSION,
};

/// Optional native saved-query definition family.
pub const WORKSPACE_SAVED_QUERIES_FAMILY: &str = "saved_queries";
/// Frozen saved-query definition contract version.
pub const WORKSPACE_SAVED_QUERIES_VERSION: u32 = 1;
/// Maximum canonical saved-query participant bytes.
pub const MAX_WORKSPACE_SAVED_QUERIES_BYTES: usize = 1024 * 1024;
/// Maximum definitions in one project.
pub const MAX_SAVED_QUERIES: usize = 1024;
/// Maximum UTF-8 bytes in one query text.
pub const MAX_SAVED_QUERY_BYTES: usize = 64 * 1024;
/// Maximum declared parameters per query.
pub const MAX_SAVED_QUERY_PARAMETERS: usize = 256;
/// Maximum UTF-8 bytes in a saved-query name or parameter name.
pub const MAX_SAVED_QUERY_NAME_BYTES: usize = 256;
/// Maximum UTF-8 bytes in a saved-query description.
pub const MAX_SAVED_QUERY_DESCRIPTION_BYTES: usize = 4096;

/// Required scalar parameter type; no defaults or bound values are stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SavedQueryParameterType {
    /// Boolean scalar.
    Boolean,
    /// Signed 64-bit integer scalar.
    Integer,
    /// Finite floating-point scalar.
    Float,
    /// UTF-8 string scalar.
    String,
    /// UUID scalar.
    Uuid,
}

/// A named read-only query definition owned by a project.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedQuery {
    /// Stable, nonnil definition identity.
    pub query_uuid: Uuid,
    /// Case-sensitive, trimmed, nonempty project-unique name.
    pub name: String,
    /// Optional explanatory text.
    pub description: Option<String>,
    /// Native read-only Cypher query text.
    pub query: String,
    /// Exact required parameter names and types, never parameter values.
    pub parameters: BTreeMap<String, SavedQueryParameterType>,
}

impl SavedQuery {
    /// Validate the definition without binding or executing it.
    ///
    /// # Errors
    /// Returns a validation error for invalid metadata, syntax, read-only
    /// behavior, or a parameter declaration that differs from query references.
    pub fn validate(&self) -> Result<(), GfError> {
        if self.query_uuid.is_nil() {
            return Err(invalid("saved-query identity must be nonnil"));
        }
        if self.name.is_empty()
            || self.name.trim() != self.name
            || self.name.len() > MAX_SAVED_QUERY_NAME_BYTES
            || self.name.chars().any(char::is_control)
        {
            return Err(invalid(
                "saved-query name must be trimmed, nonempty and bounded",
            ));
        }
        if self
            .description
            .as_ref()
            .is_some_and(|value| value.len() > MAX_SAVED_QUERY_DESCRIPTION_BYTES)
        {
            return Err(invalid("saved-query description exceeds its bound"));
        }
        if self.query.trim().is_empty() || self.query.len() > MAX_SAVED_QUERY_BYTES {
            return Err(invalid("saved-query text must be nonempty and bounded"));
        }
        if self.parameters.len() > MAX_SAVED_QUERY_PARAMETERS
            || self
                .parameters
                .keys()
                .any(|name| name.is_empty() || name.len() > MAX_SAVED_QUERY_NAME_BYTES)
        {
            return Err(invalid(
                "saved-query parameter declarations exceed their bounds",
            ));
        }
        let referenced = graphforge_cypher::read_only_query_parameters(&self.query)?;
        if !referenced.iter().eq(self.parameters.keys()) {
            return Err(invalid(
                "saved-query declarations must exactly match query parameters",
            ));
        }
        Ok(())
    }
}

/// Canonical bounded saved-query collection in one committed generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSavedQueries {
    /// Frozen record contract version.
    pub contract_version: u32,
    /// Definitions keyed by their stable identities.
    pub queries: BTreeMap<Uuid, SavedQuery>,
}

impl Default for WorkspaceSavedQueries {
    fn default() -> Self {
        Self {
            contract_version: WORKSPACE_SAVED_QUERIES_VERSION,
            queries: BTreeMap::new(),
        }
    }
}

impl WorkspaceSavedQueries {
    /// Validate identities, names, definitions and aggregate bounds.
    ///
    /// # Errors
    /// Returns a validation error for unsupported or invalid definitions.
    pub fn validate(&self) -> Result<(), GfError> {
        if self.contract_version != WORKSPACE_SAVED_QUERIES_VERSION
            || self.queries.len() > MAX_SAVED_QUERIES
        {
            return Err(invalid("unsupported or oversized saved-query collection"));
        }
        let mut names = BTreeSet::new();
        for (id, query) in &self.queries {
            if *id != query.query_uuid {
                return Err(invalid("saved-query map key differs from its identity"));
            }
            query.validate()?;
            if !names.insert(&query.name) {
                return Err(invalid("saved-query names must be unique within a project"));
            }
        }
        if self.encode()?.len() > MAX_WORKSPACE_SAVED_QUERIES_BYTES {
            return Err(invalid("saved-query collection exceeds its byte bound"));
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>, GfError> {
        let mut bytes =
            serde_json::to_vec(self).map_err(|_| invalid("saved-query encoding failed"))?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Encode validated canonical JSON plus LF.
    ///
    /// # Errors
    /// Returns a validation error when the collection violates its contract.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, GfError> {
        self.validate()?;
        self.encode()
    }

    /// Decode exact canonical JSON plus LF, refusing future or malformed records.
    ///
    /// # Errors
    /// Returns a project-corruption error when persisted metadata is invalid.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, GfError> {
        if bytes.len() > MAX_WORKSPACE_SAVED_QUERIES_BYTES {
            return Err(corrupt("saved-query collection exceeds its byte bound"));
        }
        let record: Self = serde_json::from_slice(bytes)
            .map_err(|_| corrupt("malformed saved-query collection"))?;
        record
            .validate()
            .map_err(|error| corrupt(error.to_string()))?;
        if record.encode()? != bytes {
            return Err(corrupt(
                "saved-query collection is not canonical JSON plus LF",
            ));
        }
        Ok(record)
    }

    /// Encode the optional workspace generation participant.
    ///
    /// # Errors
    /// Returns a validation error for invalid definitions.
    pub fn to_project_participant(&self) -> Result<ProjectParticipant, GfError> {
        Ok(ProjectParticipant {
            capability_id: WORKSPACE_CAPABILITY_ID.into(),
            capability_version: WORKSPACE_CAPABILITY_VERSION,
            record_family_id: WORKSPACE_SAVED_QUERIES_FAMILY.into(),
            record_version: WORKSPACE_SAVED_QUERIES_VERSION,
            encoding: ProjectParticipantEncoding::Json,
            schema_fingerprint: schema_fingerprint(),
            row_count: self.queries.len() as u64,
            bytes: self.to_canonical_json()?,
        })
    }
}

pub(crate) fn schema_fingerprint() -> [u8; 32] {
    ContractSha256::digest(b"workspace/saved_queries@1").into()
}

/// Read saved queries from a committed generation; absent optional metadata is empty.
///
/// # Errors
/// Returns a structured project error for unsupported or corrupt records.
pub fn read_workspace_saved_queries(
    generation: &ResolvedProjectGeneration,
) -> Result<WorkspaceSavedQueries, GfError> {
    let descriptor = generation
        .participant_descriptors()?
        .into_iter()
        .find(|descriptor| {
            descriptor.capability_id == WORKSPACE_CAPABILITY_ID
                && descriptor.record_family_id == WORKSPACE_SAVED_QUERIES_FAMILY
        });
    let Some(_descriptor) = descriptor else {
        return Ok(WorkspaceSavedQueries::default());
    };
    if generation
        .portable_participant_identity(WORKSPACE_CAPABILITY_ID, WORKSPACE_SAVED_QUERIES_FAMILY)?
        .byte_length()
        > MAX_WORKSPACE_SAVED_QUERIES_BYTES as u64
    {
        return Err(corrupt("saved-query participant exceeds its byte bound"));
    }
    let snapshot = generation
        .participant_snapshot(WORKSPACE_CAPABILITY_ID, WORKSPACE_SAVED_QUERIES_FAMILY)?
        .ok_or_else(|| corrupt("saved-query participant is missing"))?;
    decode_participant(
        snapshot.capability_version,
        snapshot.record_version,
        &snapshot.encoding,
        &snapshot.schema_fingerprint,
        snapshot.row_count,
        &snapshot.bytes,
    )
}

pub(crate) fn decode_participant(
    capability_version: u32,
    record_version: u32,
    encoding: &str,
    schema: &[u8; 32],
    row_count: u64,
    bytes: &[u8],
) -> Result<WorkspaceSavedQueries, GfError> {
    if capability_version != WORKSPACE_CAPABILITY_VERSION
        || record_version != WORKSPACE_SAVED_QUERIES_VERSION
        || encoding != "json"
        || *schema != schema_fingerprint()
    {
        return Err(corrupt("unsupported saved-query participant contract"));
    }
    let record = WorkspaceSavedQueries::from_canonical_json(bytes)?;
    if row_count != record.queries.len() as u64 {
        return Err(corrupt(
            "saved-query participant row count differs from its definitions",
        ));
    }
    Ok(record)
}

fn invalid(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}
fn corrupt(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests;
