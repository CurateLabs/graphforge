//! Canonical generation-managed workspace ontology and configuration records.

use std::collections::BTreeMap;

use gf_core::{GfError, OntologyMode, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{ProjectParticipant, ProjectParticipantEncoding};

/// Mandatory capability containing authoritative workspace control records.
pub const WORKSPACE_CAPABILITY_ID: &str = "workspace";
/// Frozen workspace capability contract version.
pub const WORKSPACE_CAPABILITY_VERSION: u32 = 1;
/// Canonical ontology record family.
pub const WORKSPACE_ONTOLOGY_FAMILY: &str = "ontology";
/// Canonical project-configuration record family.
pub const WORKSPACE_CONFIGURATION_FAMILY: &str = "configuration";
/// Canonical repository reconciliation snapshot record family.
pub const WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY: &str = "repository_snapshot";
/// Frozen repository snapshot contract version.
pub const WORKSPACE_REPOSITORY_SNAPSHOT_VERSION: u32 = 1;
/// Maximum canonical repository snapshot participant size.
pub const MAX_WORKSPACE_REPOSITORY_SNAPSHOT_BYTES: usize = 1024 * 1024;
/// Maximum number of declared definitions or external sources in one snapshot.
pub const MAX_WORKSPACE_REPOSITORY_SNAPSHOT_ENTRIES: usize = 10_000;
/// Maximum UTF-8 byte length of one stable definition or source identifier.
pub const MAX_WORKSPACE_REPOSITORY_SNAPSHOT_ID_BYTES: usize = 256;

/// Persisted ontology mode. Absence is explicit rather than inferred from files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceOntologyMode {
    /// No adopted ontology; runtime behavior is exploratory.
    None,
    /// Adopted ontology emits advisory validation.
    Advisory,
    /// Adopted ontology is enforced strictly.
    Strict,
}

impl WorkspaceOntologyMode {
    /// Convert the persisted mode to the execution-layer mode.
    #[must_use]
    pub const fn execution_mode(self) -> OntologyMode {
        match self {
            Self::None => OntologyMode::Exploratory,
            Self::Advisory => OntologyMode::Advisory,
            Self::Strict => OntologyMode::Strict,
        }
    }
}

/// Original syntax accepted at ontology adoption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceOntologySourceFormat {
    /// YAML or YML input.
    Yaml,
    /// JSON input.
    Json,
}

/// Canonical ontology participant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceOntology {
    /// Frozen record contract version.
    pub contract_version: u32,
    /// Explicit effective mode.
    pub mode: WorkspaceOntologyMode,
    /// Syntax used for adoption, absent when mode is `none`.
    pub source_format: Option<WorkspaceOntologySourceFormat>,
    /// SHA-256 over canonical ontology JSON, absent when mode is `none`.
    pub canonical_ontology_sha256: Option<String>,
    /// Validated canonical ontology document, absent when mode is `none`.
    pub canonical_ontology: Option<Value>,
}

/// Canonical authoritative project configuration participant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceConfiguration {
    /// Frozen record contract version.
    pub contract_version: u32,
    /// Must match the ontology participant mode.
    pub ontology_mode: WorkspaceOntologyMode,
    /// Registered capability settings ordered by stable capability ID.
    pub capability_configuration: BTreeMap<String, Value>,
    /// Registered embedding settings ordered by stable setting ID.
    pub embedding_configuration: BTreeMap<String, Value>,
}

/// One declared repository definition identified without retaining its path or contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRepositoryDefinitionDigest {
    /// Stable caller-defined definition identifier.
    pub definition_id: String,
    /// SHA-256 of the validated canonical definition tree.
    pub sha256: String,
}

/// One declared external source reference; source data is never embedded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRepositorySourceDigest {
    /// Stable caller-defined source identifier.
    pub source_id: String,
    /// Caller-declared SHA-256 of the external source.
    pub sha256: String,
}

/// Bounded Git provenance recorded without paths, diffs, messages, or repository contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRepositoryGitProvenance {
    /// Exact hexadecimal Git object ID, absent for repositories without a commit.
    pub commit_sha: Option<String>,
    /// Whether tracked repository state differed from the selected commit.
    pub dirty: bool,
}

/// Canonical secret-free desired-state evidence published by repository sync.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRepositorySnapshot {
    /// Frozen record contract version.
    pub contract_version: u32,
    /// SHA-256 of the canonical resolved, secret-free configuration.
    pub resolved_config_sha256: String,
    /// Strictly ordered declared-definition identifiers and digests.
    pub definitions: Vec<WorkspaceRepositoryDefinitionDigest>,
    /// Strictly ordered external source identifiers and digest references.
    pub sources: Vec<WorkspaceRepositorySourceDigest>,
    /// Bounded Git provenance.
    pub git: WorkspaceRepositoryGitProvenance,
    /// Caller-owned idempotency identity for the publishing operation.
    pub operation_uuid: Uuid,
    /// Optional caller-owned actor identity.
    pub actor_uuid: Option<Uuid>,
}

impl WorkspaceOntology {
    /// Construct explicit ontology absence for a new exploratory project.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            contract_version: 1,
            mode: WorkspaceOntologyMode::None,
            source_format: None,
            canonical_ontology_sha256: None,
            canonical_ontology: None,
        }
    }

    /// Validate invariants and return canonical JSON plus LF.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` for an inconsistent persisted record.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, GfError> {
        validate_ontology(self)?;
        canonical_json(self, "workspace ontology")
    }

    /// Parse exact canonical JSON plus LF.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` for future, malformed, or noncanonical data.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, GfError> {
        parse_canonical_json(bytes, "workspace ontology", validate_ontology)
    }
}

impl WorkspaceConfiguration {
    /// Construct the empty authoritative configuration for a new project.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            contract_version: 1,
            ontology_mode: WorkspaceOntologyMode::None,
            capability_configuration: BTreeMap::new(),
            embedding_configuration: BTreeMap::new(),
        }
    }

    /// Validate and return canonical JSON plus LF.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` for a future contract.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, GfError> {
        validate_configuration(self)?;
        canonical_json(self, "workspace configuration")
    }

    /// Parse exact canonical JSON plus LF.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` for future, malformed, or noncanonical data.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, GfError> {
        parse_canonical_json(bytes, "workspace configuration", validate_configuration)
    }
}

impl WorkspaceRepositorySnapshot {
    /// Validate and return canonical JSON plus LF.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` for future, malformed, noncanonical, or
    /// out-of-bounds snapshots.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, GfError> {
        validate_repository_snapshot(self)?;
        let bytes = canonical_json(self, "workspace repository snapshot")?;
        if bytes.len() > MAX_WORKSPACE_REPOSITORY_SNAPSHOT_BYTES {
            return Err(corrupt("workspace repository snapshot exceeds size limit"));
        }
        Ok(bytes)
    }

    /// Parse exact canonical JSON plus LF and enforce every contract bound.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` for future, malformed, noncanonical, or
    /// out-of-bounds snapshots.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, GfError> {
        if bytes.len() > MAX_WORKSPACE_REPOSITORY_SNAPSHOT_BYTES {
            return Err(corrupt("workspace repository snapshot exceeds size limit"));
        }
        parse_canonical_json(
            bytes,
            "workspace repository snapshot",
            validate_repository_snapshot,
        )
    }

    /// Encode this record as the registered workspace generation participant.
    ///
    /// # Errors
    /// Returns a structured error when the snapshot violates its contract.
    pub fn to_project_participant(&self) -> Result<ProjectParticipant, GfError> {
        Ok(participant(
            WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY,
            self.to_canonical_json()?,
        ))
    }
}

/// Build both mandatory participants for a new project.
///
/// # Errors
/// Returns a structured error if canonical encoding fails.
pub fn empty_workspace_participants() -> Result<Vec<ProjectParticipant>, GfError> {
    Ok(vec![
        participant(
            WORKSPACE_CONFIGURATION_FAMILY,
            WorkspaceConfiguration::empty().to_canonical_json()?,
        ),
        participant(
            WORKSPACE_ONTOLOGY_FAMILY,
            WorkspaceOntology::none().to_canonical_json()?,
        ),
    ])
}

fn participant(family: &str, bytes: Vec<u8>) -> ProjectParticipant {
    ProjectParticipant {
        capability_id: WORKSPACE_CAPABILITY_ID.into(),
        capability_version: WORKSPACE_CAPABILITY_VERSION,
        record_family_id: family.into(),
        record_version: 1,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: Sha256::digest(format!("workspace/{family}@1")).into(),
        row_count: 1,
        bytes,
    }
}

fn validate_ontology(record: &WorkspaceOntology) -> Result<(), GfError> {
    if record.contract_version != 1 {
        return Err(corrupt("unsupported workspace ontology contract"));
    }
    match record.mode {
        WorkspaceOntologyMode::None
            if record.source_format.is_none()
                && record.canonical_ontology_sha256.is_none()
                && record.canonical_ontology.is_none() =>
        {
            Ok(())
        }
        WorkspaceOntologyMode::Advisory | WorkspaceOntologyMode::Strict => {
            let document = record
                .canonical_ontology
                .as_ref()
                .ok_or_else(|| corrupt("adopted ontology document is missing"))?;
            if record.source_format.is_none() {
                return Err(corrupt("adopted ontology source format is missing"));
            }
            let canonical = serde_json::to_vec(document)
                .map_err(|_| corrupt("ontology document cannot be encoded"))?;
            let digest = encode_hex(&Sha256::digest(canonical));
            if record.canonical_ontology_sha256.as_deref() != Some(digest.as_str()) {
                return Err(corrupt("canonical ontology digest does not match"));
            }
            Ok(())
        }
        WorkspaceOntologyMode::None => Err(corrupt("workspace ontology absence is inconsistent")),
    }
}

fn validate_configuration(record: &WorkspaceConfiguration) -> Result<(), GfError> {
    if record.contract_version != 1 {
        return Err(corrupt("unsupported workspace configuration contract"));
    }
    Ok(())
}

fn validate_repository_snapshot(record: &WorkspaceRepositorySnapshot) -> Result<(), GfError> {
    if record.contract_version != WORKSPACE_REPOSITORY_SNAPSHOT_VERSION {
        return Err(corrupt(
            "unsupported workspace repository snapshot contract",
        ));
    }
    validate_sha256(&record.resolved_config_sha256, "resolved config")?;
    validate_digest_entries(
        &record.definitions,
        |entry| (&entry.definition_id, &entry.sha256),
        "definition",
    )?;
    validate_digest_entries(
        &record.sources,
        |entry| (&entry.source_id, &entry.sha256),
        "source",
    )?;
    if let Some(commit_sha) = &record.git.commit_sha
        && (!matches!(commit_sha.len(), 40 | 64)
            || !commit_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
            || commit_sha.bytes().any(|byte| byte.is_ascii_uppercase()))
    {
        return Err(corrupt("Git commit SHA is not canonical hexadecimal"));
    }
    if record.operation_uuid.is_nil() {
        return Err(corrupt("repository snapshot operation UUID is nil"));
    }
    if record.actor_uuid.is_some_and(|actor| actor.is_nil()) {
        return Err(corrupt("repository snapshot actor UUID is nil"));
    }
    Ok(())
}

fn validate_digest_entries<T>(
    entries: &[T],
    fields: impl for<'a> Fn(&'a T) -> (&'a String, &'a String),
    name: &str,
) -> Result<(), GfError> {
    if entries.len() > MAX_WORKSPACE_REPOSITORY_SNAPSHOT_ENTRIES {
        return Err(corrupt(format!(
            "workspace repository {name} count exceeds limit"
        )));
    }
    let mut prior: Option<&str> = None;
    for entry in entries {
        let (id, digest) = fields(entry);
        let canonical_id = !id.is_empty()
            && id.len() <= MAX_WORKSPACE_REPOSITORY_SNAPSHOT_ID_BYTES
            && id.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || index > 0 && byte.is_ascii_digit()
                    || index > 0 && matches!(byte, b'-' | b'_')
            });
        if !canonical_id {
            return Err(corrupt(format!(
                "workspace repository {name} identifier is invalid"
            )));
        }
        if prior.is_some_and(|prior| prior >= id.as_str()) {
            return Err(corrupt(format!(
                "workspace repository {name} identifiers are not canonical"
            )));
        }
        validate_sha256(digest, name)?;
        prior = Some(id);
    }
    Ok(())
}

fn validate_sha256(value: &str, name: &str) -> Result<(), GfError> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(corrupt(format!("{name} SHA-256 is not canonical")));
    }
    Ok(())
}

fn canonical_json(value: &impl Serialize, name: &str) -> Result<Vec<u8>, GfError> {
    let mut bytes =
        serde_json::to_vec(value).map_err(|_| corrupt(format!("{name} cannot be encoded")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn parse_canonical_json<T>(
    bytes: &[u8],
    name: &str,
    validate: impl FnOnce(&T) -> Result<(), GfError>,
) -> Result<T, GfError>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    if !bytes.ends_with(b"\n") || bytes[..bytes.len().saturating_sub(1)].contains(&b'\n') {
        return Err(corrupt(format!("{name} is not canonical JSON plus LF")));
    }
    let parsed =
        serde_json::from_slice(bytes).map_err(|_| corrupt(format!("{name} is malformed")))?;
    validate(&parsed)?;
    if canonical_json(&parsed, name)? != bytes {
        return Err(corrupt(format!("{name} is not canonically encoded")));
    }
    Ok(parsed)
}

fn corrupt(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: message.into(),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repository_snapshot() -> WorkspaceRepositorySnapshot {
        WorkspaceRepositorySnapshot {
            contract_version: WORKSPACE_REPOSITORY_SNAPSHOT_VERSION,
            resolved_config_sha256: "11".repeat(32),
            definitions: vec![
                WorkspaceRepositoryDefinitionDigest {
                    definition_id: "migrations".into(),
                    sha256: "22".repeat(32),
                },
                WorkspaceRepositoryDefinitionDigest {
                    definition_id: "ontology".into(),
                    sha256: "33".repeat(32),
                },
            ],
            sources: vec![WorkspaceRepositorySourceDigest {
                source_id: "customers".into(),
                sha256: "44".repeat(32),
            }],
            git: WorkspaceRepositoryGitProvenance {
                commit_sha: Some("a1".repeat(20)),
                dirty: true,
            },
            operation_uuid: Uuid::from_bytes([5; 16]),
            actor_uuid: Some(Uuid::from_bytes([6; 16])),
        }
    }

    #[test]
    fn empty_records_round_trip_canonically() {
        let ontology = WorkspaceOntology::none();
        let ontology_bytes = ontology.to_canonical_json().unwrap();
        assert_eq!(
            WorkspaceOntology::from_canonical_json(&ontology_bytes).unwrap(),
            ontology
        );

        let configuration = WorkspaceConfiguration::empty();
        let configuration_bytes = configuration.to_canonical_json().unwrap();
        assert_eq!(
            WorkspaceConfiguration::from_canonical_json(&configuration_bytes).unwrap(),
            configuration
        );
    }

    #[test]
    fn future_and_noncanonical_records_fail_closed() {
        let mut future = WorkspaceOntology::none();
        future.contract_version = 2;
        assert_eq!(
            future.to_canonical_json().unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );

        let bytes = br#"{"mode":"none","contract_version":1,"source_format":null,"canonical_ontology_sha256":null,"canonical_ontology":null}
"#;
        assert_eq!(
            WorkspaceOntology::from_canonical_json(bytes)
                .unwrap_err()
                .code(),
            "GF_PROJECT_CORRUPT"
        );
    }

    #[test]
    fn adopted_ontology_requires_matching_digest() {
        let record = WorkspaceOntology {
            contract_version: 1,
            mode: WorkspaceOntologyMode::Strict,
            source_format: Some(WorkspaceOntologySourceFormat::Json),
            canonical_ontology_sha256: Some("0".repeat(64)),
            canonical_ontology: Some(serde_json::json!({"ontology_id": "x"})),
        };
        assert_eq!(
            record.to_canonical_json().unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );
    }

    #[test]
    fn repository_snapshot_round_trips_canonically_as_registered_participant() {
        let snapshot = repository_snapshot();
        let bytes = snapshot.to_canonical_json().unwrap();
        assert_eq!(
            WorkspaceRepositorySnapshot::from_canonical_json(&bytes).unwrap(),
            snapshot
        );
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!bytes.windows(2).any(|window| window == b"\n\n"));
        assert!(!String::from_utf8_lossy(&bytes).contains("/Users/"));

        let participant = snapshot.to_project_participant().unwrap();
        assert_eq!(participant.capability_id, WORKSPACE_CAPABILITY_ID);
        assert_eq!(
            participant.record_family_id,
            WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY
        );
        let expected_fingerprint: [u8; 32] =
            Sha256::digest("workspace/repository_snapshot@1").into();
        assert_eq!(participant.schema_fingerprint, expected_fingerprint);
        assert_eq!(participant.bytes, bytes);
    }

    #[test]
    fn repository_snapshot_rejects_future_noncanonical_and_unbounded_records() {
        let mut future = repository_snapshot();
        future.contract_version += 1;
        assert_eq!(
            future.to_canonical_json().unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );

        let mut unordered = repository_snapshot();
        unordered.definitions.reverse();
        assert_eq!(
            unordered.to_canonical_json().unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );

        let mut duplicate = repository_snapshot();
        duplicate.sources.push(duplicate.sources[0].clone());
        assert_eq!(
            duplicate.to_canonical_json().unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );

        let mut oversized_id = repository_snapshot();
        oversized_id.sources[0].source_id =
            "x".repeat(MAX_WORKSPACE_REPOSITORY_SNAPSHOT_ID_BYTES + 1);
        assert_eq!(
            oversized_id.to_canonical_json().unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );

        let mut too_many = repository_snapshot();
        too_many.definitions = (0..=MAX_WORKSPACE_REPOSITORY_SNAPSHOT_ENTRIES)
            .map(|index| WorkspaceRepositoryDefinitionDigest {
                definition_id: format!("{index:05}"),
                sha256: "55".repeat(32),
            })
            .collect();
        assert_eq!(
            too_many.to_canonical_json().unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );

        let mut absolute_id = repository_snapshot();
        absolute_id.sources[0].source_id = "/private/data/customers".into();
        assert_eq!(
            absolute_id.to_canonical_json().unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );
        for hostile in [
            "../customers",
            "file:///private/customers",
            "customers/data",
            "customers\\data",
            ".hidden",
            "Customers",
            "9customers",
        ] {
            let mut hostile_id = repository_snapshot();
            hostile_id.sources[0].source_id = hostile.into();
            assert_eq!(
                hostile_id.to_canonical_json().unwrap_err().code(),
                "GF_PROJECT_CORRUPT",
                "{hostile}"
            );
        }

        let oversized = vec![b' '; MAX_WORKSPACE_REPOSITORY_SNAPSHOT_BYTES + 1];
        assert_eq!(
            WorkspaceRepositorySnapshot::from_canonical_json(&oversized)
                .unwrap_err()
                .code(),
            "GF_PROJECT_CORRUPT"
        );

        let with_unrestricted_field = String::from_utf8(
            repository_snapshot()
                .to_canonical_json()
                .unwrap()
                .into_iter()
                .collect(),
        )
        .unwrap()
        .replacen(
            "\"actor_uuid\"",
            "\"absolute_path\":\"/secret/data\",\"actor_uuid\"",
            1,
        );
        assert_eq!(
            WorkspaceRepositorySnapshot::from_canonical_json(with_unrestricted_field.as_bytes())
                .unwrap_err()
                .code(),
            "GF_PROJECT_CORRUPT"
        );
    }
}
