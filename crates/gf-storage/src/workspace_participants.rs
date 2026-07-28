//! Canonical generation-managed workspace ontology and configuration records.

use std::collections::BTreeMap;

use gf_core::{GfError, OntologyMode, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{ProjectParticipant, ProjectParticipantEncoding};

/// Mandatory capability containing authoritative workspace control records.
pub const WORKSPACE_CAPABILITY_ID: &str = "workspace";
/// Frozen workspace capability contract version.
pub const WORKSPACE_CAPABILITY_VERSION: u32 = 1;
/// Canonical ontology record family.
pub const WORKSPACE_ONTOLOGY_FAMILY: &str = "ontology";
/// Canonical project-configuration record family.
pub const WORKSPACE_CONFIGURATION_FAMILY: &str = "configuration";

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
}
