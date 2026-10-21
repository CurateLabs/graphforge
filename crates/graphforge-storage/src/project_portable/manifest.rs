//! The portable package manifest schema (`data/graphforge-project.json`).

use serde::Deserialize;

use super::semantic_validation::MAX_PRODUCING_CORE_VERSION_BYTES;
use super::{PortableError, PortableErrorCode};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub(super) format: String,
    pub(super) package_digest: String,
    pub(super) package_class: String,
    /// Absent only in packages written by a Core that predates the declaration;
    /// verification refuses those as `Incompatible`, never as malformed.
    #[serde(default)]
    pub(super) producing_core_version: Option<String>,
    pub(super) source_generation: SourceGeneration,
    pub(super) selection: Selection,
    pub(super) components: Vec<ManifestComponent>,
    pub(super) requirements: Requirements,
    pub(super) states: States,
}
impl Manifest {
    /// The declared producer. A package without one was written by an older
    /// Core, and data runs only on the Core that wrote it (ADR 0054 section 9).
    pub(super) fn producing_core_version(&self) -> Result<&str, PortableError> {
        match self.producing_core_version.as_deref() {
            Some(version)
                if !version.is_empty()
                    && version.len() <= MAX_PRODUCING_CORE_VERSION_BYTES
                    && !version.chars().any(char::is_control) =>
            {
                Ok(version)
            }
            _ => Err(PortableError::new(
                PortableErrorCode::Incompatible,
                "producing core version",
            )),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceGeneration {
    pub(super) generation_uuid: String,
    pub(super) manifest_sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selection {
    pub(super) roots: Vec<String>,
    pub(super) omissions: Vec<String>,
    pub(super) redactions: Vec<String>,
    pub(super) graph_subset: Option<GraphSubset>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GraphSubset {
    pub(super) selector: String,
    pub(super) closure: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManifestComponent {
    pub(super) kind: String,
    pub(super) participant_id: String,
    pub(super) required_dependencies: Vec<String>,
    pub(super) files: Vec<ManifestFile>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManifestFile {
    pub(super) media_type: String,
    pub(super) path: String,
    pub(super) length: u64,
    pub(super) sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Requirements {
    pub(super) capabilities: Vec<String>,
    pub(super) dependency_rule: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct States {
    pub(super) integrity: String,
    pub(super) compatibility: String,
    pub(super) authenticity: String,
}
