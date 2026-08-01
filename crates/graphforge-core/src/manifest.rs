//! Project manifest (`graphforge.yaml`) — the entry point that describes a
//! GraphForge project directory: its identity, ontology mode, and which
//! capabilities are enabled.
//!
//! [`GraphForge::new`](crate::GraphForge) opens a project directory and reads
//! this manifest to determine how to construct the engine.  The format is
//! intentionally forward-compatible: unknown fields are ignored (no
//! `deny_unknown_fields`) so newer manifests still load on older binaries.

use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{GfError, OntologyMode};

/// File name of the manifest within a project directory.
pub const MANIFEST_FILE: &str = "graphforge.yaml";
/// File name of the optional ontology definition within a project directory.
pub const ONTOLOGY_FILE: &str = "ontology.yaml";

/// Feature flags recording which capabilities a project uses.
///
/// All fields default to `false` (via `#[serde(default)]`) so a manifest with
/// no `capabilities:` block deserialises cleanly.  [`topology`](Self::topology)
/// and [`properties`](Self::properties) are always enabled in practice — see
/// [`ProjectManifest::enabled_capabilities`].
//
// This is a serialised manifest schema, not a state machine — each flag is an
// independent on-disk capability toggle, so the "too many bools" lint does not
// apply (the layout mirrors the `capabilities:` YAML block 1:1).
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Node/edge topology storage (always on).
    #[serde(default)]
    pub topology: bool,
    /// Per-type property storage (always on).
    #[serde(default)]
    pub properties: bool,
    /// Document attachments.
    #[serde(default)]
    pub documents: bool,
    /// Confidence + lineage provenance.
    #[serde(default)]
    pub provenance: bool,
    /// Vector embeddings.
    #[serde(default)]
    pub embeddings: bool,
    /// Text/vector indexes.
    #[serde(default)]
    pub indexes: bool,
    /// Workflow definitions.
    #[serde(default)]
    pub workflows: bool,
    /// Stored artifacts.
    #[serde(default)]
    pub artifacts: bool,
    /// Cross-project sync.
    #[serde(default)]
    pub sync: bool,
}

impl Default for Capabilities {
    fn default() -> Self {
        // topology + properties are the baseline every project has.
        Self {
            topology: true,
            properties: true,
            documents: false,
            provenance: false,
            embeddings: false,
            indexes: false,
            workflows: false,
            artifacts: false,
            sync: false,
        }
    }
}

/// Deserialised `graphforge.yaml`.
///
/// Constructed via [`load`](Self::load) (existing project) or
/// [`create_default`](Self::create_default) (new project).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectManifest {
    /// Stable project identity (UUIDv7, generated at creation).
    pub project_uuid: Uuid,
    /// Human-readable project name.
    pub name: String,
    /// Manifest schema version.
    pub version: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// Relative path to the ontology file, or `None` if the project has none.
    #[serde(default)]
    pub ontology: Option<String>,
    /// Explicit ontology mode override, or `None` to apply default-mode logic.
    #[serde(default)]
    pub ontology_mode: Option<OntologyMode>,
    /// IR schema version the project was created with.
    pub ir_version: String,
    /// GraphForge version the project was created with.
    pub graphforge_version: String,
    /// Capability flags; absent block defaults to the baseline.
    #[serde(default)]
    pub capabilities: Option<Capabilities>,
}

impl ProjectManifest {
    /// Read and parse `graphforge.yaml` from `dir`.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if the file cannot be read and
    /// [`GfError::Validation`] if its contents are not valid manifest YAML.
    pub fn load(dir: &Path) -> Result<Self, GfError> {
        let path = dir.join(MANIFEST_FILE);
        let text = std::fs::read_to_string(&path)
            .map_err(|e| GfError::Storage(format!("failed to read {}: {e}", path.display())))?;
        serde_yaml::from_str(&text)
            .map_err(|e| GfError::Validation(format!("invalid {MANIFEST_FILE}: {e}")))
    }

    /// Create a new project: generate a UUIDv7 identity and write
    /// `graphforge.yaml` into `dir`.
    ///
    /// `created_at` is supplied by the caller (RFC 3339) so this stays free of
    /// ambient clock access; pass `None` to omit a timestamp.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if the manifest cannot be written, or
    /// [`GfError::Validation`] if it cannot be serialised.
    pub fn create_default(
        dir: &Path,
        name: &str,
        created_at: impl Into<String>,
    ) -> Result<Self, GfError> {
        let manifest = Self {
            project_uuid: crate::uuid::new_v7(),
            name: name.to_owned(),
            version: "1".to_owned(),
            created_at: created_at.into(),
            ontology: None,
            ontology_mode: None,
            ir_version: "0.1.0".to_owned(),
            graphforge_version: env!("CARGO_PKG_VERSION").to_owned(),
            capabilities: Some(Capabilities::default()),
        };

        let yaml = serde_yaml::to_string(&manifest).map_err(|e| {
            GfError::Validation(format!("failed to serialise {MANIFEST_FILE}: {e}"))
        })?;
        let path = dir.join(MANIFEST_FILE);
        std::fs::write(&path, yaml)
            .map_err(|e| GfError::Storage(format!("failed to write {}: {e}", path.display())))?;

        Ok(manifest)
    }

    /// Determine the effective [`OntologyMode`], applying default-mode logic.
    ///
    /// | Condition | Mode |
    /// |---|---|
    /// | `ontology_mode` set in manifest | that value |
    /// | unset, configured `ontology` path exists in `dir` | [`Advisory`](OntologyMode::Advisory) |
    /// | unset, no configured path, `ontology.yaml` present in `dir` | [`Advisory`](OntologyMode::Advisory) |
    /// | unset, no ontology file present | [`Exploratory`](OntologyMode::Exploratory) |
    ///
    /// The configured `ontology` path is tested for existence (resolved
    /// relative to `dir`) rather than merely being present in the manifest —
    /// a manifest pointing at a missing file falls back to `Exploratory`.
    #[must_use]
    pub fn effective_ontology_mode(&self, dir: &Path) -> OntologyMode {
        if let Some(mode) = self.ontology_mode {
            return mode;
        }
        let has_ontology = self.ontology.as_deref().map_or_else(
            || dir.join(ONTOLOGY_FILE).exists(),
            |path| dir.join(path).exists(),
        );
        if has_ontology {
            OntologyMode::Advisory
        } else {
            OntologyMode::Exploratory
        }
    }

    /// Return the project's capabilities with baseline defaults applied.
    ///
    /// `topology` and `properties` are always enabled regardless of what the
    /// manifest declares.
    #[must_use]
    pub fn enabled_capabilities(&self) -> Capabilities {
        let mut caps = self.capabilities.clone().unwrap_or_default();
        caps.topology = true;
        caps.properties = true;
        caps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        // A unique scratch dir keyed by a fresh UUID (no clock/rng helpers).
        let dir = std::env::temp_dir().join(format!("gf-manifest-{}", crate::uuid::new_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn create_default_round_trips() {
        let dir = temp_dir();
        let created =
            ProjectManifest::create_default(&dir, "Investigation Alpha", "2026-06-02T00:00:00Z")
                .unwrap();
        let loaded = ProjectManifest::load(&dir).unwrap();

        assert_eq!(loaded.project_uuid, created.project_uuid);
        assert_eq!(loaded.name, "Investigation Alpha");
        assert_eq!(loaded.version, "1");
        assert_eq!(loaded.created_at, "2026-06-02T00:00:00Z");
        assert_eq!(loaded.ontology, None);
        assert_eq!(loaded.ontology_mode, None);
        assert_eq!(loaded.ir_version, "0.1.0");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn effective_mode_is_exploratory_without_ontology() {
        let dir = temp_dir();
        let m = ProjectManifest::create_default(&dir, "p", "2026-06-02T00:00:00Z").unwrap();
        assert_eq!(m.effective_ontology_mode(&dir), OntologyMode::Exploratory);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn effective_mode_is_advisory_when_ontology_file_present() {
        let dir = temp_dir();
        let m = ProjectManifest::create_default(&dir, "p", "2026-06-02T00:00:00Z").unwrap();
        std::fs::write(dir.join(ONTOLOGY_FILE), "ontology_id: x\nversion: v1\n").unwrap();
        assert_eq!(m.effective_ontology_mode(&dir), OntologyMode::Advisory);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn configured_ontology_path_must_exist_for_advisory() {
        let dir = temp_dir();
        let mut m = ProjectManifest::create_default(&dir, "p", "2026-06-02T00:00:00Z").unwrap();
        // Manifest points at a custom ontology path that does NOT exist on disk.
        m.ontology = Some("custom/missing.yaml".to_owned());
        assert_eq!(m.effective_ontology_mode(&dir), OntologyMode::Exploratory);

        // Once that file is created, the mode flips to Advisory.
        std::fs::create_dir_all(dir.join("custom")).unwrap();
        std::fs::write(dir.join("custom/missing.yaml"), "ontology_id: x\n").unwrap();
        assert_eq!(m.effective_ontology_mode(&dir), OntologyMode::Advisory);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explicit_mode_overrides_default_logic() {
        let dir = temp_dir();
        let mut m = ProjectManifest::create_default(&dir, "p", "2026-06-02T00:00:00Z").unwrap();
        m.ontology_mode = Some(OntologyMode::Strict);
        // Even with an ontology file present, the explicit mode wins.
        std::fs::write(dir.join(ONTOLOGY_FILE), "ontology_id: x\n").unwrap();
        assert_eq!(m.effective_ontology_mode(&dir), OntologyMode::Strict);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_capabilities_block_yields_baseline() {
        // A manifest YAML with no `capabilities:` field at all.
        let yaml = "\
project_uuid: 0192f3a2-0000-7000-8000-000000000000
name: minimal
version: \"1\"
created_at: \"2026-06-02T00:00:00Z\"
ir_version: 0.1.0
graphforge_version: 0.5.0
";
        let m: ProjectManifest = serde_yaml::from_str(yaml).unwrap();
        assert!(m.capabilities.is_none());
        let caps = m.enabled_capabilities();
        assert!(caps.topology);
        assert!(caps.properties);
        assert!(!caps.documents);
        assert!(!caps.embeddings);
        assert!(!caps.sync);
    }

    #[test]
    fn unknown_fields_are_ignored_for_forward_compat() {
        let yaml = "\
project_uuid: 0192f3a2-0000-7000-8000-000000000000
name: future
version: \"1\"
created_at: \"2026-06-02T00:00:00Z\"
ir_version: 0.1.0
graphforge_version: 0.5.0
some_future_field: 42
nested_future:
  a: 1
";
        let m: ProjectManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(m.name, "future");
    }

    #[test]
    fn partial_capabilities_default_missing_to_false() {
        let yaml = "\
project_uuid: 0192f3a2-0000-7000-8000-000000000000
name: partial
version: \"1\"
created_at: \"2026-06-02T00:00:00Z\"
ir_version: 0.1.0
graphforge_version: 0.5.0
capabilities:
  embeddings: true
";
        let m: ProjectManifest = serde_yaml::from_str(yaml).unwrap();
        let caps = m.capabilities.unwrap();
        assert!(caps.embeddings);
        // Unlisted flags fall back to false via #[serde(default)].
        assert!(!caps.documents);
        assert!(!caps.topology);
    }
}
