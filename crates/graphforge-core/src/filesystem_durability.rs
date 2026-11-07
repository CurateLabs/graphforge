//! Filesystem qualification evidence, independent of project paths.
use serde::{Deserialize, Serialize};

/// Warning code for a filesystem whose durability is not qualified.
pub const UNVERIFIED_FILESYSTEM_CODE: &str = "GF_UNVERIFIED_FILESYSTEM";

/// Last writer's unverified filesystem qualification, authenticated by the generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnverifiedDurability {
    /// Admission phase, currently `CLASSIFY`.
    pub phase: String,
    /// Safe classification cause.
    pub cause: String,
    /// Observed filesystem class, never a device or path.
    pub filesystem_type: String,
}

impl UnverifiedDurability {
    /// Produce a path-free runtime warning.
    #[must_use]
    pub fn warning(&self) -> FilesystemDurabilityWarning {
        FilesystemDurabilityWarning {
            code: UNVERIFIED_FILESYSTEM_CODE.into(),
            phase: self.phase.clone(),
            outcome: "unverified".into(),
            cause: self.cause.clone(),
            filesystem_type: self.filesystem_type.clone(),
            message: "Filesystem durability is unverified; publication primitives passed, but persistence is not guaranteed. tmpfs and recycled notebook runtimes lose data. Export to persistent storage. Set GRAPHFORGE_FILESYSTEM_DURABILITY=strict to refuse unproven filesystems.".into(),
        }
    }
}

/// Structured warning emitted once per project open, never per write.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FilesystemDurabilityWarning {
    /// Always `GF_UNVERIFIED_FILESYSTEM`.
    pub code: String,
    /// Admission phase.
    pub phase: String,
    /// Always `unverified`.
    pub outcome: String,
    /// Safe classification cause.
    pub cause: String,
    /// Filesystem class.
    pub filesystem_type: String,
    /// Path-free guidance.
    pub message: String,
}

/// Shared warnings; untagged serialization preserves existing Core warning JSON.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Warning {
    /// Data produced by another Core.
    CoreVersion(crate::core_version::CoreVersionWarning),
    /// Open admitted on an unproven filesystem.
    FilesystemDurability(FilesystemDurabilityWarning),
}
impl Warning {
    /// Stable diagnostic code.
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::CoreVersion(w) => &w.code,
            Self::FilesystemDurability(w) => &w.code,
        }
    }
    /// Human-readable guidance.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::CoreVersion(w) => &w.message,
            Self::FilesystemDurability(w) => &w.message,
        }
    }
}
impl From<crate::core_version::CoreVersionWarning> for Warning {
    fn from(w: crate::core_version::CoreVersionWarning) -> Self {
        Self::CoreVersion(w)
    }
}
impl From<FilesystemDurabilityWarning> for Warning {
    fn from(w: FilesystemDurabilityWarning) -> Self {
        Self::FilesystemDurability(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn warning_union_preserves_existing_wire_json() {
        let old = crate::core_version::check_core_version(Some("0.0.1")).unwrap();
        assert_eq!(
            serde_json::to_value(&old).unwrap(),
            serde_json::to_value(Warning::from(old)).unwrap()
        );
        let evidence = UnverifiedDurability {
            phase: "CLASSIFY".into(),
            cause: "filesystem_class_unproven".into(),
            filesystem_type: "tmpfs".into(),
        };
        let bytes = serde_json::to_vec(&evidence).unwrap();
        assert_eq!(
            serde_json::from_slice::<UnverifiedDurability>(&bytes).unwrap(),
            evidence
        );
        let warning = Warning::from(evidence.warning());
        assert_eq!(
            serde_json::from_value::<Warning>(serde_json::to_value(&warning).unwrap()).unwrap(),
            warning
        );
    }
}
