//! Filesystem qualification evidence, independent of project paths.
use serde::{Deserialize, Serialize};

/// Warning code for a filesystem whose durability is not qualified.
pub const UNVERIFIED_FILESYSTEM_CODE: &str = "GF_UNVERIFIED_FILESYSTEM";

/// Last writer's unverified filesystem qualification, authenticated by the generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnverifiedDurability {
    /// Admission phase, currently `CLASSIFY`.
    #[serde(deserialize_with = "diagnostic_token")]
    pub phase: String,
    /// Safe classification cause.
    #[serde(deserialize_with = "diagnostic_token")]
    pub cause: String,
    /// Observed filesystem class, never a device or path.
    #[serde(deserialize_with = "diagnostic_token")]
    pub filesystem_type: String,
}

// Generation and portable manifests are untrusted input. These machine tokens
// reach text receipts, so accept bounded ASCII names, never terminal controls,
// whitespace, path separators or Unicode display controls.
fn diagnostic_token<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(serde::de::Error::custom(
            "invalid filesystem durability diagnostic token",
        ));
    }
    Ok(value)
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
    fn untrusted_diagnostic_fields_are_bounded_terminal_safe_tokens() {
        let valid = serde_json::json!({
            "phase": "CLASSIFY",
            "cause": "filesystem_class_unproven",
            "filesystem_type": "0x794c7630"
        });
        assert!(serde_json::from_value::<UnverifiedDurability>(valid.clone()).is_ok());
        for field in ["phase", "cause", "filesystem_type"] {
            for invalid in [
                "",
                "\u{1b}[2J",
                "tmpfs\nforged receipt",
                "tmpfs\rhidden",
                "tmpfs\tvalue",
                "tmpfs\u{009b}2J",
                "tmpfs\u{202e}value",
                "/private/project",
                "C:\\private\\project",
                &"a".repeat(129),
            ] {
                let mut poisoned = valid.clone();
                poisoned[field] = invalid.into();
                let error = serde_json::from_value::<UnverifiedDurability>(poisoned).unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("invalid filesystem durability diagnostic token")
                );
                assert!(!error.to_string().contains(invalid) || invalid.is_empty());
            }
        }
    }

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
