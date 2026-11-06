//! Human-authored ontology module metadata shared by native and WASM validators.

use crate::{DiscoveryError, DiscoveryErrorCode, ItemId, ItemKind, Sha256Digest};
use serde::{Deserialize, Serialize};

/// Exact immediate source of an authored ontology fork. This declaration is
/// content, not proof of ownership or a substitute for the source module bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct OntologyForkOrigin {
    /// Canonical permanent ontology URI (`https://graphforge.sh/ont_…`).
    pub id: String,
    /// Exact opaque authored version. Discovery applies its existing public string bounds.
    pub version: String,
    /// Domain-separated source module digest, `sha256:` plus 64 lowercase hex digits.
    pub digest: Sha256Digest,
}

impl OntologyForkOrigin {
    /// Validate without rewriting any part of the source identity.
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        let valid_id = ItemId::parse_uri(&self.id)
            .is_ok_and(|id| id.kind() == ItemKind::OntologyModule && id.to_uri() == self.id);
        if !valid_id || self.digest.validate().is_err() {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MalformedResponse,
                Some("forked_from"),
                "fork origin requires a canonical ontology URI and SHA-256 digest",
            ));
        }
        Ok(())
    }
}

/// Required summary capability when module text, license or fork provenance is present.
pub const ONTOLOGY_MODULE_METADATA_CAPABILITY: &str = "ontology-module-metadata";
/// Maximum UTF-8 bytes of a module's single-line label.
pub const MAX_ONTOLOGY_MODULE_LABEL_BYTES: usize = 256;
/// Maximum UTF-8 bytes of a module's plain-text description.
pub const MAX_ONTOLOGY_MODULE_DESCRIPTION_BYTES: usize = 4096;

/// Maximum UTF-8 bytes of a module's SPDX license expression.
pub const MAX_ONTOLOGY_MODULE_LICENSE_BYTES: usize = 4096;

/// Validate optional module metadata without trimming or Unicode normalization.
///
/// Present values must contain non-whitespace text. Labels contain no control
/// characters or Unicode line/paragraph separators. Descriptions allow LF and
/// tab, but no other control characters. Both are Unicode strings, not locale
/// maps. Licenses use `spdx::Expression::parse` (strict mode), bounded to 4096
/// ASCII bytes without controls. No field is normalized; its exact content
/// contributes to the module's canonical digest.
pub fn validate_ontology_module_metadata(
    label: Option<&str>,
    description: Option<&str>,
    license: Option<&str>,
) -> Result<(), DiscoveryError> {
    for (field, value, max_bytes, multiline) in [
        ("label", label, MAX_ONTOLOGY_MODULE_LABEL_BYTES, false),
        (
            "description",
            description,
            MAX_ONTOLOGY_MODULE_DESCRIPTION_BYTES,
            true,
        ),
    ] {
        let Some(value) = value else { continue };
        if value.trim().is_empty()
            || value.len() > max_bytes
            || value.chars().any(|character| {
                (character.is_control() && !(multiline && matches!(character, '\n' | '\t')))
                    || (!multiline && matches!(character, '\u{2028}' | '\u{2029}'))
            })
        {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MalformedResponse,
                Some(field),
                "ontology module text is empty, oversized, or contains a forbidden control character",
            ));
        }
    }
    if let Some(license) = license {
        // Bound parser input before allocating its token/expression buffers.
        if license.is_empty()
            || license.len() > MAX_ONTOLOGY_MODULE_LICENSE_BYTES
            || !license.is_ascii()
            || license.bytes().any(|byte| byte.is_ascii_control())
            || spdx::Expression::parse(license).is_err()
        {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MalformedResponse,
                Some("license"),
                "ontology module license must be an SPDX expression of at most 4096 ASCII bytes without control characters",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_text_and_exact_utf8_bounds_are_shared() {
        let label = "é".repeat(MAX_ONTOLOGY_MODULE_LABEL_BYTES / 2);
        let description = "界".repeat(MAX_ONTOLOGY_MODULE_DESCRIPTION_BYTES / 3);
        validate_ontology_module_metadata(Some(&label), Some(&description), None).unwrap();
        assert!(validate_ontology_module_metadata(Some(&(label + "x")), None, None).is_err());
        assert!(
            validate_ontology_module_metadata(None, Some(&(description + "xy")), None).is_err()
        );
        validate_ontology_module_metadata(
            Some("研究 ontology"),
            Some("First line\n\tSecond line"),
            None,
        )
        .unwrap();
    }

    #[test]
    fn empty_text_controls_and_multiline_labels_are_refused() {
        for label in [
            "",
            " \t",
            "a\nb",
            "a\rb",
            "a\u{2028}b",
            "a\u{2029}b",
            "a\u{85}b",
        ] {
            assert!(
                validate_ontology_module_metadata(Some(label), None, None).is_err(),
                "{label:?}"
            );
        }
        for description in ["", "\n\t", "a\rb", "a\0b", "a\u{85}b"] {
            assert!(
                validate_ontology_module_metadata(None, Some(description), None).is_err(),
                "{description:?}"
            );
        }
    }
}
