//! Human-authored ontology module text shared by native and WASM validators.

use crate::{DiscoveryError, DiscoveryErrorCode};

/// Required summary capability when module labels or descriptions are present.
pub const ONTOLOGY_MODULE_METADATA_CAPABILITY: &str = "ontology-module-metadata";
/// Maximum UTF-8 bytes of a module's single-line label.
pub const MAX_ONTOLOGY_MODULE_LABEL_BYTES: usize = 256;
/// Maximum UTF-8 bytes of a module's plain-text description.
pub const MAX_ONTOLOGY_MODULE_DESCRIPTION_BYTES: usize = 4096;

/// Validate optional module text without trimming or Unicode normalization.
///
/// Present values must contain non-whitespace text. Labels contain no control
/// characters or Unicode line/paragraph separators. Descriptions allow LF and
/// tab, but no other control characters. Both are Unicode strings, not locale
/// maps; their exact UTF-8 content contributes to the module's canonical digest.
pub fn validate_ontology_module_metadata(
    label: Option<&str>,
    description: Option<&str>,
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_text_and_exact_utf8_bounds_are_shared() {
        let label = "é".repeat(MAX_ONTOLOGY_MODULE_LABEL_BYTES / 2);
        let description = "界".repeat(MAX_ONTOLOGY_MODULE_DESCRIPTION_BYTES / 3);
        validate_ontology_module_metadata(Some(&label), Some(&description)).unwrap();
        assert!(validate_ontology_module_metadata(Some(&(label + "x")), None).is_err());
        assert!(validate_ontology_module_metadata(None, Some(&(description + "xy"))).is_err());
        validate_ontology_module_metadata(Some("研究 ontology"), Some("First line\n\tSecond line"))
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
                validate_ontology_module_metadata(Some(label), None).is_err(),
                "{label:?}"
            );
        }
        for description in ["", "\n\t", "a\rb", "a\0b", "a\u{85}b"] {
            assert!(
                validate_ontology_module_metadata(None, Some(description)).is_err(),
                "{description:?}"
            );
        }
    }
}
