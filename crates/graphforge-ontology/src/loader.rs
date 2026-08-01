//! YAML and JSON loader for ontology definition files.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::OntologyError;
use crate::ontology::OntologyDoc;
use crate::validator::OntologyValidator;

/// Loads an [`OntologyDoc`] from a YAML or JSON source.
///
/// ```no_run
/// use graphforge_ontology::OntologyLoader;
/// use std::path::Path;
///
/// let doc = OntologyLoader::load_file(Path::new("schema.yaml")).unwrap();
/// println!("{}", doc.ontology_id);
/// ```
pub struct OntologyLoader;

impl OntologyLoader {
    /// Deserialise an [`OntologyDoc`] from a YAML byte stream and validate it.
    ///
    /// # Errors
    /// - [`OntologyError::Yaml`] on parse failure.
    /// - [`OntologyError::Validation`] if semantic rules are violated.
    pub fn load_yaml(reader: impl Read) -> Result<OntologyDoc, OntologyError> {
        let doc: OntologyDoc = serde_yaml::from_reader(reader)?;
        Self::validate(doc)
    }

    /// Deserialise an [`OntologyDoc`] from a JSON byte stream and validate it.
    ///
    /// # Errors
    /// - [`OntologyError::Json`] on parse failure.
    /// - [`OntologyError::Validation`] if semantic rules are violated.
    pub fn load_json(reader: impl Read) -> Result<OntologyDoc, OntologyError> {
        let doc: OntologyDoc = serde_json::from_reader(reader)?;
        Self::validate(doc)
    }

    /// Run the semantic validator and map errors into [`OntologyError::Validation`].
    fn validate(doc: OntologyDoc) -> Result<OntologyDoc, OntologyError> {
        OntologyValidator::validate(&doc).map_err(|errors| OntologyError::Validation {
            id: doc.ontology_id.clone(),
            count: errors.len(),
            errors,
        })?;
        Ok(doc)
    }

    /// Load an [`OntologyDoc`] from a file, inferring the format from the
    /// file extension.
    ///
    /// Recognised extensions (case-insensitive):
    /// - `.yaml`, `.yml` → YAML
    /// - `.json` → JSON
    ///
    /// # Errors
    /// - [`OntologyError::Io`] if the file cannot be opened or read.
    /// - [`OntologyError::UnknownFormat`] if the extension is not recognised.
    /// - [`OntologyError::Yaml`] / [`OntologyError::Json`] on parse failure.
    pub fn load_file(path: &Path) -> Result<OntologyDoc, OntologyError> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let file = File::open(path)?;

        match ext.as_str() {
            "yaml" | "yml" => Self::load_yaml(file),
            "json" => Self::load_json(file),
            other => Err(OntologyError::UnknownFormat {
                extension: other.to_owned(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::{EntityTypeDef, OntologyDoc};
    use std::io::Cursor;

    fn sample_doc() -> OntologyDoc {
        OntologyDoc {
            ontology_id: "test".to_string(),
            version: "1.0".to_string(),
            entity_types: vec![EntityTypeDef {
                name: "Person".to_string(),
                r#abstract: false,
                parent: None,
            }],
            relation_types: vec![],
            properties: vec![],
            constraints: vec![],
            migrations: vec![],
        }
    }

    // --- stream-based tests (no tempfiles needed) ---

    #[test]
    fn load_yaml_from_reader() {
        let doc = sample_doc();
        let yaml = serde_yaml::to_string(&doc).unwrap();
        let loaded = OntologyLoader::load_yaml(Cursor::new(yaml.as_bytes())).unwrap();
        assert_eq!(doc, loaded);
    }

    #[test]
    fn load_json_from_reader() {
        let doc = sample_doc();
        let json = serde_json::to_string(&doc).unwrap();
        let loaded = OntologyLoader::load_json(Cursor::new(json.as_bytes())).unwrap();
        assert_eq!(doc, loaded);
    }

    #[test]
    fn load_yaml_example_from_docs() {
        // Extended from the storage.md example to include MANAGED_BY so the
        // mutual-inverse rule is satisfied (the validator now runs after parse).
        let yaml = r#"
ontology_id: core
version: "2026.05"
entity_types:
  - name: Person
    abstract: false
  - name: Employee
    parent: Person
relation_types:
  - name: MANAGES
    src: Employee
    dst: Employee
    inverse: MANAGED_BY
    semantic:
      transitive: false
      symmetric: false
      functional: false
  - name: MANAGED_BY
    src: Employee
    dst: Employee
    inverse: MANAGES
properties:
  - name: name
    owner: Person
    type: utf8
    nullable: false
constraints:
  - owner: Employee
    kind: unique_property
"#;
        let doc = OntologyLoader::load_yaml(Cursor::new(yaml.as_bytes())).unwrap();
        assert_eq!(doc.ontology_id, "core");
        assert_eq!(doc.entity_types.len(), 2);
        assert_eq!(doc.relation_types[0].inverse.as_deref(), Some("MANAGED_BY"));
    }

    #[test]
    fn load_yaml_invalid_returns_error() {
        let bad = b": invalid: [yaml: {";
        let result = OntologyLoader::load_yaml(Cursor::new(bad));
        assert!(
            matches!(result, Err(OntologyError::Yaml(_))),
            "expected Yaml error, got {result:?}"
        );
    }

    #[test]
    fn load_json_invalid_returns_error() {
        let bad = b"{not valid json";
        let result = OntologyLoader::load_json(Cursor::new(bad));
        assert!(
            matches!(result, Err(OntologyError::Json(_))),
            "expected Json error, got {result:?}"
        );
    }

    // --- file-based tests (tempfile) ---

    #[test]
    fn load_file_yaml_extension() {
        let doc = sample_doc();
        let mut f = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        serde_yaml::to_writer(&mut f, &doc).unwrap();
        let loaded = OntologyLoader::load_file(f.path()).unwrap();
        assert_eq!(doc, loaded);
    }

    #[test]
    fn load_file_yml_extension() {
        let doc = sample_doc();
        let mut f = tempfile::Builder::new().suffix(".yml").tempfile().unwrap();
        serde_yaml::to_writer(&mut f, &doc).unwrap();
        let loaded = OntologyLoader::load_file(f.path()).unwrap();
        assert_eq!(doc, loaded);
    }

    #[test]
    fn load_file_json_extension() {
        let doc = sample_doc();
        let mut f = tempfile::Builder::new().suffix(".json").tempfile().unwrap();
        serde_json::to_writer(&mut f, &doc).unwrap();
        let loaded = OntologyLoader::load_file(f.path()).unwrap();
        assert_eq!(doc, loaded);
    }

    #[test]
    fn load_file_unknown_extension_returns_error() {
        let f = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        let result = OntologyLoader::load_file(f.path());
        assert!(
            matches!(result, Err(OntologyError::UnknownFormat { .. })),
            "expected UnknownFormat error, got {result:?}"
        );
    }

    #[test]
    fn load_file_nonexistent_returns_io_error() {
        let result = OntologyLoader::load_file(Path::new("/nonexistent/path/to/schema.yaml"));
        assert!(
            matches!(result, Err(OntologyError::Io(_))),
            "expected Io error, got {result:?}"
        );
    }
}
