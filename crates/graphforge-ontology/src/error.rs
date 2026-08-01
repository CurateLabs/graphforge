//! Error types for ontology loading, validation, and migration.

use std::fmt;

// ---------------------------------------------------------------------------
// Validation error
// ---------------------------------------------------------------------------

/// The semantic category of a load-time validation violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationErrorKind {
    /// Two items share a name that must be unique (e.g. two entity types named `"Person"`).
    DuplicateName,
    /// A reference points to a name that is not declared in the ontology.
    UnresolvedReference,
    /// The inheritance graph contains a cycle.
    InheritanceCycle,
    /// An inverse relation pair is declared inconsistently.
    InverseInconsistency,
    /// A migration's `from_version` is not strictly less than its `to_version`.
    MigrationVersionOrder,
}

impl fmt::Display for ValidationErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateName => write!(f, "DuplicateName"),
            Self::UnresolvedReference => write!(f, "UnresolvedReference"),
            Self::InheritanceCycle => write!(f, "InheritanceCycle"),
            Self::InverseInconsistency => write!(f, "InverseInconsistency"),
            Self::MigrationVersionOrder => write!(f, "MigrationVersionOrder"),
        }
    }
}

/// A single semantic violation found during load-time validation.
#[derive(Debug, Clone, PartialEq)]
pub struct OntologyValidationError {
    /// Category of the violation.
    pub kind: ValidationErrorKind,
    /// Human-readable field path, e.g. `"entity_types[1].parent"`.
    pub location: String,
    /// Human-readable description of the violation.
    pub message: String,
}

impl fmt::Display for OntologyValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.kind, self.location, self.message)
    }
}

// ---------------------------------------------------------------------------
// Top-level crate error
// ---------------------------------------------------------------------------

/// All errors produced by the `graphforge-ontology` crate.
#[derive(Debug, thiserror::Error)]
pub enum OntologyError {
    /// An I/O error occurred while opening or reading the ontology file.
    #[error("I/O error reading ontology: {0}")]
    Io(#[from] std::io::Error),

    /// The input could not be deserialised as valid YAML.
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// The input could not be deserialised as valid JSON.
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    /// The file extension is not recognised as a supported ontology format.
    #[error("unknown ontology file format '{extension}' — expected .yaml, .yml, or .json")]
    UnknownFormat {
        /// The unrecognised extension (without leading dot), e.g. `"toml"`.
        extension: String,
    },

    /// The ontology document failed load-time semantic validation.
    #[error("{count} validation error(s) in ontology '{id}'")]
    Validation {
        /// The `ontology_id` of the failing document.
        id: String,
        /// Number of violations (equals `errors.len()`).
        count: usize,
        /// All violations collected during validation.
        errors: Vec<OntologyValidationError>,
    },

    /// The ontology document is structurally valid but violates a schema rule.
    /// Kept for compatibility; prefer [`OntologyError::Validation`] for new code.
    #[error("ontology schema mismatch: {message}")]
    SchemaMismatch {
        /// Human-readable description of the violation with field-path context.
        message: String,
    },

    /// An Arrow error occurred while building the ontology runtime tables.
    #[error("Arrow error building ontology runtime: {0}")]
    Arrow(String),

    /// A Parquet I/O error occurred while reading or writing ontology tables.
    #[error("Parquet error: {0}")]
    Parquet(String),

    /// The checksum of a loaded Parquet snapshot does not match the expected value.
    /// The caller should re-compile the ontology from its source YAML/JSON.
    #[error("ontology checksum mismatch: cached={cached} computed={computed}")]
    ChecksumMismatch {
        /// Checksum stored in the Parquet snapshot.
        cached: String,
        /// Checksum freshly computed from the `OntologyDoc`.
        computed: String,
    },

    /// No sequence of migration steps connects the two ontology versions.
    #[error("no migration path from '{from}' to '{to}'")]
    NoMigrationPath {
        /// The version the dataset is currently at.
        from: String,
        /// The version required by the current ontology.
        to: String,
    },
}
