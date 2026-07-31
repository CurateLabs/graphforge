//! Serde data model for ontology definition files (YAML / JSON).
//!
//! This module defines the Rust types that an ontology file deserialises into.
//! No I/O, validation, or Arrow compilation is performed here — those are
//! handled by the loader (`gf-ontology::loader`) and compiler (`gf-ontology::compiler`).

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Top-level document
// ---------------------------------------------------------------------------

/// A complete ontology definition as loaded from a YAML or JSON file.
///
/// ```yaml
/// ontology_id: core
/// version: "2026.05"
/// entity_types:
///   - name: Person
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OntologyDoc {
    /// Stable identifier for this ontology (e.g. `"core"`, `"acme-kg"`).
    pub ontology_id: String,
    /// Semver-style version string (e.g. `"2026.05"`).
    pub version: String,
    /// Node class definitions.
    #[serde(default)]
    pub entity_types: Vec<EntityTypeDef>,
    /// Edge class definitions.
    #[serde(default)]
    pub relation_types: Vec<RelationTypeDef>,
    /// Property definitions.
    #[serde(default)]
    pub properties: Vec<PropertyDef>,
    /// Validation constraints.
    #[serde(default)]
    pub constraints: Vec<ConstraintDef>,
    /// Versioned upgrade transforms. Authored order is semantic: the migration
    /// planner uses it to break ties between equal-length routes.
    #[serde(default)]
    pub migrations: Vec<MigrationDef>,
}

// ---------------------------------------------------------------------------
// Entity types
// ---------------------------------------------------------------------------

/// A node class definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityTypeDef {
    /// Unique type name within this ontology (e.g. `"Person"`).
    pub name: String,
    /// If `true`, no instances of this type may be created directly.
    #[serde(default)]
    pub r#abstract: bool,
    /// Optional parent type for single-inheritance hierarchies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
}

// ---------------------------------------------------------------------------
// Relation types
// ---------------------------------------------------------------------------

/// An edge class definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationTypeDef {
    /// Unique type name (e.g. `"MANAGES"`).
    pub name: String,
    /// Source entity type name.
    pub src: String,
    /// Destination entity type name.
    pub dst: String,
    /// Name of the logical inverse relation (e.g. `"MANAGED_BY"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inverse: Option<String>,
    /// Semantic properties used by the planner.
    #[serde(default)]
    pub semantic: SemanticFlags,
}

/// Semantic properties of a relation type used during query planning.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SemanticFlags {
    /// `(a)-[:R]->(b)` and `(b)-[:R]->(c)` implies `(a)-[:R]->(c)`.
    #[serde(default)]
    pub transitive: bool,
    /// `(a)-[:R]->(b)` implies `(b)-[:R]->(a)`.
    #[serde(default)]
    pub symmetric: bool,
    /// `(a)-[:R]->(a)` holds for all `a`.
    #[serde(default)]
    pub reflexive: bool,
    /// Each node has at most one outgoing `R` edge.
    #[serde(default)]
    pub functional: bool,
    /// Each node has at most one incoming `R` edge.
    #[serde(default)]
    pub inverse_functional: bool,
    /// No cycles exist in the `R` subgraph.
    #[serde(default)]
    pub acyclic: bool,
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

/// A property definition attached to an entity or relation type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyDef {
    /// Name of the owning entity or relation type.
    pub owner: String,
    /// Property name (e.g. `"name"`).
    pub name: String,
    /// Value type.
    #[serde(rename = "type")]
    pub value_type: PropertyValueType,
    /// Whether `null` is a valid value. Defaults to `true`.
    #[serde(default = "default_true")]
    pub nullable: bool,
    /// Whether this property holds a list of values.
    #[serde(default)]
    pub multivalued: bool,
    /// Default value serialised as a JSON string (e.g. `"\"unknown\""`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_json: Option<String>,
}

/// Scalar and collection value types for ontology properties.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyValueType {
    /// Variable-length UTF-8 string.
    Utf8,
    /// 64-bit signed integer.
    Int64,
    /// 64-bit IEEE 754 float.
    Float64,
    /// Boolean.
    Bool,
    /// ISO 8601 duration.
    Duration,
    /// ISO 8601 datetime (timezone-aware).
    DateTime,
    /// Ordered list of values (homogeneous type TBD by validator).
    List,
    /// Key-value map (string keys, heterogeneous values).
    Map,
}

// ---------------------------------------------------------------------------
// Constraints
// ---------------------------------------------------------------------------

/// A validation constraint on an entity or relation type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConstraintDef {
    /// Name of the owning entity or relation type.
    pub owner: String,
    /// Constraint category.
    pub kind: ConstraintKind,
    /// Constraint expression serialised as a JSON object.
    /// Typed interpretation is deferred to the validator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expr_json: Option<String>,
}

/// Category of a `ConstraintDef`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintKind {
    /// The named property must be unique across all nodes of the owner type.
    UniqueProperty,
    /// The named property must be present and non-null.
    RequiredProperty,
    /// The property value must fall within a numeric or temporal range.
    RangeCheck,
    /// Arbitrary expression-based constraint (evaluated by the validator).
    CustomExpr,
}

// ---------------------------------------------------------------------------
// Migrations
// ---------------------------------------------------------------------------

/// A versioned ontology upgrade transform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationDef {
    /// Source version this migration applies from.
    pub from_version: String,
    /// Target version this migration produces.
    pub to_version: String,
    /// Migration strategy identifier (e.g. `"add_property"`, `"rename_type"`).
    pub transform_kind: String,
    /// Path or URI to an external migration script.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script_ref: Option<String>,
    /// SHA-256 checksum of the script for integrity verification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_doc() -> OntologyDoc {
        OntologyDoc {
            ontology_id: "test".to_string(),
            version: "1.0".to_string(),
            entity_types: vec![EntityTypeDef {
                name: "Person".to_string(),
                r#abstract: false,
                parent: None,
            }],
            relation_types: vec![RelationTypeDef {
                name: "KNOWS".to_string(),
                src: "Person".to_string(),
                dst: "Person".to_string(),
                inverse: None,
                semantic: SemanticFlags::default(),
            }],
            properties: vec![PropertyDef {
                owner: "Person".to_string(),
                name: "name".to_string(),
                value_type: PropertyValueType::Utf8,
                nullable: false,
                multivalued: false,
                default_json: None,
            }],
            constraints: vec![ConstraintDef {
                owner: "Person".to_string(),
                kind: ConstraintKind::UniqueProperty,
                expr_json: Some(r#"{"property":"id"}"#.to_string()),
            }],
            migrations: vec![],
        }
    }

    #[test]
    fn json_roundtrip() {
        let doc = minimal_doc();
        let json = serde_json::to_string(&doc).expect("serialize");
        let back: OntologyDoc = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(doc, back);
    }

    #[test]
    fn yaml_roundtrip() {
        let doc = minimal_doc();
        let yaml = serde_yaml::to_string(&doc).expect("serialize");
        let back: OntologyDoc = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(doc, back);
    }

    #[test]
    fn semantic_flags_default_all_false() {
        let f = SemanticFlags::default();
        assert!(!f.transitive);
        assert!(!f.symmetric);
        assert!(!f.reflexive);
        assert!(!f.functional);
        assert!(!f.inverse_functional);
        assert!(!f.acyclic);
    }

    #[test]
    fn property_nullable_defaults_true() {
        // When `nullable` is absent from JSON, it should default to `true`.
        let json = r#"{"owner":"Person","name":"age","type":"int64"}"#;
        let p: PropertyDef = serde_json::from_str(json).expect("deserialize");
        assert!(p.nullable, "nullable should default to true");
    }

    #[test]
    fn doc_example_from_storage_md() {
        // The exact example from docs/book/architecture/storage.md must parse without error.
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
properties:
  - name: name
    owner: Person
    type: utf8
    nullable: false
constraints:
  - owner: Employee
    kind: unique_property
"#;
        let doc: OntologyDoc = serde_yaml::from_str(yaml).expect("parse storage.md example");
        assert_eq!(doc.ontology_id, "core");
        assert_eq!(doc.entity_types.len(), 2);
        assert_eq!(doc.relation_types[0].inverse.as_deref(), Some("MANAGED_BY"));
        assert!(!doc.relation_types[0].semantic.transitive);
    }

    #[test]
    fn property_value_type_roundtrip() {
        let types = [
            PropertyValueType::Utf8,
            PropertyValueType::Int64,
            PropertyValueType::Float64,
            PropertyValueType::Bool,
            PropertyValueType::Duration,
            PropertyValueType::DateTime,
            PropertyValueType::List,
            PropertyValueType::Map,
        ];
        for t in &types {
            let json = serde_json::to_string(t).unwrap();
            let back: PropertyValueType = serde_json::from_str(&json).unwrap();
            assert_eq!(t, &back, "failed roundtrip for {json}");
        }
    }

    #[test]
    fn constraint_kind_roundtrip() {
        let kinds = [
            ConstraintKind::UniqueProperty,
            ConstraintKind::RequiredProperty,
            ConstraintKind::RangeCheck,
            ConstraintKind::CustomExpr,
        ];
        for k in &kinds {
            let json = serde_json::to_string(k).unwrap();
            let back: ConstraintKind = serde_json::from_str(&json).unwrap();
            assert_eq!(k, &back, "failed roundtrip for {json}");
        }
    }

    #[test]
    fn empty_doc_deserializes() {
        // Minimal valid doc — only required fields.
        let json = r#"{"ontology_id":"empty","version":"0.1"}"#;
        let doc: OntologyDoc = serde_json::from_str(json).expect("deserialize");
        assert!(doc.entity_types.is_empty());
        assert!(doc.migrations.is_empty());
    }
}
