//! Arrow schema constants for all ontology runtime tables.

use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

// ---------------------------------------------------------------------------
// ontology_meta
// ---------------------------------------------------------------------------

/// Schema for the single-row ontology metadata table.
pub static ONTOLOGY_META_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("ontology_id", DataType::Utf8, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("ir_min_version", DataType::Utf8, false),
        Field::new("checksum", DataType::Utf8, false),
    ]))
});

// ---------------------------------------------------------------------------
// entity_types
// ---------------------------------------------------------------------------

/// Schema for the entity type registry table.
pub static ENTITY_TYPES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("entity_type_id", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("abstract", DataType::Boolean, false),
        Field::new("parent_type_id", DataType::UInt32, true),
    ]))
});

// ---------------------------------------------------------------------------
// relation_types
// ---------------------------------------------------------------------------

/// Schema for the relation type registry table.
pub static RELATION_TYPES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("relation_type_id", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("src_type_id", DataType::UInt32, false),
        Field::new("dst_type_id", DataType::UInt32, false),
        Field::new("inverse_relation_type_id", DataType::UInt32, true),
    ]))
});

// ---------------------------------------------------------------------------
// property_types
// ---------------------------------------------------------------------------

/// Schema for the property type registry table.
pub static PROPERTY_TYPES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("property_type_id", DataType::UInt32, false),
        Field::new("owner_kind", DataType::Utf8, false),
        Field::new("owner_type_id", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value_type", DataType::Utf8, false),
        Field::new("nullable", DataType::Boolean, false),
        Field::new("multivalued", DataType::Boolean, false),
    ]))
});

// ---------------------------------------------------------------------------
// type_constraints
// ---------------------------------------------------------------------------

/// Schema for the type constraint table.
pub static TYPE_CONSTRAINTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("constraint_id", DataType::UInt32, false),
        Field::new("owner_kind", DataType::Utf8, false),
        Field::new("owner_type_id", DataType::UInt32, false),
        Field::new("constraint_kind", DataType::Utf8, false),
        Field::new("expr_json", DataType::Utf8, true),
    ]))
});

// ---------------------------------------------------------------------------
// cardinality_rules
// ---------------------------------------------------------------------------

/// Schema for the cardinality rules table.
/// Populated in a future milestone when `OntologyDoc` gains cardinality fields.
pub static CARDINALITY_RULES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("rule_id", DataType::UInt32, false),
        Field::new("relation_type_id", DataType::UInt32, false),
        Field::new("endpoint", DataType::Utf8, false),
        Field::new("min_count", DataType::UInt32, false),
        Field::new("max_count", DataType::UInt32, true),
    ]))
});

// ---------------------------------------------------------------------------
// semantic_flags
// ---------------------------------------------------------------------------

/// Schema for the semantic flags table (one row per relation type).
pub static SEMANTIC_FLAGS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("owner_kind", DataType::Utf8, false),
        Field::new("owner_type_id", DataType::UInt32, false),
        Field::new("transitive", DataType::Boolean, false),
        Field::new("symmetric", DataType::Boolean, false),
        Field::new("reflexive", DataType::Boolean, false),
        Field::new("functional", DataType::Boolean, false),
        Field::new("acyclic", DataType::Boolean, false),
    ]))
});

// ---------------------------------------------------------------------------
// aliases
// ---------------------------------------------------------------------------

/// Schema for the aliases table.
/// Populated in a future milestone when `OntologyDoc` gains alias fields.
pub static ALIASES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("owner_kind", DataType::Utf8, false),
        Field::new("owner_type_id", DataType::UInt32, false),
        Field::new("alias", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, true),
    ]))
});
