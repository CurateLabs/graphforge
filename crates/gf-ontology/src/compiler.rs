//! Compiles a validated [`OntologyDoc`] into Arrow runtime tables.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, RecordBatch, StringBuilder, UInt32Builder};
use sha2::{Digest, Sha256};

use crate::error::OntologyError;
use crate::ontology::{OntologyDoc, PropertyValueType};
use crate::schemas::{
    ALIASES_SCHEMA, CARDINALITY_RULES_SCHEMA, ENTITY_TYPES_SCHEMA, ONTOLOGY_META_SCHEMA,
    PROPERTY_TYPES_SCHEMA, RELATION_TYPES_SCHEMA, SEMANTIC_FLAGS_SCHEMA, TYPE_CONSTRAINTS_SCHEMA,
};

// ---------------------------------------------------------------------------
// OntologyRuntime
// ---------------------------------------------------------------------------

/// Namespace of the ontology type that declares a property.
///
/// Entity and relation type IDs are allocated independently, so the owner kind
/// is part of a property's lookup identity even when the numeric IDs match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PropertyOwnerKind {
    /// A node/entity type owns the property.
    Entity,
    /// A relationship/relation type owns the property.
    Relation,
}

/// The compiled, query-ready form of an [`OntologyDoc`].
///
/// All string-heavy lookups are replaced by O(1) integer ID comparisons.
/// The Arrow tables mirror the runtime table spec in `docs/book/architecture/storage.md`.
pub struct OntologyRuntime {
    // -----------------------------------------------------------------------
    // Arrow tables
    // -----------------------------------------------------------------------
    /// Single-row metadata table: `ontology_id`, `version`, `ir_min_version`, `checksum`.
    pub ontology_meta: RecordBatch,
    /// One row per entity type.
    pub entity_types: RecordBatch,
    /// One row per relation type.
    pub relation_types: RecordBatch,
    /// One row per property definition.
    pub property_types: RecordBatch,
    /// One row per constraint definition.
    pub type_constraints: RecordBatch,
    /// One row per relation type (semantic flags).
    pub semantic_flags: RecordBatch,
    /// Zero rows — populated in a future milestone when `OntologyDoc` gains cardinality fields.
    pub cardinality_rules: RecordBatch,
    /// Zero rows — populated in a future milestone when `OntologyDoc` gains alias fields.
    pub aliases: RecordBatch,

    // -----------------------------------------------------------------------
    // Hot-path lookup maps
    // -----------------------------------------------------------------------
    /// Entity type name → integer ID.
    pub entity_name_to_id: HashMap<String, u32>,
    /// Integer ID → entity type name.
    pub entity_id_to_name: HashMap<u32, String>,
    /// Relation type name → integer ID.
    pub relation_name_to_id: HashMap<String, u32>,
    /// Integer ID → relation type name.
    pub relation_id_to_name: HashMap<u32, String>,

    // -----------------------------------------------------------------------
    // Inheritance closure
    // -----------------------------------------------------------------------
    /// For each `entity_type_id`, the set of all ancestor IDs (parents, grandparents, …).
    pub ancestors: HashMap<u32, HashSet<u32>>,
    /// For each `entity_type_id`, the set of all descendant IDs.
    pub descendants: HashMap<u32, HashSet<u32>>,

    // -----------------------------------------------------------------------
    // Property lookup
    // -----------------------------------------------------------------------
    /// `(owner_kind, owner_type_id, property_name) → property_type_id`.
    ///
    /// The owner kind prevents equal numeric IDs from the independent entity
    /// and relation namespaces from colliding.
    pub property_name_to_id: HashMap<(PropertyOwnerKind, u32, String), u32>,
}

// ---------------------------------------------------------------------------
// OntologyCompiler
// ---------------------------------------------------------------------------

/// Compiles a validated [`OntologyDoc`] into an [`OntologyRuntime`].
pub struct OntologyCompiler;

impl OntologyCompiler {
    /// Compile `doc` into Arrow runtime tables and lookup structures.
    ///
    /// # Errors
    /// Returns [`OntologyError::Arrow`] if an Arrow array or RecordBatch cannot
    /// be constructed (schema mismatch, buffer overflow, etc.).
    pub fn compile(doc: &OntologyDoc) -> Result<OntologyRuntime, OntologyError> {
        // Step 1: Assign stable integer IDs.
        let entity_name_to_id: HashMap<String, u32> = doc
            .entity_types
            .iter()
            .enumerate()
            .map(|(i, e)| {
                (
                    e.name.clone(),
                    i.try_into().expect("ontology size exceeds u32"),
                )
            })
            .collect();
        let entity_id_to_name: HashMap<u32, String> = entity_name_to_id
            .iter()
            .map(|(k, &v)| (v, k.clone()))
            .collect();

        let relation_name_to_id: HashMap<String, u32> = doc
            .relation_types
            .iter()
            .enumerate()
            .map(|(i, r)| {
                (
                    r.name.clone(),
                    i.try_into().expect("ontology size exceeds u32"),
                )
            })
            .collect();
        let relation_id_to_name: HashMap<u32, String> = relation_name_to_id
            .iter()
            .map(|(k, &v)| (v, k.clone()))
            .collect();

        // Step 2: Build Arrow tables.
        let ontology_meta = build_ontology_meta(doc)?;
        let entity_types_batch = build_entity_types(doc, &entity_name_to_id)?;
        let relation_types_batch =
            build_relation_types(doc, &entity_name_to_id, &relation_name_to_id)?;
        let property_types_batch =
            build_property_types(doc, &entity_name_to_id, &relation_name_to_id)?;
        let type_constraints_batch =
            build_type_constraints(doc, &entity_name_to_id, &relation_name_to_id)?;
        let semantic_flags_batch = build_semantic_flags(doc, &relation_name_to_id)?;
        let cardinality_rules = empty_batch(&CARDINALITY_RULES_SCHEMA);
        let aliases = empty_batch(&ALIASES_SCHEMA);

        // Step 3: Precompute inheritance closure.
        let (ancestors, descendants) = build_inheritance_closure(doc, &entity_name_to_id);

        // Step 4: Build property lookup map.
        let property_name_to_id: HashMap<(PropertyOwnerKind, u32, String), u32> = doc
            .properties
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                let (owner_kind, owner_id) =
                    if let Some(&owner_id) = entity_name_to_id.get(&p.owner) {
                        (PropertyOwnerKind::Entity, owner_id)
                    } else if let Some(&owner_id) = relation_name_to_id.get(&p.owner) {
                        (PropertyOwnerKind::Relation, owner_id)
                    } else {
                        return None;
                    };
                Some((
                    (owner_kind, owner_id, p.name.clone()),
                    i.try_into().expect("ontology size exceeds u32"),
                ))
            })
            .collect();

        Ok(OntologyRuntime {
            ontology_meta,
            entity_types: entity_types_batch,
            relation_types: relation_types_batch,
            property_types: property_types_batch,
            type_constraints: type_constraints_batch,
            semantic_flags: semantic_flags_batch,
            cardinality_rules,
            aliases,
            entity_name_to_id,
            entity_id_to_name,
            relation_name_to_id,
            relation_id_to_name,
            ancestors,
            descendants,
            property_name_to_id,
        })
    }
}

// ---------------------------------------------------------------------------
// Table builders
// ---------------------------------------------------------------------------

fn arrow_err(e: &arrow::error::ArrowError) -> OntologyError {
    OntologyError::Arrow(e.to_string())
}

fn empty_batch(schema: &std::sync::LazyLock<arrow::datatypes::SchemaRef>) -> RecordBatch {
    RecordBatch::new_empty((**schema).clone())
}

fn build_ontology_meta(doc: &OntologyDoc) -> Result<RecordBatch, OntologyError> {
    // Checksum: SHA-256 of the stable JSON serialisation.
    let json = serde_json::to_string(doc).map_err(|e| OntologyError::Arrow(e.to_string()))?;
    let checksum = format!("{:x}", Sha256::digest(json.as_bytes()));

    let mut ids = StringBuilder::new();
    let mut versions = StringBuilder::new();
    let mut ir_versions = StringBuilder::new();
    let mut checksums = StringBuilder::new();

    ids.append_value(&doc.ontology_id);
    versions.append_value(&doc.version);
    ir_versions.append_value("0.1.0");
    checksums.append_value(&checksum);

    RecordBatch::try_new(
        ONTOLOGY_META_SCHEMA.clone(),
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(versions.finish()),
            Arc::new(ir_versions.finish()),
            Arc::new(checksums.finish()),
        ],
    )
    .map_err(|e| arrow_err(&e))
}

fn build_entity_types(
    doc: &OntologyDoc,
    name_to_id: &HashMap<String, u32>,
) -> Result<RecordBatch, OntologyError> {
    let mut ids = UInt32Builder::new();
    let mut names = StringBuilder::new();
    let mut abstracts = BooleanBuilder::new();
    let mut parent_ids = UInt32Builder::new();

    for e in &doc.entity_types {
        let id = name_to_id[&e.name];
        ids.append_value(id);
        names.append_value(&e.name);
        abstracts.append_value(e.r#abstract);
        match e.parent.as_deref().and_then(|p| name_to_id.get(p)) {
            Some(&pid) => parent_ids.append_value(pid),
            None => parent_ids.append_null(),
        }
    }

    RecordBatch::try_new(
        ENTITY_TYPES_SCHEMA.clone(),
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(names.finish()),
            Arc::new(abstracts.finish()),
            Arc::new(parent_ids.finish()),
        ],
    )
    .map_err(|e| arrow_err(&e))
}

fn build_relation_types(
    doc: &OntologyDoc,
    entity_name_to_id: &HashMap<String, u32>,
    relation_name_to_id: &HashMap<String, u32>,
) -> Result<RecordBatch, OntologyError> {
    let mut ids = UInt32Builder::new();
    let mut names = StringBuilder::new();
    let mut src_ids = UInt32Builder::new();
    let mut dst_ids = UInt32Builder::new();
    let mut inv_ids = UInt32Builder::new();

    for r in &doc.relation_types {
        ids.append_value(relation_name_to_id[&r.name]);
        names.append_value(&r.name);
        src_ids.append_option(entity_name_to_id.get(&r.src).copied());
        dst_ids.append_option(entity_name_to_id.get(&r.dst).copied());
        match r
            .inverse
            .as_deref()
            .and_then(|inv| relation_name_to_id.get(inv))
        {
            Some(&iid) => inv_ids.append_value(iid),
            None => inv_ids.append_null(),
        }
    }

    RecordBatch::try_new(
        RELATION_TYPES_SCHEMA.clone(),
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(names.finish()),
            Arc::new(src_ids.finish()),
            Arc::new(dst_ids.finish()),
            Arc::new(inv_ids.finish()),
        ],
    )
    .map_err(|e| arrow_err(&e))
}

fn property_value_type_str(vt: &PropertyValueType) -> &'static str {
    match vt {
        PropertyValueType::Utf8 => "utf8",
        PropertyValueType::Int64 => "int64",
        PropertyValueType::Float64 => "float64",
        PropertyValueType::Bool => "bool",
        PropertyValueType::Duration => "duration",
        PropertyValueType::DateTime => "datetime",
        PropertyValueType::List => "list",
        PropertyValueType::Map => "map",
    }
}

fn build_property_types(
    doc: &OntologyDoc,
    entity_name_to_id: &HashMap<String, u32>,
    relation_name_to_id: &HashMap<String, u32>,
) -> Result<RecordBatch, OntologyError> {
    let mut prop_ids = UInt32Builder::new();
    let mut owner_kinds = StringBuilder::new();
    let mut owner_type_ids = UInt32Builder::new();
    let mut names = StringBuilder::new();
    let mut value_types = StringBuilder::new();
    let mut nullables = BooleanBuilder::new();
    let mut multivalueds = BooleanBuilder::new();

    for (i, p) in doc.properties.iter().enumerate() {
        prop_ids.append_value(i.try_into().expect("ontology size exceeds u32"));
        names.append_value(&p.name);
        value_types.append_value(property_value_type_str(&p.value_type));
        nullables.append_value(p.nullable);
        multivalueds.append_value(p.multivalued);

        if let Some(&eid) = entity_name_to_id.get(&p.owner) {
            owner_kinds.append_value("entity");
            owner_type_ids.append_value(eid);
        } else if let Some(&rid) = relation_name_to_id.get(&p.owner) {
            owner_kinds.append_value("relation");
            owner_type_ids.append_value(rid);
        } else {
            // Unknown owner — use 0 and mark as entity (validator catches this separately).
            owner_kinds.append_value("unknown");
            owner_type_ids.append_value(0);
        }
    }

    RecordBatch::try_new(
        PROPERTY_TYPES_SCHEMA.clone(),
        vec![
            Arc::new(prop_ids.finish()) as ArrayRef,
            Arc::new(owner_kinds.finish()),
            Arc::new(owner_type_ids.finish()),
            Arc::new(names.finish()),
            Arc::new(value_types.finish()),
            Arc::new(nullables.finish()),
            Arc::new(multivalueds.finish()),
        ],
    )
    .map_err(|e| arrow_err(&e))
}

fn build_type_constraints(
    doc: &OntologyDoc,
    entity_name_to_id: &HashMap<String, u32>,
    relation_name_to_id: &HashMap<String, u32>,
) -> Result<RecordBatch, OntologyError> {
    let mut cids = UInt32Builder::new();
    let mut owner_kinds = StringBuilder::new();
    let mut owner_type_ids = UInt32Builder::new();
    let mut kinds = StringBuilder::new();
    let mut exprs = StringBuilder::new();

    for (i, c) in doc.constraints.iter().enumerate() {
        cids.append_value(i.try_into().expect("ontology size exceeds u32"));
        let kind_str = match c.kind {
            crate::ontology::ConstraintKind::UniqueProperty => "unique_property",
            crate::ontology::ConstraintKind::RequiredProperty => "required_property",
            crate::ontology::ConstraintKind::RangeCheck => "range_check",
            crate::ontology::ConstraintKind::CustomExpr => "custom_expr",
        };
        kinds.append_value(kind_str);
        match c.expr_json.as_deref() {
            Some(e) => exprs.append_value(e),
            None => exprs.append_null(),
        }
        if let Some(&eid) = entity_name_to_id.get(&c.owner) {
            owner_kinds.append_value("entity");
            owner_type_ids.append_value(eid);
        } else if let Some(&rid) = relation_name_to_id.get(&c.owner) {
            owner_kinds.append_value("relation");
            owner_type_ids.append_value(rid);
        } else {
            owner_kinds.append_value("unknown");
            owner_type_ids.append_value(0);
        }
    }

    RecordBatch::try_new(
        TYPE_CONSTRAINTS_SCHEMA.clone(),
        vec![
            Arc::new(cids.finish()) as ArrayRef,
            Arc::new(owner_kinds.finish()),
            Arc::new(owner_type_ids.finish()),
            Arc::new(kinds.finish()),
            Arc::new(exprs.finish()),
        ],
    )
    .map_err(|e| arrow_err(&e))
}

fn build_semantic_flags(
    doc: &OntologyDoc,
    relation_name_to_id: &HashMap<String, u32>,
) -> Result<RecordBatch, OntologyError> {
    let mut owner_kinds = StringBuilder::new();
    let mut owner_type_ids = UInt32Builder::new();
    let mut transitives = BooleanBuilder::new();
    let mut symmetrics = BooleanBuilder::new();
    let mut reflexives = BooleanBuilder::new();
    let mut functionals = BooleanBuilder::new();
    let mut acyclics = BooleanBuilder::new();

    for r in &doc.relation_types {
        owner_kinds.append_value("relation");
        owner_type_ids.append_value(relation_name_to_id[&r.name]);
        transitives.append_value(r.semantic.transitive);
        symmetrics.append_value(r.semantic.symmetric);
        reflexives.append_value(r.semantic.reflexive);
        functionals.append_value(r.semantic.functional);
        acyclics.append_value(r.semantic.acyclic);
    }

    RecordBatch::try_new(
        SEMANTIC_FLAGS_SCHEMA.clone(),
        vec![
            Arc::new(owner_kinds.finish()) as ArrayRef,
            Arc::new(owner_type_ids.finish()),
            Arc::new(transitives.finish()),
            Arc::new(symmetrics.finish()),
            Arc::new(reflexives.finish()),
            Arc::new(functionals.finish()),
            Arc::new(acyclics.finish()),
        ],
    )
    .map_err(|e| arrow_err(&e))
}

// ---------------------------------------------------------------------------
// Inheritance closure
// ---------------------------------------------------------------------------

type ClosureMaps = (HashMap<u32, HashSet<u32>>, HashMap<u32, HashSet<u32>>);

fn build_inheritance_closure(doc: &OntologyDoc, name_to_id: &HashMap<String, u32>) -> ClosureMaps {
    // parent_of: child_id → parent_id
    let parent_of: HashMap<u32, u32> = doc
        .entity_types
        .iter()
        .filter_map(|e| {
            e.parent
                .as_deref()
                .and_then(|p| name_to_id.get(p))
                .map(|&pid| (name_to_id[&e.name], pid))
        })
        .collect();

    let n: u32 = doc
        .entity_types
        .len()
        .try_into()
        .expect("ontology size exceeds u32");
    let all_ids: Vec<u32> = (0..n).collect();
    let mut ancestors: HashMap<u32, HashSet<u32>> = HashMap::new();
    let mut descendants: HashMap<u32, HashSet<u32>> = HashMap::new();

    // Initialise empty sets.
    for &id in &all_ids {
        ancestors.insert(id, HashSet::new());
        descendants.insert(id, HashSet::new());
    }

    // Walk each node's ancestor chain.
    for &id in &all_ids {
        let mut current = id;
        while let Some(&parent) = parent_of.get(&current) {
            if ancestors[&id].contains(&parent) {
                break; // cycle already detected by validator — stop here
            }
            ancestors.get_mut(&id).unwrap().insert(parent);
            descendants.get_mut(&parent).unwrap().insert(id);
            current = parent;
        }
    }

    (ancestors, descendants)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::{
        ConstraintDef, ConstraintKind, EntityTypeDef, OntologyDoc, PropertyDef, PropertyValueType,
        RelationTypeDef, SemanticFlags,
    };
    use arrow::array::{Array, BooleanArray, StringArray, UInt32Array};

    fn sample_doc() -> OntologyDoc {
        OntologyDoc {
            ontology_id: "test".to_string(),
            version: "1.0".to_string(),
            entity_types: vec![
                EntityTypeDef {
                    name: "Person".to_string(),
                    r#abstract: false,
                    parent: None,
                },
                EntityTypeDef {
                    name: "Employee".to_string(),
                    r#abstract: false,
                    parent: Some("Person".to_string()),
                },
            ],
            relation_types: vec![
                RelationTypeDef {
                    name: "MANAGES".to_string(),
                    src: "Employee".to_string(),
                    dst: "Employee".to_string(),
                    inverse: Some("MANAGED_BY".to_string()),
                    semantic: SemanticFlags {
                        transitive: true,
                        ..Default::default()
                    },
                },
                RelationTypeDef {
                    name: "MANAGED_BY".to_string(),
                    src: "Employee".to_string(),
                    dst: "Employee".to_string(),
                    inverse: Some("MANAGES".to_string()),
                    semantic: SemanticFlags::default(),
                },
            ],
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
    fn compile_empty_doc() {
        let doc = OntologyDoc {
            ontology_id: "empty".to_string(),
            version: "0.1".to_string(),
            entity_types: vec![],
            relation_types: vec![],
            properties: vec![],
            constraints: vec![],
            migrations: vec![],
        };
        let rt = OntologyCompiler::compile(&doc).unwrap();
        assert_eq!(rt.ontology_meta.num_rows(), 1);
        assert_eq!(rt.entity_types.num_rows(), 0);
        assert_eq!(rt.relation_types.num_rows(), 0);
        assert_eq!(rt.property_types.num_rows(), 0);
        assert_eq!(rt.type_constraints.num_rows(), 0);
        assert_eq!(rt.semantic_flags.num_rows(), 0);
        assert_eq!(rt.cardinality_rules.num_rows(), 0);
        assert_eq!(rt.aliases.num_rows(), 0);
    }

    #[test]
    fn compile_entity_types_row_count() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        assert_eq!(rt.entity_types.num_rows(), 2);
    }

    #[test]
    fn compile_entity_type_id_assignment() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let ids = rt
            .entity_types
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 0);
        assert_eq!(ids.value(1), 1);
    }

    #[test]
    fn compile_parent_type_id_null() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let parent_ids = rt
            .entity_types
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert!(parent_ids.is_null(0), "Person has no parent");
    }

    #[test]
    fn compile_parent_type_id_set() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let parent_ids = rt
            .entity_types
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert!(!parent_ids.is_null(1), "Employee has a parent");
        // Person is id 0
        assert_eq!(parent_ids.value(1), 0);
    }

    #[test]
    fn compile_relation_types() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        assert_eq!(rt.relation_types.num_rows(), 2);
        // src and dst of MANAGES are both Employee (id=1)
        let src_ids = rt
            .relation_types
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(src_ids.value(0), 1); // Employee
    }

    #[test]
    fn compile_property_types() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        assert_eq!(rt.property_types.num_rows(), 1);
        let owner_kinds = rt
            .property_types
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(owner_kinds.value(0), "entity");
    }

    #[test]
    fn compile_semantic_flags_transitive() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        assert_eq!(rt.semantic_flags.num_rows(), 2);
        let transitives = rt
            .semantic_flags
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(transitives.value(0), "MANAGES is transitive");
        assert!(!transitives.value(1), "MANAGED_BY is not transitive");
    }

    #[test]
    fn compile_name_to_id_map() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        assert!(rt.entity_name_to_id.contains_key("Person"));
        assert!(rt.entity_name_to_id.contains_key("Employee"));
        assert_eq!(rt.entity_name_to_id["Person"], 0);
        assert_eq!(rt.entity_name_to_id["Employee"], 1);
    }

    #[test]
    fn compile_ancestors() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let employee_id = rt.entity_name_to_id["Employee"];
        let person_id = rt.entity_name_to_id["Person"];
        assert!(
            rt.ancestors[&employee_id].contains(&person_id),
            "Employee's ancestors must include Person"
        );
        assert!(
            rt.ancestors[&person_id].is_empty(),
            "Person has no ancestors"
        );
    }

    #[test]
    fn compile_descendants() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let person_id = rt.entity_name_to_id["Person"];
        let employee_id = rt.entity_name_to_id["Employee"];
        assert!(
            rt.descendants[&person_id].contains(&employee_id),
            "Person's descendants must include Employee"
        );
        assert!(
            rt.descendants[&employee_id].is_empty(),
            "Employee has no descendants"
        );
    }

    #[test]
    fn compile_checksum_stable() {
        let doc = sample_doc();
        let rt1 = OntologyCompiler::compile(&doc).unwrap();
        let rt2 = OntologyCompiler::compile(&doc).unwrap();
        let chk1 = rt1
            .ontology_meta
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        let chk2 = rt2
            .ontology_meta
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(chk1, chk2, "same doc must produce same checksum");
    }

    #[test]
    fn compile_checksum_differs_on_change() {
        let doc1 = sample_doc();
        let mut doc2 = sample_doc();
        doc2.version = "2.0".to_string();
        let rt1 = OntologyCompiler::compile(&doc1).unwrap();
        let rt2 = OntologyCompiler::compile(&doc2).unwrap();
        let chk1 = rt1
            .ontology_meta
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        let chk2 = rt2
            .ontology_meta
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_ne!(
            chk1, chk2,
            "different docs must produce different checksums"
        );
    }

    #[test]
    fn compile_example_doc_row_counts() {
        // Full example from storage.md (via loader tests).
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
        let doc: OntologyDoc = serde_yaml::from_str(yaml).unwrap();
        let rt = OntologyCompiler::compile(&doc).unwrap();
        assert_eq!(rt.entity_types.num_rows(), 2);
        assert_eq!(rt.relation_types.num_rows(), 2);
        assert_eq!(rt.property_types.num_rows(), 1);
        assert_eq!(rt.type_constraints.num_rows(), 1);
        assert_eq!(rt.semantic_flags.num_rows(), 2);
    }
}
