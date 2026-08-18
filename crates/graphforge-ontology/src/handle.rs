//! [`OntologyHandle`] — cheap cloneable reference to a compiled ontology runtime.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, StringArray, UInt32Array};

use graphforge_core::{PropId, TypeId};

use crate::compiler::{OntologyRuntime, PropertyOwnerKind};
use crate::ontology::SemanticFlags;

// ---------------------------------------------------------------------------
// OntologyFormat
// ---------------------------------------------------------------------------

/// Serialisation format of an ontology definition file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OntologyFormat {
    /// YAML (`.yaml` or `.yml`).
    Yaml,
    /// JSON (`.json`).
    Json,
}

// ---------------------------------------------------------------------------
// OntologyHandle
// ---------------------------------------------------------------------------

/// Cheap cloneable reference to a compiled [`OntologyRuntime`].
///
/// All query methods run in O(1) against the precomputed lookup maps.
#[derive(Clone)]
pub struct OntologyHandle(pub(crate) Arc<OntologyRuntime>);

impl OntologyHandle {
    /// Wrap a compiled runtime in a handle.
    #[must_use]
    pub fn new(runtime: OntologyRuntime) -> Self {
        Self(Arc::new(runtime))
    }

    // -----------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------

    /// The stable ontology identifier (e.g. `"core"`).
    #[must_use]
    pub fn id(&self) -> &str {
        meta_str(&self.0, 0)
    }

    /// Version string (e.g. `"2026.05"`).
    #[must_use]
    pub fn version(&self) -> &str {
        meta_str(&self.0, 1)
    }

    /// SHA-256 checksum of the canonical serialised `OntologyDoc`.
    #[must_use]
    pub fn checksum(&self) -> &str {
        meta_str(&self.0, 3)
    }

    // -----------------------------------------------------------------------
    // Type lookups
    // -----------------------------------------------------------------------

    /// Resolve an entity type name to its integer ID.
    #[must_use]
    pub fn entity_type_id(&self, name: &str) -> Option<TypeId> {
        self.0.entity_name_to_id.get(name).map(|&id| TypeId(id))
    }

    /// Resolve a relation type name to its integer ID.
    #[must_use]
    pub fn relation_type_id(&self, name: &str) -> Option<TypeId> {
        self.0.relation_name_to_id.get(name).map(|&id| TypeId(id))
    }

    /// Resolve a property name for a kind-free owner type to its [`PropId`].
    ///
    /// [`PropId`] is distinct from [`TypeId`] — use this when the binder or
    /// planner needs to reference a property rather than an entity/relation
    /// type. Because entity and relation IDs occupy independent namespaces,
    /// this compatibility lookup returns `None` when both kinds declare the
    /// same property at the supplied numeric owner ID. Prefer the kind-specific
    /// methods when the owner kind is known.
    #[must_use]
    pub fn property_type_id(&self, owner: TypeId, name: &str) -> Option<PropId> {
        match (
            self.entity_property_type_id(owner, name),
            self.relation_property_type_id(owner, name),
        ) {
            (Some(id), None) | (None, Some(id)) => Some(id),
            (None, None) | (Some(_), Some(_)) => None,
        }
    }

    /// Resolve a property declared directly by an entity owner.
    #[must_use]
    pub fn entity_property_type_id(&self, owner: TypeId, name: &str) -> Option<PropId> {
        self.property_type_id_for_kind(PropertyOwnerKind::Entity, owner, name)
    }

    /// Resolve a property declared directly by a relation owner.
    #[must_use]
    pub fn relation_property_type_id(&self, owner: TypeId, name: &str) -> Option<PropId> {
        self.property_type_id_for_kind(PropertyOwnerKind::Relation, owner, name)
    }

    fn property_type_id_for_kind(
        &self,
        owner_kind: PropertyOwnerKind,
        owner: TypeId,
        name: &str,
    ) -> Option<PropId> {
        self.0
            .property_name_to_id
            .get(&(owner_kind, owner.0, name.to_owned()))
            .map(|&id| PropId(id))
    }

    /// Return every direct or inherited declaration of `name` visible from an
    /// entity owner. More than one result is an ambiguous shadowing contract.
    #[must_use]
    pub fn entity_property_declarations(&self, owner: TypeId, name: &str) -> Vec<(TypeId, PropId)> {
        let mut owners = vec![owner];
        owners.extend(
            self.0
                .ancestors
                .get(&owner.0)
                .into_iter()
                .flatten()
                .copied()
                .map(TypeId),
        );
        let mut declarations = owners
            .into_iter()
            .filter_map(|candidate| {
                self.entity_property_type_id(candidate, name)
                    .map(|property| (candidate, property))
            })
            .collect::<Vec<_>>();
        declarations.sort_unstable_by_key(|(candidate, property)| (candidate.0, property.0));
        declarations
    }

    /// Return the direct declaration of `name` for a relation owner, if any.
    #[must_use]
    pub fn relation_property_declarations(
        &self,
        owner: TypeId,
        name: &str,
    ) -> Vec<(TypeId, PropId)> {
        self.relation_property_type_id(owner, name)
            .map(|property| vec![(owner, property)])
            .unwrap_or_default()
    }

    /// Return every entity declaration of `name`, sorted deterministically.
    #[must_use]
    pub fn all_entity_property_declarations(&self, name: &str) -> Vec<(TypeId, PropId)> {
        self.all_property_declarations(PropertyOwnerKind::Entity, name)
    }

    /// Return every relation declaration of `name`, sorted deterministically.
    #[must_use]
    pub fn all_relation_property_declarations(&self, name: &str) -> Vec<(TypeId, PropId)> {
        self.all_property_declarations(PropertyOwnerKind::Relation, name)
    }

    fn all_property_declarations(
        &self,
        owner_kind: PropertyOwnerKind,
        name: &str,
    ) -> Vec<(TypeId, PropId)> {
        let mut declarations = self
            .0
            .property_name_to_id
            .iter()
            .filter_map(|((kind, owner, property_name), property)| {
                (*kind == owner_kind && property_name == name)
                    .then_some((TypeId(*owner), PropId(*property)))
            })
            .collect::<Vec<_>>();
        declarations.sort_unstable_by_key(|(owner, property)| (owner.0, property.0));
        declarations
    }

    /// Resolve an entity ID back to its declared name.
    #[must_use]
    pub fn entity_type_name(&self, id: TypeId) -> Option<&str> {
        self.0.entity_id_to_name.get(&id.0).map(String::as_str)
    }

    /// Resolve a relation ID back to its declared name.
    #[must_use]
    pub fn relation_type_name(&self, id: TypeId) -> Option<&str> {
        self.0.relation_id_to_name.get(&id.0).map(String::as_str)
    }

    /// Whether an entity type declares `name` directly or inherits it from an
    /// ancestor in the compiled single-inheritance hierarchy.
    #[must_use]
    pub fn has_entity_property(&self, owner: TypeId, name: &str) -> bool {
        !self.entity_property_declarations(owner, name).is_empty()
    }

    /// Return the exact property definition declared by an entity or one of its
    /// ancestors. The nearest declaration wins in the single-inheritance chain.
    #[must_use]
    pub fn entity_property_def(
        &self,
        owner: TypeId,
        name: &str,
    ) -> Option<crate::ontology::PropertyDef> {
        let declarations = self.entity_property_declarations(owner, name);
        let declaration = declarations.iter().find_map(|(candidate, _)| {
            (*candidate == owner
                || declarations
                    .iter()
                    .all(|(other, _)| other == candidate || self.is_subtype(*candidate, *other)))
            .then_some(*candidate)
        })?;
        self.property_def("entity", declaration, name)
    }

    /// Return the exact directly declared relationship property definition.
    #[must_use]
    pub fn relation_property_def(
        &self,
        owner: TypeId,
        name: &str,
    ) -> Option<crate::ontology::PropertyDef> {
        self.property_def("relation", owner, name)
    }

    fn property_def(
        &self,
        owner_kind: &str,
        owner: TypeId,
        name: &str,
    ) -> Option<crate::ontology::PropertyDef> {
        use crate::ontology::{PropertyDef, PropertyValueType};

        let owner_name = match owner_kind {
            "entity" => self.entity_type_name(owner),
            "relation" => self.relation_type_name(owner),
            _ => None,
        }?;

        let kinds = self
            .0
            .property_types
            .column_by_name("owner_kind")?
            .as_any()
            .downcast_ref::<StringArray>()?;
        let owners = self
            .0
            .property_types
            .column_by_name("owner_type_id")?
            .as_any()
            .downcast_ref::<UInt32Array>()?;
        let names = self
            .0
            .property_types
            .column_by_name("name")?
            .as_any()
            .downcast_ref::<StringArray>()?;
        let value_types = self
            .0
            .property_types
            .column_by_name("value_type")?
            .as_any()
            .downcast_ref::<StringArray>()?;
        let nullables = self
            .0
            .property_types
            .column_by_name("nullable")?
            .as_any()
            .downcast_ref::<BooleanArray>()?;
        let multivalueds = self
            .0
            .property_types
            .column_by_name("multivalued")?
            .as_any()
            .downcast_ref::<BooleanArray>()?;

        (0..self.0.property_types.num_rows()).find_map(|row| {
            if kinds.value(row) != owner_kind
                || owners.value(row) != owner.0
                || names.value(row) != name
            {
                return None;
            }
            let raw_value_type = value_types.value(row);
            let value_type = match raw_value_type {
                "utf8" => PropertyValueType::Utf8,
                "int64" => PropertyValueType::Int64,
                "float64" => PropertyValueType::Float64,
                "bool" => PropertyValueType::Bool,
                "duration" => PropertyValueType::Duration,
                "datetime" => PropertyValueType::DateTime,
                "list" => PropertyValueType::List,
                "map" => PropertyValueType::Map,
                other => crate::spatial::SpatialType::from_catalog_name(other)
                    .map(PropertyValueType::Spatial)?,
            };
            Some(PropertyDef {
                owner: owner_name.to_owned(),
                name: name.to_owned(),
                value_type,
                nullable: nullables.value(row),
                multivalued: multivalueds.value(row),
                default_json: None,
            })
        })
    }

    // -----------------------------------------------------------------------
    // Iteration helpers — used by graphforge-storage catalog registration
    // -----------------------------------------------------------------------

    /// Returns all relation type names declared in the ontology.
    #[must_use]
    pub fn relation_type_names(&self) -> Vec<&str> {
        self.0
            .relation_name_to_id
            .keys()
            .map(String::as_str)
            .collect()
    }

    /// Returns all entity type names declared in the ontology.
    #[must_use]
    pub fn entity_type_names(&self) -> Vec<&str> {
        self.0
            .entity_name_to_id
            .keys()
            .map(String::as_str)
            .collect()
    }

    /// Returns `(entity_type_name, property_defs)` pairs for all entity types.
    ///
    /// Used by `graphforge-storage` to build per-entity-type property table schemas.
    #[must_use]
    pub fn entity_property_defs(&self) -> Vec<(&str, Vec<crate::ontology::PropertyDef>)> {
        use crate::ontology::{PropertyDef, PropertyValueType};

        // Build (entity_type_name, Vec<PropertyDef>) from property_name_to_id.
        // The map key is (owner_type_id, prop_name).
        let mut by_entity: std::collections::HashMap<u32, Vec<PropertyDef>> =
            std::collections::HashMap::new();

        for ((owner_kind, owner_id, prop_name), &_prop_id) in &self.0.property_name_to_id {
            if *owner_kind != PropertyOwnerKind::Entity {
                continue;
            }
            // Resolve owner_id → entity name.
            if let Some(entity_name) = self.0.entity_id_to_name.get(owner_id) {
                let _ = entity_name; // we'll look it up again below
            }
            by_entity.entry(*owner_id).or_default().push(PropertyDef {
                owner: self
                    .0
                    .entity_id_to_name
                    .get(owner_id)
                    .cloned()
                    .unwrap_or_default(),
                name: prop_name.clone(),
                // Value type is not stored in the compiled runtime maps;
                // use Utf8 as a safe default. Operator lowering doesn't
                // depend on this type for logical-plan lowering.
                value_type: PropertyValueType::Utf8,
                nullable: true,
                multivalued: false,
                default_json: None,
            });
        }

        self.0
            .entity_name_to_id
            .keys()
            .map(|name| {
                let id = self.0.entity_name_to_id[name];
                let defs = by_entity.remove(&id).unwrap_or_default();
                (name.as_str(), defs)
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Subtype check
    // -----------------------------------------------------------------------

    /// Returns `true` if `child` is a (direct or indirect) subtype of `ancestor`.
    #[must_use]
    pub fn is_subtype(&self, child: TypeId, ancestor: TypeId) -> bool {
        self.0
            .ancestors
            .get(&child.0)
            .is_some_and(|set| set.contains(&ancestor.0))
    }

    // -----------------------------------------------------------------------
    // Semantic flags
    // -----------------------------------------------------------------------

    /// Return the semantic flags for a relation type ID.
    ///
    /// If `type_id` has no entry in the semantic flags table (e.g. the caller
    /// passes an entity type ID by mistake, or the ontology has no relations),
    /// returns [`SemanticFlags::default()`] (all `false`).
    #[must_use]
    pub fn semantic_flags(&self, type_id: TypeId) -> SemanticFlags {
        let table = &self.0.semantic_flags;
        if table.num_rows() == 0 {
            return SemanticFlags::default();
        }

        let owner_ids = table
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("owner_type_id column is UInt32");

        let row = (0..table.num_rows()).find(|&r| owner_ids.value(r) == type_id.0);

        let Some(r) = row else {
            return SemanticFlags::default();
        };

        let bool_col = |col: usize| -> bool {
            table
                .column(col)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean column")
                .value(r)
        };

        SemanticFlags {
            transitive: bool_col(2),
            symmetric: bool_col(3),
            reflexive: bool_col(4),
            functional: bool_col(5),
            acyclic: bool_col(6),
            inverse_functional: false, // not stored in semantic_flags table yet
        }
    }

    // -----------------------------------------------------------------------
    // Migration
    // -----------------------------------------------------------------------

    /// Migrate to `target_version` using the migration steps declared in `doc`.
    ///
    /// The caller must supply the original `OntologyDoc` because the compiled
    /// runtime stores Arrow tables rather than the mutable source document.
    ///
    /// # Errors
    /// - [`OntologyError::NoMigrationPath`] if no chain of steps reaches `target_version`.
    /// - [`OntologyError::Arrow`] if the migrated ontology fails to compile.
    pub fn migrate_to(
        &self,
        target_version: &str,
        doc: crate::ontology::OntologyDoc,
    ) -> Result<OntologyHandle, crate::error::OntologyError> {
        // Guard: the supplied doc must be at the same version as this handle.
        if doc.version != self.version() {
            return Err(crate::error::OntologyError::NoMigrationPath {
                from: doc.version.clone(),
                to: target_version.to_owned(),
            });
        }
        let steps = crate::migration::MigrationEngine::plan(
            self.version(),
            target_version,
            &doc.migrations,
        )?;
        let runtime = crate::migration::MigrationEngine::apply(doc, &steps)?;
        Ok(OntologyHandle::new(runtime))
    }
}

// ---------------------------------------------------------------------------
// Helper — read a string from ontology_meta
// ---------------------------------------------------------------------------

fn meta_str(rt: &OntologyRuntime, col: usize) -> &str {
    rt.ontology_meta
        .column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("ontology_meta column is Utf8")
        .value(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::OntologyCompiler;
    use crate::ontology::{
        EntityTypeDef, MigrationDef, OntologyDoc, PropertyDef, PropertyValueType, RelationTypeDef,
        SemanticFlags,
    };

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
            constraints: vec![],
            migrations: vec![],
        }
    }

    fn handle() -> OntologyHandle {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        OntologyHandle::new(rt)
    }

    #[test]
    fn handle_id_version_checksum() {
        let h = handle();
        assert_eq!(h.id(), "test");
        assert_eq!(h.version(), "1.0");
        assert!(!h.checksum().is_empty());
    }

    #[test]
    fn handle_entity_type_id_lookup() {
        let h = handle();
        assert_eq!(h.entity_type_id("Person"), Some(TypeId(0)));
        assert_eq!(h.entity_type_id("Employee"), Some(TypeId(1)));
        assert_eq!(h.entity_type_id("Ghost"), None);
    }

    #[test]
    fn handle_relation_type_id_lookup() {
        let h = handle();
        assert_eq!(h.relation_type_id("MANAGES"), Some(TypeId(0)));
        assert_eq!(h.relation_type_id("GHOST"), None);
    }

    #[test]
    fn handle_property_type_id_lookup() {
        let h = handle();
        let person_id = h.entity_type_id("Person").unwrap();
        let prop_id = h.property_type_id(person_id, "name");
        assert!(prop_id.is_some(), "Person.name should resolve");
        assert_eq!(h.property_type_id(person_id, "ghost"), None);
        let employee_id = h.entity_type_id("Employee").unwrap();
        assert!(h.has_entity_property(employee_id, "name"));
        assert!(!h.has_entity_property(employee_id, "ghost"));
    }

    #[test]
    fn property_lookup_keeps_entity_and_relation_owner_namespaces_distinct() {
        let mut doc = sample_doc();
        doc.properties = vec![
            PropertyDef {
                owner: "Person".into(),
                name: "shared".into(),
                value_type: PropertyValueType::Utf8,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
            PropertyDef {
                owner: "MANAGES".into(),
                name: "shared".into(),
                value_type: PropertyValueType::Utf8,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
        ];
        let h = OntologyHandle::new(OntologyCompiler::compile(&doc).unwrap());
        let person = h.entity_type_id("Person").unwrap();
        let manages = h.relation_type_id("MANAGES").unwrap();
        assert_eq!(person, manages, "numeric owner IDs intentionally collide");

        assert_eq!(h.entity_property_type_id(person, "shared"), Some(PropId(0)));
        assert_eq!(
            h.relation_property_type_id(manages, "shared"),
            Some(PropId(1))
        );
        assert_eq!(
            h.entity_property_declarations(person, "shared"),
            vec![(person, PropId(0))]
        );
        assert_eq!(
            h.relation_property_declarations(manages, "shared"),
            vec![(manages, PropId(1))]
        );
        assert_eq!(
            h.all_entity_property_declarations("shared"),
            vec![(person, PropId(0))]
        );
        assert_eq!(
            h.all_relation_property_declarations("shared"),
            vec![(manages, PropId(1))]
        );
        assert_eq!(
            h.property_type_id(TypeId(0), "shared"),
            None,
            "kind-free lookup must not choose across owner namespaces"
        );
    }

    #[test]
    fn inherited_property_declarations_are_complete_and_deterministic() {
        let mut doc = sample_doc();
        doc.entity_types.push(EntityTypeDef {
            name: "Executive".into(),
            r#abstract: false,
            parent: Some("Employee".into()),
        });
        doc.properties.push(PropertyDef {
            owner: "Employee".into(),
            name: "name".into(),
            value_type: PropertyValueType::Int64,
            nullable: false,
            multivalued: false,
            default_json: None,
        });
        let h = OntologyHandle::new(OntologyCompiler::compile(&doc).unwrap());
        let employee = h.entity_type_id("Employee").unwrap();
        let executive = h.entity_type_id("Executive").unwrap();

        assert_eq!(
            h.entity_property_declarations(employee, "name"),
            vec![(TypeId(0), PropId(0)), (TypeId(1), PropId(1))]
        );
        let nearest = h.entity_property_def(employee, "name").unwrap();
        assert_eq!(nearest.owner, "Employee");
        assert_eq!(nearest.value_type, PropertyValueType::Int64);
        assert!(!nearest.nullable);
        let inherited = h.entity_property_def(executive, "name").unwrap();
        assert_eq!(inherited.owner, "Employee");
        assert_eq!(inherited.value_type, PropertyValueType::Int64);
    }

    #[test]
    fn handle_is_subtype() {
        let h = handle();
        let person = h.entity_type_id("Person").unwrap();
        let employee = h.entity_type_id("Employee").unwrap();
        assert!(
            h.is_subtype(employee, person),
            "Employee is subtype of Person"
        );
        assert!(
            !h.is_subtype(person, employee),
            "Person is not subtype of Employee"
        );
        assert!(!h.is_subtype(person, person), "not subtype of self");
    }

    #[test]
    fn handle_semantic_flags_transitive() {
        let h = handle();
        let manages = h.relation_type_id("MANAGES").unwrap();
        let flags = h.semantic_flags(manages);
        assert!(flags.transitive, "MANAGES should be transitive");
    }

    #[test]
    fn handle_semantic_flags_non_transitive_relation() {
        let h = handle();
        // MANAGED_BY has transitive=false; verify the flag is false.
        let managed_by = h.relation_type_id("MANAGED_BY").unwrap();
        let flags = h.semantic_flags(managed_by);
        assert!(!flags.transitive, "MANAGED_BY should not be transitive");
    }

    #[test]
    fn handle_migration_enforces_source_version_and_reopens_transformed_runtime() {
        let mut doc = sample_doc();
        doc.migrations = vec![MigrationDef {
            from_version: "1.0".into(),
            to_version: "2.0".into(),
            transform_kind: "rename_type:Person->Human".into(),
            script_ref: None,
            checksum: None,
        }];
        let original = OntologyHandle::new(OntologyCompiler::compile(&doc).unwrap());
        let migrated = original.migrate_to("2.0", doc.clone()).unwrap();
        assert_eq!(migrated.version(), "2.0");
        assert_eq!(migrated.entity_type_id("Person"), None);
        let human = migrated.entity_type_id("Human").unwrap();
        assert!(migrated.property_type_id(human, "name").is_some());

        let mut wrong = doc.clone();
        wrong.version = "0.9".into();
        assert!(matches!(
            original.migrate_to("2.0", wrong),
            Err(crate::error::OntologyError::NoMigrationPath { .. })
        ));
        assert!(matches!(
            original.migrate_to("3.0", doc),
            Err(crate::error::OntologyError::NoMigrationPath { .. })
        ));
        assert_eq!(original.version(), "1.0");
    }
}
