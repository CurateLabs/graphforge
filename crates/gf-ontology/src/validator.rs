//! Load-time semantic validator for [`OntologyDoc`].
//!
//! All 10 validation rules are evaluated and their violations collected before
//! returning, so a single call surfaces every problem at once rather than
//! stopping at the first error.

use std::collections::{HashMap, HashSet};

use crate::error::{OntologyValidationError, ValidationErrorKind};
use crate::ontology::OntologyDoc;

/// Runs load-time semantic validation on an [`OntologyDoc`].
pub struct OntologyValidator;

impl OntologyValidator {
    /// Validate all semantic rules against `doc`.
    ///
    /// Returns `Ok(())` when every rule passes, or `Err(errors)` containing
    /// every violation found (collect-all, not fail-fast).
    ///
    /// # Errors
    /// Returns the list of [`OntologyValidationError`] values when one or more
    /// rules are violated.
    pub fn validate(doc: &OntologyDoc) -> Result<(), Vec<OntologyValidationError>> {
        let mut errors: Vec<OntologyValidationError> = Vec::new();

        // Build name sets used by multiple rules.
        let entity_names: HashSet<&str> =
            doc.entity_types.iter().map(|e| e.name.as_str()).collect();
        let relation_names: HashSet<&str> =
            doc.relation_types.iter().map(|r| r.name.as_str()).collect();
        let owner_names: HashSet<&str> = entity_names.union(&relation_names).copied().collect();

        Self::check_duplicate_entity_names(doc, &mut errors);
        Self::check_duplicate_relation_names(doc, &mut errors);
        Self::check_duplicate_property_names(doc, &mut errors);
        Self::check_parent_references(doc, &entity_names, &mut errors);
        Self::check_inheritance_acyclic(doc, &entity_names, &mut errors);
        Self::check_inverse_references(doc, &relation_names, &mut errors);
        Self::check_inverse_mutual(doc, &mut errors);
        Self::check_relation_endpoints(doc, &entity_names, &mut errors);
        Self::check_property_owners(doc, &owner_names, &mut errors);
        Self::check_migration_order(doc, &mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    // -----------------------------------------------------------------------
    // Rule implementations
    // -----------------------------------------------------------------------

    fn check_duplicate_entity_names(doc: &OntologyDoc, errors: &mut Vec<OntologyValidationError>) {
        let mut seen = HashSet::new();
        for (i, e) in doc.entity_types.iter().enumerate() {
            if !seen.insert(e.name.as_str()) {
                errors.push(OntologyValidationError {
                    kind: ValidationErrorKind::DuplicateName,
                    location: format!("entity_types[{i}].name"),
                    message: format!("entity type name '{}' is declared more than once", e.name),
                });
            }
        }
    }

    fn check_duplicate_relation_names(
        doc: &OntologyDoc,
        errors: &mut Vec<OntologyValidationError>,
    ) {
        let mut seen = HashSet::new();
        for (i, r) in doc.relation_types.iter().enumerate() {
            if !seen.insert(r.name.as_str()) {
                errors.push(OntologyValidationError {
                    kind: ValidationErrorKind::DuplicateName,
                    location: format!("relation_types[{i}].name"),
                    message: format!("relation type name '{}' is declared more than once", r.name),
                });
            }
        }
    }

    fn check_duplicate_property_names(
        doc: &OntologyDoc,
        errors: &mut Vec<OntologyValidationError>,
    ) {
        // Group by owner, then detect duplicates within each group.
        let mut by_owner: HashMap<&str, HashSet<&str>> = HashMap::new();
        for (i, p) in doc.properties.iter().enumerate() {
            let seen = by_owner.entry(p.owner.as_str()).or_default();
            if !seen.insert(p.name.as_str()) {
                errors.push(OntologyValidationError {
                    kind: ValidationErrorKind::DuplicateName,
                    location: format!("properties[{i}].name"),
                    message: format!(
                        "property '{}' is declared more than once for owner '{}'",
                        p.name, p.owner
                    ),
                });
            }
        }
    }

    fn check_parent_references(
        doc: &OntologyDoc,
        entity_names: &HashSet<&str>,
        errors: &mut Vec<OntologyValidationError>,
    ) {
        for (i, e) in doc.entity_types.iter().enumerate() {
            if let Some(parent) = &e.parent
                && !entity_names.contains(parent.as_str())
            {
                errors.push(OntologyValidationError {
                    kind: ValidationErrorKind::UnresolvedReference,
                    location: format!("entity_types[{i}].parent"),
                    message: format!(
                        "parent type '{parent}' of '{}' is not declared in entity_types",
                        e.name
                    ),
                });
            }
        }
    }

    fn check_inheritance_acyclic(
        doc: &OntologyDoc,
        entity_names: &HashSet<&str>,
        errors: &mut Vec<OntologyValidationError>,
    ) {
        // Build parent map (only for valid references).
        let parent_of: HashMap<&str, &str> = doc
            .entity_types
            .iter()
            .filter_map(|e| {
                e.parent.as_deref().and_then(|p| {
                    if entity_names.contains(p) {
                        Some((e.name.as_str(), p))
                    } else {
                        None
                    }
                })
            })
            .collect();

        let mut visited: HashSet<&str> = HashSet::new();
        let mut reported: HashSet<&str> = HashSet::new();

        for start in entity_names.iter().copied() {
            if visited.contains(start) {
                continue;
            }
            // Walk the ancestor chain; if we revisit a node from the current
            // chain we have found a cycle.
            let mut chain: Vec<&str> = Vec::new();
            let mut in_chain: HashSet<&str> = HashSet::new();
            let mut node = start;
            loop {
                if in_chain.contains(node) {
                    // Found cycle — report if not already reported.
                    if !reported.contains(node) {
                        reported.insert(node);
                        // Find where the cycle starts in the chain.
                        let cycle_start = chain.iter().position(|&n| n == node).unwrap_or(0);
                        let cycle: Vec<&str> = chain[cycle_start..].to_vec();
                        let path = cycle
                            .iter()
                            .chain(std::iter::once(&node))
                            .copied()
                            .collect::<Vec<_>>()
                            .join(" → ");
                        errors.push(OntologyValidationError {
                            kind: ValidationErrorKind::InheritanceCycle,
                            location: format!("entity_types[name='{node}'].parent"),
                            message: format!("inheritance cycle detected: {path}"),
                        });
                    }
                    break;
                }
                if visited.contains(node) {
                    break; // Already fully explored — no cycle from this path.
                }
                chain.push(node);
                in_chain.insert(node);
                visited.insert(node);
                if let Some(&parent) = parent_of.get(node) {
                    node = parent;
                } else {
                    break; // Reached a root.
                }
            }
        }
    }

    fn check_inverse_references(
        doc: &OntologyDoc,
        relation_names: &HashSet<&str>,
        errors: &mut Vec<OntologyValidationError>,
    ) {
        for (i, r) in doc.relation_types.iter().enumerate() {
            if let Some(inv) = &r.inverse
                && !relation_names.contains(inv.as_str())
            {
                errors.push(OntologyValidationError {
                    kind: ValidationErrorKind::UnresolvedReference,
                    location: format!("relation_types[{i}].inverse"),
                    message: format!(
                        "inverse relation '{inv}' of '{}' is not declared in relation_types",
                        r.name
                    ),
                });
            }
        }
    }

    fn check_inverse_mutual(doc: &OntologyDoc, errors: &mut Vec<OntologyValidationError>) {
        // Build a map from relation name to its declared inverse.
        let inverse_of: HashMap<&str, &str> = doc
            .relation_types
            .iter()
            .filter_map(|r| r.inverse.as_deref().map(|inv| (r.name.as_str(), inv)))
            .collect();

        for (i, r) in doc.relation_types.iter().enumerate() {
            let Some(inv_name) = r.inverse.as_deref() else {
                continue;
            };
            // inv_name must declare r.name as its inverse.
            match inverse_of.get(inv_name) {
                None => {
                    errors.push(OntologyValidationError {
                        kind: ValidationErrorKind::InverseInconsistency,
                        location: format!("relation_types[{i}].inverse"),
                        message: format!(
                            "'{}' declares inverse '{}', but '{}' does not declare any inverse",
                            r.name, inv_name, inv_name
                        ),
                    });
                }
                Some(&back) if back != r.name => {
                    errors.push(OntologyValidationError {
                        kind: ValidationErrorKind::InverseInconsistency,
                        location: format!("relation_types[{i}].inverse"),
                        message: format!(
                            "'{}' declares inverse '{}', but '{}' declares inverse '{}' (expected '{}')",
                            r.name, inv_name, inv_name, back, r.name
                        ),
                    });
                }
                _ => {} // Consistent.
            }
        }
    }

    fn check_relation_endpoints(
        doc: &OntologyDoc,
        entity_names: &HashSet<&str>,
        errors: &mut Vec<OntologyValidationError>,
    ) {
        for (i, r) in doc.relation_types.iter().enumerate() {
            if !entity_names.contains(r.src.as_str()) {
                errors.push(OntologyValidationError {
                    kind: ValidationErrorKind::UnresolvedReference,
                    location: format!("relation_types[{i}].src"),
                    message: format!(
                        "src type '{}' of relation '{}' is not declared in entity_types",
                        r.src, r.name
                    ),
                });
            }
            if !entity_names.contains(r.dst.as_str()) {
                errors.push(OntologyValidationError {
                    kind: ValidationErrorKind::UnresolvedReference,
                    location: format!("relation_types[{i}].dst"),
                    message: format!(
                        "dst type '{}' of relation '{}' is not declared in entity_types",
                        r.dst, r.name
                    ),
                });
            }
        }
    }

    fn check_property_owners(
        doc: &OntologyDoc,
        owner_names: &HashSet<&str>,
        errors: &mut Vec<OntologyValidationError>,
    ) {
        for (i, p) in doc.properties.iter().enumerate() {
            if !owner_names.contains(p.owner.as_str()) {
                errors.push(OntologyValidationError {
                    kind: ValidationErrorKind::UnresolvedReference,
                    location: format!("properties[{i}].owner"),
                    message: format!(
                        "owner '{}' of property '{}' is not declared as an entity or relation type",
                        p.owner, p.name
                    ),
                });
            }
        }
    }

    fn check_migration_order(doc: &OntologyDoc, errors: &mut Vec<OntologyValidationError>) {
        for (i, m) in doc.migrations.iter().enumerate() {
            if m.from_version >= m.to_version {
                errors.push(OntologyValidationError {
                    kind: ValidationErrorKind::MigrationVersionOrder,
                    location: format!("migrations[{i}]"),
                    message: format!(
                        "from_version '{}' must be strictly less than to_version '{}' \
                         (lexicographic comparison)",
                        m.from_version, m.to_version
                    ),
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::{
        ConstraintDef, ConstraintKind, EntityTypeDef, MigrationDef, OntologyDoc, PropertyDef,
        PropertyValueType, RelationTypeDef, SemanticFlags,
    };
    use std::io::Cursor;

    // Minimal valid document used as a baseline.
    fn valid_doc() -> OntologyDoc {
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
                    semantic: SemanticFlags::default(),
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
            migrations: vec![MigrationDef {
                from_version: "1.0".to_string(),
                to_version: "2.0".to_string(),
                transform_kind: "add_property".to_string(),
                script_ref: None,
                checksum: None,
            }],
        }
    }

    fn assert_has_kind(errors: &[OntologyValidationError], kind: &ValidationErrorKind) {
        assert!(
            errors.iter().any(|e| &e.kind == kind),
            "expected error kind {kind:?}, got: {errors:?}"
        );
    }

    #[test]
    fn valid_doc_passes() {
        assert!(OntologyValidator::validate(&valid_doc()).is_ok());
    }

    #[test]
    fn duplicate_entity_name() {
        let mut doc = valid_doc();
        doc.entity_types.push(EntityTypeDef {
            name: "Person".to_string(),
            r#abstract: false,
            parent: None,
        });
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::DuplicateName);
    }

    #[test]
    fn duplicate_relation_name() {
        let mut doc = valid_doc();
        doc.relation_types.push(RelationTypeDef {
            name: "MANAGES".to_string(),
            src: "Employee".to_string(),
            dst: "Employee".to_string(),
            inverse: None,
            semantic: SemanticFlags::default(),
        });
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::DuplicateName);
    }

    #[test]
    fn duplicate_property_same_owner() {
        let mut doc = valid_doc();
        doc.properties.push(PropertyDef {
            owner: "Person".to_string(),
            name: "name".to_string(), // duplicate
            value_type: PropertyValueType::Int64,
            nullable: true,
            multivalued: false,
            default_json: None,
        });
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::DuplicateName);
    }

    #[test]
    fn unresolved_parent() {
        let mut doc = valid_doc();
        doc.entity_types[1].parent = Some("Ghost".to_string());
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::UnresolvedReference);
        assert!(errs.iter().any(|e| e.location.contains("parent")));
    }

    #[test]
    fn inheritance_cycle() {
        let mut doc = valid_doc();
        // Make Person's parent = Employee, Employee's parent = Person → cycle
        doc.entity_types[0].parent = Some("Employee".to_string());
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::InheritanceCycle);
        assert!(errs.iter().any(|e| e.message.contains("→")));
    }

    #[test]
    fn unresolved_inverse() {
        let mut doc = valid_doc();
        doc.relation_types[0].inverse = Some("GHOST".to_string());
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::UnresolvedReference);
        assert!(errs.iter().any(|e| e.location.contains("inverse")));
    }

    #[test]
    fn inverse_not_mutual() {
        let mut doc = valid_doc();
        // Remove the inverse declaration from MANAGED_BY
        doc.relation_types[1].inverse = None;
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::InverseInconsistency);
    }

    #[test]
    fn inverse_wrong_mutual() {
        let mut doc = valid_doc();
        // MANAGED_BY declares inverse = "KNOWS" (wrong — should be MANAGES)
        doc.relation_types[1].inverse = Some("MANAGES_WRONG".to_string());
        // Add MANAGES_WRONG to keep the reference check clean
        doc.relation_types.push(RelationTypeDef {
            name: "MANAGES_WRONG".to_string(),
            src: "Employee".to_string(),
            dst: "Employee".to_string(),
            inverse: Some("MANAGED_BY".to_string()),
            semantic: SemanticFlags::default(),
        });
        // MANAGES still declares inverse=MANAGED_BY, but MANAGED_BY now declares
        // inverse=MANAGES_WRONG → inconsistency on MANAGES
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::InverseInconsistency);
    }

    #[test]
    fn unresolved_relation_src() {
        let mut doc = valid_doc();
        doc.relation_types[0].src = "Ghost".to_string();
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::UnresolvedReference);
        assert!(errs.iter().any(|e| e.location.contains(".src")));
    }

    #[test]
    fn unresolved_relation_dst() {
        let mut doc = valid_doc();
        doc.relation_types[0].dst = "Ghost".to_string();
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::UnresolvedReference);
        assert!(errs.iter().any(|e| e.location.contains(".dst")));
    }

    #[test]
    fn unresolved_property_owner() {
        let mut doc = valid_doc();
        doc.properties[0].owner = "Ghost".to_string();
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::UnresolvedReference);
        assert!(errs.iter().any(|e| e.location.contains("owner")));
    }

    #[test]
    fn migration_version_order_violation() {
        let mut doc = valid_doc();
        doc.migrations[0].from_version = "2.0".to_string();
        doc.migrations[0].to_version = "1.0".to_string();
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert_has_kind(&errs, &ValidationErrorKind::MigrationVersionOrder);
    }

    #[test]
    fn all_errors_collected() {
        // Build a doc with three independent violations.
        let mut doc = valid_doc();
        // 1. Duplicate entity name
        doc.entity_types.push(EntityTypeDef {
            name: "Person".to_string(),
            r#abstract: false,
            parent: None,
        });
        // 2. Unresolved property owner
        doc.properties.push(PropertyDef {
            owner: "Ghost".to_string(),
            name: "x".to_string(),
            value_type: PropertyValueType::Int64,
            nullable: true,
            multivalued: false,
            default_json: None,
        });
        // 3. Bad migration order
        doc.migrations[0].from_version = "9.9".to_string();
        doc.migrations[0].to_version = "1.0".to_string();
        let errs = OntologyValidator::validate(&doc).unwrap_err();
        assert!(
            errs.len() >= 3,
            "expected ≥3 errors, got {}: {errs:?}",
            errs.len()
        );
    }

    #[test]
    fn loader_rejects_invalid_doc() {
        use crate::error::OntologyError;
        use crate::loader::OntologyLoader;

        // A doc with an unresolved parent — should fail at the loader level.
        let yaml = r#"
ontology_id: bad
version: "1.0"
entity_types:
  - name: Employee
    parent: NonExistent
"#;
        let result = OntologyLoader::load_yaml(Cursor::new(yaml.as_bytes()));
        assert!(
            matches!(result, Err(OntologyError::Validation { .. })),
            "expected Validation error, got {result:?}"
        );
    }
}
