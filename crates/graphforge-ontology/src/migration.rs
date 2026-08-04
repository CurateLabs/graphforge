//! Ontology migration engine — plan and apply versioned schema transforms.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::compiler::{OntologyCompiler, OntologyRuntime};
use crate::error::OntologyError;
use crate::ontology::{EntityTypeDef, MigrationDef, OntologyDoc, PropertyDef, PropertyValueType};

// ---------------------------------------------------------------------------
// TransformKind
// ---------------------------------------------------------------------------

/// The semantic operation performed by a single migration step.
///
/// Unknown / unsupported transform strings are preserved as [`TransformKind::Unknown`]
/// and silently skipped during [`MigrationEngine::apply`].
#[allow(missing_docs)] // variant fields are self-documenting
#[derive(Debug, Clone)]
pub enum TransformKind {
    /// Rename an entity type and cascade to all references.
    RenameType { old_name: String, new_name: String },
    /// Rename a property on a given owner type.
    RenameProperty {
        owner: String,
        old_name: String,
        new_name: String,
    },
    /// Add a new property to an entity or relation type.
    AddProperty {
        owner: String,
        name: String,
        value_type: String,
        nullable: bool,
    },
    /// Remove a property from an entity or relation type.
    RemoveProperty { owner: String, name: String },
    /// Add a new (initially leaf) entity type.
    AddType { name: String },
    /// Remove an entity type and cascade-remove its properties and relations.
    RemoveType { name: String },
    /// An unrecognised transform — silently skipped.
    Unknown { raw: String },
}

// ---------------------------------------------------------------------------
// MigrationStep
// ---------------------------------------------------------------------------

/// A single resolved migration step produced by [`MigrationEngine::plan`].
#[derive(Debug, Clone)]
pub struct MigrationStep {
    /// The ontology version this step migrates from.
    pub from_version: String,
    /// The ontology version this step produces.
    pub to_version: String,
    /// The semantic operation to perform.
    pub transform_kind: TransformKind,
}

// ---------------------------------------------------------------------------
// Transform kind parsing
// ---------------------------------------------------------------------------

/// Parse a `MigrationDef.transform_kind` string into a typed [`TransformKind`].
///
/// Convention: `"kind"` or `"kind:payload"` where `payload` is `|`-separated fields.
///
/// | String | Meaning |
/// |---|---|
/// | `rename_type:OldName→NewName` | Rename entity type |
/// | `rename_property:Owner\|old→new` | Rename property |
/// | `add_property:Owner\|name\|value_type\|nullable` | Add property |
/// | `remove_property:Owner\|name` | Remove property |
/// | `add_type:Name` | Add entity type |
/// | `remove_type:Name` | Remove entity type |
fn parse_transform_kind(s: &str) -> TransformKind {
    let (kind, payload) = match s.split_once(':') {
        Some((k, p)) => (k.trim(), p.trim()),
        None => (s.trim(), ""),
    };

    match kind {
        "rename_type" => {
            if let Some((old, new)) = payload.split_once('→').or_else(|| payload.split_once("->"))
            {
                return TransformKind::RenameType {
                    old_name: old.trim().to_owned(),
                    new_name: new.trim().to_owned(),
                };
            }
        }
        "rename_property" => {
            let parts: Vec<&str> = payload.splitn(2, '|').collect();
            if parts.len() == 2 {
                let owner = parts[0].trim();
                if let Some((old, new)) = parts[1]
                    .split_once('→')
                    .or_else(|| parts[1].split_once("->"))
                {
                    return TransformKind::RenameProperty {
                        owner: owner.to_owned(),
                        old_name: old.trim().to_owned(),
                        new_name: new.trim().to_owned(),
                    };
                }
            }
        }
        "add_property" => {
            let parts: Vec<&str> = payload.splitn(4, '|').collect();
            if parts.len() >= 3 {
                let nullable = parts.get(3).copied().unwrap_or("true").trim() != "false";
                return TransformKind::AddProperty {
                    owner: parts[0].trim().to_owned(),
                    name: parts[1].trim().to_owned(),
                    value_type: parts[2].trim().to_owned(),
                    nullable,
                };
            }
        }
        "remove_property" => {
            let parts: Vec<&str> = payload.splitn(2, '|').collect();
            if parts.len() == 2 {
                return TransformKind::RemoveProperty {
                    owner: parts[0].trim().to_owned(),
                    name: parts[1].trim().to_owned(),
                };
            }
        }
        "add_type" if !payload.is_empty() => {
            return TransformKind::AddType {
                name: payload.to_owned(),
            };
        }
        "remove_type" if !payload.is_empty() => {
            return TransformKind::RemoveType {
                name: payload.to_owned(),
            };
        }
        _ => {}
    }

    TransformKind::Unknown { raw: s.to_owned() }
}

// ---------------------------------------------------------------------------
// MigrationEngine
// ---------------------------------------------------------------------------

/// Plans and applies versioned ontology migrations.
pub struct MigrationEngine;

impl MigrationEngine {
    /// Find the shortest migration path from `from` to `to` through the migration graph.
    ///
    /// Uses BFS over the `from_version → to_version` edges declared in `migrations`.
    ///
    /// # Errors
    /// Returns [`OntologyError::NoMigrationPath`] if no chain of steps reaches `to`.
    pub fn plan(
        from: &str,
        to: &str,
        migrations: &[MigrationDef],
    ) -> Result<Vec<MigrationStep>, OntologyError> {
        if from == to {
            return Ok(vec![]);
        }

        // Build adjacency: from_version → list of MigrationDef indices.
        let mut edges: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, m) in migrations.iter().enumerate() {
            edges.entry(m.from_version.as_str()).or_default().push(i);
        }

        // BFS to find shortest path.
        let mut visited: HashSet<&str> = HashSet::new();
        let mut queue: VecDeque<(&str, Vec<usize>)> = VecDeque::new();
        queue.push_back((from, vec![]));
        visited.insert(from);

        while let Some((current, path)) = queue.pop_front() {
            if let Some(neighbours) = edges.get(current) {
                for &idx in neighbours {
                    let m = &migrations[idx];
                    let next = m.to_version.as_str();
                    if next == to {
                        // Found — reconstruct path.
                        let mut steps: Vec<MigrationStep> = path
                            .iter()
                            .map(|&i| migration_to_step(&migrations[i]))
                            .collect();
                        steps.push(migration_to_step(m));
                        return Ok(steps);
                    }
                    if !visited.contains(next) {
                        visited.insert(next);
                        let mut new_path = path.clone();
                        new_path.push(idx);
                        queue.push_back((next, new_path));
                    }
                }
            }
        }

        Err(OntologyError::NoMigrationPath {
            from: from.to_owned(),
            to: to.to_owned(),
        })
    }

    /// Apply a sequence of migration steps to an `OntologyDoc` and compile the result.
    ///
    /// The `doc` is consumed and mutated. The returned [`OntologyRuntime`] reflects the
    /// transformed ontology at the final step's `to_version`.
    ///
    /// # Errors
    /// - [`OntologyError::NoMigrationPath`] if `doc.version` does not match the first
    ///   step's `from_version`, or if the steps are not contiguous.
    /// - [`OntologyError::Arrow`] if the final compilation fails.
    pub fn apply(
        mut doc: OntologyDoc,
        steps: &[MigrationStep],
    ) -> Result<OntologyRuntime, OntologyError> {
        // Validate that steps are contiguous and start from doc.version.
        if let Some(first) = steps.first() {
            if doc.version != first.from_version {
                return Err(OntologyError::NoMigrationPath {
                    from: doc.version.clone(),
                    to: steps
                        .last()
                        .map_or("", |s| s.to_version.as_str())
                        .to_owned(),
                });
            }
            for window in steps.windows(2) {
                if window[0].to_version != window[1].from_version {
                    return Err(OntologyError::NoMigrationPath {
                        from: window[0].to_version.clone(),
                        to: window[1].from_version.clone(),
                    });
                }
            }
        }

        for step in steps {
            apply_step(&mut doc, &step.transform_kind);
        }
        if let Some(last) = steps.last() {
            doc.version.clone_from(&last.to_version);
        }
        OntologyCompiler::compile(&doc)
    }
}

// ---------------------------------------------------------------------------
// Step application helpers
// ---------------------------------------------------------------------------

fn migration_to_step(m: &MigrationDef) -> MigrationStep {
    MigrationStep {
        from_version: m.from_version.clone(),
        to_version: m.to_version.clone(),
        transform_kind: parse_transform_kind(&m.transform_kind),
    }
}

fn apply_step(doc: &mut OntologyDoc, kind: &TransformKind) {
    match kind {
        TransformKind::RenameType { old_name, new_name } => {
            rename_type(doc, old_name, new_name);
        }
        TransformKind::RenameProperty {
            owner,
            old_name,
            new_name,
        } => {
            for p in &mut doc.properties {
                if &p.owner == owner && &p.name == old_name {
                    p.name.clone_from(new_name);
                }
            }
        }
        TransformKind::AddProperty {
            owner,
            name,
            value_type,
            nullable,
        } => {
            let vt = parse_value_type(value_type);
            doc.properties.push(PropertyDef {
                owner: owner.clone(),
                name: name.clone(),
                value_type: vt,
                nullable: *nullable,
                multivalued: false,
                default_json: None,
            });
        }
        TransformKind::RemoveProperty { owner, name } => {
            doc.properties
                .retain(|p| !(&p.owner == owner && &p.name == name));
        }
        TransformKind::AddType { name } => {
            doc.entity_types.push(EntityTypeDef {
                name: name.clone(),
                r#abstract: false,
                parent: None,
            });
        }
        TransformKind::RemoveType { name } => {
            doc.entity_types.retain(|e| &e.name != name);
            // Cascade: clear parent references, remove properties and relations.
            for e in &mut doc.entity_types {
                if e.parent.as_deref() == Some(name.as_str()) {
                    e.parent = None;
                }
            }
            doc.properties.retain(|p| &p.owner != name);
            doc.relation_types
                .retain(|r| &r.src != name && &r.dst != name);
            doc.constraints.retain(|c| &c.owner != name);
        }
        TransformKind::Unknown { .. } => {
            // Silently skip unknown transforms.
        }
    }
}

fn rename_type(doc: &mut OntologyDoc, old: &str, new: &str) {
    // Rename in entity_types.
    for e in &mut doc.entity_types {
        if e.name == old {
            new.clone_into(&mut e.name);
        }
        if e.parent.as_deref() == Some(old) {
            e.parent = Some(new.to_owned());
        }
    }
    // Cascade to relation types.
    for r in &mut doc.relation_types {
        if r.src == old {
            new.clone_into(&mut r.src);
        }
        if r.dst == old {
            new.clone_into(&mut r.dst);
        }
    }
    // Cascade to properties.
    for p in &mut doc.properties {
        if p.owner == old {
            new.clone_into(&mut p.owner);
        }
    }
    // Cascade to constraints.
    for c in &mut doc.constraints {
        if c.owner == old {
            new.clone_into(&mut c.owner);
        }
    }
}

fn parse_value_type(s: &str) -> PropertyValueType {
    match s.trim().to_lowercase().as_str() {
        "int64" => PropertyValueType::Int64,
        "float64" => PropertyValueType::Float64,
        "bool" => PropertyValueType::Bool,
        "duration" => PropertyValueType::Duration,
        "datetime" => PropertyValueType::DateTime,
        "list" => PropertyValueType::List,
        "map" => PropertyValueType::Map,
        _ => PropertyValueType::Utf8,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::{EntityTypeDef, OntologyDoc, PropertyDef, PropertyValueType};
    use arrow::array::StringArray;

    fn make_def(from: &str, to: &str, kind: &str) -> MigrationDef {
        MigrationDef {
            from_version: from.to_owned(),
            to_version: to.to_owned(),
            transform_kind: kind.to_owned(),
            script_ref: None,
            checksum: None,
        }
    }

    fn base_doc() -> OntologyDoc {
        OntologyDoc {
            ontology_id: "test".to_owned(),
            version: "v1".to_owned(),
            entity_types: vec![EntityTypeDef {
                name: "Person".to_owned(),
                r#abstract: false,
                parent: None,
            }],
            relation_types: vec![],
            properties: vec![PropertyDef {
                owner: "Person".to_owned(),
                name: "name".to_owned(),
                value_type: PropertyValueType::Utf8,
                nullable: false,
                multivalued: false,
                default_json: None,
            }],
            constraints: vec![],
            migrations: vec![],
        }
    }

    // --- plan tests ---

    #[test]
    fn plan_same_version_returns_empty() {
        let steps = MigrationEngine::plan("v1", "v1", &[]).unwrap();
        assert!(steps.is_empty());
    }

    #[test]
    fn plan_single_step() {
        let defs = vec![make_def("v1", "v2", "add_type:Manager")];
        let steps = MigrationEngine::plan("v1", "v2", &defs).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].from_version, "v1");
        assert_eq!(steps[0].to_version, "v2");
    }

    #[test]
    fn plan_multi_step_chain() {
        let defs = vec![
            make_def("v1", "v2", "add_type:Manager"),
            make_def("v2", "v3", "add_type:Director"),
        ];
        let steps = MigrationEngine::plan("v1", "v3", &defs).unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].to_version, "v2");
        assert_eq!(steps[1].to_version, "v3");
    }

    #[test]
    fn plan_no_path_returns_error() {
        let result = MigrationEngine::plan("v1", "v3", &[]);
        assert!(
            matches!(result, Err(OntologyError::NoMigrationPath { .. })),
            "expected NoMigrationPath"
        );
    }

    #[test]
    fn plan_shortest_path() {
        // Two paths: v1→v3 (direct) and v1→v2→v3.
        let defs = vec![
            make_def("v1", "v2", "add_type:A"),
            make_def("v2", "v3", "add_type:B"),
            make_def("v1", "v3", "add_type:C"), // direct shortcut
        ];
        let steps = MigrationEngine::plan("v1", "v3", &defs).unwrap();
        // BFS finds the 1-hop direct path first.
        assert_eq!(steps.len(), 1, "shortest path should be 1 hop");
    }

    // --- apply tests ---

    #[test]
    fn apply_rename_type_updates_name() {
        let doc = base_doc();
        let steps = vec![MigrationStep {
            from_version: "v1".to_owned(),
            to_version: "v2".to_owned(),
            transform_kind: TransformKind::RenameType {
                old_name: "Person".to_owned(),
                new_name: "Human".to_owned(),
            },
        }];
        let rt = MigrationEngine::apply(doc, &steps).unwrap();
        // entity_types table col 1 = name
        let names = rt
            .entity_types
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Human");
    }

    #[test]
    fn apply_rename_property() {
        let doc = base_doc();
        let steps = vec![MigrationStep {
            from_version: "v1".to_owned(),
            to_version: "v2".to_owned(),
            transform_kind: TransformKind::RenameProperty {
                owner: "Person".to_owned(),
                old_name: "name".to_owned(),
                new_name: "full_name".to_owned(),
            },
        }];
        let rt = MigrationEngine::apply(doc, &steps).unwrap();
        // property_types table col 3 = name
        let prop_names = rt
            .property_types
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(prop_names.value(0), "full_name");
    }

    #[test]
    fn apply_add_property_increases_count() {
        let doc = base_doc();
        let before = doc.properties.len();
        let steps = vec![MigrationStep {
            from_version: "v1".to_owned(),
            to_version: "v2".to_owned(),
            transform_kind: TransformKind::AddProperty {
                owner: "Person".to_owned(),
                name: "email".to_owned(),
                value_type: "utf8".to_owned(),
                nullable: true,
            },
        }];
        let rt = MigrationEngine::apply(doc, &steps).unwrap();
        assert_eq!(rt.property_types.num_rows(), before + 1);
    }

    #[test]
    fn apply_remove_property_decreases_count() {
        let doc = base_doc();
        let before = doc.properties.len();
        let steps = vec![MigrationStep {
            from_version: "v1".to_owned(),
            to_version: "v2".to_owned(),
            transform_kind: TransformKind::RemoveProperty {
                owner: "Person".to_owned(),
                name: "name".to_owned(),
            },
        }];
        let rt = MigrationEngine::apply(doc, &steps).unwrap();
        assert_eq!(rt.property_types.num_rows(), before - 1);
    }

    #[test]
    fn apply_version_updated_after_migration() {
        let doc = base_doc();
        let steps = vec![MigrationStep {
            from_version: "v1".to_owned(),
            to_version: "v2".to_owned(),
            transform_kind: TransformKind::AddType {
                name: "Manager".to_owned(),
            },
        }];
        let rt = MigrationEngine::apply(doc, &steps).unwrap();
        // ontology_meta col 1 = version
        let versions = rt
            .ontology_meta
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(versions.value(0), "v2");
    }

    #[test]
    fn plan_and_apply_chain_version_is_final() {
        let mut doc = base_doc();
        doc.migrations = vec![
            make_def("v1", "v2", "add_type:Manager"),
            make_def("v2", "v3", "add_type:Director"),
        ];
        let steps = MigrationEngine::plan("v1", "v3", &doc.migrations).unwrap();
        assert_eq!(steps.len(), 2);
        let rt = MigrationEngine::apply(doc, &steps).unwrap();
        let versions = rt
            .ontology_meta
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(versions.value(0), "v3");
    }

    #[test]
    fn transform_parser_and_apply_cover_every_closed_operation_and_unknown_noop() {
        let definitions = vec![
            make_def("v1", "v2", "rename_type:Person->Human"),
            make_def("v2", "v3", "rename_property:Human|name->display_name"),
            make_def("v3", "v4", "add_property:Human|age|int64|false"),
            make_def("v4", "v5", "remove_property:Human|display_name"),
            make_def("v5", "v6", "add_type:Company"),
            make_def("v6", "v7", "remove_type:Company"),
            make_def("v7", "v8", "future_transform:payload"),
        ];
        let steps = MigrationEngine::plan("v1", "v8", &definitions).unwrap();
        assert_eq!(steps.len(), definitions.len());
        assert!(matches!(
            steps[0].transform_kind,
            TransformKind::RenameType { .. }
        ));
        assert!(matches!(
            steps[1].transform_kind,
            TransformKind::RenameProperty { .. }
        ));
        assert!(matches!(
            steps[2].transform_kind,
            TransformKind::AddProperty { .. }
        ));
        assert!(matches!(
            steps[3].transform_kind,
            TransformKind::RemoveProperty { .. }
        ));
        assert!(matches!(
            steps[4].transform_kind,
            TransformKind::AddType { .. }
        ));
        assert!(matches!(
            steps[5].transform_kind,
            TransformKind::RemoveType { .. }
        ));
        assert!(matches!(
            steps[6].transform_kind,
            TransformKind::Unknown { .. }
        ));

        let runtime = MigrationEngine::apply(base_doc(), &steps).unwrap();
        assert!(runtime.entity_name_to_id.contains_key("Human"));
        assert!(!runtime.entity_name_to_id.contains_key("Person"));
        assert!(!runtime.entity_name_to_id.contains_key("Company"));
    }

    #[test]
    fn malformed_transform_payloads_are_unknown_and_value_types_are_total() {
        for raw in [
            "rename_type",
            "rename_property:Person",
            "add_property:Person|name",
            "remove_property:Person",
            "add_type:",
            "remove_type:",
            "unknown:anything",
        ] {
            assert!(matches!(
                parse_transform_kind(raw),
                TransformKind::Unknown { .. }
            ));
        }
        for (raw, expected) in [
            ("int64", PropertyValueType::Int64),
            ("FLOAT64", PropertyValueType::Float64),
            ("bool", PropertyValueType::Bool),
            ("duration", PropertyValueType::Duration),
            ("datetime", PropertyValueType::DateTime),
            ("list", PropertyValueType::List),
            ("map", PropertyValueType::Map),
            ("unknown", PropertyValueType::Utf8),
        ] {
            assert_eq!(parse_value_type(raw), expected);
        }
    }

    #[test]
    fn apply_rejects_wrong_start_and_noncontiguous_steps_without_mutating_input() {
        let doc = base_doc();
        let wrong_start = [MigrationStep {
            from_version: "v0".into(),
            to_version: "v2".into(),
            transform_kind: TransformKind::AddType { name: "X".into() },
        }];
        assert!(matches!(
            MigrationEngine::apply(doc.clone(), &wrong_start),
            Err(OntologyError::NoMigrationPath { .. })
        ));
        assert_eq!(doc.version, "v1");
        assert!(!doc.entity_types.iter().any(|item| item.name == "X"));

        let gap = [
            MigrationStep {
                from_version: "v1".into(),
                to_version: "v2".into(),
                transform_kind: TransformKind::Unknown { raw: "noop".into() },
            },
            MigrationStep {
                from_version: "v3".into(),
                to_version: "v4".into(),
                transform_kind: TransformKind::Unknown { raw: "noop".into() },
            },
        ];
        assert!(matches!(
            MigrationEngine::apply(doc, &gap),
            Err(OntologyError::NoMigrationPath { .. })
        ));
    }
}
