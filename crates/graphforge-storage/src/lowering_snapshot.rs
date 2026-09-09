//! Capture schema facts before entering the relational compiler.
use crate::{GfError, GraphCatalog};
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use graphforge_ir::LoweringSnapshot;
use std::path::Path;

impl GraphCatalog {
    /// Capture catalog names and, when requested, dataset schemas without graph rows.
    pub fn lowering_snapshot(&self, dir: Option<&Path>) -> Result<LoweringSnapshot, GfError> {
        let mut snapshot = LoweringSnapshot {
            property_names: self.prop_names().clone(),
            runtime_labels: self.label_names().clone(),
            runtime_relations: self.rel_names().clone(),
            semantic_relations: self.semantic_rel_routes().clone(),
            semantic_labels: self.semantic_label_routes().clone(),
            semantic_display_labels: self.semantic_label_names().clone(),
            composition: self.semantic_composition_fingerprint().map(str::to_owned),
            ..Default::default()
        };
        if let Some(schema) = self.schema("graph") {
            snapshot.typed_edge_tables = schema.table_names().into_iter().collect();
        }
        for id in self.semantic_rel_routes().keys() {
            if let Some(table) = self.semantic_edge_table(*id) {
                snapshot.semantic_edges.insert(*id, table.schema());
            }
            if let Some(table) = self.semantic_edge_property_table(*id) {
                snapshot
                    .semantic_edge_properties
                    .insert(*id, table.schema());
            }
        }
        if let Some(dir) = dir {
            let inventory = self.lowering_property_inventory();
            if inventory.is_none() && !dir.exists() {
                // A standalone writer may target a directory that it will create
                // only at execution. Capture the empty schema without creating it.
                snapshot.node_schema = Some(
                    crate::TopologyNodeTable::open_project(dir)
                        .map_err(GfError::from_plan_error)?
                        .schema(),
                );
                return Ok(snapshot);
            }
            let inventory = inventory.ok_or_else(|| {
                GfError::Validation(
                    "dataset lowering snapshot requires a retained authenticated catalog inventory"
                        .into(),
                )
            })?;
            snapshot.node_schema = Some(
                crate::TopologyNodeTable::open_project(dir)
                    .map_err(GfError::from_plan_error)?
                    .schema(),
            );
            snapshot.node_property_stems = inventory.discovered_routes(
                crate::PropertyRouteKind::Node,
                &crate::list_property_stems(dir),
            );
            snapshot.edge_property_stems = inventory.discovered_routes(
                crate::PropertyRouteKind::Edge,
                &crate::list_edge_property_stems(dir),
            );
            let mut nodes: std::collections::BTreeSet<String> =
                snapshot.node_property_stems.iter().cloned().collect();
            nodes.extend(snapshot.semantic_labels.values().cloned());
            let mut edges: std::collections::BTreeSet<String> =
                snapshot.edge_property_stems.iter().cloned().collect();
            {
                nodes.extend(
                    inventory
                        .routes(crate::PropertyRouteKind::Node)
                        .map(str::to_owned),
                );
                edges.extend(
                    inventory
                        .routes(crate::PropertyRouteKind::Edge)
                        .map(str::to_owned),
                );
            }
            for stem in nodes {
                snapshot.node_properties.insert(
                    stem.clone(),
                    crate::PropertyTable::open_authenticated(
                        dir,
                        &stem,
                        std::sync::Arc::clone(&inventory),
                    )
                    .schema(),
                );
            }
            for stem in edges {
                snapshot.edge_properties.insert(
                    stem.clone(),
                    crate::EdgePropertyTable::open_authenticated(
                        dir,
                        &stem,
                        std::sync::Arc::clone(&inventory),
                    )
                    .schema(),
                );
            }
        }
        Ok(snapshot)
    }
}

/// Capture a lowering snapshot for schema-only or dataset compilation.
pub fn lowering_snapshot(
    catalog: Option<&GraphCatalog>,
    dir: Option<&Path>,
) -> Result<LoweringSnapshot, GfError> {
    if let Some(catalog) = catalog {
        return catalog.lowering_snapshot(dir);
    }
    if dir.is_some() {
        return Err(GfError::Validation(
            "dataset lowering snapshot requires an admitted catalog".into(),
        ));
    }
    Ok(LoweringSnapshot::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_core::{OntologyMode, TypeId};
    use graphforge_ir::{IrLiteral, RuntimeCatalog};
    use graphforge_ontology::{OntologyModuleId, QualifiedSymbol, SymbolKind};
    use graphforge_value::EntityTypeId;
    use std::collections::HashMap;

    #[test]
    fn dataset_snapshot_requires_admission_but_preserves_absent_write_target() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("not-created");
        let catalog = GraphCatalog::open(&target, None, &RuntimeCatalog::new()).unwrap();
        let empty = catalog.lowering_snapshot(Some(&target)).unwrap();
        assert!(empty.node_schema.is_some());
        assert!(empty.node_properties.is_empty());
        assert!(!target.exists());
        assert!(
            matches!(lowering_snapshot(None, Some(&target)), Err(GfError::Validation(message))
            if message == "dataset lowering snapshot requires an admitted catalog")
        );
        // Malformed data must not be opened or authenticated by a fallback.
        // The retained catalog has no inventory; rejection precedes discovery.
        std::fs::create_dir_all(target.join("properties")).unwrap();
        std::fs::write(target.join("properties/Person.parquet"), b"not parquet").unwrap();
        assert!(
            matches!(catalog.lowering_snapshot(Some(&target)), Err(GfError::Validation(message))
            if message == "dataset lowering snapshot requires a retained authenticated catalog inventory")
        );
    }

    #[test]
    fn selected_semantic_schema_survives_absent_discovery_without_expanding_unions() {
        let dir = tempfile::tempdir().unwrap();
        let symbol = QualifiedSymbol {
            module: OntologyModuleId {
                ontology_id: "https://example.test/schema".into(),
                authored_version: "1".into(),
                canonical_digest: "a".repeat(64),
            },
            kind: SymbolKind::Entity,
            local_id: "Person".into(),
        };
        let route = crate::SemanticStorageBindings::opaque_route(
            crate::SemanticRouteKind::Entity,
            &symbol,
            None,
        );
        let id = EntityTypeId::ontology(TypeId(1)).unwrap();
        let uuid = graphforge_core::uuid::new_v7();
        let mut writer = crate::GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        writer.create_node(uuid, id).unwrap();
        writer
            .set_properties(
                &uuid,
                Some(&route),
                HashMap::from([("name".into(), IrLiteral::Str("retained".into()))]),
            )
            .unwrap();
        writer.flush().unwrap();
        drop(writer);
        let bindings = crate::SemanticStorageBindings {
            contract_version: 1,
            composition_fingerprint: "b".repeat(64),
            bindings: vec![crate::SemanticStorageBinding {
                route_kind: crate::SemanticRouteKind::Entity,
                storage_id: id.encode(),
                route: route.clone(),
                symbol,
                owner: None,
            }],
        };
        let catalog = GraphCatalog::open_with_semantic_bindings(
            dir.path(),
            None,
            &RuntimeCatalog::new(),
            Some(&bindings),
        )
        .unwrap();
        let before = catalog.lowering_snapshot(Some(dir.path())).unwrap();
        assert_eq!(before.node_property_stems, vec![route.clone()]);
        let expected = before.node_properties[&route].clone();
        assert!(expected.field_with_name("name").is_ok());
        std::fs::rename(
            dir.path().join("properties"),
            dir.path().join("retained-properties"),
        )
        .unwrap();
        assert!(crate::list_property_stems(dir.path()).is_empty());
        let after = catalog.lowering_snapshot(Some(dir.path())).unwrap();
        assert_eq!(after.semantic_labels[&id], route);
        assert_eq!(after.node_properties[&route], expected);
        assert!(
            after.node_property_stems.is_empty(),
            "cached selected schemas must not expand the discovered path union"
        );
        assert_eq!(after.edge_property_stems, before.edge_property_stems);
    }
}
