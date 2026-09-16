use super::super::tests::{compiled, compiled_with};
use super::super::*;
use graphforge_ontology::{
    ActivationMode, AuthoredModule, CompositionLimits, InventoryCompileRequest, MigrationDef,
    OntologyModuleId, PropertyDef, PropertyValueType, compile_inventory, module_document_digest,
};

fn renamed_compiled() -> CompiledComposition {
    let mut doc = compiled_with("1", false, true).modules.remove(0).doc;
    doc.version = "2".into();
    doc.entity_types[0].name = "Human".into();
    doc.relation_types[0].src = "Human".into();
    doc.relation_types[0].dst = "Human".into();
    for property in &mut doc.properties {
        if property.owner == "Person" {
            property.owner = "Human".into();
        }
    }
    doc.properties[0].name = "display_name".into();
    doc.migrations = vec![
        MigrationDef {
            from_version: "1".into(),
            to_version: "1.5".into(),
            transform_kind: "rename_type:Person->Human".into(),
            script_ref: None,
            checksum: None,
        },
        MigrationDef {
            from_version: "1.5".into(),
            to_version: "2".into(),
            transform_kind: "rename_property:Human|name->display_name".into(),
            script_ref: None,
            checksum: None,
        },
    ];
    let authored = AuthoredModule {
        id: OntologyModuleId {
            ontology_id: doc.ontology_id.clone(),
            authored_version: doc.version.clone(),
            canonical_digest: module_document_digest(&doc).unwrap(),
        },
        dependencies: vec![],
        doc,
        allow_projected_identity: false,
    };
    compile_inventory(InventoryCompileRequest {
        modules: &[authored],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Strict,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap()
}

#[test]
fn retained_entity_and_property_rename_materializes_deterministically() {
    use std::collections::HashMap;

    let old = compiled_with("1", false, true);
    let old_bindings = SemanticStorageBindings::project(&old, None).unwrap();
    let entity = old_bindings
        .bindings
        .iter()
        .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
        .unwrap();
    let source = tempfile::TempDir::new().unwrap();
    let mut writer =
        crate::GraphWriter::open_at(source.path(), graphforge_core::OntologyMode::Strict, 1)
            .unwrap()
            .with_semantic_composition_fingerprint(Some(old.fingerprint.clone()));
    let node = graphforge_core::uuid::new_v7();
    writer
        .create_node(
            node,
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(entity.storage_id))
                .unwrap(),
        )
        .unwrap();
    writer
        .set_properties(
            &node,
            Some(&entity.route),
            HashMap::from([
                ("name".into(), graphforge_ir::IrLiteral::Str("Ada".into())),
                ("birth_year".into(), graphforge_ir::IrLiteral::Int(1815)),
            ]),
        )
        .unwrap();
    writer.flush().unwrap();

    let next = renamed_compiled();
    let first = SemanticStorageBindings::plan_retained_data_migration(
        &old,
        &next,
        &old_bindings,
        source.path(),
    )
    .unwrap();
    let second = SemanticStorageBindings::plan_retained_data_migration(
        &old,
        &next,
        &old_bindings,
        source.path(),
    )
    .unwrap();
    assert_eq!(first, second);
    assert!(first.retained_rows_scanned > 0);
    let renamed_entity = first
        .bindings
        .bindings
        .iter()
        .find(|binding| binding.symbol.local_id == "Human")
        .unwrap();
    assert_eq!(renamed_entity.storage_id, entity.storage_id);

    let parent = tempfile::TempDir::new().unwrap();
    let candidate = parent.path().join("candidate");
    let evidence = materialize_semantic_migration(
        &first,
        source.path(),
        &candidate,
        SemanticMigrationLimits::default(),
        || Ok(()),
    )
    .unwrap();
    assert_eq!(evidence.plan_digest, first.plan_digest);
    let source_after = crate::capture_graph_files(source.path()).unwrap().0;
    assert_eq!(
        hex(Sha256::digest(crate::encode_inventory(&source_after).unwrap()).into()),
        first.source_inventory_sha256
    );
    let candidate_inventory = crate::capture_graph_files(&candidate).unwrap().0;
    let table =
        crate::graph_files::authenticate_route_table(&candidate, &candidate_inventory).unwrap();
    table
        .validate_paths(
            candidate_inventory
                .files
                .iter()
                .map(|entry| entry.relative_path.as_str()),
        )
        .unwrap();
    let routes = candidate_inventory
        .files
        .iter()
        .map(|entry| table.semantic_relative_path(&entry.relative_path).unwrap())
        .collect::<Vec<_>>();
    assert!(routes.iter().any(|path| {
        path.starts_with(&format!("properties/{}/", renamed_entity.route))
            || path == &format!("properties/{}.parquet", renamed_entity.route)
    }));
    assert!(!routes.iter().any(
        |path| path.starts_with(&format!("properties/{}/", entity.route))
            || path == &format!("properties/{}.parquet", entity.route)
    ));
    first.bindings.validate_physical_routes(&candidate).unwrap();
    let batches = crate::catalog::read_properties(&candidate, &renamed_entity.route).unwrap();
    let schema = batches.first().unwrap().schema();
    assert!(schema.field_with_name("display_name").is_ok());
    assert!(schema.field_with_name("birth_year").is_ok());
    assert!(schema.field_with_name("name").is_err());
    let batch = batches.first().unwrap();
    let birth_year = batch
        .column_by_name("birth_year")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(birth_year.value(0), 1815);
}

#[test]
fn cancelled_materialization_removes_private_candidate() {
    let old = compiled_with("1", false, true);
    let bindings = SemanticStorageBindings::project(&old, None).unwrap();
    let source = tempfile::TempDir::new().unwrap();
    std::fs::create_dir_all(source.path().join("catalog")).unwrap();
    std::fs::write(source.path().join("catalog/state.json"), b"{}\n").unwrap();
    let next = renamed_compiled();
    let plan = SemanticStorageBindings::plan_retained_data_migration(
        &old,
        &next,
        &bindings,
        source.path(),
    )
    .unwrap();
    let parent = tempfile::TempDir::new().unwrap();
    let candidate = parent.path().join("candidate");
    let mut checkpoints = 0;
    let error = materialize_semantic_migration(
        &plan,
        source.path(),
        &candidate,
        SemanticMigrationLimits::default(),
        || {
            checkpoints += 1;
            if checkpoints == 2 {
                Err(GfError::Validation("cancelled".into()))
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert!(!candidate.exists());
    assert!(std::fs::read_dir(parent.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".semantic-migration-")
    }));
}

#[test]
fn materializer_rejects_graph_inventory_drift_from_preview() {
    let old = compiled_with("1", false, true);
    let bindings = SemanticStorageBindings::project(&old, None).unwrap();
    let source = tempfile::TempDir::new().unwrap();
    let next = renamed_compiled();
    let plan = SemanticStorageBindings::plan_retained_data_migration(
        &old,
        &next,
        &bindings,
        source.path(),
    )
    .unwrap();
    std::fs::create_dir_all(source.path().join("catalog")).unwrap();
    std::fs::write(source.path().join("catalog/drift.json"), b"{}\n").unwrap();
    let parent = tempfile::TempDir::new().unwrap();
    let candidate = parent.path().join("candidate");
    let error = materialize_semantic_migration(
        &plan,
        source.path(),
        &candidate,
        SemanticMigrationLimits::default(),
        || Ok(()),
    )
    .unwrap_err();
    assert!(error.to_string().contains("differs from the planned"));
    assert!(!candidate.exists());
}

#[test]
fn non_null_property_addition_rejects_retained_owner_without_backfill() {
    let old = compiled_with("1", false, true);
    let bindings = SemanticStorageBindings::project(&old, None).unwrap();
    let entity = bindings
        .bindings
        .iter()
        .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
        .unwrap();
    let source = tempfile::TempDir::new().unwrap();
    let mut writer =
        crate::GraphWriter::open_at(source.path(), graphforge_core::OntologyMode::Strict, 1)
            .unwrap()
            .with_semantic_composition_fingerprint(Some(old.fingerprint.clone()));
    writer
        .create_node(
            graphforge_core::uuid::new_v7(),
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(entity.storage_id))
                .unwrap(),
        )
        .unwrap();
    writer.flush().unwrap();

    let mut doc = compiled_with("1", false, true).modules.remove(0).doc;
    doc.version = "2".into();
    doc.properties.push(PropertyDef {
        owner: "Person".into(),
        name: "required_code".into(),
        value_type: PropertyValueType::Utf8,
        nullable: false,
        multivalued: false,
        default_json: Some("\"unknown\"".into()),
    });
    doc.migrations.push(MigrationDef {
        from_version: "1".into(),
        to_version: "2".into(),
        transform_kind: "add_property:Person|required_code|utf8|false".into(),
        script_ref: None,
        checksum: None,
    });
    let authored = AuthoredModule {
        id: OntologyModuleId {
            ontology_id: doc.ontology_id.clone(),
            authored_version: doc.version.clone(),
            canonical_digest: module_document_digest(&doc).unwrap(),
        },
        dependencies: vec![],
        doc,
        allow_projected_identity: false,
    };
    let next = compile_inventory(InventoryCompileRequest {
        modules: &[authored],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Strict,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    let error = SemanticStorageBindings::plan_retained_data_migration(
        &old,
        &next,
        &bindings,
        source.path(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("typed retained-data backfill"));
}

#[test]
fn owner_routes_match_write_routing_and_ids_carry_across_module_upgrade() {
    let first = compiled("1");
    let initial = SemanticStorageBindings::project(&first, None).unwrap();
    let entity = initial
        .bindings
        .iter()
        .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
        .unwrap();
    let node_property = initial
        .bindings
        .iter()
        .find(|binding| binding.route_kind == SemanticRouteKind::NodeProperty)
        .unwrap();
    let relation = initial
        .bindings
        .iter()
        .find(|binding| binding.route_kind == SemanticRouteKind::Relation)
        .unwrap();
    let edge_property = initial
        .bindings
        .iter()
        .find(|binding| binding.route_kind == SemanticRouteKind::EdgeProperty)
        .unwrap();
    assert_eq!(entity.route, node_property.route);
    assert_eq!(relation.route, edge_property.route);

    let upgraded = compiled("2");
    let carried = SemanticStorageBindings::project(&upgraded, Some(&initial)).unwrap();
    for binding in &carried.bindings {
        let old = initial
            .bindings
            .iter()
            .find(|old| {
                old.route_kind == binding.route_kind
                    && old.symbol.local_id == binding.symbol.local_id
            })
            .unwrap();
        assert_eq!(binding.storage_id, old.storage_id);
    }

    assert_eq!(
        SemanticStorageBindings::project(&upgraded, Some(&carried)).unwrap(),
        carried,
        "reprojecting the same generation must be idempotent"
    );
    let undeclared = compiled_with("3", false, true);
    assert!(SemanticStorageBindings::project(&undeclared, Some(&carried)).is_err());
}

#[test]
fn removal_requires_a_pinned_scan_and_refuses_retained_property_data() {
    use std::collections::HashMap;

    let first = compiled("1");
    let initial = SemanticStorageBindings::project(&first, None).unwrap();
    let relation = initial
        .bindings
        .iter()
        .find(|binding| binding.route_kind == SemanticRouteKind::Relation)
        .unwrap();
    let next = compiled_with("2", true, false);
    assert!(SemanticStorageBindings::project(&next, Some(&initial)).is_err());

    let empty = tempfile::TempDir::new().unwrap();
    let removed =
        SemanticStorageBindings::project_with_graph_scan(&next, Some(&initial), empty.path())
            .unwrap();
    assert!(!removed.bindings.iter().any(|binding| {
        binding.route_kind == SemanticRouteKind::EdgeProperty
            && binding.symbol.local_id == "KNOWS:since"
    }));

    let other_column = tempfile::TempDir::new().unwrap();
    let mut writer = crate::GraphWriter::open_at(
        other_column.path(),
        graphforge_core::OntologyMode::Strict,
        1,
    )
    .unwrap()
    .with_semantic_composition_fingerprint(Some(first.fingerprint.clone()));
    let left = graphforge_core::uuid::new_v7();
    let right = graphforge_core::uuid::new_v7();
    writer
        .create_node(
            left,
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    writer
        .create_node(
            right,
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    let edge = graphforge_core::uuid::new_v7();
    writer
        .create_edge(edge, &relation.route, &left, &right)
        .unwrap();
    writer
        .set_edge_properties(
            &edge,
            Some(&relation.route),
            HashMap::from([("other".into(), graphforge_ir::IrLiteral::Int(1))]),
        )
        .unwrap();
    writer.flush().unwrap();
    SemanticStorageBindings::project_with_graph_scan(&next, Some(&initial), other_column.path())
        .expect("an unrelated populated owner column must not retain the removed property");

    let retained = tempfile::TempDir::new().unwrap();
    let mut writer =
        crate::GraphWriter::open_at(retained.path(), graphforge_core::OntologyMode::Strict, 1)
            .unwrap()
            .with_semantic_composition_fingerprint(Some(first.fingerprint.clone()));
    let left = graphforge_core::uuid::new_v7();
    let right = graphforge_core::uuid::new_v7();
    writer
        .create_node(
            left,
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    writer
        .create_node(
            right,
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    let edge = graphforge_core::uuid::new_v7();
    writer
        .create_edge(edge, &relation.route, &left, &right)
        .unwrap();
    writer
        .set_edge_properties(
            &edge,
            Some(&relation.route),
            HashMap::from([("since".into(), graphforge_ir::IrLiteral::Int(2026))]),
        )
        .unwrap();
    writer.flush().unwrap();
    assert!(
        SemanticStorageBindings::project_with_graph_scan(&next, Some(&initial), retained.path(),)
            .is_err()
    );
}

#[test]
fn retained_entity_scan_includes_immutable_node_shards() {
    let composition = compiled("1");
    let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
    let entity = bindings
        .bindings
        .iter()
        .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
        .unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    for generation in 1_i64..=16 {
        let mut writer = crate::GraphWriter::open_at(
            dir.path(),
            graphforge_core::OntologyMode::Strict,
            generation,
        )
        .unwrap();
        writer
            .create_node(
                graphforge_core::uuid::new_v7(),
                graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(
                    if generation == 16 {
                        entity.storage_id
                    } else {
                        999
                    },
                ))
                .unwrap(),
            )
            .unwrap();
        writer.flush().unwrap();
    }
    assert_eq!(
        crate::catalog::topology_node_files(dir.path())
            .unwrap()
            .len(),
        16
    );

    assert!(
        SemanticStorageBindings::binding_has_retained_data(entity, dir.path()).unwrap(),
        "a binding used only by an immutable node shard must remain protected"
    );
}

#[test]
fn retained_entity_scan_preflights_every_shard_before_decoding() {
    use std::io::Write as _;

    let composition = compiled("1");
    let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
    let entity = bindings
        .bindings
        .iter()
        .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
        .unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let shard = dir
        .path()
        .join("topology/nodes/00000000000000000001-00000000000000000001.parquet");
    std::fs::create_dir_all(shard.parent().unwrap()).unwrap();
    let mut file = File::create(&shard).unwrap();
    file.write_all(b"PAR1").unwrap();
    file.write_all(&(u32::MAX).to_le_bytes()).unwrap();
    file.write_all(b"PAR1").unwrap();
    drop(file);
    let error = SemanticStorageBindings::binding_has_retained_data(entity, dir.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("metadata exceeds admission limit")
    );
}

#[test]
fn legacy_scanner_resource_ladder_is_independent_of_total_rows() {
    let composition = compiled("1");
    for rows in [1_usize, 8_193] {
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap();
        for _ in 0..rows {
            writer
                .create_node(
                    graphforge_core::uuid::new_v7(),
                    graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(0)).unwrap(),
                )
                .unwrap();
        }
        writer.flush().unwrap();
        let projection =
            SemanticStorageBindings::project_legacy_unambiguous(&composition, dir.path()).unwrap();
        assert_eq!(projection.topology_rows_scanned, rows as u64);
        assert!(projection.max_topology_batch_rows <= 8_192);
    }

    let mut ambiguous = composition;
    ambiguous.modules.extend(compiled("1").modules);
    let error = SemanticStorageBindings::project_legacy_unambiguous(
        &ambiguous,
        tempfile::TempDir::new().unwrap().path(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("GF_SEMANTIC_LEGACY_AMBIGUOUS"));
}

#[test]
fn parquet_footer_limit_fails_before_decoder_allocation() {
    use std::io::Write;

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("topology/nodes.parquet");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = File::create(&path).unwrap();
    file.write_all(b"PAR1").unwrap();
    file.write_all(&(u32::MAX).to_le_bytes()).unwrap();
    file.write_all(b"PAR1").unwrap();
    drop(file);
    let error = admitted_semantic_parquet(&path).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("metadata exceeds admission limit"),
        "{error:?}"
    );
    let composition = compiled("1");
    let error =
        SemanticStorageBindings::project_legacy_unambiguous(&composition, dir.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("metadata exceeds admission limit"),
        "{error:?}"
    );
    let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
    let error = bindings.validate_physical_routes(dir.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("metadata exceeds admission limit"),
        "{error:?}"
    );
    let error = require_atomic_legacy_migration(dir.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("metadata exceeds admission limit"),
        "{error:?}"
    );
}

#[test]
fn semantic_admission_and_decode_share_one_stable_file_handle() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1).unwrap();
    writer
        .create_node(
            graphforge_core::uuid::new_v7(),
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    writer.flush().unwrap();
    let path = crate::catalog::topology_node_files(dir.path())
        .unwrap()
        .remove(0);
    let builder = admitted_semantic_parquet(&path).unwrap();
    let original = path.with_extension("admitted-original");
    std::fs::rename(&path, &original).unwrap();
    std::fs::write(&path, b"replacement is not parquet").unwrap();
    let rows = builder
        .with_batch_size(1)
        .build()
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum::<usize>();
    assert_eq!(rows, 1);
    assert!(admitted_semantic_parquet(&path).is_err());
}
