use super::super::tests::construction_session_root;
use super::super::tests::{
    edge_property_batch, fixed, node_batch, node_property_batch, node_property_batch_for,
    nonempty_project_generation_two, nonempty_project_with_nodes, open, ordinal_append_session,
};
use super::super::*;
use arrow::array::{Int64Array, StringArray};
use std::sync::Arc;
use tempfile::TempDir;

fn child_parent_property_batch(first: u128, rows: usize) -> RecordBatch {
    let uuids = (first..first + rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let mut fields = CONSTRUCTION_NODE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("score", DataType::Int64, true));
    fields.push(Field::new("nickname", DataType::Utf8, true));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(fixed(&uuids)),
            Arc::new(StringArray::from(vec!["Child"; rows])),
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(StringArray::from(vec!["kid"; rows])),
        ],
    )
    .unwrap()
}

fn colliding_property_batch(uuid: u128, label: &str, value: i64) -> RecordBatch {
    let mut fields = CONSTRUCTION_NODE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("value", DataType::Int64, true));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(fixed(&[uuid.to_be_bytes()])),
            Arc::new(StringArray::from(vec![label])),
            Arc::new(Int64Array::from(vec![value])),
        ],
    )
    .unwrap()
}

fn heterogeneous_property_batch(uuid: u128, property: usize) -> RecordBatch {
    let mut fields = CONSTRUCTION_NODE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new(format!("p{property:03}"), DataType::Int64, true));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(fixed(&[uuid.to_be_bytes()])),
            Arc::new(StringArray::from(vec!["Person"])),
            Arc::new(Int64Array::from(vec![property as i64])),
        ],
    )
    .unwrap()
}

fn semantic_authority(mode: graphforge_core::OntologyMode) -> ConstructionSemanticAuthority {
    let document = graphforge_ontology::OntologyDoc {
        ontology_id: "https://graphforge.dev/ontology/construction-test".into(),
        version: "1.0.0".into(),
        entity_types: vec![
            graphforge_ontology::EntityTypeDef {
                name: "Person".into(),
                r#abstract: false,
                parent: None,
            },
            graphforge_ontology::EntityTypeDef {
                name: "Child".into(),
                r#abstract: false,
                parent: Some("Person".into()),
            },
        ],
        relation_types: vec![graphforge_ontology::RelationTypeDef {
            name: "R".into(),
            src: "Person".into(),
            dst: "Person".into(),
            inverse: None,
            semantic: graphforge_ontology::SemanticFlags::default(),
        }],
        properties: vec![
            graphforge_ontology::PropertyDef {
                owner: "Person".into(),
                name: "score".into(),
                value_type: graphforge_ontology::PropertyValueType::Int64,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
            graphforge_ontology::PropertyDef {
                owner: "Child".into(),
                name: "nickname".into(),
                value_type: graphforge_ontology::PropertyValueType::Utf8,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
            graphforge_ontology::PropertyDef {
                owner: "R".into(),
                name: "weight".into(),
                value_type: graphforge_ontology::PropertyValueType::Int64,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
        ],
        constraints: vec![],
        migrations: vec![],
    };
    let value = serde_json::to_value(&document).unwrap();
    let legacy = crate::WorkspaceOntology {
        contract_version: 1,
        mode: match mode {
            graphforge_core::OntologyMode::Strict => crate::WorkspaceOntologyMode::Strict,
            graphforge_core::OntologyMode::Advisory => crate::WorkspaceOntologyMode::Advisory,
            graphforge_core::OntologyMode::Exploratory => crate::WorkspaceOntologyMode::Advisory,
        },
        source_format: Some(crate::WorkspaceOntologySourceFormat::Json),
        canonical_ontology_sha256: Some(sha256(&serde_json::to_vec(&value).unwrap())),
        canonical_ontology: Some(value),
    };
    let composition = crate::WorkspaceOntologyComposition::virtual_legacy(&legacy)
        .unwrap()
        .unwrap();
    let bindings =
        crate::SemanticStorageBindings::project(&composition.compile().unwrap(), None).unwrap();
    ConstructionSemanticAuthority {
        composition,
        bindings,
    }
}

fn colliding_module_authority() -> ConstructionSemanticAuthority {
    let documents = ["alpha", "beta"].map(|name| graphforge_ontology::OntologyDoc {
        ontology_id: format!("https://graphforge.dev/ontology/{name}"),
        version: "1.0.0".into(),
        entity_types: vec![graphforge_ontology::EntityTypeDef {
            name: "Thing".into(),
            r#abstract: false,
            parent: None,
        }],
        relation_types: vec![],
        properties: vec![graphforge_ontology::PropertyDef {
            owner: "Thing".into(),
            name: "value".into(),
            value_type: graphforge_ontology::PropertyValueType::Int64,
            nullable: true,
            multivalued: false,
            default_json: None,
        }],
        constraints: vec![],
        migrations: vec![],
    });
    let modules = documents
        .into_iter()
        .map(|doc| graphforge_ontology::AuthoredModule {
            id: graphforge_ontology::OntologyModuleId {
                ontology_id: doc.ontology_id.clone(),
                authored_version: doc.version.clone(),
                canonical_digest: graphforge_ontology::module_document_digest(&doc).unwrap(),
            },
            dependencies: vec![],
            doc,
            allow_projected_identity: false,
        })
        .collect::<Vec<_>>();
    let compiled =
        graphforge_ontology::compile_inventory(graphforge_ontology::InventoryCompileRequest {
            modules: &modules,
            bridges: &[],
            activation: &[],
            profile_default: graphforge_ontology::ActivationMode::Strict,
            limits: graphforge_ontology::CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
    let composition = crate::WorkspaceOntologyComposition::from_compiled(&compiled, vec![]);
    let bindings = crate::SemanticStorageBindings::project(&compiled, None).unwrap();
    ConstructionSemanticAuthority {
        composition,
        bindings,
    }
}

#[test]
fn canonical_encoder_outputs_feed_ordinary_readers_index_and_adjacency() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_320);
    let authority = semantic_authority(graphforge_core::OntologyMode::Strict);
    let route = |kind, local: &str| {
        authority
            .bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == kind && binding.symbol.local_id == local)
            .unwrap()
            .route
            .clone()
    };
    let relation_route = route(crate::SemanticRouteKind::Relation, "R");
    let node_property_route = route(crate::SemanticRouteKind::NodeProperty, "Person:score");
    let edge_property_route = route(crate::SemanticRouteKind::EdgeProperty, "R:weight");
    let mut session = GraphConstructionSession::open_with_semantic_authority(
        root.path(),
        operation,
        0,
        authority.clone(),
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(
            ConstructionChunkKind::Node,
            "nodes-a",
            &node_property_batch(1, 2),
        )
        .unwrap();
    session
        .append(
            ConstructionChunkKind::Node,
            "nodes-b",
            &node_property_batch(3, 1),
        )
        .unwrap();
    session
        .append(
            ConstructionChunkKind::Edge,
            "edges",
            &edge_property_batch(100, 2),
        )
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    assert_eq!(shape.ontology_mode, graphforge_core::OntologyMode::Strict);
    let encoding = session.encode_canonical(&shape, 1).unwrap();
    assert_eq!(encoding.evidence.prior_topology_rows_decoded, 0);
    assert_eq!(encoding.evidence.retained_topology_bytes_copied, 0);
    assert_eq!(encoding.evidence.membership_records, 5);
    assert_eq!(encoding.evidence.ordinal_records, 3);
    assert_eq!(encoding.evidence.ordinal_artifact_write_bytes, 120);
    assert_eq!(encoding.evidence.ordinal_artifact_write_operations, 2);
    assert_eq!(encoding.evidence.ordinal_ranges, 1);
    assert_eq!(encoding.evidence.ordinal_work_operations, 3);
    assert_eq!(encoding.evidence.ordinal_peak_buffer_bytes, 3 * 64 * 1024);
    assert_eq!(encoding.evidence.ordinal_publication_write_operations, 3);
    assert!(
        session.evidence().peak_cache_release_window_bytes
            <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
    );
    #[cfg(target_os = "linux")]
    {
        assert!(session.evidence().cache_release_operations > 0);
        assert!(session.evidence().cache_released_bytes > 0);
        assert_eq!(session.evidence().cache_release_unsupported_operations, 0);
    }
    #[cfg(not(target_os = "linux"))]
    assert!(session.evidence().cache_release_unsupported_operations > 0);
    // Three artifact file syncs, one artifact-directory barrier, two
    // barriers for each of receipt/manifest/lock, and four ancestor
    // directory barriers after the complete facet is installed.
    assert_eq!(encoding.evidence.ordinal_fsync_operations, 14);
    let ordinal_publication_bytes = encoding
        .artifacts
        .iter()
        .filter(|artifact| {
            artifact.path.ends_with("ordinal-v4-receipt.json")
                || artifact.path.ends_with("ordinal-v4-manifest.json")
                || artifact.path.ends_with("ordinal-v4.lock")
        })
        .try_fold(0_u64, |bytes, artifact| bytes.checked_add(artifact.bytes))
        .expect("ordinal publication byte sum overflow");
    assert_eq!(
        encoding.evidence.ordinal_publication_write_bytes,
        ordinal_publication_bytes
    );
    assert_eq!(
        encoding.evidence.ordinal_peak_temporary_bytes,
        encoding
            .evidence
            .ordinal_artifact_write_bytes
            .checked_add(ordinal_publication_bytes)
            .expect("ordinal peak byte sum overflow")
    );

    let graph = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoding.root)
        .join("graph");
    let nodes = crate::read_nodes(&graph).unwrap();
    assert_eq!(nodes.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert!(crate::has_runtime_entity_label_encoding_marker(&graph));
    let person_id = authority
        .bindings
        .bindings
        .iter()
        .find(|binding| {
            binding.route_kind == crate::SemanticRouteKind::Entity
                && binding.symbol.local_id == "Person"
        })
        .unwrap()
        .storage_id;
    for batch in &nodes {
        let ids = batch
            .column_by_name("type_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .unwrap();
        assert!(ids.values().iter().all(|id| *id == person_id));
    }
    let (files, _) = crate::capture_graph_files(&graph).unwrap();
    let admitted =
        crate::AuthenticatedPropertyInventory::from_inventory_at_root(&graph, files, None).unwrap();
    let edges = crate::read_edges_from_inventory(
        &admitted,
        &relation_route,
        graphforge_core::OntologyMode::Strict,
    )
    .unwrap();
    assert_eq!(edges.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let node_properties = crate::read_properties(&graph, &node_property_route).unwrap();
    assert_eq!(
        node_properties
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
    let edge_properties = crate::read_edge_properties(&graph, &edge_property_route).unwrap();
    assert_eq!(
        edge_properties
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    let index = crate::UuidMembershipIndex::open(&graph).unwrap();
    assert_eq!(index.count(crate::UuidIndexKind::Node), 3);
    assert_eq!(index.count(crate::UuidIndexKind::Edge), 2);
    let adjacency = crate::adjacency::build_adjacency_index_from_inventory(
        &graph,
        &graph,
        Some(&admitted),
        shape.runtime_catalog_now_micros,
        &crate::adjacency::AdjacencyBuildOptions::default(),
        || Ok(()),
    )
    .unwrap();
    assert!(!adjacency.0.is_empty());

    drop(session);
    let mut resumed_session = GraphConstructionSession::open_with_semantic_authority(
        root.path(),
        operation,
        0,
        authority,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    let resumed = resumed_session
        .prepare_canonical_encoding_with_cancellation(1, || false)
        .unwrap();
    assert!(encoding.invocation.performed);
    assert!(!encoding.invocation.reused);
    let mut encoding = encoding;
    encoding.invocation = Default::default();
    assert_eq!(resumed, encoding);
}

#[test]
fn fresh_v4_authority_transaction_failure_matrix_cleans_and_retries() {
    for (ordinal, point) in [
        "after_artifacts",
        "receipt_install",
        "manifest_install",
        "lock_install",
        "directory_sync",
    ]
    .into_iter()
    .enumerate()
    {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_600 + ordinal as u128);
        let mut session = open(&root, 9_600 + ordinal as u128);
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 3))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        if point == "after_artifacts" {
            crate::uuid_membership::inject_v4_output_cleanup_failure();
        }
        crate::uuid_membership::inject_v4_authority_failure(point);
        let error = session.encode_canonical(&shape, 1).unwrap_err().to_string();
        assert!(
            error.contains(&format!("injected v4 authority failure at {point}")),
            "{error}"
        );
        if point == "after_artifacts" {
            let primary = error.find("injected v4 authority failure").unwrap();
            let cleanup = error
                .find("unpublished artifact cleanup finalization failed")
                .unwrap();
            assert!(primary < cleanup, "{error}");
        }
        assert!(!error.contains(root.path().to_string_lossy().as_ref()));
        let membership = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join("encoded-v1/graph/topology/uuid-membership");
        if membership.exists() {
            let names = std::fs::read_dir(&membership)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            assert!(
                names.iter().all(|name| {
                    !crate::uuid_membership::is_exact_private_v4_name(name)
                        && name != "ordinal-v4-receipt.json"
                        && name != "ordinal-v4-manifest.json"
                        && name != "ordinal-v4.lock"
                }),
                "{point}: {names:?}"
            );
        }

        let encoding = session.encode_canonical(&shape, 1).unwrap();
        let manifest_path = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoding.root)
            .join("graph/topology/uuid-membership/ordinal-v4-manifest.json");
        let manifest_bytes = std::fs::read(&manifest_path).unwrap();
        let manifest: crate::V4OrdinalIdentityManifest =
            serde_json::from_slice(&manifest_bytes).unwrap();
        for artifact in manifest
            .forward_identities
            .iter()
            .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
            .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        {
            assert_eq!(
                sha256(
                    &std::fs::read(manifest_path.parent().unwrap().join(&artifact.name)).unwrap()
                ),
                artifact.sha256
            );
        }
        let reopened = session.encode_canonical(&shape, 1).unwrap();
        assert_eq!(reopened.artifacts, encoding.artifacts);
    }
}

#[test]
fn fresh_construction_publishes_selected_authenticated_v4_exact_lookup() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_321);
    let target = Uuid::from_u128(9_322);
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut session = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 3))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoding = session.encode_canonical(&shape, 1).unwrap();
    session
        .publish_canonical(&encoding, target, Uuid::from_u128(9_323))
        .unwrap();

    let selected = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(selected.generation_uuid(), target);
    let authority = selected
        .authenticated_v4_ordinal_authority()
        .unwrap()
        .expect("fresh construction publishes v4 authority");
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let materialized = TempDir::new().unwrap();
    let graph = materialized.path().join("graph");
    std::fs::create_dir(&graph).unwrap();
    crate::materialize_graph_objects(root.path(), &inventory, &graph).unwrap();
    let mut handle = match authority
        .open(&graph, crate::V4OrdinalIdentityLimits::default())
        .unwrap()
    {
        crate::V4OrdinalIdentityOpen::Ready(handle) => handle,
        crate::V4OrdinalIdentityOpen::RebuildRequired { found_version } => {
            panic!("fresh v4 unexpectedly requires rebuild from {found_version}")
        }
    };
    let lookup = handle.lookup_node_uuids(&[3, 1, 4, 2]).unwrap();
    assert_eq!(
        lookup.values,
        vec![
            Some(Uuid::from_u128(3)),
            Some(Uuid::from_u128(1)),
            None,
            Some(Uuid::from_u128(2)),
        ]
    );
}

#[test]
fn canonical_encoder_merges_bounded_heterogeneous_node_schemas() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_323);
    let authority = semantic_authority(graphforge_core::OntologyMode::Strict);
    let property_route = authority
        .bindings
        .bindings
        .iter()
        .find(|binding| {
            binding.route_kind == crate::SemanticRouteKind::NodeProperty
                && binding.symbol.local_id == "Person:score"
        })
        .unwrap()
        .route
        .clone();
    let mut session = GraphConstructionSession::open_with_semantic_authority(
        root.path(),
        operation,
        0,
        authority,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "plain", &node_batch(1, 1))
        .unwrap();
    session
        .append(
            ConstructionChunkKind::Node,
            "property",
            &node_property_batch(2, 1),
        )
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    assert_eq!(shape.node_rows.len(), 2);
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let graph = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root)
        .join("graph");
    assert_eq!(
        crate::read_nodes(&graph)
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    assert_eq!(
        crate::read_properties(&graph, &property_route)
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert!(encoded.evidence.peak_batch_rows <= 65_536);
    assert!(encoded.evidence.input_read_operations > 0);
    assert!(encoded.evidence.output_write_operations > 0);
    assert_eq!(encoded.evidence.peak_open_input_readers, 1);
}

#[test]
fn canonical_encoder_routes_inherited_property_by_declaring_owner() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_324);
    let authority = semantic_authority(graphforge_core::OntologyMode::Strict);
    let property_route = authority
        .bindings
        .bindings
        .iter()
        .find(|binding| {
            binding.route_kind == crate::SemanticRouteKind::NodeProperty
                && binding.symbol.local_id == "Person:score"
        })
        .unwrap()
        .route
        .clone();
    let mut session = GraphConstructionSession::open_with_semantic_authority(
        root.path(),
        operation,
        0,
        authority,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(
            ConstructionChunkKind::Node,
            "child",
            &node_property_batch_for(1, 2, "Child"),
        )
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let graph = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root)
        .join("graph");
    assert_eq!(
        crate::read_properties(&graph, &property_route)
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
}

#[test]
fn canonical_encoder_splits_child_and_inherited_properties_by_declaring_route() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_326);
    let authority = semantic_authority(graphforge_core::OntologyMode::Strict);
    let binding_route = |kind, local: &str| {
        authority
            .bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == kind && binding.symbol.local_id == local)
            .unwrap()
            .route
            .clone()
    };
    let parent_route = binding_route(crate::SemanticRouteKind::NodeProperty, "Person:score");
    let child_route = binding_route(crate::SemanticRouteKind::NodeProperty, "Child:nickname");
    let concrete_route = binding_route(crate::SemanticRouteKind::Entity, "Child");
    let mut session = GraphConstructionSession::open_with_semantic_authority(
        root.path(),
        operation,
        0,
        authority,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(
            ConstructionChunkKind::Node,
            "child-parent-properties",
            &child_parent_property_batch(1, 2),
        )
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let graph = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root)
        .join("graph");
    for (route, property) in [(parent_route, "score"), (child_route, "nickname")] {
        let batches = crate::read_properties(&graph, &route).unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(
            batches
                .iter()
                .all(|batch| batch.column_by_name(property).is_some())
        );
        assert!(batches.iter().all(|batch| {
            batch.schema().metadata().get("graphforge.entity_type") == Some(&concrete_route)
        }));
    }
}

#[test]
fn canonical_encoder_keeps_same_local_property_names_module_qualified() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_327);
    let authority = colliding_module_authority();
    let mut labels = authority
        .bindings
        .bindings
        .iter()
        .filter(|binding| binding.route_kind == crate::SemanticRouteKind::Entity)
        .map(|binding| binding.symbol.ambiguity_candidate())
        .collect::<Vec<_>>();
    labels.sort();
    let property_routes = authority
        .bindings
        .bindings
        .iter()
        .filter(|binding| binding.route_kind == crate::SemanticRouteKind::NodeProperty)
        .map(|binding| binding.route.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(property_routes.len(), 2);
    let mut session = GraphConstructionSession::open_with_semantic_authority(
        root.path(),
        operation,
        0,
        authority,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    for (index, label) in labels.iter().enumerate() {
        session
            .append(
                ConstructionChunkKind::Node,
                &format!("module-{index}"),
                &colliding_property_batch(index as u128 + 1, label, index as i64),
            )
            .unwrap();
    }
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let graph = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root)
        .join("graph");
    for route in property_routes {
        assert_eq!(
            crate::read_properties(&graph, &route)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
    }
}

#[test]
fn canonical_encoder_keeps_256_heterogeneous_schemas_to_one_live_reader() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_325);
    let budgets = GraphConstructionBudgets {
        max_batch_rows: 1,
        max_run_records: 4,
        ..GraphConstructionBudgets::default()
    };
    let mut session = GraphConstructionSession::open_with_mode(
        root.path(),
        operation,
        0,
        graphforge_core::OntologyMode::Exploratory,
        budgets,
    )
    .unwrap();
    for index in 0..256 {
        session
            .append(
                ConstructionChunkKind::Node,
                &format!("schema-{index:03}"),
                &heterogeneous_property_batch(index as u128 + 1, index),
            )
            .unwrap();
    }
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    assert_eq!(shape.node_rows.len(), 256);
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    assert_eq!(encoded.evidence.peak_open_input_readers, 1);
    assert!(encoded.evidence.peak_batch_rows <= 1);
    let graph = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root)
        .join("graph");
    assert_eq!(
        crate::read_nodes(&graph)
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        256
    );
}

#[test]
fn canonical_encoder_reuse_accounts_only_second_invocation_io() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 9_481);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 4))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();

    let first = session.encode_canonical(&shape, 1).unwrap();
    assert!(first.invocation.performed);
    assert!(!first.invocation.reused);
    assert!(first.invocation.evidence.output_write_bytes > 0);
    assert!(
        first.invocation.evidence.peak_cache_release_window_bytes
            <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
    );
    let cache_after_first = session.evidence().cache_release_operations;
    let writes_after_first = session.evidence().encode_application_write_bytes;

    let second = session.encode_canonical(&shape, 1).unwrap();
    assert!(!second.invocation.performed);
    assert!(second.invocation.reused);
    assert_eq!(second.invocation.evidence.output_write_bytes, 0);
    assert_eq!(second.invocation.evidence.membership_total_write_bytes, 0);
    assert_eq!(
        session.evidence().encode_application_write_bytes,
        writes_after_first
    );
    assert_eq!(
        session
            .evidence()
            .cache_release_operations
            .saturating_sub(cache_after_first),
        // Entry and exit successor authentication each read the same
        // encoded payloads once, in addition to encoder reuse authentication.
        3 * second.invocation.evidence.cache_release_operations
    );
}

#[test]
fn canonical_encoder_cancellation_recovers_and_corruption_fails_closed() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_322);
    let mut session = GraphConstructionSession::open_with_semantic_authority(
        root.path(),
        operation,
        0,
        semantic_authority(graphforge_core::OntologyMode::Strict),
        GraphConstructionBudgets {
            max_batch_rows: 2,
            ..GraphConstructionBudgets::default()
        },
    )
    .unwrap();
    session
        .append(
            ConstructionChunkKind::Node,
            "nodes",
            &node_property_batch(1, 2),
        )
        .unwrap();
    session
        .append(
            ConstructionChunkKind::Node,
            "nodes-2",
            &node_property_batch(3, 1),
        )
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let mut polls = 0;
    let error = session
        .encode_canonical_with_cancellation(&shape, 1, || {
            polls += 1;
            polls == 3
        })
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    let uuid_private = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join("encoded-v1/graph/topology/uuid-membership");
    if uuid_private.exists() {
        assert_eq!(std::fs::read_dir(&uuid_private).unwrap().count(), 0);
    }

    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let operation_root = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root);
    let victim = encoded
        .artifacts
        .iter()
        .find(|artifact| artifact.path.starts_with("topology/nodes/"))
        .unwrap();
    std::fs::write(operation_root.join("graph").join(&victim.path), b"corrupt").unwrap();
    let error = session.encode_canonical(&shape, 1).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("canonical artifact differs from inventory")
    );
}

#[test]
fn canonical_encoder_rejects_same_inode_mutate_restore_during_spool() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_328);
    let mut session = open(&root, 9_328);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let source = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&shape.runtime_catalog);
    let original = std::fs::read(&source).unwrap()[0];
    let mut mutated = false;
    let hook_source = source.clone();
    crate::graph_construction_encoding::set_source_spool_hook(Some(Box::new(move |phase| {
        use std::io::{Seek as _, SeekFrom};
        if phase == "before_read" && !mutated {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&hook_source)
                .unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&[original ^ 0xff]).unwrap();
            mutated = true;
        } else if phase == "after_read" && mutated {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&hook_source)
                .unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&[original]).unwrap();
        }
    })));
    let error = session.encode_canonical(&shape, 1).unwrap_err();
    crate::graph_construction_encoding::set_source_spool_hook(None);
    assert!(
        error
            .to_string()
            .contains("changed during authenticated spooling")
    );
    assert_eq!(std::fs::read(source).unwrap()[0], original);
}

/// The encoder owns its spool, so a shaped source mutated **after** the pass
/// that consumed it cannot change the encoded output.
///
/// This used to fail closed, but only incidentally: `retire_payload` computed a
/// full SHA-256 of every superseded payload immediately before unlinking it,
/// and tripped over the mutation on the way to deleting the file. That pass is
/// removed under #1384. The failure it uniquely caught is "a payload was
/// mutated after it was consumed but before it was deleted", which cannot
/// change any already-produced output and has no user-visible consequence.
///
/// What still fails closed, and is asserted by
/// `canonical_encoder_rejects_same_inode_mutate_restore_during_spool`, is a
/// mutation that reaches the pass that *consumes* the bytes: the spool
/// verifies the source digest over exactly what it read. So encoding
/// succeeding here is itself the proof that the owned spool, not the mutated
/// source, produced the output.
#[test]
fn canonical_encoder_uses_owned_spool_after_source_mutation() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_329);
    let mut session = open(&root, 9_329);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let source = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&shape.runtime_catalog);
    let original = std::fs::read(&source).unwrap()[0];
    let hook_source = source.clone();
    let mut changed = false;
    crate::graph_construction_encoding::set_source_spool_hook(Some(Box::new(move |phase| {
        if phase == "after_read" && !changed {
            use std::io::{Seek as _, SeekFrom};
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&hook_source)
                .unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&[original ^ 0xff]).unwrap();
            changed = true;
        }
    })));
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    crate::graph_construction_encoding::set_source_spool_hook(None);
    assert!(!encoded.invocation.reused);
    // Every encoded artifact still authenticates against the inventory the
    // encoder wrote, and the mutated source was retired rather than consumed.
    let output = session
        .root
        .open_child_directory(OsStr::new("encoded-v1"))
        .unwrap();
    crate::graph_construction_encoding::authenticate_inventory_payloads(
        &output,
        &encoded,
        &mut || false,
    )
    .unwrap();
    assert!(!source.exists());
    let replayed = session.encode_canonical(&shape, 1).unwrap();
    assert!(replayed.invocation.reused);
    assert_eq!(replayed.artifacts, encoded.artifacts);
}

#[test]
fn canonical_encoder_rejects_coherent_inventory_and_artifact_substitution() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_330);
    let mut session = open(&root, 9_330);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let mut encoded = session.encode_canonical(&shape, 1).unwrap();
    let encoded_root = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root);
    let artifact = encoded
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.path == "topology/surrogate_tails.parquet")
        .unwrap();
    let artifact_path = encoded_root.join("graph").join(&artifact.path);
    let mut body = std::fs::read(&artifact_path).unwrap();
    let last = body.len() - 1;
    body[last] ^= 0xff;
    std::fs::write(&artifact_path, &body).unwrap();
    artifact.sha256 = sha256(&body);
    std::fs::write(
        encoded_root.join("inventory.json"),
        serde_json::to_vec(&encoded).unwrap(),
    )
    .unwrap();
    let error = session.encode_canonical(&shape, 1).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("supersession encoding authority changed"),
        "{error}"
    );
}

#[test]
fn construction_checkpoint_rejects_ontology_mode_change_on_resume() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_321);
    drop(
        GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            operation,
            0,
            semantic_authority(graphforge_core::OntologyMode::Advisory),
            GraphConstructionBudgets::default(),
        )
        .unwrap(),
    );
    let error = match GraphConstructionSession::open_with_semantic_authority(
        root.path(),
        operation,
        0,
        semantic_authority(graphforge_core::OntologyMode::Strict),
        GraphConstructionBudgets::default(),
    ) {
        Ok(_) => panic!("ontology mode mismatch was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("resume parameters changed"));
}

#[test]
fn canonical_routing_is_checkpoint_bound_in_all_ontology_modes() {
    for (offset, mode) in [
        (0_u128, graphforge_core::OntologyMode::Exploratory),
        (1, graphforge_core::OntologyMode::Advisory),
        (2, graphforge_core::OntologyMode::Strict),
    ] {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_330 + offset);
        let authority =
            (mode != graphforge_core::OntologyMode::Exploratory).then(|| semantic_authority(mode));
        let mut session = if mode == graphforge_core::OntologyMode::Exploratory {
            GraphConstructionSession::open_with_mode(
                root.path(),
                operation,
                0,
                mode,
                GraphConstructionBudgets::default(),
            )
            .unwrap()
        } else {
            GraphConstructionSession::open_with_semantic_authority(
                root.path(),
                operation,
                0,
                authority.clone().unwrap(),
                GraphConstructionBudgets::default(),
            )
            .unwrap()
        };
        session
            .append(
                ConstructionChunkKind::Node,
                "nodes",
                &node_property_batch(1, 2),
            )
            .unwrap();
        session
            .append(
                ConstructionChunkKind::Edge,
                "edges",
                &edge_property_batch(100, 1),
            )
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.ontology_mode, mode);
        let encoding = session.encode_canonical(&shape, 1).unwrap();
        let graph = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoding.root)
            .join("graph");
        let route = |kind, local: &str, fallback: &str| {
            authority
                .as_ref()
                .and_then(|authority| {
                    authority.bindings.bindings.iter().find(|binding| {
                        binding.route_kind == kind && binding.symbol.local_id == local
                    })
                })
                .map_or_else(|| fallback.to_owned(), |binding| binding.route.clone())
        };
        let relation_route = route(crate::SemanticRouteKind::Relation, "R", "R");
        let (files, _) = crate::capture_graph_files(&graph).unwrap();
        let admitted =
            crate::AuthenticatedPropertyInventory::from_inventory_at_root(&graph, files, None)
                .unwrap();
        assert_eq!(
            crate::read_edges_from_inventory(&admitted, &relation_route, mode)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        let property_stem = if mode == graphforge_core::OntologyMode::Exploratory {
            assert!(
                graph
                    .join("topology/edges")
                    .join(crate::route_component::component("_exploratory"))
                    .is_dir()
            );
            "_untyped".to_owned()
        } else {
            assert!(!graph.join("topology/edges/R").exists());
            route(
                crate::SemanticRouteKind::NodeProperty,
                "Person:score",
                "Person",
            )
        };
        assert_eq!(
            crate::read_properties(&graph, &property_stem)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert_eq!(
            crate::read_edge_properties(
                &graph,
                &route(
                    crate::SemanticRouteKind::EdgeProperty,
                    "R:weight",
                    "_exploratory",
                ),
            )
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
            1
        );
    }
}

#[test]
fn generation_two_parent_index_is_structurally_referenced_without_payload_copy() {
    let project = nonempty_project_generation_two();
    assert!(crate::has_runtime_entity_label_encoding_marker(
        &project.path().join("fixture-graph")
    ));
    let operation = Uuid::from_u128(9_340);
    let mut session = GraphConstructionSession::open(
        project.path(),
        operation,
        2,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "delta", &node_batch(4, 1))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 3).unwrap();
    assert_eq!(encoded.evidence.retained_index_payload_bytes, 0);
    assert_eq!(encoded.evidence.retained_topology_bytes_copied, 0);
    assert_eq!(encoded.evidence.prior_topology_rows_decoded, 0);
    assert_eq!(encoded.evidence.retained_index_runs, 2);
    let index_outputs = encoded
        .artifacts
        .iter()
        .filter(|artifact| {
            artifact.path.contains("uuid-membership")
                && !artifact.path.contains("ordinal-v4")
                && !artifact.path.contains("forward-v4")
                && !artifact.path.contains("tombstones-v4")
        })
        .count();
    // New identity + reverse runs and the new manifest. The retained base
    // and level-one descriptors remain structural references.
    assert_eq!(index_outputs, 3);
    assert!(!encoded.retained_artifacts.is_empty());
    let assembled = project
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root)
        .join("graph");
    for retained in &encoded.retained_artifacts {
        let target = assembled.join(&retained.target_path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::hard_link(
            std::path::Path::new(&retained.source_root).join(&retained.source_path),
            target,
        )
        .unwrap();
    }
    let opened = crate::UuidMembershipIndex::open(&assembled).unwrap();
    assert_eq!(opened.count(crate::UuidIndexKind::Node), 4);
}

#[test]
fn parent_phase_observations_survive_staging_sealed_and_shaped_resumes() {
    let project = nonempty_project_generation_two();
    let operation = Uuid::from_u128(1156);
    let open = || {
        GraphConstructionSession::open(
            project.path(),
            operation,
            2,
            GraphConstructionBudgets::default(),
        )
        .unwrap()
    };
    let mut session = open();
    let initial = session.evidence().clone();
    assert!(initial.authentication_read_bytes > 0);
    assert!(initial.parent_catalog_read_bytes > 0);
    assert_eq!(
        initial.seal_application_read_bytes,
        initial.authentication_read_bytes
    );
    assert_eq!(
        initial.shape_application_read_bytes,
        initial.parent_catalog_read_bytes
    );
    let parent_bytes = initial.authentication_read_bytes + initial.parent_catalog_read_bytes;
    let parent_calls =
        initial.authentication_read_operations + initial.parent_catalog_read_operations;
    session
        .append(ConstructionChunkKind::Node, "delta", &node_batch(4, 1))
        .unwrap();
    for stage in 0..3 {
        if stage == 1 {
            session.seal().unwrap();
        }
        if stage == 2 {
            session.shape_canonical_with_cancellation(|| false).unwrap();
        }
        for _ in 0..2 {
            let before = session.evidence().clone();
            let outputs = if stage == 2 {
                read_completed_shape_outputs(&session.root, &session.checkpoint).unwrap()
            } else {
                Vec::new()
            };
            let successor_bytes: u64 = outputs.iter().map(|receipt| receipt.bytes).sum();
            let successor_calls: u64 = outputs
                .iter()
                .map(|receipt| receipt.bytes.div_ceil(BLOCK_BYTES as u64))
                .sum();
            drop(session);
            session = open();
            let after = session.evidence();
            assert_eq!(
                after.authentication_read_bytes,
                before.authentication_read_bytes
            );
            assert_eq!(
                after.authentication_read_operations,
                before.authentication_read_operations
            );
            assert_eq!(
                after.parent_catalog_read_bytes,
                initial.parent_catalog_read_bytes
            );
            assert_eq!(
                after.parent_catalog_read_operations,
                initial.parent_catalog_read_operations
            );
            assert_eq!(
                after.seal_application_read_bytes,
                before.seal_application_read_bytes
            );
            assert_eq!(
                after.shape_application_read_bytes,
                before.shape_application_read_bytes
            );
            assert_eq!(
                after.recovery_application_read_bytes - before.recovery_application_read_bytes,
                parent_bytes + successor_bytes
            );
            assert_eq!(
                after.recovery_application_read_operations
                    - before.recovery_application_read_operations,
                parent_calls + successor_calls
            );
            assert_eq!(
                after.recovery_checkpoint_fsync_operations
                    - before.recovery_checkpoint_fsync_operations,
                if stage == 2 { 9 } else { 3 }
            );
            assert_eq!(after.fsync_operations, before.fsync_operations);
            crate::ConstructionPhaseAttribution::from_construction(after)
                .unwrap()
                .validate_for_qualification()
                .unwrap();
        }
    }
}

#[test]
fn completed_encoding_replay_reauthenticates_retained_parent_payload() {
    let project = nonempty_project_generation_two();
    let operation = Uuid::from_u128(9_343);
    let mut session = GraphConstructionSession::open(
        project.path(),
        operation,
        2,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "delta", &node_batch(4, 1))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 3).unwrap();
    let retained = encoded
        .retained_artifacts
        .iter()
        .find(|artifact| artifact.bytes > 0)
        .unwrap();
    let path = std::path::Path::new(&retained.source_root).join(&retained.source_path);
    let original_permissions = std::fs::metadata(&path).unwrap().permissions();
    let mut permissions = original_permissions.clone();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o600);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(false);
    std::fs::set_permissions(&path, permissions).unwrap();
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    use std::io::{Seek as _, SeekFrom};
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    std::fs::set_permissions(&path, original_permissions).unwrap();
    let error = session.encode_canonical(&shape, 3).unwrap_err();
    assert!(error.to_string().contains("digest"), "{error}");
}

#[test]
fn generation_one_parent_uses_streamed_binary_carry_and_authenticates_result() {
    let project = nonempty_project_with_nodes(2);
    let operation = Uuid::from_u128(9_341);
    let mut session = GraphConstructionSession::open(
        project.path(),
        operation,
        1,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "delta", &node_batch(3, 1))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 2).unwrap();
    assert!(encoded.evidence.retained_index_payload_bytes > 0);
    assert!(encoded.evidence.membership_read_bytes > 0);
    assert!(
        encoded.evidence.membership_total_write_bytes > encoded.evidence.membership_write_bytes
    );

    let graph = project
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root)
        .join("graph");
    let parent_index = project
        .path()
        .join("fixture-graph/topology/uuid-membership");
    let encoded_index = graph.join("topology/uuid-membership");
    let parent_manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(parent_index.join("manifest.json")).unwrap())
            .unwrap();
    for run in parent_manifest["runs"].as_array().unwrap() {
        if !run["base"].as_bool().unwrap() {
            continue;
        }
        for field in ["identities", "node_surrogates"] {
            let name = run[field]["name"].as_str().unwrap();
            assert_eq!(std::fs::metadata(parent_index.join(name)).unwrap().len(), 0);
            std::fs::copy(parent_index.join(name), encoded_index.join(name)).unwrap();
        }
    }
    let index = crate::UuidMembershipIndex::open(&graph).unwrap();
    assert_eq!(index.count(crate::UuidIndexKind::Node), 3);
    assert_eq!(index.count(crate::UuidIndexKind::Edge), 1);
}

#[test]
fn parent_uuid_path_substitution_is_rejected_before_encoding() {
    let project = nonempty_project_with_nodes(2);
    let operation = Uuid::from_u128(9_342);
    let mut session = GraphConstructionSession::open(
        project.path(),
        operation,
        1,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "delta", &node_batch(3, 1))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let inventory = crate::resolve_project_generation(project.path())
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap();
    let victim = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path.contains("/identities-") && entry.byte_length != 0)
        .unwrap();
    let victim = crate::graph_object_path(project.path(), &victim.content_sha256).unwrap();
    let saved = victim.with_extension("uuidx.saved");
    std::fs::rename(&victim, &saved).unwrap();
    std::fs::copy(&saved, &victim).unwrap();
    let error = session.encode_canonical(&shape, 2).unwrap_err();
    assert!(error.to_string().contains("identity changed"));
}

fn encoded_publication_session(root: &TempDir, operation: Uuid) -> GraphConstructionSession {
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut session = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    session.encode_canonical(&shape, 1).unwrap();
    session
}

fn publish_empty_generation(
    root: &TempDir,
    target: Uuid,
    transaction: Uuid,
) -> crate::ProjectPublicationReceipt {
    let request = crate::ProjectGenerationRequest {
        transaction_uuid: transaction,
        generation_uuid: target,
        capabilities: vec![crate::ProjectCapability {
            capability_id: "graph".into(),
            capability_version: 1,
        }],
        participants: vec![],
    };
    let crate::ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new publication unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap()
}

fn publish_session(
    session: &mut GraphConstructionSession,
    target: Uuid,
    transaction: Uuid,
) -> crate::ProjectPublicationReceipt {
    let output = session
        .root
        .open_child_directory(OsStr::new("encoded-v1"))
        .unwrap();
    let encoded = crate::graph_construction_encoding::read_inventory(&output)
        .unwrap()
        .unwrap();
    session
        .publish_canonical(&encoded, target, transaction)
        .unwrap()
}

#[test]
fn publication_state_is_idempotent_and_rejects_changed_target() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_400);
    let target = Uuid::from_u128(9_401);
    let transaction = Uuid::from_u128(9_402);
    let mut session = encoded_publication_session(&root, operation);
    let first = session.begin_publication(target, transaction).unwrap();
    assert_eq!(
        session.checkpoint.publication_state,
        Some(ConstructionPublicationState::Publishing)
    );
    assert_eq!(
        session.begin_publication(target, transaction).unwrap(),
        first
    );
    assert!(
        session
            .begin_publication(Uuid::from_u128(9_403), transaction)
            .unwrap_err()
            .to_string()
            .contains("target changed")
    );
    let published = publish_session(&mut session, target, transaction);
    let digest = hex(&published.generation_manifest_sha256);
    let receipt = session.finish_publication(target, &digest).unwrap();
    assert_eq!(
        session.checkpoint.publication_state,
        Some(ConstructionPublicationState::Published)
    );
    assert_eq!(
        session.finish_publication(target, &digest).unwrap(),
        receipt
    );
    assert!(
        session
            .finish_publication(target, &"cd".repeat(32))
            .unwrap_err()
            .to_string()
            .contains("result changed")
    );
    let private_root = construction_session_root(&root, operation);
    let error = session.discard().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("published construction belongs to generation recovery")
    );
    assert!(private_root.exists());
}

#[test]
fn construction_append_publishes_current_complete_ordinal_authority() {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut retained = Vec::new();
    for generation in 1..=8_u64 {
        let total = 255 + generation;
        let (_source, mut session, shape) = ordinal_append_session(
            &root,
            generation,
            if generation == 1 {
                1
            } else {
                u128::from(total)
            },
            if generation == 1 { 256 } else { 1 },
        );
        let pins = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let recorded = pins.clone();
        crate::uuid_membership::set_construction_ordinal_hook(Some(Box::new(
            move |phase, generation| {
                if phase == "pin" {
                    recorded.borrow_mut().push(generation);
                }
            },
        )));
        let encoding = session.encode_canonical(&shape, generation);
        crate::uuid_membership::set_construction_ordinal_hook(None);
        let encoding = encoding.unwrap();
        assert!(
            pins.borrow().iter().all(|generation| *generation > 1),
            "the base must never be reread by ordinal compaction"
        );
        if generation <= 2 {
            assert!(pins.borrow().is_empty());
        }
        if generation == 3 {
            assert!(
                !pins.borrow().is_empty(),
                "the fixture must cross an actual carry"
            );
        }
        assert_eq!(encoding.evidence.prior_topology_rows_decoded, 0);
        assert_eq!(encoding.evidence.retained_topology_bytes_copied, 0);
        session
            .publish_canonical(
                &encoding,
                Uuid::from_u128(98_100 + u128::from(generation)),
                Uuid::from_u128(98_200 + u128::from(generation)),
            )
            .unwrap();
        drop(session);
        let selected = crate::resolve_project_generation(root.path()).unwrap();
        let authority = selected
            .authenticated_v4_ordinal_authority()
            .unwrap()
            .unwrap();
        let inventory = selected.graph_files_inventory().unwrap().unwrap();
        let materialized = TempDir::new().unwrap();
        let graph = materialized.path().join("graph");
        std::fs::create_dir(&graph).unwrap();
        crate::materialize_graph_objects(root.path(), &inventory, &graph).unwrap();
        let crate::V4OrdinalIdentityOpen::Ready(mut handle) = authority
            .open(&graph, crate::V4OrdinalIdentityLimits::default())
            .unwrap()
        else {
            panic!("published construction must retain complete v4 authority");
        };
        let (_, manifest) = selected
            .authenticated_v4_ordinal_manifest(&mut crate::GraphObjectIoTotals::default())
            .unwrap()
            .unwrap();
        assert!(manifest.forward_identities.len() <= 1 + generation.ilog2() as usize);
        let declared = manifest
            .forward_identities
            .iter()
            .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
            .chain(manifest.tombstones.iter().map(|run| &run.artifact))
            .map(|artifact| artifact.name.clone())
            .collect::<BTreeSet<_>>();
        let installed = inventory
            .files
            .iter()
            .filter_map(|entry| {
                let name = entry.relative_path.rsplit('/').next().unwrap();
                (name.contains("-v4-") && name.ends_with(".uuidx")).then(|| name.to_owned())
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            installed, declared,
            "superseded ordinal paths must leave the selected inventory"
        );
        assert_eq!(handle.topology_generation(), generation);
        let ids = (1..=total).collect::<Vec<_>>();
        assert_eq!(
            handle.lookup_node_uuids(&ids).unwrap().values,
            ids.iter()
                .map(|id| Some(Uuid::from_u128(u128::from(*id))))
                .collect::<Vec<_>>()
        );
        let mut membership = crate::UuidMembershipIndex::open(&graph).unwrap();
        let uuids = ids
            .iter()
            .map(|id| Uuid::from_u128(u128::from(*id)))
            .collect::<Vec<_>>();
        assert_eq!(membership.count(crate::UuidIndexKind::Node), total);
        assert_eq!(
            membership.lookup_node_surrogates(&uuids).unwrap().0,
            ids.iter().copied().map(Some).collect::<Vec<_>>()
        );
        retained.push((selected, materialized, handle, total));
    }
    for (selected, _materialized, mut handle, total) in retained {
        selected
            .authenticated_v4_ordinal_authority()
            .unwrap()
            .unwrap();
        assert_eq!(
            handle.lookup_node_uuids(&[1, total]).unwrap().values,
            [
                Some(Uuid::from_u128(1)),
                Some(Uuid::from_u128(u128::from(total)))
            ]
        );
    }
}

fn publish_ordinal_fixture(root: &TempDir, generation: u64) {
    let (_source, mut session, shape) =
        ordinal_append_session(root, generation, u128::from(generation), 1);
    let encoding = session.encode_canonical(&shape, generation).unwrap();
    session
        .publish_canonical(&encoding, Uuid::new_v4(), Uuid::new_v4())
        .unwrap();
}

#[test]
fn construction_ordinal_cancellation_cleans_owned_outputs_and_retries() {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    publish_ordinal_fixture(&root, 1);
    publish_ordinal_fixture(&root, 2);
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    let (_source, mut session, shape) = ordinal_append_session(&root, 3, 3, 4096);
    let cancelled = std::rc::Rc::new(std::cell::Cell::new(false));
    let signal = cancelled.clone();
    crate::uuid_membership::set_construction_ordinal_hook(Some(Box::new(move |phase, _| {
        if phase == "forward" {
            signal.set(true);
        }
    })));
    let result = session.encode_canonical_with_cancellation(&shape, 3, || cancelled.get());
    crate::uuid_membership::set_construction_ordinal_hook(None);
    assert!(
        cancelled.get(),
        "cancel only after actual compaction output"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("ordinal compaction cancelled")
    );
    assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
    let index = session
        .root
        .path()
        .join("encoded-v1/graph/topology/uuid-membership");
    let residual = std::fs::read_dir(&index)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("v4") || name.contains("compact"))
        .collect::<Vec<_>>();
    assert!(
        residual.is_empty(),
        "owned ordinal outputs left after cancellation: {residual:?}"
    );
    let encoding = session.encode_canonical(&shape, 3).unwrap();
    session
        .publish_canonical(&encoding, Uuid::new_v4(), Uuid::new_v4())
        .unwrap();
    let selected = crate::resolve_project_generation(root.path()).unwrap();
    let authority = selected
        .authenticated_v4_ordinal_authority()
        .unwrap()
        .unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let materialized = TempDir::new().unwrap();
    let graph = materialized.path().join("graph");
    std::fs::create_dir(&graph).unwrap();
    crate::materialize_graph_objects(root.path(), &inventory, &graph).unwrap();
    let crate::V4OrdinalIdentityOpen::Ready(mut handle) = authority
        .open(&graph, crate::V4OrdinalIdentityLimits::default())
        .unwrap()
    else {
        panic!("v4 required")
    };
    assert_eq!(
        handle.lookup_node_uuids(&[1, 2, 3, 4098]).unwrap().values,
        [1_u128, 2, 3, 4098].map(|id| Some(Uuid::from_u128(id)))
    );
}

#[test]
fn construction_ordinal_parent_corruption_fails_before_publication() {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    publish_ordinal_fixture(&root, 1);
    let (_source, mut session, shape) = ordinal_append_session(&root, 2, 2, 1);
    let selected = crate::resolve_project_generation(root.path()).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let manifest = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path.ends_with("ordinal-v4-manifest.json"))
        .unwrap();
    let path = crate::graph_object_path(root.path(), &manifest.content_sha256).unwrap();
    let original = std::fs::read(&path).unwrap();
    let permissions = std::fs::metadata(&path).unwrap().permissions();
    let mut writable = permissions.clone();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        writable.set_mode(0o600);
    }
    #[cfg(not(unix))]
    writable.set_readonly(false);
    std::fs::set_permissions(&path, writable).unwrap();
    let mut damaged = original.clone();
    damaged[0] ^= 1;
    std::fs::write(&path, damaged).unwrap();
    std::fs::set_permissions(&path, permissions.clone()).unwrap();
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    let result = session.encode_canonical(&shape, 2);
    assert!(result.unwrap_err().to_string().contains("digest"));
    assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
    // Restore fixture authority so its cleanup never leaves damaged CAS objects.
    let mut writable = permissions.clone();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        writable.set_mode(0o600);
    }
    #[cfg(not(unix))]
    writable.set_readonly(false);
    std::fs::set_permissions(&path, writable).unwrap();
    std::fs::write(&path, original).unwrap();
    std::fs::set_permissions(&path, permissions).unwrap();
    session.encode_canonical(&shape, 2).unwrap();
}

#[test]
fn canonical_publication_installs_compact_graph_and_advances_current_once() {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let operation = Uuid::from_u128(9_450);
    let target = Uuid::from_u128(9_451);
    let transaction = Uuid::from_u128(9_452);
    let mut session = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoding = session.encode_canonical(&shape, 1).unwrap();

    let receipt = session
        .publish_canonical(&encoding, target, transaction)
        .unwrap();
    assert_eq!(receipt.generation_uuid, target);
    assert!(!receipt.idempotent_replay);
    let current = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(current.generation_uuid(), target);
    let inventory = current.graph_files_inventory().unwrap().unwrap();
    assert!(
        inventory
            .files
            .iter()
            .any(|entry| entry.relative_path == "topology/generation.json")
    );
    assert!(
        inventory
            .files
            .iter()
            .any(|entry| entry.relative_path.starts_with("topology/nodes/"))
    );
    assert!(
        inventory
            .files
            .iter()
            .any(|entry| { entry.relative_path == "topology/uuid-membership/manifest.json" })
    );
    let current_path = root.path().join("CURRENT");
    let current_bytes = std::fs::read(&current_path).unwrap();
    std::fs::write(&current_path, b"concurrently-advanced-current\n").unwrap();
    assert_eq!(
        compact_parent_surrogate_tails(root.path(), &inventory).unwrap(),
        Some((2, 0)),
        "pinned compact-parent tails must not consult mutable CURRENT"
    );
    std::fs::write(&current_path, current_bytes).unwrap();
    let materialized = TempDir::new().unwrap();
    let materialized_graph = materialized.path().join("graph");
    std::fs::create_dir(&materialized_graph).unwrap();
    crate::materialize_graph_objects(root.path(), &inventory, &materialized_graph).unwrap();
    let uuid_index = crate::UuidMembershipIndex::open(&materialized_graph).unwrap();
    assert_eq!(uuid_index.count(crate::UuidIndexKind::Node), 2);
    assert_eq!(uuid_index.count(crate::UuidIndexKind::Edge), 0);
    drop(current);
    let receipt_path = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(PUBLICATION_RECEIPT);
    std::fs::remove_file(receipt_path).unwrap();
    session.checkpoint.publication_state = Some(ConstructionPublicationState::Publishing);
    replace_control(&session.root, CHECKPOINT, &session.checkpoint).unwrap();
    drop(session);

    let mut resumed = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    let replay = resumed
        .publish_canonical(&encoding, target, transaction)
        .unwrap();
    assert_eq!(replay.generation_uuid, target);
    assert_eq!(
        crate::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        target
    );
}

#[test]
fn canonical_publication_rejects_tampered_artifact_before_current() {
    let root = TempDir::new().unwrap();
    let parent = crate::open_or_initialize_project(root.path()).unwrap();
    let prior = parent.generation_uuid();
    drop(parent);
    let operation = Uuid::from_u128(9_460);
    let mut session = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 1))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoding = session.encode_canonical(&shape, 1).unwrap();
    let victim = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoding.root)
        .join("graph")
        .join(
            &encoding
                .artifacts
                .iter()
                .find(|artifact| artifact.path.ends_with(".parquet"))
                .unwrap()
                .path,
        );
    std::fs::write(victim, b"tampered").unwrap();

    let error = session
        .publish_canonical(&encoding, Uuid::from_u128(9_461), Uuid::from_u128(9_462))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("authenticated graph file metadata changed")
            || error.contains("digest or length changed")
            || error.contains("graph object source is not the declared regular file"),
        "unexpected corruption error: {error}"
    );
    assert_eq!(
        crate::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        prior
    );
    assert_eq!(
        session.checkpoint.publication_state,
        Some(ConstructionPublicationState::Sealed)
    );
}

#[test]
fn canonical_publication_rejects_replaced_durable_inventory_without_payload_reads() {
    let root = TempDir::new().unwrap();
    let parent = crate::open_or_initialize_project(root.path()).unwrap();
    let prior = parent.generation_uuid();
    drop(parent);
    let operation = Uuid::from_u128(9_468);
    let mut session = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 1))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoding = session.encode_canonical(&shape, 1).unwrap();
    let inventory_path = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join("encoded-v1/inventory.json");
    let mut replaced: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&inventory_path).unwrap()).unwrap();
    replaced["generation"] = serde_json::json!(2);
    std::fs::write(&inventory_path, serde_json::to_vec(&replaced).unwrap()).unwrap();

    let error = session
        .publish_canonical(&encoding, Uuid::from_u128(9_469), Uuid::from_u128(9_470))
        .unwrap_err();
    assert!(error.to_string().contains("durable encoding"));
    assert_eq!(
        crate::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        prior
    );
    assert_eq!(
        session.checkpoint.publication_state,
        Some(ConstructionPublicationState::Sealed)
    );
}

#[test]
fn canonical_publication_cancels_at_named_immediate_pre_current_boundary() {
    let root = TempDir::new().unwrap();
    let parent = crate::open_or_initialize_project(root.path()).unwrap();
    let prior = parent.generation_uuid();
    drop(parent);
    let prior_current = std::fs::read(root.path().join("CURRENT")).unwrap();
    let mut session = GraphConstructionSession::open(
        root.path(),
        Uuid::from_u128(9_465),
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoding = session.encode_canonical(&shape, 1).unwrap();
    let target = Uuid::from_u128(9_466);
    let transaction = Uuid::from_u128(9_467);
    let mut checkpoints = 0_u8;
    let error = session
        .publish_canonical_with_cancellation(&encoding, target, transaction, || {
            checkpoints += 1;
            checkpoints == 2
        })
        .unwrap_err();
    assert_eq!(error.code(), "GF_CANCELLED");
    assert!(error.to_string().contains("before_current_replace"));
    assert_eq!(
        checkpoints, 2,
        "entry and immediate pre-CURRENT checkpoints"
    );
    assert_eq!(
        std::fs::read(root.path().join("CURRENT")).unwrap(),
        prior_current
    );
    assert_ne!(target, prior);
    assert_eq!(
        session.checkpoint.publication_state,
        Some(ConstructionPublicationState::Publishing),
        "the durable intent remains recoverable without claiming commit"
    );
}

#[test]
fn project_container_parent_binding_uses_exact_uuid_and_manifest() {
    let root = TempDir::new().unwrap();
    let parent = crate::open_or_initialize_project(root.path()).unwrap();
    let expected = (parent.generation_uuid(), hex(&parent.manifest_sha256()));
    drop(parent);
    assert_eq!(
        current_parent_generation_authority(root.path()).unwrap(),
        expected
    );
}

#[test]
fn publication_reopen_recovers_each_durable_crash_boundary() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_410);
    let target = Uuid::from_u128(9_411);
    let transaction = Uuid::from_u128(9_412);
    let mut session = encoded_publication_session(&root, operation);
    session.begin_publication(target, transaction).unwrap();
    session.checkpoint.publication_state = Some(ConstructionPublicationState::Sealed);
    let interrupted = session.checkpoint.clone();
    session.checkpoint.publication_state = Some(ConstructionPublicationState::Publishing);
    let published = publish_session(&mut session, target, transaction);
    replace_control(&session.root, CHECKPOINT, &interrupted).unwrap();
    unlink_named(&session.root, PUBLICATION_RECEIPT).unwrap();
    drop(session);
    publish_empty_generation(&root, Uuid::from_u128(9_413), Uuid::from_u128(9_414));
    let mut reopened = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    assert_eq!(
        reopened.checkpoint.publication_state,
        Some(ConstructionPublicationState::Publishing)
    );
    reopened
        .finish_publication(target, &hex(&published.generation_manifest_sha256))
        .unwrap();
    reopened.checkpoint.publication_state = Some(ConstructionPublicationState::Publishing);
    replace_control(&reopened.root, CHECKPOINT, &reopened.checkpoint).unwrap();
    drop(reopened);
    let reopened = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    assert_eq!(
        reopened.checkpoint.publication_state,
        Some(ConstructionPublicationState::Published)
    );
}

#[test]
fn publication_reopen_rejects_corrupt_or_mismatched_authority() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_420);
    let mut session = encoded_publication_session(&root, operation);
    session
        .begin_publication(Uuid::from_u128(9_421), Uuid::from_u128(9_422))
        .unwrap();
    let intent_path = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(PUBLICATION_INTENT);
    let mut intent: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&intent_path).unwrap()).unwrap();
    intent["parent_generation_manifest_sha256"] = serde_json::Value::String("00".repeat(32));
    std::fs::write(&intent_path, serde_json::to_vec(&intent).unwrap()).unwrap();
    drop(session);
    let error = match GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    ) {
        Ok(_) => panic!("corrupt publication intent was accepted"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("publication intent authority changed")
    );
}

#[test]
fn publication_finish_rejects_wrong_target_and_parent() {
    let wrong_target_root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_430);
    let intended = Uuid::from_u128(9_431);
    let transaction = Uuid::from_u128(9_432);
    let mut session = encoded_publication_session(&wrong_target_root, operation);
    session.begin_publication(intended, transaction).unwrap();
    let other = publish_empty_generation(
        &wrong_target_root,
        Uuid::from_u128(9_433),
        Uuid::from_u128(9_434),
    );
    assert!(
        session
            .finish_publication(intended, &hex(&other.generation_manifest_sha256))
            .unwrap_err()
            .to_string()
            .contains("target cannot be authenticated")
    );

    let wrong_parent_root = TempDir::new().unwrap();
    let mut session = encoded_publication_session(&wrong_parent_root, Uuid::from_u128(9_440));
    let first = publish_empty_generation(
        &wrong_parent_root,
        Uuid::from_u128(9_441),
        Uuid::from_u128(9_442),
    );
    let target = Uuid::from_u128(9_443);
    let transaction = Uuid::from_u128(9_444);
    session.begin_publication(target, transaction).unwrap();
    let second = publish_empty_generation(&wrong_parent_root, target, transaction);
    assert_ne!(first.generation_uuid, second.generation_uuid);
    assert!(
        session
            .finish_publication(target, &hex(&second.generation_manifest_sha256))
            .unwrap_err()
            .to_string()
            .contains("not a child of the pinned parent")
    );
}

#[test]
fn published_receipt_reopens_after_later_current_advances() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_450);
    let mut session = encoded_publication_session(&root, operation);
    let target = Uuid::from_u128(9_451);
    let transaction = Uuid::from_u128(9_452);
    session.begin_publication(target, transaction).unwrap();
    let target_receipt = publish_session(&mut session, target, transaction);
    let construction_receipt = session
        .finish_publication(target, &hex(&target_receipt.generation_manifest_sha256))
        .unwrap();
    drop(session);
    publish_empty_generation(&root, Uuid::from_u128(9_453), Uuid::from_u128(9_454));
    let mut reopened = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    assert_eq!(
        reopened
            .finish_publication(target, &hex(&target_receipt.generation_manifest_sha256))
            .unwrap(),
        construction_receipt
    );
}

#[test]
fn persisted_authority_digests_reject_uppercase_hex() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_460);
    let target = Uuid::from_u128(9_461);
    let transaction = Uuid::from_u128(9_462);
    let mut session = encoded_publication_session(&root, operation);
    session.begin_publication(target, transaction).unwrap();
    let published = publish_empty_generation(&root, target, transaction);
    assert!(
        session
            .finish_publication(
                target,
                &hex(&published.generation_manifest_sha256).to_ascii_uppercase(),
            )
            .unwrap_err()
            .to_string()
            .contains("digest is invalid")
    );

    let intent_path = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(PUBLICATION_INTENT);
    let mut intent: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&intent_path).unwrap()).unwrap();
    intent["shape_authority_sha256"] = serde_json::Value::String(
        intent["shape_authority_sha256"]
            .as_str()
            .unwrap()
            .to_ascii_uppercase(),
    );
    std::fs::write(&intent_path, serde_json::to_vec(&intent).unwrap()).unwrap();
    drop(session);
    let error = match GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    ) {
        Ok(_) => panic!("uppercase persisted authority was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("digest is invalid"));
}

/// The encoder no longer re-reads the artifacts it just wrote after installing
/// the inventory (#1384). The boundary that consumes those bytes is the CAS
/// install at publication, which copies and hashes every artifact against the
/// inventory. This asserts the refusal *there*, by its own message, for a
/// same-inode, same-length payload mutation — not on the end-to-end result,
/// and not through the reclaim sweep, which `publish_canonical` never runs.
#[test]
fn publication_refuses_same_inode_encoded_payload_corruption_at_cas_install() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_440);
    let mut session = encoded_publication_session(&root, operation);
    let output = session
        .root
        .open_child_directory(OsStr::new("encoded-v1"))
        .unwrap();
    let encoded = crate::graph_construction_encoding::read_inventory(&output)
        .unwrap()
        .unwrap();
    let artifact = encoded
        .artifacts
        .iter()
        .find(|artifact| artifact.path == "topology/surrogate_tails.parquet")
        .unwrap();
    let artifact_path = root
        .path()
        .join(PRIVATE_ROOT)
        .join(operation.simple().to_string())
        .join(&encoded.root)
        .join("graph")
        .join(&artifact.path);
    let length_before = std::fs::metadata(&artifact_path).unwrap().len();
    let identity_before = graphforge_filesystem::path_identity(&artifact_path).unwrap();
    {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&artifact_path)
            .unwrap();
        let mut first = [0_u8; 1];
        file.read_exact(&mut first).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[first[0] ^ 0xff]).unwrap();
        file.sync_all().unwrap();
    }
    assert_eq!(
        std::fs::metadata(&artifact_path).unwrap().len(),
        length_before
    );
    assert_eq!(
        graphforge_filesystem::path_identity(&artifact_path).unwrap(),
        identity_before
    );

    let error = session
        .publish_canonical(&encoded, Uuid::from_u128(9_441), Uuid::from_u128(9_442))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("graph object source digest or length changed during install"),
        "expected the CAS install boundary to refuse the mutation, got: {error}"
    );
    assert_ne!(
        session.checkpoint.publication_state,
        Some(ConstructionPublicationState::Published)
    );

    // The reclaim sweep, when it runs, refuses the same bytes with its own
    // message. It is not what the publication boundary depends on.
    let sweep = crate::graph_construction_encoding::authenticate_inventory_payloads(
        &output,
        &encoded,
        &mut || false,
    )
    .unwrap_err();
    assert!(
        sweep
            .to_string()
            .contains("canonical artifact differs from inventory"),
        "{sweep}"
    );
}
