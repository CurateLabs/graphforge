use crate::GraphForge;
use std::collections::HashMap;

#[test]
fn runtime_guard_blocks_inside_and_outside_an_ambient_runtime() {
    let graph = GraphForge::new(None).unwrap();
    let (_, _, guard) = graph
        .execute_stream_owned("RETURN 1 AS value", &HashMap::new())
        .unwrap();
    assert_eq!(guard.block_on(async { 41 + 1 }), 42);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        assert_eq!(guard.block_on(async { 20 + 22 }), 42);
    });
}

#[test]
fn supplied_identity_must_fit_session_share_without_changing_its_authority() {
    use crate::{
        CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, GraphConstructionBudgets,
        GraphForgeOptions,
    };
    use arrow::array::{ArrayRef, FixedSizeBinaryArray, RecordBatch, StringArray};
    use graphforge_exec::{ExecutionSession, PersistentAdjacencyProvider, SessionResourceConfig};
    use std::sync::Arc;

    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new_with_options(
        Some(root.path().to_str().unwrap()),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                memory_budget_bytes: Some(512 << 20),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let uuid = graphforge_core::uuid::Uuid::from_u128(17);
    let batch = RecordBatch::try_new(
        Arc::clone(&CONSTRUCTION_NODE_SCHEMA),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid.as_bytes().as_slice()].into_iter())
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(StringArray::from(vec!["Entity"])),
        ],
    )
    .unwrap();
    let mut construction = graph
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();
    construction.append_nodes("nodes", &batch).unwrap();
    construction.seal_and_publish().unwrap();

    let resolver = Arc::clone(&graph.ordinal_identities);
    let handle = resolver
        .revalidated_handle()
        .unwrap()
        .expect("real constructed ordinal authority");
    let id = *handle.lock().unwrap().ordinal_ranges()[0].start();
    assert_eq!(
        handle
            .lock()
            .unwrap()
            .lookup_node_uuids(&[id])
            .unwrap()
            .values,
        vec![Some(uuid)]
    );
    let resources = SessionResourceConfig {
        memory_budget_bytes: 16 << 20,
        ..Default::default()
    };
    let workspace = graph.dir();
    let provider = Arc::new(
        PersistentAdjacencyProvider::new(workspace.dir.clone(), graph.ontology_mode)
            .with_memory_budget(resources.memory_budget_bytes)
            .with_inventory(graph.property_inventory_for_session()),
    );
    let attach = |resolver, resources: &SessionResourceConfig| {
        ExecutionSession::new_with_target_provider_resources_and_identity(
            graph
                .open_query_catalog(&graph.runtime_catalog, None)
                .unwrap(),
            graph.ontology.clone(),
            workspace.dir.clone(),
            graph.ontology_mode,
            Arc::clone(&provider),
            Some(resolver),
            resources,
        )
    };
    let error = attach(Arc::clone(&resolver), &resources)
        .err()
        .expect("a supplied 64 MiB identity cache cannot fit a 2 MiB share");
    assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
    assert!(
        error.to_string().contains("supplied ordinal identity"),
        "{error}"
    );
    let retained = resolver.revalidated_handle().unwrap().unwrap();
    assert!(Arc::ptr_eq(&handle, &retained));
    let cached = handle.lock().unwrap().lookup_node_uuids(&[id]).unwrap();
    assert_eq!(cached.values, vec![Some(uuid)]);
    assert_eq!(
        cached.metrics.bytes_read, 0,
        "refusal must not discard or replace the cached block"
    );
    attach(Arc::clone(&resolver), &SessionResourceConfig::default()).unwrap();

    let generation = graph.generation_for_read().unwrap();
    let fitting = crate::workspace_hydration::ordinal_identity_resolver(
        &generation,
        workspace.path(),
        resources.memory_budget_bytes,
    )
    .unwrap();
    attach(fitting, &resources).expect("an exactly fitting identity ceiling is accepted");
}
