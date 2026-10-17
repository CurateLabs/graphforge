//! Regressions for reads that take expansion fast paths or fail during setup.
use super::*;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::logical_plan::LogicalTableSource;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use graphforge_core::uuid::Uuid;

fn binary(values: &[u128]) -> ArrayRef {
    Arc::new(
        arrow::array::FixedSizeBinaryArray::try_from_iter(
            values.iter().map(|value| value.to_be_bytes()),
        )
        .unwrap(),
    )
}

/// Publish and reopen a real construction generation so the identity-only hop
/// uses an authenticated ordinal authority, rather than a mocked identity map.
fn identity_graph() -> (
    tempfile::TempDir,
    read_resource::GraphReadContext,
    Arc<V4OrdinalIdentitySession>,
) {
    use graphforge_storage::{
        CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ConstructionChunkKind,
        GraphConstructionBudgets, GraphConstructionSession,
    };
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("project");
    graphforge_storage::open_or_initialize_project(&project).unwrap();
    let mut construction = GraphConstructionSession::open_with_mode(
        &project,
        Uuid::from_u128(100),
        0,
        OntologyMode::Exploratory,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    let nodes = RecordBatch::try_new(
        Arc::clone(&CONSTRUCTION_NODE_SCHEMA),
        vec![
            binary(&[1, 2, 3, 4]),
            Arc::new(arrow::array::StringArray::from(vec!["Person"; 4])),
        ],
    )
    .unwrap();
    construction
        .append(ConstructionChunkKind::Node, "nodes", &nodes)
        .unwrap();
    let edges = RecordBatch::try_new(
        Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
        vec![
            binary(&[10, 11, 12, 13]),
            Arc::new(arrow::array::StringArray::from(vec!["KNOWS"; 4])),
            binary(&[1, 1, 2, 3]),
            binary(&[2, 3, 3, 4]),
        ],
    )
    .unwrap();
    construction
        .append(ConstructionChunkKind::Edge, "edges", &edges)
        .unwrap();
    construction.seal().unwrap();
    let shape = construction
        .shape_canonical_with_cancellation(|| false)
        .unwrap();
    let encoding = construction.encode_canonical(&shape, 1).unwrap();
    construction
        .publish_canonical(&encoding, Uuid::from_u128(101), Uuid::from_u128(102))
        .unwrap();
    let selected = graphforge_storage::resolve_project_generation(&project).unwrap();
    let inventory = selected.graph_files_inventory().unwrap().unwrap();
    let graph = dir.path().join("graph");
    std::fs::create_dir(&graph).unwrap();
    graphforge_storage::materialize_graph_objects(&project, &inventory, &graph).unwrap();
    let authority = selected
        .authenticated_v4_ordinal_authority()
        .unwrap()
        .unwrap();
    let graphforge_storage::V4OrdinalIdentityOpen::Ready(handle) = authority
        .open(
            &graph,
            graphforge_storage::V4OrdinalIdentityLimits::default(),
        )
        .unwrap()
    else {
        panic!("published ordinal authority must open")
    };
    let resolver = V4OrdinalIdentityResolver::new(Some(*handle));
    let identities = resolver
        .pin(graphforge_storage::read_cache_budget::DEFAULT_QUERY_MEMORY_BUDGET_BYTES)
        .unwrap()
        .session
        .unwrap();
    let catalog =
        graphforge_storage::GraphCatalog::open(&graph, None, &graphforge_ir::RuntimeCatalog::new())
            .unwrap();
    let resource = read_resource::GraphReadContext {
        health: crate::mutation::MutationHealth::default(),
        dir: graph,
        mode: OntologyMode::Exploratory,
        catalog: Arc::new(catalog),
        ontology: None,
    };
    (dir, resource, identities)
}

#[tokio::test]
async fn identity_only_hops_count_resolved_destinations_and_no_synthetic_ids() {
    let (_dir, resource, identities) = identity_graph();
    let provider: Arc<dyn AdjacencyProvider> = Arc::new(
        adjacency::PersistentAdjacencyProvider::new(resource.dir.clone(), resource.mode)
            .with_inventory(resource.catalog.admitted_inventory().unwrap()),
    );
    let frontier = Arc::new(Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]));
    let logical = LogicalPlanBuilder::scan(
        "var_0",
        Arc::new(LogicalTableSource::new(Arc::clone(&frontier))),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let node = graphforge_plan::ExpandNode::new(
        Arc::new(logical),
        "KNOWS",
        0,
        2,
        1,
        Direction::Out,
        None,
        graphforge_storage::EXPLORATORY_EDGE_SCHEMA
            .fields()
            .iter()
            .cloned()
            .collect(),
        vec![],
        graphforge_storage::TOPOLOGY_NODES_SCHEMA
            .fields()
            .iter()
            .cloned()
            .collect(),
    );
    for uuid_required in [false, true] {
        let batch = RecordBatch::try_new(
            Arc::clone(&frontier),
            vec![Arc::new(UInt64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&frontier), None).unwrap();
        let exec = ExpandExec::new(
            &node,
            input,
            Arc::clone(&provider),
            Some(Arc::clone(&identities)),
            true,
            &resource,
        );
        let width = exec.schema().fields().len();
        let destination_uuid = width - graphforge_storage::TOPOLOGY_NODES_SCHEMA.fields().len();
        let mut required = vec![false; width];
        required[0] = true;
        required[destination_uuid] = uuid_required;
        required[destination_uuid + 1] = true;
        let exec = exec.with_required_output(required);
        let batches = collect(exec.clone(), SessionContext::new().task_ctx())
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        let metrics = exec.metrics().unwrap();
        assert_eq!(metrics.sum_by_name("edge_rows_read").unwrap().as_usize(), 0);
        assert_eq!(
            metrics
                .sum_by_name("destination_rows_read")
                .unwrap()
                .as_usize(),
            if uuid_required { 3 } else { 0 }
        );
        if uuid_required {
            let actual: Vec<_> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(destination_uuid)
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                        .unwrap()
                        .iter()
                        .map(|uuid| Uuid::from_slice(uuid.unwrap()).unwrap().as_u128())
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(actual, [2, 3, 3, 4]);
        }
    }
}

struct RefusingAdjacency;
impl AdjacencyProvider for RefusingAdjacency {
    fn adjacency(&self, _: &str, _: Direction) -> Result<Arc<adjacency::Adjacency>, GfError> {
        Err(GfError::Execution("adjacency setup refused".into()))
    }
    fn status(&self, _: &str, _: Direction) -> adjacency::AdjacencyStatus {
        adjacency::AdjacencyStatus::Building
    }
}

/// A refused setup runs no seed search/materialization. Its time can therefore
/// only be recorded by the adjacency setup guard, in either traversal mode.
#[tokio::test]
async fn traversal_setup_compute_is_recorded_even_when_adjacency_refuses() {
    for expansion in [VarLenMode::Trails, VarLenMode::Reachability] {
        let metrics = ExpandMetrics::default();
        let schema = Arc::new(Schema::empty());
        let cfg = ExpandConfig {
            rel_type_name: "KNOWS".into(),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: Some(2),
            dir: PathBuf::new(),
            mode: OntologyMode::Strict,
            src_col_idx: 0,
            bound_dst_col_idx: None,
            reverse_edge_list: false,
            expansion,
            edge_var: 0,
            edge_list_demanded: true,
            copy_input: None,
            out_schema: Arc::clone(&schema),
            provider: Arc::new(RefusingAdjacency),
            capture_epoch: 0,
            edge_filter: None,
            read_reservation: std::sync::Arc::new(std::sync::Mutex::new(
                crate::chunked_reservation::ChunkedReservation::register("ExpandExec hop read", &{
                    let pool: std::sync::Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
                        std::sync::Arc::new(
                            datafusion::execution::memory_pool::UnboundedMemoryPool::default(),
                        );
                    pool
                }),
            )),
            lifecycle: Default::default(),
            metrics: metrics.partition(0),
        };
        let input = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty(),
        ));
        let context = SessionContext::new();
        let mut reservation =
            ChunkedReservation::register(VAR_LEN_CONSUMER, context.task_ctx().memory_pool());
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let error = expand_bfs(&cfg, input, &tx, &mut reservation)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("adjacency setup refused"),
            "{error}"
        );
        let snapshot = metrics.snapshot();
        assert!(snapshot.elapsed_compute().unwrap() > 0, "{snapshot:?}");
        assert_eq!(snapshot.sum_by_name("input_rows").unwrap().as_usize(), 0);
    }
}
