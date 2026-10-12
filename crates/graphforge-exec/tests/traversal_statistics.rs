//! `VarLenExpandExec` publishes the rows it is estimated to emit, so the join
//! memory rule can plan the joins above it (#1985).
//!
//! The estimate is the input rows times the relation's degree, from the CSR
//! manifest the persistent provider serves. A relation that is not served from
//! a fresh persisted index publishes no estimate: reading its adjacency would
//! scan the edge table while planning.

use std::path::Path;
use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use tempfile::TempDir;

use graphforge_core::OntologyMode;
use graphforge_core::uuid::{Uuid, new_v7};
use graphforge_exec::{
    AdjacencyProvider, PersistentAdjacencyProvider, ScanBuildAdjacencyProvider, VarLenExpandExec,
};
use graphforge_ir::Direction;
use graphforge_plan::{VarLenExpandNode, var_len_edge_list_field};
use graphforge_storage::adjacency::build_adjacency_index;
use graphforge_storage::{GraphWriter, TOPOLOGY_NODES_SCHEMA};

const TS: i64 = 1_700_000_000_000_000;
const PERSON: graphforge_value::EntityTypeId = match graphforge_value::EntityTypeId::decode(0) {
    Ok(id) => id,
    Err(_) => panic!("valid person fixture identity"),
};
/// Nodes on the ring; each has [`DEGREE`] outgoing and [`DEGREE`] incoming edges.
const RING: usize = 100;
const DEGREE: usize = 3;
const SEEDS: usize = 50;

/// A ring where node `i` knows `i + 1 ..= i + DEGREE`, indexed. Returns the
/// surrogate node ids.
fn write_ring(dir: &Path) -> Vec<u64> {
    let mut writer = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..RING).map(|_| new_v7()).collect();
    let ids: Vec<u64> = uuids
        .iter()
        .map(|uuid| writer.create_node(*uuid, PERSON).unwrap())
        .collect();
    for (index, source) in uuids.iter().enumerate() {
        for offset in 1..=DEGREE {
            writer
                .create_edge(new_v7(), "KNOWS", source, &uuids[(index + offset) % RING])
                .unwrap();
        }
    }
    writer.flush().unwrap();
    build_adjacency_index(dir, TS).unwrap();
    ids
}

fn admitted_inventory(dir: &Path) -> Arc<graphforge_storage::AuthenticatedPropertyInventory> {
    graphforge_storage::GraphCatalog::open(dir, None, &graphforge_ir::RuntimeCatalog::new())
        .unwrap()
        .admitted_inventory()
        .unwrap()
}

fn frontier_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]))
}

fn node(direction: Direction, min_hops: u16, max_hops: Option<u16>) -> VarLenExpandNode {
    use datafusion::logical_expr::LogicalPlanBuilder;
    let table = Arc::new(
        datafusion::logical_expr::logical_plan::LogicalTableSource::new(frontier_schema()),
    );
    let input = LogicalPlanBuilder::scan("var_0", table, None)
        .unwrap()
        .build()
        .unwrap();
    VarLenExpandNode::new(
        Arc::new(input),
        "KNOWS",
        min_hops,
        max_hops,
        0,
        1,
        2,
        direction,
        Some(graphforge_value::RelationTypeId::decode(0).unwrap()),
        TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect(),
        var_len_edge_list_field(&[]),
    )
}

/// A frontier of the first [`SEEDS`] node ids: an in-memory plan whose row count
/// is exact.
async fn frontier(ids: &[u64]) -> Arc<dyn ExecutionPlan> {
    let batch = RecordBatch::try_new(
        frontier_schema(),
        vec![Arc::new(UInt64Array::from(ids[..SEEDS].to_vec()))],
    )
    .unwrap();
    SessionContext::new()
        .read_batch(batch)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

fn persistent(dir: &Path) -> Arc<dyn AdjacencyProvider> {
    Arc::new(
        PersistentAdjacencyProvider::new(dir.to_path_buf(), OntologyMode::Strict)
            .with_inventory(admitted_inventory(dir)),
    )
}

fn expand(
    dir: &Path,
    provider: Arc<dyn AdjacencyProvider>,
    node: &VarLenExpandNode,
    input: Arc<dyn ExecutionPlan>,
) -> VarLenExpandExec {
    VarLenExpandExec::new(
        node,
        input,
        provider,
        dir.to_path_buf(),
        OntologyMode::Strict,
    )
}

/// The estimated rows and bytes of `exec`.
fn estimate(exec: &VarLenExpandExec) -> (Precision<usize>, Precision<usize>) {
    let statistics = exec.partition_statistics(None).unwrap();
    (statistics.num_rows, statistics.total_byte_size)
}

#[tokio::test]
async fn a_traversal_over_a_persisted_index_estimates_rows_from_the_relation_degree() {
    let dir = TempDir::new().unwrap();
    let ids = write_ring(dir.path());
    let entries = RING * DEGREE;

    // (direction, min, max, rows per seed): a seed reaches the sum of
    // degree^hops trails for the hop counts it walks, at most one pass over the
    // relation's adjacency entries.
    let cases = [
        (Direction::Out, 1, Some(1), DEGREE),
        (Direction::In, 1, Some(1), DEGREE),
        (Direction::Out, 0, Some(1), 1 + DEGREE),
        (Direction::Out, 1, Some(2), DEGREE + DEGREE * DEGREE),
        (Direction::Out, 2, Some(2), DEGREE * DEGREE),
        // Unbounded patterns stay finite: the sum over the hops the estimate
        // follows is far beyond the relation, so it is the relation.
        (Direction::Out, 1, None, entries),
        (Direction::Out, 1, Some(40), entries),
    ];
    for (direction, min_hops, max_hops, per_seed) in cases {
        let exec = expand(
            dir.path(),
            persistent(dir.path()),
            &node(direction, min_hops, max_hops),
            frontier(&ids).await,
        );
        let (rows, bytes) = estimate(&exec);
        assert_eq!(
            rows,
            Precision::Inexact(SEEDS * per_seed),
            "{direction:?} {min_hops}..{max_hops:?}"
        );
        // Only rows are published: the join memory rule derives the bytes from
        // the rows and the schema, and a byte size would move DataFusion's
        // collect-left thresholds.
        assert_eq!(
            bytes,
            Precision::Absent,
            "{direction:?} {min_hops}..{max_hops:?}"
        );
    }
}

#[tokio::test]
async fn a_reachability_traversal_is_bounded_by_the_nodes_it_can_reach() {
    let dir = TempDir::new().unwrap();
    let ids = write_ring(dir.path());

    // A search that emits one row per reached node never emits more rows per
    // seed than the node identities the ring spans (an exclusive extent: one
    // past the largest id), however many trails lead to them; below that bound
    // it follows the same degree arithmetic as trail enumeration.
    let extent = usize::try_from(*ids.iter().max().unwrap()).unwrap() + 1;
    assert!(extent >= RING && extent < RING * DEGREE);
    let cases = [
        (1, Some(1), DEGREE),
        (1, Some(2), DEGREE + DEGREE * DEGREE),
        (1, None, extent),
        (1, Some(40), extent),
    ];
    for (min_hops, max_hops, per_seed) in cases {
        let exec = expand(
            dir.path(),
            persistent(dir.path()),
            &node(Direction::Out, min_hops, max_hops).into_reachability(),
            frontier(&ids).await,
        );
        let (rows, bytes) = estimate(&exec);
        assert_eq!(
            rows,
            Precision::Inexact(SEEDS * per_seed),
            "reachability {min_hops}..{max_hops:?}"
        );
        assert_eq!(
            bytes,
            Precision::Absent,
            "reachability {min_hops}..{max_hops:?}"
        );
    }

    // The same pattern as trail enumeration is estimated over the relation.
    let trails = expand(
        dir.path(),
        persistent(dir.path()),
        &node(Direction::Out, 1, None),
        frontier(&ids).await,
    );
    assert_eq!(
        estimate(&trails).0,
        Precision::Inexact(SEEDS * RING * DEGREE)
    );
}

#[tokio::test]
async fn a_traversal_without_a_persisted_index_publishes_no_estimate() {
    let dir = TempDir::new().unwrap();
    let ids = write_ring(dir.path());
    // The scan-build provider would read the whole edge table to answer.
    let scan: Arc<dyn AdjacencyProvider> = Arc::new(
        ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict)
            .with_inventory(admitted_inventory(dir.path())),
    );
    let exec = expand(
        dir.path(),
        scan,
        &node(Direction::Out, 1, Some(2)),
        frontier(&ids).await,
    );
    let (rows, bytes) = estimate(&exec);
    assert_eq!((rows, bytes), (Precision::Absent, Precision::Absent));
}

#[tokio::test]
async fn an_empty_frontier_is_estimated_to_emit_nothing() {
    let dir = TempDir::new().unwrap();
    write_ring(dir.path());
    let empty = RecordBatch::new_empty(frontier_schema());
    let input = SessionContext::new()
        .read_batch(empty)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let exec = expand(
        dir.path(),
        persistent(dir.path()),
        &node(Direction::Out, 1, Some(3)),
        input,
    );
    assert_eq!(estimate(&exec).0, Precision::Exact(0));
}

/// A frontier of the first [`SEEDS`] node ids read as `partitions` partitions.
fn partitioned_frontier(ids: &[u64], partitions: usize) -> Arc<dyn ExecutionPlan> {
    use datafusion::datasource::memory::MemorySourceConfig;

    let chunks: Vec<Vec<RecordBatch>> = ids[..SEEDS]
        .chunks(SEEDS.div_ceil(partitions))
        .map(|chunk| {
            vec![
                RecordBatch::try_new(
                    frontier_schema(),
                    vec![Arc::new(UInt64Array::from(chunk.to_vec()))],
                )
                .unwrap(),
            ]
        })
        .collect();
    let input = MemorySourceConfig::try_new_exec(&chunks, frontier_schema(), None).unwrap();
    assert_eq!(
        input.properties().output_partitioning().partition_count(),
        partitions
    );
    input
}

#[tokio::test]
async fn the_only_output_partition_is_estimated_as_the_whole_plan() {
    let dir = TempDir::new().unwrap();
    let ids = write_ring(dir.path());
    let exec = expand(
        dir.path(),
        persistent(dir.path()),
        &node(Direction::Out, 1, Some(1)),
        partitioned_frontier(&ids, 1),
    );
    assert_eq!(exec.properties().output_partitioning().partition_count(), 1);
    let whole = Precision::Inexact(SEEDS * DEGREE);
    assert_eq!(exec.partition_statistics(None).unwrap().num_rows, whole);
    assert_eq!(exec.partition_statistics(Some(0)).unwrap().num_rows, whole);
}

#[tokio::test]
async fn a_partitioned_variable_length_traversal_estimates_its_even_share_of_the_whole_plan() {
    let dir = TempDir::new().unwrap();
    let ids = write_ring(dir.path());
    // The traversal expands each input partition on its own, so it has as many
    // output partitions as its input. The whole plan is estimated from the
    // whole input; a partition is its even share of that, rounded up, so the
    // shares never sum to less than the whole. (`ExpandExec` is constructed
    // only inside its crate; its own test covers the same rule.)
    let whole = SEEDS * DEGREE;
    for partitions in [2, 3, 4] {
        let exec = expand(
            dir.path(),
            persistent(dir.path()),
            &node(Direction::Out, 1, Some(1)),
            partitioned_frontier(&ids, partitions),
        );
        assert_eq!(
            exec.properties().output_partitioning().partition_count(),
            partitions
        );
        assert_eq!(
            exec.partition_statistics(None).unwrap().num_rows,
            Precision::Inexact(whole)
        );
        let mut shares = 0;
        for partition in 0..partitions {
            let share = exec.partition_statistics(Some(partition)).unwrap().num_rows;
            assert_eq!(
                share,
                Precision::Inexact(whole.div_ceil(partitions)),
                "{partitions} partitions, partition {partition}"
            );
            if let Precision::Inexact(rows) = share {
                shares += rows;
            }
        }
        assert!(shares >= whole, "{partitions} shares sum to {shares}");
    }
}
