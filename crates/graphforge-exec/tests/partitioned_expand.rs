//! `VarLenExpandExec` expands each input partition on its own (#1984).
//!
//! The operator used to declare one output partition and coalesce its input,
//! so a traversal ran on a single task however many partitions the planner
//! gave it. These tests drive the physical node directly over an input with
//! several partitions and pin three properties:
//!
//! * the output has one partition per input partition;
//! * partition `i` of the output expands only the seeds of input partition `i`;
//! * the rows are the same multiset whether the seeds are split into 1, 3 or
//!   4 partitions.

use std::collections::{BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, ListArray, StructArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, collect_partitioned};
use datafusion::prelude::SessionContext;
use datafusion_datasource::memory::MemorySourceConfig;
use tempfile::TempDir;

use graphforge_core::OntologyMode;
use graphforge_core::uuid::{Uuid, new_v7};
use graphforge_exec::{AdjacencyProvider, ScanBuildAdjacencyProvider, VarLenExpandExec};
use graphforge_ir::Direction;
use graphforge_plan::{VarLenExpandNode, var_len_edge_list_field};
use graphforge_storage::{GraphWriter, TOPOLOGY_NODES_SCHEMA};

const TS: i64 = 1_700_000_000_000_000;
const PERSON: graphforge_value::EntityTypeId = match graphforge_value::EntityTypeId::decode(0) {
    Ok(id) => id,
    Err(_) => panic!("valid person fixture identity"),
};

const NODES: usize = 200;
const EDGES: usize = 600;

/// Deterministic pseudo-random sequence (64-bit LCG), so the fixture graph is
/// the same on every run and no random-number crate is needed.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        usize::try_from((self.0 >> 33) % u64::try_from(bound).unwrap()).unwrap()
    }
}

/// Write [`NODES`] nodes joined by [`EDGES`] random `KNOWS` edges (self-loops,
/// parallel edges and cycles included). Returns the surrogate `node_id`s.
fn write_random_graph(dir: &Path) -> Vec<u64> {
    let mut writer = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..NODES).map(|_| new_v7()).collect();
    let ids = uuids
        .iter()
        .map(|uuid| writer.create_node(*uuid, PERSON).unwrap())
        .collect();
    let mut rng = Lcg(0x5eed_1984);
    for _ in 0..EDGES {
        let (from, to) = (rng.below(NODES), rng.below(NODES));
        writer
            .create_edge(new_v7(), "KNOWS", &uuids[from], &uuids[to])
            .unwrap();
    }
    writer.flush().unwrap();
    ids
}

fn frontier_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]))
}

/// Split `seeds` into `partitions` input partitions, round-robin, one batch
/// each. Every partition gets at least one seed when `seeds.len() >= partitions`.
fn split_seeds(seeds: &[u64], partitions: usize) -> Vec<Vec<u64>> {
    let mut split = vec![Vec::new(); partitions];
    for (position, seed) in seeds.iter().enumerate() {
        split[position % partitions].push(*seed);
    }
    split
}

fn partitioned_input(split: &[Vec<u64>]) -> Arc<dyn ExecutionPlan> {
    let partitions: Vec<Vec<RecordBatch>> = split
        .iter()
        .map(|seeds| {
            vec![
                RecordBatch::try_new(
                    frontier_schema(),
                    vec![Arc::new(UInt64Array::from(seeds.clone()))],
                )
                .unwrap(),
            ]
        })
        .collect();
    MemorySourceConfig::try_new_exec(&partitions, frontier_schema(), None).unwrap()
}

fn var_len_node(min_hops: u16, max_hops: u16) -> VarLenExpandNode {
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
        Some(max_hops),
        0,
        1,
        2,
        Direction::Out,
        Some(graphforge_value::RelationTypeId::decode(0).unwrap()),
        TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect(),
        var_len_edge_list_field(&[]),
    )
}

fn var_len_exec(
    dir: &Path,
    node: &VarLenExpandNode,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<VarLenExpandExec> {
    let inventory =
        graphforge_storage::GraphCatalog::open(dir, None, &graphforge_ir::RuntimeCatalog::new())
            .unwrap()
            .admitted_inventory()
            .unwrap();
    let provider: Arc<dyn AdjacencyProvider> = Arc::new(
        ScanBuildAdjacencyProvider::new(dir.to_path_buf(), OntologyMode::Strict)
            .with_inventory(inventory),
    );
    Arc::new(VarLenExpandExec::new(
        node,
        input,
        provider,
        dir.to_path_buf(),
        OntologyMode::Strict,
    ))
}

/// One output row: source `node_id`, destination `node_id`, and the edge UUIDs
/// of the path in order.
type Row = (u64, u64, Vec<Vec<u8>>);

fn rows_of(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let source = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        // The destination node's `node_id` follows the frontier column.
        let destination = batch
            .column(frontier_schema().fields().len() + 1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let paths = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("trailing column is the relationship list");
        for row in 0..batch.num_rows() {
            let path = paths.value(row);
            let edges = path.as_any().downcast_ref::<StructArray>().unwrap();
            let uuids = edges
                .column_by_name("edge_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            rows.push((
                source.value(row),
                destination.value(row),
                (0..uuids.len()).map(|i| uuids.value(i).to_vec()).collect(),
            ));
        }
    }
    rows
}

async fn run_partitioned(
    dir: &Path,
    node: &VarLenExpandNode,
    split: &[Vec<u64>],
) -> Vec<Vec<RecordBatch>> {
    let exec = var_len_exec(dir, node, partitioned_input(split));
    assert_eq!(
        exec.properties().output_partitioning().partition_count(),
        split.len()
    );
    collect_partitioned(exec, SessionContext::new().task_ctx())
        .await
        .unwrap()
}

#[tokio::test]
async fn output_has_one_partition_per_input_partition() {
    let dir = TempDir::new().unwrap();
    let ids = write_random_graph(dir.path());
    let node = var_len_node(1, 2);
    for partitions in [1_usize, 2, 4] {
        let exec = var_len_exec(
            dir.path(),
            &node,
            partitioned_input(&split_seeds(&ids, partitions)),
        );
        assert_eq!(
            exec.properties().output_partitioning().partition_count(),
            partitions,
            "VarLenExpandExec must keep the {partitions} partitions of its input"
        );
    }
}

#[tokio::test]
async fn a_round_robin_input_stays_round_robin() {
    let dir = TempDir::new().unwrap();
    let ids = write_random_graph(dir.path());
    let node = var_len_node(1, 2);
    let single = partitioned_input(&split_seeds(&ids, 1));
    let spread = RepartitionExec::try_new(single, Partitioning::RoundRobinBatch(4)).unwrap();
    let exec = var_len_exec(dir.path(), &node, Arc::new(spread));
    assert!(
        matches!(
            exec.properties().output_partitioning(),
            Partitioning::RoundRobinBatch(4)
        ),
        "a round-robin input promises nothing about row placement, so the \
         expansion must keep reporting it; got {:?}",
        exec.properties().output_partitioning()
    );
    let unspecified = var_len_exec(dir.path(), &node, partitioned_input(&split_seeds(&ids, 3)));
    assert!(
        matches!(
            unspecified.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(3)
        ),
        "an input with no declared placement stays unspecified; got {:?}",
        unspecified.properties().output_partitioning()
    );
}

#[tokio::test]
async fn each_partition_expands_only_its_own_seeds() {
    let dir = TempDir::new().unwrap();
    let ids = write_random_graph(dir.path());
    let node = var_len_node(1, 2);
    let split = split_seeds(&ids, 4);

    let output = run_partitioned(dir.path(), &node, &split).await;

    assert_eq!(output.len(), 4);
    let non_empty = output
        .iter()
        .filter(|batches| batches.iter().any(|batch| batch.num_rows() > 0))
        .count();
    assert!(
        non_empty > 1,
        "expansion must produce rows in more than one partition, got {non_empty}"
    );
    for (partition, batches) in output.iter().enumerate() {
        let seeds: HashSet<u64> = split[partition].iter().copied().collect();
        let sources: BTreeSet<u64> = rows_of(batches).into_iter().map(|row| row.0).collect();
        assert!(
            sources.iter().all(|source| seeds.contains(source)),
            "partition {partition} expanded seeds outside its own input partition"
        );
    }
}

#[tokio::test]
async fn partitioned_rows_equal_single_partition_rows() {
    let dir = TempDir::new().unwrap();
    let ids = write_random_graph(dir.path());
    for (min_hops, max_hops) in [(1, 1), (1, 3), (2, 2)] {
        let node = var_len_node(min_hops, max_hops);
        let mut expected = rows_of(
            &run_partitioned(dir.path(), &node, &split_seeds(&ids, 1))
                .await
                .concat(),
        );
        expected.sort();
        assert!(
            expected.len() > EDGES / 2,
            "*{min_hops}..{max_hops} fixture must produce a non-trivial result, got {} rows",
            expected.len()
        );
        // Three partitions split the seeds unevenly (200 is not a multiple).
        for partitions in [3_usize, 4] {
            let mut actual = rows_of(
                &run_partitioned(dir.path(), &node, &split_seeds(&ids, partitions))
                    .await
                    .concat(),
            );
            actual.sort();
            assert_eq!(
                actual, expected,
                "*{min_hops}..{max_hops} over {partitions} partitions changed the rows"
            );
        }
    }
}

#[tokio::test]
async fn execute_rejects_a_partition_the_input_does_not_have() {
    let dir = TempDir::new().unwrap();
    let ids = write_random_graph(dir.path());
    let exec = var_len_exec(
        dir.path(),
        &var_len_node(1, 2),
        partitioned_input(&split_seeds(&ids, 4)),
    );
    let context = SessionContext::new().task_ctx();
    exec.execute(3, context.clone()).unwrap();
    let error = match exec.execute(4, context) {
        Ok(_) => panic!("partition 4 of a 4-partition input must be rejected"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("has 4 partitions"),
        "unexpected error: {error}"
    );
}
