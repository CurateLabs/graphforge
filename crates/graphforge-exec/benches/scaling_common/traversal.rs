use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use graphforge_core::OntologyMode;
use graphforge_core::uuid::{Uuid, new_v7};
use graphforge_exec::{AdjacencyProvider, PersistentAdjacencyProvider, VarLenExpandExec};
use graphforge_ir::Direction;
use graphforge_plan::{VarLenExpandNode, var_len_edge_list_field};
use graphforge_storage::adjacency::build_adjacency_index;
use graphforge_storage::{GraphWriter, TOPOLOGY_NODES_SCHEMA};
use graphforge_value::EntityTypeId;
use tempfile::TempDir;

const TS: i64 = 1_700_000_000_000_000;
const PERSON: EntityTypeId = match EntityTypeId::decode(0) {
    Ok(id) => id,
    Err(_) => panic!("valid person fixture identity"),
};

pub fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(value) => value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("invalid {key}={value:?}: {error}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => panic!("invalid {key}: not valid Unicode"),
    }
}

fn frontier_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]))
}

fn make_node(min_hops: u16, max_hops: Option<u16>) -> VarLenExpandNode {
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;

    let (src_var, dst_var, edge_var) = (0u32, 1u32, 2u32);
    let table = Arc::new(LogicalTableSource::new(frontier_schema()));
    let input = LogicalPlanBuilder::scan(format!("var_{src_var}"), table, None)
        .unwrap()
        .build()
        .unwrap();
    let dst_fields = TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect();
    VarLenExpandNode::new(
        Arc::new(input),
        "KNOWS",
        min_hops,
        max_hops,
        src_var,
        dst_var,
        edge_var,
        Direction::Out,
        Some(graphforge_value::RelationTypeId::decode(0).unwrap()),
        dst_fields,
        var_len_edge_list_field(&[]),
    )
}

fn admitted_inventory(dir: &Path) -> Arc<graphforge_storage::AuthenticatedPropertyInventory> {
    graphforge_storage::GraphCatalog::open(dir, None, &graphforge_ir::RuntimeCatalog::new())
        .unwrap()
        .admitted_inventory()
        .unwrap()
}

fn persistent_provider(dir: &Path) -> Arc<dyn AdjacencyProvider> {
    Arc::new(
        PersistentAdjacencyProvider::new(dir.to_path_buf(), OntologyMode::Strict)
            .with_inventory(admitted_inventory(dir)),
    ) as Arc<dyn AdjacencyProvider>
}

fn generate_ring_graph(dir: &Path, n: usize, fan_out: usize, num_seeds: usize) -> Vec<u64> {
    assert!(n > fan_out, "fan_out must be < n");
    let mut writer = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..n).map(|_| new_v7()).collect();
    let node_ids: Vec<u64> = uuids
        .iter()
        .map(|uuid| writer.create_node(*uuid, PERSON).unwrap())
        .collect();

    let total = n * fan_out;
    let stride: u64 = 1_000_003;
    for step in 0..total {
        let scrambled =
            u64::try_from(step).unwrap().wrapping_mul(stride) % u64::try_from(total).unwrap();
        let edge = usize::try_from(scrambled).unwrap();
        let src = edge / fan_out;
        let dst = (src + 1 + (edge % fan_out)) % n;
        writer
            .create_edge(new_v7(), "KNOWS", &uuids[src], &uuids[dst])
            .unwrap();
    }
    writer.flush().unwrap();

    let step = (n / num_seeds).max(1);
    (0..num_seeds)
        .map(|index| node_ids[(index * step) % n])
        .collect()
}

fn generate_ring_natural(dir: &Path, n: usize, fan_out: usize, num_seeds: usize) -> Vec<u64> {
    let mut writer = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..n).map(|_| new_v7()).collect();
    let node_ids: Vec<u64> = uuids
        .iter()
        .map(|uuid| writer.create_node(*uuid, PERSON).unwrap())
        .collect();
    for src in 0..n {
        for offset in 1..=fan_out {
            writer
                .create_edge(new_v7(), "KNOWS", &uuids[src], &uuids[(src + offset) % n])
                .unwrap();
        }
    }
    writer.flush().unwrap();
    let base = n / 2;
    (0..num_seeds)
        .map(|index| node_ids[(base + index) % n])
        .collect()
}

async fn run_expand(dir: &Path, provider: Arc<dyn AdjacencyProvider>, seeds: &[u64], hops: u16) {
    let node = make_node(1, Some(hops));
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        frontier_schema(),
        vec![Arc::new(UInt64Array::from(seeds.to_vec()))],
    )
    .unwrap();
    let input: Arc<dyn ExecutionPlan> = ctx
        .read_batch(batch)
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let exec = Arc::new(VarLenExpandExec::new(
        &node,
        input,
        provider,
        dir.to_path_buf(),
        OntologyMode::Strict,
    ));
    collect(exec, ctx.task_ctx()).await.unwrap();
}

pub struct WarmTraversalFixture {
    dir: TempDir,
    seeds: Vec<u64>,
    provider: Arc<dyn AdjacencyProvider>,
    runtime: tokio::runtime::Runtime,
}

impl WarmTraversalFixture {
    pub fn localized(n: usize, fan_out: usize, num_seeds: usize) -> Self {
        Self::build(n, fan_out, num_seeds, true)
    }

    pub fn scattered(n: usize, fan_out: usize, num_seeds: usize) -> Self {
        Self::build(n, fan_out, num_seeds, false)
    }

    fn build(n: usize, fan_out: usize, num_seeds: usize, localized: bool) -> Self {
        let dir = TempDir::new().unwrap();
        let seeds = if localized {
            generate_ring_natural(dir.path(), n, fan_out, num_seeds)
        } else {
            generate_ring_graph(dir.path(), n, fan_out, num_seeds)
        };
        build_adjacency_index(dir.path(), TS).unwrap();
        let provider = persistent_provider(dir.path());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let fixture = Self {
            dir,
            seeds,
            provider,
            runtime,
        };
        fixture.warm();
        fixture
    }

    fn warm(&self) {
        self.runtime.block_on(run_expand(
            self.dir.path(),
            Arc::clone(&self.provider),
            &self.seeds,
            1,
        ));
    }

    pub fn expand_hops(&self, hops: u16) {
        self.runtime.block_on(run_expand(
            self.dir.path(),
            Arc::clone(&self.provider),
            &self.seeds,
            hops,
        ));
    }
}
