use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;

use super::*;
use crate::graph_construction_encoding::{BulkBatchReader, BulkSource};

struct Never;

impl BulkBatchReader for Never {
    fn task_rows(&self, _: usize) -> usize {
        unreachable!("planning only")
    }

    fn read_task(
        &self,
        _: usize,
        _: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        unreachable!("planning only")
    }
}

struct AccountingReader {
    task: u64,
    source_level: u64,
}

impl BulkBatchReader for AccountingReader {
    fn task_rows(&self, _: usize) -> usize {
        1
    }

    fn read_task(
        &self,
        _: usize,
        _: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        unreachable!("planning only")
    }

    fn max_task_workspace_bytes(&self) -> u64 {
        self.task
    }

    fn source_level_workspace_bytes(&self) -> u64 {
        self.source_level
    }
}

fn rung(scale: u32) -> BulkBuildPlan<'static> {
    let source = |rows: u64| BulkSource {
        reader: Arc::new(Never),
        tasks: 1,
        rows,
        property_free: true,
        decoded_bytes: 0,
    };
    BulkBuildPlan {
        nodes: vec![source(1 << scale)],
        edges: vec![source(16 << scale)],
        memory_budget: None,
    }
}

const GIB: u64 = 1 << 30;

#[test]
fn source_pool_reserves_full_task_and_shared_source_bytes_once() {
    let source = |task, source_level| BulkSource {
        reader: Arc::new(AccountingReader { task, source_level }),
        tasks: 1,
        rows: 1,
        property_free: true,
        decoded_bytes: 0,
    };
    let mut plan = BulkBuildPlan {
        nodes: vec![source(96 << 20, 8 << 20), source(64 << 20, 8 << 20)],
        edges: Vec::new(),
        memory_budget: Some(4 * GIB),
    };
    let decoder = plan.source_decoder_bytes();
    assert_eq!(decoder, 96 << 20);
    assert_eq!(plan.source_level_workspace_bytes(), 8 << 20);
    let workspace = ScratchPlan::source_pool_bytes(
        &plan,
        4 * GIB,
        crate::graph_construction_encoding::GraphConstructionBudgets::default(),
    );
    let working = scratch_working_bytes(
        &plan,
        4 * GIB,
        crate::graph_construction_encoding::GraphConstructionBudgets::default(),
        false,
        false,
    );
    assert!(working / 4 > decoder);
    assert_eq!(workspace, working / 4 + (8 << 20));
    // Two source readers report the same build-level cap; it is reserved once.
    plan.nodes[1].reader = Arc::new(AccountingReader {
        task: 64 << 20,
        source_level: 8 << 20,
    });
    assert_eq!(plan.source_level_workspace_bytes(), 8 << 20);
}

#[test]
fn large_task_workspace_moves_node_tables_to_scratch_without_pool_overgrant() {
    let task = 500 << 20;
    let source_level = 64 << 20;
    let source = BulkSource {
        reader: Arc::new(AccountingReader { task, source_level }),
        tasks: 1,
        rows: 1 << 24,
        property_free: true,
        decoded_bytes: 0,
    };
    let plan = BulkBuildPlan {
        nodes: vec![source],
        edges: Vec::new(),
        memory_budget: Some(3 << 29), // 1.5 GiB
    };
    let budget = plan.memory_budget.unwrap();
    let extra = source_level + task.saturating_sub(CSR_WORKSPACE_BYTES);
    let workspace = ScratchPlan::source_pool_bytes(
        &plan,
        budget,
        crate::graph_construction_encoding::GraphConstructionBudgets::default(),
    );

    assert!(node_tables_on_scratch(
        &plan,
        budget,
        crate::graph_construction_encoding::GraphConstructionBudgets::default(),
    ));
    assert_eq!(
        source_phase_extra(
            &plan,
            crate::graph_construction_encoding::GraphConstructionBudgets::default(),
        ),
        extra
    );
    assert_eq!(workspace, task + source_level);
    assert!(plan.scratch_floor_bytes() + extra <= budget);
}

#[test]
fn route_estimate_includes_shared_source_level_workspace() {
    let source = |source_level| BulkSource {
        reader: Arc::new(AccountingReader {
            task: 0,
            source_level,
        }),
        tasks: 1,
        rows: 1,
        property_free: true,
        decoded_bytes: 0,
    };
    let baseline = BulkBuildPlan {
        nodes: vec![source(0)],
        edges: Vec::new(),
        memory_budget: None,
    };
    let with_pending = BulkBuildPlan {
        nodes: vec![source(8 << 20)],
        edges: Vec::new(),
        memory_budget: None,
    };
    let baseline_estimate = baseline.estimated_resident_bytes();
    let pending_estimate = with_pending.estimated_resident_bytes();

    assert_eq!(pending_estimate - baseline_estimate, (8 << 20) * 5 / 4);
    assert_eq!(
        with_pending.route_for(baseline_estimate),
        super::BulkRoute::Scratch
    );
    assert_eq!(
        with_pending.route_for(pending_estimate),
        super::BulkRoute::Memory
    );
}

#[test]
fn partition_sizes_and_concurrency_follow_the_budget() {
    for scale in [22, 24, 26] {
        let plan = rung(scale);
        let mut previous = None::<ScratchPlan>;
        for budget in [8 * GIB, 4 * GIB, 2 * GIB, GIB + GIB / 2] {
            if budget < plan.node_tables_resident_bytes() {
                continue;
            }
            let sized = ScratchPlan::derive(&plan, budget, 16);
            assert!((1..=16).contains(&sized.concurrency), "{sized:?}");
            assert!(sized.staging_bytes >= 32, "{sized:?}");
            let working = budget - plan.node_tables_resident_bytes() + MIN_WORKING_BYTES;
            let buffers = sized.concurrency as u64
                * (sized.edge_partitions.max(2 * sized.csr_partitions) as u64)
                * sized.staging_bytes as u64;
            assert!(buffers <= working / 8, "{sized:?}");
            assert!(
                plan.node_tables_resident_bytes() - MIN_WORKING_BYTES
                    + sized.gate_bytes
                    + working / 4
                    <= budget,
                "{sized:?}"
            );
            // The partitions in flight reserve at most half the gate, so a
            // partition twice its share still fits.
            let edges = 16_u64 << scale;
            let in_flight = sized.edge_cost(edges.div_ceil(sized.edge_partitions as u64))
                * sized.concurrency as u64;
            assert!(
                sized.edge_partitions as u64 == MAX_PARTITIONS || in_flight <= sized.gate_bytes / 2,
                "S{scale} budget {budget}: {sized:?} reserves {in_flight}"
            );
            // A smaller budget reserves no more in flight.
            if let Some(larger) = previous {
                assert!(
                    sized.gate_bytes <= larger.gate_bytes,
                    "{sized:?} {larger:?}"
                );
            }
            previous = Some(sized);
        }
    }
}

/// SNB BI SF1's shape: 3.0M nodes and 17.2M edges, both with properties,
/// from 1.19 GB of Parquet.
fn property_plan() -> BulkBuildPlan<'static> {
    let source = |rows: u64| BulkSource {
        reader: Arc::new(Never),
        tasks: 4,
        rows,
        property_free: false,
        decoded_bytes: 600 << 20,
    };
    BulkBuildPlan {
        nodes: vec![source(3_000_000)],
        edges: vec![source(17_200_000)],
        memory_budget: None,
    }
}

#[test]
fn property_builds_derive_concurrency_and_sizes_from_the_budget() {
    let plan = property_plan();
    let budgets = crate::graph_construction::GraphConstructionBudgets::default();
    let floor = plan.node_tables_resident_bytes() + property_extra_workspace(&plan, budgets);
    let mut previous = 0;
    let mut distinct = std::collections::BTreeSet::new();
    for budget in [
        floor,
        floor + (64 << 20),
        floor + (128 << 20),
        floor + (256 << 20),
        floor + (512 << 20),
        2 * GIB,
        4 * GIB,
        16 * GIB,
    ] {
        if budget < floor {
            continue;
        }
        let sized = ScratchPlan::derive_with_budgets(&plan, budget, 16, budgets);
        let workers = sized.concurrency as u64;
        assert!((1..=16).contains(&workers), "{sized:?}");
        assert!(
            workers >= previous,
            "a larger budget lost workers: {sized:?}"
        );
        previous = workers;
        distinct.insert(workers);
        // The node tables, the overlay writer's workspace and the shared
        // run pool fit the budget, and the decode pool takes no more than
        // the working set leaves.
        let shared = floor - MIN_WORKING_BYTES;
        let property = sized.property;
        assert!(
            shared + (workers - 1) * WORKER_BYTES + property.retained_bytes + sized.decode_bytes
                <= budget + MIN_WORKING_BYTES
                || property.retained_bytes <= 1 << 20,
            "budget {budget}: {sized:?}"
        );
        // One merge holds a frame set per input, for every worker at once.
        let per_input =
            MERGE_FRAMES_PER_INPUT * property.frame_bytes as u64 + plan.max_source_schema_bytes();
        assert!(property.fan_in >= 2 && property.fan_in as u64 <= MAX_FAN_IN);
        assert!(
            workers * property.fan_in as u64 * per_input <= property.retained_bytes
                || property.fan_in == 2,
            "budget {budget}: {sized:?}"
        );
        assert!(
            property.run_bytes as u64 <= MAX_RUN_BYTES && property.run_bytes >= 1 << 20,
            "{sized:?}"
        );
    }
    assert!(
        distinct.len() >= 3,
        "concurrency never grew with the budget: {distinct:?}"
    );
    // A property-free plan of the same size is not charged for property runs.
    let free = rung(21);
    assert!(
        ScratchPlan::derive(&free, 4 * GIB, 16).concurrency >= 1,
        "property-free plans keep deriving their own concurrency"
    );
}

#[test]
fn a_tiny_input_is_one_partition_and_the_largest_is_bounded() {
    let tiny = ScratchPlan::derive(&rung(4), GIB, 16);
    assert_eq!((tiny.edge_partitions, tiny.csr_partitions), (1, 1));
    let huge = ScratchPlan::derive(&rung(30), 4 * GIB, 16);
    assert!(huge.edge_partitions as u64 <= MAX_PARTITIONS);
    assert!(huge.csr_partitions as u64 <= MAX_PARTITIONS);
}
