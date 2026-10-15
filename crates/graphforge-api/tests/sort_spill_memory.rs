//! Real unbounded sort spill, exact output, and last-consumer cleanup (#2146).
use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use graphforge_api::{
    ExecutionResourcePolicy, ExecutionResult, GraphForge, GraphForgeOptions, ResourcePolicyMode,
    ResultSinkFormat, ResultSinkOptions, SpillPolicy,
};
use graphforge_exec::demand;
use std::collections::HashMap;
use tempfile::TempDir;

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const NODES: usize = 65_536;
const MIB: u64 = 1024 * 1024;

fn query() -> String {
    let payload = std::iter::repeat_n("n.name", 16)
        .collect::<Vec<_>>()
        .join(" + '|' + ");
    format!("MATCH (n:Entity) WITH {payload} AS payload RETURN payload ORDER BY payload DESC")
}

fn open(project: &TempDir, spill: &TempDir, budget: u64) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.path().to_str().unwrap()),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(2),
                target_partitions: Some(1),
                io_concurrency: Some(2),
                compute_threads: Some(1),
                batch_size: Some(256),
                memory_budget_bytes: Some(budget),
                spill: SpillPolicy {
                    enabled: true,
                    directory: Some(spill.path().to_path_buf()),
                    max_bytes: Some(1024 * MIB),
                },
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

fn values(result: &ExecutionResult) -> Vec<String> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "payload");
            let column = batch.column(0);
            assert_eq!(column.null_count(), 0);
            match column.data_type() {
                DataType::Utf8 => column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned())
                    .collect::<Vec<_>>(),
                DataType::Utf8View => column
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned())
                    .collect::<Vec<_>>(),
                DataType::LargeUtf8 => column
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned())
                    .collect::<Vec<_>>(),
                data_type => panic!("expected Arrow text, got {data_type:?}"),
            }
        })
        .collect()
}

#[test]
fn an_unbounded_sort_spills_with_exact_descending_output_and_releases_its_readers() {
    let project = TempDir::new().unwrap();
    bulk_fixture::generate_bulk_graph_with_properties(project.path(), NODES, 0, false);
    let statement = query();
    let mut independent: Vec<_> = (0..NODES)
        .map(|index| {
            let name = bulk_fixture::fixture_node_name(index);
            std::iter::repeat_n(name.as_str(), 16)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    independent.sort_unstable_by(|left, right| right.cmp(left));
    let roomy_spill = TempDir::new().unwrap();
    let roomy_forge = open(&project, &roomy_spill, 512 * MIB);
    let roomy = roomy_forge.execute(&statement).unwrap();
    assert_eq!(values(&roomy), independent);
    let spill = TempDir::new().unwrap();
    let forge = open(&project, &spill, 64 * MIB);
    let (tight, evidence) = demand::capture(|| forge.execute(&statement));
    let tight = tight.expect("a fitting spilling sort must complete under its original pool");
    assert_eq!(tight.schema.fields(), roomy.schema.fields());
    let stable_metadata = |result: &ExecutionResult| {
        let mut metadata = result.schema.metadata().clone();
        let query_id = metadata
            .remove("graphforge.query_id")
            .expect("every query has an ID");
        let query_id = uuid::Uuid::parse_str(&query_id).expect("query IDs are valid UUIDs");
        assert!(!query_id.is_nil());
        (query_id, metadata)
    };
    let (tight_id, tight_metadata) = stable_metadata(&tight);
    let (roomy_id, roomy_metadata) = stable_metadata(&roomy);
    assert_ne!(
        tight_id, roomy_id,
        "independent executions have unique query IDs"
    );
    assert_eq!(tight_metadata, roomy_metadata);
    assert_eq!(values(&tight), independent);
    assert!(tight.stats.peak_memory_bytes <= 64 * MIB);
    assert!(!evidence.sorts.is_empty());
    assert!(
        evidence
            .sorts
            .iter()
            .any(|sort| sort.spill_count > 0 && sort.spilled_rows > 0 && sort.spilled_bytes > 0),
        "actual spill metrics: {evidence:#?}"
    );
    drop(tight);
    drop(roomy);
    // The sink consumes every output before evidence is finalized, allowing
    // cleanup to be checked after the final result consumer drops its arrays.
    let outputs = TempDir::new().unwrap();
    let roomy_receipt = roomy_forge
        .execute_to_result_sink_with_evidence(
            &statement,
            &HashMap::new(),
            outputs.path().join("roomy.arrow").to_str().unwrap(),
            ResultSinkFormat::ArrowIpc,
            &ResultSinkOptions::default(),
            None,
        )
        .unwrap();
    assert!(
        roomy_receipt
            .evidence
            .sorts
            .iter()
            .all(|sort| sort.spill_count == 0 && sort.retained_bytes == 0)
    );
    assert_eq!(roomy_receipt.evidence.memory_reserved_before, 0);
    let receipt = forge
        .execute_to_result_sink_with_evidence(
            &statement,
            &HashMap::new(),
            outputs.path().join("sorted.arrow").to_str().unwrap(),
            ResultSinkFormat::ArrowIpc,
            &ResultSinkOptions::default(),
            None,
        )
        .unwrap();
    assert!(
        receipt
            .evidence
            .sorts
            .iter()
            .any(|sort| sort.spill_count > 0 && sort.spilled_rows > 0 && sort.spilled_bytes > 0)
    );
    assert!(
        receipt
            .evidence
            .sorts
            .iter()
            .all(|sort| sort.retained_bytes == 0)
    );
    assert_eq!(receipt.evidence.memory_reserved_before, 0);
    // Evidence is sampled while the physical plan is still held. Its node
    // scan retains HashJoinInput even without spilling; use the identical
    // roomy sink phase as the oracle for that existing plan-owned residual.
    assert_eq!(
        receipt.evidence.memory_reserved_after,
        roomy_receipt.evidence.memory_reserved_after
    );
    assert_eq!(std::fs::read_dir(spill.path()).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(roomy_spill.path()).unwrap().count(), 0);
}
