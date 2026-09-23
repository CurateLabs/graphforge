use super::*;
use crate::construction_record_layout::ENDPOINT_WIDTH;
use crate::graph_construction::partition::PartitionPlan;
use crate::graph_construction::partition_shaping::{
    FixedRangePartitioner, PartitionFamily, fixed_spill_name, load_fixed_partition,
};
use std::sync::atomic::AtomicBool;

/// One hub node: every endpoint shares the 16-byte key, so no splitter set can
/// divide the partition. Routed in descending tail order so the sort has work.
fn hub_endpoint(position: u64) -> [u8; ENDPOINT_WIDTH] {
    let mut record = [0_u8; ENDPOINT_WIDTH];
    record[..16].copy_from_slice(&0x1506_u128.to_be_bytes());
    record[24..32].copy_from_slice(&position.to_be_bytes());
    record
}

const HUB_RECORDS: u64 = 8_192;

/// Seal one hub partition the way shaping does and return its segment names.
fn seal_hub_partition(
    session: &mut crate::graph_construction::GraphConstructionSession,
) -> Vec<String> {
    let mut partitioner = FixedRangePartitioner::<ENDPOINT_WIDTH>::new(
        &session.root,
        PartitionFamily::Endpoints,
        1,
        None,
        false,
    )
    .unwrap();
    let wire = (0..HUB_RECORDS)
        .rev()
        .flat_map(hub_endpoint)
        .collect::<Vec<_>>();
    partitioner
        .route_slice(0, &wire, HUB_RECORDS, &mut session.checkpoint.evidence)
        .unwrap();
    partitioner
        .seal(1, &mut session.checkpoint.evidence)
        .unwrap();
    vec![fixed_spill_name(PartitionFamily::Endpoints, 1, 0)]
}

#[test]
fn over_budget_hub_partition_is_refused_in_memory_and_sorted_by_bounded_external_sort() {
    let root = tempfile::TempDir::new().unwrap();
    let mut session = crate::graph_construction::tests::open(&root, 0x1506);
    let names = seal_hub_partition(&mut session);
    let partition_bytes = HUB_RECORDS * ENDPOINT_WIDTH as u64;
    let budget = partition_bytes / 4;

    // Current contract: the recorded budget refuses before allocating.
    let refused = load_fixed_partition::<ENDPOINT_WIDTH>(
        &session.root,
        &names,
        Some(HUB_RECORDS),
        None,
        budget,
        &AtomicBool::new(false),
    )
    .err()
    .expect("an over-budget hub partition must be refused");
    assert!(
        refused.to_string().contains("exceeds recorded budget"),
        "{refused}"
    );

    // Reference order from an admitted in-memory load of the same spill.
    let (reference, _) = load_fixed_partition::<ENDPOINT_WIDTH>(
        &session.root,
        &names,
        Some(HUB_RECORDS),
        None,
        partition_bytes,
        &AtomicBool::new(false),
    )
    .unwrap();
    let reference = reference.iter().flatten().copied().collect::<Vec<_>>();

    // Candidate: the same spill under a pool of the same budget completes.
    let scratch = tempfile::TempDir::new_in(root.path()).unwrap();
    let mut streamed = Vec::new();
    let outcome = external_sort_fixed_partition::<ENDPOINT_WIDTH>(
        &session.root,
        &names,
        HUB_RECORDS,
        usize::try_from(budget).unwrap(),
        scratch.path(),
        256,
        |record| {
            streamed.extend_from_slice(record);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        streamed, reference,
        "external sort must equal the in-memory order"
    );
    assert_eq!(outcome.records, HUB_RECORDS);
    assert!(outcome.spill_count > 0, "{outcome:?}");
    assert!(outcome.spilled_rows > 0, "{outcome:?}");
    assert!(
        outcome.peak_reserved_bytes <= outcome.pool_limit_bytes,
        "{outcome:?}"
    );
    // Library scratch is removed when the runtime drops; nothing is left for
    // GraphForge recovery to find or mistake for authority.
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

#[test]
fn external_sort_refuses_when_the_pool_cannot_hold_one_batch() {
    let root = tempfile::TempDir::new().unwrap();
    let mut session = crate::graph_construction::tests::open(&root, 0x1507);
    let names = seal_hub_partition(&mut session);
    let scratch = tempfile::TempDir::new_in(root.path()).unwrap();
    let mut emitted = 0_u64;
    let error = external_sort_fixed_partition::<ENDPOINT_WIDTH>(
        &session.root,
        &names,
        HUB_RECORDS,
        1_024,
        scratch.path(),
        4_096,
        |_| {
            emitted += 1;
            Ok(())
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("Resources exhausted"), "{error}");
    assert_eq!(emitted, 0, "a refused sort emits no partial output");
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

#[test]
fn external_sort_rejects_a_count_that_disagrees_with_the_routed_count() {
    let root = tempfile::TempDir::new().unwrap();
    let mut session = crate::graph_construction::tests::open(&root, 0x1508);
    let names = seal_hub_partition(&mut session);
    let scratch = tempfile::TempDir::new_in(root.path()).unwrap();
    let error = external_sort_fixed_partition::<ENDPOINT_WIDTH>(
        &session.root,
        &names,
        HUB_RECORDS - 1,
        1 << 20,
        scratch.path(),
        256,
        |_| Ok(()),
    )
    .unwrap_err();
    assert!(error.to_string().contains("record count"), "{error}");
}

/// UUIDv7-shaped keys: one shared millisecond prefix, deterministic tails.
fn v7_records(count: u64) -> Vec<[u8; 16]> {
    let mut state = 0x1506_u64;
    (0..count)
        .map(|_| {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut mixed = state;
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            let mut key = [0_u8; 16];
            key[..6].copy_from_slice(&0x0199_0000_0000_u64.to_be_bytes()[2..]);
            key[8..].copy_from_slice(&(mixed ^ (mixed >> 31)).to_be_bytes());
            key
        })
        .collect()
}

#[test]
fn hash_partitions_lose_the_concatenation_order_that_recorded_range_splitters_keep() {
    let records = v7_records(4_096);
    let mut expected = records.clone();
    expected.sort_unstable();

    let (merged, outcome) = hash_repartition_then_merge(&records, 16, 512, true).unwrap();
    assert_eq!(merged, expected, "the global merge restores the order");
    assert_eq!(outcome.nonempty_partitions, 16);
    assert!(
        !outcome.concat_is_globally_sorted,
        "hash partitions need a global merge: {outcome:?}"
    );

    // The recorded range plan over the same keys: sorted partitions concatenate.
    let plan = PartitionPlan::from_sorted_sample(16, 16, &expected).unwrap();
    let mut partitions = vec![Vec::new(); plan.partitions()];
    for key in &records {
        partitions[plan.partition_of(key)].push(*key);
    }
    let concatenated = partitions
        .into_iter()
        .flat_map(|mut partition| {
            partition.sort_unstable();
            partition
        })
        .collect::<Vec<_>>();
    assert_eq!(concatenated, expected);

    // One hash partition trivially keeps the order: the loss is structural.
    let (_, single) = hash_repartition_then_merge(&records, 1, 512, true).unwrap();
    assert!(single.concat_is_globally_sorted);
}

#[test]
fn datafusion_sort_refuses_to_nest_inside_a_tokio_runtime() {
    let keys: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([[2_u8; 16], [1; 16]].iter()).unwrap());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let error = runtime
        .block_on(async { datafusion_sort_indices(Arc::clone(&keys)) })
        .unwrap_err();
    assert!(
        error.to_string().contains("nest a Tokio runtime"),
        "{error}"
    );
    assert_eq!(
        datafusion_sort_indices(keys).unwrap().values().to_vec(),
        vec![1, 0]
    );
}
