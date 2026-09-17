use super::*;

/// A UUIDv7-shaped key: 48 bits of Unix-millisecond timestamp, then entropy.
/// Every key minted inside one ingest shares a near-identical high prefix,
/// which is exactly the distribution a high-bit formula cannot partition.
fn v7_like(millis: u64, entropy: u64) -> [u8; 16] {
    let mut key = [0_u8; 16];
    key[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
    key[6] = 0x70;
    key[7] = (entropy >> 56) as u8;
    key[8] = 0x80 | ((entropy >> 50) as u8 & 0x3f);
    key[9..].copy_from_slice(&(entropy.wrapping_mul(0x9e37_79b9_7f4a_7c15)).to_be_bytes()[1..]);
    key
}

/// One ingest's worth of time-ordered identities, sorted as the staged runs are.
fn ingest_keys(count: u64) -> Vec<[u8; 16]> {
    let base_millis = 1_800_000_000_000_u64;
    let mut keys = (0..count)
        .map(|index| {
            v7_like(
                base_millis + index / 4_096,
                index.wrapping_mul(0x2545_f491_4f6c_dd1d),
            )
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// The formula the design forbids: split the 128-bit key space evenly on its
/// high bits. It is a valid *range* partition, so it preserves global order and
/// every determinism test still passes — it just routes an entire UUIDv7 ingest
/// into one partition.
fn formula_splitters(partition_count: u32) -> Vec<[u8; 16]> {
    (1..u128::from(partition_count))
        .map(|index| {
            let boundary = index
                .wrapping_mul(u128::MAX / u128::from(partition_count))
                .to_be_bytes();
            boundary
        })
        .collect()
}

fn plan_for(keys: &[[u8; 16]], partition_count: u32) -> PartitionPlan {
    let mut sampler = IdentitySampler::new(partition_count, keys.len() as u64).unwrap();
    assert!(sampler.cut() <= partition_count);
    let positions = sampler.positions().collect::<Vec<_>>();
    for position in positions {
        sampler
            .admit(keys[usize::try_from(position).unwrap()])
            .unwrap();
    }
    let plan = sampler.into_plan(partition_count).unwrap();
    assert!(plan.partitions() as u64 <= u64::from(partition_count));
    plan
}

fn balance_for(keys: &[[u8; 16]], plan: &PartitionPlan) -> PartitionBalance {
    let mut balance = PartitionBalance::new(plan.partitions());
    for key in keys {
        balance.record(plan.partition_of(key)).unwrap();
    }
    balance
}

#[test]
fn partition_assignment_is_monotone_so_concatenation_is_globally_sorted() {
    let keys = ingest_keys(40_000);
    let plan = plan_for(&keys, 64);
    assert!(plan.partitions() > 1, "{}", plan.partitions());
    let mut previous = 0;
    for key in &keys {
        let partition = plan.partition_of(key);
        assert!(
            partition >= previous,
            "partition {partition} followed {previous}"
        );
        assert!(partition < plan.partitions());
        previous = partition;
    }
}

#[test]
fn sampled_splitters_balance_a_uuid_v7_ingest() {
    for partition_count in [2_u32, 16, 64, 256] {
        let keys = ingest_keys(80_000);
        let plan = plan_for(&keys, partition_count);
        let balance = balance_for(&keys, &plan);
        assert_eq!(balance.total(), keys.len() as u64);
        balance
            .assert_balanced("sampled")
            .unwrap_or_else(|error| panic!("P={partition_count}: {error}"));
        // Sampled quantiles should land far inside the tolerance, not just
        // scrape past it.
        let mean = balance.total() / balance.rows().len() as u64;
        assert!(
            balance.max_rows() <= mean * 2,
            "P={partition_count} max={} mean={mean}",
            balance.max_rows()
        );
    }
}

#[test]
fn balance_assertion_refuses_formula_splitters_over_a_uuid_v7_ingest() {
    let keys = ingest_keys(80_000);
    let plan = PartitionPlan::from_recorded(256, formula_splitters(256)).unwrap();
    let balance = balance_for(&keys, &plan);
    assert_eq!(balance.total(), keys.len() as u64);
    // The collapse this guards against: essentially every row in one partition.
    assert_eq!(
        balance.max_rows(),
        keys.len() as u64,
        "formula splitters were expected to collapse a v7 ingest"
    );
    let error = balance
        .assert_balanced("formula")
        .expect_err("skewed partitioning must be refused");
    assert!(
        error.to_string().contains("skewed"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn balance_assertion_refuses_a_single_hot_partition() {
    let mut balance = PartitionBalance::new(8);
    for _ in 0..1_000 {
        balance.record(3).unwrap();
    }
    for partition in 0..8 {
        for _ in 0..16 {
            balance.record(partition).unwrap();
        }
    }
    balance
        .assert_balanced("hot")
        .expect_err("a single hot partition must be refused");
}

#[test]
fn balance_assertion_is_quiet_below_the_meaningful_row_floor() {
    let mut balance = PartitionBalance::new(256);
    for partition in 0..3 {
        balance.record(partition).unwrap();
    }
    balance.assert_balanced("tiny").unwrap();
}

#[test]
fn splitters_round_trip_through_the_recorded_form() {
    let keys = ingest_keys(40_000);
    let plan = plan_for(&keys, 128);
    let recorded =
        PartitionPlan::from_recorded(plan.partition_count(), plan.splitters().to_vec()).unwrap();
    assert_eq!(plan, recorded);
    for key in keys.iter().step_by(97) {
        assert_eq!(plan.partition_of(key), recorded.partition_of(key));
    }
}

#[test]
fn sampling_is_a_pure_function_of_the_input_and_the_partition_count() {
    let keys = ingest_keys(40_000);
    let first = plan_for(&keys, 64);
    let second = plan_for(&keys, 64);
    assert_eq!(first, second);
}

#[test]
fn recorded_splitters_must_be_strictly_increasing_and_bounded() {
    let repeated = vec![[1_u8; 16], [1_u8; 16]];
    PartitionPlan::from_recorded(8, repeated).expect_err("repeated splitters must be refused");
    let descending = vec![[2_u8; 16], [1_u8; 16]];
    PartitionPlan::from_recorded(8, descending).expect_err("descending splitters must be refused");
    let overfull = vec![[1_u8; 16], [2_u8; 16], [3_u8; 16]];
    PartitionPlan::from_recorded(2, overfull).expect_err("overfull splitters must be refused");
    PartitionPlan::from_recorded(0, Vec::new()).expect_err("zero partitions must be refused");
    PartitionPlan::from_recorded(MAX_PARTITION_COUNT + 1, Vec::new())
        .expect_err("oversized partition counts must be refused");
}

/// The cut is bounded so that a small graph does not pay a large graph's
/// durability price: each partition costs a durable spill in every family.
#[test]
fn the_cut_is_bounded_by_the_recorded_record_count() {
    for (records, partition_count, expected) in [
        (0_u64, 256_u32, 1_u32),
        (15, 256, 1),
        (16, 256, 1),
        (32, 256, 2),
        (1_953, 256, 122),
        (5_025, 256, 256),
        (67_108_864, 256, 256),
        (67_108_864, 1, 1),
    ] {
        let sampler = IdentitySampler::new(partition_count, records).unwrap();
        assert_eq!(sampler.cut(), expected, "records={records}");
    }
}

/// #1439: past `DEFAULT_PARTITION_COUNT * 16` records the flat ceiling used
/// to be the only bound, so partition *count* plateaued while partition
/// *size* (and therefore resident memory) kept growing with data. Requesting
/// the full `MAX_PARTITION_COUNT` ceiling -- production's new default --
/// exercises the data-driven scaling term and must reproduce exactly the
/// S18/S19/S20/S22 ladder-rung table from the issue: partition count doubles
/// as record count doubles, holding rows-per-partition at
/// `TARGET_ROWS_PER_PARTITION` (16,384) until the ceiling itself binds.
#[test]
fn the_cut_scales_with_data_once_past_the_flat_default_instead_of_plateauing() {
    for (records, expected_partitions) in [
        (4_194_304_u64, 256_u32), // S18: unchanged from the pre-#1439 default.
        (8_388_608, 512),        // S19: partitions double as records double.
        (16_777_216, 1_024),     // S20: partitions double again.
        (67_108_864, 4_096),     // S22: exactly MAX_PARTITION_COUNT.
    ] {
        let sampler = IdentitySampler::new(MAX_PARTITION_COUNT, records).unwrap();
        assert_eq!(sampler.cut(), expected_partitions, "records={records}");
        assert_eq!(
            records / u64::from(expected_partitions),
            16_384,
            "rows-per-partition must hold constant while the target is binding: records={records}"
        );
    }
    // Beyond S22's record count the ceiling itself binds: rows-per-partition
    // now grows with data again, but only past the point R1's cross-host
    // determinism proof and the durability trade in #1439 accepted.
    let far_beyond = IdentitySampler::new(MAX_PARTITION_COUNT, 1_000_000_000).unwrap();
    assert_eq!(far_beyond.cut(), MAX_PARTITION_COUNT);
}

#[test]
fn fewer_distinct_keys_than_partitions_collapse_rather_than_emptying_partitions() {
    let keys = ingest_keys(5);
    let plan = plan_for(&keys, 256);
    assert!(plan.partitions() <= keys.len(), "{}", plan.partitions());
    let balance = balance_for(&keys, &plan);
    assert!(
        balance.rows().iter().all(|rows| *rows > 0),
        "{:?}",
        balance.rows()
    );
    assert_eq!(balance.total(), keys.len() as u64);
}

#[test]
fn an_empty_identity_domain_yields_the_single_partition_plan() {
    let plan = plan_for(&[], 256);
    assert_eq!(plan.partitions(), 1);
    assert_eq!(plan.partition_of(&[0_u8; 16]), 0);
    assert_eq!(plan.partition_count(), 256);
}

#[test]
fn every_partition_is_reachable_for_a_well_sampled_ingest() {
    let keys = ingest_keys(80_000);
    let plan = plan_for(&keys, 64);
    let balance = balance_for(&keys, &plan);
    assert_eq!(balance.rows().len(), plan.partitions());
    assert!(
        balance.rows().iter().all(|rows| *rows > 0),
        "{:?}",
        balance.rows()
    );
}
