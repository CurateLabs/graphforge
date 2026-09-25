use super::*;

/// The default DataFusion pool is `budget/4`, capped at 64 MiB and floored at
/// 16 KiB.  Setting it to `budget` (the prior default) causes DataFusion to
/// exhaust its own pool during the in-memory merge phase when the pool is large
/// and the partition spills — empirically proven on the 9M-star benchmark:
/// pool=256 MiB fails, pool=64 MiB publishes.
/// See `docs/development/evidence/partition-refusal-1584.md` and ADR 0047
/// ("Tested pool size" obligation).
#[test]
fn datafusion_pool_default_is_quarter_budget_capped_not_equal_budget() {
    // Production budget (256 MiB): default pool = 64 MiB (cap).
    // The old default (256 MiB = budget) is proven to fail: ExternalSorterMerge[0]
    // + ExternalSorter[0] exhaust the pool before the first output batch.
    assert_eq!(
        default_datafusion_pool_bytes(256 << 20),
        TESTED_POOL_MAX_BYTES
    );

    // Half-budget: pool = 32 MiB (budget/4).
    assert_eq!(default_datafusion_pool_bytes(128 << 20), 32 << 20);

    // Tiny-budget tests (e.g. 16 KiB): floor kicks in, pool equals the floor.
    assert_eq!(
        default_datafusion_pool_bytes(16 << 10),
        TESTED_POOL_MIN_BYTES
    );
    // With floor, budget/4 = 4 KiB < floor = 16 KiB.
    assert!(default_datafusion_pool_bytes(16 << 10) > (16u64 << 10) / 4);
}

#[test]
fn hybrid_routes_exactly_the_partitions_the_baseline_refuses() {
    // 100 fixed 48-byte records retain 4,800 bytes resident.
    for (limit, external) in [(4_800, false), (4_799, true)] {
        assert_eq!(
            selects_external::<48>(Mode::OnRefusal, Some(100), None, || Ok(4_800), limit).unwrap(),
            external
        );
        assert!(
            admit_materialization(resident_bytes::<48>(Some(100), None, 4_800), limit).is_err()
                == external
        );
    }
    // The baseline never reads the segment length.
    assert!(
        !selects_external::<48>(
            Mode::Baseline,
            Some(100),
            None,
            || panic!("baseline read segment bytes"),
            1
        )
        .unwrap()
    );
    assert!(
        selects_external::<48>(
            Mode::Always,
            Some(100),
            None,
            || panic!("always read segment bytes"),
            u64::MAX
        )
        .unwrap()
    );
    // Overflowing estimates are refusals, so the hybrid takes them.
    assert!(
        selects_external::<48>(Mode::OnRefusal, Some(u64::MAX), None, || Ok(0), u64::MAX).unwrap()
    );
    // A failure to read the segment length is an error, not a routing choice.
    assert!(
        selects_external::<48>(
            Mode::OnRefusal,
            Some(100),
            None,
            || Err(storage("unreadable")),
            1
        )
        .is_err()
    );
}

#[test]
fn multiset_guard_ignores_order_and_detects_changed_or_moved_records() {
    let records: [&[u8]; 3] = [b"alpha", b"bravo", b"charlie"];
    let mut forward = Multiset::default();
    let mut reverse = Multiset::default();
    for record in records {
        forward.add(record);
    }
    for record in records.iter().rev() {
        reverse.add(record);
    }
    assert_eq!(forward, reverse);
    let mut flipped = Multiset::default();
    for record in [&b"alpha"[..], b"bravx", b"charlie"] {
        flipped.add(record);
    }
    assert_ne!(forward, flipped);
    let mut duplicated = Multiset::default();
    for record in [&b"alpha"[..], b"alpha", b"charlie"] {
        duplicated.add(record);
    }
    assert_ne!(forward, duplicated);
    let mut dropped = Multiset::default();
    for record in &records[..2] {
        dropped.add(record);
    }
    assert_ne!(forward, dropped);
}

#[test]
fn batches_shrink_with_the_pool_within_bounds() {
    assert_eq!(batch_records::<48>(16 << 10), MIN_BATCH_RECORDS);
    assert_eq!(batch_records::<48>(8 * 48 * 1_000), 1_000);
    assert_eq!(batch_records::<48>(256 << 20), MAX_BATCH_RECORDS);
    assert_eq!(batch_records::<48>(0), MIN_BATCH_RECORDS);
}
