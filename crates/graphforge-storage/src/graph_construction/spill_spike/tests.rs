use super::*;

#[test]
fn hybrid_routes_exactly_the_partitions_the_baseline_refuses() {
    // 100 fixed 48-byte records retain 4,800 bytes resident.
    for (limit, external) in [(4_800, false), (4_799, true)] {
        assert_eq!(
            selects_external::<48>(Mode::OnRefusal, Some(100), None, 4_800, limit),
            external
        );
        assert!(
            admit_materialization(resident_bytes::<48>(Some(100), None, 4_800), limit).is_err()
                == external
        );
    }
    assert!(!selects_external::<48>(
        Mode::Baseline,
        Some(100),
        None,
        4_800,
        1
    ));
    assert!(selects_external::<48>(
        Mode::Always,
        Some(100),
        None,
        4_800,
        u64::MAX
    ));
    // Overflowing estimates are refusals, so the hybrid takes them.
    assert!(selects_external::<48>(
        Mode::OnRefusal,
        Some(u64::MAX),
        None,
        0,
        u64::MAX
    ));
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
