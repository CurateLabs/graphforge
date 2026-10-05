use std::num::NonZeroUsize;
use std::sync::Arc;

use super::{MIN_PROBES_PER_LANE, NodeIndex, NodeIndexBuilder};
use crate::graph_construction::cpu_admission::ConstructionCpuAdmission;

fn uuid(value: u64) -> [u8; 16] {
    let mut uuid = [0_u8; 16];
    uuid[..8].copy_from_slice(&value.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
    uuid[8..].copy_from_slice(&value.to_be_bytes());
    uuid
}

/// `count` nodes with sorted pseudo-random UUIDs whose surrogates follow `base`.
fn index(count: u64, base: u64) -> (NodeIndex, Vec<[u8; 16]>) {
    let mut uuids: Vec<_> = (0..count).map(uuid).collect();
    uuids.sort_unstable();
    let mut builder = NodeIndexBuilder::default();
    for (rank, uuid) in uuids.iter().enumerate() {
        builder.push(*uuid, base + rank as u64 + 1).unwrap();
    }
    (builder.finish(), uuids)
}

#[test]
fn resolves_each_uuid_to_its_dense_rank_after_the_base() {
    let (index, uuids) = index(1_000, 41);
    let probes = [uuids[0], uuids[999], uuids[500]];
    let mut surrogates = [0; 3];
    index.resolve(&probes, &mut surrogates, None).unwrap();
    assert_eq!(surrogates, [42, 1_041, 542]);
}

#[test]
fn refuses_an_endpoint_that_is_not_a_new_node() {
    let (index, _) = index(100, 0);
    let mut surrogates = [0; 1];
    let error = index
        .resolve(&[uuid(100)], &mut surrogates, None)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("endpoint UUID lacks node surrogate")
    );
}

#[test]
fn refuses_surrogates_that_are_not_dense_ranks() {
    let mut gap = NodeIndexBuilder::default();
    gap.push([1; 16], 1).unwrap();
    assert!(gap.push([2; 16], 3).is_err());

    let mut unordered = NodeIndexBuilder::default();
    unordered.push([9; 16], 1).unwrap();
    assert!(unordered.push([1; 16], 2).is_err());

    let mut zero = NodeIndexBuilder::default();
    assert!(zero.push([1; 16], 0).is_err());
}

#[test]
fn laned_resolution_matches_serial_resolution() {
    let (index, uuids) = index(50_000, 7);
    // Enough probes for several lanes, in an order unrelated to the index.
    let probes: Vec<_> = (0..MIN_PROBES_PER_LANE * 12)
        .map(|position| uuids[(position * 7_919) % uuids.len()])
        .collect();
    let mut serial = vec![0; probes.len()];
    index.resolve(&probes, &mut serial, None).unwrap();
    let admission = Arc::new(ConstructionCpuAdmission::new(NonZeroUsize::new(6).unwrap()));
    let mut laned = vec![0; probes.len()];
    index
        .resolve(&probes, &mut laned, Some(&admission))
        .unwrap();
    assert_eq!(laned, serial);
    assert!(admission.peak() > 1, "the probes ran on more than one lane");
    assert_eq!(admission.in_use(), 0, "the probe lease is released");
}

#[test]
fn a_missing_endpoint_on_any_lane_is_refused() {
    let (index, uuids) = index(10_000, 0);
    let mut probes: Vec<_> = (0..MIN_PROBES_PER_LANE * 4)
        .map(|position| uuids[position % uuids.len()])
        .collect();
    let last = probes.len() - 1;
    probes[last] = [0xFF; 16];
    let admission = Arc::new(ConstructionCpuAdmission::new(NonZeroUsize::new(4).unwrap()));
    let mut surrogates = vec![0; probes.len()];
    assert!(
        index
            .resolve(&probes, &mut surrogates, Some(&admission))
            .is_err()
    );
}
