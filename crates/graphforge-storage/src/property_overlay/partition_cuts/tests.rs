use super::*;

fn key(value: u128) -> [u8; 16] {
    value.to_be_bytes()
}

fn segment(low: u128, high: u128, rows: u64) -> KeySegment {
    KeySegment { low, high, rows }
}

/// The interval each of `partitions` partitions covers, as `(low, high)` with
/// `None` for unbounded.
fn intervals(cuts: &[[u8; 16]], partitions: usize) -> Vec<(u128, Option<u128>)> {
    (0..partitions)
        .map(|index| {
            let (low, high) = partition_interval(cuts, index);
            (u128::from_be_bytes(low), high.map(u128::from_be_bytes))
        })
        .collect()
}

fn assert_valid(cuts: &[[u8; 16]]) {
    let values = cuts
        .iter()
        .copied()
        .map(u128::from_be_bytes)
        .collect::<Vec<_>>();
    assert!(
        values.iter().all(|value| *value > 0),
        "a zero cut is an empty first interval"
    );
    assert!(
        values.windows(2).all(|pair| pair[0] < pair[1]),
        "cuts must be strictly ascending: {values:?}"
    );
}

#[test]
fn disjoint_uniform_slices_split_into_equal_row_shares() {
    // Four fragments of 1000 rows over adjacent 1000-key spans.
    let segments = (0..4)
        .map(|slice| segment(slice * 1000, slice * 1000 + 999, 1000))
        .collect::<Vec<_>>();
    let cuts = balanced_cuts(&segments, 4);
    assert_valid(&cuts);
    let values = cuts
        .iter()
        .copied()
        .map(u128::from_be_bytes)
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1000, 2000, 3000]);
}

#[test]
fn spanning_fragments_split_the_shared_span_evenly() {
    // A base and a mutation fragment over the same span add up to uniform
    // density, so the quartiles of the span are the cuts.
    let segments = [segment(0, 3999, 4000), segment(0, 3999, 400)];
    let cuts = balanced_cuts(&segments, 4);
    assert_valid(&cuts);
    let values = cuts
        .iter()
        .copied()
        .map(u128::from_be_bytes)
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1000, 2000, 3000]);
}

#[test]
fn a_dense_segment_attracts_the_cuts() {
    // 9,000 of 10,000 rows sit in the first tenth of the key space.
    let segments = [segment(0, 999, 9000), segment(1000, 9999, 1000)];
    let cuts = balanced_cuts(&segments, 4);
    assert_valid(&cuts);
    assert_eq!(cuts.len(), 3);
    assert!(
        cuts.iter().all(|cut| u128::from_be_bytes(*cut) < 1000),
        "every quartile of the rows lies in the dense tenth: {cuts:?}"
    );
}

#[test]
fn a_route_that_does_not_separate_yields_fewer_cuts() {
    // One key cannot be split: the first cut falls at that key, and every
    // later share lands on the same key, so the duplicates collapse.
    let single_key = [segment(7, 7, 100)];
    let cuts = balanced_cuts(&single_key, 4);
    assert_valid(&cuts);
    assert!(cuts.len() <= 1, "{cuts:?}");

    assert!(balanced_cuts(&[], 4).is_empty(), "no segments, no cuts");
    assert!(
        balanced_cuts(&[segment(0, 99, 0)], 4).is_empty(),
        "no rows, no cuts"
    );
    assert!(
        balanced_cuts(&[segment(0, 99, 50)], 1).is_empty(),
        "one partition has nothing to cut"
    );
}

#[test]
fn cuts_stay_ascending_for_arbitrary_overlap() {
    let segments = [
        segment(5, 40, 33),
        segment(0, u128::MAX, 90),
        segment(30, 31, 500),
        segment(u128::MAX - 10, u128::MAX, 7),
    ];
    for partitions in 2..=16 {
        let cuts = balanced_cuts(&segments, partitions);
        assert_valid(&cuts);
        assert!(cuts.len() < partitions);
    }
}

#[test]
fn partition_intervals_tile_the_key_space() {
    let cuts = [key(10), key(20), key(30)];
    assert_eq!(
        intervals(&cuts, 4),
        vec![(0, Some(10)), (10, Some(20)), (20, Some(30)), (30, None),]
    );
}

#[test]
fn partition_intervals_without_cuts_are_one_unbounded_interval() {
    assert_eq!(intervals(&[], 1), vec![(0, None)]);
}

#[test]
fn partitions_past_the_last_cut_are_empty() {
    let cuts = [key(10)];
    let (low, high) = partition_interval(&cuts, 2);
    assert_eq!(Some(low), high, "an empty half-open interval");
    let (low, high) = partition_interval(&cuts, 9);
    assert_eq!(Some(low), high);
    // The last real partition is still unbounded.
    assert_eq!(partition_interval(&cuts, 1), (key(10), None));
}
