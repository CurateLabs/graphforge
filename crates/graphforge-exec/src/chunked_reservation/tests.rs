use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::execution::memory_pool::GreedyMemoryPool;

use super::*;

const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;

/// A greedy pool that counts the calls that reach it.
#[derive(Debug)]
struct CountingPool {
    inner: GreedyMemoryPool,
    calls: AtomicUsize,
}

impl CountingPool {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: GreedyMemoryPool::new(limit),
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl fmt::Display for CountingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingPool({})", self.inner)
    }
}

impl MemoryPool for CountingPool {
    fn name(&self) -> &str {
        "CountingPool"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

fn register(pool: &Arc<CountingPool>) -> ChunkedReservation {
    let dynamic: Arc<dyn MemoryPool> = pool.clone();
    ChunkedReservation::register("test consumer", &dynamic)
}

#[test]
fn chunk_scales_with_the_budget_between_its_bounds() {
    assert_eq!(chunk_for(&MemoryLimit::Finite(64 * KIB)), MIN_CHUNK_BYTES);
    assert_eq!(chunk_for(&MemoryLimit::Finite(64 * MIB)), 128 * KIB);
    assert_eq!(
        chunk_for(&MemoryLimit::Finite(64 * 1024 * MIB)),
        MAX_CHUNK_BYTES
    );
    assert_eq!(chunk_for(&MemoryLimit::Infinite), MAX_CHUNK_BYTES);
    assert_eq!(chunk_for(&MemoryLimit::Unknown), MAX_CHUNK_BYTES);
}

#[test]
fn many_small_charges_reach_the_pool_once_per_chunk() {
    let pool = CountingPool::new(usize::MAX / 2);
    let mut reservation = register(&pool);
    let charges = 100_000;
    for _ in 0..charges {
        reservation.try_grow(100).unwrap();
    }
    let bytes = charges * 100;
    assert_eq!(reservation.used, bytes);
    assert!(reservation.inner.size() >= bytes);
    assert!(reservation.inner.size() < bytes + MAX_CHUNK_BYTES);
    assert!(
        pool.calls() <= bytes.div_ceil(MAX_CHUNK_BYTES),
        "{} pool calls for {charges} charges of {bytes} bytes",
        pool.calls()
    );
}

#[test]
fn a_charge_and_release_of_the_same_bytes_never_reaches_the_pool() {
    let pool = CountingPool::new(usize::MAX / 2);
    let mut reservation = register(&pool);
    reservation.try_grow(10 * KIB).unwrap();
    let before = pool.calls();
    for _ in 0..10_000 {
        reservation.try_grow(96).unwrap();
        reservation.shrink(96);
    }
    assert_eq!(pool.calls(), before);
    assert_eq!(reservation.used, 10 * KIB);
}

#[test]
fn the_slack_never_exceeds_the_bound() {
    let pool = CountingPool::new(usize::MAX / 2);
    let mut reservation = register(&pool);
    let bound = MAX_SLACK_CHUNKS * reservation.chunk;
    // A deterministic mix of growing and shrinking sizes.
    let mut state = 0x9e37_79b9_u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        usize::try_from(state >> 40).unwrap()
    };
    for _ in 0..50_000 {
        let bytes = next() % (3 * KIB);
        if next() % 2 == 0 || reservation.used < bytes {
            reservation.try_grow(bytes).unwrap();
        } else {
            reservation.shrink(bytes);
        }
        let slack = reservation.inner.size() - reservation.used;
        assert!(slack <= bound, "slack {slack} exceeds {bound}");
        assert_eq!(pool.reserved(), reservation.inner.size());
    }
}

#[test]
fn a_fall_of_more_than_the_hysteresis_gives_the_surplus_back() {
    let pool = CountingPool::new(usize::MAX / 2);
    let mut reservation = register(&pool);
    let chunk = reservation.chunk;
    reservation.try_grow(10 * chunk).unwrap();
    reservation.shrink(10 * chunk);
    assert_eq!(reservation.used, 0);
    assert_eq!(
        reservation.inner.size(),
        chunk,
        "one chunk of slack remains"
    );
    assert_eq!(pool.reserved(), chunk);
    drop(reservation);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn the_budget_is_exact_when_a_chunk_does_not_fit() {
    // 100 KiB is not a multiple of the 16 KiB chunk: the last 4 KiB fit only
    // as an exact charge.
    let limit = 100 * KIB;
    let pool = CountingPool::new(limit);
    let mut reservation = register(&pool);
    let mut charged = 0;
    while reservation.try_grow(KIB).is_ok() {
        charged += KIB;
    }
    assert_eq!(charged, limit, "the whole budget is usable");
    assert_eq!(reservation.used, limit);
    assert_eq!(pool.reserved(), limit);
    let error = reservation.try_grow(1).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
    assert_eq!(reservation.used, limit, "a refused charge is not recorded");
    drop(reservation);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn a_nearly_spent_budget_asks_for_chunks_again_after_usage_falls() {
    let limit = 100 * KIB;
    let pool = CountingPool::new(limit);
    let mut reservation = register(&pool);
    let chunk = reservation.chunk;
    while reservation.try_grow(KIB).is_ok() {}
    assert!(reservation.tight_mark.is_some());

    reservation.shrink(2 * chunk);
    assert!(reservation.tight_mark.is_none());
    let before = pool.calls();
    for _ in 0..8 {
        reservation.try_grow(512).unwrap();
    }
    assert!(pool.calls() <= before + 1, "chunked again after the fall");
}

#[test]
fn partitions_share_one_budget() {
    let limit = 4 * MIB;
    let pool = CountingPool::new(limit);
    let mut partitions: Vec<ChunkedReservation> = (0..4).map(|_| register(&pool)).collect();
    let chunk = partitions[0].chunk;
    let mut charged = 0;
    'fill: loop {
        for partition in &mut partitions {
            if partition.try_grow(KIB).is_err() {
                break 'fill;
            }
            charged += KIB;
        }
    }
    // The exact usage fits the budget, and the refusal came no earlier than
    // the slack the other partitions hold can explain.
    assert!(charged <= limit);
    assert!(
        charged + partitions.len() * MAX_SLACK_CHUNKS * chunk >= limit,
        "refused at {charged} bytes of a {limit} byte budget"
    );
    drop(partitions);
    assert_eq!(pool.reserved(), 0);
}
