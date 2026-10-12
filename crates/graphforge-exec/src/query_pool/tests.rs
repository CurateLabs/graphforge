use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use super::*;

const MIB: usize = 1024 * 1024;

fn aggregate(pool: &Arc<dyn MemoryPool>, partition: usize) -> MemoryReservation {
    MemoryConsumer::new(format!(
        "{AGGREGATE_CONSUMER_PREFIX}[{partition}] (max(a.pad))"
    ))
    .with_can_spill(true)
    .register(pool)
}

fn query_pool(size: usize) -> Arc<dyn MemoryPool> {
    Arc::new(QueryMemoryPool::new(size))
}

/// The interleaving of merge group run 38245644578. Partition 1 holds the
/// pool except for the 8 MiB partition 0 has; partition 0 spills (releases its
/// state), partition 1 takes the released memory, and partition 0 reserves the
/// memory to sort the batch it emitted, a quarter more than it released.
fn spill_while_the_sibling_takes_the_released_memory(
    pool: &Arc<dyn MemoryPool>,
) -> Result<(MemoryReservation, MemoryReservation)> {
    let spilling = aggregate(pool, 0);
    let sibling = aggregate(pool, 1);
    sibling.try_grow(56 * MIB)?;
    spilling.try_grow(8 * MIB)?;
    spilling.shrink(8 * MIB);
    sibling.try_grow(8 * MIB)?;
    spilling.try_grow(10 * MIB)?;
    Ok((spilling, sibling))
}

#[test]
fn a_greedy_pool_lets_the_sibling_take_the_memory_a_spill_needs() {
    let greedy: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * MIB));
    let error = spill_while_the_sibling_takes_the_released_memory(&greedy)
        .unwrap_err()
        .to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
}

#[test]
fn a_spilling_aggregate_reserves_the_memory_it_released_whatever_the_sibling_took() {
    let pool = query_pool(64 * MIB);
    let _held = spill_while_the_sibling_takes_the_released_memory(&pool).unwrap();
    // The grant passed the budget by the sort reservation, and nothing else
    // fits until it is released.
    assert_eq!(pool.reserved(), 74 * MIB);
    let other = aggregate(&pool, 2);
    let error = other.try_grow(MIB).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
}

/// A spill makes small requests (emitting, resetting its state) between the
/// release and the sort; they are served from free memory and the grant waits
/// for the request that needs it.
#[test]
fn small_requests_after_the_release_leave_the_grant_for_the_sort() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    spilling.try_grow(8 * MIB).unwrap();
    spilling.shrink(8 * MIB);
    sibling.try_grow(64 * MIB).unwrap();
    spilling.grow(16);
    spilling.shrink(16);
    // Free memory is exhausted: nothing but the grant serves this.
    spilling.try_grow(10 * MIB).unwrap();
}

#[test]
fn the_grant_serves_one_request() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    spilling.try_grow(8 * MIB).unwrap();
    spilling.shrink(8 * MIB);
    sibling.try_grow(64 * MIB).unwrap();
    spilling.try_grow(8 * MIB).unwrap();
    assert!(spilling.try_grow(MIB).is_err());
}

/// What the aggregate of the remaining failure of the skewed test did, in the
/// order DataFusion does it: it emitted its state and released it down to what
/// the emptied hash table keeps (1.75 MiB), a sibling took the memory
/// released, the sort of the emitted batch was reserved (and so served from the
/// grant, the pool being full) and released when the batch was written, and
/// only then the restarted state was requested, 0.5 MiB for the first batch.
/// The pool is as full as it was before the sort reservation, so nothing but a
/// renewed grant serves the request, and there is no state left to spill when
/// it fails.
fn restart_after_the_sort_was_served_from_the_grant(
    pool: &Arc<dyn MemoryPool>,
) -> Result<(MemoryReservation, MemoryReservation)> {
    let spilling = aggregate(pool, 0);
    let sibling = aggregate(pool, 1);
    sibling.try_grow(56 * MIB)?;
    spilling.try_grow(8 * MIB)?;
    spilling.shrink(8 * MIB - 1792 * 1024);
    sibling.try_grow(64 * MIB - sibling.size() - spilling.size())?;
    assert_eq!(pool.reserved(), 64 * MIB);
    spilling.try_grow(1280 * 1024)?;
    spilling.shrink(1280 * 1024);
    spilling.try_grow(512 * 1024)?;
    Ok((spilling, sibling))
}

#[test]
fn the_restart_after_a_sort_served_from_the_grant_is_served_too() {
    let pool = query_pool(64 * MIB);
    let (spilling, _sibling) = restart_after_the_sort_was_served_from_the_grant(&pool).unwrap();
    assert_eq!(spilling.size(), 1792 * 1024 + 512 * 1024);
    // One loan at a time: a second request is refused until memory is released.
    let error = spilling.try_grow(MIB).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
}

/// The same release and restart on a pool where the sort fits in the free
/// memory and a repartition fills the pool before the restart; nothing but the
/// grant the release armed serves the restart.
#[test]
fn the_restart_after_a_sort_served_from_free_memory_is_served_too() {
    for greedy in [true, false] {
        let pool: Arc<dyn MemoryPool> = if greedy {
            Arc::new(GreedyMemoryPool::new(64 * MIB))
        } else {
            query_pool(64 * MIB)
        };
        let spilling = aggregate(&pool, 0);
        let repartition = MemoryConsumer::new("RepartitionExec[0]")
            .with_can_spill(true)
            .register(&pool);
        spilling.try_grow(8 * MIB).unwrap();
        spilling.shrink(8 * MIB - 1792 * 1024);
        spilling.try_grow(1280 * 1024).unwrap();
        spilling.shrink(1280 * 1024);
        repartition.try_grow(64 * MIB - spilling.size()).unwrap();
        let restart = spilling.try_grow(512 * 1024);
        assert_eq!(restart.is_ok(), !greedy, "greedy: {greedy}: {restart:?}");
    }
}

/// A loan is repaid by releasing what was lent, not by any release.
#[test]
fn a_loan_is_repaid_by_releasing_what_was_lent() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    spilling.try_grow(8 * MIB).unwrap();
    spilling.shrink(8 * MIB - 1792 * 1024);
    sibling
        .try_grow(64 * MIB - sibling.size() - spilling.size())
        .unwrap();
    spilling.try_grow(1024 * 1024).unwrap();
    spilling.shrink(512 * 1024);
    assert!(spilling.try_grow(64 * 1024).is_err());
    spilling.shrink(512 * 1024);
    // Repaid in full: the restart is served again.
    spilling.try_grow(64 * 1024).unwrap();
}

#[test]
fn the_grant_never_passes_the_size_of_the_pool() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    spilling.try_grow(8 * MIB).unwrap();
    spilling.shrink(8 * MIB);
    sibling.try_grow(64 * MIB).unwrap();
    assert!(spilling.try_grow(64 * MIB + 1).is_err());
}

#[test]
fn only_a_bulk_release_by_a_spilling_aggregate_grants_anything() {
    let pool = query_pool(64 * MIB);
    let join = MemoryConsumer::new("HashJoinInput[0]")
        .with_can_spill(false)
        .register(&pool);
    join.try_grow(16 * MIB).unwrap();
    join.shrink(16 * MIB);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(64 * MIB).unwrap();
    assert!(
        join.try_grow(MIB).is_err(),
        "a hash join build is not granted"
    );
    sibling.shrink(64 * MIB);

    let sorter = MemoryConsumer::new("ExternalSorter[0]")
        .with_can_spill(true)
        .register(&pool);
    sorter.try_grow(8 * MIB).unwrap();
    sorter.shrink(8 * MIB);
    sibling.try_grow(64 * MIB).unwrap();
    assert!(sorter.try_grow(MIB).is_err(), "a sort is not granted");
    sibling.shrink(64 * MIB);

    // A shrink that leaves most of the reservation in place is served from
    // free memory like any other request.
    let partial = aggregate(&pool, 3);
    partial.try_grow(8 * MIB).unwrap();
    partial.shrink(MIB);
    sibling.try_grow(57 * MIB).unwrap();
    assert!(partial.try_grow(MIB).is_err());
}

/// A consumer that restarts empty after a spill, and whose first batch is
/// larger than what it released, is served from the grant too.
#[test]
fn a_restarted_aggregate_is_not_starved_by_a_sibling_that_filled_the_pool() {
    let pool = query_pool(64 * MIB);
    let restarted = aggregate(&pool, 0);
    restarted.grow(64 * 1024);
    restarted.shrink(64 * 1024);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(64 * MIB).unwrap();
    restarted.try_grow(3 * MIB).unwrap();
}

#[test]
fn ordinary_growth_is_bounded_by_the_budget() {
    let pool = query_pool(64 * MIB);
    let one = aggregate(&pool, 0);
    one.try_grow(60 * MIB).unwrap();
    let error = one.try_grow(5 * MIB).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
    assert!(error.contains("4.0 MB remain available"), "{error}");
    one.grow(5 * MIB);
    assert_eq!(pool.reserved(), 65 * MIB);
}

#[test]
fn a_consumer_that_goes_away_takes_its_grant_with_it() {
    let pool = Arc::new(QueryMemoryPool::new(64 * MIB));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let spilling = aggregate(&dynamic, 0);
    spilling.try_grow(8 * MIB).unwrap();
    spilling.shrink(8 * MIB);
    assert_eq!(pool.locked().grants.len(), 1);
    drop(spilling);
    assert!(pool.locked().grants.is_empty());
    assert_eq!(pool.reserved(), 0);
}

/// Records the name of every spilling consumer registered with it.
#[derive(Debug)]
struct Spy {
    inner: QueryMemoryPool,
    spilling: Mutex<Vec<String>>,
}

impl std::fmt::Display for Spy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl MemoryPool for Spy {
    fn name(&self) -> &str {
        "spy"
    }
    fn register(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            self.spilling
                .lock()
                .unwrap()
                .push(consumer.name().to_owned());
        }
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

/// The grant finds the aggregates by the name DataFusion gives their memory
/// consumers; a rename there would silently switch it off.
#[tokio::test]
async fn datafusion_names_the_consumer_of_a_grouped_aggregate_as_the_pool_expects() {
    let spy = Arc::new(Spy {
        inner: QueryMemoryPool::new(256 * MIB),
        spilling: Mutex::new(Vec::new()),
    });
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&spy) as Arc<dyn MemoryPool>)
        .build_arc()
        .unwrap();
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    ctx.sql("SELECT k, max(pad) FROM t GROUP BY k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let names = spy.spilling.lock().unwrap().clone();
    assert!(
        names
            .iter()
            .any(|name| name.starts_with(AGGREGATE_CONSUMER_PREFIX)),
        "no spilling consumer named {AGGREGATE_CONSUMER_PREFIX}: {names:?}"
    );
}
