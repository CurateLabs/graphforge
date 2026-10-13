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

/// An exchange, the consumer DataFusion's repartition registers.
fn exchange(pool: &Arc<dyn MemoryPool>, partition: usize) -> MemoryReservation {
    MemoryConsumer::new(format!("RepartitionExec[{partition}]"))
        .with_can_spill(true)
        .register(pool)
}

/// An aggregate that has spilled and holds nothing: it released what it held in
/// a bulk shrink.
fn spilled(pool: &Arc<dyn MemoryPool>, partition: usize) -> MemoryReservation {
    let spilled = aggregate(pool, partition);
    spilled.try_grow(MIB).unwrap();
    spilled.shrink(MIB);
    spilled
}

fn is_refused(result: Result<()>) -> bool {
    result.is_err_and(|error| error.to_string().contains("Resources exhausted"))
}

/// The interleaving of merge group run 38245644578. Partition 1 holds the
/// pool except for the 8 MiB partition 0 has; partition 0 spills (releases its
/// state), partition 1 takes the released memory, and partition 0 reserves the
/// memory to sort the batch it emitted, a quarter more than it released.
#[test]
fn a_greedy_pool_lets_the_sibling_take_the_memory_a_spill_needs() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * MIB));
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(56 * MIB).unwrap();
    spilling.try_grow(8 * MIB).unwrap();
    spilling.shrink(8 * MIB);
    sibling.try_grow(8 * MIB).unwrap();
    assert!(is_refused(spilling.try_grow(10 * MIB)));
}

/// The same interleaving on the query pool: the release reserves the memory for
/// the aggregate that made it, so the sibling is refused the memory the sort
/// needs and the sort is served.
#[test]
fn a_spilled_aggregate_keeps_its_floor_whatever_the_sibling_asks() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(48 * MIB).unwrap();
    spilling.try_grow(8 * MIB).unwrap();
    spilling.shrink(8 * MIB);
    let error = sibling.try_grow(8 * MIB).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
    assert!(error.contains("6.0 MB remain available"), "{error}");
    sibling.try_grow(6 * MIB).unwrap();
    spilling.try_grow(10 * MIB).unwrap();
    assert_eq!(pool.reserved(), 64 * MIB);
}

/// A sort larger than the floor is lent whatever the sibling took, and the pool
/// passes its limit by the loan until it is released.
#[test]
fn a_spilling_aggregate_reserves_the_memory_it_released_whatever_the_sibling_took() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(34 * MIB).unwrap();
    spilling.try_grow(30 * MIB).unwrap();
    spilling.shrink(30 * MIB);
    sibling.try_grow(20 * MIB).unwrap();
    // The sort of the 30 MiB emitted is a quarter larger.
    spilling.try_grow(36 * MIB).unwrap();
    assert_eq!(pool.reserved(), 90 * MIB);
    let other = aggregate(&pool, 2);
    assert!(is_refused(other.try_grow(MIB)));
}

/// A spill makes small requests (emitting, resetting its state) between the
/// release and the sort; they are served from the floor and the grant waits for
/// the request that needs it.
#[test]
fn small_requests_after_the_release_leave_the_grant_for_the_sort() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    spilling.try_grow(20 * MIB).unwrap();
    spilling.shrink(20 * MIB);
    sibling.try_grow(54 * MIB).unwrap();
    spilling.try_grow(16).unwrap();
    spilling.shrink(16);
    // Free memory is exhausted and the floor is too small: nothing but the
    // grant serves this.
    spilling.try_grow(24 * MIB).unwrap();
}

#[test]
fn the_grant_serves_one_request() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    spilling.try_grow(20 * MIB).unwrap();
    spilling.shrink(20 * MIB);
    sibling.try_grow(54 * MIB).unwrap();
    spilling.try_grow(20 * MIB).unwrap();
    assert!(is_refused(spilling.try_grow(MIB)));
}

/// The remaining failure of the skewed test as it was before the floors: the
/// aggregate emitted its state and released it down to what the emptied hash
/// table keeps (1.75 MiB), a sibling took the memory released, the sort of the
/// emitted batch was reserved and released when the batch was written, and only
/// then the restarted state was requested. The sort and the restart are served
/// from the floor when they fit it, and from the grant when they do not (the
/// state is 30 MiB here, the restart 12 MiB).
#[test]
fn the_restart_after_a_sort_served_from_the_grant_is_served_too() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(34 * MIB).unwrap();
    spilling.try_grow(30 * MIB).unwrap();
    spilling.shrink(30 * MIB - 1792 * 1024);
    // The sibling takes what the floor leaves it.
    sibling.try_grow(20 * MIB).unwrap();
    assert!(is_refused(sibling.try_grow(MIB)));
    // The sort is served from the grant, and released when the batch is written.
    spilling.try_grow(36 * MIB).unwrap();
    spilling.shrink(36 * MIB);
    // The restart is larger than what is left of the floor: the release
    // renewed the grant.
    spilling.try_grow(12 * MIB).unwrap();
    assert_eq!(spilling.size(), 1792 * 1024 + 12 * MIB);
    // One loan at a time: a second request is refused until memory is released.
    assert!(is_refused(spilling.try_grow(MIB)));
}

/// The same release and restart on a pool where an exchange then fills the
/// pool; on the greedy pool the exchange takes everything and the restart is
/// refused, on the query pool the exchange is refused the floor and the restart
/// is served from it.
#[test]
fn the_restart_after_a_release_is_served_while_an_exchange_fills_the_pool() {
    for greedy in [true, false] {
        let pool: Arc<dyn MemoryPool> = if greedy {
            Arc::new(GreedyMemoryPool::new(64 * MIB))
        } else {
            query_pool(64 * MIB)
        };
        let spilling = aggregate(&pool, 0);
        let repartition = exchange(&pool, 0);
        spilling.try_grow(8 * MIB).unwrap();
        spilling.shrink(8 * MIB - 1792 * 1024);
        spilling.try_grow(1280 * 1024).unwrap();
        spilling.shrink(1280 * 1024);
        while repartition.try_grow(MIB).is_ok() {}
        let restart = spilling.try_grow(512 * 1024);
        assert_eq!(restart.is_ok(), !greedy, "greedy: {greedy}: {restart:?}");
    }
}

/// A loan is repaid by releasing what was lent, not by any release.
#[test]
fn a_loan_is_repaid_by_releasing_what_was_lent() {
    let pool = query_pool(64 * MIB);
    let spilling = spilled(&pool, 0);
    let sibling = aggregate(&pool, 1);
    spilling.try_grow(20 * MIB).unwrap();
    sibling.try_grow(44 * MIB).unwrap();
    spilling.try_grow(MIB).unwrap();
    spilling.shrink(512 * 1024);
    assert!(is_refused(spilling.try_grow(64 * 1024)));
    spilling.shrink(512 * 1024);
    // Repaid in full: the restart is served again.
    spilling.try_grow(64 * 1024).unwrap();
}

#[test]
fn the_grant_never_passes_the_size_of_the_pool() {
    let pool = query_pool(64 * MIB);
    let spilling = spilled(&pool, 0);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(54 * MIB).unwrap();
    assert!(is_refused(spilling.try_grow(64 * MIB + 1)));
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
/// larger than the floor, is served from the grant.
#[test]
fn a_restarted_aggregate_is_not_starved_by_a_sibling_that_filled_the_pool() {
    let pool = query_pool(64 * MIB);
    let restarted = aggregate(&pool, 0);
    restarted.grow(64 * 1024);
    restarted.shrink(64 * 1024);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(54 * MIB).unwrap();
    restarted.try_grow(12 * MIB).unwrap();
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

/// The merge of an aggregate's spill files with the pool full, in the order
/// DataFusion reserves its read buffers: two batches of the first stream, two of
/// the second (refused at the limit, so the merge retries with one batch per
/// stream). The exchange of a busy partition cannot take the floor, so every
/// request is served and the pool never passes its limit.
#[test]
fn the_merge_of_the_spill_files_is_served_while_an_exchange_fills_the_pool() {
    let pool = query_pool(64 * MIB);
    let busy = spilled(&pool, 0);
    let idle = spilled(&pool, 1);
    let repartition = exchange(&pool, 0);
    busy.try_grow(8 * MIB).unwrap();
    while repartition.try_grow(MIB).is_ok() {}
    let error = repartition.try_grow(MIB).unwrap_err().to_string();
    assert!(error.contains("0.0 B remain available"), "{error}");

    idle.try_grow(3_670_016).unwrap();
    idle.try_grow(974_400).unwrap();
    idle.shrink(3_670_016);
    idle.try_grow(1_835_008).unwrap();
    idle.try_grow(487_200).unwrap();
    assert!(pool.reserved() <= 64 * MIB, "{}", pool.reserved());
}

/// The floors are reserved while they are unfilled: an exchange or a sibling
/// that is not a spilled aggregate gets the pool less every floor, and an
/// aggregate that holds its floor reserves nothing more.
#[test]
fn others_are_refused_beyond_the_pool_less_the_unfilled_floors() {
    let pool = query_pool(64 * MIB);
    let _first = spilled(&pool, 0);
    let second = spilled(&pool, 1);
    let repartition = exchange(&pool, 0);
    repartition.try_grow(44 * MIB).unwrap();
    assert!(is_refused(repartition.try_grow(1)));
    // The floor of the second fills as it is used, and a request within it is
    // served although the pool is full.
    second.try_grow(10 * MIB).unwrap();
    assert!(is_refused(repartition.try_grow(1)));
    assert!(pool.reserved() <= 64 * MIB);

    // An aggregate that holds more than its floor reserves nothing more.
    let pool = query_pool(64 * MIB);
    let big = spilled(&pool, 0);
    big.try_grow(20 * MIB).unwrap();
    let repartition = exchange(&pool, 0);
    repartition.try_grow(44 * MIB).unwrap();
    assert!(is_refused(repartition.try_grow(1)));
}

/// A consumer that cannot spill is refused only at the limit of the pool. Its
/// refusal fails the query: the ungrouped aggregate that counts the groups asked
/// for a few bytes when the first of them arrived, with 2.6 MiB of the pool free
/// and 2.8 MiB of a spilled aggregate's floor unfilled, and was refused (the
/// failure of a hundred runs of the skewed aggregate test).
#[test]
fn a_consumer_that_cannot_spill_is_refused_only_at_the_limit_of_the_pool() {
    let pool = query_pool(64 * MIB);
    let busy = spilled(&pool, 0);
    busy.try_grow(28 * MIB).unwrap();
    let idle = spilled(&pool, 1);
    idle.try_grow(7 * MIB).unwrap();
    let repartition = exchange(&pool, 0);
    repartition.try_grow(26 * MIB).unwrap();
    assert_eq!(pool.reserved(), 61 * MIB);

    // The idle aggregate has 3 MiB of its floor to fill. The exchange, which
    // spills when it is refused, is kept from it.
    assert!(is_refused(repartition.try_grow(8)));

    let counting = MemoryConsumer::new("AggregateStream[1]").register(&pool);
    counting.try_grow(8).unwrap();
    counting.try_grow(3 * MIB - 8).unwrap();
    assert_eq!(pool.reserved(), 64 * MIB);
    assert!(is_refused(counting.try_grow(1)));

    // The aggregate that owns the floor still reserves it; the pool passes its
    // limit by what the other consumer took from it.
    idle.try_grow(3 * MIB).unwrap();
    assert_eq!(pool.reserved(), 67 * MIB);
}

/// A pool 10 MiB past its limit by the loan of a sort: the pool is full, a
/// spilled aggregate holds 20 MiB, another aggregate holds 34 MiB, and the
/// first one's sort of 20 MiB was lent although nothing was free.
struct LoanedPool {
    pool: Arc<QueryMemoryPool>,
    dynamic: Arc<dyn MemoryPool>,
    lending: MemoryReservation,
    other: MemoryReservation,
    sibling: MemoryReservation,
}

fn pool_past_its_limit_by_a_loan() -> LoanedPool {
    let pool = Arc::new(QueryMemoryPool::new(64 * MIB));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let lending = spilled(&dynamic, 0);
    let other = spilled(&dynamic, 1);
    other.try_grow(20 * MIB).unwrap();
    let sibling = aggregate(&dynamic, 2);
    sibling.try_grow(34 * MIB).unwrap();
    lending.try_grow(20 * MIB).unwrap();
    assert_eq!(pool.reserved(), 74 * MIB);
    assert_eq!(
        pool.locked().spillers[&lending.consumer().id()].grant,
        Grant::Lent(20 * MIB)
    );
    LoanedPool {
        pool,
        dynamic,
        lending,
        other,
        sibling,
    }
}

/// A request for no bytes is served whatever the pool holds, and uses no grant:
/// the ungrouped aggregate that counts the groups was refused one while a loan
/// had the pool past its limit (`used: 66.3 MB, pool_size: 64.0 MB`).
#[test]
fn a_request_for_no_bytes_is_always_served() {
    let LoanedPool {
        pool,
        dynamic,
        lending: _lending,
        other,
        sibling,
    } = pool_past_its_limit_by_a_loan();
    let counting = MemoryConsumer::new("AggregateStream[1]").register(&dynamic);
    counting.try_grow(0).unwrap();
    sibling.try_grow(0).unwrap();
    // The grant of an aggregate that could have been lent a request is left
    // for the request that needs it, not used up by one for no bytes.
    other.try_grow(0).unwrap();
    assert_eq!(
        pool.locked().spillers[&other.consumer().id()].grant,
        Grant::Armed
    );
    assert_eq!(pool.reserved(), 74 * MIB);
}

/// A consumer that cannot spill is not charged for the loans: they are bytes the
/// pool stopped counting and took back, repaid within a spill, and the consumer
/// fails its query on a refusal. It is still refused at the limit of the pool
/// without them.
#[test]
fn a_consumer_that_cannot_spill_is_not_charged_for_a_loan() {
    let LoanedPool {
        pool,
        dynamic,
        lending,
        other: _other,
        sibling,
    } = pool_past_its_limit_by_a_loan();
    let counting = MemoryConsumer::new("AggregateStream[1]").register(&dynamic);
    // The pool holds 54 MiB without the loan, so 10 MiB are free to it.
    counting.try_grow(10 * MIB).unwrap();
    let error = counting.try_grow(1).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
    assert!(error.contains("0.0 B remain available"), "{error}");
    // A consumer that can spill still sees the loan, and is refused.
    assert!(is_refused(sibling.try_grow(1)));
    assert_eq!(pool.reserved(), 84 * MIB);
    // Repaid, the loan is gone from the pool.
    lending.shrink(20 * MIB);
    assert_eq!(pool.reserved(), 64 * MIB);
}

/// The floors together never pass half the pool, however many aggregates have
/// spilled.
#[test]
fn the_floors_together_stay_within_half_the_pool() {
    for (spillers, others_get) in [(1, 54), (2, 44), (3, 34), (4, 32), (8, 32), (16, 32)] {
        let pool = query_pool(64 * MIB);
        let _spilled: Vec<_> = (0..spillers).map(|n| spilled(&pool, n)).collect();
        let repartition = exchange(&pool, 0);
        repartition.try_grow(others_get * MIB).unwrap();
        assert!(
            is_refused(repartition.try_grow(1)),
            "{spillers} spilled aggregates"
        );
    }
}

/// A request that keeps a spilled aggregate within its floor is served even by
/// a pool that was full when the aggregate spilled.
#[test]
fn a_floor_is_served_although_the_pool_was_full_when_the_aggregate_spilled() {
    let pool = query_pool(64 * MIB);
    let exchange = exchange(&pool, 0);
    let late = aggregate(&pool, 0);
    exchange.try_grow(60 * MIB).unwrap();
    late.try_grow(4 * MIB).unwrap();
    late.shrink(4 * MIB);
    // The exchange keeps what it has, is refused more, and the aggregate gets
    // its floor.
    assert!(is_refused(exchange.try_grow(MIB)));
    late.try_grow(10 * MIB).unwrap();
}

#[test]
fn a_consumer_that_goes_away_takes_its_floor_and_grant_with_it() {
    let pool = Arc::new(QueryMemoryPool::new(64 * MIB));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let spilling = aggregate(&dynamic, 0);
    spilling.try_grow(8 * MIB).unwrap();
    spilling.shrink(8 * MIB);
    assert_eq!(pool.locked().spillers.len(), 1);
    let repartition = exchange(&dynamic, 0);
    assert!(is_refused(repartition.try_grow(60 * MIB)));
    drop(spilling);
    assert!(pool.locked().spillers.is_empty());
    assert_eq!(pool.reserved(), 0);
    repartition.try_grow(60 * MIB).unwrap();
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
