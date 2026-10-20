use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use super::*;

const MIB: usize = 1024 * 1024;

fn aggregate(pool: &Arc<dyn MemoryPool>, partition: usize) -> MemoryReservation {
    MemoryConsumer::new(format!(
        "{AGGREGATE_CONSUMER_PREFIX}{partition}] (max(a.pad))"
    ))
    .with_can_spill(true)
    .register(pool)
}

fn query_pool(size: usize) -> Arc<dyn MemoryPool> {
    Arc::new(QueryMemoryPool::new(size))
}

/// An exchange, the consumer DataFusion's repartition registers.
pub(super) fn exchange(pool: &Arc<dyn MemoryPool>, partition: usize) -> MemoryReservation {
    MemoryConsumer::new(format!("RepartitionExec[{partition}]"))
        .with_can_spill(true)
        .register(pool)
}

/// An aggregate that has spilled and holds nothing: it released what it held in
/// a bulk shrink.
pub(super) fn spilled(pool: &Arc<dyn MemoryPool>, partition: usize) -> MemoryReservation {
    let spilled = aggregate(pool, partition);
    spilled.try_grow(MIB).unwrap();
    spilled.shrink(MIB);
    spilled
}

/// A consumer that cannot spill and reads back what was spilled: the replay of
/// a batch an exchange spilled, or the encoder of a spill.
fn reader(pool: &Arc<dyn MemoryPool>) -> MemoryReservation {
    MemoryConsumer::new("SpillReaderDecode").register(pool)
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
/// needs and the sort is served. Every consumer that can spill sees the pool
/// less the last floor (10 MiB here), which only the readers of spilled data
/// may take, so the sibling holds 10 MiB less than on the greedy pool.
#[test]
fn a_spilled_aggregate_keeps_its_floor_whatever_the_sibling_asks() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(38 * MIB).unwrap();
    spilling.try_grow(8 * MIB).unwrap();
    spilling.shrink(8 * MIB);
    let error = sibling.try_grow(8 * MIB).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
    assert!(error.contains("6.0 MB remain available"), "{error}");
    sibling.try_grow(6 * MIB).unwrap();
    spilling.try_grow(10 * MIB).unwrap();
    assert_eq!(pool.reserved(), 54 * MIB);
    let reader = reader(&pool);
    reader.try_grow(10 * MIB).unwrap();
    assert_eq!(pool.reserved(), 64 * MIB);
}

/// A sort larger than the floor is lent whatever the sibling took, and the pool
/// passes its limit by the loan until it is released.
#[test]
fn a_spilling_aggregate_reserves_the_memory_it_released_whatever_the_sibling_took() {
    let pool = query_pool(64 * MIB);
    let spilling = aggregate(&pool, 0);
    let sibling = aggregate(&pool, 1);
    sibling.try_grow(24 * MIB).unwrap();
    spilling.try_grow(30 * MIB).unwrap();
    spilling.shrink(30 * MIB);
    sibling.try_grow(20 * MIB).unwrap();
    // The sort of the 30 MiB emitted is a quarter larger.
    spilling.try_grow(36 * MIB).unwrap();
    assert_eq!(pool.reserved(), 80 * MIB);
    // An aggregate that has not spilled and holds nothing sees the pool less
    // the loan, as a consumer that cannot spill does: its first batch is
    // admitted; from then on the floors keep it short.
    let other = aggregate(&pool, 2);
    other.try_grow(MIB).unwrap();
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
    sibling.try_grow(44 * MIB).unwrap();
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
    sibling.try_grow(44 * MIB).unwrap();
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
    sibling.try_grow(24 * MIB).unwrap();
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
    sibling.try_grow(34 * MIB).unwrap();
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
    sibling.try_grow(44 * MIB).unwrap();
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
    // Consumers that can spill are admitted 10 MiB short of the pool, so the
    // sibling fills it by unconditional growth.
    sibling.grow(64 * MIB);
    assert!(
        join.try_grow(MIB).is_err(),
        "a hash join build is not granted"
    );
    sibling.shrink(64 * MIB);

    let sorter = MemoryConsumer::new("ExternalSorter[0]")
        .with_can_spill(true)
        .register(&pool);
    // The sibling's earlier bulk release established its 10 MiB floor.
    // This ordinary request fits alongside that floor and producer headroom.
    sorter.try_grow(4 * MIB).unwrap();
    sorter.shrink(4 * MIB);
    sibling.grow(64 * MIB);
    assert!(sorter.try_grow(MIB).is_err(), "a sort is not granted");
    sibling.shrink(64 * MIB);

    // A shrink that leaves most of the reservation in place is served from
    // free memory like any other request.
    let partial = aggregate(&pool, 3);
    partial.try_grow(8 * MIB).unwrap();
    partial.shrink(MIB);
    sibling.try_grow(47 * MIB).unwrap();
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
    sibling.try_grow(44 * MIB).unwrap();
    restarted.try_grow(12 * MIB).unwrap();
}

#[test]
fn ordinary_growth_is_bounded_by_the_budget() {
    let pool = query_pool(64 * MIB);
    let one = aggregate(&pool, 0);
    one.try_grow(50 * MIB).unwrap();
    let error = one.try_grow(5 * MIB).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
    // The pool less the 10 MiB reserved for the readers of spilled data.
    assert!(error.contains("4.0 MB remain available"), "{error}");
    one.grow(15 * MIB);
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
/// that is not a spilled aggregate gets the pool less every floor and less the
/// last floor kept for the readers of spilled data, and an aggregate that holds
/// its floor reserves nothing more.
#[test]
fn others_are_refused_beyond_the_pool_less_the_unfilled_floors() {
    let pool = query_pool(64 * MIB);
    let _first = spilled(&pool, 0);
    let second = spilled(&pool, 1);
    let repartition = exchange(&pool, 0);
    repartition.try_grow(34 * MIB).unwrap();
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
    repartition.try_grow(34 * MIB).unwrap();
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
    repartition.try_grow(16 * MIB).unwrap();
    assert_eq!(pool.reserved(), 51 * MIB);

    // The idle aggregate has 3 MiB of its floor to fill, and 10 MiB are kept
    // for the readers of spilled data. The exchange, which spills when it is
    // refused, is kept from both.
    assert!(is_refused(repartition.try_grow(8)));

    // The counting aggregate, which cannot spill, is served up to the pool.
    let counting = MemoryConsumer::new("AggregateStream[1]").register(&pool);
    counting.try_grow(8).unwrap();
    counting.try_grow(13 * MIB - 8).unwrap();
    assert_eq!(pool.reserved(), 64 * MIB);
    assert!(is_refused(counting.try_grow(1)));

    // The aggregate that owns the floor still reserves it; the pool passes its
    // limit by what the other consumer took from it.
    idle.try_grow(3 * MIB).unwrap();
    assert_eq!(pool.reserved(), 67 * MIB);
}

/// A sort is kept three quarters of the pool short of the limit, so the consumer that
/// cannot spill and grows with the data (the path state of a variable-length
/// expansion, #1985) is served from the memory the sorts leave: a sort buffers
/// until it is refused, and without the headroom it holds the pool.
#[test]
fn a_sort_is_kept_short_of_the_memory_a_consumer_that_cannot_spill_may_take() {
    let pool = query_pool(64 * MIB);
    let sort = |partition: usize| {
        MemoryConsumer::new(format!("ExternalSorter[{partition}]"))
            .with_can_spill(true)
            .register(&pool)
    };
    let first = sort(0);
    let second = sort(1);
    first.try_grow(10 * MIB).unwrap();
    second.try_grow(6 * MIB).unwrap();
    assert_eq!(pool.reserved(), 16 * MIB);

    // Three quarters of the pool is left to the consumers that cannot spill, so the
    // sorts are refused and spill.
    assert!(is_refused(first.try_grow(MIB)));
    assert!(is_refused(second.try_grow(MIB)));

    // The path state of an expansion is served up to the limit of the pool.
    let path_state = MemoryConsumer::new("VarLenExpand path state").register(&pool);
    path_state.try_grow(48 * MIB).unwrap();
    assert_eq!(pool.reserved(), 64 * MIB);
    assert!(is_refused(path_state.try_grow(1)));

    // The merge reservation of a sort cannot spill and is not held back: the
    // headroom is for exactly such a consumer.
    first.shrink(10 * MIB);
    second.shrink(6 * MIB);
    let merge = MemoryConsumer::new("ExternalSorterMerge[0]").register(&pool);
    merge.try_grow(10 * MIB).unwrap();
    assert_eq!(pool.reserved(), 58 * MIB);
}

/// The headroom is a sort's alone: an exchange and an aggregate that has not
/// spilled fill the pool less the last floor, as they did before the headroom.
#[test]
fn the_headroom_of_the_sorts_leaves_the_other_consumers_that_spill_unbounded() {
    let pool = query_pool(64 * MIB);
    let repartition = exchange(&pool, 0);
    repartition.try_grow(54 * MIB).unwrap();
    assert_eq!(pool.reserved(), 54 * MIB);
    repartition.shrink(54 * MIB);

    let grouped = aggregate(&pool, 0);
    grouped.try_grow(54 * MIB).unwrap();
    assert_eq!(pool.reserved(), 54 * MIB);
}

/// A pool 10 MiB past its limit by the loan of a sort: the pool is full for the
/// consumers that can spill (the last floor is kept for the readers), a spilled
/// aggregate holds 20 MiB, another aggregate holds 24 MiB, and the first one's
/// sort of 30 MiB was lent although nothing was free.
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
    sibling.try_grow(24 * MIB).unwrap();
    lending.try_grow(30 * MIB).unwrap();
    assert_eq!(pool.reserved(), 74 * MIB);
    assert_eq!(
        pool.locked().spillers[&lending.consumer().id()].grant,
        Grant::Lent(30 * MIB)
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
    // The pool holds 44 MiB without the loan, so 20 MiB are free to it.
    counting.try_grow(20 * MIB).unwrap();
    let error = counting.try_grow(1).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
    assert!(error.contains("0.0 B remain available"), "{error}");
    // A consumer that can spill still sees the loan, and is refused.
    assert!(is_refused(sibling.try_grow(1)));
    assert_eq!(pool.reserved(), 94 * MIB);
    // Repaid, the loan is gone from the pool.
    lending.shrink(30 * MIB);
    assert_eq!(pool.reserved(), 64 * MIB);
}

/// The floors together never pass half the pool, however many aggregates have
/// spilled; the floor kept for the readers of spilled data is one of them, and
/// the consumers that can spill get the rest.
#[test]
fn the_floors_together_stay_within_half_the_pool() {
    for spillers in [1, 2, 3, 4, 8, 16] {
        let pool = query_pool(64 * MIB);
        let _spilled: Vec<_> = (0..spillers).map(|n| spilled(&pool, n)).collect();
        let repartition = exchange(&pool, 0);
        // One floor for each spilled aggregate and one for the readers.
        let floor = SPILLER_FLOOR_BYTES.min(32 * MIB / (spillers + 1));
        let others_get = 64 * MIB - (spillers + 1) * floor;
        assert!(others_get >= 32 * MIB, "{spillers} spilled aggregates");
        repartition.try_grow(others_get).unwrap();
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
    exchange.try_grow(50 * MIB).unwrap();
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
    assert!(is_refused(repartition.try_grow(50 * MIB)));
    drop(spilling);
    assert!(pool.locked().spillers.is_empty());
    assert_eq!(pool.reserved(), 0);
    repartition.try_grow(50 * MIB).unwrap();
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

/// Existing nonspilling reservations fill the intended headroom. They are
/// counted once; failed reservations and releases preserve the real pool cap.
#[test]
fn sort_headroom_counts_memory_already_held_by_nonspilling_consumers() {
    let pool = query_pool(128 * MIB);
    let retained = MemoryConsumer::new("VarLenExpand path state").register(&pool);
    retained.try_grow(27 * MIB).unwrap();
    let sort = MemoryConsumer::new("ExternalSorter[0]")
        .with_can_spill(true)
        .register(&pool);
    sort.try_grow(16 * MIB).unwrap();
    // A quarter of the pool plus the memory the retained state holds, less the
    // last floor kept for the readers of spilled data (10 MiB).
    sort.try_grow(6 * MIB).unwrap();
    assert_eq!(pool.reserved(), 49 * MIB);
    assert!(is_refused(sort.try_grow(1)));
    retained.try_grow(79 * MIB).unwrap();
    assert_eq!(pool.reserved(), 128 * MIB);
    assert!(is_refused(retained.try_grow(1)));
    assert!(is_refused(sort.try_grow(1)));
    retained.shrink(106 * MIB);
    assert_eq!(pool.reserved(), 22 * MIB);
    assert!(is_refused(sort.try_grow(1)));
}

#[test]
fn an_empty_sort_can_start_beyond_headroom_but_never_beyond_the_real_pool() {
    let pool = query_pool(64 * MIB);
    let sorter = |partition| {
        MemoryConsumer::new(format!("ExternalSorter[{partition}]"))
            .with_can_spill(true)
            .register(&pool)
    };
    let sibling = sorter(0);
    sibling.try_grow(16 * MIB).unwrap();
    let starting = sorter(1);
    starting.try_grow(8 * MIB).unwrap();
    assert_eq!(pool.reserved(), 24 * MIB);
    // Once it has a batch to spill, ordinary producer headroom applies.
    assert!(is_refused(starting.try_grow(1)));
    let oversized = sorter(2);
    assert!(is_refused(oversized.try_grow(40 * MIB + 1)));
    assert_eq!(pool.reserved(), 24 * MIB);
    drop((sibling, starting, oversized));
    assert_eq!(pool.reserved(), 0);
}

/// A sibling's buffered runs must not make an empty external sorter fail
/// before it can spill. This reproduces the SF1 IC3 first-buffer refusal.
async fn assert_external_sort_spills_with_sibling(rows: i64, coalesce_runs: bool) {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use futures::StreamExt;

    let pool = Arc::new(QueryMemoryPool::new(64 * MIB));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let sibling = MemoryConsumer::new("ExternalSorter[sibling]")
        .with_can_spill(true)
        .register(&dynamic);
    sibling.try_grow(15 * MIB).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(dynamic)
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()
        .unwrap();
    let context = SessionContext::new_with_config_rt(
        SessionConfig::new().with_sort_spill_reservation_bytes(MIB),
        runtime,
    );
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let batches = [rows, 0]
        .into_iter()
        .flat_map(|offset| {
            let batch_rows = if coalesce_runs { 8_192 } else { rows };
            (offset..offset + rows)
                .step_by(batch_rows as usize)
                .map(move |start| (start, batch_rows))
        })
        .map(|(offset, batch_rows)| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values(
                    (offset..offset + batch_rows).rev(),
                ))],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let source = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
    let source = if coalesce_runs {
        Arc::new(crate::sort_runs::SortRunCoalesceExec::new(
            source,
            rows as usize * size_of::<i64>(),
        )) as Arc<dyn ExecutionPlan>
    } else {
        source
    };
    let sort = SortExec::new(
        [PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0)))].into(),
        source,
    );
    let mut stream = sort.execute(0, context.task_ctx()).unwrap();
    let mut expected = 0_i64;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for value in values.values() {
            assert_eq!(*value, expected);
            expected += 1;
        }
    }
    drop(stream);
    assert_eq!(expected, 2 * rows);
    assert!(sort.metrics().unwrap().spill_count().unwrap() > 0);
    assert!(pool.peak_gauge().load(std::sync::atomic::Ordering::Relaxed) <= 64 * MIB);
    assert_eq!(pool.reserved(), 15 * MIB);
    drop(sibling);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn a_real_external_sort_starts_and_spills_while_a_sibling_holds_the_buffer_share() {
    assert_external_sort_spills_with_sibling(131_072, false).await;
}

#[tokio::test]
async fn a_real_external_sort_drains_spill_output_before_retaining_the_next_batch() {
    // A larger source still has room to spill, but retaining every sorted
    // output crosses the buffered-sort share while most of the pool is free.
    assert_external_sort_spills_with_sibling(262_144, true).await;
}

/// A consumer that can spill is kept one floor short of the pool. The reader of
/// a batch an exchange spilled, and the writer of a spill, cannot spill and
/// fail the query when refused, while the pool admits spillable consumers to
/// the last byte; an aggregate that had grown to within a batch of the limit
/// left the exchange's reader 95 KiB for a 398 KiB batch (run 39356061798,
/// #2173). Invariant: while the other consumers that can spill hold nothing,
/// a spillable consumer is admitted up to the pool less one floor, and the
/// last floor is served to a consumer that cannot spill.
#[test]
fn a_spilling_consumer_leaves_the_last_floor_to_the_readers_of_spilled_data() {
    let pool = query_pool(64 * MIB);
    let busy = aggregate(&pool, 0);
    while busy.try_grow(2 * MIB).is_ok() {}
    assert_eq!(busy.size(), 54 * MIB);
    let reader = reader(&pool);
    reader.try_grow(10 * MIB).unwrap();
    assert_eq!(pool.reserved(), 64 * MIB);
}

/// A request, registration or unregistration of a consumer a merge pass is
/// made of, with the number of output batches already delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PassEvent {
    delivered: usize,
    consumer: String,
    bytes: usize,
    registered: bool,
    /// Spill readers alive when the event happened.
    readers: usize,
}

/// Delegates to the query pool and records every request of the consumers a
/// sort's merge is made of: its reservation (`ExternalSorterMerge`, which
/// carries the copies) and the readers and writers of its spill files.
#[derive(Debug)]
struct PassRecorder {
    inner: QueryMemoryPool,
    delivered: std::sync::atomic::AtomicUsize,
    readers: std::sync::atomic::AtomicUsize,
    events: Mutex<Vec<PassEvent>>,
}

impl PassRecorder {
    fn new(size: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: QueryMemoryPool::new(size),
            delivered: Default::default(),
            readers: Default::default(),
            events: Mutex::default(),
        })
    }
    fn record(&self, consumer: &MemoryConsumer, bytes: usize, registered: bool) {
        let name = consumer.name();
        if name.starts_with("ExternalSorterMerge")
            || name.starts_with(AGGREGATE_CONSUMER_PREFIX)
            || name.starts_with("SpillReader")
            || name.starts_with("SpillWriter")
        {
            self.events.lock().unwrap().push(PassEvent {
                delivered: self.delivered.load(std::sync::atomic::Ordering::SeqCst),
                consumer: name.to_string(),
                bytes,
                registered,
                readers: self.readers.load(std::sync::atomic::Ordering::SeqCst),
            });
        }
    }
}

impl std::fmt::Display for PassRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl MemoryPool for PassRecorder {
    fn name(&self) -> &str {
        "pass-recorder"
    }
    fn register(&self, consumer: &MemoryConsumer) {
        if consumer.name().starts_with("SpillReader") {
            self.readers
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.record(consumer, 0, true);
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        if consumer.name().starts_with("SpillReader") {
            self.readers
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.record(reservation.consumer(), additional, false);
        self.inner.grow(reservation, additional);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.record(reservation.consumer(), additional, false);
        self.inner.try_grow(reservation, additional)
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// What a sort that spills at least twice did to the pool while it merged.
struct MergeRun {
    /// The events from the first reader of the merge on.
    events: Vec<PassEvent>,
    /// The events before it.
    before: Vec<PassEvent>,
    passes: usize,
}

/// Sorts 2 x 131072 rows of a Utf8 key `k`, an Int64 key `j` and a 100-byte
/// payload under a pool of `pool` bytes and records the requests of the merge.
/// `keys` choose the cursor of the merge: the Int64 column reads with a
/// primitive Field cursor (the schema holds only Int64 and Utf8), the Utf8
/// column with a Field cursor, and both with a Row cursor.
async fn merge_of_spilled_sort(keys: &[&str], pool: usize) -> MergeRun {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use futures::StreamExt;
    use std::sync::atomic::Ordering;

    const KEYS: i64 = 131_072;
    const ROWS: usize = 8_192;
    let pool = PassRecorder::new(pool);
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(dynamic)
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()
        .unwrap();
    let context = SessionContext::new_with_config_rt(
        SessionConfig::new().with_sort_spill_reservation_bytes(MIB),
        runtime,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("j", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]));
    let pad = "x".repeat(100);
    let batches = (0..2 * KEYS)
        .rev()
        .step_by(ROWS)
        .map(|top| {
            let keys = (top - ROWS as i64 + 1..=top).rev().collect::<Vec<_>>();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from_iter_values(
                        keys.iter().map(|key| format!("{key:010}")),
                    )),
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(StringArray::from(vec![pad.as_str(); ROWS])),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let source = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
    let order = keys
        .iter()
        .map(|key| {
            let index = schema.index_of(key).unwrap();
            PhysicalSortExpr::new_default(Arc::new(Column::new(key, index)))
        })
        .collect::<Vec<_>>();
    let sort = SortExec::new(LexOrdering::new(order).expect("sort keys"), source);
    let mut stream = sort.execute(0, context.task_ctx()).unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch
            .unwrap_or_else(|error| panic!("{error}: {:?}", pool.events.lock().unwrap()))
            .num_rows();
        pool.delivered.fetch_add(1, Ordering::SeqCst);
    }
    drop(stream);
    assert_eq!(rows, 2 * KEYS as usize);
    assert!(sort.metrics().unwrap().spill_count().unwrap() >= 2);
    assert!(pool.delivered.load(Ordering::SeqCst) > 2);
    let events = pool.events.lock().unwrap().clone();
    // The merge starts with its first reader; the sort wrote its own spill
    // files before it, and an intermediate file of the merge registers a
    // writer after it.
    let first_reader = events
        .iter()
        .position(|event| event.consumer.starts_with("SpillReader"))
        .expect("the merge reads its spill files");
    let intermediates = events[first_reader..]
        .iter()
        .filter(|event| event.registered && event.consumer == "SpillWriterCodec")
        .count();
    MergeRun {
        events: events[first_reader..].to_vec(),
        before: events[..first_reader].to_vec(),
        passes: intermediates + 1,
    }
}

/// Nothing a merge pass is made of asks the pool for bytes once the pass has
/// been admitted (#2173): not the readers' decoders and metadata, not the
/// writer of an intermediate file, not the copies' interleave workspace and
/// outputs on the merge reservation, which selects the files of the next pass
/// only while no reader of the last is alive.
fn assert_the_passes_ask_for_nothing_after_their_selection(run: &MergeRun) {
    let fresh = run
        .events
        .iter()
        .filter(|event| {
            !event.registered
                && event.bytes > 0
                && (event.consumer.starts_with("SpillReader")
                    || event.consumer.starts_with("SpillWriter"))
        })
        .collect::<Vec<_>>();
    assert!(fresh.is_empty(), "readers and writers asked: {fresh:?}");
    let during = run
        .events
        .iter()
        .filter(|event| {
            !event.registered
                && event.bytes > 0
                && (event.consumer.starts_with("ExternalSorterMerge")
                    || event.consumer.starts_with(AGGREGATE_CONSUMER_PREFIX))
                && event.readers > 0
        })
        .collect::<Vec<_>>();
    assert!(
        during.is_empty(),
        "a pass asked after its selection: {during:?}"
    );
}

#[tokio::test]
async fn a_merge_pass_with_a_row_cursor_asks_the_pool_for_nothing_after_its_selection() {
    let run = merge_of_spilled_sort(&["k", "j"], 128 * MIB).await;
    assert_the_passes_ask_for_nothing_after_their_selection(&run);
}

#[tokio::test]
async fn a_merge_pass_with_a_primitive_field_cursor_asks_the_pool_for_nothing_after_its_selection()
{
    let run = merge_of_spilled_sort(&["j"], 128 * MIB).await;
    assert_the_passes_ask_for_nothing_after_their_selection(&run);
}

#[tokio::test]
async fn a_merge_pass_with_a_field_cursor_asks_the_pool_for_nothing_after_its_selection() {
    let run = merge_of_spilled_sort(&["k"], 128 * MIB).await;
    assert_the_passes_ask_for_nothing_after_their_selection(&run);
}

/// The files of the sort do not fit one pass: the passes narrow their breadth
/// to the pool, write their intermediate files from the bank they were
/// admitted, and the last one reads them back. No pass asks the pool for
/// anything after its selection.
#[tokio::test]
async fn merge_passes_that_write_intermediate_files_ask_the_pool_for_nothing_after_their_selection()
{
    for keys in [&["k", "j"][..], &["j"][..], &["k"][..]] {
        let run = merge_of_spilled_sort(keys, 48 * MIB).await;
        assert!(run.passes > 2, "{keys:?}: {} passes", run.passes);
        assert_the_passes_ask_for_nothing_after_their_selection(&run);
    }
}

/// Runs `sql` over `rows` rows of an Int64 group `g` (`groups` of them) and a
/// `pad`-byte payload under a query pool of `pool` bytes, one partition and
/// batches of `batch_size` rows; the requests of the consumers of the merge
/// pass of its aggregate, and the result.
async fn spilled_aggregate(
    sql: &str,
    groups: i64,
    rows: i64,
    pad: usize,
    batch_size: usize,
    pool: usize,
) -> (Result<Vec<RecordBatch>>, MergeRun) {
    let pool = PassRecorder::new(pool);
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(dynamic)
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()
        .unwrap();
    let context = SessionContext::new_with_config_rt(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(batch_size),
        runtime,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]));
    let text = "x".repeat(pad);
    let batches = (0..rows)
        .step_by(8192)
        .map(|first| {
            let keys = (first..(first + 8192).min(rows))
                .map(|row| row % groups)
                .collect::<Vec<_>>();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(keys.clone())),
                    Arc::new(StringArray::from(vec![text.as_str(); keys.len()])),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    context
        .register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()),
        )
        .unwrap();
    let result = context.sql(sql).await.unwrap().collect().await;
    let events = pool.events.lock().unwrap().clone();
    let first = events
        .iter()
        .position(|event| event.consumer.starts_with("SpillReader"))
        .unwrap_or(events.len());
    (
        result,
        MergeRun {
            events: events[first..].to_vec(),
            before: events[..first].to_vec(),
            passes: 1,
        },
    )
}

/// A grouped aggregate that has spilled merges its spill files holding the
/// bound it admitted when it began to: after its first reader exists, neither
/// the state of its groups, nor its readers, nor the copies of its merge ask
/// the pool for a byte.
#[tokio::test]
async fn a_merging_aggregate_holds_its_state_bound_and_asks_the_pool_for_nothing() {
    let (result, run) = spilled_aggregate(
        "SELECT g, max(pad) FROM t GROUP BY g",
        262_144,
        262_144,
        100,
        8192,
        24 * MIB,
    )
    .await;
    assert_eq!(
        result.unwrap().iter().map(|b| b.num_rows()).sum::<usize>(),
        262_144
    );
    assert!(
        !run.events.is_empty(),
        "the groups exceed the pool, so the aggregate merged spill files"
    );
    assert_the_passes_ask_for_nothing_after_their_selection(&run);
}

/// The consumer of a spilled aggregate admits its state bound and the
/// narrowest pass of its merge as one request, right after its final spill
/// (#2173): the pool guarantees a spiller one request there (its floor, or the
/// loan the spill armed), and a merge that needs two before its pass can start
/// is refused on the second whenever the pool is full. Between the writer of
/// the final spill and the first reader, the aggregate asks exactly once.
#[tokio::test]
async fn a_spilled_aggregate_admits_its_minimum_pass_in_one_request() {
    let (result, run) = spilled_aggregate(
        "SELECT g, max(pad) FROM t GROUP BY g",
        262_144,
        262_144,
        100,
        8192,
        24 * MIB,
    )
    .await;
    result.unwrap();
    let last_spill = run
        .before
        .iter()
        .rposition(|event| {
            !event.registered && event.bytes > 0 && event.consumer.starts_with("SpillWriter")
        })
        .expect("the aggregate spilled");
    let asked = run.before[last_spill..]
        .iter()
        .filter(|event| {
            !event.registered
                && event.bytes > 0
                && event.consumer.starts_with(AGGREGATE_CONSUMER_PREFIX)
        })
        .collect::<Vec<_>>();
    // The first request is the state bound and the narrowest pass together,
    // past the spiller's floor of 10 MiB and so served by the loan the final
    // spill armed; what follows is the selection growing above it.
    let first = asked.first().expect("the aggregate admits its merge");
    assert!(
        first.bytes > 10 * MIB,
        "state bound and narrowest pass in one request: {asked:?}"
    );
}

/// The pool state of `stress-fixb/run-13.log` (#2173, round 3): two partial
/// aggregates that emitted early and keep what their emptied hash tables
/// retain, a final aggregate still growing its first state, the exchange and
/// its spill writer, 38.1 MiB in all. The other final aggregate has never
/// held a byte: its first state and the restart of its empty state (64 B, the
/// size of its group ordering) are served within the limit of the pool, as a
/// consumer that cannot spill is, because a refusal fails its query the same
/// way: it has nothing to spill. From its next request on the floors apply.
#[test]
fn an_aggregate_that_has_not_spilled_is_served_its_first_state_within_the_pool_alone() {
    let pool = query_pool(64 * MIB);
    let partials = [aggregate(&pool, 0), aggregate(&pool, 1)];
    for partial in &partials {
        partial.try_grow(15_138_880).unwrap();
        partial.shrink(13_238_272);
    }
    let busy = aggregate(&pool, 0);
    busy.try_grow(32_296_140).unwrap();
    let exchanges = [exchange(&pool, 0), exchange(&pool, 1)];
    exchanges[0].try_grow(1_835_418).unwrap();
    exchanges[1].try_grow(957_415).unwrap();
    let writer = reader(&pool);
    writer.try_grow(1_060_557).unwrap();
    assert_eq!(pool.reserved(), 39_950_746);
    // The first state of the late final aggregate: one batch of groups with
    // the sort headroom. Served; the floors apply from the next request.
    let late = aggregate(&pool, 1);
    late.try_grow(2_031_616).unwrap();
    assert!(is_refused(late.try_grow(MIB)));
    // What it spills enters it into the map, and its restart is within its floor.
    late.shrink(2_031_616);
    late.try_grow(64).unwrap();
    // An aggregate whose first state was refused spilled nothing and asks for
    // its empty state, 64 B, from nothing: run-13's request, served the same way.
    let emptied = aggregate(&pool, 1);
    emptied.try_grow(64).unwrap();
    assert!(is_refused(emptied.try_grow(2_031_552)));
}

/// A partial aggregate emits early and never merges: its emit is a bulk shrink
/// like a spill's, but it gets no floor and no loan. The exchange beside it is
/// admitted up to the pool less the readers' reserve and what the partial
/// keeps, and the partial's own request past the free memory is refused.
#[test]
fn a_partial_aggregate_holds_no_floor_and_takes_no_loan() {
    let pool = query_pool(64 * MIB);
    let partial = MemoryConsumer::new("GroupedHashAggregateStream(partial)[0] (max(a.pad))")
        .with_can_spill(true)
        .register(&pool);
    partial.try_grow(15_138_880).unwrap();
    partial.shrink(13_238_272);
    let exchange = exchange(&pool, 0);
    exchange.try_grow(64 * MIB - 10 * MIB - 1_900_608).unwrap();
    assert!(is_refused(exchange.try_grow(1)));
    assert!(is_refused(partial.try_grow(MIB)));
}
