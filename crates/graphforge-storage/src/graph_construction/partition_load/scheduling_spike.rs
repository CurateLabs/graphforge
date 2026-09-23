//! Scheduling and cancellation candidates for the ordered partition load pool
//! (#1508, a spike under #1504). Test-only: nothing here is reachable from a
//! production build.
//!
//! Every candidate meets the contract [`super::consume_in_partition_order`]
//! already meets, so each can be swapped in at the real call site
//! (`FixedRangePartitioner::with_load_scheduler`) and compared on the same
//! bytes:
//!
//! * loads run off the coordinator; `consume` runs on the calling thread in
//!   partition index order and may borrow non-`Send` state;
//! * at most `workers` partitions are materialized at once, counting loads in
//!   flight, loaded results not yet consumed, and the one being consumed;
//! * the first error stops dispatch and is returned only after every started
//!   load has returned. This *joined* return is what lets the caller delete
//!   spill files without racing a reader.
//!
//! What each library contributes, and what the adapter has to add, is recorded
//! in `docs/development/evidence/construction-scheduling-spike-1508.md`. Two
//! adapter costs are visible in the signatures below: Tokio and DataFusion
//! tasks are `'static`, so the load must own its inputs (an `Arc` closure over
//! a duplicated directory handle), and neither library joins a running
//! blocking task when its handle is dropped, so the adapter drains every
//! handle itself.

use super::{consume_in_partition_order, storage};
use futures::StreamExt;
use graphforge_core::GfError;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

/// How often a candidate coordinator polls the caller's cancellation while it
/// waits for the head partition. The production pool polls only in `consume`,
/// so it notices cancellation only once the head partition has loaded.
pub(in crate::graph_construction) const CANCEL_POLL: Duration = Duration::from_millis(1);

/// A scheduler for the finish-time partition loads.
#[derive(Clone, Copy, Debug)]
pub(in crate::graph_construction) enum LoadScheduler<'a> {
    /// Production: scoped `std::thread` workers and a condvar window.
    Baseline,
    /// `rayon::ThreadPool::in_place_scope` on the given pool, or on a pool of
    /// `workers` threads built for the call.
    Rayon(Option<&'a rayon::ThreadPool>),
    /// `tokio::task::spawn_blocking` on the given multi-thread runtime, or on
    /// a current-thread runtime built for the call with its blocking pool
    /// capped at `workers`.
    TokioBlocking(Option<&'a tokio::runtime::Handle>),
    /// DataFusion's `SpawnedTask::spawn_blocking`: a Tokio blocking task
    /// behind an abort-on-drop handle. Same runtime choice as `TokioBlocking`.
    DataFusionSpawned(Option<&'a tokio::runtime::Handle>),
    /// Async shape without a spawn: every load is a future polled on the
    /// coordinator through `buffered(workers)`. The negative control for "an
    /// async API is parallel".
    TokioInline,
}

impl LoadScheduler<'_> {
    pub(in crate::graph_construction) fn name(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Rayon(None) => "rayon",
            Self::Rayon(Some(_)) => "rayon-shared-pool",
            Self::TokioBlocking(None) => "tokio-blocking",
            Self::TokioBlocking(Some(_)) => "tokio-blocking-shared-runtime",
            Self::DataFusionSpawned(None) => "datafusion-spawned",
            Self::DataFusionSpawned(Some(_)) => "datafusion-spawned-shared-runtime",
            Self::TokioInline => "tokio-inline",
        }
    }
}

/// Load `partitions` with `scheduler` and consume every result on the calling
/// thread in partition index order.
///
/// `cancelled` is polled by candidate coordinators while they wait; the
/// baseline ignores it, as production does, and relies on `consume` to poll.
pub(in crate::graph_construction) fn run<T, L, C>(
    scheduler: LoadScheduler<'_>,
    partitions: usize,
    workers: NonZeroUsize,
    load: Arc<L>,
    consume: C,
    cancelled: &dyn Fn() -> bool,
) -> Result<(), GfError>
where
    T: Send + 'static,
    L: Fn(usize, &AtomicBool) -> Result<T, GfError> + Send + Sync + 'static,
    C: FnMut(usize, T) -> Result<(), GfError>,
{
    if partitions == 0 {
        return Ok(());
    }
    match scheduler {
        LoadScheduler::Baseline => consume_in_partition_order(
            partitions,
            workers,
            |index, stop| load(index, stop),
            consume,
        ),
        LoadScheduler::Rayon(Some(pool)) => {
            rayon_ordered(pool, partitions, workers, &*load, consume, cancelled)
        }
        LoadScheduler::Rayon(None) => {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers.get())
                .build()
                .map_err(storage)?;
            rayon_ordered(&pool, partitions, workers, &*load, consume, cancelled)
        }
        LoadScheduler::TokioBlocking(handle) => tokio_ordered(
            handle,
            Spawner::Tokio,
            partitions,
            workers,
            load,
            consume,
            cancelled,
        ),
        LoadScheduler::DataFusionSpawned(handle) => tokio_ordered(
            handle,
            Spawner::DataFusion,
            partitions,
            workers,
            load,
            consume,
            cancelled,
        ),
        LoadScheduler::TokioInline => tokio_inline(partitions, workers, &*load, consume),
    }
}

fn cancelled_error() -> GfError {
    storage("construction cancelled")
}

/// Turn a panicking load into an error for its partition. Rayon would
/// otherwise resume the panic only when the scope ends, after the coordinator
/// has already waited for a result that will never arrive.
fn contain_panic<T>(load: impl FnOnce() -> Result<T, GfError>) -> Result<T, GfError> {
    catch_unwind(AssertUnwindSafe(load)).unwrap_or_else(|_| Err(storage("partition load panicked")))
}

fn rayon_ordered<T, L, C>(
    pool: &rayon::ThreadPool,
    partitions: usize,
    workers: NonZeroUsize,
    load: &L,
    mut consume: C,
    cancelled: &dyn Fn() -> bool,
) -> Result<(), GfError>
where
    T: Send,
    L: Fn(usize, &AtomicBool) -> Result<T, GfError> + Sync,
    C: FnMut(usize, T) -> Result<(), GfError>,
{
    let window = workers.get();
    let stop = AtomicBool::new(false);
    let (sender, receiver) = std::sync::mpsc::channel();
    // `in_place_scope` runs this closure on the calling thread, so `consume`
    // needs no `Send`, and it returns only after every spawned load has
    // finished: the joined return comes from the library.
    pool.in_place_scope(|scope| {
        let spawn = |index: usize| {
            let sender = sender.clone();
            let stop = &stop;
            scope.spawn(move |_| {
                let loaded = contain_panic(|| load(index, stop));
                // A closed channel only means the coordinator stopped wanting
                // this partition.
                let _ = sender.send((index, loaded));
            });
        };
        let outcome = (|| {
            let mut next = 0;
            while next < partitions.min(window) {
                spawn(next);
                next += 1;
            }
            let mut ready = BTreeMap::new();
            for index in 0..partitions {
                let loaded = loop {
                    if let Some(loaded) = ready.remove(&index) {
                        break loaded;
                    }
                    if cancelled() {
                        return Err(cancelled_error());
                    }
                    match receiver.recv_timeout(CANCEL_POLL) {
                        Ok((delivered, loaded)) => {
                            ready.insert(delivered, loaded);
                        }
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            return Err(storage("partition loaders disconnected"));
                        }
                    }
                };
                consume(index, loaded?)?;
                // The consumed value is gone; its slot admits the next load.
                if next < partitions {
                    spawn(next);
                    next += 1;
                }
            }
            Ok(())
        })();
        stop.store(true, Ordering::Release);
        outcome
    })
}

#[derive(Clone, Copy)]
enum Spawner {
    Tokio,
    DataFusion,
}

type Joined<T> =
    Pin<Box<dyn Future<Output = Result<Result<T, GfError>, tokio::task::JoinError>> + Send>>;

fn refuse_nested_runtime() -> Result<(), GfError> {
    // `block_on` panics on a thread already driving a runtime ("Cannot start
    // a runtime from within a runtime"); the facility test pins that panic.
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(storage(
            "tokio load scheduler cannot block inside an async runtime",
        ));
    }
    Ok(())
}

fn tokio_ordered<T, L, C>(
    handle: Option<&tokio::runtime::Handle>,
    spawner: Spawner,
    partitions: usize,
    workers: NonZeroUsize,
    load: Arc<L>,
    consume: C,
    cancelled: &dyn Fn() -> bool,
) -> Result<(), GfError>
where
    T: Send + 'static,
    L: Fn(usize, &AtomicBool) -> Result<T, GfError> + Send + Sync + 'static,
    C: FnMut(usize, T) -> Result<(), GfError>,
{
    refuse_nested_runtime()?;
    let window = workers.get();
    let driven = drive(spawner, partitions, window, load, consume, cancelled);
    match handle {
        Some(handle) => handle.block_on(driven),
        None => {
            // A current-thread runtime's `Handle::block_on` cannot drive its
            // timer, so the owned runtime blocks directly.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .max_blocking_threads(window)
                .build()
                .map_err(storage)?;
            runtime.block_on(driven)
        }
    }
}

async fn drive<T, L, C>(
    spawner: Spawner,
    partitions: usize,
    window: usize,
    load: Arc<L>,
    mut consume: C,
    cancelled: &dyn Fn() -> bool,
) -> Result<(), GfError>
where
    T: Send + 'static,
    L: Fn(usize, &AtomicBool) -> Result<T, GfError> + Send + Sync + 'static,
    C: FnMut(usize, T) -> Result<(), GfError>,
{
    let stop = Arc::new(AtomicBool::new(false));
    let spawn = |index: usize| -> Joined<T> {
        let load = Arc::clone(&load);
        let stop = Arc::clone(&stop);
        let task = move || load(index, &stop);
        match spawner {
            Spawner::Tokio => Box::pin(tokio::task::spawn_blocking(task)),
            Spawner::DataFusion => Box::pin(
                datafusion::common::runtime::SpawnedTask::spawn_blocking(task),
            ),
        }
    };
    // In index order: the head is always the next partition to consume.
    let mut in_flight: VecDeque<Joined<T>> = VecDeque::with_capacity(window);
    let mut next = 0;
    while next < partitions.min(window) {
        in_flight.push_back(spawn(next));
        next += 1;
    }
    let outcome = async {
        for index in 0..partitions {
            let head = in_flight
                .front_mut()
                .ok_or_else(|| storage("partition load window is empty"))?;
            let joined = loop {
                tokio::select! {
                    joined = &mut *head => break joined,
                    () = tokio::time::sleep(CANCEL_POLL) => {
                        if cancelled() {
                            return Err(cancelled_error());
                        }
                    }
                }
            };
            in_flight.pop_front();
            let loaded = joined.map_err(|error| {
                if error.is_panic() {
                    storage("partition load panicked")
                } else {
                    storage(format!("partition load task failed: {error}"))
                }
            })??;
            consume(index, loaded)?;
            if next < partitions {
                in_flight.push_back(spawn(next));
                next += 1;
            }
        }
        Ok(())
    }
    .await;
    stop.store(true, Ordering::Release);
    // Dropping a Tokio handle detaches a running blocking task, and
    // DataFusion's abort-on-drop only cancels one that has not started. Either
    // way the load would outlive this return; join every one explicitly.
    for pending in in_flight {
        let _ = pending.await;
    }
    outcome
}

fn tokio_inline<T, L, C>(
    partitions: usize,
    workers: NonZeroUsize,
    load: &L,
    mut consume: C,
) -> Result<(), GfError>
where
    L: Fn(usize, &AtomicBool) -> Result<T, GfError>,
    C: FnMut(usize, T) -> Result<(), GfError>,
{
    refuse_nested_runtime()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        let stop = AtomicBool::new(false);
        let stop = &stop;
        let mut loads = futures::stream::iter(0..partitions)
            .map(|index| async move { load(index, stop) })
            .buffered(workers.get());
        let mut index = 0;
        while let Some(loaded) = loads.next().await {
            consume(index, loaded?)?;
            index += 1;
        }
        Ok(())
    })
}

mod tests;
