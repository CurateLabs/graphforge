//! #1508 spike tests: the ordered-pool contract under every candidate, the
//! library behaviour each adapter rests on, DataFusion operator probes, the
//! real fixed-partition finish, and an `#[ignore]`d measurement report.

use super::*;
use std::collections::{BTreeSet, HashSet};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::thread::ThreadId;
use std::time::Instant;

const SPIN_LIMIT: Duration = Duration::from_secs(20);

/// Every candidate, each building its pool or runtime per call.
const PER_CALL: [LoadScheduler<'static>; 5] = [
    LoadScheduler::Baseline,
    LoadScheduler::Rayon(None),
    LoadScheduler::TokioBlocking(None),
    LoadScheduler::DataFusionSpawned(None),
    LoadScheduler::TokioInline,
];

/// The candidates that run loads off the coordinator.
const PARALLEL: [LoadScheduler<'static>; 4] = [
    LoadScheduler::Baseline,
    LoadScheduler::Rayon(None),
    LoadScheduler::TokioBlocking(None),
    LoadScheduler::DataFusionSpawned(None),
];

fn workers(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}

fn spin_until(condition: impl Fn() -> bool, what: &str) {
    let started = Instant::now();
    while !condition() {
        assert!(
            started.elapsed() < SPIN_LIMIT,
            "timed out waiting for {what}"
        );
        std::thread::yield_now();
    }
}

/// Loads running now, values materialized now, and their peaks. A value is
/// materialized from the start of its load until the coordinator drops it.
#[derive(Default)]
struct Gauges {
    started: AtomicUsize,
    running: AtomicUsize,
    peak_running: AtomicUsize,
    live: AtomicUsize,
    peak_live: AtomicUsize,
    threads: Mutex<HashSet<ThreadId>>,
}

struct Running(Arc<Gauges>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.running.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Value {
    index: usize,
    gauges: Arc<Gauges>,
}

impl Drop for Value {
    fn drop(&mut self) {
        self.gauges.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Gauges {
    fn enter(self: &Arc<Self>, index: usize) -> (Running, Value) {
        self.started.fetch_add(1, Ordering::SeqCst);
        let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_running.fetch_max(running, Ordering::SeqCst);
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_live.fetch_max(live, Ordering::SeqCst);
        self.threads
            .lock()
            .unwrap()
            .insert(std::thread::current().id());
        (
            Running(Arc::clone(self)),
            Value {
                index,
                gauges: Arc::clone(self),
            },
        )
    }

    fn at_rest(&self) -> bool {
        self.running.load(Ordering::SeqCst) == 0 && self.live.load(Ordering::SeqCst) == 0
    }
}

fn never() -> bool {
    false
}

#[test]
fn every_parallel_candidate_consumes_in_order_and_holds_the_window() {
    const PARTITIONS: usize = 8;
    for scheduler in PARALLEL {
        for count in [1, 2, 3, 8] {
            let gauges = Arc::new(Gauges::default());
            let completed = Arc::new(AtomicUsize::new(0));
            let slots = count.min(PARTITIONS);
            let load = {
                let (gauges, completed) = (Arc::clone(&gauges), Arc::clone(&completed));
                Arc::new(move |index: usize, _stop: &AtomicBool| {
                    let (_running, value) = gauges.enter(index);
                    if index == 0 {
                        // Hold the head until every other window slot has
                        // loaded: an unbounded pool would admit all seven.
                        spin_until(
                            || completed.load(Ordering::SeqCst) >= slots - 1,
                            "the other window slots to load",
                        );
                    } else {
                        completed.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(value)
                })
            };
            let mut order = Vec::new();
            let consume = |index: usize, value: Value| {
                assert_eq!(value.index, index);
                assert!(gauges.live.load(Ordering::SeqCst) <= count);
                order.push(index);
                Ok(())
            };
            run(scheduler, PARTITIONS, workers(count), load, consume, &never).unwrap();
            let label = format!("{} workers={count}", scheduler.name());
            assert_eq!(order, (0..PARTITIONS).collect::<Vec<_>>(), "{label}");
            assert!(gauges.at_rest(), "{label}");
            assert_eq!(gauges.peak_live.load(Ordering::SeqCst), slots, "{label}");
        }
    }
}

#[test]
fn parallel_candidates_overlap_loads_on_distinct_threads() {
    const WINDOW: usize = 3;
    for scheduler in PARALLEL {
        let gauges = Arc::new(Gauges::default());
        let load = {
            let gauges = Arc::clone(&gauges);
            Arc::new(move |index: usize, _stop: &AtomicBool| {
                let (_running, value) = gauges.enter(index);
                if index < WINDOW {
                    // Only a pool that really runs the first window together
                    // gets past this. The peak, not the live count: a load
                    // that saw the window full may already have left it.
                    spin_until(
                        || gauges.peak_running.load(Ordering::SeqCst) >= WINDOW,
                        "the first window to run together",
                    );
                }
                Ok(value)
            })
        };
        run(
            scheduler,
            6,
            workers(WINDOW),
            load,
            |_, _: Value| Ok(()),
            &never,
        )
        .unwrap();
        let threads = gauges.threads.lock().unwrap();
        assert_eq!(
            gauges.peak_running.load(Ordering::SeqCst),
            WINDOW,
            "{}",
            scheduler.name()
        );
        assert!(threads.len() >= WINDOW, "{}", scheduler.name());
        assert!(
            !threads.contains(&std::thread::current().id()),
            "{} loaded on the coordinator",
            scheduler.name()
        );
    }
}

#[test]
fn inline_async_loads_run_one_at_a_time_on_the_coordinator() {
    let gauges = Arc::new(Gauges::default());
    let load = {
        let gauges = Arc::clone(&gauges);
        Arc::new(move |index: usize, _stop: &AtomicBool| {
            let (_running, value) = gauges.enter(index);
            // Give a second load every chance to overlap; none can.
            let until = Instant::now() + Duration::from_millis(20);
            while Instant::now() < until {
                std::hint::spin_loop();
            }
            Ok(value)
        })
    };
    let mut order = Vec::new();
    run(
        LoadScheduler::TokioInline,
        6,
        workers(3),
        load,
        |index, value: Value| {
            assert_eq!(value.index, index);
            order.push(index);
            Ok(())
        },
        &never,
    )
    .unwrap();
    assert_eq!(order, (0..6).collect::<Vec<_>>());
    // `buffered(3)` materializes up to three results, but never runs two
    // loads at once, and every load ran on this thread.
    assert_eq!(gauges.peak_running.load(Ordering::SeqCst), 1);
    assert!(gauges.peak_live.load(Ordering::SeqCst) <= 3);
    assert_eq!(
        *gauges.threads.lock().unwrap(),
        HashSet::from([std::thread::current().id()])
    );
    assert!(gauges.at_rest());
}

#[test]
fn cancellation_before_the_first_consume_consumes_nothing() {
    for scheduler in PER_CALL {
        let gauges = Arc::new(Gauges::default());
        let load = {
            let gauges = Arc::clone(&gauges);
            Arc::new(move |index: usize, _stop: &AtomicBool| Ok(gauges.enter(index).1))
        };
        let mut consumed = Vec::new();
        let error = run(
            scheduler,
            6,
            workers(2),
            load,
            |index, _: Value| {
                // Production polls in `consume`; so does this fixture.
                consumed.push(index);
                Err(cancelled_error())
            },
            &|| true,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("construction cancelled"), "{error}");
        // The baseline and inline candidates notice in `consume`, the others
        // while waiting; neither consumes a partition past the cancellation.
        assert!(consumed.len() <= 1, "{}", scheduler.name());
        assert!(gauges.at_rest(), "{}", scheduler.name());
    }
}

/// A head load that runs until the coordinator stops it, bounded so the
/// baseline, which cannot stop it, still returns.
fn head_load_until_stopped(
    gauges: &Arc<Gauges>,
    head_started: &Arc<AtomicBool>,
    head_bound: Duration,
) -> Arc<impl Fn(usize, &AtomicBool) -> Result<Value, GfError> + Send + Sync + 'static> {
    let (gauges, head_started) = (Arc::clone(gauges), Arc::clone(head_started));
    Arc::new(move |index: usize, stop: &AtomicBool| {
        let (_running, value) = gauges.enter(index);
        if index == 0 {
            head_started.store(true, Ordering::SeqCst);
            let until = Instant::now() + head_bound;
            while Instant::now() < until {
                if stop.load(Ordering::Acquire) {
                    return Err(storage("partition load abandoned after coordinator stop"));
                }
                std::hint::spin_loop();
            }
        }
        Ok(value)
    })
}

#[test]
fn cancellation_while_the_head_load_runs_returns_only_after_it_exits() {
    for scheduler in PARALLEL {
        let gauges = Arc::new(Gauges::default());
        let head_started = Arc::new(AtomicBool::new(false));
        let cancel = Arc::new(AtomicBool::new(false));
        let load = head_load_until_stopped(&gauges, &head_started, Duration::from_millis(300));
        let canceller = {
            let (head_started, cancel) = (Arc::clone(&head_started), Arc::clone(&cancel));
            std::thread::spawn(move || {
                spin_until(|| head_started.load(Ordering::SeqCst), "the head load");
                cancel.store(true, Ordering::SeqCst);
            })
        };
        let mut consumed = Vec::new();
        let error = run(
            scheduler,
            6,
            workers(2),
            load,
            |index, _: Value| {
                if cancel.load(Ordering::SeqCst) {
                    return Err(cancelled_error());
                }
                consumed.push(index);
                Ok(())
            },
            &|| cancel.load(Ordering::SeqCst),
        )
        .unwrap_err()
        .to_string();
        canceller.join().unwrap();
        assert!(error.contains("construction cancelled"), "{error}");
        assert!(!error.contains("abandoned"), "{error}");
        assert!(consumed.is_empty(), "{}", scheduler.name());
        // Joined: no load outlives the return.
        assert!(gauges.at_rest(), "{}", scheduler.name());
    }
}

#[test]
fn cancellation_during_consume_keeps_the_consumed_prefix_only() {
    for scheduler in PER_CALL {
        let gauges = Arc::new(Gauges::default());
        let cancel = AtomicBool::new(false);
        let load = {
            let gauges = Arc::clone(&gauges);
            Arc::new(move |index: usize, _stop: &AtomicBool| Ok(gauges.enter(index).1))
        };
        let mut consumed = Vec::new();
        let error = run(
            scheduler,
            6,
            workers(2),
            load,
            |index, _: Value| {
                if index == 2 {
                    // Cancelled partway through writing partition 2; the
                    // writer's own poll notices.
                    cancel.store(true, Ordering::SeqCst);
                }
                if cancel.load(Ordering::SeqCst) {
                    return Err(cancelled_error());
                }
                consumed.push(index);
                Ok(())
            },
            &|| cancel.load(Ordering::SeqCst),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("construction cancelled"), "{error}");
        assert_eq!(consumed, vec![0, 1], "{}", scheduler.name());
        assert!(gauges.at_rest(), "{}", scheduler.name());
    }
}

#[test]
fn load_error_surfaces_at_its_partition_and_stops_dispatch() {
    const PARTITIONS: usize = 6;
    for scheduler in PER_CALL {
        for count in [1, 2, 3] {
            let gauges = Arc::new(Gauges::default());
            let load = {
                let gauges = Arc::clone(&gauges);
                Arc::new(move |index: usize, _stop: &AtomicBool| {
                    let (_running, value) = gauges.enter(index);
                    if index == 2 {
                        return Err(storage("injected partition load failure"));
                    }
                    Ok(value)
                })
            };
            let mut consumed = Vec::new();
            let error = run(
                scheduler,
                PARTITIONS,
                workers(count),
                load,
                |index, _: Value| {
                    consumed.push(index);
                    Ok(())
                },
                &never,
            )
            .unwrap_err()
            .to_string();
            let label = format!("{} workers={count}", scheduler.name());
            assert!(
                error.contains("injected partition load failure"),
                "{label}: {error}"
            );
            assert_eq!(consumed, vec![0, 1], "{label}");
            assert!(
                gauges.started.load(Ordering::SeqCst) <= 2 + count,
                "{label}"
            );
            assert!(gauges.at_rest(), "{label}");
        }
    }
}

#[test]
fn consume_error_returns_only_after_in_flight_loads_exit() {
    for scheduler in PARALLEL {
        let gauges = Arc::new(Gauges::default());
        let third_started = Arc::new(AtomicBool::new(false));
        let load = {
            let (gauges, third_started) = (Arc::clone(&gauges), Arc::clone(&third_started));
            Arc::new(move |index: usize, stop: &AtomicBool| {
                let (_running, value) = gauges.enter(index);
                if index >= 2 {
                    third_started.store(true, Ordering::SeqCst);
                    spin_until(|| stop.load(Ordering::Acquire), "the stop flag");
                    // Still running when the coordinator gives up: a return
                    // that does not join would leave this reading a spill.
                    std::thread::sleep(Duration::from_millis(20));
                    return Err(storage("abandoned"));
                }
                if index == 1 {
                    spin_until(
                        || third_started.load(Ordering::SeqCst),
                        "partition 2 to start",
                    );
                }
                Ok(value)
            })
        };
        let error = run(
            scheduler,
            6,
            workers(3),
            load,
            |index, _: Value| {
                if index == 1 {
                    return Err(storage("injected consume failure"));
                }
                Ok(())
            },
            &never,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("injected consume failure"), "{error}");
        assert!(!error.contains("abandoned"), "{error}");
        assert!(
            gauges.at_rest(),
            "{} returned before joining",
            scheduler.name()
        );
    }
}

/// Run `scheduler` with a panicking head load on a detached thread and report
/// what the caller saw within `limit`: `Ok(Err(message))` for a structured
/// error, `Ok(Ok(()))` for success, `Err("panic")` or `Err("no return")`.
fn outcome_of_a_panicking_load(
    scheduler: LoadScheduler<'static>,
    limit: Duration,
) -> Result<Result<(), String>, &'static str> {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let load = Arc::new(|index: usize, _stop: &AtomicBool| {
                if index == 0 {
                    std::thread::sleep(Duration::from_millis(20));
                    panic!("injected partition load panic");
                }
                Ok(index)
            });
            run(scheduler, 8, workers(2), load, |_, _| Ok(()), &never)
                .map_err(|error| error.to_string())
        }));
        let _ = sender.send(outcome.map_err(|_| "panic"));
    });
    receiver.recv_timeout(limit).unwrap_or(Err("no return"))
}

#[test]
fn a_panicking_load_is_a_structured_error_under_every_parallel_candidate() {
    // The production pool joined this list with #1564; before it, the same
    // input hung the pool forever.
    for scheduler in PARALLEL {
        let outcome = outcome_of_a_panicking_load(scheduler, SPIN_LIMIT);
        let Ok(Err(message)) = outcome else {
            panic!("{}: {outcome:?}", scheduler.name());
        };
        assert!(
            message.contains("partition load panicked"),
            "{}: {message}",
            scheduler.name()
        );
    }
}

#[test]
fn dropping_or_aborting_a_started_blocking_task_does_not_stop_it() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    let _entered = runtime.enter();
    for how in ["tokio-drop", "tokio-abort", "datafusion-drop"] {
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let task = {
            let (started, release, finished) = (
                Arc::clone(&started),
                Arc::clone(&release),
                Arc::clone(&finished),
            );
            move || {
                started.store(true, Ordering::SeqCst);
                spin_until(|| release.load(Ordering::SeqCst), "the release");
                finished.store(true, Ordering::SeqCst);
            }
        };
        match how {
            "tokio-drop" => {
                let handle = tokio::task::spawn_blocking(task);
                spin_until(|| started.load(Ordering::SeqCst), "the task to start");
                drop(handle);
            }
            "tokio-abort" => {
                let handle = tokio::task::spawn_blocking(task);
                spin_until(|| started.load(Ordering::SeqCst), "the task to start");
                handle.abort();
            }
            _ => {
                let handle = datafusion::common::runtime::SpawnedTask::spawn_blocking(task);
                spin_until(|| started.load(Ordering::SeqCst), "the task to start");
                drop(handle);
            }
        }
        // The handle is gone and the task is still running: whoever drops a
        // handle has returned while the work it owned continues.
        std::thread::sleep(Duration::from_millis(20));
        assert!(!finished.load(Ordering::SeqCst), "{how}");
        release.store(true, Ordering::SeqCst);
        spin_until(|| finished.load(Ordering::SeqCst), "the detached task");
    }
}

#[test]
fn blocking_on_a_runtime_from_inside_a_runtime_panics() {
    let outer = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let inner = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let nested =
        outer.block_on(async { catch_unwind(AssertUnwindSafe(|| inner.block_on(async {}))) });
    assert!(nested.is_err());
}

#[test]
fn tokio_candidates_refuse_to_nest_and_thread_candidates_do_not_care() {
    let outer = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    outer.block_on(async {
        for scheduler in PER_CALL {
            let load = Arc::new(|index: usize, _stop: &AtomicBool| Ok(index));
            let outcome = run(scheduler, 4, workers(2), load, |_, _| Ok(()), &never);
            match scheduler {
                LoadScheduler::Baseline | LoadScheduler::Rayon(_) => {
                    outcome.unwrap();
                }
                _ => {
                    let error = outcome.unwrap_err().to_string();
                    assert!(error.contains("inside an async runtime"), "{error}");
                }
            }
        }
    });
}

#[test]
fn a_shared_runtime_or_pool_bounds_loads_across_concurrent_imports() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_time()
        .build()
        .unwrap();
    let handle = runtime.handle();
    for (scheduler, bound) in [
        (LoadScheduler::Baseline, 4),
        (LoadScheduler::Rayon(Some(&pool)), 2),
        (LoadScheduler::TokioBlocking(Some(handle)), 2),
        (LoadScheduler::DataFusionSpawned(Some(handle)), 2),
    ] {
        let gauges = Arc::new(Gauges::default());
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let gauges = Arc::clone(&gauges);
                scope.spawn(move || {
                    let load = Arc::new(move |index: usize, _stop: &AtomicBool| {
                        let (_running, value) = gauges.enter(index);
                        std::thread::sleep(Duration::from_millis(5));
                        Ok(value)
                    });
                    run(
                        scheduler,
                        16,
                        workers(2),
                        load,
                        |_, _: Value| Ok(()),
                        &never,
                    )
                    .unwrap();
                });
            }
        });
        // Two imports of two workers each: independent pools admit four
        // loads; a shared pool or blocking cap admits its own size.
        assert!(
            gauges.peak_running.load(Ordering::SeqCst) <= bound,
            "{}",
            scheduler.name()
        );
        assert!(gauges.at_rest(), "{}", scheduler.name());
    }
}

mod datafusion_operators {
    //! What DataFusion's own partition operators do with a partitioned input,
    //! observed rather than read off their signatures.

    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::compute::SortOptions;
    use datafusion::error::DataFusionError;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
    };
    use std::fmt;

    /// `partitions` inputs, each one batch holding its own index, produced
    /// when first polled. Records how many were produced and on which threads.
    #[derive(Debug)]
    struct ProbeExec {
        schema: SchemaRef,
        props: Arc<PlanProperties>,
        produced: Arc<AtomicUsize>,
        threads: Arc<Mutex<HashSet<ThreadId>>>,
    }

    impl ProbeExec {
        fn new(partitions: usize) -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "partition",
                DataType::Int64,
                false,
            )]));
            let props = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self {
                schema,
                props,
                produced: Arc::new(AtomicUsize::new(0)),
                threads: Arc::new(Mutex::new(HashSet::new())),
            }
        }
    }

    impl DisplayAs for ProbeExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "ProbeExec")
        }
    }

    impl ExecutionPlan for ProbeExec {
        fn name(&self) -> &'static str {
            "ProbeExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.props
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            let (schema, produced, threads) = (
                Arc::clone(&self.schema),
                Arc::clone(&self.produced),
                Arc::clone(&self.threads),
            );
            let batch_schema = Arc::clone(&schema);
            let stream = futures::stream::once(async move {
                produced.fetch_add(1, Ordering::SeqCst);
                threads.lock().unwrap().insert(std::thread::current().id());
                let index = i64::try_from(partition).unwrap();
                RecordBatch::try_new(batch_schema, vec![Arc::new(Int64Array::from(vec![index]))])
                    .map_err(DataFusionError::from)
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }
    }

    const PARTITIONS: usize = 8;

    #[test]
    fn coalesce_partitions_starts_every_input_without_a_window() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();
        let probe = Arc::new(ProbeExec::new(PARTITIONS));
        let produced = Arc::clone(&probe.produced);
        let coalesce = CoalescePartitionsExec::new(probe);
        let _entered = runtime.enter();
        let stream = coalesce
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        // Nothing has consumed a single batch, yet every input is produced
        // and buffered: the only bound is the input partition count.
        spin_until(
            || produced.load(Ordering::SeqCst) == PARTITIONS,
            "every coalesced input",
        );
        drop(stream);
    }

    /// Order through `SortPreservingMergeExec`: the first merged row, how
    /// many inputs were produced by then, and the threads that produced them.
    fn first_merged_row(runtime: &tokio::runtime::Runtime) -> (i64, usize, HashSet<ThreadId>) {
        let probe = Arc::new(ProbeExec::new(PARTITIONS));
        let (produced, threads) = (Arc::clone(&probe.produced), Arc::clone(&probe.threads));
        let ordering = LexOrdering::new([PhysicalSortExpr::new(
            col("partition", &probe.schema).unwrap(),
            SortOptions::default(),
        )])
        .unwrap();
        let merge = SortPreservingMergeExec::new(ordering, probe);
        let (first, produced_at_first) = runtime.block_on(async {
            let mut stream = merge.execute(0, Arc::new(TaskContext::default())).unwrap();
            let first = stream.next().await.unwrap().unwrap();
            (first, produced.load(Ordering::SeqCst))
        });
        let first = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let threads = threads.lock().unwrap().clone();
        (first, produced_at_first, threads)
    }

    #[test]
    fn sort_preserving_merge_materializes_every_input_and_parallelizes_by_runtime_flavor() {
        let multi = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();
        let current = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        // Ordered output either way, and every input is materialized before
        // the first row: there is no window below the input count.
        let (first, produced, multi_threads) = first_merged_row(&multi);
        assert_eq!((first, produced), (0, PARTITIONS));
        let (first, produced, current_threads) = first_merged_row(&current);
        assert_eq!((first, produced), (0, PARTITIONS));
        // `spawn_buffered` spawns one task per input only on a multi-thread
        // runtime, so inputs are polled on runtime workers, never on the
        // caller; on a current-thread runtime every input is polled inline on
        // the caller. The same plan is parallel or serial depending on who
        // drives it.
        let caller = std::thread::current().id();
        assert!(!multi_threads.contains(&caller), "{multi_threads:?}");
        assert_eq!(current_threads, HashSet::from([caller]));
    }
}

mod real_finish {
    //! The candidates at the real call site: the same routed spills finished
    //! by `FixedRangePartitioner` under each scheduler.

    use super::*;
    use crate::graph_construction::partition::IdentitySampler;
    use crate::graph_construction::partition_load::tests::relabelled;
    use crate::graph_construction::partition_shaping::{FixedRangePartitioner, PartitionFamily};
    use crate::graph_construction::{GraphConstructionSession, hex, tests::open};
    use sha2::Digest;
    use std::ffi::OsStr;
    use std::io::Read;
    use std::path::Path;

    pub(super) fn splitmix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Distinct pseudo-random 16-byte identities, so every partition sort
    /// does real work.
    pub(super) fn keys(records: u64, seed: u64) -> Vec<[u8; 16]> {
        let mut state = seed;
        (0..records)
            .map(|index| {
                let high = u128::from(splitmix(&mut state)) << 64;
                // The low half carries the index, so keys are distinct.
                (high | u128::from(index)).to_be_bytes()
            })
            .collect()
    }

    pub(super) fn files(root: &Path) -> BTreeSet<String> {
        let mut found = BTreeSet::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    found.insert(path.strip_prefix(root).unwrap().display().to_string());
                }
            }
        }
        found
    }

    /// Outcome of one finish: output digest and relabelled evidence, or the
    /// error.
    pub(super) struct Finished {
        pub(super) outcome: Result<(String, serde_json::Value), String>,
        pub(super) finish: crate::concurrency_attribution::RegionConcurrency,
        pub(super) files_before: BTreeSet<String>,
        pub(super) files_after: BTreeSet<String>,
    }

    pub(super) fn finish(
        keys: &[[u8; 16]],
        requested_partitions: u32,
        count: usize,
        scheduler: Option<LoadScheduler<'static>>,
        cancel_after_polls: Option<usize>,
    ) -> Finished {
        let records = u64::try_from(keys.len()).unwrap();
        let root = tempfile::TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = open(&root, 0x1508);
        let GraphConstructionSession {
            root: session_root,
            checkpoint,
            ..
        } = &mut session;
        let mut sampler = IdentitySampler::new(requested_partitions, records).unwrap();
        for position in sampler.positions().collect::<Vec<_>>() {
            sampler
                .admit(keys[usize::try_from(position).unwrap()])
                .unwrap();
        }
        let plan = sampler.into_plan(requested_partitions).unwrap();
        assert!(plan.partitions() > 1, "{}", plan.partitions());
        let files_before = files(root.path());
        let mut partitioner = FixedRangePartitioner::<16>::new(
            session_root,
            PartitionFamily::Identities,
            plan.partitions(),
            None,
            true,
        )
        .unwrap()
        .with_load_workers(workers(count));
        if let Some(scheduler) = scheduler {
            partitioner = partitioner.with_load_scheduler(scheduler);
        }
        for key in keys {
            partitioner
                .route(&plan, key, key, &mut checkpoint.evidence)
                .unwrap();
        }
        let mut polls = 0_usize;
        let mut cancelled = || {
            polls += 1;
            cancel_after_polls.is_some_and(|limit| polls > limit)
        };
        let (finished, region) = crate::concurrency_attribution::measure(|| {
            partitioner.finish_optional(
                "staged-identities.run",
                0,
                false,
                &mut cancelled,
                &mut checkpoint.evidence,
            )
        });
        let outcome = finished.map_err(|error| error.to_string()).map(|output| {
            let output = output.unwrap();
            let mut bytes = Vec::new();
            session_root
                .open_child_file(OsStr::new(&output))
                .unwrap()
                .read_to_end(&mut bytes)
                .unwrap();
            assert_eq!(bytes.len() as u64, records * 16);
            (
                hex(&sha2::Sha256::digest(&bytes)),
                relabelled(&checkpoint.evidence),
            )
        });
        Finished {
            outcome,
            finish: region,
            files_before,
            files_after: files(root.path()),
        }
    }

    #[test]
    fn every_candidate_publishes_identical_bytes_and_evidence() {
        let keys = keys(4_096, 0x1508);
        let (expected_digest, expected_evidence) =
            finish(&keys, 16, 2, None, None).outcome.unwrap();
        for scheduler in PER_CALL {
            for count in [1, 2, 3] {
                let (digest, evidence) = finish(&keys, 16, count, Some(scheduler), None)
                    .outcome
                    .unwrap();
                let label = format!("{} workers={count}", scheduler.name());
                assert_eq!(digest, expected_digest, "output bytes differ: {label}");
                assert_eq!(evidence, expected_evidence, "evidence differs: {label}");
            }
        }
    }

    #[test]
    fn cancelling_a_real_finish_publishes_nothing_under_every_candidate() {
        let keys = keys(4_096, 0x1508);
        let mut expected: Option<Vec<String>> = None;
        for scheduler in PER_CALL {
            // Cancelled at the second poll: after partition 0 is consumed,
            // while later partitions are loaded or loading.
            let finished = finish(&keys, 16, 2, Some(scheduler), Some(1));
            let error = finished.outcome.unwrap_err();
            assert!(
                error.contains("construction cancelled"),
                "{}: {error}",
                scheduler.name()
            );
            let leftover = finished
                .files_after
                .difference(&finished.files_before)
                .cloned()
                .collect::<Vec<_>>();
            // The output and its temporary are gone. What remains is the
            // sealed, receipted partition spills that recovery owns: the
            // same set under every scheduler.
            assert!(
                leftover
                    .iter()
                    .all(|name| !name.contains("staged-identities")),
                "{}: {leftover:?}",
                scheduler.name()
            );
            match &expected {
                None => expected = Some(leftover),
                Some(expected) => assert_eq!(&leftover, expected, "{}", scheduler.name()),
            }
        }
    }
}

mod report {
    //! `#[ignore]`d: the measurements behind
    //! `docs/development/evidence/construction-scheduling-spike-1508.md`.
    //! Run alone in a quiet process, release build, on an admitted
    //! filesystem (`TMPDIR` on ext4/xfs/btrfs):
    //!
    //! ```text
    //! TMPDIR=<ext4 dir> cargo test --release -p graphforge-storage --lib \
    //!   scheduling_spike::tests::report -- --ignored --nocapture --test-threads=1
    //! ```
    //!
    //! Each line of output is one JSON observation.

    use super::real_finish::{finish, keys, splitmix};
    use super::*;
    use crate::concurrency_attribution::{RegionConcurrency, measure};
    use serde_json::json;

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn millis(nanos: u64) -> f64 {
        #[allow(clippy::cast_precision_loss, reason = "reporting")]
        let millis = nanos as f64 / 1e6;
        millis
    }

    fn region(region: &RegionConcurrency) -> serde_json::Value {
        json!({
            "wall_ms": millis(region.wall_nanos),
            "cpu_ms": millis(region.cpu_nanos),
            "effective_cores": region.effective_cores(),
        })
    }

    /// Rotate the candidate order per repetition so no candidate always runs
    /// first (cold) or last.
    fn rotated<T: Copy>(candidates: &[T], repetition: usize) -> Vec<T> {
        let mut order = candidates.to_vec();
        order.rotate_left(repetition % candidates.len());
        order
    }

    fn cpu_bound_sort(
        index: usize,
        elements: usize,
        stop: &AtomicBool,
    ) -> Result<Vec<u64>, GfError> {
        let mut state = 0x1508 ^ u64::try_from(index).unwrap();
        let mut values = Vec::with_capacity(elements);
        for position in 0..elements {
            if position % 4096 == 0 && stop.load(Ordering::Acquire) {
                return Err(storage("abandoned"));
            }
            values.push(splitmix(&mut state));
        }
        values.sort_unstable();
        Ok(values)
    }

    #[test]
    #[ignore = "measurement report; run alone in a quiet release process"]
    fn scheduling_spike_report() {
        let repetitions = env_usize("GF_1508_REPETITIONS", 5);
        println!(
            "{}",
            json!({
                "section": "host",
                "logical_cpus": std::thread::available_parallelism().map(NonZeroUsize::get).ok(),
                "repetitions": repetitions,
                "debug_assertions": cfg!(debug_assertions),
            })
        );

        // 1. CPU-bound loads: on-CPU parallelism by candidate.
        let elements = env_usize("GF_1508_SORT_ELEMENTS", 1 << 19);
        for count in [2, 4] {
            for repetition in 0..repetitions {
                for scheduler in rotated(&PER_CALL, repetition) {
                    let gauges = Arc::new(Gauges::default());
                    let load = {
                        let gauges = Arc::clone(&gauges);
                        Arc::new(move |index: usize, stop: &AtomicBool| {
                            let (_running, _value) = gauges.enter(index);
                            cpu_bound_sort(index, elements, stop)
                        })
                    };
                    let mut checksum = 0_u64;
                    let (outcome, measured) = measure(|| {
                        run(
                            scheduler,
                            32,
                            workers(count),
                            load,
                            |_, values: Vec<u64>| {
                                checksum ^= values[0] ^ values[values.len() - 1];
                                Ok(())
                            },
                            &never,
                        )
                    });
                    outcome.unwrap();
                    println!(
                        "{}",
                        json!({
                            "section": "cpu_sort",
                            "scheduler": scheduler.name(),
                            "workers": count,
                            "repetition": repetition,
                            "partitions": 32,
                            "elements_per_partition": elements,
                            "region": region(&measured),
                            "peak_running_loads": gauges.peak_running.load(Ordering::SeqCst),
                            "load_threads": gauges.threads.lock().unwrap().len(),
                            "checksum": format!("{checksum:016x}"),
                        })
                    );
                }
            }
        }

        // 2. The real fixed-partition finish over routed spills.
        let records = u64::try_from(env_usize("GF_1508_RECORDS", 1 << 22)).unwrap();
        let requested = u32::try_from(env_usize("GF_1508_PARTITIONS", 64)).unwrap();
        let identities = keys(records, 0x1508);
        for count in [2, 4] {
            for repetition in 0..repetitions {
                for scheduler in rotated(&PER_CALL, repetition) {
                    let finished = finish(&identities, requested, count, Some(scheduler), None);
                    let (digest, _) = finished.outcome.unwrap();
                    println!(
                        "{}",
                        json!({
                            "section": "real_finish",
                            "scheduler": scheduler.name(),
                            "workers": count,
                            "repetition": repetition,
                            "records": records,
                            "requested_partitions": requested,
                            "region": region(&finished.finish),
                            "output_sha256": digest,
                        })
                    );
                }
            }
        }

        // 3. Cancellation latency while the head partition is loading.
        for repetition in 0..repetitions {
            for scheduler in rotated(&PARALLEL, repetition) {
                let gauges = Arc::new(Gauges::default());
                let head_started = Arc::new(AtomicBool::new(false));
                let cancel = Arc::new(AtomicBool::new(false));
                let load =
                    head_load_until_stopped(&gauges, &head_started, Duration::from_millis(500));
                let cancelled_at = Arc::new(Mutex::new(None));
                let canceller = {
                    let (head_started, cancel, cancelled_at) = (
                        Arc::clone(&head_started),
                        Arc::clone(&cancel),
                        Arc::clone(&cancelled_at),
                    );
                    std::thread::spawn(move || {
                        spin_until(|| head_started.load(Ordering::SeqCst), "the head load");
                        std::thread::sleep(Duration::from_millis(50));
                        *cancelled_at.lock().unwrap() = Some(Instant::now());
                        cancel.store(true, Ordering::SeqCst);
                    })
                };
                let error = run(
                    scheduler,
                    8,
                    workers(2),
                    load,
                    |_, _: Value| {
                        if cancel.load(Ordering::SeqCst) {
                            return Err(cancelled_error());
                        }
                        Ok(())
                    },
                    &|| cancel.load(Ordering::SeqCst),
                )
                .unwrap_err();
                let returned = Instant::now();
                canceller.join().unwrap();
                let latency = returned - cancelled_at.lock().unwrap().unwrap();
                println!(
                    "{}",
                    json!({
                        "section": "cancel_latency",
                        "scheduler": scheduler.name(),
                        "repetition": repetition,
                        "head_load_bound_ms": 500,
                        "cancel_after_head_start_ms": 50,
                        "latency_ms": latency.as_secs_f64() * 1e3,
                        "joined": gauges.at_rest(),
                        "error": error.to_string(),
                    })
                );
            }
        }

        // 4. Two concurrent imports: admission across them.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(2)
            .enable_time()
            .build()
            .unwrap();
        let shared = [
            LoadScheduler::Baseline,
            LoadScheduler::Rayon(Some(&pool)),
            LoadScheduler::TokioBlocking(Some(runtime.handle())),
            LoadScheduler::DataFusionSpawned(Some(runtime.handle())),
        ];
        for repetition in 0..repetitions {
            for scheduler in rotated(&shared, repetition) {
                let gauges = Arc::new(Gauges::default());
                let (walls, measured) = measure(|| {
                    std::thread::scope(|scope| {
                        let imports = (0..2)
                            .map(|_| {
                                let gauges = Arc::clone(&gauges);
                                scope.spawn(move || {
                                    let load = Arc::new(move |index: usize, stop: &AtomicBool| {
                                        let (_running, _value) = gauges.enter(index);
                                        cpu_bound_sort(index, elements / 4, stop)
                                    });
                                    let started = Instant::now();
                                    run(scheduler, 16, workers(2), load, |_, _| Ok(()), &never)
                                        .unwrap();
                                    started.elapsed().as_secs_f64() * 1e3
                                })
                            })
                            .collect::<Vec<_>>();
                        imports
                            .into_iter()
                            .map(|import| import.join().unwrap())
                            .collect::<Vec<_>>()
                    })
                });
                println!(
                    "{}",
                    json!({
                        "section": "concurrent_imports",
                        "scheduler": scheduler.name(),
                        "repetition": repetition,
                        "imports": 2,
                        "workers_per_import": 2,
                        "region": region(&measured),
                        "import_wall_ms": walls,
                        "peak_running_loads": gauges.peak_running.load(Ordering::SeqCst),
                    })
                );
            }
        }

        // 5. A panicking load with the other worker alive.
        for scheduler in PER_CALL {
            let outcome = outcome_of_a_panicking_load(scheduler, Duration::from_secs(5));
            println!(
                "{}",
                json!({
                    "section": "load_panic",
                    "scheduler": scheduler.name(),
                    "outcome": format!("{outcome:?}"),
                })
            );
        }
    }
}
