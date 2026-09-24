use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const SPIN_LIMIT: Duration = Duration::from_secs(20);

/// Counts materialized values: a value exists from the start of its load
/// until the coordinator drops it after consuming it.
#[derive(Default)]
struct Gauge {
    live: AtomicUsize,
    peak: AtomicUsize,
}

impl Gauge {
    fn enter(&self) {
        let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
    }
}

struct Materialized<'a> {
    index: usize,
    gauge: &'a Gauge,
}

impl Drop for Materialized<'_> {
    fn drop(&mut self) {
        self.gauge.live.fetch_sub(1, Ordering::SeqCst);
    }
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

fn workers(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}

#[test]
fn slow_first_partition_is_consumed_first_and_holds_the_pool_at_the_bound() {
    const PARTITIONS: usize = 8;
    for count in [1, 2, 3, 8, 11] {
        let gauge = Gauge::default();
        // Loads other than partition 0 that completed while 0 was blocked.
        let completed = AtomicUsize::new(0);
        let completed_before_zero = AtomicUsize::new(usize::MAX);
        let slots = count.min(PARTITIONS);
        let load = |index: usize, _stop: &AtomicBool| {
            gauge.enter();
            if index == 0 {
                // Block until every other slot in the window has been loaded.
                // An unbounded reorder buffer (#1448) would let all seven
                // other partitions complete here; the window admits exactly
                // `slots - 1`, and the spin would time out if it admitted
                // fewer.
                spin_until(
                    || completed.load(Ordering::SeqCst) >= slots - 1,
                    "the other window slots to load",
                );
                completed_before_zero.store(completed.load(Ordering::SeqCst), Ordering::SeqCst);
            } else {
                completed.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Materialized {
                index,
                gauge: &gauge,
            })
        };
        let mut order = Vec::new();
        let consume = |index: usize, value: Materialized<'_>| {
            assert_eq!(value.index, index);
            assert!(
                gauge.live.load(Ordering::SeqCst) <= count,
                "workers={count}"
            );
            order.push(index);
            Ok(())
        };
        consume_in_partition_order(PARTITIONS, workers(count), load, &mut || false, consume)
            .unwrap();
        assert_eq!(
            order,
            (0..PARTITIONS).collect::<Vec<_>>(),
            "workers={count}"
        );
        assert_eq!(gauge.live.load(Ordering::SeqCst), 0, "workers={count}");
        // The bound is reached under skew and never exceeded.
        assert_eq!(gauge.peak.load(Ordering::SeqCst), slots, "workers={count}");
        assert_eq!(
            completed_before_zero.load(Ordering::SeqCst),
            slots - 1,
            "workers={count}"
        );
    }
}

#[test]
fn load_error_surfaces_at_its_partition_and_stops_dispatch() {
    const PARTITIONS: usize = 6;
    for count in [1, 2, 3] {
        let dispatched = AtomicUsize::new(0);
        let load = |index: usize, _stop: &AtomicBool| {
            dispatched.fetch_add(1, Ordering::SeqCst);
            if index == 2 {
                return Err(storage("injected partition load failure"));
            }
            Ok(index)
        };
        let mut consumed = Vec::new();
        let consume = |index: usize, value: usize| {
            assert_eq!(index, value);
            consumed.push(index);
            Ok(())
        };
        let error =
            consume_in_partition_order(PARTITIONS, workers(count), load, &mut || false, consume)
                .unwrap_err()
                .to_string();
        assert!(error.contains("injected partition load failure"), "{error}");
        assert_eq!(consumed, vec![0, 1], "workers={count}");
        // Dispatch never ran past the window ahead of the failing partition.
        assert!(
            dispatched.load(Ordering::SeqCst) <= 2 + count,
            "workers={count} dispatched={}",
            dispatched.load(Ordering::SeqCst)
        );
    }
}

#[test]
fn consume_error_stops_the_pool_and_abandoned_loads_are_never_observed() {
    const PARTITIONS: usize = 6;
    let abandoned = AtomicUsize::new(0);
    let third_started = AtomicBool::new(false);
    let load = |index: usize, stop: &AtomicBool| {
        if index >= 2 {
            // An in-flight load sees the coordinator stop and exits. A load
            // dispatched after the stop never starts, so only partition 2,
            // held in flight below, is certain to be abandoned.
            third_started.store(true, Ordering::SeqCst);
            spin_until(|| stop.load(Ordering::Acquire), "the stop flag");
            abandoned.fetch_add(1, Ordering::SeqCst);
            return Err(storage("abandoned"));
        }
        if index == 1 {
            // Three slots dispatch 0, 1 and 2 together; hold 1 until 2 is
            // genuinely in flight so the failure below abandons it.
            spin_until(
                || third_started.load(Ordering::SeqCst),
                "partition 2 to start",
            );
        }
        Ok(index)
    };
    let consume = |index: usize, _value: usize| {
        if index == 1 {
            return Err(storage("injected consume failure"));
        }
        Ok(())
    };
    let error = consume_in_partition_order(PARTITIONS, workers(3), load, &mut || false, consume)
        .unwrap_err()
        .to_string();
    assert!(error.contains("injected consume failure"), "{error}");
    assert!(!error.contains("abandoned"), "{error}");
    let abandoned = abandoned.load(Ordering::SeqCst);
    assert!((1..=2).contains(&abandoned), "abandoned={abandoned}");
}

#[test]
fn cancellation_while_the_head_load_runs_returns_promptly_and_joined() {
    // #1508 F15, as a contract: with a head load that runs up to 500 ms while
    // polling its stop flag, a cancel 50 ms in must not return 450 ms late.
    // The pre-#1581 pool noticed the cancel only at the consume boundary,
    // after the head load had run out its limit.
    const PARTITIONS: usize = 3;
    const CANCEL_AFTER: Duration = Duration::from_millis(50);
    const HEAD_LOAD_LIMIT: Duration = Duration::from_millis(500);
    const RETURN_BOUND: Duration = Duration::from_millis(250);

    let gauge = Gauge::default();
    let head_exited_via_stop = AtomicBool::new(false);
    let load = |index: usize, stop: &AtomicBool| {
        if index == 0 {
            // Hold the coordinator on the head partition while polling the
            // stop flag, like the production loads.
            let started = Instant::now();
            while !stop.load(Ordering::Acquire) && started.elapsed() < HEAD_LOAD_LIMIT {
                std::thread::yield_now();
            }
            if stop.load(Ordering::Acquire) {
                head_exited_via_stop.store(true, Ordering::SeqCst);
                return Err(storage("partition load abandoned after coordinator stop"));
            }
        }
        Ok(Materialized {
            index,
            gauge: &gauge,
        })
    };
    let mut consumed = Vec::new();
    let consume = |index: usize, value: Materialized<'_>| {
        assert_eq!(value.index, index);
        consumed.push(index);
        Ok(())
    };
    let started = Instant::now();
    let mut cancelled = || started.elapsed() >= CANCEL_AFTER;
    let outcome = consume_in_partition_order(PARTITIONS, workers(2), load, &mut cancelled, consume);
    let elapsed = started.elapsed();
    assert!(
        elapsed < RETURN_BOUND,
        "returned {elapsed:?} after a cancel {CANCEL_AFTER:?} in, bound {RETURN_BOUND:?}"
    );
    let error = outcome.unwrap_err().to_string();
    assert!(error.contains("construction cancelled"), "{error}");
    // The pool stopped the workers, and the head load saw it.
    assert!(
        head_exited_via_stop.load(Ordering::SeqCst),
        "the head load must observe the coordinator stop"
    );
    // Joined: nothing was consumed, no load is running (the scope joined) and
    // nothing is materialized, including the partition that was loaded but
    // never consumed.
    assert!(consumed.is_empty(), "consumed {consumed:?}");
    assert_eq!(gauge.live.load(Ordering::SeqCst), 0);
}

#[test]
fn abandon_polls_only_at_the_record_interval() {
    let stop = AtomicBool::new(true);
    abandon_if_stopped(1, &stop).unwrap();
    abandon_if_stopped(4095, &stop).unwrap();
    let error = abandon_if_stopped(4096, &stop).unwrap_err().to_string();
    assert!(
        error.contains("abandoned after coordinator stop"),
        "{error}"
    );
    stop.store(false, Ordering::Release);
    abandon_if_stopped(4096, &stop).unwrap();
}

#[test]
fn zero_partitions_is_a_no_op() {
    let load = |_index: usize, _stop: &AtomicBool| -> Result<(), GfError> {
        panic!("no partition to load")
    };
    let consume = |_index: usize, (): ()| panic!("no partition to consume");
    consume_in_partition_order(0, workers(3), load, &mut || false, consume).unwrap();
}

#[test]
fn merge_is_total_and_touches_nothing_else() {
    let counters = PartitionLoadCounters {
        spill_bytes: 3 * super::super::BLOCK_BYTES as u64 + 1,
        records: 5,
        read_bytes: 7,
        read_operations: 11,
        cache_release: graphforge_filesystem::FileCacheReleaseEvidence {
            sync_operations: 0,
            release_operations: 17,
            unsupported_operations: 19,
            released_bytes: 23,
            peak_window_bytes: 29,
        },
    };
    let mut evidence = GraphConstructionEvidence::default();
    counters.merge_into(&mut evidence).unwrap();
    counters.merge_into(&mut evidence).unwrap();
    let expected = GraphConstructionEvidence {
        merge_read_blocks: 8,
        merge_read_records: 10,
        merge_read_bytes: 14,
        merge_read_operations: 22,
        cache_release_operations: 34,
        cache_release_unsupported_operations: 38,
        cache_released_bytes: 46,
        peak_cache_release_window_bytes: 29,
        peak_partition_records: 5,
        ..GraphConstructionEvidence::default()
    };
    assert_eq!(evidence, expected);
}

#[test]
fn merge_refuses_disagreeing_read_bytes_and_operations() {
    let counters = PartitionLoadCounters {
        read_bytes: 1,
        read_operations: 0,
        ..PartitionLoadCounters::default()
    };
    let error = counters
        .merge_into(&mut GraphConstructionEvidence::default())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("read bytes and submissions disagree"),
        "{error}"
    );
}

#[test]
fn materialized_records_bound_scales_with_the_smaller_of_workers_and_partitions() {
    assert_eq!(materialized_records_bound(workers(1), 8, 100), Some(100));
    assert_eq!(materialized_records_bound(workers(2), 8, 100), Some(200));
    assert_eq!(materialized_records_bound(workers(11), 8, 100), Some(800));
    assert_eq!(materialized_records_bound(workers(2), 0, 100), Some(0));
    assert_eq!(materialized_records_bound(workers(2), 8, u64::MAX), None);
}

/// The evidence with native file identities relabelled in order of first
/// appearance in the transition log, so runs against different roots compare.
pub(super) fn relabelled(evidence: &GraphConstructionEvidence) -> serde_json::Value {
    let mut labels = BTreeMap::new();
    let mut label = |identity: &str| -> String {
        let next = labels.len();
        labels
            .entry(identity.to_owned())
            .or_insert_with(|| format!("identity-{next}"))
            .clone()
    };
    let mut copy = evidence.clone();
    copy.storage_allocation_transitions = evidence
        .storage_allocation_transitions
        .iter()
        .map(|transition| crate::StorageAllocationTransition {
            installed: transition
                .installed
                .iter()
                .map(|(identity, bytes)| (label(identity), *bytes))
                .collect(),
            removed: transition
                .removed
                .iter()
                .map(|identity| label(identity))
                .collect(),
        })
        .collect();
    copy.storage_active_identity_allocated_bytes = evidence
        .storage_active_identity_allocated_bytes
        .iter()
        .map(|(identity, bytes)| (label(identity), *bytes))
        .collect();
    serde_json::to_value(copy).unwrap()
}

/// Replay the ordered ledger and return the high-water mark of retained bytes
/// together with how many identities coexisted at that mark.
fn replay_peak(
    initial_bytes: u64,
    initial_identities: usize,
    transitions: &[crate::StorageAllocationTransition],
) -> (u64, usize) {
    let mut bytes = initial_bytes;
    let mut identities = initial_identities;
    let mut peak = (bytes, identities);
    for transition in transitions {
        for installed in transition.installed.values() {
            bytes += installed;
            identities += 1;
        }
        if bytes > peak.0 {
            peak = (bytes, identities);
        }
        identities -= transition.removed.len();
        // Removed bytes are looked up from the install that retained them.
        for removed in &transition.removed {
            let installed = transitions
                .iter()
                .find_map(|earlier| earlier.installed.get(removed))
                .expect("removed identity was installed");
            bytes -= installed;
        }
    }
    peak
}

#[test]
fn fixed_partition_finish_is_schedule_independent_across_worker_counts() {
    use super::super::GraphConstructionSession;
    use super::super::partition::IdentitySampler;
    use super::super::partition_shaping::{FixedRangePartitioner, PartitionFamily};
    use sha2::Digest;
    use std::ffi::OsStr;
    use std::io::Read;

    const RECORDS: u64 = 4_096;
    const REQUESTED_PARTITIONS: u32 = 16;
    let keys = (0..RECORDS)
        .map(|index| u128::from(index + 1).to_be_bytes())
        .collect::<Vec<_>>();
    let mut baseline: Option<(serde_json::Value, String, u64)> = None;
    for count in [1, 2, 3, 5] {
        let root = tempfile::TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = super::super::tests::open(&root, 0x1456 + count as u128);
        let GraphConstructionSession {
            root: session_root,
            checkpoint,
            ..
        } = &mut session;
        let before = checkpoint.evidence.clone();
        let mut sampler = IdentitySampler::new(REQUESTED_PARTITIONS, RECORDS).unwrap();
        let positions = sampler.positions().collect::<Vec<_>>();
        for position in positions {
            sampler
                .admit(keys[usize::try_from(position).unwrap()])
                .unwrap();
        }
        let plan = sampler.into_plan(REQUESTED_PARTITIONS).unwrap();
        assert!(plan.partitions() > 1, "{}", plan.partitions());
        let mut partitioner = FixedRangePartitioner::<16>::new(
            session_root,
            PartitionFamily::Identities,
            plan.partitions(),
            None,
            true,
        )
        .unwrap()
        .with_load_workers(workers(count));
        for key in &keys {
            partitioner
                .route(&plan, key, key, &mut checkpoint.evidence)
                .unwrap();
        }
        let output = partitioner
            .finish_optional(
                "staged-identities.run",
                0,
                false,
                &mut || false,
                &mut checkpoint.evidence,
            )
            .unwrap()
            .unwrap();
        let mut bytes = Vec::new();
        session_root
            .open_child_file(OsStr::new(&output))
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes.len() as u64, RECORDS * 16, "workers={count}");
        let digest = super::super::hex(&sha2::Sha256::digest(&bytes));
        let after = &checkpoint.evidence;

        // Plain sums are what a sequential pass credits: every record routed
        // and read back once (this fixture routes from memory, so the only
        // descriptor reads are the partition spills), and every spill byte
        // read exactly once.
        assert_eq!(after.merge_read_records, RECORDS * 2, "workers={count}");
        assert_eq!(after.merge_read_bytes, RECORDS * 16, "workers={count}");
        assert_eq!(after.partition_rows, RECORDS, "workers={count}");
        assert_eq!(after.partition_outputs, 1, "workers={count}");
        assert!(after.peak_partition_records > 0, "workers={count}");

        // The ordered ledger still proves coexistence: replaying it reaches
        // the recorded exact peak with every spill and the output retained.
        let appended =
            &after.storage_allocation_transitions[before.storage_allocation_transitions.len()..];
        let (replayed_peak, coexisting) = replay_peak(
            before
                .storage_active_identity_allocated_bytes
                .values()
                .sum(),
            before.storage_active_identity_allocated_bytes.len(),
            appended,
        );
        assert_eq!(
            after.storage_transient_peak_total_allocated_bytes,
            replayed_peak.max(before.storage_transient_peak_total_allocated_bytes),
            "workers={count}"
        );
        assert!(coexisting >= 2, "workers={count} coexisting={coexisting}");

        // The reservation bound is the same figure for every schedule, and it
        // covers the window the scheduler enforces.
        let bound = materialized_records_bound(
            PARTITION_LOAD_WORKERS,
            plan.partitions(),
            after.peak_partition_records,
        )
        .unwrap();
        assert!(bound >= after.peak_partition_records, "workers={count}");

        let fingerprint = relabelled(after);
        match &baseline {
            None => baseline = Some((fingerprint, digest, bound)),
            Some((expected, expected_digest, expected_bound)) => {
                assert_eq!(
                    &digest, expected_digest,
                    "output bytes differ at workers={count}"
                );
                assert_eq!(&bound, expected_bound, "bound differs at workers={count}");
                assert_eq!(
                    &fingerprint, expected,
                    "evidence differs at workers={count}"
                );
            }
        }
    }
}

#[test]
fn failed_consume_cannot_admit_a_replacement_partition() {
    let shared = Shared::<()> {
        state: Mutex::new(State {
            next: 3,
            released: 0,
            ready: BTreeMap::new(),
            live_workers: 3,
        }),
        changed: Condvar::new(),
        stop: AtomicBool::new(false),
    };
    let error = shared
        .release_consumed(Err(storage("injected consume failure")))
        .unwrap_err();
    assert!(error.to_string().contains("injected consume failure"));
    {
        let state = shared.lock();
        assert_eq!(
            state.released, 0,
            "a failed consume must retain the full dispatch window"
        );
        assert!(
            state.next >= state.released + 3,
            "replacement work must remain inadmissible"
        );
    }
    shared.release_consumed(Ok(())).unwrap();
    let state = shared.lock();
    assert_eq!(state.released, 1);
    assert!(
        state.next < state.released + 3,
        "successful consumption admits the next partition"
    );
}

/// Run `consume_in_partition_order` on a detached thread and wait at most
/// `SPIN_LIMIT` for it: a regression to the #1564 hang fails the test rather
/// than hanging the suite. `None` means it never returned; `Some(Err(_))` means
/// it panicked on the caller.
fn bounded<R: Send + 'static>(
    run: impl FnOnce() -> R + Send + 'static,
) -> Option<std::thread::Result<R>> {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        let _ = sender.send(outcome);
    });
    receiver.recv_timeout(SPIN_LIMIT).ok()
}

/// Loads running now and values not yet dropped, shared with a detached pool.
#[derive(Default)]
struct Joined {
    running: AtomicUsize,
    live: AtomicUsize,
}

struct Held(std::sync::Arc<Joined>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}

#[test]
fn a_panicking_load_fails_the_pool_instead_of_hanging_it() {
    const PARTITIONS: usize = 8;
    for count in [1, 2, 3] {
        let joined = std::sync::Arc::new(Joined::default());
        let pool = std::sync::Arc::clone(&joined);
        let outcome = bounded(move || {
            let load = |index: usize, _stop: &AtomicBool| {
                pool.running.fetch_add(1, Ordering::SeqCst);
                pool.live.fetch_add(1, Ordering::SeqCst);
                let held = Held(std::sync::Arc::clone(&pool));
                // Hold the panic until the other workers are genuinely in
                // flight, which is the state that used to hang.
                std::thread::sleep(Duration::from_millis(20));
                pool.running.fetch_sub(1, Ordering::SeqCst);
                assert!(index != 0, "injected partition load panic");
                Ok(held)
            };
            consume_in_partition_order(PARTITIONS, workers(count), load, &mut || false, |_, _| {
                Ok(())
            })
            .map_err(|error| error.to_string())
        });
        let Some(Ok(Err(error))) = outcome else {
            panic!("workers={count}: expected a structured error, got {outcome:?}");
        };
        assert!(
            error.contains("partition load panicked"),
            "workers={count}: {error}"
        );
        // Joined: every worker exited and every loaded value was dropped
        // before the pool returned.
        assert_eq!(joined.running.load(Ordering::SeqCst), 0, "workers={count}");
        assert_eq!(joined.live.load(Ordering::SeqCst), 0, "workers={count}");
    }
}

#[test]
fn a_panicking_consume_resumes_on_the_caller_after_the_workers_exit() {
    let joined = std::sync::Arc::new(Joined::default());
    let pool = std::sync::Arc::clone(&joined);
    let outcome = bounded(move || {
        let load = |_index: usize, _stop: &AtomicBool| {
            pool.running.fetch_add(1, Ordering::SeqCst);
            pool.live.fetch_add(1, Ordering::SeqCst);
            let held = Held(std::sync::Arc::clone(&pool));
            pool.running.fetch_sub(1, Ordering::SeqCst);
            Ok(held)
        };
        let consume = |index: usize, _held: Held| {
            assert!(index != 1, "injected consume panic");
            Ok(())
        };
        consume_in_partition_order(8, workers(3), load, &mut || false, consume)
    });
    // The caller's panic is the caller's to see, not a hang and not an error.
    assert!(matches!(outcome, Some(Err(_))), "{outcome:?}");
    assert_eq!(joined.running.load(Ordering::SeqCst), 0);
    assert_eq!(joined.live.load(Ordering::SeqCst), 0);
}
