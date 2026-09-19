//! Bounded, calling-thread region trees. Process CPU includes all process threads.
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;

use serde::Serialize;

use super::{
    process_cpu_time,
    scheduler::{self, SchedulerSample},
};

thread_local! {
    static ACTIVE: RefCell<Vec<Rc<RefCell<State>>>> = const { RefCell::new(Vec::new()) };
}

/// Boundary samples in nanoseconds. Proc reads are sequential, not atomic.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RegionMeasurement {
    /// Elapsed time. Never sum inclusive parents and children.
    pub wall_ns: u64,
    /// Upper bound for the two sequential sampling windows, excluding CPU tick quantization.
    pub sampling_uncertainty_ns: u64,
    /// All process threads, including unrelated work; Linux has 10 ms resolution.
    pub process_cpu_ns: Option<u64>,
    /// Calling-thread on-CPU runtime from Linux schedstat, not pool occupancy.
    pub thread_running_ns: Option<u64>,
    /// Calling-thread runqueue delay; unavailable when schedstats is disabled.
    pub thread_runnable_ns: Option<u64>,
    /// Measured non-runnable sleep, including uninterruptible blocking. Not PSI.
    pub thread_sleeping_ns: Option<u64>,
    /// Uninterruptible subset of sleeping; never add this to sleeping.
    pub thread_uninterruptible_ns: Option<u64>,
    /// I/O-wait subset of uninterruptible time, not a syscall classifier.
    pub thread_iowait_ns: Option<u64>,
    /// Unclassified wall remainder, not a measurement of blocking or process inactivity.
    pub thread_unknown_ns: Option<u64>,
}

impl RegionMeasurement {
    fn add(&mut self, other: &Self) {
        self.wall_ns = self.wall_ns.saturating_add(other.wall_ns);
        self.sampling_uncertainty_ns = self
            .sampling_uncertainty_ns
            .saturating_add(other.sampling_uncertainty_ns);
        self.process_cpu_ns = add(self.process_cpu_ns, other.process_cpu_ns);
        self.thread_running_ns = add(self.thread_running_ns, other.thread_running_ns);
        self.thread_runnable_ns = add(self.thread_runnable_ns, other.thread_runnable_ns);
        self.thread_sleeping_ns = add(self.thread_sleeping_ns, other.thread_sleeping_ns);
        self.thread_uninterruptible_ns = add(
            self.thread_uninterruptible_ns,
            other.thread_uninterruptible_ns,
        );
        self.thread_iowait_ns = add(self.thread_iowait_ns, other.thread_iowait_ns);
        self.thread_unknown_ns = add(self.thread_unknown_ns, other.thread_unknown_ns);
    }

    fn residual(&self, children: &Self) -> Self {
        Self {
            wall_ns: self.wall_ns.saturating_sub(children.wall_ns),
            sampling_uncertainty_ns: self
                .sampling_uncertainty_ns
                .saturating_add(children.sampling_uncertainty_ns),
            process_cpu_ns: subtract(self.process_cpu_ns, children.process_cpu_ns),
            thread_running_ns: subtract(self.thread_running_ns, children.thread_running_ns),
            thread_runnable_ns: subtract(self.thread_runnable_ns, children.thread_runnable_ns),
            thread_sleeping_ns: subtract(self.thread_sleeping_ns, children.thread_sleeping_ns),
            thread_uninterruptible_ns: subtract(
                self.thread_uninterruptible_ns,
                children.thread_uninterruptible_ns,
            ),
            thread_iowait_ns: subtract(self.thread_iowait_ns, children.thread_iowait_ns),
            thread_unknown_ns: subtract(self.thread_unknown_ns, children.thread_unknown_ns),
        }
    }
}

fn add(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    left?.checked_add(right?)
}

fn subtract(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    left?.checked_sub(right?)
}

fn zero() -> RegionMeasurement {
    RegionMeasurement {
        wall_ns: 0,
        sampling_uncertainty_ns: 0,
        process_cpu_ns: Some(0),
        thread_running_ns: Some(0),
        thread_runnable_ns: Some(0),
        thread_sleeping_ns: Some(0),
        thread_uninterruptible_ns: Some(0),
        thread_iowait_ns: Some(0),
        thread_unknown_ns: Some(0),
    }
}

/// One path in the region tree. Repeated sequential calls aggregate at that path.
#[derive(Clone, Debug, Serialize)]
pub struct RegionRow {
    /// Number of completed scopes, including failed operations.
    pub calls: u64,
    /// Parent includes descendants; this is the CPU/wall execution scope.
    pub inclusive: RegionMeasurement,
    /// Parent minus immediate children; sum residuals, never inclusive rows.
    pub residual: RegionMeasurement,
    /// Successfully completed work, kept separate from time and concurrency.
    pub work: BTreeMap<String, u64>,
}

/// Non-durable diagnostics for one invocation on the capturing thread.
#[derive(Clone, Debug, Serialize)]
pub struct RegionSnapshot {
    /// Versioned observation contract.
    pub contract: &'static str,
    /// Process CPU is shared; worker thread scopes are not captured by this tree.
    pub cpu_scope: &'static str,
    /// Scheduler counters cover the calling thread, not the process or PSI.
    pub scheduler_scope: &'static str,
    /// Slash-separated static region paths; no user input, paths or identifiers.
    pub regions: BTreeMap<String, RegionRow>,
    /// False when guards were closed out of order; incomplete trees have no rows.
    pub complete: bool,
}

#[derive(Debug, Default)]
struct State {
    stack: Vec<(u64, String, RegionMeasurement)>,
    next_id: u64,
    invalid: bool,
    rows: BTreeMap<String, RegionRow>,
    work: BTreeMap<String, BTreeMap<String, u64>>,
}

/// Isolated, thread-bound capture of nested lifecycle regions.
///
/// Nested captures restore the previous capture. Captures never reset or
/// difference a global counter. Spawned worker threads are intentionally not
/// attributed as though their process CPU deltas were disjoint.
pub struct RegionCapture {
    state: Rc<RefCell<State>>,
    root: Option<CaptureRegion>,
}

impl RegionCapture {
    /// Start a capture with a static root name. Finish after child guards drop.
    #[must_use]
    pub fn start(name: &'static str) -> Self {
        let state = Rc::new(RefCell::new(State::default()));
        ACTIVE.with(|active| active.borrow_mut().push(state.clone()));
        let root = CaptureRegion::enter(name);
        Self { state, root }
    }

    /// Finish the root and return the complete tree, including explicit residuals.
    #[must_use]
    pub fn finish(mut self) -> RegionSnapshot {
        drop(self.root.take());
        RegionSnapshot {
            contract: "graphforge-region-diagnostics/1",
            cpu_scope: "shared_process_inclusive_do_not_sum",
            scheduler_scope: "calling_thread_only_unknown_is_not_blocked_or_psi",
            regions: self.state.borrow().rows.clone(),
            complete: !self.state.borrow().invalid,
        }
    }
}

impl Drop for RegionCapture {
    fn drop(&mut self) {
        drop(self.root.take());
        ACTIVE.with(|active| {
            active
                .borrow_mut()
                .retain(|state| !Rc::ptr_eq(state, &self.state));
        });
    }
}

#[derive(Debug)]
pub(super) struct CaptureRegion {
    state: Rc<RefCell<State>>,
    sample: Sample,
    id: u64,
}

impl CaptureRegion {
    pub(super) fn enter(name: &'static str) -> Option<Self> {
        ACTIVE.with(|active| {
            let state = active.borrow().last()?.clone();
            if state.borrow().invalid {
                return None;
            }
            let path = state.borrow().stack.last().map_or_else(
                || name.to_owned(),
                |(_, parent, _)| format!("{parent}/{name}"),
            );
            if state.borrow().stack.len() >= 16
                || (state.borrow().rows.len() >= 256 && !state.borrow().rows.contains_key(&path))
            {
                let mut state = state.borrow_mut();
                state.invalid = true;
                state.rows.clear();
                state.work.clear();
                return None;
            }
            let id = state.borrow().next_id;
            state.borrow_mut().next_id += 1;
            state.borrow_mut().stack.push((id, path, zero()));
            Some(Self {
                state,
                sample: Sample::now(),
                id,
            })
        })
    }
}

impl Drop for CaptureRegion {
    fn drop(&mut self) {
        let measured = self.sample.elapsed();
        let mut state = self.state.borrow_mut();
        if state.invalid || state.stack.last().is_none_or(|frame| frame.0 != self.id) {
            state.invalid = true;
            state.rows.clear();
            state.stack.clear();
            return;
        }
        let (_, path, children) = state.stack.pop().expect("checked above");
        let residual = measured.residual(&children);
        if let Some((_, _, parent_children)) = state.stack.last_mut() {
            parent_children.add(&measured);
        }
        if state.rows.len() >= 256 && !state.rows.contains_key(&path) {
            state.invalid = true;
            state.rows.clear();
            state.work.clear();
            return;
        }
        let work = state.work.remove(&path).unwrap_or_default();
        let row = state.rows.entry(path).or_insert_with(|| RegionRow {
            calls: 0,
            inclusive: zero(),
            residual: zero(),
            work: BTreeMap::new(),
        });
        for (unit, count) in work {
            let total = row.work.entry(unit).or_default();
            *total = total.saturating_add(count);
        }
        row.calls = row.calls.saturating_add(1);
        row.inclusive.add(&measured);
        row.residual.add(&residual);
    }
}

pub(super) fn record_work(unit: &'static str, amount: u64) {
    ACTIVE.with(|active| {
        let Some(state) = active.borrow().last().cloned() else {
            return;
        };
        let mut state = state.borrow_mut();
        if state.invalid {
            return;
        }
        let Some((_, path, _)) = state.stack.last() else {
            return;
        };
        let path = path.clone();
        let total = state
            .work
            .entry(path)
            .or_default()
            .entry(unit.to_owned())
            .or_default();
        *total = total.saturating_add(amount);
    });
}

#[derive(Debug)]
struct Sample {
    wall: Instant,
    cpu: Option<u64>,
    scheduler: SchedulerSample,
    sampling_ns: u64,
}

impl Sample {
    fn now() -> Self {
        let wall = Instant::now();
        let cpu = process_cpu_time().and_then(|d| u64::try_from(d.as_nanos()).ok());
        let scheduler = scheduler::sample();
        Self {
            wall,
            cpu,
            scheduler,
            sampling_ns: u64::try_from(wall.elapsed().as_nanos()).unwrap_or(u64::MAX),
        }
    }

    fn elapsed(&self) -> RegionMeasurement {
        let end = Self::now();
        let wall_ns =
            u64::try_from(end.wall.duration_since(self.wall).as_nanos()).unwrap_or(u64::MAX);
        let running = subtract(end.scheduler.running, self.scheduler.running);
        let runnable = subtract(end.scheduler.runnable, self.scheduler.runnable);
        let sleeping = subtract(end.scheduler.sleeping, self.scheduler.sleeping);
        RegionMeasurement {
            wall_ns,
            sampling_uncertainty_ns: self.sampling_ns.saturating_add(end.sampling_ns),
            process_cpu_ns: subtract(end.cpu, self.cpu),
            thread_running_ns: running,
            thread_runnable_ns: runnable,
            thread_sleeping_ns: sleeping,
            thread_uninterruptible_ns: subtract(
                end.scheduler.uninterruptible,
                self.scheduler.uninterruptible,
            ),
            thread_iowait_ns: subtract(end.scheduler.iowait, self.scheduler.iowait),
            thread_unknown_ns: running.zip(Some(runnable.unwrap_or(0))).and_then(|(r, q)| {
                wall_ns
                    .checked_sub(r)?
                    .checked_sub(q)?
                    .checked_sub(sleeping.unwrap_or(0))
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrency_attribution::RegionScope;

    #[test]
    fn nested_regions_reconcile_without_double_counting() {
        let capture = RegionCapture::start("ingest");
        {
            let _validate = RegionScope::named("validate");
            let _seal = RegionScope::named("seal");
            std::hint::black_box((0..1000_u64).sum::<u64>());
        }
        let snapshot = capture.finish();
        let root = &snapshot.regions["ingest"].inclusive;
        assert_eq!(
            snapshot
                .regions
                .values()
                .map(|r| r.residual.wall_ns)
                .sum::<u64>(),
            root.wall_ns
        );
        if let Some(cpu) = root.process_cpu_ns {
            assert_eq!(
                snapshot
                    .regions
                    .values()
                    .map(|r| r.residual.process_cpu_ns.unwrap())
                    .sum::<u64>(),
                cpu
            );
        }
        assert!(
            !serde_json::to_string(&snapshot)
                .unwrap()
                .contains("serial_fraction")
        );
    }

    #[test]
    fn captures_are_isolated_and_restore_the_outer_capture() {
        let outer = RegionCapture::start("outer");
        let inner = RegionCapture::start("inner");
        assert_eq!(inner.finish().regions.len(), 1);
        {
            let _scope = RegionScope::named("after");
        }
        let snapshot = outer.finish();
        assert_eq!(snapshot.regions.len(), 2);
        assert!(snapshot.regions.contains_key("outer/after"));
        assert!(!snapshot.regions.contains_key("inner"));
    }

    #[test]
    fn active_parent_rows_also_count_toward_the_capture_bound() {
        let capture = RegionCapture::start("root");
        for index in 0..256 {
            let name = Box::leak(format!("child{index}").into_boxed_str());
            let _scope = RegionScope::named(name);
        }
        let report = capture.finish();
        assert!(!report.complete);
        assert!(report.regions.is_empty());
    }

    #[test]
    fn misordered_guards_refuse_plausible_but_wrong_rows() {
        let capture = RegionCapture::start("root");
        let a = RegionScope::named("a");
        let b = RegionScope::named("b");
        drop(a);
        drop(b);
        let report = capture.finish();
        assert!(!report.complete);
        assert!(report.regions.is_empty());
        let capture = RegionCapture::start("root");
        let child = RegionScope::named("child");
        assert!(!capture.finish().complete);
        drop(child);
    }

    #[test]
    fn misordered_captures_do_not_resurrect_a_completed_capture() {
        let outer = RegionCapture::start("outer");
        let inner = RegionCapture::start("inner");
        drop(outer);
        assert!(inner.finish().complete);
        assert!(RegionScope::named("after").is_none());
    }
}
