//! Process CPU against elapsed wall time, per lifecycle region.
//!
//! # Why
//! #1387 budgets a *serialized fraction* of the ingest path, and nothing on that
//! path computes one (#1462). `effective_cores` existed only in
//! `benches/m6_storage_io.rs`, so no receipt, ladder rung or test could report
//! it, and every "the ingest path is ~68-80% serial" figure in the plan is
//! inferred from phase totals rather than measured.
//!
//! # What this measures, and what it does not
//! **Effective cores** is process CPU divided by elapsed wall for a region:
//! `1.0` means the region ran on one core's worth of CPU. Both terms are
//! measured, so the ratio is a measurement.
//!
//! **It conflates two different things, and that matters.** A region that is
//! perfectly serial and a region that is perfectly parallel but blocked on I/O
//! both report a low value. Effective cores answers "how much of this machine
//! did we use", which is the number #1387 quotes; it does not by itself
//! establish *why* the rest was idle. Read it with the phase's I/O attribution,
//! not instead of it.
//!
//! **Serial fraction is derived, not measured.** [`serial_fraction`] inverts
//! Amdahl's law for a matched throughput speedup and known worker count.
//! It inherits Amdahl's assumptions and
//! is only as good as the worker count handed to it, so it is reported as an
//! estimate, never from CPU/wall. Stock region receipts do not emit it.
//!
//! # This is a process-level measurement
//! `process_cpu_time` is process-wide, so a region's CPU delta includes **every
//! thread in the process**, including work unrelated to the region. That is the
//! right semantics for its purpose — a ladder rung runs one phase per
//! single-purpose process invocation, and the question is how much of the
//! machine that phase used — but it means a region is only meaningful when the
//! process is doing one thing.
//!
//! Measured while writing the calibration below: a region that does nothing but
//! `sleep` reports **4.9 effective cores** when other tests run concurrently in
//! the same process. The number is not wrong, it is answering a different
//! question. The timing calibrations are therefore `#[ignore]`d and must run in
//! a quiet process; they are the known-positive check #1462 requires, not a
//! gate that can run under `cargo test`'s default parallelism. This is the same
//! shape as #1460 and the reason that issue exists.
//!
//! # Engine independence
//! Regions are timed at their boundaries through [`measure`] and
//! [`RegionScope`]. Nothing here reaches into the partitioner, the spill
//! machinery or any other construction internal, so the instrument survives the
//! engine replacement in #1456 rather than dying with the code it instruments.
//!
//! # Running the calibration
//! ```text
//! cargo test -p graphforge-storage --lib concurrency_attribution \
//!     -- --ignored --test-threads=1
//! ```
//!
//! # Cost
//! Boundary reads of Linux proc counters and `Instant`, per region. Captures
//! report their sampling windows; they are not atomic or zero-overhead.
//! Named scopes are inert without a capture. Global snapshots are inclusive
//! shared-process totals and must never be summed across overlapping phases.

mod capture;
mod scheduler;
pub use capture::{RegionCapture, RegionMeasurement, RegionRow, RegionSnapshot};

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::storage_attribution::StorageIoPhase;

/// Elapsed wall and process CPU for one region.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionConcurrency {
    /// Elapsed wall-clock nanoseconds.
    pub wall_nanos: u64,
    /// Process CPU nanoseconds (user + system) consumed during the region.
    pub cpu_nanos: u64,
    /// Whether process CPU was available on this platform.
    pub cpu_available: bool,
}

impl RegionConcurrency {
    /// Process CPU divided by elapsed wall: how many cores' worth the region used.
    ///
    /// `None` when CPU is unavailable or no wall time elapsed, rather than a
    /// fabricated zero — a caller must be able to tell "not measured" from
    /// "measured as idle".
    #[must_use]
    pub fn effective_cores(&self) -> Option<f64> {
        if !self.cpu_available || self.wall_nanos == 0 {
            return None;
        }
        #[allow(clippy::cast_precision_loss, reason = "reporting-only ratio")]
        Some(self.cpu_nanos as f64 / self.wall_nanos as f64)
    }

    /// Fold another region's totals into this one.
    pub fn merge(&mut self, other: &Self) {
        self.wall_nanos = self.wall_nanos.saturating_add(other.wall_nanos);
        self.cpu_nanos = self.cpu_nanos.saturating_add(other.cpu_nanos);
        self.cpu_available &= other.cpu_available;
    }
}

/// The serialized fraction implied by an observed speedup on `workers` workers.
///
/// Inverts `S = 1 / (s + (1 - s) / n)`. **Derived, not measured** — see the
/// module docs. Returns `None` when the inversion is not defined (one worker or
/// fewer, non-positive speedup) or when the result falls outside `0..=1`, which
/// means the observation does not fit the model rather than that the region is
/// perfectly parallel.
#[must_use]
pub fn serial_fraction(speedup: f64, workers: u32) -> Option<f64> {
    if workers <= 1 || speedup <= 0.0 {
        return None;
    }
    let n = f64::from(workers);
    let fraction = (1.0 / speedup - 1.0 / n) / (1.0 - 1.0 / n);
    (0.0..=1.0).contains(&fraction).then_some(fraction)
}

/// Process CPU time (user + system) sampled at a point, where available.
///
/// Reads `/proc/self/stat` rather than calling `getrusage`, because this crate
/// is `#![forbid(unsafe_code)]` and the FFI call cannot be made here. The two
/// are equivalent for this purpose: `utime + stime` in `/proc/self/stat` is
/// process-wide across threads, exactly as `RUSAGE_SELF` is.
///
/// Returns `None` on any parse failure rather than a partial value — a caller
/// must be able to tell "not measured" from "measured as zero".
#[cfg(target_os = "linux")]
#[must_use]
pub fn process_cpu_time() -> Option<Duration> {
    parse_proc_stat_cpu(&std::fs::read_to_string("/proc/self/stat").ok()?)
}

/// `utime + stime` from the body of a `/proc/<pid>/stat` line.
///
/// The second field is the executable name in parentheses and may itself
/// contain spaces and parentheses, so fields are counted from the *last* `)`.
/// After it, index 0 is `state` (field 3), which puts `utime` (field 14) at
/// index 11 and `stime` (field 15) at index 12.
#[cfg(target_os = "linux")]
fn parse_proc_stat_cpu(stat: &str) -> Option<Duration> {
    /// Times in `/proc` are reported in `USER_HZ`, which the Linux `/proc` ABI
    /// fixes at 100 regardless of the kernel's internal `CONFIG_HZ`.
    const USER_HZ: u64 = 100;

    let after_comm = &stat[stat.rfind(')')? + 1..];
    let fields = after_comm.split_whitespace().collect::<Vec<_>>();
    let utime = fields.get(11)?.parse::<u64>().ok()?;
    let stime = fields.get(12)?.parse::<u64>().ok()?;
    let ticks = utime.checked_add(stime)?;
    Some(Duration::new(
        ticks / USER_HZ,
        u32::try_from((ticks % USER_HZ) * (1_000_000_000 / USER_HZ)).ok()?,
    ))
}

/// Platforms without a `/proc` process-CPU surface report CPU unavailable, and
/// every derived figure degrades to `None` rather than to a fabricated zero.
#[cfg(not(target_os = "linux"))]
#[must_use]
pub fn process_cpu_time() -> Option<Duration> {
    None
}

/// Time `region` and return its result alongside what it cost.
///
/// This is the form tests use, because it returns the region's own measurement
/// rather than reading a process-global total. #1460 is the standing reason:
/// a test that measures a region by differencing a global is racing every other
/// producer in the process.
pub fn measure<T>(region: impl FnOnce() -> T) -> (T, RegionConcurrency) {
    let cpu_before = process_cpu_time();
    let started = Instant::now();
    let value = region();
    let wall = started.elapsed();
    let cpu = process_cpu_time();
    let (cpu_nanos, cpu_available) = match (cpu_before, cpu) {
        (Some(before), Some(after)) => (
            u64::try_from(after.saturating_sub(before).as_nanos()).unwrap_or(u64::MAX),
            true,
        ),
        _ => (0, false),
    };
    (
        value,
        RegionConcurrency {
            wall_nanos: u64::try_from(wall.as_nanos()).unwrap_or(u64::MAX),
            cpu_nanos,
            cpu_available,
        },
    )
}

fn phase_name(phase: StorageIoPhase) -> &'static str {
    match phase {
        StorageIoPhase::AppendMerge => "append_merge",
        StorageIoPhase::SealAuthentication => "seal_authentication",
        StorageIoPhase::ShapeConsumeReauthentication => "shaping",
        StorageIoPhase::EncodeWritePostwriteAuthentication => "canonical_encoding",
        StorageIoPhase::PublicationPreauthentication => "publication",
        StorageIoPhase::CasInstallReadWrite => "cas_install",
        StorageIoPhase::HydrationVerification => "hydration",
        StorageIoPhase::FsyncSynchronization => "fsync",
        StorageIoPhase::RecoveryReauthentication => "recovery",
        StorageIoPhase::ReadPathScan => "read_path",
    }
}

static PHASES: Mutex<BTreeMap<StorageIoPhase, RegionConcurrency>> = Mutex::new(BTreeMap::new());

/// Time a lifecycle phase and fold the result into the process-wide table when
/// the guard drops.
#[derive(Debug)]
pub struct RegionScope {
    phase: StorageIoPhase,
    capture: Option<capture::CaptureRegion>,
    started: Instant,
    cpu_before: Option<Duration>,
}

impl RegionScope {
    /// Count successfully completed work in the innermost captured region.
    pub fn record_work(unit: &'static str, amount: u64) {
        capture::record_work(unit, amount);
    }

    /// Record a named region in the current thread capture, if one is active.
    #[must_use]
    pub fn named(name: &'static str) -> Option<impl Drop> {
        capture::CaptureRegion::enter(name)
    }

    /// Begin timing `phase`.
    #[must_use]
    pub fn enter(phase: StorageIoPhase) -> Self {
        Self::enter_named(phase, phase_name(phase))
    }

    pub(crate) fn enter_named(phase: StorageIoPhase, name: &'static str) -> Self {
        Self {
            phase,
            capture: capture::CaptureRegion::enter(name),
            started: Instant::now(),
            cpu_before: process_cpu_time(),
        }
    }
}

impl Drop for RegionScope {
    fn drop(&mut self) {
        drop(self.capture.take());
        let wall = self.started.elapsed();
        let (cpu_nanos, cpu_available) = match (self.cpu_before, process_cpu_time()) {
            (Some(before), Some(after)) => (
                u64::try_from(after.saturating_sub(before).as_nanos()).unwrap_or(u64::MAX),
                true,
            ),
            _ => (0, false),
        };
        let region = RegionConcurrency {
            wall_nanos: u64::try_from(wall.as_nanos()).unwrap_or(u64::MAX),
            cpu_nanos,
            cpu_available,
        };
        if let Ok(mut phases) = PHASES.lock() {
            phases
                .entry(self.phase)
                .and_modify(|total| total.merge(&region))
                .or_insert(region);
        }
    }
}

/// Every phase timed so far in this process.
#[must_use]
pub fn snapshot() -> BTreeMap<StorageIoPhase, RegionConcurrency> {
    PHASES
        .lock()
        .map(|phases| phases.clone())
        .unwrap_or_default()
}

/// Zero the process-wide table.
#[doc(hidden)]
pub fn reset() {
    if let Ok(mut phases) = PHASES.lock() {
        phases.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Burn CPU for roughly `millis`, in a way the optimiser cannot elide.
    fn burn(millis: u64) -> u64 {
        let deadline = Instant::now() + Duration::from_millis(millis);
        let mut acc = 0_u64;
        while Instant::now() < deadline {
            for value in 0..4_096_u64 {
                acc = acc.wrapping_add(value).wrapping_mul(2_654_435_761);
            }
        }
        acc
    }

    #[test]
    #[ignore = "Process-wide CPU: other tests running concurrently in this process would be counted, so this must run alone. Run: --ignored --test-threads=1"]
    fn one_busy_thread_reports_about_one_effective_core() {
        // The known positive #1462 requires: force one worker and confirm the
        // instrument says so. Without this, a low reading from real ingest
        // cannot be distinguished from an instrument that always reads low.
        let (_, region) = measure(|| burn(250));
        let cores = region
            .effective_cores()
            .expect("cpu available on this host");
        assert!(
            (0.7..1.3).contains(&cores),
            "one busy thread should be ~1 effective core, measured {cores}"
        );
    }

    #[test]
    #[ignore = "Process-wide CPU: needs a quiet process, and needs the four burn threads to be the only busy ones. Run: --ignored --test-threads=1"]
    fn four_busy_threads_report_about_four_effective_cores() {
        // The other end of the same calibration: if the instrument could not
        // see parallelism it would read ~1 here too, and every conclusion drawn
        // from it would be wrong in the same direction.
        let (_, region) = measure(|| {
            std::thread::scope(|scope| {
                let handles = (0..4)
                    .map(|_| scope.spawn(|| burn(250)))
                    .collect::<Vec<_>>();
                for handle in handles {
                    handle.join().expect("burn thread");
                }
            });
        });
        let cores = region
            .effective_cores()
            .expect("cpu available on this host");
        assert!(
            (2.5..5.0).contains(&cores),
            "four busy threads should be ~4 effective cores, measured {cores}"
        );
    }

    #[test]
    #[ignore = "Measured at 4.9 effective cores under concurrent tests; only meaningful in a quiet process. Run: --ignored --test-threads=1"]
    fn a_sleeping_region_is_not_reported_as_cpu() {
        // Effective cores must distinguish elapsed time from consumed CPU, or
        // it would report a blocked pipeline as a busy one.
        let (_, region) = measure(|| std::thread::sleep(Duration::from_millis(200)));
        let cores = region
            .effective_cores()
            .expect("cpu available on this host");
        assert!(
            cores < 0.5,
            "a sleeping region consumes no CPU, measured {cores}"
        );
        assert!(
            region.wall_nanos >= 150_000_000,
            "wall time was still elapsed"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn first_valid_global_sample_preserves_cpu_availability() {
        {
            let _scope = RegionScope::enter(StorageIoPhase::RecoveryReauthentication);
        }
        assert!(snapshot()[&StorageIoPhase::RecoveryReauthentication].cpu_available);
    }

    #[test]
    fn serial_fraction_inverts_amdahl_at_both_ends() {
        // One effective core on n workers is a fully serial region.
        let all_serial = serial_fraction(1.0, 16).expect("defined at S=1");
        assert!((all_serial - 1.0).abs() < 1e-9, "measured {all_serial}");
        // n effective cores on n workers is a fully parallel one.
        let none_serial = serial_fraction(16.0, 16).expect("defined at S=n");
        assert!(none_serial.abs() < 1e-9, "measured {none_serial}");
        // And a midpoint that has to be computed rather than special-cased.
        let quarter = serial_fraction(4.0, 16).expect("defined at S=4");
        assert!((quarter - 0.2).abs() < 1e-9, "measured {quarter}");
    }

    #[test]
    fn serial_fraction_refuses_what_it_cannot_invert() {
        assert!(
            serial_fraction(1.0, 1).is_none(),
            "one worker has no speedup"
        );
        assert!(serial_fraction(0.0, 16).is_none(), "no speedup to invert");
        // A speedup above the worker count does not fit the model; reporting a
        // negative serial fraction would be worse than reporting nothing.
        assert!(
            serial_fraction(20.0, 16).is_none(),
            "superlinear does not fit"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_stat_is_parsed_past_a_comm_containing_spaces_and_parens() {
        // The executable name is attacker-shaped on purpose: a naive
        // split_whitespace would read the wrong columns and silently report a
        // plausible-but-wrong CPU time, which is the failure mode #1449 warns
        // about. utime=1500, stime=500 ticks => 20.00s at USER_HZ 100.
        let stat = "42 (od ) (d :) name) S 1 42 42 0 -1 4194304 100 0 0 0 1500 500 0 0 20 0 1 0";
        let parsed = parse_proc_stat_cpu(stat).expect("parses");
        assert_eq!(parsed, Duration::from_secs(20));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_malformed_proc_stat_reports_none_rather_than_a_guess() {
        assert!(parse_proc_stat_cpu("no parenthesis here").is_none());
        assert!(
            parse_proc_stat_cpu("42 (x) S 1 2").is_none(),
            "too few fields"
        );
    }

    #[test]
    fn unavailable_cpu_reports_none_rather_than_zero() {
        let region = RegionConcurrency {
            wall_nanos: 1_000,
            cpu_nanos: 0,
            cpu_available: false,
        };
        assert!(region.effective_cores().is_none());
    }
}
