//! Two-sided verdict engine for the bulk-ingest gate (#1476).
//!
//! Measurement lives in the `m6_storage_io` bench; this module owns only the
//! judgment:
//! given [`IngestObservation`] rows and the banked [`GateLimits`], decide which
//! limits were breached in *either* direction and, on the ratchet side, print
//! the exact constant to write. Being pure, it is unit-tested against a
//! deliberate regression and a deliberate improvement (see
//! `tests/ingest_gate_verdict.rs`) before a clean pass is trusted.
//!
//! Every gate is evaluated on two sides (#1476):
//!
//! - **regression side** — the measurement moved the wrong way past the
//!   banked constant. Fails the gate.
//! - **ratchet side** — the measurement beat the banked constant by more than
//!   that metric's margin. This is an **unbanked gain**: the gain is real but
//!   no constant records it, so the gate fails and prints the constant to
//!   write. This is the prose ratchet rule ("raise it in the pull request
//!   that wins the gain, or the gain is unprotected") enforced instead of
//!   remembered.
//!
//! This is a ratchet, not a wall: the remedy is one constant in the PR that
//! won the gain. Throughput is the one metric excluded from the ratchet side,
//! because wall clock is the one measurement host contention can move without
//! any code change.

use std::collections::HashMap;

/// One measured bulk-ingest publication.
#[derive(Debug, Clone)]
pub struct IngestObservation {
    /// Edges staged and published by the measured generation.
    pub edges: u64,
    /// Vertices the edges span (`edges / fan_out`, at least 1).
    pub vertices: u64,
    /// Wall-clock time of the complete publication.
    pub wall: std::time::Duration,
    /// Process CPU consumed inside the measured region, where the platform can
    /// report it. `None` means the platform exposes no per-process CPU clock,
    /// and the CPU columns are then reported as unavailable rather than faked.
    pub cpu: Option<std::time::Duration>,
    /// Application-observed read bytes across every construction phase.
    pub read_bytes: u64,
    /// Application-observed write bytes across every construction phase.
    pub write_bytes: u64,
    /// High-water mark of simultaneously retained construction artifacts. This
    /// is the transient disk the runner has to have; it is reported so the
    /// nightly budget is visible, and is not gated.
    pub transient_peak_bytes: u64,
}

impl IngestObservation {
    /// Published edges divided by the wall-clock seconds of the publication.
    #[must_use]
    pub fn edges_per_second(&self) -> f64 {
        #[allow(clippy::cast_precision_loss, reason = "reporting-only ratio")]
        let edges = self.edges as f64;
        edges / self.wall.as_secs_f64()
    }

    /// Application-observed device read bytes divided by published edges.
    #[must_use]
    pub fn bytes_read_per_edge(&self) -> f64 {
        #[allow(clippy::cast_precision_loss, reason = "reporting-only ratio")]
        let (read, edges) = (self.read_bytes as f64, self.edges as f64);
        read / edges
    }

    /// Application-observed device write bytes divided by published edges.
    #[must_use]
    pub fn bytes_written_per_edge(&self) -> f64 {
        #[allow(clippy::cast_precision_loss, reason = "reporting-only ratio")]
        let (written, edges) = (self.write_bytes as f64, self.edges as f64);
        written / edges
    }

    /// Microseconds of process CPU per published edge, where the platform can
    /// report process CPU at all.
    #[must_use]
    pub fn cpu_micros_per_edge(&self) -> Option<f64> {
        #[allow(clippy::cast_precision_loss, reason = "reporting-only ratio")]
        let edges = self.edges as f64;
        self.cpu.map(|cpu| cpu.as_secs_f64() * 1e6 / edges)
    }

    /// Effective cores: process CPU divided by elapsed wall time. 1.0 means the
    /// whole ingest ran on one core. #1387 targets at least 12 of 16.
    #[must_use]
    pub fn effective_cores(&self) -> Option<f64> {
        self.cpu
            .map(|cpu| cpu.as_secs_f64() / self.wall.as_secs_f64())
    }
}

/// Execution scope, denominator and units of one gated metric (#1476).
///
/// Recorded so a number measured in one scope is never transferred into
/// another: read/write bytes here are attribution counters for device traffic
/// of one complete ingest publication, not logical record sizes and not
/// harness or whole-rung traffic, and the wall-clock and CPU figures cover the
/// same publication and nothing else.
#[derive(Debug)]
pub struct MetricDescriptor {
    /// Metric name as it appears in the gate report.
    pub metric: &'static str,
    /// What work the measurement covers.
    pub execution_scope: &'static str,
    /// What every observation is divided by.
    pub denominator: &'static str,
    /// Unit the limit constants and the gate report are written in.
    pub units: &'static str,
}

/// Scope records for every gated metric (#1476 acceptance: "every metric
/// records its execution scope, denominator and units").
pub const METRIC_DESCRIPTORS: [MetricDescriptor; 4] = [
    MetricDescriptor {
        metric: "edges_per_second",
        execution_scope: "wall clock of one complete generation publication \
                          (append, seal, canonical encoding, publish) from an \
                          empty parent generation, at each swept size",
        denominator: "published edges",
        units: "edges/second",
    },
    MetricDescriptor {
        metric: "bytes_read_per_edge",
        execution_scope: "application-observed device read bytes summed over \
                          every construction phase of the same complete \
                          publication (storage-layer attribution counters, \
                          not logical record sizes, not harness or whole-rung \
                          traffic)",
        denominator: "published edges",
        units: "bytes/edge",
    },
    MetricDescriptor {
        metric: "cpu_micros_per_edge",
        execution_scope: "process CPU time (user + system) inside the same \
                          measured publication region; unavailable where the \
                          platform exposes no per-process CPU clock",
        denominator: "published edges",
        units: "cpu microseconds/edge",
    },
    MetricDescriptor {
        metric: "read_degradation_ratio",
        execution_scope: "bytes read per edge at the largest swept size \
                          divided by the smallest, same publication scope as \
                          bytes_read_per_edge",
        denominator: "dimensionless (bytes/edge over bytes/edge)",
        units: "ratio",
    },
];

/// How a gate's ratchet side judges "measured better than the constant".
#[derive(Debug, Clone, Copy)]
pub enum RatchetPolicy {
    /// Fail the ratchet side when the measurement beats the banked constant by
    /// more than this fraction of the constant. Each margin comes from that
    /// metric's recorded reproducibility, not one global tolerance (#1476).
    Margin(f64),
    /// Never fail the ratchet side; report an unbanked gain as a note instead.
    /// Used for wall-clock throughput, whose ±48% under-load swing is host
    /// contention, not code (#1476).
    Excluded {
        /// Why the ratchet side is off and what lifts the exclusion.
        reason: &'static str,
    },
}

/// Why wall-clock throughput is not auto-ratcheted (#1476).
pub const THROUGHPUT_RATCHET_EXCLUSION: &str = "wall-clock edges/second moved ±48% under load on a shared host with no \
     code change; the ratchet side stays off until this floor's baseline is \
     banked from the isolated codspeed-macro runner the nightly already runs on";

/// Ratchet margin for bytes read per edge: reproduced to the byte across
/// loaded-host runs, so 10% covers genuine cross-run drift several times over.
pub const INGEST_RATCHET_MARGIN_BYTES_READ_PER_EDGE: f64 = 0.10;

/// Ratchet margin for CPU microseconds per edge: the metric moved ±15% under
/// load, so the ratchet margin is set wider (25%) to keep run-to-run noise
/// from demanding a re-bank (#1476).
pub const INGEST_RATCHET_MARGIN_CPU_MICROS_PER_EDGE: f64 = 0.25;

/// Ratchet margin for the read-degradation ratio: computed from the same
/// to-the-byte deterministic read counters, so 10% like its absolute metric.
pub const INGEST_RATCHET_MARGIN_READ_DEGRADATION_RATIO: f64 = 0.10;

/// The banked constants and per-metric ratchet policies the gate enforces.
///
/// Filled in from the `INGEST_*` constants in `m6_storage_io.rs`, which stay
/// there so `scripts/ci/check-m6-benchmarks.py` can freeze them.
#[derive(Debug, Clone, Copy)]
pub struct GateLimits {
    /// Throughput floor in edges per second, enforced at every swept size.
    pub floor_edges_per_second: f64,
    /// Ceiling on bytes read per edge, enforced at every swept size.
    pub ceiling_bytes_read_per_edge: f64,
    /// Ceiling on CPU microseconds per edge, enforced at every swept size.
    pub ceiling_cpu_micros_per_edge: f64,
    /// Ceiling on read-byte growth across the sweep.
    pub max_read_degradation_ratio: f64,
    /// Ratchet policy for wall-clock throughput.
    pub ratchet_edges_per_second: RatchetPolicy,
    /// Ratchet policy for bytes read per edge.
    pub ratchet_bytes_read_per_edge: RatchetPolicy,
    /// Ratchet policy for CPU microseconds per edge.
    pub ratchet_cpu_micros_per_edge: RatchetPolicy,
    /// Ratchet policy for the read-degradation ratio.
    pub ratchet_read_degradation_ratio: RatchetPolicy,
}

/// Outcome of judging one gate run.
#[derive(Debug, Default)]
pub struct GateVerdict {
    /// Regression-side failures: a measurement moved the wrong way past its
    /// banked constant.
    pub breaches: Vec<String>,
    /// Ratchet-side failures: unbanked gains. Each message carries the exact
    /// constant to write (#1476).
    pub ratchet_breaches: Vec<String>,
    /// Informational lines that never fail the gate.
    pub notes: Vec<String>,
    /// The read-degradation ratio the verdict was judged on.
    pub read_degradation_ratio: f64,
}

impl GateVerdict {
    /// True when the gate must fail: any regression or any unbanked gain.
    #[must_use]
    pub fn must_fail(&self) -> bool {
        !self.breaches.is_empty() || !self.ratchet_breaches.is_empty()
    }

    /// The margin of a policy, when the ratchet side is armed.
    #[must_use]
    pub fn ratchet_margin(policy: RatchetPolicy) -> Option<f64> {
        match policy {
            RatchetPolicy::Margin(margin) => Some(margin),
            RatchetPolicy::Excluded { .. } => None,
        }
    }
}

/// Judge one gate run on both sides.
///
/// `rows` are the swept sizes in ascending order, exactly as measured; the
/// regression checks reproduce the pre-#1476 one-sided behaviour verbatim, so
/// no existing assertion is weakened.
pub fn evaluate_ingest_gate(rows: &[IngestObservation], limits: &GateLimits) -> GateVerdict {
    let mut verdict = GateVerdict::default();
    if rows.is_empty() {
        verdict
            .breaches
            .push("ingest gate swept no sizes".to_owned());
        return verdict;
    }
    let first = &rows[0];
    let last = &rows[rows.len() - 1];
    let read_ratio = last.bytes_read_per_edge() / first.bytes_read_per_edge();
    verdict.read_degradation_ratio = read_ratio;
    regression_breaches(rows, limits, read_ratio, &mut verdict);
    ratchet_breaches(rows, limits, read_ratio, &mut verdict);
    verdict
}

/// Regression side: a measurement moved the wrong way past its banked
/// constant. Messages are the pre-#1476 one-sided gate's, preserved verbatim.
fn regression_breaches(
    rows: &[IngestObservation],
    limits: &GateLimits,
    read_ratio: f64,
    verdict: &mut GateVerdict,
) {
    for row in rows {
        if row.edges_per_second() < limits.floor_edges_per_second {
            verdict.breaches.push(format!(
                "{} edges: {:.0} edges/sec is below the {:.0} edges/sec floor",
                row.edges,
                row.edges_per_second(),
                limits.floor_edges_per_second,
            ));
        }
        if row.bytes_read_per_edge() > limits.ceiling_bytes_read_per_edge {
            verdict.breaches.push(format!(
                "{} edges: {:.0} bytes read per edge exceeds the {:.0} byte ceiling",
                row.edges,
                row.bytes_read_per_edge(),
                limits.ceiling_bytes_read_per_edge,
            ));
        }
        if let Some(cpu) = row.cpu_micros_per_edge()
            && cpu > limits.ceiling_cpu_micros_per_edge
        {
            verdict.breaches.push(format!(
                "{} edges: {cpu:.2} us CPU per edge exceeds the {:.2} us ceiling",
                row.edges, limits.ceiling_cpu_micros_per_edge,
            ));
        }
    }
    if read_ratio > limits.max_read_degradation_ratio {
        let first = &rows[0];
        let last = &rows[rows.len() - 1];
        verdict.breaches.push(format!(
            "bytes read per edge grows {read_ratio:.3}x from {} to {} edges, \
             over the {:.2}x limit",
            first.edges, last.edges, limits.max_read_degradation_ratio,
        ));
    }
}

/// Ratchet side: a measurement beat its banked constant by more than the
/// metric's margin. Each armed metric fails on its *worst* observation, so a
/// gain counts only once the whole sweep has improved, and every failure
/// prints the exact constant to write.
fn ratchet_breaches(
    rows: &[IngestObservation],
    limits: &GateLimits,
    read_ratio: f64,
    verdict: &mut GateVerdict,
) {
    throughput_ratchet(rows, limits, verdict);
    if let Some(margin) = GateVerdict::ratchet_margin(limits.ratchet_bytes_read_per_edge) {
        let worst = worst_of(rows, IngestObservation::bytes_read_per_edge);
        let trigger = limits.ceiling_bytes_read_per_edge * (1.0 - margin);
        if worst < trigger {
            let suggested = snap_ceil(worst * (1.0 + margin / 2.0), 0);
            verdict.ratchet_breaches.push(format!(
                "unbanked gain: worst {worst:.0} bytes read per edge is more than \
                 {percent:.0}% under the {:.0} byte ceiling; write const \
                 INGEST_CEILING_BYTES_READ_PER_EDGE: f64 = {suggested:.1};",
                limits.ceiling_bytes_read_per_edge,
                percent = margin * 100.0,
            ));
        }
    }
    let cpu_worst = rows
        .iter()
        .filter_map(IngestObservation::cpu_micros_per_edge)
        .fold(f64::NEG_INFINITY, f64::max);
    if cpu_worst.is_finite()
        && let Some(margin) = GateVerdict::ratchet_margin(limits.ratchet_cpu_micros_per_edge)
    {
        let trigger = limits.ceiling_cpu_micros_per_edge * (1.0 - margin);
        if cpu_worst < trigger {
            let suggested = snap_ceil(cpu_worst * (1.0 + margin / 2.0), 2);
            verdict.ratchet_breaches.push(format!(
                "unbanked gain: worst {cpu_worst:.2} us CPU per edge is more than \
                 {percent:.0}% under the {:.2} us ceiling; write const \
                 INGEST_CEILING_CPU_MICROS_PER_EDGE: f64 = {suggested:.2};",
                limits.ceiling_cpu_micros_per_edge,
                percent = margin * 100.0,
            ));
        }
    }
    if let Some(margin) = GateVerdict::ratchet_margin(limits.ratchet_read_degradation_ratio) {
        let trigger = limits.max_read_degradation_ratio * (1.0 - margin);
        if read_ratio < trigger {
            let suggested = snap_ceil(read_ratio * (1.0 + margin / 2.0), 3);
            verdict.ratchet_breaches.push(format!(
                "unbanked gain: bytes read per edge grows only {read_ratio:.3}x \
                 across the sweep, more than {percent:.0}% under the {:.2}x limit; \
                 write const INGEST_MAX_READ_DEGRADATION_RATIO: f64 = {suggested:.3};",
                limits.max_read_degradation_ratio,
                percent = margin * 100.0,
            ));
        }
    }
}

/// The throughput ratchet side: armed it fails per row with the constant to
/// write; excluded it reports every unbanked gain as a note (#1476).
fn throughput_ratchet(rows: &[IngestObservation], limits: &GateLimits, verdict: &mut GateVerdict) {
    for row in rows {
        if row.edges_per_second() > limits.floor_edges_per_second {
            match limits.ratchet_edges_per_second {
                RatchetPolicy::Margin(_) => verdict.ratchet_breaches.push(format!(
                    "unbanked gain: {} edges: {:.0} edges/sec beats the {:.0} \
                     edges/sec floor; write const INGEST_FLOOR_EDGES_PER_SECOND: \
                     f64 = {:.1};",
                    row.edges,
                    row.edges_per_second(),
                    limits.floor_edges_per_second,
                    snap_ceil(row.edges_per_second(), 0),
                )),
                RatchetPolicy::Excluded { reason } => verdict.notes.push(format!(
                    "unbanked gain not gated: {} edges: {:.0} edges/sec beats \
                     the {:.0} edges/sec floor ({reason})",
                    row.edges,
                    row.edges_per_second(),
                    limits.floor_edges_per_second,
                )),
            }
        }
    }
}

/// The largest value a metric reports across the sweep; `NEG_INFINITY` when
/// the sweep is empty, which the callers treat as "nothing to judge".
#[must_use]
fn worst_of(rows: &[IngestObservation], metric: impl Fn(&IngestObservation) -> f64) -> f64 {
    rows.iter().map(metric).fold(f64::NEG_INFINITY, f64::max)
}

/// The gate report's limit and policy section, keyed for the JSON artifact.
#[must_use]
pub fn limits_report(limits: &GateLimits) -> HashMap<&'static str, serde_json::Value> {
    let policy_value = |policy: RatchetPolicy| match policy {
        RatchetPolicy::Margin(margin) => serde_json::json!({ "margin": margin }),
        RatchetPolicy::Excluded { reason } => serde_json::json!({ "excluded": reason }),
    };
    HashMap::from([
        (
            "floor_edges_per_second",
            serde_json::json!(limits.floor_edges_per_second),
        ),
        (
            "ceiling_bytes_read_per_edge",
            serde_json::json!(limits.ceiling_bytes_read_per_edge),
        ),
        (
            "ceiling_cpu_micros_per_edge",
            serde_json::json!(limits.ceiling_cpu_micros_per_edge),
        ),
        (
            "max_read_degradation_ratio",
            serde_json::json!(limits.max_read_degradation_ratio),
        ),
        (
            "ratchet_edges_per_second",
            policy_value(limits.ratchet_edges_per_second),
        ),
        (
            "ratchet_bytes_read_per_edge",
            policy_value(limits.ratchet_bytes_read_per_edge),
        ),
        (
            "ratchet_cpu_micros_per_edge",
            policy_value(limits.ratchet_cpu_micros_per_edge),
        ),
        (
            "ratchet_read_degradation_ratio",
            policy_value(limits.ratchet_read_degradation_ratio),
        ),
    ])
}

/// Snap `value` to `decimals` places (so float noise at a decimal boundary
/// cannot push a suggestion one step high), then round it up at that
/// precision. The result is the exact constant printed for a ratchet breach.
pub fn snap_ceil(value: f64, decimals: u32) -> f64 {
    let factor = 10f64.powi(i32::try_from(decimals).unwrap_or(0));
    let scaled = value * factor;
    let snapped = if (scaled - scaled.round()).abs() < 1e-6 {
        scaled.round()
    } else {
        scaled
    };
    snapped.ceil() / factor
}
