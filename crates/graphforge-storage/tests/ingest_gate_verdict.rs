//! Acceptance tests for the two-sided ingest gate (#1476).
//!
//! The gate binary runs a minutes-long durable measurement, so its judgment is
//! exercised here directly: a deliberate regression and a deliberate
//! improvement must each fail the gate in the expected direction — including
//! the ratchet side printing the exact constant to write — before a clean pass
//! is trusted. The engine is shared verbatim with the bench via `#[path]`, so
//! what is tested here is what the nightly runs.

// The module is written for the bench binary, which uses every item; this
// test crate exercises the judgment surface, so the measurement-reporting
// items that only the bench consumes would be flagged as dead here.
#[allow(
    dead_code,
    reason = "the bench binary consumes the full surface; this crate tests the verdict"
)]
#[path = "../benches/ingest_gate.rs"]
mod ingest_gate;

use std::time::Duration;

use ingest_gate::{
    GateLimits, GateVerdict, IngestObservation, RatchetPolicy, THROUGHPUT_RATCHET_EXCLUSION,
    evaluate_ingest_gate,
};

/// The production policy the bench wires in `gate_limits()`.
fn production_limits() -> GateLimits {
    GateLimits {
        floor_edges_per_second: 15_000.0,
        ceiling_bytes_read_per_edge: 2_500.0,
        ceiling_cpu_micros_per_edge: 14.0,
        max_read_degradation_ratio: 1.20,
        ratchet_edges_per_second: RatchetPolicy::Excluded {
            reason: THROUGHPUT_RATCHET_EXCLUSION,
        },
        ratchet_bytes_read_per_edge: RatchetPolicy::Margin(0.10),
        ratchet_cpu_micros_per_edge: RatchetPolicy::Margin(0.25),
        ratchet_read_degradation_ratio: RatchetPolicy::Margin(0.10),
    }
}

/// Production limits with exactly one ratchet side armed, so each metric's
/// ratchet is tested in isolation from its neighbours.
fn limits_with_only(armed: fn(&mut GateLimits)) -> GateLimits {
    let mut limits = production_limits();
    limits.ratchet_edges_per_second = excluded();
    limits.ratchet_bytes_read_per_edge = excluded();
    limits.ratchet_cpu_micros_per_edge = excluded();
    limits.ratchet_read_degradation_ratio = excluded();
    armed(&mut limits);
    limits
}

fn excluded() -> RatchetPolicy {
    RatchetPolicy::Excluded {
        reason: "not under test",
    }
}

/// A synthetic observation with the given per-edge and throughput figures.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "test fixture construction"
)]
fn observation(
    edges: u64,
    bytes_per_edge: f64,
    cpu_micros: f64,
    edges_per_second: f64,
) -> IngestObservation {
    IngestObservation {
        edges,
        vertices: edges / 16,
        wall: Duration::from_secs_f64(edges as f64 / edges_per_second),
        cpu: Some(Duration::from_secs_f64(cpu_micros * 1e-6 * edges as f64)),
        read_bytes: (bytes_per_edge * edges as f64).round() as u64,
        write_bytes: 0,
        transient_peak_bytes: 0,
    }
}

/// The same fixture with the CPU clock reported unavailable.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "test fixture construction"
)]
fn observation_without_cpu(
    edges: u64,
    bytes_per_edge: f64,
    edges_per_second: f64,
) -> IngestObservation {
    IngestObservation {
        cpu: None,
        ..observation(edges, bytes_per_edge, 0.0, edges_per_second)
    }
}

fn joined(breaches: &[String]) -> String {
    breaches.join("\n")
}

/// The documented repeated-run band passes both sides with no breach.
#[test]
fn healthy_documented_band_passes() {
    let rows = vec![
        observation(524_288, 2_138.937, 11.93, 30_000.0),
        observation(8_388_608, 2_412.016, 11.93, 30_000.0),
    ];
    let verdict = evaluate_ingest_gate(&rows, &production_limits());
    assert!(verdict.breaches.is_empty(), "{:?}", verdict.breaches);
    assert!(
        verdict.ratchet_breaches.is_empty(),
        "{:?}",
        verdict.ratchet_breaches
    );
    assert!(!verdict.must_fail());
    assert!((verdict.read_degradation_ratio - 2_412.016 / 2_138.937).abs() < 1e-9);
}

/// A deliberate regression on every metric fails each gate on its regression
/// side, with the pre-#1476 messages preserved verbatim.
#[test]
fn deliberate_regression_fails_each_low_side() {
    let rows = vec![
        observation(524_288, 2_600.0, 15.5, 10_000.0),
        observation(8_388_608, 3_380.0, 15.5, 10_000.0),
    ];
    let verdict = evaluate_ingest_gate(&rows, &production_limits());
    let breaches = joined(&verdict.breaches);
    assert!(
        breaches.contains("edges/sec is below the 15000 edges/sec floor"),
        "{breaches}"
    );
    assert!(
        breaches.contains("bytes read per edge exceeds the 2500 byte ceiling"),
        "{breaches}"
    );
    assert!(
        breaches.contains("us CPU per edge exceeds the 14.00 us ceiling"),
        "{breaches}"
    );
    assert!(breaches.contains("over the 1.20x limit"), "{breaches}");
    assert!(verdict.ratchet_breaches.is_empty());
    assert!(verdict.must_fail());
}

/// A deliberate improvement on the read-byte ceiling fails the ratchet side
/// and the failure prints the exact constant to write (#1476).
#[test]
fn byte_improvement_fails_ratchet_side_with_constant() {
    let rows = vec![
        observation(524_288, 1_900.0, 12.0, 30_000.0),
        observation(8_388_608, 2_000.0, 12.0, 30_000.0),
    ];
    let limits = limits_with_only(|limits| {
        limits.ratchet_bytes_read_per_edge = RatchetPolicy::Margin(0.10);
    });
    let verdict = evaluate_ingest_gate(&rows, &limits);
    assert!(verdict.breaches.is_empty(), "{:?}", verdict.breaches);
    assert_eq!(verdict.ratchet_breaches.len(), 1);
    let ratchet = joined(&verdict.ratchet_breaches);
    assert!(ratchet.contains("unbanked gain"), "{ratchet}");
    assert!(
        ratchet.contains("write const INGEST_CEILING_BYTES_READ_PER_EDGE: f64 = 2100.0;"),
        "{ratchet}"
    );
    assert!(verdict.must_fail());
}

/// A deliberate CPU improvement fails the ratchet side with the constant to
/// write; the 25% margin keeps the ±15% observed noise from triggering it.
#[test]
fn cpu_improvement_fails_ratchet_side_with_constant() {
    // 11.93 µs is the historical mid-band: inside the 10.5 trigger, no ratchet.
    let mid = vec![
        observation(524_288, 2_100.0, 11.93, 30_000.0),
        observation(8_388_608, 2_100.0, 11.93, 30_000.0),
    ];
    let limits = limits_with_only(|limits| {
        limits.ratchet_cpu_micros_per_edge = RatchetPolicy::Margin(0.25);
    });
    let quiet = evaluate_ingest_gate(&mid, &limits);
    assert!(quiet.ratchet_breaches.is_empty(), "{quiet:?}");

    // A genuine 24% improvement clears the 10.5 µs trigger and must bank.
    let improved = vec![
        observation(524_288, 2_100.0, 9.0, 30_000.0),
        observation(8_388_608, 2_100.0, 9.0, 30_000.0),
    ];
    let verdict = evaluate_ingest_gate(&improved, &limits);
    assert_eq!(verdict.ratchet_breaches.len(), 1);
    let ratchet = joined(&verdict.ratchet_breaches);
    assert!(
        ratchet.contains("write const INGEST_CEILING_CPU_MICROS_PER_EDGE: f64 = 10.13;"),
        "{ratchet}"
    );
}

/// A deliberate flattening of the read-growth curve fails the ratio gate's
/// ratchet side with the constant to write.
#[test]
fn ratio_improvement_fails_ratchet_side_with_constant() {
    let rows = vec![
        observation(524_288, 2_000.0, 11.93, 30_000.0),
        observation(8_388_608, 2_000.0, 11.93, 30_000.0),
    ];
    let limits = limits_with_only(|limits| {
        limits.ratchet_read_degradation_ratio = RatchetPolicy::Margin(0.10);
    });
    let verdict = evaluate_ingest_gate(&rows, &limits);
    assert_eq!(verdict.ratchet_breaches.len(), 1);
    let ratchet = joined(&verdict.ratchet_breaches);
    assert!(
        ratchet.contains("write const INGEST_MAX_READ_DEGRADATION_RATIO: f64 = 1.050;"),
        "{ratchet}"
    );
}

/// Wall-clock throughput is excluded from auto-ratcheting (#1476): an
/// improvement far past the floor is reported as a note, never a failure.
#[test]
fn throughput_improvement_is_excluded_from_the_ratchet() {
    // Other metrics sit in their healthy band so the throughput exclusion is
    // the only observable policy here.
    let rows = vec![
        observation(524_288, 2_300.0, 11.93, 40_000.0),
        observation(8_388_608, 2_500.0, 11.93, 40_000.0),
    ];
    let limits = production_limits();
    assert!(matches!(
        limits.ratchet_edges_per_second,
        RatchetPolicy::Excluded { .. }
    ));
    assert_eq!(
        GateVerdict::ratchet_margin(limits.ratchet_edges_per_second),
        None
    );
    let verdict = evaluate_ingest_gate(&rows, &limits);
    assert!(verdict.ratchet_breaches.is_empty());
    assert!(!verdict.must_fail());
    let notes = joined(&verdict.notes);
    assert!(notes.contains("unbanked gain not gated"), "{notes}");
    assert!(notes.contains("codspeed-macro"), "{notes}");
}

/// The exclusion is a policy, not a missing feature: arming the throughput
/// ratchet side makes the same improvement fail with the constant to write.
#[test]
fn armed_throughput_ratchet_fails_with_constant() {
    let rows = vec![observation(524_288, 2_100.0, 11.93, 40_000.0)];
    let limits = limits_with_only(|limits| {
        limits.ratchet_edges_per_second = RatchetPolicy::Margin(0.25);
    });
    let verdict = evaluate_ingest_gate(&rows, &limits);
    assert_eq!(verdict.ratchet_breaches.len(), 1);
    let ratchet = joined(&verdict.ratchet_breaches);
    assert!(
        ratchet.contains("write const INGEST_FLOOR_EDGES_PER_SECOND: f64 = 40000.0;"),
        "{ratchet}"
    );
}

/// Rows with no platform CPU clock skip both CPU sides instead of inventing a
/// value.
#[test]
fn cpu_unavailable_rows_skip_cpu_sides() {
    let rows = vec![
        observation_without_cpu(524_288, 2_300.0, 30_000.0),
        observation_without_cpu(8_388_608, 2_500.0, 30_000.0),
    ];
    let verdict = evaluate_ingest_gate(&rows, &production_limits());
    assert!(!verdict.must_fail());
    assert!(verdict.breaches.is_empty());
    assert!(verdict.ratchet_breaches.is_empty());
}

/// An empty sweep fails closed rather than passing vacuously.
#[test]
fn empty_sweep_fails_closed() {
    let verdict = evaluate_ingest_gate(&[], &production_limits());
    assert!(verdict.must_fail());
    assert!(joined(&verdict.breaches).contains("swept no sizes"));
}

/// Suggested constants snap through float noise at a decimal boundary, so a
/// printed suggestion never lands one step above the measured gain.
#[test]
fn suggestion_snapping_is_boundary_safe() {
    assert!((ingest_gate::snap_ceil(2_100.000_000_000_000_5, 0) - 2_100.0).abs() < 1e-9);
    assert!((ingest_gate::snap_ceil(10.125, 2) - 10.13).abs() < 1e-9);
    assert!((ingest_gate::snap_ceil(1.05, 3) - 1.05).abs() < 1e-9);
    assert!((ingest_gate::snap_ceil(1.000_000_000_1, 3) - 1.0).abs() < 1e-9);
    assert!((ingest_gate::snap_ceil(1_999.6, 0) - 2_000.0).abs() < 1e-9);
}
