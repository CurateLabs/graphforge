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
    GateLimits, GateVerdict, HostBoundJudgment, IngestObservation, RatchetPolicy,
    evaluate_ingest_gate,
};

/// The pre-#1672 policy, banked on the shared development host with
/// throughput excluded from the ratchet side. The engine tests below were
/// written against these figures and keep them, judged as on the banked host,
/// so the engine's behaviour is pinned independently of whichever constants
/// the bench currently banks.
fn production_limits() -> GateLimits {
    GateLimits {
        floor_edges_per_second: 15_000.0,
        ceiling_bytes_read_per_edge: 2_500.0,
        ceiling_cpu_micros_per_edge: 14.0,
        max_read_degradation_ratio: 1.20,
        ratchet_edges_per_second: RatchetPolicy::Excluded {
            reason: "baseline not yet banked from the isolated codspeed-macro runner",
        },
        ratchet_bytes_read_per_edge: RatchetPolicy::Margin(0.10),
        ratchet_cpu_micros_per_edge: RatchetPolicy::Margin(0.25),
        ratchet_read_degradation_ratio: RatchetPolicy::Margin(0.10),
        host_bound: HostBoundJudgment::Judged,
    }
}

/// The limits #1672 banked from the `codspeed-macro` nightlies, throughput
/// two-sided. This is a frozen record of that banking decision, proved against
/// the nights it was made from; it deliberately does not follow later
/// re-banks, which change one constant in the bench and nothing here.
fn runner_limits() -> GateLimits {
    GateLimits {
        floor_edges_per_second: 9_000.0,
        ceiling_bytes_read_per_edge: 1_325.0,
        ceiling_cpu_micros_per_edge: 25.0,
        max_read_degradation_ratio: 1.070,
        ratchet_edges_per_second: RatchetPolicy::Margin(0.40),
        ratchet_bytes_read_per_edge: RatchetPolicy::Margin(0.10),
        ratchet_cpu_micros_per_edge: RatchetPolicy::Margin(0.25),
        ratchet_read_degradation_ratio: RatchetPolicy::Margin(0.10),
        host_bound: HostBoundJudgment::Judged,
    }
}

/// The same limits as any host other than the banked runner sees them.
fn off_host_limits() -> GateLimits {
    GateLimits {
        host_bound: HostBoundJudgment::ReportOnly {
            reason: "banked from the codspeed-macro runner",
        },
        ..runner_limits()
    }
}

/// The twelve scheduled `codspeed-macro` nightlies the constants were banked
/// from (2026-09-19 through 2026-09-30, tabulated on #1672), as
/// `(edges/sec, cpu us/edge)` at the 524,288-edge rung then the
/// 8,388,608-edge rung. Bytes read per edge were 1,238 and 1,261 every night.
const BANKING_NIGHTS: [[(f64, f64); 2]; 12] = [
    [(9_959.0, 23.79), (17_083.0, 17.80)],
    [(10_950.0, 23.46), (18_250.0, 17.98)],
    [(11_169.0, 23.57), (16_673.0, 18.71)],
    [(10_361.0, 23.56), (16_458.0, 19.27)],
    [(10_301.0, 23.63), (16_568.0, 19.38)],
    [(10_556.0, 23.63), (15_732.0, 19.42)],
    [(11_035.0, 23.84), (17_692.0, 19.53)],
    [(10_458.0, 23.95), (17_338.0, 19.50)],
    [(10_438.0, 23.94), (18_959.0, 19.52)],
    [(10_646.0, 23.81), (16_515.0, 19.33)],
    [(11_954.0, 23.72), (17_101.0, 19.31)],
    [(11_106.0, 23.98), (16_655.0, 19.43)],
];

fn banking_night(night: &[(f64, f64); 2]) -> Vec<IngestObservation> {
    vec![
        observation(524_288, 1_238.0, night[0].1, night[0].0),
        observation(8_388_608, 1_261.0, night[1].1, night[1].0),
    ]
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
/// ratchet side makes a sweep-wide improvement fail once, with the constant
/// to write.
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
        ratchet.contains("write const INGEST_FLOOR_EDGES_PER_SECOND: f64 = 35000.0;"),
        "{ratchet}"
    );

    // A mixed sweep whose best run still beats the floor by more than the
    // margin also fails, exactly once.
    let mixed = vec![
        observation(524_288, 2_100.0, 11.93, 40_000.0),
        observation(8_388_608, 2_100.0, 11.93, 30_000.0),
    ];
    let verdict = evaluate_ingest_gate(&mixed, &limits);
    assert_eq!(verdict.ratchet_breaches.len(), 1);

    // But a sweep whose *best* run stays inside the margin trigger never
    // fails, even when individual rows clear the floor.
    let contained = vec![
        observation(524_288, 2_100.0, 11.93, 40_000.0),
        observation(8_388_608, 2_100.0, 11.93, 16_000.0),
    ];
    let verdict = evaluate_ingest_gate(&contained, &limits);
    assert!(verdict.ratchet_breaches.is_empty(), "{verdict:?}");
    let borderline = vec![observation(524_288, 2_100.0, 11.93, 18_000.0)];
    let verdict = evaluate_ingest_gate(&borderline, &limits);
    assert!(verdict.ratchet_breaches.is_empty(), "{verdict:?}");
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
    assert!((ingest_gate::snap_floor(35_000.000_000_000_01, 0) - 35_000.0).abs() < 1e-9);
    assert!((ingest_gate::snap_floor(1_999.6, 0) - 1_999.0).abs() < 1e-9);
    assert!((ingest_gate::snap_floor(10.125, 2) - 10.12).abs() < 1e-9);
}

/// Every night the constants were banked from passes both sides on the banked
/// runner: the limits describe the runner, not one lucky run (#1672).
#[test]
fn every_banking_night_passes_on_the_runner() {
    for night in &BANKING_NIGHTS {
        let verdict = evaluate_ingest_gate(&banking_night(night), &runner_limits());
        assert!(!verdict.must_fail(), "{night:?}: {verdict:?}");
        assert!(verdict.notes.is_empty(), "{night:?}: {:?}", verdict.notes);
    }
}

/// The runner-banked limits still catch a regression and an unbanked gain on
/// each host-bound metric, in the expected direction.
#[test]
fn runner_limits_fail_both_sides_of_the_host_bound_metrics() {
    let slow = vec![
        observation(524_288, 1_238.0, 23.7, 8_500.0),
        observation(8_388_608, 1_261.0, 19.4, 16_500.0),
    ];
    let verdict = evaluate_ingest_gate(&slow, &runner_limits());
    assert!(
        joined(&verdict.breaches).contains("8500 edges/sec is below the 9000 edges/sec floor"),
        "{verdict:?}"
    );

    let costly = vec![
        observation(524_288, 1_238.0, 25.6, 10_600.0),
        observation(8_388_608, 1_261.0, 19.4, 16_500.0),
    ];
    let verdict = evaluate_ingest_gate(&costly, &runner_limits());
    assert!(
        joined(&verdict.breaches).contains("25.60 us CPU per edge exceeds the 25.00 us ceiling"),
        "{verdict:?}"
    );

    let faster = vec![
        observation(524_288, 1_238.0, 23.7, 13_000.0),
        observation(8_388_608, 1_261.0, 19.4, 20_000.0),
    ];
    let verdict = evaluate_ingest_gate(&faster, &runner_limits());
    assert!(verdict.breaches.is_empty(), "{verdict:?}");
    assert!(
        joined(&verdict.ratchet_breaches)
            .contains("write const INGEST_FLOOR_EDGES_PER_SECOND: f64 = 10400.0;"),
        "{verdict:?}"
    );

    let cheaper = vec![
        observation(524_288, 1_238.0, 18.0, 10_600.0),
        observation(8_388_608, 1_261.0, 15.0, 16_500.0),
    ];
    let verdict = evaluate_ingest_gate(&cheaper, &runner_limits());
    assert!(verdict.breaches.is_empty(), "{verdict:?}");
    assert!(
        joined(&verdict.ratchet_breaches)
            .contains("write const INGEST_CEILING_CPU_MICROS_PER_EDGE: f64 = 20.25;"),
        "{verdict:?}"
    );
}

/// Off the banked host the host-bound limits never fail the gate in either
/// direction; each finding is reported as a note that names the reason.
#[test]
fn host_bound_limits_are_report_only_off_the_banked_host() {
    // A development machine: far faster wall clock and far cheaper CPU than the
    // runner's constants, which judged would be two unbanked gains.
    let development_host = vec![
        observation(524_288, 1_238.0, 11.9, 40_000.0),
        observation(8_388_608, 1_261.0, 11.9, 40_000.0),
    ];
    let verdict = evaluate_ingest_gate(&development_host, &off_host_limits());
    assert!(!verdict.must_fail(), "{verdict:?}");
    let notes = joined(&verdict.notes);
    assert!(
        notes.contains("not judged (banked from the codspeed-macro runner)"),
        "{notes}"
    );
    assert!(notes.contains("INGEST_FLOOR_EDGES_PER_SECOND"), "{notes}");
    assert!(
        notes.contains("INGEST_CEILING_CPU_MICROS_PER_EDGE"),
        "{notes}"
    );
    assert_eq!(
        evaluate_ingest_gate(&development_host, &runner_limits())
            .ratchet_breaches
            .len(),
        2,
        "the same run is two unbanked gains when judged"
    );

    // A loaded machine: slower and costlier than the runner's constants.
    let loaded_host = vec![
        observation(524_288, 1_238.0, 30.0, 5_000.0),
        observation(8_388_608, 1_261.0, 30.0, 5_000.0),
    ];
    let verdict = evaluate_ingest_gate(&loaded_host, &off_host_limits());
    assert!(!verdict.must_fail(), "{verdict:?}");
    assert_eq!(verdict.notes.len(), 4, "{:?}", verdict.notes);
}

/// The deterministic byte-counter limits are judged on every host: report-only
/// covers the host-bound metrics and nothing else.
#[test]
fn deterministic_limits_are_judged_off_the_banked_host() {
    let more_reads = vec![
        observation(524_288, 1_400.0, 11.9, 40_000.0),
        observation(8_388_608, 1_600.0, 11.9, 40_000.0),
    ];
    let verdict = evaluate_ingest_gate(&more_reads, &off_host_limits());
    let breaches = joined(&verdict.breaches);
    assert!(
        breaches.contains("bytes read per edge exceeds the 1325 byte ceiling"),
        "{breaches}"
    );
    assert!(breaches.contains("over the 1.07x limit"), "{breaches}");

    let fewer_reads = vec![
        observation(524_288, 1_000.0, 11.9, 40_000.0),
        observation(8_388_608, 1_010.0, 11.9, 40_000.0),
    ];
    let verdict = evaluate_ingest_gate(&fewer_reads, &off_host_limits());
    assert!(verdict.breaches.is_empty(), "{verdict:?}");
    assert!(
        joined(&verdict.ratchet_breaches)
            .contains("write const INGEST_CEILING_BYTES_READ_PER_EDGE: f64 ="),
        "{verdict:?}"
    );
}
