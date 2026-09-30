//! Unit and integration coverage for the diagnostic Rust BDD timing reports
//! and the test-only TCK fault injection (#1654).
//!
//! Threshold behaviour moved to the provenance-gated `make tck-perf` consumer;
//! its parity cases live in `benchmarks/tests/test_tck_perf.py`.

#[path = "bdd/fault.rs"]
mod fault;
#[path = "bdd/fixture.rs"]
mod fixture;
#[path = "bdd/timing.rs"]
mod timing;

use std::path::Path;
use std::time::{Duration, Instant};

use fault::{Fault, Scope};
use timing::{
    LegacyFile, REPORT_KIND, REPORT_SCHEMA_VERSION, ScenarioOutcome, ScenarioTimer, ScenarioTiming,
    Suite, build_report, distribution, legacy_notice, load_legacy, non_passing_scenario_keys,
    render_markdown, write_artifacts,
};

#[test]
fn fixture_pool_reuses_infrastructure_without_leaking_scenario_state() {
    let pool = fixture::FixturePool::default();
    let forge = pool.acquire().expect("first fixture");
    forge
        .execute("CREATE (:Person {name: 'Alice'})")
        .expect("seed leased fixture");
    forge
        .register_procedure(graphforge_api::ProcedureDefinition {
            name: "test.leak".into(),
            inputs: vec![],
            outputs: vec![],
            rows: vec![vec![]],
        })
        .expect("register scenario procedure");
    pool.release(forge);

    let reused = pool.acquire().expect("reused fixture");
    assert_eq!(pool.created_count(), 1);
    let result = reused
        .execute("MATCH (n) RETURN n")
        .expect("read reset fixture");
    assert_eq!(
        result
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    assert!(reused.execute("CALL test.leak()").is_err());
}

#[test]
fn timed_tck_profile_uses_fixed_bounded_parallelism() {
    assert_eq!(fixture::TCK_CONCURRENCY, 1);
}

#[test]
fn global_tck_fixture_lifecycle_returns_world_state_to_the_pool() {
    let _run = fixture::activate();
    let mut slot = None;
    fixture::replace_with_fresh(&mut slot);
    slot.as_ref()
        .expect("leased fixture")
        .execute("CREATE (:Person)")
        .expect("seed leased fixture");
    fixture::release(&mut slot);
    assert!(slot.is_none());

    fixture::replace_with_fresh(&mut slot);
    assert_eq!(fixture::created_count(), 1);
    let result = slot
        .as_ref()
        .expect("reused fixture")
        .execute("MATCH (n) RETURN n")
        .expect("read reset fixture");
    assert_eq!(
        result
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    fixture::release(&mut slot);
}

fn record(
    suite: Suite,
    key: &str,
    feature: &str,
    outcome: ScenarioOutcome,
    ms: u64,
) -> ScenarioTiming {
    ScenarioTiming {
        suite,
        key: key.to_owned(),
        feature: feature.to_owned(),
        line: 1,
        name: key.to_owned(),
        outcome,
        elapsed_us: ms * 1_000,
    }
}

#[test]
fn timer_handles_interleaved_scenarios_with_monotonic_instants() {
    let started = Instant::now();
    let mut timer = ScenarioTimer::default();
    timer
        .start(
            Suite::Tck,
            "a:1:first".to_owned(),
            0,
            "a".to_owned(),
            1,
            "first".to_owned(),
            started,
        )
        .unwrap();
    timer
        .start(
            Suite::Tck,
            "b:2:second".to_owned(),
            0,
            "b".to_owned(),
            2,
            "second".to_owned(),
            started + Duration::from_millis(10),
        )
        .unwrap();
    timer.mark_skipped("b:2:second", 0);
    let second = timer
        .finish("b:2:second", 0, started + Duration::from_millis(30))
        .unwrap();
    timer.mark_failed("a:1:first", 0);
    let first = timer
        .finish("a:1:first", 0, started + Duration::from_millis(50))
        .unwrap();

    assert_eq!(second.elapsed_us, 20_000);
    assert_eq!(second.outcome, ScenarioOutcome::Skipped);
    assert_eq!(first.elapsed_us, 50_000);
    assert_eq!(first.outcome, ScenarioOutcome::Failed);
}

#[test]
fn distribution_uses_midpoint_median_and_nearest_rank_percentiles() {
    let values: Vec<u64> = (1..=100).map(|value| value * 1_000).collect();
    let stats = distribution(&values);
    assert_eq!(stats.count, 100);
    assert_eq!(stats.sum_ms, 5_050.0);
    assert_eq!(stats.median_ms, 50.5);
    assert_eq!(stats.p90_ms, 90.0);
    assert_eq!(stats.p95_ms, 95.0);
    assert_eq!(stats.p99_ms, 99.0);
}

#[test]
fn correctness_failure_keys_include_skips_and_are_suite_scoped_and_stably_ordered() {
    let records = vec![
        record(Suite::Api, "z:1:failed", "z", ScenarioOutcome::Failed, 1),
        record(Suite::Tck, "a:1:failed", "a", ScenarioOutcome::Failed, 1),
        record(Suite::Api, "a:1:failed", "a", ScenarioOutcome::Failed, 1),
        record(Suite::Api, "s:1:skipped", "s", ScenarioOutcome::Skipped, 1),
        record(Suite::Api, "p:1:passed", "p", ScenarioOutcome::Passed, 1),
    ];
    assert_eq!(
        non_passing_scenario_keys(&records, Suite::Api),
        ["a:1:failed", "s:1:skipped", "z:1:failed"]
    );
}

fn workspace_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

#[test]
fn report_is_diagnostic_only_and_carries_no_threshold_fields() {
    let records = vec![
        record(
            Suite::Tck,
            "feature:1:name",
            "feature",
            ScenarioOutcome::Passed,
            10,
        ),
        record(Suite::Api, "api:1:name", "api", ScenarioOutcome::Passed, 5),
    ];
    let report = build_report(&records, false, fixture::TCK_CONCURRENCY, None);
    assert_eq!(report.schema_version, REPORT_SCHEMA_VERSION);
    assert_eq!(report.report_kind, REPORT_KIND);
    let json: serde_json::Value = serde_json::to_value(&report).unwrap();
    assert_eq!(json["report_kind"], "diagnostic");
    for removed in [
        "findings",
        "baseline_status",
        "unbaselined_tck_scenarios",
        "missing_baseline_tck_scenarios",
    ] {
        assert!(
            json.get(removed).is_none(),
            "{removed} must not be reported"
        );
    }
    let tck = &report.suites[1];
    assert_eq!(tck.suite, Suite::Tck);
    assert_eq!(tck.distribution.count, 1);
    assert_eq!(tck.slowest[0].key, "feature:1:name");
    let markdown = render_markdown(&report);
    assert!(markdown.contains("Slowest TCK scenarios"));
    assert!(markdown.contains("drive no performance threshold"));
    assert!(!markdown.contains("Performance warnings"));
}

#[test]
fn report_is_privacy_safe() {
    let records = vec![record(
        Suite::Tck,
        "feature:1:name",
        "feature",
        ScenarioOutcome::Passed,
        10,
    )];
    let notice = legacy_notice(&[("tests/tck/performance_baseline.json", LegacyFile::Schema2)]);
    let report = build_report(&records, false, fixture::TCK_CONCURRENCY, notice);
    let json = serde_json::to_string(&report).unwrap();
    assert!(!json.contains("query"));
    assert!(!json.contains("/tmp"));
    assert!(!json.contains("/home"));
}

#[test]
fn artifacts_are_the_report_and_summary_only() {
    let dir = tempfile::TempDir::new().unwrap();
    let records = vec![record(
        Suite::Tck,
        "feature:1:name",
        "feature",
        ScenarioOutcome::Passed,
        10,
    )];
    let report = build_report(&records, false, fixture::TCK_CONCURRENCY, None);
    let output = dir.path().join("artifacts");
    write_artifacts(&output, &report).unwrap();
    let mut names: Vec<String> = std::fs::read_dir(&output)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["report.json", "summary.md"]);
}

#[test]
fn committed_schema_2_policy_and_baseline_load_as_one_legacy_diagnostic_notice() {
    let root = workspace_root();
    let files: Vec<(&str, LegacyFile)> = [
        "tests/tck/performance_policy.json",
        "tests/tck/performance_baseline.json",
    ]
    .into_iter()
    .map(|name| (name, load_legacy(&root.join(name))))
    .collect();
    assert_eq!(files[0].1, LegacyFile::Schema2);
    assert_eq!(files[1].1, LegacyFile::Schema2);
    let notice = legacy_notice(&files).expect("legacy files present");
    assert!(
        notice.starts_with("legacy diagnostic baseline: "),
        "{notice}"
    );
    assert!(notice.contains("tests/tck/performance_policy.json (schema 2)"));
    assert!(notice.contains("tests/tck/performance_baseline.json (schema 2)"));
    assert!(notice.contains("not a performance threshold authority"));
    assert_eq!(notice.matches("legacy diagnostic baseline").count(), 1);
}

#[test]
fn absent_malformed_or_unknown_legacy_files_never_panic() {
    let dir = tempfile::TempDir::new().unwrap();
    assert_eq!(
        load_legacy(&dir.path().join("missing.json")),
        LegacyFile::Absent
    );
    assert_eq!(legacy_notice(&[("missing.json", LegacyFile::Absent)]), None);

    let malformed = dir.path().join("malformed.json");
    std::fs::write(&malformed, "{ not json").unwrap();
    assert!(matches!(
        load_legacy(&malformed),
        LegacyFile::Unrecognised(_)
    ));

    let future = dir.path().join("future.json");
    std::fs::write(&future, r#"{"schema_version": 7}"#).unwrap();
    assert_eq!(
        load_legacy(&future),
        LegacyFile::Unrecognised("schema 7".to_owned())
    );
    let notice = legacy_notice(&[("future.json", load_legacy(&future))]).unwrap();
    assert!(notice.contains("future.json (schema 7)"), "{notice}");
}

#[test]
fn fault_injection_parses_a_named_scenario_or_every_scenario() {
    assert_eq!(Fault::parse(None, None), Ok(None));
    let named = Fault::parse(Some("250"), Some("Delete5:34:[1] Delete node from a list"))
        .unwrap()
        .unwrap();
    assert_eq!(
        named.scope,
        Scope::Scenario("Delete5:34:[1] Delete node from a list".to_owned())
    );
    assert_eq!(
        named.delay_for("Delete5:34:[1] Delete node from a list"),
        Some(Duration::from_millis(250))
    );
    assert_eq!(named.delay_for("Delete5:35:[2] other"), None);
    assert_eq!(
        named.announcement(),
        "TCK PERF FAULT INJECTION: delay_ms=250 scenario=Delete5:34:[1] Delete node from a list"
    );

    let all = Fault::parse(Some("5"), Some("*")).unwrap().unwrap();
    assert_eq!(all.scope, Scope::All);
    assert_eq!(
        all.delay_for("anything:1:x"),
        Some(Duration::from_millis(5))
    );
    assert_eq!(
        all.announcement(),
        "TCK PERF FAULT INJECTION: delay_ms=5 scenario=*"
    );
}

#[test]
fn fault_injection_rejects_partial_or_invalid_configuration() {
    for (delay, scenario) in [
        (Some("5"), None),
        (None, Some("*")),
        (Some("0"), Some("*")),
        (Some("-1"), Some("*")),
        (Some("fast"), Some("*")),
        (Some("5"), Some("  ")),
    ] {
        assert!(
            Fault::parse(delay, scenario).is_err(),
            "{delay:?} / {scenario:?} must be rejected"
        );
    }
}

#[test]
fn fault_injection_is_inactive_without_configuration() {
    // cargo test and CI never set GF_TCK_PERF_FAULT_*; the child-process tests
    // in tck_scenario_bench.rs cover an active injection end to end.
    if std::env::var_os(fault::DELAY_ENV).is_none()
        && std::env::var_os(fault::SCENARIO_ENV).is_none()
    {
        assert!(fault::active().is_none());
        // With no injection configured this is a no-op.
        fault::inject("any:1:scenario");
    }
}
