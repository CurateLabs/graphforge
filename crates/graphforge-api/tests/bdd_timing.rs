//! Unit and integration coverage for Rust BDD timing reports.

#[path = "bdd/fixture.rs"]
mod fixture;
#[path = "bdd/timing.rs"]
mod timing;

use std::time::{Duration, Instant};

use timing::{
    SCHEMA_VERSION, ScenarioOutcome, ScenarioTimer, ScenarioTiming, Suite, TimingBaseline,
    TimingPolicy, annotation_messages, baseline_candidate, build_report, distribution,
    escape_github_command, load_baseline, load_policy, non_passing_scenario_keys, render_markdown,
    write_artifacts,
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

fn policy() -> TimingPolicy {
    TimingPolicy {
        schema_version: SCHEMA_VERSION,
        baseline_required: true,
        fixture_profile: "pooled-isolated-serial-v1".to_owned(),
        tck_concurrency: fixture::TCK_CONCURRENCY,
        per_scenario_multiplier: 2.0,
        per_scenario_min_delta_ms: 250.0,
        aggregate_multiplier: 1.25,
        aggregate_min_delta_ms: 15_000.0,
        absolute_slow_ms: Some(2_000.0),
        max_warning_annotations: 25,
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

#[test]
fn only_tck_baseline_regressions_create_findings() {
    let records = vec![
        record(
            Suite::Api,
            "api:1:slow",
            "api",
            ScenarioOutcome::Passed,
            5_000,
        ),
        record(
            Suite::Tck,
            "tck:1:slow",
            "tck",
            ScenarioOutcome::Passed,
            2_001,
        ),
        record(
            Suite::Tck,
            "tck:2:new",
            "tck",
            ScenarioOutcome::Passed,
            9_000,
        ),
    ];
    let policy = policy();
    let mut baseline = baseline_candidate(&records, false, &policy);
    baseline.scenarios.remove("tck:2:new");
    baseline.scenarios.insert("tck:1:slow".to_owned(), 500_000);
    baseline
        .scenarios
        .insert("tck:9:removed".to_owned(), 10_000);
    baseline.scenario_count = baseline.scenarios.len();
    baseline.total_elapsed_us = 500_000;
    baseline.features.insert("tck".to_owned(), 500_000);
    let report = build_report(&records, &policy, Some(&baseline), false).unwrap();

    assert_eq!(report.findings.len(), 1);
    assert_eq!(report.findings[0].key.as_deref(), Some("tck:1:slow"));
    assert_eq!(report.unbaselined_tck_scenarios, ["tck:2:new"]);
    assert_eq!(report.missing_baseline_tck_scenarios, ["tck:9:removed"]);
}

#[test]
fn aggregate_degradation_reports_largest_feature_contributors() {
    let records = vec![
        record(Suite::Tck, "a:1:x", "a", ScenarioOutcome::Passed, 20_000),
        record(Suite::Tck, "b:1:y", "b", ScenarioOutcome::Passed, 10_000),
    ];
    let baseline = TimingBaseline {
        schema_version: SCHEMA_VERSION,
        partial: false,
        fixture_profile: "pooled-isolated-serial-v1".to_owned(),
        tck_concurrency: fixture::TCK_CONCURRENCY,
        runner: "ubuntu".to_owned(),
        rust_toolchain: "1.96.0".to_owned(),
        source_commit: "abc".to_owned(),
        scenario_count: 2,
        total_elapsed_us: 10_000_000,
        features: [("a".to_owned(), 6_000_000), ("b".to_owned(), 4_000_000)]
            .into_iter()
            .collect(),
        scenarios: [
            ("a:1:x".to_owned(), 6_000_000),
            ("b:1:y".to_owned(), 4_000_000),
        ]
        .into_iter()
        .collect(),
        suggested_absolute_slow_ms: 1_000.0,
    };
    let mut aggregate_policy = policy();
    aggregate_policy.absolute_slow_ms = None;
    aggregate_policy.per_scenario_multiplier = 100.0;
    aggregate_policy.aggregate_min_delta_ms = 1.0;
    let report = build_report(&records, &aggregate_policy, Some(&baseline), false).unwrap();

    assert_eq!(report.findings.len(), 1);
    assert!(report.findings[0].key.is_none());
    assert_eq!(report.findings[0].contributors[0].feature, "a");
}

#[test]
fn partial_runs_never_compare_with_the_full_baseline() {
    let records = vec![record(
        Suite::Tck,
        "tck:1:slow",
        "tck",
        ScenarioOutcome::Passed,
        99_000,
    )];
    let policy = policy();
    let baseline = baseline_candidate(&records, false, &policy);
    let report = build_report(&records, &policy, Some(&baseline), true).unwrap();
    assert!(report.findings.is_empty());
    assert_eq!(report.baseline_status, "partial_not_compared");
}

#[test]
fn report_is_privacy_safe_and_github_commands_are_escaped() {
    let records = vec![record(
        Suite::Tck,
        "feature:1:name",
        "feature",
        ScenarioOutcome::Passed,
        10,
    )];
    let report = build_report(
        &records,
        &TimingPolicy {
            baseline_required: false,
            ..policy()
        },
        None,
        false,
    )
    .unwrap();
    let json = serde_json::to_string(&report).unwrap();
    assert!(!json.contains("query"));
    assert!(!json.contains("/tmp"));
    assert!(render_markdown(&report).contains("Slowest TCK scenarios"));
    assert_eq!(escape_github_command("a:b,c%\n"), "a%3Ab%2Cc%25%0A");
}

#[test]
fn policy_baseline_and_artifacts_round_trip() {
    let dir = tempfile::TempDir::new().unwrap();
    let policy_path = dir.path().join("policy.json");
    std::fs::write(
        &policy_path,
        serde_json::to_string_pretty(&TimingPolicy {
            baseline_required: false,
            ..policy()
        })
        .unwrap(),
    )
    .unwrap();
    let loaded_policy = load_policy(&policy_path).unwrap();
    assert!(!loaded_policy.baseline_required);

    let records = vec![record(
        Suite::Tck,
        "feature:1:name",
        "feature",
        ScenarioOutcome::Passed,
        10,
    )];
    let candidate = baseline_candidate(&records, false, &loaded_policy);
    let report = build_report(&records, &loaded_policy, None, false).unwrap();
    let output = dir.path().join("artifacts");
    write_artifacts(&output, &report, &candidate).unwrap();
    assert!(output.join("report.json").is_file());
    assert!(output.join("summary.md").is_file());

    let baseline_path = output.join("tck-baseline-candidate.json");
    let loaded_baseline = load_baseline(&baseline_path, true).unwrap().unwrap();
    assert_eq!(loaded_baseline.scenario_count, 1);
}

#[test]
fn threshold_boundaries_are_strict_and_annotations_are_capped() {
    let mut records = Vec::new();
    let mut scenarios = std::collections::BTreeMap::new();
    let mut features = std::collections::BTreeMap::new();
    for index in 0..5 {
        let key = format!("feature:{index}:scenario");
        records.push(record(
            Suite::Tck,
            &key,
            "feature",
            ScenarioOutcome::Passed,
            501,
        ));
        scenarios.insert(key, 250_000);
    }
    features.insert("feature".to_owned(), 1_250_000);
    let baseline = TimingBaseline {
        schema_version: SCHEMA_VERSION,
        partial: false,
        fixture_profile: "pooled-isolated-serial-v1".to_owned(),
        tck_concurrency: fixture::TCK_CONCURRENCY,
        runner: "ubuntu".to_owned(),
        rust_toolchain: "1.96.0".to_owned(),
        source_commit: "abc".to_owned(),
        scenario_count: 5,
        total_elapsed_us: 1_250_000,
        features,
        scenarios,
        suggested_absolute_slow_ms: 1_000.0,
    };
    let mut boundary_policy = policy();
    boundary_policy.absolute_slow_ms = None;
    boundary_policy.aggregate_multiplier = 100.0;
    let report = build_report(&records, &boundary_policy, Some(&baseline), false).unwrap();
    assert_eq!(report.findings.len(), 5);
    let messages = annotation_messages(&report, 3);
    assert_eq!(messages.len(), 3);
    assert!(messages[2].contains("5 TCK performance warning(s)"));

    for record in &mut records {
        record.elapsed_us = 500_000;
    }
    let at_boundary = build_report(&records, &boundary_policy, Some(&baseline), false).unwrap();
    assert!(at_boundary.findings.is_empty());
}

#[test]
fn required_or_malformed_monitor_configuration_is_blocking() {
    let records = vec![record(
        Suite::Tck,
        "feature:1:name",
        "feature",
        ScenarioOutcome::Passed,
        10,
    )];
    assert!(
        build_report(&records, &policy(), None, false)
            .unwrap_err()
            .contains("required TCK performance baseline")
    );

    let mut invalid = policy();
    invalid.max_warning_annotations = 0;
    assert!(
        build_report(&records, &invalid, None, true)
            .unwrap_err()
            .contains("invalid TCK timing policy")
    );

    let configured = policy();
    let mut wrong_profile = baseline_candidate(&records, false, &configured);
    wrong_profile.tck_concurrency = 64;
    assert!(
        build_report(&records, &configured, Some(&wrong_profile), false)
            .unwrap_err()
            .contains("does not match policy")
    );
}
