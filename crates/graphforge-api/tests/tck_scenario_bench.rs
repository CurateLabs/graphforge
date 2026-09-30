//! Direct tests for the in-process Divan TCK benchmark (#1653).
//!
//! Each case re-runs this binary as a child that installs a small feature
//! corpus and runs the real `benches/tck_scenarios` Divan benchmark in-process
//! with `CODSPEED_ENV` set, so CodSpeed's walltime `raw_results` are written to
//! a temporary workspace root. The parent then inspects the exit status and the
//! raw results:
//!
//! * a passing scenario in bench mode yields one raw result keyed by scenario
//!   (the known positive that makes the negative cases meaningful);
//! * a deliberately failing step aborts the run and its scenario yields no
//!   timing;
//! * test mode executes the scenario and yields no performance evidence.

#[cfg(feature = "search")]
#[path = "bdd/api_steps.rs"]
mod api_steps;
#[path = "bdd/corpus.rs"]
mod corpus;
#[path = "bdd/fixture.rs"]
mod fixture;
#[path = "../benches/tck_scenarios/runner.rs"]
mod runner;
#[path = "bdd/tck_steps.rs"]
mod tck_steps;
#[path = "bdd/world.rs"]
mod world;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use world::GraphForgeWorld;

const CHILD_MODE: &str = "GF_TCK_BENCH_CHILD_MODE";
const CHILD_CORPUS: &str = "GF_TCK_BENCH_CHILD_CORPUS";
const CHILD_SAMPLES: u32 = 3;

const PASSING_KEY: &str = "BenchFault:3:[1] Passing scenario";
const FAILING_KEY: &str = "BenchFault:13:[2] Deliberately failing scenario";

const PASSING_SCENARIO: &str = r#"Feature: BenchFault

  Scenario: [1] Passing scenario
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
"#;

/// Appended after `PASSING_SCENARIO`: a registered step whose assertion fails,
/// because `RETURN 1` is not empty.
const FAILING_SCENARIO: &str = r#"
  Scenario: [2] Deliberately failing scenario
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS x
      """
    Then the result should be empty
"#;

/// Subprocess entry point. It does nothing unless a parent test launched it.
#[test]
#[ignore = "subprocess entry point for the Divan benchmark tests in this file"]
fn divan_child() {
    let mode = std::env::var(CHILD_MODE).expect("child mode");
    let corpus = PathBuf::from(std::env::var_os(CHILD_CORPUS).expect("child corpus"));
    let (_normalized, cases) = runner::load_normalized(&corpus);
    runner::install_corpus(cases);
    let guard = fixture::activate();
    let divan = divan::Divan::default().sample_count(CHILD_SAMPLES);
    match mode.as_str() {
        "bench" => divan.run_benches(),
        "test" => divan.test_benches(),
        other => panic!("unknown child mode {other}"),
    }
    runner::assert_fixture_profile();
    drop(guard);
}

struct ChildRun {
    output: Output,
    raw_results: Vec<serde_json::Value>,
}

impl ChildRun {
    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).into_owned()
    }

    fn names(&self) -> Vec<&str> {
        self.raw_results
            .iter()
            .map(|result| result["name"].as_str().expect("raw result name"))
            .collect()
    }
}

fn run_child(mode: &str, feature: &str) -> ChildRun {
    let scratch = tempfile::TempDir::new().expect("scratch dir");
    let corpus = scratch.path().join("features");
    let workspace = scratch.path().join("workspace");
    std::fs::create_dir_all(&corpus).expect("corpus dir");
    std::fs::create_dir_all(&workspace).expect("workspace dir");
    std::fs::write(corpus.join("BenchFault.feature"), feature).expect("write feature");

    let output = Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "divan_child",
            "--exact",
            "--include-ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        // A local `TCK_ONLY` filter would drop this corpus's feature file.
        .env_remove("TCK_ONLY")
        .env(CHILD_MODE, mode)
        .env(CHILD_CORPUS, &corpus)
        .env("CODSPEED_ENV", "local")
        .env("CODSPEED_CARGO_WORKSPACE_ROOT", &workspace)
        .output()
        .expect("spawn Divan child");
    ChildRun {
        output,
        raw_results: raw_results(&workspace),
    }
}

/// Every walltime raw result CodSpeed's Divan integration wrote under `root`.
fn raw_results(root: &Path) -> Vec<serde_json::Value> {
    let dir = root.join("target/codspeed/walltime/raw_results/divan");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    entries
        .map(|entry| {
            let path = entry.expect("raw result entry").path();
            serde_json::from_slice(&std::fs::read(&path).expect("read raw result"))
                .expect("raw result JSON")
        })
        .collect()
}

#[test]
fn passing_scenario_emits_walltime_raw_results_keyed_by_scenario() {
    let run = run_child("bench", PASSING_SCENARIO);
    assert!(
        run.output.status.success(),
        "bench failed:\n{}",
        run.stderr()
    );
    assert_eq!(run.names(), [format!("scenario[{PASSING_KEY}]")]);
    let stats = &run.raw_results[0]["stats"];
    assert_eq!(stats["rounds"], u64::from(CHILD_SAMPLES));
    assert_eq!(stats["iter_per_round"], 1);
    assert!(stats["min_ns"].as_f64().expect("min_ns") > 0.0);
}

#[test]
fn failing_step_aborts_the_bench_without_recording_timing() {
    let run = run_child("bench", &format!("{PASSING_SCENARIO}{FAILING_SCENARIO}"));
    let stderr = run.stderr();
    assert!(
        !run.output.status.success(),
        "a failing scenario must abort the bench:\n{stderr}"
    );
    assert!(
        stderr.contains(&format!(
            "TCK benchmark scenario failed; no timing is recorded: {FAILING_KEY} at step \
             `Then the result should be empty`"
        )),
        "the abort must come from the failing step's verdict:\n{stderr}"
    );
    let failing = format!("scenario[{FAILING_KEY}]");
    assert!(
        !run.names().contains(&failing.as_str()),
        "the failing scenario yielded timing: {:?}",
        run.names()
    );
}

#[test]
fn test_mode_executes_scenarios_without_performance_evidence() {
    let run = run_child("test", PASSING_SCENARIO);
    assert!(
        run.output.status.success(),
        "test mode failed:\n{}",
        run.stderr()
    );
    assert!(
        run.raw_results.is_empty(),
        "test mode wrote performance evidence: {:?}",
        run.names()
    );

    // Test mode still executes, and so still enforces, every scenario's verdict.
    let run = run_child("test", &format!("{PASSING_SCENARIO}{FAILING_SCENARIO}"));
    assert!(
        !run.output.status.success(),
        "test mode must fail on a failing scenario:\n{}",
        run.stderr()
    );
    assert!(run.raw_results.is_empty());
}

#[test]
fn scenario_keys_match_the_cucumber_runner() {
    let scratch = tempfile::TempDir::new().expect("scratch dir");
    let feature = format!("{PASSING_SCENARIO}{FAILING_SCENARIO}");
    std::fs::write(scratch.path().join("BenchFault.feature"), feature).expect("write feature");
    let cases = runner::load_scenarios(scratch.path());
    let keys: Vec<String> = cases.iter().map(ToString::to_string).collect();
    assert_eq!(keys, [PASSING_KEY, FAILING_KEY]);
}
