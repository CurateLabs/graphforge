
// #1509: the integrated construction reuse experiment. Combines the #1507
// external-sort hybrid (`GF_SHAPE_SPILL_SPIKE=datafusion`) with the #1508
// load schedulers (`GF_SHAPE_LOAD_SCHEDULER`), with the partition budget taken
// from the recorded-budget override (`GF_SHAPE_MAX_PARTITION_BYTES`) rather
// than passed by the harness, so the override is itself under test.
//
// Each case is a complete construction (shape, encode, publish, reopen,
// adjacency hydration) in a fresh process, reusing the #1507 child body
// (`spill_spike_subprocess`). The parent compares every child's fingerprint
// with an uninterrupted control on the production path.

/// Environment every integrated child starts without, so a variable set on the
/// parent test process can never leak into a case.
const INTEGRATED_ENV: [&str; 12] = [
    "GF_SHAPE_MAX_EXTERNAL_PARTITION_BYTES",
    "GF_SHAPE_SPILL_SPIKE",
    "GF_SHAPE_SPILL_FAULT",
    "GF_SHAPE_SPILL_GUARD",
    "GF_SHAPE_SPILL_POOL_BYTES",
    "GF_SHAPE_SPILL_TEMP_BYTES",
    "GF_SHAPE_LOAD_SCHEDULER",
    "GF_SHAPE_LOAD_WORKERS",
    "GF_SHAPE_MAX_PARTITION_BYTES",
    "GF_SHAPE_SORT_SPIKE",
    "GF_CONSTRUCTION_FAILPOINT",
    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
];

/// A budget every uniform partition of the hub fixture fits and its hub
/// endpoint partition does not (the #1507 premise, re-proved below).
const INTEGRATED_TIGHT_BUDGET: &str = "16384";

/// Bound on one child. A regression that hangs a scheduler fails the case
/// instead of hanging the suite.
const INTEGRATED_CHILD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

struct IntegratedOutcome {
    recorded: serde_json::Value,
    scratch: Vec<String>,
    exited: bool,
    /// Exit code; `None` when a signal ended the child (an abort).
    code: Option<i32>,
    stderr: String,
}

fn run_integrated_case(
    root: &Path,
    name: &str,
    project: &str,
    env: &[(&str, &str)],
) -> IntegratedOutcome {
    use wait_timeout::ChildExt;
    let scratch = root.join(format!("{name}-scratch"));
    std::fs::create_dir_all(&scratch).unwrap();
    let stderr_path = root.join(format!("{name}.stderr"));
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "graph_construction::tests::determinism::spill_spike_subprocess",
            "--nocapture",
        ])
        .env("GF_SPILL_TEST_ROOT", root)
        .env("GF_SPILL_TEST_CASE", name)
        .env("GF_SPILL_TEST_PROJECT", project)
        .env("GF_SHAPE_SPILL_DIR", &scratch)
        .env_remove("GF_SPILL_TEST_PARTITION_BYTES")
        .env_remove("GF_SPILL_TEST_RESUME")
        .env_remove("GF_SPILL_TEST_CANCEL_AFTER_SPILL")
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(&stderr_path).unwrap());
    for key in INTEGRATED_ENV {
        command.env_remove(key);
    }
    for (key, value) in env {
        command.env(key, value);
    }
    let mut child = command.spawn().unwrap();
    let Some(status) = child.wait_timeout(INTEGRATED_CHILD_TIMEOUT).unwrap() else {
        child.kill().unwrap();
        child.wait().unwrap();
        panic!("{name}: the child did not return within {INTEGRATED_CHILD_TIMEOUT:?}");
    };
    let recorded = std::fs::read(root.join(format!("{name}.json")))
        .ok()
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        .unwrap_or(serde_json::Value::Null);
    IntegratedOutcome {
        recorded,
        scratch: relative_files(&scratch),
        exited: status.success(),
        code: status.code(),
        stderr: std::fs::read_to_string(&stderr_path).unwrap_or_default(),
    }
}

fn integrated_control(root: &Path) -> String {
    let control = run_integrated_case(root, "control", "control", &[]);
    let result = &control.recorded["result"];
    assert!(result.is_object(), "control failed: {}", control.recorded);
    assert_eq!(result["reopened_edge_count"], 8_192);
    assert!(control.recorded["external"].as_array().unwrap().is_empty());
    result["fingerprint"].as_str().unwrap().to_owned()
}

/// The hybrid under `scheduler` at `workers`, on the tight recorded budget.
fn hybrid_env<'a>(scheduler: &'a str, workers: &'a str) -> Vec<(&'a str, &'a str)> {
    vec![
        ("GF_SHAPE_MAX_PARTITION_BYTES", INTEGRATED_TIGHT_BUDGET),
        ("GF_SHAPE_SPILL_SPIKE", "datafusion"),
        ("GF_SHAPE_LOAD_SCHEDULER", scheduler),
        ("GF_SHAPE_LOAD_WORKERS", workers),
    ]
}

fn assert_publishes_control(outcome: &IntegratedOutcome, name: &str, fingerprint: &str) {
    let recorded = &outcome.recorded;
    assert_eq!(
        recorded["result"]["fingerprint"].as_str(),
        Some(fingerprint),
        "{name}: {recorded} {}",
        outcome.stderr
    );
    assert_eq!(recorded["result"]["reopened_edge_count"], 8_192, "{name}");
    assert_eq!(recorded["construction_temps_clean"], true, "{name}");
    assert!(outcome.scratch.is_empty(), "{name} left runs: {:?}", outcome.scratch);
}

/// Every scheduler on its own (default budget) and every scheduler combined
/// with the hybrid (tight recorded budget) at 1, 2 and 3 workers.
///
/// The thread schedulers publish the control's bytes in every combination.
/// The Tokio-driven schedulers publish the control's bytes on their own, but
/// cannot host the hybrid: the external partition streams its merge on the
/// coordinator with `Runtime::block_on`, and a Tokio scheduler's coordinator
/// is itself inside `block_on`. That combination must fail closed (nothing
/// published, no runs left), never hang.
#[test]
fn integrated_schedulers_with_the_hybrid_publish_the_control_graph() {
    let root = TempDir::new().unwrap();
    let fingerprint = integrated_control(root.path());

    // The tight budget refuses on the production path, so the hybrid below is
    // exercising partitions the baseline cannot load.
    let refused = run_integrated_case(
        root.path(),
        "baseline-tight",
        "baseline-tight",
        // The refusal this experiment was measured against: since ADR 0047 a
        // session refuses only when its recorded external bound is zero.
        &[
            ("GF_SHAPE_MAX_PARTITION_BYTES", INTEGRATED_TIGHT_BUDGET),
            ("GF_SHAPE_MAX_EXTERNAL_PARTITION_BYTES", "0"),
        ],
    );
    assert!(
        error_of(&refused.recorded).contains("exceeds recorded budget"),
        "{}",
        refused.recorded
    );
    assert!(refused.recorded["published"].is_null());

    let mut observed = Vec::new();
    for scheduler in ["baseline", "rayon", "tokio-blocking", "datafusion-spawned"] {
        for workers in ["1", "2", "3"] {
            let name = format!("alone-{scheduler}-{workers}");
            let alone = run_integrated_case(
                root.path(),
                &name,
                &name,
                &[
                    ("GF_SHAPE_LOAD_SCHEDULER", scheduler),
                    ("GF_SHAPE_LOAD_WORKERS", workers),
                ],
            );
            assert_publishes_control(&alone, &name, &fingerprint);

            let name = format!("hybrid-{scheduler}-{workers}");
            let hybrid = run_integrated_case(root.path(), &name, &name, &hybrid_env(scheduler, workers));
            let thread_scheduler = matches!(scheduler, "baseline" | "rayon");
            if thread_scheduler {
                assert_publishes_control(&hybrid, &name, &fingerprint);
                let external = hybrid.recorded["external"].as_array().unwrap();
                assert!(
                    external
                        .iter()
                        .any(|partition| partition["spill_count"].as_u64().unwrap() > 0),
                    "{name}: the external sort never spilled: {}",
                    hybrid.recorded
                );
            } else {
                assert!(!hybrid.exited, "{name}: a Tokio coordinator hosted the hybrid");
                assert!(
                    hybrid
                        .stderr
                        .contains("Cannot start a runtime from within a runtime"),
                    "{name}: {}",
                    hybrid.stderr
                );
                // The child panicked before recording; it published nothing.
                let project = root.path().join(&name);
                assert!(
                    crate::resolve_project_generation(&project)
                        .ok()
                        .and_then(|generation| generation.graph_files_inventory().ok().flatten())
                        .is_none(),
                    "{name} published"
                );
            }
            observed.push(serde_json::json!({
                "case": name,
                "exited": hybrid.exited,
                "external": hybrid.recorded["external"].as_array().map(Vec::len),
                "scratch_left": hybrid.scratch.len(),
            }));
        }
    }
    println!("INTEGRATED_SCHEDULER_MATRIX {}", serde_json::Value::from(observed));
}

/// The integrated candidate (hybrid + Rayon, two workers) keeps every #1507
/// refusal, fails closed on invalid selections, and cancels cleanly.
#[test]
fn integrated_candidate_fails_closed() {
    let root = TempDir::new().unwrap();
    let fingerprint = integrated_control(root.path());
    let candidate = |extra: &[(&'static str, &'static str)]| {
        let mut env = hybrid_env("rayon", "2");
        env.extend_from_slice(extra);
        env
    };

    let refusals: [(&str, Vec<(&str, &str)>, &str); 7] = [
        (
            "flip-guarded",
            candidate(&[("GF_SHAPE_SPILL_FAULT", "flip")]),
            "differs from its input record multiset",
        ),
        ("truncate", candidate(&[("GF_SHAPE_SPILL_FAULT", "truncate")]), ""),
        (
            "disk-quota",
            candidate(&[("GF_SHAPE_SPILL_TEMP_BYTES", "4096")]),
            "exceeded the allowable limit",
        ),
        (
            "pool-refusal",
            candidate(&[("GF_SHAPE_SPILL_POOL_BYTES", "1024")]),
            "Resources exhausted",
        ),
        (
            "bogus-scheduler",
            vec![("GF_SHAPE_LOAD_SCHEDULER", "bogus")],
            "invalid load scheduler experiment mode",
        ),
        (
            "zero-workers",
            vec![("GF_SHAPE_LOAD_WORKERS", "0")],
            "invalid load worker experiment count",
        ),
        (
            "bogus-budget",
            vec![("GF_SHAPE_MAX_PARTITION_BYTES", "lots")],
            "invalid digit",
        ),
    ];
    for (name, env, expected) in refusals {
        let outcome = run_integrated_case(root.path(), name, name, &env);
        let error = error_of(&outcome.recorded);
        assert!(!error.is_empty(), "{name} did not fail: {}", outcome.recorded);
        assert!(error.contains(expected), "{name}: {error}");
        assert!(outcome.recorded["published"].is_null(), "{name}");
        assert_eq!(outcome.recorded["construction_temps_clean"], true, "{name}");
        assert!(outcome.scratch.is_empty(), "{name} left runs: {:?}", outcome.scratch);
        println!("INTEGRATED_REFUSAL {name}: {error}");
    }

    // A zero budget is recorded and then refused by budget validation.
    let zero = run_integrated_case(
        root.path(),
        "zero-budget",
        "zero-budget",
        &[("GF_SHAPE_MAX_PARTITION_BYTES", "0")],
    );
    assert!(
        error_of(&zero.recorded).contains("invalid construction budgets"),
        "{}",
        zero.recorded
    );

    // Cancellation once an external sort has spilled runs, then a clean retry
    // of the same session.
    let cancel_env = candidate(&[("GF_SPILL_TEST_CANCEL_AFTER_SPILL", "1")]);
    let cancelled = run_integrated_case(root.path(), "cancel", "cancel", &cancel_env);
    assert_eq!(cancelled.recorded["spill_observed"], true, "{}", cancelled.recorded);
    assert!(error_of(&cancelled.recorded).contains("cancel"), "{}", cancelled.recorded);
    assert!(cancelled.recorded["published"].is_null());
    assert_eq!(cancelled.recorded["construction_temps_clean"], true);
    assert!(cancelled.scratch.is_empty(), "{:?}", cancelled.scratch);
    let retry_env = candidate(&[("GF_SPILL_TEST_RESUME", "1")]);
    let retried = run_integrated_case(root.path(), "cancel-retry", "cancel", &retry_env);
    assert_publishes_control(&retried, "cancel-retry", &fingerprint);
}

/// A crash inside the integrated candidate resumes to the control's graph
/// under the same selection; a resume that presents a different recorded
/// budget is refused instead of silently re-shaping under new authority.
#[test]
fn integrated_candidate_crash_resumes_to_the_control_graph() {
    let root = TempDir::new().unwrap();
    let fingerprint = integrated_control(root.path());
    let crashes: [(&str, Vec<(&str, &str)>, Option<i32>); 2] = [
        // The external sort aborts the process after spilling runs.
        (
            "abort-after-spill",
            vec![("GF_SHAPE_SPILL_FAULT", "abort")],
            None,
        ),
        // The process exits after a shaped partition output is installed.
        (
            "partition-output-installed",
            vec![
                (
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                ),
                (
                    "GF_CONSTRUCTION_FAILPOINT",
                    "shape.partition_output.after_install",
                ),
            ],
            // `construction_failpoint` exits with 86.
            Some(86),
        ),
    ];
    for (name, fault, code) in crashes {
        let mut env = hybrid_env("rayon", "2");
        env.extend(fault);
        let crashed = run_integrated_case(root.path(), name, name, &env);
        assert!(
            !crashed.exited && crashed.recorded.is_null() && crashed.code == code,
            "{name}: the child did not crash where intended ({:?}): {} {}",
            crashed.code,
            crashed.recorded,
            crashed.stderr
        );

        let mut changed = hybrid_env("rayon", "2");
        changed[0] = ("GF_SHAPE_MAX_PARTITION_BYTES", "32768");
        changed.push(("GF_SPILL_TEST_RESUME", "1"));
        let refused = run_integrated_case(root.path(), &format!("{name}-other-budget"), name, &changed);
        let error = error_of(&refused.recorded);
        assert!(!error.is_empty(), "{name}: resumed under a different budget");
        assert!(refused.recorded["published"].is_null(), "{name}");
        println!("INTEGRATED_BUDGET_CHANGE {name}: {error}");

        let mut resume = hybrid_env("rayon", "2");
        resume.push(("GF_SPILL_TEST_RESUME", "1"));
        let resumed = run_integrated_case(root.path(), &format!("{name}-resume"), name, &resume);
        assert_eq!(
            resumed.recorded["result"]["fingerprint"].as_str(),
            Some(fingerprint.as_str()),
            "{name}: {} {}",
            resumed.recorded,
            resumed.stderr
        );
        assert_eq!(resumed.recorded["result"]["reopened_edge_count"], 8_192);
    }
}
