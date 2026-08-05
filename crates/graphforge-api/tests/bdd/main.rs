//! cucumber-rs BDD runner for the GraphForge API and TCK feature files.
//!
//! Two suites run per `cargo test --test bdd`:
//!   * Required GraphForge public-API scenarios under `tests/features/api/`,
//!     which fail closed on failed, skipped, or undefined steps.
//!   * the WHOLE vendored openCypher TCK corpus under `tests/tck/features/`, run
//!     **advisorily** — every scenario, no `@skip-rust` filter, via cucumber
//!     `run()` (which does not exit on failure). A custom writer records the set
//!     of scenarios that pass and gates it against `tests/tck/passing_baseline.txt`:
//!     any baseline scenario that stops passing fails CI (a regression); newly
//!     passing scenarios are surfaced as `TCK XPASS`. See the comment on the TCK
//!     run below and `docs/reference/tck-compliance.md`.

mod api_steps;
mod fixture;
mod tck_steps;
mod timing;

use cucumber::{World, WriterExt};
use futures::FutureExt;

use timing::{
    ScenarioTimer, ScenarioTiming, Suite, annotation_messages, baseline_candidate, build_report,
    escape_github_command, load_baseline, load_policy, non_passing_scenario_keys, write_artifacts,
};

/// Shared cucumber [`World`] for the GraphForge BDD suites (public API + TCK).
#[derive(Debug, Default, World)]
pub struct GraphForgeWorld {
    /// The forge instance under test (None until a Given step creates it).
    pub forge: Option<graphforge_api::GraphForge>,
    /// Owns a persistent-project fixture directory for lifecycle scenarios.
    pub persistent_fixture: Option<tempfile::TempDir>,
    /// Owns an ontology fixture directory for load scenarios.
    pub ontology_fixture: Option<tempfile::TempDir>,
    /// Ontology fixture path selected by the Given step.
    pub ontology_path: Option<std::path::PathBuf>,
    /// Last error returned by a When step.
    pub last_error: Option<String>,
    /// Stable public code for the last typed Rust facade error.
    pub last_error_code: Option<&'static str>,
    /// Last interim `RecordBatch` returned by a stubbed When step (schema()/etc.).
    pub last_result: Option<graphforge_api::RecordBatch>,
    /// Last Arrow-backed result returned by `execute()`.
    pub last_exec: Option<graphforge_api::ExecutionResult>,
    /// Most recent Arrow result returned by an analyst verb.
    pub last_algorithm_result: Option<arrow::record_batch::RecordBatch>,
    /// Previous analyst result, retained for comparison scenarios.
    pub previous_algorithm_result: Option<arrow::record_batch::RecordBatch>,
    /// Query parameters bound by openCypher TCK `And parameters are:` steps.
    pub params: std::collections::HashMap<String, graphforge_api::IrLiteral>,
    /// Node handles by name.
    pub nodes: std::collections::HashMap<String, graphforge_api::NodeHandle>,
    /// Most recently created node handle for result-focused assertions.
    pub last_node_handle: Option<graphforge_api::NodeHandle>,
    /// Most recently created edge handle for result-focused assertions.
    pub last_edge_handle: Option<graphforge_api::EdgeHandle>,
    /// Number of explicit public index calls made in this scenario.
    pub index_calls: usize,
    /// Stored query/index vector for find/index scenarios.
    pub stored_vector: Option<Vec<f32>>,
    /// Caller-defined vector space used by find/index fixtures.
    pub stored_space: Option<String>,
    /// Stored node UUID (hex or hyphenated) for index upsert scenarios.
    pub stored_paper_id: Option<String>,
}

#[tokio::main]
async fn main() {
    // Resolve paths relative to the workspace root so the runner works from
    // any working directory.
    let workspace_root = {
        let manifest = std::env::var("CARGO_MANIFEST_DIR")
            .expect("CARGO_MANIFEST_DIR must be set by cargo test");
        // crates/graphforge-core  →  ../..  →  workspace root
        std::path::PathBuf::from(manifest)
            .join("../..")
            .canonicalize()
            .expect("workspace root must exist")
    };

    let features_dir = workspace_root.join("tests/features");

    // Required public-API scenarios run strictly. Product gaps use the single
    // issue-backed exclusion inventory and never contribute to passing totals.
    let api_results = std::sync::Arc::new(std::sync::Mutex::new(ScenarioOutcomes::default()));
    let api_only = std::env::var("API_ONLY").ok();
    if let Some(needle) = &api_only {
        eprintln!("API_ONLY subset: only scenarios matching {needle:?} will be evaluated");
    }
    GraphForgeWorld::cucumber()
        .with_writer(cucumber::writer::Tee::new(
            cucumber::writer::Basic::stdout().summarized(),
            ScenarioCollector::new(Suite::Api, std::sync::Arc::clone(&api_results), false),
        ))
        .with_default_cli()
        .filter_run(features_dir.join("api"), move |_, _, scenario| {
            !scenario
                .tags
                .iter()
                .any(|tag| tag == "excluded-api-bdd" || tag == "binding-only")
                && api_only
                    .as_deref()
                    .is_none_or(|needle| scenario.name.contains(needle))
        })
        .await;
    let api_results = std::sync::Arc::into_inner(api_results)
        .expect("API collector dropped after run")
        .into_inner()
        .expect("API outcomes mutex");

    // openCypher TCK scenarios — the WHOLE vendored corpus, run over an EPHEMERAL
    // normalized copy so the vendored files stay byte-for-byte upstream (#886).
    // The Rust gherkin parser (0.14) rejects a scenario whose FIRST step uses the
    // `And`/`But` continuation keyword (Match5 has `And having executed:` continuing
    // the Background's `Given`); we rewrite only such block-leading continuations to
    // `Given` at load time, into a temp dir — never touching the vendored sources.
    //
    // ADVISORY model (matches the v0.4.0 run-up): run EVERY scenario — no
    // `@skip-rust` filter — with `run()`, which does NOT exit on failures, so an
    // unsupported scenario failing does not break CI. A custom writer records the
    // SET of scenarios that pass; we gate that set against the committed baseline
    // `tests/tck/passing_baseline.txt`. Any baseline scenario that stops passing
    // is a REGRESSION (fails CI) — even if a *different* scenario newly passes, so
    // an XPASS can't mask a regression (the whole point: a count would). New
    // passes are surfaced as `TCK XPASS` and locked in by re-blessing. No manual
    // per-scenario un-skipping.
    let corpus = workspace_root.join("tests/tck/features");
    let normalized = tempfile::TempDir::new().expect("temp dir for normalized TCK corpus");
    copy_features_normalized(&corpus, normalized.path());
    let root = normalized.path().to_path_buf();
    let passing = std::sync::Arc::new(std::sync::Mutex::new(ScenarioOutcomes::default()));
    let fixture_guard = fixture::activate();
    // `with_default_cli()` MUST come AFTER `with_writer()`: `with_writer` resets the
    // builder's parsed CLI to `None` (the CLI type depends on the writer), and a
    // `None` CLI makes `run()` fall back to parsing the process argv. That parse
    // rejects the libtest-style `--no-fail-fast` that `cargo test … -- --no-fail-fast`
    // forwards to this `harness = false` binary (see `.github/workflows/bdd.yml`),
    // since the custom writer's `Cli = Empty` doesn't accept it. Setting a default CLI
    // last skips the argv parse entirely (we scope scenarios by corpus path, not CLI).
    GraphForgeWorld::cucumber()
        .max_concurrent_scenarios(fixture::TCK_CONCURRENCY)
        .after(|_, _, _, _, world| {
            async move {
                if let Some(world) = world {
                    fixture::release(&mut world.forge);
                }
            }
            .boxed_local()
        })
        .with_writer(ScenarioCollector::new(
            Suite::Tck,
            std::sync::Arc::clone(&passing),
            std::env::var_os("TCK_DUMP_FAILURES").is_some(),
        ))
        .with_default_cli()
        .run(root)
        .await;
    let created_fixtures = fixture::created_count();
    assert!(
        created_fixtures <= fixture::TCK_CONCURRENCY,
        "TCK fixture pool created {created_fixtures} engines for concurrency {}",
        fixture::TCK_CONCURRENCY
    );
    eprintln!(
        "TCK fixture profile: pooled-isolated-serial-v1, concurrency {}, engines created {created_fixtures}",
        fixture::TCK_CONCURRENCY
    );
    drop(fixture_guard);

    let outcomes = std::sync::Arc::into_inner(passing)
        .expect("collector dropped after run")
        .into_inner()
        .expect("outcomes mutex");
    let api_passing = api_results.passing.len();
    let api_skipped = api_results
        .timings
        .iter()
        .filter(|record| record.outcome == timing::ScenarioOutcome::Skipped)
        .count();
    let api_failed = api_results
        .timings
        .iter()
        .filter(|record| record.outcome == timing::ScenarioOutcome::Failed)
        .count();
    let mut timing_records = api_results.timings;
    timing_records.extend(outcomes.timings.iter().cloned());

    // Measurement aid (#27): when `TCK_DUMP_FAILURES=<path>` is set, write one
    // JSON record per FAILING scenario (failing step + error + query) so the set
    // can be bucketed by failure cause offline. Off in CI (env unset) → no-op.
    if let Some(path) = std::env::var_os("TCK_DUMP_FAILURES") {
        let body: String = outcomes
            .failures
            .iter()
            .map(|f| {
                let rec = serde_json::json!({
                    "key": f.key,
                    "feature": f.feature,
                    "line": f.line,
                    "name": f.name,
                    "query": f.query,
                    "setup": f.setup,
                    "fail_kind": f.fail_kind,
                    "fail_step": f.fail_step,
                    "error": f.error,
                });
                format!("{rec}\n")
            })
            .collect();
        std::fs::write(&path, body).expect("write TCK failure dump");
        eprintln!(
            "TCK_DUMP_FAILURES: wrote {} failing-scenario record(s) to {}",
            outcomes.failures.len(),
            std::path::Path::new(&path).display(),
        );
    }

    // Timing artifacts are written before subset returns, blessing exits, and
    // correctness assertions so a failing run still leaves diagnostic evidence.
    write_timing_report(
        &workspace_root,
        &timing_records,
        tck_only_filter().is_some(),
    );

    let api_failures = non_passing_scenario_keys(&timing_records, Suite::Api);
    eprintln!("API BDD required: {api_passing} passed, {api_failed} failed, {api_skipped} skipped");
    assert!(
        api_failures.is_empty(),
        "API BDD correctness failure(s):\n{}",
        api_failures
            .iter()
            .map(|key| format!("  - {key}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    let actual = outcomes.passing;

    // `TCK_ONLY` subset run (local iteration): report the subset's pass count and
    // skip the whole-corpus baseline gate (a subset can't satisfy it).
    if tck_only_filter().is_some() {
        eprintln!(
            "\nTCK_ONLY subset: {} passing of {} scenarios (baseline gate skipped)",
            actual.len(),
            outcomes.total,
        );
        return;
    }

    let baseline_path = workspace_root.join("tests/tck/passing_baseline.txt");

    // BLESS mode: rewrite the baseline to the current passing set (used to seed
    // or intentionally update it). Otherwise GATE on it.
    if std::env::var_os("BLESS_TCK_BASELINE").is_some() {
        let mut keys: Vec<&String> = actual.iter().collect();
        keys.sort();
        let body: String = keys.iter().map(|k| format!("{k}\n")).collect();
        std::fs::write(&baseline_path, body).expect("write passing baseline");
        eprintln!("blessed TCK baseline: {} passing scenarios", actual.len());
        return;
    }

    let baseline = load_passing_baseline(&baseline_path);
    let regressions: Vec<&String> = baseline.difference(&actual).collect();
    let xpasses: Vec<&String> = actual.difference(&baseline).collect();
    eprintln!(
        "\nopenCypher TCK (advisory, whole corpus): {} passing of {} scenarios — baseline {} \
         ({} regressed, {} xpass)",
        actual.len(),
        outcomes.total,
        baseline.len(),
        regressions.len(),
        xpasses.len(),
    );
    if !xpasses.is_empty() {
        eprintln!(
            "::warning title=TCK XPASS::{} scenario(s) now pass that weren't in the baseline — \
             lock them in with `BLESS_TCK_BASELINE=1 cargo test -p graphforge-api --test bdd`",
            xpasses.len(),
        );
        for k in xpasses.iter().take(25) {
            eprintln!("  + {k}");
        }
    }
    assert!(
        regressions.is_empty(),
        "TCK REGRESSION: {} scenario(s) that passed in the baseline now fail (advisory model gates \
         on WHICH scenarios pass, not just the count — an unrelated xpass does not offset a \
         regression):\n{}",
        regressions.len(),
        regressions
            .iter()
            .map(|k| format!("  - {k}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

fn write_timing_report(
    workspace_root: &std::path::Path,
    records: &[ScenarioTiming],
    partial: bool,
) {
    let policy_path = workspace_root.join("tests/tck/performance_policy.json");
    let baseline_path = workspace_root.join("tests/tck/performance_baseline.json");
    let policy = load_policy(&policy_path).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        policy.tck_concurrency,
        fixture::TCK_CONCURRENCY,
        "timing policy concurrency must match the cucumber fixture profile"
    );
    let baseline = load_baseline(&baseline_path, policy.baseline_required && !partial)
        .unwrap_or_else(|error| panic!("{error}"));
    let report = build_report(records, &policy, baseline.as_ref(), partial)
        .unwrap_or_else(|error| panic!("failed to build BDD timing report: {error}"));
    let candidate = baseline_candidate(records, partial, &policy);
    let configured = std::env::var_os("BDD_TIMING_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target/bdd-timings"));
    let output_dir = if configured.is_absolute() {
        configured
    } else {
        workspace_root.join(configured)
    };
    let markdown =
        write_artifacts(&output_dir, &report, &candidate).unwrap_or_else(|error| panic!("{error}"));
    eprintln!("\n{markdown}");

    for message in annotation_messages(&report, policy.max_warning_annotations) {
        eprintln!("TCK PERF WARNING: {message}");
        if std::env::var("GITHUB_ACTIONS").is_ok_and(|value| value == "true") {
            eprintln!(
                "::warning title=TCK performance::{}",
                escape_github_command(&message)
            );
        }
    }
}

/// Passing-scenario keys (`<feature-name>:<line>:<name>`) recorded by the
/// advisory corpus run, plus the total scenario count.
#[derive(Default)]
struct ScenarioOutcomes {
    passing: std::collections::BTreeSet<String>,
    total: usize,
    /// Per-failing-scenario diagnostics — populated only when
    /// `TCK_DUMP_FAILURES` is set (measurement aid; empty in CI).
    failures: Vec<FailureRecord>,
    /// Privacy-safe first-pass scenario timings for this suite.
    timings: Vec<ScenarioTiming>,
}

/// One failing scenario's failure diagnostics, dumped as JSONL when
/// `TCK_DUMP_FAILURES=<path>` is set. Never consulted by the baseline gate.
#[derive(Default)]
struct FailureRecord {
    key: String,
    feature: String,
    line: usize,
    name: String,
    /// The `When executing query:` doc-string, if the scenario had one.
    query: Option<String>,
    /// The `having executed:` setup doc-string, if any.
    setup: Option<String>,
    /// "failed" (a step panicked / asserted) or "skipped" (an undefined step —
    /// missing step vocabulary — or the first skip in a post-failure cascade).
    fail_kind: String,
    /// The failing step rendered as `<keyword><value>`.
    fail_step: String,
    /// The failure message (panic payload) for a failed step; empty for skipped.
    error: String,
}

/// A cucumber [`Writer`] that records outcomes and true execution durations.
///
/// It accepts raw happened-before events and tracks each active scenario by
/// identity, so concurrent scenarios do not need `writer::Normalize`. Timing
/// inside `Normalize` would measure buffered replay for queued scenarios rather
/// than their execution.
struct ScenarioCollector {
    suite: Suite,
    outcomes: std::sync::Arc<std::sync::Mutex<ScenarioOutcomes>>,
    /// State accumulated independently for every interleaved scenario.
    active: std::collections::HashMap<(String, usize), CurrentScenario>,
    timer: ScenarioTimer,
    /// Whether to capture per-failure diagnostics (`TCK_DUMP_FAILURES` set).
    dump: bool,
}

/// Mutable per-scenario diagnostics collected across step events.
#[derive(Default)]
struct CurrentScenario {
    key: String,
    feature: String,
    line: usize,
    name: String,
    /// True until the first failed/skipped step.
    ok: bool,
    query: Option<String>,
    setup: Option<String>,
    /// First failure's kind / step / error (subsequent skips are cascade noise).
    fail_kind: Option<&'static str>,
    fail_step: Option<String>,
    error: Option<String>,
}

impl ScenarioCollector {
    fn new(
        suite: Suite,
        outcomes: std::sync::Arc<std::sync::Mutex<ScenarioOutcomes>>,
        dump: bool,
    ) -> Self {
        Self {
            suite,
            outcomes,
            active: std::collections::HashMap::new(),
            timer: ScenarioTimer::default(),
            dump,
        }
    }
}

// The collector is insensitive to cross-scenario event ordering because every
// lookup includes the stable scenario identity and retry attempt.
impl cucumber::writer::Normalized for ScenarioCollector {}

impl cucumber::Writer<GraphForgeWorld> for ScenarioCollector {
    type Cli = cucumber::cli::Empty;

    async fn handle_event(
        &mut self,
        ev: cucumber::parser::Result<cucumber::Event<cucumber::event::Cucumber<GraphForgeWorld>>>,
        _cli: &Self::Cli,
    ) {
        use cucumber::event::{Cucumber, Feature, Rule, Scenario, Step};
        let Ok(ev) = ev else { return };
        let Cucumber::Feature(feature, feature_event) = ev.value else {
            return;
        };
        let (scenario, retry) = match feature_event {
            Feature::Scenario(scenario, retry) => (scenario, retry),
            Feature::Rule(_, Rule::Scenario(scenario, retry)) => (scenario, retry),
            _ => return,
        };
        let key = format!(
            "{}:{}:{}",
            feature.name, scenario.position.line, scenario.name
        );
        let attempt = retry.retries.map_or(0, |retries| retries.current);
        let active_key = (key.clone(), attempt);
        match retry.event {
            Scenario::Started => {
                // Key by FEATURE NAME (unique per TCK file) + line + scenario
                // name. Deliberately NOT the file path: the normalized corpus
                // lives under a temp dir whose canonicalization differs by
                // platform (macOS `/private` symlinks), which would make keys —
                // and thus the baseline — non-portable between local and CI.
                self.timer
                    .start(
                        self.suite,
                        key.clone(),
                        attempt,
                        feature.name.clone(),
                        scenario.position.line,
                        scenario.name.clone(),
                        std::time::Instant::now(),
                    )
                    .expect("unique scenario start");
                let previous = self.active.insert(
                    active_key,
                    CurrentScenario {
                        key,
                        feature: feature.name.clone(),
                        line: scenario.position.line,
                        name: scenario.name.clone(),
                        ok: true,
                        ..CurrentScenario::default()
                    },
                );
                assert!(previous.is_none(), "duplicate active scenario");
            }
            // Record the query / setup doc-strings as steps start, so a later
            // failing Then step's record still carries the query that ran.
            Scenario::Step(step, Step::Started) | Scenario::Background(step, Step::Started) => {
                if let Some(cur) = self.active.get_mut(&active_key) {
                    let v = step.value.trim_start();
                    if v.starts_with("executing query:") {
                        cur.query = step.docstring.clone();
                    } else if v.starts_with("having executed:") {
                        cur.setup = step.docstring.clone();
                    }
                }
            }
            // A failed step (panic / assertion) — capture the first one's error.
            Scenario::Step(step, Step::Failed(_, _, _, err))
            | Scenario::Background(step, Step::Failed(_, _, _, err)) => {
                self.timer.mark_failed(&key, attempt);
                if let Some(cur) = self.active.get_mut(&active_key) {
                    cur.ok = false;
                    if cur.fail_kind.is_none() {
                        cur.fail_kind = Some("failed");
                        cur.fail_step = Some(format!("{}{}", step.keyword, step.value));
                        cur.error = Some(err.to_string());
                    }
                }
            }
            // A skipped step (undefined step under no fail_on_skipped, or a skip
            // cascading after an earlier failure) — also means "did not pass".
            Scenario::Step(step, Step::Skipped) | Scenario::Background(step, Step::Skipped) => {
                self.timer.mark_skipped(&key, attempt);
                if let Some(cur) = self.active.get_mut(&active_key) {
                    cur.ok = false;
                    if cur.fail_kind.is_none() {
                        cur.fail_kind = Some("skipped");
                        cur.fail_step = Some(format!("{}{}", step.keyword, step.value));
                    }
                }
            }
            Scenario::Finished => {
                let Some(cur) = self.active.remove(&active_key) else {
                    return;
                };
                let timing = self
                    .timer
                    .finish(&key, attempt, std::time::Instant::now())
                    .expect("scenario finish matches start");
                let mut o = self.outcomes.lock().expect("outcomes mutex");
                o.total += 1;
                o.timings.push(timing);
                if cur.ok {
                    o.passing.insert(cur.key);
                } else if self.dump {
                    o.failures.push(FailureRecord {
                        key: cur.key,
                        feature: cur.feature,
                        line: cur.line,
                        name: cur.name,
                        query: cur.query,
                        setup: cur.setup,
                        fail_kind: cur.fail_kind.unwrap_or("unknown").to_string(),
                        fail_step: cur.fail_step.unwrap_or_default(),
                        error: cur.error.unwrap_or_default(),
                    });
                }
            }
            _ => {}
        }
    }
}

/// Load the committed passing baseline (`<feature-name>:<line>:<name>` per
/// line). A missing file → empty (first run / pre-bless); any *other* I/O error
/// panics rather than silently yielding an empty baseline — an empty baseline
/// would make the regression diff vacuously pass (fail-closed, not fail-open).
fn load_passing_baseline(path: &std::path::Path) -> std::collections::BTreeSet<String> {
    let content = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => panic!("failed to read TCK baseline {}: {e}", path.display()),
    };
    content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// Recursively copy the vendored TCK feature tree from `src` into `dst`, rewriting
/// only block-leading `And`/`But` continuation keywords to `Given` (see
/// [`normalize_leading_continuations`]). The vendored source files are never modified.
/// The `TCK_ONLY` local-iteration substring filter, or `None` if unset/empty.
/// An empty value is treated as unset so a stray `TCK_ONLY=` can't silently
/// bypass the baseline gate (`contains("")` is always true).
fn tck_only_filter() -> Option<String> {
    std::env::var("TCK_ONLY")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

fn copy_features_normalized(src: &std::path::Path, dst: &std::path::Path) {
    for entry in std::fs::read_dir(src).expect("read TCK feature dir") {
        let path = entry.expect("dir entry").path();
        let target = dst.join(path.file_name().expect("entry file name"));
        if path.is_dir() {
            std::fs::create_dir_all(&target).expect("create temp subdir");
            copy_features_normalized(&path, &target);
        } else if path.extension().is_some_and(|e| e == "feature") {
            // Local iteration: `TCK_ONLY=<substr>` restricts the corpus to
            // feature files whose path contains `<substr>` (e.g.
            // `TCK_ONLY=Temporal`) for a fast subset run. Unset in CI → the
            // whole corpus. The baseline gate is skipped when set (see `main`),
            // since a subset can't satisfy the whole-corpus baseline.
            if let Some(filter) = tck_only_filter()
                && !path.to_string_lossy().contains(&filter)
            {
                continue;
            }
            let content = std::fs::read_to_string(&path).expect("read feature file");
            std::fs::write(&target, normalize_leading_continuations(&content))
                .expect("write temp feature");
        }
    }
}

/// Rewrite a step that is the FIRST step of its `Scenario`/`Scenario Outline`/
/// `Background`/`Rule`/`Example` block and uses the `And`/`But` continuation keyword
/// into `Given`. The Rust `gherkin` parser rejects a block-leading `And`/`But`, while
/// cucumber-js accepts it; semantics are unchanged (the continuation inherits `Given`).
/// Only block-leading steps are touched — `And`/`But` after a concrete step are left as-is.
fn normalize_leading_continuations(content: &str) -> String {
    let mut out = String::with_capacity(content.len() + 64);
    let mut awaiting_first_step = false;
    for line in content.lines() {
        let trimmed = line.trim_start();
        let is_header = trimmed.starts_with("Scenario:")
            || trimmed.starts_with("Scenario Outline:")
            || trimmed.starts_with("Background:")
            || trimmed.starts_with("Rule:")
            || trimmed.starts_with("Example:");
        let is_step = ["Given ", "When ", "Then ", "And ", "But "]
            .iter()
            .any(|kw| trimmed.starts_with(kw));
        if is_header {
            awaiting_first_step = true;
            out.push_str(line);
        } else if awaiting_first_step
            && (trimmed.starts_with("And ") || trimmed.starts_with("But "))
        {
            let indent = &line[..line.len() - trimmed.len()];
            let rest = trimmed
                .strip_prefix("And ")
                .or_else(|| trimmed.strip_prefix("But "))
                .expect("And/But prefix present");
            out.push_str(indent);
            out.push_str("Given ");
            out.push_str(rest);
            awaiting_first_step = false;
        } else {
            if is_step {
                awaiting_first_step = false;
            }
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}
