//! Scenario loading, execution and the Divan benchmark over the TCK corpus.
//!
//! Every step runs through the step functions registered against
//! [`GraphForgeWorld`] and resolved with `GraphForgeWorld::collection().find()`,
//! the same registry the Cucumber correctness run uses. There is no second
//! semantics engine.
//!
//! One timed iteration is one whole scenario: world construction, feature and
//! rule backgrounds, every scenario step (including `Given an empty graph`,
//! which leases and clears the pooled fixture) and the fixture release that the
//! Cucumber run performs in its `after` hook. Each iteration's pass/fail verdict
//! is recorded inside the timed region and checked outside it, before the next
//! sample starts and before the benchmark function returns. Divan emits a
//! benchmark's timing only after that function returns, so a failing scenario
//! panics first and never yields timing.

use std::cell::RefCell;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use cucumber::gherkin;
use cucumber::{Parser as _, World as _};
use futures::{FutureExt as _, StreamExt as _};

use crate::GraphForgeWorld;

/// One expanded TCK scenario, keyed `<feature>:<line>:<name>` exactly as the
/// Cucumber runner and `tests/tck/passing_baseline.txt` key it.
#[derive(Clone)]
pub struct ScenarioCase {
    key: String,
    feature: Arc<gherkin::Feature>,
    rule: Option<Arc<gherkin::Rule>>,
    scenario: Arc<gherkin::Scenario>,
}

impl ScenarioCase {
    fn new(
        feature: &Arc<gherkin::Feature>,
        rule: Option<&Arc<gherkin::Rule>>,
        scenario: &gherkin::Scenario,
    ) -> Self {
        Self {
            key: crate::corpus::scenario_key(&feature.name, scenario.position.line, &scenario.name),
            feature: Arc::clone(feature),
            rule: rule.cloned(),
            scenario: Arc::new(scenario.clone()),
        }
    }

    /// Steps in execution order: feature background, rule background, then
    /// the scenario's own steps (the order Cucumber's runner uses).
    fn steps(&self) -> impl Iterator<Item = &gherkin::Step> {
        let feature_background = self.feature.background.iter().flat_map(|b| &b.steps);
        let rule_background = self
            .rule
            .iter()
            .flat_map(|rule| rule.background.iter().flat_map(|b| &b.steps));
        feature_background
            .chain(rule_background)
            .chain(&self.scenario.steps)
    }
}

/// Divan names each benchmark `scenario[<key>]` from this display value.
impl fmt::Display for ScenarioCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key)
    }
}

/// Why a scenario did not pass. Cucumber reports the same three cases as a
/// failed or skipped step, and the correctness run counts none of them as
/// passing.
#[derive(Debug)]
pub struct ScenarioFailure {
    pub key: String,
    pub step: String,
    pub reason: String,
}

impl fmt::Display for ScenarioFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TCK benchmark scenario failed; no timing is recorded: {} at step `{}`: {}",
            self.key, self.step, self.reason
        )
    }
}

/// Parse a normalized feature tree with Cucumber's own parser, which expands
/// scenario outlines exactly as the correctness run does. Any parse error, an
/// empty corpus or a duplicate key fails closed.
pub fn load_scenarios(root: &Path) -> Vec<ScenarioCase> {
    let parsed: Vec<_> = futures::executor::block_on(
        cucumber::parser::Basic::default()
            .parse(root, cucumber::parser::basic::Cli::default())
            .collect(),
    );
    let mut cases = Vec::new();
    for feature in parsed {
        let feature =
            Arc::new(feature.unwrap_or_else(|error| panic!("TCK feature parse error: {error}")));
        for scenario in &feature.scenarios {
            cases.push(ScenarioCase::new(&feature, None, scenario));
        }
        for rule in &feature.rules {
            let rule = Arc::new(rule.clone());
            for scenario in &rule.scenarios {
                cases.push(ScenarioCase::new(&feature, Some(&rule), scenario));
            }
        }
    }
    assert!(
        !cases.is_empty(),
        "no TCK scenarios found under {}",
        root.display()
    );
    let mut keys = std::collections::BTreeSet::new();
    for case in &cases {
        assert!(
            keys.insert(case.key.as_str()),
            "duplicate TCK scenario key {}",
            case.key
        );
    }
    cases
}

/// Normalize the feature tree under `source` into a fresh temporary directory,
/// exactly as the Cucumber run does, and load its scenarios. The directory is
/// returned so it outlives the run.
pub fn load_normalized(source: &Path) -> (tempfile::TempDir, Vec<ScenarioCase>) {
    let normalized = tempfile::TempDir::new().expect("temp dir for normalized TCK corpus");
    crate::corpus::copy_features_normalized(source, normalized.path());
    let cases = load_scenarios(normalized.path());
    (normalized, cases)
}

static CORPUS: OnceLock<Vec<ScenarioCase>> = OnceLock::new();

/// Install the scenario set the Divan benchmark iterates. Once per process.
pub fn install_corpus(cases: Vec<ScenarioCase>) {
    assert!(
        CORPUS.set(cases).is_ok(),
        "the TCK benchmark corpus is installed once per process"
    );
}

/// Divan argument source: the installed corpus. Uninstalled fails closed
/// rather than benchmarking an empty set.
fn scenarios() -> Vec<ScenarioCase> {
    CORPUS
        .get()
        .expect("install_corpus must run before Divan")
        .clone()
}

fn collection() -> &'static cucumber::step::Collection<GraphForgeWorld> {
    static COLLECTION: OnceLock<cucumber::step::Collection<GraphForgeWorld>> = OnceLock::new();
    COLLECTION.get_or_init(GraphForgeWorld::collection)
}

/// The same multi-thread runtime flavour `#[tokio::main]` gives the Cucumber run.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("TCK benchmark runtime")
    })
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

async fn run_steps(
    world: &mut GraphForgeWorld,
    case: &ScenarioCase,
) -> Result<(), ScenarioFailure> {
    let failure = |step: &gherkin::Step, reason: String| ScenarioFailure {
        key: case.key.clone(),
        step: format!("{}{}", step.keyword, step.value),
        reason,
    };
    for step in case.steps() {
        let (step_fn, _captures, _location, context) = match collection().find(step) {
            Ok(Some(found)) => found,
            Ok(None) => return Err(failure(step, "no registered step matches".to_owned())),
            Err(error) => return Err(failure(step, format!("ambiguous step: {error}"))),
        };
        if let Err(payload) = AssertUnwindSafe(step_fn(world, context))
            .catch_unwind()
            .await
        {
            return Err(failure(step, panic_message(payload.as_ref())));
        }
    }
    Ok(())
}

/// Execute one whole scenario and return its verdict.
///
/// A test-only fault injection (`tests/bdd/fault.rs`) that targets this
/// scenario delays it here, inside the timed region.
pub fn execute(case: &ScenarioCase) -> Result<(), ScenarioFailure> {
    crate::fault::inject(&case.key);
    runtime().block_on(async {
        let mut world = GraphForgeWorld::new()
            .await
            .unwrap_or_else(|error| panic!("GraphForgeWorld::new: {error}"));
        let verdict = run_steps(&mut world, case).await;
        // The Cucumber run's `after` hook: return the fixture to the pool,
        // which clears it on the next lease.
        crate::fixture::release(&mut world.forge);
        verdict
    })
}

/// Fail closed on any recorded non-passing verdict. Runs outside the timed
/// region, before Divan can record the sample it belongs to.
fn require_passed(verdicts: &RefCell<Vec<Result<(), ScenarioFailure>>>) {
    for verdict in verdicts.borrow_mut().drain(..) {
        if let Err(failure) = verdict {
            panic!("{failure}");
        }
    }
}

/// One Divan benchmark per TCK scenario, named `scenario[<feature>:<line>:<name>]`.
///
/// `sample_size = 1` makes each sample exactly one scenario execution; the
/// sample count is a default that `--sample-count` overrides.
#[divan::bench(args = scenarios(), sample_count = 10, sample_size = 1)]
fn scenario(bencher: divan::Bencher, case: &ScenarioCase) {
    let verdicts = RefCell::new(Vec::with_capacity(1));
    bencher
        .with_inputs(|| require_passed(&verdicts))
        .bench_local_values(|()| {
            let verdict = execute(case);
            verdicts.borrow_mut().push(verdict);
        });
    require_passed(&verdicts);
}

/// The fixture profile both runners share: one pooled engine per concurrency slot.
pub fn assert_fixture_profile() {
    let created = crate::fixture::created_count();
    assert!(
        created <= crate::fixture::TCK_CONCURRENCY,
        "TCK fixture pool created {created} engines for concurrency {}",
        crate::fixture::TCK_CONCURRENCY
    );
}
