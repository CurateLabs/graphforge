//! Deterministic timing reports and warning policy for the Rust BDD runner.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Suite {
    Api,
    Tck,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioOutcome {
    Passed,
    Skipped,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScenarioTiming {
    pub suite: Suite,
    pub key: String,
    pub feature: String,
    pub line: usize,
    pub name: String,
    pub outcome: ScenarioOutcome,
    pub elapsed_us: u64,
}

#[derive(Debug)]
struct ActiveTiming {
    suite: Suite,
    key: String,
    feature: String,
    line: usize,
    name: String,
    outcome: ScenarioOutcome,
    started: Instant,
}

/// Tracks raw cucumber events without assuming scenarios finish in start order.
#[derive(Default, Debug)]
pub struct ScenarioTimer {
    active: HashMap<(String, usize), ActiveTiming>,
}

impl ScenarioTimer {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        &mut self,
        suite: Suite,
        key: String,
        attempt: usize,
        feature: String,
        line: usize,
        name: String,
        now: Instant,
    ) -> Result<(), String> {
        let active_key = (key.clone(), attempt);
        if self
            .active
            .insert(
                active_key,
                ActiveTiming {
                    suite,
                    key,
                    feature,
                    line,
                    name,
                    outcome: ScenarioOutcome::Passed,
                    started: now,
                },
            )
            .is_some()
        {
            return Err("duplicate scenario start event".to_owned());
        }
        Ok(())
    }

    pub fn mark_failed(&mut self, key: &str, attempt: usize) {
        if let Some(active) = self.active.get_mut(&(key.to_owned(), attempt)) {
            active.outcome = ScenarioOutcome::Failed;
        }
    }

    pub fn mark_skipped(&mut self, key: &str, attempt: usize) {
        if let Some(active) = self.active.get_mut(&(key.to_owned(), attempt))
            && active.outcome == ScenarioOutcome::Passed
        {
            active.outcome = ScenarioOutcome::Skipped;
        }
    }

    pub fn finish(
        &mut self,
        key: &str,
        attempt: usize,
        now: Instant,
    ) -> Result<ScenarioTiming, String> {
        let active = self
            .active
            .remove(&(key.to_owned(), attempt))
            .ok_or_else(|| "scenario finish without matching start".to_owned())?;
        let elapsed_us = now
            .checked_duration_since(active.started)
            .ok_or_else(|| "scenario clock moved backwards".to_owned())?
            .as_micros()
            .try_into()
            .map_err(|_| "scenario duration exceeds u64 microseconds".to_owned())?;
        let timing = ScenarioTiming {
            suite: active.suite,
            key: active.key,
            feature: active.feature,
            line: active.line,
            name: active.name,
            outcome: active.outcome,
            elapsed_us,
        };
        Ok(timing)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimingPolicy {
    pub schema_version: u32,
    pub baseline_required: bool,
    pub fixture_profile: String,
    pub tck_concurrency: usize,
    pub per_scenario_multiplier: f64,
    pub per_scenario_min_delta_ms: f64,
    pub aggregate_multiplier: f64,
    pub aggregate_min_delta_ms: f64,
    pub absolute_slow_ms: Option<f64>,
    pub max_warning_annotations: usize,
}

impl TimingPolicy {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(format!(
                "unsupported timing policy schema {}",
                self.schema_version
            ));
        }
        if self.per_scenario_multiplier < 1.0
            || self.aggregate_multiplier < 1.0
            || self.per_scenario_min_delta_ms < 0.0
            || self.aggregate_min_delta_ms < 0.0
            || self.absolute_slow_ms.is_some_and(|value| value <= 0.0)
            || self.max_warning_annotations == 0
            || self.fixture_profile.trim().is_empty()
            || self.tck_concurrency == 0
        {
            return Err("invalid TCK timing policy values".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimingBaseline {
    pub schema_version: u32,
    pub partial: bool,
    pub fixture_profile: String,
    pub tck_concurrency: usize,
    pub runner: String,
    pub rust_toolchain: String,
    pub source_commit: String,
    pub scenario_count: usize,
    pub total_elapsed_us: u64,
    pub features: BTreeMap<String, u64>,
    pub scenarios: BTreeMap<String, u64>,
    pub suggested_absolute_slow_ms: f64,
}

impl TimingBaseline {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(format!(
                "unsupported timing baseline schema {}",
                self.schema_version
            ));
        }
        if self.scenario_count != self.scenarios.len() {
            return Err(format!(
                "timing baseline count {} does not match {} scenario entries",
                self.scenario_count,
                self.scenarios.len()
            ));
        }
        if self.fixture_profile.trim().is_empty() || self.tck_concurrency == 0 {
            return Err("invalid timing baseline fixture profile".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Distribution {
    pub count: usize,
    pub sum_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub median_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScenarioReport {
    pub key: String,
    pub feature: String,
    pub line: usize,
    pub name: String,
    pub outcome: ScenarioOutcome,
    pub elapsed_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct FeatureReport {
    pub feature: String,
    pub distribution: Distribution,
}

#[derive(Clone, Debug, Serialize)]
pub struct SuiteReport {
    pub suite: Suite,
    pub distribution: Distribution,
    pub features: Vec<FeatureReport>,
    pub slowest: Vec<ScenarioReport>,
    pub scenarios: Vec<ScenarioReport>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DeltaContributor {
    pub feature: String,
    pub delta_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    ScenarioRegression,
    AggregateRegression,
}

#[derive(Clone, Debug, Serialize)]
pub struct PerformanceFinding {
    pub kind: FindingKind,
    pub message: String,
    pub key: Option<String>,
    pub baseline_ms: f64,
    pub current_ms: f64,
    pub threshold_ms: f64,
    pub delta_ms: f64,
    pub contributors: Vec<DeltaContributor>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TimingReport {
    pub schema_version: u32,
    pub partial: bool,
    pub fixture_profile: String,
    pub tck_concurrency: usize,
    pub baseline_status: String,
    pub suites: Vec<SuiteReport>,
    pub findings: Vec<PerformanceFinding>,
    pub unbaselined_tck_scenarios: Vec<String>,
    pub missing_baseline_tck_scenarios: Vec<String>,
}

fn micros_to_ms(value: u64) -> f64 {
    value as f64 / 1_000.0
}

fn ms_to_micros(value: f64) -> f64 {
    value * 1_000.0
}

pub fn distribution(values: &[u64]) -> Distribution {
    if values.is_empty() {
        return Distribution {
            count: 0,
            sum_ms: 0.0,
            min_ms: 0.0,
            max_ms: 0.0,
            mean_ms: 0.0,
            median_ms: 0.0,
            p90_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
        };
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let sum: u64 = sorted.iter().sum();
    let midpoint = sorted.len() / 2;
    let median_us = if sorted.len().is_multiple_of(2) {
        (sorted[midpoint - 1] as f64 + sorted[midpoint] as f64) / 2.0
    } else {
        sorted[midpoint] as f64
    };
    let nearest_rank = |percentile: f64| {
        let rank = (percentile * sorted.len() as f64).ceil() as usize;
        sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
    };
    Distribution {
        count: sorted.len(),
        sum_ms: micros_to_ms(sum),
        min_ms: micros_to_ms(sorted[0]),
        max_ms: micros_to_ms(*sorted.last().expect("non-empty durations")),
        mean_ms: micros_to_ms(sum) / sorted.len() as f64,
        median_ms: median_us / 1_000.0,
        p90_ms: micros_to_ms(nearest_rank(0.90)),
        p95_ms: micros_to_ms(nearest_rank(0.95)),
        p99_ms: micros_to_ms(nearest_rank(0.99)),
    }
}

pub fn failed_scenario_keys(records: &[ScenarioTiming], suite: Suite) -> Vec<&str> {
    let mut failures: Vec<&str> = records
        .iter()
        .filter(|record| record.suite == suite && record.outcome == ScenarioOutcome::Failed)
        .map(|record| record.key.as_str())
        .collect();
    failures.sort_unstable();
    failures
}

fn suite_report(records: &[ScenarioTiming], suite: Suite) -> SuiteReport {
    let mut selected: Vec<&ScenarioTiming> = records
        .iter()
        .filter(|record| record.suite == suite)
        .collect();
    selected.sort_by(|left, right| left.key.cmp(&right.key));
    let durations: Vec<u64> = selected.iter().map(|record| record.elapsed_us).collect();

    let mut by_feature: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    for record in &selected {
        by_feature
            .entry(&record.feature)
            .or_default()
            .push(record.elapsed_us);
    }
    let features = by_feature
        .into_iter()
        .map(|(feature, values)| FeatureReport {
            feature: feature.to_owned(),
            distribution: distribution(&values),
        })
        .collect();

    let scenarios: Vec<ScenarioReport> = selected
        .iter()
        .map(|record| ScenarioReport {
            key: record.key.clone(),
            feature: record.feature.clone(),
            line: record.line,
            name: record.name.clone(),
            outcome: record.outcome,
            elapsed_ms: micros_to_ms(record.elapsed_us),
        })
        .collect();
    let slow_limit = if suite == Suite::Tck { 25 } else { 10 };
    let mut slowest = scenarios.clone();
    slowest.sort_by(|left, right| {
        right
            .elapsed_ms
            .total_cmp(&left.elapsed_ms)
            .then_with(|| left.key.cmp(&right.key))
    });
    slowest.truncate(slow_limit);

    SuiteReport {
        suite,
        distribution: distribution(&durations),
        features,
        slowest,
        scenarios,
    }
}

fn current_feature_totals(records: &[ScenarioTiming]) -> BTreeMap<String, u64> {
    let mut totals = BTreeMap::new();
    for record in records.iter().filter(|record| record.suite == Suite::Tck) {
        *totals.entry(record.feature.clone()).or_default() += record.elapsed_us;
    }
    totals
}

pub fn baseline_candidate(
    records: &[ScenarioTiming],
    partial: bool,
    policy: &TimingPolicy,
) -> TimingBaseline {
    let tck: Vec<&ScenarioTiming> = records
        .iter()
        .filter(|record| record.suite == Suite::Tck)
        .collect();
    let scenarios = tck
        .iter()
        .map(|record| (record.key.clone(), record.elapsed_us))
        .collect();
    let features = current_feature_totals(records);
    let durations: Vec<u64> = tck.iter().map(|record| record.elapsed_us).collect();
    let stats = distribution(&durations);
    let suggested = ((2.0 * stats.p99_ms).max(stats.max_ms + 250.0) / 100.0).ceil() * 100.0;
    TimingBaseline {
        schema_version: SCHEMA_VERSION,
        partial,
        fixture_profile: policy.fixture_profile.clone(),
        tck_concurrency: policy.tck_concurrency,
        runner: std::env::var("BDD_RUNNER_LABEL")
            .or_else(|_| std::env::var("RUNNER_OS"))
            .unwrap_or_else(|_| "local".to_owned()),
        rust_toolchain: "1.96.0".to_owned(),
        source_commit: std::env::var("GITHUB_SHA").unwrap_or_else(|_| "local".to_owned()),
        scenario_count: tck.len(),
        total_elapsed_us: tck.iter().map(|record| record.elapsed_us).sum(),
        features,
        scenarios,
        suggested_absolute_slow_ms: suggested,
    }
}

pub fn build_report(
    records: &[ScenarioTiming],
    policy: &TimingPolicy,
    baseline: Option<&TimingBaseline>,
    partial: bool,
) -> Result<TimingReport, String> {
    policy.validate()?;
    if let Some(baseline) = baseline {
        baseline.validate()?;
        if baseline.fixture_profile != policy.fixture_profile
            || baseline.tck_concurrency != policy.tck_concurrency
        {
            return Err(format!(
                "timing baseline fixture profile {} / concurrency {} does not match policy {} / concurrency {}",
                baseline.fixture_profile,
                baseline.tck_concurrency,
                policy.fixture_profile,
                policy.tck_concurrency
            ));
        }
    } else if policy.baseline_required && !partial {
        return Err("required TCK performance baseline is missing".to_owned());
    }

    let mut findings = Vec::new();
    let mut unbaselined = Vec::new();
    let mut missing = Vec::new();
    let current: BTreeMap<&str, &ScenarioTiming> = records
        .iter()
        .filter(|record| record.suite == Suite::Tck)
        .map(|record| (record.key.as_str(), record))
        .collect();

    if !partial && let Some(baseline) = baseline {
        for (key, record) in &current {
            let Some(baseline_us) = baseline.scenarios.get(*key) else {
                unbaselined.push((*key).to_owned());
                continue;
            };
            if record.outcome != ScenarioOutcome::Passed {
                continue;
            }
            let relative_threshold = (*baseline_us as f64 * policy.per_scenario_multiplier)
                .max(*baseline_us as f64 + ms_to_micros(policy.per_scenario_min_delta_ms));
            let threshold = policy
                .absolute_slow_ms
                .map(ms_to_micros)
                .map_or(relative_threshold, |absolute| {
                    absolute.min(relative_threshold)
                });
            if record.elapsed_us as f64 > threshold {
                let baseline_ms = micros_to_ms(*baseline_us);
                let current_ms = micros_to_ms(record.elapsed_us);
                let threshold_ms = threshold / 1_000.0;
                findings.push(PerformanceFinding {
                    kind: FindingKind::ScenarioRegression,
                    message: format!(
                        "{key}: {current_ms:.3} ms (baseline {baseline_ms:.3} ms, warning threshold {threshold_ms:.3} ms)"
                    ),
                    key: Some((*key).to_owned()),
                    baseline_ms,
                    current_ms,
                    threshold_ms,
                    delta_ms: current_ms - baseline_ms,
                    contributors: Vec::new(),
                });
            }
        }
        missing.extend(
            baseline
                .scenarios
                .keys()
                .filter(|key| !current.contains_key(key.as_str()))
                .cloned(),
        );

        let current_total: u64 = current.values().map(|record| record.elapsed_us).sum();
        let aggregate_threshold = (baseline.total_elapsed_us as f64 * policy.aggregate_multiplier)
            .max(baseline.total_elapsed_us as f64 + ms_to_micros(policy.aggregate_min_delta_ms));
        if current_total as f64 > aggregate_threshold {
            let current_features = current_feature_totals(records);
            let mut contributors: Vec<DeltaContributor> = current_features
                .iter()
                .filter_map(|(feature, current_us)| {
                    let baseline_us = baseline.features.get(feature).copied().unwrap_or(0);
                    current_us
                        .checked_sub(baseline_us)
                        .map(|delta| DeltaContributor {
                            feature: feature.clone(),
                            delta_ms: micros_to_ms(delta),
                        })
                })
                .collect();
            contributors.sort_by(|left, right| {
                right
                    .delta_ms
                    .total_cmp(&left.delta_ms)
                    .then_with(|| left.feature.cmp(&right.feature))
            });
            contributors.truncate(10);
            let baseline_ms = micros_to_ms(baseline.total_elapsed_us);
            let current_ms = micros_to_ms(current_total);
            let threshold_ms = aggregate_threshold / 1_000.0;
            findings.push(PerformanceFinding {
                kind: FindingKind::AggregateRegression,
                message: format!(
                    "openCypher TCK total: {current_ms:.3} ms (baseline {baseline_ms:.3} ms, warning threshold {threshold_ms:.3} ms)"
                ),
                key: None,
                baseline_ms,
                current_ms,
                threshold_ms,
                delta_ms: current_ms - baseline_ms,
                contributors,
            });
        }
    } else if baseline.is_none() {
        unbaselined.extend(current.keys().map(|key| (*key).to_owned()));
    }

    findings.sort_by(|left, right| {
        right
            .delta_ms
            .total_cmp(&left.delta_ms)
            .then_with(|| left.key.cmp(&right.key))
    });
    Ok(TimingReport {
        schema_version: SCHEMA_VERSION,
        partial,
        fixture_profile: policy.fixture_profile.clone(),
        tck_concurrency: policy.tck_concurrency,
        baseline_status: if partial {
            "partial_not_compared"
        } else if baseline.is_some() {
            "compared"
        } else {
            "unbaselined"
        }
        .to_owned(),
        suites: vec![
            suite_report(records, Suite::Api),
            suite_report(records, Suite::Tck),
        ],
        findings,
        unbaselined_tck_scenarios: unbaselined,
        missing_baseline_tck_scenarios: missing,
    })
}

pub fn load_policy(path: &Path) -> Result<TimingPolicy, String> {
    let body = fs::read_to_string(path)
        .map_err(|error| format!("failed to read timing policy {}: {error}", path.display()))?;
    let policy: TimingPolicy = serde_json::from_str(&body)
        .map_err(|error| format!("invalid timing policy {}: {error}", path.display()))?;
    policy.validate()?;
    Ok(policy)
}

pub fn load_baseline(path: &Path, required: bool) -> Result<Option<TimingBaseline>, String> {
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => return Ok(None),
        Err(error) => {
            return Err(format!(
                "failed to read timing baseline {}: {error}",
                path.display()
            ));
        }
    };
    let baseline: TimingBaseline = serde_json::from_str(&body)
        .map_err(|error| format!("invalid timing baseline {}: {error}", path.display()))?;
    baseline.validate()?;
    Ok(Some(baseline))
}

pub fn render_markdown(report: &TimingReport) -> String {
    let mut lines = vec![
        "# Rust BDD timing report".to_owned(),
        String::new(),
        format!("Baseline status: `{}`", report.baseline_status),
        format!(
            "Fixture profile: `{}` with TCK concurrency `{}`",
            report.fixture_profile, report.tck_concurrency
        ),
        String::new(),
        "| Suite | Scenarios | Sum | Min | Mean | Median | p90 | p95 | p99 | Max |".to_owned(),
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|".to_owned(),
    ];
    for suite in &report.suites {
        let name = if suite.suite == Suite::Tck {
            "TCK"
        } else {
            "API"
        };
        let d = &suite.distribution;
        lines.push(format!(
            "| {name} | {} | {:.3} ms | {:.3} ms | {:.3} ms | {:.3} ms | {:.3} ms | {:.3} ms | {:.3} ms | {:.3} ms |",
            d.count, d.sum_ms, d.min_ms, d.mean_ms, d.median_ms, d.p90_ms, d.p95_ms, d.p99_ms, d.max_ms
        ));
    }
    for suite in &report.suites {
        let name = if suite.suite == Suite::Tck {
            "TCK"
        } else {
            "API"
        };
        lines.extend([
            String::new(),
            format!("## Slowest {name} scenarios"),
            String::new(),
            "| Scenario | Outcome | Elapsed |".to_owned(),
            "|---|---|---:|".to_owned(),
        ]);
        for scenario in &suite.slowest {
            lines.push(format!(
                "| `{}` | `{:?}` | {:.3} ms |",
                scenario.key, scenario.outcome, scenario.elapsed_ms
            ));
        }

        let mut features: Vec<&FeatureReport> = suite.features.iter().collect();
        features.sort_by(|left, right| {
            right
                .distribution
                .sum_ms
                .total_cmp(&left.distribution.sum_ms)
                .then_with(|| left.feature.cmp(&right.feature))
        });
        features.truncate(10);
        lines.extend([
            String::new(),
            format!("### Highest-total {name} features"),
            String::new(),
            "| Feature | Scenarios | Sum | Mean | p95 | Max |".to_owned(),
            "|---|---:|---:|---:|---:|---:|".to_owned(),
        ]);
        for feature in features {
            let d = &feature.distribution;
            lines.push(format!(
                "| `{}` | {} | {:.3} ms | {:.3} ms | {:.3} ms | {:.3} ms |",
                feature.feature, d.count, d.sum_ms, d.mean_ms, d.p95_ms, d.max_ms
            ));
        }
    }
    lines.extend([
        String::new(),
        "## Performance warnings".to_owned(),
        String::new(),
    ]);
    if report.findings.is_empty() {
        lines.push("None.".to_owned());
    } else {
        lines.extend(
            report
                .findings
                .iter()
                .map(|finding| format!("- {}", finding.message)),
        );
    }
    lines.push(String::new());
    lines.join("\n")
}

pub fn write_artifacts(
    output_dir: &Path,
    report: &TimingReport,
    candidate: &TimingBaseline,
) -> Result<String, String> {
    fs::create_dir_all(output_dir).map_err(|error| {
        format!(
            "failed to create timing output directory {}: {error}",
            output_dir.display()
        )
    })?;
    let report_json = serde_json::to_string_pretty(report)
        .map_err(|error| format!("failed to serialize timing report: {error}"))?;
    let candidate_json = serde_json::to_string_pretty(candidate)
        .map_err(|error| format!("failed to serialize timing baseline candidate: {error}"))?;
    let markdown = render_markdown(report);
    fs::write(output_dir.join("report.json"), format!("{report_json}\n"))
        .map_err(|error| format!("failed to write timing report: {error}"))?;
    fs::write(
        output_dir.join("tck-baseline-candidate.json"),
        format!("{candidate_json}\n"),
    )
    .map_err(|error| format!("failed to write timing baseline candidate: {error}"))?;
    fs::write(output_dir.join("summary.md"), &markdown)
        .map_err(|error| format!("failed to write timing summary: {error}"))?;
    Ok(markdown)
}

pub fn escape_github_command(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
        .replace(':', "%3A")
        .replace(',', "%2C")
}

pub fn annotation_messages(report: &TimingReport, maximum: usize) -> Vec<String> {
    assert!(maximum > 0, "annotation maximum must be positive");
    if report.findings.len() <= maximum {
        return report
            .findings
            .iter()
            .map(|finding| finding.message.clone())
            .collect();
    }
    let detailed = maximum.saturating_sub(1);
    let mut messages: Vec<String> = report
        .findings
        .iter()
        .take(detailed)
        .map(|finding| finding.message.clone())
        .collect();
    messages.push(format!(
        "{} TCK performance warning(s); {detailed} detailed annotation(s) emitted — see report.json",
        report.findings.len()
    ));
    messages
}
