//! Diagnostic timing reports for the Rust BDD runner.
//!
//! Cucumber scenario timings are shared diagnostics (#1654): distributions and
//! slowest-scenario lists only. Performance thresholds belong to the
//! provenance-gated `make tck-perf` consumer, which reads BenchExec and Divan
//! evidence. `scripts/ci/benchmark-measurement-policy.py` rejects threshold
//! consumers in this directory.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// The report schema. Version 3 is the first diagnostic-only version.
pub const REPORT_SCHEMA_VERSION: u32 = 3;
pub const REPORT_KIND: &str = "diagnostic";
pub const THRESHOLD_AUTHORITY: &str = "none; thresholds are owned by make tck-perf";
/// The schema of the pre-#1654 policy and baseline files.
pub const LEGACY_SCHEMA_VERSION: u32 = 2;
/// The versioned TCK fixture profile (see `fixture.rs`).
pub const FIXTURE_PROFILE: &str = "pooled-isolated-serial-v1";

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

/// A pre-#1654 file under `tests/tck/` (`performance_policy.json` or
/// `performance_baseline.json`). Schema 2 made Cucumber timings a threshold
/// authority; they no longer are. These files are only read, to report that
/// they are legacy diagnostic context, and never drive a comparison.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LegacyFile {
    Absent,
    Schema2,
    /// Present but unreadable, malformed or not schema 2. Reported, never a panic.
    Unrecognised(String),
}

/// The shape both schema-2 files share. Only the version is inspected.
#[derive(Deserialize)]
struct LegacyHeader {
    schema_version: u32,
}

pub fn load_legacy(path: &Path) -> LegacyFile {
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return LegacyFile::Absent,
        Err(error) => return LegacyFile::Unrecognised(format!("unreadable: {}", error.kind())),
    };
    match serde_json::from_str::<LegacyHeader>(&body) {
        Ok(header) if header.schema_version == LEGACY_SCHEMA_VERSION => LegacyFile::Schema2,
        Ok(header) => LegacyFile::Unrecognised(format!("schema {}", header.schema_version)),
        Err(error) => LegacyFile::Unrecognised(format!("malformed JSON at line {}", error.line())),
    }
}

/// The single notice for the legacy files, or `None` when neither exists.
/// `files` pairs a repository-relative display name with its load result.
pub fn legacy_notice(files: &[(&str, LegacyFile)]) -> Option<String> {
    let present: Vec<String> = files
        .iter()
        .filter_map(|(name, file)| match file {
            LegacyFile::Absent => None,
            LegacyFile::Schema2 => Some(format!("{name} (schema 2)")),
            LegacyFile::Unrecognised(reason) => Some(format!("{name} ({reason})")),
        })
        .collect();
    if present.is_empty() {
        return None;
    }
    Some(format!(
        "legacy diagnostic baseline: {} loaded as diagnostic context only; Cucumber timings are \
         not a performance threshold authority. Run `make tck-perf` for the provenance-gated \
         comparison (#1654).",
        present.join(", ")
    ))
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

/// Cucumber timing report. Diagnostic only: it carries distributions and the
/// slowest scenarios, and never findings or a baseline comparison.
#[derive(Clone, Debug, Serialize)]
pub struct TimingReport {
    pub schema_version: u32,
    pub report_kind: &'static str,
    pub threshold_authority: &'static str,
    pub partial: bool,
    pub fixture_profile: &'static str,
    pub tck_concurrency: usize,
    pub legacy_notice: Option<String>,
    pub suites: Vec<SuiteReport>,
}
fn micros_to_ms(value: u64) -> f64 {
    value as f64 / 1_000.0
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

pub fn non_passing_scenario_keys(records: &[ScenarioTiming], suite: Suite) -> Vec<&str> {
    let mut failures: Vec<&str> = records
        .iter()
        .filter(|record| record.suite == suite && record.outcome != ScenarioOutcome::Passed)
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

pub fn build_report(
    records: &[ScenarioTiming],
    partial: bool,
    tck_concurrency: usize,
    legacy_notice: Option<String>,
) -> TimingReport {
    TimingReport {
        schema_version: REPORT_SCHEMA_VERSION,
        report_kind: REPORT_KIND,
        threshold_authority: THRESHOLD_AUTHORITY,
        partial,
        fixture_profile: FIXTURE_PROFILE,
        tck_concurrency,
        legacy_notice,
        suites: vec![
            suite_report(records, Suite::Api),
            suite_report(records, Suite::Tck),
        ],
    }
}

pub fn render_markdown(report: &TimingReport) -> String {
    let mut lines = vec![
        "# Rust BDD timing report (diagnostic)".to_owned(),
        String::new(),
        "Diagnostic only: these timings drive no performance threshold. \
         Run `make tck-perf` for the provenance-gated comparison."
            .to_owned(),
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
    if let Some(notice) = &report.legacy_notice {
        lines.extend([String::new(), format!("Notice: {notice}")]);
    }
    lines.push(String::new());
    lines.join("\n")
}

pub fn write_artifacts(output_dir: &Path, report: &TimingReport) -> Result<String, String> {
    fs::create_dir_all(output_dir).map_err(|error| {
        format!(
            "failed to create timing output directory {}: {error}",
            output_dir.display()
        )
    })?;
    let report_json = serde_json::to_string_pretty(report)
        .map_err(|error| format!("failed to serialize timing report: {error}"))?;
    let markdown = render_markdown(report);
    fs::write(output_dir.join("report.json"), format!("{report_json}\n"))
        .map_err(|error| format!("failed to write timing report: {error}"))?;
    fs::write(output_dir.join("summary.md"), &markdown)
        .map_err(|error| format!("failed to write timing summary: {error}"))?;
    Ok(markdown)
}
