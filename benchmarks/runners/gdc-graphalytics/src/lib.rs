//! GDC Graphalytics suite: algorithm mapping, reference validation, evidence.
//!
//! Workload semantics live here (not in shared `gdc_contracts`). Product
//! algorithm behavior remains in GraphForge Rust crates; this runner only maps
//! Graphalytics jobs onto public analyst-verb contracts and validates outputs.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::Path;
use std::str::FromStr;

pub const EVIDENCE_SCHEMA: &str = "graphforge-gdc-graphalytics-evidence/1";
pub const LADDER_SCHEMA: &str = "graphforge-gdc-graphalytics-ladder/1";
pub const JOB_SCHEMA: &str = "graphforge-gdc-graphalytics-job/1";
pub const SUITE_ID: &str = "graphalytics";

/// Graphalytics relative epsilon for PR / LCC / SSSP (`|r-s| <= ε|r|`).
pub const EPSILON: f64 = 0.0001;

/// Graphalytics BFS unreachable sentinel (`i64::MAX`).
pub const BFS_UNREACHABLE: i64 = 9_223_372_036_854_775_807;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    Bfs,
    Pr,
    Wcc,
    Cdlp,
    Lcc,
    Sssp,
}

impl Algorithm {
    pub const ALL: [Self; 6] = [
        Self::Bfs,
        Self::Pr,
        Self::Wcc,
        Self::Cdlp,
        Self::Lcc,
        Self::Sssp,
    ];

    pub fn workload_key(self) -> &'static str {
        match self {
            Self::Bfs => "bfs",
            Self::Pr => "pr",
            Self::Wcc => "wcc",
            Self::Cdlp => "cdlp",
            Self::Lcc => "lcc",
            Self::Sssp => "sssp",
        }
    }

    pub fn validation_mode(self) -> ValidationMode {
        match self {
            Self::Bfs | Self::Cdlp => ValidationMode::Exact,
            Self::Wcc => ValidationMode::Equivalence,
            Self::Pr | Self::Lcc | Self::Sssp => ValidationMode::Epsilon { epsilon: EPSILON },
        }
    }
}

impl fmt::Display for Algorithm {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.workload_key())
    }
}

impl FromStr for Algorithm {
    type Err = SuiteError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "bfs" => Ok(Self::Bfs),
            "pr" => Ok(Self::Pr),
            "wcc" => Ok(Self::Wcc),
            "cdlp" => Ok(Self::Cdlp),
            "lcc" => Ok(Self::Lcc),
            "sssp" => Ok(Self::Sssp),
            _ => Err(SuiteError::InvalidDocument(format!(
                "unknown Graphalytics algorithm: {value}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ValidationMode {
    Exact,
    Equivalence,
    Epsilon { epsilon: f64 },
}

impl ValidationMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Equivalence => "equivalence",
            Self::Epsilon { .. } => "epsilon",
        }
    }
}

/// Declared public GraphForge analyst-verb contract for a compatible mapping.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicApiMapping {
    pub verb: String,
    pub by: String,
    pub directed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight_property: Option<String>,
    pub notes: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MappingOutcome {
    Compatible(PublicApiMapping),
    SemanticIncompatibility { cause: &'static str, detail: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlgorithmJob {
    pub schema: String,
    pub suite_id: String,
    pub dataset_id: String,
    pub algorithm: Algorithm,
    pub directed: bool,
    #[serde(default)]
    pub source_vertex: Option<u64>,
    #[serde(default)]
    pub damping: Option<f64>,
    #[serde(default)]
    pub max_iterations: Option<u32>,
    #[serde(default)]
    pub weight_property: Option<String>,
}

impl AlgorithmJob {
    pub fn validate_schema(&self) -> Result<(), SuiteError> {
        if self.schema != JOB_SCHEMA {
            return Err(SuiteError::InvalidDocument(format!(
                "unexpected job schema: {}",
                self.schema
            )));
        }
        if self.suite_id != SUITE_ID {
            return Err(SuiteError::InvalidDocument(
                "job suite_id must be graphalytics".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetLadder {
    pub schema: String,
    pub suite_id: String,
    pub datasets: Vec<LadderEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LadderEntry {
    pub id: String,
    pub order: u32,
    pub role: String,
}

impl DatasetLadder {
    pub fn validate(&self) -> Result<(), SuiteError> {
        if self.schema != LADDER_SCHEMA {
            return Err(SuiteError::InvalidDocument(format!(
                "unexpected ladder schema: {}",
                self.schema
            )));
        }
        if self.suite_id != SUITE_ID {
            return Err(SuiteError::InvalidDocument(
                "ladder suite_id must be graphalytics".into(),
            ));
        }
        if self.datasets.is_empty() {
            return Err(SuiteError::InvalidDocument(
                "dataset ladder must declare at least one dataset".into(),
            ));
        }
        let mut orders = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for entry in &self.datasets {
            if !orders.insert(entry.order) {
                return Err(SuiteError::InvalidDocument(format!(
                    "duplicate ladder order: {}",
                    entry.order
                )));
            }
            if !ids.insert(entry.id.clone()) {
                return Err(SuiteError::InvalidDocument(format!(
                    "duplicate ladder dataset: {}",
                    entry.id
                )));
            }
        }
        let mut sorted = self.datasets.clone();
        sorted.sort_by_key(|entry| entry.order);
        if sorted.first().map(|entry| entry.id.as_str()) != Some("ga-tiny") {
            return Err(SuiteError::InvalidDocument(
                "ordered ladder must begin with bounded fixture ga-tiny".into(),
            ));
        }
        Ok(())
    }

    pub fn ordered_ids(&self) -> Vec<String> {
        let mut sorted = self.datasets.clone();
        sorted.sort_by_key(|entry| entry.order);
        sorted.into_iter().map(|entry| entry.id).collect()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmStatus {
    Passed,
    Failed,
    SemanticIncompatibility,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AlgorithmOutcome {
    pub algorithm: Algorithm,
    pub workload_key: String,
    pub status: AlgorithmStatus,
    pub validation_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_api: Option<PublicApiMapping>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SuiteEvidence {
    pub schema: String,
    pub suite_id: String,
    pub dataset_id: String,
    pub status: AlgorithmStatus,
    pub identities: serde_json::Value,
    pub algorithms: Vec<AlgorithmOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SuiteError {
    InvalidDocument(String),
    ReferenceMismatch(String),
    SemanticIncompatibility { cause: String, detail: String },
}

impl fmt::Display for SuiteError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDocument(message) => write!(output, "invalid_document: {message}"),
            Self::ReferenceMismatch(message) => write!(output, "reference_mismatch: {message}"),
            Self::SemanticIncompatibility { cause, detail } => {
                write!(output, "semantic_incompatibility:{cause}: {detail}")
            }
        }
    }
}

impl std::error::Error for SuiteError {}

/// Map a Graphalytics job onto the public GraphForge analyst-verb surface.
///
/// Unsupported official semantics fail closed with a typed cause instead of
/// silently approximating with a different algorithm contract.
pub fn map_algorithm(job: &AlgorithmJob) -> Result<MappingOutcome, SuiteError> {
    job.validate_schema()?;
    match job.algorithm {
        Algorithm::Bfs => {
            let Some(source) = job.source_vertex else {
                return Err(SuiteError::InvalidDocument(
                    "bfs requires source_vertex".into(),
                ));
            };
            Ok(MappingOutcome::Compatible(PublicApiMapping {
                verb: "paths".into(),
                by: "bfs".into(),
                directed: job.directed,
                weight_property: None,
                notes: format!(
                    "derive hop depth from paths(source={source}, by=bfs); unreachable={BFS_UNREACHABLE}"
                ),
            }))
        }
        Algorithm::Pr => {
            // Graphalytics PR is fixed-iteration; GraphForge pagerank converges
            // with opaque iteration control and no public max_iterations knob.
            if job.max_iterations.is_some() {
                return Ok(MappingOutcome::SemanticIncompatibility {
                    cause: "fixed_iteration_pagerank_not_exposed",
                    detail: "Graphalytics PR requires max_iterations; GraphForge rank(by=pagerank) uses convergence without a public iteration bound".into(),
                });
            }
            if job.damping.is_some_and(|damping| (damping - 0.85).abs() > f64::EPSILON) {
                return Ok(MappingOutcome::SemanticIncompatibility {
                    cause: "pagerank_damping_not_configurable",
                    detail: "GraphForge rank(by=pagerank) fixes damping at 0.85".into(),
                });
            }
            Ok(MappingOutcome::SemanticIncompatibility {
                cause: "fixed_iteration_pagerank_not_exposed",
                detail: "Graphalytics PR is iteration-bounded; GraphForge exposes only convergent pagerank".into(),
            })
        }
        Algorithm::Wcc => Ok(MappingOutcome::Compatible(PublicApiMapping {
            verb: "cluster".into(),
            by: "components".into(),
            directed: false,
            weight_property: None,
            notes: "weak connectivity via cluster(by=components); validation uses equivalence match"
                .into(),
        })),
        Algorithm::Cdlp => Ok(MappingOutcome::SemanticIncompatibility {
            cause: "synchronous_cdlp_not_exposed",
            detail: "Graphalytics CDLP is synchronous with max_iterations and min-label ties; GraphForge cluster(by=label_propagation) is asynchronous without a public iteration bound".into(),
        }),
        Algorithm::Lcc => Ok(MappingOutcome::Compatible(PublicApiMapping {
            verb: "rank".into(),
            by: "clustering_coefficient".into(),
            directed: job.directed,
            weight_property: None,
            notes: "rank(by=clustering_coefficient) with Graphalytics epsilon match".into(),
        })),
        Algorithm::Sssp => {
            let Some(source) = job.source_vertex else {
                return Err(SuiteError::InvalidDocument(
                    "sssp requires source_vertex".into(),
                ));
            };
            let weight = job
                .weight_property
                .clone()
                .unwrap_or_else(|| "weight".into());
            Ok(MappingOutcome::Compatible(PublicApiMapping {
                verb: "paths".into(),
                by: "dijkstra".into(),
                directed: job.directed,
                weight_property: Some(weight.clone()),
                notes: format!(
                    "derive distances from paths(source={source}, by=dijkstra, weight={weight}); unreachable=+infinity"
                ),
            }))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum VertexValue {
    Int(i64),
    Float(f64),
}

pub type VertexMap = BTreeMap<u64, VertexValue>;

pub fn parse_vertex_value_file(text: &str) -> Result<VertexMap, SuiteError> {
    let mut values = VertexMap::new();
    for (index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let vertex = parts
            .next()
            .ok_or_else(|| {
                SuiteError::InvalidDocument(format!("line {}: missing vertex", index + 1))
            })?
            .parse::<u64>()
            .map_err(|_| {
                SuiteError::InvalidDocument(format!("line {}: invalid vertex id", index + 1))
            })?;
        let value_token = parts.next().ok_or_else(|| {
            SuiteError::InvalidDocument(format!("line {}: missing value", index + 1))
        })?;
        if parts.next().is_some() {
            return Err(SuiteError::InvalidDocument(format!(
                "line {}: unexpected trailing tokens",
                index + 1
            )));
        }
        if values.contains_key(&vertex) {
            return Err(SuiteError::InvalidDocument(format!(
                "duplicate vertex id: {vertex}"
            )));
        }
        let value = parse_value_token(value_token)?;
        values.insert(vertex, value);
    }
    Ok(values)
}

fn parse_value_token(token: &str) -> Result<VertexValue, SuiteError> {
    let lowered = token.to_ascii_lowercase();
    if matches!(lowered.as_str(), "infinity" | "+infinity" | "inf" | "+inf") {
        return Ok(VertexValue::Float(f64::INFINITY));
    }
    if matches!(lowered.as_str(), "-infinity" | "-inf") {
        return Ok(VertexValue::Float(f64::NEG_INFINITY));
    }
    if let Ok(int_value) = token.parse::<i64>() {
        return Ok(VertexValue::Int(int_value));
    }
    let float_value = token
        .parse::<f64>()
        .map_err(|_| SuiteError::InvalidDocument(format!("invalid vertex value token: {token}")))?;
    if float_value.is_nan() {
        return Err(SuiteError::InvalidDocument(
            "NaN vertex values are invalid".into(),
        ));
    }
    Ok(VertexValue::Float(float_value))
}

pub fn load_vertex_value_file(path: &Path) -> Result<VertexMap, SuiteError> {
    let text = fs::read_to_string(path).map_err(|error| {
        SuiteError::InvalidDocument(format!("failed to read {}: {error}", path.display()))
    })?;
    parse_vertex_value_file(&text)
}

pub fn validate_reference(
    algorithm: Algorithm,
    reference: &VertexMap,
    system: &VertexMap,
) -> Result<(), SuiteError> {
    if reference.keys().ne(system.keys()) {
        let missing: Vec<_> = reference
            .keys()
            .filter(|key| !system.contains_key(key))
            .copied()
            .collect();
        let unexpected: Vec<_> = system
            .keys()
            .filter(|key| !reference.contains_key(key))
            .copied()
            .collect();
        return Err(SuiteError::ReferenceMismatch(format!(
            "{} vertex set mismatch missing={missing:?} unexpected={unexpected:?}",
            algorithm.workload_key()
        )));
    }
    match algorithm.validation_mode() {
        ValidationMode::Exact => validate_exact(reference, system),
        ValidationMode::Equivalence => validate_equivalence(reference, system),
        ValidationMode::Epsilon { epsilon } => validate_epsilon(reference, system, epsilon),
    }
}

fn as_int(value: &VertexValue) -> Result<i64, SuiteError> {
    match value {
        VertexValue::Int(value) => Ok(*value),
        VertexValue::Float(value) if value.fract() == 0.0 && value.is_finite() => Ok(*value as i64),
        VertexValue::Float(_) => Err(SuiteError::ReferenceMismatch(
            "expected integer vertex value".into(),
        )),
    }
}

fn as_float(value: &VertexValue) -> Result<f64, SuiteError> {
    match value {
        VertexValue::Float(value) => Ok(*value),
        VertexValue::Int(value) => Ok(*value as f64),
    }
}

fn validate_exact(reference: &VertexMap, system: &VertexMap) -> Result<(), SuiteError> {
    for (vertex, expected) in reference {
        let actual = &system[vertex];
        let expected_int = as_int(expected)?;
        let actual_int = as_int(actual)?;
        if expected_int != actual_int {
            return Err(SuiteError::ReferenceMismatch(format!(
                "exact mismatch at vertex {vertex}: expected {expected_int} got {actual_int}"
            )));
        }
    }
    Ok(())
}

fn validate_equivalence(reference: &VertexMap, system: &VertexMap) -> Result<(), SuiteError> {
    let mut ref_to_sys: BTreeMap<i64, i64> = BTreeMap::new();
    let mut sys_to_ref: BTreeMap<i64, i64> = BTreeMap::new();
    for (vertex, expected) in reference {
        let expected_label = as_int(expected)?;
        let actual_label = as_int(&system[vertex])?;
        if let Some(mapped) = ref_to_sys.insert(expected_label, actual_label) {
            if mapped != actual_label {
                return Err(SuiteError::ReferenceMismatch(format!(
                    "equivalence mismatch: reference label {expected_label} maps to both {mapped} and {actual_label}"
                )));
            }
        }
        if let Some(mapped) = sys_to_ref.insert(actual_label, expected_label) {
            if mapped != expected_label {
                return Err(SuiteError::ReferenceMismatch(format!(
                    "equivalence mismatch: system label {actual_label} maps to both {mapped} and {expected_label}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_epsilon(
    reference: &VertexMap,
    system: &VertexMap,
    epsilon: f64,
) -> Result<(), SuiteError> {
    for (vertex, expected) in reference {
        let expected_float = as_float(expected)?;
        let actual_float = as_float(&system[vertex])?;
        if expected_float.is_infinite() || actual_float.is_infinite() {
            if expected_float == actual_float {
                continue;
            }
            return Err(SuiteError::ReferenceMismatch(format!(
                "epsilon mismatch at vertex {vertex}: infinity inequality"
            )));
        }
        let delta = (expected_float - actual_float).abs();
        let bound = epsilon * expected_float.abs();
        if delta > bound {
            return Err(SuiteError::ReferenceMismatch(format!(
                "epsilon mismatch at vertex {vertex}: |{expected_float}-{actual_float}|={delta} > {bound}"
            )));
        }
    }
    Ok(())
}

pub fn run_job(
    job: &AlgorithmJob,
    reference: &VertexMap,
    system_output: Option<&VertexMap>,
) -> AlgorithmOutcome {
    let mode = job.algorithm.validation_mode().name().to_string();
    let mapping = match map_algorithm(job) {
        Ok(mapping) => mapping,
        Err(error) => {
            return AlgorithmOutcome {
                algorithm: job.algorithm,
                workload_key: job.algorithm.workload_key().into(),
                status: AlgorithmStatus::Failed,
                validation_mode: mode,
                cause: Some(error.to_string()),
                public_api: None,
            };
        }
    };
    match mapping {
        MappingOutcome::SemanticIncompatibility { cause, detail } => AlgorithmOutcome {
            algorithm: job.algorithm,
            workload_key: job.algorithm.workload_key().into(),
            status: AlgorithmStatus::SemanticIncompatibility,
            validation_mode: mode,
            cause: Some(format!("{cause}: {detail}")),
            public_api: None,
        },
        MappingOutcome::Compatible(public_api) => {
            let Some(system) = system_output else {
                return AlgorithmOutcome {
                    algorithm: job.algorithm,
                    workload_key: job.algorithm.workload_key().into(),
                    status: AlgorithmStatus::Failed,
                    validation_mode: mode,
                    cause: Some("missing_system_output".into()),
                    public_api: Some(public_api),
                };
            };
            match validate_reference(job.algorithm, reference, system) {
                Ok(()) => AlgorithmOutcome {
                    algorithm: job.algorithm,
                    workload_key: job.algorithm.workload_key().into(),
                    status: AlgorithmStatus::Passed,
                    validation_mode: mode,
                    cause: None,
                    public_api: Some(public_api),
                },
                Err(error) => AlgorithmOutcome {
                    algorithm: job.algorithm,
                    workload_key: job.algorithm.workload_key().into(),
                    status: AlgorithmStatus::Failed,
                    validation_mode: mode,
                    cause: Some(error.to_string()),
                    public_api: Some(public_api),
                },
            }
        }
    }
}

pub fn assemble_evidence(
    dataset_id: &str,
    identities: serde_json::Value,
    outcomes: Vec<AlgorithmOutcome>,
) -> SuiteEvidence {
    let status = if outcomes
        .iter()
        .any(|outcome| matches!(outcome.status, AlgorithmStatus::Failed))
    {
        AlgorithmStatus::Failed
    } else if outcomes
        .iter()
        .all(|outcome| matches!(outcome.status, AlgorithmStatus::SemanticIncompatibility))
    {
        AlgorithmStatus::SemanticIncompatibility
    } else if outcomes
        .iter()
        .any(|outcome| matches!(outcome.status, AlgorithmStatus::SemanticIncompatibility))
    {
        // Mixed pass + semantic incompatibility is still an admissible suite
        // outcome when every algorithm either validated or failed closed.
        AlgorithmStatus::Passed
    } else {
        AlgorithmStatus::Passed
    };
    SuiteEvidence {
        schema: EVIDENCE_SCHEMA.into(),
        suite_id: SUITE_ID.into(),
        dataset_id: dataset_id.into(),
        status,
        identities,
        algorithms: outcomes,
    }
}

pub fn determinism_rules() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        (
            "bfs",
            "exact integer hop depths; unreachable=9223372036854775807; deterministic topology order",
        ),
        (
            "pr",
            "epsilon=1e-4 on 64-bit IEEE-754 values; GraphForge mapping fails closed on fixed-iteration jobs",
        ),
        (
            "wcc",
            "equivalence match on component labels; weak connectivity ignores edge direction",
        ),
        (
            "cdlp",
            "exact match in Graphalytics; GraphForge mapping fails closed (async label_propagation ≠ sync CDLP)",
        ),
        (
            "lcc",
            "epsilon=1e-4; vertices with fewer than two neighbors score 0.0",
        ),
        (
            "sssp",
            "epsilon=1e-4; unreachable=+infinity; non-negative finite edge weights",
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job(algorithm: Algorithm) -> AlgorithmJob {
        AlgorithmJob {
            schema: JOB_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            dataset_id: "ga-tiny".into(),
            algorithm,
            directed: true,
            source_vertex: Some(1),
            damping: Some(0.85),
            max_iterations: Some(2),
            weight_property: Some("weight".into()),
        }
    }

    #[test]
    fn all_six_algorithms_declare_validation_modes() {
        for algorithm in Algorithm::ALL {
            assert!(!algorithm.validation_mode().name().is_empty());
            assert!(determinism_rules().contains_key(algorithm.workload_key()));
        }
    }

    #[test]
    fn bfs_wcc_lcc_sssp_map_to_public_api() {
        for algorithm in [
            Algorithm::Bfs,
            Algorithm::Wcc,
            Algorithm::Lcc,
            Algorithm::Sssp,
        ] {
            let outcome = map_algorithm(&sample_job(algorithm)).unwrap();
            assert!(
                matches!(outcome, MappingOutcome::Compatible(_)),
                "{algorithm}"
            );
        }
    }

    #[test]
    fn pr_and_cdlp_fail_closed_on_unsupported_semantics() {
        let pr = map_algorithm(&sample_job(Algorithm::Pr)).unwrap();
        assert!(matches!(
            pr,
            MappingOutcome::SemanticIncompatibility {
                cause: "fixed_iteration_pagerank_not_exposed",
                ..
            }
        ));
        let cdlp = map_algorithm(&sample_job(Algorithm::Cdlp)).unwrap();
        assert!(matches!(
            cdlp,
            MappingOutcome::SemanticIncompatibility {
                cause: "synchronous_cdlp_not_exposed",
                ..
            }
        ));
    }

    #[test]
    fn exact_and_epsilon_and_equivalence_validation() {
        let reference = parse_vertex_value_file("1 0\n2 1\n3 1\n").unwrap();
        let exact_ok = parse_vertex_value_file("1 0\n2 1\n3 1\n").unwrap();
        validate_reference(Algorithm::Bfs, &reference, &exact_ok).unwrap();

        let exact_bad = parse_vertex_value_file("1 0\n2 2\n3 1\n").unwrap();
        assert!(validate_reference(Algorithm::Bfs, &reference, &exact_bad).is_err());

        let ref_wcc = parse_vertex_value_file("1 7\n2 7\n3 9\n").unwrap();
        let sys_wcc = parse_vertex_value_file("1 0\n2 0\n3 1\n").unwrap();
        validate_reference(Algorithm::Wcc, &ref_wcc, &sys_wcc).unwrap();

        let ref_eps = parse_vertex_value_file("1 1.0\n2 0.0\n").unwrap();
        let sys_eps = parse_vertex_value_file("1 1.00005\n2 0.0\n").unwrap();
        validate_reference(Algorithm::Lcc, &ref_eps, &sys_eps).unwrap();
        let sys_eps_bad = parse_vertex_value_file("1 1.01\n2 0.0\n").unwrap();
        assert!(validate_reference(Algorithm::Lcc, &ref_eps, &sys_eps_bad).is_err());
    }

    #[test]
    fn ladder_requires_ga_tiny_first() {
        let ladder = DatasetLadder {
            schema: LADDER_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            datasets: vec![
                LadderEntry {
                    id: "wiki-Talk".into(),
                    order: 1,
                    role: "first_engineering_dataset".into(),
                },
                LadderEntry {
                    id: "ga-tiny".into(),
                    order: 2,
                    role: "engineering_fixture".into(),
                },
            ],
        };
        assert!(ladder.validate().is_err());
    }
}
