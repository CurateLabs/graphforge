//! GDC Graphalytics suite: algorithm mapping, reference validation, evidence.
//!
//! Workload semantics live here (not in shared `gdc_contracts`). Product
//! algorithm behavior remains in GraphForge Rust crates; this runner only maps
//! Graphalytics jobs onto public analyst-verb contracts and validates outputs.

#![forbid(unsafe_code)]

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    ClusterAlgorithm, ClusterOptions, GraphForge, NodeHandle, NodeSelector, PathAlgorithm,
    PathsOptions, PropValue, RankAlgorithm, RankOptions,
};
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    StaticReplay,
    LivePublicApi,
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
    pub certification: bool,
    pub execution_mode: ExecutionMode,
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
        Algorithm::Lcc if job.directed => Ok(MappingOutcome::SemanticIncompatibility {
            cause: "directed_lcc_semantics_not_exposed",
            detail: "Graphalytics v1.0.5 counts directed edges among unique direction-agnostic neighbors; GraphForge rank(by=clustering_coefficient) uses reciprocal-degree/Fagiolo normalization and fails the official directed validation vector".into(),
        }),
        Algorithm::Lcc => Ok(MappingOutcome::Compatible(PublicApiMapping {
            verb: "rank".into(),
            by: "clustering_coefficient".into(),
            directed: false,
            weight_property: None,
            notes: "undirected rank(by=clustering_coefficient) with Graphalytics epsilon match".into(),
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

pub struct LiveGraph {
    graph: GraphForge,
    handles: BTreeMap<u64, NodeHandle>,
    ids_by_uuid: BTreeMap<[u8; 16], u64>,
}

/// Load a committed edge-list fixture through the published in-memory facade.
pub fn load_live_graph(path: &Path) -> Result<LiveGraph, SuiteError> {
    let text = fs::read_to_string(path).map_err(|error| {
        SuiteError::InvalidDocument(format!("failed to read {}: {error}", path.display()))
    })?;
    let mut edges = Vec::new();
    let mut ids = BTreeSet::new();
    for (index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let source = parse_edge_vertex(parts.next(), index + 1, "source")?;
        let target = parse_edge_vertex(parts.next(), index + 1, "target")?;
        if parts.next().is_some() {
            return Err(SuiteError::InvalidDocument(format!(
                "edge line {}: unexpected trailing tokens",
                index + 1
            )));
        }
        ids.insert(source);
        ids.insert(target);
        edges.push((source, target));
    }
    if ids.is_empty() {
        return Err(SuiteError::InvalidDocument(
            "live edge fixture contains no vertices".into(),
        ));
    }

    let graph = GraphForge::new(None)
        .map_err(|error| SuiteError::InvalidDocument(format!("live GraphForge open: {error}")))?;
    let mut handles = BTreeMap::new();
    let mut ids_by_uuid = BTreeMap::new();
    for id in ids {
        let graphalytics_id = i64::try_from(id).map_err(|_| {
            SuiteError::InvalidDocument(format!("vertex id exceeds signed 64-bit range: {id}"))
        })?;
        let handle = graph
            .add_node(
                "Vertex",
                &BTreeMap::from([(
                    "graphalytics_id".to_owned(),
                    PropValue::Int(graphalytics_id),
                )])
                .into_iter()
                .collect(),
            )
            .map_err(|error| {
                SuiteError::InvalidDocument(format!("live node construction failed: {error}"))
            })?;
        ids_by_uuid.insert(*handle.uuid.as_bytes(), id);
        handles.insert(id, handle);
    }
    for (source, target) in edges {
        graph
            .add_edge(
                &handles[&source],
                "EDGE",
                &handles[&target],
                &BTreeMap::from([("weight".to_owned(), PropValue::Float(1.0))])
                    .into_iter()
                    .collect(),
            )
            .map_err(|error| {
                SuiteError::InvalidDocument(format!("live edge construction failed: {error}"))
            })?;
    }
    Ok(LiveGraph {
        graph,
        handles,
        ids_by_uuid,
    })
}

fn parse_edge_vertex(token: Option<&str>, line: usize, endpoint: &str) -> Result<u64, SuiteError> {
    token
        .ok_or_else(|| {
            SuiteError::InvalidDocument(format!("edge line {line}: missing {endpoint} vertex"))
        })?
        .parse()
        .map_err(|_| {
            SuiteError::InvalidDocument(format!("edge line {line}: invalid {endpoint} vertex"))
        })
}

/// Execute one compatible job through `graphforge-api`, then reuse the normal
/// Rust reference validator. Incompatible jobs never call a substitute kernel.
pub fn run_live_job(
    live: &LiveGraph,
    job: &AlgorithmJob,
    reference: &VertexMap,
) -> AlgorithmOutcome {
    match map_algorithm(job) {
        Ok(MappingOutcome::SemanticIncompatibility { .. }) => run_job(job, reference, None),
        Ok(MappingOutcome::Compatible(_)) => match execute_live_output(live, job) {
            Ok(output) => run_job(job, reference, Some(&output)),
            Err(error) => AlgorithmOutcome {
                algorithm: job.algorithm,
                workload_key: job.algorithm.workload_key().into(),
                status: AlgorithmStatus::Failed,
                validation_mode: job.algorithm.validation_mode().name().into(),
                cause: Some(format!("live_execution_failed: {error}")),
                public_api: match map_algorithm(job) {
                    Ok(MappingOutcome::Compatible(mapping)) => Some(mapping),
                    _ => None,
                },
            },
        },
        Err(error) => AlgorithmOutcome {
            algorithm: job.algorithm,
            workload_key: job.algorithm.workload_key().into(),
            status: AlgorithmStatus::Failed,
            validation_mode: job.algorithm.validation_mode().name().into(),
            cause: Some(error.to_string()),
            public_api: None,
        },
    }
}

fn execute_live_output(live: &LiveGraph, job: &AlgorithmJob) -> Result<VertexMap, SuiteError> {
    match job.algorithm {
        Algorithm::Bfs => {
            let source = required_source(live, job)?;
            let selector = NodeSelector::Handle(source.clone());
            let batch = live
                .graph
                .paths(
                    Some(&selector),
                    None,
                    PathsOptions {
                        by: PathAlgorithm::Bfs,
                        directed: job.directed,
                        ..Default::default()
                    },
                )
                .map_err(live_api_error)?;
            normalize_paths(live, &batch, true)
        }
        Algorithm::Wcc => {
            let batch = live
                .graph
                .cluster(
                    "Vertex",
                    ClusterOptions {
                        by: ClusterAlgorithm::Components,
                        directed: false,
                        ..Default::default()
                    },
                )
                .map_err(live_api_error)?;
            normalize_int_column(live, &batch, "community_id")
        }
        Algorithm::Lcc => {
            let batch = live
                .graph
                .rank(
                    "Vertex",
                    RankOptions {
                        by: RankAlgorithm::ClusteringCoefficient,
                        directed: job.directed,
                        ..Default::default()
                    },
                )
                .map_err(live_api_error)?;
            normalize_float_column(live, &batch, "score")
        }
        Algorithm::Sssp => {
            let source = required_source(live, job)?;
            let selector = NodeSelector::Handle(source.clone());
            let batch = live
                .graph
                .paths(
                    Some(&selector),
                    None,
                    PathsOptions {
                        by: PathAlgorithm::Dijkstra,
                        directed: job.directed,
                        weight: job.weight_property.clone(),
                        ..Default::default()
                    },
                )
                .map_err(live_api_error)?;
            normalize_paths(live, &batch, false)
        }
        Algorithm::Pr | Algorithm::Cdlp => Err(SuiteError::SemanticIncompatibility {
            cause: "unsupported_live_dispatch".into(),
            detail: "unsupported jobs must fail in semantic mapping before dispatch".into(),
        }),
    }
}

fn required_source<'a>(
    live: &'a LiveGraph,
    job: &AlgorithmJob,
) -> Result<&'a NodeHandle, SuiteError> {
    let id = job.source_vertex.ok_or_else(|| {
        SuiteError::InvalidDocument(format!("{} requires source_vertex", job.algorithm))
    })?;
    live.handles.get(&id).ok_or_else(|| {
        SuiteError::InvalidDocument(format!(
            "{} source vertex is absent from fixture: {id}",
            job.algorithm
        ))
    })
}

fn live_api_error(error: graphforge_api::GfError) -> SuiteError {
    SuiteError::InvalidDocument(format!("public GraphForge API: {error}"))
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, SuiteError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            SuiteError::InvalidDocument(format!(
                "live output missing fixed-size UUID column {name}"
            ))
        })
}

fn graphalytics_id(live: &LiveGraph, uuid: &[u8], column: &str) -> Result<u64, SuiteError> {
    let bytes: [u8; 16] = uuid.try_into().map_err(|_| {
        SuiteError::InvalidDocument(format!("live output {column} has malformed UUID"))
    })?;
    live.ids_by_uuid.get(&bytes).copied().ok_or_else(|| {
        SuiteError::InvalidDocument(format!("live output {column} UUID is not in the fixture"))
    })
}

fn normalize_int_column(
    live: &LiveGraph,
    batch: &RecordBatch,
    value_name: &str,
) -> Result<VertexMap, SuiteError> {
    let uuids = uuid_column(batch, "node_uuid")?;
    let values = batch
        .column_by_name(value_name)
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| {
            SuiteError::InvalidDocument(format!("live output missing int64 column {value_name}"))
        })?;
    let mut output = VertexMap::new();
    for row in 0..batch.num_rows() {
        if uuids.is_null(row) || values.is_null(row) {
            return Err(SuiteError::InvalidDocument(
                "live algorithm output contains null identity/value".into(),
            ));
        }
        let id = graphalytics_id(live, uuids.value(row), "node_uuid")?;
        if output
            .insert(id, VertexValue::Int(values.value(row)))
            .is_some()
        {
            return Err(SuiteError::InvalidDocument(format!(
                "live algorithm output duplicates vertex {id}"
            )));
        }
    }
    Ok(output)
}

fn normalize_float_column(
    live: &LiveGraph,
    batch: &RecordBatch,
    value_name: &str,
) -> Result<VertexMap, SuiteError> {
    let uuids = uuid_column(batch, "node_uuid")?;
    let values = batch
        .column_by_name(value_name)
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| {
            SuiteError::InvalidDocument(format!("live output missing float64 column {value_name}"))
        })?;
    let mut output = VertexMap::new();
    for row in 0..batch.num_rows() {
        if uuids.is_null(row) || values.is_null(row) || values.value(row).is_nan() {
            return Err(SuiteError::InvalidDocument(
                "live algorithm output contains null/NaN identity/value".into(),
            ));
        }
        let id = graphalytics_id(live, uuids.value(row), "node_uuid")?;
        if output
            .insert(id, VertexValue::Float(values.value(row)))
            .is_some()
        {
            return Err(SuiteError::InvalidDocument(format!(
                "live algorithm output duplicates vertex {id}"
            )));
        }
    }
    Ok(output)
}

fn normalize_paths(
    live: &LiveGraph,
    batch: &RecordBatch,
    integral: bool,
) -> Result<VertexMap, SuiteError> {
    let targets = uuid_column(batch, "target_uuid")?;
    let costs = batch
        .column_by_name("cost")
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| {
            SuiteError::InvalidDocument("live paths output missing float64 cost".into())
        })?;
    let unreachable = if integral {
        VertexValue::Int(BFS_UNREACHABLE)
    } else {
        VertexValue::Float(f64::INFINITY)
    };
    let mut output = live
        .handles
        .keys()
        .map(|id| (*id, unreachable.clone()))
        .collect::<VertexMap>();
    let mut seen = BTreeSet::new();
    for row in 0..batch.num_rows() {
        if targets.is_null(row) || costs.is_null(row) {
            return Err(SuiteError::InvalidDocument(
                "live paths output contains null target/cost".into(),
            ));
        }
        let id = graphalytics_id(live, targets.value(row), "target_uuid")?;
        if !seen.insert(id) {
            return Err(SuiteError::InvalidDocument(format!(
                "live paths output duplicates target {id}"
            )));
        }
        let cost = costs.value(row);
        let value = if integral {
            if !cost.is_finite() || cost.fract() != 0.0 || cost < 0.0 || cost >= -(i64::MIN as f64)
            {
                return Err(SuiteError::InvalidDocument(format!(
                    "live BFS emitted non-integral cost for vertex {id}: {cost}"
                )));
            }
            VertexValue::Int(cost as i64)
        } else {
            if cost.is_nan() {
                return Err(SuiteError::InvalidDocument(format!(
                    "live SSSP emitted NaN for vertex {id}"
                )));
            }
            VertexValue::Float(cost)
        };
        output.insert(id, value);
    }
    Ok(output)
}

fn as_int(value: &VertexValue) -> Result<i64, SuiteError> {
    match value {
        VertexValue::Int(value) => Ok(*value),
        VertexValue::Float(value)
            if value.fract() == 0.0 && *value >= i64::MIN as f64 && *value < -(i64::MIN as f64) =>
        {
            Ok(*value as i64)
        }
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
    execution_mode: ExecutionMode,
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
        certification: false,
        execution_mode,
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
    fn compatible_algorithms_map_to_public_api() {
        for algorithm in [Algorithm::Bfs, Algorithm::Wcc, Algorithm::Sssp] {
            let outcome = map_algorithm(&sample_job(algorithm)).unwrap();
            assert!(
                matches!(outcome, MappingOutcome::Compatible(_)),
                "{algorithm}"
            );
        }
        let mut undirected_lcc = sample_job(Algorithm::Lcc);
        undirected_lcc.directed = false;
        assert!(matches!(
            map_algorithm(&undirected_lcc).unwrap(),
            MappingOutcome::Compatible(_)
        ));
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
    fn directed_lcc_fails_closed_on_official_semantic_difference() {
        assert!(matches!(
            map_algorithm(&sample_job(Algorithm::Lcc)).unwrap(),
            MappingOutcome::SemanticIncompatibility {
                cause: "directed_lcc_semantics_not_exposed",
                ..
            }
        ));
    }

    #[test]
    fn bfs_rejects_cost_at_exclusive_i64_upper_bound() {
        use std::sync::Arc;
        let uuid = [1_u8; 16];
        let live = LiveGraph {
            graph: GraphForge::new(None).unwrap(),
            handles: BTreeMap::new(),
            ids_by_uuid: BTreeMap::from([(uuid, 1)]),
        };
        let targets = FixedSizeBinaryArray::try_from_iter([uuid].into_iter()).unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            ("target_uuid", Arc::new(targets) as arrow::array::ArrayRef),
            (
                "cost",
                Arc::new(Float64Array::from(vec![-(i64::MIN as f64)])),
            ),
        ])
        .unwrap();
        assert!(normalize_paths(&live, &batch, true).is_err());
    }

    #[test]
    fn integer_validation_rejects_saturating_float_payloads() {
        for value in [1e20, -1e20, -(i64::MIN as f64), f64::INFINITY, f64::NAN] {
            assert!(as_int(&VertexValue::Float(value)).is_err());
        }
        assert_eq!(
            as_int(&VertexValue::Float(i64::MIN as f64)).unwrap(),
            i64::MIN
        );
        let reference = BTreeMap::from([(1, VertexValue::Int(i64::MAX))]);
        let actual = BTreeMap::from([(1, VertexValue::Float(1e20))]);
        assert!(validate_reference(Algorithm::Bfs, &reference, &actual).is_err());
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
    fn ga_tiny_pr_reference_is_official_two_iteration_vector() {
        let computed = official_ga_tiny_pagerank(0.85, 2);
        assert_ne!(
            computed,
            [0.25, 0.25, 0.25, 0.25],
            "initialization PR_0=1/|V| is not the official 2-iteration vector"
        );
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/gdc/graphalytics-tiny/compatible/references/ga-tiny-pr.ref");
        let text = fs::read_to_string(&fixture).unwrap();
        assert_eq!(
            text,
            "1 1.019140625000000e-01\n2 1.404296875000000e-01\n3 3.077734375000000e-01\n4 4.498828124999999e-01\n"
        );
        assert!(!text.contains("0.25"), "must not store uniform PR_0");
        let parsed = parse_vertex_value_file(&text).unwrap();
        assert_eq!(parsed.len(), 4);
        for (index, vertex) in [1_u64, 2, 3, 4].into_iter().enumerate() {
            match parsed.get(&vertex) {
                Some(VertexValue::Float(value)) => {
                    let ulps = value.to_bits().abs_diff(computed[index].to_bits());
                    assert!(
                        ulps <= 1,
                        "vertex {vertex}: parsed={value} computed={} ulps={ulps}",
                        computed[index]
                    );
                    assert_ne!(*value, 0.25, "vertex {vertex} must not carry PR_0");
                }
                other => panic!("vertex {vertex} expected float, got {other:?}"),
            }
        }
    }

    #[test]
    fn ga_tiny_cdlp_reference_is_truthful_two_iteration_labels() {
        let computed = official_ga_tiny_cdlp(2);
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/gdc/graphalytics-tiny/compatible/references/ga-tiny-cdlp.ref");
        let parsed = load_vertex_value_file(&fixture).unwrap();
        for (vertex, label) in computed {
            match parsed.get(&vertex) {
                Some(VertexValue::Int(value)) => assert_eq!(*value, label, "vertex {vertex}"),
                other => panic!("vertex {vertex} expected int, got {other:?}"),
            }
        }
    }

    fn official_ga_tiny_pagerank(damping: f64, max_iterations: u32) -> [f64; 4] {
        // Graphalytics v1.0.5 definition.tex: teleport + importance + sink redistribution.
        let vertices = [1_u64, 2, 3, 4];
        let edges = [(1_u64, 2_u64), (1, 3), (2, 3), (3, 4)];
        let n = vertices.len() as f64;
        let mut outdeg = [0_u32; 4];
        let mut incoming: [Vec<usize>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for (source, target) in edges {
            outdeg[(source - 1) as usize] += 1;
            incoming[(target - 1) as usize].push((source - 1) as usize);
        }
        let mut ranks = [1.0 / n; 4];
        for _ in 0..max_iterations {
            let teleport = (1.0 - damping) / n;
            let sink_mass: f64 = ranks
                .iter()
                .enumerate()
                .filter(|(index, _)| outdeg[*index] == 0)
                .map(|(_, rank)| *rank)
                .sum();
            let redistributed = (damping / n) * sink_mass;
            let mut nxt = [0.0; 4];
            for vertex in 0..4 {
                let mut importance = 0.0;
                for source in &incoming[vertex] {
                    if outdeg[*source] == 0 {
                        continue;
                    }
                    importance += ranks[*source] / f64::from(outdeg[*source]);
                }
                nxt[vertex] = teleport + damping * importance + redistributed;
            }
            ranks = nxt;
        }
        ranks
    }

    fn official_ga_tiny_cdlp(max_iterations: u32) -> [(u64, i64); 4] {
        let edges = [(1_u64, 2_u64), (1, 3), (2, 3), (3, 4)];
        let mut incoming: [Vec<usize>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        let mut outgoing: [Vec<usize>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for (source, target) in edges {
            incoming[(target - 1) as usize].push((source - 1) as usize);
            outgoing[(source - 1) as usize].push((target - 1) as usize);
        }
        let mut labels = [1_i64, 2, 3, 4];
        for _ in 0..max_iterations {
            let mut nxt = labels;
            for vertex in 0..4 {
                let mut counts = BTreeMap::<i64, u32>::new();
                for neighbor in incoming[vertex].iter().chain(outgoing[vertex].iter()) {
                    *counts.entry(labels[*neighbor]).or_insert(0) += 1;
                }
                nxt[vertex] = counts
                    .into_iter()
                    .max_by_key(|(label, freq)| (*freq, -label))
                    .map(|(label, _)| label)
                    .unwrap_or(labels[vertex]);
            }
            labels = nxt;
        }
        [
            (1, labels[0]),
            (2, labels[1]),
            (3, labels[2]),
            (4, labels[3]),
        ]
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
