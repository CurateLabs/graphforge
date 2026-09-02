//! GDC SNB BI suite: operation mapping, reference validation, resources, evidence.
//!
//! Workload semantics live here (not in shared `gdc_contracts`). Product graph
//! behavior remains in GraphForge Rust crates; this runner only maps LDBC SNB
//! Business Intelligence operations onto the public Cypher / analyst-verb
//! surface, validates read outputs against pinned references, records per-phase
//! resource evidence *separately* from correctness, and fails closed on
//! semantics the public property-graph + Cypher surface does not expose.
//!
//! Results here are engineering evidence only. They never masquerade as an
//! audited GDC certification (`SuiteEvidence::certification` is always `false`).

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::time::Instant;

pub const EVIDENCE_SCHEMA: &str = "graphforge-gdc-snb-bi-evidence/1";
pub const JOB_SCHEMA: &str = "graphforge-gdc-snb-bi-job/1";
pub const LADDER_SCHEMA: &str = "graphforge-gdc-snb-bi-ladder/1";
pub const LIVE_EVIDENCE_SCHEMA: &str = "graphforge-gdc-snb-bi-live-evidence/2";
pub const LIVE_IDENTITY_SCHEMA: &str = "graphforge-gdc-snb-bi-live-identity/2";
pub const LIVE_PARAMETERS_SCHEMA: &str = "graphforge-gdc-snb-bi-live-parameters/2";
pub const RESOURCE_SCHEMA: &str = "graphforge-gdc-snb-bi-resources/1";
pub const SUITE_ID: &str = "snb-bi";

/// Historical ID of the synthetic static validator fixture (not official SF0.003).
pub const BOUNDED_TINY_DATASET: &str = "snb-bi-sf0.003";

/// SNB BI workload category.
///
/// The 20 BI queries are analytical reads; the maintenance stream applies batch
/// inserts and batch deletes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
    AnalyticalRead,
    BatchInsert,
    BatchDelete,
}

impl Category {
    pub fn name(self) -> &'static str {
        match self {
            Self::AnalyticalRead => "analytical_read",
            Self::BatchInsert => "batch_insert",
            Self::BatchDelete => "batch_delete",
        }
    }
}

/// Reference-validation mode for a compatible read operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationMode {
    /// Ordered, row-for-row comparison (the operation has a total result order).
    Exact,
    /// Order-insensitive multiset comparison with per-row whitespace
    /// normalization (the operation's result is a grouped set without a
    /// spec-mandated total tie-break).
    Normalized,
}

impl ValidationMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Normalized => "normalized",
        }
    }
}

/// LDBC SNB BI operations modeled by this suite.
///
/// Analytical reads (`BI1`..`BI20`) plus the batch maintenance stream: inserts
/// (`INS1`..`INS8`) and deletes (`DEL1`..`DEL8`).
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Operation {
    #[serde(rename = "BI1")]
    Bi1,
    #[serde(rename = "BI2")]
    Bi2,
    #[serde(rename = "BI3")]
    Bi3,
    #[serde(rename = "BI4")]
    Bi4,
    #[serde(rename = "BI5")]
    Bi5,
    #[serde(rename = "BI6")]
    Bi6,
    #[serde(rename = "BI7")]
    Bi7,
    #[serde(rename = "BI8")]
    Bi8,
    #[serde(rename = "BI9")]
    Bi9,
    #[serde(rename = "BI10")]
    Bi10,
    #[serde(rename = "BI11")]
    Bi11,
    #[serde(rename = "BI12")]
    Bi12,
    #[serde(rename = "BI13")]
    Bi13,
    #[serde(rename = "BI14")]
    Bi14,
    #[serde(rename = "BI15")]
    Bi15,
    #[serde(rename = "BI16")]
    Bi16,
    #[serde(rename = "BI17")]
    Bi17,
    #[serde(rename = "BI18")]
    Bi18,
    #[serde(rename = "BI19")]
    Bi19,
    #[serde(rename = "BI20")]
    Bi20,
    #[serde(rename = "INS1")]
    Ins1,
    #[serde(rename = "INS2")]
    Ins2,
    #[serde(rename = "INS3")]
    Ins3,
    #[serde(rename = "INS4")]
    Ins4,
    #[serde(rename = "INS5")]
    Ins5,
    #[serde(rename = "INS6")]
    Ins6,
    #[serde(rename = "INS7")]
    Ins7,
    #[serde(rename = "INS8")]
    Ins8,
    #[serde(rename = "DEL1")]
    Del1,
    #[serde(rename = "DEL2")]
    Del2,
    #[serde(rename = "DEL3")]
    Del3,
    #[serde(rename = "DEL4")]
    Del4,
    #[serde(rename = "DEL5")]
    Del5,
    #[serde(rename = "DEL6")]
    Del6,
    #[serde(rename = "DEL7")]
    Del7,
    #[serde(rename = "DEL8")]
    Del8,
}

impl Operation {
    pub const ALL: [Self; 36] = [
        Self::Bi1,
        Self::Bi2,
        Self::Bi3,
        Self::Bi4,
        Self::Bi5,
        Self::Bi6,
        Self::Bi7,
        Self::Bi8,
        Self::Bi9,
        Self::Bi10,
        Self::Bi11,
        Self::Bi12,
        Self::Bi13,
        Self::Bi14,
        Self::Bi15,
        Self::Bi16,
        Self::Bi17,
        Self::Bi18,
        Self::Bi19,
        Self::Bi20,
        Self::Ins1,
        Self::Ins2,
        Self::Ins3,
        Self::Ins4,
        Self::Ins5,
        Self::Ins6,
        Self::Ins7,
        Self::Ins8,
        Self::Del1,
        Self::Del2,
        Self::Del3,
        Self::Del4,
        Self::Del5,
        Self::Del6,
        Self::Del7,
        Self::Del8,
    ];

    pub fn code(self) -> &'static str {
        match self {
            Self::Bi1 => "BI1",
            Self::Bi2 => "BI2",
            Self::Bi3 => "BI3",
            Self::Bi4 => "BI4",
            Self::Bi5 => "BI5",
            Self::Bi6 => "BI6",
            Self::Bi7 => "BI7",
            Self::Bi8 => "BI8",
            Self::Bi9 => "BI9",
            Self::Bi10 => "BI10",
            Self::Bi11 => "BI11",
            Self::Bi12 => "BI12",
            Self::Bi13 => "BI13",
            Self::Bi14 => "BI14",
            Self::Bi15 => "BI15",
            Self::Bi16 => "BI16",
            Self::Bi17 => "BI17",
            Self::Bi18 => "BI18",
            Self::Bi19 => "BI19",
            Self::Bi20 => "BI20",
            Self::Ins1 => "INS1",
            Self::Ins2 => "INS2",
            Self::Ins3 => "INS3",
            Self::Ins4 => "INS4",
            Self::Ins5 => "INS5",
            Self::Ins6 => "INS6",
            Self::Ins7 => "INS7",
            Self::Ins8 => "INS8",
            Self::Del1 => "DEL1",
            Self::Del2 => "DEL2",
            Self::Del3 => "DEL3",
            Self::Del4 => "DEL4",
            Self::Del5 => "DEL5",
            Self::Del6 => "DEL6",
            Self::Del7 => "DEL7",
            Self::Del8 => "DEL8",
        }
    }

    pub fn category(self) -> Category {
        match self {
            Self::Bi1
            | Self::Bi2
            | Self::Bi3
            | Self::Bi4
            | Self::Bi5
            | Self::Bi6
            | Self::Bi7
            | Self::Bi8
            | Self::Bi9
            | Self::Bi10
            | Self::Bi11
            | Self::Bi12
            | Self::Bi13
            | Self::Bi14
            | Self::Bi15
            | Self::Bi16
            | Self::Bi17
            | Self::Bi18
            | Self::Bi19
            | Self::Bi20 => Category::AnalyticalRead,
            Self::Ins1
            | Self::Ins2
            | Self::Ins3
            | Self::Ins4
            | Self::Ins5
            | Self::Ins6
            | Self::Ins7
            | Self::Ins8 => Category::BatchInsert,
            Self::Del1
            | Self::Del2
            | Self::Del3
            | Self::Del4
            | Self::Del5
            | Self::Del6
            | Self::Del7
            | Self::Del8 => Category::BatchDelete,
        }
    }

    /// The reference-validation mode for a compatible read, or `None` for an
    /// operation this suite fails closed on (no reference comparison happens).
    pub fn validation_mode(self) -> Option<ValidationMode> {
        match map_operation(self) {
            MappingOutcome::Compatible(_) => Some(self.intended_validation()),
            MappingOutcome::SemanticIncompatibility { .. } => None,
        }
    }

    /// The validation mode a read *would* use if compatible. Reads with a
    /// spec-mandated total order use `Exact`; grouped aggregations whose ties
    /// are not totally ordered use `Normalized`.
    fn intended_validation(self) -> ValidationMode {
        match self {
            // Grouped/set-shaped aggregations without a spec-mandated total
            // tie-break (message-count histogram, related-tag co-occurrence):
            // compared order-insensitively. BI2 has the explicit total order
            // `diff DESC, tag.name ASC` and therefore remains exact.
            Self::Bi12 | Self::Bi16 => ValidationMode::Normalized,
            _ => ValidationMode::Exact,
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.code())
    }
}

impl FromStr for Operation {
    type Err = SuiteError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|operation| operation.code() == value)
            .ok_or_else(|| {
                SuiteError::InvalidDocument(format!("unknown SNB BI operation: {value}"))
            })
    }
}

/// Declared public GraphForge contract for a compatible operation mapping.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicApiMapping {
    /// Public surface used: `cypher` or `analyst_verb`.
    pub interface: String,
    /// Concrete Cypher / analyst-verb shape that realizes the operation.
    pub cypher_shape: String,
    /// Honest engineering note about the mapping decision.
    pub notes: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MappingOutcome {
    Compatible(PublicApiMapping),
    SemanticIncompatibility { cause: &'static str, detail: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationJob {
    pub schema: String,
    pub suite_id: String,
    pub dataset_id: String,
    pub operation: Operation,
}

impl OperationJob {
    pub fn validate_schema(&self) -> Result<(), SuiteError> {
        if self.schema != JOB_SCHEMA {
            return Err(SuiteError::InvalidDocument(format!(
                "unexpected job schema: {}",
                self.schema
            )));
        }
        if self.suite_id != SUITE_ID {
            return Err(SuiteError::InvalidDocument(
                "job suite_id must be snb-bi".into(),
            ));
        }
        Ok(())
    }
}

/// Ordered load ladder; every ladder must begin on the bounded tiny dataset.
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
    /// Anything beyond the bounded tiny fixture is opt-in / external only.
    #[serde(default)]
    pub opt_in: bool,
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
                "ladder suite_id must be snb-bi".into(),
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
        let first = sorted.first().expect("non-empty checked above");
        if first.id != BOUNDED_TINY_DATASET {
            return Err(SuiteError::InvalidDocument(format!(
                "ordered ladder must begin with bounded fixture {BOUNDED_TINY_DATASET}"
            )));
        }
        if first.opt_in {
            return Err(SuiteError::InvalidDocument(
                "bounded tiny fixture must not be opt-in".into(),
            ));
        }
        // Everything beyond the bounded tiny fixture is opt-in / external only.
        for entry in sorted.iter().skip(1) {
            if !entry.opt_in {
                return Err(SuiteError::InvalidDocument(format!(
                    "scale beyond the tiny fixture must be opt-in: {}",
                    entry.id
                )));
            }
        }
        Ok(())
    }

    pub fn ordered_ids(&self) -> Vec<String> {
        let mut sorted = self.datasets.clone();
        sorted.sort_by_key(|entry| entry.order);
        sorted.into_iter().map(|entry| entry.id).collect()
    }
}

/// Per-phase resource evidence, recorded distinctly from correctness.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceReport {
    pub schema: String,
    pub dataset_id: String,
    pub load: LoadResource,
    pub query: QueryResource,
    pub spill: SpillResource,
    pub rss: RssResource,
    pub io: IoResource,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LoadResource {
    pub wall_ms: u64,
    pub rows_loaded: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryResource {
    pub wall_ms: u64,
    pub queries_executed: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SpillResource {
    pub bytes: u64,
    pub events: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RssResource {
    pub peak_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IoResource {
    pub read_bytes: u64,
    pub write_bytes: u64,
}

impl ResourceReport {
    pub fn validate(&self, dataset_id: &str) -> Result<(), SuiteError> {
        if self.schema != RESOURCE_SCHEMA {
            return Err(SuiteError::InvalidDocument(format!(
                "unexpected resource schema: {}",
                self.schema
            )));
        }
        if self.dataset_id != dataset_id {
            return Err(SuiteError::InvalidDocument(format!(
                "resource dataset_id {} does not match suite dataset {dataset_id}",
                self.dataset_id
            )));
        }
        Ok(())
    }
}

pub fn load_resource_report(path: &Path) -> Result<ResourceReport, SuiteError> {
    let text = fs::read_to_string(path).map_err(|error| {
        SuiteError::InvalidDocument(format!("failed to read {}: {error}", path.display()))
    })?;
    serde_json::from_str(&text)
        .map_err(|error| SuiteError::InvalidDocument(format!("invalid resource report: {error}")))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Passed,
    Failed,
    SemanticIncompatibility,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct OperationOutcome {
    pub operation: Operation,
    pub category: String,
    pub status: OperationStatus,
    pub validation_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_api: Option<PublicApiMapping>,
}

/// SNB BI lifecycle phases, kept explicitly separate.
///
/// The BI workload bulk-loads the graph, applies batch maintenance updates,
/// runs the analytical query mix, then validates results against references.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Load,
    Updates,
    Query,
    Validation,
}

impl Phase {
    pub const ALL: [Self; 4] = [Self::Load, Self::Updates, Self::Query, Self::Validation];

    pub fn name(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Updates => "updates",
            Self::Query => "query",
            Self::Validation => "validation",
        }
    }
}

pub fn phase_names() -> Vec<String> {
    Phase::ALL
        .iter()
        .map(|phase| phase.name().to_string())
        .collect()
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SuiteEvidence {
    pub schema: String,
    pub suite_id: String,
    pub dataset_id: String,
    pub status: OperationStatus,
    /// Engineering evidence flag: never an audited GDC certification.
    pub certification: bool,
    pub phases: Vec<String>,
    pub identities: serde_json::Value,
    /// Per-phase resource evidence, kept distinct from the correctness outcomes.
    pub resources: ResourceReport,
    /// Per-operation correctness outcomes.
    pub operations: Vec<OperationOutcome>,
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

/// Map an SNB BI operation onto the public GraphForge surface.
///
/// Analytical reads that are ordinary graph traversals, aggregations, grouped
/// counts, or top-k rankings map to Cypher. Reads that require a weighted
/// shortest-path computation (BI15/BI19/BI20) and the entire batch maintenance
/// stream (inserts/deletes) fail closed with a typed cause instead of silently
/// approximating semantics the public surface does not expose.
pub fn map_operation(operation: Operation) -> MappingOutcome {
    match operation {
        Operation::Bi1 => compatible(
            "cypher",
            "MATCH (m:Message) WHERE m.creationDate<$date WITH m, m.creationDate.year AS year, \
             (m:Comment) AS isComment, CASE ... END AS lengthCategory \
             RETURN year, isComment, lengthCategory, count(*), sum(m.length) ORDER BY year DESC, isComment, lengthCategory",
            "posting summary: grouped counts and length aggregation over messages before a date",
        ),
        Operation::Bi2 => compatible(
            "cypher",
            "MATCH (t:Tag)<-[:HAS_TAG]-(m:Message) WHERE m.creationDate window \
             RETURN t.name, countWindow1, countWindow2, abs(diff) ORDER BY diff DESC, t.name ASC",
            "tag evolution: per-tag message counts across two windows; official total order diff DESC, tag.name ASC",
        ),
        Operation::Bi3 => compatible(
            "cypher",
            "MATCH (co:Country {name:$country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(p:Person)<-[:HAS_MODERATOR]-(f:Forum)-[:CONTAINER_OF]->(post)<-[:REPLY_OF*0..]-(msg)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(tc:TagClass {name:$class}) \
             RETURN f.id, f.title, f.creationDate, p.id, count(DISTINCT msg) ORDER BY count DESC, f.id LIMIT 20",
            "popular topics in a country: traversal + distinct count top-k",
        ),
        Operation::Bi4 => compatible(
            "cypher",
            "MATCH (co:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(p:Person)<-[:HAS_MEMBER]-(f:Forum) \
             RETURN co.name, top forum by member count, p ORDER BY ... LIMIT 100",
            "top forum member per country: aggregation with deterministic tie-break",
        ),
        Operation::Bi5 => compatible(
            "cypher",
            "MATCH (t:Tag {name:$tag})<-[:HAS_TAG]-(msg:Message)-[:HAS_CREATOR]->(p:Person) \
             OPTIONAL MATCH (msg)<-[l:LIKES]-() OPTIONAL MATCH (msg)<-[:REPLY_OF]-(c) \
             RETURN p.id, count(DISTINCT l), count(DISTINCT c), count(DISTINCT msg), score ORDER BY score DESC, p.id LIMIT 100",
            "top posters of a tag: per-person like/reply/message aggregation top-k",
        ),
        Operation::Bi6 => compatible(
            "cypher",
            "MATCH (t:Tag {name:$tag})<-[:HAS_TAG]-(msg)-[:HAS_CREATOR]->(p:Person)<-[:HAS_CREATOR]-(m2)<-[:LIKES]-(liker) \
             RETURN p.id, sum(likes) AS authority ORDER BY authority DESC, p.id LIMIT 100",
            "authoritative users on a topic: summed like counts as authority score",
        ),
        Operation::Bi7 => compatible(
            "cypher",
            "MATCH (t:Tag {name:$tag})<-[:HAS_TAG]-(msg)<-[:REPLY_OF]-(comment)-[:HAS_TAG]->(related:Tag) \
             WHERE NOT (comment)-[:HAS_TAG]->(t) RETURN related.name, count(DISTINCT comment) ORDER BY count DESC, related.name LIMIT 100",
            "related topics: co-occurring tags on replies to a tag's messages",
        ),
        Operation::Bi8 => compatible(
            "cypher",
            "MATCH (t:Tag {name:$tag}) OPTIONAL MATCH (p:Person)-[:HAS_INTEREST]->(t) \
             OPTIONAL MATCH (p)<-[:HAS_CREATOR]-(msg)-[:HAS_TAG]->(t) WITH p, interestScore \
             OPTIONAL MATCH (p)-[:KNOWS]-(f) RETURN p.id, score + friendScore ORDER BY total DESC, p.id LIMIT 100",
            "central person for a tag: interest + friends' interest scoring top-k",
        ),
        Operation::Bi9 => compatible(
            "cypher",
            "MATCH (p:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF*0..]-(reply) \
             WHERE post.creationDate window RETURN p.id, count(DISTINCT post), count(DISTINCT reply), sum(reply.length) ORDER BY threadCount DESC, p.id LIMIT 100",
            "top thread initiators: variable-length reply-tree traversal with aggregation",
        ),
        Operation::Bi10 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS*1..2]-(f:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name:$country}) \
             OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(msg)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name:$class}) \
             RETURN f.id, count(msg) AS score ORDER BY score DESC, f.id LIMIT 100",
            "experts in a social circle: bounded KNOWS traversal + tag-class filter",
        ),
        Operation::Bi11 => compatible(
            "cypher",
            "MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name:$country}) \
             MATCH (a)-[k1:KNOWS]-(b)-[k2:KNOWS]-(c)-[k3:KNOWS]-(a) WHERE dates within window AND a.id<b.id<c.id \
             RETURN count(*)",
            "friend triangles in a country: undirected 3-cycle pattern count",
        ),
        Operation::Bi12 => compatible(
            "cypher",
            "MATCH (p:Person) OPTIONAL MATCH (p)<-[:HAS_CREATOR]-(msg:Message) \
             WHERE msg.content IS NOT NULL AND msg.length>$len WITH p, count(msg) AS messageCount \
             RETURN messageCount, count(p) ORDER BY count(p) DESC, messageCount DESC",
            "message-count histogram: persons grouped by message count; grouped set (normalized validation)",
        ),
        Operation::Bi13 => compatible(
            "cypher",
            "MATCH (co:Country {name:$country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person) \
             WHERE zombie.creationDate<$date WITH zombie WHERE messageCount below threshold \
             WITH collect(zombie) AS zombies MATCH ... RETURN zombie.id, zombieScore ORDER BY zombieScore DESC, zombie.id LIMIT 100",
            "zombies in a country: two-pass zombie-set membership then like-ratio scoring, expressible with WITH",
        ),
        Operation::Bi14 => compatible(
            "cypher",
            "MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(c1:Country {name:$countryX}) \
             MATCH (b:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(c2:Country {name:$countryY}) \
             MATCH (a)-[:KNOWS]-(b) RETURN a, b, symmetricInteractionScore ORDER BY score DESC, a.id, b.id LIMIT 100",
            "international dialog: symmetric interaction scoring over cross-country friend pairs",
        ),
        Operation::Bi15 => MappingOutcome::SemanticIncompatibility {
            cause: "weighted_shortest_path_not_exposed",
            detail: "BI15 computes the minimum-weight trusted connection path between two persons where each \
                     KNOWS edge weight is a dynamically computed interaction cost; the public surface exposes \
                     unweighted single-path analyst verbs and pattern matching but not weighted shortest-path \
                     search over a computed edge-weight function"
                .into(),
        },
        Operation::Bi16 => compatible(
            "cypher",
            "MATCH (p:Person)<-[:HAS_CREATOR]-(msg)-[:HAS_TAG]->(:Tag {name:$tagA}) WHERE msg.creationDate=$dateA \
             MATCH (p)<-[:HAS_CREATOR]-(msg2)-[:HAS_TAG]->(:Tag {name:$tagB}) WHERE msg2.creationDate=$dateB \
             RETURN p.id, countA, countB ORDER BY countA+countB DESC, p.id",
            "fake-news detection: per-person tagged-message counts on two days; grouped set (normalized validation)",
        ),
        Operation::Bi17 => compatible(
            "cypher",
            "MATCH (t:Tag {name:$tag}) MATCH (p1)<-[:HAS_CREATOR]-(m1)-[:HAS_TAG]->(t) \
             MATCH (m1)<-[:REPLY_OF]-(m2)-[:HAS_CREATOR]->(p2) ... with temporal and knows constraints \
             RETURN p1.id, count(DISTINCT structuralMatch) ORDER BY count DESC, p1.id LIMIT 10",
            "information propagation: multi-hop message/reply pattern with temporal constraints",
        ),
        Operation::Bi18 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS]-(f)-[:KNOWS]-(fof) WHERE NOT (p)-[:KNOWS]-(fof) AND p<>fof \
             OPTIONAL MATCH (fof)-[:HAS_INTEREST]->(t:Tag)<-[:HAS_INTEREST]-(p) \
             RETURN fof.id, count(DISTINCT f) AS mutual, count(DISTINCT t) ORDER BY mutual DESC, fof.id LIMIT 20",
            "friend recommendation: friends-of-friends by mutual-friend count top-k",
        ),
        Operation::Bi19 => MappingOutcome::SemanticIncompatibility {
            cause: "weighted_shortest_path_not_exposed",
            detail: "BI19 finds the minimum-cost interaction path between persons located in two cities, where \
                     each KNOWS edge cost is derived from the reciprocal of the reply/comment interaction count; \
                     weighted shortest-path search over a computed edge-weight function is not on the public surface"
                .into(),
        },
        Operation::Bi20 => MappingOutcome::SemanticIncompatibility {
            cause: "weighted_shortest_path_not_exposed",
            detail: "BI20 (recruitment) computes the minimum-weight path from a person to a company's employees \
                     over the KNOWS graph with per-person derived edge weights; the public surface exposes \
                     unweighted path verbs only, not weighted shortest-path search over a computed weight function"
                .into(),
        },
        Operation::Ins1 => batch_update_incompatible("INS1 inserts a Person with dependency-time-ordered edges"),
        Operation::Ins2 => batch_update_incompatible("INS2 inserts a Person-likes-Post interaction"),
        Operation::Ins3 => batch_update_incompatible("INS3 inserts a Person-likes-Comment interaction"),
        Operation::Ins4 => batch_update_incompatible("INS4 inserts a Forum"),
        Operation::Ins5 => batch_update_incompatible("INS5 inserts a Forum membership"),
        Operation::Ins6 => batch_update_incompatible("INS6 inserts a Post"),
        Operation::Ins7 => batch_update_incompatible("INS7 inserts a Comment reply"),
        Operation::Ins8 => batch_update_incompatible("INS8 inserts a KNOWS friendship"),
        Operation::Del1 => batch_update_incompatible("DEL1 deletes a Person and its dependent graph"),
        Operation::Del2 => batch_update_incompatible("DEL2 deletes a Post-likes edge"),
        Operation::Del3 => batch_update_incompatible("DEL3 deletes a Comment-likes edge"),
        Operation::Del4 => batch_update_incompatible("DEL4 deletes a Forum and its dependents"),
        Operation::Del5 => batch_update_incompatible("DEL5 deletes a Forum membership"),
        Operation::Del6 => batch_update_incompatible("DEL6 deletes a Post and its reply subtree"),
        Operation::Del7 => batch_update_incompatible("DEL7 deletes a Comment and its reply subtree"),
        Operation::Del8 => batch_update_incompatible("DEL8 deletes a KNOWS friendship"),
    }
}

fn compatible(interface: &str, cypher_shape: &str, notes: &str) -> MappingOutcome {
    MappingOutcome::Compatible(PublicApiMapping {
        interface: interface.into(),
        cypher_shape: cypher_shape.into(),
        notes: notes.into(),
    })
}

fn batch_update_incompatible(detail: &str) -> MappingOutcome {
    MappingOutcome::SemanticIncompatibility {
        cause: "bi_batch_update_stream_not_exposed",
        detail: format!(
            "{detail}; the SNB BI batch maintenance stream requires the official driver's transactional \
             insert/delete semantics, dependency-time ordering, cascading deletes, and write validation, none \
             of which the public property-graph + Cypher surface exposes"
        ),
    }
}

/// A read result as an ordered list of normalized rows.
pub type ResultRows = Vec<String>;

pub fn parse_result_rows(text: &str) -> ResultRows {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(normalize_row)
        .collect()
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LiveIdentity {
    pub schema: String,
    pub suite_id: String,
    pub operation: Operation,
    pub source_mode: String,
    pub certification: bool,
    pub upstream_spec: UpstreamIdentity,
    pub upstream_query: UpstreamQueryIdentity,
    pub fixture: LiveFixtureIdentity,
    pub parameters: LiveParametersIdentity,
    pub reference: LiveReferenceIdentity,
    pub driver: LiveDriverIdentity,
    pub normalization: LiveNormalizationIdentity,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpstreamIdentity {
    pub source: String,
    pub release: String,
    pub commit: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpstreamQueryIdentity {
    pub source: String,
    pub release: String,
    pub commit: String,
    pub path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LiveFixtureIdentity {
    pub dataset_id: String,
    pub kind: String,
    pub provenance_kind: String,
    pub seed: u64,
    pub scale_factor: Option<String>,
    pub path: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LiveParametersIdentity {
    pub kind: String,
    pub path: String,
    pub file_sha256: String,
    pub names_kinds_values_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LiveReferenceIdentity {
    pub authority: String,
    pub captured_from_engine: bool,
    pub validation: String,
    pub path: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LiveDriverIdentity {
    pub kind: String,
    pub source_mode: String,
    pub interface: String,
    pub durable: bool,
    pub validator: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LiveNormalizationIdentity {
    pub upstream_temporal_window: String,
    pub internal_equivalent: String,
    pub reason: String,
    pub result_order: Vec<String>,
    pub numeric_format: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LiveParameters {
    pub schema: String,
    pub operation: Operation,
    pub bindings: LiveBindings,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LiveBindings {
    #[serde(rename = "tagClass")]
    pub tag_class: StringBinding,
    #[serde(rename = "window1Start")]
    pub window_1_start: Int64Binding,
    #[serde(rename = "window2Start")]
    pub window_2_start: Int64Binding,
    #[serde(rename = "windowEnd")]
    pub window_end: Int64Binding,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StringBinding {
    pub kind: String,
    pub value: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Int64Binding {
    pub kind: String,
    pub value: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LiveSeed {
    schema: String,
    seed: u64,
    tag_classes: Vec<String>,
    tags: Vec<LiveSeedTag>,
    messages: Vec<LiveSeedMessage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LiveSeedTag {
    name: String,
    tag_class: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LiveSeedMessage {
    id: i64,
    creation_day: i64,
    tags: Vec<String>,
}

struct ValidatedLiveContext {
    identity: LiveIdentity,
    parameters: LiveParameters,
    seed: LiveSeed,
    reference: ResultRows,
}

const LIVE_BI2_QUERY: &str = "\
MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass}) \
OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag) \
WHERE $window1Start <= message1.creationDate AND message1.creationDate < $window2Start \
WITH tag, count(message1) AS countWindow1 \
OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag) \
WHERE $window2Start <= message2.creationDate AND message2.creationDate < $windowEnd \
WITH tag, countWindow1, count(message2) AS countWindow2 \
RETURN tag.name AS tagName, countWindow1, countWindow2, \
abs(countWindow1 - countWindow2) AS diff \
ORDER BY diff DESC, tagName ASC LIMIT 100";

fn expected_live_identity() -> LiveIdentity {
    LiveIdentity {
        schema: LIVE_IDENTITY_SCHEMA.into(),
        suite_id: SUITE_ID.into(),
        operation: Operation::Bi2,
        source_mode: "runner_owned_rust_api".into(),
        certification: false,
        upstream_spec: UpstreamIdentity {
            source: "https://github.com/ldbc/ldbc_snb_docs".into(),
            release: "v2.2.4".into(),
            commit: "5f7956e07a214373c363b371a3b88bc83ddcd118".into(),
        },
        upstream_query: UpstreamQueryIdentity {
            source: "https://github.com/ldbc/ldbc_snb_bi".into(),
            release: "v1.0.0".into(),
            commit: "abf8cd4862f2b96ba9267e6298a1f7402439040b".into(),
            path: "cypher/queries/bi-2.cypher".into(),
        },
        fixture: LiveFixtureIdentity {
            dataset_id: "snb-bi-synthetic-minimal".into(),
            kind: "synthetic_minimal_snb_shaped".into(),
            provenance_kind: "content_addressed_committed_fixture".into(),
            seed: 963,
            scale_factor: None,
            path: "seed.json".into(),
            sha256: "9dc8c9987d4ce403a30808c268d25a6f9fdb451b7a5c68fbd2d37d700b4cb88e".into(),
        },
        parameters: LiveParametersIdentity {
            kind: "typed_internal_deterministic_bi2_parameters".into(),
            path: "parameters.json".into(),
            file_sha256: "55a5c78e5573909531b6290a38ccc7d670b1a8eb09136ebfc4b4668c45646d7a".into(),
            names_kinds_values_sha256:
                "79ef56283a4ac9320c82415977eb1f2c27574970bccb247e04cfaf02e59384b5".into(),
        },
        reference: LiveReferenceIdentity {
            authority: "independent_semantic_derivation_from_seed".into(),
            captured_from_engine: false,
            validation: "exact_order_numeric_text_normalized".into(),
            path: "expected-bi2.ref".into(),
            sha256: "bd4c037d3aae1881e9329e148743d153d4bf671afc1f35d3d7ab4eaab0748d7b".into(),
        },
        driver: LiveDriverIdentity {
            kind: "internal_rust_public_api_driver".into(),
            source_mode: "runner_executes_query_no_caller_result".into(),
            interface: "graphforge_api::GraphForge::execute_with_params".into(),
            durable: false,
            validator: "same_process_exact_order".into(),
        },
        normalization: LiveNormalizationIdentity {
            upstream_temporal_window: "$date plus 100-day durations".into(),
            internal_equivalent: "precomputed integer day boundaries [0,100), [100,200)".into(),
            reason: "exercise supported Cypher while not claiming temporal-literal parity".into(),
            result_order: vec!["diff DESC".into(), "tag.name ASC".into()],
            numeric_format: "base10_i64".into(),
        },
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn read_bytes(path: &Path, label: &str) -> Result<Vec<u8>, SuiteError> {
    fs::read(path).map_err(|error| {
        SuiteError::InvalidDocument(format!(
            "failed to read {label} {}: {error}",
            path.display()
        ))
    })
}

fn parse_closed_json<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    label: &str,
) -> Result<T, SuiteError> {
    serde_json::from_slice(bytes)
        .map_err(|error| SuiteError::InvalidDocument(format!("invalid {label}: {error}")))
}

/// Independently derive official BI2 rows from the seed and typed windows.
///
/// Semantics match upstream `bi-2.cypher`: every tag of `$tagClass` appears,
/// `countWindow1`/`countWindow2` use the half-open windows, `diff` is the
/// absolute count delta, and the total order is `diff DESC, tag.name ASC`
/// with `LIMIT 100`. This derivation is the reference authority; the committed
/// `.ref` file must match it and is never captured from GraphForge.
fn derive_expected_bi2(seed: &LiveSeed, bindings: &LiveBindings) -> ResultRows {
    let tag_class = &bindings.tag_class.value;
    let window_1_start = bindings.window_1_start.value;
    let window_2_start = bindings.window_2_start.value;
    let window_end = bindings.window_end.value;
    let mut rows: Vec<(i64, String, i64, i64)> = seed
        .tags
        .iter()
        .filter(|tag| tag.tag_class == *tag_class)
        .map(|tag| {
            let count_window_1 = seed
                .messages
                .iter()
                .filter(|message| {
                    message.tags.iter().any(|name| name == &tag.name)
                        && window_1_start <= message.creation_day
                        && message.creation_day < window_2_start
                })
                .count() as i64;
            let count_window_2 = seed
                .messages
                .iter()
                .filter(|message| {
                    message.tags.iter().any(|name| name == &tag.name)
                        && window_2_start <= message.creation_day
                        && message.creation_day < window_end
                })
                .count() as i64;
            let diff = (count_window_1 - count_window_2).abs();
            (diff, tag.name.clone(), count_window_1, count_window_2)
        })
        .collect();
    rows.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    rows.truncate(100);
    rows.into_iter()
        .map(|(diff, name, count_window_1, count_window_2)| {
            format!("{name} {count_window_1} {count_window_2} {diff}")
        })
        .collect()
}

fn validate_live_context(fixture: &Path) -> Result<ValidatedLiveContext, SuiteError> {
    let identity_bytes = read_bytes(&fixture.join("identity.json"), "live identity")?;
    let identity: LiveIdentity = parse_closed_json(&identity_bytes, "live identity")?;
    if identity != expected_live_identity() {
        return Err(SuiteError::InvalidDocument(
            "live_identity_drift: complete expected identity mismatch".into(),
        ));
    }

    let seed_bytes = read_bytes(&fixture.join(&identity.fixture.path), "live seed")?;
    if sha256_bytes(&seed_bytes) != identity.fixture.sha256 {
        return Err(SuiteError::InvalidDocument(
            "live_fixture_checksum_mismatch".into(),
        ));
    }
    let seed: LiveSeed = parse_closed_json(&seed_bytes, "live seed")?;
    if seed.schema != "graphforge-gdc-snb-bi-synthetic-seed/1" || seed.seed != identity.fixture.seed
    {
        return Err(SuiteError::InvalidDocument(
            "live_fixture_identity_mismatch".into(),
        ));
    }

    let parameter_bytes = read_bytes(&fixture.join(&identity.parameters.path), "live parameters")?;
    if sha256_bytes(&parameter_bytes) != identity.parameters.file_sha256 {
        return Err(SuiteError::InvalidDocument(
            "live_parameter_file_checksum_mismatch".into(),
        ));
    }
    let parameters: LiveParameters = parse_closed_json(&parameter_bytes, "live parameters")?;
    if parameters.schema != LIVE_PARAMETERS_SCHEMA || parameters.operation != Operation::Bi2 {
        return Err(SuiteError::InvalidDocument(
            "live_parameter_identity_mismatch".into(),
        ));
    }
    let binding_bytes = serde_json::to_vec(&parameters.bindings).map_err(|error| {
        SuiteError::InvalidDocument(format!("failed to canonicalize live parameters: {error}"))
    })?;
    if sha256_bytes(&binding_bytes) != identity.parameters.names_kinds_values_sha256 {
        return Err(SuiteError::InvalidDocument(
            "live_parameter_binding_digest_mismatch".into(),
        ));
    }
    let expected_parameters = LiveParameters {
        schema: LIVE_PARAMETERS_SCHEMA.into(),
        operation: Operation::Bi2,
        bindings: LiveBindings {
            tag_class: StringBinding {
                kind: "string".into(),
                value: "MusicalArtist".into(),
            },
            window_1_start: Int64Binding {
                kind: "int64".into(),
                value: 0,
            },
            window_2_start: Int64Binding {
                kind: "int64".into(),
                value: 100,
            },
            window_end: Int64Binding {
                kind: "int64".into(),
                value: 200,
            },
        },
    };
    if parameters != expected_parameters {
        return Err(SuiteError::InvalidDocument(
            "live_parameter_values_mismatch".into(),
        ));
    }

    let reference_bytes = read_bytes(&fixture.join(&identity.reference.path), "live reference")?;
    if sha256_bytes(&reference_bytes) != identity.reference.sha256 {
        return Err(SuiteError::InvalidDocument(
            "live_reference_checksum_mismatch".into(),
        ));
    }
    let reference = parse_result_rows(std::str::from_utf8(&reference_bytes).map_err(|error| {
        SuiteError::InvalidDocument(format!("reference is not UTF-8: {error}"))
    })?);
    let derived = derive_expected_bi2(&seed, &parameters.bindings);
    if derived != reference {
        return Err(SuiteError::ReferenceMismatch(
            "live_reference_does_not_match_independent_semantic_derivation".into(),
        ));
    }
    Ok(ValidatedLiveContext {
        identity,
        parameters,
        seed,
        reference,
    })
}

pub fn validate_live_fixture_context(fixture: &Path) -> Result<(), SuiteError> {
    validate_live_context(fixture).map(|_| ())
}

fn api_error(context: &str, error: impl fmt::Display) -> SuiteError {
    SuiteError::InvalidDocument(format!("live_api_execution_failed:{context}: {error}"))
}

fn execute_with_params(
    forge: &graphforge_api::GraphForge,
    query: &str,
    params: std::collections::HashMap<String, graphforge_api::IrLiteral>,
) -> Result<(), SuiteError> {
    forge
        .execute_with_params(query, &params)
        .map(|_| ())
        .map_err(|error| api_error("load", error))
}

fn load_live_seed(forge: &graphforge_api::GraphForge, seed: &LiveSeed) -> Result<u64, SuiteError> {
    use graphforge_api::IrLiteral;
    use std::collections::HashMap;

    for name in &seed.tag_classes {
        execute_with_params(
            forge,
            "CREATE (:TagClass {name: $name})",
            HashMap::from([("name".into(), IrLiteral::Str(name.clone()))]),
        )?;
    }
    for tag in &seed.tags {
        execute_with_params(
            forge,
            "CREATE (:Tag {name: $name})",
            HashMap::from([("name".into(), IrLiteral::Str(tag.name.clone()))]),
        )?;
        execute_with_params(
            forge,
            "MATCH (tag:Tag {name: $tag}), (class:TagClass {name: $class}) \
             CREATE (tag)-[:HAS_TYPE]->(class)",
            HashMap::from([
                ("tag".into(), IrLiteral::Str(tag.name.clone())),
                ("class".into(), IrLiteral::Str(tag.tag_class.clone())),
            ]),
        )?;
    }
    let mut tagged_edges = 0_u64;
    for message in &seed.messages {
        execute_with_params(
            forge,
            "CREATE (:Message {id: $id, creationDate: $creationDate})",
            HashMap::from([
                ("id".into(), IrLiteral::Int(message.id)),
                ("creationDate".into(), IrLiteral::Int(message.creation_day)),
            ]),
        )?;
        for tag in &message.tags {
            execute_with_params(
                forge,
                "MATCH (message:Message {id: $id}), (tag:Tag {name: $tag}) \
                 CREATE (message)-[:HAS_TAG]->(tag)",
                HashMap::from([
                    ("id".into(), IrLiteral::Int(message.id)),
                    ("tag".into(), IrLiteral::Str(tag.clone())),
                ]),
            )?;
            tagged_edges += 1;
        }
    }
    Ok(
        (seed.tag_classes.len() + seed.tags.len() + seed.tags.len() + seed.messages.len()) as u64
            + tagged_edges,
    )
}

fn execute_live_bi2(
    forge: &graphforge_api::GraphForge,
    bindings: &LiveBindings,
) -> Result<ResultRows, SuiteError> {
    use arrow::array::{Int64Array, StringArray};
    use graphforge_api::IrLiteral;
    use std::collections::HashMap;

    let params = HashMap::from([
        (
            "tagClass".into(),
            IrLiteral::Str(bindings.tag_class.value.clone()),
        ),
        (
            "window1Start".into(),
            IrLiteral::Int(bindings.window_1_start.value),
        ),
        (
            "window2Start".into(),
            IrLiteral::Int(bindings.window_2_start.value),
        ),
        (
            "windowEnd".into(),
            IrLiteral::Int(bindings.window_end.value),
        ),
    ]);
    let result = forge
        .execute_with_params(LIVE_BI2_QUERY, &params)
        .map_err(|error| api_error("query", error))?;
    let mut rows = Vec::new();
    for batch in result.batches {
        let tags = batch
            .column_by_name("tagName")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| SuiteError::InvalidDocument("BI2 tagName is not Utf8".into()))?;
        let count_1 = batch
            .column_by_name("countWindow1")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| SuiteError::InvalidDocument("BI2 countWindow1 is not Int64".into()))?;
        let count_2 = batch
            .column_by_name("countWindow2")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| SuiteError::InvalidDocument("BI2 countWindow2 is not Int64".into()))?;
        let diff = batch
            .column_by_name("diff")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| SuiteError::InvalidDocument("BI2 diff is not Int64".into()))?;
        for row in 0..batch.num_rows() {
            rows.push(format!(
                "{} {} {} {}",
                tags.value(row),
                count_1.value(row),
                count_2.value(row),
                diff.value(row)
            ));
        }
    }
    Ok(rows)
}

pub fn run_live_fixture(
    fixture: &Path,
    executable: &Path,
) -> Result<serde_json::Value, SuiteError> {
    let context = validate_live_context(fixture)?;
    let executable_sha256 = sha256_bytes(&read_bytes(executable, "runner executable")?);
    let forge =
        graphforge_api::GraphForge::new(None).map_err(|error| api_error("initialize", error))?;

    let load_started = Instant::now();
    let rows_loaded = load_live_seed(&forge, &context.seed)?;
    let load_ms = u64::try_from(load_started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let query_started = Instant::now();
    let rows = execute_live_bi2(&forge, &context.parameters.bindings)?;
    let query_ms = u64::try_from(query_started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let validation_started = Instant::now();
    let derived = derive_expected_bi2(&context.seed, &context.parameters.bindings);
    validate_result(ValidationMode::Exact, &derived, &context.reference)?;
    validate_result(ValidationMode::Exact, &derived, &rows)?;
    let validation_ms = u64::try_from(validation_started.elapsed().as_millis()).unwrap_or(u64::MAX);

    Ok(serde_json::json!({
        "schema": LIVE_EVIDENCE_SCHEMA,
        "suite_id": SUITE_ID,
        "lane": "live_in_memory",
        "operation": "BI2",
        "source_mode": "runner_owned_rust_api",
        "status": "passed",
        "certification": false,
        "phases": ["load", "query", "validation"],
        "identity": context.identity,
        "execution_authority": {
            "interface": "graphforge_api::GraphForge::execute_with_params",
            "runner_executable_sha256": executable_sha256,
            "caller_supplied_result": false
        },
        "correctness": {
            "status": "passed",
            "validation_mode": "exact",
            "numeric_format": "base10_i64",
            "reference_authority": "independent_semantic_derivation_from_seed",
            "rows": rows
        },
        "resources": {
            "correctness_authority": false,
            "load": {"wall_ms": load_ms, "rows_loaded": rows_loaded},
            "query": {"wall_ms": query_ms, "rows_returned": context.reference.len()},
            "validation": {"wall_ms": validation_ms},
            "unobserved": ["spill_bytes", "peak_rss_bytes", "io_bytes"]
        }
    }))
}

fn normalize_row(row: &str) -> String {
    row.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn load_result_rows(path: &Path) -> Result<ResultRows, SuiteError> {
    let text = fs::read_to_string(path).map_err(|error| {
        SuiteError::InvalidDocument(format!("failed to read {}: {error}", path.display()))
    })?;
    Ok(parse_result_rows(&text))
}

pub fn validate_result(
    mode: ValidationMode,
    reference: &ResultRows,
    system: &ResultRows,
) -> Result<(), SuiteError> {
    match mode {
        ValidationMode::Exact => {
            if reference == system {
                Ok(())
            } else {
                Err(SuiteError::ReferenceMismatch(format!(
                    "exact row mismatch: expected {reference:?} got {system:?}"
                )))
            }
        }
        ValidationMode::Normalized => {
            let mut reference_sorted = reference.clone();
            let mut system_sorted = system.clone();
            reference_sorted.sort();
            system_sorted.sort();
            if reference_sorted == system_sorted {
                Ok(())
            } else {
                Err(SuiteError::ReferenceMismatch(format!(
                    "normalized multiset mismatch: expected {reference_sorted:?} got {system_sorted:?}"
                )))
            }
        }
    }
}

pub fn run_job(
    job: &OperationJob,
    reference: Option<&ResultRows>,
    system: Option<&ResultRows>,
) -> OperationOutcome {
    let operation = job.operation;
    let category = operation.category().name().to_string();
    if let Err(error) = job.validate_schema() {
        return OperationOutcome {
            operation,
            category,
            status: OperationStatus::Failed,
            validation_mode: "none".into(),
            cause: Some(error.to_string()),
            public_api: None,
        };
    }
    match map_operation(operation) {
        MappingOutcome::SemanticIncompatibility { cause, detail } => OperationOutcome {
            operation,
            category,
            status: OperationStatus::SemanticIncompatibility,
            validation_mode: "none".into(),
            cause: Some(format!("{cause}: {detail}")),
            public_api: None,
        },
        MappingOutcome::Compatible(public_api) => {
            let mode = operation.intended_validation();
            let Some(reference) = reference else {
                return OperationOutcome {
                    operation,
                    category,
                    status: OperationStatus::Failed,
                    validation_mode: mode.name().into(),
                    cause: Some("missing_reference".into()),
                    public_api: Some(public_api),
                };
            };
            let Some(system) = system else {
                return OperationOutcome {
                    operation,
                    category,
                    status: OperationStatus::Failed,
                    validation_mode: mode.name().into(),
                    cause: Some("missing_system_output".into()),
                    public_api: Some(public_api),
                };
            };
            match validate_result(mode, reference, system) {
                Ok(()) => OperationOutcome {
                    operation,
                    category,
                    status: OperationStatus::Passed,
                    validation_mode: mode.name().into(),
                    cause: None,
                    public_api: Some(public_api),
                },
                Err(error) => OperationOutcome {
                    operation,
                    category,
                    status: OperationStatus::Failed,
                    validation_mode: mode.name().into(),
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
    resources: ResourceReport,
    outcomes: Vec<OperationOutcome>,
) -> SuiteEvidence {
    let status = if outcomes
        .iter()
        .any(|outcome| matches!(outcome.status, OperationStatus::Failed))
    {
        OperationStatus::Failed
    } else if outcomes
        .iter()
        .all(|outcome| matches!(outcome.status, OperationStatus::SemanticIncompatibility))
    {
        OperationStatus::SemanticIncompatibility
    } else {
        // Mixed pass + semantic incompatibility is admissible when every
        // operation either validated or failed closed with a typed cause.
        OperationStatus::Passed
    };
    SuiteEvidence {
        schema: EVIDENCE_SCHEMA.into(),
        suite_id: SUITE_ID.into(),
        dataset_id: dataset_id.into(),
        status,
        certification: false,
        phases: phase_names(),
        identities,
        resources,
        operations: outcomes,
    }
}

/// Human/operator-facing summary of every modeled operation.
pub fn operation_rules() -> BTreeMap<&'static str, String> {
    let mut rules = BTreeMap::new();
    for operation in Operation::ALL {
        let (mapping, cause) = match map_operation(operation) {
            MappingOutcome::Compatible(_) => ("compatible".to_string(), None),
            MappingOutcome::SemanticIncompatibility { cause, .. } => {
                ("semantic_incompatibility".to_string(), Some(cause))
            }
        };
        let validation = operation
            .validation_mode()
            .map(|mode| mode.name())
            .unwrap_or("none");
        let mut line = format!(
            "category={} validation={} mapping={}",
            operation.category().name(),
            validation,
            mapping
        );
        if let Some(cause) = cause {
            line.push(':');
            line.push_str(cause);
        }
        rules.insert(operation.code(), line);
    }
    rules
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn sample_job(operation: Operation) -> OperationJob {
        OperationJob {
            schema: JOB_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            dataset_id: BOUNDED_TINY_DATASET.into(),
            operation,
        }
    }

    fn sample_resources() -> ResourceReport {
        ResourceReport {
            schema: RESOURCE_SCHEMA.into(),
            dataset_id: BOUNDED_TINY_DATASET.into(),
            load: LoadResource {
                wall_ms: 12,
                rows_loaded: 4096,
            },
            query: QueryResource {
                wall_ms: 34,
                queries_executed: 17,
            },
            spill: SpillResource {
                bytes: 0,
                events: 0,
            },
            rss: RssResource {
                peak_bytes: 104_857_600,
            },
            io: IoResource {
                read_bytes: 2048,
                write_bytes: 512,
            },
        }
    }

    #[test]
    fn all_operations_declare_a_mapping_and_validation_disposition() {
        assert_eq!(Operation::ALL.len(), 36);
        for operation in Operation::ALL {
            let outcome = map_operation(operation);
            match outcome {
                MappingOutcome::Compatible(mapping) => {
                    assert!(operation.validation_mode().is_some(), "{operation}");
                    assert_eq!(mapping.interface, "cypher", "{operation}");
                    assert!(!mapping.cypher_shape.is_empty(), "{operation}");
                    assert!(!mapping.notes.is_empty(), "{operation}");
                }
                MappingOutcome::SemanticIncompatibility { cause, detail } => {
                    assert!(operation.validation_mode().is_none(), "{operation}");
                    assert!(!cause.is_empty());
                    assert!(!detail.is_empty());
                }
            }
        }
    }

    #[test]
    fn analytical_reads_map_to_public_api_except_weighted_paths() {
        let reads = Operation::ALL
            .into_iter()
            .filter(|operation| operation.category() == Category::AnalyticalRead);
        for operation in reads {
            if matches!(
                operation,
                Operation::Bi15 | Operation::Bi19 | Operation::Bi20
            ) {
                assert!(
                    matches!(
                        map_operation(operation),
                        MappingOutcome::SemanticIncompatibility { .. }
                    ),
                    "{operation} weighted path must fail closed"
                );
                continue;
            }
            assert!(
                matches!(map_operation(operation), MappingOutcome::Compatible(_)),
                "{operation} should be compatible"
            );
        }
    }

    #[test]
    fn weighted_path_reads_fail_closed_with_typed_cause() {
        for operation in [Operation::Bi15, Operation::Bi19, Operation::Bi20] {
            match map_operation(operation) {
                MappingOutcome::SemanticIncompatibility { cause, .. } => {
                    assert_eq!(cause, "weighted_shortest_path_not_exposed", "{operation}");
                }
                MappingOutcome::Compatible(_) => panic!("{operation} must fail closed"),
            }
        }
    }

    #[test]
    fn batch_updates_fail_closed_with_typed_cause() {
        for operation in Operation::ALL {
            if !matches!(
                operation.category(),
                Category::BatchInsert | Category::BatchDelete
            ) {
                continue;
            }
            match map_operation(operation) {
                MappingOutcome::SemanticIncompatibility { cause, .. } => {
                    assert_eq!(cause, "bi_batch_update_stream_not_exposed", "{operation}");
                }
                MappingOutcome::Compatible(_) => panic!("{operation} must fail closed"),
            }
        }
    }

    #[test]
    fn exact_validation_passes_and_mismatches() {
        let reference = parse_result_rows("100 Alice\n200 Bob\n");
        let ok = parse_result_rows("100 Alice\n200 Bob\n");
        validate_result(ValidationMode::Exact, &reference, &ok).unwrap();

        let reordered = parse_result_rows("200 Bob\n100 Alice\n");
        assert!(validate_result(ValidationMode::Exact, &reference, &reordered).is_err());

        let changed = parse_result_rows("100 Alice\n200 Carol\n");
        assert!(validate_result(ValidationMode::Exact, &reference, &changed).is_err());
    }

    #[test]
    fn normalized_validation_is_order_insensitive() {
        let reference = parse_result_rows("tag-a 3\ntag-b 2\n");
        let reordered = parse_result_rows("tag-b 2\ntag-a 3\n");
        validate_result(ValidationMode::Normalized, &reference, &reordered).unwrap();

        let missing = parse_result_rows("tag-a 3\ntag-c 9\n");
        assert!(validate_result(ValidationMode::Normalized, &reference, &missing).is_err());
    }

    fn sample_live_seed() -> LiveSeed {
        LiveSeed {
            schema: "graphforge-gdc-snb-bi-synthetic-seed/1".into(),
            seed: 963,
            tag_classes: vec!["MusicalArtist".into(), "SportsTeam".into()],
            tags: vec![
                LiveSeedTag {
                    name: "Alpha".into(),
                    tag_class: "MusicalArtist".into(),
                },
                LiveSeedTag {
                    name: "Beta".into(),
                    tag_class: "MusicalArtist".into(),
                },
                LiveSeedTag {
                    name: "Gamma".into(),
                    tag_class: "MusicalArtist".into(),
                },
                LiveSeedTag {
                    name: "Strikers".into(),
                    tag_class: "SportsTeam".into(),
                },
            ],
            messages: vec![
                LiveSeedMessage {
                    id: 1,
                    creation_day: 10,
                    tags: vec!["Alpha".into()],
                },
                LiveSeedMessage {
                    id: 2,
                    creation_day: 20,
                    tags: vec!["Alpha".into(), "Gamma".into()],
                },
                LiveSeedMessage {
                    id: 3,
                    creation_day: 30,
                    tags: vec!["Beta".into(), "Gamma".into()],
                },
                LiveSeedMessage {
                    id: 4,
                    creation_day: 110,
                    tags: vec!["Alpha".into()],
                },
                LiveSeedMessage {
                    id: 5,
                    creation_day: 120,
                    tags: vec!["Beta".into(), "Gamma".into()],
                },
                LiveSeedMessage {
                    id: 6,
                    creation_day: 130,
                    tags: vec!["Beta".into()],
                },
                LiveSeedMessage {
                    id: 7,
                    creation_day: 140,
                    tags: vec!["Beta".into()],
                },
                LiveSeedMessage {
                    id: 8,
                    creation_day: 15,
                    tags: vec!["Strikers".into()],
                },
            ],
        }
    }

    fn sample_live_bindings() -> LiveBindings {
        LiveBindings {
            tag_class: StringBinding {
                kind: "string".into(),
                value: "MusicalArtist".into(),
            },
            window_1_start: Int64Binding {
                kind: "int64".into(),
                value: 0,
            },
            window_2_start: Int64Binding {
                kind: "int64".into(),
                value: 100,
            },
            window_end: Int64Binding {
                kind: "int64".into(),
                value: 200,
            },
        }
    }

    #[test]
    fn live_bi2_has_total_order_and_rejects_swapped_ties() {
        assert_eq!(
            Operation::Bi2.validation_mode(),
            Some(ValidationMode::Exact)
        );
        assert!(LIVE_BI2_QUERY.contains("ORDER BY diff DESC, tagName ASC"));
        let reference = parse_result_rows("Beta 1 3 2\nAlpha 2 1 1\nGamma 2 1 1\n");
        validate_result(ValidationMode::Exact, &reference, &reference).unwrap();
        let swapped_tie = parse_result_rows("Beta 1 3 2\nGamma 2 1 1\nAlpha 2 1 1\n");
        assert!(matches!(
            validate_result(ValidationMode::Exact, &reference, &swapped_tie),
            Err(SuiteError::ReferenceMismatch(_))
        ));
    }

    #[test]
    fn independent_bi2_derivation_orders_beta_alpha_gamma() {
        let derived = derive_expected_bi2(&sample_live_seed(), &sample_live_bindings());
        assert_eq!(
            derived,
            vec![
                "Beta 1 3 2".to_string(),
                "Alpha 2 1 1".to_string(),
                "Gamma 2 1 1".to_string(),
            ]
        );
        let official_swapped_gamma_first =
            parse_result_rows("Beta 1 3 2\nGamma 2 1 1\nAlpha 2 1 1\n");
        assert_ne!(derived, official_swapped_gamma_first);
    }

    #[test]
    fn committed_live_fixture_matches_closed_identity_and_derivation() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/gdc/snb-bi-live");
        validate_live_fixture_context(&fixture).unwrap();
        let context = validate_live_context(&fixture).unwrap();
        assert_eq!(
            context.reference,
            vec![
                "Beta 1 3 2".to_string(),
                "Alpha 2 1 1".to_string(),
                "Gamma 2 1 1".to_string(),
            ]
        );
        assert!(!context.identity.certification);
        assert_eq!(
            context.identity.fixture.provenance_kind,
            "content_addressed_committed_fixture"
        );
        assert!(context.identity.upstream_spec.release.starts_with('v'));
        assert_eq!(context.identity.upstream_query.release, "v1.0.0");
    }

    #[test]
    fn trusted_runner_executes_public_in_memory_graphforge() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/gdc/snb-bi-live");
        let executable = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let evidence = run_live_fixture(&fixture, &executable).unwrap();
        assert_eq!(evidence["schema"], LIVE_EVIDENCE_SCHEMA);
        assert_eq!(evidence["source_mode"], "runner_owned_rust_api");
        assert_eq!(evidence["certification"], serde_json::json!(false));
        assert_eq!(
            evidence["execution_authority"]["caller_supplied_result"],
            serde_json::json!(false)
        );
        assert_eq!(
            evidence["correctness"]["rows"],
            serde_json::json!(["Beta 1 3 2", "Alpha 2 1 1", "Gamma 2 1 1"])
        );
        assert_eq!(evidence["correctness"]["validation_mode"], "exact");
        assert_eq!(
            evidence["resources"]["unobserved"],
            serde_json::json!(["spill_bytes", "peak_rss_bytes", "io_bytes"])
        );
    }

    #[test]
    fn live_expected_identity_is_closed_and_non_certifying() {
        let identity = expected_live_identity();
        assert_eq!(identity.schema, LIVE_IDENTITY_SCHEMA);
        assert_eq!(identity.suite_id, SUITE_ID);
        assert_eq!(identity.operation, Operation::Bi2);
        assert_eq!(identity.source_mode, "runner_owned_rust_api");
        assert!(!identity.certification);
        assert!(!identity.reference.captured_from_engine);
        assert_eq!(
            identity.reference.validation,
            "exact_order_numeric_text_normalized"
        );
        assert!(!identity.driver.durable);
        assert_eq!(
            identity.driver.interface,
            "graphforge_api::GraphForge::execute_with_params"
        );
    }

    #[test]
    fn run_job_passes_compatible_read_and_reports_incompatibility() {
        let reference = parse_result_rows("1 x\n");
        let system = parse_result_rows("1 x\n");
        let read = run_job(&sample_job(Operation::Bi1), Some(&reference), Some(&system));
        assert_eq!(read.status, OperationStatus::Passed);
        assert!(read.public_api.is_some());

        let insert = run_job(&sample_job(Operation::Ins1), None, None);
        assert_eq!(insert.status, OperationStatus::SemanticIncompatibility);
        assert!(insert
            .cause
            .as_deref()
            .unwrap()
            .contains("bi_batch_update_stream_not_exposed"));

        let weighted = run_job(&sample_job(Operation::Bi15), None, None);
        assert_eq!(weighted.status, OperationStatus::SemanticIncompatibility);
        assert!(weighted
            .cause
            .as_deref()
            .unwrap()
            .contains("weighted_shortest_path_not_exposed"));

        let missing_output = run_job(&sample_job(Operation::Bi1), Some(&reference), None);
        assert_eq!(missing_output.status, OperationStatus::Failed);
        assert_eq!(
            missing_output.cause.as_deref(),
            Some("missing_system_output")
        );
    }

    #[test]
    fn run_job_reports_reference_mismatch_as_failed() {
        let reference = parse_result_rows("1 x\n");
        let system = parse_result_rows("1 WRONG\n");
        let read = run_job(&sample_job(Operation::Bi1), Some(&reference), Some(&system));
        assert_eq!(read.status, OperationStatus::Failed);
        assert!(read
            .cause
            .as_deref()
            .unwrap()
            .contains("reference_mismatch"));
    }

    #[test]
    fn phases_are_separated_in_order() {
        assert_eq!(
            phase_names(),
            vec!["load", "updates", "query", "validation"]
        );
    }

    #[test]
    fn ladder_requires_tiny_dataset_first_and_larger_scales_opt_in() {
        let bad_first = DatasetLadder {
            schema: LADDER_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            datasets: vec![
                LadderEntry {
                    id: "snb-bi-sf1".into(),
                    order: 1,
                    role: "ladder".into(),
                    opt_in: true,
                },
                LadderEntry {
                    id: BOUNDED_TINY_DATASET.into(),
                    order: 2,
                    role: "fixture".into(),
                    opt_in: false,
                },
            ],
        };
        assert!(bad_first.validate().is_err());

        let not_opt_in = DatasetLadder {
            schema: LADDER_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            datasets: vec![
                LadderEntry {
                    id: BOUNDED_TINY_DATASET.into(),
                    order: 1,
                    role: "engineering_fixture".into(),
                    opt_in: false,
                },
                LadderEntry {
                    id: "snb-bi-sf1".into(),
                    order: 2,
                    role: "ladder".into(),
                    opt_in: false,
                },
            ],
        };
        assert!(not_opt_in.validate().is_err());

        let good = DatasetLadder {
            schema: LADDER_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            datasets: vec![
                LadderEntry {
                    id: BOUNDED_TINY_DATASET.into(),
                    order: 1,
                    role: "engineering_fixture".into(),
                    opt_in: false,
                },
                LadderEntry {
                    id: "snb-bi-sf1".into(),
                    order: 2,
                    role: "ladder".into(),
                    opt_in: true,
                },
            ],
        };
        good.validate().unwrap();
        assert_eq!(good.ordered_ids()[0], BOUNDED_TINY_DATASET);
    }

    #[test]
    fn resource_report_validates_schema_and_dataset() {
        let resources = sample_resources();
        resources.validate(BOUNDED_TINY_DATASET).unwrap();

        let mut wrong_schema = sample_resources();
        wrong_schema.schema = "wrong".into();
        assert!(wrong_schema.validate(BOUNDED_TINY_DATASET).is_err());

        let mut wrong_dataset = sample_resources();
        wrong_dataset.dataset_id = "snb-bi-sf1".into();
        assert!(wrong_dataset.validate(BOUNDED_TINY_DATASET).is_err());
    }

    #[test]
    fn evidence_keeps_resources_distinct_from_correctness() {
        let passed = run_job(
            &sample_job(Operation::Bi1),
            Some(&parse_result_rows("1 x\n")),
            Some(&parse_result_rows("1 x\n")),
        );
        let incompatible = run_job(&sample_job(Operation::Ins1), None, None);
        let evidence = assemble_evidence(
            BOUNDED_TINY_DATASET,
            serde_json::json!({}),
            sample_resources(),
            vec![passed, incompatible],
        );
        assert_eq!(evidence.status, OperationStatus::Passed);
        assert!(!evidence.certification);
        // Resource evidence is a distinct section, not an operation outcome.
        assert_eq!(evidence.resources.query.queries_executed, 17);
        assert_eq!(evidence.resources.rss.peak_bytes, 104_857_600);
        assert_eq!(evidence.operations.len(), 2);
        let rendered = serde_json::to_value(&evidence).unwrap();
        assert!(rendered.get("resources").is_some());
        assert!(rendered.get("operations").is_some());
        assert!(rendered["operations"][0].get("load").is_none());
    }
}
