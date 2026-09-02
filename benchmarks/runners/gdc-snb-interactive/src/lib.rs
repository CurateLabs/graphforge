//! GDC SNB Interactive suite: operation mapping, reference validation, evidence.
//!
//! Workload semantics live here (not in shared `gdc_contracts`). Product graph
//! behavior remains in GraphForge Rust crates; this runner only maps LDBC SNB
//! Interactive operations onto the public Cypher / analyst-verb surface,
//! validates read outputs against pinned references, and fails closed on
//! semantics the public property-graph + Cypher surface does not expose.
//!
//! Results here are engineering evidence only. They never masquerade as an
//! audited GDC certification (`SuiteEvidence::certification` is always `false`).

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{self, Write as _};
use std::fs;
use std::path::Path;
use std::str::FromStr;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use graphforge_api::{GraphForge, IrLiteral, PropValue};
use sha2::{Digest, Sha256};

pub const EVIDENCE_SCHEMA: &str = "graphforge-gdc-snb-interactive-evidence/1";
pub const JOB_SCHEMA: &str = "graphforge-gdc-snb-interactive-job/1";
pub const LADDER_SCHEMA: &str = "graphforge-gdc-snb-interactive-ladder/1";
pub const SUITE_ID: &str = "snb-interactive";

/// The bounded engineering fixture dataset every ladder and suite run begins on.
pub const BOUNDED_TINY_DATASET: &str = "snb-interactive-static-synthetic-v1";
pub const LIVE_IS1_DATASET: &str = "snb-interactive-live-is1-synthetic-v1";
pub const LIVE_IS1_QUERY: &str = "\
MATCH (person:Person)-[:IS_LOCATED_IN]->(city:City)
WHERE person.id = $personId
RETURN person.firstName AS firstName,
       person.lastName AS lastName,
       person.birthday AS birthday,
       person.locationIP AS locationIP,
       person.browserUsed AS browserUsed,
       city.id AS cityId,
       person.gender AS gender,
       person.creationDate AS creationDate";

const LIVE_GRAPH: &str =
    include_str!("../../../fixtures/gdc/snb-interactive-live-is1/graph.json");
const LIVE_JOB: &str = include_str!("../../../fixtures/gdc/snb-interactive-live-is1/IS1.json");
const LIVE_REFERENCE: &str =
    include_str!("../../../fixtures/gdc/snb-interactive-live-is1/IS1.ref");
const LIVE_ACQUISITION: &str =
    include_str!("../../../fixtures/gdc/snb-interactive-live-is1/acquisition.json");
const LIVE_IDENTITY: &str =
    include_str!("../../../profiles/gdc/snb-interactive-live-is1-identity.json");

const LIVE_GRAPH_SHA256: &str =
    "58e3c52a4ac2d74456439a322211adf1e8f560a7762e3fd2d376bbe96d243d6f";
const LIVE_JOB_SHA256: &str =
    "0143a649da769c093d5e235e66c6036a4aa38ab05a5c68f5744ad4025a503831";
const LIVE_REFERENCE_SHA256: &str =
    "71465ea5b672abd79693590e316cb4cc023cd25737c57d8daa13467542972385";
const LIVE_ACQUISITION_SHA256: &str =
    "fe8167c8b9cb939306495a937b45c375ea09b08725772a439c487099363f25e2";
const LIVE_IDENTITY_SHA256: &str =
    "a7b31720ac9ba61a5968f752d4e8eb8d709353226f36a2dcd065016657d4f030";

/// SNB Interactive workload category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
    ComplexRead,
    ShortRead,
    Update,
}

impl Category {
    pub fn name(self) -> &'static str {
        match self {
            Self::ComplexRead => "complex_read",
            Self::ShortRead => "short_read",
            Self::Update => "update",
        }
    }
}

/// Reference-validation mode for a compatible read operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationMode {
    /// Ordered, row-for-row comparison (the operation has a total result order).
    Exact,
    /// Order-insensitive multiset comparison with per-row whitespace
    /// normalization (the operation's result is a set without a spec-mandated
    /// total tie-break).
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

/// LDBC SNB Interactive operations modeled by this suite.
///
/// Complex reads (`IC1`..`IC14`), short reads (`IS1`..`IS7`), and the
/// representative update/insert stream (`IU1`..`IU8`).
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Operation {
    #[serde(rename = "IC1")]
    Ic1,
    #[serde(rename = "IC2")]
    Ic2,
    #[serde(rename = "IC3")]
    Ic3,
    #[serde(rename = "IC4")]
    Ic4,
    #[serde(rename = "IC5")]
    Ic5,
    #[serde(rename = "IC6")]
    Ic6,
    #[serde(rename = "IC7")]
    Ic7,
    #[serde(rename = "IC8")]
    Ic8,
    #[serde(rename = "IC9")]
    Ic9,
    #[serde(rename = "IC10")]
    Ic10,
    #[serde(rename = "IC11")]
    Ic11,
    #[serde(rename = "IC12")]
    Ic12,
    #[serde(rename = "IC13")]
    Ic13,
    #[serde(rename = "IC14")]
    Ic14,
    #[serde(rename = "IS1")]
    Is1,
    #[serde(rename = "IS2")]
    Is2,
    #[serde(rename = "IS3")]
    Is3,
    #[serde(rename = "IS4")]
    Is4,
    #[serde(rename = "IS5")]
    Is5,
    #[serde(rename = "IS6")]
    Is6,
    #[serde(rename = "IS7")]
    Is7,
    #[serde(rename = "IU1")]
    Iu1,
    #[serde(rename = "IU2")]
    Iu2,
    #[serde(rename = "IU3")]
    Iu3,
    #[serde(rename = "IU4")]
    Iu4,
    #[serde(rename = "IU5")]
    Iu5,
    #[serde(rename = "IU6")]
    Iu6,
    #[serde(rename = "IU7")]
    Iu7,
    #[serde(rename = "IU8")]
    Iu8,
}

impl Operation {
    pub const ALL: [Self; 29] = [
        Self::Ic1,
        Self::Ic2,
        Self::Ic3,
        Self::Ic4,
        Self::Ic5,
        Self::Ic6,
        Self::Ic7,
        Self::Ic8,
        Self::Ic9,
        Self::Ic10,
        Self::Ic11,
        Self::Ic12,
        Self::Ic13,
        Self::Ic14,
        Self::Is1,
        Self::Is2,
        Self::Is3,
        Self::Is4,
        Self::Is5,
        Self::Is6,
        Self::Is7,
        Self::Iu1,
        Self::Iu2,
        Self::Iu3,
        Self::Iu4,
        Self::Iu5,
        Self::Iu6,
        Self::Iu7,
        Self::Iu8,
    ];

    pub fn code(self) -> &'static str {
        match self {
            Self::Ic1 => "IC1",
            Self::Ic2 => "IC2",
            Self::Ic3 => "IC3",
            Self::Ic4 => "IC4",
            Self::Ic5 => "IC5",
            Self::Ic6 => "IC6",
            Self::Ic7 => "IC7",
            Self::Ic8 => "IC8",
            Self::Ic9 => "IC9",
            Self::Ic10 => "IC10",
            Self::Ic11 => "IC11",
            Self::Ic12 => "IC12",
            Self::Ic13 => "IC13",
            Self::Ic14 => "IC14",
            Self::Is1 => "IS1",
            Self::Is2 => "IS2",
            Self::Is3 => "IS3",
            Self::Is4 => "IS4",
            Self::Is5 => "IS5",
            Self::Is6 => "IS6",
            Self::Is7 => "IS7",
            Self::Iu1 => "IU1",
            Self::Iu2 => "IU2",
            Self::Iu3 => "IU3",
            Self::Iu4 => "IU4",
            Self::Iu5 => "IU5",
            Self::Iu6 => "IU6",
            Self::Iu7 => "IU7",
            Self::Iu8 => "IU8",
        }
    }

    pub fn category(self) -> Category {
        match self {
            Self::Ic1
            | Self::Ic2
            | Self::Ic3
            | Self::Ic4
            | Self::Ic5
            | Self::Ic6
            | Self::Ic7
            | Self::Ic8
            | Self::Ic9
            | Self::Ic10
            | Self::Ic11
            | Self::Ic12
            | Self::Ic13
            | Self::Ic14 => Category::ComplexRead,
            Self::Is1 | Self::Is2 | Self::Is3 | Self::Is4 | Self::Is5 | Self::Is6 | Self::Is7 => {
                Category::ShortRead
            }
            Self::Iu1
            | Self::Iu2
            | Self::Iu3
            | Self::Iu4
            | Self::Iu5
            | Self::Iu6
            | Self::Iu7
            | Self::Iu8 => Category::Update,
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
    /// spec-mandated total order use `Exact`; set-shaped aggregations whose ties
    /// are not totally ordered use `Normalized`.
    fn intended_validation(self) -> ValidationMode {
        match self {
            Self::Ic4 | Self::Ic6 | Self::Ic10 => ValidationMode::Normalized,
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
                SuiteError::InvalidDocument(format!("unknown SNB Interactive operation: {value}"))
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<BTreeMap<String, serde_json::Value>>,
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
                "job suite_id must be snb-interactive".into(),
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
                "ladder suite_id must be snb-interactive".into(),
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
        if sorted.first().map(|entry| entry.id.as_str()) != Some(BOUNDED_TINY_DATASET) {
            return Err(SuiteError::InvalidDocument(format!(
                "ordered ladder must begin with bounded fixture {BOUNDED_TINY_DATASET}"
            )));
        }
        Ok(())
    }

    pub fn ordered_ids(&self) -> Vec<String> {
        let mut sorted = self.datasets.clone();
        sorted.sort_by_key(|entry| entry.order);
        sorted.into_iter().map(|entry| entry.id).collect()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Passed,
    Failed,
    SemanticIncompatibility,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceLane {
    StaticReplay,
    LiveInMemory,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    Passed,
    Failed,
    NotExecuted,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PhaseEvidence {
    pub phase: String,
    pub status: PhaseStatus,
    pub detail: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LiveParameterContext {
    pub name: String,
    pub data_type: String,
    pub value: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LiveRowField {
    pub name: String,
    pub data_type: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LiveExecutionContext {
    pub operation: Operation,
    pub parameter: LiveParameterContext,
    pub fixture_sha256: String,
    pub job_sha256: String,
    pub reference_sha256: String,
    pub acquisition_sha256: String,
    pub identity_sha256: String,
    pub public_api: String,
    pub mode: String,
    pub query: String,
    pub row_schema: Vec<LiveRowField>,
    pub row_order: Vec<String>,
    pub normalization: String,
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

/// SNB Interactive lifecycle phases, kept explicitly separate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Load,
    Warmup,
    Execution,
    Validation,
}

impl Phase {
    pub const ALL: [Self; 4] = [Self::Load, Self::Warmup, Self::Execution, Self::Validation];

    pub fn name(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Warmup => "warmup",
            Self::Execution => "execution",
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
    pub lane: EvidenceLane,
    pub status: OperationStatus,
    /// Engineering evidence flag: never an audited GDC certification.
    pub certification: bool,
    pub phases: Vec<String>,
    pub phase_evidence: Vec<PhaseEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_context: Option<LiveExecutionContext>,
    pub identities: serde_json::Value,
    pub operations: Vec<OperationOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SuiteError {
    InvalidDocument(String),
    ReferenceMismatch(String),
    LiveExecution(String),
    SemanticIncompatibility { cause: String, detail: String },
}

impl fmt::Display for SuiteError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDocument(message) => write!(output, "invalid_document: {message}"),
            Self::ReferenceMismatch(message) => write!(output, "reference_mismatch: {message}"),
            Self::LiveExecution(message) => write!(output, "live_execution: {message}"),
            Self::SemanticIncompatibility { cause, detail } => {
                write!(output, "semantic_incompatibility:{cause}: {detail}")
            }
        }
    }
}

impl std::error::Error for SuiteError {}

/// Map an SNB Interactive operation onto the public GraphForge surface.
///
/// Read-only complex/short reads that are ordinary graph traversals or
/// aggregations map to Cypher (or a public analyst path verb). Operations that
/// require semantics the public property-graph + Cypher surface does not expose
/// fail closed with a typed cause instead of silently approximating.
pub fn map_operation(operation: Operation) -> MappingOutcome {
    match operation {
        Operation::Ic1 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS*1..3]-(f:Person) WHERE f.firstName=$name \
             RETURN f ORDER BY distance, f.lastName, f.id LIMIT 20",
            "transitive KNOWS traversal to depth 3 with name filter; ordinary Cypher variable-length match",
        ),
        Operation::Ic2 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS]-(f)<-[:HAS_CREATOR]-(m:Message) \
             WHERE m.creationDate<=$date RETURN f,m ORDER BY m.creationDate DESC, m.id LIMIT 20",
            "friends' recent messages before a date; traversal + filter + order",
        ),
        Operation::Ic3 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS*1..2]-(f)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(c:Country) \
             WHERE c.name IN [$countryX,$countryY] RETURN f, count(*) ORDER BY count(*) DESC, f.id LIMIT 20",
            "friends/friends-of-friends filtered by country with aggregation",
        ),
        Operation::Ic4 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS]-(f)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(t:Tag) \
             WHERE post.creationDate>=$start AND post.creationDate<$end RETURN t.name, count(*)",
            "new-topic tag aggregation over friends' posts; result is a tag set (normalized validation)",
        ),
        Operation::Ic5 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS*1..2]-(f)<-[:HAS_MEMBER]-(forum:Forum) \
             WHERE forum.joinDate>$date OPTIONAL MATCH (forum)-[:CONTAINER_OF]->(post)-[:HAS_CREATOR]->(f) \
             RETURN forum, count(post) ORDER BY count(post) DESC, forum.id LIMIT 20",
            "new forum groups among friends and friends-of-friends with post counts",
        ),
        Operation::Ic6 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS*1..2]-(f)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(t:Tag {name:$tag}) \
             MATCH (post)-[:HAS_TAG]->(other:Tag) WHERE other.name<>$tag RETURN other.name, count(*)",
            "tag co-occurrence over friends-of-friends posts; result is a tag set (normalized validation)",
        ),
        Operation::Ic7 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})<-[:HAS_CREATOR]-(m:Message)<-[l:LIKES]-(liker:Person) \
             RETURN liker, m, l.creationDate ORDER BY l.creationDate DESC, liker.id LIMIT 20",
            "recent likers of a person's messages; traversal + order",
        ),
        Operation::Ic8 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})<-[:HAS_CREATOR]-(m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(a:Person) \
             RETURN a, c ORDER BY c.creationDate DESC, c.id LIMIT 20",
            "recent replies to a person's messages; traversal + order",
        ),
        Operation::Ic9 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS*1..2]-(f)<-[:HAS_CREATOR]-(m:Message) \
             WHERE m.creationDate<$date RETURN f,m ORDER BY m.creationDate DESC, m.id LIMIT 20",
            "recent messages by friends-of-friends before a date",
        ),
        Operation::Ic10 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS*2..2]-(f) WHERE f.birthday matches window \
             OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(post)-[:HAS_TAG]->(t)<-[:HAS_INTEREST]-(p) \
             RETURN f, commonInterestScore ORDER BY score DESC, f.id",
            "friend recommendation by common-interest score; equal-score ties are a set (normalized validation)",
        ),
        Operation::Ic11 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS*1..2]-(f)-[w:WORK_AT]->(co:Company)-[:IS_LOCATED_IN]->(c:Country {name:$country}) \
             WHERE w.workFrom<$year RETURN f, co, w.workFrom ORDER BY w.workFrom, f.id, co.name LIMIT 10",
            "job referral: friends working at companies in a country ordered by start year",
        ),
        Operation::Ic12 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:KNOWS]-(f)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_TAG]->(t:Tag)-[:HAS_TYPE]->(tc:TagClass) \
             WHERE tc.name=$class OR (tc)-[:IS_SUBCLASS_OF*]->(:TagClass {name:$class}) RETURN f, count(c) ORDER BY count(c) DESC, f.id LIMIT 20",
            "expert search over a tag-class hierarchy; variable-length subclass traversal in Cypher",
        ),
        Operation::Ic13 => MappingOutcome::Compatible(PublicApiMapping {
            interface: "analyst_verb".into(),
            cypher_shape: "paths(source=$person1, target=$person2, by=bfs) over the undirected KNOWS graph".into(),
            notes: "single shortest path length between two persons via the public bfs path verb".into(),
        }),
        Operation::Ic14 => MappingOutcome::SemanticIncompatibility {
            cause: "weighted_interaction_path_enumeration_not_exposed",
            detail: "IC14 enumerates ALL shortest paths between two persons and scores each edge by a \
                     dynamically computed interaction weight (reply/comment counts); the public surface \
                     exposes single-path analyst verbs and pattern matching but not all-shortest-path \
                     enumeration with a per-edge computed weight function"
                .into(),
        },
        Operation::Is1 => compatible(
            "cypher",
            "MATCH (p:Person)-[:IS_LOCATED_IN]->(city:City) WHERE p.id=$personId \
             RETURN p.firstName, p.lastName, p.birthday, p.locationIP, p.browserUsed, \
             city.id, p.gender, p.creationDate",
            "SNB IS1 person profile lookup by explicit personId, including residence city",
        ),
        Operation::Is2 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})<-[:HAS_CREATOR]-(m:Message) RETURN m ORDER BY m.creationDate DESC, m.id DESC LIMIT 10",
            "person's ten most recent messages",
        ),
        Operation::Is3 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[k:KNOWS]-(f:Person) RETURN f.id, f.firstName, k.creationDate ORDER BY k.creationDate DESC, f.id",
            "person's friends ordered by friendship date",
        ),
        Operation::Is4 => compatible(
            "cypher",
            "MATCH (m:Message {id:$id}) RETURN coalesce(m.content, m.imageFile), m.creationDate",
            "message content lookup by id",
        ),
        Operation::Is5 => compatible(
            "cypher",
            "MATCH (m:Message {id:$id})-[:HAS_CREATOR]->(p:Person) RETURN p.id, p.firstName, p.lastName",
            "message creator lookup",
        ),
        Operation::Is6 => compatible(
            "cypher",
            "MATCH (m:Message {id:$id})-[:REPLY_OF*0..]->(post:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person) \
             RETURN f.id, f.title, mod.id",
            "forum and moderator of the post a message belongs to",
        ),
        Operation::Is7 => compatible(
            "cypher",
            "MATCH (m:Message {id:$id})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(a:Person) \
             RETURN c.id, c.content, a.id ORDER BY c.creationDate DESC, a.id",
            "direct replies to a message",
        ),
        Operation::Iu1 => update_incompatible("IU1 inserts a Person with dependency-time ordered edges"),
        Operation::Iu2 => update_incompatible("IU2 records a Person-likes-Post interaction"),
        Operation::Iu3 => update_incompatible("IU3 records a Person-likes-Comment interaction"),
        Operation::Iu4 => update_incompatible("IU4 inserts a Forum"),
        Operation::Iu5 => update_incompatible("IU5 inserts a Forum membership"),
        Operation::Iu6 => update_incompatible("IU6 inserts a Post"),
        Operation::Iu7 => update_incompatible("IU7 inserts a Comment reply"),
        Operation::Iu8 => update_incompatible("IU8 inserts a KNOWS friendship"),
    }
}

fn compatible(interface: &str, cypher_shape: &str, notes: &str) -> MappingOutcome {
    MappingOutcome::Compatible(PublicApiMapping {
        interface: interface.into(),
        cypher_shape: cypher_shape.into(),
        notes: notes.into(),
    })
}

fn update_incompatible(detail: &str) -> MappingOutcome {
    MappingOutcome::SemanticIncompatibility {
        cause: "interactive_update_stream_not_exposed",
        detail: format!(
            "{detail}; the SNB Interactive update stream requires the official driver's transactional \
             semantics, dependency-time ordering, and write validation, none of which the public surface exposes"
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
        lane: EvidenceLane::StaticReplay,
        status,
        certification: false,
        phases: phase_names(),
        phase_evidence: Phase::ALL
            .iter()
            .map(|phase| PhaseEvidence {
                phase: phase.name().into(),
                status: PhaseStatus::NotExecuted,
                detail: "static replay reads committed system-output rows; no GraphForge execution"
                    .into(),
            })
            .collect(),
        live_context: None,
        identities,
        operations: outcomes,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveGraphFixture {
    classification: String,
    nodes: Vec<LiveFixtureNode>,
    edges: Vec<LiveFixtureEdge>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveFixtureNode {
    key: String,
    label: String,
    properties: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveFixtureEdge {
    source: String,
    #[serde(rename = "type")]
    rel_type: String,
    destination: String,
}

#[derive(Clone, Copy)]
struct TrustedLiveAssets<'a> {
    graph: &'a str,
    job: &'a str,
    reference: &'a str,
    acquisition: &'a str,
    identity: &'a str,
}

fn embedded_live_assets() -> TrustedLiveAssets<'static> {
    TrustedLiveAssets {
        graph: LIVE_GRAPH,
        job: LIVE_JOB,
        reference: LIVE_REFERENCE,
        acquisition: LIVE_ACQUISITION,
        identity: LIVE_IDENTITY,
    }
}

fn sha256(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn require_sha(label: &str, text: &str, expected: &str) -> Result<(), SuiteError> {
    let actual = sha256(text);
    if actual == expected {
        Ok(())
    } else {
        Err(SuiteError::InvalidDocument(format!(
            "trusted live {label} checksum mismatch: expected {expected}, got {actual}"
        )))
    }
}

fn validate_trusted_live_assets(
    assets: TrustedLiveAssets<'_>,
) -> Result<(LiveGraphFixture, OperationJob, ResultRows, serde_json::Value), SuiteError> {
    for (label, text, digest) in [
        ("fixture", assets.graph, LIVE_GRAPH_SHA256),
        ("job", assets.job, LIVE_JOB_SHA256),
        ("reference", assets.reference, LIVE_REFERENCE_SHA256),
        ("acquisition", assets.acquisition, LIVE_ACQUISITION_SHA256),
        ("identity", assets.identity, LIVE_IDENTITY_SHA256),
    ] {
        require_sha(label, text, digest)?;
    }

    let fixture: LiveGraphFixture = serde_json::from_str(assets.graph)
        .map_err(|error| SuiteError::InvalidDocument(format!("invalid live fixture: {error}")))?;
    if fixture.classification != "synthetic_engineering_fixture" {
        return Err(SuiteError::InvalidDocument(
            "live fixture classification must be synthetic_engineering_fixture".into(),
        ));
    }
    let job: OperationJob = serde_json::from_str(assets.job)
        .map_err(|error| SuiteError::InvalidDocument(format!("invalid live job: {error}")))?;
    validate_live_job(&job)?;

    let identity: serde_json::Value = serde_json::from_str(assets.identity)
        .map_err(|error| SuiteError::InvalidDocument(format!("invalid live identity: {error}")))?;
    let acquisition: serde_json::Value = serde_json::from_str(assets.acquisition).map_err(|error| {
        SuiteError::InvalidDocument(format!("invalid live acquisition: {error}"))
    })?;
    if identity["suite_id"] != SUITE_ID
        || identity["datasets"][0]["id"] != LIVE_IS1_DATASET
        || identity["datasets"][0]["role"] != "other"
        || identity["datasets"][0]["checksum_sha256"] != LIVE_GRAPH_SHA256
        || identity["references"][0]["checksum_sha256"] != LIVE_REFERENCE_SHA256
        || acquisition["recorded_spec"] != identity["spec"]
        || acquisition["recorded_generator"] != identity["generator"]
        || acquisition["recorded_driver"] != identity["driver"]
        || acquisition["assets"][0]["checksum_sha256"] != LIVE_GRAPH_SHA256
        || acquisition["references"][0]["checksum_sha256"] != LIVE_REFERENCE_SHA256
    {
        return Err(SuiteError::InvalidDocument(
            "trusted live identity or acquisition context is inconsistent".into(),
        ));
    }
    let identities = serde_json::json!({
        "spec": identity["spec"].clone(),
        "generator": identity["generator"].clone(),
        "driver": identity["driver"].clone(),
        "fixture": {"classification": "synthetic_engineering_fixture"},
        "runner": {
            "name": "graphforge-benchmark-gdc-snb-interactive",
            "release": "workspace",
            "commit": null
        }
    });
    Ok((fixture, job, parse_result_rows(assets.reference), identities))
}

fn validate_live_job(job: &OperationJob) -> Result<(), SuiteError> {
    job.validate_schema()?;
    if job.operation != Operation::Is1 {
        return Err(SuiteError::InvalidDocument(
            "trusted live lane supports only IS1".into(),
        ));
    }
    if job.dataset_id != LIVE_IS1_DATASET {
        return Err(SuiteError::InvalidDocument(
            "trusted live IS1 requires its fixed synthetic fixture".into(),
        ));
    }
    let Some(parameters) = &job.parameters else {
        return Err(SuiteError::InvalidDocument(
            "trusted live IS1 requires explicit parameters".into(),
        ));
    };
    if parameters.len() != 1
        || parameters.get("personId").and_then(serde_json::Value::as_i64) != Some(1001)
    {
        return Err(SuiteError::InvalidDocument(
            "trusted live IS1 requires exactly personId:int64=1001".into(),
        ));
    }
    Ok(())
}

fn fixture_property(value: &serde_json::Value) -> Result<PropValue, SuiteError> {
    match value {
        serde_json::Value::String(value) => Ok(PropValue::Str(value.clone())),
        serde_json::Value::Number(value) => value.as_i64().map(PropValue::Int).ok_or_else(|| {
            SuiteError::InvalidDocument("fixture numbers must be signed 64-bit integers".into())
        }),
        serde_json::Value::Bool(value) => Ok(PropValue::Bool(*value)),
        _ => Err(SuiteError::InvalidDocument(
            "fixture properties must be non-null scalar strings, integers, or booleans".into(),
        )),
    }
}

fn load_live_fixture(forge: &GraphForge, fixture: LiveGraphFixture) -> Result<(), SuiteError> {
    let mut handles = BTreeMap::new();
    for node in fixture.nodes {
        let properties = node
            .properties
            .iter()
            .map(|(name, value)| Ok((name.clone(), fixture_property(value)?)))
            .collect::<Result<HashMap<_, _>, SuiteError>>()?;
        let handle = forge
            .add_node(&node.label, &properties)
            .map_err(|error| SuiteError::LiveExecution(error.to_string()))?;
        if handles.insert(node.key, handle).is_some() {
            return Err(SuiteError::InvalidDocument(
                "fixture node keys must be unique".into(),
            ));
        }
    }
    for edge in fixture.edges {
        let source = handles.get(&edge.source).ok_or_else(|| {
            SuiteError::InvalidDocument("fixture edge source key is missing".into())
        })?;
        let destination = handles.get(&edge.destination).ok_or_else(|| {
            SuiteError::InvalidDocument("fixture edge destination key is missing".into())
        })?;
        forge
            .add_edge(source, &edge.rel_type, destination, &HashMap::new())
            .map_err(|error| SuiteError::LiveExecution(error.to_string()))?;
    }
    Ok(())
}

fn normalize_live_result(
    result: &graphforge_api::ExecutionResult,
) -> Result<ResultRows, SuiteError> {
    let expected = [
        ("firstName", DataType::Utf8),
        ("lastName", DataType::Utf8),
        ("birthday", DataType::Utf8),
        ("locationIP", DataType::Utf8),
        ("browserUsed", DataType::Utf8),
        ("cityId", DataType::Int64),
        ("gender", DataType::Utf8),
        ("creationDate", DataType::Utf8),
    ];
    if result.schema.fields().len() != expected.len()
        || result
            .schema
            .fields()
            .iter()
            .zip(expected.iter())
            .any(|(actual, (name, data_type))| {
                actual.name() != name || actual.data_type() != data_type
            })
    {
        return Err(SuiteError::LiveExecution(format!(
            "IS1 Arrow schema drifted: expected {expected:?}, got {:?}",
            result.schema
        )));
    }
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row_index in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(expected.len());
            for column in batch.columns() {
                if column.is_null(row_index) {
                    return Err(SuiteError::LiveExecution(
                        "IS1 returned a null field".into(),
                    ));
                }
                values.push(
                    array_value_to_string(column.as_ref(), row_index)
                        .map_err(|error| SuiteError::LiveExecution(error.to_string()))?,
                );
            }
            rows.push(normalize_row(&values.join("\t")));
        }
    }
    Ok(rows)
}

pub fn run_trusted_live_is1() -> Result<SuiteEvidence, SuiteError> {
    let (fixture, job, reference, identities) =
        validate_trusted_live_assets(embedded_live_assets())?;
    let forge = GraphForge::new(None)
        .map_err(|error| SuiteError::LiveExecution(format!("in-memory open failed: {error}")))?;
    load_live_fixture(&forge, fixture)?;
    let parameters = HashMap::from([("personId".into(), IrLiteral::Int(1001))]);
    forge
        .execute_with_params(LIVE_IS1_QUERY, &parameters)
        .map_err(|error| SuiteError::LiveExecution(format!("warmup failed: {error}")))?;
    let execution = forge
        .execute_with_params(LIVE_IS1_QUERY, &parameters)
        .map_err(|error| SuiteError::LiveExecution(format!("execution failed: {error}")))?;
    let system = normalize_live_result(&execution)?;
    let outcome = run_live_is1_job(&job, &reference, &system);
    if !matches!(outcome.status, OperationStatus::Passed) {
        return Err(SuiteError::ReferenceMismatch(
            outcome
                .cause
                .clone()
                .unwrap_or_else(|| "trusted live IS1 validation failed".into()),
        ));
    }
    Ok(assemble_live_is1_evidence(
        LIVE_IS1_DATASET,
        identities,
        outcome,
    ))
}

fn run_live_is1_job(
    job: &OperationJob,
    reference: &ResultRows,
    system: &ResultRows,
) -> OperationOutcome {
    if let Err(error) = validate_live_job(job) {
        return failed_live_is1(job.operation, &error.to_string());
    }
    run_job(job, Some(reference), Some(system))
}

fn failed_live_is1(operation: Operation, cause: &str) -> OperationOutcome {
    OperationOutcome {
        operation,
        category: operation.category().name().into(),
        status: OperationStatus::Failed,
        validation_mode: ValidationMode::Exact.name().into(),
        cause: Some(format!("invalid_live_job: {cause}")),
        public_api: None,
    }
}

fn assemble_live_is1_evidence(
    dataset_id: &str,
    identities: serde_json::Value,
    is1: OperationOutcome,
) -> SuiteEvidence {
    let validation_status = if matches!(is1.status, OperationStatus::Passed) {
        PhaseStatus::Passed
    } else {
        PhaseStatus::Failed
    };
    let mut outcomes = vec![is1];
    outcomes.push(run_job(
        &OperationJob {
            schema: JOB_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            dataset_id: dataset_id.into(),
            operation: Operation::Ic14,
            parameters: None,
        },
        None,
        None,
    ));
    for operation in [
        Operation::Iu1,
        Operation::Iu2,
        Operation::Iu3,
        Operation::Iu4,
        Operation::Iu5,
        Operation::Iu6,
        Operation::Iu7,
        Operation::Iu8,
    ] {
        outcomes.push(run_job(
            &OperationJob {
                schema: JOB_SCHEMA.into(),
                suite_id: SUITE_ID.into(),
                dataset_id: dataset_id.into(),
                operation,
                parameters: None,
            },
            None,
            None,
        ));
    }
    let status = if matches!(outcomes[0].status, OperationStatus::Passed) {
        OperationStatus::Passed
    } else {
        OperationStatus::Failed
    };
    SuiteEvidence {
        schema: EVIDENCE_SCHEMA.into(),
        suite_id: SUITE_ID.into(),
        dataset_id: dataset_id.into(),
        lane: EvidenceLane::LiveInMemory,
        status,
        certification: false,
        phases: phase_names(),
        phase_evidence: [
            ("load", "synthetic fixture loaded with public GraphForge construction API"),
            (
                "warmup",
                "IS1 executed once with explicit parameters through public GraphForge.execute",
            ),
            (
                "execution",
                "IS1 executed through the real in-memory engine and returned an Arrow table",
            ),
            (
                "validation",
                "Arrow rows normalized then checked by the Rust authoritative validator",
            ),
        ]
        .into_iter()
        .map(|(phase, detail)| PhaseEvidence {
            phase: phase.into(),
            status: if phase == "validation" {
                validation_status
            } else {
                PhaseStatus::Passed
            },
            detail: detail.into(),
        })
        .collect(),
        live_context: Some(LiveExecutionContext {
            operation: Operation::Is1,
            parameter: LiveParameterContext {
                name: "personId".into(),
                data_type: "int64".into(),
                value: 1001,
            },
            fixture_sha256: LIVE_GRAPH_SHA256.into(),
            job_sha256: LIVE_JOB_SHA256.into(),
            reference_sha256: LIVE_REFERENCE_SHA256.into(),
            acquisition_sha256: LIVE_ACQUISITION_SHA256.into(),
            identity_sha256: LIVE_IDENTITY_SHA256.into(),
            public_api: "graphforge_api::GraphForge".into(),
            mode: "in_memory".into(),
            query: LIVE_IS1_QUERY.into(),
            row_schema: [
                ("firstName", "utf8"),
                ("lastName", "utf8"),
                ("birthday", "utf8"),
                ("locationIP", "utf8"),
                ("browserUsed", "utf8"),
                ("cityId", "int64"),
                ("gender", "utf8"),
                ("creationDate", "utf8"),
            ]
            .into_iter()
            .map(|(name, data_type)| LiveRowField {
                name: name.into(),
                data_type: data_type.into(),
            })
            .collect(),
            row_order: vec![
                "firstName".into(),
                "lastName".into(),
                "birthday".into(),
                "locationIP".into(),
                "browserUsed".into(),
                "cityId".into(),
                "gender".into(),
                "creationDate".into(),
            ],
            normalization: "Arrow scalar display followed by authoritative row normalization"
                .into(),
        }),
        identities,
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

    fn sample_job(operation: Operation) -> OperationJob {
        OperationJob {
            schema: JOB_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            dataset_id: BOUNDED_TINY_DATASET.into(),
            operation,
            parameters: None,
        }
    }

    #[test]
    fn all_operations_declare_a_mapping_and_validation_disposition() {
        assert_eq!(Operation::ALL.len(), 29);
        for operation in Operation::ALL {
            // Every operation maps to exactly one outcome.
            let outcome = map_operation(operation);
            match outcome {
                MappingOutcome::Compatible(mapping) => {
                    assert!(operation.validation_mode().is_some(), "{operation}");
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
    fn reads_map_to_public_api() {
        let reads = Operation::ALL.into_iter().filter(|operation| {
            matches!(
                operation.category(),
                Category::ComplexRead | Category::ShortRead
            )
        });
        for operation in reads {
            if operation == Operation::Ic14 {
                continue; // IC14 is the one honest read exception.
            }
            assert!(
                matches!(map_operation(operation), MappingOutcome::Compatible(_)),
                "{operation} should be compatible"
            );
        }
    }

    #[test]
    fn updates_fail_closed_with_typed_cause() {
        for operation in Operation::ALL {
            if operation.category() != Category::Update {
                continue;
            }
            match map_operation(operation) {
                MappingOutcome::SemanticIncompatibility { cause, .. } => {
                    assert_eq!(
                        cause, "interactive_update_stream_not_exposed",
                        "{operation}"
                    );
                }
                MappingOutcome::Compatible(_) => panic!("{operation} must fail closed"),
            }
        }
    }

    #[test]
    fn ic14_fails_closed_with_path_enumeration_cause() {
        match map_operation(Operation::Ic14) {
            MappingOutcome::SemanticIncompatibility { cause, .. } => {
                assert_eq!(cause, "weighted_interaction_path_enumeration_not_exposed");
            }
            MappingOutcome::Compatible(_) => panic!("IC14 must fail closed"),
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

    #[test]
    fn trusted_live_runner_executes_graphforge_and_binds_complete_context() {
        let evidence = run_trusted_live_is1().unwrap();
        assert_eq!(evidence.lane, EvidenceLane::LiveInMemory);
        assert_eq!(evidence.status, OperationStatus::Passed);
        assert!(!evidence.certification);
        assert!(evidence.phase_evidence.iter().all(|phase| {
            phase.status == PhaseStatus::Passed && !phase.detail.is_empty()
        }));
        let context = evidence.live_context.unwrap();
        assert_eq!(context.operation, Operation::Is1);
        assert_eq!(context.parameter.name, "personId");
        assert_eq!(context.parameter.data_type, "int64");
        assert_eq!(context.parameter.value, 1001);
        assert_eq!(context.fixture_sha256, LIVE_GRAPH_SHA256);
        assert_eq!(context.job_sha256, LIVE_JOB_SHA256);
        assert_eq!(context.reference_sha256, LIVE_REFERENCE_SHA256);
        assert_eq!(context.acquisition_sha256, LIVE_ACQUISITION_SHA256);
        assert_eq!(context.identity_sha256, LIVE_IDENTITY_SHA256);
        assert_eq!(context.public_api, "graphforge_api::GraphForge");
        assert_eq!(context.mode, "in_memory");
        assert_eq!(context.query, LIVE_IS1_QUERY);
        assert_eq!(context.row_schema.len(), 8);
        assert_eq!(context.row_order.len(), 8);
        assert_eq!(
            evidence.identities["fixture"]["classification"],
            "synthetic_engineering_fixture"
        );
        assert_eq!(
            evidence.identities["spec"]["commit"],
            "5f7956e07a214373c363b371a3b88bc83ddcd118"
        );
        assert_eq!(
            evidence.identities["generator"]["commit"],
            "2459f4e45834c78902a50511fc64a05c48dd4029"
        );
        assert_eq!(
            evidence.identities["driver"]["commit"],
            "f9c394a92cd55e535893f6c9907b141d6533c817"
        );
    }

    #[test]
    fn every_trusted_live_asset_mutation_fails_before_execution() {
        let original = embedded_live_assets();
        let mutations = [
            TrustedLiveAssets {
                graph: "{}",
                ..original
            },
            TrustedLiveAssets {
                job: "{}",
                ..original
            },
            TrustedLiveAssets {
                reference: "Ada Lovelace",
                ..original
            },
            TrustedLiveAssets {
                acquisition: "{}",
                ..original
            },
            TrustedLiveAssets {
                identity: "{}",
                ..original
            },
        ];
        for mutation in mutations {
            let error = validate_trusted_live_assets(mutation).unwrap_err();
            assert!(error.to_string().contains("checksum mismatch"));
        }
    }

    #[test]
    fn run_job_passes_compatible_read_and_reports_incompatibility() {
        let reference = parse_result_rows("1 x\n");
        let system = parse_result_rows("1 x\n");
        let read = run_job(&sample_job(Operation::Is1), Some(&reference), Some(&system));
        assert_eq!(read.status, OperationStatus::Passed);
        assert!(read.public_api.is_some());

        let update = run_job(&sample_job(Operation::Iu1), None, None);
        assert_eq!(update.status, OperationStatus::SemanticIncompatibility);
        assert!(
            update
                .cause
                .as_deref()
                .unwrap()
                .contains("interactive_update_stream_not_exposed")
        );

        let missing_output = run_job(&sample_job(Operation::Is1), Some(&reference), None);
        assert_eq!(missing_output.status, OperationStatus::Failed);
        assert_eq!(
            missing_output.cause.as_deref(),
            Some("missing_system_output")
        );
    }

    #[test]
    fn phases_are_separated_in_order() {
        assert_eq!(
            phase_names(),
            vec!["load", "warmup", "execution", "validation"]
        );
    }

    #[test]
    fn ladder_requires_tiny_dataset_first() {
        let bad = DatasetLadder {
            schema: LADDER_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            datasets: vec![
                LadderEntry {
                    id: "snb-sf1".into(),
                    order: 1,
                    role: "ladder".into(),
                },
                LadderEntry {
                    id: BOUNDED_TINY_DATASET.into(),
                    order: 2,
                    role: "fixture".into(),
                },
            ],
        };
        assert!(bad.validate().is_err());

        let good = DatasetLadder {
            schema: LADDER_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            datasets: vec![
                LadderEntry {
                    id: BOUNDED_TINY_DATASET.into(),
                    order: 1,
                    role: "engineering_fixture".into(),
                },
                LadderEntry {
                    id: "snb-sf1".into(),
                    order: 2,
                    role: "ladder".into(),
                },
            ],
        };
        good.validate().unwrap();
        assert_eq!(good.ordered_ids()[0], BOUNDED_TINY_DATASET);
    }

    #[test]
    fn evidence_status_reflects_mixed_outcomes() {
        let passed = run_job(
            &sample_job(Operation::Is1),
            Some(&parse_result_rows("1 x\n")),
            Some(&parse_result_rows("1 x\n")),
        );
        let incompatible = run_job(&sample_job(Operation::Iu1), None, None);
        let evidence = assemble_evidence(
            BOUNDED_TINY_DATASET,
            serde_json::json!({}),
            vec![passed, incompatible],
        );
        assert_eq!(evidence.status, OperationStatus::Passed);
        assert!(!evidence.certification);
        assert_eq!(evidence.lane, EvidenceLane::StaticReplay);
        assert!(
            evidence
                .phase_evidence
                .iter()
                .all(|phase| phase.status == PhaseStatus::NotExecuted)
        );
    }

    #[test]
    fn live_is1_requires_explicit_parameters_and_keeps_gaps_typed() {
        let reference = parse_result_rows("Ada Lovelace 1815-12-10 192.0.2.10 Firefox 2001 female 2026-01-02T03:04:05Z");
        let mut job = sample_job(Operation::Is1);
        job.dataset_id = LIVE_IS1_DATASET.into();
        assert_eq!(
            run_live_is1_job(&job, &reference, &reference).status,
            OperationStatus::Failed
        );
        job.parameters = Some(BTreeMap::from([(
            "personId".into(),
            serde_json::json!(1001),
        )]));
        assert_eq!(
            run_live_is1_job(&job, &reference, &reference).status,
            OperationStatus::Passed
        );
        assert!(matches!(
            map_operation(Operation::Ic14),
            MappingOutcome::SemanticIncompatibility {
                cause: "weighted_interaction_path_enumeration_not_exposed",
                ..
            }
        ));
        for operation in Operation::ALL {
            if operation.category() == Category::Update {
                assert!(matches!(
                    map_operation(operation),
                    MappingOutcome::SemanticIncompatibility {
                        cause: "interactive_update_stream_not_exposed",
                        ..
                    }
                ));
            }
        }
    }
}
