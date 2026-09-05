//! GDC FinBench Transaction suite: operation mapping, reference validation,
//! and three-lane failure evidence.
//!
//! Workload semantics live here (not in shared `gdc_contracts`). Product graph
//! behavior remains in GraphForge Rust crates; this runner only maps LDBC
//! FinBench Transaction operations onto the public Cypher / analyst-verb
//! surface, validates read outputs against pinned references, and fails closed
//! on semantics the public property-graph + Cypher surface does not expose.
//!
//! Evidence keeps three failure classes strictly separated so they are never
//! conflated: a **correctness** mismatch (system output disagrees with the
//! pinned reference), a **resource** event (the outer orchestrator reported a
//! resource limit for the operation), and a **harness** error (the runner or
//! its inputs are broken, e.g. a malformed job or a missing system output).
//! Each class has a distinct per-operation status and a dedicated top-level
//! evidence section.
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

pub const EVIDENCE_SCHEMA: &str = "graphforge-gdc-finbench-transaction-evidence/1";
pub const JOB_SCHEMA: &str = "graphforge-gdc-finbench-transaction-job/1";
pub const LADDER_SCHEMA: &str = "graphforge-gdc-finbench-transaction-ladder/1";
pub const SUITE_ID: &str = "finbench-transaction";
pub const LIVE_IDENTITY_SCHEMA: &str = "graphforge-gdc-finbench-live-identity/1";
pub const LIVE_PARAMETERS_SCHEMA: &str = "graphforge-gdc-finbench-live-parameters/1";
pub const LIVE_SEED_SCHEMA: &str = "graphforge-gdc-finbench-synthetic-seed/1";
pub const LIVE_DATASET_ID: &str = "finbench-engineering-live-tcr10-v1";
pub const VALIDATOR_INTERFACE: &str = "graphforge-finbench-rust-reference-validator/1";

/// The bounded engineering fixture dataset every ladder and suite run begins on.
pub const BOUNDED_TINY_DATASET: &str = "finbench-engineering-tiny-v1";

/// Typed cause emitted when a write / read-write transaction fails closed.
pub const WRITE_CAUSE: &str = "finbench_transaction_write_semantics_not_exposed";

/// FinBench Transaction workload category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
    ComplexRead,
    SimpleRead,
    Write,
    ReadWrite,
}

impl Category {
    pub fn name(self) -> &'static str {
        match self {
            Self::ComplexRead => "complex_read",
            Self::SimpleRead => "simple_read",
            Self::Write => "write",
            Self::ReadWrite => "read_write",
        }
    }
}

/// Reference-validation mode for a compatible read operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationMode {
    /// Ordered, row-for-row comparison (the operation has a total result order).
    Exact,
    /// Order-insensitive multiset comparison with per-row whitespace
    /// normalization (the operation's result is a set/multiset without a
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

/// LDBC FinBench Transaction operations modeled by this suite.
///
/// Complex reads (`TCR1`..`TCR12`), simple reads (`TSR1`..`TSR6`), write
/// queries (`TW1`..`TW19`), and read-write transactions (`TRW1`..`TRW3`).
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Operation {
    #[serde(rename = "TCR1")]
    Tcr1,
    #[serde(rename = "TCR2")]
    Tcr2,
    #[serde(rename = "TCR3")]
    Tcr3,
    #[serde(rename = "TCR4")]
    Tcr4,
    #[serde(rename = "TCR5")]
    Tcr5,
    #[serde(rename = "TCR6")]
    Tcr6,
    #[serde(rename = "TCR7")]
    Tcr7,
    #[serde(rename = "TCR8")]
    Tcr8,
    #[serde(rename = "TCR9")]
    Tcr9,
    #[serde(rename = "TCR10")]
    Tcr10,
    #[serde(rename = "TCR11")]
    Tcr11,
    #[serde(rename = "TCR12")]
    Tcr12,
    #[serde(rename = "TSR1")]
    Tsr1,
    #[serde(rename = "TSR2")]
    Tsr2,
    #[serde(rename = "TSR3")]
    Tsr3,
    #[serde(rename = "TSR4")]
    Tsr4,
    #[serde(rename = "TSR5")]
    Tsr5,
    #[serde(rename = "TSR6")]
    Tsr6,
    #[serde(rename = "TW1")]
    Tw1,
    #[serde(rename = "TW2")]
    Tw2,
    #[serde(rename = "TW3")]
    Tw3,
    #[serde(rename = "TW4")]
    Tw4,
    #[serde(rename = "TW5")]
    Tw5,
    #[serde(rename = "TW6")]
    Tw6,
    #[serde(rename = "TW7")]
    Tw7,
    #[serde(rename = "TW8")]
    Tw8,
    #[serde(rename = "TW9")]
    Tw9,
    #[serde(rename = "TW10")]
    Tw10,
    #[serde(rename = "TW11")]
    Tw11,
    #[serde(rename = "TW12")]
    Tw12,
    #[serde(rename = "TW13")]
    Tw13,
    #[serde(rename = "TW14")]
    Tw14,
    #[serde(rename = "TW15")]
    Tw15,
    #[serde(rename = "TW16")]
    Tw16,
    #[serde(rename = "TW17")]
    Tw17,
    #[serde(rename = "TW18")]
    Tw18,
    #[serde(rename = "TW19")]
    Tw19,
    #[serde(rename = "TRW1")]
    Trw1,
    #[serde(rename = "TRW2")]
    Trw2,
    #[serde(rename = "TRW3")]
    Trw3,
}

impl Operation {
    pub const ALL: [Self; 40] = [
        Self::Tcr1,
        Self::Tcr2,
        Self::Tcr3,
        Self::Tcr4,
        Self::Tcr5,
        Self::Tcr6,
        Self::Tcr7,
        Self::Tcr8,
        Self::Tcr9,
        Self::Tcr10,
        Self::Tcr11,
        Self::Tcr12,
        Self::Tsr1,
        Self::Tsr2,
        Self::Tsr3,
        Self::Tsr4,
        Self::Tsr5,
        Self::Tsr6,
        Self::Tw1,
        Self::Tw2,
        Self::Tw3,
        Self::Tw4,
        Self::Tw5,
        Self::Tw6,
        Self::Tw7,
        Self::Tw8,
        Self::Tw9,
        Self::Tw10,
        Self::Tw11,
        Self::Tw12,
        Self::Tw13,
        Self::Tw14,
        Self::Tw15,
        Self::Tw16,
        Self::Tw17,
        Self::Tw18,
        Self::Tw19,
        Self::Trw1,
        Self::Trw2,
        Self::Trw3,
    ];

    pub fn code(self) -> &'static str {
        match self {
            Self::Tcr1 => "TCR1",
            Self::Tcr2 => "TCR2",
            Self::Tcr3 => "TCR3",
            Self::Tcr4 => "TCR4",
            Self::Tcr5 => "TCR5",
            Self::Tcr6 => "TCR6",
            Self::Tcr7 => "TCR7",
            Self::Tcr8 => "TCR8",
            Self::Tcr9 => "TCR9",
            Self::Tcr10 => "TCR10",
            Self::Tcr11 => "TCR11",
            Self::Tcr12 => "TCR12",
            Self::Tsr1 => "TSR1",
            Self::Tsr2 => "TSR2",
            Self::Tsr3 => "TSR3",
            Self::Tsr4 => "TSR4",
            Self::Tsr5 => "TSR5",
            Self::Tsr6 => "TSR6",
            Self::Tw1 => "TW1",
            Self::Tw2 => "TW2",
            Self::Tw3 => "TW3",
            Self::Tw4 => "TW4",
            Self::Tw5 => "TW5",
            Self::Tw6 => "TW6",
            Self::Tw7 => "TW7",
            Self::Tw8 => "TW8",
            Self::Tw9 => "TW9",
            Self::Tw10 => "TW10",
            Self::Tw11 => "TW11",
            Self::Tw12 => "TW12",
            Self::Tw13 => "TW13",
            Self::Tw14 => "TW14",
            Self::Tw15 => "TW15",
            Self::Tw16 => "TW16",
            Self::Tw17 => "TW17",
            Self::Tw18 => "TW18",
            Self::Tw19 => "TW19",
            Self::Trw1 => "TRW1",
            Self::Trw2 => "TRW2",
            Self::Trw3 => "TRW3",
        }
    }

    pub fn category(self) -> Category {
        match self {
            Self::Tcr1
            | Self::Tcr2
            | Self::Tcr3
            | Self::Tcr4
            | Self::Tcr5
            | Self::Tcr6
            | Self::Tcr7
            | Self::Tcr8
            | Self::Tcr9
            | Self::Tcr10
            | Self::Tcr11
            | Self::Tcr12 => Category::ComplexRead,
            Self::Tsr1 | Self::Tsr2 | Self::Tsr3 | Self::Tsr4 | Self::Tsr5 | Self::Tsr6 => {
                Category::SimpleRead
            }
            Self::Tw1
            | Self::Tw2
            | Self::Tw3
            | Self::Tw4
            | Self::Tw5
            | Self::Tw6
            | Self::Tw7
            | Self::Tw8
            | Self::Tw9
            | Self::Tw10
            | Self::Tw11
            | Self::Tw12
            | Self::Tw13
            | Self::Tw14
            | Self::Tw15
            | Self::Tw16
            | Self::Tw17
            | Self::Tw18
            | Self::Tw19 => Category::Write,
            Self::Trw1 | Self::Trw2 | Self::Trw3 => Category::ReadWrite,
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
    /// spec-mandated total order use `Exact`; ratio/similarity aggregations
    /// whose ties are not totally ordered use `Normalized`.
    fn intended_validation(self) -> ValidationMode {
        match self {
            Self::Tcr7 | Self::Tcr9 | Self::Tcr10 => ValidationMode::Normalized,
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
                SuiteError::InvalidDocument(format!(
                    "unknown FinBench Transaction operation: {value}"
                ))
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
    pub result_schema: Vec<String>,
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
    pub pid1: Int64Binding,
    pub pid2: Int64Binding,
    #[serde(rename = "startTime")]
    pub start_time: Int64Binding,
    #[serde(rename = "endTime")]
    pub end_time: Int64Binding,
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
    persons: Vec<i64>,
    companies: Vec<i64>,
    invests: Vec<LiveInvest>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LiveInvest {
    person: i64,
    company: i64,
    timestamp: i64,
}

struct ValidatedLiveContext {
    identity: LiveIdentity,
    parameters: LiveParameters,
    seed: LiveSeed,
    reference: ResultRows,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ValidatorEvidence {
    pub interface: String,
    pub reference_derivation: String,
}

/// Explicit validator boundary shared by static and live lanes.
pub trait ResultValidator {
    fn interface(&self) -> &'static str;
    fn validate(
        &self,
        mode: ValidationMode,
        reference: &ResultRows,
        system: &ResultRows,
    ) -> Result<(), SuiteError>;
}

/// Existing Rust normalizer/reference comparator exposed as an interface.
pub struct RustReferenceValidator;

impl ResultValidator for RustReferenceValidator {
    fn interface(&self) -> &'static str {
        VALIDATOR_INTERFACE
    }

    fn validate(
        &self,
        mode: ValidationMode,
        reference: &ResultRows,
        system: &ResultRows,
    ) -> Result<(), SuiteError> {
        validate_result(mode, reference, system)
    }
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
                "job suite_id must be finbench-transaction".into(),
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
                "ladder suite_id must be finbench-transaction".into(),
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

/// Per-operation outcome status. The three failure classes are distinct enum
/// variants so a correctness mismatch, a resource-limit event, and a
/// harness/runner error are never conflated into one bucket.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Passed,
    /// Correctness lane: the system output disagreed with the reference.
    CorrectnessFailed,
    /// Resource lane: the outer orchestrator reported a resource-limit event.
    ResourceExceeded,
    /// Harness lane: the runner or its inputs were broken (not a workload result).
    HarnessError,
    /// The public surface does not expose this operation's semantics.
    SemanticIncompatibility,
}

impl OperationStatus {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::CorrectnessFailed => "correctness_failed",
            Self::ResourceExceeded => "resource_exceeded",
            Self::HarnessError => "harness_error",
            Self::SemanticIncompatibility => "semantic_incompatibility",
        }
    }
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

/// A resource-limit observation reported by the outer orchestrator for an
/// operation. Recorded in its own evidence section, distinct from correctness.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourceEvent {
    pub operation: Operation,
    pub phase: String,
    pub detail: String,
}

/// A harness/runner failure for an operation (malformed job, missing system
/// output, unreadable input). Recorded in its own evidence section, distinct
/// from both correctness and resource evidence.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct HarnessFailure {
    pub operation: Operation,
    pub phase: String,
    pub detail: String,
}

/// The signal the outer orchestrator hands the runner for a compatible read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionSignal {
    /// The system produced result rows to validate against the reference.
    Produced(ResultRows),
    /// The outer orchestrator reported a resource-limit event for this op.
    ResourceLimit(String),
    /// The runner/harness could not execute the op (broken input/environment).
    HarnessError(String),
}

/// FinBench Transaction lifecycle phases, kept explicitly separate.
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
    pub status: OperationStatus,
    /// Engineering evidence flag: never an audited GDC certification.
    pub certification: bool,
    pub execution_mode: String,
    pub validator: ValidatorEvidence,
    pub phases: Vec<String>,
    pub identities: serde_json::Value,
    /// Per-operation correctness / semantic outcomes.
    pub operations: Vec<OperationOutcome>,
    /// Resource-limit events, isolated from correctness evidence.
    pub resource_events: Vec<ResourceEvent>,
    /// Harness/runner failures, isolated from correctness and resource evidence.
    pub harness_failures: Vec<HarnessFailure>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SuiteError {
    InvalidDocument(String),
    LiveExecution {
        phase: String,
        resource: bool,
        detail: String,
    },
    ReferenceMismatch(String),
    SemanticIncompatibility {
        cause: String,
        detail: String,
    },
}

impl fmt::Display for SuiteError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LiveExecution {
                phase,
                resource,
                detail,
            } => write!(
                output,
                "live_execution:phase={phase}:resource={resource}: {detail}"
            ),
            Self::InvalidDocument(message) => write!(output, "invalid_document: {message}"),
            Self::ReferenceMismatch(message) => write!(output, "reference_mismatch: {message}"),
            Self::SemanticIncompatibility { cause, detail } => {
                write!(output, "semantic_incompatibility:{cause}: {detail}")
            }
        }
    }
}

impl std::error::Error for SuiteError {}

/// Map a FinBench Transaction operation onto the public GraphForge surface.
///
/// Read-only complex/simple reads that are ordinary graph traversals,
/// temporal-window filters, aggregations, or top-k map to public Cypher.
/// Operations that require semantics the public property-graph + Cypher surface
/// does not expose fail closed with a typed cause instead of silently
/// approximating:
///
/// * recursive temporal path filtering (paths whose transfer timestamps must be
///   monotonically ordered) — TCR1, TCR2;
/// * temporally filtered shortest transfer path — TCR3;
/// * transfer-cycle detection under temporal constraints — TCR4;
/// * hub-vertex truncation (native truncation-limit ordering by timestamp
///   windows that changes the reference result) — TCR5;
/// * write and read-write transaction semantics (ACID transactions, insert /
///   delete / in-place update streams, truncation, and read-before-write risk
///   checks) — TW1..TW19, TRW1..TRW3.
pub fn map_operation(operation: Operation) -> MappingOutcome {
    match operation {
        Operation::Tcr1 => recursive_path_incompatible(
            "TCR1 traces downstream transfer paths whose transfer timestamps must be strictly \
             increasing along the path",
        ),
        Operation::Tcr2 => recursive_path_incompatible(
            "TCR2 traces the fund-flow paths reaching an account with transfer timestamps ordered \
             monotonically along each path",
        ),
        Operation::Tcr3 => MappingOutcome::SemanticIncompatibility {
            cause: "temporal_shortest_transfer_path_not_exposed",
            detail: "TCR3 computes the shortest transfer path length between two accounts using \
                     only transfer edges inside a [startTime,endTime] window; the public bfs path \
                     verb runs over the whole graph and cannot restrict shortest-path search to a \
                     per-edge temporal predicate"
                .into(),
        },
        Operation::Tcr4 => MappingOutcome::SemanticIncompatibility {
            cause: "temporal_transfer_cycle_detection_not_exposed",
            detail:
                "TCR4 detects transfer cycles whose edges satisfy temporal ordering constraints \
                     within a time window; the public surface exposes pattern matching and single \
                     path verbs but not temporally constrained cycle enumeration"
                    .into(),
        },
        Operation::Tcr5 => MappingOutcome::SemanticIncompatibility {
            cause: "hub_vertex_truncation_not_exposed",
            detail: "TCR5's reference result depends on FinBench native hub-vertex truncation \
                     (truncationLimit edges kept per vertex ordered by timestamp window); public \
                     Cypher has no mid-traversal truncation operator, so the untruncated result \
                     would silently disagree with the reference"
                .into(),
        },
        Operation::Tcr6 => compatible(
            "cypher",
            "MATCH (src:Account {id:$id})-[t:transfer]->(mid:Account)-[w:withdraw]->(dst:Account) \
             WHERE t.createTime>=$start AND t.createTime<$end RETURN dst.id, sum(w.amount) AS amount \
             ORDER BY amount DESC, dst.id LIMIT $topk",
            "two-hop transfer-then-withdraw neighborhood with temporal filter and ordered top-k",
        ),
        Operation::Tcr7 => compatible(
            "cypher",
            "MATCH (src:Account {id:$id})<-[in:transfer]-(a:Account) \
             WHERE in.createTime>=$start AND in.createTime<$end \
             OPTIONAL MATCH (src)-[out:transfer]->(b:Account) \
             RETURN count(DISTINCT a) AS senders, sum(in.amount) AS in_amt, sum(out.amount) AS out_amt",
            "transfer-in versus transfer-out aggregate ratio over a time window; unordered ratio set (normalized validation)",
        ),
        Operation::Tcr8 => compatible(
            "cypher",
            "MATCH (loan:Loan {id:$id})-[d:deposit]->(a:Account)-[t:transfer*1..3]->(dst:Account) \
             WHERE ALL(e IN t WHERE e.createTime>=$start AND e.createTime<$end) \
             RETURN dst.id, count(*) AS hops ORDER BY hops DESC, dst.id LIMIT $topk",
            "loan-money bounded multi-hop transfer reachability with a per-edge time-window filter and ordered top-k",
        ),
        Operation::Tcr9 => compatible(
            "cypher",
            "MATCH (a:Account {id:$id}) \
             OPTIONAL MATCH (a)-[r:repay]->(:Loan) WHERE r.createTime>=$start AND r.createTime<$end \
             OPTIONAL MATCH (a)<-[dep:deposit]-(:Loan) WHERE dep.createTime>=$start AND dep.createTime<$end \
             RETURN sum(r.amount) AS repaid, sum(dep.amount) AS received",
            "repay-to-deposit ratio aggregation over a time window; unordered ratio set (normalized validation)",
        ),
        Operation::Tcr10 => compatible(
            "cypher",
            LIVE_TCR10_QUERY,
            "official TCR10 investor-relationship Jaccard with open startTime < timestamp < endTime \
             window and 3-decimal rounding; single-row similarity (normalized validation)",
        ),
        Operation::Tcr11 => compatible(
            "cypher",
            "MATCH (a:Account {id:$id})-[g:guarantee*1..3]->(other:Account)-[:apply]->(loan:Loan) \
             WHERE ALL(e IN g WHERE e.createTime>=$start AND e.createTime<$end) \
             RETURN sum(loan.amount) AS total, count(DISTINCT loan) AS loans",
            "guarantee-chain bounded traversal summing downstream loan exposure with a time-window filter",
        ),
        Operation::Tcr12 => compatible(
            "cypher",
            "MATCH (p:Person {id:$id})-[:own]->(a:Account)-[t:transfer]->(:Account)<-[:own]-(c:Company) \
             WHERE t.createTime>=$start AND t.createTime<$end \
             RETURN c.id, sum(t.amount) AS amount ORDER BY amount DESC, c.id LIMIT $topk",
            "person-to-company transfer aggregation over owned accounts with temporal filter and ordered top-k",
        ),
        Operation::Tsr1 => compatible(
            "cypher",
            "MATCH (a:Account {id:$id}) RETURN a.id, a.type, a.nickname, a.isBlocked, a.createTime",
            "single-account property lookup by id",
        ),
        Operation::Tsr2 => compatible(
            "cypher",
            "MATCH (a:Account {id:$id})-[t:transfer]->(dst:Account) \
             WHERE t.createTime>=$start AND t.createTime<$end \
             RETURN count(t) AS edges, max(t.amount) AS max_amount, sum(t.amount) AS sum_amount",
            "one-hop outgoing transfer summary for an account within a time window",
        ),
        Operation::Tsr3 => compatible(
            "cypher",
            "MATCH (a:Account {id:$id})<-[t:transfer]-(src:Account) \
             WHERE t.createTime>=$start AND t.createTime<$end \
             RETURN count(t) AS edges, max(t.amount) AS max_amount, sum(t.amount) AS sum_amount",
            "one-hop incoming transfer summary for an account within a time window",
        ),
        Operation::Tsr4 => compatible(
            "cypher",
            "MATCH (src:Account {id:$id})-[t:transfer]->(dst:Account {id:$dst}) \
             RETURN count(t) AS edges, sum(t.amount) AS sum_amount ORDER BY edges DESC",
            "transfer messages between a fixed source and destination account pair",
        ),
        Operation::Tsr5 => compatible(
            "cypher",
            "MATCH (src:Account {id:$src})-[t:transfer]->(dst:Account {id:$id}) \
             RETURN count(t) AS edges, sum(t.amount) AS sum_amount ORDER BY edges DESC",
            "transfer messages received by a fixed destination account from a source",
        ),
        Operation::Tsr6 => compatible(
            "cypher",
            "MATCH (a:Account {id:$id})-[:own]-(p:Person) RETURN p.id, p.name, p.isBlocked",
            "owner lookup for an account",
        ),
        Operation::Tw1 => write_incompatible("TW1 inserts a Person vertex with its properties"),
        Operation::Tw2 => write_incompatible("TW2 inserts a Company vertex with its properties"),
        Operation::Tw3 => write_incompatible("TW3 inserts a Medium vertex with its properties"),
        Operation::Tw4 => write_incompatible("TW4 inserts an Account owned by a Person"),
        Operation::Tw5 => write_incompatible("TW5 inserts an Account owned by a Company"),
        Operation::Tw6 => write_incompatible("TW6 inserts a Loan applied for by a Person"),
        Operation::Tw7 => write_incompatible("TW7 inserts a Loan applied for by a Company"),
        Operation::Tw8 => write_incompatible("TW8 inserts a Person-invest-Company edge"),
        Operation::Tw9 => write_incompatible("TW9 inserts a Company-invest-Company edge"),
        Operation::Tw10 => write_incompatible("TW10 inserts a Person-guarantee-Person edge"),
        Operation::Tw11 => write_incompatible("TW11 inserts a Company-guarantee-Company edge"),
        Operation::Tw12 => write_incompatible("TW12 inserts an Account-transfer-Account edge"),
        Operation::Tw13 => write_incompatible("TW13 inserts an Account-withdraw-Account edge"),
        Operation::Tw14 => write_incompatible("TW14 inserts an Account-repay-Loan edge"),
        Operation::Tw15 => write_incompatible("TW15 inserts a Loan-deposit-Account edge"),
        Operation::Tw16 => write_incompatible("TW16 inserts a Person/Company-signIn-Medium edge"),
        Operation::Tw17 => {
            write_incompatible("TW17 deletes an Account and all of its adjacent edges")
        }
        Operation::Tw18 => {
            write_incompatible("TW18 marks an Account as blocked (in-place state update)")
        }
        Operation::Tw19 => {
            write_incompatible("TW19 marks a Person as blocked (in-place state update)")
        }
        Operation::Trw1 => write_incompatible(
            "TRW1 checks a risky transfer pattern and, only if the check passes, writes the transfer \
             inside one transaction",
        ),
        Operation::Trw2 => write_incompatible(
            "TRW2 checks a guarantee-and-loan risk pattern before conditionally writing edges inside \
             one transaction",
        ),
        Operation::Trw3 => write_incompatible(
            "TRW3 checks a transfer-cycle risk pattern before conditionally writing the transfer \
             inside one transaction",
        ),
    }
}

fn compatible(interface: &str, cypher_shape: &str, notes: &str) -> MappingOutcome {
    MappingOutcome::Compatible(PublicApiMapping {
        interface: interface.into(),
        cypher_shape: cypher_shape.into(),
        notes: notes.into(),
    })
}

fn recursive_path_incompatible(detail: &str) -> MappingOutcome {
    MappingOutcome::SemanticIncompatibility {
        cause: "recursive_temporal_path_filtering_not_exposed",
        detail: format!(
            "{detail}; standard Cypher variable-length matching cannot enforce a monotonically \
             increasing per-edge timestamp constraint along the traversed path (FinBench recursive \
             path filtering choke point), so the public surface cannot reproduce the reference result"
        ),
    }
}

fn write_incompatible(detail: &str) -> MappingOutcome {
    MappingOutcome::SemanticIncompatibility {
        cause: WRITE_CAUSE,
        detail: format!(
            "{detail}; FinBench write and read-write transactions require the official driver's ACID \
             transaction, insert/delete/in-place-update stream, truncation, and read-before-write risk \
             semantics, none of which the public property-graph + Cypher surface exposes"
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

fn outcome(
    operation: Operation,
    status: OperationStatus,
    validation_mode: &str,
    cause: Option<String>,
    public_api: Option<PublicApiMapping>,
) -> OperationOutcome {
    OperationOutcome {
        operation,
        category: operation.category().name().to_string(),
        status,
        validation_mode: validation_mode.to_string(),
        cause,
        public_api,
    }
}

/// Evaluate one operation into exactly one lane (correctness / resource /
/// harness / semantic). `signal` is only consulted for compatible reads.
pub fn run_job(
    job: &OperationJob,
    reference: Option<&ResultRows>,
    signal: Option<ExecutionSignal>,
) -> OperationOutcome {
    let operation = job.operation;
    if let Err(error) = job.validate_schema() {
        // A malformed job document is a harness/runner failure, never a
        // correctness result.
        return outcome(
            operation,
            OperationStatus::HarnessError,
            "none",
            Some(error.to_string()),
            None,
        );
    }
    match map_operation(operation) {
        MappingOutcome::SemanticIncompatibility { cause, detail } => outcome(
            operation,
            OperationStatus::SemanticIncompatibility,
            "none",
            Some(format!("{cause}: {detail}")),
            None,
        ),
        MappingOutcome::Compatible(public_api) => {
            let mode = operation.intended_validation();
            let Some(reference) = reference else {
                return outcome(
                    operation,
                    OperationStatus::HarnessError,
                    mode.name(),
                    Some("missing_reference".into()),
                    Some(public_api),
                );
            };
            match signal {
                None => outcome(
                    operation,
                    OperationStatus::HarnessError,
                    mode.name(),
                    Some("missing_system_output".into()),
                    Some(public_api),
                ),
                Some(ExecutionSignal::HarnessError(detail)) => outcome(
                    operation,
                    OperationStatus::HarnessError,
                    mode.name(),
                    Some(detail),
                    Some(public_api),
                ),
                Some(ExecutionSignal::ResourceLimit(detail)) => outcome(
                    operation,
                    OperationStatus::ResourceExceeded,
                    mode.name(),
                    Some(detail),
                    Some(public_api),
                ),
                Some(ExecutionSignal::Produced(system)) => {
                    match validate_result(mode, reference, &system) {
                        Ok(()) => outcome(
                            operation,
                            OperationStatus::Passed,
                            mode.name(),
                            None,
                            Some(public_api),
                        ),
                        Err(error) => outcome(
                            operation,
                            OperationStatus::CorrectnessFailed,
                            mode.name(),
                            Some(error.to_string()),
                            Some(public_api),
                        ),
                    }
                }
            }
        }
    }
}

const LIVE_TCR10_QUERY: &str = "\
MATCH (p1:Person {id:$pid1}), (p2:Person {id:$pid2}) \
OPTIONAL MATCH (p1)-[e1:invest]->(c1:Company) \
WHERE $startTime < e1.timestamp AND e1.timestamp < $endTime \
WITH p1, p2, count(DISTINCT c1) AS p1Neighbors \
OPTIONAL MATCH (p2)-[e2:invest]->(c2:Company) \
WHERE $startTime < e2.timestamp AND e2.timestamp < $endTime \
WITH p1, p2, p1Neighbors, count(DISTINCT c2) AS p2Neighbors \
OPTIONAL MATCH (p1)-[e3:invest]->(mid:Company)<-[e4:invest]-(p2) \
WHERE $startTime < e3.timestamp AND e3.timestamp < $endTime \
AND $startTime < e4.timestamp AND e4.timestamp < $endTime \
WITH p1Neighbors, p2Neighbors, count(DISTINCT mid) AS intersection \
WITH intersection, p1Neighbors + p2Neighbors - intersection AS unionSize \
RETURN CASE WHEN unionSize = 0 THEN 0.0 ELSE round(toFloat(intersection) / toFloat(unionSize) * 1000) / 1000 END AS jaccardSimilarity";

fn expected_live_identity() -> LiveIdentity {
    LiveIdentity {
        schema: LIVE_IDENTITY_SCHEMA.into(),
        suite_id: SUITE_ID.into(),
        operation: Operation::Tcr10,
        source_mode: "runner_owned_rust_api".into(),
        certification: false,
        upstream_spec: UpstreamIdentity {
            source: "https://github.com/ldbc/ldbc_finbench_docs".into(),
            release: "v0.1.0".into(),
            commit: "d3ec7036bf6919df8cd3eeaa3a986048e779ea02".into(),
        },
        upstream_query: UpstreamQueryIdentity {
            source: "https://github.com/ldbc/ldbc_finbench_transaction_impls".into(),
            release: "galaxybase-cypher".into(),
            commit: "4891ab426377ec98564822d54bdadb1d02e5fe41".into(),
            path: "galaxybase-cypher/queries/transaction-complex-read-10.cypher".into(),
        },
        fixture: LiveFixtureIdentity {
            dataset_id: LIVE_DATASET_ID.into(),
            kind: "synthetic_minimal_finbench_shaped".into(),
            provenance_kind: "content_addressed_committed_fixture".into(),
            seed: 964,
            scale_factor: None,
            path: "seed.json".into(),
            sha256: "3360c7722b55bb9b60e242fa9c7d7c8b2beb32503892a2402ed606e6b0c50117".into(),
        },
        parameters: LiveParametersIdentity {
            kind: "typed_internal_deterministic_tcr10_parameters".into(),
            path: "parameters.json".into(),
            file_sha256: "87dbdbf8e6a16039e86efd00042509af055c519fdad285039b2b09a52db75364".into(),
            names_kinds_values_sha256:
                "46e5bf9bf7d961ecf8f97a6d0e31e2bcbc82a5f4c278aea31cbb4fdb1c431af7".into(),
        },
        reference: LiveReferenceIdentity {
            authority: "independent_semantic_derivation_from_seed".into(),
            captured_from_engine: false,
            validation: "normalized_rounded_jaccard".into(),
            path: "expected-tcr10.ref".into(),
            sha256: "40b8483eaba45fe3d11f5a68004d1ce712d6e4340e5cfbe1a916337073cc68a0".into(),
        },
        driver: LiveDriverIdentity {
            kind: "internal_rust_public_api_driver".into(),
            source_mode: "runner_executes_query_no_caller_result".into(),
            interface: "graphforge_api::GraphForge::execute_with_params".into(),
            durable: false,
            validator: "same_process_normalized".into(),
        },
        normalization: LiveNormalizationIdentity {
            upstream_temporal_window: "startTime < invest.timestamp < endTime".into(),
            internal_equivalent: "precomputed integer timestamps with the same open window".into(),
            reason: "exercise supported Cypher while not claiming DateTime-literal parity".into(),
            result_schema: vec!["jaccardSimilarity".into()],
            numeric_format: "float64_rounded_3dp".into(),
        },
    }
}

fn expected_live_parameters() -> LiveParameters {
    LiveParameters {
        schema: LIVE_PARAMETERS_SCHEMA.into(),
        operation: Operation::Tcr10,
        bindings: LiveBindings {
            pid1: Int64Binding {
                kind: "int64".into(),
                value: 1,
            },
            pid2: Int64Binding {
                kind: "int64".into(),
                value: 2,
            },
            start_time: Int64Binding {
                kind: "int64".into(),
                value: 100,
            },
            end_time: Int64Binding {
                kind: "int64".into(),
                value: 200,
            },
        },
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
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
    if seed.schema != LIVE_SEED_SCHEMA || seed.seed != identity.fixture.seed {
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
    if parameters.schema != LIVE_PARAMETERS_SCHEMA || parameters.operation != Operation::Tcr10 {
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
    if parameters != expected_live_parameters() {
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
    let independent = independently_derived_jaccard(&seed, &parameters.bindings);
    if reference != vec![independent.clone()] {
        return Err(SuiteError::InvalidDocument(format!(
            "live_reference_not_independently_derived: expected {independent} from seed"
        )));
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

/// Official TCR10 Jaccard from the committed seed, never from engine output.
fn independently_derived_jaccard(seed: &LiveSeed, bindings: &LiveBindings) -> String {
    let window_start = bindings.start_time.value;
    let window_end = bindings.end_time.value;
    let neighborhood = |person: i64| -> BTreeSet<i64> {
        seed.invests
            .iter()
            .filter(|edge| {
                edge.person == person
                    && window_start < edge.timestamp
                    && edge.timestamp < window_end
            })
            .map(|edge| edge.company)
            .collect()
    };
    let left = neighborhood(bindings.pid1.value);
    let right = neighborhood(bindings.pid2.value);
    let intersection = left.intersection(&right).count();
    let union = left.union(&right).count();
    let similarity = if union == 0 {
        0.0
    } else {
        ((intersection as f64) / (union as f64) * 1000.0).round() / 1000.0
    };
    format!("{similarity:.3}")
}

fn api_error(context: &str, error: graphforge_api::GfError) -> SuiteError {
    SuiteError::LiveExecution {
        phase: context.into(),
        resource: error.code() == "GF_RESOURCE_LIMIT",
        detail: error.to_string(),
    }
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

    for person in &seed.persons {
        execute_with_params(
            forge,
            "CREATE (:Person {id: $id})",
            HashMap::from([("id".into(), IrLiteral::Int(*person))]),
        )?;
    }
    for company in &seed.companies {
        execute_with_params(
            forge,
            "CREATE (:Company {id: $id})",
            HashMap::from([("id".into(), IrLiteral::Int(*company))]),
        )?;
    }
    for edge in &seed.invests {
        execute_with_params(
            forge,
            "MATCH (person:Person {id: $person}), (company:Company {id: $company}) \
             CREATE (person)-[:invest {timestamp: $timestamp}]->(company)",
            HashMap::from([
                ("person".into(), IrLiteral::Int(edge.person)),
                ("company".into(), IrLiteral::Int(edge.company)),
                ("timestamp".into(), IrLiteral::Int(edge.timestamp)),
            ]),
        )?;
    }
    Ok((seed.persons.len() + seed.companies.len() + seed.invests.len()) as u64)
}

fn execute_live_tcr10(
    forge: &graphforge_api::GraphForge,
    bindings: &LiveBindings,
) -> Result<ResultRows, SuiteError> {
    use arrow::array::{Array, Float64Array};
    use graphforge_api::IrLiteral;
    use std::collections::HashMap;

    let params = HashMap::from([
        ("pid1".into(), IrLiteral::Int(bindings.pid1.value)),
        ("pid2".into(), IrLiteral::Int(bindings.pid2.value)),
        (
            "startTime".into(),
            IrLiteral::Int(bindings.start_time.value),
        ),
        ("endTime".into(), IrLiteral::Int(bindings.end_time.value)),
    ]);
    let result = forge
        .execute_with_params(LIVE_TCR10_QUERY, &params)
        .map_err(|error| api_error("execution", error))?;
    if result.schema.fields().len() != 1 || result.schema.field(0).name() != "jaccardSimilarity" {
        return Err(SuiteError::InvalidDocument(format!(
            "TCR10 result schema drifted: expected [jaccardSimilarity], got {:?}",
            result
                .schema
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>()
        )));
    }
    let mut rows = Vec::new();
    for batch in result.batches {
        let similarities = batch
            .column_by_name("jaccardSimilarity")
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                SuiteError::InvalidDocument("TCR10 jaccardSimilarity is not Float64".into())
            })?;
        for row in 0..batch.num_rows() {
            if similarities.is_null(row) {
                return Err(SuiteError::InvalidDocument(
                    "TCR10 returned a null jaccardSimilarity".into(),
                ));
            }
            rows.push(format!("{:.3}", similarities.value(row)));
        }
    }
    Ok(rows)
}

fn live_identities(identity: &LiveIdentity, executable_sha256: String) -> serde_json::Value {
    serde_json::json!({
        "spec": identity.upstream_spec,
        "generator": {
            "name": "finbench-datagen",
            "source": "https://github.com/ldbc/ldbc_finbench_datagen",
            "release": "0.1.0",
            "commit": "eddcc0551861eaefeb9b37497b10de1bb0f52672"
        },
        "driver": identity.driver,
        "live": identity,
        "execution_authority": {
            "interface": "graphforge_api::GraphForge::execute_with_params",
            "runner_executable_sha256": executable_sha256,
            "caller_supplied_result": false
        }
    })
}

fn live_tcr10_outcome(
    reference: &ResultRows,
    rows: Result<ResultRows, SuiteError>,
    mapping: PublicApiMapping,
) -> OperationOutcome {
    let validation = rows.and_then(|rows| {
        RustReferenceValidator.validate(ValidationMode::Normalized, reference, &rows)
    });
    let status = match &validation {
        Ok(()) => OperationStatus::Passed,
        Err(SuiteError::ReferenceMismatch(_)) => OperationStatus::CorrectnessFailed,
        Err(SuiteError::LiveExecution { resource: true, .. }) => OperationStatus::ResourceExceeded,
        Err(_) => OperationStatus::HarnessError,
    };
    outcome(
        Operation::Tcr10,
        status,
        ValidationMode::Normalized.name(),
        validation.err().map(|error| error.to_string()),
        Some(mapping),
    )
}

pub fn run_live_fixture(fixture: &Path, executable: &Path) -> Result<SuiteEvidence, SuiteError> {
    if fixture.is_file() {
        return Err(SuiteError::InvalidDocument(
            "static output rejected: live lane requires a fixture directory, not a result envelope"
                .into(),
        ));
    }
    let context = validate_live_context(fixture)?;
    let executable_sha256 = sha256_bytes(&read_bytes(executable, "runner executable")?);
    let rows = (|| {
        let forge =
            graphforge_api::GraphForge::new(None).map_err(|error| api_error("load", error))?;
        let load_started = Instant::now();
        let _rows_loaded = load_live_seed(&forge, &context.seed)?;
        let _load_ms = u64::try_from(load_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let query_started = Instant::now();
        let rows = execute_live_tcr10(&forge, &context.parameters.bindings);
        let _query_ms = u64::try_from(query_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        rows
    })();

    let MappingOutcome::Compatible(mapping) = map_operation(Operation::Tcr10) else {
        return Err(SuiteError::InvalidDocument(
            "live operation is not mapped to the public API".into(),
        ));
    };
    if mapping.cypher_shape != LIVE_TCR10_QUERY {
        return Err(SuiteError::InvalidDocument(
            "live query drifted from the pinned public-API mapping".into(),
        ));
    }

    let validator = RustReferenceValidator;
    let failure_phase = match &rows {
        Err(SuiteError::LiveExecution { phase, .. }) => Some(phase.clone()),
        Err(_) => Some("execution".into()),
        Ok(_) => None,
    };
    let tcr10 = live_tcr10_outcome(&context.reference, rows, mapping);
    let tcr1 = run_job(
        &OperationJob {
            schema: JOB_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            dataset_id: LIVE_DATASET_ID.into(),
            operation: Operation::Tcr1,
        },
        None,
        None,
    );
    let tw1 = run_job(
        &OperationJob {
            schema: JOB_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            dataset_id: LIVE_DATASET_ID.into(),
            operation: Operation::Tw1,
        },
        None,
        None,
    );
    let mut evidence = assemble_evidence_with_context(
        LIVE_DATASET_ID,
        live_identities(&context.identity, executable_sha256),
        vec![tcr10, tcr1, tw1],
        "live_graphforge",
        ValidatorEvidence {
            interface: validator.interface().into(),
            reference_derivation: context.identity.reference.authority.clone(),
        },
    );
    if let Some(phase) = failure_phase {
        for event in &mut evidence.resource_events {
            event.phase = phase.clone();
        }
        for failure in &mut evidence.harness_failures {
            failure.phase = phase.clone();
        }
    }
    Ok(evidence)
}

/// Aggregate per-operation outcomes into the suite evidence, projecting the
/// resource and harness lanes into their own dedicated sections. The top-level
/// status keeps the lanes separate by strict precedence: a harness error (the
/// evidence itself is untrustworthy) outranks a correctness failure, which
/// outranks a resource-limit event; an all-semantic run reports
/// `semantic_incompatibility`; otherwise the run passed.
pub fn assemble_evidence(
    dataset_id: &str,
    identities: serde_json::Value,
    outcomes: Vec<OperationOutcome>,
) -> SuiteEvidence {
    assemble_evidence_with_context(
        dataset_id,
        identities,
        outcomes,
        "static_replay",
        ValidatorEvidence {
            interface: VALIDATOR_INTERFACE.into(),
            reference_derivation: "committed_reference".into(),
        },
    )
}

pub fn assemble_evidence_with_context(
    dataset_id: &str,
    identities: serde_json::Value,
    outcomes: Vec<OperationOutcome>,
    execution_mode: &str,
    validator: ValidatorEvidence,
) -> SuiteEvidence {
    let resource_events = outcomes
        .iter()
        .filter(|item| item.status == OperationStatus::ResourceExceeded)
        .map(|item| ResourceEvent {
            operation: item.operation,
            phase: Phase::Execution.name().to_string(),
            detail: item.cause.clone().unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    let harness_failures = outcomes
        .iter()
        .filter(|item| item.status == OperationStatus::HarnessError)
        .map(|item| HarnessFailure {
            operation: item.operation,
            phase: Phase::Validation.name().to_string(),
            detail: item.cause.clone().unwrap_or_default(),
        })
        .collect::<Vec<_>>();

    let has = |status: OperationStatus| outcomes.iter().any(|item| item.status == status);
    let status = if has(OperationStatus::HarnessError) {
        OperationStatus::HarnessError
    } else if has(OperationStatus::CorrectnessFailed) {
        OperationStatus::CorrectnessFailed
    } else if has(OperationStatus::ResourceExceeded) {
        OperationStatus::ResourceExceeded
    } else if outcomes
        .iter()
        .all(|item| item.status == OperationStatus::SemanticIncompatibility)
    {
        OperationStatus::SemanticIncompatibility
    } else {
        OperationStatus::Passed
    };

    SuiteEvidence {
        schema: EVIDENCE_SCHEMA.into(),
        suite_id: SUITE_ID.into(),
        dataset_id: dataset_id.into(),
        status,
        certification: false,
        execution_mode: execution_mode.into(),
        validator,
        phases: phase_names(),
        identities,
        operations: outcomes,
        resource_events,
        harness_failures,
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

    #[test]
    fn live_public_api_errors_retain_resource_and_harness_classification() {
        use graphforge_api::{ApiErrorCode, GfError, ProjectErrorCode};
        let errors = [
            (
                GfError::Api {
                    code: ApiErrorCode::ResourceLimit,
                    message: "test budget".into(),
                },
                OperationStatus::ResourceExceeded,
            ),
            (
                GfError::Project {
                    code: ProjectErrorCode::ResourceLimit,
                    message: "test budget".into(),
                },
                OperationStatus::ResourceExceeded,
            ),
            (
                GfError::Execution("test runtime failure".into()),
                OperationStatus::HarnessError,
            ),
        ];
        for (error, expected) in errors {
            let MappingOutcome::Compatible(mapping) = map_operation(Operation::Tcr10) else {
                panic!("TCR10 mapping")
            };
            let translated = api_error("execution", error);
            let result = live_tcr10_outcome(&vec![], Err(translated), mapping);
            let evidence = assemble_evidence(LIVE_DATASET_ID, serde_json::json!({}), vec![result]);
            assert_eq!(evidence.status, expected);
            assert_eq!(
                evidence.resource_events.len(),
                usize::from(expected == OperationStatus::ResourceExceeded)
            );
            assert_eq!(
                evidence.harness_failures.len(),
                usize::from(expected == OperationStatus::HarnessError)
            );
            let json = serde_json::to_string(&evidence).unwrap();
            assert!(json.contains(expected.name()));
        }
    }

    #[test]
    fn all_operations_declare_a_mapping_and_validation_disposition() {
        assert_eq!(Operation::ALL.len(), 40);
        for operation in Operation::ALL {
            match map_operation(operation) {
                MappingOutcome::Compatible(mapping) => {
                    assert!(operation.validation_mode().is_some(), "{operation}");
                    assert_eq!(mapping.interface, "cypher", "{operation}");
                    assert!(!mapping.cypher_shape.is_empty(), "{operation}");
                    assert!(!mapping.notes.is_empty(), "{operation}");
                }
                MappingOutcome::SemanticIncompatibility { cause, detail } => {
                    assert!(operation.validation_mode().is_none(), "{operation}");
                    assert!(!cause.is_empty(), "{operation}");
                    assert!(!detail.is_empty(), "{operation}");
                }
            }
        }
    }

    #[test]
    fn compatible_reads_map_to_public_api() {
        let compatible_reads = [
            Operation::Tcr6,
            Operation::Tcr7,
            Operation::Tcr8,
            Operation::Tcr9,
            Operation::Tcr10,
            Operation::Tcr11,
            Operation::Tcr12,
            Operation::Tsr1,
            Operation::Tsr2,
            Operation::Tsr3,
            Operation::Tsr4,
            Operation::Tsr5,
            Operation::Tsr6,
        ];
        for operation in compatible_reads {
            assert!(
                matches!(map_operation(operation), MappingOutcome::Compatible(_)),
                "{operation} should be compatible"
            );
        }
        // All simple reads are compatible.
        for operation in Operation::ALL {
            if operation.category() == Category::SimpleRead {
                assert!(
                    matches!(map_operation(operation), MappingOutcome::Compatible(_)),
                    "{operation}"
                );
            }
        }
    }

    #[test]
    fn writes_and_read_writes_fail_closed_with_typed_cause() {
        for operation in Operation::ALL {
            match operation.category() {
                Category::Write | Category::ReadWrite => match map_operation(operation) {
                    MappingOutcome::SemanticIncompatibility { cause, .. } => {
                        assert_eq!(cause, WRITE_CAUSE, "{operation}");
                    }
                    MappingOutcome::Compatible(_) => panic!("{operation} must fail closed"),
                },
                _ => {}
            }
        }
    }

    #[test]
    fn unsupported_reads_fail_closed_with_specific_causes() {
        let cases = [
            (
                Operation::Tcr1,
                "recursive_temporal_path_filtering_not_exposed",
            ),
            (
                Operation::Tcr2,
                "recursive_temporal_path_filtering_not_exposed",
            ),
            (
                Operation::Tcr3,
                "temporal_shortest_transfer_path_not_exposed",
            ),
            (
                Operation::Tcr4,
                "temporal_transfer_cycle_detection_not_exposed",
            ),
            (Operation::Tcr5, "hub_vertex_truncation_not_exposed"),
        ];
        for (operation, expected) in cases {
            match map_operation(operation) {
                MappingOutcome::SemanticIncompatibility { cause, .. } => {
                    assert_eq!(cause, expected, "{operation}");
                }
                MappingOutcome::Compatible(_) => panic!("{operation} must fail closed"),
            }
        }
    }

    #[test]
    fn exact_validation_passes_and_mismatches() {
        let reference = parse_result_rows("acct-1 100\nacct-2 200\n");
        let ok = parse_result_rows("acct-1 100\nacct-2 200\n");
        validate_result(ValidationMode::Exact, &reference, &ok).unwrap();

        let reordered = parse_result_rows("acct-2 200\nacct-1 100\n");
        assert!(validate_result(ValidationMode::Exact, &reference, &reordered).is_err());

        let changed = parse_result_rows("acct-1 100\nacct-2 999\n");
        assert!(validate_result(ValidationMode::Exact, &reference, &changed).is_err());
    }

    #[test]
    fn normalized_validation_is_order_insensitive() {
        let reference = parse_result_rows("company-a 3\ncompany-b 2\n");
        let reordered = parse_result_rows("company-b 2\ncompany-a 3\n");
        validate_result(ValidationMode::Normalized, &reference, &reordered).unwrap();

        let missing = parse_result_rows("company-a 3\ncompany-c 9\n");
        assert!(validate_result(ValidationMode::Normalized, &reference, &missing).is_err());
    }

    #[test]
    fn run_job_passes_compatible_read_and_reports_incompatibility() {
        let reference = parse_result_rows("1 x\n");
        let read = run_job(
            &sample_job(Operation::Tsr1),
            Some(&reference),
            Some(ExecutionSignal::Produced(parse_result_rows("1 x\n"))),
        );
        assert_eq!(read.status, OperationStatus::Passed);
        assert!(read.public_api.is_some());

        let write = run_job(&sample_job(Operation::Tw1), None, None);
        assert_eq!(write.status, OperationStatus::SemanticIncompatibility);
        assert!(write.cause.as_deref().unwrap().contains(WRITE_CAUSE));

        let read_write = run_job(&sample_job(Operation::Trw1), None, None);
        assert_eq!(read_write.status, OperationStatus::SemanticIncompatibility);
        assert!(read_write.cause.as_deref().unwrap().contains(WRITE_CAUSE));
    }

    #[test]
    fn correctness_resource_and_harness_failures_are_distinguished() {
        // Correctness lane: output disagrees with reference.
        let correctness = run_job(
            &sample_job(Operation::Tsr1),
            Some(&parse_result_rows("1 x\n")),
            Some(ExecutionSignal::Produced(parse_result_rows("1 WRONG\n"))),
        );
        assert_eq!(correctness.status, OperationStatus::CorrectnessFailed);

        // Resource lane: the orchestrator reported a resource-limit event.
        let resource = run_job(
            &sample_job(Operation::Tsr2),
            Some(&parse_result_rows("1 x\n")),
            Some(ExecutionSignal::ResourceLimit("rss_limit_exceeded".into())),
        );
        assert_eq!(resource.status, OperationStatus::ResourceExceeded);

        // Harness lane: the runner could not execute the op.
        let harness = run_job(
            &sample_job(Operation::Tsr3),
            Some(&parse_result_rows("1 x\n")),
            Some(ExecutionSignal::HarnessError("driver_crashed".into())),
        );
        assert_eq!(harness.status, OperationStatus::HarnessError);

        // A missing system output is a harness error, not a correctness failure.
        let missing = run_job(
            &sample_job(Operation::Tsr4),
            Some(&parse_result_rows("1 x\n")),
            None,
        );
        assert_eq!(missing.status, OperationStatus::HarnessError);
        assert_eq!(missing.cause.as_deref(), Some("missing_system_output"));

        // A malformed job is a harness error, never a correctness result.
        let bad_job = OperationJob {
            schema: "wrong-schema".into(),
            suite_id: SUITE_ID.into(),
            dataset_id: BOUNDED_TINY_DATASET.into(),
            operation: Operation::Tsr1,
        };
        assert_eq!(
            run_job(&bad_job, None, None).status,
            OperationStatus::HarnessError
        );

        let evidence = assemble_evidence(
            BOUNDED_TINY_DATASET,
            serde_json::json!({}),
            vec![correctness, resource, harness],
        );
        // Each lane lands only in its own dedicated section.
        assert_eq!(evidence.resource_events.len(), 1);
        assert_eq!(evidence.resource_events[0].operation, Operation::Tsr2);
        assert_eq!(evidence.harness_failures.len(), 1);
        assert_eq!(evidence.harness_failures[0].operation, Operation::Tsr3);
        // Correctness failures are never projected into the other two lanes.
        assert!(
            !evidence
                .resource_events
                .iter()
                .any(|item| item.operation == Operation::Tsr1)
        );
        assert!(
            !evidence
                .harness_failures
                .iter()
                .any(|item| item.operation == Operation::Tsr1)
        );
        // Harness error is the worst class and drives the top-level status.
        assert_eq!(evidence.status, OperationStatus::HarnessError);
    }

    #[test]
    fn resource_only_run_reports_resource_status() {
        let resource = run_job(
            &sample_job(Operation::Tsr2),
            Some(&parse_result_rows("1 x\n")),
            Some(ExecutionSignal::ResourceLimit("wall_clock_exceeded".into())),
        );
        let passed = run_job(
            &sample_job(Operation::Tsr1),
            Some(&parse_result_rows("1 x\n")),
            Some(ExecutionSignal::Produced(parse_result_rows("1 x\n"))),
        );
        let evidence = assemble_evidence(
            BOUNDED_TINY_DATASET,
            serde_json::json!({}),
            vec![passed, resource],
        );
        assert_eq!(evidence.status, OperationStatus::ResourceExceeded);
        assert_eq!(evidence.harness_failures.len(), 0);
        assert_eq!(evidence.resource_events.len(), 1);
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
                    id: "finbench-sf1".into(),
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
                    id: "finbench-sf1".into(),
                    order: 2,
                    role: "ladder".into(),
                },
            ],
        };
        good.validate().unwrap();
        assert_eq!(good.ordered_ids()[0], BOUNDED_TINY_DATASET);
    }

    #[test]
    fn evidence_status_reflects_mixed_outcomes_and_never_certifies() {
        let passed = run_job(
            &sample_job(Operation::Tsr1),
            Some(&parse_result_rows("1 x\n")),
            Some(ExecutionSignal::Produced(parse_result_rows("1 x\n"))),
        );
        let incompatible = run_job(&sample_job(Operation::Tw1), None, None);
        let evidence = assemble_evidence(
            BOUNDED_TINY_DATASET,
            serde_json::json!({}),
            vec![passed, incompatible],
        );
        assert_eq!(evidence.status, OperationStatus::Passed);
        assert!(!evidence.certification);
        assert!(evidence.resource_events.is_empty());
        assert!(evidence.harness_failures.is_empty());
    }

    #[test]
    fn official_tcr10_jaccard_schema_is_truthful_for_normalized_and_exact() {
        let reference = parse_result_rows("0.667\n");
        let matching = parse_result_rows("0.667\n");
        validate_result(ValidationMode::Normalized, &reference, &matching).unwrap();
        validate_result(ValidationMode::Exact, &reference, &matching).unwrap();

        let obsolete_companies = parse_result_rows("company-1\ncompany-2\ncompany-3\n");
        assert!(
            validate_result(ValidationMode::Normalized, &reference, &obsolete_companies).is_err()
        );
        assert!(validate_result(ValidationMode::Exact, &reference, &obsolete_companies).is_err());

        let wrong = parse_result_rows("0.500\n");
        assert!(validate_result(ValidationMode::Normalized, &reference, &wrong).is_err());
        assert!(validate_result(ValidationMode::Exact, &reference, &wrong).is_err());
    }

    #[test]
    fn static_tcr10_fixtures_use_official_jaccard_and_stay_non_live() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/gdc/finbench-transaction-tiny");
        let compatible_ref = load_result_rows(
            &root.join("compatible/references/finbench-engineering-tiny-v1-TCR10.ref"),
        )
        .unwrap();
        let compatible_out = load_result_rows(
            &root.join("compatible/system-outputs/finbench-engineering-tiny-v1-TCR10.out"),
        )
        .unwrap();
        assert_eq!(compatible_ref, vec!["0.667".to_string()]);
        assert_eq!(compatible_out, vec!["0.667".to_string()]);
        validate_result(ValidationMode::Normalized, &compatible_ref, &compatible_out).unwrap();
        validate_result(ValidationMode::Exact, &compatible_ref, &compatible_out).unwrap();

        let mismatch_ref = load_result_rows(
            &root.join("reference-mismatch/references/finbench-engineering-tiny-v1-TCR10.ref"),
        )
        .unwrap();
        let mismatch_out = load_result_rows(
            &root.join("reference-mismatch/system-outputs/finbench-engineering-tiny-v1-TCR10.out"),
        )
        .unwrap();
        assert_eq!(mismatch_ref, vec!["0.667".to_string()]);
        assert_eq!(mismatch_out, vec!["0.500".to_string()]);
        assert!(validate_result(ValidationMode::Normalized, &mismatch_ref, &mismatch_out).is_err());
        assert!(validate_result(ValidationMode::Exact, &mismatch_ref, &mismatch_out).is_err());

        let MappingOutcome::Compatible(mapping) = map_operation(Operation::Tcr10) else {
            panic!("TCR10 must stay compatible");
        };
        assert!(mapping.cypher_shape.contains("jaccardSimilarity"));
        assert_eq!(
            Operation::Tcr10.intended_validation(),
            ValidationMode::Normalized
        );
    }

    #[test]
    fn live_tcr10_mapping_is_official_jaccard_with_open_window() {
        let MappingOutcome::Compatible(mapping) = map_operation(Operation::Tcr10) else {
            panic!("TCR10 must stay compatible");
        };
        assert_eq!(mapping.cypher_shape, LIVE_TCR10_QUERY);
        assert!(LIVE_TCR10_QUERY.contains("$startTime < e1.timestamp"));
        assert!(LIVE_TCR10_QUERY.contains("e1.timestamp < $endTime"));
        assert!(LIVE_TCR10_QUERY.contains("jaccardSimilarity"));
        assert!(LIVE_TCR10_QUERY.contains("round("));
        assert!(!LIVE_TCR10_QUERY.contains("$id1"));
        assert!(!LIVE_TCR10_QUERY.contains("createTime"));
    }

    #[test]
    fn live_expected_identity_is_closed_and_non_certifying() {
        let identity = expected_live_identity();
        assert_eq!(identity.schema, LIVE_IDENTITY_SCHEMA);
        assert_eq!(identity.suite_id, SUITE_ID);
        assert_eq!(identity.operation, Operation::Tcr10);
        assert_eq!(identity.source_mode, "runner_owned_rust_api");
        assert!(!identity.certification);
        assert!(!identity.reference.captured_from_engine);
        assert!(!identity.driver.durable);
        assert_eq!(
            identity.driver.interface,
            "graphforge_api::GraphForge::execute_with_params"
        );
        assert_eq!(identity.normalization.result_schema, ["jaccardSimilarity"]);
    }

    #[test]
    fn independent_seed_jaccard_is_officially_rounded_to_667() {
        let seed = LiveSeed {
            schema: LIVE_SEED_SCHEMA.into(),
            seed: 964,
            persons: vec![1, 2, 3],
            companies: vec![10, 11, 12, 13, 14, 15],
            invests: vec![
                LiveInvest {
                    person: 1,
                    company: 10,
                    timestamp: 110,
                },
                LiveInvest {
                    person: 1,
                    company: 11,
                    timestamp: 120,
                },
                LiveInvest {
                    person: 1,
                    company: 13,
                    timestamp: 100,
                },
                LiveInvest {
                    person: 1,
                    company: 14,
                    timestamp: 200,
                },
                LiveInvest {
                    person: 1,
                    company: 15,
                    timestamp: 90,
                },
                LiveInvest {
                    person: 2,
                    company: 10,
                    timestamp: 130,
                },
                LiveInvest {
                    person: 2,
                    company: 11,
                    timestamp: 140,
                },
                LiveInvest {
                    person: 2,
                    company: 12,
                    timestamp: 150,
                },
                LiveInvest {
                    person: 2,
                    company: 13,
                    timestamp: 100,
                },
                LiveInvest {
                    person: 2,
                    company: 14,
                    timestamp: 200,
                },
                LiveInvest {
                    person: 3,
                    company: 10,
                    timestamp: 160,
                },
            ],
        };
        let bindings = expected_live_parameters().bindings;
        assert_eq!(independently_derived_jaccard(&seed, &bindings), "0.667");
        let mut inclusive_start = bindings.clone();
        inclusive_start.start_time.value = 99;
        assert_eq!(
            independently_derived_jaccard(&seed, &inclusive_start),
            "0.750"
        );
        let mut empty = bindings;
        empty.start_time.value = 300;
        empty.end_time.value = 400;
        assert_eq!(independently_derived_jaccard(&seed, &empty), "0.000");
    }

    #[test]
    fn trusted_live_runner_executes_graphforge_and_keeps_failure_lanes() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/gdc/finbench-transaction-live");
        let evidence = run_live_fixture(
            &fixture,
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
        )
        .unwrap();
        assert_eq!(evidence.dataset_id, LIVE_DATASET_ID);
        assert_eq!(evidence.execution_mode, "live_graphforge");
        assert!(!evidence.certification);
        assert_eq!(evidence.status, OperationStatus::Passed);
        assert!(evidence.resource_events.is_empty());
        assert!(evidence.harness_failures.is_empty());
        let status_of = |operation: Operation| {
            evidence
                .operations
                .iter()
                .find(|item| item.operation == operation)
                .map(|item| item.status.clone())
        };
        assert_eq!(status_of(Operation::Tcr10), Some(OperationStatus::Passed));
        assert_eq!(
            status_of(Operation::Tcr1),
            Some(OperationStatus::SemanticIncompatibility)
        );
        assert_eq!(
            status_of(Operation::Tw1),
            Some(OperationStatus::SemanticIncompatibility)
        );
        assert_eq!(
            evidence.identities["execution_authority"]["caller_supplied_result"],
            false
        );
        assert_eq!(
            evidence.identities["live"]["normalization"]["result_schema"][0],
            "jaccardSimilarity"
        );
    }

    #[test]
    fn static_envelope_cannot_claim_live_execution() {
        let error = run_live_fixture(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("Cargo.toml")
                .as_path(),
            Path::new(env!("CARGO_MANIFEST_DIR")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("static output rejected"));
    }
}
