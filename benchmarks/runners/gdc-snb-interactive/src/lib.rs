//! GDC SNB Interactive suite: operation mapping, phases, reference validation.
//!
//! Workload semantics live here (not in shared `gdc_contracts`). Product query
//! behavior remains in GraphForge Rust crates; this runner maps Interactive
//! operations onto public Cypher interfaces, declares gaps, and validates
//! engineering-scale fixtures. Evidence never claims audited GDC certification.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::Path;
use std::str::FromStr;

pub const EVIDENCE_SCHEMA: &str = "graphforge-gdc-snb-interactive-evidence/1";
pub const LADDER_SCHEMA: &str = "graphforge-gdc-snb-interactive-ladder/1";
pub const JOB_SCHEMA: &str = "graphforge-gdc-snb-interactive-job/1";
pub const PHASES_SCHEMA: &str = "graphforge-gdc-snb-interactive-phases/1";
pub const SUITE_ID: &str = "snb-interactive";
pub const TINY_DATASET: &str = "snb-sf0.003";

/// Required lifecycle phases for Interactive engineering runs.
pub const PHASES: [&str; 4] = ["load", "warmup", "execution", "validation"];

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationClass {
    ComplexRead,
    ShortRead,
    Update,
}

impl OperationClass {
    pub fn name(self) -> &'static str {
        match self {
            Self::ComplexRead => "complex_read",
            Self::ShortRead => "short_read",
            Self::Update => "update",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Ic1,
    Ic2,
    Ic3,
    Ic4,
    Ic5,
    Ic6,
    Ic7,
    Ic8,
    Ic9,
    Ic10,
    Ic11,
    Ic12,
    Ic13,
    Ic14,
    Is1,
    Is2,
    Is3,
    Is4,
    Is5,
    Is6,
    Is7,
    Iu1,
    Iu2,
    Iu3,
    Iu4,
    Iu5,
    Iu6,
    Iu7,
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

    pub fn workload_key(self) -> &'static str {
        match self {
            Self::Ic1 => "ic1",
            Self::Ic2 => "ic2",
            Self::Ic3 => "ic3",
            Self::Ic4 => "ic4",
            Self::Ic5 => "ic5",
            Self::Ic6 => "ic6",
            Self::Ic7 => "ic7",
            Self::Ic8 => "ic8",
            Self::Ic9 => "ic9",
            Self::Ic10 => "ic10",
            Self::Ic11 => "ic11",
            Self::Ic12 => "ic12",
            Self::Ic13 => "ic13",
            Self::Ic14 => "ic14",
            Self::Is1 => "is1",
            Self::Is2 => "is2",
            Self::Is3 => "is3",
            Self::Is4 => "is4",
            Self::Is5 => "is5",
            Self::Is6 => "is6",
            Self::Is7 => "is7",
            Self::Iu1 => "iu1",
            Self::Iu2 => "iu2",
            Self::Iu3 => "iu3",
            Self::Iu4 => "iu4",
            Self::Iu5 => "iu5",
            Self::Iu6 => "iu6",
            Self::Iu7 => "iu7",
            Self::Iu8 => "iu8",
        }
    }

    pub fn class(self) -> OperationClass {
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
            | Self::Ic14 => OperationClass::ComplexRead,
            Self::Is1 | Self::Is2 | Self::Is3 | Self::Is4 | Self::Is5 | Self::Is6 | Self::Is7 => {
                OperationClass::ShortRead
            }
            Self::Iu1
            | Self::Iu2
            | Self::Iu3
            | Self::Iu4
            | Self::Iu5
            | Self::Iu6
            | Self::Iu7
            | Self::Iu8 => OperationClass::Update,
        }
    }

    pub fn validation_mode(self) -> ValidationMode {
        match self.class() {
            OperationClass::Update => ValidationMode::None,
            OperationClass::ComplexRead | OperationClass::ShortRead => ValidationMode::Exact,
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.workload_key())
    }
}

impl FromStr for Operation {
    type Err = SuiteError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        for operation in Self::ALL {
            if operation.workload_key() == value {
                return Ok(operation);
            }
        }
        Err(SuiteError::InvalidDocument(format!(
            "unknown SNB Interactive operation: {value}"
        )))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ValidationMode {
    Exact,
    None,
}

impl ValidationMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::None => "none",
        }
    }
}

/// Declared public GraphForge surface for a compatible Interactive mapping.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicApiMapping {
    pub interface: String,
    pub entrypoint: String,
    pub notes: String,
}

#[derive(Clone, Debug, PartialEq)]
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
    #[serde(default)]
    pub person_id: Option<u64>,
    #[serde(default)]
    pub message_id: Option<u64>,
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
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
        if sorted.first().map(|entry| entry.id.as_str()) != Some(TINY_DATASET) {
            return Err(SuiteError::InvalidDocument(format!(
                "ordered ladder must begin with bounded fixture {TINY_DATASET}"
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PhaseName {
    Load,
    Warmup,
    Execution,
    Validation,
}

impl PhaseName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Warmup => "warmup",
            Self::Execution => "execution",
            Self::Validation => "validation",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PhaseRecord {
    pub phase: PhaseName,
    pub status: PhaseStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhasePlan {
    pub schema: String,
    pub suite_id: String,
    pub phases: Vec<PhaseRecord>,
}

impl PhasePlan {
    pub fn validate(&self) -> Result<(), SuiteError> {
        if self.schema != PHASES_SCHEMA {
            return Err(SuiteError::InvalidDocument(format!(
                "unexpected phases schema: {}",
                self.schema
            )));
        }
        if self.suite_id != SUITE_ID {
            return Err(SuiteError::InvalidDocument(
                "phases suite_id must be snb-interactive".into(),
            ));
        }
        if self.phases.len() != PHASES.len() {
            return Err(SuiteError::InvalidDocument(format!(
                "phases must declare exactly {} entries in order",
                PHASES.len()
            )));
        }
        for (index, expected) in PHASES.iter().enumerate() {
            let actual = self.phases[index].phase.as_str();
            if actual != *expected {
                return Err(SuiteError::InvalidDocument(format!(
                    "phase {index} must be {expected}, got {actual}"
                )));
            }
            if matches!(self.phases[index].status, PhaseStatus::Skipped) {
                return Err(SuiteError::InvalidDocument(format!(
                    "phase {expected} must not be skipped; missing semantics are reported as gaps"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Passed,
    Failed,
    SemanticIncompatibility,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct OperationOutcome {
    pub operation: Operation,
    pub workload_key: String,
    pub class: OperationClass,
    pub status: OperationStatus,
    pub validation_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_api: Option<PublicApiMapping>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct CompletenessReport {
    pub policy: String,
    pub catalog_size: usize,
    pub supported: usize,
    pub unsupported: usize,
    pub failed: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SuiteEvidence {
    pub schema: String,
    pub suite_id: String,
    pub dataset_id: String,
    pub run_class: String,
    pub audited_gdc_certification: bool,
    pub status: OperationStatus,
    pub completeness: CompletenessReport,
    pub phases: Vec<PhaseRecord>,
    pub identities: serde_json::Value,
    pub operations: Vec<OperationOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SuiteError {
    InvalidDocument(String),
    ReferenceMismatch(String),
    SemanticIncompatibility { cause: String, detail: String },
    CertificationMasquerade(String),
}

impl fmt::Display for SuiteError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDocument(message) => write!(output, "invalid_document: {message}"),
            Self::ReferenceMismatch(message) => write!(output, "reference_mismatch: {message}"),
            Self::SemanticIncompatibility { cause, detail } => {
                write!(output, "semantic_incompatibility:{cause}: {detail}")
            }
            Self::CertificationMasquerade(message) => {
                write!(output, "certification_masquerade: {message}")
            }
        }
    }
}

impl std::error::Error for SuiteError {}

/// Map an Interactive operation onto the public GraphForge Cypher surface.
///
/// Unsupported official Interactive semantics fail closed with a typed cause
/// instead of being silently omitted from the catalog.
pub fn map_operation(job: &OperationJob) -> Result<MappingOutcome, SuiteError> {
    job.validate_schema()?;
    match job.operation {
        Operation::Is1 => {
            let Some(person_id) = job.person_id else {
                return Err(SuiteError::InvalidDocument("is1 requires person_id".into()));
            };
            Ok(MappingOutcome::Compatible(PublicApiMapping {
                interface: "cypher".into(),
                entrypoint: "query".into(),
                notes: format!(
                    "MATCH (p:Person {{id: {person_id}}}) RETURN p.firstName, p.lastName, p.birthday, p.locationIP, p.browserUsed, p.cityId, p.gender, p.creationDate"
                ),
            }))
        }
        Operation::Is3 => {
            let Some(person_id) = job.person_id else {
                return Err(SuiteError::InvalidDocument("is3 requires person_id".into()));
            };
            Ok(MappingOutcome::Compatible(PublicApiMapping {
                interface: "cypher".into(),
                entrypoint: "query".into(),
                notes: format!(
                    "MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f:Person) RETURN f.id, f.firstName, f.lastName ORDER BY f.id"
                ),
            }))
        }
        Operation::Is4 => {
            let Some(message_id) = job.message_id else {
                return Err(SuiteError::InvalidDocument(
                    "is4 requires message_id".into(),
                ));
            };
            Ok(MappingOutcome::Compatible(PublicApiMapping {
                interface: "cypher".into(),
                entrypoint: "query".into(),
                notes: format!(
                    "MATCH (m:Message {{id: {message_id}}}) RETURN m.creationDate, m.content"
                ),
            }))
        }
        Operation::Is2 | Operation::Is5 | Operation::Is6 | Operation::Is7 => {
            Ok(MappingOutcome::SemanticIncompatibility {
                cause: "short_read_requires_interactive_result_contract",
                detail: format!(
                    "{} needs LDBC Interactive short-read result ordering and substitution parameters not exposed as a public GraphForge Interactive binding",
                    job.operation.workload_key()
                ),
            })
        }
        op if matches!(op.class(), OperationClass::ComplexRead) => {
            Ok(MappingOutcome::SemanticIncompatibility {
                cause: "complex_read_requires_interactive_driver",
                detail: format!(
                    "{} requires the official Interactive complex-read driver contract (parameter substitution + ordered multi-row validation); GraphForge exposes Cypher only without an Interactive driver binding",
                    op.workload_key()
                ),
            })
        }
        op if matches!(op.class(), OperationClass::Update) => {
            Ok(MappingOutcome::SemanticIncompatibility {
                cause: "update_stream_protocol_not_exposed",
                detail: format!(
                    "{} requires the LDBC Interactive update-stream protocol and transactional IU semantics; GraphForge has no public Interactive update binding",
                    op.workload_key()
                ),
            })
        }
        other => Err(SuiteError::InvalidDocument(format!(
            "unhandled operation: {}",
            other.workload_key()
        ))),
    }
}

pub fn load_result_file(path: &Path) -> Result<String, SuiteError> {
    fs::read_to_string(path).map_err(|error| {
        SuiteError::InvalidDocument(format!("failed to read {}: {error}", path.display()))
    })
}

pub fn validate_reference(reference: &str, system: &str) -> Result<(), SuiteError> {
    if reference == system {
        Ok(())
    } else {
        Err(SuiteError::ReferenceMismatch(
            "exact result text mismatch".into(),
        ))
    }
}

pub fn run_operation(
    job: &OperationJob,
    reference: Option<&str>,
    system_output: Option<&str>,
) -> OperationOutcome {
    let mode = job.operation.validation_mode().name().to_string();
    let mapping = match map_operation(job) {
        Ok(mapping) => mapping,
        Err(error) => {
            return OperationOutcome {
                operation: job.operation,
                workload_key: job.operation.workload_key().into(),
                class: job.operation.class(),
                status: OperationStatus::Failed,
                validation_mode: mode,
                cause: Some(error.to_string()),
                public_api: None,
            };
        }
    };
    match mapping {
        MappingOutcome::SemanticIncompatibility { cause, detail } => OperationOutcome {
            operation: job.operation,
            workload_key: job.operation.workload_key().into(),
            class: job.operation.class(),
            status: OperationStatus::SemanticIncompatibility,
            validation_mode: mode,
            cause: Some(format!("{cause}: {detail}")),
            public_api: None,
        },
        MappingOutcome::Compatible(public_api) => {
            let Some(reference_text) = reference else {
                return OperationOutcome {
                    operation: job.operation,
                    workload_key: job.operation.workload_key().into(),
                    class: job.operation.class(),
                    status: OperationStatus::Failed,
                    validation_mode: mode,
                    cause: Some("missing_reference".into()),
                    public_api: Some(public_api),
                };
            };
            let Some(system) = system_output else {
                return OperationOutcome {
                    operation: job.operation,
                    workload_key: job.operation.workload_key().into(),
                    class: job.operation.class(),
                    status: OperationStatus::Failed,
                    validation_mode: mode,
                    cause: Some("missing_system_output".into()),
                    public_api: Some(public_api),
                };
            };
            match validate_reference(reference_text, system) {
                Ok(()) => OperationOutcome {
                    operation: job.operation,
                    workload_key: job.operation.workload_key().into(),
                    class: job.operation.class(),
                    status: OperationStatus::Passed,
                    validation_mode: mode,
                    cause: None,
                    public_api: Some(public_api),
                },
                Err(error) => OperationOutcome {
                    operation: job.operation,
                    workload_key: job.operation.workload_key().into(),
                    class: job.operation.class(),
                    status: OperationStatus::Failed,
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
    phases: Vec<PhaseRecord>,
    outcomes: Vec<OperationOutcome>,
    audited_gdc_certification: bool,
) -> Result<SuiteEvidence, SuiteError> {
    if audited_gdc_certification {
        return Err(SuiteError::CertificationMasquerade(
            "engineering SNB Interactive suite must set audited_gdc_certification=false".into(),
        ));
    }
    if outcomes.len() != Operation::ALL.len() {
        return Err(SuiteError::InvalidDocument(format!(
            "completeness requires all {} catalog operations, got {}",
            Operation::ALL.len(),
            outcomes.len()
        )));
    }
    let supported = outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome.status,
                OperationStatus::Passed | OperationStatus::Failed
            ) && outcome.public_api.is_some()
        })
        .count();
    let unsupported = outcomes
        .iter()
        .filter(|outcome| matches!(outcome.status, OperationStatus::SemanticIncompatibility))
        .count();
    let failed = outcomes
        .iter()
        .filter(|outcome| matches!(outcome.status, OperationStatus::Failed))
        .count();
    let status = if failed > 0 {
        OperationStatus::Failed
    } else if unsupported == outcomes.len() {
        OperationStatus::SemanticIncompatibility
    } else {
        // Mixed pass + declared gaps is admissible for engineering completeness.
        OperationStatus::Passed
    };
    Ok(SuiteEvidence {
        schema: EVIDENCE_SCHEMA.into(),
        suite_id: SUITE_ID.into(),
        dataset_id: dataset_id.into(),
        run_class: "engineering".into(),
        audited_gdc_certification: false,
        status,
        completeness: CompletenessReport {
            policy: "full_catalog_declare_gaps".into(),
            catalog_size: Operation::ALL.len(),
            supported,
            unsupported,
            failed,
        },
        phases,
        identities,
        operations: outcomes,
    })
}

pub fn unsupported_query_policy() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        (
            "complex_read",
            "report semantic_incompatibility:complex_read_requires_interactive_driver; never skip",
        ),
        (
            "short_read_gap",
            "report semantic_incompatibility:short_read_requires_interactive_result_contract; never skip",
        ),
        (
            "update",
            "report semantic_incompatibility:update_stream_protocol_not_exposed; never skip",
        ),
        (
            "certification",
            "audited_gdc_certification must remain false for engineering evidence",
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job(operation: Operation) -> OperationJob {
        OperationJob {
            schema: JOB_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            dataset_id: TINY_DATASET.into(),
            operation,
            person_id: Some(1),
            message_id: Some(10),
            parameters: BTreeMap::new(),
        }
    }

    #[test]
    fn catalog_covers_all_interactive_v1_operations() {
        assert_eq!(Operation::ALL.len(), 29);
        let mut keys = BTreeSet::new();
        for operation in Operation::ALL {
            assert!(keys.insert(operation.workload_key()));
        }
    }

    #[test]
    fn supported_short_reads_map_to_cypher() {
        for operation in [Operation::Is1, Operation::Is3, Operation::Is4] {
            let outcome = map_operation(&sample_job(operation)).unwrap();
            assert!(
                matches!(outcome, MappingOutcome::Compatible(_)),
                "{operation}"
            );
        }
    }

    #[test]
    fn complex_reads_and_updates_fail_closed() {
        let ic1 = map_operation(&sample_job(Operation::Ic1)).unwrap();
        assert!(matches!(
            ic1,
            MappingOutcome::SemanticIncompatibility {
                cause: "complex_read_requires_interactive_driver",
                ..
            }
        ));
        let iu1 = map_operation(&sample_job(Operation::Iu1)).unwrap();
        assert!(matches!(
            iu1,
            MappingOutcome::SemanticIncompatibility {
                cause: "update_stream_protocol_not_exposed",
                ..
            }
        ));
        let is2 = map_operation(&sample_job(Operation::Is2)).unwrap();
        assert!(matches!(
            is2,
            MappingOutcome::SemanticIncompatibility {
                cause: "short_read_requires_interactive_result_contract",
                ..
            }
        ));
    }

    #[test]
    fn phases_must_be_ordered_and_unskipped() {
        let plan = PhasePlan {
            schema: PHASES_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            phases: vec![
                PhaseRecord {
                    phase: PhaseName::Load,
                    status: PhaseStatus::Passed,
                    notes: None,
                },
                PhaseRecord {
                    phase: PhaseName::Warmup,
                    status: PhaseStatus::Passed,
                    notes: None,
                },
                PhaseRecord {
                    phase: PhaseName::Execution,
                    status: PhaseStatus::Passed,
                    notes: None,
                },
                PhaseRecord {
                    phase: PhaseName::Validation,
                    status: PhaseStatus::Passed,
                    notes: None,
                },
            ],
        };
        plan.validate().unwrap();

        let mut skipped = plan.clone();
        skipped.phases[1].status = PhaseStatus::Skipped;
        assert!(skipped.validate().is_err());
    }

    #[test]
    fn evidence_rejects_certification_masquerade() {
        let err = assemble_evidence(
            TINY_DATASET,
            serde_json::json!({}),
            Vec::new(),
            Vec::new(),
            true,
        )
        .unwrap_err();
        assert!(matches!(err, SuiteError::CertificationMasquerade(_)));
    }

    #[test]
    fn ladder_requires_tiny_first() {
        let ladder = DatasetLadder {
            schema: LADDER_SCHEMA.into(),
            suite_id: SUITE_ID.into(),
            datasets: vec![
                LadderEntry {
                    id: "snb-sf0.1".into(),
                    order: 1,
                    role: "engineering".into(),
                },
                LadderEntry {
                    id: TINY_DATASET.into(),
                    order: 2,
                    role: "engineering_fixture".into(),
                },
            ],
        };
        assert!(ladder.validate().is_err());
    }

    #[test]
    fn exact_reference_validation() {
        validate_reference("a\n", "a\n").unwrap();
        assert!(validate_reference("a\n", "b\n").is_err());
    }
}
