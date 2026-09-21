//! Immutable directed derivation edges for research lineage (#1349).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint, uuid_v8,
};
use uuid::Uuid;

use crate::{
    KNOWLEDGE_CAPABILITY_VERSION, KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry,
    check_limit, fixed_column, invalid, require_schema, require_uuid, required_i64, required_text,
    required_u32, string_column, timestamp_column, u32_column, uuid_at, uuid_field,
};

/// Immutable derivation-edge record contract.
pub const ARTIFACT_DERIVATION_CONTRACT_VERSION: u32 = 1;
/// Closed derivation-subject-kind registry version.
pub const DERIVATION_SUBJECT_KIND_REGISTRY_VERSION: u32 = 1;
/// Closed derivation-role registry version.
pub const DERIVATION_ROLE_REGISTRY_VERSION: u32 = 1;

/// Authoritative `knowledge/artifact_derivations.parquet` schema.
pub static ARTIFACT_DERIVATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("derivation_uuid", false),
        uuid_field("output_uuid", false),
        Field::new("output_kind", DataType::Utf8, false),
        uuid_field("input_uuid", false),
        Field::new("input_kind", DataType::Utf8, false),
        Field::new("derivation_role", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt32, false),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static ARTIFACT_DERIVATION_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"artifact_derivation/1|derivation_uuid:fixed[16]:required|output_uuid:fixed[16]:required|output_kind:utf8:required|input_uuid:fixed[16]:required|input_kind:utf8:required|derivation_role:utf8:required|ordinal:u32:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered artifact derivation schema is within canonical bounds")
});

/// Closed UUID subject kind for one derivation endpoint.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivationSubjectKind {
    /// Immutable research Source.
    Source,
    /// Immutable research Artifact.
    Artifact,
    /// Public graph node UUID.
    Node,
    /// Public graph edge UUID.
    Edge,
    /// Immutable assertion UUID.
    Assertion,
    /// Evidence-link UUID.
    EvidenceLink,
    /// Algorithm-run UUID.
    AlgorithmRun,
}

impl DerivationSubjectKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Artifact => "artifact",
            Self::Node => "node",
            Self::Edge => "edge",
            Self::Assertion => "assertion",
            Self::EvidenceLink => "evidence_link",
            Self::AlgorithmRun => "algorithm_run",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "source" => Ok(Self::Source),
            "artifact" => Ok(Self::Artifact),
            "node" => Ok(Self::Node),
            "edge" => Ok(Self::Edge),
            "assertion" => Ok(Self::Assertion),
            "evidence_link" => Ok(Self::EvidenceLink),
            "algorithm_run" => Ok(Self::AlgorithmRun),
            _ => Err(invalid("derivation_subject_kind", "unknown closed value")),
        }
    }
}

/// Closed role for one derivation edge.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivationRole {
    /// Ordered input to one output.
    Input,
}

impl DerivationRole {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "input" => Ok(Self::Input),
            _ => Err(invalid("derivation_role", "unknown closed value")),
        }
    }
}

/// One immutable directed derivation edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactDerivation {
    /// Deterministic UUIDv8 identity derived from canonical edge bytes.
    pub derivation_uuid: Uuid,
    /// Derived subject UUID.
    pub output_uuid: Uuid,
    /// Closed output subject kind.
    pub output_kind: DerivationSubjectKind,
    /// Input subject UUID.
    pub input_uuid: Uuid,
    /// Closed input subject kind.
    pub input_kind: DerivationSubjectKind,
    /// Closed derivation role.
    pub derivation_role: DerivationRole,
    /// Per-output input ordering.
    pub ordinal: u32,
    /// Provenance event that published the edge.
    pub provenance_uuid: Uuid,
    /// Durable transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl ArtifactDerivation {
    /// Construct one validated immutable derivation edge.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        output_uuid: Uuid,
        output_kind: DerivationSubjectKind,
        input_uuid: Uuid,
        input_kind: DerivationSubjectKind,
        derivation_role: DerivationRole,
        ordinal: u32,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        let derivation_uuid = derivation_identity_uuid(
            output_uuid,
            output_kind,
            input_uuid,
            input_kind,
            derivation_role,
            ordinal,
        )?;
        let row = Self {
            derivation_uuid,
            output_uuid,
            output_kind,
            input_uuid,
            input_kind,
            derivation_role,
            ordinal,
            provenance_uuid,
            recorded_at_micros,
            contract_version: ARTIFACT_DERIVATION_CONTRACT_VERSION,
        };
        validate_derivation(&row)?;
        Ok(row)
    }
}

/// Validated immutable derivation-edge table.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactDerivationLedger {
    /// Edges ordered by `(recorded_at, derivation_uuid)`.
    pub derivations: Vec<ArtifactDerivation>,
}

impl ArtifactDerivationLedger {
    /// Validate, sort, and construct a derivation table.
    pub fn new(mut derivations: Vec<ArtifactDerivation>) -> Result<Self, KnowledgeError> {
        derivations.sort_by_key(|row| (row.recorded_at_micros, row.derivation_uuid));
        validate_derivations(&derivations)?;
        Ok(Self { derivations })
    }

    /// Merge staged rows into an existing ledger.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut merged = self.derivations.clone();
        for row in &staged.derivations {
            if let Some(existing) = merged
                .iter()
                .find(|candidate| candidate.derivation_uuid == row.derivation_uuid)
            {
                if existing != row {
                    return Err(KnowledgeError::Conflict("derivation_uuid"));
                }
                continue;
            }
            merged.push(row.clone());
        }
        Self::new(merged)
    }

    /// Encode the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        derivation_batch(&self.derivations)
    }

    /// Decode one or more Arrow batches.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut derivations = Vec::new();
        for batch in batches {
            require_schema(
                batch,
                &ARTIFACT_DERIVATION_SCHEMA,
                "artifact_derivations.schema",
            )?;
            let ids = fixed_column(batch, "derivation_uuid")?;
            let outputs = fixed_column(batch, "output_uuid")?;
            let output_kinds = string_column(batch, "output_kind")?;
            let inputs = fixed_column(batch, "input_uuid")?;
            let input_kinds = string_column(batch, "input_kind")?;
            let roles = string_column(batch, "derivation_role")?;
            let ordinals = u32_column(batch, "ordinal")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                derivations.push(ArtifactDerivation {
                    derivation_uuid: uuid_at(ids, row, "derivation_uuid")?,
                    output_uuid: uuid_at(outputs, row, "output_uuid")?,
                    output_kind: DerivationSubjectKind::parse(required_text(
                        output_kinds,
                        row,
                        "output_kind",
                    )?)?,
                    input_uuid: uuid_at(inputs, row, "input_uuid")?,
                    input_kind: DerivationSubjectKind::parse(required_text(
                        input_kinds,
                        row,
                        "input_kind",
                    )?)?,
                    derivation_role: DerivationRole::parse(required_text(
                        roles,
                        row,
                        "derivation_role",
                    )?)?,
                    ordinal: required_u32(ordinals, row, "ordinal")?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(derivations)
    }
}

pub(crate) fn schema_registry_entry() -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "knowledge",
        capability_version: KNOWLEDGE_CAPABILITY_VERSION,
        record_family: "artifact_derivations",
        record_version: ARTIFACT_DERIVATION_CONTRACT_VERSION,
        schema: Arc::clone(&ARTIFACT_DERIVATION_SCHEMA),
        schema_fingerprint: *ARTIFACT_DERIVATION_SCHEMA_FINGERPRINT,
        enum_registry_versions: &[
            ("output_kind", DERIVATION_SUBJECT_KIND_REGISTRY_VERSION),
            ("input_kind", DERIVATION_SUBJECT_KIND_REGISTRY_VERSION),
            ("derivation_role", DERIVATION_ROLE_REGISTRY_VERSION),
        ],
        sort_key: &["recorded_at", "derivation_uuid"],
        diff_identity_fields: &["derivation_uuid"],
        diff_record_uuid_field: Some("derivation_uuid"),
        fingerprint_domain: CanonicalDomain::ArtifactDerivation,
        owner: "graphforge-knowledge",
        implementation_issue: 1349,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn derivation_identity_uuid(
    output_uuid: Uuid,
    output_kind: DerivationSubjectKind,
    input_uuid: Uuid,
    input_kind: DerivationSubjectKind,
    derivation_role: DerivationRole,
    ordinal: u32,
) -> Result<Uuid, KnowledgeError> {
    let mut writer = CanonicalWriter::new();
    writer.raw(output_uuid.as_bytes())?;
    writer.text(output_kind.as_str())?;
    writer.raw(input_uuid.as_bytes())?;
    writer.text(input_kind.as_str())?;
    writer.text(derivation_role.as_str())?;
    writer.u32(ordinal)?;
    let digest = fingerprint(
        CanonicalDomain::ArtifactDerivation,
        ARTIFACT_DERIVATION_CONTRACT_VERSION,
        &writer.finish(),
    )?;
    Ok(uuid_v8(digest))
}

fn validate_derivation(row: &ArtifactDerivation) -> Result<(), KnowledgeError> {
    if row.contract_version != ARTIFACT_DERIVATION_CONTRACT_VERSION {
        return Err(invalid(
            "derivation.contract_version",
            "unsupported version",
        ));
    }
    require_uuid(row.derivation_uuid, "derivation_uuid")?;
    require_uuid(row.output_uuid, "output_uuid")?;
    require_uuid(row.input_uuid, "input_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if row.output_uuid == row.input_uuid && row.output_kind == row.input_kind {
        return Err(invalid("derivation", "self-loop is forbidden"));
    }
    let expected = derivation_identity_uuid(
        row.output_uuid,
        row.output_kind,
        row.input_uuid,
        row.input_kind,
        row.derivation_role,
        row.ordinal,
    )?;
    if row.derivation_uuid != expected {
        return Err(invalid("derivation_uuid", "identity mismatch"));
    }
    Ok(())
}

fn validate_derivations(derivations: &[ArtifactDerivation]) -> Result<(), KnowledgeError> {
    check_limit("artifact_derivations", derivations.len())?;
    let mut ids = HashSet::with_capacity(derivations.len());
    let mut ordinals: HashMap<(Uuid, DerivationSubjectKind), HashSet<u32>> = HashMap::new();
    for row in derivations {
        validate_derivation(row)?;
        if !ids.insert(row.derivation_uuid) {
            return Err(KnowledgeError::Duplicate("derivation_uuid"));
        }
        ordinals
            .entry((row.output_uuid, row.output_kind))
            .or_default()
            .insert(row.ordinal);
    }
    for ((output_uuid, output_kind), seen) in &ordinals {
        if seen.len()
            != derivations
                .iter()
                .filter(|row| row.output_uuid == *output_uuid && row.output_kind == *output_kind)
                .count()
        {
            return Err(KnowledgeError::Duplicate("ordinal"));
        }
    }
    detect_cycles(derivations)?;
    Ok(())
}

fn detect_cycles(derivations: &[ArtifactDerivation]) -> Result<(), KnowledgeError> {
    let mut adjacency: HashMap<(Uuid, DerivationSubjectKind), Vec<(Uuid, DerivationSubjectKind)>> =
        HashMap::new();
    for row in derivations {
        adjacency
            .entry((row.input_uuid, row.input_kind))
            .or_default()
            .push((row.output_uuid, row.output_kind));
    }
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    for start in adjacency.keys().copied().collect::<Vec<_>>() {
        if visited.contains(&start) {
            continue;
        }
        visiting.clear();
        if dfs_cycle(start, &adjacency, &mut visiting, &mut visited) {
            return Err(invalid("derivation", "cycle detected"));
        }
    }
    Ok(())
}

fn dfs_cycle(
    node: (Uuid, DerivationSubjectKind),
    adjacency: &HashMap<(Uuid, DerivationSubjectKind), Vec<(Uuid, DerivationSubjectKind)>>,
    visiting: &mut HashSet<(Uuid, DerivationSubjectKind)>,
    visited: &mut HashSet<(Uuid, DerivationSubjectKind)>,
) -> bool {
    if visiting.contains(&node) {
        return true;
    }
    if visited.contains(&node) {
        return false;
    }
    visiting.insert(node);
    if let Some(neighbors) = adjacency.get(&node) {
        for neighbor in neighbors {
            if dfs_cycle(*neighbor, adjacency, visiting, visited) {
                return true;
            }
        }
    }
    visiting.remove(&node);
    visited.insert(node);
    false
}

fn derivation_batch(rows: &[ArtifactDerivation]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut outputs = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut inputs = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.derivation_uuid.as_bytes())?;
        outputs.append_value(row.output_uuid.as_bytes())?;
        inputs.append_value(row.input_uuid.as_bytes())?;
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&ARTIFACT_DERIVATION_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(outputs.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.output_kind.as_str()),
            )),
            Arc::new(inputs.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.input_kind.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.derivation_role.as_str()),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.ordinal),
            )),
            Arc::new(provenance.finish()),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.recorded_at_micros),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::uuid7;

    #[test]
    fn derivation_round_trip_and_cycle_rejection() {
        let scan = ArtifactDerivation::new(
            uuid7(10),
            DerivationSubjectKind::Artifact,
            uuid7(1),
            DerivationSubjectKind::Artifact,
            DerivationRole::Input,
            0,
            uuid7(20),
            100,
        )
        .unwrap();
        let ocr = ArtifactDerivation::new(
            uuid7(11),
            DerivationSubjectKind::Artifact,
            uuid7(10),
            DerivationSubjectKind::Artifact,
            DerivationRole::Input,
            0,
            uuid7(21),
            200,
        )
        .unwrap();
        let ledger = ArtifactDerivationLedger::new(vec![scan.clone(), ocr.clone()]).unwrap();
        let reopened = ArtifactDerivationLedger::from_batches(&[ledger.batch().unwrap()]).unwrap();
        assert_eq!(reopened, ledger);
        let cycle = ArtifactDerivation::new(
            uuid7(1),
            DerivationSubjectKind::Artifact,
            uuid7(11),
            DerivationSubjectKind::Artifact,
            DerivationRole::Input,
            0,
            uuid7(22),
            300,
        )
        .unwrap();
        assert!(matches!(
            ArtifactDerivationLedger::new(vec![scan, ocr, cycle]),
            Err(KnowledgeError::Invalid { .. })
        ));
    }

    #[test]
    fn derivation_registries_round_trip_and_reject_unknown_tokens() {
        for kind in [
            DerivationSubjectKind::Source,
            DerivationSubjectKind::Artifact,
            DerivationSubjectKind::Node,
            DerivationSubjectKind::Edge,
            DerivationSubjectKind::Assertion,
            DerivationSubjectKind::EvidenceLink,
            DerivationSubjectKind::AlgorithmRun,
        ] {
            assert_eq!(DerivationSubjectKind::parse(kind.as_str()).unwrap(), kind);
        }
        assert_eq!(
            DerivationRole::parse(DerivationRole::Input.as_str()).unwrap(),
            DerivationRole::Input
        );
        assert!(DerivationSubjectKind::parse("passage").is_err());
        assert!(DerivationRole::parse("output").is_err());
    }
}
