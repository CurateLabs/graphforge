//! Required-local versus outside-reference retention pins (#1349).

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use arrow::array::{FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use uuid::Uuid;

use crate::{
    KNOWLEDGE_CAPABILITY_VERSION, KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry,
    check_limit, fixed_column, invalid, require_schema, require_uuid, require_v7, required_i64,
    required_text, required_u32, string_column, timestamp_column, u32_column, uuid_at, uuid_field,
};

/// Immutable retention-dependency record contract.
pub const RETENTION_DEPENDENCY_CONTRACT_VERSION: u32 = 1;
/// Closed required-subject-kind registry version.
pub const RETENTION_REQUIRED_KIND_REGISTRY_VERSION: u32 = 1;
/// Closed dependency-class registry version.
pub const RETENTION_DEPENDENCY_CLASS_REGISTRY_VERSION: u32 = 1;

/// Authoritative `knowledge/retention_dependencies.parquet` schema.
pub static RETENTION_DEPENDENCY_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("dependency_uuid", false),
        uuid_field("scope_uuid", false),
        uuid_field("required_uuid", false),
        Field::new("required_kind", DataType::Utf8, false),
        Field::new("dependency_class", DataType::Utf8, false),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static RETENTION_DEPENDENCY_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"retention_dependency/1|dependency_uuid:fixed[16]:required|scope_uuid:fixed[16]:required|required_uuid:fixed[16]:required|required_kind:utf8:required|dependency_class:utf8:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered retention dependency schema is within canonical bounds")
});

/// Closed subject kind for one retention dependency endpoint.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionRequiredKind {
    /// Immutable research Artifact.
    Artifact,
    /// Evidence-link UUID.
    EvidenceLink,
    /// Immutable research Source.
    Source,
    /// Immutable assertion UUID.
    Assertion,
}

impl RetentionRequiredKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Artifact => "artifact",
            Self::EvidenceLink => "evidence_link",
            Self::Source => "source",
            Self::Assertion => "assertion",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "artifact" => Ok(Self::Artifact),
            "evidence_link" => Ok(Self::EvidenceLink),
            "source" => Ok(Self::Source),
            "assertion" => Ok(Self::Assertion),
            _ => Err(invalid("retention_required_kind", "unknown closed value")),
        }
    }
}

/// Closed retention dependency class.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionDependencyClass {
    /// Locally retained bytes required for the scoped selection.
    RequiredLocalBytes,
    /// Historical outside reference without local substitution.
    OutsideReference,
    /// Comparison baseline retained for analyst review.
    ComparisonBaseline,
}

impl RetentionDependencyClass {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequiredLocalBytes => "required_local_bytes",
            Self::OutsideReference => "outside_reference",
            Self::ComparisonBaseline => "comparison_baseline",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "required_local_bytes" => Ok(Self::RequiredLocalBytes),
            "outside_reference" => Ok(Self::OutsideReference),
            "comparison_baseline" => Ok(Self::ComparisonBaseline),
            _ => Err(invalid("dependency_class", "unknown closed value")),
        }
    }
}

/// One explicit retention dependency pin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionDependency {
    /// Caller-supplied UUIDv7 identity and idempotency key.
    pub dependency_uuid: Uuid,
    /// Selection or retention root UUID.
    pub scope_uuid: Uuid,
    /// Required subject UUID.
    pub required_uuid: Uuid,
    /// Closed required subject kind.
    pub required_kind: RetentionRequiredKind,
    /// Closed dependency class.
    pub dependency_class: RetentionDependencyClass,
    /// Provenance event that published the dependency.
    pub provenance_uuid: Uuid,
    /// Durable transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl RetentionDependency {
    /// Construct one validated retention dependency.
    pub fn new(
        dependency_uuid: Uuid,
        scope_uuid: Uuid,
        required_uuid: Uuid,
        required_kind: RetentionRequiredKind,
        dependency_class: RetentionDependencyClass,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        let row = Self {
            dependency_uuid,
            scope_uuid,
            required_uuid,
            required_kind,
            dependency_class,
            provenance_uuid,
            recorded_at_micros,
            contract_version: RETENTION_DEPENDENCY_CONTRACT_VERSION,
        };
        validate_dependency(&row)?;
        Ok(row)
    }
}

/// Validated retention-dependency table.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetentionDependencyLedger {
    /// Dependencies ordered by `(recorded_at, dependency_uuid)`.
    pub dependencies: Vec<RetentionDependency>,
}

impl RetentionDependencyLedger {
    /// Validate, sort, and construct a dependency table.
    pub fn new(mut dependencies: Vec<RetentionDependency>) -> Result<Self, KnowledgeError> {
        dependencies.sort_by_key(|row| (row.recorded_at_micros, row.dependency_uuid));
        validate_dependencies(&dependencies)?;
        Ok(Self { dependencies })
    }

    /// Merge staged rows into an existing ledger.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut merged = self.dependencies.clone();
        for row in &staged.dependencies {
            if let Some(existing) = merged
                .iter()
                .find(|candidate| candidate.dependency_uuid == row.dependency_uuid)
            {
                if existing != row {
                    return Err(KnowledgeError::Conflict("dependency_uuid"));
                }
                continue;
            }
            merged.push(row.clone());
        }
        Self::new(merged)
    }

    /// Encode the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        dependency_batch(&self.dependencies)
    }

    /// Decode one or more Arrow batches.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut dependencies = Vec::new();
        for batch in batches {
            require_schema(batch, &RETENTION_DEPENDENCY_SCHEMA, "retention_dependencies.schema")?;
            let ids = fixed_column(batch, "dependency_uuid")?;
            let scopes = fixed_column(batch, "scope_uuid")?;
            let required = fixed_column(batch, "required_uuid")?;
            let kinds = string_column(batch, "required_kind")?;
            let classes = string_column(batch, "dependency_class")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                dependencies.push(RetentionDependency {
                    dependency_uuid: uuid_at(ids, row, "dependency_uuid")?,
                    scope_uuid: uuid_at(scopes, row, "scope_uuid")?,
                    required_uuid: uuid_at(required, row, "required_uuid")?,
                    required_kind: RetentionRequiredKind::parse(
                        required_text(kinds, row, "required_kind")?,
                    )?,
                    dependency_class: RetentionDependencyClass::parse(
                        required_text(classes, row, "dependency_class")?,
                    )?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(dependencies)
    }
}

pub(crate) fn schema_registry_entry() -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "knowledge",
        capability_version: KNOWLEDGE_CAPABILITY_VERSION,
        record_family: "retention_dependencies",
        record_version: RETENTION_DEPENDENCY_CONTRACT_VERSION,
        schema: Arc::clone(&RETENTION_DEPENDENCY_SCHEMA),
        schema_fingerprint: *RETENTION_DEPENDENCY_SCHEMA_FINGERPRINT,
        enum_registry_versions: &[
            ("required_kind", RETENTION_REQUIRED_KIND_REGISTRY_VERSION),
            ("dependency_class", RETENTION_DEPENDENCY_CLASS_REGISTRY_VERSION),
        ],
        sort_key: &["recorded_at", "dependency_uuid"],
        diff_identity_fields: &["dependency_uuid"],
        diff_record_uuid_field: Some("dependency_uuid"),
        fingerprint_domain: CanonicalDomain::Schema,
        owner: "graphforge-knowledge",
        implementation_issue: 1349,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn validate_dependency(row: &RetentionDependency) -> Result<(), KnowledgeError> {
    if row.contract_version != RETENTION_DEPENDENCY_CONTRACT_VERSION {
        return Err(invalid(
            "dependency.contract_version",
            "unsupported version",
        ));
    }
    require_v7(row.dependency_uuid, "dependency_uuid")?;
    require_uuid(row.scope_uuid, "scope_uuid")?;
    require_uuid(row.required_uuid, "required_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    Ok(())
}

fn validate_dependencies(dependencies: &[RetentionDependency]) -> Result<(), KnowledgeError> {
    check_limit("retention_dependencies", dependencies.len())?;
    let mut ids = HashSet::with_capacity(dependencies.len());
    for row in dependencies {
        validate_dependency(row)?;
        if !ids.insert(row.dependency_uuid) {
            return Err(KnowledgeError::Duplicate("dependency_uuid"));
        }
    }
    Ok(())
}

fn dependency_batch(rows: &[RetentionDependency]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut scopes = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut required = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.dependency_uuid.as_bytes())?;
        scopes.append_value(row.scope_uuid.as_bytes())?;
        required.append_value(row.required_uuid.as_bytes())?;
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&RETENTION_DEPENDENCY_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(scopes.finish()),
            Arc::new(required.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.required_kind.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.dependency_class.as_str()),
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
    fn retention_dependency_round_trip() {
        let local = RetentionDependency::new(
            uuid7(1),
            uuid7(2),
            uuid7(3),
            RetentionRequiredKind::Artifact,
            RetentionDependencyClass::RequiredLocalBytes,
            uuid7(4),
            100,
        )
        .unwrap();
        let outside = RetentionDependency::new(
            uuid7(5),
            uuid7(2),
            uuid7(6),
            RetentionRequiredKind::Artifact,
            RetentionDependencyClass::OutsideReference,
            uuid7(7),
            200,
        )
        .unwrap();
        let ledger = RetentionDependencyLedger::new(vec![local, outside]).unwrap();
        let reopened =
            RetentionDependencyLedger::from_batches(&[ledger.batch().unwrap()]).unwrap();
        assert_eq!(reopened, ledger);
    }

    #[test]
    fn retention_registries_round_trip_and_reject_unknown_tokens() {
        for kind in [
            RetentionRequiredKind::Artifact,
            RetentionRequiredKind::EvidenceLink,
            RetentionRequiredKind::Source,
            RetentionRequiredKind::Assertion,
        ] {
            assert_eq!(RetentionRequiredKind::parse(kind.as_str()).unwrap(), kind);
        }
        for class in [
            RetentionDependencyClass::RequiredLocalBytes,
            RetentionDependencyClass::OutsideReference,
            RetentionDependencyClass::ComparisonBaseline,
        ] {
            assert_eq!(RetentionDependencyClass::parse(class.as_str()).unwrap(), class);
        }
        assert!(RetentionRequiredKind::parse("node").is_err());
        assert!(RetentionDependencyClass::parse("genealogy").is_err());
    }
}
