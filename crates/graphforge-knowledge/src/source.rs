//! Immutable research Source identities and Arrow encoding (#1349).

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use arrow::array::{FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use uuid::Uuid;

use crate::{
    KNOWLEDGE_CAPABILITY_VERSION, KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry,
    check_limit, fixed_column, invalid, optional_text, require_schema, require_uuid, require_v7,
    required_i64, required_text, required_u32, string_column, timestamp_column, u32_column,
    uuid_at, uuid_field,
};

/// Immutable research Source record contract.
pub const SOURCE_CONTRACT_VERSION: u32 = 1;
/// Closed research-source-kind registry version.
pub const SOURCE_KIND_REGISTRY_VERSION: u32 = 1;
/// Maximum UTF-8 bytes for one bounded Source label.
pub const MAX_SOURCE_LABEL_BYTES: usize = 4_096;
/// Maximum UTF-8 bytes for one bounded external identity URI.
pub const MAX_SOURCE_IDENTITY_URI_BYTES: usize = 4_096;

/// Authoritative `knowledge/sources.parquet` schema.
pub static SOURCE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("source_uuid", false),
        Field::new("label", DataType::Utf8, false),
        Field::new("source_kind", DataType::Utf8, false),
        Field::new("identity_uri", DataType::Utf8, true),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static SOURCE_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"source/1|source_uuid:fixed[16]:required|label:utf8:required|source_kind:utf8:required|identity_uri:utf8:nullable|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered source schema is within canonical bounds")
});

/// Closed kind for one acquired research Source.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Manuscript,
    Edition,
    Pdf,
    Epub,
    Web,
    Photograph,
    Recording,
    DatabaseExport,
    Other,
}

impl SourceKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manuscript => "manuscript",
            Self::Edition => "edition",
            Self::Pdf => "pdf",
            Self::Epub => "epub",
            Self::Web => "web",
            Self::Photograph => "photograph",
            Self::Recording => "recording",
            Self::DatabaseExport => "database_export",
            Self::Other => "other",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "manuscript" => Ok(Self::Manuscript),
            "edition" => Ok(Self::Edition),
            "pdf" => Ok(Self::Pdf),
            "epub" => Ok(Self::Epub),
            "web" => Ok(Self::Web),
            "photograph" => Ok(Self::Photograph),
            "recording" => Ok(Self::Recording),
            "database_export" => Ok(Self::DatabaseExport),
            "other" => Ok(Self::Other),
            _ => Err(invalid("source_kind", "unknown closed value")),
        }
    }
}

/// One immutable research Source identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Source {
    /// Caller-supplied UUIDv7 identity and idempotency key.
    pub source_uuid: Uuid,
    /// Bounded human-readable label; never raw source text.
    pub label: String,
    /// Closed source kind.
    pub source_kind: SourceKind,
    /// Stable external identity such as DOI or canonical URL; never fetched.
    pub identity_uri: Option<String>,
    /// Provenance event that published the Source.
    pub provenance_uuid: Uuid,
    /// Durable transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl Source {
    /// Construct one validated immutable Source.
    pub fn new(
        source_uuid: Uuid,
        label: String,
        source_kind: SourceKind,
        identity_uri: Option<String>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        let row = Self {
            source_uuid,
            label,
            source_kind,
            identity_uri,
            provenance_uuid,
            recorded_at_micros,
            contract_version: SOURCE_CONTRACT_VERSION,
        };
        validate_source(&row)?;
        Ok(row)
    }
}

/// Validated immutable Source table.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SourceLedger {
    /// Sources ordered by `(recorded_at, source_uuid)`.
    pub sources: Vec<Source>,
}

impl SourceLedger {
    /// Validate, sort, and construct a Source table.
    pub fn new(mut sources: Vec<Source>) -> Result<Self, KnowledgeError> {
        sources.sort_by_key(|row| (row.recorded_at_micros, row.source_uuid));
        validate_sources(&sources)?;
        Ok(Self { sources })
    }

    /// Merge staged rows into an existing ledger.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut merged = self.sources.clone();
        for row in &staged.sources {
            if let Some(existing) = merged
                .iter()
                .find(|candidate| candidate.source_uuid == row.source_uuid)
            {
                if existing != row {
                    return Err(KnowledgeError::Conflict("source_uuid"));
                }
                continue;
            }
            merged.push(row.clone());
        }
        Self::new(merged)
    }

    /// Encode the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        source_batch(&self.sources)
    }

    /// Decode one or more Arrow batches.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut sources = Vec::new();
        for batch in batches {
            require_schema(batch, &SOURCE_SCHEMA, "sources.schema")?;
            let ids = fixed_column(batch, "source_uuid")?;
            let labels = string_column(batch, "label")?;
            let kinds = string_column(batch, "source_kind")?;
            let uris = string_column(batch, "identity_uri")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                sources.push(Source {
                    source_uuid: uuid_at(ids, row, "source_uuid")?,
                    label: required_text(labels, row, "label")?.to_string(),
                    source_kind: SourceKind::parse(required_text(kinds, row, "source_kind")?)?,
                    identity_uri: optional_text(uris, row),
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(sources)
    }
}

pub(crate) fn schema_registry_entry() -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "knowledge",
        capability_version: KNOWLEDGE_CAPABILITY_VERSION,
        record_family: "sources",
        record_version: SOURCE_CONTRACT_VERSION,
        schema: Arc::clone(&SOURCE_SCHEMA),
        schema_fingerprint: *SOURCE_SCHEMA_FINGERPRINT,
        enum_registry_versions: &[("source_kind", SOURCE_KIND_REGISTRY_VERSION)],
        sort_key: &["recorded_at", "source_uuid"],
        diff_identity_fields: &["source_uuid"],
        diff_record_uuid_field: Some("source_uuid"),
        fingerprint_domain: CanonicalDomain::ResearchSource,
        owner: "graphforge-knowledge",
        implementation_issue: 1349,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn validate_source(row: &Source) -> Result<(), KnowledgeError> {
    if row.contract_version != SOURCE_CONTRACT_VERSION {
        return Err(invalid("source.contract_version", "unsupported version"));
    }
    require_v7(row.source_uuid, "source_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    validate_bounded_text("source.label", &row.label, MAX_SOURCE_LABEL_BYTES)?;
    if let Some(uri) = &row.identity_uri {
        validate_bounded_text("source.identity_uri", uri, MAX_SOURCE_IDENTITY_URI_BYTES)?;
    }
    Ok(())
}

fn validate_sources(sources: &[Source]) -> Result<(), KnowledgeError> {
    check_limit("sources", sources.len())?;
    let mut ids = HashSet::with_capacity(sources.len());
    for row in sources {
        validate_source(row)?;
        if !ids.insert(row.source_uuid) {
            return Err(KnowledgeError::Duplicate("source_uuid"));
        }
    }
    Ok(())
}

fn validate_bounded_text(
    field: &'static str,
    value: &str,
    limit: usize,
) -> Result<(), KnowledgeError> {
    if value.is_empty() {
        return Err(invalid(field, "value must not be empty"));
    }
    if value.len() > limit {
        return Err(KnowledgeError::Limit {
            participant: field,
            observed: value.len(),
            limit,
        });
    }
    Ok(())
}

fn source_batch(rows: &[Source]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.source_uuid.as_bytes())?;
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&SOURCE_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.label.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.source_kind.as_str()),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.identity_uri.as_deref())
                    .collect::<Vec<_>>(),
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
    fn source_round_trip_and_merge_idempotency() {
        let first = Source::new(
            uuid7(1),
            "Arabian Nights manuscript".into(),
            SourceKind::Manuscript,
            Some("https://example.org/ark:/12345".into()),
            uuid7(2),
            100,
        )
        .unwrap();
        let ledger = SourceLedger::new(vec![first.clone()]).unwrap();
        let batch = ledger.batch().unwrap();
        let reopened = SourceLedger::from_batches(&[batch]).unwrap();
        assert_eq!(reopened, ledger);
        let duplicate = SourceLedger::new(vec![first.clone()]).unwrap();
        assert_eq!(ledger.merge(&duplicate).unwrap(), ledger);
        let conflicting = Source::new(
            first.source_uuid,
            "Different label".into(),
            SourceKind::Pdf,
            None,
            uuid7(3),
            200,
        )
        .unwrap();
        assert!(matches!(
            ledger.merge(&SourceLedger::new(vec![conflicting]).unwrap()),
            Err(KnowledgeError::Conflict("source_uuid"))
        ));
    }

    #[test]
    fn source_kind_registry_round_trip_and_reject_unknown_tokens() {
        for kind in [
            SourceKind::Manuscript,
            SourceKind::Edition,
            SourceKind::Pdf,
            SourceKind::Epub,
            SourceKind::Web,
            SourceKind::Photograph,
            SourceKind::Recording,
            SourceKind::DatabaseExport,
            SourceKind::Other,
        ] {
            assert_eq!(SourceKind::parse(kind.as_str()).unwrap(), kind);
        }
        assert!(SourceKind::parse("scan").is_err());
    }
}
