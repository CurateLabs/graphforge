//! Immutable research Artifact identities and Arrow encoding (#1349).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use uuid::Uuid;

use crate::{
    KNOWLEDGE_CAPABILITY_VERSION, KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry,
    check_limit, fixed_column, invalid, optional_fixed_32, optional_text, require_schema,
    require_uuid, require_v7, required_i64, required_text, required_u32, string_column,
    timestamp_column, u32_column, uuid_at, uuid_field,
};

/// Immutable research Artifact record contract.
pub const ARTIFACT_CONTRACT_VERSION: u32 = 1;
/// Closed artifact-kind registry version.
pub const ARTIFACT_KIND_REGISTRY_VERSION: u32 = 1;
/// Closed payload-kind registry version.
pub const ARTIFACT_PAYLOAD_KIND_REGISTRY_VERSION: u32 = 1;
/// Closed availability registry version.
pub const ARTIFACT_AVAILABILITY_REGISTRY_VERSION: u32 = 1;
/// Maximum UTF-8 bytes for one bounded media type.
pub const MAX_ARTIFACT_MEDIA_TYPE_BYTES: usize = 256;
/// Maximum UTF-8 bytes for one bounded external URI reference.
pub const MAX_ARTIFACT_EXTERNAL_URI_BYTES: usize = 4_096;

/// Authoritative `knowledge/artifacts.parquet` schema.
pub static ARTIFACT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("artifact_uuid", false),
        uuid_field("source_uuid", false),
        Field::new("artifact_kind", DataType::Utf8, false),
        Field::new("media_type", DataType::Utf8, false),
        Field::new("payload_kind", DataType::Utf8, false),
        Field::new("content_sha256", DataType::FixedSizeBinary(32), true),
        Field::new("content_length", DataType::UInt64, true),
        Field::new("external_uri", DataType::Utf8, true),
        Field::new("external_fingerprint", DataType::FixedSizeBinary(32), true),
        Field::new("availability", DataType::Utf8, false),
        uuid_field("run_uuid", true),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static ARTIFACT_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"artifact/1|artifact_uuid:fixed[16]:required|source_uuid:fixed[16]:required|artifact_kind:utf8:required|media_type:utf8:required|payload_kind:utf8:required|content_sha256:fixed[32]:nullable|content_length:u64:nullable|external_uri:utf8:nullable|external_fingerprint:fixed[32]:nullable|availability:utf8:required|run_uuid:fixed[16]:nullable|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered artifact schema is within canonical bounds")
});

/// Closed representation kind for one Artifact.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    /// Original raw scan capture.
    RawScan,
    /// Processed scan derivative.
    ProcessedScan,
    /// OCR text output.
    OcrText,
    /// Normalized text representation.
    NormalizedText,
    /// Passage or excerpt extract.
    PassageExtract,
    /// Other closed kind not listed above.
    Other,
}

impl ArtifactKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RawScan => "raw_scan",
            Self::ProcessedScan => "processed_scan",
            Self::OcrText => "ocr_text",
            Self::NormalizedText => "normalized_text",
            Self::PassageExtract => "passage_extract",
            Self::Other => "other",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "raw_scan" => Ok(Self::RawScan),
            "processed_scan" => Ok(Self::ProcessedScan),
            "ocr_text" => Ok(Self::OcrText),
            "normalized_text" => Ok(Self::NormalizedText),
            "passage_extract" => Ok(Self::PassageExtract),
            "other" => Ok(Self::Other),
            _ => Err(invalid("artifact_kind", "unknown closed value")),
        }
    }
}

/// Closed payload reference kind.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPayloadKind {
    /// Locally retained bytes addressed by SHA-256.
    LocalSha256,
    /// External-only historical reference.
    ExternalReference,
    /// Explicitly absent bytes.
    Absent,
}

impl ArtifactPayloadKind {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalSha256 => "local_sha256",
            Self::ExternalReference => "external_reference",
            Self::Absent => "absent",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "local_sha256" => Ok(Self::LocalSha256),
            "external_reference" => Ok(Self::ExternalReference),
            "absent" => Ok(Self::Absent),
            _ => Err(invalid("payload_kind", "unknown closed value")),
        }
    }
}

/// Closed byte availability disclosure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactAvailability {
    /// Local bytes verified at publication time.
    LocalVerified,
    /// Only an external reference is retained.
    ExternalOnly,
    /// Local bytes are missing.
    MissingLocal,
    /// Integrity could not be verified.
    Unverifiable,
}

impl ArtifactAvailability {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalVerified => "local_verified",
            Self::ExternalOnly => "external_only",
            Self::MissingLocal => "missing_local",
            Self::Unverifiable => "unverifiable",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "local_verified" => Ok(Self::LocalVerified),
            "external_only" => Ok(Self::ExternalOnly),
            "missing_local" => Ok(Self::MissingLocal),
            "unverifiable" => Ok(Self::Unverifiable),
            _ => Err(invalid("availability", "unknown closed value")),
        }
    }
}

/// One immutable research Artifact identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Artifact {
    /// Caller-supplied UUIDv7 identity and idempotency key.
    pub artifact_uuid: Uuid,
    /// Parent Source identity.
    pub source_uuid: Uuid,
    /// Closed artifact kind.
    pub artifact_kind: ArtifactKind,
    /// MIME-like media type label.
    pub media_type: String,
    /// Closed payload reference kind.
    pub payload_kind: ArtifactPayloadKind,
    /// Content digest when locally retained.
    pub content_sha256: Option<[u8; 32]>,
    /// Content length when locally retained.
    pub content_length: Option<u64>,
    /// Historical external URI; never fetched by Core.
    pub external_uri: Option<String>,
    /// Known integrity fingerprint for external references.
    pub external_fingerprint: Option<[u8; 32]>,
    /// Closed availability disclosure.
    pub availability: ArtifactAvailability,
    /// Optional algorithm-run identity for extraction/OCR.
    pub run_uuid: Option<Uuid>,
    /// Provenance event that published the Artifact.
    pub provenance_uuid: Uuid,
    /// Durable transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl Artifact {
    /// Construct one validated immutable Artifact.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        artifact_uuid: Uuid,
        source_uuid: Uuid,
        artifact_kind: ArtifactKind,
        media_type: String,
        payload_kind: ArtifactPayloadKind,
        content_sha256: Option<[u8; 32]>,
        content_length: Option<u64>,
        external_uri: Option<String>,
        external_fingerprint: Option<[u8; 32]>,
        availability: ArtifactAvailability,
        run_uuid: Option<Uuid>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        let row = Self {
            artifact_uuid,
            source_uuid,
            artifact_kind,
            media_type,
            payload_kind,
            content_sha256,
            content_length,
            external_uri,
            external_fingerprint,
            availability,
            run_uuid,
            provenance_uuid,
            recorded_at_micros,
            contract_version: ARTIFACT_CONTRACT_VERSION,
        };
        validate_artifact(&row)?;
        Ok(row)
    }
}

/// Validated immutable Artifact table.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactLedger {
    /// Artifacts ordered by `(recorded_at, artifact_uuid)`.
    pub artifacts: Vec<Artifact>,
}

impl ArtifactLedger {
    /// Validate, sort, and construct an Artifact table.
    pub fn new(mut artifacts: Vec<Artifact>) -> Result<Self, KnowledgeError> {
        artifacts.sort_by_key(|row| (row.recorded_at_micros, row.artifact_uuid));
        validate_artifacts(&artifacts)?;
        Ok(Self { artifacts })
    }

    /// Merge staged rows into an existing ledger.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut merged = self.artifacts.clone();
        for row in &staged.artifacts {
            if let Some(existing) = merged
                .iter()
                .find(|candidate| candidate.artifact_uuid == row.artifact_uuid)
            {
                if existing != row {
                    return Err(KnowledgeError::Conflict("artifact_uuid"));
                }
                continue;
            }
            merged.push(row.clone());
        }
        Self::new(merged)
    }

    /// Encode the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        artifact_batch(&self.artifacts)
    }

    /// Decode one or more Arrow batches.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut artifacts = Vec::new();
        for batch in batches {
            require_schema(batch, &ARTIFACT_SCHEMA, "artifacts.schema")?;
            let ids = fixed_column(batch, "artifact_uuid")?;
            let sources = fixed_column(batch, "source_uuid")?;
            let kinds = string_column(batch, "artifact_kind")?;
            let media_types = string_column(batch, "media_type")?;
            let payload_kinds = string_column(batch, "payload_kind")?;
            let content_hashes = fixed_column(batch, "content_sha256")?;
            let content_lengths = batch
                .column_by_name("content_length")
                .ok_or_else(|| invalid("content_length", "missing column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| invalid("content_length", "invalid column type"))?;
            let external_uris = string_column(batch, "external_uri")?;
            let external_fingerprints = fixed_column(batch, "external_fingerprint")?;
            let availability = string_column(batch, "availability")?;
            let runs = fixed_column(batch, "run_uuid")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                artifacts.push(Artifact {
                    artifact_uuid: uuid_at(ids, row, "artifact_uuid")?,
                    source_uuid: uuid_at(sources, row, "source_uuid")?,
                    artifact_kind: ArtifactKind::parse(required_text(
                        kinds,
                        row,
                        "artifact_kind",
                    )?)?,
                    media_type: required_text(media_types, row, "media_type")?.to_string(),
                    payload_kind: ArtifactPayloadKind::parse(required_text(
                        payload_kinds,
                        row,
                        "payload_kind",
                    )?)?,
                    content_sha256: optional_fixed_32(content_hashes, row, "content_sha256")?,
                    content_length: (!content_lengths.is_null(row))
                        .then(|| content_lengths.value(row)),
                    external_uri: optional_text(external_uris, row),
                    external_fingerprint: optional_fixed_32(
                        external_fingerprints,
                        row,
                        "external_fingerprint",
                    )?,
                    availability: ArtifactAvailability::parse(required_text(
                        availability,
                        row,
                        "availability",
                    )?)?,
                    run_uuid: optional_uuid_at(runs, row, "run_uuid")?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(artifacts)
    }

    /// Index artifacts by Source for bounded lookup.
    #[must_use]
    pub fn by_source(&self) -> HashMap<Uuid, Vec<&Artifact>> {
        let mut grouped = HashMap::new();
        for artifact in &self.artifacts {
            grouped
                .entry(artifact.source_uuid)
                .or_insert_with(Vec::new)
                .push(artifact);
        }
        grouped
    }
}

pub(crate) fn schema_registry_entry() -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "knowledge",
        capability_version: KNOWLEDGE_CAPABILITY_VERSION,
        record_family: "artifacts",
        record_version: ARTIFACT_CONTRACT_VERSION,
        schema: Arc::clone(&ARTIFACT_SCHEMA),
        schema_fingerprint: *ARTIFACT_SCHEMA_FINGERPRINT,
        enum_registry_versions: &[
            ("artifact_kind", ARTIFACT_KIND_REGISTRY_VERSION),
            ("payload_kind", ARTIFACT_PAYLOAD_KIND_REGISTRY_VERSION),
            ("availability", ARTIFACT_AVAILABILITY_REGISTRY_VERSION),
        ],
        sort_key: &["recorded_at", "artifact_uuid"],
        diff_identity_fields: &["artifact_uuid"],
        diff_record_uuid_field: Some("artifact_uuid"),
        fingerprint_domain: CanonicalDomain::ResearchArtifact,
        owner: "graphforge-knowledge",
        implementation_issue: 1349,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn optional_uuid_at(
    array: &arrow::array::FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Option<Uuid>, KnowledgeError> {
    if array.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(uuid_at(array, row, field)?))
    }
}

fn validate_artifact(row: &Artifact) -> Result<(), KnowledgeError> {
    if row.contract_version != ARTIFACT_CONTRACT_VERSION {
        return Err(invalid("artifact.contract_version", "unsupported version"));
    }
    require_v7(row.artifact_uuid, "artifact_uuid")?;
    require_v7(row.source_uuid, "source_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if let Some(run_uuid) = row.run_uuid {
        require_v7(run_uuid, "run_uuid")?;
    }
    validate_bounded_text(
        "artifact.media_type",
        &row.media_type,
        MAX_ARTIFACT_MEDIA_TYPE_BYTES,
    )?;
    if let Some(uri) = &row.external_uri {
        validate_bounded_text(
            "artifact.external_uri",
            uri,
            MAX_ARTIFACT_EXTERNAL_URI_BYTES,
        )?;
    }
    match row.payload_kind {
        ArtifactPayloadKind::LocalSha256 => {
            if row.content_sha256.is_none() || row.content_length.is_none() {
                return Err(invalid(
                    "payload_kind",
                    "local_sha256 requires content_sha256 and content_length",
                ));
            }
            if row.external_uri.is_some() {
                return Err(invalid(
                    "external_uri",
                    "local_sha256 artifacts cannot retain external_uri",
                ));
            }
        }
        ArtifactPayloadKind::ExternalReference => {
            if row.external_uri.is_none() {
                return Err(invalid(
                    "external_uri",
                    "external_reference requires external_uri",
                ));
            }
            if row.content_sha256.is_some() || row.content_length.is_some() {
                return Err(invalid(
                    "content_sha256",
                    "external_reference cannot retain local bytes",
                ));
            }
        }
        ArtifactPayloadKind::Absent => {
            if row.content_sha256.is_some()
                || row.content_length.is_some()
                || row.external_uri.is_some()
                || row.external_fingerprint.is_some()
            {
                return Err(invalid(
                    "payload_kind",
                    "absent cannot retain payload fields",
                ));
            }
        }
    }
    Ok(())
}

fn validate_artifacts(artifacts: &[Artifact]) -> Result<(), KnowledgeError> {
    check_limit("artifacts", artifacts.len())?;
    let mut ids = HashSet::with_capacity(artifacts.len());
    for row in artifacts {
        validate_artifact(row)?;
        if !ids.insert(row.artifact_uuid) {
            return Err(KnowledgeError::Duplicate("artifact_uuid"));
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

fn artifact_batch(rows: &[Artifact]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut content_hashes = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
    let mut external_fingerprints = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
    let mut runs = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut content_lengths = UInt64Array::builder(rows.len());
    for row in rows {
        ids.append_value(row.artifact_uuid.as_bytes())?;
        sources.append_value(row.source_uuid.as_bytes())?;
        match row.content_sha256 {
            Some(value) => content_hashes.append_value(value)?,
            None => content_hashes.append_null(),
        }
        match row.content_length {
            Some(value) => content_lengths.append_value(value),
            None => content_lengths.append_null(),
        }
        match row.external_fingerprint {
            Some(value) => external_fingerprints.append_value(value)?,
            None => external_fingerprints.append_null(),
        }
        match row.run_uuid {
            Some(value) => runs.append_value(value.as_bytes())?,
            None => runs.append_null(),
        }
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&ARTIFACT_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(sources.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.artifact_kind.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.media_type.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.payload_kind.as_str()),
            )),
            Arc::new(content_hashes.finish()),
            Arc::new(content_lengths.finish()),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.external_uri.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(external_fingerprints.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.availability.as_str()),
            )),
            Arc::new(runs.finish()),
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
    fn artifact_round_trip_and_payload_invariants() {
        let local = Artifact::new(
            uuid7(1),
            uuid7(2),
            ArtifactKind::RawScan,
            "image/tiff".into(),
            ArtifactPayloadKind::LocalSha256,
            Some([7; 32]),
            Some(1024),
            None,
            None,
            ArtifactAvailability::LocalVerified,
            None,
            uuid7(3),
            100,
        )
        .unwrap();
        let external = Artifact::new(
            uuid7(4),
            uuid7(2),
            ArtifactKind::OcrText,
            "text/plain".into(),
            ArtifactPayloadKind::ExternalReference,
            None,
            None,
            Some("https://example.org/historical".into()),
            None,
            ArtifactAvailability::ExternalOnly,
            Some(uuid7(5)),
            uuid7(6),
            200,
        )
        .unwrap();
        let ledger = ArtifactLedger::new(vec![local.clone(), external]).unwrap();
        let reopened = ArtifactLedger::from_batches(&[ledger.batch().unwrap()]).unwrap();
        assert_eq!(reopened, ledger);
        assert!(matches!(
            Artifact::new(
                uuid7(7),
                uuid7(2),
                ArtifactKind::Other,
                "text/plain".into(),
                ArtifactPayloadKind::LocalSha256,
                None,
                Some(1),
                None,
                None,
                ArtifactAvailability::MissingLocal,
                None,
                uuid7(8),
                1,
            ),
            Err(KnowledgeError::Invalid { .. })
        ));
    }

    #[test]
    fn artifact_registries_round_trip_and_reject_unknown_tokens() {
        for kind in [
            ArtifactKind::RawScan,
            ArtifactKind::ProcessedScan,
            ArtifactKind::OcrText,
            ArtifactKind::NormalizedText,
            ArtifactKind::PassageExtract,
            ArtifactKind::Other,
        ] {
            assert_eq!(ArtifactKind::parse(kind.as_str()).unwrap(), kind);
        }
        for payload in [
            ArtifactPayloadKind::LocalSha256,
            ArtifactPayloadKind::ExternalReference,
            ArtifactPayloadKind::Absent,
        ] {
            assert_eq!(
                ArtifactPayloadKind::parse(payload.as_str()).unwrap(),
                payload
            );
        }
        for availability in [
            ArtifactAvailability::LocalVerified,
            ArtifactAvailability::ExternalOnly,
            ArtifactAvailability::MissingLocal,
            ArtifactAvailability::Unverifiable,
        ] {
            assert_eq!(
                ArtifactAvailability::parse(availability.as_str()).unwrap(),
                availability
            );
        }
        assert!(ArtifactKind::parse("scan").is_err());
    }
}
