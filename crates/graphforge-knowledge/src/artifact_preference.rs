//! Append-only preferred-representation events per research Source (#1349).

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use arrow::array::{FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use uuid::Uuid;

use crate::{
    KNOWLEDGE_CAPABILITY_VERSION, KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry,
    check_limit, fixed_column, invalid, optional_fixed_16, require_schema, require_uuid,
    require_v7, required_i64, required_text, required_u32, string_column, timestamp_column,
    u32_column, uuid_at, uuid_field,
};

/// Immutable artifact-preference event contract.
pub const ARTIFACT_PREFERENCE_CONTRACT_VERSION: u32 = 1;
/// Maximum UTF-8 bytes for one bounded preference reason.
pub const MAX_ARTIFACT_PREFERENCE_REASON_BYTES: usize = 4_096;

/// Authoritative `knowledge/artifact_preference_events.parquet` schema.
pub static ARTIFACT_PREFERENCE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("preference_event_uuid", false),
        uuid_field("source_uuid", false),
        uuid_field("artifact_uuid", false),
        uuid_field("prior_artifact_uuid", true),
        Field::new("reason", DataType::Utf8, false),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static ARTIFACT_PREFERENCE_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"artifact_preference/1|preference_event_uuid:fixed[16]:required|source_uuid:fixed[16]:required|artifact_uuid:fixed[16]:required|prior_artifact_uuid:fixed[16]:nullable|reason:utf8:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered artifact preference schema is within canonical bounds")
});

/// One append-only preferred-representation event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactPreferenceEvent {
    /// Caller-supplied UUIDv7 identity and idempotency key.
    pub preference_event_uuid: Uuid,
    /// Parent Source whose preferred representation changed.
    pub source_uuid: Uuid,
    /// Newly preferred Artifact.
    pub artifact_uuid: Uuid,
    /// Previously preferred Artifact, when known.
    pub prior_artifact_uuid: Option<Uuid>,
    /// Bounded human-readable reason; never credentials or raw content.
    pub reason: String,
    /// Provenance event that published the preference.
    pub provenance_uuid: Uuid,
    /// Durable transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl ArtifactPreferenceEvent {
    /// Construct one validated preference event.
    pub fn new(
        preference_event_uuid: Uuid,
        source_uuid: Uuid,
        artifact_uuid: Uuid,
        prior_artifact_uuid: Option<Uuid>,
        reason: String,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        let row = Self {
            preference_event_uuid,
            source_uuid,
            artifact_uuid,
            prior_artifact_uuid,
            reason,
            provenance_uuid,
            recorded_at_micros,
            contract_version: ARTIFACT_PREFERENCE_CONTRACT_VERSION,
        };
        validate_preference_event(&row)?;
        Ok(row)
    }
}

/// Validated append-only artifact-preference table.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactPreferenceLedger {
    /// Events ordered by `(recorded_at, preference_event_uuid)`.
    pub events: Vec<ArtifactPreferenceEvent>,
}

impl ArtifactPreferenceLedger {
    /// Validate, sort, and construct a preference table.
    pub fn new(mut events: Vec<ArtifactPreferenceEvent>) -> Result<Self, KnowledgeError> {
        events.sort_by_key(|row| (row.recorded_at_micros, row.preference_event_uuid));
        validate_preference_events(&events)?;
        Ok(Self { events })
    }

    /// Merge staged rows into an existing ledger.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut merged = self.events.clone();
        for row in &staged.events {
            if let Some(existing) = merged
                .iter()
                .find(|candidate| candidate.preference_event_uuid == row.preference_event_uuid)
            {
                if existing != row {
                    return Err(KnowledgeError::Conflict("preference_event_uuid"));
                }
                continue;
            }
            merged.push(row.clone());
        }
        Self::new(merged)
    }

    /// Return the current preferred Artifact for one Source, if any.
    #[must_use]
    pub fn current_preferred_artifact(&self, source_uuid: Uuid) -> Option<Uuid> {
        self.events
            .iter()
            .filter(|row| row.source_uuid == source_uuid)
            .max_by_key(|row| (row.recorded_at_micros, row.preference_event_uuid))
            .map(|row| row.artifact_uuid)
    }

    /// Encode the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        preference_batch(&self.events)
    }

    /// Decode one or more Arrow batches.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut events = Vec::new();
        for batch in batches {
            require_schema(
                batch,
                &ARTIFACT_PREFERENCE_SCHEMA,
                "artifact_preference_events.schema",
            )?;
            let ids = fixed_column(batch, "preference_event_uuid")?;
            let sources = fixed_column(batch, "source_uuid")?;
            let artifacts = fixed_column(batch, "artifact_uuid")?;
            let priors = fixed_column(batch, "prior_artifact_uuid")?;
            let reasons = string_column(batch, "reason")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                events.push(ArtifactPreferenceEvent {
                    preference_event_uuid: uuid_at(ids, row, "preference_event_uuid")?,
                    source_uuid: uuid_at(sources, row, "source_uuid")?,
                    artifact_uuid: uuid_at(artifacts, row, "artifact_uuid")?,
                    prior_artifact_uuid: optional_fixed_16(priors, row, "prior_artifact_uuid")?,
                    reason: required_text(reasons, row, "reason")?.to_string(),
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(events)
    }
}

pub(crate) fn schema_registry_entry() -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "knowledge",
        capability_version: KNOWLEDGE_CAPABILITY_VERSION,
        record_family: "artifact_preference_events",
        record_version: ARTIFACT_PREFERENCE_CONTRACT_VERSION,
        schema: Arc::clone(&ARTIFACT_PREFERENCE_SCHEMA),
        schema_fingerprint: *ARTIFACT_PREFERENCE_SCHEMA_FINGERPRINT,
        enum_registry_versions: &[],
        sort_key: &["recorded_at", "preference_event_uuid"],
        diff_identity_fields: &["preference_event_uuid"],
        diff_record_uuid_field: Some("preference_event_uuid"),
        fingerprint_domain: CanonicalDomain::Schema,
        owner: "graphforge-knowledge",
        implementation_issue: 1349,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn validate_preference_event(row: &ArtifactPreferenceEvent) -> Result<(), KnowledgeError> {
    if row.contract_version != ARTIFACT_PREFERENCE_CONTRACT_VERSION {
        return Err(invalid(
            "preference.contract_version",
            "unsupported version",
        ));
    }
    require_v7(row.preference_event_uuid, "preference_event_uuid")?;
    require_v7(row.source_uuid, "source_uuid")?;
    require_v7(row.artifact_uuid, "artifact_uuid")?;
    if let Some(prior) = row.prior_artifact_uuid {
        require_v7(prior, "prior_artifact_uuid")?;
        if prior == row.artifact_uuid {
            return Err(invalid(
                "prior_artifact_uuid",
                "must differ from artifact_uuid",
            ));
        }
    }
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    validate_bounded_text(
        "preference.reason",
        &row.reason,
        MAX_ARTIFACT_PREFERENCE_REASON_BYTES,
    )?;
    Ok(())
}

fn validate_preference_events(events: &[ArtifactPreferenceEvent]) -> Result<(), KnowledgeError> {
    check_limit("artifact_preference_events", events.len())?;
    let mut ids = HashSet::with_capacity(events.len());
    for row in events {
        validate_preference_event(row)?;
        if !ids.insert(row.preference_event_uuid) {
            return Err(KnowledgeError::Duplicate("preference_event_uuid"));
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

fn preference_batch(rows: &[ArtifactPreferenceEvent]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut artifacts = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut priors = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.preference_event_uuid.as_bytes())?;
        sources.append_value(row.source_uuid.as_bytes())?;
        artifacts.append_value(row.artifact_uuid.as_bytes())?;
        match row.prior_artifact_uuid {
            Some(value) => priors.append_value(value.as_bytes())?,
            None => priors.append_null(),
        }
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&ARTIFACT_PREFERENCE_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(sources.finish()),
            Arc::new(artifacts.finish()),
            Arc::new(priors.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.reason.as_str()),
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
    fn preference_round_trip_and_current_selection() {
        let first = ArtifactPreferenceEvent::new(
            uuid7(1),
            uuid7(2),
            uuid7(3),
            None,
            "initial OCR".into(),
            uuid7(4),
            100,
        )
        .unwrap();
        let second = ArtifactPreferenceEvent::new(
            uuid7(5),
            uuid7(2),
            uuid7(6),
            Some(uuid7(3)),
            "better OCR".into(),
            uuid7(7),
            200,
        )
        .unwrap();
        let ledger = ArtifactPreferenceLedger::new(vec![first, second]).unwrap();
        let reopened = ArtifactPreferenceLedger::from_batches(&[ledger.batch().unwrap()]).unwrap();
        assert_eq!(reopened, ledger);
        assert_eq!(ledger.current_preferred_artifact(uuid7(2)), Some(uuid7(6)));
    }
}
