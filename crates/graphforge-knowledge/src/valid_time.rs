//! Optional append-only epistemic assertion valid-time events.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder, UInt32Array, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use uuid::{Uuid, Version};

use crate::{KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry};

/// Optional valid-time capability contract.
pub const VALID_TIME_CAPABILITY_VERSION: u32 = 1;
/// Assertion-validity record contract.
pub const ASSERTION_VALIDITY_CONTRACT_VERSION: u32 = 1;
/// Half-open interval interpretation policy.
pub const ASSERTION_VALIDITY_POLICY_VERSION: u32 = 1;

/// Authoritative assertion-validity event schema.
pub static ASSERTION_VALIDITY_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("validity_event_uuid", false),
        uuid_field("assertion_uuid", false),
        timestamp_field("valid_from", true),
        timestamp_field("valid_to", true),
        uuid_field("reasoning_uuid", true),
        uuid_field("provenance_uuid", false),
        timestamp_field("recorded_at", false),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static ASSERTION_VALIDITY_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"assertion_validity/1|validity_event_uuid:fixed[16]:required|assertion_uuid:fixed[16]:required|valid_from:timestamp_us_utc:nullable|valid_to:timestamp_us_utc:nullable|reasoning_uuid:fixed[16]:nullable|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered assertion-validity schema is within canonical bounds")
});

/// One immutable correction to an assertion's valid-time interpretation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssertionValidityEvent {
    /// Caller-supplied UUIDv7 identity/idempotency key.
    pub validity_event_uuid: Uuid,
    /// Existing immutable assertion.
    pub assertion_uuid: Uuid,
    /// Inclusive lower bound, or unbounded when absent.
    pub valid_from_micros: Option<i64>,
    /// Exclusive upper bound, or unbounded when absent.
    pub valid_to_micros: Option<i64>,
    /// Optional existing immutable reasoning record.
    pub reasoning_uuid: Option<Uuid>,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
    /// Mandatory transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl AssertionValidityEvent {
    /// Validate and construct one immutable half-open interval event.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        validity_event_uuid: Uuid,
        assertion_uuid: Uuid,
        valid_from_micros: Option<i64>,
        valid_to_micros: Option<i64>,
        reasoning_uuid: Option<Uuid>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        let event = Self {
            validity_event_uuid,
            assertion_uuid,
            valid_from_micros,
            valid_to_micros,
            reasoning_uuid,
            provenance_uuid,
            recorded_at_micros,
            contract_version: ASSERTION_VALIDITY_CONTRACT_VERSION,
        };
        validate_event(&event)?;
        Ok(event)
    }

    /// Evaluate the half-open interval `[valid_from, valid_to)`.
    ///
    /// Equal bounds form a valid empty interval. Missing bounds are unbounded.
    #[must_use]
    pub fn contains(&self, valid_time_micros: i64) -> bool {
        self.valid_from_micros
            .is_none_or(|from| from <= valid_time_micros)
            && self.valid_to_micros.is_none_or(|to| valid_time_micros < to)
    }
}

/// Validated append-only assertion-validity participant.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AssertionValidityLedger {
    /// Events ordered by `(recorded_at, validity_event_uuid)`.
    pub events: Vec<AssertionValidityEvent>,
}

impl AssertionValidityLedger {
    /// Validate, sort, and construct one complete participant.
    pub fn new(mut events: Vec<AssertionValidityEvent>) -> Result<Self, KnowledgeError> {
        if events.len() > MAX_KNOWLEDGE_ROWS {
            return Err(KnowledgeError::Limit {
                participant: "assertion_validity_events",
                observed: events.len(),
                limit: MAX_KNOWLEDGE_ROWS,
            });
        }
        let mut ids = HashSet::with_capacity(events.len());
        for event in &events {
            validate_event(event)?;
            if !ids.insert(event.validity_event_uuid) {
                return Err(KnowledgeError::Duplicate("validity_event_uuid"));
            }
        }
        events.sort_by_key(|row| (row.recorded_at_micros, row.validity_event_uuid));
        Ok(Self { events })
    }

    /// Merge staged append-only events with exact replay semantics.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut events = self.events.clone();
        let mut by_id = events
            .iter()
            .cloned()
            .map(|row| (row.validity_event_uuid, row))
            .collect::<HashMap<_, _>>();
        for event in &staged.events {
            if let Some(existing) = by_id.get(&event.validity_event_uuid) {
                if existing != event {
                    return Err(KnowledgeError::Conflict("validity_event_uuid"));
                }
            } else {
                events.push(event.clone());
                by_id.insert(event.validity_event_uuid, event.clone());
            }
        }
        Self::new(events)
    }

    /// Select the validity interpretation visible at transaction cutoff.
    #[must_use]
    pub fn current_for_at(
        &self,
        assertion_uuid: Uuid,
        transaction_cutoff_micros: i64,
    ) -> Option<&AssertionValidityEvent> {
        self.events
            .iter()
            .filter(|row| {
                row.assertion_uuid == assertion_uuid
                    && row.recorded_at_micros <= transaction_cutoff_micros
            })
            .max_by_key(|row| (row.recorded_at_micros, row.validity_event_uuid))
    }

    /// Test validity using the interpretation visible at transaction cutoff.
    #[must_use]
    pub fn is_valid_at(
        &self,
        assertion_uuid: Uuid,
        transaction_cutoff_micros: i64,
        valid_time_micros: i64,
    ) -> Option<bool> {
        self.current_for_at(assertion_uuid, transaction_cutoff_micros)
            .map(|event| event.contains(valid_time_micros))
    }

    /// Canonical fingerprint over one exact immutable event.
    pub fn event_fingerprint(&self, validity_event_uuid: Uuid) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .events
            .iter()
            .find(|row| row.validity_event_uuid == validity_event_uuid)
            .ok_or(KnowledgeError::Dangling("validity_event_uuid"))?;
        let mut writer = CanonicalWriter::new();
        writer.raw(row.validity_event_uuid.as_bytes())?;
        writer.raw(row.assertion_uuid.as_bytes())?;
        optional_i64(&mut writer, row.valid_from_micros)?;
        optional_i64(&mut writer, row.valid_to_micros)?;
        optional_uuid(&mut writer, row.reasoning_uuid)?;
        writer.raw(row.provenance_uuid.as_bytes())?;
        writer.i64(row.recorded_at_micros)?;
        writer.u32(row.contract_version)?;
        fingerprint(
            CanonicalDomain::AssertionValidity,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )
        .map_err(Into::into)
    }

    /// Build the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        let mut ids = FixedSizeBinaryBuilder::with_capacity(self.events.len(), 16);
        let mut assertions = FixedSizeBinaryBuilder::with_capacity(self.events.len(), 16);
        let mut from = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut to = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut reasoning = FixedSizeBinaryBuilder::with_capacity(self.events.len(), 16);
        let mut provenance = FixedSizeBinaryBuilder::with_capacity(self.events.len(), 16);
        let mut times = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut versions = UInt32Builder::new();
        for row in &self.events {
            append_uuid(&mut ids, row.validity_event_uuid, "validity_event_uuid")?;
            append_uuid(&mut assertions, row.assertion_uuid, "assertion_uuid")?;
            from.append_option(row.valid_from_micros);
            to.append_option(row.valid_to_micros);
            append_optional_uuid(&mut reasoning, row.reasoning_uuid)?;
            append_uuid(&mut provenance, row.provenance_uuid, "provenance_uuid")?;
            times.append_value(row.recorded_at_micros);
            versions.append_value(row.contract_version);
        }
        RecordBatch::try_new(
            Arc::clone(&ASSERTION_VALIDITY_SCHEMA),
            vec![
                Arc::new(ids.finish()),
                Arc::new(assertions.finish()),
                Arc::new(from.finish()),
                Arc::new(to.finish()),
                Arc::new(reasoning.finish()),
                Arc::new(provenance.finish()),
                Arc::new(times.finish()),
                Arc::new(versions.finish()),
            ],
        )
        .map_err(|_| invalid("assertion_validity", "Arrow batch construction failed"))
    }

    /// Decode and validate exact Arrow batches.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut events = Vec::new();
        for batch in batches {
            if batch.schema().as_ref() != ASSERTION_VALIDITY_SCHEMA.as_ref() {
                return Err(invalid("assertion_validity.schema", "schema mismatch"));
            }
            let ids = fixed(batch, "validity_event_uuid")?;
            let assertions = fixed(batch, "assertion_uuid")?;
            let from = timestamp(batch, "valid_from")?;
            let to = timestamp(batch, "valid_to")?;
            let reasoning = fixed(batch, "reasoning_uuid")?;
            let provenance = fixed(batch, "provenance_uuid")?;
            let times = timestamp(batch, "recorded_at")?;
            let versions = uint32(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                events.push(AssertionValidityEvent {
                    validity_event_uuid: uuid_at(ids, row, "validity_event_uuid")?,
                    assertion_uuid: uuid_at(assertions, row, "assertion_uuid")?,
                    valid_from_micros: optional_timestamp_at(from, row),
                    valid_to_micros: optional_timestamp_at(to, row),
                    reasoning_uuid: optional_uuid_at(reasoning, row, "reasoning_uuid")?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: times.value(row),
                    contract_version: versions.value(row),
                });
            }
        }
        Self::new(events)
    }
}

pub(crate) fn schema_registry_entry() -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "valid_time",
        capability_version: VALID_TIME_CAPABILITY_VERSION,
        record_family: "assertion_validity_events",
        record_version: ASSERTION_VALIDITY_CONTRACT_VERSION,
        schema: Arc::clone(&ASSERTION_VALIDITY_SCHEMA),
        schema_fingerprint: *ASSERTION_VALIDITY_SCHEMA_FINGERPRINT,
        enum_registry_versions: &[(
            "assertion_validity_policy",
            ASSERTION_VALIDITY_POLICY_VERSION,
        )],
        sort_key: &["recorded_at", "validity_event_uuid"],
        diff_identity_fields: &["validity_event_uuid"],
        diff_record_uuid_field: Some("validity_event_uuid"),
        fingerprint_domain: CanonicalDomain::AssertionValidity,
        owner: "graphforge-knowledge",
        implementation_issue: 781,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn validate_event(row: &AssertionValidityEvent) -> Result<(), KnowledgeError> {
    if row.contract_version != ASSERTION_VALIDITY_CONTRACT_VERSION {
        return Err(invalid(
            "assertion_validity.contract_version",
            "unsupported version",
        ));
    }
    require_v7(row.validity_event_uuid, "validity_event_uuid")?;
    require_v7(row.assertion_uuid, "assertion_uuid")?;
    if let Some(reasoning_uuid) = row.reasoning_uuid {
        require_v7(reasoning_uuid, "reasoning_uuid")?;
    }
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if matches!(
        (row.valid_from_micros, row.valid_to_micros),
        (Some(from), Some(to)) if from > to
    ) {
        return Err(invalid(
            "assertion_validity.interval",
            "valid_from must not exceed valid_to",
        ));
    }
    Ok(())
}

fn optional_i64(writer: &mut CanonicalWriter, value: Option<i64>) -> Result<(), KnowledgeError> {
    match value {
        Some(value) => {
            writer.u8(1)?;
            writer.i64(value)?;
        }
        None => writer.u8(0)?,
    }
    Ok(())
}

fn optional_uuid(writer: &mut CanonicalWriter, value: Option<Uuid>) -> Result<(), KnowledgeError> {
    match value {
        Some(value) => {
            writer.u8(1)?;
            writer.raw(value.as_bytes())?;
        }
        None => writer.u8(0)?,
    }
    Ok(())
}

fn append_uuid(
    builder: &mut FixedSizeBinaryBuilder,
    value: Uuid,
    field: &'static str,
) -> Result<(), KnowledgeError> {
    builder
        .append_value(value.as_bytes())
        .map_err(|_| invalid(field, "invalid UUID width"))
}

fn append_optional_uuid(
    builder: &mut FixedSizeBinaryBuilder,
    value: Option<Uuid>,
) -> Result<(), KnowledgeError> {
    if let Some(value) = value {
        append_uuid(builder, value, "reasoning_uuid")?;
    } else {
        builder.append_null();
    }
    Ok(())
}

fn optional_uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Option<Uuid>, KnowledgeError> {
    (!values.is_null(row))
        .then(|| uuid_at(values, row, field))
        .transpose()
}

fn optional_timestamp_at(values: &TimestampMicrosecondArray, row: usize) -> Option<i64> {
    (!values.is_null(row)).then(|| values.value(row))
}

fn require_v7(value: Uuid, field: &'static str) -> Result<(), KnowledgeError> {
    if value.get_version() != Some(Version::SortRand) {
        return Err(invalid(field, "must be UUIDv7"));
    }
    require_uuid(value, field)
}

fn require_uuid(value: Uuid, field: &'static str) -> Result<(), KnowledgeError> {
    if value.is_nil() {
        return Err(invalid(field, "must not be nil"));
    }
    Ok(())
}

const fn invalid(field: &'static str, message: &'static str) -> KnowledgeError {
    KnowledgeError::Invalid { field, message }
}

fn uuid_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), nullable)
}

fn timestamp_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        nullable,
    )
}

fn fixed<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a FixedSizeBinaryArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn timestamp<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a TimestampMicrosecondArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn uint32<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a UInt32Array, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Uuid, KnowledgeError> {
    Uuid::from_slice(values.value(row)).map_err(|_| invalid(field, "invalid UUID bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn event(
        id: u8,
        assertion: u8,
        from: Option<i64>,
        to: Option<i64>,
        recorded_at: i64,
    ) -> AssertionValidityEvent {
        AssertionValidityEvent::new(
            uuid7(id),
            uuid7(assertion),
            from,
            to,
            Some(uuid7(id.wrapping_add(40))),
            uuid7(id.wrapping_add(80)),
            recorded_at,
        )
        .unwrap()
    }

    #[test]
    fn half_open_unbounded_empty_and_invalid_intervals_are_explicit() {
        let always = event(1, 20, None, None, 1);
        assert!(always.contains(i64::MIN));
        assert!(always.contains(i64::MAX));

        let bounded = event(2, 20, Some(10), Some(20), 2);
        assert!(!bounded.contains(9));
        assert!(bounded.contains(10));
        assert!(bounded.contains(19));
        assert!(!bounded.contains(20));

        let empty = event(3, 20, Some(10), Some(10), 3);
        assert!(!empty.contains(10));
        assert!(
            AssertionValidityEvent::new(
                uuid7(4),
                uuid7(20),
                Some(11),
                Some(10),
                None,
                uuid7(84),
                4,
            )
            .is_err()
        );
    }

    #[test]
    fn transaction_cutoff_and_uuid_tie_breaking_preserve_prior_views() {
        let ledger = AssertionValidityLedger::new(vec![
            event(1, 20, Some(0), Some(10), 5),
            event(2, 20, Some(10), None, 5),
            event(3, 20, None, Some(0), 8),
        ])
        .unwrap();
        assert_eq!(
            ledger.current_for_at(uuid7(20), 4),
            None,
            "no event is visible before its transaction time"
        );
        assert_eq!(
            ledger
                .current_for_at(uuid7(20), 5)
                .unwrap()
                .validity_event_uuid,
            uuid7(2)
        );
        assert_eq!(ledger.is_valid_at(uuid7(20), 5, 10), Some(true));
        assert_eq!(ledger.is_valid_at(uuid7(20), 7, 10), Some(true));
        assert_eq!(ledger.is_valid_at(uuid7(20), 8, 10), Some(false));
    }

    #[test]
    fn round_trip_merge_replay_and_fingerprint_are_deterministic() {
        let first = event(2, 20, Some(10), None, 2);
        let second = event(1, 21, None, Some(10), 1);
        let ledger = AssertionValidityLedger::new(vec![first.clone(), second]).unwrap();
        let decoded = AssertionValidityLedger::from_batches(&[ledger.batch().unwrap()]).unwrap();
        assert_eq!(decoded, ledger);
        assert_eq!(decoded.events[0].validity_event_uuid, uuid7(1));
        assert_eq!(
            decoded.event_fingerprint(uuid7(2)).unwrap(),
            ledger.event_fingerprint(uuid7(2)).unwrap()
        );
        assert_eq!(
            ledger
                .merge(&AssertionValidityLedger::new(vec![first.clone()]).unwrap())
                .unwrap(),
            ledger
        );

        let mut conflicting = first;
        conflicting.valid_to_micros = Some(30);
        assert!(
            ledger
                .merge(&AssertionValidityLedger {
                    events: vec![conflicting]
                })
                .is_err()
        );
    }

    #[test]
    fn schema_registry_freezes_capability_family_and_order() {
        let entry = schema_registry_entry();
        assert_eq!(entry.capability_id, "valid_time");
        assert_eq!(entry.capability_version, 1);
        assert_eq!(entry.record_family, "assertion_validity_events");
        assert_eq!(entry.sort_key, &["recorded_at", "validity_event_uuid"]);
        assert_eq!(entry.implementation_issue, 781);
    }
}
