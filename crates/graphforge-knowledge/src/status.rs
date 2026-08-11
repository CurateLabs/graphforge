//! Append-only epistemic assertion-status events.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt32Array, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use uuid::{Uuid, Version};

use crate::{
    EPISTEMIC_CAPABILITY_VERSION, KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry,
};

/// Assertion-status record contract.
pub const ASSERTION_STATUS_CONTRACT_VERSION: u32 = 1;
/// Closed status registry.
pub const ASSERTION_STATUS_REGISTRY_VERSION: u32 = 1;

/// Authoritative `knowledge/assertion_status_events.parquet` schema.
pub static ASSERTION_STATUS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("status_event_uuid", false),
        uuid_field("assertion_uuid", false),
        Field::new("status", DataType::Utf8, false),
        uuid_field("confidence_uuid", true),
        uuid_field("reasoning_uuid", true),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static ASSERTION_STATUS_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::AssertionStatus,
        CANONICAL_CONTRACT_VERSION,
        b"assertion_status/1|status_event_uuid:fixed[16]:required|assertion_uuid:fixed[16]:required|status:utf8:required|confidence_uuid:fixed[16]:nullable|reasoning_uuid:fixed[16]:nullable|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered assertion-status schema is within canonical bounds")
});

/// Closed `assertion-status@1` value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AssertionStatus {
    /// An explicit hypothesis.
    Hypothesis,
    /// Supported by the current interpretation.
    Supported,
    /// Refuted by the current interpretation.
    Refuted,
    /// Actively disputed.
    Disputed,
    /// Explicitly retracted.
    Retracted,
    /// Replaced through the atomic supersession operation.
    Superseded,
}

impl AssertionStatus {
    /// Stable persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hypothesis => "hypothesis",
            Self::Supported => "supported",
            Self::Refuted => "refuted",
            Self::Disputed => "disputed",
            Self::Retracted => "retracted",
            Self::Superseded => "superseded",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "hypothesis" => Ok(Self::Hypothesis),
            "supported" => Ok(Self::Supported),
            "refuted" => Ok(Self::Refuted),
            "disputed" => Ok(Self::Disputed),
            "retracted" => Ok(Self::Retracted),
            "superseded" => Ok(Self::Superseded),
            _ => Err(invalid("assertion_status.status", "unknown registry value")),
        }
    }

    /// Whether this value is terminal under `assertion-status@1`.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Superseded)
    }
}

/// One immutable assertion-status event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssertionStatusEvent {
    /// Caller-supplied UUIDv7 identity/idempotency key.
    pub status_event_uuid: Uuid,
    /// Existing immutable knowledge assertion.
    pub assertion_uuid: Uuid,
    /// Closed status value.
    pub status: AssertionStatus,
    /// Optional existing immutable knowledge confidence assessment.
    pub confidence_uuid: Option<Uuid>,
    /// Optional existing immutable epistemic reasoning record.
    pub reasoning_uuid: Option<Uuid>,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
    /// Mandatory transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl AssertionStatusEvent {
    /// Validate and construct one immutable event.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        status_event_uuid: Uuid,
        assertion_uuid: Uuid,
        status: AssertionStatus,
        confidence_uuid: Option<Uuid>,
        reasoning_uuid: Option<Uuid>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        require_v7(status_event_uuid, "status_event_uuid")?;
        require_v7(assertion_uuid, "assertion_uuid")?;
        if let Some(value) = confidence_uuid {
            require_v7(value, "confidence_uuid")?;
        }
        if let Some(value) = reasoning_uuid {
            require_v7(value, "reasoning_uuid")?;
        }
        require_uuid(provenance_uuid, "provenance_uuid")?;
        Ok(Self {
            status_event_uuid,
            assertion_uuid,
            status,
            confidence_uuid,
            reasoning_uuid,
            provenance_uuid,
            recorded_at_micros,
            contract_version: ASSERTION_STATUS_CONTRACT_VERSION,
        })
    }
}

/// Validated append-only assertion-status participant.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AssertionStatusLedger {
    /// Events ordered by `(recorded_at, status_event_uuid)`.
    pub events: Vec<AssertionStatusEvent>,
}

impl AssertionStatusLedger {
    /// Validate, sort, and construct one complete participant.
    pub fn new(mut events: Vec<AssertionStatusEvent>) -> Result<Self, KnowledgeError> {
        if events.len() > MAX_KNOWLEDGE_ROWS {
            return Err(KnowledgeError::Limit {
                participant: "assertion_status_events",
                observed: events.len(),
                limit: MAX_KNOWLEDGE_ROWS,
            });
        }
        let mut ids = HashSet::with_capacity(events.len());
        for event in &events {
            validate_event(event)?;
            if !ids.insert(event.status_event_uuid) {
                return Err(KnowledgeError::Duplicate("status_event_uuid"));
            }
        }
        events.sort_by_key(|row| (row.recorded_at_micros, row.status_event_uuid));
        let mut terminal_assertions = HashSet::new();
        for event in &events {
            if terminal_assertions.contains(&event.assertion_uuid) && !event.status.is_terminal() {
                return Err(invalid("assertion_status.status", "superseded is terminal"));
            }
            if event.status.is_terminal() {
                terminal_assertions.insert(event.assertion_uuid);
            }
        }
        Ok(Self { events })
    }

    /// Merge staged append-only events with exact replay semantics.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut events = self.events.clone();
        let terminal_assertions = self
            .events
            .iter()
            .filter(|row| row.status.is_terminal())
            .map(|row| row.assertion_uuid)
            .collect::<HashSet<_>>();
        let mut by_id = events
            .iter()
            .cloned()
            .map(|row| (row.status_event_uuid, row))
            .collect::<HashMap<_, _>>();
        for event in &staged.events {
            if let Some(existing) = by_id.get(&event.status_event_uuid) {
                if existing != event {
                    return Err(KnowledgeError::Conflict("status_event_uuid"));
                }
            } else {
                if terminal_assertions.contains(&event.assertion_uuid)
                    && !event.status.is_terminal()
                {
                    return Err(invalid("assertion_status.status", "superseded is terminal"));
                }
                events.push(event.clone());
                by_id.insert(event.status_event_uuid, event.clone());
            }
        }
        Self::new(events)
    }

    /// Canonical fingerprint over one exact immutable event.
    pub fn event_fingerprint(&self, status_event_uuid: Uuid) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .events
            .iter()
            .find(|row| row.status_event_uuid == status_event_uuid)
            .ok_or(KnowledgeError::Dangling("status_event_uuid"))?;
        let mut writer = CanonicalWriter::new();
        writer.raw(row.status_event_uuid.as_bytes())?;
        writer.raw(row.assertion_uuid.as_bytes())?;
        writer.text(row.status.as_str())?;
        optional_uuid(&mut writer, row.confidence_uuid)?;
        optional_uuid(&mut writer, row.reasoning_uuid)?;
        writer.raw(row.provenance_uuid.as_bytes())?;
        writer.i64(row.recorded_at_micros)?;
        writer.u32(row.contract_version)?;
        fingerprint(
            CanonicalDomain::AssertionStatus,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )
        .map_err(Into::into)
    }

    /// Resolve current status deterministically, returning `None` for statusless assertions.
    #[must_use]
    pub fn current_for(&self, assertion_uuid: Uuid) -> Option<&AssertionStatusEvent> {
        self.events
            .iter()
            .filter(|row| row.assertion_uuid == assertion_uuid)
            .max_by_key(|row| (row.recorded_at_micros, row.status_event_uuid))
    }

    /// Build the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        let mut ids = FixedSizeBinaryBuilder::with_capacity(self.events.len(), 16);
        let mut assertions = FixedSizeBinaryBuilder::with_capacity(self.events.len(), 16);
        let mut statuses = StringBuilder::new();
        let mut confidence = FixedSizeBinaryBuilder::with_capacity(self.events.len(), 16);
        let mut reasoning = FixedSizeBinaryBuilder::with_capacity(self.events.len(), 16);
        let mut provenance = FixedSizeBinaryBuilder::with_capacity(self.events.len(), 16);
        let mut times = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut versions = UInt32Builder::new();
        for row in &self.events {
            ids.append_value(row.status_event_uuid.as_bytes())
                .map_err(|_| invalid("status_event_uuid", "invalid UUID width"))?;
            assertions
                .append_value(row.assertion_uuid.as_bytes())
                .map_err(|_| invalid("assertion_uuid", "invalid UUID width"))?;
            statuses.append_value(row.status.as_str());
            append_optional_uuid(&mut confidence, row.confidence_uuid)?;
            append_optional_uuid(&mut reasoning, row.reasoning_uuid)?;
            provenance
                .append_value(row.provenance_uuid.as_bytes())
                .map_err(|_| invalid("provenance_uuid", "invalid UUID width"))?;
            times.append_value(row.recorded_at_micros);
            versions.append_value(row.contract_version);
        }
        RecordBatch::try_new(
            Arc::clone(&ASSERTION_STATUS_SCHEMA),
            vec![
                Arc::new(ids.finish()),
                Arc::new(assertions.finish()),
                Arc::new(statuses.finish()),
                Arc::new(confidence.finish()),
                Arc::new(reasoning.finish()),
                Arc::new(provenance.finish()),
                Arc::new(times.finish()),
                Arc::new(versions.finish()),
            ],
        )
        .map_err(|_| invalid("assertion_status", "Arrow batch construction failed"))
    }

    /// Decode and validate exact Arrow batches.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut events = Vec::new();
        for batch in batches {
            if batch.schema().as_ref() != ASSERTION_STATUS_SCHEMA.as_ref() {
                return Err(invalid("assertion_status.schema", "schema mismatch"));
            }
            let ids = fixed(batch, "status_event_uuid")?;
            let assertions = fixed(batch, "assertion_uuid")?;
            let statuses = string(batch, "status")?;
            let confidence = fixed(batch, "confidence_uuid")?;
            let reasoning = fixed(batch, "reasoning_uuid")?;
            let provenance = fixed(batch, "provenance_uuid")?;
            let times = timestamp(batch, "recorded_at")?;
            let versions = uint32(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                events.push(AssertionStatusEvent {
                    status_event_uuid: uuid_at(ids, row, "status_event_uuid")?,
                    assertion_uuid: uuid_at(assertions, row, "assertion_uuid")?,
                    status: AssertionStatus::parse(statuses.value(row))?,
                    confidence_uuid: optional_uuid_at(confidence, row, "confidence_uuid")?,
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
        capability_id: "epistemic",
        capability_version: EPISTEMIC_CAPABILITY_VERSION,
        record_family: "assertion_status_events",
        record_version: ASSERTION_STATUS_CONTRACT_VERSION,
        schema: Arc::clone(&ASSERTION_STATUS_SCHEMA),
        schema_fingerprint: *ASSERTION_STATUS_SCHEMA_FINGERPRINT,
        enum_registry_versions: &[("assertion_status", ASSERTION_STATUS_REGISTRY_VERSION)],
        sort_key: &["recorded_at", "status_event_uuid"],
        diff_identity_fields: &["status_event_uuid"],
        diff_record_uuid_field: Some("status_event_uuid"),
        fingerprint_domain: CanonicalDomain::AssertionStatus,
        owner: "graphforge-knowledge",
        implementation_issue: 777,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn validate_event(row: &AssertionStatusEvent) -> Result<(), KnowledgeError> {
    if row.contract_version != ASSERTION_STATUS_CONTRACT_VERSION {
        return Err(invalid(
            "assertion_status.contract_version",
            "unsupported version",
        ));
    }
    require_v7(row.status_event_uuid, "status_event_uuid")?;
    require_v7(row.assertion_uuid, "assertion_uuid")?;
    if let Some(value) = row.confidence_uuid {
        require_v7(value, "confidence_uuid")?;
    }
    if let Some(value) = row.reasoning_uuid {
        require_v7(value, "reasoning_uuid")?;
    }
    require_uuid(row.provenance_uuid, "provenance_uuid")
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

fn append_optional_uuid(
    builder: &mut FixedSizeBinaryBuilder,
    value: Option<Uuid>,
) -> Result<(), KnowledgeError> {
    if let Some(value) = value {
        builder
            .append_value(value.as_bytes())
            .map_err(|_| invalid("assertion_status", "invalid UUID width"))?;
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

fn fixed<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a FixedSizeBinaryArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn string<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a StringArray, KnowledgeError> {
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

    fn event(id: u8, assertion: u8, status: AssertionStatus, time: i64) -> AssertionStatusEvent {
        AssertionStatusEvent::new(
            uuid7(id),
            uuid7(assertion),
            status,
            None,
            None,
            uuid7(id.wrapping_add(100)),
            time,
        )
        .unwrap()
    }

    #[test]
    fn round_trip_fingerprint_statusless_and_identical_time_order_are_stable() {
        assert_eq!(
            ASSERTION_STATUS_SCHEMA_FINGERPRINT
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "83e696b90c9151fefe6b92c18b752c0d717fa4fd967997f60e995c79d59f54cd"
        );
        let ledger = AssertionStatusLedger::new(vec![
            event(2, 20, AssertionStatus::Supported, 10),
            event(1, 20, AssertionStatus::Hypothesis, 10),
        ])
        .unwrap();
        let decoded = AssertionStatusLedger::from_batches(&[ledger.batch().unwrap()]).unwrap();
        assert_eq!(decoded, ledger);
        assert_eq!(
            decoded.event_fingerprint(uuid7(2)).unwrap(),
            ledger.event_fingerprint(uuid7(2)).unwrap()
        );
        assert_eq!(
            ledger.current_for(uuid7(20)).unwrap().status_event_uuid,
            uuid7(2)
        );
        assert!(ledger.current_for(uuid7(21)).is_none());
    }

    #[test]
    fn every_nonterminal_transition_is_allowed_and_superseded_is_terminal() {
        let nonterminal = [
            AssertionStatus::Hypothesis,
            AssertionStatus::Supported,
            AssertionStatus::Refuted,
            AssertionStatus::Disputed,
            AssertionStatus::Retracted,
        ];
        let mut id = 1_u8;
        for from in nonterminal {
            for to in nonterminal {
                assert!(
                    AssertionStatusLedger::new(vec![
                        event(id, id, from, 1),
                        event(id.wrapping_add(1), id, to, 2),
                    ])
                    .is_ok()
                );
                id = id.wrapping_add(2);
            }
            assert!(
                AssertionStatusLedger::new(vec![
                    event(id, id, from, 1),
                    event(id.wrapping_add(1), id, AssertionStatus::Superseded, 2),
                ])
                .is_ok()
            );
            id = id.wrapping_add(2);
        }
        for to in [
            AssertionStatus::Hypothesis,
            AssertionStatus::Supported,
            AssertionStatus::Refuted,
            AssertionStatus::Disputed,
            AssertionStatus::Retracted,
        ] {
            assert!(matches!(
                AssertionStatusLedger::new(vec![
                    event(id, id, AssertionStatus::Superseded, 1),
                    event(id.wrapping_add(1), id, to, 2),
                ]),
                Err(KnowledgeError::Invalid {
                    field: "assertion_status.status",
                    message: "superseded is terminal",
                })
            ));
            id = id.wrapping_add(2);
        }
        assert!(
            AssertionStatusLedger::new(vec![
                event(id, id, AssertionStatus::Superseded, 1),
                event(id.wrapping_add(1), id, AssertionStatus::Superseded, 2),
            ])
            .is_ok(),
            "multiple terminal events preserve explicit supersession branches"
        );
        let terminal =
            AssertionStatusLedger::new(vec![event(id, id, AssertionStatus::Superseded, 10)])
                .unwrap();
        let second_terminal = AssertionStatusLedger::new(vec![event(
            id.wrapping_add(2),
            id,
            AssertionStatus::Superseded,
            11,
        )])
        .unwrap();
        assert!(terminal.merge(&second_terminal).is_ok());
        let backdated = AssertionStatusLedger::new(vec![event(
            id.wrapping_add(1),
            id,
            AssertionStatus::Supported,
            1,
        )])
        .unwrap();
        assert!(matches!(
            terminal.merge(&backdated),
            Err(KnowledgeError::Invalid {
                field: "assertion_status.status",
                message: "superseded is terminal",
            })
        ));
    }

    #[test]
    fn replay_is_idempotent_and_conflicting_identity_is_rejected() {
        let base = AssertionStatusLedger::new(vec![event(1, 20, AssertionStatus::Hypothesis, 10)])
            .unwrap();
        assert_eq!(base.merge(&base).unwrap(), base);
        let conflict =
            AssertionStatusLedger::new(vec![event(1, 20, AssertionStatus::Refuted, 10)]).unwrap();
        assert!(matches!(
            base.merge(&conflict),
            Err(KnowledgeError::Conflict("status_event_uuid"))
        ));
    }

    #[test]
    fn assertion_status_registry_is_closed_and_terminal_only_for_superseded() {
        let values = [
            AssertionStatus::Hypothesis,
            AssertionStatus::Supported,
            AssertionStatus::Refuted,
            AssertionStatus::Disputed,
            AssertionStatus::Retracted,
            AssertionStatus::Superseded,
        ];
        for value in values {
            assert_eq!(AssertionStatus::parse(value.as_str()).unwrap(), value);
            assert_eq!(value.is_terminal(), value == AssertionStatus::Superseded);
        }
        assert!(AssertionStatus::parse("unknown").is_err());
    }
}
