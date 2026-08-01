//! Immutable M21 reasoning records and explicit amendment chains.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray,
    StringBuilder, TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt32Array,
    UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use uuid::{Uuid, Version};

use crate::{KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry};

/// Immutable reasoning record contract.
pub const REASONING_CONTRACT_VERSION: u32 = 1;
/// M21 epistemic capability contract.
pub const EPISTEMIC_CAPABILITY_VERSION: u32 = 1;
/// Closed reasoning-kind registry.
pub const REASONING_KIND_REGISTRY_VERSION: u32 = 1;
/// Closed content-format registry.
pub const REASONING_CONTENT_FORMAT_REGISTRY_VERSION: u32 = 1;
/// Maximum exact reasoning payload accepted by the public API.
pub const MAX_REASONING_CONTENT_BYTES: usize = 65_536;

/// Authoritative `knowledge/reasoning.parquet` schema.
pub static REASONING_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("reasoning_uuid", false),
        uuid_field("assertion_uuid", false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("content_format", DataType::Utf8, false),
        Field::new("content", DataType::Binary, false),
        uuid_field("supersedes_reasoning_uuid", true),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static REASONING_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"reasoning/1|reasoning_uuid:fixed[16]:required|assertion_uuid:fixed[16]:required|kind:utf8:required|content_format:utf8:required|content:binary:required|supersedes_reasoning_uuid:fixed[16]:nullable|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered reasoning schema is within canonical bounds")
});

/// Closed purpose of one reasoning record.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReasoningKind {
    /// Interpretation of cited evidence.
    EvidenceInterpretation,
    /// Explicit logical inference.
    LogicalInference,
    /// Method or procedure explanation.
    MethodologicalNote,
    /// Rationale for a human or automated decision.
    DecisionRationale,
}

impl ReasoningKind {
    /// Stable persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EvidenceInterpretation => "evidence_interpretation",
            Self::LogicalInference => "logical_inference",
            Self::MethodologicalNote => "methodological_note",
            Self::DecisionRationale => "decision_rationale",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "evidence_interpretation" => Ok(Self::EvidenceInterpretation),
            "logical_inference" => Ok(Self::LogicalInference),
            "methodological_note" => Ok(Self::MethodologicalNote),
            "decision_rationale" => Ok(Self::DecisionRationale),
            _ => Err(invalid("reasoning.kind", "unknown registry value")),
        }
    }
}

/// Closed encoding contract for exact reasoning content.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReasoningContentFormat {
    /// UTF-8 plain text.
    TextPlain,
    /// UTF-8 Markdown, stored but never rendered or executed by the engine.
    TextMarkdown,
    /// Canonical caller-supplied UTF-8 JSON bytes.
    ApplicationJson,
}

impl ReasoningContentFormat {
    /// Stable persisted media type.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TextPlain => "text/plain",
            Self::TextMarkdown => "text/markdown",
            Self::ApplicationJson => "application/json",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "text/plain" => Ok(Self::TextPlain),
            "text/markdown" => Ok(Self::TextMarkdown),
            "application/json" => Ok(Self::ApplicationJson),
            _ => Err(invalid(
                "reasoning.content_format",
                "unknown registry value",
            )),
        }
    }
}

/// One immutable reasoning record or explicit amendment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReasoningRecord {
    /// Caller-supplied UUIDv7 identity/idempotency key.
    pub reasoning_uuid: Uuid,
    /// Existing immutable M20 assertion.
    pub assertion_uuid: Uuid,
    /// Closed reasoning purpose.
    pub kind: ReasoningKind,
    /// Closed exact-content encoding.
    pub content_format: ReasoningContentFormat,
    /// Exact accepted bytes.
    pub content: Vec<u8>,
    /// Prior reasoning record amended by this record.
    pub supersedes_reasoning_uuid: Option<Uuid>,
    /// Producing provenance event.
    pub provenance_uuid: Uuid,
    /// Mandatory transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl ReasoningRecord {
    /// Validate and construct one immutable record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reasoning_uuid: Uuid,
        assertion_uuid: Uuid,
        kind: ReasoningKind,
        content_format: ReasoningContentFormat,
        content: Vec<u8>,
        supersedes_reasoning_uuid: Option<Uuid>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        require_v7(reasoning_uuid, "reasoning_uuid")?;
        require_v7(assertion_uuid, "assertion_uuid")?;
        require_uuid(provenance_uuid, "provenance_uuid")?;
        if let Some(previous) = supersedes_reasoning_uuid {
            require_v7(previous, "supersedes_reasoning_uuid")?;
            if previous == reasoning_uuid {
                return Err(invalid(
                    "reasoning.supersedes_reasoning_uuid",
                    "self-link is forbidden",
                ));
            }
        }
        validate_content(content_format, &content)?;
        Ok(Self {
            reasoning_uuid,
            assertion_uuid,
            kind,
            content_format,
            content,
            supersedes_reasoning_uuid,
            provenance_uuid,
            recorded_at_micros,
            contract_version: REASONING_CONTRACT_VERSION,
        })
    }
}

/// Validated append-only reasoning participant content.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReasoningLedger {
    /// Records ordered by `(recorded_at, reasoning_uuid)`.
    pub records: Vec<ReasoningRecord>,
}

impl ReasoningLedger {
    /// Validate, sort, and construct one complete participant.
    pub fn new(mut records: Vec<ReasoningRecord>) -> Result<Self, KnowledgeError> {
        if records.len() > MAX_KNOWLEDGE_ROWS {
            return Err(KnowledgeError::Limit {
                participant: "reasoning",
                observed: records.len(),
                limit: MAX_KNOWLEDGE_ROWS,
            });
        }
        let mut by_id = HashMap::with_capacity(records.len());
        for record in &records {
            validate_record(record)?;
            if by_id.insert(record.reasoning_uuid, record).is_some() {
                return Err(KnowledgeError::Duplicate("reasoning_uuid"));
            }
        }
        for record in &records {
            if let Some(previous_uuid) = record.supersedes_reasoning_uuid {
                let previous = by_id
                    .get(&previous_uuid)
                    .ok_or(KnowledgeError::Dangling("supersedes_reasoning_uuid"))?;
                if previous.assertion_uuid != record.assertion_uuid {
                    return Err(invalid(
                        "reasoning.supersedes_reasoning_uuid",
                        "cross-assertion amendment is forbidden",
                    ));
                }
            }
        }
        let mut proven_acyclic = HashSet::with_capacity(records.len());
        for record in &records {
            reject_cycle(record.reasoning_uuid, &by_id, &mut proven_acyclic)?;
        }
        records.sort_by_key(|row| (row.recorded_at_micros, row.reasoning_uuid));
        Ok(Self { records })
    }

    /// Merge staged append-only records with idempotent exact replay.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut records = self.records.clone();
        let mut by_id = records
            .iter()
            .cloned()
            .map(|row| (row.reasoning_uuid, row))
            .collect::<HashMap<_, _>>();
        for record in &staged.records {
            if let Some(existing) = by_id.get(&record.reasoning_uuid) {
                if existing != record {
                    return Err(KnowledgeError::Conflict("reasoning_uuid"));
                }
            } else {
                records.push(record.clone());
                by_id.insert(record.reasoning_uuid, record.clone());
            }
        }
        Self::new(records)
    }

    /// Canonical fingerprint over the exact immutable record.
    pub fn record_fingerprint(&self, reasoning_uuid: Uuid) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .records
            .iter()
            .find(|row| row.reasoning_uuid == reasoning_uuid)
            .ok_or(KnowledgeError::Dangling("reasoning_uuid"))?;
        let mut writer = CanonicalWriter::new();
        writer.raw(row.reasoning_uuid.as_bytes())?;
        writer.raw(row.assertion_uuid.as_bytes())?;
        writer.text(row.kind.as_str())?;
        writer.text(row.content_format.as_str())?;
        writer.binary(&row.content)?;
        match row.supersedes_reasoning_uuid {
            Some(value) => {
                writer.u8(1)?;
                writer.raw(value.as_bytes())?;
            }
            None => writer.u8(0)?,
        }
        writer.raw(row.provenance_uuid.as_bytes())?;
        writer.i64(row.recorded_at_micros)?;
        writer.u32(row.contract_version)?;
        let bytes = writer.finish();
        fingerprint(
            CanonicalDomain::Reasoning,
            CANONICAL_CONTRACT_VERSION,
            &bytes,
        )
        .map_err(Into::into)
    }

    /// Resolve the current leaf without mutating or hiding branch history.
    #[must_use]
    pub fn current_for(&self, assertion_uuid: Uuid) -> Option<&ReasoningRecord> {
        let records = self
            .records
            .iter()
            .filter(|row| row.assertion_uuid == assertion_uuid)
            .collect::<Vec<_>>();
        let superseded = records
            .iter()
            .filter_map(|row| row.supersedes_reasoning_uuid)
            .collect::<HashSet<_>>();
        records
            .into_iter()
            .filter(|row| !superseded.contains(&row.reasoning_uuid))
            .max_by_key(|row| (row.recorded_at_micros, row.reasoning_uuid))
    }

    /// Build the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        let mut ids = FixedSizeBinaryBuilder::with_capacity(self.records.len(), 16);
        let mut assertions = FixedSizeBinaryBuilder::with_capacity(self.records.len(), 16);
        let mut kinds = StringBuilder::with_capacity(self.records.len(), 128);
        let mut formats = StringBuilder::with_capacity(self.records.len(), 128);
        let mut contents = BinaryBuilder::new();
        let mut predecessors = FixedSizeBinaryBuilder::with_capacity(self.records.len(), 16);
        let mut provenance = FixedSizeBinaryBuilder::with_capacity(self.records.len(), 16);
        let mut times =
            TimestampMicrosecondBuilder::with_capacity(self.records.len()).with_timezone("UTC");
        let mut versions = UInt32Builder::with_capacity(self.records.len());
        for row in &self.records {
            ids.append_value(row.reasoning_uuid.as_bytes())?;
            assertions.append_value(row.assertion_uuid.as_bytes())?;
            kinds.append_value(row.kind.as_str());
            formats.append_value(row.content_format.as_str());
            contents.append_value(&row.content);
            match row.supersedes_reasoning_uuid {
                Some(value) => predecessors.append_value(value.as_bytes())?,
                None => predecessors.append_null(),
            }
            provenance.append_value(row.provenance_uuid.as_bytes())?;
            times.append_value(row.recorded_at_micros);
            versions.append_value(row.contract_version);
        }
        RecordBatch::try_new(
            Arc::clone(&REASONING_SCHEMA),
            vec![
                Arc::new(ids.finish()),
                Arc::new(assertions.finish()),
                Arc::new(kinds.finish()),
                Arc::new(formats.finish()),
                Arc::new(contents.finish()),
                Arc::new(predecessors.finish()),
                Arc::new(provenance.finish()),
                Arc::new(times.finish()),
                Arc::new(versions.finish()),
            ],
        )
        .map_err(Into::into)
    }

    /// Decode authoritative batches and revalidate the complete ledger.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut records = Vec::new();
        for batch in batches {
            if batch.schema().as_ref() != REASONING_SCHEMA.as_ref() {
                return Err(invalid("reasoning.schema", "schema mismatch"));
            }
            let ids = fixed(batch, "reasoning_uuid")?;
            let assertions = fixed(batch, "assertion_uuid")?;
            let kinds = string(batch, "kind")?;
            let formats = string(batch, "content_format")?;
            let contents = binary(batch, "content")?;
            let predecessors = fixed(batch, "supersedes_reasoning_uuid")?;
            let provenance = fixed(batch, "provenance_uuid")?;
            let times = timestamp(batch, "recorded_at")?;
            let versions = uint32(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                records.push(ReasoningRecord {
                    reasoning_uuid: uuid_at(ids, row, "reasoning_uuid")?,
                    assertion_uuid: uuid_at(assertions, row, "assertion_uuid")?,
                    kind: ReasoningKind::parse(kinds.value(row))?,
                    content_format: ReasoningContentFormat::parse(formats.value(row))?,
                    content: contents.value(row).to_vec(),
                    supersedes_reasoning_uuid: (!predecessors.is_null(row))
                        .then(|| uuid_at(predecessors, row, "supersedes_reasoning_uuid"))
                        .transpose()?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: times.value(row),
                    contract_version: versions.value(row),
                });
            }
        }
        Self::new(records)
    }
}

pub(crate) fn schema_registry_entry() -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "epistemic",
        capability_version: EPISTEMIC_CAPABILITY_VERSION,
        record_family: "reasoning",
        record_version: REASONING_CONTRACT_VERSION,
        schema: Arc::clone(&REASONING_SCHEMA),
        schema_fingerprint: *REASONING_SCHEMA_FINGERPRINT,
        enum_registry_versions: &[
            ("reasoning_kind", REASONING_KIND_REGISTRY_VERSION),
            (
                "reasoning_content_format",
                REASONING_CONTENT_FORMAT_REGISTRY_VERSION,
            ),
        ],
        sort_key: &["recorded_at", "reasoning_uuid"],
        diff_identity_fields: &["reasoning_uuid"],
        diff_record_uuid_field: Some("reasoning_uuid"),
        fingerprint_domain: CanonicalDomain::Reasoning,
        owner: "graphforge-knowledge",
        implementation_issue: 780,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn validate_record(row: &ReasoningRecord) -> Result<(), KnowledgeError> {
    if row.contract_version != REASONING_CONTRACT_VERSION {
        return Err(invalid("reasoning.contract_version", "unsupported version"));
    }
    require_v7(row.reasoning_uuid, "reasoning_uuid")?;
    require_v7(row.assertion_uuid, "assertion_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if row.supersedes_reasoning_uuid == Some(row.reasoning_uuid) {
        return Err(invalid(
            "reasoning.supersedes_reasoning_uuid",
            "self-link is forbidden",
        ));
    }
    if let Some(previous) = row.supersedes_reasoning_uuid {
        require_v7(previous, "supersedes_reasoning_uuid")?;
    }
    validate_content(row.content_format, &row.content)
}

fn validate_content(
    content_format: ReasoningContentFormat,
    content: &[u8],
) -> Result<(), KnowledgeError> {
    if content.is_empty() {
        return Err(invalid("reasoning.content", "must not be empty"));
    }
    if content.len() > MAX_REASONING_CONTENT_BYTES {
        return Err(KnowledgeError::Limit {
            participant: "reasoning.content",
            observed: content.len(),
            limit: MAX_REASONING_CONTENT_BYTES,
        });
    }
    let text =
        std::str::from_utf8(content).map_err(|_| invalid("reasoning.content", "must be UTF-8"))?;
    if content_format == ReasoningContentFormat::ApplicationJson {
        serde_json::from_str::<serde_json::Value>(text)
            .map_err(|_| invalid("reasoning.content", "must be valid JSON"))?;
    }
    Ok(())
}

fn reject_cycle(
    start: Uuid,
    by_id: &HashMap<Uuid, &ReasoningRecord>,
    proven_acyclic: &mut HashSet<Uuid>,
) -> Result<(), KnowledgeError> {
    if proven_acyclic.contains(&start) {
        return Ok(());
    }
    let mut path = Vec::new();
    let mut visited = HashSet::new();
    let mut cursor = Some(start);
    while let Some(current) = cursor {
        if proven_acyclic.contains(&current) {
            break;
        }
        if !visited.insert(current) {
            return Err(invalid(
                "reasoning.supersedes_reasoning_uuid",
                "amendment cycle",
            ));
        }
        path.push(current);
        cursor = by_id
            .get(&current)
            .and_then(|row| row.supersedes_reasoning_uuid);
    }
    proven_acyclic.extend(path);
    Ok(())
}

fn uuid_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), nullable)
}

fn require_v7(value: Uuid, field: &'static str) -> Result<(), KnowledgeError> {
    if value.get_version() != Some(Version::SortRand) {
        return Err(invalid(field, "must be UUIDv7"));
    }
    Ok(())
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

fn binary<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a BinaryArray, KnowledgeError> {
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

    fn row(id: u8, assertion: u8, predecessor: Option<u8>, time: i64) -> ReasoningRecord {
        ReasoningRecord::new(
            uuid7(id),
            uuid7(assertion),
            ReasoningKind::LogicalInference,
            ReasoningContentFormat::TextPlain,
            format!("reasoning-{id}").into_bytes(),
            predecessor.map(uuid7),
            uuid7(id.wrapping_add(100)),
            time,
        )
        .unwrap()
    }

    #[test]
    fn exact_content_chain_round_trips_and_fingerprints_stably() {
        assert_eq!(
            REASONING_SCHEMA_FINGERPRINT
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "f04a1f3f4ce2fa00fb25d56e46bf470c870090504d8b6437197ab41921ffe33e"
        );
        let ledger =
            ReasoningLedger::new(vec![row(2, 20, Some(1), 20), row(1, 20, None, 10)]).unwrap();
        let batch = ledger.batch().unwrap();
        let decoded =
            ReasoningLedger::from_batches(&[batch.slice(0, 1), batch.slice(1, 1)]).unwrap();
        assert_eq!(decoded, ledger);
        assert_eq!(
            decoded.record_fingerprint(uuid7(2)).unwrap(),
            ledger.record_fingerprint(uuid7(2)).unwrap()
        );
        assert_eq!(
            decoded.current_for(uuid7(20)).unwrap().reasoning_uuid,
            uuid7(2)
        );
    }

    #[test]
    fn replay_is_idempotent_and_conflicting_uuid_is_rejected() {
        let base = ReasoningLedger::new(vec![row(1, 20, None, 10)]).unwrap();
        assert_eq!(base.merge(&base).unwrap(), base);
        let mut changed = row(1, 20, None, 10);
        changed.content = b"different".to_vec();
        let conflict = ReasoningLedger::new(vec![changed]).unwrap();
        assert!(matches!(
            base.merge(&conflict),
            Err(KnowledgeError::Conflict("reasoning_uuid"))
        ));
    }

    #[test]
    fn self_cycle_missing_and_cross_assertion_predecessors_fail() {
        let mut self_link = row(1, 20, None, 10);
        self_link.supersedes_reasoning_uuid = Some(uuid7(1));
        assert!(ReasoningLedger::new(vec![self_link]).is_err());
        assert!(matches!(
            ReasoningLedger::new(vec![row(1, 20, Some(2), 10), row(2, 20, Some(1), 20)]),
            Err(KnowledgeError::Invalid {
                field: "reasoning.supersedes_reasoning_uuid",
                message: "amendment cycle",
            })
        ));
        assert!(ReasoningLedger::new(vec![row(2, 20, Some(1), 20)]).is_err());
        assert!(ReasoningLedger::new(vec![row(1, 20, None, 10), row(2, 21, Some(1), 20)]).is_err());
    }

    #[test]
    fn amendment_branch_history_is_preserved_and_current_is_deterministic() {
        let ledger = ReasoningLedger::new(vec![
            row(1, 20, None, 10),
            row(2, 20, Some(1), 20),
            row(3, 20, Some(1), 20),
        ])
        .unwrap();
        assert_eq!(ledger.records.len(), 3);
        assert_eq!(
            ledger.current_for(uuid7(20)).unwrap().reasoning_uuid,
            uuid7(3)
        );
    }

    #[test]
    fn payload_encoding_and_size_limits_are_sanitized() {
        assert!(matches!(
            ReasoningRecord::new(
                uuid7(1),
                uuid7(2),
                ReasoningKind::MethodologicalNote,
                ReasoningContentFormat::TextPlain,
                vec![0xff],
                None,
                uuid7(3),
                1,
            ),
            Err(KnowledgeError::Invalid {
                field: "reasoning.content",
                ..
            })
        ));
        assert!(matches!(
            ReasoningRecord::new(
                uuid7(1),
                uuid7(2),
                ReasoningKind::MethodologicalNote,
                ReasoningContentFormat::ApplicationJson,
                b"{not-json}".to_vec(),
                None,
                uuid7(3),
                1,
            ),
            Err(KnowledgeError::Invalid {
                field: "reasoning.content",
                ..
            })
        ));
        let exact_json = br#"{"explanation":"kept byte-for-byte"}"#.to_vec();
        assert_eq!(
            ReasoningRecord::new(
                uuid7(1),
                uuid7(2),
                ReasoningKind::MethodologicalNote,
                ReasoningContentFormat::ApplicationJson,
                exact_json.clone(),
                None,
                uuid7(3),
                1,
            )
            .unwrap()
            .content,
            exact_json
        );
        let oversized = vec![b'x'; MAX_REASONING_CONTENT_BYTES + 1];
        assert!(matches!(
            ReasoningRecord::new(
                uuid7(1),
                uuid7(2),
                ReasoningKind::MethodologicalNote,
                ReasoningContentFormat::TextPlain,
                oversized,
                None,
                uuid7(3),
                1,
            ),
            Err(KnowledgeError::Limit {
                participant: "reasoning.content",
                ..
            })
        ));
    }
}
