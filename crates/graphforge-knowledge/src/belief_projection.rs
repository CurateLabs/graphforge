//! Append-only epistemic attachments connecting resolved interpretation to completed knowledge runs.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder, ListArray,
    ListBuilder, TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt32Array,
    UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, MAX_CANONICAL_BINARY_BYTES,
    fingerprint,
};
use uuid::{Uuid, Version};

use crate::{
    EPISTEMIC_CAPABILITY_VERSION, KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry,
};

/// Frozen `belief_projection_attachment/1` record contract.
pub const BELIEF_PROJECTION_ATTACHMENT_CONTRACT_VERSION: u32 = 1;

/// Authoritative `algorithm_interpretation_attachments` schema.
pub static ALGORITHM_INTERPRETATION_ATTACHMENT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let uuid_list = DataType::List(Arc::new(Field::new(
        "item",
        DataType::FixedSizeBinary(16),
        false,
    )));
    Arc::new(Schema::new(vec![
        uuid_field("attachment_uuid", false),
        uuid_field("run_uuid", false),
        uuid_field("source_generation_uuid", false),
        timestamp_field("transaction_cutoff", false),
        timestamp_field("valid_time", true),
        Field::new("policy_version", DataType::UInt32, false),
        Field::new("policy_bytes", DataType::Binary, false),
        fingerprint_field("policy_fingerprint", false),
        fingerprint_field("snapshot_fingerprint", false),
        fingerprint_field("valid_time_fingerprint", true),
        fingerprint_field("graph_content_fingerprint", false),
        fingerprint_field("descriptor_fingerprint", false),
        Field::new("source_record_uuids", uuid_list, false),
        uuid_field("provenance_uuid", false),
        timestamp_field("recorded_at", false),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"belief_projection_attachment/1|attachment_uuid:fixed[16]:required|run_uuid:fixed[16]:required|source_generation_uuid:fixed[16]:required|transaction_cutoff:timestamp_us_utc:required|valid_time:timestamp_us_utc:nullable|policy_version:u32:required|policy_bytes:binary:required|policy_fingerprint:fixed[32]:required|snapshot_fingerprint:fixed[32]:required|valid_time_fingerprint:fixed[32]:nullable|graph_content_fingerprint:fixed[32]:required|descriptor_fingerprint:fixed[32]:required|source_record_uuids:list<fixed[16]>:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered belief-projection attachment schema is bounded")
});

/// One immutable interpretation attachment for a completed algorithm run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeliefProjectionAttachment {
    /// Stable attachment identity and idempotency key.
    pub attachment_uuid: Uuid,
    /// Completed knowledge run identity.
    pub run_uuid: Uuid,
    /// Source project generation pinned during resolution.
    pub source_generation_uuid: Uuid,
    /// Mandatory transaction-time cutoff.
    pub transaction_cutoff_micros: i64,
    /// Optional valid time applied after transaction reconstruction.
    pub valid_time_micros: Option<i64>,
    /// Explicit resolved-belief policy version.
    pub policy_version: u32,
    /// Canonical language-neutral policy bytes.
    pub policy_bytes: Vec<u8>,
    /// Canonical fingerprint of `policy_bytes`.
    pub policy_fingerprint: [u8; 32],
    /// Fingerprint of the transaction-time epistemic snapshot.
    pub snapshot_fingerprint: [u8; 32],
    /// Fingerprint of the optional valid-time interpretation.
    pub valid_time_fingerprint: Option<[u8; 32]>,
    /// Canonical logical graph-content fingerprint.
    pub graph_content_fingerprint: [u8; 32],
    /// Neutral algorithm invocation-descriptor fingerprint.
    pub descriptor_fingerprint: [u8; 32],
    /// Sorted and deduplicated decision-relevant epistemic records.
    pub source_record_uuids: Vec<Uuid>,
    /// Producing provenance event.
    pub provenance_uuid: Uuid,
    /// Mandatory attachment transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl BeliefProjectionAttachment {
    /// Validate and construct an attachment, canonicalizing source UUIDs.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        attachment_uuid: Uuid,
        run_uuid: Uuid,
        source_generation_uuid: Uuid,
        transaction_cutoff_micros: i64,
        valid_time_micros: Option<i64>,
        policy_version: u32,
        policy_bytes: Vec<u8>,
        snapshot_fingerprint: [u8; 32],
        valid_time_fingerprint: Option<[u8; 32]>,
        graph_content_fingerprint: [u8; 32],
        descriptor_fingerprint: [u8; 32],
        mut source_record_uuids: Vec<Uuid>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        source_record_uuids.sort_unstable();
        source_record_uuids.dedup();
        let policy_fingerprint = fingerprint(
            CanonicalDomain::BeliefProjectionPolicy,
            CANONICAL_CONTRACT_VERSION,
            &policy_bytes,
        )?;
        let row = Self {
            attachment_uuid,
            run_uuid,
            source_generation_uuid,
            transaction_cutoff_micros,
            valid_time_micros,
            policy_version,
            policy_bytes,
            policy_fingerprint,
            snapshot_fingerprint,
            valid_time_fingerprint,
            graph_content_fingerprint,
            descriptor_fingerprint,
            source_record_uuids,
            provenance_uuid,
            recorded_at_micros,
            contract_version: BELIEF_PROJECTION_ATTACHMENT_CONTRACT_VERSION,
        };
        validate(&row)?;
        Ok(row)
    }
}

/// Validated append-only interpretation-attachment participant.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BeliefProjectionAttachmentLedger {
    /// Attachments ordered by `(recorded_at, attachment_uuid)`.
    pub attachments: Vec<BeliefProjectionAttachment>,
}

impl BeliefProjectionAttachmentLedger {
    /// Validate, sort, and construct a complete participant.
    pub fn new(mut attachments: Vec<BeliefProjectionAttachment>) -> Result<Self, KnowledgeError> {
        if attachments.len() > MAX_KNOWLEDGE_ROWS {
            return Err(KnowledgeError::Limit {
                participant: "algorithm_interpretation_attachments",
                observed: attachments.len(),
                limit: MAX_KNOWLEDGE_ROWS,
            });
        }
        let mut ids = HashSet::with_capacity(attachments.len());
        for row in &attachments {
            validate(row)?;
            if !ids.insert(row.attachment_uuid) {
                return Err(KnowledgeError::Duplicate("attachment_uuid"));
            }
        }
        attachments.sort_by_key(|row| (row.recorded_at_micros, row.attachment_uuid));
        Ok(Self { attachments })
    }

    /// Merge with exact replay and transaction-conflict semantics.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut rows = self.attachments.clone();
        let mut by_id = rows
            .iter()
            .cloned()
            .map(|row| (row.attachment_uuid, row))
            .collect::<HashMap<_, _>>();
        for row in &staged.attachments {
            if let Some(existing) = by_id.get(&row.attachment_uuid) {
                if existing != row {
                    return Err(KnowledgeError::TransactionConflict("attachment_uuid"));
                }
            } else {
                rows.push(row.clone());
                by_id.insert(row.attachment_uuid, row.clone());
            }
        }
        Self::new(rows)
    }

    /// Canonical fingerprint over one exact immutable attachment.
    pub fn attachment_fingerprint(&self, id: Uuid) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .attachments
            .iter()
            .find(|row| row.attachment_uuid == id)
            .ok_or(KnowledgeError::Dangling("attachment_uuid"))?;
        let mut writer = CanonicalWriter::new();
        writer.raw(row.attachment_uuid.as_bytes())?;
        writer.raw(row.run_uuid.as_bytes())?;
        writer.raw(row.source_generation_uuid.as_bytes())?;
        writer.i64(row.transaction_cutoff_micros)?;
        optional_i64(&mut writer, row.valid_time_micros)?;
        writer.u32(row.policy_version)?;
        writer.binary(&row.policy_bytes)?;
        writer.raw(&row.policy_fingerprint)?;
        writer.raw(&row.snapshot_fingerprint)?;
        optional_fingerprint(&mut writer, row.valid_time_fingerprint)?;
        writer.raw(&row.graph_content_fingerprint)?;
        writer.raw(&row.descriptor_fingerprint)?;
        writer.u64(u64::try_from(row.source_record_uuids.len()).unwrap_or(u64::MAX))?;
        for source in &row.source_record_uuids {
            writer.raw(source.as_bytes())?;
        }
        writer.raw(row.provenance_uuid.as_bytes())?;
        writer.i64(row.recorded_at_micros)?;
        writer.u32(row.contract_version)?;
        fingerprint(
            CanonicalDomain::BeliefProjectionAttachment,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )
        .map_err(Into::into)
    }

    /// Build the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        let len = self.attachments.len();
        let mut attachment = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut run = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut generation = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut cutoff = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut valid_time = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut policy_version = UInt32Builder::new();
        let mut policy_bytes = BinaryBuilder::new();
        let mut policy_fp = FixedSizeBinaryBuilder::with_capacity(len, 32);
        let mut snapshot_fp = FixedSizeBinaryBuilder::with_capacity(len, 32);
        let mut valid_fp = FixedSizeBinaryBuilder::with_capacity(len, 32);
        let mut graph_fp = FixedSizeBinaryBuilder::with_capacity(len, 32);
        let mut descriptor_fp = FixedSizeBinaryBuilder::with_capacity(len, 32);
        let mut sources = ListBuilder::new(FixedSizeBinaryBuilder::new(16)).with_field(Arc::new(
            Field::new("item", DataType::FixedSizeBinary(16), false),
        ));
        let mut provenance = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut recorded = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut version = UInt32Builder::new();
        for row in &self.attachments {
            append(&mut attachment, row.attachment_uuid.as_bytes())?;
            append(&mut run, row.run_uuid.as_bytes())?;
            append(&mut generation, row.source_generation_uuid.as_bytes())?;
            cutoff.append_value(row.transaction_cutoff_micros);
            valid_time.append_option(row.valid_time_micros);
            policy_version.append_value(row.policy_version);
            policy_bytes.append_value(&row.policy_bytes);
            append(&mut policy_fp, &row.policy_fingerprint)?;
            append(&mut snapshot_fp, &row.snapshot_fingerprint)?;
            append_optional(&mut valid_fp, row.valid_time_fingerprint.as_ref())?;
            append(&mut graph_fp, &row.graph_content_fingerprint)?;
            append(&mut descriptor_fp, &row.descriptor_fingerprint)?;
            for source in &row.source_record_uuids {
                append(sources.values(), source.as_bytes())?;
            }
            sources.append(true);
            append(&mut provenance, row.provenance_uuid.as_bytes())?;
            recorded.append_value(row.recorded_at_micros);
            version.append_value(row.contract_version);
        }
        Ok(RecordBatch::try_new(
            Arc::clone(&ALGORITHM_INTERPRETATION_ATTACHMENT_SCHEMA),
            vec![
                Arc::new(attachment.finish()),
                Arc::new(run.finish()),
                Arc::new(generation.finish()),
                Arc::new(cutoff.finish()),
                Arc::new(valid_time.finish()),
                Arc::new(policy_version.finish()),
                Arc::new(policy_bytes.finish()),
                Arc::new(policy_fp.finish()),
                Arc::new(snapshot_fp.finish()),
                Arc::new(valid_fp.finish()),
                Arc::new(graph_fp.finish()),
                Arc::new(descriptor_fp.finish()),
                Arc::new(sources.finish()),
                Arc::new(provenance.finish()),
                Arc::new(recorded.finish()),
                Arc::new(version.finish()),
            ],
        )?)
    }

    /// Decode exact Arrow batches and re-run every invariant.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut rows = Vec::new();
        for batch in batches {
            if batch.schema().as_ref() != ALGORITHM_INTERPRETATION_ATTACHMENT_SCHEMA.as_ref() {
                return Err(invalid(
                    "belief_projection_attachment.schema",
                    "schema mismatch",
                ));
            }
            let attachment = fixed(batch, "attachment_uuid")?;
            let run = fixed(batch, "run_uuid")?;
            let generation = fixed(batch, "source_generation_uuid")?;
            let cutoff = timestamp(batch, "transaction_cutoff")?;
            let valid_time = timestamp(batch, "valid_time")?;
            let policy_version = uint32(batch, "policy_version")?;
            let policy_bytes = binary(batch, "policy_bytes")?;
            let policy_fp = fixed(batch, "policy_fingerprint")?;
            let snapshot_fp = fixed(batch, "snapshot_fingerprint")?;
            let valid_fp = fixed(batch, "valid_time_fingerprint")?;
            let graph_fp = fixed(batch, "graph_content_fingerprint")?;
            let descriptor_fp = fixed(batch, "descriptor_fingerprint")?;
            let sources = list(batch, "source_record_uuids")?;
            let provenance = fixed(batch, "provenance_uuid")?;
            let recorded = timestamp(batch, "recorded_at")?;
            let versions = uint32(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                rows.push(BeliefProjectionAttachment {
                    attachment_uuid: uuid_at(attachment, row, "attachment_uuid")?,
                    run_uuid: uuid_at(run, row, "run_uuid")?,
                    source_generation_uuid: uuid_at(generation, row, "source_generation_uuid")?,
                    transaction_cutoff_micros: cutoff.value(row),
                    valid_time_micros: optional_timestamp(valid_time, row),
                    policy_version: policy_version.value(row),
                    policy_bytes: policy_bytes.value(row).to_vec(),
                    policy_fingerprint: bytes32(policy_fp, row, "policy_fingerprint")?,
                    snapshot_fingerprint: bytes32(snapshot_fp, row, "snapshot_fingerprint")?,
                    valid_time_fingerprint: optional_bytes32(
                        valid_fp,
                        row,
                        "valid_time_fingerprint",
                    )?,
                    graph_content_fingerprint: bytes32(graph_fp, row, "graph_content_fingerprint")?,
                    descriptor_fingerprint: bytes32(descriptor_fp, row, "descriptor_fingerprint")?,
                    source_record_uuids: uuid_list_at(sources, row)?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: recorded.value(row),
                    contract_version: versions.value(row),
                });
            }
        }
        Self::new(rows)
    }
}

pub(crate) fn schema_registry_entry() -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "epistemic",
        capability_version: EPISTEMIC_CAPABILITY_VERSION,
        record_family: "algorithm_interpretation_attachments",
        record_version: BELIEF_PROJECTION_ATTACHMENT_CONTRACT_VERSION,
        schema: Arc::clone(&ALGORITHM_INTERPRETATION_ATTACHMENT_SCHEMA),
        schema_fingerprint: *SCHEMA_FINGERPRINT,
        enum_registry_versions: &[],
        sort_key: &["recorded_at", "attachment_uuid"],
        diff_identity_fields: &["attachment_uuid"],
        diff_record_uuid_field: Some("attachment_uuid"),
        fingerprint_domain: CanonicalDomain::BeliefProjectionAttachment,
        owner: "graphforge-knowledge",
        implementation_issue: 2004,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn validate(row: &BeliefProjectionAttachment) -> Result<(), KnowledgeError> {
    if row.contract_version != BELIEF_PROJECTION_ATTACHMENT_CONTRACT_VERSION {
        return Err(invalid(
            "belief_projection_attachment.contract_version",
            "unsupported version",
        ));
    }
    require_v7(row.attachment_uuid, "attachment_uuid")?;
    require_v7(row.run_uuid, "run_uuid")?;
    require_uuid(row.source_generation_uuid, "source_generation_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if row.policy_version == 0 {
        return Err(invalid(
            "belief_projection_attachment.policy_version",
            "must be positive",
        ));
    }
    if u64::try_from(row.policy_bytes.len()).unwrap_or(u64::MAX) > MAX_CANONICAL_BINARY_BYTES {
        return Err(KnowledgeError::Limit {
            participant: "policy_bytes",
            observed: row.policy_bytes.len(),
            limit: usize::try_from(MAX_CANONICAL_BINARY_BYTES).unwrap_or(usize::MAX),
        });
    }
    let expected = fingerprint(
        CanonicalDomain::BeliefProjectionPolicy,
        CANONICAL_CONTRACT_VERSION,
        &row.policy_bytes,
    )?;
    if row.policy_fingerprint != expected {
        return Err(invalid(
            "belief_projection_attachment.policy_fingerprint",
            "does not match policy bytes",
        ));
    }
    if row
        .source_record_uuids
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid(
            "belief_projection_attachment.source_record_uuids",
            "must be sorted and deduplicated",
        ));
    }
    for source in &row.source_record_uuids {
        require_uuid(*source, "source_record_uuid")?;
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
fn optional_fingerprint(
    writer: &mut CanonicalWriter,
    value: Option<[u8; 32]>,
) -> Result<(), KnowledgeError> {
    match value {
        Some(value) => {
            writer.u8(1)?;
            writer.raw(&value)?;
        }
        None => writer.u8(0)?,
    }
    Ok(())
}
fn append(builder: &mut FixedSizeBinaryBuilder, value: &[u8]) -> Result<(), KnowledgeError> {
    builder
        .append_value(value)
        .map_err(|_| invalid("belief_projection_attachment", "invalid fixed-width value"))
}
fn append_optional(
    builder: &mut FixedSizeBinaryBuilder,
    value: Option<&[u8; 32]>,
) -> Result<(), KnowledgeError> {
    if let Some(value) = value {
        append(builder, value)
    } else {
        builder.append_null();
        Ok(())
    }
}
fn uuid_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), nullable)
}
fn fingerprint_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(32), nullable)
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
        .and_then(|v| v.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}
fn timestamp<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a TimestampMicrosecondArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|v| v.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}
fn uint32<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a UInt32Array, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|v| v.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}
fn binary<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a BinaryArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|v| v.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}
fn list<'a>(batch: &'a RecordBatch, name: &'static str) -> Result<&'a ListArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|v| v.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}
fn uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Uuid, KnowledgeError> {
    Uuid::from_slice(values.value(row)).map_err(|_| invalid(field, "invalid UUID bytes"))
}
fn bytes32(
    values: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<[u8; 32], KnowledgeError> {
    values
        .value(row)
        .try_into()
        .map_err(|_| invalid(field, "invalid fingerprint width"))
}
fn optional_bytes32(
    values: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Option<[u8; 32]>, KnowledgeError> {
    (!values.is_null(row))
        .then(|| bytes32(values, row, field))
        .transpose()
}
fn optional_timestamp(values: &TimestampMicrosecondArray, row: usize) -> Option<i64> {
    (!values.is_null(row)).then(|| values.value(row))
}
fn uuid_list_at(values: &ListArray, row: usize) -> Result<Vec<Uuid>, KnowledgeError> {
    let value = values.value(row);
    let fixed = value
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| invalid("source_record_uuids", "item type mismatch"))?;
    (0..fixed.len())
        .map(|index| uuid_at(fixed, index, "source_record_uuid"))
        .collect()
}
fn require_v7(value: Uuid, field: &'static str) -> Result<(), KnowledgeError> {
    if value.get_version() != Some(Version::SortRand) {
        return Err(invalid(field, "must be UUIDv7"));
    }
    require_uuid(value, field)
}
fn require_uuid(value: Uuid, field: &'static str) -> Result<(), KnowledgeError> {
    if value.is_nil() {
        Err(invalid(field, "must not be nil"))
    } else {
        Ok(())
    }
}
const fn invalid(field: &'static str, message: &'static str) -> KnowledgeError {
    KnowledgeError::Invalid { field, message }
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
    fn attachment(id: u8, sources: Vec<Uuid>) -> BeliefProjectionAttachment {
        BeliefProjectionAttachment::new(
            uuid7(id),
            uuid7(id + 1),
            uuid7(id + 2),
            10,
            Some(11),
            1,
            b"policy-v1".to_vec(),
            [3; 32],
            Some([4; 32]),
            [5; 32],
            [6; 32],
            sources,
            uuid7(id + 3),
            20,
        )
        .unwrap()
    }

    #[test]
    fn sources_are_canonical_and_arrow_round_trip_is_exact() {
        let row = attachment(1, vec![uuid7(9), uuid7(8), uuid7(9)]);
        assert_eq!(row.source_record_uuids, vec![uuid7(8), uuid7(9)]);
        let ledger = BeliefProjectionAttachmentLedger::new(vec![row]).unwrap();
        let decoded =
            BeliefProjectionAttachmentLedger::from_batches(&[ledger.batch().unwrap()]).unwrap();
        assert_eq!(decoded, ledger);
        assert_eq!(
            decoded.attachment_fingerprint(uuid7(1)).unwrap(),
            ledger.attachment_fingerprint(uuid7(1)).unwrap()
        );
    }

    #[test]
    fn replay_is_idempotent_and_conflict_has_transaction_code() {
        let row = attachment(10, vec![]);
        let ledger = BeliefProjectionAttachmentLedger::new(vec![row.clone()]).unwrap();
        assert_eq!(
            ledger
                .merge(&BeliefProjectionAttachmentLedger::new(vec![row.clone()]).unwrap())
                .unwrap(),
            ledger
        );
        let mut different = row;
        different.graph_content_fingerprint = [99; 32];
        let error = ledger
            .merge(&BeliefProjectionAttachmentLedger {
                attachments: vec![different],
            })
            .unwrap_err();
        assert_eq!(error.code(), "GF_TRANSACTION_CONFLICT");
    }

    #[test]
    fn registry_is_epistemic_owned_and_frozen() {
        let entry = schema_registry_entry();
        assert_eq!(entry.capability_id, "epistemic");
        assert_eq!(entry.record_family, "algorithm_interpretation_attachments");
        assert_eq!(entry.sort_key, &["recorded_at", "attachment_uuid"]);
        assert_eq!(entry.implementation_issue, 2004);
    }

    #[test]
    fn defensive_attachment_validation_and_optional_fingerprints_are_exact() {
        let row = attachment(30, vec![uuid7(40)]);
        assert!(matches!(
            BeliefProjectionAttachmentLedger::new(vec![row.clone(), row.clone()]),
            Err(KnowledgeError::Duplicate("attachment_uuid"))
        ));

        let mut invalid = row.clone();
        invalid.contract_version += 1;
        assert!(
            validate(&invalid)
                .unwrap_err()
                .to_string()
                .contains("unsupported version")
        );
        invalid = row.clone();
        invalid.policy_version = 0;
        assert!(
            validate(&invalid)
                .unwrap_err()
                .to_string()
                .contains("must be positive")
        );
        invalid = row.clone();
        invalid.policy_fingerprint = [0; 32];
        assert!(
            validate(&invalid)
                .unwrap_err()
                .to_string()
                .contains("does not match policy bytes")
        );
        invalid = row.clone();
        invalid.source_record_uuids = vec![uuid7(42), uuid7(41)];
        assert!(
            validate(&invalid)
                .unwrap_err()
                .to_string()
                .contains("sorted and deduplicated")
        );
        invalid = row;
        invalid.source_record_uuids = vec![Uuid::nil()];
        assert!(
            validate(&invalid)
                .unwrap_err()
                .to_string()
                .contains("must not be nil")
        );

        let without_optional = BeliefProjectionAttachment::new(
            uuid7(50),
            uuid7(51),
            uuid7(52),
            10,
            None,
            1,
            b"policy-v1".to_vec(),
            [3; 32],
            None,
            [5; 32],
            [6; 32],
            vec![],
            uuid7(53),
            20,
        )
        .unwrap();
        BeliefProjectionAttachmentLedger::new(vec![without_optional])
            .unwrap()
            .attachment_fingerprint(uuid7(50))
            .unwrap();

        let wrong = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert!(
            BeliefProjectionAttachmentLedger::from_batches(&[wrong])
                .unwrap_err()
                .to_string()
                .contains("schema mismatch")
        );
    }
}
