//! Typed Arrow contracts for frozen claims and current authority history.
use super::{
    ClaimRelationKind, ClaimRelationRecord, MAX_RESEARCH_ROWS, ResearchAuthority, ResearchCategory,
    ResearchClaimRecord, ResearchDecisionKind, ResearchDecisionRecord, ResearchSubjectKind,
    ResearchSuppressionRecord,
};
use crate::{
    KnowledgeError, fixed_32_at, fixed_column, invalid, optional_fixed_16, require_schema,
    required_i64, required_text, required_u32, string_column, timestamp_column, u32_column,
    uuid_at, uuid_field,
};
use arrow::{
    array::{
        Array, ArrayRef, FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray,
        UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use std::sync::{Arc, LazyLock};
use uuid::Uuid;

/// Frozen record version for contextual research metadata and decisions.
pub const RESEARCH_RECORD_VERSION: u32 = 1;

/// Authoritative typed claim record schema.
pub static RESEARCH_CLAIM_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("assertion_uuid", false),
        uuid_field("conceptual_uuid", false),
        Field::new("category", DataType::Utf8, false),
        uuid_field("creator_uuid", false),
        uuid_field("run_uuid", true),
        uuid_field("origin_branch_uuid", true),
        uuid_field("origin_version_uuid", true),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

pub(super) fn claim_batch(rows: &[ResearchClaimRecord]) -> Result<RecordBatch, KnowledgeError> {
    RecordBatch::try_new(
        Arc::clone(&RESEARCH_CLAIM_SCHEMA),
        vec![
            uuid_array(rows.iter().map(|row| Some(row.assertion_uuid)))?,
            uuid_array(rows.iter().map(|row| Some(row.conceptual_uuid)))?,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.category.as_str()),
            )),
            uuid_array(rows.iter().map(|row| Some(row.creator_uuid)))?,
            uuid_array(rows.iter().map(|row| row.run_uuid))?,
            uuid_array(rows.iter().map(|row| row.origin_branch_uuid))?,
            uuid_array(rows.iter().map(|row| row.origin_version_uuid))?,
            uuid_array(rows.iter().map(|row| Some(row.provenance_uuid)))?,
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|row| row.recorded_at))
                    .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from(vec![RESEARCH_RECORD_VERSION; rows.len()])),
        ],
    )
    .map_err(Into::into)
}

pub(super) fn claim_rows(
    batches: &[RecordBatch],
) -> Result<Vec<ResearchClaimRecord>, KnowledgeError> {
    let mut rows = Vec::new();
    for batch in batches {
        require_schema(batch, &RESEARCH_CLAIM_SCHEMA, "research.claim.schema")?;
        bound_batch(rows.len(), batch.num_rows())?;
        let assertion_uuid = fixed_column(batch, "assertion_uuid")?;
        let conceptual_uuid = fixed_column(batch, "conceptual_uuid")?;
        let category = string_column(batch, "category")?;
        let creator_uuid = fixed_column(batch, "creator_uuid")?;
        let run_uuid = fixed_column(batch, "run_uuid")?;
        let origin_branch_uuid = fixed_column(batch, "origin_branch_uuid")?;
        let origin_version_uuid = fixed_column(batch, "origin_version_uuid")?;
        let provenance_uuid = fixed_column(batch, "provenance_uuid")?;
        let recorded_at = timestamp_column(batch, "recorded_at")?;
        let contract_version = u32_column(batch, "contract_version")?;
        for row in 0..batch.num_rows() {
            if required_u32(contract_version, row, "contract_version")? != RESEARCH_RECORD_VERSION {
                return Err(invalid("research.contract_version", "unsupported version"));
            }
            rows.push(ResearchClaimRecord {
                assertion_uuid: uuid_at(assertion_uuid, row, "assertion_uuid")?,
                conceptual_uuid: uuid_at(conceptual_uuid, row, "conceptual_uuid")?,
                category: ResearchCategory::parse(required_text(category, row, "category")?)?,
                creator_uuid: uuid_at(creator_uuid, row, "creator_uuid")?,
                run_uuid: optional_fixed_16(run_uuid, row, "run_uuid")?,
                origin_branch_uuid: optional_fixed_16(
                    origin_branch_uuid,
                    row,
                    "origin_branch_uuid",
                )?,
                origin_version_uuid: optional_fixed_16(
                    origin_version_uuid,
                    row,
                    "origin_version_uuid",
                )?,
                provenance_uuid: uuid_at(provenance_uuid, row, "provenance_uuid")?,
                recorded_at: required_i64(recorded_at, row, "recorded_at")?,
            });
        }
    }
    Ok(rows)
}

/// Authoritative typed relation record schema.
pub static CLAIM_RELATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("relation_uuid", false),
        uuid_field("source_assertion_uuid", false),
        uuid_field("target_assertion_uuid", false),
        Field::new("kind", DataType::Utf8, false),
        uuid_field("creator_uuid", false),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

pub(super) fn relation_batch(rows: &[ClaimRelationRecord]) -> Result<RecordBatch, KnowledgeError> {
    RecordBatch::try_new(
        Arc::clone(&CLAIM_RELATION_SCHEMA),
        vec![
            uuid_array(rows.iter().map(|row| Some(row.relation_uuid)))?,
            uuid_array(rows.iter().map(|row| Some(row.source_assertion_uuid)))?,
            uuid_array(rows.iter().map(|row| Some(row.target_assertion_uuid)))?,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.kind.as_str()),
            )),
            uuid_array(rows.iter().map(|row| Some(row.creator_uuid)))?,
            uuid_array(rows.iter().map(|row| Some(row.provenance_uuid)))?,
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|row| row.recorded_at))
                    .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from(vec![RESEARCH_RECORD_VERSION; rows.len()])),
        ],
    )
    .map_err(Into::into)
}

pub(super) fn relation_rows(
    batches: &[RecordBatch],
) -> Result<Vec<ClaimRelationRecord>, KnowledgeError> {
    let mut rows = Vec::new();
    for batch in batches {
        require_schema(batch, &CLAIM_RELATION_SCHEMA, "research.relation.schema")?;
        bound_batch(rows.len(), batch.num_rows())?;
        let relation_uuid = fixed_column(batch, "relation_uuid")?;
        let source_assertion_uuid = fixed_column(batch, "source_assertion_uuid")?;
        let target_assertion_uuid = fixed_column(batch, "target_assertion_uuid")?;
        let kind = string_column(batch, "kind")?;
        let creator_uuid = fixed_column(batch, "creator_uuid")?;
        let provenance_uuid = fixed_column(batch, "provenance_uuid")?;
        let recorded_at = timestamp_column(batch, "recorded_at")?;
        let contract_version = u32_column(batch, "contract_version")?;
        for row in 0..batch.num_rows() {
            if required_u32(contract_version, row, "contract_version")? != RESEARCH_RECORD_VERSION {
                return Err(invalid("research.contract_version", "unsupported version"));
            }
            rows.push(ClaimRelationRecord {
                relation_uuid: uuid_at(relation_uuid, row, "relation_uuid")?,
                source_assertion_uuid: uuid_at(
                    source_assertion_uuid,
                    row,
                    "source_assertion_uuid",
                )?,
                target_assertion_uuid: uuid_at(
                    target_assertion_uuid,
                    row,
                    "target_assertion_uuid",
                )?,
                kind: ClaimRelationKind::parse(required_text(kind, row, "kind")?)?,
                creator_uuid: uuid_at(creator_uuid, row, "creator_uuid")?,
                provenance_uuid: uuid_at(provenance_uuid, row, "provenance_uuid")?,
                recorded_at: required_i64(recorded_at, row, "recorded_at")?,
            });
        }
    }
    Ok(rows)
}

/// Authoritative typed decision record schema.
pub static RESEARCH_DECISION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("sequence", DataType::UInt64, false),
        uuid_field("decision_uuid", false),
        uuid_field("operation_uuid", false),
        Field::new("request_sha256", DataType::FixedSizeBinary(32), false),
        uuid_field("project_uuid", false),
        uuid_field("community_uuid", true),
        uuid_field("context_uuid", false),
        Field::new("subject_kind", DataType::Utf8, false),
        uuid_field("subject_uuid", false),
        Field::new("kind", DataType::Utf8, false),
        uuid_field("creator_uuid", false),
        uuid_field("source_version_uuid", true),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

pub(super) fn decision_batch(
    rows: &[ResearchDecisionRecord],
) -> Result<RecordBatch, KnowledgeError> {
    RecordBatch::try_new(
        Arc::clone(&RESEARCH_DECISION_SCHEMA),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.sequence),
            )),
            uuid_array(rows.iter().map(|row| Some(row.decision_uuid)))?,
            uuid_array(rows.iter().map(|row| Some(row.operation_uuid)))?,
            hash_array(rows.iter().map(|row| row.request_sha256))?,
            uuid_array(rows.iter().map(|row| Some(row.authority.project_uuid)))?,
            uuid_array(rows.iter().map(|row| row.authority.community_uuid))?,
            uuid_array(rows.iter().map(|row| Some(row.authority.context_uuid)))?,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.subject_kind.as_str()),
            )),
            uuid_array(rows.iter().map(|row| Some(row.subject_uuid)))?,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.kind.as_str()),
            )),
            uuid_array(rows.iter().map(|row| Some(row.creator_uuid)))?,
            uuid_array(rows.iter().map(|row| row.source_version_uuid))?,
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|row| row.recorded_at))
                    .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from(vec![RESEARCH_RECORD_VERSION; rows.len()])),
        ],
    )
    .map_err(Into::into)
}

pub(super) fn decision_rows(
    batches: &[RecordBatch],
) -> Result<Vec<ResearchDecisionRecord>, KnowledgeError> {
    let mut rows = Vec::new();
    for batch in batches {
        require_schema(batch, &RESEARCH_DECISION_SCHEMA, "research.decision.schema")?;
        bound_batch(rows.len(), batch.num_rows())?;
        let sequence = u64_column(batch, "sequence")?;
        let decision_uuid = fixed_column(batch, "decision_uuid")?;
        let operation_uuid = fixed_column(batch, "operation_uuid")?;
        let request_sha256 = fixed_column(batch, "request_sha256")?;
        let project_uuid = fixed_column(batch, "project_uuid")?;
        let community_uuid = fixed_column(batch, "community_uuid")?;
        let context_uuid = fixed_column(batch, "context_uuid")?;
        let subject_kind = string_column(batch, "subject_kind")?;
        let subject_uuid = fixed_column(batch, "subject_uuid")?;
        let kind = string_column(batch, "kind")?;
        let creator_uuid = fixed_column(batch, "creator_uuid")?;
        let source_version_uuid = fixed_column(batch, "source_version_uuid")?;
        let recorded_at = timestamp_column(batch, "recorded_at")?;
        let contract_version = u32_column(batch, "contract_version")?;
        for row in 0..batch.num_rows() {
            if required_u32(contract_version, row, "contract_version")? != RESEARCH_RECORD_VERSION {
                return Err(invalid("research.contract_version", "unsupported version"));
            }
            rows.push(ResearchDecisionRecord {
                authority: ResearchAuthority {
                    project_uuid: uuid_at(project_uuid, row, "project_uuid")?,
                    community_uuid: optional_fixed_16(community_uuid, row, "community_uuid")?,
                    context_uuid: uuid_at(context_uuid, row, "context_uuid")?,
                },
                sequence: required_u64(sequence, row, "sequence")?,
                decision_uuid: uuid_at(decision_uuid, row, "decision_uuid")?,
                operation_uuid: uuid_at(operation_uuid, row, "operation_uuid")?,
                request_sha256: fixed_32_at(request_sha256, row, "request_sha256")?,
                subject_kind: ResearchSubjectKind::parse(required_text(
                    subject_kind,
                    row,
                    "subject_kind",
                )?)?,
                subject_uuid: uuid_at(subject_uuid, row, "subject_uuid")?,
                kind: ResearchDecisionKind::parse(required_text(kind, row, "kind")?)?,
                creator_uuid: uuid_at(creator_uuid, row, "creator_uuid")?,
                source_version_uuid: optional_fixed_16(
                    source_version_uuid,
                    row,
                    "source_version_uuid",
                )?,
                recorded_at: required_i64(recorded_at, row, "recorded_at")?,
            });
        }
    }
    Ok(rows)
}

/// Authoritative typed suppression record schema.
pub static CLAIM_SUPPRESSION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("suppression_uuid", false),
        uuid_field("assertion_uuid", false),
        uuid_field("context_uuid", false),
        uuid_field("creator_uuid", false),
        uuid_field("provenance_uuid", false),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

pub(super) fn suppression_batch(
    rows: &[ResearchSuppressionRecord],
) -> Result<RecordBatch, KnowledgeError> {
    RecordBatch::try_new(
        Arc::clone(&CLAIM_SUPPRESSION_SCHEMA),
        vec![
            uuid_array(rows.iter().map(|row| Some(row.suppression_uuid)))?,
            uuid_array(rows.iter().map(|row| Some(row.assertion_uuid)))?,
            uuid_array(rows.iter().map(|row| Some(row.context_uuid)))?,
            uuid_array(rows.iter().map(|row| Some(row.creator_uuid)))?,
            uuid_array(rows.iter().map(|row| Some(row.provenance_uuid)))?,
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|row| row.recorded_at))
                    .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from(vec![RESEARCH_RECORD_VERSION; rows.len()])),
        ],
    )
    .map_err(Into::into)
}

pub(super) fn suppression_rows(
    batches: &[RecordBatch],
) -> Result<Vec<ResearchSuppressionRecord>, KnowledgeError> {
    let mut rows = Vec::new();
    for batch in batches {
        require_schema(
            batch,
            &CLAIM_SUPPRESSION_SCHEMA,
            "research.suppression.schema",
        )?;
        bound_batch(rows.len(), batch.num_rows())?;
        let suppression_uuid = fixed_column(batch, "suppression_uuid")?;
        let assertion_uuid = fixed_column(batch, "assertion_uuid")?;
        let context_uuid = fixed_column(batch, "context_uuid")?;
        let creator_uuid = fixed_column(batch, "creator_uuid")?;
        let provenance_uuid = fixed_column(batch, "provenance_uuid")?;
        let recorded_at = timestamp_column(batch, "recorded_at")?;
        let contract_version = u32_column(batch, "contract_version")?;
        for row in 0..batch.num_rows() {
            if required_u32(contract_version, row, "contract_version")? != RESEARCH_RECORD_VERSION {
                return Err(invalid("research.contract_version", "unsupported version"));
            }
            rows.push(ResearchSuppressionRecord {
                suppression_uuid: uuid_at(suppression_uuid, row, "suppression_uuid")?,
                assertion_uuid: uuid_at(assertion_uuid, row, "assertion_uuid")?,
                context_uuid: uuid_at(context_uuid, row, "context_uuid")?,
                creator_uuid: uuid_at(creator_uuid, row, "creator_uuid")?,
                provenance_uuid: uuid_at(provenance_uuid, row, "provenance_uuid")?,
                recorded_at: required_i64(recorded_at, row, "recorded_at")?,
            });
        }
    }
    Ok(rows)
}

fn uuid_array(rows: impl Iterator<Item = Option<Uuid>>) -> Result<ArrayRef, KnowledgeError> {
    let mut builder = FixedSizeBinaryBuilder::new(16);
    for row in rows {
        match row {
            Some(id) => builder.append_value(id.as_bytes())?,
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}
fn hash_array(rows: impl Iterator<Item = [u8; 32]>) -> Result<ArrayRef, KnowledgeError> {
    let mut builder = FixedSizeBinaryBuilder::new(32);
    for row in rows {
        builder.append_value(row)?;
    }
    Ok(Arc::new(builder.finish()))
}
fn bound_batch(existing: usize, incoming: usize) -> Result<(), KnowledgeError> {
    let observed = existing.saturating_add(incoming);
    if observed > MAX_RESEARCH_ROWS {
        return Err(KnowledgeError::Limit {
            participant: "research",
            observed,
            limit: MAX_RESEARCH_ROWS,
        });
    }
    Ok(())
}
fn u64_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a UInt64Array, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "missing or wrong Arrow type"))
}
fn required_u64(
    array: &UInt64Array,
    row: usize,
    field: &'static str,
) -> Result<u64, KnowledgeError> {
    if array.is_null(row) {
        return Err(invalid(field, "must not be null"));
    }
    Ok(array.value(row))
}
