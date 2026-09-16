//! Checkpoint restoration identity and publication records.

use super::{
    Arc, ArrayRef, ArrowWriter, CANONICAL_CONTRACT_VERSION, CanonicalDomain, DataType, Field,
    FixedSizeBinaryBuilder, GfError, ProjectParticipant, ProjectParticipantEncoding,
    RESTORATION_CONTRACT_VERSION, RESTORATION_FAMILY, RecordBatch, Schema, Sha256, StringArray,
    TimeUnit, TimestampMicrosecondArray, UInt32Array, Uuid, append_actor, append_bytes,
    fingerprint, registry_corrupt,
};
use sha2::Digest as _;

pub(super) fn revert_request_digest(
    operation_uuid: Uuid,
    name: &str,
    checkpoint_uuid: Uuid,
    source_generation_uuid: Uuid,
    source_manifest_sha256: [u8; 32],
    reason: &str,
    actor_uuid: Option<Uuid>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-checkpoint-revert-request/1");
    hasher.update(operation_uuid.as_bytes());
    append_bytes(&mut hasher, name.as_bytes());
    hasher.update(checkpoint_uuid.as_bytes());
    hasher.update(source_generation_uuid.as_bytes());
    hasher.update(source_manifest_sha256);
    append_bytes(&mut hasher, reason.as_bytes());
    append_actor(&mut hasher, actor_uuid);
    hasher.finalize().into()
}

pub(super) fn revert_transaction_uuid(operation_uuid: Uuid) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-checkpoint-revert-transaction/1");
    hasher.update(operation_uuid.as_bytes());
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

pub(super) fn restoration_uuid(operation_uuid: Uuid, request_digest: [u8; 32]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-restoration-transition-uuid/1");
    hasher.update(operation_uuid.as_bytes());
    hasher.update(request_digest);
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

pub(super) fn restored_generation_uuid(
    transaction_uuid: Uuid,
    checkpoint_uuid: Uuid,
    source_generation_uuid: Uuid,
    source_manifest_sha256: [u8; 32],
    prior_current_generation_uuid: Uuid,
    restored_at: i64,
    request_digest: [u8; 32],
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-checkpoint-restored-generation/1");
    hasher.update(transaction_uuid.as_bytes());
    hasher.update(checkpoint_uuid.as_bytes());
    hasher.update(source_generation_uuid.as_bytes());
    hasher.update(source_manifest_sha256);
    hasher.update(prior_current_generation_uuid.as_bytes());
    hasher.update(restored_at.to_be_bytes());
    hasher.update(request_digest);
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

pub(super) fn snapshot_to_participant(
    snapshot: crate::ProjectParticipantSnapshot,
) -> Result<ProjectParticipant, GfError> {
    let encoding = match snapshot.encoding.as_str() {
        "parquet" => ProjectParticipantEncoding::Parquet,
        "arrow" => ProjectParticipantEncoding::Arrow,
        "json" => ProjectParticipantEncoding::Json,
        _ => {
            return Err(registry_corrupt(
                "checkpoint participant encoding is unsupported",
            ));
        }
    };
    Ok(ProjectParticipant {
        capability_id: snapshot.capability_id,
        capability_version: snapshot.capability_version,
        record_family_id: snapshot.record_family_id,
        record_version: snapshot.record_version,
        encoding,
        schema_fingerprint: snapshot.schema_fingerprint,
        row_count: snapshot.row_count,
        bytes: snapshot.bytes,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn restoration_participant(
    restoration_uuid: Uuid,
    checkpoint_uuid: Uuid,
    source_generation_uuid: Uuid,
    source_manifest_sha256: [u8; 32],
    prior_current_generation_uuid: Uuid,
    restored_generation_uuid: Uuid,
    operation_uuid: Uuid,
    actor_uuid: Option<Uuid>,
    reason: &str,
    restored_at: i64,
) -> Result<ProjectParticipant, GfError> {
    let schema = Arc::new(Schema::new(vec![
        uuid_field("restoration_uuid", false),
        uuid_field("checkpoint_uuid", false),
        uuid_field("source_generation_uuid", false),
        Field::new(
            "source_manifest_sha256",
            DataType::FixedSizeBinary(32),
            false,
        ),
        uuid_field("prior_current_generation_uuid", false),
        uuid_field("restored_generation_uuid", false),
        uuid_field("operation_uuid", false),
        uuid_field("actor_uuid", true),
        Field::new("reason", DataType::Utf8, false),
        Field::new(
            "restored_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]));
    let mut columns = Vec::<ArrayRef>::new();
    for value in [
        Some(restoration_uuid),
        Some(checkpoint_uuid),
        Some(source_generation_uuid),
        Some(prior_current_generation_uuid),
        Some(restored_generation_uuid),
        Some(operation_uuid),
        actor_uuid,
    ] {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(1, 16);
        match value {
            Some(uuid) => builder.append_value(uuid.as_bytes()).map_err(arrow_error)?,
            None => builder.append_null(),
        }
        columns.push(Arc::new(builder.finish()));
    }
    let mut source_digest = FixedSizeBinaryBuilder::with_capacity(1, 32);
    source_digest
        .append_value(source_manifest_sha256)
        .map_err(arrow_error)?;
    columns.insert(3, Arc::new(source_digest.finish()));
    columns.push(Arc::new(StringArray::from(vec![reason])));
    columns.push(Arc::new(
        TimestampMicrosecondArray::from(vec![restored_at]).with_timezone("UTC"),
    ));
    columns.push(Arc::new(UInt32Array::from(vec![
        RESTORATION_CONTRACT_VERSION,
    ])));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(arrow_error)?;
    let properties = crate::permanent_parquet::writer_properties()
        .set_created_by("graphforge-restoration-transition/1".into())
        .build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), schema, Some(properties)).map_err(parquet_error)?;
    writer.write(&batch).map_err(parquet_error)?;
    let bytes = writer.into_inner().map_err(parquet_error)?;
    let schema_fingerprint = fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"restoration_transition/1|restoration_uuid:fixed[16]:required|checkpoint_uuid:fixed[16]:required|source_generation_uuid:fixed[16]:required|source_manifest_sha256:fixed[32]:required|prior_current_generation_uuid:fixed[16]:required|restored_generation_uuid:fixed[16]:required|operation_uuid:fixed[16]:required|actor_uuid:fixed[16]:optional|reason:utf8:required|restored_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .map_err(|error| GfError::Validation(error.to_string()))?;
    Ok(ProjectParticipant {
        capability_id: crate::WORKSPACE_CAPABILITY_ID.into(),
        capability_version: crate::WORKSPACE_CAPABILITY_VERSION,
        record_family_id: RESTORATION_FAMILY.into(),
        record_version: RESTORATION_CONTRACT_VERSION,
        encoding: ProjectParticipantEncoding::Parquet,
        schema_fingerprint,
        row_count: 1,
        bytes,
    })
}

fn uuid_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), nullable)
}

fn arrow_error(error: arrow::error::ArrowError) -> GfError {
    let message = format!("restoration Arrow encoding failed: {error}");
    drop(error);
    GfError::Storage(message)
}

fn parquet_error(error: parquet::errors::ParquetError) -> GfError {
    let message = format!("restoration Parquet encoding failed: {error}");
    drop(error);
    GfError::Storage(message)
}

#[cfg(test)]
mod tests;
