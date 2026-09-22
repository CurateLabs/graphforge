//! Arrow data and request/endpoint-bound deterministic continuations.
use super::{ResearchComparisonDetail, ResearchComparisonRequest, delta::Row, state::State};
use crate::{ExecutionResult, GfError};
use arrow::{
    array::{ArrayRef, BooleanArray, FixedSizeBinaryBuilder, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;
fn ids(values: impl Iterator<Item = Option<Uuid>>) -> Result<ArrayRef, GfError> {
    let mut b = FixedSizeBinaryBuilder::new(16);
    for id in values {
        match id {
            Some(id) => b
                .append_value(id.as_bytes())
                .map_err(|_| super::invalid("invalid comparison identity"))?,
            None => b.append_null(),
        }
    }
    Ok(Arc::new(b.finish()))
}
fn hashes(values: impl Iterator<Item = Option<[u8; 32]>>) -> Result<ArrayRef, GfError> {
    let mut b = FixedSizeBinaryBuilder::new(32);
    for id in values {
        match id {
            Some(id) => b
                .append_value(id)
                .map_err(|_| super::invalid("invalid comparison commitment"))?,
            None => b.append_null(),
        }
    }
    Ok(Arc::new(b.finish()))
}
fn endpoint(state: &State) -> serde_json::Value {
    serde_json::json!({"version_uuid":state.version,"generation_uuid":state.generation,"context_uuid":state.context,"branch_uuid":state.branch})
}
pub(super) fn render(
    request: &ResearchComparisonRequest,
    left: &State,
    right: &State,
    owner: Uuid,
    rows: &[Row],
) -> Result<ExecutionResult, GfError> {
    let mut binding_request = request.clone();
    binding_request.after = None;
    let binding=serde_json::to_vec(&serde_json::json!({"request":binding_request,"left":endpoint(left),"right":endpoint(right),"authority":if request.left_authority.is_some()||request.right_authority.is_some(){Some(owner)}else{None}})).map_err(|_|super::invalid("invalid comparison binding"))?;
    let mut digest = Sha256::new();
    digest.update(binding);
    // Retention-dependent rows may change even when both endpoint Versions are
    // immutable. Hash each row without allocating another full result buffer.
    for row in rows {
        digest.update(
            serde_json::to_vec(row)
                .map_err(|_| super::invalid("invalid comparison row binding"))?,
        );
    }
    let binding = super::hex(&digest.finalize());
    let offset = match request.after.as_deref() {
        None => 0,
        Some(token) => {
            let (identity, position) = token.split_once(':').ok_or_else(stale)?;
            if identity != binding {
                return Err(stale());
            }
            position.parse::<usize>().map_err(|_| stale())?
        }
    };
    if offset > rows.len() {
        return Err(stale());
    }
    let end = offset.saturating_add(request.page_size).min(rows.len());
    let metadata = HashMap::from([
        ("graphforge.comparison.contract".into(), "1".into()),
        (
            "graphforge.comparison.left".into(),
            endpoint(left).to_string(),
        ),
        (
            "graphforge.comparison.right".into(),
            endpoint(right).to_string(),
        ),
        (
            "graphforge.comparison.authority_generation".into(),
            owner.to_string(),
        ),
        (
            "graphforge.comparison.total_rows".into(),
            rows.len().to_string(),
        ),
        (
            "graphforge.comparison.next_cursor".into(),
            if end < rows.len() {
                format!("{binding}:{end}")
            } else {
                String::new()
            },
        ),
    ]);
    let batch = match request.detail {
        ResearchComparisonDetail::Changes => changes(&rows[offset..end], metadata)?,
        ResearchComparisonDetail::Summary => summary(left, right, rows, metadata)?,
    };
    if batch.get_array_memory_size() > request.max_bytes {
        return Err(super::limit());
    }
    Ok(crate::knowledge::assertion_result(batch))
}
fn changes(rows: &[Row], metadata: HashMap<String, String>) -> Result<RecordBatch, GfError> {
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (name, values) in [
        (
            "object_kind",
            rows.iter().map(|r| r.key.0.as_str()).collect::<Vec<_>>(),
        ),
        ("field", rows.iter().map(|r| r.key.2.as_str()).collect()),
        ("change", rows.iter().map(|r| r.change).collect()),
        ("local_state", rows.iter().map(|r| r.disposition).collect()),
        ("detail", rows.iter().map(|r| r.detail.as_str()).collect()),
    ] {
        fields.push(Field::new(name, DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from(values)));
    }
    for (name, values, nullable) in [
        (
            "object_uuid",
            rows.iter().map(|r| Some(r.key.1)).collect::<Vec<_>>(),
            false,
        ),
        (
            "origin_version_uuid",
            rows.iter().map(|r| r.origin).collect(),
            true,
        ),
        (
            "incorporated_version_uuid",
            rows.iter().map(|r| r.incorporated).collect(),
            true,
        ),
        (
            "contribution_uuid",
            rows.iter().map(|r| r.contribution).collect(),
            true,
        ),
        (
            "accepted_source_version_uuid",
            rows.iter().map(|r| r.accepted_source).collect(),
            true,
        ),
        (
            "accepted_destination_version_uuid",
            rows.iter().map(|r| r.accepted_destination).collect(),
            true,
        ),
    ] {
        fields.push(Field::new(name, DataType::FixedSizeBinary(16), nullable));
        columns.push(ids(values.into_iter())?);
    }
    for (name, values) in [
        (
            "baseline_sha256",
            rows.iter().map(|r| r.baseline).collect::<Vec<_>>(),
        ),
        ("left_sha256", rows.iter().map(|r| r.left).collect()),
        ("right_sha256", rows.iter().map(|r| r.right).collect()),
    ] {
        fields.push(Field::new(name, DataType::FixedSizeBinary(32), true));
        columns.push(hashes(values.into_iter())?);
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        columns,
    )
    .map_err(|e| super::invalid(&e.to_string()))
}
fn summary(
    left: &State,
    right: &State,
    rows: &[Row],
    mut metadata: HashMap<String, String>,
) -> Result<RecordBatch, GfError> {
    metadata.insert("graphforge.comparison.next_cursor".into(), String::new());
    let count = |kind: &str| rows.iter().filter(|r| r.change == kind).count() as u64;
    let local = count("local");
    let upstream = count("upstream");
    let conflict = count("conflict");
    let changed = count("changed");
    let missing = count("dependency_unavailable");
    let parent = left.branch.is_some() && left.parent_branch == right.branch;
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (name, value) in [
        ("compares_parent", parent),
        (
            "current_with_parent",
            parent && local + upstream + conflict + changed + missing == 0,
        ),
        ("updates_available", upstream + conflict > 0),
        ("research_divergence", local + conflict + changed > 0),
        ("conflicts_require_review", conflict + missing > 0),
    ] {
        fields.push(Field::new(name, DataType::Boolean, false));
        columns.push(Arc::new(BooleanArray::from(vec![value])));
    }
    for name in [
        "local",
        "upstream",
        "conflict",
        "changed",
        "accepted",
        "equivalent",
        "dependency_unavailable",
    ] {
        fields.push(Field::new(name, DataType::UInt64, false));
        columns.push(Arc::new(UInt64Array::from(vec![count(name)])));
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        columns,
    )
    .map_err(|e| super::invalid(&e.to_string()))
}
pub(super) fn stale() -> GfError {
    GfError::Api{code:graphforge_core::ApiErrorCode::PageSnapshotGone,message:"comparison continuation is stale; restart with the current endpoints and identical request".into()}
}
