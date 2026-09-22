//! Arrow rows with source- and selection-bound continuation.
use super::{
    ApiErrorCode, ExecutionResult, GfError, Inclusion, Object, PageRequest, Selection,
    SlicePageKind, SliceRequest, Uuid, checkpoint, error, invalid, limit,
};
use crate::{ExecutionStats, MAX_PAGE_LIMIT, PageToken};
use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(super) fn page(
    selection: &Selection,
    request: &SliceRequest,
    snapshot: Uuid,
    kind: SlicePageKind,
    page: PageRequest,
    frozen_binding: Option<&[u8]>,
) -> Result<ExecutionResult, GfError> {
    let cancellation = page.cancellation;
    checkpoint(cancellation.as_ref())?;
    if !(1..=MAX_PAGE_LIMIT).contains(&page.limit) {
        return Err(invalid("invalid Slice page limit"));
    }
    let digest = fingerprint(selection, request, frozen_binding)?;
    let method = method(kind);
    let count_rows = if kind == SlicePageKind::Counts {
        Some(counts(selection)?)
    } else {
        None
    };
    let rows = match kind {
        SlicePageKind::Included | SlicePageKind::Explanations => &selection.active,
        SlicePageKind::Boundary => &selection.boundary,
        _ => &selection.required,
    };
    let count = count_rows
        .as_ref()
        .map_or(rows.len(), RecordBatch::num_rows);
    let offset = if let Some(token) = &page.after {
        let (offset, expected) =
            token.decode_bound(method, request.request_uuid, snapshot, page.limit)?;
        if expected != digest {
            return Err(error(
                ApiErrorCode::PageInvalid,
                "Slice selection changed since the preceding page",
            ));
        }
        offset
    } else {
        0
    };
    if offset > count {
        return Err(error(
            ApiErrorCode::PageInvalid,
            "Slice cursor exceeds result rows",
        ));
    }
    let end = offset.saturating_add(page.limit as usize).min(count);
    let batch = if let Some(batch) = count_rows {
        batch.slice(offset, end - offset)
    } else {
        let page_rows = rows
            .iter()
            .skip(offset)
            .take(end - offset)
            .map(|(object, why)| (object.clone(), why.clone()))
            .collect();
        records(&page_rows)?
    };
    if batch.get_array_memory_size() as u64 > request.limits.response_bytes {
        return Err(limit());
    }
    let mut metadata = HashMap::from([
        ("graphforge.slice.contract".into(), "1".into()),
        (
            "graphforge.slice.source".into(),
            serde_json::to_string(&request.source).map_err(|_| invalid("invalid Slice source"))?,
        ),
        (
            "graphforge.slice.snapshot_uuid".into(),
            snapshot.to_string(),
        ),
        (
            "graphforge.slice.membership_is_retention".into(),
            "false".into(),
        ),
    ]);
    if end < count {
        metadata.insert(
            "graphforge.next_page_token".into(),
            PageToken::new_bound(
                method,
                request.request_uuid,
                snapshot,
                page.limit,
                end,
                digest,
            )
            .as_str()
            .into(),
        );
    }
    let schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().clone(),
        metadata,
    ));
    let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
        .map_err(|_| invalid("Slice Arrow schema mismatch"))?;
    check_response(&batch, request.limits.response_bytes)?;
    Ok(ExecutionResult {
        schema,
        batches: vec![batch],
        stats: ExecutionStats::default(),
        side_effects: None,
        mutation_receipt: None,
    })
}
fn records(rows: &BTreeMap<Object, Inclusion>) -> Result<RecordBatch, GfError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("object_kind", DataType::Utf8, false),
        Field::new("object_uuid", DataType::Utf8, false),
        Field::new("reason", DataType::Utf8, false),
        Field::new("root_uuid", DataType::Utf8, false),
        Field::new("predecessor_uuid", DataType::Utf8, true),
        Field::new("via_edge_uuid", DataType::Utf8, true),
        Field::new("depth", DataType::UInt32, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            rows.keys().map(|o| o.kind.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.keys().map(|o| o.uuid.to_string()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.values().map(|v| v.reason.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.values().map(|v| v.root.to_string()),
        )),
        Arc::new(StringArray::from(
            rows.values()
                .map(|v| v.predecessor.map(|id| id.to_string()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.values()
                .map(|v| v.via_edge.map(|id| id.to_string()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.values().map(|v| v.depth),
        )),
    ];
    RecordBatch::try_new(schema, columns).map_err(|_| invalid("Slice Arrow schema mismatch"))
}
fn counts(selection: &Selection) -> Result<RecordBatch, GfError> {
    let mut counts: BTreeMap<(String, String), u64> = BTreeMap::new();
    for (role, rows) in [
        ("active", &selection.active),
        ("required", &selection.required),
        ("boundary", &selection.boundary),
    ] {
        for object in rows.keys() {
            *counts
                .entry((role.into(), object.kind.clone()))
                .or_default() += 1;
            if role == "active" {
                for label in selection.labels.get(object).into_iter().flatten() {
                    *counts
                        .entry((role.into(), format!("{}:{label}", object.kind)))
                        .or_default() += 1;
                }
            }
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("role", DataType::Utf8, false),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("count", DataType::UInt64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                counts.keys().map(|(r, _)| r.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                counts.keys().map(|(_, t)| t.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(counts.values().copied())),
        ],
    )
    .map_err(|_| invalid("Slice counts schema mismatch"))
}

pub(super) fn check_response(batch: &RecordBatch, maximum: u64) -> Result<(), GfError> {
    struct BoundedCounter {
        count: u64,
        maximum: u64,
    }
    impl std::io::Write for BoundedCounter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.count = self
                .count
                .checked_add(bytes.len() as u64)
                .filter(|n| *n <= self.maximum)
                .ok_or_else(|| std::io::Error::other("Slice response bound"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(
        BoundedCounter { count: 0, maximum },
        batch.schema().as_ref(),
    )
    .map_err(|_| limit())?;
    writer
        .write(batch)
        .and_then(|()| writer.finish())
        .map_err(|_| limit())
}

fn fingerprint(
    selection: &Selection,
    request: &SliceRequest,
    frozen_binding: Option<&[u8]>,
) -> Result<[u8; 32], GfError> {
    let bytes = serde_json::to_vec(request).map_err(|_| invalid("invalid Slice request"))?;
    let mut digest = Sha256::new();
    digest.update(bytes);
    if let Some(binding) = frozen_binding {
        digest.update(binding);
    }
    for rows in [&selection.active, &selection.required, &selection.boundary] {
        for (object, why) in rows {
            digest.update(format!(
                "{}:{}:{}:{}:{:?}:{:?}:{}\n",
                object.kind,
                object.uuid,
                why.reason,
                why.root,
                why.predecessor,
                why.via_edge,
                why.depth
            ));
        }
    }
    for (object, labels) in &selection.labels {
        digest.update(object.uuid.as_bytes());
        for label in labels {
            digest.update((label.len() as u64).to_le_bytes());
            digest.update(label.as_bytes());
        }
    }
    Ok(digest.finalize().into())
}

fn method(kind: SlicePageKind) -> &'static str {
    match kind {
        SlicePageKind::Included => "slice_included",
        SlicePageKind::Boundary => "slice_boundary",
        SlicePageKind::Explanations => "slice_explanations",
        SlicePageKind::Dependencies => "slice_dependencies",
        SlicePageKind::Counts => "slice_counts",
    }
}
