//! Exact buffers for decoded Parquet pieces (#1918).
//!
//! The native reader appends byte-array values, and the children of nested
//! values, to vectors that grow by doubling after reserving what a page's
//! average suggests, so a decoded piece can hold up to twice its values in
//! capacity. The builder charges a batch what its buffers hold against the
//! intake window, so a piece is copied into buffers of exactly its size before
//! it is normalized, and only where a buffer holds slack. The copy is
//! transient, admitted in the task's reservation beside the decoded piece.

use std::sync::Arc;

use arrow::array::{ArrayData, ArrayRef, Capacities, MutableArrayData, make_array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;

use super::storage;

/// Slack a buffer may keep: the allocator rounds a buffer up to this.
const SLACK_BYTES: usize = 64;

/// `batch` with every column whose buffers hold more than they use copied
/// into buffers of exactly its size.
pub(super) fn exact(batch: RecordBatch) -> Result<RecordBatch, GfError> {
    if !batch
        .columns()
        .iter()
        .any(|column| has_slack(&column.to_data()))
    {
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let data = column.to_data();
            if has_slack(&data) {
                exact_array(&data)
            } else {
                Arc::clone(column)
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(batch.schema(), columns).map_err(storage)
}

fn has_slack(data: &ArrayData) -> bool {
    data.buffers()
        .iter()
        .chain(data.nulls().map(arrow::buffer::NullBuffer::buffer))
        .any(|buffer| buffer.capacity() > buffer.len().saturating_add(SLACK_BYTES))
        || data.child_data().iter().any(has_slack)
}

fn exact_array(data: &ArrayData) -> ArrayRef {
    if let DataType::Struct(_) = data.data_type() {
        // `MutableArrayData` sizes a struct's children only inside a list. A
        // struct column is its children side by side: copy each exactly and
        // keep the struct's own validity.
        let children = data
            .child_data()
            .iter()
            .map(|child| exact_array(&child.slice(data.offset(), data.len())).to_data())
            .collect::<Vec<_>>();
        let rebuilt = ArrayData::builder(data.data_type().clone())
            .len(data.len())
            .nulls(data.nulls().cloned())
            .child_data(children)
            .build();
        return match rebuilt {
            Ok(rebuilt) => make_array(rebuilt),
            Err(_) => make_array(data.clone()),
        };
    }
    let Some(capacities) = capacities(data) else {
        // A type the copy cannot size exactly keeps its buffers.
        return make_array(data.clone());
    };
    let mut copy = MutableArrayData::with_capacities(vec![data], data.null_count() > 0, capacities);
    copy.extend(0, 0, data.len());
    make_array(copy.freeze())
}

/// Exactly what `data` uses, per buffer, in the shape `MutableArrayData`
/// sizes from; `None` for a type it cannot size that way.
fn capacities(data: &ArrayData) -> Option<Capacities> {
    let len = data.len();
    match data.data_type() {
        DataType::Utf8 | DataType::Binary => {
            Some(Capacities::Binary(len, Some(value_bytes::<i32>(data))))
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            Some(Capacities::Binary(len, Some(value_bytes::<i64>(data))))
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let child = data.child_data().first()?;
            Some(Capacities::List(len, Some(Box::new(capacities(child)?))))
        }
        DataType::Struct(_) => Some(Capacities::Struct(
            len,
            Some(
                data.child_data()
                    .iter()
                    .map(capacities)
                    .collect::<Option<Vec<_>>>()?,
            ),
        )),
        DataType::Map(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Union(_, _)
        | DataType::RunEndEncoded(_, _) => None,
        _ => Some(Capacities::Array(len)),
    }
}

/// Bytes the values of a byte-array `data` span, from its offsets.
fn value_bytes<O: arrow::array::OffsetSizeTrait>(data: &ArrayData) -> usize {
    let offsets: &[O] = data.buffer(0);
    let first = data.offset();
    let last = first + data.len();
    match (offsets.get(first), offsets.get(last)) {
        (Some(start), Some(end)) => end.as_usize().saturating_sub(start.as_usize()),
        _ => 0,
    }
}

#[cfg(test)]
#[path = "piece_buffers/tests.rs"]
mod tests;
