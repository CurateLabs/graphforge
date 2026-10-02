//! Fixed maximum size of one property fragment, set at write time (#1388).
//!
//! First-touch admission authenticates a property fragment whole, so an
//! unbounded fragment makes a bounded property read unbounded. Every writer
//! therefore cuts its output at this cap, the way the CSR shard writer clamps
//! at `DEFAULT_CSR_SHARD_EDGES`. The cap is a format constant: it is recorded
//! in no manifest and does not vary with a session budget, so the same logical
//! rows always cut at the same places (ADR 0038).
//!
//! # Derivation
//!
//! A full CSR shard (1,048,576 edges at 1.6-1.7 B/edge) is about 1.7 MiB
//! encoded, and its decoded buffers are bounded at 24 MiB. A property fragment
//! should admit at that order, not at the size of the route. The byte cap is
//! counted on **logical** bytes, the Arrow value, offset and presence bytes of
//! the rows, because that measure is a pure function of the rows: it does not
//! depend on the Parquet encoder, the compression codec or the writer version,
//! so the cut is reproducible. Encoded bytes also include Parquet pages,
//! dictionaries, compression framing and the footer; compression can expand
//! its input, so this logical cap is not an exact encoded-size limit. Admission
//! uses the manifest's declared file length. Typical property data encodes to
//! about half the logical bytes, which puts one fragment at the size of one
//! CSR shard. The row cap equals the default construction batch
//! (`GraphConstructionBudgets::max_batch_rows`) and bounds the UUID range, the
//! merge scratch and the decoder state of narrow rows whose byte count alone
//! would admit hundreds of thousands of them.
//!
//! A single row larger than the byte cap cannot be split and becomes a
//! fragment of its own.
//!
//! Fragments written before this cap existed may exceed it. Readers do not
//! enforce the cap; it constrains only what writers produce.

use std::ops::Range;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::DataType;
use graphforge_core::GfError;

/// Maximum rows in one property fragment.
pub const MAX_PROPERTY_FRAGMENT_ROWS: usize = 65_536;
/// Maximum logical bytes (see the module documentation) in one property
/// fragment.
pub const MAX_PROPERTY_FRAGMENT_BYTES: u64 = 4 << 20;

/// A contiguous run of rows that belongs to one fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FragmentPiece {
    pub(crate) rows: Range<usize>,
    /// This run is the first run of a new fragment.
    pub(crate) opens_fragment: bool,
}

/// Greedy in-order cut of a row stream into capped fragments. State carries
/// across [`push`](Self::push) calls, so a writer that sees its rows in chunks
/// cuts at the same rows as one that sees them at once.
#[derive(Debug, Default)]
pub(crate) struct FragmentSplitter {
    rows: usize,
    bytes: u64,
}

impl FragmentSplitter {
    /// Place `charges` (one logical byte count per row) after the rows already
    /// placed and return the runs, in order.
    pub(crate) fn push(&mut self, charges: &[u64]) -> Vec<FragmentPiece> {
        let mut pieces = Vec::new();
        let mut start = 0;
        let mut run_opens = self.rows == 0;
        for (row, &charge) in charges.iter().enumerate() {
            let fits = self.rows < MAX_PROPERTY_FRAGMENT_ROWS
                && self.bytes.saturating_add(charge) <= MAX_PROPERTY_FRAGMENT_BYTES;
            if self.rows > 0 && !fits {
                if start < row {
                    pieces.push(FragmentPiece {
                        rows: start..row,
                        opens_fragment: run_opens,
                    });
                }
                start = row;
                run_opens = true;
                self.rows = 0;
                self.bytes = 0;
            }
            self.rows += 1;
            self.bytes = self.bytes.saturating_add(charge);
        }
        if start < charges.len() {
            pieces.push(FragmentPiece {
                rows: start..charges.len(),
                opens_fragment: run_opens,
            });
        }
        pieces
    }
}

/// Logical bytes of every row of `batch`: the sum over its columns of
/// [`range_charge`] for that row. Additive by construction, so any grouping of
/// rows charges the same total.
pub(crate) fn row_charges(batch: &RecordBatch) -> Vec<u64> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| range_charge(column.as_ref(), row..row + 1))
                .fold(0_u64, u64::saturating_add)
        })
        .collect()
}

/// A validated Arrow offset or width as an index. Arrow rejects negative ones
/// at construction, so the saturated value is unreachable.
fn index<T: TryInto<usize>>(value: T) -> usize {
    value.try_into().unwrap_or(usize::MAX)
}

fn width(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Value, offset and presence bytes of `array[range]`: one presence byte per
/// element, plus the fixed width, or the offset width and payload of a
/// variable-width value, recursively through lists and structs. Types without
/// a closed form charge their sliced buffer size, which is also a function of
/// the rows alone.
fn range_charge(array: &dyn Array, range: Range<usize>) -> u64 {
    let count = width(range.len());
    let present = count;
    match array.data_type() {
        DataType::Null => 0,
        DataType::Boolean => present.saturating_add(count),
        DataType::FixedSizeBinary(size) => width(index(*size))
            .saturating_mul(count)
            .saturating_add(present),
        DataType::Utf8 => {
            let offsets = array.as_string::<i32>().value_offsets();
            variable(present, count, 4, offsets[range.end] - offsets[range.start])
        }
        DataType::LargeUtf8 => {
            let offsets = array.as_string::<i64>().value_offsets();
            variable(present, count, 8, offsets[range.end] - offsets[range.start])
        }
        DataType::Binary => {
            let offsets = array.as_binary::<i32>().value_offsets();
            variable(present, count, 4, offsets[range.end] - offsets[range.start])
        }
        DataType::LargeBinary => {
            let offsets = array.as_binary::<i64>().value_offsets();
            variable(present, count, 8, offsets[range.end] - offsets[range.start])
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .map(|child| range_charge(child.as_ref(), range.clone()))
            .fold(present, u64::saturating_add),
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let child = index(offsets[range.start])..index(offsets[range.end]);
            range_charge(list.values().as_ref(), child)
                .saturating_add(present)
                .saturating_add(count.saturating_mul(4))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            let child = index(offsets[range.start])..index(offsets[range.end]);
            range_charge(list.values().as_ref(), child)
                .saturating_add(present)
                .saturating_add(count.saturating_mul(8))
        }
        DataType::FixedSizeList(_, size) => {
            let list = array.as_fixed_size_list();
            let size = index(*size);
            range_charge(list.values().as_ref(), range.start * size..range.end * size)
                .saturating_add(present)
        }
        other => match other.primitive_width() {
            Some(bytes) => width(bytes).saturating_mul(count).saturating_add(present),
            None => array
                .to_data()
                .slice(range.start, range.len())
                .get_slice_memory_size()
                .map_or(u64::MAX, width),
        },
    }
}

fn variable<T: TryInto<u64>>(present: u64, count: u64, offset_width: u64, payload: T) -> u64 {
    payload
        .try_into()
        .unwrap_or(u64::MAX)
        .saturating_add(present)
        .saturating_add(count.saturating_mul(offset_width))
}

/// Cut one fragment batch into capped fragments, in row order, and stamp each
/// with its own ordinal starting at `first_ordinal`.
pub(crate) fn split_into_fragments(
    fragment: &RecordBatch,
    first_ordinal: u64,
) -> Result<Vec<RecordBatch>, GfError> {
    let charges = row_charges(fragment);
    FragmentSplitter::default()
        .push(&charges)
        .into_iter()
        .enumerate()
        .map(|(index, piece)| {
            let ordinal = first_ordinal
                .checked_add(index as u64)
                .ok_or_else(|| GfError::Storage("property fragment ordinal overflows".into()))?;
            with_fragment_ordinal(&fragment.slice(piece.rows.start, piece.rows.len()), ordinal)
        })
        .collect()
}

/// `batch` with its schema's fragment ordinal metadata set to `ordinal`.
pub(crate) fn with_fragment_ordinal(
    batch: &RecordBatch,
    ordinal: u64,
) -> Result<RecordBatch, GfError> {
    let mut metadata = batch.schema().metadata().clone();
    metadata.insert(super::PROPERTY_ORDINAL_KEY.to_owned(), ordinal.to_string());
    let schema = batch.schema().as_ref().clone().with_metadata(metadata);
    RecordBatch::try_new(std::sync::Arc::new(schema), batch.columns().to_vec())
        .map_err(|error| GfError::Storage(error.to_string()))
}

#[cfg(test)]
pub(crate) mod tests;
