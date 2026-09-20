//! Conservative admission for Arrow partition buffers and reorder scratch.
//! This is separate from allocator metadata, routing and process RSS.
use super::{partition::admit_materialization, storage};
use arrow::{array::ArrayData, datatypes::DataType, record_batch::RecordBatch};
use graphforge_core::GfError;

#[derive(Clone, Copy, Default)]
pub(super) struct RowReservation {
    buffers: u64,
    elements: u64,
    rows: u64,
    batches: u64,
}

fn add(a: u64, b: u64) -> Result<u64, GfError> {
    a.checked_add(b)
        .ok_or_else(|| storage("partition memory accounting overflows"))
}
fn mul(a: u64, b: u64) -> Result<u64, GfError> {
    a.checked_mul(b)
        .ok_or_else(|| storage("partition memory accounting overflows"))
}
fn aligned(bytes: u64) -> Result<u64, GfError> {
    mul(add(bytes, 63)? / 64, 64)
}

// Capacity floors matter even for empty nested lists: MutableArrayData reserves
// child buffers using its parent's requested capacity. Full child data is a
// conservative charge for a sliced list; fixed/text leaves use slice lengths.
fn array_reservation(data: &ArrayData, floor: u64) -> Result<(u64, u64), GfError> {
    let n = (data.len() as u64).max(floor);
    let mut bytes = aligned(add(n, 7)? / 8)?;
    let mut elements = n;
    let mut child_floor = n;
    let own = if let Some(width) = data.data_type().primitive_width() {
        mul(n, width as u64)?
    } else {
        match data.data_type() {
            DataType::Null | DataType::Struct(_) => 0,
            DataType::Boolean => add(n, 7)? / 8,
            DataType::FixedSizeBinary(width) => mul(n, u64::try_from(*width).map_err(storage)?)?,
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
                let width = if matches!(
                    data.data_type(),
                    DataType::LargeUtf8 | DataType::LargeBinary
                ) {
                    8
                } else {
                    4
                };
                // Slice memory includes selected values, but not the terminal
                // offset. Additional capacity covers generic byte preallocation.
                add(
                    data.get_slice_memory_size().map_err(storage)? as u64,
                    add(mul(add(n, 1)?, width)?, n)?,
                )?
            }
            DataType::List(_) | DataType::Map(_, _) => mul(add(n, 1)?, 4)?,
            DataType::LargeList(_) => mul(add(n, 1)?, 8)?,
            DataType::FixedSizeList(_, width) => {
                child_floor = mul(n, u64::try_from(*width).map_err(storage)?)?;
                0
            }
            other => {
                return Err(storage(format!(
                    "partition memory accounting does not support {other:?}"
                )));
            }
        }
    };
    bytes = add(bytes, aligned(own)?)?;
    for child in data.child_data() {
        let (child_bytes, child_elements) = array_reservation(child, child_floor)?;
        bytes = add(bytes, child_bytes)?;
        elements = add(elements, child_elements)?;
    }
    Ok((bytes, elements))
}

impl RowReservation {
    pub(super) fn with_batch(self, batch: &RecordBatch) -> Result<Self, GfError> {
        let mut next = self;
        for column in batch.columns() {
            let (bytes, elements) = array_reservation(&column.to_data(), 0)?;
            next.buffers = add(next.buffers, bytes)?;
            next.elements = add(next.elements, elements)?;
        }
        next.rows = add(next.rows, batch.num_rows() as u64)?;
        next.batches = add(next.batches, 1)?;
        Ok(next)
    }

    pub(super) fn admit(self, spill_bytes: u64, limit: u64) -> Result<(), GfError> {
        // Retained IPC bodies and decoded buffers, concat/take output, allocator
        // growth/alignment, UUID keys and permutation/nested take indexes.
        let bytes = add(mul(spill_bytes, 2)?, mul(self.buffers, 6)?)?;
        let bytes = add(bytes, mul(self.rows, 20)?)?;
        let bytes = add(bytes, mul(self.elements, 8)?)?;
        let bytes = add(bytes, mul(self.batches, 64)?)?;
        admit_materialization(Some(bytes), limit)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, ListArray, StringArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch(array: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap()
    }

    #[test]
    fn sliced_strings_charge_selected_values_not_entire_backing_buffer() {
        let array = StringArray::from(vec!["x".repeat(1 << 20), "small".to_owned()]);
        let selected = batch(Arc::new(array.slice(1, 1)));
        let reservation = RowReservation::default().with_batch(&selected).unwrap();
        reservation.admit(0, 4096).unwrap();
        assert!(
            RowReservation::default()
                .with_batch(&batch(Arc::new(array)))
                .unwrap()
                .admit(0, 4096)
                .is_err()
        );
    }

    #[test]
    fn empty_nested_lists_charge_wide_child_capacity_and_tiny_batches_accumulate() {
        let values = FixedSizeBinaryArray::new(
            4096,
            ScalarBuffer::<u8>::from(Vec::new()).into_inner(),
            None,
        );
        let field = Arc::new(Field::new("item", DataType::FixedSizeBinary(4096), true));
        let list = ListArray::new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 0, 0])),
            Arc::new(values),
            None,
        );
        let nested = batch(Arc::new(list));
        let reservation = RowReservation::default().with_batch(&nested).unwrap();
        assert!(
            reservation.buffers >= 2 * 4096,
            "empty children still inherit parent capacity"
        );
        assert!(reservation.admit(0, 8192).is_err());
        let tiny = batch(Arc::new(StringArray::from(vec!["a"])));
        let mut accumulated = RowReservation::default();
        for _ in 0..100 {
            accumulated = accumulated.with_batch(&tiny).unwrap();
        }
        assert!(accumulated.admit(0, 4096).is_err());
        assert!(
            RowReservation {
                buffers: u64::MAX,
                ..Default::default()
            }
            .admit(0, u64::MAX)
            .is_err()
        );
    }
}
