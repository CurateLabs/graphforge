//! Bounded row gathering for property frames.

use arrow::array::{Array, MutableArrayData, make_array};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

/// Gather rows without materializing a second index per nested list child.
/// Arrow's record-batch interleave is retained for schemas without lists.
pub(super) fn gather_record_batch(
    batches: &[&RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch, ArrowError> {
    let first = batches.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError("cannot gather rows without a source batch".into())
    })?;
    if indices.is_empty() {
        return Ok(RecordBatch::new_empty(first.schema()));
    }
    if batches
        .iter()
        .any(|batch| batch.schema().as_ref() != first.schema().as_ref())
    {
        return Err(ArrowError::InvalidArgumentError(
            "gather source schemas do not match".into(),
        ));
    }
    if first
        .schema()
        .fields()
        .iter()
        .all(|field| !contains_list(field.data_type()))
    {
        return arrow::compute::interleave_record_batch(batches, indices);
    }

    let columns = (0..first.num_columns())
        .map(|column| {
            let arrays = batches
                .iter()
                .map(|batch| batch.column(column).as_ref())
                .collect::<Vec<_>>();
            if contains_list(first.column(column).data_type()) {
                gather_list_column(&arrays, indices)
            } else {
                arrow::compute::interleave(&arrays, indices)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(first.schema(), columns)
}

fn contains_list(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => true,
        DataType::Struct(fields) => fields.iter().any(|field| contains_list(field.data_type())),
        _ => false,
    }
}

fn gather_list_column(
    arrays: &[&dyn Array],
    indices: &[(usize, usize)],
) -> Result<arrow::array::ArrayRef, ArrowError> {
    if arrays.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "cannot gather an empty list column".into(),
        ));
    }
    let data = arrays
        .iter()
        .map(|array| array.to_data())
        .collect::<Vec<_>>();
    let data_refs = data.iter().collect::<Vec<_>>();
    let mut output = MutableArrayData::new(data_refs, false, indices.len());
    if indices.is_empty() {
        return Ok(make_array(output.freeze()));
    }
    if indices
        .iter()
        .any(|(array, row)| *array >= arrays.len() || *row >= arrays[*array].len())
    {
        return Err(ArrowError::InvalidArgumentError(
            "gather index is outside its source array".into(),
        ));
    }
    let mut indices = indices.iter().copied();
    let Some((mut source, first_row)) = indices.next() else {
        return Ok(make_array(output.freeze()));
    };
    let mut start = first_row;
    let mut end = first_row + 1;
    for (next_source, row) in indices {
        if next_source == source && row == end {
            end += 1;
            continue;
        }
        output.extend(source, start, end);
        source = next_source;
        start = row;
        end = row + 1;
    }
    output.extend(source, start, end);
    Ok(make_array(output.freeze()))
}
