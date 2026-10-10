//! Resident bytes of a decoded batch (#1918).
//!
//! `RecordBatch::get_array_memory_size` adds up the capacity of every buffer of
//! every column. An Arrow IPC reader slices all of a record batch's columns out of
//! one message body, so each column reports that whole body and a wide batch is
//! charged its width times over: forty integer columns of 2.6 MB read from an
//! IPC file add up to 105 MB, past the 64 MiB intake window. The memory is the
//! body, once.

use std::collections::HashSet;

use arrow::array::ArrayData;
use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;

/// The bytes a batch's buffers occupy, counting each allocation once.
///
/// An allocation is told from another by where it starts: a slice of a buffer
/// reports its offset into the allocation it shares.
pub(super) fn resident_bytes(batch: &RecordBatch) -> usize {
    let mut seen = HashSet::new();
    let mut total = 0_usize;
    for column in batch.columns() {
        add_data(&column.to_data(), &mut seen, &mut total);
    }
    total
}

fn add_data(data: &ArrayData, seen: &mut HashSet<usize>, total: &mut usize) {
    for buffer in data.buffers() {
        add_buffer(buffer, seen, total);
    }
    if let Some(nulls) = data.nulls() {
        add_buffer(nulls.buffer(), seen, total);
    }
    for child in data.child_data() {
        add_data(child, seen, total);
    }
}

fn add_buffer(buffer: &Buffer, seen: &mut HashSet<usize>, total: &mut usize) {
    // `ptr_offset` is how far into its allocation this buffer starts; an empty
    // buffer has no allocation to count.
    let start = buffer.as_ptr() as usize - buffer.ptr_offset();
    if buffer.capacity() > 0 && seen.insert(start) {
        *total += buffer.capacity();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::FileReader;
    use arrow::ipc::writer::FileWriter;

    use super::*;

    fn wide(columns: usize, rows: usize) -> RecordBatch {
        let fields = (0..columns)
            .map(|column| Field::new(format!("p{column:03}"), DataType::Int64, true))
            .collect::<Vec<_>>();
        let arrays = (0..columns)
            .map(|column| {
                Arc::new(Int64Array::from(
                    (0..rows as i64)
                        .map(|row| row + column as i64)
                        .collect::<Vec<_>>(),
                )) as Arc<dyn arrow::array::Array>
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    #[test]
    fn the_columns_of_an_ipc_batch_share_one_body_that_is_counted_once() {
        let (columns, rows) = (40, 8_192);
        let written = wide(columns, rows);
        let file = tempfile::tempfile().unwrap();
        let mut writer = FileWriter::try_new(file.try_clone().unwrap(), &written.schema()).unwrap();
        writer.write(&written).unwrap();
        writer.finish().unwrap();
        let read = FileReader::try_new(file, None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        let data = columns * rows * 8;
        // The reader reports the body once per column...
        assert!(
            read.get_array_memory_size() > 20 * data,
            "{} bytes of {data}",
            read.get_array_memory_size()
        );
        // ...and it is there once.
        let resident = resident_bytes(&read);
        assert!(
            (data..=data + 64 * 1024).contains(&resident),
            "{resident} of {data}"
        );
    }

    #[test]
    fn independent_buffers_are_each_counted() {
        let strings = StringArray::from(vec!["a"; 1_000]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Utf8, true),
            ])),
            vec![Arc::new(strings.clone()), Arc::new(strings)],
        )
        .unwrap();
        // Two columns over clones of one array share their buffers...
        assert!(resident_bytes(&batch) <= batch.get_array_memory_size() / 2 + 64);
        // ...two built apart do not.
        let apart = RecordBatch::try_new(
            batch.schema(),
            vec![
                Arc::new(StringArray::from(vec!["a"; 1_000])),
                Arc::new(StringArray::from(vec!["a"; 1_000])),
            ],
        )
        .unwrap();
        // (`get_array_memory_size` also counts each array's own header.)
        let counted = resident_bytes(&apart);
        assert!(counted <= apart.get_array_memory_size());
        assert!(apart.get_array_memory_size() - counted < 1_024, "{counted}");
        assert!(counted > 2 * 4_000, "{counted}");
    }
}
