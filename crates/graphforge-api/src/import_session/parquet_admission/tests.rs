//! The sizes `SourceScan` states match what the Arrow reader decodes, for every
//! encoding whose expansion differs from its stored bytes (#1918).

use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, Int64Builder, ListBuilder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::{Compression, Encoding};
use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use super::{SourceScan, needs_values};
use crate::import_session::parquet_scan::{PageKind, encoding};

fn write(batch: &RecordBatch, properties: WriterProperties) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), Some(properties)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    file
}

fn scan(file: &tempfile::NamedTempFile, batch_rows: u64) -> SourceScan {
    let handle = File::open(file.path()).unwrap();
    let metadata = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .metadata()
        .clone();
    let metadata = ArrowReaderMetadata::try_new(
        Arc::new(metadata.as_ref().clone()),
        ArrowReaderOptions::new(),
    )
    .unwrap();
    SourceScan::build(handle, &metadata, batch_rows, 1 << 40, u64::MAX, None).unwrap()
}

/// Bytes Arrow holds for the data of `array`: values, offsets and validity, not
/// the capacity its buffers were allocated with.
fn arrow_bytes(array: &dyn Array) -> u64 {
    array.to_data().get_slice_memory_size().unwrap() as u64
}

#[test]
fn source_scan_projects_visible_leaves_across_an_omitted_map_gap() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let schema = Arc::new(
        parse_message_type(
            "message schema {
                required int64 before;
                optional group hidden (MAP) {
                    repeated group key_value {
                        required binary key (UTF8);
                        optional group value {}
                    }
                }
                optional binary after (UTF8);
            }",
        )
        .unwrap(),
    );
    let properties = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(true)
            .build(),
    );
    let mut writer = SerializedFileWriter::new(file.reopen().unwrap(), schema, properties).unwrap();
    let rows = 4;
    let mut row_group = writer.next_row_group().unwrap();
    let mut before = row_group.next_column().unwrap().unwrap();
    before
        .typed::<Int64Type>()
        .write_batch(&[10, 11, 12, 13], None, None)
        .unwrap();
    before.close().unwrap();
    let mut hidden = row_group.next_column().unwrap().unwrap();
    hidden
        .typed::<ByteArrayType>()
        .write_batch(
            &(0..rows)
                .map(|row| ByteArray::from(format!("hidden-key-{row:064}").into_bytes()))
                .collect::<Vec<_>>(),
            Some(&[2; 4]),
            Some(&[0; 4]),
        )
        .unwrap();
    hidden.close().unwrap();
    let mut after = row_group.next_column().unwrap().unwrap();
    after
        .typed::<ByteArrayType>()
        .write_batch(
            &(0..rows)
                .map(|row| ByteArray::from(format!("visible-value-{row:064}").into_bytes()))
                .collect::<Vec<_>>(),
            Some(&[1; 4]),
            None,
        )
        .unwrap();
    after.close().unwrap();
    row_group.close().unwrap();
    writer.close().unwrap();

    let metadata =
        ArrowReaderMetadata::load(&file.reopen().unwrap(), ArrowReaderOptions::new()).unwrap();
    let scan = SourceScan::build(
        File::open(file.path()).unwrap(),
        &metadata,
        2,
        1 << 20,
        u64::MAX,
        None,
    )
    .unwrap();
    assert_eq!(
        scan.shape
            .leaves
            .iter()
            .map(|leaf| leaf.column_index)
            .collect::<Vec<_>>(),
        [0, 2],
    );
    assert_eq!(
        scan.groups[0]
            .leaves
            .iter()
            .map(|leaf| leaf.physical_column_index)
            .collect::<Vec<_>>(),
        [0, 2],
    );
    assert_eq!(scan.batch_bytes(0), 182);
    assert_eq!(scan.batch_bytes(1), 182);

    let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .with_batch_size(2)
        .build()
        .unwrap();
    let mut decoded_rows = Vec::new();
    let mut decoded_bytes = 0_u64;
    for batch in reader {
        let batch = batch.unwrap();
        assert_eq!(batch.schema().fields().len(), 2);
        assert_eq!(batch.schema().field(0).name(), "before");
        assert_eq!(batch.schema().field(1).name(), "after");
        let before = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let after = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            decoded_rows.push((before.value(row), after.value(row).to_owned()));
        }
        decoded_bytes += batch
            .columns()
            .iter()
            .map(|column| arrow_bytes(column.as_ref()))
            .sum::<u64>();
    }
    assert_eq!(decoded_rows.len(), 4);
    assert_eq!(decoded_rows[0], (10, format!("visible-value-{:064}", 0)));
    assert_eq!(decoded_rows[3], (13, format!("visible-value-{:064}", 3)));
    assert_eq!(decoded_bytes, 368);
    assert_eq!(scan.decoded_bytes(), 364);
}

/// Every batch's stated size covers what the reader decodes, and exceeds it by
/// no more than `slack` times: the estimate rounds booleans and decimals up and
/// counts the pages a window only touches.
fn assert_sizes(batch: &RecordBatch, properties: WriterProperties, batch_rows: usize, slack: f64) {
    let file = write(batch, properties);
    let scan = scan(&file, batch_rows as u64);
    let mut total = 0_u64;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .with_batch_size(batch_rows)
        .build()
        .unwrap();
    let mut seen = 0;
    for (index, decoded) in reader.enumerate() {
        let decoded = decoded.unwrap();
        let actual = decoded
            .columns()
            .iter()
            .map(|column| arrow_bytes(column.as_ref()))
            .sum::<u64>();
        let stated = scan.batch_bytes(index as u64);
        total += actual;
        assert!(
            stated as f64 >= actual as f64 * 0.98,
            "batch {index}: stated {stated} bytes, decoded {actual}"
        );
        assert!(
            stated as f64 <= actual as f64 * slack + 4096.0,
            "batch {index}: stated {stated} bytes, decoded only {actual}"
        );
        seen += 1;
    }
    assert!(seen > 1, "the fixture must span several batches");
    // The total a build plans its retention from apportions the pages a batch
    // touches, so it is not inflated by them.
    let planned = scan.decoded_bytes();
    assert!(
        planned as f64 >= total as f64 * 0.95 && planned as f64 <= total as f64 * slack.max(1.25),
        "planned {planned} bytes, decoded {total}"
    );
}

fn builder() -> WriterPropertiesBuilder {
    WriterProperties::builder().set_max_row_group_row_count(Some(1_000))
}

fn strings(rows: usize, value: impl Fn(usize) -> String) -> RecordBatch {
    let values = (0..rows).map(value).collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(values)) as ArrayRef],
    )
    .unwrap()
}

#[test]
fn dictionary_strings_are_sized_by_the_rows_that_use_each_entry() {
    // Two entries, one fat: half the rows expand to 8 KiB, half to a few bytes.
    // The footer stores each entry once.
    let batch = strings(4_000, |row| {
        if row % 2 == 0 {
            "x".repeat(8 << 10)
        } else {
            "short".to_owned()
        }
    });
    let file = write(&batch, builder().build());
    let scan = scan(&file, 500);
    let footer: u64 = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .metadata()
        .row_groups()
        .iter()
        .map(|group| u64::try_from(group.total_byte_size()).unwrap())
        .sum();
    assert!(
        scan.decoded_bytes() > footer * 50,
        "decoded {} bytes from a {footer}-byte footer",
        scan.decoded_bytes()
    );
    assert_sizes(&batch, builder().build(), 500, 1.1);
}

#[test]
fn a_dictionary_with_a_huge_unused_entry_is_sized_by_the_selected_values() {
    // One 4 MiB entry is in the dictionary page, referenced by one row of 2,000:
    // sizing by the largest entry would charge every row for it.
    let batch = strings(2_000, |row| {
        if row == 1_999 {
            "y".repeat(4 << 20)
        } else {
            format!("v{}", row % 7)
        }
    });
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2_000))
        .build();
    let file = write(&batch, properties);
    let scan = scan(&file, 500);
    let sizes = (0..4)
        .map(|batch| scan.batch_bytes(batch))
        .collect::<Vec<_>>();
    assert!(sizes[..3].iter().all(|size| *size < 64 << 10), "{sizes:?}");
    assert!(sizes[3] > 4 << 20, "{sizes:?}");
}

#[test]
fn plain_strings_are_sized_from_their_pages() {
    let batch = strings(4_000, |row| format!("{row:0width$}", width = 20 + row % 50));
    // A 500-row batch touches the one page its row group's 1,000 rows share.
    assert_sizes(
        &batch,
        builder().set_dictionary_enabled(false).build(),
        500,
        2.2,
    );
}

#[test]
fn delta_encoded_strings_are_sized_by_their_decoded_values() {
    // Each value extends the previous one, so a page of a few kilobytes expands
    // to many times its size.
    let batch = strings(4_000, |row| {
        format!("{}{row}", "p".repeat(2_000 + row % 400))
    });
    // Decoded from the values, so exact; stored back to back, so bounded by the
    // page a batch touches.
    for (encoding, slack) in [
        (Encoding::DELTA_BYTE_ARRAY, 1.1),
        (Encoding::DELTA_LENGTH_BYTE_ARRAY, 2.2),
    ] {
        let properties = builder()
            .set_dictionary_enabled(false)
            .set_encoding(encoding)
            .build();
        assert_sizes(&batch, properties, 500, slack);
    }
    let file = write(
        &batch,
        builder()
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::DELTA_BYTE_ARRAY)
            .build(),
    );
    let scan = scan(&file, 500);
    assert!(
        scan.groups
            .iter()
            .all(|group| needs_values(&group.leaves[0]))
    );
    let stored: u64 = scan
        .groups
        .iter()
        .flat_map(|group| group.leaves[0].pages.iter().flatten())
        .filter(|page| page.kind == PageKind::Data)
        .map(|page| u64::from(page.uncompressed))
        .sum();
    assert!(scan.decoded_bytes() > stored * 10, "{stored} stored bytes");
}

#[test]
fn fixed_width_and_delta_integers_are_sized_by_arithmetic() {
    let ints = Int64Array::from((0..4_000).map(|row| row * 3).collect::<Vec<_>>());
    let narrow = Int32Array::from((0..4_000).map(|row| row % 11).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("wide", DataType::Int64, true),
            Field::new("narrow", DataType::Int32, false),
        ])),
        vec![Arc::new(ints) as ArrayRef, Arc::new(narrow) as ArrayRef],
    )
    .unwrap();
    assert_sizes(&batch, builder().build(), 500, 1.1);
    assert_sizes(
        &batch,
        builder()
            .set_dictionary_enabled(false)
            .set_column_encoding("wide".into(), Encoding::DELTA_BINARY_PACKED)
            .set_column_encoding("narrow".into(), Encoding::DELTA_BINARY_PACKED)
            .build(),
        500,
        1.1,
    );
}

#[test]
fn repeated_values_are_sized_by_the_children_each_row_holds() {
    let mut builder_ = ListBuilder::new(Int64Builder::new());
    let mut labels = ListBuilder::new(StringBuilder::new());
    for row in 0..4_000_i64 {
        // A few rows hold thousands of children; most hold one or none.
        let children = if row % 400 == 0 { 3_000 } else { row % 3 };
        for child in 0..children {
            builder_.values().append_value(row * 10 + child);
            labels.values().append_value(format!("child-{child}"));
        }
        builder_.append(row % 17 != 0);
        labels.append(true);
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "numbers",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new(
                "labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ])),
        vec![
            Arc::new(builder_.finish()) as ArrayRef,
            Arc::new(labels.finish()) as ArrayRef,
        ],
    )
    .unwrap();
    // Each child also costs the levels the reader holds and the index the sink
    // takes it by, 8 bytes beside the 8 of an integer or the 12 of a string.
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        assert_sizes(
            &batch,
            builder().set_writer_version(version).build(),
            500,
            2.2,
        );
    }
}

#[test]
fn equal_width_dictionary_values_have_exact_sizes() {
    // Thirty-six characters wherever the indices point (a UUID rendered as text).
    let batch = strings(4_000, |row| format!("{:036}", row % 13));
    assert_sizes(&batch, builder().build(), 500, 1.05);
    let file = write(&batch, builder().build());
    let scan = scan(&file, 500);
    let leaf = &scan.groups[0].leaves[0];
    assert!(needs_values(leaf) && leaf.summary.dictionary_encoded);
    // Four bytes of offset and a validity bit per row, around the 36.
    assert_eq!(scan.value_bytes[0], 500 * (36 + 4) + 500_u64.div_ceil(8));

    // Null values contribute offsets and validity, but no referenced payload;
    // every nonnull dictionary index is still validated and the size is exact.
    let with_nulls = (0..4_000)
        .map(|row| (row % 9 != 0).then(|| format!("{:036}", row % 13)))
        .collect::<Vec<_>>();
    let nullable = RecordBatch::try_new(
        batch.schema(),
        vec![Arc::new(StringArray::from(with_nulls)) as ArrayRef],
    )
    .unwrap();
    assert_sizes(&nullable, builder().build(), 500, 1.05);
}

#[test]
fn a_page_that_bounds_a_small_batch_too_coarsely_is_replaced_by_the_exact_size() {
    // One 8 MB page holds 2,000 rows of 4 KB; a 100-row batch is 400 KB of it.
    // Bounded by the page it touches, the batch would look twenty times too big.
    let batch = strings(2_000, |row| format!("{row:04}").repeat(1_024));
    let properties = builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(64 << 20)
        .set_data_page_row_count_limit(usize::MAX)
        .set_max_row_group_row_count(Some(2_000))
        .build();
    let file = write(&batch, properties);
    let handle = File::open(file.path()).unwrap();
    let metadata = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .metadata()
        .clone();
    let metadata = ArrowReaderMetadata::try_new(
        Arc::new(metadata.as_ref().clone()),
        ArrowReaderOptions::new(),
    )
    .unwrap();
    // Admitting by the bound alone: a 1 MiB window refuses every batch.
    let coarse = SourceScan::build(
        handle.try_clone().unwrap(),
        &metadata,
        100,
        1 << 40,
        u64::MAX,
        None,
    )
    .unwrap();
    assert!(
        coarse.batch_bytes(0) > 8_000_000,
        "{}",
        coarse.batch_bytes(0)
    );
    // With the window known, the columns it would refuse are sized from their values.
    let exact = SourceScan::build(handle, &metadata, 100, 1 << 40, 1 << 20, None).unwrap();
    let sizes = (0..20)
        .map(|batch| exact.batch_bytes(batch))
        .collect::<Vec<_>>();
    assert!(
        sizes
            .iter()
            .all(|size| (400 << 10..=420 << 10).contains(size)),
        "{sizes:?}"
    );
}

#[test]
fn nulls_and_compression_do_not_change_what_a_batch_decodes_to() {
    let batch = strings(4_000, |row| {
        if row % 5 == 0 {
            String::new()
        } else {
            "z".repeat(row % 300)
        }
    });
    let values = (0..4_000)
        .map(|row| (row % 5 != 0).then(|| "z".repeat(row % 300)))
        .collect::<Vec<_>>();
    let nullable = RecordBatch::try_new(
        batch.schema(),
        vec![Arc::new(StringArray::from(values)) as ArrayRef],
    )
    .unwrap();
    for compression in [
        Compression::UNCOMPRESSED,
        Compression::SNAPPY,
        Compression::ZSTD(Default::default()),
    ] {
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            assert_sizes(
                &nullable,
                builder()
                    .set_compression(compression)
                    .set_writer_version(version)
                    .build(),
                500,
                1.3,
            );
        }
    }
}

#[test]
fn pages_of_a_dictionary_encoded_column_are_recognised() {
    let batch = strings(2_000, |row| format!("v{}", row % 5));
    let file = write(&batch, builder().build());
    let scan = scan(&file, 500);
    let leaf = &scan.groups[0].leaves[0];
    assert!(leaf.summary.dictionary_encoded);
    assert!(!leaf.summary.delta_byte_array);
    assert!(leaf.summary.dictionary_entries == 5);
    let pages = leaf.pages.as_ref().unwrap();
    assert!(
        pages
            .iter()
            .any(|page| page.kind == PageKind::Data && page.encoding == encoding::RLE_DICTIONARY)
    );
}
