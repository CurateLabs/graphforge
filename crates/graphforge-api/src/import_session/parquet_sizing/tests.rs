use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use graphforge_core::GfError;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::{Compression, Encoding, Type};
use parquet::column::page::Page;
use parquet::file::properties::WriterProperties;

use super::{SizingPreflight, open_page_values};
use crate::CancellationToken;
use crate::import_session::inventory_budget::InventoryBudget;
use crate::import_session::parquet_admission::SourceScan;
use crate::import_session::parquet_page::read_header;
use crate::import_session::parquet_page_decode::DecodedPage;
use crate::import_session::parquet_reader::PagePreflight;

fn write_batch(batch: &RecordBatch) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(true)
        .set_data_page_row_count_limit(batch.num_rows())
        .build();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), Some(properties)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    file
}

fn metadata(file: &tempfile::NamedTempFile) -> Arc<parquet::file::metadata::ParquetMetaData> {
    ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .metadata()
        .clone()
}

fn scan(
    file: &tempfile::NamedTempFile,
    metadata: &parquet::file::metadata::ParquetMetaData,
    batch_rows: u64,
) -> Result<SourceScan, GfError> {
    let metadata =
        ArrowReaderMetadata::try_new(Arc::new(metadata.clone()), ArrowReaderOptions::new())
            .unwrap();
    SourceScan::build(
        File::open(file.path()).unwrap(),
        &metadata,
        batch_rows,
        1 << 30,
        u64::MAX,
        None,
    )
}

#[test]
fn uniform_dictionary_entries_still_validate_every_index() {
    let values = (0..8)
        .map(|row| ["aaa", "bbb", "ccc"][row % 3])
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(values)) as ArrayRef],
    )
    .unwrap();
    let file = write_batch(&batch);
    let metadata = metadata(&file);
    let good = scan(&file, &metadata, 8).unwrap();
    assert_eq!(good.batch_bytes(0), 8 * (3 + 4) + 1);

    let column = metadata.row_group(0).column(0);
    let chunk_start = column
        .dictionary_page_offset()
        .unwrap_or_else(|| column.data_page_offset());
    let chunk_start = u64::try_from(chunk_start).unwrap();
    let mut source = File::open(file.path()).unwrap();
    source.seek(SeekFrom::Start(chunk_start)).unwrap();
    let mut remaining = u64::try_from(column.compressed_size()).unwrap();
    let mut target_body = None;
    while remaining > 0 {
        let (header_bytes, header) = read_header(&mut source, remaining, None).unwrap();
        let header_length = u64::try_from(header_bytes).unwrap();
        remaining = remaining.checked_sub(header_length).unwrap();
        let body_length = usize::try_from(header.compressed.unwrap()).unwrap();
        let body_start = source.stream_position().unwrap();
        if header.kind == Some(0) && matches!(header.encoding, Some(2 | 8)) {
            assert_eq!(header.uncompressed, header.compressed);
            assert!(
                body_length >= 4,
                "dictionary index fixture body is too short"
            );
            target_body = Some((body_start, body_length));
            break;
        }
        source
            .seek(SeekFrom::Current(i64::try_from(body_length).unwrap()))
            .unwrap();
        remaining = remaining
            .checked_sub(u64::try_from(body_length).unwrap())
            .unwrap();
    }
    let (body_start, body_length) = target_body.expect("dictionary data page");
    let mut mutated = OpenOptions::new().write(true).open(file.path()).unwrap();
    let mut body = vec![0; body_length];
    source.seek(SeekFrom::Start(body_start)).unwrap();
    source.read_exact(&mut body).unwrap();
    assert_eq!(body[0], 2, "three entries use two index bits");
    // One bit-packed group of eight indices (hybrid run header 3); the first index becomes 3,
    // outside the three-entry dictionary while all entries remain equal-sized.
    body[1] = 3;
    body[2] = 3;
    body[3] = 0;
    mutated.seek(SeekFrom::Start(body_start)).unwrap();
    mutated.write_all(&body).unwrap();
    mutated.flush().unwrap();

    let error = match scan(&file, &metadata, 8) {
        Ok(_) => panic!("out-of-range index must be refused"),
        Err(error) => error,
    };
    assert!(
        matches!(error, GfError::Storage(_)),
        "unexpected error: {error}"
    );
}

#[test]
fn flat_dictionary_validity_is_packed_across_row_groups_and_batch_boundaries() {
    for nullable in [false, true] {
        let values = (0..8)
            .map(|row| {
                if nullable && row % 2 == 1 {
                    None
                } else {
                    Some("aaa")
                }
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            nullable,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(values)) as ArrayRef],
        )
        .unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(true)
            .set_max_row_group_row_count(Some(4))
            .build();
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = metadata(&file);
        assert_eq!(metadata.num_row_groups(), 2);
        let sized = scan(&file, &metadata, 5).unwrap();
        // The second window contains rows 5..8: with alternating nulls,
        // only row 6 contributes a three-byte value.
        let expected = if nullable { [30, 16] } else { [36, 22] };
        assert_eq!(sized.batch_bytes(0), expected[0]);
        assert_eq!(sized.batch_bytes(1), expected[1]);
        assert_eq!(sized.decoded_bytes(), expected.into_iter().sum::<u64>());
        let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
            .unwrap()
            .with_batch_size(5)
            .build()
            .unwrap();
        let actual_sizes = reader
            .enumerate()
            .map(|(index, decoded)| {
                let decoded = decoded.unwrap();
                assert_eq!(decoded.num_rows(), [5, 3][index]);
                let values = decoded
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..values.len() {
                    let source_row = index * 5 + row;
                    assert_eq!(values.is_null(row), nullable && source_row % 2 == 1);
                    if !values.is_null(row) {
                        assert_eq!(values.value(row), "aaa");
                    }
                }
                values.to_data().get_slice_memory_size().unwrap() as u64
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_sizes, if nullable { [30, 16] } else { [35, 21] });
    }
}

fn v1_page(repetition: u8) -> DecodedPage {
    // One RLE hybrid event each for repetition=0/1 and definition=1.
    // V1 permits a page to continue a row that started in the prior page;
    // each level stream carries its own little-endian length prefix.
    let body = Bytes::from(vec![
        2, 0, 0, 0, 2, repetition, 2, 0, 0, 0, 2, 1, 7, 0, 0, 0,
    ]);
    DecodedPage {
        page: Page::DataPage {
            buf: body,
            num_values: 1,
            encoding: Encoding::PLAIN,
            def_level_encoding: Encoding::RLE,
            rep_level_encoding: Encoding::RLE,
            statistics: None,
        },
        body_capacity: 16,
        physical_bytes: 16,
    }
}

#[test]
fn one_repeated_row_can_continue_across_many_owned_pages() {
    let mut budget = InventoryBudget::new(32 << 10);
    let mut charges = Vec::new();
    let mut add = |row, bytes| {
        charges.push((row, bytes));
        Ok(())
    };
    let mut preflight = SizingPreflight {
        physical: Type::INT32,
        fixed_width: 4,
        max_rep: 1,
        max_def: 1,
        rows: 1,
        row_base: 0,
        capacity: 32 << 10,
        budget: &mut budget,
        cancellation: Option::<CancellationToken>::None,
        dictionary: None,
        dictionary_charge: 0,
        row_slots: 0,
        row_payload: 0,
        row_open: false,
        finished_rows: 0,
        add_row: &mut add,
    };
    for index in 0..16 {
        let page = v1_page(u8::from(index != 0));
        preflight.validate(&page).unwrap();
    }
    preflight.finish().unwrap();
    assert_eq!(charges, [(0, 16 * 4 + 4 + 2 + 16 * 8)]);
}

#[test]
fn fixed_byte_stream_split_validates_whole_value_geometry() {
    let malformed = [0; 5];
    assert!(
        open_page_values(
            Type::FIXED_LEN_BYTE_ARRAY,
            4,
            None,
            Encoding::BYTE_STREAM_SPLIT,
            &malformed,
            1,
            None,
        )
        .is_err()
    );
    let ordinary = [0; 8];
    let mut fixed = open_page_values(
        Type::FIXED_LEN_BYTE_ARRAY,
        4,
        None,
        Encoding::BYTE_STREAM_SPLIT,
        &ordinary,
        1,
        None,
    )
    .unwrap();
    assert_eq!(fixed.next_length().unwrap(), Some(4));
    assert_eq!(fixed.next_length().unwrap(), None);

    // The pinned fixed-width numeric decoder permits an unused byte tail.
    // Keep that behavior distinct from its variable-width binary decoder.
    let mut numeric = open_page_values(
        Type::INT32,
        4,
        None,
        Encoding::BYTE_STREAM_SPLIT,
        &malformed,
        1,
        None,
    )
    .unwrap();
    assert_eq!(numeric.next_length().unwrap(), Some(4));
    assert_eq!(numeric.next_length().unwrap(), None);
}
