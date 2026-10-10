use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use graphforge_core::{GfError, ProjectErrorCode};
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::arrow::schema::parquet_to_arrow_field_levels;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::{WriterProperties, WriterVersion};

use super::OwnedRowGroups;
use crate::CancellationToken;
use crate::import_session::inventory_budget::InventoryBudget;
use crate::import_session::parquet_page_decode::DecodedPage;
use crate::import_session::parquet_reader::{PageFailures, PagePreflight};

const I32: u8 = 5;
const STRUCT: u8 = 12;

#[derive(Clone, Copy)]
enum EmptyPageVersion {
    V1,
    V2,
}

#[derive(Clone)]
struct CountPages {
    finished: Arc<AtomicUsize>,
    data_events: Arc<AtomicUsize>,
    refuse_empty: bool,
}

impl PagePreflight for CountPages {
    fn remaining_workspace(&self) -> Result<usize, GfError> {
        Ok(1 << 20)
    }

    fn validate(&mut self, decoded: &DecodedPage) -> Result<(), GfError> {
        if decoded.page.is_data_page() {
            let values = usize::try_from(decoded.page.num_values()).unwrap();
            self.data_events.fetch_add(values, Ordering::Relaxed);
            if self.refuse_empty && values == 0 {
                return Err(resource_limit("empty data page refused by preflight"));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), GfError> {
        self.finished.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct Fixture {
    bytes: Bytes,
    metadata: Arc<ParquetMetaData>,
    schema: Arc<Schema>,
    rows: usize,
}

impl Fixture {
    fn new(version: EmptyPageVersion, footer_events: i64) -> Self {
        let rows = 32_usize;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(
                0..i32::try_from(rows).unwrap(),
            ))],
        )
        .unwrap();
        let writer_version = match version {
            EmptyPageVersion::V1 => WriterVersion::PARQUET_1_0,
            EmptyPageVersion::V2 => WriterVersion::PARQUET_2_0,
        };
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_writer_version(writer_version)
            .build();
        let mut file_bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut file_bytes, schema.clone(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file_bytes = Bytes::from(file_bytes);
        let source_metadata = ParquetRecordBatchReaderBuilder::try_new(file_bytes.clone())
            .unwrap()
            .metadata()
            .clone();
        let source_group = source_metadata.row_group(0);
        let source_column = source_group.column(0);
        assert_eq!(usize::try_from(source_group.num_rows()).unwrap(), rows);
        assert_eq!(source_column.num_values(), i64::try_from(rows).unwrap());
        assert_eq!(source_column.dictionary_page_offset(), None);
        let (chunk_start, chunk_length) = source_column.byte_range();
        let chunk_start = usize::try_from(chunk_start).unwrap();
        let chunk_end = chunk_start
            .checked_add(usize::try_from(chunk_length).unwrap())
            .unwrap();

        let mut chunk = Vec::new();
        for _ in 0..2 {
            chunk.extend_from_slice(&empty_data_page(version));
        }
        chunk.extend_from_slice(&file_bytes[chunk_start..chunk_end]);
        let chunk_length = i64::try_from(chunk.len()).unwrap();

        let column = source_column
            .clone()
            .into_builder()
            .set_num_values(footer_events)
            .set_total_compressed_size(chunk_length)
            .set_total_uncompressed_size(chunk_length)
            .set_data_page_offset(0)
            .set_dictionary_page_offset(None)
            .build()
            .unwrap();
        let group = RowGroupMetaData::builder(source_group.schema_descr_ptr())
            .set_num_rows(i64::try_from(rows).unwrap())
            .set_total_byte_size(chunk_length)
            .add_column_metadata(column)
            .build()
            .unwrap();
        let metadata = Arc::new(ParquetMetaData::new(
            source_metadata.file_metadata().clone(),
            vec![group],
        ));

        Self {
            bytes: Bytes::from(chunk),
            metadata,
            schema,
            rows,
        }
    }

    fn row_groups<F>(
        &self,
        failures: PageFailures,
        factory: F,
    ) -> OwnedRowGroups<Bytes, F, CountPages>
    where
        F: Fn(usize, usize) -> Result<CountPages, GfError> + Send + Sync + 'static,
    {
        let mut budget = InventoryBudget::new(1 << 20);
        OwnedRowGroups::new(
            Arc::new(self.bytes.clone()),
            Arc::clone(&self.metadata),
            &[0],
            &mut budget,
            factory,
            None::<CancellationToken>,
            failures,
        )
        .unwrap()
    }

    fn reader(
        &self,
        batch_rows: usize,
        selection: Option<RowSelection>,
        finished: Arc<AtomicUsize>,
        data_events: Arc<AtomicUsize>,
        failures: PageFailures,
        refuse_empty: bool,
    ) -> ParquetRecordBatchReader {
        let finished_counter = finished;
        let event_counter = data_events;
        let groups = self.row_groups(failures, {
            move |_, _| {
                Ok(CountPages {
                    finished: Arc::clone(&finished_counter),
                    data_events: Arc::clone(&event_counter),
                    refuse_empty,
                })
            }
        });
        let levels = parquet_to_arrow_field_levels(
            self.metadata.file_metadata().schema_descr(),
            ProjectionMask::all(),
            Some(self.schema.fields()),
        )
        .unwrap();
        ParquetRecordBatchReader::try_new_with_row_groups(&levels, &groups, batch_rows, selection)
            .unwrap()
    }
}

#[test]
fn consecutive_empty_v1_and_v2_pages_do_not_hide_following_data() {
    for version in [EmptyPageVersion::V1, EmptyPageVersion::V2] {
        assert_empty_pages_and_data(version);
    }
}

#[test]
fn row_selection_skips_rows_after_consecutive_empty_pages() {
    for version in [EmptyPageVersion::V1, EmptyPageVersion::V2] {
        let fixture = Fixture::new(version, 32);
        let finished = Arc::new(AtomicUsize::new(0));
        let data_events = Arc::new(AtomicUsize::new(0));
        let failures = PageFailures::new();
        let selection = RowSelection::from(vec![
            RowSelector::skip(7),
            RowSelector::select(fixture.rows - 7),
        ]);
        let batches = fixture
            .reader(
                5,
                Some(selection),
                Arc::clone(&finished),
                Arc::clone(&data_events),
                failures,
                false,
            )
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let output = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(output, (7..32).collect::<Vec<_>>());
        assert_eq!(data_events.load(Ordering::Relaxed), 32);
    }
}

#[test]
fn an_overstated_footer_event_count_fails_instead_of_finishing_as_eof() {
    let fixture = Fixture::new(EmptyPageVersion::V1, 33);
    let finished = Arc::new(AtomicUsize::new(0));
    let data_events = Arc::new(AtomicUsize::new(0));
    let failures = PageFailures::new();
    let reader = fixture.reader(
        fixture.rows,
        None,
        Arc::clone(&finished),
        Arc::clone(&data_events),
        failures.clone(),
        false,
    );
    let error = reader.collect::<Result<Vec<_>, _>>().unwrap_err();
    assert!(error.to_string().contains("footer event count"));
    assert_eq!(finished.load(Ordering::Relaxed), 0);
    assert_eq!(data_events.load(Ordering::Relaxed), 32);
    assert!(matches!(failures.take(), Some(GfError::Storage(_))));
}

#[test]
fn empty_page_preflight_refusal_is_typed_before_later_data() {
    let fixture = Fixture::new(EmptyPageVersion::V1, 32);
    let finished = Arc::new(AtomicUsize::new(0));
    let data_events = Arc::new(AtomicUsize::new(0));
    let failures = PageFailures::new();
    let reader = fixture.reader(
        fixture.rows,
        None,
        Arc::clone(&finished),
        Arc::clone(&data_events),
        failures.clone(),
        true,
    );
    let error = reader.into_iter().next().unwrap().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("empty data page refused by preflight")
    );
    assert_eq!(data_events.load(Ordering::Relaxed), 0);
    assert_eq!(finished.load(Ordering::Relaxed), 0);
    assert!(matches!(
        failures.take(),
        Some(GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        })
    ));
}

fn assert_empty_pages_and_data(version: EmptyPageVersion) {
    let fixture = Fixture::new(version, 32);
    let finished = Arc::new(AtomicUsize::new(0));
    let data_events = Arc::new(AtomicUsize::new(0));
    let reader = fixture.reader(
        9,
        None,
        Arc::clone(&finished),
        Arc::clone(&data_events),
        PageFailures::new(),
        false,
    );
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let output = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(output, (0..32).collect::<Vec<_>>());
    assert_eq!(data_events.load(Ordering::Relaxed), 32);
    assert_eq!(finished.load(Ordering::Relaxed), 1);
}

fn empty_data_page(version: EmptyPageVersion) -> Vec<u8> {
    match version {
        EmptyPageVersion::V1 => empty_v1_data_page(),
        EmptyPageVersion::V2 => empty_v2_data_page(),
    }
}

fn empty_v1_data_page() -> Vec<u8> {
    let mut output = Vec::new();
    let mut previous = 0;
    field(&mut output, &mut previous, 1, I32);
    compact_int(0, &mut output); // DATA_PAGE
    field(&mut output, &mut previous, 2, I32);
    compact_int(0, &mut output); // compressed_page_size
    field(&mut output, &mut previous, 3, I32);
    compact_int(0, &mut output); // uncompressed_page_size
    field(&mut output, &mut previous, 5, STRUCT);
    let mut data_previous = 0;
    field(&mut output, &mut data_previous, 1, I32);
    compact_int(0, &mut output); // num_values
    field(&mut output, &mut data_previous, 2, I32);
    compact_int(0, &mut output); // PLAIN
    field(&mut output, &mut data_previous, 3, I32);
    compact_int(3, &mut output); // RLE definition levels
    field(&mut output, &mut data_previous, 4, I32);
    compact_int(3, &mut output); // RLE repetition levels
    output.push(0); // end DataPageHeader
    output.push(0); // end PageHeader
    output
}

fn empty_v2_data_page() -> Vec<u8> {
    let mut output = Vec::new();
    let mut previous = 0;
    field(&mut output, &mut previous, 1, I32);
    compact_int(3, &mut output); // DATA_PAGE_V2
    field(&mut output, &mut previous, 2, I32);
    compact_int(0, &mut output); // compressed_page_size
    field(&mut output, &mut previous, 3, I32);
    compact_int(0, &mut output); // uncompressed_page_size
    field(&mut output, &mut previous, 8, STRUCT);
    let mut data_previous = 0;
    field(&mut output, &mut data_previous, 1, I32);
    compact_int(0, &mut output); // num_values
    field(&mut output, &mut data_previous, 2, I32);
    compact_int(0, &mut output); // num_nulls
    field(&mut output, &mut data_previous, 3, I32);
    compact_int(0, &mut output); // num_rows
    field(&mut output, &mut data_previous, 4, I32);
    compact_int(0, &mut output); // PLAIN
    field(&mut output, &mut data_previous, 5, I32);
    compact_int(0, &mut output); // definition_levels_byte_length
    field(&mut output, &mut data_previous, 6, I32);
    compact_int(0, &mut output); // repetition_levels_byte_length
    output.push(0); // end DataPageHeaderV2
    output.push(0); // end PageHeader
    output
}

fn field(output: &mut Vec<u8>, previous: &mut i16, id: i16, kind: u8) {
    let delta = id - *previous;
    if (1..=15).contains(&delta) {
        output.push((u8::try_from(delta).unwrap() << 4) | kind);
    } else {
        output.push(kind);
        compact_int(i64::from(id), output);
    }
    *previous = id;
}

#[allow(clippy::cast_possible_wrap)] // Compact-protocol ZigZag encoding uses the sign mask.
fn compact_int(value: i64, output: &mut Vec<u8>) {
    let encoded = ((value as u64) << 1) ^ ((value >> 63) as u64);
    let mut remaining = encoded;
    loop {
        let byte = u8::try_from(remaining & 0x7f).unwrap();
        remaining >>= 7;
        output.push(byte | if remaining == 0 { 0 } else { 0x80 });
        if remaining == 0 {
            break;
        }
    }
}

fn resource_limit(message: &str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ResourceLimit,
        message: message.into(),
    }
}
