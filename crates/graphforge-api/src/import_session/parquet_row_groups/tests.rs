use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use graphforge_core::{GfError, ProjectErrorCode};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::array_reader::RowGroups;
use parquet::arrow::schema::parquet_to_arrow_field_levels;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use super::OwnedRowGroups;
use crate::CancellationToken;
use crate::import_session::inventory_budget::InventoryBudget;
use crate::import_session::parquet_page_decode::DecodedPage;
use crate::import_session::parquet_reader::{PageFailures, PagePreflight};

#[derive(Clone)]
struct Probe {
    calls: Arc<AtomicUsize>,
    refusal: bool,
}

impl PagePreflight for Probe {
    fn remaining_workspace(&self) -> Result<usize, GfError> {
        Ok(64 << 20)
    }

    fn validate(&mut self, _: &DecodedPage) -> Result<(), GfError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.refusal {
            return Err(resource_limit("test preflight refusal"));
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), GfError> {
        Ok(())
    }
}

fn source() -> (Bytes, Arc<parquet::file::metadata::ParquetMetaData>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..12)),
            Arc::new(StringArray::from_iter_values(
                (0..12).map(|value| format!("row-{value}")),
            )),
        ],
    )
    .unwrap();
    let mut output = Vec::new();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut writer = ArrowWriter::try_new(&mut output, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = Bytes::from(output);
    let metadata = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .metadata()
        .clone();
    assert_eq!(metadata.num_row_groups(), 3);
    (bytes, metadata)
}

fn row_groups<F>(
    bytes: Bytes,
    metadata: Arc<parquet::file::metadata::ParquetMetaData>,
    groups: &[usize],
    budget: &mut InventoryBudget,
    factory: F,
) -> Result<OwnedRowGroups<Bytes, F, Probe>, GfError>
where
    F: Fn(usize, usize) -> Result<Probe, GfError> + Send + Sync + 'static,
{
    OwnedRowGroups::new(
        Arc::new(bytes),
        metadata,
        groups,
        budget,
        factory,
        None,
        PageFailures::new(),
    )
}

#[test]
fn public_arrow_reader_routes_all_selected_physical_columns_through_preflight() {
    let (bytes, metadata) = source();
    let calls = Arc::new(AtomicUsize::new(0));
    let chunks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut budget = InventoryBudget::new(1 << 20);
    let groups = row_groups(bytes, Arc::clone(&metadata), &[0, 1, 2], &mut budget, {
        let calls = Arc::clone(&calls);
        let chunks = Arc::clone(&chunks);
        move |group, column| {
            chunks.lock().unwrap().push((group, column));
            Ok(Probe {
                calls: Arc::clone(&calls),
                refusal: false,
            })
        }
    })
    .unwrap();
    assert_eq!(groups.num_rows(), 12);
    assert_eq!(groups.row_groups().count(), 3);

    let levels = parquet_to_arrow_field_levels(
        metadata.file_metadata().schema_descr(),
        ProjectionMask::all(),
        None,
    )
    .unwrap();
    let mut reader =
        ParquetRecordBatchReader::try_new_with_row_groups(&levels, &groups, 6, None).unwrap();
    let batches = reader.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        [6, 6]
    );
    assert_eq!(
        batches[0].column(0).as_ref(),
        &Int32Array::from_iter_values(0..6)
    );
    assert_eq!(
        batches[1].column(1).as_ref(),
        &StringArray::from_iter_values((6..12).map(|value| format!("row-{value}")))
    );
    let mut chunk_indices = chunks.lock().unwrap().clone();
    chunk_indices.sort_unstable();
    assert_eq!(
        chunk_indices,
        [(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)]
    );
    assert!(
        calls.load(Ordering::Relaxed) >= 6,
        "each dispatched data page must pass through the preflight callback"
    );
}

#[test]
fn projected_column_keeps_its_original_physical_leaf_index() {
    let (bytes, metadata) = source();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut budget = InventoryBudget::new(1 << 20);
    let groups = row_groups(bytes, Arc::clone(&metadata), &[1], &mut budget, {
        let calls = Arc::clone(&calls);
        move |group, column| {
            assert_eq!(group, 1);
            assert_eq!(column, 1, "projection must not renumber physical leaves");
            Ok(Probe {
                calls: Arc::clone(&calls),
                refusal: false,
            })
        }
    })
    .unwrap();
    let levels = parquet_to_arrow_field_levels(
        metadata.file_metadata().schema_descr(),
        ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [1]),
        None,
    )
    .unwrap();
    let batches = ParquetRecordBatchReader::try_new_with_row_groups(&levels, &groups, 2, None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    assert_eq!(batches[0].num_columns(), 1);
    assert!(calls.load(Ordering::Relaxed) > 0);
}

#[test]
fn typed_callback_refusal_reaches_arrow_before_the_page_is_decoded() {
    let (bytes, metadata) = source();
    let mut budget = InventoryBudget::new(1 << 20);
    let failures = PageFailures::new();
    let groups = OwnedRowGroups::new(
        Arc::new(bytes),
        Arc::clone(&metadata),
        &[0],
        &mut budget,
        |_, _| {
            Ok(Probe {
                calls: Arc::new(AtomicUsize::new(0)),
                refusal: true,
            })
        },
        None,
        failures.clone(),
    )
    .unwrap();
    let levels = parquet_to_arrow_field_levels(
        metadata.file_metadata().schema_descr(),
        ProjectionMask::all(),
        None,
    )
    .unwrap();
    let mut reader =
        ParquetRecordBatchReader::try_new_with_row_groups(&levels, &groups, 4, None).unwrap();
    let error = reader.next().unwrap().unwrap_err();
    assert!(error.to_string().contains("test preflight refusal"));
    failures.record(GfError::Storage(
        "later failure must not replace the first".into(),
    ));
    let typed = failures
        .take()
        .expect("the shared slot retains the original typed GraphForge error");
    assert!(matches!(
        &typed,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
    assert!(typed.to_string().contains("test preflight refusal"));
    assert!(
        failures.take().is_none(),
        "the captured failure is consumed once"
    );
    assert!(
        reader.next().is_none(),
        "terminal refusal must not retry an unvalidated page"
    );
}

#[test]
fn row_group_selection_and_input_geometry_are_validated() {
    let (bytes, metadata) = source();
    for selection in [&[1, 0][..], &[1, 1][..], &[3][..]] {
        let mut budget = InventoryBudget::new(1 << 20);
        let result = row_groups(
            bytes.clone(),
            Arc::clone(&metadata),
            selection,
            &mut budget,
            |_, _| {
                Ok(Probe {
                    calls: Arc::new(AtomicUsize::new(0)),
                    refusal: false,
                })
            },
        );
        assert!(
            result.is_err(),
            "invalid selection {selection:?} was accepted"
        );
    }

    let final_column = metadata.row_group(2).column(1);
    let (final_start, final_length) = final_column.byte_range();
    let short_len = usize::try_from(
        final_start
            .checked_add(final_length)
            .unwrap()
            .checked_sub(1)
            .unwrap(),
    )
    .unwrap();
    let short = Bytes::from(bytes[..short_len].to_vec());
    let mut budget = InventoryBudget::new(1 << 20);
    assert!(
        row_groups(short, metadata, &[0, 1, 2], &mut budget, |_, _| {
            Ok(Probe {
                calls: Arc::new(AtomicUsize::new(0)),
                refusal: false,
            })
        },)
        .is_err()
    );
}

#[test]
fn physical_column_index_and_inventory_budget_are_checked() {
    let (bytes, metadata) = source();
    let mut budget = InventoryBudget::new(1 << 20);
    let groups = row_groups(
        bytes.clone(),
        Arc::clone(&metadata),
        &[0],
        &mut budget,
        |_, _| {
            Ok(Probe {
                calls: Arc::new(AtomicUsize::new(0)),
                refusal: false,
            })
        },
    )
    .unwrap();
    assert!(groups.column_chunks(2).is_err());

    let mut exhausted = InventoryBudget::new(0);
    let error = match row_groups(bytes, metadata, &[0], &mut exhausted, |_, _| {
        Ok(Probe {
            calls: Arc::new(AtomicUsize::new(0)),
            refusal: false,
        })
    }) {
        Ok(_) => panic!("zero-byte index budget admitted a retained selection"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
}

#[test]
fn empty_selection_still_observes_cancellation() {
    let (bytes, metadata) = source();
    let token = CancellationToken::new();
    token.cancel();
    let mut budget = InventoryBudget::new(1 << 20);
    let result = OwnedRowGroups::<Bytes, _, Probe>::new(
        Arc::new(bytes),
        metadata,
        &[],
        &mut budget,
        |_, _| {
            Ok(Probe {
                calls: Arc::new(AtomicUsize::new(0)),
                refusal: false,
            })
        },
        Some(token),
        PageFailures::new(),
    );
    assert!(result.is_err());
}

#[test]
fn empty_selection_is_a_valid_zero_row_collection() {
    let (bytes, metadata) = source();
    let mut budget = InventoryBudget::new(0);
    let groups = OwnedRowGroups::<Bytes, _, Probe>::new(
        Arc::new(bytes),
        metadata,
        &[],
        &mut budget,
        |_, _| {
            Ok(Probe {
                calls: Arc::new(AtomicUsize::new(0)),
                refusal: false,
            })
        },
        None,
        PageFailures::new(),
    )
    .unwrap();
    assert_eq!(groups.num_rows(), 0);
    assert_eq!(groups.row_groups().count(), 0);
    assert_eq!(groups.column_chunks(0).unwrap().count(), 0);
}

fn resource_limit(message: &str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ResourceLimit,
        message: message.into(),
    }
}
