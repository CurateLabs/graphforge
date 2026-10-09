//! A Parquet footer is parsed into structures larger than its bytes; the
//! planner refuses a footer it could not hold before reading it (#1918).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

use super::{FOOTER_PARSE_FACTOR, require_footer_fits};

/// Parsed metadata bytes per footer byte of a file of this shape.
fn parse_ratio(columns: usize, groups: usize, rows_per_group: usize) -> f64 {
    let fields = (0..columns)
        .map(|index| {
            Field::new(
                format!("column_{index:05}"),
                if index % 2 == 0 {
                    DataType::Int64
                } else {
                    DataType::Utf8
                },
                true,
            )
        })
        .collect::<Vec<_>>();
    let arrays = (0..columns)
        .map(|index| -> ArrayRef {
            if index % 2 == 0 {
                Arc::new(Int64Array::from(vec![7_i64; rows_per_group]))
            } else {
                Arc::new(StringArray::from(vec!["value"; rows_per_group]))
            }
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_group))
        .build();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(properties)).unwrap();
    for _ in 0..groups {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    let length = std::fs::metadata(file.path()).unwrap().len();
    let mut handle = File::open(file.path()).unwrap();
    handle.seek(SeekFrom::Start(length - 8)).unwrap();
    let mut trailer = [0_u8; 8];
    handle.read_exact(&mut trailer).unwrap();
    let footer = u64::from(u32::from_le_bytes(trailer[..4].try_into().unwrap()));
    let metadata = ParquetRecordBatchReaderBuilder::try_new(File::open(file.path()).unwrap())
        .unwrap()
        .metadata()
        .clone();
    metadata.memory_size() as f64 / footer as f64
}

#[test]
fn parsed_footer_metadata_stays_within_the_factor_the_planner_assumes() {
    let mut worst = 0.0_f64;
    for (columns, groups, rows) in [(3, 400, 8), (60, 200, 8), (4_000, 2, 16), (8, 3, 100_000)] {
        let ratio = parse_ratio(columns, groups, rows);
        println!("footer shape {columns}x{groups}: {ratio:.2}");
        worst = worst.max(ratio);
    }
    assert!(
        worst <= FOOTER_PARSE_FACTOR as f64 / 2.0,
        "parsed metadata is {worst:.2} times its footer"
    );
}

#[test]
fn a_footer_the_budget_cannot_parse_is_refused_before_it_is_read() {
    require_footer_fits(1 << 20, 1 << 30).unwrap();
    let error = require_footer_fits(100 << 20, 1 << 30).unwrap_err();
    assert!(matches!(
        error,
        graphforge_core::GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
    // Under a gigabyte the build is staged, which this check does not gate.
    require_footer_fits(19_313, 512 << 10).unwrap();
}
