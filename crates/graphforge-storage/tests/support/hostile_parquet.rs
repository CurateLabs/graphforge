//! Hand-built Parquet files that the encoding profile must refuse.
//!
//! Shared by the storage unit tests and the ingress integration test through
//! `#[path]`; it depends only on `arrow`, `parquet`, `bytes` and `thrift`.
#![allow(dead_code, deprecated, reason = "shared test support")]

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::{
    ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter, RowGroupMetaData,
};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::thrift::TSerializable;

/// Writer properties for a single column forced onto `encoding`.
pub fn properties(encoding: Encoding, version: WriterVersion, rows: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(version)
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(encoding)
        .set_write_batch_size(rows.max(1))
        .set_data_page_row_count_limit(rows.max(1))
        .set_max_row_group_row_count(Some(rows.max(1)))
        .build()
}

/// Write one column and return the complete file.
pub fn write(array: ArrayRef, nullable: bool, properties: WriterProperties) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        array.data_type().clone(),
        nullable,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    output
}

/// `rows` distinct short strings.
pub fn strings(rows: usize) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("value-{row}")),
    ))
}

/// A required string column written with `encoding` on the given page version.
pub fn string_file(encoding: Encoding, version: WriterVersion, rows: usize) -> Vec<u8> {
    write(strings(rows), false, properties(encoding, version, rows))
}

pub fn metadata(file: &[u8]) -> ParquetMetaData {
    let length = file.len();
    let footer = u32::from_le_bytes(file[length - 8..length - 4].try_into().unwrap()) as usize;
    ParquetMetaDataReader::decode_metadata(&file[length - 8 - footer..length - 8]).unwrap()
}

/// The same pages with a footer that declares `encodings` for every column.
/// Page offsets are untouched: only the declaration lies.
pub fn declaring(file: &[u8], encodings: &[Encoding]) -> Vec<u8> {
    let original = metadata(file);
    let length = file.len();
    let footer = u32::from_le_bytes(file[length - 8..length - 4].try_into().unwrap()) as usize;
    let groups = original
        .row_groups()
        .iter()
        .map(|group| {
            let columns = group
                .columns()
                .iter()
                .map(|column| {
                    column
                        .clone()
                        .into_builder()
                        .set_encodings(encodings.to_vec())
                        .build()
                        .unwrap()
                })
                .collect();
            RowGroupMetaData::builder(group.schema_descr_ptr())
                .set_num_rows(group.num_rows())
                .set_total_byte_size(group.total_byte_size())
                .set_column_metadata(columns)
                .build()
                .unwrap()
        })
        .collect();
    let lying = ParquetMetaData::new(original.file_metadata().clone(), groups);
    let mut output = file[..length - 8 - footer].to_vec();
    ParquetMetaDataWriter::new(&mut output, &lying)
        .finish()
        .unwrap();
    output
}

/// Parse the page header at `offset`, edit it, and write it back in place.
/// Panics unless the edited header has the same encoded length.
pub fn patch_header(
    file: &mut [u8],
    offset: u64,
    edit: impl FnOnce(&mut parquet::format::PageHeader),
) {
    let offset = usize::try_from(offset).unwrap();
    let mut cursor = Cursor::new(&file[offset..]);
    let mut input = thrift::protocol::TCompactInputProtocol::new(&mut cursor);
    let mut header = parquet::format::PageHeader::read_from_in_protocol(&mut input).unwrap();
    drop(input);
    let length = usize::try_from(cursor.position()).unwrap();
    edit(&mut header);
    let mut encoded = Vec::new();
    let mut output = thrift::protocol::TCompactOutputProtocol::new(&mut encoded);
    header.write_to_out_protocol(&mut output).unwrap();
    drop(output);
    assert_eq!(encoded.len(), length, "edit changed the header length");
    file[offset..offset + length].copy_from_slice(&encoded);
}

/// Offset of the first data page of the sole column.
pub fn data_page_offset(file: &[u8]) -> u64 {
    u64::try_from(metadata(file).row_group(0).column(0).data_page_offset()).unwrap()
}

/// A required string column of `rows` empty values in one `DELTA_LENGTH_BYTE_ARRAY`
/// page. `rows` must need a four byte varint (2^21 ..< 2^28).
pub fn single_page_delta_length(rows: usize) -> Vec<u8> {
    assert!((1 << 21..1 << 28).contains(&rows));
    let array: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n("", rows)));
    let file = write(
        array,
        false,
        properties(
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            WriterVersion::PARQUET_1_0,
            rows,
        ),
    );
    assert_eq!(
        metadata(&file).row_group(0).column(0).num_values() as usize,
        rows
    );
    file
}

/// Rewrite the delta count embedded in the sole `DELTA_LENGTH_BYTE_ARRAY` page of
/// [`single_page_delta_length`] to `count`, keeping every length and the footer.
/// `parquet` 58.4 allocates a vector of `count` lengths before reading any.
pub fn inflate_embedded_count(file: &mut [u8], count: u64) {
    assert!((1 << 21..1 << 28).contains(&count));
    let offset = usize::try_from(data_page_offset(file)).unwrap();
    let mut cursor = Cursor::new(&file[offset..]);
    let mut input = thrift::protocol::TCompactInputProtocol::new(&mut cursor);
    parquet::format::PageHeader::read_from_in_protocol(&mut input).unwrap();
    drop(input);
    let mut at = offset + usize::try_from(cursor.position()).unwrap();
    // DELTA_BINARY_PACKED header: block size, miniblocks per block, total count.
    for _ in 0..2 {
        while file[at] & 0x80 != 0 {
            at += 1;
        }
        at += 1;
    }
    let width = file[at..]
        .iter()
        .take_while(|byte| **byte & 0x80 != 0)
        .count()
        + 1;
    assert_eq!(
        width, 4,
        "the page's total count must be a four byte varint"
    );
    let mut value = count;
    for index in 0..4 {
        let byte = u8::try_from(value & 0x7f).unwrap();
        value >>= 7;
        file[at + index] = if index < 3 { byte | 0x80 } else { byte };
    }
}
