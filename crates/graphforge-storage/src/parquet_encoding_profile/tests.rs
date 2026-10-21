#![allow(
    deprecated,
    reason = "Parquet 58 page headers are parquet::format types"
)]
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use bytes::Bytes;
use parquet::basic::Encoding;
use parquet::errors::Result as ParquetResult;
use parquet::file::properties::WriterVersion;
use parquet::file::reader::{ChunkReader, Length};

use super::*;

#[path = "../../tests/support/hostile_parquet.rs"]
mod hostile;
use hostile::{
    data_page_offset, declaring, metadata, patch_header, properties, string_file, strings, write,
};

fn admit(file: Vec<u8>) -> Result<(), PageAdmissionError> {
    admit_pages(&Bytes::from(file))
}

#[test]
fn unfunded_value_encodings_are_refused_for_version_1_and_2_pages() {
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        for encoding in [
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DELTA_BYTE_ARRAY,
        ] {
            let file = string_file(encoding, version, 64);
            assert!(
                metadata(&file)
                    .row_group(0)
                    .column(0)
                    .encodings()
                    .any(|declared| declared == encoding),
                "{encoding:?} must really be the written encoding"
            );
            assert_eq!(
                admit(file),
                Err(PageAdmissionError::Unfunded),
                "{encoding:?} on {version:?}"
            );
        }
    }
}

#[test]
fn the_funded_profile_is_admitted() {
    let ints: ArrayRef = Arc::new(Int64Array::from_iter_values(0..256));
    let doubles: ArrayRef = Arc::new(Float64Array::from_iter_values((0..256).map(f64::from)));
    let flags: ArrayRef = Arc::new(BooleanArray::from(vec![true; 256]));
    let cases: Vec<(ArrayRef, Encoding)> = vec![
        (strings(256), Encoding::PLAIN),
        (ints.clone(), Encoding::PLAIN),
        (ints, Encoding::DELTA_BINARY_PACKED),
        (doubles, Encoding::BYTE_STREAM_SPLIT),
        (flags, Encoding::RLE),
    ];
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        for (array, encoding) in &cases {
            let file = write(array.clone(), false, properties(*encoding, version, 256));
            assert_eq!(admit(file), Ok(()), "{encoding:?} on {version:?}");
        }
    }
    // Dictionary pages and RLE levels of a nullable column are funded too.
    let nullable: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("a")]));
    let file = write(
        nullable,
        true,
        parquet::file::properties::WriterProperties::builder().build(),
    );
    assert_eq!(admit(file), Ok(()));
}

#[test]
fn a_footer_declaring_plain_cannot_hide_unfunded_pages() {
    let file = string_file(
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        WriterVersion::PARQUET_1_0,
        64,
    );
    let lying = declaring(&file, &[Encoding::PLAIN, Encoding::RLE]);
    assert!(
        metadata(&lying)
            .row_group(0)
            .column(0)
            .encodings()
            .all(|declared| matches!(declared, Encoding::PLAIN | Encoding::RLE)),
        "the footer must declare only funded encodings"
    );
    assert!(declared_encoding_is_funded(
        Encoding::PLAIN,
        Type::BYTE_ARRAY
    ));
    assert_eq!(admit(lying), Err(PageAdmissionError::Unfunded));
}

#[test]
fn dictionary_page_encodings_are_checked() {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..64).map(|row| format!("v{}", row % 4)),
    ));
    let mut file = write(
        values,
        false,
        parquet::file::properties::WriterProperties::builder().build(),
    );
    assert_eq!(admit(file.clone()), Ok(()), "unmodified dictionary file");
    let column_offset = u64::try_from(
        metadata(&file)
            .row_group(0)
            .column(0)
            .dictionary_page_offset()
            .expect("dictionary page"),
    )
    .unwrap();
    patch_header(&mut file, column_offset, |header| {
        header.dictionary_page_header.as_mut().unwrap().encoding =
            parquet::format::Encoding::DELTA_LENGTH_BYTE_ARRAY;
    });
    assert_eq!(admit(file), Err(PageAdmissionError::Unfunded));
}

#[test]
fn version_1_level_encodings_are_checked() {
    let nullable: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]));
    for definition in [true, false] {
        let mut file = write(
            nullable.clone(),
            true,
            properties(Encoding::PLAIN, WriterVersion::PARQUET_1_0, 3),
        );
        assert_eq!(admit(file.clone()), Ok(()), "unmodified nullable file");
        let offset = data_page_offset(&file);
        patch_header(&mut file, offset, |header| {
            let page = header.data_page_header.as_mut().unwrap();
            if definition {
                page.definition_level_encoding = parquet::format::Encoding::DELTA_BINARY_PACKED;
            } else {
                page.repetition_level_encoding = parquet::format::Encoding::DELTA_BINARY_PACKED;
            }
        });
        assert_eq!(
            admit(file),
            Err(PageAdmissionError::Unfunded),
            "definition levels: {definition}"
        );
    }
}

#[derive(Debug)]
struct Recording {
    bytes: Bytes,
    reads: std::sync::Mutex<Vec<(u64, usize)>>,
}

impl Length for Recording {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

impl ChunkReader for Recording {
    type T = std::io::Cursor<Bytes>;
    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        Ok(std::io::Cursor::new(self.bytes.slice(start as usize..)))
    }
    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.reads.lock().unwrap().push((start, length));
        self.bytes.get_bytes(start, length)
    }
}

#[test]
fn only_the_footer_and_the_first_header_window_are_read_before_refusal() {
    // Large values make the page body far larger than any header window.
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..16).map(|row| format!("{row}{}", "x".repeat(64 * 1024))),
    ));
    let file = write(
        values,
        false,
        properties(
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            WriterVersion::PARQUET_1_0,
            16,
        ),
    );
    let column = metadata(&file).row_group(0).column(0).clone();
    let body = u64::try_from(column.compressed_size()).unwrap();
    let start = u64::try_from(column.data_page_offset()).unwrap();
    let footer = u32::from_le_bytes(file[file.len() - 8..file.len() - 4].try_into().unwrap());
    let footer_start = file.len() as u64 - 8 - u64::from(footer);
    let source = Recording {
        bytes: Bytes::from(file),
        reads: std::sync::Mutex::new(Vec::new()),
    };
    assert_eq!(admit_pages(&source), Err(PageAdmissionError::Unfunded));
    assert!(
        body > 1024 * 1024,
        "the page body must dwarf a header window"
    );
    for (offset, length) in source.reads.lock().unwrap().iter().copied() {
        assert!(
            offset >= footer_start || (offset == start && length <= FIRST_HEADER_WINDOW),
            "unexpected read of {length} bytes at {offset}"
        );
    }
}

#[test]
fn objects_that_are_not_readable_parquet_are_not_admitted() {
    let good = string_file(Encoding::PLAIN, WriterVersion::PARQUET_1_0, 8);
    let mut no_magic = good.clone();
    let last = no_magic.len() - 1;
    no_magic[last] ^= 0xff;
    let mut bad_footer_length = good.clone();
    let length = bad_footer_length.len();
    bad_footer_length[length - 8..length - 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut bad_page = good.clone();
    let offset = usize::try_from(data_page_offset(&good)).unwrap();
    bad_page[offset..offset + 4].fill(0xff);
    for (name, file) in [
        ("short", b"PAR1".to_vec()),
        ("no footer magic", no_magic),
        ("oversized footer", bad_footer_length),
        ("garbage page header", bad_page),
    ] {
        assert!(
            matches!(admit(file), Err(PageAdmissionError::Malformed(_))),
            "{name}"
        );
    }
}
