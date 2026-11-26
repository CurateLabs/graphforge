use std::io::{Cursor, Read};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::{FileReader, SerializedFileReader};
use thrift::protocol::{
    TCompactOutputProtocol, TFieldIdentifier, TOutputProtocol, TStructIdentifier, TType,
};

use super::{HeaderError, PageEncoding, PageHeader, PageType};

#[path = "../../tests/support/hostile_parquet.rs"]
mod hostile;

fn write_file(version: WriterVersion, dictionary: bool) -> bytes::Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..64));
    let names: ArrayRef = Arc::new(
        (0..64)
            .map(|row| (row % 3 != 0).then(|| format!("n{}", row % 5)))
            .collect::<StringArray>(),
    );
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![ids, names]).unwrap();
    let properties = WriterProperties::builder()
        .set_writer_version(version)
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(dictionary)
        .build();
    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes::Bytes::from(output)
}

/// Every page header Parquet itself writes decodes to the page type, sizes,
/// value count and encodings Parquet's own reader reports for that page.
#[test]
fn decodes_every_header_parquet_writes() {
    for (version, dictionary) in [
        (WriterVersion::PARQUET_1_0, false),
        (WriterVersion::PARQUET_1_0, true),
        (WriterVersion::PARQUET_2_0, false),
        (WriterVersion::PARQUET_2_0, true),
    ] {
        let file = write_file(version, dictionary);
        let reader = SerializedFileReader::new(file.clone()).unwrap();
        let metadata = reader.metadata();
        let mut pages_seen = 0;
        for group in 0..metadata.num_row_groups() {
            let row_group = reader.get_row_group(group).unwrap();
            for column in 0..metadata.row_group(group).num_columns() {
                let chunk = metadata.row_group(group).column(column);
                let start = chunk
                    .dictionary_page_offset()
                    .unwrap_or(chunk.data_page_offset());
                let end = start + chunk.compressed_size();
                let mut position = u64::try_from(start).unwrap();
                let mut pages = row_group.get_column_page_reader(column).unwrap();
                while position < u64::try_from(end).unwrap() {
                    let mut input = Cursor::new(&file[usize::try_from(position).unwrap()..]);
                    let (header, consumed) = PageHeader::read(&mut input).unwrap();
                    assert_eq!(consumed, input.position());
                    let page = pages.get_next_page().unwrap().unwrap();
                    assert_eq!(page.encoding() as i32, value_encoding(&header).0);
                    assert_eq!(
                        i32::try_from(page.num_values()).unwrap(),
                        value_count(&header)
                    );
                    let expected_type = match page.page_type() {
                        parquet::basic::PageType::DATA_PAGE => PageType::DATA_PAGE,
                        parquet::basic::PageType::DATA_PAGE_V2 => PageType::DATA_PAGE_V2,
                        parquet::basic::PageType::DICTIONARY_PAGE => PageType::DICTIONARY_PAGE,
                        parquet::basic::PageType::INDEX_PAGE => panic!("unexpected index page"),
                    };
                    assert_eq!(header.type_, expected_type);
                    assert_eq!(
                        i32::try_from(page.buffer().len()).unwrap(),
                        header.uncompressed_page_size
                    );
                    if let parquet::column::page::Page::DataPageV2 { is_compressed, .. } = page {
                        let v2 = header.data_page_v2.unwrap();
                        assert_eq!(v2.is_compressed.unwrap_or(true), is_compressed);
                    }
                    if let Some(v1) = header.data_page {
                        assert_eq!(v1.definition_level_encoding, PageEncoding::RLE);
                        assert_eq!(v1.repetition_level_encoding, PageEncoding::RLE);
                    }
                    position += consumed + u64::try_from(header.compressed_page_size).unwrap();
                    pages_seen += 1;
                }
                assert_eq!(position, u64::try_from(end).unwrap());
                assert!(pages.get_next_page().unwrap().is_none());
            }
        }
        assert!(pages_seen >= 2, "{version:?} wrote {pages_seen} pages");
    }
}

fn value_encoding(header: &PageHeader) -> PageEncoding {
    header
        .data_page
        .map(|page| page.encoding)
        .or(header.dictionary_page.map(|page| page.encoding))
        .or(header.data_page_v2.map(|page| page.encoding))
        .unwrap()
}

fn value_count(header: &PageHeader) -> i32 {
    header
        .data_page
        .map(|page| page.num_values)
        .or(header.dictionary_page.map(|page| page.num_values))
        .or(header.data_page_v2.map(|page| page.num_values))
        .unwrap()
}

/// Encode a page header whose data page carries `statistics`.
fn header_with(statistics: impl FnOnce(&mut TCompactOutputProtocol<&mut Vec<u8>>)) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut out = TCompactOutputProtocol::new(&mut bytes);
        out.write_struct_begin(&TStructIdentifier::new("PageHeader"))
            .unwrap();
        for (id, value) in [(1, 0), (2, 100), (3, 100)] {
            out.write_field_begin(&TFieldIdentifier::new("f", TType::I32, id))
                .unwrap();
            out.write_i32(value).unwrap();
            out.write_field_end().unwrap();
        }
        out.write_field_begin(&TFieldIdentifier::new("data", TType::Struct, 5))
            .unwrap();
        out.write_struct_begin(&TStructIdentifier::new("DataPageHeader"))
            .unwrap();
        for (id, value) in [(1, 10), (2, 0), (3, 3), (4, 3)] {
            out.write_field_begin(&TFieldIdentifier::new("f", TType::I32, id))
                .unwrap();
            out.write_i32(value).unwrap();
            out.write_field_end().unwrap();
        }
        statistics(&mut out);
        out.write_field_stop().unwrap();
        out.write_struct_end().unwrap();
        out.write_field_end().unwrap();
        out.write_field_stop().unwrap();
        out.write_struct_end().unwrap();
        out.flush().unwrap();
    }
    bytes
}

#[test]
fn skips_statistics_and_unknown_fields() {
    let bytes = header_with(|out| {
        out.write_field_begin(&TFieldIdentifier::new("statistics", TType::Struct, 5))
            .unwrap();
        out.write_struct_begin(&TStructIdentifier::new("Statistics"))
            .unwrap();
        out.write_field_begin(&TFieldIdentifier::new("max", TType::String, 1))
            .unwrap();
        out.write_bytes(&[7; 300]).unwrap();
        out.write_field_end().unwrap();
        out.write_field_begin(&TFieldIdentifier::new("list", TType::List, 9))
            .unwrap();
        out.write_list_begin(&thrift::protocol::TListIdentifier::new(TType::Bool, 20))
            .unwrap();
        for index in 0..20 {
            out.write_bool(index % 2 == 0).unwrap();
        }
        out.write_list_end().unwrap();
        out.write_field_end().unwrap();
        out.write_field_stop().unwrap();
        out.write_struct_end().unwrap();
        out.write_field_end().unwrap();
    });
    let (header, consumed) = PageHeader::read(&mut bytes.as_slice()).unwrap();
    assert_eq!(usize::try_from(consumed).unwrap(), bytes.len());
    let data = header.data_page.unwrap();
    assert_eq!((data.num_values, data.encoding), (10, PageEncoding::PLAIN));
    assert_eq!(data.definition_level_encoding, PageEncoding::RLE);
}

/// A statistics value declaring a 4 GiB length under a 64-byte header bound
/// fails as a short read: the declared length is never allocated.
#[test]
fn a_declared_binary_length_beyond_the_bound_is_a_short_read() {
    let mut bytes = header_with(|out| {
        out.write_field_begin(&TFieldIdentifier::new("statistics", TType::Struct, 5))
            .unwrap();
        out.write_struct_begin(&TStructIdentifier::new("Statistics"))
            .unwrap();
        out.write_field_begin(&TFieldIdentifier::new("max", TType::String, 1))
            .unwrap();
    });
    // The encoder closed the data page header and the page header after the
    // dangling `max` field header; replace those two stops with a maximal u32
    // varint length and no body.
    assert_eq!(bytes[bytes.len() - 2..], [0, 0]);
    bytes.truncate(bytes.len() - 2);
    bytes.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]);
    // Bytes beyond the bound, so the bound and not the input ends the read.
    assert!(bytes.len() < 64);
    bytes.resize(4096, 7);
    let mut bounded = bytes.as_slice().take(64);
    match PageHeader::read(&mut bounded) {
        Err(HeaderError::Io(error)) => {
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        }
        other => panic!("expected a short read, got {other:?}"),
    }
}

#[test]
fn refuses_missing_required_fields_mistyped_fields_and_deep_nesting() {
    // No compressed size.
    let mut bytes = Vec::new();
    {
        let mut out = TCompactOutputProtocol::new(&mut bytes);
        out.write_struct_begin(&TStructIdentifier::new("PageHeader"))
            .unwrap();
        for (id, value) in [(1, 0), (2, 100)] {
            out.write_field_begin(&TFieldIdentifier::new("f", TType::I32, id))
                .unwrap();
            out.write_i32(value).unwrap();
            out.write_field_end().unwrap();
        }
        out.write_field_stop().unwrap();
        out.write_struct_end().unwrap();
    }
    assert!(matches!(
        PageHeader::read(&mut bytes.as_slice()),
        Err(HeaderError::Malformed)
    ));

    // The page type as a string.
    let mut bytes = Vec::new();
    {
        let mut out = TCompactOutputProtocol::new(&mut bytes);
        out.write_struct_begin(&TStructIdentifier::new("PageHeader"))
            .unwrap();
        out.write_field_begin(&TFieldIdentifier::new("type", TType::String, 1))
            .unwrap();
        out.write_bytes(b"x").unwrap();
        out.write_field_end().unwrap();
        out.write_field_stop().unwrap();
        out.write_struct_end().unwrap();
    }
    assert!(matches!(
        PageHeader::read(&mut bytes.as_slice()),
        Err(HeaderError::Malformed)
    ));

    // An unknown field nested deeper than the bound.
    let mut bytes = vec![0x9c]; // field 9, struct
    bytes.extend(std::iter::repeat_n(0x1c, 64)); // field +1, struct
    bytes.extend(std::iter::repeat_n(0x00, 66));
    assert!(matches!(
        PageHeader::read(&mut bytes.as_slice()),
        Err(HeaderError::Malformed)
    ));

    // A varint longer than an i32 allows.
    let bytes = [0x15, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
    assert!(matches!(
        PageHeader::read(&mut bytes.as_slice()),
        Err(HeaderError::Malformed)
    ));
}

#[test]
fn encoding_wire_values_match_parquet() {
    for (encoding, wire) in [
        (Encoding::PLAIN, PageEncoding::PLAIN),
        (Encoding::PLAIN_DICTIONARY, PageEncoding::PLAIN_DICTIONARY),
        (Encoding::RLE, PageEncoding::RLE),
        (
            Encoding::DELTA_BINARY_PACKED,
            PageEncoding::DELTA_BINARY_PACKED,
        ),
        (Encoding::DELTA_LENGTH_BYTE_ARRAY, PageEncoding(6)),
        (Encoding::DELTA_BYTE_ARRAY, PageEncoding(7)),
        (Encoding::RLE_DICTIONARY, PageEncoding::RLE_DICTIONARY),
        (Encoding::BYTE_STREAM_SPLIT, PageEncoding::BYTE_STREAM_SPLIT),
    ] {
        assert_eq!(encoding as i32, wire.0, "{encoding:?}");
    }
    // Parquet deprecates the BIT_PACKED variant; its wire value still exists.
    let bit_packed: Encoding = "BIT_PACKED".parse().unwrap();
    assert_eq!(bit_packed as i32, PageEncoding::BIT_PACKED.0);
}

/// A page header with its required fields followed by `tail`, then the stop.
fn required_fields_then(tail: &[u8]) -> Vec<u8> {
    // Field 1 i32 0, field 2 i32 100, field 3 i32 100 (zigzag 200).
    let mut bytes = vec![0x15, 0x00, 0x15, 0xc8, 0x01, 0x15, 0xc8, 0x01];
    bytes.extend_from_slice(tail);
    bytes.push(0x00);
    bytes
}

#[test]
fn skipped_varints_beyond_sixty_four_bits_are_malformed() {
    // An unknown i64 field (id 9, delta 6): nine continuation bytes and a
    // tenth byte carrying only bit 63 is the largest valid encoding.
    let mut largest = vec![0x66];
    largest.extend(std::iter::repeat_n(0xff, 9));
    largest.push(0x01);
    let bytes = required_fields_then(&largest);
    let (header, consumed) = PageHeader::read(&mut bytes.as_slice()).unwrap();
    assert_eq!(usize::try_from(consumed).unwrap(), bytes.len());
    assert_eq!(header.compressed_page_size, 100);

    // A tenth byte with bits beyond 64.
    let mut overflowing = largest.clone();
    *overflowing.last_mut().unwrap() = 0x02;
    let bytes = required_fields_then(&overflowing);
    assert!(matches!(
        PageHeader::read(&mut bytes.as_slice()),
        Err(HeaderError::Malformed)
    ));

    // An eleventh byte.
    let mut long = vec![0x66];
    long.extend(std::iter::repeat_n(0x80, 10));
    long.push(0x00);
    let bytes = required_fields_then(&long);
    assert!(matches!(
        PageHeader::read(&mut bytes.as_slice()),
        Err(HeaderError::Malformed)
    ));
}

#[test]
fn skips_unknown_collections_and_refuses_deep_list_nesting() {
    // Field 9 (delta 6): a map of two i32 -> binary entries; field 10: a set
    // of three bools; field 11: a long-form list of 16 i16s.
    let mut tail = vec![0x6b, 0x02, 0x58, 0x02, 0x01, b'a', 0x04, 0x01, b'b'];
    tail.extend([0x1a, 0x31, 0x01, 0x02, 0x01]);
    tail.extend([0x19, 0xf4, 0x10]);
    tail.extend(std::iter::repeat_n(0x02, 16));
    let bytes = required_fields_then(&tail);
    let (header, consumed) = PageHeader::read(&mut bytes.as_slice()).unwrap();
    assert_eq!(usize::try_from(consumed).unwrap(), bytes.len());
    assert_eq!(header.type_, PageType::DATA_PAGE);

    // Lists of lists nested past the bound, each declaring one element.
    let mut tail = vec![0x69];
    tail.extend(std::iter::repeat_n(0x19, 64));
    let bytes = required_fields_then(&tail);
    assert!(matches!(
        PageHeader::read(&mut bytes.as_slice()),
        Err(HeaderError::Malformed)
    ));

    // A required field declared with another type: compressed size as i64.
    let bytes = [0x15, 0x00, 0x15, 0xc8, 0x01, 0x16, 0xc8, 0x01, 0x00];
    assert!(matches!(
        PageHeader::read(&mut bytes.as_slice()),
        Err(HeaderError::Malformed)
    ));
}

/// The test support's generic Thrift tree re-encodes a genuine footer
/// byte for byte, so hostile controls edit only the field they name.
#[test]
fn raw_thrift_round_trips_a_genuine_footer_byte_for_byte() {
    let file = hostile::string_file(Encoding::PLAIN, WriterVersion::PARQUET_2_0, 32);
    let length = file.len();
    let footer = u32::from_le_bytes(file[length - 8..length - 4].try_into().unwrap()) as usize;
    let bytes = &file[length - 8 - footer..length - 8];
    let (decoded, consumed) = hostile::raw_thrift::Struct::read(bytes);
    assert_eq!(consumed, footer);
    assert_eq!(decoded.encode(), bytes);
}
