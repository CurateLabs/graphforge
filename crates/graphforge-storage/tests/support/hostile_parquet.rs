//! Hand-built Parquet files that the encoding profile must refuse.
//!
//! Shared by the storage unit tests and the ingress integration test through
//! `#[path]`; it depends only on `arrow`, `parquet`, `bytes` and `thrift`.
#![allow(dead_code, reason = "shared test support")]

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::{
    ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter, RowGroupMetaData,
};
use parquet::file::properties::{WriterProperties, WriterVersion};

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
pub fn patch_header(file: &mut [u8], offset: u64, edit: impl FnOnce(&mut raw_thrift::Struct)) {
    let offset = usize::try_from(offset).unwrap();
    let (mut header, length) = raw_thrift::Struct::read(&file[offset..]);
    edit(&mut header);
    let encoded = header.encode();
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
    let (_, header_length) = raw_thrift::Struct::read(&file[offset..]);
    let mut at = offset + header_length;
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

/// Field ids of the Parquet Thrift structs these controls edit.
pub mod field {
    /// `PageHeader`.
    pub const PAGE_TYPE: i16 = 1;
    pub const UNCOMPRESSED_PAGE_SIZE: i16 = 2;
    pub const COMPRESSED_PAGE_SIZE: i16 = 3;
    pub const DATA_PAGE_HEADER: i16 = 5;
    pub const DICTIONARY_PAGE_HEADER: i16 = 7;
    pub const DATA_PAGE_HEADER_V2: i16 = 8;
    /// `DataPageHeader`, `DictionaryPageHeader` and `DataPageHeaderV2`.
    pub const NUM_VALUES: i16 = 1;
    /// `DataPageHeader` and `DictionaryPageHeader`.
    pub const ENCODING: i16 = 2;
    /// `DataPageHeader`.
    pub const DEFINITION_LEVEL_ENCODING: i16 = 3;
    pub const REPETITION_LEVEL_ENCODING: i16 = 4;
    /// `DataPageHeaderV2`.
    pub const V2_ENCODING: i16 = 4;
    pub const V2_IS_COMPRESSED: i16 = 7;
    /// `FileMetaData`.
    pub const SCHEMA: i16 = 2;
    pub const KEY_VALUE_METADATA: i16 = 5;
    /// `SchemaElement`.
    pub const NUM_CHILDREN: i16 = 5;
    /// `PageType::DATA_PAGE_V2`.
    pub const DATA_PAGE_V2: i32 = 3;
}

/// A generic Thrift compact-protocol struct, decoded and re-encoded field by
/// field so a control can edit one field of a genuine Parquet header or footer
/// and keep every other byte, including fields it does not model.
pub mod raw_thrift {
    use std::io::Cursor;

    use thrift::protocol::{
        TCompactInputProtocol, TCompactOutputProtocol, TFieldIdentifier, TInputProtocol,
        TListIdentifier, TMapIdentifier, TOutputProtocol, TSetIdentifier, TStructIdentifier, TType,
    };

    #[derive(Debug, Clone, PartialEq)]
    pub enum Value {
        Bool(bool),
        I8(i8),
        I16(i16),
        I32(i32),
        I64(i64),
        Double(f64),
        Binary(Vec<u8>),
        List(TType, Vec<Value>),
        Set(TType, Vec<Value>),
        Map(TType, TType, Vec<(Value, Value)>),
        Struct(Struct),
    }

    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct Struct {
        pub fields: Vec<(i16, Value)>,
    }

    impl Struct {
        /// Decode the struct at the start of `bytes`, with its encoded length.
        pub fn read(bytes: &[u8]) -> (Self, usize) {
            let mut cursor = Cursor::new(bytes);
            let value = {
                let mut input = TCompactInputProtocol::new(&mut cursor);
                read_struct(&mut input)
            };
            (value, usize::try_from(cursor.position()).unwrap())
        }

        pub fn encode(&self) -> Vec<u8> {
            let mut encoded = Vec::new();
            {
                let mut output = TCompactOutputProtocol::new(&mut encoded);
                write_struct(&mut output, self);
                output.flush().unwrap();
            }
            encoded
        }

        pub fn get(&self, id: i16) -> Option<&Value> {
            self.fields
                .iter()
                .find(|(field, _)| *field == id)
                .map(|(_, value)| value)
        }

        pub fn get_mut(&mut self, id: i16) -> Option<&mut Value> {
            self.fields
                .iter_mut()
                .find(|(field, _)| *field == id)
                .map(|(_, value)| value)
        }

        /// Replace field `id`, or insert it in id order when absent.
        pub fn set(&mut self, id: i16, value: Value) {
            if let Some(existing) = self.get_mut(id) {
                *existing = value;
                return;
            }
            let at = self
                .fields
                .iter()
                .position(|(field, _)| *field > id)
                .unwrap_or(self.fields.len());
            self.fields.insert(at, (id, value));
        }

        pub fn i32(&self, id: i16) -> i32 {
            match self.get(id) {
                Some(Value::I32(value)) => *value,
                other => panic!("field {id} is not an i32: {other:?}"),
            }
        }

        pub fn set_i32(&mut self, id: i16, value: i32) {
            self.set(id, Value::I32(value));
        }

        pub fn bool(&self, id: i16) -> Option<bool> {
            match self.get(id) {
                Some(Value::Bool(value)) => Some(*value),
                None => None,
                other => panic!("field {id} is not a bool: {other:?}"),
            }
        }

        pub fn child(&self, id: i16) -> &Struct {
            match self.get(id) {
                Some(Value::Struct(value)) => value,
                other => panic!("field {id} is not a struct: {other:?}"),
            }
        }

        pub fn child_mut(&mut self, id: i16) -> &mut Struct {
            match self.get_mut(id) {
                Some(Value::Struct(value)) => value,
                other => panic!("field {id} is not a struct: {other:?}"),
            }
        }

        pub fn list_mut(&mut self, id: i16) -> &mut Vec<Value> {
            match self.get_mut(id) {
                Some(Value::List(_, values)) => values,
                other => panic!("field {id} is not a list: {other:?}"),
            }
        }
    }

    impl Value {
        pub fn as_struct_mut(&mut self) -> &mut Struct {
            match self {
                Value::Struct(value) => value,
                other => panic!("not a struct: {other:?}"),
            }
        }
    }

    fn read_struct(input: &mut dyn TInputProtocol) -> Struct {
        input.read_struct_begin().unwrap();
        let mut fields = Vec::new();
        loop {
            let field = input.read_field_begin().unwrap();
            if field.field_type == TType::Stop {
                break;
            }
            fields.push((field.id.unwrap(), read_value(input, field.field_type)));
            input.read_field_end().unwrap();
        }
        input.read_struct_end().unwrap();
        Struct { fields }
    }

    fn read_value(input: &mut dyn TInputProtocol, kind: TType) -> Value {
        match kind {
            TType::Bool => Value::Bool(input.read_bool().unwrap()),
            TType::I08 => Value::I8(input.read_i8().unwrap()),
            TType::I16 => Value::I16(input.read_i16().unwrap()),
            TType::I32 => Value::I32(input.read_i32().unwrap()),
            TType::I64 => Value::I64(input.read_i64().unwrap()),
            TType::Double => Value::Double(input.read_double().unwrap()),
            TType::String => Value::Binary(input.read_bytes().unwrap()),
            TType::List => {
                let list = input.read_list_begin().unwrap();
                let values = (0..list.size)
                    .map(|_| read_value(input, list.element_type))
                    .collect();
                input.read_list_end().unwrap();
                Value::List(list.element_type, values)
            }
            TType::Set => {
                let set = input.read_set_begin().unwrap();
                let values = (0..set.size)
                    .map(|_| read_value(input, set.element_type))
                    .collect();
                input.read_set_end().unwrap();
                Value::Set(set.element_type, values)
            }
            TType::Map => {
                let map = input.read_map_begin().unwrap();
                let key = map.key_type.unwrap_or(TType::Stop);
                let value = map.value_type.unwrap_or(TType::Stop);
                let entries = (0..map.size)
                    .map(|_| (read_value(input, key), read_value(input, value)))
                    .collect();
                input.read_map_end().unwrap();
                Value::Map(key, value, entries)
            }
            TType::Struct => Value::Struct(read_struct(input)),
            other => panic!("unsupported Thrift type {other:?}"),
        }
    }

    fn kind(value: &Value) -> TType {
        match value {
            Value::Bool(_) => TType::Bool,
            Value::I8(_) => TType::I08,
            Value::I16(_) => TType::I16,
            Value::I32(_) => TType::I32,
            Value::I64(_) => TType::I64,
            Value::Double(_) => TType::Double,
            Value::Binary(_) => TType::String,
            Value::List(..) => TType::List,
            Value::Set(..) => TType::Set,
            Value::Map(..) => TType::Map,
            Value::Struct(_) => TType::Struct,
        }
    }

    fn write_struct(output: &mut dyn TOutputProtocol, value: &Struct) {
        output
            .write_struct_begin(&TStructIdentifier::new("raw"))
            .unwrap();
        for (id, field) in &value.fields {
            output
                .write_field_begin(&TFieldIdentifier::new("raw", kind(field), *id))
                .unwrap();
            write_value(output, field);
            output.write_field_end().unwrap();
        }
        output.write_field_stop().unwrap();
        output.write_struct_end().unwrap();
    }

    fn write_value(output: &mut dyn TOutputProtocol, value: &Value) {
        match value {
            Value::Bool(value) => output.write_bool(*value).unwrap(),
            Value::I8(value) => output.write_i8(*value).unwrap(),
            Value::I16(value) => output.write_i16(*value).unwrap(),
            Value::I32(value) => output.write_i32(*value).unwrap(),
            Value::I64(value) => output.write_i64(*value).unwrap(),
            Value::Double(value) => output.write_double(*value).unwrap(),
            Value::Binary(value) => output.write_bytes(value).unwrap(),
            Value::List(element, values) => {
                output
                    .write_list_begin(&TListIdentifier::new(
                        *element,
                        i32::try_from(values.len()).unwrap(),
                    ))
                    .unwrap();
                for value in values {
                    write_value(output, value);
                }
                output.write_list_end().unwrap();
            }
            Value::Set(element, values) => {
                output
                    .write_set_begin(&TSetIdentifier::new(
                        *element,
                        i32::try_from(values.len()).unwrap(),
                    ))
                    .unwrap();
                for value in values {
                    write_value(output, value);
                }
                output.write_set_end().unwrap();
            }
            Value::Map(key, element, entries) => {
                output
                    .write_map_begin(&TMapIdentifier::new(
                        *key,
                        *element,
                        i32::try_from(entries.len()).unwrap(),
                    ))
                    .unwrap();
                for (key, value) in entries {
                    write_value(output, key);
                    write_value(output, value);
                }
                output.write_map_end().unwrap();
            }
            Value::Struct(value) => write_struct(output, value),
        }
    }
}
