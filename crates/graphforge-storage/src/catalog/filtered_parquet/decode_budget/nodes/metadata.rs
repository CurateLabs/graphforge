//! Staged admission of the existing node reader's metadata allocations.

use super::{add, mul, refused};
use arrow::datatypes::{DataType, Field};
use base64::Engine;
use datafusion::error::DataFusionError;
use parquet::file::reader::ChunkReader;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use thrift::protocol::TType;

mod compact;
use compact::{Scan, footer};

#[derive(Default)]
pub(super) struct Admission {
    pub(super) held: usize,
    pub(super) retained: usize,
    active: usize,
}
impl Admission {
    fn reserve(
        &mut self,
        required: usize,
        admit: &mut impl FnMut(usize) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if required > self.held {
            admit(required - self.held)?;
            self.held = required;
        }
        Ok(())
    }

    pub(super) fn file(
        &mut self,
        path: &Path,
        admit: &mut impl FnMut(usize) -> Result<(), DataFusionError>,
    ) -> Result<(usize, u64), DataFusionError> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
        }
        let mut file = options.open(path).map_err(DataFusionError::IoError)?;
        if !file.metadata().map_err(DataFusionError::IoError)?.is_file() {
            return Err(refused());
        }
        let length = file.metadata().map_err(DataFusionError::IoError)?.len();
        crate::catalog::preflight_parquet_handle(&mut file, length)?;
        file.seek(SeekFrom::End(-8))
            .map_err(DataFusionError::IoError)?;
        let mut trailer = [0u8; 8];
        file.read_exact(&mut trailer)
            .map_err(DataFusionError::IoError)?;
        let encoded = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as usize;
        // Footer Bytes and Base64's 3*ceil(n/4) decoded buffer coexist. The
        // bounded compact/FlatBuffer verifier workspace has its own floor.
        let first = add(add(encoded, mul(encoded.div_ceil(4), 3)?)?, 64 * 1024)?;
        self.reserve(add(self.retained, self.active.max(first))?, admit)?;
        let source = crate::catalog::admitted_path_file(file)?;
        let bytes = source
            .get_bytes(length - 8 - encoded as u64, encoded)
            .map_err(crate::catalog::parquet_err)?;
        let facts = footer(&bytes).map_err(|error| match error {
            DataFusionError::ResourcesExhausted(_) => error,
            other => {
                DataFusionError::Context("node metadata footer inspection".into(), Box::new(other))
            }
        })?;
        let (ipc_objects, ipc_strings) = match facts.arrow_schema {
            Some((start, end)) => inspect_arrow_schema(&bytes[start..end])?,
            None => (9, 0),
        };
        let schema = add(
            mul(add(9, ipc_objects)?, schema_node_bytes())?,
            mul(add(facts.strings, ipc_strings)?, 4)?,
        )?;
        let decoded = add(
            add(mul(facts.objects, object_bytes())?, mul(facts.slots, 4)?)?,
            add(mul(facts.strings, 8)?, mul(ipc_strings, 4)?)?,
        )?;
        // Pinned local paths may hold an outer projected schema while opening
        // the next reader/retry. Three complete parse/conversion workspaces
        // cover footer decoding, schema conversion and those local overlaps;
        // schema-only owners of earlier files remain separate in retained.
        let active = add(first, mul(decoded, 3)?)?;
        self.retained = add(self.retained, schema)?;
        self.active = self.active.max(active);
        self.reserve(add(self.retained, self.active)?, admit)?;
        Ok((active, length - 8 - encoded as u64))
    }

    pub(super) fn indexes(
        &mut self,
        path: &Path,
        metadata: &parquet::file::metadata::ParquetMetaData,
        file_active: usize,
        footer_start: u64,
        admit: &mut impl FnMut(usize) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let mut low = u64::MAX;
        let mut high = 0u64;
        let range = |offset: Option<i64>,
                     length: Option<i32>|
         -> Result<Option<std::ops::Range<u64>>, DataFusionError> {
            match (offset, length) {
                (Some(offset), Some(length)) => {
                    let start = u64::try_from(offset).map_err(|_| refused())?;
                    let length = u64::try_from(length).map_err(|_| refused())?;
                    let end = start.checked_add(length).ok_or_else(refused)?;
                    if start < 4 || end > footer_start {
                        return Err(refused());
                    }
                    Ok(Some(start..end))
                }
                (None, None) => Ok(None),
                _ => Err(refused()),
            }
        };
        for group in metadata.row_groups() {
            for column in group.columns() {
                for entry in [
                    range(column.column_index_offset(), column.column_index_length())?,
                    range(column.offset_index_offset(), column.offset_index_length())?,
                ]
                .into_iter()
                .flatten()
                {
                    low = low.min(entry.start);
                    high = high.max(entry.end);
                }
            }
        }
        if high == 0 {
            return Ok(());
        }
        let file = std::fs::File::open(path).map_err(DataFusionError::IoError)?;
        if high > file.metadata().map_err(DataFusionError::IoError)?.len() {
            return Err(refused());
        }
        let transport_span = usize::try_from(high - low).map_err(|_| refused())?;
        // The pinned reader fetches the bounding span, including any gaps.
        let transport = add(mul(transport_span, 2)?, 64 * 1024)?;
        self.reserve(
            add(self.retained, self.active.max(add(file_active, transport)?))?,
            admit,
        )?;
        let source = crate::catalog::admitted_path_file(file)?;
        let bytes = source
            .get_bytes(low, transport_span)
            .map_err(crate::catalog::parquet_err)?;
        let mut decoded = 0usize;
        for group in metadata.row_groups() {
            for column in group.columns() {
                for entry in [
                    range(column.column_index_offset(), column.column_index_length())?,
                    range(column.offset_index_offset(), column.offset_index_length())?,
                ]
                .into_iter()
                .flatten()
                {
                    let begin = usize::try_from(entry.start - low).map_err(|_| refused())?;
                    let end = usize::try_from(entry.end - low).map_err(|_| refused())?;
                    // Every reference is decoded independently, even aliases.
                    let mut scan = Scan::new(&bytes[begin..end]);
                    scan.structure(0, false).map_err(|error| match error {
                        DataFusionError::ResourcesExhausted(_) => error,
                        other => DataFusionError::Context(
                            format!(
                                "node metadata index inspection at {}..{}",
                                entry.start, entry.end
                            ),
                            Box::new(other),
                        ),
                    })?;
                    decoded = add(
                        decoded,
                        add(
                            add(mul(scan.objects, object_bytes())?, mul(scan.slots, 4)?)?,
                            mul(scan.strings, 4)?,
                        )?,
                    )?;
                }
            }
        }
        self.active = self.active.max(add(file_active, add(transport, decoded)?)?);
        self.reserve(add(self.retained, self.active)?, admit)
    }
}

fn schema_node_bytes() -> usize {
    // One occurrence can create Field/DataType/Arc and a metadata HashMap
    // entry. Include all of them rather than an average serialized ratio.
    4 * (std::mem::size_of::<Field>()
        + std::mem::size_of::<DataType>()
        + std::mem::size_of::<(String, String)>()
        + 4 * std::mem::size_of::<usize>())
}

#[allow(
    deprecated,
    reason = "the bound includes the actual intermediate schema element type"
)]
fn object_bytes() -> usize {
    // Each observed wire struct/list slot can own one of these pinned types.
    // The largest pinned type covers any wire object; four copies include
    // growth, temporary conversions and retained parent/child representations.
    4 * struct_slot_bytes()
}

#[allow(deprecated, reason = "includes the actual intermediate schema element")]
fn struct_slot_bytes() -> usize {
    [
        std::mem::size_of::<parquet::file::metadata::ColumnChunkMetaData>(),
        std::mem::size_of::<parquet::file::metadata::RowGroupMetaData>(),
        std::mem::size_of::<parquet::file::metadata::FileMetaData>(),
        std::mem::size_of::<parquet::schema::types::ColumnDescriptor>(),
        std::mem::size_of::<parquet::schema::types::SchemaDescriptor>(),
        std::mem::size_of::<parquet::format::SchemaElement>(),
        schema_node_bytes(),
    ]
    .into_iter()
    .max()
    .unwrap()
}

fn slot_bytes(kind: TType) -> Result<usize, DataFusionError> {
    Ok(match kind {
        TType::Bool | TType::I08 => 1,
        TType::I16 => 2,
        TType::I32 => 4,
        TType::I64 | TType::Double => 8,
        // Borrowed min/max slices coexist with typed <=16-byte values and
        // byte-array offsets. Strings elsewhere use the same larger bound.
        TType::String => {
            std::mem::size_of::<String>()
                + std::mem::size_of::<&[u8]>()
                + 16
                + 2 * std::mem::size_of::<usize>()
        }
        TType::Struct => struct_slot_bytes(),
        TType::List | TType::Set | TType::Map => 3 * std::mem::size_of::<usize>(),
        _ => return Err(refused()),
    })
}

fn inspect_arrow_schema(encoded: &[u8]) -> Result<(usize, usize), DataFusionError> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(crate::catalog::parquet_err)?;
    let slice = if decoded.len() > 8 && decoded[..4] == [255u8; 4] {
        &decoded[8..]
    } else {
        &decoded
    };
    let message = arrow::ipc::root_as_message(slice).map_err(crate::catalog::parquet_err)?;
    let schema = message.header_as_schema().ok_or_else(refused)?;
    let fields = schema.fields().ok_or_else(refused)?;
    if !matches!(fields.len(), 5 | 6) {
        return Err(refused());
    }
    let mut objects = 0;
    let mut strings = 0;
    if let Some(metadata) = schema.custom_metadata() {
        for kv in metadata {
            objects = add(objects, 1)?;
            strings = add(
                strings,
                add(kv.key().map_or(0, str::len), kv.value().map_or(0, str::len))?,
            )?;
        }
    }
    for (index, field) in fields.iter().enumerate() {
        let expected = if fields.len() == 5 && index >= 3 {
            index + 1
        } else {
            index
        };
        inspect_field(
            field,
            crate::TOPOLOGY_NODES_SCHEMA.field(expected),
            &mut objects,
            &mut strings,
        )?;
    }
    Ok((objects, strings))
}

fn inspect_field(
    field: arrow::ipc::Field<'_>,
    expected: &Field,
    objects: &mut usize,
    strings: &mut usize,
) -> Result<(), DataFusionError> {
    use arrow::ipc::Type;
    if field.name() != Some(expected.name().as_str()) || field.dictionary().is_some() {
        return Err(refused());
    }
    *objects = add(*objects, 1)?;
    *strings = add(*strings, field.name().map_or(0, str::len))?;
    if let Some(metadata) = field.custom_metadata() {
        for kv in metadata {
            *objects = add(*objects, 1)?;
            *strings = add(
                *strings,
                add(kv.key().map_or(0, str::len), kv.value().map_or(0, str::len))?,
            )?;
        }
    }
    let children = field.children();
    match (field.type_type(), expected.data_type()) {
        (Type::FixedSizeBinary, DataType::FixedSizeBinary(16))
            if field
                .type_as_fixed_size_binary()
                .is_some_and(|value| value.byteWidth() == 16) => {}
        (Type::Int, DataType::UInt64)
            if field
                .type_as_int()
                .is_some_and(|value| value.bitWidth() == 64 && !value.is_signed()) => {}
        (Type::Int, DataType::UInt32)
            if field
                .type_as_int()
                .is_some_and(|value| value.bitWidth() == 32 && !value.is_signed()) => {}
        (Type::Timestamp, DataType::Timestamp(_, _))
            if field
                .type_as_timestamp()
                .is_some_and(|value| value.unit() == arrow::ipc::TimeUnit::MICROSECOND) =>
        {
            *strings = add(
                *strings,
                field
                    .type_as_timestamp()
                    .and_then(|value| value.timezone())
                    .map_or(0, str::len),
            )?;
        }
        (Type::List, DataType::List(child)) if field.type_as_list().is_some() => {
            let children = children.ok_or_else(refused)?;
            if children.len() != 1 {
                return Err(refused());
            }
            inspect_field(children.get(0), child, objects, strings)?;
            return Ok(());
        }
        _ => return Err(refused()),
    }
    if children.is_some_and(|children| !children.is_empty()) {
        return Err(refused());
    }
    Ok(())
}
