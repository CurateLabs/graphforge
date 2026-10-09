//! Bounded physical Parquet objects for a lossless logical Parquet stream.
//!
//! Ordinary fragments retain their encoding. Oversized fragments are stored as
//! consecutive, independently authenticated Parquet envelopes. Envelope payloads
//! are opaque slices of the original Parquet bytes, including its schema and
//! footer. Authentication remains the caller's existing graph-object admission.

use std::fmt;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use graphforge_core::{GfError, ProjectErrorCode};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::{ChunkReader, Length};

/// Maximum complete physical object, including the envelope's Parquet footer.
pub const MAX_PROPERTY_OBJECT_BYTES: usize = 4 << 20;
/// Fixed payload size; reserved space covers the fixed envelope schema/footer.
pub(crate) const PROPERTY_OBJECT_PAYLOAD_BYTES: usize = MAX_PROPERTY_OBJECT_BYTES - (16 << 10);
/// Encoder reservation: input and Arrow copies, bounded plain-page staging,
/// output growth, and fixed schema/one-column writer state. No codec workspace.
pub(crate) const PROPERTY_OBJECT_ENCODER_MEMORY_BYTES: usize =
    8 * MAX_PROPERTY_OBJECT_BYTES + (1 << 20);

const FORMAT_KEY: &str = "graphforge.property_object";
const FORMAT_VERSION: &str = "1";
const LENGTH_KEY: &str = "graphforge.property_object.length";
const COUNT_KEY: &str = "graphforge.property_object.parts";
const INDEX_KEY: &str = "graphforge.property_object.index";
const PAYLOAD_FIELD: &str = "payload";

fn invalid(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: format!("property object: {}", message.into()),
    }
}

fn parquet_error(error: impl fmt::Display) -> GfError {
    invalid(error.to_string())
}

/// Complete logical stream shape, repeated in every physical envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EnvelopeLayout {
    pub(crate) logical_length: u64,
    pub(crate) part_count: u64,
}

impl EnvelopeLayout {
    fn validate(self) -> Result<(), GfError> {
        if self.logical_length == 0
            || self.part_count
                != self
                    .logical_length
                    .div_ceil(PROPERTY_OBJECT_PAYLOAD_BYTES as u64)
        {
            return Err(invalid("invalid logical length or part count"));
        }
        Ok(())
    }

    fn payload_length(self, index: u64) -> Result<usize, GfError> {
        self.validate()?;
        if index >= self.part_count {
            return Err(invalid("part index exceeds declared part count"));
        }
        let offset = index
            .checked_mul(PROPERTY_OBJECT_PAYLOAD_BYTES as u64)
            .ok_or_else(|| invalid("part offset overflows"))?;
        usize::try_from((self.logical_length - offset).min(PROPERTY_OBJECT_PAYLOAD_BYTES as u64))
            .map_err(|_| invalid("part length overflows"))
    }
}

/// The anchor keeps the canonical fragment name; later parts are sidecars.
pub(crate) fn part_path(anchor: &Path, index: u64) -> PathBuf {
    if index == 0 {
        return anchor.to_owned();
    }
    let mut path = anchor.as_os_str().to_owned();
    path.push(format!(".part-{index:020}.parquet"));
    path.into()
}

/// Recognize a canonical sidecar name without listing any directory.
pub(crate) fn split_part_path(candidate: &Path) -> Option<(PathBuf, u64)> {
    let name = candidate.file_name()?.to_str()?;
    let (anchor_name, suffix) = name.rsplit_once(".part-")?;
    let digits = suffix.strip_suffix(".parquet")?;
    if !anchor_name.ends_with(".parquet")
        || digits.len() != 20
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let index = digits.parse::<u64>().ok()?;
    if index == 0 {
        return None;
    }
    let anchor = candidate.with_file_name(anchor_name);
    (part_path(&anchor, index) == candidate).then_some((anchor, index))
}

#[cfg(test)]
pub(crate) fn part_index(anchor: &Path, candidate: &Path) -> Option<u64> {
    if anchor == candidate {
        Some(0)
    } else {
        let (found_anchor, index) = split_part_path(candidate)?;
        (found_anchor == anchor).then_some(index)
    }
}

/// Validate the caller's authenticated inventory, including the anchor.
pub(crate) fn validate_parts(layout: EnvelopeLayout, indexes: &[u64]) -> Result<(), GfError> {
    layout.validate()?;
    if u64::try_from(indexes.len()).ok() != Some(layout.part_count)
        || indexes
            .iter()
            .enumerate()
            .any(|(position, &index)| u64::try_from(position).ok() != Some(index))
    {
        return Err(invalid(
            "parts are missing, duplicated, extra or out of order",
        ));
    }
    Ok(())
}

fn inspect_schema(schema: &Schema) -> Result<Option<(EnvelopeLayout, u64)>, GfError> {
    let metadata = schema.metadata();
    let Some(version) = metadata.get(FORMAT_KEY) else {
        return Ok(None);
    };
    if version != FORMAT_VERSION {
        return Err(invalid("unsupported envelope version"));
    }
    if metadata.len() != 4
        || schema.fields().len() != 1
        || schema.field(0).name() != PAYLOAD_FIELD
        || schema.field(0).data_type() != &DataType::Binary
        || schema.field(0).is_nullable()
        || !schema.field(0).metadata().is_empty()
    {
        return Err(invalid("invalid envelope schema"));
    }
    let number = |key| -> Result<u64, GfError> {
        let value = metadata
            .get(key)
            .ok_or_else(|| invalid("envelope metadata is incomplete"))?;
        let parsed = value
            .parse::<u64>()
            .map_err(|_| invalid("invalid envelope integer"))?;
        if parsed.to_string() != *value {
            return Err(invalid("noncanonical envelope integer"));
        }
        Ok(parsed)
    };
    let layout = EnvelopeLayout {
        logical_length: number(LENGTH_KEY)?,
        part_count: number(COUNT_KEY)?,
    };
    let index = number(INDEX_KEY)?;
    layout.payload_length(index)?;
    Ok(Some((layout, index)))
}

fn inspect_builder<R: ChunkReader + 'static>(
    builder: &ParquetRecordBatchReaderBuilder<R>,
    physical_length: u64,
) -> Result<Option<(EnvelopeLayout, u64)>, GfError> {
    let Some(info) = inspect_schema(builder.schema().as_ref())? else {
        return Ok(None);
    };
    let metadata = builder.metadata();
    if physical_length > MAX_PROPERTY_OBJECT_BYTES as u64
        || metadata.file_metadata().num_rows() != 1
        || metadata.num_row_groups() != 1
        || metadata.row_group(0).num_rows() != 1
        || metadata.row_group(0).num_columns() != 1
    {
        return Err(invalid("envelope exceeds physical or row bounds"));
    }
    let column = metadata.row_group(0).column(0);
    if column.compression() != Compression::UNCOMPRESSED
        || u64::try_from(column.uncompressed_size())
            .map_or(true, |bytes| bytes > MAX_PROPERTY_OBJECT_BYTES as u64)
        || column.dictionary_page_offset().is_some()
    {
        return Err(invalid("invalid envelope payload encoding"));
    }
    Ok(Some(info))
}

/// Detect envelopes through their footer alone. Ordinary Parquet returns None.
pub(crate) fn inspect_envelope<R: ChunkReader + 'static>(
    source: R,
) -> Result<Option<(EnvelopeLayout, u64)>, GfError> {
    let physical_length = source.len();
    // Format is unknown until the footer has decoded. Preserve the ordinary
    // property-Parquet corruption contract for plain or malformed fragments.
    let builder = ParquetRecordBatchReaderBuilder::try_new(source).map_err(super::parquet_error)?;
    inspect_builder(&builder, physical_length)
}

fn encode_part(layout: EnvelopeLayout, index: u64, payload: &[u8]) -> Result<Bytes, GfError> {
    if payload.len() != layout.payload_length(index)? {
        return Err(invalid("payload length disagrees with envelope layout"));
    }
    let schema = Arc::new(
        Schema::new(vec![Field::new(PAYLOAD_FIELD, DataType::Binary, false)]).with_metadata(
            [
                (FORMAT_KEY.to_owned(), FORMAT_VERSION.to_owned()),
                (LENGTH_KEY.to_owned(), layout.logical_length.to_string()),
                (COUNT_KEY.to_owned(), layout.part_count.to_string()),
                (INDEX_KEY.to_owned(), index.to_string()),
            ]
            .into_iter()
            .collect(),
        ),
    );
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BinaryArray::from_vec(vec![payload])) as ArrayRef],
    )
    .map_err(parquet_error)?;
    let properties = WriterProperties::builder()
        .set_created_by("graphforge property object/1".to_owned())
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_offset_index_disabled(true)
        .set_max_row_group_row_count(Some(1))
        .build();
    let mut output = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut output, schema, Some(properties)).map_err(parquet_error)?;
        writer.write(&batch).map_err(parquet_error)?;
        writer.close().map_err(parquet_error)?;
    }
    if output.len() > MAX_PROPERTY_OBJECT_BYTES {
        return Err(invalid("encoded envelope exceeds fixed object bound"));
    }
    Ok(output.into())
}

/// Stream logical bytes into consecutive envelopes, holding one part at a time.
/// Emitted objects stay private until this function succeeds; the caller must
/// not publish their inventory or generation authority before then.
pub(crate) fn encode_parts<R: Read>(
    input: &mut R,
    logical_length: u64,
    mut emit: impl FnMut(u64, Bytes) -> Result<(), GfError>,
) -> Result<u64, GfError> {
    let layout = EnvelopeLayout {
        logical_length,
        part_count: logical_length.div_ceil(PROPERTY_OBJECT_PAYLOAD_BYTES as u64),
    };
    layout.validate()?;
    let mut buffer = vec![0_u8; PROPERTY_OBJECT_PAYLOAD_BYTES];
    for index in 0..layout.part_count {
        let length = layout.payload_length(index)?;
        input
            .read_exact(&mut buffer[..length])
            .map_err(parquet_error)?;
        emit(index, encode_part(layout, index, &buffer[..length])?)?;
    }
    let mut extra = [0_u8; 1];
    if input.read(&mut extra).map_err(parquet_error)? != 0 {
        return Err(invalid("logical input exceeds declared length"));
    }
    Ok(layout.part_count)
}

fn decode_part(bytes: Bytes, layout: EnvelopeLayout, index: u64) -> Result<Bytes, GfError> {
    if bytes.len() > MAX_PROPERTY_OBJECT_BYTES {
        return Err(invalid("physical part exceeds fixed object bound"));
    }
    let physical_length = bytes.len() as u64;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(parquet_error)?;
    if inspect_builder(&builder, physical_length)? != Some((layout, index)) {
        return Err(invalid("part does not match anchor layout and position"));
    }
    let mut reader = builder.with_batch_size(1).build().map_err(parquet_error)?;
    let batch = reader
        .next()
        .transpose()
        .map_err(parquet_error)?
        .ok_or_else(|| invalid("envelope has no payload row"))?;
    if batch.num_rows() != 1 || reader.next().transpose().map_err(parquet_error)?.is_some() {
        return Err(invalid("envelope does not contain exactly one row"));
    }
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| invalid("envelope payload is not binary"))?;
    if values.is_null(0) || values.value(0).len() != layout.payload_length(index)? {
        return Err(invalid("decoded payload length disagrees with layout"));
    }
    let start = usize::try_from(values.value_offsets()[0])
        .map_err(|_| invalid("negative envelope payload offset"))?;
    // Transfer ownership of Arrow's decoded value buffer without a second
    // payload copy while the physical envelope is still retained by the reader.
    Ok(Bytes::from_owner(DecodedPayload(
        values
            .values()
            .slice_with_length(start, values.value(0).len()),
    )))
}

struct DecodedPayload(arrow::buffer::Buffer);

impl AsRef<[u8]> for DecodedPayload {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

type PartLoader = dyn Fn(u64) -> Result<Bytes, GfError> + Send + Sync;

struct SourceInner {
    layout: EnvelopeLayout,
    loader: Arc<PartLoader>,
    cache: Mutex<Option<(u64, Bytes)>>,
}

/// Random-access logical Parquet, retaining at most one decoded physical part.
/// The loader must authenticate each part before returning its physical bytes.
#[derive(Clone)]
pub(crate) struct SegmentedSource {
    inner: Arc<SourceInner>,
}

impl fmt::Debug for SegmentedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentedSource")
            .field("layout", &self.inner.layout)
            .finish_non_exhaustive()
    }
}

impl SegmentedSource {
    pub(crate) fn new(layout: EnvelopeLayout, loader: Arc<PartLoader>) -> Result<Self, GfError> {
        layout.validate()?;
        Ok(Self {
            inner: Arc::new(SourceInner {
                layout,
                loader,
                cache: Mutex::new(None),
            }),
        })
    }

    pub(crate) fn len(&self) -> u64 {
        self.inner.layout.logical_length
    }

    pub(crate) fn read_at(&self, output: &mut [u8], offset: u64) -> io::Result<usize> {
        if offset >= self.len() || output.is_empty() {
            return Ok(0);
        }
        let wanted = usize::try_from((self.len() - offset).min(output.len() as u64))
            .map_err(io::Error::other)?;
        let mut copied = 0;
        let mut cache = self
            .inner
            .cache
            .lock()
            .map_err(|_| io::Error::other("property object cache lock poisoned"))?;
        while copied < wanted {
            let position = offset + copied as u64;
            let index = position / PROPERTY_OBJECT_PAYLOAD_BYTES as u64;
            let within = usize::try_from(position % PROPERTY_OBJECT_PAYLOAD_BYTES as u64)
                .map_err(io::Error::other)?;
            if cache.as_ref().is_none_or(|(loaded, _)| *loaded != index) {
                // Drop the previous decoded part before admitting the next.
                *cache = None;
                let physical = (self.inner.loader)(index).map_err(io::Error::other)?;
                let decoded =
                    decode_part(physical, self.inner.layout, index).map_err(io::Error::other)?;
                *cache = Some((index, decoded));
            }
            let payload = &cache.as_ref().expect("part loaded above").1;
            let length = (wanted - copied).min(payload.len() - within);
            output[copied..copied + length].copy_from_slice(&payload[within..within + length]);
            copied += length;
        }
        Ok(copied)
    }
}

impl Length for SegmentedSource {
    fn len(&self) -> u64 {
        self.len()
    }
}

pub(crate) struct SegmentedRead {
    source: SegmentedSource,
    position: u64,
}

impl Read for SegmentedRead {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.source.read_at(buffer, self.position)?;
        self.position += count as u64;
        Ok(count)
    }
}

impl ChunkReader for SegmentedSource {
    type T = SegmentedRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        if start > self.len() {
            return Err(ParquetError::EOF(
                "logical property offset exceeds length".into(),
            ));
        }
        Ok(SegmentedRead {
            source: self.clone(),
            position: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        if start
            .checked_add(length as u64)
            .is_none_or(|end| end > self.len())
        {
            return Err(ParquetError::EOF(
                "logical property range exceeds length".into(),
            ));
        }
        let mut output = vec![0_u8; length];
        let count = self
            .read_at(&mut output, start)
            .map_err(|error| ParquetError::General(error.to_string()))?;
        if count != length {
            return Err(ParquetError::EOF("logical property read was short".into()));
        }
        Ok(output.into())
    }
}

#[cfg(test)]
mod tests;
