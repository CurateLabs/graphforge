//! Checked Arrow IPC block reading (#1918).
//!
//! Footer-only planning trusts the lengths it can see. A compressed IPC
//! buffer states an expanded length in an eight-byte prefix, and Arrow's own
//! LZ4 path expands with `read_to_end` before checking it, so a prefix that
//! lies *downward* passes every plan-time bound and then allocates without
//! one. This module therefore plans the whole file from its footer once
//! ([`ipc_plan`]), and a task reads each block through
//! [`CheckedIpcReader`]: the block is read once into an owned buffer,
//! re-checked against the plan, the file and the admitted workspace, every
//! compressed buffer is expanded here in fixed-size chunks that stop at the
//! advertised length, and only then are those same bytes handed to Arrow's
//! `FileDecoder` - so the decoder validates and decodes exactly the bytes
//! that were checked, never a re-read the file could have changed.

use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::ipc::MetadataVersion;
use arrow::ipc::reader::FileDecoder;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;

use super::bulk_source::{bulk_build_memory_budget, schema_owned_bytes};
use super::{limit, storage};

/// Output chunk the bounded frame verification reads through: no expanded
/// buffer is ever built here.
const VERIFY_CHUNK_BYTES: usize = 8 * 1024;

/// The frame decoder one LZ4 verification builds holds, at most, its
/// compressed-block buffer, twice that plus the 64 KiB window in linked mode,
/// and the chunk above (`lz4_flex` 0.13.1 `frame::FrameDecoder
/// read_frame_info`, whose block size a frame header can state up to 4 MiB).
/// Verifications run one at a time, so a task holds one of these.
const LZ4_VERIFY_WORKSPACE_BYTES: u64 =
    ((4 + 2 * 4) * (1 << 20)) as u64 + (VERIFY_CHUNK_BYTES as u64);

/// What decoding one block holds: the owned metadata and body, plus twice the
/// decoded output - Arrow's geometric output growth and the copy that
/// alignment or normalization keeps beside it. The frame decoder's workspace
/// is charged once per task beside these blocks, since verifications run one
/// at a time.
fn block_workspace(block_bytes: u64, output: u64) -> u64 {
    block_bytes.saturating_add(output.saturating_mul(2))
}

/// One message block the footer inventories: its file range. What decoding it
/// holds is re-derived from the owned bytes when the block is read.
#[derive(Clone, Copy)]
pub(super) struct IpcBlock {
    offset: u64,
    metadata_length: u64,
    body_length: u64,
}

impl IpcBlock {
    /// The decoder's block for a buffer holding exactly this message:
    /// metadata at its start, body after it.
    fn decoder_block(self) -> Result<arrow::ipc::Block, GfError> {
        Ok(arrow::ipc::Block::new(
            i64::try_from(self.offset).map_err(storage)?,
            i32::try_from(self.metadata_length).map_err(storage)?,
            i64::try_from(self.body_length).map_err(storage)?,
        ))
    }
}

/// The whole planned Arrow source: footer version, block inventory, schema and
/// the workspace decoding holds. A task needs nothing else from the file, so
/// it never parses the footer again.
pub(super) struct IpcPlan {
    /// The footer's metadata version; every message must match it.
    version: MetadataVersion,
    pub(super) schema: SchemaRef,
    pub(super) columns: usize,
    /// Rows of every record batch, in footer order: the logical batch indices
    /// the build publishes.
    pub(super) rows: Vec<u64>,
    dictionaries: Vec<IpcBlock>,
    batches: Vec<IpcBlock>,
    pub(super) schema_bytes: u64,
    /// What one task decodes: the dictionaries, retained for the whole task,
    /// plus the widest record batch, live one at a time, plus one frame
    /// decoder's workspaces when any block holds an LZ4 frame.
    pub(super) decoding_bytes: u64,
    /// The retained plan structure itself, charged to the build like the
    /// footer it summarizes.
    pub(super) inventory_bytes: u64,
}

/// Footer-only sizing precedes any decode: Arrow's file reader eagerly decodes
/// every dictionary, so constructing one is already a payload allocation.
#[allow(clippy::too_many_lines)]
pub(super) fn ipc_plan(path: &Path) -> Result<IpcPlan, GfError> {
    let budget = bulk_build_memory_budget()?;
    let mut file = File::open(path).map_err(storage)?;
    let length = file.metadata().map_err(storage)?.len();
    if length < 10 {
        return Err(storage("Arrow source is too short for an IPC footer"));
    }
    file.seek(SeekFrom::Start(length - 10)).map_err(storage)?;
    let mut tail = [0; 10];
    file.read_exact(&mut tail).map_err(storage)?;
    let footer_length = u64::try_from(i32::from_le_bytes(tail[..4].try_into().expect("4 bytes")))
        .map_err(|_| storage("Arrow footer length is negative"))?;
    if &tail[4..] != b"ARROW1" || footer_length + 10 > length {
        return Err(storage("Arrow source is not an IPC file"));
    }
    if footer_length > budget {
        return Err(super::limit(
            "Arrow source footer exceeds construction memory budget",
        ));
    }
    let mut footer_bytes = vec![0; usize::try_from(footer_length).map_err(storage)?];
    file.seek(SeekFrom::Start(length - 10 - footer_length))
        .map_err(storage)?;
    file.read_exact(&mut footer_bytes).map_err(storage)?;
    let footer = arrow::ipc::root_as_footer(&footer_bytes).map_err(storage)?;
    let ipc_schema = footer
        .schema()
        .ok_or_else(|| storage("Arrow footer has no schema"))?;
    let mut schema_charge =
        (std::mem::size_of::<arrow::datatypes::Schema>() as u64).saturating_add(256);
    if let Some(fields) = ipc_schema.fields() {
        for field in fields {
            ipc_field_bytes(field, &mut schema_charge, budget, 0)?;
        }
    }
    if let Some(metadata) = ipc_schema.custom_metadata() {
        for entry in metadata {
            schema_charge = schema_charge
                .saturating_add(entry.key().map_or(0, str::len) as u64)
                .saturating_add(entry.value().map_or(0, str::len) as u64)
                .saturating_add(128);
        }
    }
    if footer_length.saturating_add(schema_charge) > budget {
        return Err(super::limit(
            "Arrow source schema exceeds construction memory budget",
        ));
    }
    let schema = arrow::ipc::convert::fb_to_schema(ipc_schema);
    let schema_bytes = schema_owned_bytes(&schema);
    let columns = schema.fields().len();
    let version = footer.version();
    let batch_count = footer.recordBatches().map_or(0, |blocks| blocks.len());
    let dictionary_count = footer.dictionaries().map_or(0, |blocks| blocks.len());
    let rows_bytes = (batch_count as u64).saturating_mul(8);
    let inventory_bytes = ((batch_count + dictionary_count) as u64)
        .saturating_mul(std::mem::size_of::<IpcBlock>() as u64)
        .saturating_add(rows_bytes)
        .saturating_add(256);
    if footer_length
        .saturating_add(schema_bytes)
        .saturating_add(rows_bytes)
        .saturating_add(inventory_bytes)
        > budget
    {
        return Err(super::limit(
            "Arrow source row inventory exceeds construction memory budget",
        ));
    }
    let mut rows = Vec::with_capacity(batch_count);
    let mut dictionaries = Vec::with_capacity(dictionary_count);
    let mut batches = Vec::with_capacity(batch_count);
    let mut dictionary_workspace = 0_u64;
    let mut batch_workspace = 0_u64;
    let mut frames_any = false;
    for (dictionary, blocks) in [
        (true, footer.dictionaries()),
        (false, footer.recordBatches()),
    ] {
        for block in blocks.into_iter().flatten() {
            let offset = u64::try_from(block.offset()).map_err(storage)?;
            let metadata_length = u64::try_from(block.metaDataLength()).map_err(storage)?;
            let blocks_bytes = if dictionary {
                dictionary_workspace.saturating_add(
                    (dictionaries.capacity() as u64)
                        .saturating_mul(std::mem::size_of::<IpcBlock>() as u64),
                )
            } else {
                batch_workspace.saturating_add(
                    (batches.capacity() as u64)
                        .saturating_mul(std::mem::size_of::<IpcBlock>() as u64),
                )
            };
            if footer_length
                .saturating_add(schema_bytes)
                .saturating_add(metadata_length)
                .saturating_add(blocks_bytes)
                .saturating_add(inventory_bytes)
                > budget
                || offset
                    .checked_add(metadata_length)
                    .is_none_or(|end| end > length)
            {
                return Err(super::limit(
                    "Arrow message metadata exceeds construction memory budget",
                ));
            }
            let footer_body_length = u64::try_from(block.bodyLength())
                .map_err(|_| storage("Arrow footer block body length is negative"))?;
            if offset
                .checked_add(metadata_length)
                .and_then(|start| start.checked_add(footer_body_length))
                .is_none_or(|end| end > length)
            {
                return Err(storage("Arrow footer block body extends beyond its source"));
            }
            if metadata_length.saturating_add(footer_body_length) > budget {
                return Err(super::limit(
                    "Arrow footer block allocation exceeds construction memory budget",
                ));
            }
            let mut header = vec![0; usize::try_from(metadata_length).map_err(storage)?];
            file.seek(SeekFrom::Start(offset)).map_err(storage)?;
            file.read_exact(&mut header).map_err(storage)?;
            let skip = if header.starts_with(&[0xff; 4]) { 8 } else { 4 };
            let message = arrow::ipc::root_as_message(header.get(skip..).unwrap_or_default())
                .map_err(storage)?;
            let body_length = u64::try_from(message.bodyLength()).map_err(storage)?;
            if body_length != footer_body_length {
                return Err(storage("Arrow footer and message body lengths disagree"));
            }
            let body_start = offset
                .checked_add(metadata_length)
                .ok_or_else(|| storage("Arrow message offset overflows"))?;
            if body_start
                .checked_add(body_length)
                .is_none_or(|end| end > length)
            {
                return Err(storage("Arrow message body is truncated"));
            }
            let batch = if dictionary {
                message
                    .header_as_dictionary_batch()
                    .and_then(|dictionary| dictionary.data())
            } else {
                message.header_as_record_batch()
            }
            .ok_or_else(|| storage("Arrow footer block has the wrong message kind"))?;
            let (decoded, frames) = ipc_buffer_bytes(&mut file, batch, body_start, body_length)?;
            frames_any = frames_any || frames;
            let workspace = block_workspace(metadata_length.saturating_add(body_length), decoded);
            if dictionary {
                dictionary_workspace = dictionary_workspace.saturating_add(workspace);
                dictionaries.push(IpcBlock {
                    offset,
                    metadata_length,
                    body_length,
                });
            } else {
                batch_workspace = batch_workspace.max(workspace);
                batches.push(IpcBlock {
                    offset,
                    metadata_length,
                    body_length,
                });
                rows.push(u64::try_from(batch.length()).map_err(storage)?);
            }
        }
    }
    let decoding_bytes = dictionary_workspace
        .saturating_add(batch_workspace)
        .saturating_add(u64::from(frames_any) * LZ4_VERIFY_WORKSPACE_BYTES);
    if decoding_bytes > budget {
        return Err(super::limit(
            "Arrow source decoding exceeds construction memory budget",
        ));
    }
    Ok(IpcPlan {
        version,
        schema: Arc::new(schema),
        columns,
        rows,
        dictionaries,
        batches,
        schema_bytes,
        decoding_bytes,
        inventory_bytes,
    })
}

fn ipc_field_bytes(
    field: arrow::ipc::Field<'_>,
    bytes: &mut u64,
    budget: u64,
    depth: usize,
) -> Result<(), GfError> {
    *bytes = bytes
        .saturating_add(field.name().map_or(0, str::len) as u64)
        .saturating_add(256);
    if let Some(timestamp) = field.type_as_timestamp() {
        *bytes = bytes.saturating_add(timestamp.timezone().map_or(0, str::len) as u64);
    }
    if let Some(union) = field.type_as_union() {
        *bytes = bytes.saturating_add(
            union
                .typeIds()
                .map_or(0, |ids| ids.len() as u64)
                .saturating_mul(4),
        );
    }

    if let Some(metadata) = field.custom_metadata() {
        for entry in metadata {
            *bytes = bytes
                .saturating_add(entry.key().map_or(0, str::len) as u64)
                .saturating_add(entry.value().map_or(0, str::len) as u64)
                .saturating_add(128);
        }
    }
    if *bytes > budget || depth > 64 {
        return Err(super::limit(
            "Arrow source schema exceeds construction memory budget",
        ));
    }
    if let Some(children) = field.children() {
        for child in children {
            ipc_field_bytes(child, bytes, budget, depth + 1)?;
        }
    }
    Ok(())
}

/// The decoded bytes a batch message holds - every buffer's expanded length,
/// read from the file where compression states one, and its varlen nodes -
/// and whether any buffer holds an LZ4 frame the reader will have to expand.
fn ipc_buffer_bytes(
    file: &mut File,
    batch: arrow::ipc::RecordBatch<'_>,
    body_start: u64,
    body_length: u64,
) -> Result<(u64, bool), GfError> {
    let lz4 = batch
        .compression()
        .is_some_and(|body| body.codec() == arrow::ipc::CompressionType::LZ4_FRAME);
    let mut decoded = 0_u64;
    let mut frames = false;
    for buffer in batch.buffers().into_iter().flatten() {
        let offset = u64::try_from(buffer.offset()).map_err(storage)?;
        let length = u64::try_from(buffer.length()).map_err(storage)?;
        if offset
            .checked_add(length)
            .is_none_or(|end| end > body_length)
        {
            return Err(storage("Arrow buffer extends beyond its message body"));
        }
        let expanded = if batch.compression().is_some() && length != 0 {
            if length < 8 {
                return Err(storage("Arrow compressed buffer lacks expanded length"));
            }
            file.seek(SeekFrom::Start(body_start + offset))
                .map_err(storage)?;
            let mut prefix = [0; 8];
            file.read_exact(&mut prefix).map_err(storage)?;
            match stated_expansion(&prefix)? {
                StatedExpansion::Raw => length - 8,
                StatedExpansion::Empty => 0,
                StatedExpansion::Frame(expanded) => {
                    frames = frames || lz4;
                    expanded
                }
            }
        } else {
            length
        };
        decoded = decoded.saturating_add(expanded);
    }
    decoded = decoded.saturating_add(
        batch
            .nodes()
            .map_or(0, |nodes| nodes.len() as u64)
            .saturating_mul(256),
    );
    Ok((decoded, frames))
}

/// The message kind a footer block must hold.
#[derive(Clone, Copy)]
enum BlockKind {
    Dictionary,
    Record,
}

/// A task's checked reader over a planned Arrow source.
///
/// Dictionaries decode once, then record batches decode at their footer
/// indices, exactly as the eager file reader's `set_index` did - but every
/// block passes through [`Self::read_block`] first.
pub(super) struct CheckedIpcReader<'a> {
    plan: &'a IpcPlan,
    file: File,
    file_length: u64,
    decoder: FileDecoder,
    /// Workspace the dictionary decodes keep beside the one being decoded.
    retained: u64,
    /// The verification's output chunk, allocated for the first LZ4 frame and
    /// reused for the rest.
    chunk: Option<Vec<u8>>,
}

impl<'a> CheckedIpcReader<'a> {
    pub(super) fn new(plan: &'a IpcPlan, file: File) -> Result<Self, GfError> {
        let file_length = file.metadata().map_err(storage)?.len();
        Ok(Self {
            plan,
            file,
            file_length,
            decoder: FileDecoder::new(Arc::clone(&plan.schema), plan.version),
            retained: 0,
            chunk: None,
        })
    }

    /// Decode the source's dictionaries once, checked.
    pub(super) fn read_dictionaries(&mut self) -> Result<(), GfError> {
        for index in 0..self.plan.dictionaries.len() {
            let block = self.plan.dictionaries[index];
            let (buffer, workspace) = self.read_block(&block, BlockKind::Dictionary)?;
            self.retained = self.retained.saturating_add(workspace);
            self.decoder
                .read_dictionary(&block.decoder_block()?, &buffer)
                .map_err(storage)?;
        }
        Ok(())
    }

    /// Decode the record batch at its footer index, checked.
    pub(super) fn read_record_batch(&mut self, index: usize) -> Result<RecordBatch, GfError> {
        let Some(block) = self.plan.batches.get(index) else {
            return Err(storage("Arrow source ended before its footer count"));
        };
        let (buffer, _) = self.read_block(block, BlockKind::Record)?;
        self.decoder
            .read_record_batch(&block.decoder_block()?, &buffer)
            .map_err(storage)?
            .ok_or_else(|| storage("Arrow footer block has the wrong message kind"))
    }

    /// Refuse a workspace the plan did not admit, before it is allocated.
    fn charge(&self, bytes: u64) -> Result<(), GfError> {
        if bytes > self.plan.decoding_bytes {
            return Err(limit(format!(
                "Arrow source decoding needs {bytes} bytes, above the {} bytes its plan admitted",
                self.plan.decoding_bytes
            )));
        }
        Ok(())
    }

    /// Read one footer block into an owned buffer, check it against the plan,
    /// the file and the admitted workspace, and expand its compressed buffers
    /// in bounded chunks. The returned buffer holds the exact bytes Arrow's
    /// decoder will see.
    fn read_block(&mut self, block: &IpcBlock, kind: BlockKind) -> Result<(Buffer, u64), GfError> {
        let total = block.metadata_length.saturating_add(block.body_length);
        let end = block
            .offset
            .checked_add(total)
            .ok_or_else(|| storage("Arrow message offset overflows"))?;
        if end > self.file_length {
            return Err(storage("Arrow message body is truncated"));
        }
        // The block is charged before it is allocated; its buffers and the
        // frames they expand are charged from their own bytes below.
        self.charge(self.retained.saturating_add(total))?;
        let mut bytes = vec![0_u8; usize::try_from(total).map_err(storage)?];
        self.file
            .seek(SeekFrom::Start(block.offset))
            .map_err(storage)?;
        self.file.read_exact(&mut bytes).map_err(storage)?;
        let skip = if bytes.starts_with(&[0xff_u8; 4]) {
            8
        } else {
            4
        };
        let message =
            arrow::ipc::root_as_message(bytes.get(skip..).unwrap_or_default()).map_err(storage)?;
        let body_length = u64::try_from(message.bodyLength()).map_err(storage)?;
        if body_length != block.body_length {
            return Err(storage("Arrow footer and message body lengths disagree"));
        }
        if self.plan.version != MetadataVersion::V1 && message.version() != self.plan.version {
            return Err(storage(
                "Arrow message metadata version differs from its footer",
            ));
        }
        let batch = match kind {
            BlockKind::Dictionary => message
                .header_as_dictionary_batch()
                .and_then(|dictionary| dictionary.data()),
            BlockKind::Record => message.header_as_record_batch(),
        }
        .ok_or_else(|| storage("Arrow footer block has the wrong message kind"))?;
        let metadata_length = usize::try_from(block.metadata_length).map_err(storage)?;
        let body = bytes
            .get(metadata_length..)
            .ok_or_else(|| storage("Arrow message body is truncated"))?;
        let workspace = self.verify_buffers(batch, body, total)?;
        Ok((Buffer::from_vec(bytes), workspace))
    }

    /// Check a parsed message's buffers against the owned body, charge what
    /// expanding them holds, and expand every LZ4 frame here in fixed-size
    /// chunks that stop at its advertised length. Arrow's decoder sizes the
    /// LZ4 output by the same prefix and expands before checking
    /// (`arrow-ipc` `decompress_lz4`), so the prefix must be made exact on
    /// these bytes before the decoder runs. A ZSTD blob needs no such check:
    /// the bulk decompressor refuses a frame larger than the charged length,
    /// and Arrow re-checks the exact expanded length afterwards.
    fn verify_buffers(
        &mut self,
        batch: arrow::ipc::RecordBatch<'_>,
        body: &[u8],
        block_bytes: u64,
    ) -> Result<u64, GfError> {
        let lz4 = batch
            .compression()
            .is_some_and(|body| body.codec() == arrow::ipc::CompressionType::LZ4_FRAME);
        let mut output = batch
            .nodes()
            .map_or(0, |nodes| nodes.len() as u64)
            .saturating_mul(256);
        let mut frames = false;
        for buffer in batch.buffers().into_iter().flatten() {
            let offset = u64::try_from(buffer.offset()).map_err(storage)?;
            let length = u64::try_from(buffer.length()).map_err(storage)?;
            if offset
                .checked_add(length)
                .is_none_or(|end| end > body.len() as u64)
            {
                return Err(storage("Arrow buffer extends beyond its message body"));
            }
            let expanded = if batch.compression().is_some() && length != 0 {
                let expansion = stated_expansion(buffer_slice(body, offset, length)?)?;
                if matches!(expansion, StatedExpansion::Frame(_)) && lz4 {
                    frames = true;
                }
                match expansion {
                    StatedExpansion::Raw => length - 8,
                    StatedExpansion::Empty => 0,
                    StatedExpansion::Frame(expanded) => expanded,
                }
            } else {
                length
            };
            output = output.saturating_add(expanded);
        }
        let workspace = block_workspace(block_bytes, output)
            .saturating_add(u64::from(frames) * LZ4_VERIFY_WORKSPACE_BYTES);
        // The expanded output, the frame decoder's workspaces and the owned
        // block are all live while Arrow's decoder runs: charge them against
        // the plan's admitted workspace before any of it is allocated.
        self.charge(self.retained.saturating_add(workspace))?;
        for buffer in batch.buffers().into_iter().flatten() {
            let offset = u64::try_from(buffer.offset()).map_err(storage)?;
            let length = u64::try_from(buffer.length()).map_err(storage)?;
            if length == 0 {
                continue;
            }
            let slice = buffer_slice(body, offset, length)?;
            if lz4 && let StatedExpansion::Frame(expanded) = stated_expansion(slice)? {
                self.verify_lz4_frame(&slice[8..], expanded)?;
            }
        }
        Ok(workspace)
    }

    /// Expand one LZ4 frame through a fixed chunk, counting its output and
    /// refusing as soon as it passes the advertised length: the frame never
    /// becomes an expanded buffer here.
    fn verify_lz4_frame(&mut self, compressed: &[u8], expanded: u64) -> Result<(), GfError> {
        use lz4_flex::frame::FrameDecoder;
        let mut decoder = FrameDecoder::new(compressed);
        let mut count = 0_u64;
        loop {
            let read = decoder
                .read(
                    self.chunk
                        .get_or_insert_with(|| vec![0; VERIFY_CHUNK_BYTES])
                        .as_mut_slice(),
                )
                .map_err(|error| storage(format!("Arrow LZ4 frame did not decode: {error}")))?;
            if read == 0 {
                break;
            }
            count = count.saturating_add(read as u64);
            if count > expanded {
                return Err(limit(format!(
                    "Arrow compressed buffer expands past its advertised {expanded} bytes"
                )));
            }
        }
        if count != expanded {
            return Err(limit(format!(
                "Arrow compressed buffer expands to {count} bytes, not its advertised {expanded}"
            )));
        }
        Ok(())
    }
}

/// The owned bytes of a message buffer within the body it was read into.
fn buffer_slice(body: &[u8], offset: u64, length: u64) -> Result<&[u8], GfError> {
    let offset = usize::try_from(offset).map_err(storage)?;
    let length = usize::try_from(length).map_err(storage)?;
    body.get(offset..offset + length)
        .ok_or_else(|| storage("Arrow buffer extends beyond its message body"))
}

/// What a compressed buffer's first eight bytes state: `-1` stores the buffer
/// plain, `0` states it empty, anything else is a frame expanding to that many
/// bytes.
enum StatedExpansion {
    Raw,
    Empty,
    Frame(u64),
}

fn stated_expansion(buffer: &[u8]) -> Result<StatedExpansion, GfError> {
    let prefix = buffer
        .get(..8)
        .and_then(|prefix| <[u8; 8]>::try_from(prefix).ok())
        .ok_or_else(|| storage("Arrow compressed buffer lacks expanded length"))?;
    Ok(match i64::from_le_bytes(prefix) {
        -1 => StatedExpansion::Raw,
        0 => StatedExpansion::Empty,
        expanded => StatedExpansion::Frame(u64::try_from(expanded).map_err(storage)?),
    })
}

#[cfg(test)]
mod ipc_planning_tests {
    use super::ipc_plan;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
    use arrow::record_batch::RecordBatch;
    use std::fs::File;
    use std::sync::Arc;

    fn write_source(path: &std::path::Path, compression: bool) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..1024).map(i64::from).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(compression.then_some(arrow::ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer =
            FileWriter::try_new_with_options(File::create(path).unwrap(), &schema, options)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn ipc_footer_planning_counts_expansion_without_decoding_arrays() {
        let root = tempfile::tempdir().unwrap();
        for compressed in [false, true] {
            let path = root.path().join(format!("{compressed}.arrow"));
            write_source(&path, compressed);
            let plan = ipc_plan(&path).unwrap();
            assert_eq!(plan.rows, vec![1024]);
            assert_eq!(plan.columns, 1);
            assert!(plan.decoding_bytes >= 8192);
        }
    }

    /// A compressed buffer states its own expansion in its first eight bytes. A
    /// file whose footer and messages agree and whose buffer claims eight
    /// gigabytes is refused when planned, before any reader allocates for it.
    #[test]
    fn a_compressed_buffer_that_advertises_a_huge_expansion_is_refused_when_planned() {
        use arrow::array::StringArray;
        let root = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let text = "x".repeat(4_096);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![text.as_str(); 1_000]))],
        )
        .unwrap();
        for (compression, frame) in [
            (arrow::ipc::CompressionType::ZSTD, [0x28, 0xB5, 0x2F, 0xFD]),
            // An LZ4 frame header repeats the content size, so the expansion the
            // IPC buffer states is the copy that precedes the frame's magic.
            (
                arrow::ipc::CompressionType::LZ4_FRAME,
                [0x04, 0x22, 0x4D, 0x18],
            ),
        ] {
            let path = root.path().join(format!("{compression:?}.arrow"));
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(compression))
                .unwrap();
            let mut writer =
                FileWriter::try_new_with_options(File::create(&path).unwrap(), &schema, options)
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();

            super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(1 << 30)));
            let honest = ipc_plan(&path).unwrap();
            assert!(
                honest.decoding_bytes >= 4_096_000,
                "{}",
                honest.decoding_bytes
            );
            // The doubled output and the LZ4 frame decoder's bounded workspaces
            // are charged beside the expansion.
            assert!(
                honest.decoding_bytes < (8 << 20) + super::LZ4_VERIFY_WORKSPACE_BYTES,
                "{}",
                honest.decoding_bytes
            );

            let mut bytes = std::fs::read(&path).unwrap();
            let at = bytes
                .windows(12)
                .position(|window| {
                    window[..8] == 4_096_000_u64.to_le_bytes() && window[8..] == frame
                })
                .expect("the values buffer states its expansion");
            bytes[at..at + 8].copy_from_slice(&(8_u64 << 30).to_le_bytes());
            std::fs::write(&path, bytes).unwrap();
            let refused = ipc_plan(&path);
            super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
            let error = refused
                .err()
                .expect("an 8 GiB expansion exceeds a 1 GiB budget");
            assert!(
                matches!(
                    error,
                    graphforge_core::GfError::Project {
                        code: graphforge_core::ProjectErrorCode::ResourceLimit,
                        ..
                    }
                ),
                "{error}"
            );
            assert!(error.to_string().contains("decoding"), "{error}");
        }
    }

    #[test]
    fn ipc_timezone_schema_expansion_is_admitted_before_conversion() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("timezone.arrow");
        let zone = std::iter::repeat_n('x', 400_000).collect::<String>();
        let schema = Schema::new(vec![Field::new(
            "when",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some(zone.into())),
            true,
        )]);
        let mut writer = FileWriter::try_new(File::create(&path).unwrap(), &schema).unwrap();
        writer.finish().unwrap();
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(600 << 10)));
        let refused = ipc_plan(&path);
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        let error = refused
            .err()
            .expect("footer plus cloned timezone must be charged before conversion");
        assert!(error.to_string().contains("schema"), "{error}");
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(2 << 20)));
        let accepted = ipc_plan(&path);
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        let accepted = accepted.unwrap();
        assert!(accepted.rows.is_empty());
        assert!(accepted.schema_bytes >= 400_000);
    }

    #[test]
    fn ipc_footer_allocation_lengths_are_validated_before_decoder_creation() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("input.arrow");
        write_source(&path, false);
        let original = std::fs::read(&path).unwrap();
        let footer_length = i32::from_le_bytes(
            original[original.len() - 10..original.len() - 6]
                .try_into()
                .unwrap(),
        ) as usize;
        let footer_start = original.len() - 10 - footer_length;
        let footer =
            arrow::ipc::root_as_footer(&original[footer_start..original.len() - 10]).unwrap();
        let slot = footer_start
            + footer._tab.loc()
            + usize::from(
                footer
                    ._tab
                    .vtable()
                    .get(arrow::ipc::Footer::VT_RECORDBATCHES),
            );
        let vector =
            slot + u32::from_le_bytes(original[slot..slot + 4].try_into().unwrap()) as usize;
        // IPC Block is a fixed struct: offset8, metadata length4, padding4,
        // body length8. This mutates the allocation FileReader actually uses.
        for invalid in [-1_i64, i64::MAX] {
            let mut corrupt = original.clone();
            corrupt[vector + 4 + 16..vector + 4 + 24].copy_from_slice(&invalid.to_le_bytes());
            std::fs::write(&path, corrupt).unwrap();
            let error = ipc_plan(&path)
                .err()
                .expect("bad footer must refuse before allocating");
            assert!(error.to_string().contains("footer block body"), "{error}");
        }
    }
}
