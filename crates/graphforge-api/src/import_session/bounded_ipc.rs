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
//!
//! What the LZ4 frames themselves hold is not a constant, but it is also
//! bounded by what the consumers initialize: Arrow's compressed decode
//! (`arrow-ipc` 58.4 `compression.rs`) constructs a fresh `FrameDecoder`
//! per buffer and issues one `read_to_end`, and this module's verifier
//! reads until the first zero - both stop there, so only the FIRST
//! initialized frame is ever decoded and no later frame, malformed
//! suffix, skippable magic or legacy tail is interpreted. A frame header
//! declares its block size (up to 8 MiB for the legacy format) and linked
//! frames keep a window and a doubled output buffer. Planning and the
//! reader both derive each buffer's exact geometry from that first header
//! alone ([`scan_first_frame_geometry`], at most 19 bytes, expanding
//! nothing), admit one codec context for the whole file before anything
//! is constructed, and hold the reader's frame decoder to that context
//! before it is built. Block payloads, checksums and the actual expanded
//! length stay the fixed-chunk verifier's job on the same owned bytes.

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

/// The lz4 frame format's 64 KiB linked-block window (`lz4_flex` 0.13.1
/// `block::WINDOW_SIZE`): a linked block may reference this many bytes
/// behind it, so a linked frame's decoder keeps the window beside its
/// output.
const LZ4_WINDOW_BYTES: u64 = 64 * 1024;

/// The block sizes a standard frame header may declare (`lz4_flex` 0.13.1
/// `frame::header::BlockSize`, codes 4-7).
const LZ4_BLOCK_SIZES: [u64; 4] = [64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024];

/// The block size a legacy frame header implies (`header.rs` legacy magic:
/// 8 MiB, independent blocks, no checksums).
const LZ4_LEGACY_BLOCK_BYTES: u64 = 8 * 1024 * 1024;

const LZ4_FRAME_MAGIC: u32 = 0x184D_2204;
const LZ4_LEGACY_MAGIC: u32 = 0x184C_2102;
/// Skippable frame magics (`header.rs`): the frame decoder refuses these
/// rather than skipping them, so the geometry scan refuses them too.
const LZ4_SKIPPABLE_RANGE: std::ops::RangeInclusive<u32> = 0x184D_2A50..=0x184D_2A5F;

/// What expanding one compressed IPC buffer holds: the frame decoder's
/// reserved `src`/`dst` capacities for the first frame its consumer
/// initializes. A fresh decoder starts with empty vectors, so the pinned
/// `read_frame_info` (`lz4_flex` 0.13.1 `frame/decompress.rs`) reserves
/// exactly these - no resize transient and no retained-previous-frame
/// envelope, and a later frame is never initialized by these consumers.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
struct FrameGeometry {
    src: u64,
    dst: u64,
    peak: u64,
}

impl FrameGeometry {
    /// The geometry of no initialized frame: input the pinned decoder ends
    /// as an empty stream - nothing follows the magic, or only the magic
    /// does - charges nothing.
    const EMPTY: Self = Self {
        src: 0,
        dst: 0,
        peak: 0,
    };

    /// What the pinned decoder reserves when it initializes one frame: the
    /// declared block size of `src`, the block size - or twice it plus the
    /// 64 KiB linked window - of `dst`. The legacy format's four magic
    /// bytes declare 8 MiB independent blocks.
    fn first_frame(max_block: u64, linked: bool) -> Result<Self, GfError> {
        let overflow = || limit("Arrow LZ4 frame geometry overflows");
        let dst = if linked {
            max_block
                .checked_mul(2)
                .and_then(|dst| dst.checked_add(LZ4_WINDOW_BYTES))
                .ok_or_else(overflow)?
        } else {
            max_block
        };
        let peak = max_block.checked_add(dst).ok_or_else(overflow)?;
        Ok(Self {
            src: max_block,
            dst,
            peak,
        })
    }
}

/// A sequential byte source a frame geometry scan walks without ever
/// holding the frame it measures.
trait FrameBytes {
    /// Fill `buffer` from the current position and return how many bytes
    /// were available: fewer than asked for, or none, at the end.
    fn next_bytes(&mut self, buffer: &mut [u8]) -> Result<usize, GfError>;
}

/// The owned compressed buffer a task has already read.
struct SliceFrameBytes<'a> {
    bytes: &'a [u8],
}

impl<'a> SliceFrameBytes<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl FrameBytes for SliceFrameBytes<'_> {
    fn next_bytes(&mut self, buffer: &mut [u8]) -> Result<usize, GfError> {
        let taken = buffer.len().min(self.bytes.len());
        let (head, tail) = self.bytes.split_at(taken);
        buffer[..taken].copy_from_slice(head);
        self.bytes = tail;
        Ok(taken)
    }
}

/// A planned file's compressed buffer, read in small fixed pieces: planning
/// scans the first frame's header by seeking, never holding the body or a
/// per-block vector of it.
struct FileFrameBytes<'a> {
    file: &'a mut File,
    remaining: u64,
}

impl FrameBytes for FileFrameBytes<'_> {
    fn next_bytes(&mut self, buffer: &mut [u8]) -> Result<usize, GfError> {
        let want = (buffer.len() as u64).min(self.remaining);
        if want == 0 {
            return Ok(0);
        }
        let mut taken = 0_usize;
        while taken < want as usize {
            let read = self
                .file
                .read(&mut buffer[taken..want as usize])
                .map_err(storage)?;
            if read == 0 {
                return Err(storage("Arrow compressed buffer is truncated"));
            }
            taken += read;
        }
        self.remaining -= want;
        Ok(want as usize)
    }
}

/// Derive what a compressed buffer's consumer allocates from the first
/// frame's header alone - at most 19 bytes, never a payload byte - the way
/// the pinned decoder's `read_frame_info` (`lz4_flex` 0.13.1
/// `frame/decompress.rs`) and `FrameInfo::read` (`frame/header.rs`) read
/// it: an empty stream and any magic followed by nothing end as zero; a
/// partial magic or header is an error; legacy magic alone initializes 8
/// MiB blocks; a skippable, dictionary, wrong-version, reserved-bit or
/// unsupported-block-size header is refused where the decoder refuses it.
/// The header checksum itself stays the frame decoder's verification on
/// the owned bytes - a header it rejects there charged nothing but is
/// refused by the verifier anyway, so the geometry is an upper bound on
/// what the bytes can allocate. What the header accepts is charged and
/// nothing more: these consumers stop at the first frame's first zero,
/// so no block, suffix or later frame is read, charged or refused here.
fn scan_first_frame_geometry(bytes: &mut dyn FrameBytes) -> Result<FrameGeometry, GfError> {
    let mut magic = [0_u8; 4];
    match bytes.next_bytes(&mut magic)? {
        0 => return Ok(FrameGeometry::EMPTY),
        4 => {}
        _ => return Err(storage("Arrow LZ4 frame magic is truncated")),
    }
    let magic = u32::from_le_bytes(magic);
    if magic == LZ4_LEGACY_MAGIC {
        return FrameGeometry::first_frame(LZ4_LEGACY_BLOCK_BYTES, false);
    }
    let mut base = [0_u8; 3];
    match bytes.next_bytes(&mut base)? {
        0 => return Ok(FrameGeometry::EMPTY),
        3 => {}
        _ => return Err(storage("Arrow LZ4 frame header is truncated")),
    }
    if LZ4_SKIPPABLE_RANGE.contains(&magic) {
        return Err(storage(
            "Arrow compressed buffer holds a skippable LZ4 frame, which the frame decoder refuses",
        ));
    }
    if magic != LZ4_FRAME_MAGIC {
        return Err(storage(
            "Arrow compressed buffer is not an LZ4 frame the decoder accepts",
        ));
    }
    let [flg, bd] = base;
    if flg & 0b1100_0000 != 0b0100_0000 {
        return Err(storage("Arrow LZ4 frame header has an unsupported version"));
    }
    if flg & 0b0000_0010 != 0 || bd & !0b0111_0000 != 0 {
        return Err(storage("Arrow LZ4 frame header sets reserved bits"));
    }
    if flg & 0b0000_0001 != 0 {
        return Err(storage(
            "Arrow LZ4 frame header names a dictionary, which the frame decoder refuses",
        ));
    }
    let block_code = usize::from((bd & 0b0111_0000) >> 4);
    let max_block = block_code
        .checked_sub(4)
        .and_then(|index| LZ4_BLOCK_SIZES.get(index))
        .ok_or_else(|| storage("Arrow LZ4 frame header declares an unsupported block size"))?;
    if flg & 0b0000_1000 != 0 {
        let mut content_size = [0_u8; 8];
        if bytes.next_bytes(&mut content_size)? != 8 {
            return Err(storage("Arrow LZ4 frame header is truncated"));
        }
    }
    FrameGeometry::first_frame(*max_block, flg & 0b0010_0000 == 0)
}

/// What decoding one block holds and retains beside later blocks: the owned
/// metadata and body, plus twice the decoded output - Arrow's geometric
/// output growth and the copy that alignment or normalization keeps beside
/// it. The codec context a compressed block's frames need is a temporary,
/// derived from their exact geometry and charged once per task beside these
/// blocks, since frame decoders never run concurrently.
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
    /// plus the widest record batch, live one at a time, plus one LZ4 codec
    /// context when any block holds a frame.
    pub(super) decoding_bytes: u64,
    /// One LZ4 codec context: the largest first-frame workspace any
    /// compressed buffer's consumer initializes (its header-scanned
    /// geometry) plus the persistent verification chunk. It covers the
    /// verification decoder and Arrow's own decoder of the same bytes,
    /// which never run concurrently. Zero when no block holds a frame.
    codec_context: u64,
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
    let mut frames = IpcFrames::default();
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
            let (decoded, block_frames) =
                ipc_buffer_bytes(&mut file, batch, body_start, body_length)?;
            frames.peak = frames.peak.max(block_frames.peak);
            frames.present |= block_frames.present;
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
    // One codec context for the whole file, the largest individual
    // first-frame charge across its compressed buffers - the verification
    // decoder and Arrow's own decoder of the same bytes never run
    // concurrently, and the persistent verification chunk stays allocated
    // beside whichever runs. Zero when no block holds a frame: ordinary
    // small files admit small budgets.
    let codec_context = if frames.present {
        frames
            .peak
            .checked_add(VERIFY_CHUNK_BYTES as u64)
            .ok_or_else(|| limit("Arrow source frame geometry overflows"))?
    } else {
        0
    };
    let decoding_bytes = dictionary_workspace
        .saturating_add(batch_workspace)
        .saturating_add(codec_context);
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
        codec_context,
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

/// The codec charge one batch message's LZ4 frames add: `present` when any
/// of its buffers runs the frame verifier - even a first frame whose
/// header alone initializes nothing does, since the verifier's chunk is
/// allocated for its read loop - and `peak`, the largest first-frame
/// allocation among them.
#[derive(Clone, Copy, Default)]
struct IpcFrames {
    peak: u64,
    present: bool,
}

/// The decoded bytes a batch message holds - every buffer's expanded length,
/// read from the file where compression states one, and its varlen nodes -
/// plus what its LZ4 frames charge: the largest first-frame workspace,
/// scanned from the file in small fixed reads.
fn ipc_buffer_bytes(
    file: &mut File,
    batch: arrow::ipc::RecordBatch<'_>,
    body_start: u64,
    body_length: u64,
) -> Result<(u64, IpcFrames), GfError> {
    let lz4 = batch
        .compression()
        .is_some_and(|body| body.codec() == arrow::ipc::CompressionType::LZ4_FRAME);
    let mut decoded = 0_u64;
    let mut frames = IpcFrames::default();
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
                    if lz4 {
                        let geometry = scan_first_frame_geometry(&mut FileFrameBytes {
                            file,
                            remaining: length - 8,
                        })?;
                        frames.peak = frames.peak.max(geometry.peak);
                        frames.present = true;
                    }
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
            let (buffer, retained) = self.read_block(&block, BlockKind::Dictionary)?;
            // Only the block's bytes and its decoded output stay beside the
            // later blocks. The codec context is a temporary - each frame
            // decoder is dropped before the next block is read - so it is
            // charged once by the plan and never retained per dictionary.
            self.retained = self.retained.saturating_add(retained);
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
    /// decoder will see, and the returned workspace the retained share of the
    /// block - its bytes and decoded output - never the codec context.
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
    /// the bulk decompressor is given the stated size and refuses a frame
    /// larger than it, and Arrow re-checks the exact expanded length
    /// afterwards. Returns the block's retained share: its bytes plus twice
    /// the decoded output; the codec context is a temporary the plan
    /// admitted once.
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
        // The verification's frame decoder and Arrow's own decoder of the
        // same bytes never run concurrently, so the plan's one codec context
        // covers whichever is live. The chunk persists once allocated, so
        // later raw and uncompressed blocks still hold it.
        let codec_live = frames || self.chunk.is_some();
        let block_workspace = block_workspace(block_bytes, output);
        let workspace = block_workspace.saturating_add(if codec_live {
            self.plan.codec_context
        } else {
            0
        });
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
        Ok(block_workspace)
    }

    /// Expand one LZ4 frame through a fixed chunk, counting its output and
    /// refusing as soon as it passes the advertised length: the frame never
    /// becomes an expanded buffer here. The read loop is the consumer
    /// boundary itself - a fresh decoder whose reads stop at its first
    /// zero - so a buffer's later frames are neither decoded nor charged.
    fn verify_lz4_frame(&mut self, compressed: &[u8], expanded: u64) -> Result<(), GfError> {
        use lz4_flex::frame::FrameDecoder;
        // What the consumer of these bytes allocates, derived from the
        // first frame's header without allocating before anything is
        // constructed: the source bytes can have changed since the plan,
        // so the geometry must still fit the admitted context alongside
        // the chunk it shares.
        let needed = scan_first_frame_geometry(&mut SliceFrameBytes::new(compressed))?
            .peak
            .saturating_add(VERIFY_CHUNK_BYTES as u64);
        if needed > self.plan.codec_context {
            return Err(limit(format!(
                "Arrow LZ4 frame needs {needed} bytes of codec context, above the {} bytes its plan admitted",
                self.plan.codec_context
            )));
        }
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
            // The 4 MiB values buffer makes Arrow's writer declare a 4 MiB
            // independent frame, and the LZ4 plan's codec context is its
            // exact scanned geometry plus the persistent chunk. A ZSTD
            // buffer runs no frame decoder, so it admits none.
            if compression == arrow::ipc::CompressionType::LZ4_FRAME {
                assert_eq!(
                    honest.codec_context,
                    2 * (4 << 20) + super::VERIFY_CHUNK_BYTES as u64
                );
            } else {
                assert_eq!(honest.codec_context, 0);
            }
            // The doubled output and the codec context are charged beside
            // the compressed block.
            let doubled_output = 2 * (4_096_000 + 4_008);
            assert!(
                honest.decoding_bytes < doubled_output + honest.codec_context + (1 << 20),
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

    /// Write a compressed single-batch file holding one non-nullable Int64
    /// column, and return the values it holds: the message declares LZ4
    /// compression and its body ends with the values buffer, so a test can
    /// rebuild that buffer's frame without touching anything else.
    fn write_int_source(path: &std::path::Path, rows: usize) -> Vec<i64> {
        let values: Vec<i64> = (0..rows).map(|row| row as i64 * 3 - 1).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.clone()))],
        )
        .unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer =
            FileWriter::try_new_with_options(File::create(path).unwrap(), &schema, options)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        values
    }

    /// One crafted lz4 frame: explicit block size and mode, no content size,
    /// no checksums - an ordinary writer's frame.
    fn lz4_frame(values: &[u8], block_size: lz4_flex::frame::BlockSize, linked: bool) -> Vec<u8> {
        use std::io::Write as _;
        let mut info = lz4_flex::frame::FrameInfo::default();
        info.block_size = block_size;
        info.block_mode = if linked {
            lz4_flex::frame::BlockMode::Linked
        } else {
            lz4_flex::frame::BlockMode::Independent
        };
        let mut encoder = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
        encoder.write_all(values).unwrap();
        encoder.finish().unwrap()
    }

    /// One crafted lz4 frame with a content size: its header is 15 bytes,
    /// so a cut before the eighth optional byte is a header truncation.
    fn lz4_frame_with_content_size(values: &[u8]) -> Vec<u8> {
        use std::io::Write as _;
        let mut info = lz4_flex::frame::FrameInfo::default();
        info.content_size = Some(values.len() as u64);
        info.block_size = lz4_flex::frame::BlockSize::Max64KB;
        let mut encoder = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
        encoder.write_all(values).unwrap();
        encoder.finish().unwrap()
    }

    /// A hand-built legacy frame: the magic, then uncompressed-flagged
    /// blocks to the end of input, exactly what the pinned decoder accepts.
    fn legacy_frame(values: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&super::LZ4_LEGACY_MAGIC.to_le_bytes());
        frame.extend_from_slice(&(0x8000_0000_u32 | values.len() as u32).to_le_bytes());
        frame.extend_from_slice(values);
        frame
    }

    /// The i64 values as little-endian bytes: exactly what the fixture's
    /// values buffer holds.
    fn values_bytes(values: &[i64]) -> Vec<u8> {
        values.iter().copied().flat_map(i64::to_le_bytes).collect()
    }

    /// Run the exact consumer both production paths share: a fresh frame
    /// decoder and one `read_to_end`, which stops at the first zero. This
    /// is the parity evidence for what a scan's geometry must cover.
    fn decode_frames(frames: &[u8]) -> Vec<u8> {
        use std::io::Read as _;
        let mut output = Vec::new();
        lz4_flex::frame::FrameDecoder::new(frames)
            .read_to_end(&mut output)
            .unwrap();
        output
    }

    /// The consumer errors on these bytes: the pinned decoder's `read_to_end`
    /// returns `Err`, as it must for the fixture to compare semantics.
    fn decode_frames_err(frames: &[u8]) {
        use std::io::Read as _;
        let mut output = Vec::new();
        assert!(
            lz4_flex::frame::FrameDecoder::new(frames)
                .read_to_end(&mut output)
                .is_err()
        );
    }

    fn scan_geometry(frames: &[u8]) -> super::FrameGeometry {
        let mut source = super::SliceFrameBytes::new(frames);
        super::scan_first_frame_geometry(&mut source).unwrap()
    }

    fn scan_geometry_err(frames: &[u8]) -> graphforge_core::GfError {
        let mut source = super::SliceFrameBytes::new(frames);
        super::scan_first_frame_geometry(&mut source)
            .err()
            .expect("the scan must refuse these bytes")
    }

    #[test]
    fn frame_geometry_matches_the_pinned_decoder_budgets() {
        let values = vec![7_u8; 200 * 1024];

        // Linked: the output buffer is twice the block size plus the 64 KiB
        // window the linked decoder keeps beside its output.
        let linked = lz4_frame(&values, lz4_flex::frame::BlockSize::Max64KB, true);
        let geometry = scan_geometry(&linked);
        assert_eq!(geometry.src, 64 * 1024);
        assert_eq!(geometry.dst, 2 * 64 * 1024 + super::LZ4_WINDOW_BYTES);
        assert_eq!(geometry.peak, 3 * 64 * 1024 + super::LZ4_WINDOW_BYTES);
        assert_eq!(decode_frames(&linked), values);

        // Independent: no window credit is added and none is needed.
        let independent = lz4_frame(&values, lz4_flex::frame::BlockSize::Max64KB, false);
        let geometry = scan_geometry(&independent);
        assert_eq!(geometry.src, 64 * 1024);
        assert_eq!(geometry.dst, 64 * 1024);
        assert_eq!(geometry.peak, 2 * 64 * 1024);
        assert_eq!(decode_frames(&independent), values);

        // The largest standard block size, linked: the widest ordinary
        // decoder a standard header can produce.
        let wide = lz4_frame(&values, lz4_flex::frame::BlockSize::Max4MB, true);
        let geometry = scan_geometry(&wide);
        assert_eq!(geometry.src, 4 << 20);
        assert_eq!(geometry.dst, 2 * (4 << 20) + super::LZ4_WINDOW_BYTES);
        assert_eq!(geometry.peak, 3 * (4 << 20) + super::LZ4_WINDOW_BYTES);
        assert_eq!(decode_frames(&wide), values);

        // Legacy: 8 MiB blocks, independent, so eight mebibytes of src
        // beside eight of dst - twice the 4 MiB a standard header can state.
        let legacy = legacy_frame(&values);
        let geometry = scan_geometry(&legacy);
        assert_eq!(geometry.src, 8 << 20);
        assert_eq!(geometry.dst, 8 << 20);
        assert_eq!(geometry.peak, 16 << 20);
        assert_eq!(decode_frames(&legacy), values);
    }

    /// The Arrow consumer constructs a fresh decoder and stops at the first
    /// zero its one `read_to_end` returns, so a second frame after the
    /// first - larger, legacy, skippable, malformed or garbage - is never
    /// initialized. Its geometry must charge the first header only, and its
    /// output must equal the first frame's payload alone.
    #[test]
    fn only_the_first_frame_initializes_so_its_header_charges_the_geometry() {
        let values = vec![9_u8; 80 * 1024];
        let first = lz4_frame(
            &values[..64 * 1024],
            lz4_flex::frame::BlockSize::Max64KB,
            true,
        );
        let second = lz4_frame(
            &values[64 * 1024..],
            lz4_flex::frame::BlockSize::Max4MB,
            false,
        );
        let first_payload = &values[..64 * 1024];
        let alone = scan_geometry(&first);
        assert_eq!(alone.src, 64 * 1024);
        assert_eq!(alone.dst, 2 * 64 * 1024 + super::LZ4_WINDOW_BYTES);
        assert_eq!(alone.peak, 3 * 64 * 1024 + super::LZ4_WINDOW_BYTES);

        // A larger valid frame behind the first: one read_to_end returns
        // only the first payload, and the geometry stays the first
        // frame's charge.
        let mut both = first.clone();
        both.extend_from_slice(&second);
        assert_eq!(scan_geometry(&both), alone);
        assert_eq!(decode_frames(&both), first_payload);

        // The same holds for a legacy suffix, a skippable header, a
        // truncated second magic and plain garbage: none of them may add
        // credit or refuse.
        let mut legacy = first.clone();
        legacy.extend_from_slice(&legacy_frame(&values));
        assert_eq!(scan_geometry(&legacy), alone);
        assert_eq!(decode_frames(&legacy), first_payload);

        let mut skippable = first.clone();
        skippable.extend_from_slice(&0x184D_2A50_u32.to_le_bytes());
        skippable.extend_from_slice(&[0, 0, 0, 0]);
        assert_eq!(scan_geometry(&skippable), alone);
        assert_eq!(decode_frames(&skippable), first_payload);

        let mut truncated_magic = first.clone();
        truncated_magic.extend_from_slice(&[0x04, 0x22, 0x4D]);
        assert_eq!(scan_geometry(&truncated_magic), alone);
        assert_eq!(decode_frames(&truncated_magic), first_payload);

        let mut garbage = first.clone();
        garbage.extend_from_slice(&[0, 0, 0]);
        assert_eq!(scan_geometry(&garbage), alone);
        assert_eq!(decode_frames(&garbage), first_payload);
    }

    /// A legacy FIRST frame declares its 8 MiB blocks with its four magic
    /// bytes alone - both allocations are charged before any block is read
    /// - and a first frame whose block returns zero output stops the
    /// consumer there: nothing behind it is read, and the header's
    /// geometry is all the scan charges.
    #[test]
    fn a_legacy_first_frame_and_a_zero_output_block_charge_their_headers_only() {
        // The legacy magic alone: the pinned decoder reserves 8 MiB of src
        // beside 8 MiB of dst, then ends the stream at the block info's
        // caught EOF - zero output, no error.
        assert_eq!(
            scan_geometry(&super::LZ4_LEGACY_MAGIC.to_le_bytes()),
            super::FrameGeometry {
                src: 8 << 20,
                dst: 8 << 20,
                peak: 16 << 20,
            }
        );
        assert_eq!(decode_frames(&super::LZ4_LEGACY_MAGIC.to_le_bytes()), b"");

        // A standard header followed by an Uncompressed(0) block word: the
        // consumer returns zero and stops, leaving the suffix unread. The
        // header bytes are a real frame's, so the header checksum holds.
        let real = lz4_frame(b"payload", lz4_flex::frame::BlockSize::Max64KB, false);
        let mut stops = real[..7].to_vec();
        stops.extend_from_slice(&0x8000_0000_u32.to_le_bytes());
        stops.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            scan_geometry(&stops),
            super::FrameGeometry {
                src: 64 * 1024,
                dst: 64 * 1024,
                peak: 2 * 64 * 1024,
            }
        );
        assert_eq!(decode_frames(&stops), b"");
    }

    /// Headers the pinned decoder truly refuses are refused by the scan on
    /// the same bytes, and the corner cases it ends as an empty stream
    /// charge nothing: the scan's accept and refuse sets are the first
    /// header's, not a whole-buffer grammar. A cut payload is not a header
    /// refusal any more - the header charged its geometry, and the payload's
    /// validity stays the verifier's job on the owned bytes.
    #[test]
    fn first_header_refusals_match_decoder_rejection() {
        // A skippable frame with its full header is an error for the
        // pinned decoder, not a skip.
        let mut skippable = 0x184D_2A50_u32.to_le_bytes().to_vec();
        skippable.extend_from_slice(&[0, 0, 0, 0]);
        assert!(
            scan_geometry_err(&skippable)
                .to_string()
                .contains("skippable")
        );
        decode_frames_err(&skippable);

        // A dictionary id, reserved bits, a wrong version and block codes
        // 0-3 are refused by both.
        let mut dictionary = super::LZ4_FRAME_MAGIC.to_le_bytes().to_vec();
        dictionary.extend_from_slice(&[0b0100_0001, 4 << 4, 0xAA, 0xBB, 0xCC, 0xDD, 0]);
        assert!(
            scan_geometry_err(&dictionary)
                .to_string()
                .contains("dictionary")
        );
        decode_frames_err(&dictionary);

        let mut reserved = super::LZ4_FRAME_MAGIC.to_le_bytes().to_vec();
        reserved.extend_from_slice(&[0b0100_0010, 4 << 4, 0]);
        assert!(
            scan_geometry_err(&reserved)
                .to_string()
                .contains("reserved")
        );
        decode_frames_err(&reserved);
        let mut version = super::LZ4_FRAME_MAGIC.to_le_bytes().to_vec();
        version.extend_from_slice(&[0b1000_0000, 4 << 4, 0]);
        assert!(scan_geometry_err(&version).to_string().contains("version"));
        decode_frames_err(&version);
        for code in 0_u8..4 {
            let mut header = super::LZ4_FRAME_MAGIC.to_le_bytes().to_vec();
            header.extend_from_slice(&[0b0100_0000, code << 4, 0]);
            assert!(
                scan_geometry_err(&header)
                    .to_string()
                    .contains("block size")
            );
            decode_frames_err(&header);
        }

        // A wrong magic and every truncation of the first frame's head are
        // refused by both.
        let mut wrong = [1, 2, 3, 4].to_vec();
        wrong.extend_from_slice(&[5, 6, 7]);
        assert!(scan_geometry_err(&wrong).to_string().contains("not an LZ4"));
        decode_frames_err(&wrong);

        assert!(
            scan_geometry_err(&[0x04, 0x22])
                .to_string()
                .contains("truncated")
        );
        decode_frames_err(&[0x04, 0x22]);
        let mut partial_header = super::LZ4_FRAME_MAGIC.to_le_bytes().to_vec();
        partial_header.extend_from_slice(&[0b0100_0000]);
        assert!(
            scan_geometry_err(&partial_header)
                .to_string()
                .contains("truncated")
        );
        decode_frames_err(&partial_header);
        // A content-size header cut before its eight optional bytes: both
        // refuse.
        let sized = lz4_frame_with_content_size(b"payload");
        assert_eq!(sized[4] & 0b0000_1000, 0b0000_1000, "content size flagged");
        let mut content = sized;
        content.truncate(10);
        assert!(
            scan_geometry_err(&content)
                .to_string()
                .contains("truncated")
        );
        decode_frames_err(&content);

        // What the decoder ends as an empty stream charges nothing: empty
        // input, and any non-legacy magic followed by nothing.
        assert_eq!(scan_geometry(&[]), super::FrameGeometry::EMPTY);
        let standard_magic = super::LZ4_FRAME_MAGIC.to_le_bytes().to_vec();
        assert_eq!(scan_geometry(&standard_magic), super::FrameGeometry::EMPTY);
        assert_eq!(decode_frames(&standard_magic), b"");
        let skippable_magic = 0x184D_2A50_u32.to_le_bytes().to_vec();
        assert_eq!(scan_geometry(&skippable_magic), super::FrameGeometry::EMPTY);
        assert_eq!(decode_frames(&skippable_magic), b"");
        let wrong_magic = [9_u8, 9, 9, 9].to_vec();
        assert_eq!(scan_geometry(&wrong_magic), super::FrameGeometry::EMPTY);
        assert_eq!(decode_frames(&wrong_magic), b"");

        // A payload cut short - or a stream cut at a block boundary - no
        // longer refuses the scan: only the header's geometry is charged,
        // and what the bytes decode to stays the verifier's job.
        let good = lz4_frame(b"payload", lz4_flex::frame::BlockSize::Max64KB, false);
        let mut cut_payload = good.clone();
        cut_payload.truncate(good.len() - 10);
        assert_eq!(
            scan_geometry(&cut_payload),
            super::FrameGeometry {
                src: 64 * 1024,
                dst: 64 * 1024,
                peak: 2 * 64 * 1024,
            }
        );
        // The pinned decoder errors on a block cut mid-payload, and the
        // verifier refuses the buffer for it.
        decode_frames_err(&cut_payload);
        let mut boundary = good.clone();
        boundary.truncate(boundary.len() - 4);
        assert_eq!(scan_geometry(&boundary).src, 64 * 1024);
        assert_eq!(decode_frames(&boundary), b"payload");
    }

    /// Rebuild the fixture's single record-batch values buffer as
    /// `[8-byte expansion prefix][frames..]`, patching the values slot's
    /// length, the message's body length and the footer block around the
    /// new body. An Int64 array carries two buffer slots - the validity
    /// bitmap and the values - and every byte before the values buffer
    /// stays verbatim; the frames must expand to exactly the values the
    /// file held, so the decoded batch is unchanged.
    fn values_buffer_as_frames(path: &std::path::Path, frames: &[Vec<u8>], expansion: u64) {
        use arrow::ipc::Message as IpcMessage;
        use arrow::ipc::RecordBatch as IpcRecordBatch;
        let original = std::fs::read(path).unwrap();
        let plan = ipc_plan(path).unwrap();
        assert_eq!(plan.batches.len(), 1, "single-batch fixture");
        assert_eq!(plan.dictionaries.len(), 0, "dictionary-free fixture");
        let block = plan.batches[0];
        let block_at = usize::try_from(block.offset).unwrap();
        let body_start = block_at + usize::try_from(block.metadata_length).unwrap();

        let message_skip = if original[block_at..].starts_with(&[0xff_u8; 4]) {
            8
        } else {
            4
        };
        let message =
            arrow::ipc::root_as_message(&original[block_at + message_skip..body_start]).unwrap();
        let batch = message.header_as_record_batch().unwrap();
        // The values buffer: a struct vector of (offset i64, length i32)
        // slots, validity first and the nonempty values last.
        let buffers_slot = block_at
            + message_skip
            + batch._tab.loc()
            + usize::from(batch._tab.vtable().get(IpcRecordBatch::VT_BUFFERS));
        let buffers = buffers_slot
            + u32::from_le_bytes(original[buffers_slot..buffers_slot + 4].try_into().unwrap())
                as usize;
        let slot_count =
            u32::from_le_bytes(original[buffers..buffers + 4].try_into().unwrap()) as usize;
        assert_eq!(slot_count, 2, "an Int64 array carries validity and values");
        let values_at = buffers + (slot_count - 1) * 16;
        let values_offset =
            u64::from_le_bytes(original[values_at..values_at + 8].try_into().unwrap());
        let values_length = u64::try_from(i32::from_le_bytes(
            original[values_at + 8..values_at + 12].try_into().unwrap(),
        ))
        .unwrap();
        assert!(values_length > 8, "the values buffer holds a frame");
        assert!(
            values_offset + values_length <= block.body_length,
            "the values buffer ends its body"
        );
        // The message's bodyLength field.
        let body_length_at = block_at
            + message_skip
            + message._tab.loc()
            + usize::from(message._tab.vtable().get(IpcMessage::VT_BODYLENGTH));

        let new_body = values_offset + 8 + frames.iter().map(Vec::len).sum::<u64>();
        let new_values_length = 8 + frames.iter().map(Vec::len).sum::<u64>();
        let footer_length = i32::from_le_bytes(
            original[original.len() - 10..original.len() - 6]
                .try_into()
                .unwrap(),
        ) as usize;
        let footer_start = original.len() - 10 - footer_length;
        let footer = original[footer_start..original.len() - 10].to_vec();
        let footer = arrow::ipc::root_as_footer(&footer).unwrap();
        let footer_block_slot = footer_start
            + footer._tab.loc()
            + usize::from(
                footer
                    ._tab
                    .vtable()
                    .get(arrow::ipc::Footer::VT_RECORDBATCHES),
            );
        let footer_block = footer_block_slot
            + u32::from_le_bytes(
                original[footer_block_slot..footer_block_slot + 4].try_into().unwrap(),
            )
            as usize
            // Block struct: offset8, metaDataLength4, padding4, bodyLength8.
            + 4
            + 16;

        let mut rebuilt = Vec::with_capacity(body_start + new_body as usize + footer_length + 10);
        rebuilt.extend_from_slice(&original[..body_start]);
        rebuilt.extend_from_slice(&original[body_start..body_start + values_offset as usize]);
        rebuilt.extend_from_slice(&expansion.to_le_bytes());
        for frame in frames {
            rebuilt.extend_from_slice(frame);
        }
        rebuilt[body_length_at..body_length_at + 8]
            .copy_from_slice(&(new_body as i64).to_le_bytes());
        rebuilt[values_at + 8..values_at + 12]
            .copy_from_slice(&(new_values_length as i32).to_le_bytes());
        rebuilt.extend_from_slice(&original[footer_start..footer_block]);
        rebuilt.extend_from_slice(&(new_body as i64).to_le_bytes());
        rebuilt.extend_from_slice(&original[footer_block + 8..original.len() - 10]);
        rebuilt.extend_from_slice(&original[original.len() - 10..]);
        std::fs::write(path, rebuilt).unwrap();
    }

    fn resource_limit(error: &graphforge_core::GfError) -> bool {
        matches!(
            error,
            graphforge_core::GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                ..
            }
        )
    }

    /// An ordinary 64 KiB linked frame - the geometry a third-party writer
    /// produces for small blocks - is planned by its exact scanned workspace
    /// and decodes at exactly that budget: no arbitrary floor, and the 64 KiB
    /// window is counted.
    #[test]
    fn a_linked_frame_buffer_decodes_within_its_exact_geometry_budget() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("linked.arrow");
        let values = write_int_source(&path, 2048);
        let frame = lz4_frame(
            &values_bytes(&values),
            lz4_flex::frame::BlockSize::Max64KB,
            true,
        );
        values_buffer_as_frames(&path, &[frame], 16_384);

        let plan = ipc_plan(&path).unwrap();
        assert_eq!(plan.rows, vec![2048]);
        let chunk = super::VERIFY_CHUNK_BYTES as u64;
        // src 64 KiB beside dst 2*64 KiB + the 64 KiB window, plus the chunk.
        assert_eq!(plan.codec_context, 4 * 64 * 1024 + chunk);

        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(plan.decoding_bytes)));
        assert!(ipc_plan(&path).is_ok(), "the exact plan budget admits it");
        let refused = ipc_plan(&path);
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        let error = refused.err().expect("one byte less must refuse");
        assert!(
            resource_limit(&error) && error.to_string().contains("decoding"),
            "{error}"
        );

        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(plan.decoding_bytes)));
        let file = File::open(&path).unwrap();
        let mut reader = super::CheckedIpcReader::new(&plan, file).unwrap();
        reader.read_dictionaries().unwrap();
        let batch = reader.read_record_batch(0).unwrap();
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        let decoded = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(&decoded.values()[..], values.as_slice());
    }

    /// A legacy 8 MiB frame is admitted by its own geometry - sixteen
    /// mebibytes of codec context, which the old 12 MiB constant
    /// under-admitted - and refused one byte earlier.
    #[test]
    fn a_legacy_frame_is_admitted_by_its_own_geometry() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("legacy.arrow");
        let values = write_int_source(&path, 2048);
        let frame = legacy_frame(&values_bytes(&values));
        values_buffer_as_frames(&path, &[frame], 16_384);

        let plan = ipc_plan(&path).unwrap();
        let chunk = super::VERIFY_CHUNK_BYTES as u64;
        assert_eq!(plan.codec_context, (16 << 20) + chunk);
        assert!(plan.decoding_bytes > (16 << 20) + chunk);

        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(plan.decoding_bytes)));
        assert!(ipc_plan(&path).is_ok());
        let refused = ipc_plan(&path);
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        assert!(resource_limit(&refused.err().unwrap()));

        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(plan.decoding_bytes)));
        let file = File::open(&path).unwrap();
        let mut reader = super::CheckedIpcReader::new(&plan, file).unwrap();
        reader.read_dictionaries().unwrap();
        let batch = reader.read_record_batch(0).unwrap();
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        let decoded = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(&decoded.values()[..], values.as_slice());
    }

    /// The reader derives each owned frame's geometry before the frame
    /// decoder is built: a file rewritten with a larger-geometry frame after
    /// planning is refused against the plan's admitted codec context, not
    /// silently decoded with an unadmitted workspace. A frame's encoded
    /// length does not depend on its declared block size, so the rewrite
    /// keeps every length the plan checked while multiplying the workspace
    /// the decoder would reserve.
    #[test]
    fn a_frame_beyond_the_planned_codec_context_is_refused_before_the_decoder_is_built() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("grown.arrow");
        let values = write_int_source(&path, 2048);
        let small = lz4_frame(
            &values_bytes(&values),
            lz4_flex::frame::BlockSize::Max64KB,
            true,
        );
        values_buffer_as_frames(&path, &[small], 16_384);
        let plan = ipc_plan(&path).unwrap();
        assert_eq!(
            plan.codec_context,
            4 * 64 * 1024 + super::VERIFY_CHUNK_BYTES as u64
        );

        // The same values, now in a 4 MiB linked frame: one block either
        // way, so the encoded length is the same and only the header's
        // block code differs - same stated expansion, a body of the same
        // length, forty-eight times the codec context.
        let grown = lz4_frame(
            &values_bytes(&values),
            lz4_flex::frame::BlockSize::Max4MB,
            true,
        );
        assert_eq!(
            grown.len(),
            small.len(),
            "one block, a different header only"
        );
        values_buffer_as_frames(&path, &[grown], 16_384);

        let file = File::open(&path).unwrap();
        let mut reader = super::CheckedIpcReader::new(&plan, file).unwrap();
        reader.read_dictionaries().unwrap();
        let error = reader
            .read_record_batch(0)
            .err()
            .expect("the rewritten frame must refuse");
        assert!(resource_limit(&error), "{error}");
        assert!(error.to_string().contains("codec context"), "{error}");
    }

    /// Two LZ4 dictionary messages and a record batch decode within the
    /// exact planned budget: the codec context is a temporary, charged once,
    /// never retained per dictionary - so the retained dictionaries plus the
    /// widest batch plus one context is enough, and one byte less refuses.
    #[test]
    fn two_lz4_dictionaries_and_a_batch_decode_within_the_exact_planned_budget() {
        use arrow::array::Array as _;
        use arrow::array::StringDictionaryBuilder;
        use arrow::datatypes::Int32Type;
        fn dictionary(values: &[&str]) -> arrow::array::ArrayRef {
            let mut builder = StringDictionaryBuilder::<Int32Type>::new();
            for value in values {
                builder.append_value(value);
            }
            Arc::new(builder.finish())
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("dictionaries.arrow");
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "first",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new(
                "second",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]));
        let first = dictionary(&["alpha", "beta", "gamma"]);
        let second = dictionary(&["one", "two", "three"]);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![first, second]).unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer =
            FileWriter::try_new_with_options(File::create(&path).unwrap(), &schema, options)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let plan = ipc_plan(&path).unwrap();
        assert_eq!(plan.dictionaries.len(), 2, "two dictionary messages");
        // The codec context covers one ordinary 64 KiB independent frame:
        // every compressed buffer here is smaller than a block.
        assert_eq!(
            plan.codec_context,
            2 * 64 * 1024 + super::VERIFY_CHUNK_BYTES as u64
        );

        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(plan.decoding_bytes)));
        assert!(ipc_plan(&path).is_ok(), "the exact plan budget admits it");
        let refused = ipc_plan(&path);
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        let error = refused.err().expect("one byte less must refuse");
        assert!(resource_limit(&error), "{error}");

        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(plan.decoding_bytes)));
        let file = File::open(&path).unwrap();
        let mut reader = super::CheckedIpcReader::new(&plan, file).unwrap();
        reader.read_dictionaries().unwrap();
        let decoded = reader.read_record_batch(0).unwrap();
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        for column in [0, 1] {
            let dictionary = decoded
                .column(column)
                .as_any()
                .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
                .unwrap();
            let dict_values = dictionary
                .values()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for index in 0..dictionary.len() {
                let key = dictionary.keys().value(index);
                assert!(
                    !dict_values.value(key as usize).is_empty(),
                    "dictionary {column} decoded"
                );
            }
        }
    }
}
