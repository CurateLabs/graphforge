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

#[cfg(test)]
use super::bulk_source::bulk_build_memory_budget;
use super::bulk_source::schema_owned_bytes;
use super::inventory_budget::InventoryBudget;
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
        let want = usize::try_from((buffer.len() as u64).min(self.remaining)).map_err(storage)?;
        if want == 0 {
            return Ok(0);
        }
        let mut taken = 0_usize;
        while taken < want {
            let read = self.file.read(&mut buffer[taken..want]).map_err(storage)?;
            if read == 0 {
                return Err(storage("Arrow compressed buffer is truncated"));
            }
            taken += read;
        }
        self.remaining -= want as u64;
        Ok(want)
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
    let [flg, bd, _hc] = base;
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

/// Native buffers the pinned frame decoder initializes for this owned stream.
/// Parquet's historical framed LZ4 consumer has the same first-zero lifetime
/// as the IPC verifier, so both must admit the same header-derived geometry.
pub(super) fn lz4_frame_workspace(input: &[u8]) -> Result<usize, GfError> {
    let geometry = scan_first_frame_geometry(&mut SliceFrameBytes::new(input))?;
    usize::try_from(geometry.peak).map_err(storage)
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
#[cfg(test)]
pub(super) fn ipc_plan(path: &Path) -> Result<IpcPlan, GfError> {
    ipc_plan_with_budget(path, bulk_build_memory_budget()?)
}

// Keep the footer and every message header checked in one ordered pass so no
// IPC decoder can observe metadata before its workspace has been admitted.
#[allow(clippy::too_many_lines)]
pub(super) fn ipc_plan_with_budget(path: &Path, budget: u64) -> Result<IpcPlan, GfError> {
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
    let mut workspace = InventoryBudget::new(budget);
    workspace.admit(footer_length, "the Arrow IPC footer snapshot")?;
    let footer_size = usize::try_from(footer_length).map_err(storage)?;
    let mut footer_bytes = Vec::new();
    footer_bytes
        .try_reserve_exact(footer_size)
        .map_err(|_| super::limit("the allocator refused the Arrow IPC footer snapshot"))?;
    footer_bytes.resize(footer_size, 0);
    file.seek(SeekFrom::Start(length - 10 - footer_length))
        .map_err(storage)?;
    file.read_exact(&mut footer_bytes).map_err(storage)?;
    let footer = arrow::ipc::root_as_footer(&footer_bytes).map_err(storage)?;
    let ipc_schema = footer
        .schema()
        .ok_or_else(|| storage("Arrow footer has no schema"))?;
    let schema_facts =
        super::ipc_schema_admission::preflight(ipc_schema, workspace.remaining(), None)?;
    workspace.admit(
        schema_facts.peak_request_bytes,
        "the converted Arrow IPC schema",
    )?;
    let schema = arrow::ipc::convert::fb_to_schema(ipc_schema);
    let schema_bytes = schema_owned_bytes(&schema);
    workspace.release(
        schema_facts
            .peak_request_bytes
            .saturating_sub(schema_facts.retained_request_bytes),
    );
    let columns = schema.fields().len();
    let version = footer.version();
    let batch_count = footer.recordBatches().map_or(0, |blocks| blocks.len());
    let dictionary_count = footer.dictionaries().map_or(0, |blocks| blocks.len());
    let rows_bytes = (batch_count as u64).saturating_mul(8);
    let inventory_bytes = ((batch_count + dictionary_count) as u64)
        .saturating_mul(std::mem::size_of::<IpcBlock>() as u64)
        .saturating_add(rows_bytes)
        .saturating_add(256);
    workspace.admit(inventory_bytes, "the Arrow IPC row and block inventory")?;
    // Keep the pre-existing logical-size estimate in the check as well; the
    // conversion request envelope is an allocator bound, while this captures
    // schema values retained by the planned reader.
    if footer_length
        .saturating_add(schema_bytes)
        .saturating_add(rows_bytes)
        > budget
    {
        return Err(super::limit(
            "Arrow source row inventory exceeds construction memory budget",
        ));
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(batch_count)
        .map_err(|_| super::limit("the allocator refused the Arrow IPC row inventory"))?;
    let mut dictionaries = Vec::new();
    dictionaries
        .try_reserve_exact(dictionary_count)
        .map_err(|_| super::limit("the allocator refused the Arrow IPC dictionary inventory"))?;
    let mut batches = Vec::new();
    batches
        .try_reserve_exact(batch_count)
        .map_err(|_| super::limit("the allocator refused the Arrow IPC batch inventory"))?;
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
            workspace.admit(metadata_length, "an Arrow IPC message header")?;
            let header_len = usize::try_from(metadata_length).map_err(storage)?;
            let mut header = Vec::new();
            header
                .try_reserve_exact(header_len)
                .map_err(|_| super::limit("the allocator refused an Arrow IPC message header"))?;
            header.resize(header_len, 0);
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
            let batch_rows = (!dictionary)
                .then(|| u64::try_from(batch.length()).map_err(storage))
                .transpose()?;
            drop(header);
            workspace.release(metadata_length);
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
                rows.push(
                    batch_rows.ok_or_else(|| storage("IPC record batch row count is missing"))?,
                );
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
#[path = "bounded_ipc/tests.rs"]
mod ipc_planning_tests;
