//! Query spill files that are validated before DataFusion decodes them (#2354).
//!
//! DataFusion 56's spill reader decodes its IPC frames with Arrow validation
//! switched off (`StreamDecoder::with_skip_validation(true)` in
//! `datafusion-physical-plan/src/spill/mod.rs`), on the grounds that it only
//! reads files it wrote. A spill file is a file on disk, though, and corrupt
//! bytes there would become Arrow arrays whose offsets and lengths are not
//! checked; using one is undefined behaviour or a panic (an invalid list
//! offset aborted the process). No DataFusion configuration enables
//! validation, so GraphForge installs its spill files through DataFusion's
//! public [`TempFileFactory`] seam and checks every frame of the byte stream
//! before handing it on:
//!
//! - framing and the IPC buffer descriptors are checked first: every buffer
//!   lies inside its message body at an 8-byte aligned offset, as the IPC
//!   format requires, and every field node has a non-negative length and a
//!   null count no larger than that length;
//! - the complete frame is then decoded by Arrow's validating
//!   [`StreamDecoder`] (validation is its default), which checks list and
//!   string offsets, child lengths, UTF-8 and dictionary keys; the decoded
//!   arrays are dropped.
//!
//! Only frames that pass are forwarded, byte for byte, so a valid spill
//! round-trips exactly and DataFusion's own reader never sees a frame Arrow
//! would refuse. A refused frame is a [`CorruptSpillFile`] error.
//!
//! Memory: no allocation is ever sized from an unvalidated length. The reader
//! holds only the bytes of the current frame that are actually present in the
//! file (the chunks DataFusion's file stream already reads) plus one
//! contiguous copy of that frame while it is validated; both are released or
//! forwarded before DataFusion decodes the frame. A header that claims more
//! bytes than the file holds is refused when the file ends, without
//! allocating the claimed size.

use std::collections::VecDeque;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::buffer::{Buffer, MutableBuffer};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamDecoder;
use arrow::ipc::{MessageHeader, root_as_message};
use bytes::Bytes;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::disk_manager::{DiskManager, DiskManagerBuilder};
use datafusion::execution::{AsyncSpillWriter, SpillFile, SpillWriter, TempFileFactory};
use futures::Stream;

/// IPC continuation marker preceding a message's metadata length.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// IPC buffers start at 8-byte aligned offsets of their message body.
const IPC_BUFFER_ALIGNMENT: i64 = 8;

/// A query spill file whose bytes are not a valid Arrow IPC stream.
#[derive(Debug, thiserror::Error)]
#[error("corrupt query spill file: {reason}")]
pub(crate) struct CorruptSpillFile {
    reason: String,
    #[source]
    source: Option<ArrowError>,
}

impl CorruptSpillFile {
    fn refuse(reason: impl Into<String>) -> DataFusionError {
        DataFusionError::External(Box::new(Self {
            reason: reason.into(),
            source: None,
        }))
    }

    fn arrow(error: ArrowError) -> DataFusionError {
        DataFusionError::External(Box::new(Self {
            reason: format!("Arrow refused a frame: {error}"),
            source: Some(error),
        }))
    }
}

/// A disk manager builder whose spill files come from `files` and are
/// validated as they are read back.
///
/// The returned builder carries `files`' temporary-directory cap and merge
/// fan-in, so DataFusion's runtime reports and applies the same settings; the
/// cap itself is enforced by `files`' writers.
pub(crate) fn validated_disk_manager(files: DiskManagerBuilder) -> Result<DiskManagerBuilder> {
    let files = Arc::new(files.build()?);
    let max_temp_directory_size = files.max_temp_directory_size();
    let max_spill_merge_fan_in = files.max_spill_merge_fan_in();
    Ok(DiskManagerBuilder::default()
        .with_temp_file_factory(Arc::new(ValidatedSpillFiles { files }))
        .with_max_temp_directory_size(max_temp_directory_size)
        .with_max_spill_merge_fan_in(max_spill_merge_fan_in))
}

/// Creates spill files through a stock [`DiskManager`] and validates them on read.
#[derive(Debug)]
struct ValidatedSpillFiles {
    files: Arc<DiskManager>,
}

impl TempFileFactory for ValidatedSpillFiles {
    fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>> {
        Ok(Arc::new(ValidatedSpillFile {
            file: self.files.create_tmp_file(description)?,
        }))
    }
}

struct ValidatedSpillFile {
    file: Arc<dyn SpillFile>,
}

#[async_trait::async_trait]
impl SpillFile for ValidatedSpillFile {
    fn path(&self) -> Option<&Path> {
        self.file.path()
    }

    fn size(&self) -> Option<u64> {
        self.file.size()
    }

    fn read_stream(&self) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>> {
        Ok(Box::pin(ValidatedSpillStream {
            bytes: self.file.read_stream()?,
            frames: FrameValidator::default(),
            ready: VecDeque::new(),
            done: false,
        }))
    }

    fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
        self.file.open_writer()
    }

    async fn open_async_writer(&self) -> Result<Box<dyn AsyncSpillWriter>> {
        self.file.open_async_writer().await
    }
}

/// Forwards a spill file's bytes one validated frame at a time.
struct ValidatedSpillStream {
    bytes: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
    frames: FrameValidator,
    ready: VecDeque<Bytes>,
    done: bool,
}

impl Stream for ValidatedSpillStream {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(bytes) = this.ready.pop_front() {
                return Poll::Ready(Some(Ok(bytes)));
            }
            if this.done {
                return Poll::Ready(None);
            }
            let outcome = match futures::ready!(this.bytes.as_mut().poll_next(cx)) {
                Some(Ok(chunk)) => this.frames.push(chunk, &mut this.ready),
                Some(Err(error)) => Err(error),
                None => {
                    this.done = true;
                    this.frames.finish()
                }
            };
            if let Err(error) = outcome {
                this.done = true;
                this.ready.clear();
                return Poll::Ready(Some(Err(error)));
            }
        }
    }
}

/// The parsed header of the frame being assembled.
struct FrameHeader {
    /// Length prefix plus flatbuffer metadata.
    head: Vec<u8>,
    body_len: usize,
}

/// Splits an IPC byte stream into frames and validates each one.
#[derive(Default)]
struct FrameValidator {
    /// Received bytes not yet validated, in file order.
    held: VecDeque<Bytes>,
    held_len: usize,
    header: Option<FrameHeader>,
    decoder: StreamDecoder,
    finished: bool,
}

impl FrameValidator {
    /// Accepts the next chunk and moves every frame it completes to `ready`.
    fn push(&mut self, chunk: Bytes, ready: &mut VecDeque<Bytes>) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        if self.finished {
            return Err(CorruptSpillFile::refuse(
                "bytes follow the end-of-stream marker",
            ));
        }
        self.held_len += chunk.len();
        self.held.push_back(chunk);
        while self.next_frame(ready)? {}
        Ok(())
    }

    /// Checks that the stream ended on a frame boundary.
    fn finish(&mut self) -> Result<()> {
        if self.held_len != 0 {
            return Err(CorruptSpillFile::refuse(format!(
                "the file ends inside a frame ({} trailing bytes)",
                self.held_len
            )));
        }
        self.decoder.finish().map_err(CorruptSpillFile::arrow)
    }

    /// Validates and forwards one complete frame; false when more bytes are needed.
    fn next_frame(&mut self, ready: &mut VecDeque<Bytes>) -> Result<bool> {
        if self.finished {
            if self.held_len != 0 {
                return Err(CorruptSpillFile::refuse(
                    "bytes follow the end-of-stream marker",
                ));
            }
            return Ok(false);
        }
        if self.header.is_none() {
            let Some(prefix) = self.copy_front(4) else {
                return Ok(false);
            };
            let (prefix_len, metadata_len) = if prefix[..] == CONTINUATION_MARKER {
                let Some(prefix) = self.copy_front(8) else {
                    return Ok(false);
                };
                (
                    8,
                    u32::from_le_bytes(prefix[4..8].try_into().expect("4 bytes")),
                )
            } else {
                (
                    4,
                    u32::from_le_bytes(prefix[..].try_into().expect("4 bytes")),
                )
            };
            if metadata_len == 0 {
                self.decode(Buffer::from_vec(self.copy_front(prefix_len).expect("held")))?;
                self.forward(prefix_len, ready);
                self.finished = true;
                return Ok(true);
            }
            let head_len = prefix_len + metadata_len as usize;
            let Some(head) = self.copy_front(head_len) else {
                return Ok(false);
            };
            let body_len = check_descriptors(&head[prefix_len..])?;
            self.header = Some(FrameHeader { head, body_len });
        }
        let header = self.header.as_ref().expect("header parsed");
        let head_len = header.head.len();
        let Some(frame_len) = head_len.checked_add(header.body_len) else {
            return Err(CorruptSpillFile::refuse("frame length overflows"));
        };
        if self.held_len < frame_len {
            return Ok(false);
        }
        let header = self.header.take().expect("header parsed");
        let mut body = MutableBuffer::new(header.body_len);
        self.copy_range(head_len, header.body_len, &mut body);
        self.decode(Buffer::from_vec(header.head))?;
        self.decode(body.into())?;
        self.forward(frame_len, ready);
        Ok(true)
    }

    /// Runs Arrow's validating decoder over `buffer`, dropping what it decodes.
    fn decode(&mut self, mut buffer: Buffer) -> Result<()> {
        while !buffer.is_empty() {
            let before = buffer.len();
            let decoded = self
                .decoder
                .decode(&mut buffer)
                .map_err(CorruptSpillFile::arrow)?;
            if decoded.is_none() && buffer.len() == before {
                return Err(CorruptSpillFile::refuse("Arrow consumed no frame bytes"));
            }
        }
        Ok(())
    }

    /// Copies the first `len` held bytes, or `None` if fewer are held.
    fn copy_front(&self, len: usize) -> Option<Vec<u8>> {
        if self.held_len < len {
            return None;
        }
        let mut out = Vec::with_capacity(len);
        let mut remaining = len;
        for chunk in &self.held {
            let take = remaining.min(chunk.len());
            out.extend_from_slice(&chunk[..take]);
            remaining -= take;
            if remaining == 0 {
                break;
            }
        }
        Some(out)
    }

    /// Appends held bytes `start..start + len` to `out`; they must be held.
    fn copy_range(&self, mut start: usize, len: usize, out: &mut MutableBuffer) {
        let mut remaining = len;
        for chunk in &self.held {
            if remaining == 0 {
                break;
            }
            if start >= chunk.len() {
                start -= chunk.len();
                continue;
            }
            let take = remaining.min(chunk.len() - start);
            out.extend_from_slice(&chunk[start..start + take]);
            remaining -= take;
            start = 0;
        }
    }

    /// Moves the first `len` held bytes, now validated, to `ready` without copying.
    fn forward(&mut self, mut len: usize, ready: &mut VecDeque<Bytes>) {
        self.held_len -= len;
        while len > 0 {
            let mut chunk = self.held.pop_front().expect("forwarded bytes are held");
            if chunk.len() > len {
                let rest = chunk.split_off(len);
                self.held.push_front(rest);
            }
            len -= chunk.len();
            ready.push_back(chunk);
        }
    }
}

/// Parses a frame's metadata, checks its buffer and node descriptors, and
/// returns its body length.
fn check_descriptors(metadata: &[u8]) -> Result<usize> {
    let message = root_as_message(metadata)
        .map_err(|error| CorruptSpillFile::refuse(format!("invalid IPC message: {error}")))?;
    let body_len = message.bodyLength();
    let body = usize::try_from(body_len)
        .map_err(|_| CorruptSpillFile::refuse(format!("negative IPC body length {body_len}")))?;
    let batch = match message.header_type() {
        MessageHeader::RecordBatch => message.header_as_record_batch(),
        MessageHeader::DictionaryBatch => message
            .header_as_dictionary_batch()
            .and_then(|dictionary| dictionary.data()),
        _ => return Ok(body),
    }
    .ok_or_else(|| CorruptSpillFile::refuse("IPC batch message without a batch header"))?;
    if batch.length() < 0 {
        return Err(CorruptSpillFile::refuse(format!(
            "negative IPC batch length {}",
            batch.length()
        )));
    }
    for (index, buffer) in batch.buffers().into_iter().flatten().enumerate() {
        let (offset, length) = (buffer.offset(), buffer.length());
        let inside = offset >= 0
            && length >= 0
            && offset
                .checked_add(length)
                .is_some_and(|end| end <= body_len);
        if !inside {
            return Err(CorruptSpillFile::refuse(format!(
                "IPC buffer {index} ({offset}, {length}) lies outside the {body_len}-byte body"
            )));
        }
        if offset % IPC_BUFFER_ALIGNMENT != 0 {
            return Err(CorruptSpillFile::refuse(format!(
                "IPC buffer {index} starts at unaligned offset {offset}"
            )));
        }
    }
    for (index, node) in batch.nodes().into_iter().flatten().enumerate() {
        let (length, nulls) = (node.length(), node.null_count());
        if length < 0 || nulls < 0 || nulls > length {
            return Err(CorruptSpillFile::refuse(format!(
                "IPC field node {index} has length {length} and null count {nulls}"
            )));
        }
    }
    Ok(body)
}

#[cfg(test)]
#[path = "validated_spill_tests.rs"]
mod tests;
