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
        FileWriter::try_new_with_options(File::create(path).unwrap(), &schema, options).unwrap();
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
            .position(|window| window[..8] == 4_096_000_u64.to_le_bytes() && window[8..] == frame)
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
    let footer = arrow::ipc::root_as_footer(&original[footer_start..original.len() - 10]).unwrap();
    let slot = footer_start
        + footer._tab.loc()
        + usize::from(
            footer
                ._tab
                .vtable()
                .get(arrow::ipc::Footer::VT_RECORDBATCHES),
        );
    let vector = slot + u32::from_le_bytes(original[slot..slot + 4].try_into().unwrap()) as usize;
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
        FileWriter::try_new_with_options(File::create(path).unwrap(), &schema, options).unwrap();
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
    // The values buffer: a struct vector of (offset i64, length i64)
    // slots, validity first and the nonempty values last.
    let buffers_slot = block_at
        + message_skip
        + batch._tab.loc()
        + usize::from(batch._tab.vtable().get(IpcRecordBatch::VT_BUFFERS));
    let buffers = buffers_slot
        + u32::from_le_bytes(original[buffers_slot..buffers_slot + 4].try_into().unwrap()) as usize;
    let slot_count =
        u32::from_le_bytes(original[buffers..buffers + 4].try_into().unwrap()) as usize;
    assert_eq!(slot_count, 2, "an Int64 array carries validity and values");
    let values_at = buffers + 4 + (slot_count - 1) * 16;
    let values_offset = u64::from_le_bytes(original[values_at..values_at + 8].try_into().unwrap());
    let values_length = u64::try_from(i64::from_le_bytes(
        original[values_at + 8..values_at + 16].try_into().unwrap(),
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

    let frame_bytes =
        |frames: &[Vec<u8>]| -> u64 { frames.iter().map(|frame| frame.len() as u64).sum() };
    let new_body = values_offset + 8 + frame_bytes(frames);
    let new_values_length = 8 + frame_bytes(frames);
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
    rebuilt[body_length_at..body_length_at + 8].copy_from_slice(&(new_body as i64).to_le_bytes());
    rebuilt[values_at + 8..values_at + 16]
        .copy_from_slice(&(new_values_length as i64).to_le_bytes());
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
    super::super::bulk_source::TEST_BUDGET
        .with(|budget| budget.set(Some(plan.decoding_bytes.checked_sub(1).unwrap())));
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
    super::super::bulk_source::TEST_BUDGET
        .with(|budget| budget.set(Some(plan.decoding_bytes.checked_sub(1).unwrap())));
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
    let small_len = small.len();
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
    assert_eq!(grown.len(), small_len, "one block, a different header only");
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
        for row in 0..2048 {
            builder.append_value(values[row % values.len()]);
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
    // Compressible dictionary payloads and indices ensure Arrow actually uses
    // frames: tiny strings alone legitimately choose the -1/raw representation.
    let first_values = ["alpha".repeat(64), "beta".repeat(64), "gamma".repeat(64)];
    let second_values = ["one".repeat(64), "two".repeat(64), "three".repeat(64)];
    let first = dictionary(&first_values.iter().map(String::as_str).collect::<Vec<_>>());
    let second = dictionary(&second_values.iter().map(String::as_str).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![first, second]).unwrap();
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
        .unwrap();
    let mut writer =
        FileWriter::try_new_with_options(File::create(&path).unwrap(), &schema, options).unwrap();
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
    super::super::bulk_source::TEST_BUDGET
        .with(|budget| budget.set(Some(plan.decoding_bytes.checked_sub(1).unwrap())));
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
        assert_eq!(
            decoded.column(column).as_ref(),
            batch.column(column).as_ref()
        );
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
