use std::io::Write;

use bytes::Bytes;
use graphforge_core::{GfError, ProjectErrorCode};
use parquet::basic::Compression;
use parquet::column::page::Page;
use parquet::compression::{CodecOptions, create_codec};

use super::decode;
use crate::CancellationToken;
use crate::import_session::parquet_codec::gzip_workspace;
use crate::import_session::parquet_page::CompressedPage;
use crate::import_session::parquet_scan::RawHeader;

fn compress(codec: Compression, data: &[u8]) -> Vec<u8> {
    let mut compressor = create_codec(codec, &CodecOptions::default())
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    compressor.compress(data, &mut bytes).unwrap();
    bytes
}

fn page(body: Vec<u8>, expanded: usize) -> CompressedPage {
    let body_capacity = body.capacity();
    CompressedPage {
        header: RawHeader {
            kind: Some(0),
            compressed: Some(body.len() as i64),
            uncompressed: Some(expanded as i64),
            values: Some(1),
            encoding: Some(0),
            definition_encoding: Some(3),
            repetition_encoding: Some(3),
            ..Default::default()
        },
        physical_bytes: body.len() as u64,
        body: Bytes::from(body),
        body_capacity,
    }
}

fn is_limit(error: GfError) -> bool {
    matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    )
}

#[test]
fn real_page_codecs_use_the_same_owned_fixed_destination() {
    let expected = (0..(128 << 10))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    for codec in [
        Compression::SNAPPY,
        Compression::GZIP(Default::default()),
        Compression::BROTLI(Default::default()),
        Compression::ZSTD(Default::default()),
        Compression::LZ4,
        Compression::LZ4_RAW,
    ] {
        let compressed = page(compress(codec, &expected), expected.len());
        let capacity = compressed.body_capacity + expected.len() + (32 << 20);
        let decoded = decode(compressed, codec, capacity, None).unwrap();
        assert_eq!(&decoded.page.buffer()[..], &expected, "{codec:?}");
        assert_eq!(decoded.body_capacity, expected.len());
    }
}

#[test]
fn compressed_and_decoded_bodies_coexist_in_the_same_budget() {
    let bytes = compress(Compression::SNAPPY, b"payload");
    let compressed = page(bytes.clone(), 7);
    let capacity = compressed.body_capacity + 7;
    let decoded = decode(compressed, Compression::SNAPPY, capacity, None).unwrap();
    assert_eq!(&decoded.page.buffer()[..], b"payload");
    let compressed = page(bytes, 7);
    assert!(is_limit(
        decode(compressed, Compression::SNAPPY, capacity - 1, None)
            .err()
            .unwrap()
    ));
}

#[test]
fn gzip_state_credit_is_reserved_beside_both_page_bodies() {
    let codec = Compression::GZIP(Default::default());
    let compressed = page(compress(codec, b"payload"), 7);
    let capacity = compressed.body_capacity + 7 + gzip_workspace() - 1;
    assert!(is_limit(
        decode(compressed, codec, capacity, None).err().unwrap()
    ));
}

#[test]
fn small_headers_cannot_authorize_gzip_or_brotli_output_growth() {
    let enormous = vec![42; 2 << 20];
    for codec in [
        Compression::GZIP(Default::default()),
        Compression::BROTLI(Default::default()),
    ] {
        let compressed = page(compress(codec, &enormous), 16);
        let capacity = compressed.body_capacity + 16 + (32 << 20);
        assert!(decode(compressed, codec, capacity, None).is_err());
    }
    // A tiny Snappy body declares 1GiB while the owned page states 16 bytes.
    let compressed = page(vec![0x80, 0x80, 0x80, 0x80, 0x04], 16);
    assert!(decode(compressed, Compression::SNAPPY, 21, None).is_err());
}

#[test]
fn v2_level_prefix_is_copied_and_only_its_value_suffix_is_decompressed() {
    let codec = Compression::SNAPPY;
    let mut body = vec![2, 1, 2, 1];
    body.extend(compress(codec, b"value"));
    let mut compressed = page(body, 9);
    compressed.header.kind = Some(3);
    compressed.header.definition_bytes = Some(2);
    compressed.header.repetition_bytes = Some(2);
    compressed.header.rows = Some(1);
    compressed.header.nulls = Some(0);
    let capacity = compressed.body_capacity + 9;
    let decoded = decode(compressed, codec, capacity, None).unwrap();
    assert_eq!(&decoded.page.buffer()[..], b"\x02\x01\x02\x01value");
    assert!(matches!(
        decoded.page,
        Page::DataPageV2 {
            is_compressed: false,
            ..
        }
    ));
}

#[test]
fn null_only_v2_page_does_not_initialize_a_codec_or_consume_ignored_suffix() {
    let mut compressed = page(vec![2, 0, 99, 88], 2);
    compressed.header.kind = Some(3);
    compressed.header.definition_bytes = Some(2);
    compressed.header.repetition_bytes = Some(0);
    compressed.header.rows = Some(1);
    compressed.header.nulls = Some(1);
    let capacity = compressed.body_capacity + 2;
    // LZO is unsupported by the ordinary codec, but no codec is needed here.
    let decoded = decode(compressed, Compression::LZO, capacity, None).unwrap();
    assert_eq!(&decoded.page.buffer()[..], &[2, 0]);
}

#[test]
fn v2_invalid_levels_and_counts_are_checked_before_decoding() {
    for (rep, def, rows, nulls) in [(3, 0, 1, 0), (0, -1, 1, 0), (0, 0, 2, 0), (0, 0, 1, 2)] {
        let mut compressed = page(vec![0; 2], 2);
        compressed.header.kind = Some(3);
        compressed.header.repetition_bytes = Some(rep);
        compressed.header.definition_bytes = Some(def);
        compressed.header.rows = Some(rows);
        compressed.header.nulls = Some(nulls);
        assert!(decode(compressed, Compression::SNAPPY, 4, None).is_err());
    }
}

#[test]
fn uncompressed_and_dictionary_pages_move_the_original_allocation() {
    let mut compressed = page(vec![7; 16], 16);
    let pointer = compressed.body.as_ptr();
    let capacity = compressed.body_capacity;
    compressed.header.kind = Some(2);
    compressed.header.sorted_dictionary = Some(true);
    let decoded = decode(compressed, Compression::UNCOMPRESSED, capacity, None).unwrap();
    assert_eq!(decoded.page.buffer().as_ptr(), pointer);
    assert_eq!(decoded.body_capacity, capacity);
    assert_eq!(decoded.physical_bytes, 16);
    assert!(matches!(
        decoded.page,
        Page::DictionaryPage {
            is_sorted: true,
            ..
        }
    ));
}

#[test]
fn framed_lz4_admits_actual_native_buffers_and_ignores_later_frames() {
    let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
    encoder.write_all(b"value").unwrap();
    let mut body = encoder.finish().unwrap();
    let first = body.clone();
    let mut next = lz4_flex::frame::FrameEncoder::new(Vec::new());
    next.write_all(b"ignored").unwrap();
    body.extend(next.finish().unwrap());
    let needed = crate::import_session::bounded_ipc::lz4_frame_workspace(&first).unwrap();
    let compressed = page(body.clone(), 5);
    let capacity = compressed.body_capacity + 5 + needed;
    let decoded = decode(compressed, Compression::LZ4, capacity, None).unwrap();
    assert_eq!(&decoded.page.buffer()[..], b"value");
    let compressed = page(body, 5);
    assert!(is_limit(
        decode(compressed, Compression::LZ4, capacity - 1, None)
            .err()
            .unwrap()
    ));
}

#[test]
fn cancellation_is_checked_before_any_page_decode() {
    let token = CancellationToken::new();
    token.cancel();
    let compressed = page(vec![7], 1);
    let error = decode(compressed, Compression::UNCOMPRESSED, 1, Some(&token))
        .err()
        .unwrap();
    assert!(matches!(
        error,
        GfError::Api {
            code: graphforge_core::ApiErrorCode::Cancelled,
            ..
        }
    ));
}

#[test]
fn raw_lz4_token_decoder_matches_the_pinned_encoder_at_overlap_boundaries() {
    for period in [1, 3, 7, 17, 255, 8191, 8192, 8193, 32767] {
        let expected = (0..100_003)
            .map(|index| ((index % period) % 251) as u8)
            .collect::<Vec<_>>();
        let body = lz4_flex::block::compress(&expected);
        let compressed = page(body, expected.len());
        let capacity = compressed.body_capacity + expected.len();
        let decoded = decode(compressed, Compression::LZ4_RAW, capacity, None).unwrap();
        assert_eq!(&decoded.page.buffer()[..], &expected, "period={period}");
    }
    for body in [vec![0xf0], vec![0x10, 1, 0, 0], vec![0x00, 1, 0]] {
        assert!(decode(page(body, 1), Compression::LZ4_RAW, 1024, None).is_err());
    }
}

#[test]
fn raw_lz4_match_requires_the_pinned_final_literal_token() {
    let bytes = [0x10, b'a', 0x01, 0x00];
    assert!(lz4_flex::block::decompress_into(&bytes, &mut [0; 5]).is_err());
    let compressed = page(bytes.to_vec(), 5);
    assert!(decode(compressed, Compression::LZ4_RAW, 9, None).is_err());
}

#[test]
fn gzip_and_brotli_callbacks_return_typed_cancellation_on_otherwise_valid_streams() {
    let token = CancellationToken::new();
    token.cancel();
    let gzip = compress(Compression::GZIP(Default::default()), b"payload");
    let error = crate::import_session::parquet_codec::gzip_cancellable(
        &gzip,
        &mut [0; 7],
        gzip_workspace(),
        Some(&token),
    )
    .err()
    .unwrap();
    assert!(matches!(
        error,
        GfError::Api {
            code: graphforge_core::ApiErrorCode::Cancelled,
            ..
        }
    ));
    let brotli = compress(Compression::BROTLI(Default::default()), b"payload");
    let error = crate::import_session::parquet_brotli::decode_cancellable(
        &brotli,
        &mut [0; 7],
        32 << 20,
        Some(&token),
    )
    .err()
    .unwrap();
    assert!(matches!(
        error,
        GfError::Api {
            code: graphforge_core::ApiErrorCode::Cancelled,
            ..
        }
    ));
}
