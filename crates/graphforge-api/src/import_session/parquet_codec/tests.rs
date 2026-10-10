use graphforge_core::{GfError, ProjectErrorCode};
use parquet::basic::Compression;
use parquet::compression::{CodecOptions, create_codec};

use super::{ZSTD_WORKSPACE, gzip, gzip_workspace, snappy, zstd};

fn compress(kind: Compression, data: &[u8]) -> Vec<u8> {
    let mut codec = create_codec(kind, &CodecOptions::default())
        .unwrap()
        .unwrap();
    let mut compressed = Vec::new();
    codec.compress(data, &mut compressed).unwrap();
    compressed
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
fn gzip_decodes_real_members_into_only_the_admitted_destination() {
    let values = vec![7; 256 << 10];
    let mut bytes = compress(Compression::GZIP(Default::default()), &values);
    bytes.extend(compress(Compression::GZIP(Default::default()), b"second"));
    let mut expected = values;
    expected.extend_from_slice(b"second");
    let mut output = vec![0; expected.len()];
    gzip(&bytes, &mut output, gzip_workspace()).unwrap();
    assert_eq!(output, expected);
    let mut short = vec![0; expected.len() - 1];
    assert!(gzip(&bytes, &mut short, gzip_workspace()).is_err());
    let mut long = vec![0; expected.len() + 1];
    assert!(gzip(&bytes, &mut long, gzip_workspace()).is_err());
}

#[test]
fn gzip_optional_header_fields_are_borrowed_and_checksums_validated() {
    let values = b"ordinary payload";
    let original = compress(Compression::GZIP(Default::default()), values);
    let mut bytes = original[..10].to_vec();
    bytes[3] = 2 | 4 | 8 | 16;
    bytes.extend_from_slice(&3_u16.to_le_bytes());
    bytes.extend_from_slice(b"abc");
    bytes.extend(std::iter::repeat_n(b'n', 2 << 20));
    bytes.push(0);
    bytes.extend_from_slice(b"comment\0");
    bytes.extend_from_slice(&(crc32fast::hash(&bytes) as u16).to_le_bytes());
    bytes.extend_from_slice(&original[10..]);
    let mut output = [0; 16];
    gzip(&bytes, &mut output, gzip_workspace()).unwrap();
    assert_eq!(&output, values);
    bytes[13] ^= 1;
    assert!(gzip(&bytes, &mut output, gzip_workspace()).is_err());
}

#[test]
fn gzip_checks_workspace_before_constructing_or_parsing_the_decoder() {
    assert!(is_limit(
        gzip(b"invalid", &mut [], gzip_workspace() - 1).unwrap_err()
    ));
}

#[test]
fn gzip_validates_footer_and_empty_members() {
    let mut bytes = compress(Compression::GZIP(Default::default()), b"");
    gzip(&bytes, &mut [], gzip_workspace()).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    assert!(gzip(&bytes, &mut [], gzip_workspace()).is_err());
    let mut bytes = compress(Compression::GZIP(Default::default()), b"abc");
    let checksum = bytes.len() - 8;
    bytes[checksum] ^= 1;
    assert!(gzip(&bytes, &mut [0; 3], gzip_workspace()).is_err());
}

#[test]
fn snappy_body_counts_cannot_resize_the_destination() {
    // Raw varint decoded-size prefix advertises 1 GiB, with no literal body.
    let bytes = [0x80, 0x80, 0x80, 0x80, 0x04];
    let mut output = [0xa5; 16];
    assert!(snappy(&bytes, &mut output).is_err());
    assert_eq!(output, [0xa5; 16]);
    let values = vec![42; 128 << 10];
    let bytes = compress(Compression::SNAPPY, &values);
    let mut output = vec![0; values.len()];
    snappy(&bytes, &mut output).unwrap();
    assert_eq!(output, values);
    assert!(snappy(&bytes, &mut [0; 1]).is_err());
}

#[test]
fn zstd_fixed_context_decodes_modern_members_and_checks_total_size() {
    let values = vec![9; 256 << 10];
    let mut bytes = compress(Compression::ZSTD(Default::default()), &values);
    bytes.extend(compress(Compression::ZSTD(Default::default()), b"abc"));
    let mut expected = values;
    expected.extend_from_slice(b"abc");
    let mut output = vec![0; expected.len()];
    zstd(&bytes, &mut output, ZSTD_WORKSPACE).unwrap();
    assert_eq!(output, expected);
    assert!(zstd(&bytes, &mut [0; 1], ZSTD_WORKSPACE).is_err());
    let mut long = vec![0; expected.len() + 1];
    assert!(zstd(&bytes, &mut long, ZSTD_WORKSPACE).is_err());
    assert!(is_limit(
        zstd(b"invalid", &mut [], ZSTD_WORKSPACE - 1).unwrap_err()
    ));
}

#[test]
fn gzip_stream_resumes_at_actual_input_and_output_chunk_boundaries() {
    let values = (0..1_000_003)
        .map(|index| {
            let value = (index as u64).wrapping_mul(0x9E3779B97F4A7C15);
            (value ^ (value >> 27) ^ (value >> 43)) as u8
        })
        .collect::<Vec<_>>();
    let bytes = compress(Compression::GZIP(Default::default()), &values);
    assert!(
        bytes.len() > 8 << 10,
        "fixture crosses real encoded input chunks"
    );
    let mut output = vec![0; values.len()];
    gzip(&bytes, &mut output, gzip_workspace()).unwrap();
    assert_eq!(output, values);
}
