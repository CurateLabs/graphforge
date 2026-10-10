use std::io::{Cursor, Read};

use graphforge_core::{GfError, ProjectErrorCode};

use super::{READ_BLOCK, read};
use crate::import_session::parquet_scan::read_header;

fn varint(mut value: u64, bytes: &mut Vec<u8>) {
    loop {
        let byte = (value & 127) as u8;
        value >>= 7;
        bytes.push(byte | if value == 0 { 0 } else { 128 });
        if value == 0 {
            break;
        }
    }
}

fn header(length: usize, crc: Option<u32>) -> Vec<u8> {
    let mut bytes = vec![0x15, 0]; // kind DATA_PAGE
    bytes.push(0x15);
    varint((length as u64) << 1, &mut bytes);
    bytes.push(0x15);
    varint((length as u64) << 1, &mut bytes);
    if let Some(crc) = crc {
        bytes.push(0x15);
        let signed = i64::from(i32::from_ne_bytes(crc.to_ne_bytes()));
        varint(
            (signed.wrapping_shl(1) ^ signed.wrapping_shr(63)) as u64,
            &mut bytes,
        );
    }
    bytes.push(0);
    bytes
}

#[test]
fn current_owned_page_bytes_and_checksum_are_read_together() {
    let body = vec![42; READ_BLOCK * 3 + 7];
    let mut bytes = header(body.len(), Some(crc32fast::hash(&body)));
    let header_len = bytes.len();
    bytes.extend_from_slice(&body);
    let page = read(
        &mut Cursor::new(&bytes),
        bytes.len() as u64,
        body.len(),
        None,
    )
    .unwrap();
    assert_eq!(&page.body[..], &body);
    assert_eq!(page.header.compressed, Some(body.len() as i64));
    assert_eq!(page.physical_bytes as usize, header_len + body.len());
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    assert!(
        read(
            &mut Cursor::new(&bytes),
            bytes.len() as u64,
            body.len(),
            None
        )
        .is_err()
    );
}

#[test]
fn workspace_refusal_precedes_the_first_body_read() {
    struct HeaderOnly {
        bytes: Cursor<Vec<u8>>,
    }
    impl Read for HeaderOnly {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            assert!(
                self.bytes.position() < self.bytes.get_ref().len() as u64,
                "refused page attempted body read"
            );
            self.bytes.read(output)
        }
    }
    let length = 64 << 10;
    let bytes = header(length, None);
    let remaining = bytes.len() as u64 + length as u64;
    let mut input = HeaderOnly {
        bytes: Cursor::new(bytes),
    };
    let error = match read(&mut input, remaining, length - 1, None) {
        Ok(_) => panic!("over-budget body admitted"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
}

#[test]
fn chunk_bounds_refuse_a_huge_body_claim_before_allocation() {
    let bytes = header(1 << 30, None);
    assert!(
        read(
            &mut Cursor::new(&bytes),
            bytes.len() as u64,
            usize::MAX,
            None
        )
        .is_err()
    );
}

#[test]
fn compact_header_rejects_high_bits_in_the_tenth_varint_byte() {
    // Unknown scalar field is still subject to compact integer grammar.
    let mut bytes = vec![0x45];
    bytes.extend_from_slice(&[0x80; 9]);
    bytes.push(2);
    bytes.push(0);
    assert!(read_header(&mut Cursor::new(bytes)).is_err());
    let mut valid = vec![0x45];
    valid.extend_from_slice(&[0x80; 9]);
    valid.push(1);
    valid.push(0);
    assert!(read_header(&mut Cursor::new(valid)).is_ok());
}
