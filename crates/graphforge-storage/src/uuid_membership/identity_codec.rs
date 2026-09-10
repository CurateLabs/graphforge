//! Current permanent identity framing. Live edges omit their defined-zero
//! membership surrogate; topology retains the canonical edge identifier.
use std::io::Read;

use graphforge_core::GfError;

use super::storage_err;

pub(super) const WIDTH: usize = 25;
pub(super) const EDGE_WIDTH: usize = 17;
pub(super) type Record = [u8; WIDTH];

fn width(kind: u8) -> Result<usize, GfError> {
    match kind {
        1 => Ok(EDGE_WIDTH),
        0 | 2 | 3 => Ok(WIDTH),
        _ => Err(storage_err("identity record kind is invalid")),
    }
}

pub(super) fn encoded(record: &Record) -> Result<&[u8], GfError> {
    let length = width(record[16])?;
    if length == EDGE_WIDTH && record[EDGE_WIDTH..].iter().any(|byte| *byte != 0) {
        return Err(storage_err("live edge membership surrogate must be zero"));
    }
    Ok(&record[..length])
}

pub(super) fn read(reader: &mut impl Read) -> Result<Option<Record>, GfError> {
    let mut record = [0_u8; WIDTH];
    loop {
        match reader.read(&mut record[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(storage_err(error)),
        }
    }
    reader
        .read_exact(&mut record[1..EDGE_WIDTH])
        .map_err(storage_err)?;
    let length = width(record[16])?;
    reader
        .read_exact(&mut record[EDGE_WIDTH..length])
        .map_err(storage_err)?;
    Ok(Some(record))
}

pub(super) fn take(bytes: &mut &[u8]) -> Result<Option<Record>, GfError> {
    read(bytes)
}

/// Validate complete block framing and ordered UUID fences without allocation.
pub(super) fn layout(bytes: &[u8]) -> Result<(u64, &[u8], &[u8]), GfError> {
    let mut offset = 0;
    let mut count = 0_u64;
    let mut previous: Option<&[u8]> = None;
    let mut last = 0;
    while offset < bytes.len() {
        let suffix = &bytes[offset..];
        if suffix.len() < EDGE_WIDTH {
            return Err(storage_err(
                "partial identity record in authenticated block",
            ));
        }
        let length = width(suffix[16])?;
        if suffix.len() < length {
            return Err(storage_err(
                "partial identity record in authenticated block",
            ));
        }
        let key = &suffix[..16];
        if previous.is_some_and(|prior| prior >= key) {
            return Err(storage_err("identity block UUIDs are not strictly ordered"));
        }
        previous = Some(key);
        last = offset;
        offset += length;
        count += 1;
    }
    if count == 0 {
        return Err(storage_err("empty identity block"));
    }
    Ok((count, &bytes[..16], &bytes[last..last + 16]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_width_nodes_and_tombstones_and_zero_edge_round_trip() {
        for kind in 0..=3 {
            for surrogate in [0, 1, u64::from(u32::MAX), u64::from(u32::MAX) + 1, u64::MAX] {
                let mut record = [u8::MAX; WIDTH];
                record[16] = kind;
                record[17..].copy_from_slice(&surrogate.to_be_bytes());
                if kind == 1 && surrogate != 0 {
                    assert!(encoded(&record).is_err());
                    continue;
                }
                let bytes = encoded(&record).unwrap();
                assert_eq!(bytes.len(), if kind == 1 { EDGE_WIDTH } else { WIDTH });
                assert_eq!(read(&mut &bytes[..]).unwrap(), Some(record));
            }
        }
    }

    #[test]
    fn partial_records_unknown_kinds_and_cross_block_records_fail_closed() {
        for kind in 0..=3 {
            let mut record = [0_u8; WIDTH];
            record[16] = kind;
            let bytes = encoded(&record).unwrap();
            for length in 1..bytes.len() {
                assert!(read(&mut &bytes[..length]).is_err());
                assert!(layout(&bytes[..length]).is_err());
            }
            assert_eq!(layout(bytes).unwrap().0, 1);
        }
        let mut invalid = [0_u8; WIDTH];
        invalid[16] = 4;
        assert!(encoded(&invalid).is_err());
        assert!(read(&mut &invalid[..]).is_err());
        assert!(layout(&invalid).is_err());
        assert_eq!(read(&mut &[][..]).unwrap(), None);
    }

    #[test]
    fn mixed_records_have_exact_budget_and_ordered_fences() {
        let mut bytes = Vec::new();
        for i in 0..69_634_u64 {
            let mut record = [0_u8; WIDTH];
            record[..16].copy_from_slice(&u128::from(i + 1).to_be_bytes());
            record[16] = u8::from(i >= 4097);
            if record[16] == 0 {
                record[17..].copy_from_slice(&(i + 1).to_be_bytes());
            }
            bytes.extend_from_slice(encoded(&record).unwrap());
        }
        assert_eq!(bytes.len(), 1_216_554);
        assert_eq!(layout(&bytes).unwrap().0, 69_634);
        let mut input = bytes.as_slice();
        for i in 0..69_634_u64 {
            let record = take(&mut input).unwrap().unwrap();
            assert_eq!(&record[..16], &(u128::from(i) + 1).to_be_bytes());
            assert_eq!(
                u64::from_be_bytes(record[17..].try_into().unwrap()),
                if i < 4097 { i + 1 } else { 0 }
            );
        }
        assert!(input.is_empty());
    }
}
