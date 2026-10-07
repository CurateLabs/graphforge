//! Canonical local PAX extended headers for portable-v2 bundles.
//!
//! A bundle entry carries exactly one local PAX header when its path does not
//! fit the ustar `name`/`prefix` split or its length exceeds the 11-digit octal
//! ustar `size` field (8 GiB - 1). That header always holds a `path` record
//! and, only for an oversized entry, a following POSIX.1-2001 `size` record.
//! The regular header's ustar `size` field is then zero and the record is
//! authoritative. Writer and readers share this one encoding; readers refuse
//! every other byte form, including a `size` record the ustar field could have
//! carried, because a canonical bundle has exactly one encoding per entry.

use super::{PortableV2Error, PortableV2ErrorCode};

/// Largest length the 11-digit octal ustar `size` field can carry.
pub(crate) const USTAR_MAX_ENTRY_BYTES: u64 = 0o77_777_777_777;

/// Bound on local PAX header bytes beyond the path itself: the `path` record's
/// length digits, keyword and delimiters, plus one maximal `size` record
/// (`29 size=<20 digits>\n`, 29 bytes).
pub(crate) const PAX_RECORD_OVERHEAD_BYTES: usize = 64;

/// The records of one canonical local PAX header.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PaxHeader {
    pub(crate) path: String,
    pub(crate) size: Option<u64>,
}

/// Largest entry length the bundle writes in the ustar `size` field.
///
/// Tests lower it per thread to exercise the PAX `size` path without writing
/// gigabytes; production always uses [`USTAR_MAX_ENTRY_BYTES`].
pub(crate) fn ustar_size_limit() -> u64 {
    #[cfg(test)]
    if let Some(limit) = test_seam::LIMIT.with(std::cell::Cell::get) {
        return limit;
    }
    USTAR_MAX_ENTRY_BYTES
}

/// Encode one record with the POSIX decimal record-length rule: the leading
/// length counts its own digits, the space, `keyword=value`, and the newline.
pub(crate) fn record(keyword: &str, value: &str) -> String {
    let body = format!(" {keyword}={value}\n");
    let mut digits = 1;
    loop {
        let length = digits + body.len();
        let actual_digits = length.to_string().len();
        if actual_digits == digits {
            return format!("{length}{body}");
        }
        digits = actual_digits;
    }
}

/// Encode the canonical local PAX header data for one entry.
pub(crate) fn encode(path: &str, size: u64) -> String {
    let mut data = record("path", path);
    if size > ustar_size_limit() {
        data.push_str(&record("size", &size.to_string()));
    }
    data
}

/// Parse canonical local PAX header data: one `path` record, optionally
/// followed by one `size` record for a length the ustar field cannot carry.
pub(crate) fn parse(text: &str) -> Result<PaxHeader, PortableV2Error> {
    let (path, rest) = next_record(text, "path")?;
    if rest.is_empty() {
        return Ok(PaxHeader {
            path: path.into(),
            size: None,
        });
    }
    let (value, rest) = next_record(rest, "size")?;
    if !rest.is_empty() {
        return Err(invalid("unsupported PAX record"));
    }
    let size = value
        .parse::<u64>()
        .ok()
        .filter(|size| value.bytes().all(|b| b.is_ascii_digit()) && size.to_string() == value)
        .ok_or_else(|| invalid("PAX size is not a canonical decimal"))?;
    if size <= ustar_size_limit() {
        return Err(invalid("PAX size fits the ustar size field"));
    }
    Ok(PaxHeader {
        path: path.into(),
        size: Some(size),
    })
}

/// Resolve a regular entry's length from its ustar `size` field and the
/// preceding PAX `size` record, refusing any contradictory or non-canonical
/// pairing rather than choosing one.
pub(crate) fn entry_size(field: u64, pax_size: Option<u64>) -> Result<u64, PortableV2Error> {
    match pax_size {
        Some(size) if field == 0 => Ok(size),
        Some(_) => Err(invalid("PAX size contradicts the ustar size field")),
        None if field > ustar_size_limit() => Err(invalid("entry size requires a PAX size record")),
        None => Ok(field),
    }
}

fn next_record<'a>(text: &'a str, keyword: &str) -> Result<(&'a str, &'a str), PortableV2Error> {
    let space = text.find(' ').ok_or_else(|| invalid("PAX record"))?;
    let digits = &text[..space];
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid("PAX length"));
    }
    let length = digits.parse::<usize>().map_err(|_| invalid("PAX length"))?;
    let (record_text, rest) = text
        .split_at_checked(length)
        .ok_or_else(|| invalid("PAX length"))?;
    let value = record_text
        .strip_suffix('\n')
        .and_then(|record| record.get(space + 1..))
        .and_then(|body| body.strip_prefix(keyword))
        .and_then(|body| body.strip_prefix('='))
        .ok_or_else(|| invalid("PAX record"))?;
    if record(keyword, value) != record_text {
        return Err(invalid("non-canonical PAX record"));
    }
    Ok((value, rest))
}

fn invalid(detail: &'static str) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, detail)
}

/// Per-thread override of the ustar size limit, for tests only.
#[cfg(test)]
pub(crate) mod test_seam {
    use std::cell::Cell;

    thread_local! {
        pub(super) static LIMIT: Cell<Option<u64>> = const { Cell::new(None) };
    }

    /// Restores the production limit when dropped.
    pub(crate) struct UstarSizeLimit(());

    impl Drop for UstarSizeLimit {
        fn drop(&mut self) {
            LIMIT.with(|limit| limit.set(None));
        }
    }

    /// Lower the ustar size limit on this thread until the guard drops.
    pub(crate) fn lower_ustar_size_limit(limit: u64) -> UstarSizeLimit {
        LIMIT.with(|cell| cell.set(Some(limit)));
        UstarSizeLimit(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_size_appears_only_above_the_ustar_field() {
        let path = "data/components/graph-data/graph-tree/graph-objects/sha256/abc";
        assert_eq!(
            parse(&encode(path, USTAR_MAX_ENTRY_BYTES)).unwrap(),
            PaxHeader {
                path: path.into(),
                size: None
            }
        );
        let large = USTAR_MAX_ENTRY_BYTES + 1;
        let data = encode(path, large);
        assert!(data.ends_with(&format!("19 size={large}\n")));
        assert_eq!(
            parse(&data).unwrap(),
            PaxHeader {
                path: path.into(),
                size: Some(large)
            }
        );
        let maximal = record("size", &u64::MAX.to_string());
        assert_eq!(maximal.len(), 29);
        assert!(maximal.len() + 12 <= PAX_RECORD_OVERHEAD_BYTES);
    }

    #[test]
    fn malformed_and_contradictory_size_records_are_refused() {
        let path = record("path", "data/x");
        let large = USTAR_MAX_ENTRY_BYTES + 1;
        let refused = [
            // A size the ustar field can carry is not canonical as a record.
            format!(
                "{path}{}",
                record("size", &USTAR_MAX_ENTRY_BYTES.to_string())
            ),
            format!("{path}{}", record("size", "0")),
            // Leading zeros, signs, non-digits and overflow.
            format!("{path}{}", record("size", &format!("0{large}"))),
            format!("{path}{}", record("size", &format!("+{large}"))),
            format!("{path}{}", record("size", "8589934592x")),
            format!("{path}{}", record("size", "18446744073709551616")),
            format!("{path}{}", record("size", "")),
            // Record order, duplicates and other keywords.
            format!("{}{path}", record("size", &large.to_string())),
            format!(
                "{path}{}{}",
                record("size", &large.to_string()),
                record("size", &large.to_string())
            ),
            format!("{path}{}", record("mtime", "0")),
            // Wrong declared record lengths.
            format!("{path}30 size={large}\n"),
            format!("{path}28 size={large}\n"),
            format!("{path}028 size={large}\n"),
            format!("{path}19 size={large}"),
        ];
        for text in refused {
            let error = parse(&text).unwrap_err();
            assert_eq!(
                error.code,
                PortableV2ErrorCode::InvalidStructure,
                "{text:?}"
            );
        }
        assert!(entry_size(0, Some(large)).is_ok());
        for (field, pax) in [(1, Some(large)), (large, Some(large)), (large, None)] {
            assert_eq!(
                entry_size(field, pax).unwrap_err().code,
                PortableV2ErrorCode::InvalidStructure
            );
        }
        assert_eq!(entry_size(7, None).unwrap(), 7);
    }

    #[test]
    fn the_test_seam_is_scoped_to_its_guard() {
        {
            let _limit = test_seam::lower_ustar_size_limit(8);
            assert_eq!(ustar_size_limit(), 8);
            assert!(encode("data/x", 9).contains(" size=9\n"));
            assert!(parse(&encode("data/x", 9)).is_ok());
        }
        assert_eq!(ustar_size_limit(), USTAR_MAX_ENTRY_BYTES);
        assert!(!encode("data/x", 9).contains("size="));
    }
}
