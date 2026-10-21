//! The Parquet encodings whose decoder allocation the admission budgets fund.
//!
//! Parquet 58.4's `DELTA_LENGTH_BYTE_ARRAY` decoder allocates a vector from an
//! embedded delta count before it checks the decoded payload lengths, and
//! `DELTA_BYTE_ARRAY` builds on it. Neither is bounded by outer page or footer
//! sizes, so a page that uses one is refused before any decode instead of being
//! charged a larger envelope.
//!
//! One profile, enforced where external Parquet bytes become project data and
//! where a page is about to be decoded:
//!
//! * [`admit_pages`] walks the *actual* page headers of a Parquet object and
//!   never trusts the footer's declared encodings. Like the Parquet reader, it
//!   requires only the trailing `PAR1` footer magic. Portable verification and
//!   import call it for every Parquet entry of a package, so every later read
//!   of those checksum-protected files sees only funded encodings.
//! * `property_overlay::parquet_budget` applies [`page_encodings_are_funded`]
//!   to each page header it already walks for resource admission.
//! * `catalog::filtered_parquet::decode_budget` applies
//!   [`declared_encoding_is_funded`] as a cheap secondary check on footers of
//!   already-admitted project files.
//!
//! This module depends only on the `parquet` and `thrift` crates.

use std::io::Cursor;

use parquet::basic::{Encoding as FooterEncoding, Type};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::ChunkReader;
use parquet::thrift::TSerializable;

/// Thrift wire encoding, as carried by page headers.
#[allow(
    deprecated,
    reason = "Parquet 58 exposes raw page headers only through parquet::format"
)]
pub(crate) type PageEncoding = parquet::format::Encoding;
#[allow(
    deprecated,
    reason = "Parquet 58 exposes raw page headers only through parquet::format"
)]
pub(crate) type PageHeader = parquet::format::PageHeader;

/// Largest footer a page walk will buffer. The decoders read the same bytes.
const MAX_FOOTER_BYTES: u64 = 64 * 1024 * 1024;
/// A page header is a short Thrift struct; most fit the first window.
const FIRST_HEADER_WINDOW: usize = 4 * 1024;
/// Page headers larger than this are malformed, as in resource admission.
const MAX_HEADER_BYTES: usize = 64 * 1024;

/// Why a Parquet object cannot be admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageAdmissionError {
    /// A page uses an encoding outside the funded profile.
    Unfunded,
    /// The object is not a Parquet file the decoders could read either.
    Malformed(&'static str),
}

/// Convert a footer-declared encoding to its page-header form.
#[allow(
    deprecated,
    reason = "Parquet 58 exposes raw page headers only through parquet::format"
)]
pub(crate) fn from_footer(encoding: FooterEncoding) -> PageEncoding {
    // `basic::Encoding` discriminants are the Thrift wire values.
    parquet::format::Encoding(encoding as i32)
}

/// Whether a data (or dictionary) page may use `encoding` for its values.
#[allow(deprecated, reason = "Parquet 58 page encodings are format::Encoding")]
pub(crate) fn value_encoding_is_funded(encoding: PageEncoding, physical: Type) -> bool {
    match encoding {
        PageEncoding::PLAIN | PageEncoding::PLAIN_DICTIONARY | PageEncoding::RLE_DICTIONARY => true,
        PageEncoding::RLE => physical == Type::BOOLEAN,
        PageEncoding::DELTA_BINARY_PACKED => matches!(physical, Type::INT32 | Type::INT64),
        PageEncoding::BYTE_STREAM_SPLIT => matches!(
            physical,
            Type::INT32 | Type::INT64 | Type::FLOAT | Type::DOUBLE | Type::FIXED_LEN_BYTE_ARRAY
        ),
        _ => false,
    }
}

/// Whether definition or repetition levels may use `encoding`.
#[allow(deprecated, reason = "Parquet 58 page encodings are format::Encoding")]
pub(crate) fn level_encoding_is_funded(encoding: PageEncoding) -> bool {
    matches!(encoding, PageEncoding::RLE | PageEncoding::BIT_PACKED)
}

/// Whether a column chunk's footer-declared encoding is inside the profile.
/// The footer list mixes value and level encodings, so either qualifies.
pub(crate) fn declared_encoding_is_funded(encoding: FooterEncoding, physical: Type) -> bool {
    let encoding = from_footer(encoding);
    value_encoding_is_funded(encoding, physical) || level_encoding_is_funded(encoding)
}

/// Whether every encoding one page header declares is funded: the dictionary
/// page's, the data page's value encoding, and, for version 1 pages, both
/// level encodings. Version 2 pages fix their levels to RLE.
#[allow(deprecated, reason = "Parquet 58 page headers are format::PageHeader")]
pub(crate) fn page_encodings_are_funded(header: &PageHeader, physical: Type) -> bool {
    match header.type_ {
        parquet::format::PageType::DICTIONARY_PAGE => header
            .dictionary_page_header
            .as_ref()
            .is_none_or(|page| value_encoding_is_funded(page.encoding, physical)),
        parquet::format::PageType::DATA_PAGE => {
            header.data_page_header.as_ref().is_none_or(|page| {
                value_encoding_is_funded(page.encoding, physical)
                    && level_encoding_is_funded(page.definition_level_encoding)
                    && level_encoding_is_funded(page.repetition_level_encoding)
            })
        }
        parquet::format::PageType::DATA_PAGE_V2 => header
            .data_page_header_v2
            .as_ref()
            .is_none_or(|page| value_encoding_is_funded(page.encoding, physical)),
        _ => true,
    }
}

/// Walk the actual page headers of a Parquet object and refuse any encoding
/// outside the profile, before a single page body is decoded.
///
/// Only the footer and the page headers are read; memory is bounded by the
/// footer and one header window. The footer's declared encodings are ignored.
pub(crate) fn admit_pages<R: ChunkReader>(source: &R) -> Result<(), PageAdmissionError> {
    let length = source.len();
    let malformed = PageAdmissionError::Malformed;
    if length < 8 {
        return Err(malformed("object is too short to be Parquet"));
    }
    let read = |start: u64, bytes: u64| {
        usize::try_from(bytes)
            .ok()
            .and_then(|bytes| source.get_bytes(start, bytes).ok())
            .ok_or(malformed("object is unreadable"))
    };
    let tail = read(length - 8, 8)?;
    if &tail[4..] != b"PAR1" {
        return Err(malformed("object lacks the Parquet footer magic"));
    }
    let footer = u64::from(u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]));
    if footer > MAX_FOOTER_BYTES || footer + 8 > length {
        return Err(malformed("Parquet footer exceeds its bound"));
    }
    let metadata = ParquetMetaDataReader::decode_metadata(&read(length - 8 - footer, footer)?)
        .map_err(|_| malformed("Parquet footer is undecodable"))?;
    admit_pages_with_metadata(&metadata, source, length - 8 - footer)
}

/// [`admit_pages`] for a footer the caller already decoded. `pages_end` is the
/// offset where the footer begins; every column chunk must end at or before it.
#[allow(deprecated, reason = "Parquet 58 page headers are format::PageHeader")]
pub(crate) fn admit_pages_with_metadata<R: ChunkReader>(
    metadata: &ParquetMetaData,
    source: &R,
    pages_end: u64,
) -> Result<(), PageAdmissionError> {
    let malformed = PageAdmissionError::Malformed;
    for group in metadata.row_groups() {
        for column in group.columns() {
            let offset = |value: i64| {
                u64::try_from(value).map_err(|_| malformed("column chunk has a negative offset"))
            };
            let data = offset(column.data_page_offset())?;
            let start = column
                .dictionary_page_offset()
                .map(offset)
                .transpose()?
                .map_or(data, |dictionary| dictionary.min(data));
            let end = offset(column.compressed_size())
                .ok()
                .and_then(|size| start.checked_add(size))
                .filter(|end| *end <= pages_end)
                .ok_or(malformed("column chunk escapes the object"))?;
            let physical = column.column_descr().physical_type();
            let mut position = start;
            while position < end {
                let (header, header_bytes) = read_header(source, position, end)?;
                let body = u64::try_from(header.compressed_page_size)
                    .map_err(|_| malformed("page has a negative compressed size"))?;
                if !page_encodings_are_funded(&header, physical) {
                    return Err(PageAdmissionError::Unfunded);
                }
                position = position
                    .checked_add(header_bytes)
                    .and_then(|next| next.checked_add(body))
                    .filter(|next| *next <= end)
                    .ok_or(malformed("page escapes its column chunk"))?;
            }
        }
    }
    Ok(())
}

/// Parse one page header at `position`, reading a small window first.
fn read_header<R: ChunkReader>(
    source: &R,
    position: u64,
    end: u64,
) -> Result<(PageHeader, u64), PageAdmissionError> {
    let malformed = PageAdmissionError::Malformed;
    let available = usize::try_from(end - position).unwrap_or(usize::MAX);
    for window in [FIRST_HEADER_WINDOW, MAX_HEADER_BYTES] {
        let window = window.min(available);
        let bytes = source
            .get_bytes(position, window)
            .map_err(|_| malformed("page header is unreadable"))?;
        let mut cursor = Cursor::new(bytes.as_ref());
        let mut protocol = thrift::protocol::TCompactInputProtocol::new(&mut cursor);
        if let Ok(header) = PageHeader::read_from_in_protocol(&mut protocol) {
            drop(protocol);
            let consumed = cursor.position();
            if consumed == 0 {
                break;
            }
            return Ok((header, consumed));
        }
        if window == available {
            break;
        }
    }
    Err(malformed("page header is malformed or oversized"))
}

#[cfg(test)]
mod tests;
