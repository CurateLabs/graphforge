//! The Parquet encodings whose decoder allocation the admission budgets fund.
//!
//! Parquet 58.4's `DELTA_LENGTH_BYTE_ARRAY` decoder allocates a vector from an
//! embedded delta count before it checks the decoded payload lengths, and
//! `DELTA_BYTE_ARRAY` builds on it. Neither is bounded by outer page or footer
//! sizes, so a page that uses one is refused before any decode instead of being
//! charged a larger envelope. Admission points share this one profile:
//! the actual-page walk in `property_overlay::parquet_budget` and the footer
//! envelope in `catalog::filtered_parquet::decode_budget`.

use parquet::basic::{Encoding as FooterEncoding, Type};

/// Thrift wire encoding, as carried by page headers.
#[allow(
    deprecated,
    reason = "Parquet 58 exposes raw page headers only through parquet::format"
)]
pub(crate) type PageEncoding = parquet::format::Encoding;

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
