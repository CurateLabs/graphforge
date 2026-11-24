//! Bounded, allocation-free decoding of Parquet page headers.
//!
//! Page admission reads each page header before Parquet decodes the page, to
//! refuse unfunded encodings and to admit page sizes. Parquet 60 no longer
//! exposes its Thrift page-header types, so this module decodes the Thrift
//! compact protocol itself, keeping only the fields admission reads.
//!
//! Decoding never allocates. Every byte comes from the caller's reader, which
//! bounds the header size; binary fields (page statistics) are consumed in
//! fixed-size chunks rather than buffered, so a hostile declared length cannot
//! request memory before the reader runs out. Nesting is bounded, and every
//! skipped value consumes at least one byte, so work is linear in the bytes the
//! reader allows.

use std::io::Read;

/// The Thrift `PageType` of a page header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PageType(pub(crate) i32);

impl PageType {
    pub(crate) const DATA_PAGE: Self = Self(0);
    pub(crate) const DICTIONARY_PAGE: Self = Self(2);
    pub(crate) const DATA_PAGE_V2: Self = Self(3);
}

/// The Thrift `Encoding` a page header declares. The discriminants are the
/// wire values, which `parquet::basic::Encoding` shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PageEncoding(pub(crate) i32);

impl PageEncoding {
    pub(crate) const PLAIN: Self = Self(0);
    pub(crate) const PLAIN_DICTIONARY: Self = Self(2);
    pub(crate) const RLE: Self = Self(3);
    pub(crate) const BIT_PACKED: Self = Self(4);
    pub(crate) const DELTA_BINARY_PACKED: Self = Self(5);
    pub(crate) const RLE_DICTIONARY: Self = Self(8);
    pub(crate) const BYTE_STREAM_SPLIT: Self = Self(9);
}

/// The fields of a version 1 `DataPageHeader` admission reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DataPageHeader {
    pub(crate) num_values: i32,
    pub(crate) encoding: PageEncoding,
    pub(crate) definition_level_encoding: PageEncoding,
    pub(crate) repetition_level_encoding: PageEncoding,
}

/// The fields of a `DictionaryPageHeader` admission reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DictionaryPageHeader {
    pub(crate) num_values: i32,
    pub(crate) encoding: PageEncoding,
}

/// The fields of a `DataPageHeaderV2` admission reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DataPageHeaderV2 {
    pub(crate) num_values: i32,
    pub(crate) encoding: PageEncoding,
    /// Absent means compressed, as the Parquet format defines.
    pub(crate) is_compressed: Option<bool>,
}

/// The fields of a Parquet `PageHeader` admission reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PageHeader {
    pub(crate) type_: PageType,
    pub(crate) uncompressed_page_size: i32,
    pub(crate) compressed_page_size: i32,
    pub(crate) data_page_header: Option<DataPageHeader>,
    pub(crate) dictionary_page_header: Option<DictionaryPageHeader>,
    pub(crate) data_page_header_v2: Option<DataPageHeaderV2>,
}

/// Why a page header could not be decoded.
#[derive(Debug)]
pub(crate) enum HeaderError {
    /// The reader failed, including reaching its bound before the header ended.
    Io(#[allow(dead_code, reason = "kept for diagnostics and asserted by tests")] std::io::Error),
    /// The bytes are not a well-formed page header.
    Malformed,
}

impl From<std::io::Error> for HeaderError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Deepest nesting a page header may use. The format nests two levels
/// (statistics inside a data page header); the margin admits extensions.
const MAX_DEPTH: usize = 16;

// Thrift compact protocol type identifiers.
const STOP: u8 = 0;
const BOOL_TRUE: u8 = 1;
const BOOL_FALSE: u8 = 2;
const BYTE: u8 = 3;
const I16: u8 = 4;
const I32: u8 = 5;
const I64: u8 = 6;
const DOUBLE: u8 = 7;
const BINARY: u8 = 8;
const LIST: u8 = 9;
const SET: u8 = 10;
const MAP: u8 = 11;
const STRUCT: u8 = 12;

impl PageHeader {
    /// Decode one page header from `input`, returning it with the number of
    /// bytes it occupies. `input` must bound how many bytes it yields.
    pub(crate) fn read(input: &mut impl Read) -> Result<(Self, u64), HeaderError> {
        let mut decoder = Decoder { input, consumed: 0 };
        let header = decoder.page_header()?;
        Ok((header, decoder.consumed))
    }
}

struct Decoder<'a, R: Read> {
    input: &'a mut R,
    consumed: u64,
}

/// One field header inside a struct.
struct Field {
    id: i16,
    kind: u8,
}

impl<R: Read> Decoder<'_, R> {
    fn byte(&mut self) -> Result<u8, HeaderError> {
        let mut byte = [0_u8];
        self.input.read_exact(&mut byte)?;
        self.consumed += 1;
        Ok(byte[0])
    }

    fn varint(&mut self, max_bytes: u32) -> Result<u64, HeaderError> {
        let mut value = 0_u64;
        for index in 0..max_bytes {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7f) << (7 * index);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(HeaderError::Malformed)
    }

    fn zigzag(&mut self, max_bytes: u32) -> Result<i64, HeaderError> {
        let raw = self.varint(max_bytes)?;
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }

    fn i32(&mut self) -> Result<i32, HeaderError> {
        i32::try_from(self.zigzag(5)?).map_err(|_| HeaderError::Malformed)
    }

    fn i16(&mut self) -> Result<i16, HeaderError> {
        i16::try_from(self.zigzag(3)?).map_err(|_| HeaderError::Malformed)
    }

    /// Consume `length` bytes without buffering them.
    fn discard(&mut self, mut length: u64) -> Result<(), HeaderError> {
        let mut chunk = [0_u8; 256];
        while length > 0 {
            let take = usize::try_from(length.min(chunk.len() as u64))
                .map_err(|_| HeaderError::Malformed)?;
            self.input.read_exact(&mut chunk[..take])?;
            self.consumed += take as u64;
            length -= take as u64;
        }
        Ok(())
    }

    /// Read the next field header, or `None` at the struct's end.
    fn field(&mut self, last: &mut i16) -> Result<Option<Field>, HeaderError> {
        let byte = self.byte()?;
        if byte == STOP {
            return Ok(None);
        }
        let kind = byte & 0x0f;
        let delta = byte >> 4;
        let id = if delta == 0 {
            self.i16()?
        } else {
            last.checked_add(i16::from(delta))
                .ok_or(HeaderError::Malformed)?
        };
        *last = id;
        Ok(Some(Field { id, kind }))
    }

    fn expect_i32(&mut self, field: &Field) -> Result<i32, HeaderError> {
        if field.kind != I32 {
            return Err(HeaderError::Malformed);
        }
        self.i32()
    }

    fn expect_bool(field: &Field) -> Result<bool, HeaderError> {
        match field.kind {
            BOOL_TRUE => Ok(true),
            BOOL_FALSE => Ok(false),
            _ => Err(HeaderError::Malformed),
        }
    }

    fn expect_struct(field: &Field) -> Result<(), HeaderError> {
        if field.kind == STRUCT {
            Ok(())
        } else {
            Err(HeaderError::Malformed)
        }
    }

    /// Consume one value of compact type `kind`.
    fn skip(&mut self, kind: u8, depth: usize) -> Result<(), HeaderError> {
        if depth > MAX_DEPTH {
            return Err(HeaderError::Malformed);
        }
        match kind {
            // A boolean field carries its value in the field header; inside a
            // collection each element is one byte.
            BOOL_TRUE | BOOL_FALSE => Ok(()),
            BYTE => self.byte().map(drop),
            I16 => self.zigzag(3).map(drop),
            I32 => self.zigzag(5).map(drop),
            I64 => self.zigzag(10).map(drop),
            DOUBLE => self.discard(8),
            BINARY => {
                let length = self.varint(5)?;
                self.discard(length)
            }
            LIST | SET => {
                let header = self.byte()?;
                let element = header & 0x0f;
                let count = match header >> 4 {
                    15 => self.varint(5)?,
                    count => u64::from(count),
                };
                for _ in 0..count {
                    self.skip_element(element, depth + 1)?;
                }
                Ok(())
            }
            MAP => {
                let count = self.varint(5)?;
                if count == 0 {
                    return Ok(());
                }
                let types = self.byte()?;
                for _ in 0..count {
                    self.skip_element(types >> 4, depth + 1)?;
                    self.skip_element(types & 0x0f, depth + 1)?;
                }
                Ok(())
            }
            STRUCT => {
                let mut last = 0;
                while let Some(field) = self.field(&mut last)? {
                    self.skip(field.kind, depth + 1)?;
                }
                Ok(())
            }
            _ => Err(HeaderError::Malformed),
        }
    }

    /// Consume one collection element, where booleans occupy a byte.
    fn skip_element(&mut self, kind: u8, depth: usize) -> Result<(), HeaderError> {
        if matches!(kind, BOOL_TRUE | BOOL_FALSE) {
            return self.byte().map(drop);
        }
        self.skip(kind, depth)
    }

    fn page_header(&mut self) -> Result<PageHeader, HeaderError> {
        let mut type_ = None;
        let mut uncompressed = None;
        let mut compressed = None;
        let mut data = None;
        let mut dictionary = None;
        let mut data_v2 = None;
        let mut last = 0;
        while let Some(field) = self.field(&mut last)? {
            match field.id {
                1 => type_ = Some(PageType(self.expect_i32(&field)?)),
                2 => uncompressed = Some(self.expect_i32(&field)?),
                3 => compressed = Some(self.expect_i32(&field)?),
                5 => {
                    Self::expect_struct(&field)?;
                    data = Some(self.data_page_header()?);
                }
                7 => {
                    Self::expect_struct(&field)?;
                    dictionary = Some(self.dictionary_page_header()?);
                }
                8 => {
                    Self::expect_struct(&field)?;
                    data_v2 = Some(self.data_page_header_v2()?);
                }
                _ => self.skip(field.kind, 1)?,
            }
        }
        Ok(PageHeader {
            type_: type_.ok_or(HeaderError::Malformed)?,
            uncompressed_page_size: uncompressed.ok_or(HeaderError::Malformed)?,
            compressed_page_size: compressed.ok_or(HeaderError::Malformed)?,
            data_page_header: data,
            dictionary_page_header: dictionary,
            data_page_header_v2: data_v2,
        })
    }

    fn data_page_header(&mut self) -> Result<DataPageHeader, HeaderError> {
        let (mut values, mut encoding, mut definition, mut repetition) = (None, None, None, None);
        let mut last = 0;
        while let Some(field) = self.field(&mut last)? {
            match field.id {
                1 => values = Some(self.expect_i32(&field)?),
                2 => encoding = Some(PageEncoding(self.expect_i32(&field)?)),
                3 => definition = Some(PageEncoding(self.expect_i32(&field)?)),
                4 => repetition = Some(PageEncoding(self.expect_i32(&field)?)),
                _ => self.skip(field.kind, 2)?,
            }
        }
        Ok(DataPageHeader {
            num_values: values.ok_or(HeaderError::Malformed)?,
            encoding: encoding.ok_or(HeaderError::Malformed)?,
            definition_level_encoding: definition.ok_or(HeaderError::Malformed)?,
            repetition_level_encoding: repetition.ok_or(HeaderError::Malformed)?,
        })
    }

    fn dictionary_page_header(&mut self) -> Result<DictionaryPageHeader, HeaderError> {
        let (mut values, mut encoding) = (None, None);
        let mut last = 0;
        while let Some(field) = self.field(&mut last)? {
            match field.id {
                1 => values = Some(self.expect_i32(&field)?),
                2 => encoding = Some(PageEncoding(self.expect_i32(&field)?)),
                _ => self.skip(field.kind, 2)?,
            }
        }
        Ok(DictionaryPageHeader {
            num_values: values.ok_or(HeaderError::Malformed)?,
            encoding: encoding.ok_or(HeaderError::Malformed)?,
        })
    }

    fn data_page_header_v2(&mut self) -> Result<DataPageHeaderV2, HeaderError> {
        let (mut values, mut nulls, mut rows, mut encoding) = (None, None, None, None);
        let (mut definition, mut repetition, mut is_compressed) = (None, None, None);
        let mut last = 0;
        while let Some(field) = self.field(&mut last)? {
            match field.id {
                1 => values = Some(self.expect_i32(&field)?),
                2 => nulls = Some(self.expect_i32(&field)?),
                3 => rows = Some(self.expect_i32(&field)?),
                4 => encoding = Some(PageEncoding(self.expect_i32(&field)?)),
                5 => definition = Some(self.expect_i32(&field)?),
                6 => repetition = Some(self.expect_i32(&field)?),
                7 => is_compressed = Some(Self::expect_bool(&field)?),
                _ => self.skip(field.kind, 2)?,
            }
        }
        // Required by the format; admission does not otherwise read them.
        nulls.ok_or(HeaderError::Malformed)?;
        rows.ok_or(HeaderError::Malformed)?;
        definition.ok_or(HeaderError::Malformed)?;
        repetition.ok_or(HeaderError::Malformed)?;
        Ok(DataPageHeaderV2 {
            num_values: values.ok_or(HeaderError::Malformed)?,
            encoding: encoding.ok_or(HeaderError::Malformed)?,
            is_compressed,
        })
    }
}

#[cfg(test)]
mod tests;
