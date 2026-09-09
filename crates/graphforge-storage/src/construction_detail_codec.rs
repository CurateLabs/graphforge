//! Bounded private detail records. Version six retains its padded wire layout.
use std::io::{self, Read};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DetailCodec {
    Legacy,
    Compact,
}

impl DetailCodec {
    pub(crate) fn from_version(version: u32) -> io::Result<Self> {
        match version {
            6 => Ok(Self::Legacy),
            7 => Ok(Self::Compact),
            _ => Err(invalid("unsupported construction detail version")),
        }
    }

    pub(crate) fn validate_size(self, width: usize, rows: u64, bytes: u64) -> io::Result<()> {
        if !matches!(width, 272 | 304) {
            return Err(invalid("invalid construction detail record domain"));
        }
        let minimum_width = match self {
            Self::Legacy => width,
            Self::Compact => width - 254,
        };
        let minimum = rows
            .checked_mul(minimum_width as u64)
            .ok_or_else(|| invalid("detail byte bound overflow"))?;
        let maximum = rows
            .checked_mul(width as u64)
            .ok_or_else(|| invalid("detail byte bound overflow"))?;
        if bytes < minimum || bytes > maximum {
            return Err(invalid("detail bytes disagree with bounded row count"));
        }
        Ok(())
    }

    pub(crate) fn bytes<const N: usize>(self, record: &[u8; N]) -> io::Result<&[u8]> {
        let prefix = prefix::<N>()?;
        let length = usize::from(record[prefix]);
        if length == 0 || record[prefix + 1 + length..].iter().any(|byte| *byte != 0) {
            return Err(invalid("invalid construction detail name or padding"));
        }
        std::str::from_utf8(&record[prefix + 1..prefix + 1 + length])
            .map_err(|_| invalid("construction detail name is not UTF-8"))?;
        Ok(match self {
            Self::Legacy => record,
            Self::Compact => &record[..prefix + 1 + length],
        })
    }

    /// Return a bounded padded in-memory record, preserving existing consumers.
    pub(crate) fn read<const N: usize>(
        self,
        reader: &mut impl Read,
    ) -> io::Result<Option<[u8; N]>> {
        let prefix = prefix::<N>()?;
        let mut record = [0; N];
        loop {
            match reader.read(&mut record[..1]) {
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        reader.read_exact(&mut record[1..=prefix])?;
        let length = usize::from(record[prefix]);
        if length == 0 {
            return Err(invalid("empty construction detail name"));
        }
        let end = match self {
            Self::Legacy => N,
            Self::Compact => prefix + 1 + length,
        };
        reader.read_exact(&mut record[prefix + 1..end])?;
        self.bytes(&record)?;
        Ok(Some(record))
    }
}

fn prefix<const N: usize>() -> io::Result<usize> {
    match N {
        272 => Ok(16),
        304 => Ok(48),
        _ => Err(invalid("invalid construction detail record domain")),
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<const N: usize>(name: &str) {
        let prefix = prefix::<N>().unwrap();
        let mut record = [0; N];
        for (index, byte) in record[..prefix].iter_mut().enumerate() {
            *byte = u8::try_from(index + 1).unwrap();
        }
        record[prefix] = u8::try_from(name.len()).unwrap();
        record[prefix + 1..prefix + 1 + name.len()].copy_from_slice(name.as_bytes());
        let mut golden = record[..prefix + 1].to_vec();
        golden.extend_from_slice(name.as_bytes());
        assert_eq!(DetailCodec::Compact.bytes(&record).unwrap(), golden);
        assert_eq!(DetailCodec::Legacy.bytes(&record).unwrap(), record);
        for codec in [DetailCodec::Legacy, DetailCodec::Compact] {
            let bytes = codec.bytes(&record).unwrap();
            assert_eq!(codec.read::<N>(&mut &*bytes).unwrap(), Some(record));
            for length in 1..bytes.len() {
                assert!(codec.read::<N>(&mut &bytes[..length]).is_err());
            }
            assert_eq!(codec.read::<N>(&mut &[][..]).unwrap(), None);
        }
    }

    #[test]
    fn detail_codec_golden_roundtrips_and_truncation() {
        for name in [
            "a".to_owned(),
            "EDGE".to_owned(),
            "x".repeat(255),
            format!("{}a", "é".repeat(127)),
        ] {
            roundtrip::<272>(&name);
            roundtrip::<304>(&name);
        }
    }

    #[test]
    fn detail_codec_streaming_partitions_and_invalid_order() {
        for width in [272, 304] {
            for codec in [DetailCodec::Legacy, DetailCodec::Compact] {
                let prefix = width - 256;
                let mut records = Vec::new();
                for id in 1..=3_u8 {
                    let mut record = vec![0_u8; width];
                    record[15] = id;
                    record[prefix] = 4;
                    record[prefix + 1..prefix + 5].copy_from_slice("éé".as_bytes());
                    if codec == DetailCodec::Compact {
                        record.truncate(prefix + 5);
                    }
                    records.push(record);
                }
                let wire = records.concat();
                for split in 0..=wire.len() {
                    let mut validator = DetailValidator::new(codec, width).unwrap();
                    validator.consume(&wire[..split]).unwrap();
                    validator.consume(&wire[split..]).unwrap();
                    assert_eq!(validator.finish().unwrap(), 3);
                }
                let mut validator = DetailValidator::new(codec, width).unwrap();
                for byte in &wire {
                    validator.consume(std::slice::from_ref(byte)).unwrap();
                }
                assert_eq!(validator.finish().unwrap(), 3);
                let mut invalid_utf8 = wire.clone();
                invalid_utf8[prefix + 1] = 255;
                let cases = [
                    [records[0].clone(), records[0].clone()].concat(),
                    [records[1].clone(), records[0].clone()].concat(),
                    invalid_utf8,
                    wire[..wire.len() - 1].to_vec(),
                ];
                for malformed in cases {
                    for split in 0..=malformed.len() {
                        let mut validator = DetailValidator::new(codec, width).unwrap();
                        let result = validator
                            .consume(&malformed[..split])
                            .and_then(|()| validator.consume(&malformed[split..]))
                            .and_then(|()| validator.finish());
                        assert!(
                            result.is_err(),
                            "codec={codec:?} width={width} split={split}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn detail_codec_rejects_invalid_names_padding_and_versions() {
        let mut record = [0; 304];
        assert!(DetailCodec::Compact.bytes(&record).is_err());
        record[48] = 1;
        record[49] = 255;
        assert!(DetailCodec::Compact.bytes(&record).is_err());
        record[49] = b'a';
        record[303] = 1;
        assert!(DetailCodec::Legacy.bytes(&record).is_err());
        assert!(DetailCodec::Compact.bytes(&record).is_err());
        assert_eq!(DetailCodec::from_version(6).unwrap(), DetailCodec::Legacy);
        assert_eq!(DetailCodec::from_version(7).unwrap(), DetailCodec::Compact);
        assert!(DetailCodec::from_version(5).is_err());
        assert!(DetailCodec::from_version(8).is_err());
    }
}

/// Incremental validation retains one bounded record and its preceding UUID.
/// Input block sizes do not change parsing or memory bounds.
pub(crate) struct DetailValidator {
    codec: DetailCodec,
    width: usize,
    record: [u8; 304],
    filled: usize,
    previous: Option<[u8; 16]>,
    records: u64,
}

impl DetailValidator {
    pub(crate) fn new(codec: DetailCodec, width: usize) -> io::Result<Self> {
        if !matches!(width, 272 | 304) {
            return Err(invalid("invalid construction detail record domain"));
        }
        Ok(Self {
            codec,
            width,
            record: [0; 304],
            filled: 0,
            previous: None,
            records: 0,
        })
    }

    pub(crate) fn consume(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        let prefix = self.width - 256;
        while !bytes.is_empty() {
            let target = if self.filled <= prefix {
                prefix + 1
            } else {
                let length = usize::from(self.record[prefix]);
                if length == 0 {
                    return Err(invalid("empty construction detail name"));
                }
                match self.codec {
                    DetailCodec::Legacy => self.width,
                    DetailCodec::Compact => prefix + 1 + length,
                }
            };
            let count = (target - self.filled).min(bytes.len());
            self.record[self.filled..self.filled + count].copy_from_slice(&bytes[..count]);
            self.filled += count;
            bytes = &bytes[count..];
            if self.filled == target && target > prefix + 1 {
                if self.width == 272 {
                    let record: &[u8; 272] = self.record[..272].try_into().expect("fixed domain");
                    self.codec.bytes(record)?;
                } else {
                    self.codec.bytes(&self.record)?;
                }
                let uuid: [u8; 16] = self.record[..16].try_into().expect("UUID prefix");
                if self.previous.is_some_and(|previous| previous >= uuid) {
                    return Err(invalid(
                        "construction detail UUIDs are not strictly ordered",
                    ));
                }
                self.previous = Some(uuid);
                self.records = self
                    .records
                    .checked_add(1)
                    .ok_or_else(|| invalid("construction detail row count overflow"))?;
                self.record.fill(0);
                self.filled = 0;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(&self) -> io::Result<u64> {
        if self.filled != 0 {
            return Err(invalid("truncated construction detail record"));
        }
        Ok(self.records)
    }
}
