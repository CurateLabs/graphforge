//! Resident records for one fixed-width partition. Details retain their compact
//! wire bytes; sorting moves offsets instead of padded 272/304-byte arrays.
use super::storage;
use crate::construction_detail_codec::DetailCodec;
use graphforge_core::GfError;

#[cfg(any(test, feature = "test-support"))]
pub(super) mod sort_spike;

pub(super) enum PartitionRecords<const N: usize> {
    Fixed(Vec<[u8; N]>),
    Details { bytes: Vec<u8>, offsets: Vec<usize> },
}

impl<const N: usize> PartitionRecords<N> {
    pub(super) fn new(
        codec: Option<DetailCodec>,
        expected_records: Option<u64>,
        spill_bytes: u64,
    ) -> Result<Self, GfError> {
        let count = expected_records
            .map(usize::try_from)
            .transpose()
            .map_err(storage)?
            .unwrap_or(0);
        if let Some(codec) = codec {
            // Validate the domain before using N - 256 as the length offset.
            codec.validate_size(N, 0, 0).map_err(storage)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(usize::try_from(spill_bytes).map_err(storage)?)
                .map_err(storage)?;
            let mut offsets = Vec::new();
            offsets.try_reserve_exact(count).map_err(storage)?;
            Ok(Self::Details { bytes, offsets })
        } else {
            let mut records = Vec::new();
            records.try_reserve_exact(count).map_err(storage)?;
            Ok(Self::Fixed(records))
        }
    }

    /// The caller decoded this record with the selected codec, validating its
    /// name and zero-initializing the omitted padding.
    pub(super) fn push(&mut self, record: [u8; N]) {
        match self {
            Self::Fixed(records) => records.push(record),
            Self::Details { bytes, offsets } => {
                offsets.push(bytes.len());
                let length = N - 256;
                bytes.extend_from_slice(&record[..length + 1 + usize::from(record[length])]);
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Fixed(records) => records.len(),
            Self::Details { offsets, .. } => offsets.len(),
        }
    }

    pub(super) fn sort(&mut self) {
        match self {
            Self::Fixed(records) => records.sort_unstable(),
            Self::Details { bytes, offsets } => {
                // Compare the whole wire record, retaining the original tie
                // order even for duplicate UUIDs. The length byte precedes the
                // name, so dropping zero padding cannot change this order.
                offsets.sort_unstable_by(|&left, &right| {
                    detail::<N>(bytes, left).cmp(detail::<N>(bytes, right))
                });
            }
        }
    }

    /// Sort using the #1506 spike selector when compiled for tests/test-support.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn sort_selected(&mut self) -> Result<(), GfError> {
        sort_spike::apply(self)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).map(|index| match self {
            Self::Fixed(records) => records[index].as_slice(),
            Self::Details { bytes, offsets } => detail::<N>(bytes, offsets[index]),
        })
    }
}

pub(super) fn detail<const N: usize>(bytes: &[u8], offset: usize) -> &[u8] {
    let length = N - 256;
    &bytes[offset..offset + length + 1 + usize::from(bytes[offset + length])]
}

#[cfg(test)]
mod tests;
