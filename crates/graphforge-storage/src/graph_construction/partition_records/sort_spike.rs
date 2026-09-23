//! Test-support-only partition sort experiment (#1506).
//!
//! `GF_SHAPE_SORT_SPIKE` selects who orders a materialized construction
//! partition. Every mode sorts the same records with the same comparator (the
//! whole wire record, bytewise), so the sorted bytes are identical; only the
//! sorting machinery and its transient memory differ.
//!
//! | Mode | Fixed partitions | Compact details | Arrow row partitions |
//! | --- | --- | --- | --- |
//! | unset / `baseline` | `sort_unstable` | offset `sort_unstable_by` | key `sort_unstable_by_key` |
//! | `arrow-fixed` (#1511 slice) | Arrow `sort_to_indices` | baseline | baseline |
//! | `arrow` | Arrow `sort_to_indices` | Arrow `sort_to_indices` over a zero-copy `LargeBinary` view | Arrow `sort_to_indices` on the key column |
//! | `datafusion` | DataFusion `SortExec` | `SortExec` over the same view | `SortExec` on the key column |
//!
//! The candidates return a permutation; the GraphForge representation is then
//! reordered by it, so compact details still move offsets rather than padded
//! records. Invalid modes fail closed. Nothing here is selected by default.
//! Protocol: `docs/development/construction-reuse-inventory-protocol-1505.md`.
use super::{PartitionRecords, storage};
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, LargeBinaryArray, UInt32Array};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::{SortOptions, sort_to_indices};
use graphforge_core::GfError;
use std::sync::Arc;

pub(super) fn apply<const N: usize>(records: &mut PartitionRecords<N>) -> Result<(), GfError> {
    apply_mode(records, mode()?)
}

/// Permutation for an Arrow row partition's key column, or `None` when the
/// baseline comparator is selected.
pub(in crate::graph_construction) fn row_order(
    keys: &FixedSizeBinaryArray,
) -> Result<Option<UInt32Array>, GfError> {
    row_order_for_mode(keys, mode()?)
}

pub(in crate::graph_construction) fn row_order_for_mode(
    keys: &FixedSizeBinaryArray,
    mode: Mode,
) -> Result<Option<UInt32Array>, GfError> {
    match mode {
        Mode::Baseline | Mode::ArrowFixed => Ok(None),
        Mode::Arrow => arrow_indices(keys).map(Some),
        Mode::DataFusion => {
            super::super::sort_partition_spike::datafusion_sort_indices(Arc::new(keys.clone()))
                .map(Some)
        }
    }
}

pub(in crate::graph_construction) fn apply_mode<const N: usize>(
    records: &mut PartitionRecords<N>,
    mode: Mode,
) -> Result<(), GfError> {
    if records.len() <= 1 {
        return Ok(());
    }
    match (mode, &mut *records) {
        (Mode::Baseline, _) | (Mode::ArrowFixed, PartitionRecords::Details { .. }) => {
            records.sort();
            Ok(())
        }
        (Mode::Arrow | Mode::ArrowFixed, PartitionRecords::Fixed(fixed)) => {
            let indices = arrow_indices(&fixed_array(fixed)?)?;
            permute_fixed(fixed, &indices)
        }
        (Mode::DataFusion, PartitionRecords::Fixed(fixed)) => {
            let indices = super::super::sort_partition_spike::datafusion_sort_indices(Arc::new(
                fixed_array(fixed)?,
            ))?;
            permute_fixed(fixed, &indices)
        }
        (Mode::Arrow | Mode::DataFusion, PartitionRecords::Details { bytes, offsets }) => {
            sort_details(bytes, offsets, N, mode)
        }
    }
}

pub(in crate::graph_construction) fn mode() -> Result<Mode, GfError> {
    match std::env::var("GF_SHAPE_SORT_SPIKE") {
        Err(std::env::VarError::NotPresent) => Ok(Mode::Baseline),
        Ok(mode) => Mode::parse(&mode),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(storage("invalid shape sort experiment mode"))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::graph_construction) enum Mode {
    Baseline,
    ArrowFixed,
    Arrow,
    DataFusion,
}

impl Mode {
    pub(in crate::graph_construction) const ALL: [Self; 4] = [
        Self::Baseline,
        Self::ArrowFixed,
        Self::Arrow,
        Self::DataFusion,
    ];

    pub(in crate::graph_construction) fn parse(value: &str) -> Result<Self, GfError> {
        match value {
            "baseline" => Ok(Self::Baseline),
            "arrow-fixed" => Ok(Self::ArrowFixed),
            "arrow" => Ok(Self::Arrow),
            "datafusion" => Ok(Self::DataFusion),
            _ => Err(storage("invalid shape sort experiment mode")),
        }
    }

    pub(in crate::graph_construction) const fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::ArrowFixed => "arrow-fixed",
            Self::Arrow => "arrow",
            Self::DataFusion => "datafusion",
        }
    }
}

const ASCENDING: SortOptions = SortOptions {
    descending: false,
    nulls_first: true,
};

fn arrow_indices(keys: &dyn Array) -> Result<UInt32Array, GfError> {
    sort_to_indices(keys, Some(ASCENDING), None).map_err(storage)
}

/// Copies the records into one Arrow buffer: `Vec<[u8; N]>` has no safe
/// zero-copy conversion to an Arrow `Buffer`.
fn fixed_array<const N: usize>(records: &[[u8; N]]) -> Result<FixedSizeBinaryArray, GfError> {
    let width = i32::try_from(N).map_err(storage)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(records.len() * N)
        .map_err(storage)?;
    for record in records {
        values.extend_from_slice(record);
    }
    FixedSizeBinaryArray::try_new(width, Buffer::from_vec(values), None).map_err(storage)
}

fn checked_indices(indices: &UInt32Array, len: usize) -> Result<(), GfError> {
    if indices.len() != len || indices.null_count() != 0 {
        return Err(storage("sort experiment permutation has the wrong shape"));
    }
    Ok(())
}

/// Gather into a fresh vector: a second resident copy of the partition, which
/// the baseline in-place sort never holds.
fn permute_fixed<const N: usize>(
    records: &mut Vec<[u8; N]>,
    indices: &UInt32Array,
) -> Result<(), GfError> {
    checked_indices(indices, records.len())?;
    let mut sorted = Vec::new();
    sorted.try_reserve_exact(records.len()).map_err(storage)?;
    for index in indices.values() {
        sorted.push(
            *records
                .get(*index as usize)
                .ok_or_else(|| storage("sort experiment index is out of range"))?,
        );
    }
    *records = sorted;
    Ok(())
}

/// Compact detail records are contiguous in push order, so the wire bytes
/// plus one terminal offset are exactly a `LargeBinary` array. The wire buffer
/// moves into Arrow and back without copying; only offsets are widened and
/// permuted.
fn sort_details(
    bytes: &mut Vec<u8>,
    offsets: &mut Vec<usize>,
    width: usize,
    mode: Mode,
) -> Result<(), GfError> {
    let mut arrow_offsets = Vec::new();
    arrow_offsets
        .try_reserve_exact(offsets.len() + 1)
        .map_err(storage)?;
    for (index, offset) in offsets.iter().enumerate() {
        // Fail closed if a caller ever breaks the contiguity this view relies on.
        let end = offsets.get(index + 1).copied().unwrap_or(bytes.len());
        if end.checked_sub(*offset) != Some(detail_len(bytes, *offset, width)?) {
            return Err(storage("compact detail records are not contiguous"));
        }
        arrow_offsets.push(i64::try_from(*offset).map_err(storage)?);
    }
    arrow_offsets.push(i64::try_from(bytes.len()).map_err(storage)?);
    let values = Buffer::from_vec(std::mem::take(bytes));
    let array = LargeBinaryArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from(arrow_offsets)),
        values,
        None,
    )
    .map_err(storage)?;
    let indices = match mode {
        Mode::DataFusion => super::super::sort_partition_spike::datafusion_sort_indices(Arc::new(
            array.clone(),
        )
            as ArrayRef),
        _ => arrow_indices(&array),
    };
    let (_, values, _) = array.into_parts();
    *bytes = values
        .into_vec::<u8>()
        .map_err(|_| storage("arrow detail sort retained the wire buffer"))?;
    let indices = indices?;
    checked_indices(&indices, offsets.len())?;
    let mut sorted = Vec::new();
    sorted.try_reserve_exact(offsets.len()).map_err(storage)?;
    for index in indices.values() {
        sorted.push(
            *offsets
                .get(*index as usize)
                .ok_or_else(|| storage("sort experiment index is out of range"))?,
        );
    }
    *offsets = sorted;
    Ok(())
}

fn detail_len(bytes: &[u8], offset: usize, width: usize) -> Result<usize, GfError> {
    let length = width - 256;
    let name = *bytes
        .get(offset + length)
        .ok_or_else(|| storage("compact detail record is truncated"))?;
    Ok(length + 1 + usize::from(name))
}

#[cfg(test)]
mod tests {
    use super::super::detail;
    use super::*;
    use crate::construction_detail_codec::DetailCodec;

    fn fixed_records(keys: &[u128]) -> PartitionRecords<33> {
        let mut records = PartitionRecords::<33>::new(None, Some(keys.len() as u64), 0).unwrap();
        for (position, key) in keys.iter().enumerate() {
            let mut record = [0_u8; 33];
            record[..16].copy_from_slice(&key.to_be_bytes());
            // Hub endpoints share the key; the tail keeps the records distinct.
            record[24..32].copy_from_slice(&(position as u64).to_be_bytes());
            records.push(record);
        }
        records
    }

    fn detail_records() -> PartitionRecords<272> {
        let names = ["b", "", "zz", "a", "\u{e9}", "a"];
        let keys = [5_u128, 1, 5, 1, 9, 1];
        let mut records =
            PartitionRecords::<272>::new(Some(DetailCodec::Compact), Some(6), 6 * 20).unwrap();
        for (key, name) in keys.iter().zip(names) {
            let mut record = [0_u8; 272];
            record[..16].copy_from_slice(&key.to_be_bytes());
            record[16] = u8::try_from(name.len()).unwrap();
            record[17..17 + name.len()].copy_from_slice(name.as_bytes());
            records.push(record);
        }
        records
    }

    fn sorted_bytes<const N: usize>(mut records: PartitionRecords<N>, mode: Mode) -> Vec<u8> {
        apply_mode(&mut records, mode).unwrap();
        records.iter().flatten().copied().collect()
    }

    #[test]
    fn every_mode_sorts_fixed_hub_partitions_to_baseline_bytes() {
        let keys = [9_u128, 1, 7, 7, 7, 3, 5, 7, 1];
        let expected = sorted_bytes(fixed_records(&keys), Mode::Baseline);
        for mode in Mode::ALL {
            assert_eq!(
                sorted_bytes(fixed_records(&keys), mode),
                expected,
                "{mode:?}"
            );
        }
    }

    #[test]
    fn every_mode_sorts_compact_details_to_baseline_bytes_without_padding() {
        let expected = sorted_bytes(detail_records(), Mode::Baseline);
        for mode in Mode::ALL {
            let mut records = detail_records();
            apply_mode(&mut records, mode).unwrap();
            let PartitionRecords::Details { bytes, offsets } = &records else {
                panic!("expected compact details");
            };
            // Compact representation retained: no record grew to 272 bytes.
            assert!(bytes.len() < 6 * 20, "{mode:?}");
            assert_eq!(detail::<272>(bytes, offsets[0]).len(), 17, "{mode:?}");
            assert_eq!(
                records.iter().flatten().copied().collect::<Vec<_>>(),
                expected,
                "{mode:?}"
            );
        }
    }

    #[test]
    fn row_order_candidates_match_the_baseline_key_order() {
        let keys = FixedSizeBinaryArray::try_from_iter(
            [4_u128, 2, 8, 1].iter().map(|key| key.to_be_bytes()),
        )
        .unwrap();
        for indices in [
            arrow_indices(&keys).unwrap(),
            super::super::super::sort_partition_spike::datafusion_sort_indices(Arc::new(
                keys.clone(),
            ))
            .unwrap(),
        ] {
            assert_eq!(indices.values().to_vec(), vec![3, 1, 0, 2]);
        }
    }

    #[test]
    fn invalid_modes_fail_closed() {
        for invalid in ["", "ARROW", "datafusion-spill", "arrow "] {
            assert!(matches!(
                Mode::parse(invalid),
                Err(error) if error.to_string().contains("invalid shape sort experiment mode")
            ));
        }
        for mode in Mode::ALL {
            assert_eq!(Mode::parse(mode.as_str()).unwrap(), mode);
        }
    }
}
