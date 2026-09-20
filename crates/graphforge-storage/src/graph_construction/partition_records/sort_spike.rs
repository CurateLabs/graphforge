//! Test-support-only fixed-partition sort experiment (#1506).
//!
//! When `GF_SHAPE_SORT_SPIKE=arrow`, fixed-width partition records sort through
//! Arrow `sort_to_indices` + `take` on a `FixedSizeBinary` column. Compact
//! detail offset sorting stays on the baseline path: rebuilding padded rows
//! just to hand Arrow a column would defeat the compact representation.
//!
//! Unset / `baseline` keep `sort_unstable`. Invalid modes fail closed.
//! Protocol: `docs/development/construction-reuse-inventory-protocol-1505.md`.
use super::{PartitionRecords, detail, storage};
use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::compute::{SortOptions, sort_to_indices, take};
use graphforge_core::GfError;

pub(super) fn apply<const N: usize>(records: &mut PartitionRecords<N>) -> Result<(), GfError> {
    apply_mode(records, mode()?)
}

fn apply_mode<const N: usize>(
    records: &mut PartitionRecords<N>,
    mode: Mode,
) -> Result<(), GfError> {
    match mode {
        Mode::Baseline => {
            records.sort();
            Ok(())
        }
        Mode::Arrow => match records {
            PartitionRecords::Fixed(fixed) => sort_fixed_arrow::<N>(fixed),
            PartitionRecords::Details { .. } => {
                // Hybrid retain: compact wire sort is not an Arrow array today.
                records.sort();
                Ok(())
            }
        },
    }
}

fn mode() -> Result<Mode, GfError> {
    match std::env::var("GF_SHAPE_SORT_SPIKE") {
        Err(std::env::VarError::NotPresent) => Ok(Mode::Baseline),
        Ok(mode) if mode == "baseline" => Ok(Mode::Baseline),
        Ok(mode) if mode == "arrow" => Ok(Mode::Arrow),
        _ => Err(storage("invalid shape sort experiment mode")),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Arrow,
}

fn sort_fixed_arrow<const N: usize>(records: &mut Vec<[u8; N]>) -> Result<(), GfError> {
    if records.len() <= 1 {
        return Ok(());
    }
    let array = FixedSizeBinaryArray::try_from_iter(records.iter().map(|record| record.as_slice()))
        .map_err(|error| storage(error.to_string()))?;
    let indices = sort_to_indices(
        &array,
        Some(SortOptions {
            descending: false,
            nulls_first: true,
        }),
        None,
    )
    .map_err(|error| storage(error.to_string()))?;
    let sorted = take(&array, &indices, None).map_err(|error| storage(error.to_string()))?;
    let sorted = sorted
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| storage("arrow sort produced non-fixed binary array"))?;
    if sorted.len() != records.len() {
        return Err(storage("arrow sort changed partition record count"));
    }
    // Fixed construction records are unique on the UUID prefix, so instability
    // does not change the total order (see load_fixed_partition).
    for (slot, index) in records.iter_mut().zip(0..sorted.len()) {
        slot.copy_from_slice(sorted.value(index));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_fixed_sort_matches_baseline_bytes() {
        let mut arrow_sorted = PartitionRecords::<16>::new(None, Some(5), 80).unwrap();
        let mut baseline = PartitionRecords::<16>::new(None, Some(5), 80).unwrap();
        for key in [9_u128, 1, 7, 3, 5] {
            arrow_sorted.push(key.to_be_bytes());
            baseline.push(key.to_be_bytes());
        }
        apply_mode(&mut arrow_sorted, Mode::Arrow).unwrap();
        baseline.sort();
        assert_eq!(
            arrow_sorted.iter().flatten().copied().collect::<Vec<_>>(),
            baseline.iter().flatten().copied().collect::<Vec<_>>()
        );
    }

    #[test]
    fn invalid_mode_string_fails_closed() {
        assert!(matches!(
            parse_mode_str("datafusion"),
            Err(error) if error.to_string().contains("invalid shape sort experiment mode")
        ));
        assert_eq!(parse_mode_str("arrow").unwrap(), Mode::Arrow);
        assert_eq!(parse_mode_str("baseline").unwrap(), Mode::Baseline);
    }

    fn parse_mode_str(value: &str) -> Result<Mode, GfError> {
        match value {
            "baseline" => Ok(Mode::Baseline),
            "arrow" => Ok(Mode::Arrow),
            _ => Err(storage("invalid shape sort experiment mode")),
        }
    }

    #[test]
    fn detail_path_retains_baseline_under_arrow_mode() {
        let mut records = PartitionRecords::<272>::new(
            Some(crate::construction_detail_codec::DetailCodec::Compact),
            Some(2),
            100,
        )
        .unwrap();
        let mut first = [0_u8; 272];
        first[..16].copy_from_slice(&2_u128.to_be_bytes());
        first[16] = 1;
        first[17] = b'b';
        let mut second = [0_u8; 272];
        second[..16].copy_from_slice(&1_u128.to_be_bytes());
        second[16] = 1;
        second[17] = b'a';
        records.push(first);
        records.push(second);
        apply_mode(&mut records, Mode::Arrow).unwrap();
        let keys: Vec<u128> = records
            .iter()
            .map(|bytes| u128::from_be_bytes(bytes[..16].try_into().unwrap()))
            .collect();
        assert_eq!(keys, vec![1, 2]);
        let PartitionRecords::Details { bytes, offsets } = &records else {
            panic!("expected compact details");
        };
        assert_eq!(detail::<272>(bytes, offsets[0]).len(), 18);
    }
}
