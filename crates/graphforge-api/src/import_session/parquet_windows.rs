//! Physical-to-logical batch identity mapping for bounded Parquet pieces.

use graphforge_core::GfError;

use super::storage;

/// Maps bounded physical Parquet pieces back to the logical construction
/// batches whose operation UUID and row ordinals must remain stable.
///
/// Physical pieces divide logical batches so no piece crosses an identity
/// boundary. The map is scalar state: it does not allocate one entry per row
/// or batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PhysicalBatchMap {
    rows: u64,
    logical_rows: u64,
    physical_rows: u64,
}

impl PhysicalBatchMap {
    pub(super) fn new(rows: u64, logical_rows: u64, physical_rows: u64) -> Result<Self, GfError> {
        if logical_rows == 0 || physical_rows == 0 || physical_rows > logical_rows {
            return Err(storage("Parquet physical batch sizes are out of range"));
        }
        if !logical_rows.is_multiple_of(physical_rows) {
            return Err(storage(
                "Parquet physical batches must divide logical batch boundaries",
            ));
        }
        Ok(Self {
            rows,
            logical_rows,
            physical_rows,
        })
    }

    pub(super) fn physical_rows(self) -> u64 {
        self.physical_rows
    }

    pub(super) fn physical_batches(self) -> u64 {
        div_ceil(self.rows, self.physical_rows)
    }

    pub(super) fn physical_range(self, index: u64) -> Result<(u64, u64), GfError> {
        if index >= self.physical_batches() {
            return Err(storage("Parquet physical batch index is out of range"));
        }
        let start = index
            .checked_mul(self.physical_rows)
            .ok_or_else(|| storage("Parquet physical batch start overflows"))?;
        let remaining = self
            .rows
            .checked_sub(start)
            .ok_or_else(|| storage("Parquet physical batch starts beyond the source"))?;
        let end = start
            .checked_add(self.physical_rows.min(remaining))
            .ok_or_else(|| storage("Parquet physical batch end overflows"))?;
        Ok((start, end))
    }

    /// `(logical batch index, first ordinal within it, physical row count)`.
    pub(super) fn identity(self, index: u64) -> Result<(u64, u64, u64), GfError> {
        let (start, end) = self.physical_range(index)?;
        Ok((
            start / self.logical_rows,
            start % self.logical_rows,
            end - start,
        ))
    }
}

fn div_ceil(value: u64, divisor: u64) -> u64 {
    value / divisor + u64::from(!value.is_multiple_of(divisor))
}
