//! Admission of the plan-time page inventory of one Parquet source scan
//! (#1918).
//!
//! Reading page headers keeps facts about every page of every column resident
//! for the life of the plan, and a column chunk of many tiny pages would grow
//! those facts without bound before the first value is checked. Every array the
//! scan builds — a chunk's page facts, a row group's leaf scans, the row-group
//! starts, the per-batch sizes — is therefore reserved through this budget
//! against the source workspace the caller passed, so an inventory that cannot
//! fit is refused, as a typed resource limit, before the allocation that would
//! exhaust it. Temporary buffers the scan holds while it works are admitted the
//! same way and released when dropped, so the charged total is what is live,
//! not a sum over the whole run.

use graphforge_core::GfError;

use super::limit;

/// The live total of everything one source scan has admitted so far.
pub(super) struct InventoryBudget {
    /// Bytes the caller lets this scan hold at once.
    capacity: u64,
    charged: u64,
}

impl InventoryBudget {
    /// A budget of `capacity` bytes: the source workspace the scan runs in.
    pub(super) fn new(capacity: u64) -> Self {
        Self {
            capacity,
            charged: 0,
        }
    }

    /// Admit `bytes` more live bytes for `what`, or refuse the scan before it
    /// allocates them.
    pub(super) fn admit(&mut self, bytes: u64, what: &str) -> Result<(), GfError> {
        let total = self
            .charged
            .checked_add(bytes)
            .ok_or_else(|| limit("the Parquet page inventory exceeds a countable size"))?;
        if total > self.capacity {
            return Err(limit(format!(
                "the Parquet page inventory of {what} would take this scan past its \
                 {capacity}-byte source workspace",
                capacity = self.capacity
            )));
        }
        self.charged = total;
        Ok(())
    }

    /// The scan dropped `bytes` it had been admitted, so later columns and row
    /// groups may use them again.
    pub(super) fn release(&mut self, bytes: u64) {
        self.charged = self.charged.saturating_sub(bytes);
    }

    /// Actual retained vector capacities after the scan's temporaries drop.
    pub(super) fn live_bytes(&self) -> u64 {
        self.charged
    }
}

/// Grow `vec` to hold `additional` more elements, admitting the growth against
/// `budget` before the allocator sees it.
///
/// Capacity doubles from what is in use, so a scan of many small pages stays
/// logarithmic in reallocations; a reallocation holds its old and its new
/// buffer at once, and `admit` covers both while they coexist. The allocation
/// is requested with `try_reserve_exact` at exactly the admitted size, so an
/// exhausted allocator fails the scan instead of aborting the process.
pub(super) fn reserve<E>(
    vec: &mut Vec<E>,
    additional: usize,
    budget: &mut InventoryBudget,
    what: &str,
) -> Result<(), GfError> {
    let element = u64::try_from(std::mem::size_of::<E>()).unwrap_or(u64::MAX);
    let needed = vec
        .len()
        .checked_add(additional)
        .ok_or_else(|| limit("the Parquet page inventory exceeds a countable size"))?;
    if needed <= vec.capacity() {
        return Ok(());
    }
    // Bounded geometric growth: double until `needed` fits.
    let mut cap = u64::try_from(vec.capacity().max(1)).unwrap_or(u64::MAX);
    while cap < u64::try_from(needed).unwrap_or(u64::MAX) {
        cap = cap
            .checked_mul(2)
            .ok_or_else(|| limit("the Parquet page inventory exceeds a countable size"))?;
    }
    let admitted = cap
        .checked_mul(element)
        .ok_or_else(|| limit("the Parquet page inventory exceeds a countable size"))?;
    let old = u64::try_from(vec.capacity())
        .unwrap_or(u64::MAX)
        .saturating_mul(element);
    // The old buffer is charged already; admitting the new one covers both
    // while the reallocation moves the elements across.
    budget.admit(admitted, what)?;
    let grown = usize::try_from(cap).unwrap_or(usize::MAX);
    if vec.try_reserve_exact(grown - vec.len()).is_err() {
        budget.release(admitted);
        return Err(limit(format!(
            "the allocator refused {admitted} bytes for the Parquet page inventory of {what}"
        )));
    }
    let actual = u64::try_from(vec.capacity())
        .unwrap_or(u64::MAX)
        .saturating_mul(element);
    if actual > admitted {
        // The allocator returned more than was asked for; hold it only if the
        // budget still covers it.
        budget.admit(actual - admitted, what)?;
    } else {
        // The exact reservation took less than the geometric bound admitted.
        budget.release(admitted - actual);
    }
    // The old buffer is freed once the new one holds the elements.
    budget.release(old);
    Ok(())
}
