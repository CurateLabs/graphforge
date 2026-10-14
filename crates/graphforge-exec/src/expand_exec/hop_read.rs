//! Admission and ownership of single-hop read buffers.

use super::SingleHopConfig;
use crate::chunked_reservation::ChunkedReservation;
use arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use graphforge_core::GfError;

/// What a single-hop chunk has charged to [`SingleHopConfig::read_reservation`],
/// released when the chunk ends, by whatever path it ends.
pub(super) struct HopReadCharge<'a> {
    reservation: &'a std::sync::Mutex<ChunkedReservation>,
    held: usize,
}

impl<'a> HopReadCharge<'a> {
    pub(super) fn new(reservation: &'a std::sync::Mutex<ChunkedReservation>) -> Self {
        Self {
            reservation,
            held: 0,
        }
    }

    /// Charge `bytes` that are now live. Fails, naming [`HOP_READ_CONSUMER`],
    /// when the pool cannot hold them; the failed charge is not recorded.
    pub(super) fn charge(&mut self, bytes: usize) -> Result<(), GfError> {
        self.reservation
            .lock()
            .map_err(|_| GfError::Execution("ExpandExec hop read reservation poisoned".into()))?
            .try_grow(bytes)
            .map_err(GfError::from_execution_error)?;
        self.held += bytes;
        Ok(())
    }

    /// Transfer a completed decoder envelope to the exact returned buffers.
    /// Grow/shrink only the difference while holding one reservation lock.
    pub(super) fn replace(&mut self, old: usize, new: usize) -> Result<(), GfError> {
        if old > self.held {
            return Err(GfError::Execution(
                "ExpandExec invalid hop reservation transfer".into(),
            ));
        }
        let mut reservation = self
            .reservation
            .lock()
            .map_err(|_| GfError::Execution("ExpandExec hop read reservation poisoned".into()))?;
        if new > old {
            reservation
                .try_grow(new - old)
                .map_err(GfError::from_execution_error)?;
        } else {
            reservation.shrink(old - new);
        }
        self.held = self.held - old + new;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn held_bytes(&self) -> usize {
        self.held
    }

    /// Release `bytes` that are no longer live.
    pub(super) fn release(&mut self, bytes: usize) {
        let bytes = bytes.min(self.held);
        if let Ok(mut reservation) = self.reservation.lock() {
            reservation.shrink(bytes);
            self.held -= bytes;
        }
    }
}

impl Drop for HopReadCharge<'_> {
    fn drop(&mut self) {
        self.release(self.held);
    }
}

/// The pool consumer a single-hop chunk's edge and destination-node reads
/// charge: the decoded batches, their concatenation, and the id-to-row tables.
pub(super) const HOP_READ_CONSUMER: &str = "ExpandExec hop read";

/// Bytes the decoded `batches` hold.
pub(super) fn batches_bytes(batches: &[RecordBatch]) -> usize {
    batches
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .fold(0, usize::saturating_add)
}

/// The rows of `batches` as one batch, charging `charge` for every live copy.
/// A lone batch is shared, not copied, so its charge stays; otherwise the copy
/// is charged before it is allocated, so an over-budget read fails before the
/// copy, and the decoded batches' charge is released once they are dropped.
/// Fails, typed, when the pool cannot hold the charge.
pub(super) fn concat_charged(
    charge: &mut HopReadCharge<'_>,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch, GfError> {
    let decoded = batches_bytes(&batches);
    charge.charge(decoded)?;
    concat_precharged(charge, batches)
}

/// Concatenate batches whose decoded buffers are already charged.
pub(super) fn concat_precharged(
    charge: &mut HopReadCharge<'_>,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch, GfError> {
    let decoded = batches_bytes(&batches);
    if let [only] = batches.as_slice() {
        return Ok(only.clone());
    }
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| GfError::Execution("Expand: scan returned no batches".into()))?;
    charge.charge(decoded)?;
    let joined = arrow::compute::concat_batches(&schema, &batches)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    drop(batches);
    charge.release(decoded);
    Ok(joined)
}

impl SingleHopConfig {
    /// Charge the held input batch and the output batch to the session memory pool.
    pub(super) fn account(
        &self,
        held: Option<&RecordBatch>,
        output: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let bytes =
            held.map_or(0, RecordBatch::get_array_memory_size) + output.get_array_memory_size();
        self.reservation
            .lock()
            .map_err(|_| DataFusionError::Internal("ExpandExec reservation poisoned".into()))?
            .try_resize(bytes)
    }
}
