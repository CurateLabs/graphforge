//! Original-batch normalization windows; only the coordinator appends results.

use arrow::array::ArrayData;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_storage::concurrency_attribution::RegionScope;
use std::path::Path;
use uuid::Uuid;

use super::{
    SourceRecord, cancelled, for_each_source_batch, import_batch_operation, normalize_batch,
};
use crate::{BulkInputKind, CancellationToken, GraphForge};

/// Includes retained Arrow plus a conservative row/container scratch estimate.
/// This is admission accounting, not an allocator measurement. A decoded
/// lookahead can coexist with this window; an oversized batch runs alone.
fn batch_weight(kind: BulkInputKind, batch: &RecordBatch) -> usize {
    let row_scratch = match kind {
        BulkInputKind::Node => 256,
        BulkInputKind::Edge => 512,
    };
    let mut bytes = batch
        .get_array_memory_size()
        .saturating_mul(3)
        .saturating_add(batch.num_rows().saturating_mul(row_scratch));
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let required = match kind {
            BulkInputKind::Node => matches!(field.name().as_str(), "node_uuid" | "label"),
            BulkInputKind::Edge => matches!(
                field.name().as_str(),
                "edge_uuid" | "rel_type" | "source_uuid" | "target_uuid"
            ),
        };
        if !required {
            // Bitmap-packed booleans and nested values need row/cell space in
            // PropValue/BTreeMap form, not merely their compact Arrow bytes.
            bytes = bytes
                .saturating_add(
                    batch
                        .num_rows()
                        .saturating_mul(192_usize.saturating_add(field.name().len())),
                )
                .saturating_add(value_scratch(&array.to_data()));
        }
    }
    bytes
}

fn value_scratch(data: &ArrayData) -> usize {
    data.child_data()
        .iter()
        .fold(data.len().saturating_mul(128), |bytes, child| {
            bytes.saturating_add(value_scratch(child))
        })
}

struct Window<'a> {
    graph: &'a GraphForge,
    operation_uuid: Uuid,
    source_sequence: u64,
    kind: BulkInputKind,
    cancellation: Option<&'a CancellationToken>,
    pending: Vec<(u64, RecordBatch)>,
    admitted_bytes: usize,
    byte_budget: usize,
    workers: usize,
    /// Test-only observation of batches normalizing at once (#1586).
    #[cfg(test)]
    probe: Option<std::sync::Arc<InFlightProbe>>,
}

/// Counts batches normalizing at once; test-only.
#[cfg(test)]
#[derive(Default)]
struct InFlightProbe {
    current: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

impl Window<'_> {
    fn push(
        &mut self,
        index: u64,
        batch: RecordBatch,
        consume: &mut impl FnMut(u64, RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let weight = batch_weight(self.kind, &batch);
        if !self.pending.is_empty() && self.admitted_bytes.saturating_add(weight) > self.byte_budget
        {
            self.flush(consume)?;
        }
        self.admitted_bytes = self.admitted_bytes.saturating_add(weight);
        self.pending.push((index, batch));
        if self.pending.len() >= self.workers || self.admitted_bytes >= self.byte_budget {
            self.flush(consume)?;
        }
        Ok(())
    }

    fn flush(
        &mut self,
        consume: &mut impl FnMut(u64, RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending);
        self.admitted_bytes = 0;
        // #1586: lease this flush's lanes from the instance construction
        // admission on the calling thread, so no compute-pool worker ever
        // blocks waiting for one, then map at most that many batches at once.
        // Each batch normalizes independently, so the grouping changes
        // neither results nor their order.
        let cancellation = self.cancellation;
        let want =
            std::num::NonZeroUsize::new(pending.len()).unwrap_or(std::num::NonZeroUsize::MIN);
        let lease = self
            .graph
            .construction_cpu_admission
            .acquire(want, &mut || {
                cancellation.is_some_and(CancellationToken::is_cancelled)
            })
            .map_err(|error| {
                if cancellation.is_some_and(CancellationToken::is_cancelled) {
                    cancelled()
                } else {
                    error
                }
            })?;
        let lanes = lease.lanes().get();
        let region = RegionScope::named("normalization");
        let mut normalized = Vec::with_capacity(pending.len());
        for group in pending.chunks(lanes) {
            normalized.extend(
                self.graph
                    .compute_pool
                    .map_ordered(group, |(index, batch)| {
                        #[cfg(test)]
                        if let Some(probe) = &self.probe {
                            use std::sync::atomic::Ordering;
                            let now = probe.current.fetch_add(1, Ordering::AcqRel) + 1;
                            probe.peak.fetch_max(now, Ordering::AcqRel);
                            // Hold the lane long enough for any overlap to show.
                            std::thread::sleep(std::time::Duration::from_millis(20));
                            probe.current.fetch_sub(1, Ordering::AcqRel);
                        }
                        let result = if self
                            .cancellation
                            .is_some_and(CancellationToken::is_cancelled)
                        {
                            Err(cancelled())
                        } else {
                            normalize_batch(
                                self.graph,
                                import_batch_operation(
                                    self.operation_uuid,
                                    self.source_sequence,
                                    *index,
                                ),
                                self.kind,
                                batch,
                            )
                        };
                        (*index, result)
                    }),
            );
        }
        drop(lease);
        RegionScope::record_work(
            "rows",
            normalized
                .iter()
                .filter_map(|(_, result)| result.as_ref().ok())
                .map(|batch| batch.num_rows() as u64)
                .sum(),
        );
        drop(region);
        drop(pending);
        // Keep individual Results ordered. Collecting Result<Vec<_>> instead
        // would discard the durable successful prefix on a later refusal.
        for (index, result) in normalized {
            if self
                .cancellation
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(cancelled());
            }
            consume(index, result?)?;
        }
        Ok(())
    }
}

pub(super) fn for_each(
    graph: &GraphForge,
    root: &Path,
    source: &SourceRecord,
    batch_rows: usize,
    operation_uuid: Uuid,
    cancellation: Option<&CancellationToken>,
    mut consume: impl FnMut(u64, RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let mut window = Window {
        graph,
        operation_uuid,
        source_sequence: source.sequence,
        kind: source.kind.input_kind(),
        cancellation,
        pending: Vec::new(),
        admitted_bytes: 0,
        byte_budget: usize::try_from(graph.resource_policy.memory_budget_bytes / 4)
            .unwrap_or(usize::MAX)
            .clamp(1, 256 << 20),
        workers: graph.compute_pool.num_threads().min(4),
        #[cfg(test)]
        probe: None,
    };
    let mut decoded_index = 0_u64;
    for_each_source_batch(root, source, batch_rows, |batch| {
        let Some(batch) = batch else {
            return window.flush(&mut consume);
        };
        let index = decoded_index;
        decoded_index += 1;
        if index < source.batches_staged {
            return Ok(());
        }
        window.push(index, batch, &mut consume)
    })
}

#[cfg(test)]
mod tests;
