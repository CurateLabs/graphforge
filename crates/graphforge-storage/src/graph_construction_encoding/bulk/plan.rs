//! Inputs and measurements of the bulk builder.

use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use serde::{Deserialize, Serialize};

/// Decodes and normalizes the batches of one input source on demand.
///
/// Tasks are the unit of parallel work (a Parquet row group, an Arrow IPC
/// record batch range). Every task yields canonical construction batches in
/// the source's row order, each at most `max_batch_rows` rows. The builder
/// calls tasks concurrently and from any thread.
pub trait BulkBatchReader: Send + Sync {
    /// Decode, normalize and hand every canonical batch of `task`, in order, to `sink`.
    ///
    /// # Errors
    /// Returns the intake refusal for the first rejected batch, or the error
    /// `sink` returned.
    fn read_task(
        &self,
        task: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError>;
}

/// One planned input source (pass 0).
#[derive(Clone)]
pub struct BulkSource<'a> {
    /// Decoder for this source's tasks.
    pub reader: Arc<dyn BulkBatchReader + 'a>,
    /// Number of tasks, fixed from the source's footer.
    pub tasks: usize,
    /// Total rows, fixed from the source's footer.
    pub rows: u64,
    /// Whether the source carries only the required columns.
    pub property_free: bool,
    /// Decoded size of the source's rows, from its footer. Only a
    /// property-bearing source retains its decoded batches.
    pub decoded_bytes: u64,
}

/// The planned inputs of one initial build: node sources, then edge sources.
#[derive(Clone, Default)]
pub struct BulkBuildPlan<'a> {
    /// Node sources in registration order.
    pub nodes: Vec<BulkSource<'a>>,
    /// Edge sources in registration order.
    pub edges: Vec<BulkSource<'a>>,
}

/// Resident bytes per node the builder holds: sorted UUID, label id, the
/// endpoint index slots, the identity stream and the encoded window in flight.
const BYTES_PER_NODE: u64 = 96;
/// Resident bytes per edge: UUID, two endpoint ranks, relation id, the decode
/// chunks that coexist with the assembled arrays, both CSR entry arrays and the
/// encoded window in flight. Calibrated against the peak RSS the builder
/// reports (about 104 B/edge at S20).
const BYTES_PER_EDGE: u64 = 128;

impl BulkBuildPlan<'_> {
    /// An upper estimate of the builder's peak resident bytes, from the
    /// footers alone. A property-bearing kind also retains its decoded batches
    /// and their sorted copy.
    #[must_use]
    pub fn estimated_resident_bytes(&self) -> u64 {
        let rows = |sources: &[BulkSource<'_>]| sources.iter().map(|source| source.rows).sum::<u64>();
        let retained = |sources: &[BulkSource<'_>]| {
            if sources.iter().all(|source| source.property_free) {
                0
            } else {
                sources
                    .iter()
                    .map(|source| source.decoded_bytes)
                    .sum::<u64>()
                    .saturating_mul(2)
            }
        };
        rows(&self.nodes)
            .saturating_mul(BYTES_PER_NODE)
            .saturating_add(rows(&self.edges).saturating_mul(BYTES_PER_EDGE))
            .saturating_add(retained(&self.nodes))
            .saturating_add(retained(&self.edges))
    }
}

/// Work attributed to one pass of the builder. Sums are over the pass only.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BulkPassReport {
    /// Wall-clock milliseconds.
    pub wall_ms: u64,
    /// Process CPU milliseconds (user plus system).
    pub cpu_ms: u64,
    /// `cpu_ms / wall_ms`, in thousandths of a core.
    pub effective_millicores: u64,
    /// Bytes passed to `write` (`wchar`) during the pass.
    pub logical_write_bytes: u64,
    /// Bytes the block layer accounted as written (`write_bytes`).
    pub physical_write_bytes: u64,
    /// Bytes passed to `read` (`rchar`).
    pub logical_read_bytes: u64,
    /// Process peak resident bytes (`VmHWM`) at the end of the pass.
    pub peak_rss_bytes: u64,
}

/// Measurements of one bulk build.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BulkBuildReport {
    /// Worker threads used.
    pub workers: usize,
    /// Nodes built.
    pub nodes: u64,
    /// Edges built.
    pub edges: u64,
    /// Per-pass measurements by pass name (`plan`, `nodes`, `edges`, `catalog`,
    /// `tables`, `adjacency`, `membership`, `properties`, `finalize`). Keys and
    /// values are numeric-only so receipts stay within the certification
    /// runner's sanitizer.
    pub passes: std::collections::BTreeMap<String, BulkPassReport>,
}

#[derive(Clone, Copy, Default)]
struct Sample {
    cpu_micros: u64,
    wchar: u64,
    write_bytes: u64,
    rchar: u64,
}

fn sample() -> Sample {
    let mut out = Sample::default();
    #[cfg(target_os = "linux")]
    {
        // utime and stime are fields 14 and 15 of /proc/self/stat, in clock
        // ticks (USER_HZ is 100 on Linux). The command name may contain spaces
        // and parentheses, so fields count from the last ')'.
        if let Ok(text) = std::fs::read_to_string("/proc/self/stat")
            && let Some((_, rest)) = text.rsplit_once(')')
        {
            let fields = rest.split_ascii_whitespace().collect::<Vec<_>>();
            let ticks = |index: usize| {
                fields
                    .get(index)
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0)
            };
            out.cpu_micros = (ticks(11) + ticks(12)).saturating_mul(10_000);
        }
        if let Ok(text) = std::fs::read_to_string("/proc/self/io") {
            for line in text.lines() {
                let Some((key, value)) = line.split_once(':') else {
                    continue;
                };
                let value = value.trim().parse::<u64>().unwrap_or(0);
                match key {
                    "wchar" => out.wchar = value,
                    "write_bytes" => out.write_bytes = value,
                    "rchar" => out.rchar = value,
                    _ => {}
                }
            }
        }
    }
    out
}

fn peak_rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    if let Ok(text) = std::fs::read_to_string("/proc/self/status") {
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                let kib = rest
                    .split_ascii_whitespace()
                    .next()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0);
                return kib.saturating_mul(1024);
            }
        }
    }
    0
}

/// Measures one pass from construction to [`Self::finish`].
pub(super) struct PassMeter {
    name: &'static str,
    started: Instant,
    before: Sample,
}

impl PassMeter {
    pub(super) fn start(name: &'static str) -> Self {
        Self {
            name,
            started: Instant::now(),
            before: sample(),
        }
    }

    pub(super) fn finish(self) -> (String, BulkPassReport) {
        let after = sample();
        let wall = self.started.elapsed();
        let wall_ms = u64::try_from(wall.as_millis()).unwrap_or(u64::MAX);
        let cpu_micros = after.cpu_micros.saturating_sub(self.before.cpu_micros);
        let wall_micros = u64::try_from(wall.as_micros()).unwrap_or(u64::MAX).max(1);
        let report = BulkPassReport {
            wall_ms,
            cpu_ms: cpu_micros / 1000,
            effective_millicores: cpu_micros.saturating_mul(1000) / wall_micros,
            logical_write_bytes: after.wchar.saturating_sub(self.before.wchar),
            physical_write_bytes: after.write_bytes.saturating_sub(self.before.write_bytes),
            logical_read_bytes: after.rchar.saturating_sub(self.before.rchar),
            peak_rss_bytes: peak_rss_bytes(),
        };
        (self.name.to_owned(), report)
    }
}
