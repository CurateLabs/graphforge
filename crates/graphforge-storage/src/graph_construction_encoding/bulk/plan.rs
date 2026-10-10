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

    /// Exactly how many rows `task` will emit, known from the footer. Each task
    /// decodes straight into its own slice of the final columns, so a decoded
    /// copy and an assembled copy never coexist. A task that emits a different
    /// number of rows fails the build.
    fn task_rows(&self, task: usize) -> usize;

    /// Owned bytes of the source's cached Arrow schema, including nested
    /// fields and metadata. Readers retaining schema buffers report them here
    /// so the scratch builder reserves them before decoding. A reader with no
    /// retained schema may use the default.
    fn schema_resident_bytes(&self) -> u64 {
        0
    }

    /// Cached footer/schema memory retained while the plan exists.
    fn retained_metadata_bytes(&self) -> u64 {
        self.schema_resident_bytes()
    }

    /// Maximum raw decoder workspace, including retained dictionaries and
    /// compressed-message expansion, known without decoding payload arrays.
    fn decoded_workspace_bytes(&self) -> u64 {
        0
    }

    /// The smallest and largest identity UUID among `task`'s rows, when the
    /// source's footer states them exactly (no nulls, so no derived UUIDs). The
    /// over-budget route uses them to split edges into UUID ranges of equal
    /// size without reading any data; without them it samples.
    fn uuid_bounds(&self, _task: usize) -> Option<([u8; 16], [u8; 16])> {
        None
    }

    /// Whether every batch this source emits already passed canonical-schema
    /// validation and the construction admission windows when it was accepted,
    /// so the builder need not repeat them. A decoded copy can be larger than
    /// the batch that was admitted (buffers shared by one IPC body count once
    /// per column), so repeating the byte window would refuse an admitted batch.
    fn admitted(&self) -> bool {
        false
    }
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
    /// Decoded size of the source's rows, from its footer. The resident peak
    /// model charges retained batches only for property-bearing kinds.
    pub decoded_bytes: u64,
}

/// Bytes a task holds while it decodes, per byte of the rows it reads: the
/// row groups' pages as read, the copy the source digest keeps until the file
/// is hashed in order, and the batches decoded and normalized from them.
const DECODE_EXPANSION: u64 = 4;
/// Fewest bytes a decoding task reserves.
const MIN_TASK_DECODE_BYTES: u64 = 4 << 20;

impl BulkSource<'_> {
    /// Bytes decoding `task` is expected to hold at once, from the footer's
    /// uncompressed size shared out by rows, plus what the reader says its
    /// decoder needs. Several tasks decode at once on the scratch route; each
    /// reserves this much from a pool sized from the budget before it reads.
    pub(super) fn task_decode_bytes(&self, task: usize) -> u64 {
        let rows = self.reader.task_rows(task) as u64;
        let share = if self.rows == 0 {
            0
        } else {
            u64::try_from(u128::from(self.decoded_bytes) * u128::from(rows) / u128::from(self.rows))
                .unwrap_or(u64::MAX)
        };
        share
            .saturating_mul(DECODE_EXPANSION)
            .saturating_add(self.reader.decoded_workspace_bytes())
            .max(MIN_TASK_DECODE_BYTES)
    }
}

/// The planned inputs of one initial build: node sources, then edge sources.
#[derive(Clone, Default)]
pub struct BulkBuildPlan<'a> {
    /// Node sources in registration order.
    pub nodes: Vec<BulkSource<'a>>,
    /// Edge sources in registration order.
    pub edges: Vec<BulkSource<'a>>,
    /// Resident bytes the build may plan to use, or `None` for no limit. A
    /// build whose in-memory estimate exceeds it runs on scratch files
    /// (ADR 0058, #1900).
    pub memory_budget: Option<u64>,
}

/// Peak-RSS model of the builder, fitted to measured runs (#1883):
/// `peak = BASE + BYTES_PER_EDGE * edges + BYTES_PER_NODE * nodes` by least
/// squares over Graph500 S18, S20, S22 and S24 plus two S22 node sets with 8 and
/// 24 row groups of edges, all property-free: 671 MB + 24.2 B/edge + 72.8 B/node,
/// worst measured/fitted ratio 1.14 (at S20, where the base dominates). The
/// constants below round the fit up so no measured run exceeds the model before
/// the margin. A Graph500 rung (16 edges per node) costs about 29 B/edge.
const BASE_BYTES: u64 = 768 << 20;
const BYTES_PER_EDGE: u64 = 26;
const BYTES_PER_NODE: u64 = 76;
/// A property-bearing kind retains its decoded batches, then a concatenated and
/// a sorted copy per schema group. Measured at S20 with a `name` node property
/// and a `weight` edge property: peak RSS exceeded the fitted property-free
/// model by 5.5 times the footers' uncompressed bytes.
pub(super) const RETAINED_FACTOR: u64 = 6;
/// Safety margin on the sum, as a fraction: 5/4.
const MARGIN_NUMERATOR: u64 = 5;
const MARGIN_DENOMINATOR: u64 = 4;

impl BulkBuildPlan<'_> {
    /// Peak resident bytes the builder is expected to need, from the footers
    /// alone: the fitted model plus a 25% margin.
    #[must_use]
    pub fn estimated_resident_bytes(&self) -> u64 {
        let rows =
            |sources: &[BulkSource<'_>]| sources.iter().map(|source| source.rows).sum::<u64>();
        let retained = |sources: &[BulkSource<'_>]| {
            if sources.iter().all(|source| source.property_free) {
                0
            } else {
                sources
                    .iter()
                    .map(|source| source.decoded_bytes)
                    .sum::<u64>()
                    .saturating_mul(RETAINED_FACTOR)
            }
        };
        BASE_BYTES
            .saturating_add(rows(&self.nodes).saturating_mul(BYTES_PER_NODE))
            .saturating_add(rows(&self.edges).saturating_mul(BYTES_PER_EDGE))
            .saturating_add(retained(&self.nodes))
            .saturating_add(retained(&self.edges))
            .saturating_add(
                self.nodes
                    .iter()
                    .chain(&self.edges)
                    .map(|source| source.reader.retained_metadata_bytes())
                    .fold(0_u64, u64::saturating_add),
            )
            .saturating_add(self.max_source_schema_bytes().saturating_mul(8))
            .saturating_add(self.source_decoder_bytes())
            .saturating_mul(MARGIN_NUMERATOR)
            / MARGIN_DENOMINATOR
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
    /// Per-pass measurements by pass name (`plan`, `nodes`, `edges`,
    /// `edge-refs` and `endpoints` (node tables on scratch only), `ranks`
    /// (scratch only), `catalog`, `tables`, `ordinal`, `adjacency`,
    /// `properties`, `finalize`). Keys and values are numeric-only so receipts
    /// stay within the certification runner's sanitizer.
    pub passes: std::collections::BTreeMap<String, BulkPassReport>,
    /// Partitions in flight on the over-budget route; zero when the build ran in memory.
    #[serde(default)]
    pub scratch_concurrency: u64,
    /// Edge-UUID range partitions of the over-budget route.
    #[serde(default)]
    pub edge_partitions: u64,
    /// Node-range partitions per direction of the over-budget route.
    #[serde(default)]
    pub csr_partitions: u64,
    /// Bytes the over-budget route wrote to scratch files, block headers included.
    #[serde(default)]
    pub scratch_write_bytes: u64,
    /// Bytes it read back.
    #[serde(default)]
    pub scratch_read_bytes: u64,
    /// The largest number of bytes reserved for its scratch files at once,
    /// block headers and bytes still buffered in a writer included. Files
    /// leave the occupancy as soon as their final read reclaims them, so on
    /// a successful build this is strictly less than the cumulative
    /// `scratch_write_bytes` when anything was reclaimed early. This is a
    /// conservative bound on logical reserved file bytes, not the
    /// filesystem's allocated blocks or an exact physical overlap, and it
    /// says nothing about input, output or process memory.
    #[serde(default)]
    pub scratch_peak_occupied_bytes: u64,
    /// Edges in the largest edge-UUID range partition of the over-budget route.
    #[serde(default)]
    pub largest_edge_partition: u64,
    /// Radix refinements of oversized UUID ranges (zero for balanced input).
    #[serde(default)]
    pub edge_refinement_steps: u64,
    /// Additional scratch writes needed to refine skewed edge UUID ranges.
    #[serde(default)]
    pub edge_refinement_write_bytes: u64,
    /// Scratch reads performed by adaptive edge refinement.
    #[serde(default)]
    pub edge_refinement_read_bytes: u64,
    /// Additional scratch writes for relation CSR spools.
    #[serde(default)]
    pub csr_spool_write_bytes: u64,
    /// Scratch reads from relation CSR spools.
    #[serde(default)]
    pub csr_spool_read_bytes: u64,
    /// Largest single unfinished CSR shard. Relation count does not multiply it.
    #[serde(default)]
    pub peak_csr_carry_entries: u64,
    /// Property IPC frames written, including CRC headers and temporary runs.
    #[serde(default)]
    pub property_scratch_write_bytes: u64,
    /// Property IPC frames read; repeated catalog/window scans are included.
    #[serde(default)]
    pub property_scratch_read_bytes: u64,
    /// Fixed property workspace reserved before decoding any source.
    #[serde(default)]
    pub property_workspace_reserved_bytes: u64,
    /// Node-UUID range partitions when the node tables ran on scratch (#1929);
    /// zero when they were resident.
    #[serde(default)]
    pub node_partitions: u64,
    /// Nodes in the largest node-UUID range partition after refinement.
    #[serde(default)]
    pub largest_node_partition: u64,
    /// Radix refinements of oversized node UUID ranges.
    #[serde(default)]
    pub node_refinement_steps: u64,
    /// Additional scratch writes needed to refine skewed node UUID ranges.
    #[serde(default)]
    pub node_refinement_write_bytes: u64,
    /// Scratch reads performed by node refinement.
    #[serde(default)]
    pub node_refinement_read_bytes: u64,
    /// Scratch bytes written for nodes: the scatter, its refinement, and the
    /// sorted runs.
    #[serde(default)]
    pub node_scratch_write_bytes: u64,
    /// Scratch bytes read back for nodes.
    #[serde(default)]
    pub node_scratch_read_bytes: u64,
    /// Scratch bytes written to resolve edge endpoints over scratch node
    /// tables: the references routed to node leaves and the resolved records
    /// routed back to edge leaves.
    #[serde(default)]
    pub endpoint_scratch_write_bytes: u64,
    /// Scratch bytes read back for endpoint resolution.
    #[serde(default)]
    pub endpoint_scratch_read_bytes: u64,
    /// Uncompressed bytes of the property-bearing sources, from their footers:
    /// the denominator of the property scratch traffic per input byte.
    #[serde(default)]
    pub property_source_bytes: u64,
    /// Sorted property runs written from the input.
    #[serde(default)]
    pub property_runs: u64,
    /// Runs one property merge may hold open.
    #[serde(default)]
    pub property_merge_fan_in: u64,
    /// The most runs any property merge did hold open.
    #[serde(default)]
    pub property_merge_inputs_peak: u64,
    /// One shared capacity for node and edge property merge jobs.
    #[serde(default)]
    pub property_merge_budget_bytes: u64,
    /// Most bytes all concurrently admitted property merge jobs reserved.
    #[serde(default)]
    pub property_merge_peak_reserved_bytes: u64,
    /// Most bytes the workers held at once while forming property runs, against
    /// the `property_retained_budget_bytes` they were allowed.
    #[serde(default)]
    pub property_peak_retained_bytes: u64,
    /// Bytes all workers together could hold while forming property runs.
    #[serde(default)]
    pub property_retained_budget_bytes: u64,
    /// Bytes the tasks decoding at once may reserve, and the most they did.
    #[serde(default)]
    pub decode_pool_bytes: u64,
    /// The most bytes the decoding tasks reserved at once.
    #[serde(default)]
    pub decode_peak_bytes: u64,
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
