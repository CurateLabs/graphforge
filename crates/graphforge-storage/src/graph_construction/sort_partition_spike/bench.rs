//! Repeated kernel measurements for the #1506 sort/partition candidates.
//!
//! Driven by `examples/sort_partition_spike.rs` (feature `test-support`),
//! which owns a counting global allocator and passes it in as a [`HeapProbe`].
//! Every observation regenerates its input outside the timed region, rotates
//! the mode order per round, and checks the output digest against the
//! baseline's, so a faster wrong answer is a failure, not a result.
//!
//! These are supporting kernel measurements. Complete-ingest pairs through the
//! `gf` CLI are the protocol's headline evidence.
use super::super::GraphConstructionEvidence;
use super::super::partition::PartitionPlan;
use super::super::partition_records::PartitionRecords;
use super::super::partition_records::sort_spike::{Mode, apply_mode, row_order_for_mode};
use super::super::partition_shaping::{
    FixedRangePartitioner, PartitionFamily, fixed_spill_name, load_fixed_partition,
};
use super::{external_sort_fixed_partition, hash_repartition_then_merge};
use crate::construction_detail_codec::DetailCodec;
use crate::construction_directory::ConstructionDirectory;
use crate::construction_record_layout::{
    BASE_IDENTITY_WIDTH, ENDPOINT_WIDTH, RESOLVED_ENDPOINT_WIDTH,
};
use arrow::array::{Array, FixedSizeBinaryArray};
use graphforge_core::GfError;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use super::super::storage;

/// Process heap observations supplied by the driver's global allocator.
pub trait HeapProbe {
    /// Reset the high-water mark to the current live bytes; return them.
    fn reset_peak(&self) -> u64;
    /// High-water live bytes since the last reset.
    fn peak(&self) -> u64;
}

/// Measurement shape. Defaults are the recorded #1506 kernel envelope.
#[derive(Clone, Debug)]
pub struct SortPartitionBenchConfig {
    /// Alternating rounds per workload.
    pub rounds: usize,
    /// Records per in-memory sort workload.
    pub records: u64,
    /// Records in the over-budget hub partition.
    pub hub_records: u64,
    /// Recorded materialization budget and DataFusion pool, in bytes.
    pub budget_bytes: u64,
    /// Records per external-sort input batch.
    pub batch_records: usize,
    /// Keys and partitions for the range-versus-hash comparison.
    pub partition_records: u64,
    /// Range/hash partitions.
    pub partitions: usize,
    /// Directory on an admitted filesystem for spills and library scratch.
    pub scratch: PathBuf,
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    /// UUIDv7-shaped key: one shared millisecond prefix, random tail.
    fn v7(&mut self) -> [u8; 16] {
        let mut key = [0_u8; 16];
        key[..6].copy_from_slice(&0x0199_0000_0000_u64.to_be_bytes()[2..]);
        key[6..8].copy_from_slice(&0x7000_u16.to_be_bytes());
        key[8..].copy_from_slice(&self.next().to_be_bytes());
        key
    }
}

fn digest(bytes: impl Iterator<Item = u8>) -> String {
    let mut hasher = Sha256::new();
    let mut buffer = Vec::with_capacity(1 << 16);
    for byte in bytes {
        buffer.push(byte);
        if buffer.len() == buffer.capacity() {
            hasher.update(&buffer);
            buffer.clear();
        }
    }
    hasher.update(&buffer);
    super::super::hex(&hasher.finalize())
}

fn records_digest<const N: usize>(records: &PartitionRecords<N>) -> String {
    digest(records.iter().flatten().copied())
}

/// Fixed-width partition with unique v7 keys and a deterministic tail.
fn unique_fixed<const N: usize>(count: u64, seed: u64) -> Result<PartitionRecords<N>, GfError> {
    let mut rng = Rng(seed);
    let mut records = PartitionRecords::<N>::new(None, Some(count), 0)?;
    for position in 0..count {
        let mut record = [0_u8; N];
        record[..16].copy_from_slice(&rng.v7());
        let tail = position.to_be_bytes();
        let width = (N - 16).min(8);
        record[16..16 + width].copy_from_slice(&tail[8 - width..]);
        records.push(record);
    }
    Ok(records)
}

/// Endpoints keyed by node UUID with a power-law node choice: a few hubs own
/// most of the partition, as in Graph500 degree distributions.
fn power_law_endpoints(count: u64, seed: u64) -> Result<PartitionRecords<ENDPOINT_WIDTH>, GfError> {
    let mut rng = Rng(seed);
    let nodes = (count / 16).max(1);
    let node_keys = (0..nodes).map(|_| rng.v7()).collect::<Vec<_>>();
    let mut records = PartitionRecords::<ENDPOINT_WIDTH>::new(None, Some(count), 0)?;
    for edge in 0..count {
        #[allow(clippy::cast_precision_loss, reason = "synthetic skew draw")]
        let unit = (rng.next() >> 11) as f64 / (1_u64 << 53) as f64;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "bounded synthetic index"
        )]
        let node = ((unit.powi(4) * nodes as f64) as u64).min(nodes - 1);
        let mut record = [0_u8; ENDPOINT_WIDTH];
        record[..16].copy_from_slice(&node_keys[usize::try_from(node).map_err(storage)?]);
        record[16..24].copy_from_slice(&rng.next().to_be_bytes());
        record[24..32].copy_from_slice(&edge.to_be_bytes());
        records.push(record);
    }
    Ok(records)
}

/// Compact node details with short names, the ladder's common shape.
fn compact_node_details(count: u64, seed: u64) -> Result<PartitionRecords<272>, GfError> {
    let mut rng = Rng(seed);
    let mut records =
        PartitionRecords::<272>::new(Some(DetailCodec::Compact), Some(count), count * 21)?;
    for position in 0..count {
        let mut record = [0_u8; 272];
        record[..16].copy_from_slice(&rng.v7());
        let name = format!("n{}", position % 1000);
        record[16] = u8::try_from(name.len()).map_err(storage)?;
        record[17..17 + name.len()].copy_from_slice(name.as_bytes());
        records.push(record);
    }
    Ok(records)
}

#[derive(serde::Serialize)]
struct Observation {
    round: usize,
    mode: &'static str,
    wall_ns: u64,
    transient_peak_bytes: u64,
    output_sha256: String,
}

fn median(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

fn summarize(observations: &[Observation], modes: &[&'static str]) -> Value {
    let baseline = {
        let mut walls = observations
            .iter()
            .filter(|o| o.mode == modes[0])
            .map(|o| o.wall_ns)
            .collect::<Vec<_>>();
        median(&mut walls)
    };
    let mut summary = serde_json::Map::new();
    for mode in modes {
        let mut walls = observations
            .iter()
            .filter(|o| o.mode == *mode)
            .map(|o| o.wall_ns)
            .collect::<Vec<_>>();
        let mut peaks = observations
            .iter()
            .filter(|o| o.mode == *mode)
            .map(|o| o.transient_peak_bytes)
            .collect::<Vec<_>>();
        let median_wall = median(&mut walls);
        #[allow(clippy::cast_precision_loss, reason = "reporting ratio")]
        let ratio = median_wall as f64 / baseline as f64;
        summary.insert(
            (*mode).to_owned(),
            json!({
                "observations": walls.len(),
                "median_wall_ns": median_wall,
                "min_wall_ns": walls.first(),
                "max_wall_ns": walls.last(),
                "median_ratio_to_baseline": ratio,
                "median_transient_peak_bytes": median(&mut peaks),
            }),
        );
    }
    Value::Object(summary)
}

fn rotated(modes: &[Mode], round: usize) -> Vec<Mode> {
    let mut order = modes.to_vec();
    order.rotate_left(round % modes.len());
    order
}

/// Time `apply_mode` on freshly generated records for every mode.
fn sort_workload<const N: usize>(
    name: &str,
    modes: &[Mode],
    config: &SortPartitionBenchConfig,
    heap: &dyn HeapProbe,
    generate: impl Fn() -> Result<PartitionRecords<N>, GfError>,
) -> Result<Value, GfError> {
    let expected = {
        let mut records = generate()?;
        records.sort();
        records_digest(&records)
    };
    let mut observations = Vec::new();
    for round in 0..config.rounds {
        for mode in rotated(modes, round) {
            let mut records = generate()?;
            let live = heap.reset_peak();
            let started = Instant::now();
            apply_mode(&mut records, mode)?;
            let wall = started.elapsed();
            let peak = heap.peak().saturating_sub(live);
            let output = records_digest(&records);
            if output != expected {
                return Err(storage(format!("{name}: {} output differs", mode.as_str())));
            }
            observations.push(Observation {
                round,
                mode: mode.as_str(),
                wall_ns: u64::try_from(wall.as_nanos()).map_err(storage)?,
                transient_peak_bytes: peak,
                output_sha256: output,
            });
        }
    }
    let labels = modes.iter().map(|mode| mode.as_str()).collect::<Vec<_>>();
    Ok(json!({
        "workload": name,
        "record_width": N,
        "records": config.records,
        "summary": summarize(&observations, &labels),
        "observations": observations,
    }))
}

/// Arrow row partitions: the baseline extracts keys and sorts a `u32` order.
fn row_workload(config: &SortPartitionBenchConfig, heap: &dyn HeapProbe) -> Result<Value, GfError> {
    let generate = || -> Result<FixedSizeBinaryArray, GfError> {
        let mut rng = Rng(0x1506_0005);
        FixedSizeBinaryArray::try_from_iter((0..config.records).map(|_| rng.v7())).map_err(storage)
    };
    let baseline = |keys: &FixedSizeBinaryArray| -> Result<Vec<u32>, GfError> {
        // Mirrors `RowRangePartitioner::load_partition`'s baseline comparator.
        let rows = u32::try_from(keys.len()).map_err(storage)?;
        let mut order = (0..rows).collect::<Vec<_>>();
        let mut values = Vec::with_capacity(keys.len());
        for row in 0..keys.len() {
            let value: [u8; 16] = keys.value(row).try_into().map_err(storage)?;
            values.push(value);
        }
        order.sort_unstable_by_key(|index| values[*index as usize]);
        Ok(order)
    };
    let expected = digest(
        baseline(&generate()?)?
            .iter()
            .flat_map(|index| index.to_be_bytes()),
    );
    let modes = [Mode::Baseline, Mode::Arrow, Mode::DataFusion];
    let mut observations = Vec::new();
    for round in 0..config.rounds {
        for mode in rotated(&modes, round) {
            let keys = generate()?;
            let live = heap.reset_peak();
            let started = Instant::now();
            let order = match row_order_for_mode(&keys, mode)? {
                Some(order) => order.values().to_vec(),
                None => baseline(&keys)?,
            };
            let wall = started.elapsed();
            let peak = heap.peak().saturating_sub(live);
            let output = digest(order.iter().flat_map(|index| index.to_be_bytes()));
            if output != expected {
                return Err(storage(format!("rows: {} order differs", mode.as_str())));
            }
            observations.push(Observation {
                round,
                mode: mode.as_str(),
                wall_ns: u64::try_from(wall.as_nanos()).map_err(storage)?,
                transient_peak_bytes: peak,
                output_sha256: output,
            });
        }
    }
    let labels = modes.iter().map(|mode| mode.as_str()).collect::<Vec<_>>();
    Ok(json!({
        "workload": "row-partition-order",
        "record_width": 16,
        "records": config.records,
        "summary": summarize(&observations, &labels),
        "observations": observations,
    }))
}

/// Arrow's comparator kernel alone, without the adapter copies into and out
/// of Arrow: separates the kernel's cost from the representation boundary.
fn arrow_kernel_workload(
    config: &SortPartitionBenchConfig,
    heap: &dyn HeapProbe,
) -> Result<Value, GfError> {
    let generate = || -> Result<Vec<[u8; BASE_IDENTITY_WIDTH]>, GfError> {
        match unique_fixed::<BASE_IDENTITY_WIDTH>(config.records, 0x1506_0001)? {
            PartitionRecords::Fixed(records) => Ok(records),
            PartitionRecords::Details { .. } => Err(storage("expected fixed records")),
        }
    };
    let expected = {
        let mut records = generate()?;
        records.sort_unstable();
        digest(records.iter().flatten().copied())
    };
    let modes = ["baseline-sort-unstable", "arrow-sort-to-indices-kernel"];
    let mut observations = Vec::new();
    for round in 0..config.rounds {
        let order = if round % 2 == 0 {
            modes
        } else {
            [modes[1], modes[0]]
        };
        for mode in order {
            let mut records = generate()?;
            let (wall, peak) = if mode == modes[0] {
                let live = heap.reset_peak();
                let started = Instant::now();
                records.sort_unstable();
                (started.elapsed(), heap.peak().saturating_sub(live))
            } else {
                let array = FixedSizeBinaryArray::try_from_iter(records.iter()).map_err(storage)?;
                let live = heap.reset_peak();
                let started = Instant::now();
                let indices =
                    arrow::compute::sort_to_indices(&array, None, None).map_err(storage)?;
                let timed = (started.elapsed(), heap.peak().saturating_sub(live));
                records = indices
                    .values()
                    .iter()
                    .map(|index| records[*index as usize])
                    .collect();
                timed
            };
            let output = digest(records.iter().flatten().copied());
            if output != expected {
                return Err(storage(format!("kernel: {mode} output differs")));
            }
            observations.push(Observation {
                round,
                mode,
                wall_ns: u64::try_from(wall.as_nanos()).map_err(storage)?,
                transient_peak_bytes: peak,
                output_sha256: output,
            });
        }
    }
    Ok(json!({
        "workload": "identities-kernel-only",
        "record_width": BASE_IDENTITY_WIDTH,
        "records": config.records,
        "summary": summarize(&observations, &modes),
        "observations": observations,
    }))
}

/// Route and seal one hub partition exactly as shaping does.
fn seal_hub_spill(
    config: &SortPartitionBenchConfig,
) -> Result<(tempfile::TempDir, ConstructionDirectory, Vec<String>), GfError> {
    let project = tempfile::TempDir::new_in(&config.scratch).map_err(storage)?;
    let root = ConstructionDirectory::open(project.path()).map_err(storage)?;
    // Seeded exactly as a session open seeds its checkpoint evidence.
    let mut evidence = GraphConstructionEvidence::default();
    for category in crate::ArtifactCategory::ALL {
        evidence.storage_current.entry(category).or_default();
        evidence
            .storage_receipt_category_authorities
            .entry(category)
            .or_default();
        evidence
            .storage_transient_peak_allocated_bytes
            .entry(category)
            .or_default();
        evidence
            .storage_receipt_transient_peak_authorities
            .entry(category)
            .or_default();
    }
    let mut partitioner = FixedRangePartitioner::<ENDPOINT_WIDTH>::new(
        &root,
        PartitionFamily::Endpoints,
        1,
        None,
        false,
    )?;
    let mut wire = Vec::with_capacity(65_536 * ENDPOINT_WIDTH);
    let mut routed = 0_u64;
    for position in (0..config.hub_records).rev() {
        let mut record = [0_u8; ENDPOINT_WIDTH];
        record[..16].copy_from_slice(&0x1506_u128.to_be_bytes());
        record[24..32].copy_from_slice(&position.to_be_bytes());
        wire.extend_from_slice(&record);
        routed += 1;
        if routed == 65_536 || position == 0 {
            partitioner.route_slice(0, &wire, routed, &mut evidence)?;
            wire.clear();
            routed = 0;
        }
    }
    partitioner.seal(1, &mut evidence)?;
    drop(partitioner);
    Ok((
        project,
        root,
        vec![fixed_spill_name(PartitionFamily::Endpoints, 1, 0)],
    ))
}

/// One hub partition larger than the budget: the loader refuses it, an
/// admitted in-memory load (budget raised to fit) sorts it, and the bounded
/// external sort sorts it inside the original budget.
fn hub_workload(config: &SortPartitionBenchConfig, heap: &dyn HeapProbe) -> Result<Value, GfError> {
    let (_project, root, names) = seal_hub_spill(config)?;
    let partition_bytes = config.hub_records * ENDPOINT_WIDTH as u64;
    let stop = AtomicBool::new(false);

    let started = Instant::now();
    let refusal = load_fixed_partition::<ENDPOINT_WIDTH>(
        &root,
        &names,
        Some(config.hub_records),
        None,
        config.budget_bytes,
        &stop,
    )
    .err()
    .ok_or_else(|| storage("hub partition was admitted inside the budget"))?;
    let refusal_ns = u64::try_from(started.elapsed().as_nanos()).map_err(storage)?;

    let mut observations = Vec::new();
    let mut outcomes = Vec::new();
    let mut expected: Option<String> = None;
    for round in 0..config.rounds {
        let order: [&str; 2] = if round % 2 == 0 {
            ["in-memory-raised-budget", "datafusion-external"]
        } else {
            ["datafusion-external", "in-memory-raised-budget"]
        };
        for mode in order {
            let live = heap.reset_peak();
            let started = Instant::now();
            let output = if mode == "datafusion-external" {
                let scratch = tempfile::TempDir::new_in(&config.scratch).map_err(storage)?;
                let mut hasher = Sha256::new();
                let outcome = external_sort_fixed_partition::<ENDPOINT_WIDTH>(
                    &root,
                    &names,
                    config.hub_records,
                    usize::try_from(config.budget_bytes).map_err(storage)?,
                    scratch.path(),
                    config.batch_records,
                    |record| {
                        hasher.update(record);
                        Ok(())
                    },
                )?;
                if std::fs::read_dir(scratch.path()).map_err(storage)?.count() != 0 {
                    return Err(storage("external sort left library scratch behind"));
                }
                outcomes.push(outcome);
                super::super::hex(&hasher.finalize())
            } else {
                let (records, _) = load_fixed_partition::<ENDPOINT_WIDTH>(
                    &root,
                    &names,
                    Some(config.hub_records),
                    None,
                    partition_bytes,
                    &stop,
                )?;
                records_digest(&records)
            };
            let wall = started.elapsed();
            let peak = heap.peak().saturating_sub(live);
            if expected.get_or_insert_with(|| output.clone()) != &output {
                return Err(storage(format!("hub: {mode} output differs")));
            }
            observations.push(Observation {
                round,
                mode,
                wall_ns: u64::try_from(wall.as_nanos()).map_err(storage)?,
                transient_peak_bytes: peak,
                output_sha256: output,
            });
        }
    }
    Ok(json!({
        "workload": "over-budget-hub-partition",
        "record_width": ENDPOINT_WIDTH,
        "records": config.hub_records,
        "partition_bytes": partition_bytes,
        "budget_bytes": config.budget_bytes,
        "batch_records": config.batch_records,
        "current_contract": {"outcome": "refused", "error": refusal.to_string(), "wall_ns": refusal_ns},
        "summary": summarize(&observations, &["in-memory-raised-budget", "datafusion-external"]),
        "external_sort_outcomes": outcomes,
        "observations": observations,
    }))
}

/// Recorded range splitters with local sorts versus DataFusion hash
/// repartitioning with local sorts and a global merge, single-threaded.
fn partition_workload(
    config: &SortPartitionBenchConfig,
    heap: &dyn HeapProbe,
) -> Result<Value, GfError> {
    let generate = || {
        let mut rng = Rng(0x1506_0007);
        (0..config.partition_records)
            .map(|_| rng.v7())
            .collect::<Vec<[u8; 16]>>()
    };
    let partitions = u32::try_from(config.partitions).map_err(storage)?;
    let range = |records: &[[u8; 16]]| -> Result<Vec<[u8; 16]>, GfError> {
        // A systematic sample, as the staged-domain sampler draws.
        let stride = (records.len() / (config.partitions * 64)).max(1);
        let mut sample = records.iter().step_by(stride).copied().collect::<Vec<_>>();
        sample.sort_unstable();
        let plan = PartitionPlan::from_sorted_sample(partitions, partitions, &sample)?;
        let mut routed = vec![Vec::new(); plan.partitions()];
        for key in records {
            routed[plan.partition_of(key)].push(*key);
        }
        let mut output = Vec::with_capacity(records.len());
        for mut partition in routed {
            partition.sort_unstable();
            output.extend(partition);
        }
        Ok(output)
    };
    let expected = {
        let mut records = generate();
        records.sort_unstable();
        digest(records.iter().flatten().copied())
    };
    let (_, inspected) =
        hash_repartition_then_merge(&generate(), config.partitions, config.batch_records, true)?;
    let mut observations = Vec::new();
    for round in 0..config.rounds {
        let order: [&str; 2] = if round % 2 == 0 {
            ["range-recorded-splitters", "datafusion-hash-merge"]
        } else {
            ["datafusion-hash-merge", "range-recorded-splitters"]
        };
        for mode in order {
            let records = generate();
            let live = heap.reset_peak();
            let started = Instant::now();
            let output = if mode == "range-recorded-splitters" {
                range(&records)?
            } else {
                hash_repartition_then_merge(
                    &records,
                    config.partitions,
                    config.batch_records,
                    false,
                )?
                .0
            };
            let wall = started.elapsed();
            let peak = heap.peak().saturating_sub(live);
            let output = digest(output.iter().flatten().copied());
            if output != expected {
                return Err(storage(format!("partition: {mode} output differs")));
            }
            observations.push(Observation {
                round,
                mode,
                wall_ns: u64::try_from(wall.as_nanos()).map_err(storage)?,
                transient_peak_bytes: peak,
                output_sha256: output,
            });
        }
    }
    Ok(json!({
        "workload": "range-vs-hash-partitioning",
        "record_width": 16,
        "records": config.partition_records,
        "partitions": config.partitions,
        "hash_partition_inspection": inspected,
        "summary": summarize(&observations, &["range-recorded-splitters", "datafusion-hash-merge"]),
        "observations": observations,
    }))
}

/// Run every workload and return one JSON evidence document.
///
/// # Errors
/// Returns an error if any candidate's output differs from the baseline's,
/// if the hub partition is admitted inside the budget, or on I/O failure.
pub fn run_sort_partition_bench(
    config: &SortPartitionBenchConfig,
    heap: &dyn HeapProbe,
) -> Result<Value, GfError> {
    let fixed_modes = Mode::ALL;
    let detail_modes = [Mode::Baseline, Mode::Arrow, Mode::DataFusion];
    let count = config.records;
    Ok(json!({
        "schema": "graphforge-sort-partition-spike-1506/1",
        "config": {
            "rounds": config.rounds,
            "records": config.records,
            "hub_records": config.hub_records,
            "budget_bytes": config.budget_bytes,
            "batch_records": config.batch_records,
            "partition_records": config.partition_records,
            "partitions": config.partitions,
        },
        "workloads": [
            sort_workload::<BASE_IDENTITY_WIDTH>("identities", &fixed_modes, config, heap, || unique_fixed(count, 0x1506_0001))?,
            sort_workload::<RESOLVED_ENDPOINT_WIDTH>("resolved-endpoints", &fixed_modes, config, heap, || unique_fixed(count, 0x1506_0002))?,
            sort_workload::<ENDPOINT_WIDTH>("power-law-endpoints", &fixed_modes, config, heap, || power_law_endpoints(count, 0x1506_0003))?,
            sort_workload::<272>("compact-node-details", &detail_modes, config, heap, || compact_node_details(count, 0x1506_0004))?,
            arrow_kernel_workload(config, heap)?,
            row_workload(config, heap)?,
            hub_workload(config, heap)?,
            partition_workload(config, heap)?,
        ],
    }))
}
