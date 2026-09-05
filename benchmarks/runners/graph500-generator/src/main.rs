#![forbid(unsafe_code)]

use std::env;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{FixedSizeBinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

const BATCH_ROWS: u64 = 65_536;
const A: u64 = 57;
const B: u64 = 19;
const C: u64 = 19;
const D: u64 = 5;
// Separate global label keys from the per-tuple quadrant stream (ASCII GRAPH500).
const SCRAMBLE_DOMAIN: u64 = 0x4752_4150_4835_3030;

#[derive(Debug)]
struct Config {
    scale: u32,
    edge_factor: u64,
    seed: u64,
    nodes: PathBuf,
    edges: PathBuf,
}

fn main() -> Result<(), String> {
    let config = parse_args()?;
    if !(1..=26).contains(&config.scale) || config.edge_factor != 16 {
        return Err("scale must be 1..26 and edge factor must be 16".to_owned());
    }
    let node_count = 1_u64 << config.scale;
    let _node_cache_release = write_nodes(&config.nodes, node_count)?;
    let _edge_cache_release = write_edges(
        &config.edges,
        config.scale,
        node_count.saturating_mul(config.edge_factor),
        config.seed,
    )?;
    Ok(())
}

fn parse_args() -> Result<Config, String> {
    let mut values = env::args().skip(1);
    let mut scale = None;
    let mut edge_factor = None;
    let mut seed = None;
    let mut nodes = None;
    let mut edges = None;
    while let Some(flag) = values.next() {
        let value = values
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--scale" => scale = Some(value.parse().map_err(|_| "invalid scale")?),
            "--edge-factor" => {
                edge_factor = Some(value.parse().map_err(|_| "invalid edge factor")?)
            }
            "--seed" => seed = Some(value.parse().map_err(|_| "invalid seed")?),
            "--nodes" => nodes = Some(PathBuf::from(value)),
            "--edges" => edges = Some(PathBuf::from(value)),
            _ => return Err(format!("unknown argument: {flag}")),
        }
    }
    Ok(Config {
        scale: scale.ok_or("missing --scale")?,
        edge_factor: edge_factor.ok_or("missing --edge-factor")?,
        seed: seed.ok_or("missing --seed")?,
        nodes: nodes.ok_or("missing --nodes")?,
        edges: edges.ok_or("missing --edges")?,
    })
}

fn node_uuid(index: u64) -> [u8; 16] {
    entity_uuid(0x10, index)
}

fn edge_uuid(index: u64) -> [u8; 16] {
    entity_uuid(0x20, index)
}

fn entity_uuid(namespace: u8, index: u64) -> [u8; 16] {
    let mut value = [0_u8; 16];
    value[0] = namespace;
    value[6] = 0x70;
    value[8] = 0x80;
    value[8..].copy_from_slice(&(index | (1_u64 << 63)).to_be_bytes());
    value
}

fn binary(values: impl Iterator<Item = [u8; 16]>) -> Result<FixedSizeBinaryArray, String> {
    FixedSizeBinaryArray::try_from_iter(values.map(|value| value.to_vec()))
        .map_err(|error| error.to_string())
}

type DurableArrowWriter = ArrowWriter<graphforge_filesystem::DurableFileCacheWriter>;

fn writer(path: &Path, schema: Arc<Schema>) -> Result<DurableArrowWriter, String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let output = graphforge_filesystem::DurableFileCacheWriter::new(
        File::create(path).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    ArrowWriter::try_new(output, schema, None).map_err(|error| error.to_string())
}

fn finish_writer(
    output: DurableArrowWriter,
) -> Result<graphforge_filesystem::FileCacheReleaseEvidence, String> {
    // `into_inner` finalizes and flushes the Parquet footer before the durable
    // writer synchronizes and releases the final clean cache window.
    let mut output = output.into_inner().map_err(|error| error.to_string())?;
    output
        .sync_all_and_release()
        .map_err(|error| error.to_string())?;
    Ok(output.evidence())
}

fn write_nodes(
    path: &Path,
    count: u64,
) -> Result<graphforge_filesystem::FileCacheReleaseEvidence, String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
    ]));
    let mut output = writer(path, Arc::clone(&schema))?;
    for start in (0..count).step_by(BATCH_ROWS as usize) {
        let end = count.min(start + BATCH_ROWS);
        let rows = usize::try_from(end - start).map_err(|_| "node batch overflow")?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(binary((start..end).map(node_uuid))?),
                Arc::new(StringArray::from(vec!["Node"; rows])),
            ],
        )
        .map_err(|error| error.to_string())?;
        output.write(&batch).map_err(|error| error.to_string())?;
    }
    finish_writer(output)
}

fn write_edges(
    path: &Path,
    scale: u32,
    count: u64,
    seed: u64,
) -> Result<graphforge_filesystem::FileCacheReleaseEvidence, String> {
    write_edges_in_batches(path, scale, count, seed, BATCH_ROWS)
}

fn write_edges_in_batches(
    path: &Path,
    scale: u32,
    count: u64,
    seed: u64,
    batch_rows: u64,
) -> Result<graphforge_filesystem::FileCacheReleaseEvidence, String> {
    assert!(batch_rows > 0);
    let schema = Arc::new(Schema::new(vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("rel_type", DataType::Utf8, false),
        Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
    ]));
    let mut output = writer(path, Arc::clone(&schema))?;
    let mut generator = EdgeGenerator::new(scale, seed);
    for start in (0..count).step_by(batch_rows as usize) {
        let end = count.min(start + batch_rows);
        let rows = usize::try_from(end - start).map_err(|_| "edge batch overflow")?;
        let mut sources = Vec::with_capacity(rows);
        let mut targets = Vec::with_capacity(rows);
        for _ in start..end {
            let (source, target) = generator.next();
            sources.push(node_uuid(source));
            targets.push(node_uuid(target));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(binary((start..end).map(edge_uuid))?),
                Arc::new(StringArray::from(vec!["EDGE"; rows])),
                Arc::new(binary(sources.into_iter())?),
                Arc::new(binary(targets.into_iter())?),
            ],
        )
        .map_err(|error| error.to_string())?;
        output.write(&batch).map_err(|error| error.to_string())?;
    }
    finish_writer(output)
}

struct EdgeGenerator {
    scale: u32,
    random: SplitMix64,
    keys: (u64, u64),
}

impl EdgeGenerator {
    fn new(scale: u32, seed: u64) -> Self {
        let mut key_stream = SplitMix64(seed ^ SCRAMBLE_DOMAIN);
        Self {
            scale,
            random: SplitMix64(seed),
            keys: (key_stream.next(), key_stream.next()),
        }
    }

    fn next(&mut self) -> (u64, u64) {
        // Independent pseudorandom tuples already have randomized presentation;
        // do not sort, deduplicate, remove loops, or replenish rejected edges.
        let (source, target) = rmat_edge(self.scale, &mut self.random);
        (
            scramble(source, self.scale, self.keys),
            scramble(target, self.scale, self.keys),
        )
    }
}

// Port of graph500-3.0.0 generator/graph_generator.c::scramble, commit
// 9dbb76c7db4d00dc12fc44d02ba8bd2532236292.
// Copyright (C) 2009-2010 The Trustees of Indiana University.
// Authors: Jeremiah Willcock, Andrew Lumsdaine.
// Distributed under the Boost Software License, Version 1.0; see ../LICENSE_1_0.txt.
fn scramble(vertex: u64, scale: u32, (key0, key1): (u64, u64)) -> u64 {
    debug_assert!((1..=63).contains(&scale));
    debug_assert!(vertex < (1_u64 << scale));
    let mut value = vertex.wrapping_add(key0).wrapping_add(key1);
    value = value.wrapping_mul(key0 | 0x4519_8402_1149_3211);
    value = value.reverse_bits() >> (64 - scale);
    value = value.wrapping_mul(key1 | 0x3050_8521_02C8_43A5);
    value.reverse_bits() >> (64 - scale)
}

fn sample_quadrant(mut next: impl FnMut() -> u64) -> (bool, bool) {
    const TOTAL: u64 = A + B + C + D;
    // Discard the short residue class: the accepted range has a length exactly
    // divisible by 100. Plain modulo would bias the initiator probabilities.
    const REJECT_BELOW: u64 = TOTAL.wrapping_neg() % TOTAL;
    let choice = loop {
        let value = next();
        if value >= REJECT_BELOW {
            break value % TOTAL;
        }
    };
    match choice {
        0..A => (false, false),
        value if value < A + B => (false, true),
        value if value < A + B + C => (true, false),
        _ => (true, true),
    }
}

fn rmat_edge(scale: u32, random: &mut SplitMix64) -> (u64, u64) {
    let mut source = 0_u64;
    let mut target = 0_u64;
    for bit in (0..scale).rev() {
        let (source_bit, target_bit) = sample_quadrant(|| random.next());
        source |= u64::from(source_bit) << bit;
        target |= u64::from(target_bit) << bit;
    }
    (source, target)
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector_rows(text: &str) -> impl Iterator<Item = Vec<u64>> + '_ {
        text.lines()
            .filter(|line| !line.starts_with('#'))
            .map(|line| {
                line.split_whitespace()
                    .map(|value| value.parse().unwrap())
                    .collect()
            })
    }

    #[test]
    fn scramble_matches_pinned_upstream_c_vectors() {
        for row in vector_rows(include_str!("../tests/scramble-vectors.tsv")) {
            assert_eq!(scramble(row[1], row[0] as u32, (row[2], row[3])), row[4]);
        }
    }

    #[test]
    fn complete_generator_matches_independent_vectors() {
        let mut generator = EdgeGenerator::new(1, 1);
        for row in vector_rows(include_str!("../tests/generator-vectors.tsv")) {
            if row[2] == 0 {
                generator = EdgeGenerator::new(row[0] as u32, row[1]);
            }
            assert_eq!(generator.next(), (row[3], row[4]), "{row:?}");
        }
    }

    #[test]
    fn sample_rejects_short_residue_class_and_obeys_quadrant_boundaries() {
        for rejected in 0..16 {
            let mut values = [rejected, 100].into_iter();
            assert_eq!(sample_quadrant(|| values.next().unwrap()), (false, false));
            assert_eq!(values.next(), None);
        }
        for (value, expected) in [
            (16, (false, false)),
            (156, (false, false)),
            (157, (false, true)),
            (175, (false, true)),
            (176, (true, false)),
            (194, (true, false)),
            (195, (true, true)),
            (199, (true, true)),
            (u64::MAX, (false, false)),
        ] {
            assert_eq!(sample_quadrant(|| value), expected);
        }
        let mut values = [0, 15, 0, 199].into_iter();
        assert_eq!(sample_quadrant(|| values.next().unwrap()), (true, true));
        assert_eq!(values.next(), None);
    }

    #[test]
    fn scramble_is_a_bijection_at_every_small_scale() {
        for seed in [0, 1, 7, u64::MAX] {
            for scale in 1..=12 {
                let generator = EdgeGenerator::new(scale, seed);
                let mut seen = vec![false; 1 << scale];
                for vertex in 0..(1 << scale) {
                    let output = scramble(vertex, scale, generator.keys) as usize;
                    assert!(!seen[output], "collision: seed={seed}, scale={scale}");
                    seen[output] = true;
                }
                assert!(seen.into_iter().all(|present| present));
            }
        }
    }

    #[test]
    fn scale26_endpoints_and_identifiers_preserve_wide_indices() {
        let mut generator = EdgeGenerator::new(26, u64::MAX);
        for _ in 0..4096 {
            let (source, target) = generator.next();
            assert!(source < (1 << 26) && target < (1 << 26));
        }
        // The supported generation limit is SCALE26. The UUID representation
        // independently carries at least 48 bits without truncation/aliasing.
        for index in [0, (1 << 26) - 1, (1 << 30) - 1, (1 << 48) - 1, 1 << 48] {
            for uuid in [node_uuid(index), edge_uuid(index)] {
                let encoded = u64::from_be_bytes(uuid[8..].try_into().unwrap());
                assert_eq!(encoded & !(1 << 63), index);
                assert_eq!(uuid[6] >> 4, 7);
                assert_eq!(uuid[8] >> 6, 2);
            }
            assert_ne!(node_uuid(index), edge_uuid(index));
        }
        assert_eq!(
            node_uuid(0),
            [16, 0, 0, 0, 0, 0, 112, 0, 128, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    type EdgeRow = ([u8; 16], [u8; 16], [u8; 16]);

    fn parquet_edges(path: &Path) -> Vec<EdgeRow> {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(path).unwrap(),
        )
        .unwrap()
        .with_batch_size(113)
        .build()
        .unwrap();
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let ids = [0, 2, 3].map(|column| {
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
            });
            for index in 0..batch.num_rows() {
                rows.push((
                    ids[0].value(index).try_into().unwrap(),
                    ids[1].value(index).try_into().unwrap(),
                    ids[2].value(index).try_into().unwrap(),
                ));
            }
        }
        rows
    }

    #[test]
    fn parquet_retains_exact_raw_loops_duplicates_and_distinct_edge_ids() {
        let root = tempfile::tempdir().unwrap();
        let edges = root.path().join("edges.parquet");
        write_edges(&edges, 1, 32, 1).unwrap();
        let rows = parquet_edges(&edges);
        assert_eq!(rows.len(), 32);
        let expected: Vec<EdgeRow> = vector_rows(include_str!("../tests/generator-vectors.tsv"))
            .take(32)
            .map(|row| (edge_uuid(row[2]), node_uuid(row[3]), node_uuid(row[4])))
            .collect();
        assert_eq!(rows, expected);
        let mut pairs = std::collections::HashSet::new();
        let mut loops = 0;
        let mut duplicates = 0;
        for (index, (id, source, target)) in rows.into_iter().enumerate() {
            assert_eq!(id, edge_uuid(index as u64));
            loops += usize::from(source == target);
            duplicates += usize::from(!pairs.insert((source, target)));
        }
        assert!(loops > 0);
        assert!(duplicates > 0);
    }

    #[test]
    fn actual_parquet_tuple_order_is_independent_of_output_batch_boundaries() {
        let root = tempfile::tempdir().unwrap();
        let count = 16 * (1 << 13);
        let baseline = root.path().join("default.parquet");
        write_edges(&baseline, 13, count, 7).unwrap();
        let expected = parquet_edges(&baseline);
        assert_eq!(expected.len(), count as usize);
        for batch_rows in [97, count] {
            let path = root.path().join(format!("batch-{batch_rows}.parquet"));
            write_edges_in_batches(&path, 13, count, 7, batch_rows).unwrap();
            assert_eq!(parquet_edges(&path), expected);
        }
    }

    #[test]
    fn generator_is_deterministic_and_namespaced() {
        let mut left = SplitMix64(7);
        let mut right = SplitMix64(7);
        let first: Vec<_> = (0..8).map(|_| rmat_edge(6, &mut left)).collect();
        let second: Vec<_> = (0..8).map(|_| rmat_edge(6, &mut right)).collect();
        assert_eq!(first, second);
        assert_ne!(node_uuid(1), edge_uuid(1));
        assert!(
            first
                .iter()
                .all(|(source, target)| *source < 64 && *target < 64)
        );
    }

    #[test]
    fn generator_outputs_are_finalized_before_bounded_durable_release() {
        let root = tempfile::tempdir().unwrap();
        let nodes = root.path().join("nodes.parquet");
        let edges = root.path().join("edges.parquet");
        let node_evidence = write_nodes(&nodes, 128).unwrap();
        let edge_evidence = write_edges(&edges, 7, 256, 13_907_095_936_298_285_200).unwrap();

        for (path, evidence) in [(&nodes, node_evidence), (&edges, edge_evidence)] {
            assert!(path.metadata().unwrap().len() > 0);
            assert!(
                evidence.peak_window_bytes
                    <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
            );
            assert!(evidence.sync_operations > 0);
            #[cfg(target_os = "linux")]
            {
                assert!(evidence.release_operations > 0);
                assert_eq!(evidence.unsupported_operations, 0);
            }
            #[cfg(not(target_os = "linux"))]
            assert!(evidence.unsupported_operations > 0);

            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            assert!(
                reader
                    .map(Result::unwrap)
                    .map(|batch| batch.num_rows())
                    .sum::<usize>()
                    > 0
            );
        }
    }
}
