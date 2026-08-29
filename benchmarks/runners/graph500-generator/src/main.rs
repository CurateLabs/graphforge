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
    write_nodes(&config.nodes, node_count)?;
    write_edges(
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

fn writer(path: &Path, schema: Arc<Schema>) -> Result<ArrowWriter<File>, String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    ArrowWriter::try_new(
        File::create(path).map_err(|error| error.to_string())?,
        schema,
        None,
    )
    .map_err(|error| error.to_string())
}

fn write_nodes(path: &Path, count: u64) -> Result<(), String> {
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
    output.close().map_err(|error| error.to_string())?;
    Ok(())
}

fn write_edges(path: &Path, scale: u32, count: u64, seed: u64) -> Result<(), String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("rel_type", DataType::Utf8, false),
        Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
    ]));
    let mut output = writer(path, Arc::clone(&schema))?;
    let mut random = SplitMix64(seed);
    for start in (0..count).step_by(BATCH_ROWS as usize) {
        let end = count.min(start + BATCH_ROWS);
        let rows = usize::try_from(end - start).map_err(|_| "edge batch overflow")?;
        let mut sources = Vec::with_capacity(rows);
        let mut targets = Vec::with_capacity(rows);
        for _ in start..end {
            let (source, target) = rmat_edge(scale, &mut random);
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
    output.close().map_err(|error| error.to_string())?;
    Ok(())
}

fn rmat_edge(scale: u32, random: &mut SplitMix64) -> (u64, u64) {
    let mut source = 0_u64;
    let mut target = 0_u64;
    for bit in (0..scale).rev() {
        let choice = random.next() % (A + B + C + D);
        if choice >= A {
            if choice < A + B {
                target |= 1_u64 << bit;
            } else if choice < A + B + C {
                source |= 1_u64 << bit;
            } else {
                source |= 1_u64 << bit;
                target |= 1_u64 << bit;
            }
        }
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
}
