//! Diagnostic sparse weighted MST measurements through the public Rust facade.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    AnalyzeAlgorithm, AnalyzeOptions, GraphForge, OperationId, bulk_edge_input_schema,
    bulk_node_input_schema,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

fn id(value: u128) -> Uuid {
    Uuid::from_u128(0x018f_0f4e_7b8c_7000_8000_0000_0000_0000 | value)
}

fn identities(values: impl IntoIterator<Item = u128>) -> FixedSizeBinaryArray {
    let mut builder = FixedSizeBinaryBuilder::new(16);
    for value in values {
        builder.append_value(id(value).as_bytes()).unwrap();
    }
    builder.finish()
}

fn main() {
    let sizes = std::env::args()
        .skip(1)
        .map(|arg| arg.parse::<usize>().expect("node count must be an integer"))
        .collect::<Vec<_>>();
    let sizes = if sizes.is_empty() {
        vec![1_000, 10_000, 100_000]
    } else {
        sizes
    };
    for n in sizes {
        assert!(n > 17);
        let graph = GraphForge::new(None).unwrap();
        let nodes = RecordBatch::try_new(
            bulk_node_input_schema(Vec::new()).unwrap(),
            vec![
                Arc::new(identities(0..n as u128)),
                Arc::new(StringArray::from(vec!["Person"; n])),
            ],
        )
        .unwrap();
        for (chunk, offset) in (0..n).step_by(16_384).enumerate() {
            graph
                .publish_bulk_nodes(
                    OperationId(id((3 * n + chunk) as u128)),
                    &[nodes.slice(offset, (n - offset).min(16_384))],
                )
                .unwrap();
        }
        let mut sources = Vec::with_capacity(2 * n);
        let mut targets = Vec::with_capacity(2 * n);
        let mut weights = Vec::with_capacity(2 * n);
        let mut digest = Sha256::new();
        for i in 0..n {
            for k in [1, 17] {
                let weight = (n - i) as f64 + if k == 1 { 0.5 } else { 0.0 };
                sources.push(i as u128);
                targets.push(((i + k) % n) as u128);
                weights.push(weight);
                digest.update(id((n + 2 * i + usize::from(k == 17)) as u128).as_bytes());
                digest.update(id(i as u128).as_bytes());
                digest.update(id(((i + k) % n) as u128).as_bytes());
                digest.update(weight.to_bits().to_be_bytes());
            }
        }
        let edges = RecordBatch::try_new(
            bulk_edge_input_schema(vec![Field::new("w", DataType::Float64, false)]).unwrap(),
            vec![
                Arc::new(identities(n as u128..3 * n as u128)),
                Arc::new(StringArray::from(vec!["LINK"; 2 * n])),
                Arc::new(identities(sources)),
                Arc::new(identities(targets)),
                Arc::new(Float64Array::from(weights)),
            ],
        )
        .unwrap();
        for (chunk, offset) in (0..2 * n).step_by(16_384).enumerate() {
            graph
                .publish_bulk_edges(
                    OperationId(id((4 * n + chunk) as u128)),
                    &[edges.slice(offset, (2 * n - offset).min(16_384))],
                )
                .unwrap();
        }
        let options = AnalyzeOptions {
            by: AnalyzeAlgorithm::MinimumSpanningTree,
            directed: false,
            weight: Some("w".into()),
            ..AnalyzeOptions::default()
        };
        let start = Instant::now();
        let result = graph.analyze(Some("Person"), options).unwrap();
        let elapsed = start.elapsed();
        assert_eq!(result.num_rows(), n - 1);
        let weights = result
            .column_by_name("weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ids = ["edge_uuid", "source_uuid", "target_uuid"].map(|name| {
            result
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
        });
        let mut output_digest = Sha256::new();
        for row in 0..result.num_rows() {
            for column in ids {
                output_digest.update(column.value(row));
            }
            output_digest.update(weights.value(row).to_bits().to_be_bytes());
        }
        println!(
            "nodes={n} edges={} ms={:.3} rows={} weight={:.1} input_sha256={} output_sha256={}",
            2 * n,
            elapsed.as_secs_f64() * 1_000.0,
            result.num_rows(),
            weights.values().iter().sum::<f64>(),
            digest
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            output_digest
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
    }
}
