//! Property fragments are cut at the fixed write-time cap, and the cut changes
//! no query answer (#1388, slice E2).
//!
//! A construction import whose property column is several times the cap
//! publishes several fragments per route. Queries over it are checked against
//! answers computed from the input alone, covering projection, a filter on a
//! property, ORDER BY a property and aggregation. The manifest's declared
//! lengths bound what first-touch admission of any one fragment reads, so a
//! bounded property read costs at most touched fragments times the cap.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, RecordBatch, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge};
use graphforge_storage::property_overlay::{
    MAX_PROPERTY_FRAGMENT_BYTES, MAX_PROPERTY_FRAGMENT_ROWS,
};
use graphforge_storage::resolve_project_generation;

const NODES: usize = 3_000;
const PAYLOAD_BYTES: usize = 4096;
const BUCKETS: i64 = 5;

fn payload(node: usize) -> String {
    let mut state = (node as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = String::with_capacity(PAYLOAD_BYTES + 16);
    while out.len() < PAYLOAD_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out.truncate(PAYLOAD_BYTES);
    out
}

/// A permutation of `0..NODES`, so ORDER BY and equality have one answer.
fn rank(node: usize) -> i64 {
    ((node * 1_237) % NODES) as i64
}

fn bucket(node: usize) -> i64 {
    node as i64 % BUCKETS
}

fn build(path: &std::path::Path) {
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();
    let mut fields = CONSTRUCTION_NODE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("rank", DataType::Int64, true));
    fields.push(Field::new("bucket", DataType::Int64, true));
    fields.push(Field::new("payload", DataType::Utf8, true));
    let mut identities = FixedSizeBinaryBuilder::with_capacity(NODES, 16);
    for node in 0..NODES {
        identities
            .append_value((node as u128 + 1).to_be_bytes())
            .unwrap();
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(identities.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Entity"; NODES])),
            Arc::new(Int64Array::from_iter_values((0..NODES).map(rank))),
            Arc::new(Int64Array::from_iter_values((0..NODES).map(bucket))),
            Arc::new(StringArray::from_iter_values((0..NODES).map(payload))),
        ],
    )
    .unwrap();
    session.append_nodes("nodes", &batch).unwrap();
    session.seal_and_publish().unwrap();
}

fn column(batches: &[RecordBatch], name: &str, to: &DataType) -> ArrayRef {
    let arrays = batches
        .iter()
        .map(|batch| cast(batch.column_by_name(name).expect("column"), to).unwrap())
        .collect::<Vec<_>>();
    let refs = arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    arrow::compute::concat(&refs).unwrap()
}

fn ints(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let array = column(batches, name, &DataType::Int64);
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .map(Option::unwrap)
        .collect()
}

fn strings(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let array = column(batches, name, &DataType::Utf8);
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect()
}

#[test]
fn split_property_fragments_answer_every_query_shape_unchanged() {
    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    build(&path);

    // The import is split: several fragments, each within the cap by its
    // declared length, which is what first-touch admission reads.
    let inventory = resolve_project_generation(&path)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("compact generation declares an inventory");
    let fragments = inventory
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("properties/"))
        .collect::<Vec<_>>();
    let declared = fragments.iter().map(|file| file.byte_length).sum::<u64>();
    assert!(
        fragments.len() >= 3,
        "{} fragments, {declared} bytes",
        fragments.len()
    );
    assert!(NODES <= MAX_PROPERTY_FRAGMENT_ROWS * fragments.len());
    for file in &fragments {
        assert!(
            file.byte_length <= MAX_PROPERTY_FRAGMENT_BYTES,
            "{} is {} bytes",
            file.relative_path,
            file.byte_length
        );
    }

    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();

    // Projection of every property, ordered by one of them.
    let result = forge
        .execute("MATCH (n) RETURN n.rank AS rank, n.payload AS payload ORDER BY rank")
        .unwrap();
    let mut expected = (0..NODES)
        .map(|n| (rank(n), payload(n)))
        .collect::<Vec<_>>();
    expected.sort();
    let got = ints(&result.batches, "rank")
        .into_iter()
        .zip(strings(&result.batches, "payload"))
        .collect::<Vec<_>>();
    assert_eq!(got, expected);

    // Filter on a property, projecting a property from the same fragment.
    let target = 1_234_usize % NODES;
    let owner = (0..NODES).find(|n| rank(*n) == target as i64).unwrap();
    let result = forge
        .execute(&format!(
            "MATCH (n) WHERE n.rank = {target} RETURN n.payload AS payload"
        ))
        .unwrap();
    assert_eq!(strings(&result.batches, "payload"), vec![payload(owner)]);

    // ORDER BY a property with LIMIT.
    let result = forge
        .execute("MATCH (n) RETURN n.rank AS rank ORDER BY rank DESC LIMIT 5")
        .unwrap();
    let top = (NODES as i64 - 5..NODES as i64).rev().collect::<Vec<_>>();
    assert_eq!(ints(&result.batches, "rank"), top);

    // Aggregation across every fragment.
    let result = forge
        .execute(
            "MATCH (n) RETURN n.bucket AS bucket, count(n) AS c, sum(n.rank) AS s ORDER BY bucket",
        )
        .unwrap();
    let expected_counts = (0..BUCKETS)
        .map(|b| (0..NODES).filter(|n| bucket(*n) == b).count() as i64)
        .collect::<Vec<_>>();
    let expected_sums = (0..BUCKETS)
        .map(|b| {
            (0..NODES)
                .filter(|n| bucket(*n) == b)
                .map(rank)
                .sum::<i64>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ints(&result.batches, "bucket"),
        (0..BUCKETS).collect::<Vec<_>>()
    );
    assert_eq!(ints(&result.batches, "c"), expected_counts);
    assert_eq!(ints(&result.batches, "s"), expected_sums);
}
