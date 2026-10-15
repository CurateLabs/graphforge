//! Wide property intermediates share the facade's query pool even when the
//! final answer is a single count.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, GraphConstructionBudgets, GraphForge,
    GraphForgeOptions, ResourcePolicyMode,
};

const ROWS: usize = 1024;
const WIDTH: usize = 8192;

#[test]
fn property_scan_mixed_scalars_preserve_types_and_nullable_predicates_after_reopen() {
    use graphforge_value::Literal;

    let root = tempfile::tempdir().unwrap();
    let writer = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
    writer
        .execute(
            "CREATE (root:ScanRoot), (a:ScanValue {slot:0,value:7}), \
             (b:ScanValue {slot:1,value:2.5}), (c:ScanValue {slot:2,value:'xx'}), \
             (d:ScanValue {slot:3,value:true}), (e:ScanValue {slot:4}), \
             (root)-[:R]->(a), (root)-[:R]->(b), (root)-[:R]->(c), \
             (root)-[:R]->(d), (root)-[:R]->(e)",
        )
        .unwrap();
    drop(writer);

    let reader = open(root.path(), 256 * 1024 * 1024);
    let result = reader
        .execute(
            "MATCH (:ScanRoot)-[:R]->(n:ScanValue) \
             RETURN n.value AS value ORDER BY n.slot",
        )
        .unwrap();
    let mut actual = Vec::new();
    for batch in result.batches {
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("mixed property types retain their native scalar representation");
        for row in 0..values.len() {
            actual.push(graphforge_value::heterogeneous::decode_scalar(values, row).unwrap());
        }
    }
    assert_eq!(
        actual,
        vec![
            Literal::Int(7),
            Literal::Float(2.5),
            Literal::Str("xx".into()),
            Literal::Bool(true),
            Literal::Null,
        ]
    );
    for (predicate, expected) in [
        ("n.value IS NOT NULL", 4),
        ("n.value IS NULL OR n.value > 'x'", 2),
    ] {
        let result = reader
            .execute(&format!(
                "MATCH (:ScanRoot)-[:R]->(n:ScanValue) \
                 WHERE {predicate} RETURN count(*) AS matches"
            ))
            .unwrap();
        assert_eq!(
            result
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        let count = result.batches[0]
            .column_by_name("matches")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), expected, "{predicate}");
    }
}

fn open(path: &std::path::Path, bytes: u64) -> GraphForge {
    GraphForge::new_with_options(
        Some(path.to_str().unwrap()),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                memory_budget_bytes: Some(bytes),
                target_partitions: Some(1),
                tokio_worker_threads: Some(2),
                io_concurrency: Some(1),
                compute_threads: Some(1),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

#[test]
fn property_predicate_count_uses_query_pool_and_fitting_reopen_is_exact() {
    let root = tempfile::tempdir().unwrap();
    let forge = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("id", DataType::Int64, false)));
    fields.push(Arc::new(Field::new("payload", DataType::Utf8, false)));
    let schema = Arc::new(Schema::new(fields));
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(ROWS, 16);
    for row in 0..ROWS {
        uuids.append_value((row as u128 + 1).to_be_bytes()).unwrap();
    }
    let text = "x".repeat(WIDTH);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(uuids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Item"; ROWS])),
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
            Arc::new(StringArray::from(vec![text.as_str(); ROWS])),
        ],
    )
    .unwrap();
    let mut construction = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();
    construction.append_nodes("items", &batch).unwrap();
    construction.seal_and_publish().unwrap();
    drop(forge);

    let query = "MATCH (n:Item) WHERE size(n.payload) = 8192 RETURN count(n) AS matched";
    let small = open(root.path(), 16 * 1024 * 1024);
    let error = small
        .execute(query)
        .expect_err("wide intermediate must be admitted");
    let message = error.to_string();
    assert!(
        message.contains("PropertyScan") && message.contains("GF_RESOURCE_LIMIT"),
        "{message}"
    );
    drop(small);

    let roomy = open(root.path(), 256 * 1024 * 1024);
    let result = roomy.execute(query).unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    let count = result.batches[0]
        .column_by_name("matched")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), ROWS as i64);

    // A selective persisted equality read remains exact through the same
    // admitted manifest/shard/selection path; its payload is small enough.
    let anchor = roomy
        .execute("MATCH (n:Item {id: 7}) RETURN n.payload AS payload")
        .unwrap();
    assert_eq!(
        anchor
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    let payload = anchor
        .batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .unwrap()
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(payload.value(0), text);
}
