//! A variable-length expand copies each seed row once per emitted path. When the
//! seed row carries a list, one output batch holding every path would exceed
//! Arrow's 32-bit list offsets (`offset overflow`, #1986), as the FinBench TCR2
//! path-list intermediates did. The expand must stream bounded batches instead
//! and return the exact rows.
use arrow::array::{Array, Int64Array};
use graphforge_api::GraphForge;

const PATHS: i64 = 21_600;
const LIST_LEN: i64 = 100_000;

#[test]
fn wide_seed_rows_never_overflow_arrow_offsets() {
    // One batch holding every path would hold PATHS copies of the list, more
    // child elements than a 32-bit offset can address.
    assert!(
        PATHS * LIST_LEN > i64::from(i32::MAX),
        "the scenario must exceed 32-bit list offsets in a single batch"
    );
    let forge = GraphForge::new(None).unwrap();
    forge.execute("CREATE (:Seed {name: 'hub'})").unwrap();
    forge
        .execute(&format!(
            "MATCH (s:Seed) UNWIND range(1, {PATHS}) AS i CREATE (s)-[:E]->(:Leaf {{n: i}})"
        ))
        .unwrap();

    let result = forge
        .execute(&format!(
            "MATCH (s:Seed) WITH s, [i IN range(1, {LIST_LEN}) | i % 2 = 0] AS w \
             MATCH (s)-[:E*1..2]->(b:Leaf) \
             RETURN count(*) AS paths, sum(size(w)) AS elements"
        ))
        .expect("wide variable-length expand must answer, not panic");

    let batch = &result.batches[0];
    let column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(column(0), PATHS);
    assert_eq!(column(1), PATHS * LIST_LEN);
}
