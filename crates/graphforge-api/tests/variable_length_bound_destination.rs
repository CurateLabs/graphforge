//! A variable-length expansion whose destination is already bound keeps only the
//! paths that end at that node; the traversal drops the others before
//! materialising them (#1986). The answer must equal the unpushed form.
use arrow::array::{Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use graphforge_api::GraphForge;

fn rows(forge: &GraphForge, query: &str) -> Vec<Vec<i64>> {
    let result = forge.execute(query).unwrap();
    let mut rows = Vec::new();
    for batch in &result.batches {
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| cast(c, &DataType::Int64).unwrap())
            .collect();
        let columns: Vec<&Int64Array> = columns
            .iter()
            .map(|c| c.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            rows.push(columns.iter().map(|c| c.value(row)).collect());
        }
    }
    rows
}

#[test]
fn bound_destination_returns_exactly_the_paths_ending_there() {
    let forge = GraphForge::new(None).unwrap();
    // A ring of eight nodes plus chords, so several paths reach the same node.
    forge
        .execute("UNWIND range(0, 7) AS i CREATE (:N {id: i})")
        .unwrap();
    forge
        .execute(
            "MATCH (a:N), (b:N) WHERE b.id = (a.id + 1) % 8 OR b.id = (a.id + 3) % 8 \
             CREATE (a)-[:E]->(b)",
        )
        .unwrap();

    // Destination bound by an earlier MATCH, then compared after the expansion.
    let bound = rows(
        &forge,
        "MATCH (x:N {id: 5}) MATCH (o:N)-[r:E*1..3]->(x) \
         RETURN o.id AS origin, length(r) AS len, count(*) AS c ORDER BY origin, len",
    );
    let compared = rows(
        &forge,
        "MATCH (o:N)-[r:E*1..3]->(d:N) WHERE d.id = 5 \
         RETURN o.id AS origin, length(r) AS len, count(*) AS c ORDER BY origin, len",
    );
    assert!(
        bound.iter().map(|row| row[2]).sum::<i64>() > 1,
        "several distinct paths must reach the node"
    );
    assert_eq!(bound, compared);
}
