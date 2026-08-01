//! Multi-label predicate semantics and deterministic scaling gates (#1275).

use arrow::array::{Array, BooleanArray, Int64Array, ListArray, StringArray, StructArray};
use graphforge_api::GraphForge;

fn label_clause(prefix: &str, count: usize) -> String {
    (0..count)
        .map(|index| format!(":{prefix}{index}"))
        .collect()
}

fn logical_section(explain: &str) -> &str {
    explain
        .split_once("LogicalPlan\n-----------\n")
        .expect("logical-plan section")
        .1
        .split_once("\n\nPhysicalPlan\n------------\n")
        .expect("physical-plan section")
        .0
}

fn predicate_work(label_count: usize) -> usize {
    let forge = GraphForge::new(None).expect("in-memory forge");
    let labels = label_clause("L", label_count);
    forge
        .execute(&format!("CREATE ({labels})"))
        .expect("multi-label fixture creates");
    let explain = forge
        .explain(&format!("MATCH (n{labels}) RETURN 1 AS value"))
        .expect("multi-label query explains");
    let logical = logical_section(&explain);
    assert!(!logical.contains("cypher_in"), "{logical}");
    assert!(!logical.contains("array_concat"), "{logical}");
    logical.matches("array_has(").count()
}

fn string_labels(batch: &arrow::record_batch::RecordBatch, column: &str) -> Vec<String> {
    let node = batch
        .column_by_name(column)
        .expect("node result column")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("node result is a struct");
    let labels = node
        .column_by_name("labels")
        .expect("node labels field")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("node labels are a list");
    let values = labels.value(0);
    let strings = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("node labels are strings");
    (0..strings.len())
        .map(|index| strings.value(index).to_owned())
        .collect()
}

#[test]
fn multi_label_predicate_work_is_linear() {
    let small = predicate_work(16);
    let large = predicate_work(32);
    assert_eq!(small, 16);
    assert_eq!(large, 32);
    assert!(large <= small * 3, "small={small}, large={large}");
}

#[test]
fn many_label_tck_shape_returns_complete_node_values() {
    let forge = GraphForge::new(None).expect("in-memory forge");
    forge
        .execute(
            "CREATE (a:A:B:C:D:E:F:G:H:I:J:K:L:M), (b:U:V:W:X:Y:Z) \
             CREATE (a)-[:T]->(b)",
        )
        .expect("TCK-shaped fixture creates");
    let result = forge
        .execute(
            "MATCH (n:A:B:C:D:E:F:G:H:I:J:K:L:M)-[:T]->(m:Z:Y:X:W:V:U) \
             RETURN n, m",
        )
        .expect("TCK-shaped query executes");
    assert_eq!(result.stats.rows_produced, 1);
    let batch = &result.batches[0];
    assert_eq!(
        string_labels(batch, "n"),
        [
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"
        ]
    );
    assert_eq!(string_labels(batch, "m"), ["U", "V", "W", "X", "Y", "Z"]);
}

#[test]
fn label_membership_preserves_present_absent_dynamic_and_null_results() {
    let forge = GraphForge::new(None).expect("in-memory forge");
    forge
        .execute("CREATE (:Known), (:Other)")
        .expect("label fixture creates");

    let result = forge
        .execute(
            "MATCH (n:Known) \
             WITH n, 'Known' AS wanted \
             RETURN 'Known' IN labels(n) AS present, \
                    'Other' IN labels(n) AS absent, \
                    'Unmapped' IN labels(n) AS unmapped, \
                    wanted IN labels(n) AS dynamic",
        )
        .expect("label membership query executes");
    let batch = &result.batches[0];
    for (name, expected) in [
        ("present", true),
        ("absent", false),
        ("unmapped", false),
        ("dynamic", true),
    ] {
        let values = batch
            .column_by_name(name)
            .expect("membership result")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("membership result is boolean");
        assert!(!values.is_null(0), "{name}");
        assert_eq!(values.value(0), expected, "{name}");
    }

    let duplicate = forge
        .execute("MATCH (n:Known:Known) RETURN count(*) AS count")
        .expect("duplicate label query executes");
    assert_eq!(duplicate.stats.rows_produced, 1);
    assert_eq!(
        duplicate.batches[0]
            .column_by_name("count")
            .expect("duplicate match count")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64")
            .value(0),
        1
    );

    let optional = forge
        .execute(
            "OPTIONAL MATCH (n:Missing) \
             RETURN 'Missing' IN labels(n) AS membership",
        )
        .expect("optional label query executes");
    let values = optional.batches[0]
        .column_by_name("membership")
        .expect("optional membership result")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("optional membership is boolean");
    assert!(values.is_null(0));
}
