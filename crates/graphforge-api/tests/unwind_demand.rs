//! UNWIND copies only the input columns its parent reads (#1984, cause of
//! #1982).
//!
//! Exploding a list repeats every input column once per element. When the
//! input rows carry a wide struct (the friends a query collected, a node with
//! many properties) and the parent reads two columns, copying the rest for every
//! element is the dominant cost. UNWIND now takes the columns its parent
//! demands, plus the columns the list expression reads, and no more. The
//! plan-shape test runs the same query over nodes with few and with many
//! properties: the columns UNWIND copies must not grow with the properties
//! nobody asked for. The result tests compute the expected rows by hand from the
//! fixture, so they do not depend on how the plan was pruned.

use std::collections::HashMap;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;
use graphforge_ir::IrLiteral;

/// Four `W` nodes and these `K` relationships:
///
/// ```text
/// 1 -> 2   (twice: a parallel pair)
/// 2 -> 3
/// 3 -> 1
/// 3 -> 2
/// ```
///
/// `w` is `10 + id`. `extra` further properties `x0`, `x1`, .. that no query
/// reads widen every node.
fn forge(extra: usize) -> GraphForge {
    let wide: String = (0..extra).map(|i| format!(", x{i}: {i}")).collect();
    let forge = GraphForge::new(None).expect("in-memory forge");
    forge
        .execute(&format!(
            "CREATE (n1:W {{id: 1, w: 11{wide}}}), (n2:W {{id: 2, w: 12{wide}}}), \
             (n3:W {{id: 3, w: 13{wide}}}), (n4:W {{id: 4, w: 14{wide}}}), \
             (n1)-[:K]->(n2), (n1)-[:K]->(n2), (n2)-[:K]->(n3), \
             (n3)-[:K]->(n1), (n3)-[:K]->(n2)"
        ))
        .expect("fixture creates");
    forge
}

/// Every cell of `batches` rendered, nulls as `NULL`, one entry per row,
/// columns joined by `,`. The rows come back sorted: no query here orders its
/// result.
fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|column| {
                    let array = batch.column(column);
                    if array.is_null(row) {
                        "NULL".to_owned()
                    } else {
                        array_value_to_string(array, row).expect("cell renders")
                    }
                })
                .collect();
            out.push(cells.join(","));
        }
    }
    out.sort();
    out
}

fn run(forge: &GraphForge, query: &str) -> Vec<RecordBatch> {
    forge
        .execute_with_params(query, &HashMap::<String, IrLiteral>::new())
        .unwrap_or_else(|e| panic!("{query}: {e:?}"))
        .batches
}

/// The query's rows must equal `expected` on a narrow and on a wide fixture:
/// pruning the unread columns cannot change an answer.
fn expect(query: &str, expected: &[&str]) {
    let expected: Vec<String> = expected.iter().map(|row| (*row).to_owned()).collect();
    for extra in [0, 12] {
        assert_eq!(
            rows(&run(&forge(extra), query)),
            expected,
            "{query} over {extra} extra properties"
        );
    }
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

#[test]
fn unwinding_null_produces_no_rows() {
    expect("MATCH (a:W) UNWIND null AS x RETURN a.id AS id, x", &[]);
}

#[test]
fn unwinding_an_empty_list_produces_no_rows() {
    expect("MATCH (a:W) UNWIND [] AS x RETURN a.id AS id, x", &[]);
}

#[test]
fn unwinding_per_row_null_empty_and_populated_lists() {
    // Node 1 explodes to two rows, node 2 (empty) and node 3 (null) to none,
    // node 4 to one.
    expect(
        "MATCH (a:W) WITH a, CASE WHEN a.id = 1 THEN [10, 20] WHEN a.id = 2 THEN [] \
         WHEN a.id = 3 THEN null ELSE [30] END AS xs UNWIND xs AS x \
         RETURN a.w AS w, x",
        &["11,10", "11,20", "14,30"],
    );
}

#[test]
fn unwinding_a_list_of_lists_keeps_each_inner_list_whole() {
    expect(
        "MATCH (a:W) WHERE a.id <= 2 UNWIND [[1, 2], [3]] AS x RETURN a.id AS id, x",
        &["1,[1, 2]", "1,[3]", "2,[1, 2]", "2,[3]"],
    );
}

#[test]
fn unwinding_twice_explodes_the_inner_lists() {
    expect(
        "MATCH (a:W) WHERE a.id = 4 UNWIND [[1, 2], [3]] AS xs UNWIND xs AS x \
         RETURN a.w AS w, x",
        &["14,1", "14,2", "14,3"],
    );
}

#[test]
fn the_unwound_list_can_be_returned_with_its_elements() {
    expect(
        "MATCH (a:W) WHERE a.id <= 2 WITH a, [a.id, a.id + 100] AS xs \
         UNWIND xs AS x RETURN a.id AS id, xs, x",
        &[
            "1,[1, 101],1",
            "1,[1, 101],101",
            "2,[2, 102],102",
            "2,[2, 102],2",
        ],
    );
}

#[test]
fn an_input_column_only_above_the_unwind_survives_it() {
    // `w` is neither the list nor read by the UNWIND: the parent reads it.
    expect(
        "MATCH (a:W) WHERE a.id <= 2 WITH a, [a.id] AS xs UNWIND xs AS x \
         RETURN a.w AS w, x",
        &["11,1", "12,2"],
    );
}

#[test]
fn collecting_then_unwinding_returns_each_collected_row() {
    // Collected per `a`: 1 -> [2, 2], 2 -> [3], 3 -> [1, 2].
    expect(
        "MATCH (a:W)-[:K]->(b:W) WITH a, collect(b) AS bs UNWIND bs AS b \
         RETURN a.id AS a, b.id AS b",
        &["1,2", "1,2", "2,3", "3,1", "3,2"],
    );
}

#[test]
fn collecting_then_unwinding_with_the_list_size_returned() {
    expect(
        "MATCH (a:W)-[:K]->(b:W) WITH a, collect(b) AS bs UNWIND bs AS b \
         RETURN a.w AS w, size(bs) AS n, b.w AS bw",
        &["11,2,12", "11,2,12", "12,1,13", "13,2,11", "13,2,12"],
    );
}

#[test]
fn collecting_then_unwinding_feeds_a_further_match() {
    // The pairs (a, b) are (1,2) twice, (2,3), (3,1), (3,2); b's own
    // relationships: 2 -> 3, 3 -> 1 and 3 -> 2, 1 -> 2 twice.
    expect(
        "MATCH (a:W)-[:K]->(b:W) WITH a, collect(b) AS bs UNWIND bs AS b \
         MATCH (b)-[:K]->(c:W) RETURN a.id AS a, c.id AS c",
        &["1,3", "1,3", "2,1", "2,2", "3,2", "3,2", "3,3"],
    );
}

// ---------------------------------------------------------------------------
// Plan shape
// ---------------------------------------------------------------------------

/// Collect each node's neighbours, then explode them: the input of the UNWIND
/// carries the collected list and the grouped node's whole row, and the parent
/// reads two identities.
const COLLECT_THEN_UNWIND: &str = "MATCH (a:W)-[:K]->(b:W) WITH a, collect(b) AS bs UNWIND bs AS b \
     RETURN a.id AS a, b.id AS b";

/// The `N` of the `copied=N` the plan's `UnwindExec` line prints: how many
/// input columns UNWIND copies for every element it emits.
fn unwind_copied_columns(forge: &GraphForge, query: &str) -> usize {
    let explain = forge
        .explain(query)
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    let plan = explain
        .split_once("PhysicalPlan\n------------\n")
        .expect("physical-plan section")
        .1;
    let line = plan
        .lines()
        .find(|line| line.contains("UnwindExec"))
        .unwrap_or_else(|| panic!("no `UnwindExec` in the plan:\n{plan}"));
    let (_, tail) = line
        .split_once("copied=")
        .unwrap_or_else(|| panic!("`UnwindExec` prints no copied columns: {line}\n{plan}"));
    tail.chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or_else(|_| panic!("no number after `copied=` in `{line}`"))
}

/// The columns UNWIND copies per element are the ones the parent reads, however
/// many properties the grouped node carries, and the collected list itself is
/// not one of them.
#[test]
fn unwind_copies_the_same_columns_over_wide_nodes() {
    let narrow = unwind_copied_columns(&forge(0), COLLECT_THEN_UNWIND);
    let wide = unwind_copied_columns(&forge(40), COLLECT_THEN_UNWIND);
    assert_eq!(
        narrow, wide,
        "the columns UNWIND copies grew with the unread properties"
    );
    assert!(
        wide <= 3,
        "UNWIND copies {wide} columns for a parent that reads two identities"
    );
}
