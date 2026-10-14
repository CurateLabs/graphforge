//! A correlated sub-plan (an OPTIONAL MATCH, or a pattern predicate) demands
//! only the outer columns it reads (#1984, cause of #1982).
//!
//! The join that attaches a correlated sub-plan to its outer rows used to ask
//! the outer plan for every column, so a query over nodes with many properties
//! materialised and copied every column of every outer row even when the
//! sub-plan read two identities. The sub-plan now demands what it reads through
//! its seed leaf, and the outer plan produces those columns plus the ones the
//! parent above the join reads. The plan-shape tests compare queries that read
//! more properties than the pattern predicate does: the seed's demand and the
//! outer expansion's width must follow what is read, not the width of the row.
//! The result tests compute the expected rows by hand from the fixture, so they
//! do not depend on how the plan was pruned.

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
/// Node 4 has no relationships. `w` is `10 + id`; `t` (a threshold the
/// OPTIONAL MATCH filters on) is 1, 3, 1, 0 for nodes 1 to 4. `extra` further
/// properties `x0`, `x1`, .. that no query reads widen every node.
fn forge(extra: usize) -> GraphForge {
    let wide: String = (0..extra).map(|i| format!(", x{i}: {i}")).collect();
    let forge = GraphForge::new(None).expect("in-memory forge");
    forge
        .execute(&format!(
            "CREATE (n1:W {{id: 1, w: 11, t: 1{wide}}}), (n2:W {{id: 2, w: 12, t: 3{wide}}}), \
             (n3:W {{id: 3, w: 13, t: 1{wide}}}), (n4:W {{id: 4, w: 14, t: 0{wide}}}), \
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
/// pruning the unread properties cannot change an answer.
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
fn optional_match_keeps_a_null_row_for_an_outer_row_without_a_match() {
    // Node 1's parallel pair doubles its row; node 4 has nothing to match.
    expect(
        "MATCH (a:W) OPTIONAL MATCH (a)-[:K]->(c:W) RETURN a.id AS id, c.id AS cid",
        &["1,2", "1,2", "2,3", "3,1", "3,2", "4,NULL"],
    );
}

#[test]
fn optional_match_returns_an_outer_column_the_sub_plan_never_reads() {
    // `w` is read above the OPTIONAL MATCH only: the join must still carry it.
    expect(
        "MATCH (a:W) OPTIONAL MATCH (a)-[:K]->(c:W) RETURN a.w AS w, c.id AS cid",
        &["11,2", "11,2", "12,3", "13,1", "13,2", "14,NULL"],
    );
}

#[test]
fn optional_match_filter_reads_an_outer_column_the_parent_does_not() {
    // The parent returns `c.id` alone; the OPTIONAL MATCH's own WHERE reads the
    // outer `t`. Node 1 (t = 1) matches 2 twice; node 2 (t = 3) matches nothing
    // above 3; node 3 (t = 1) keeps 2 but not 1; node 4 has no relationship.
    expect(
        "MATCH (a:W) OPTIONAL MATCH (a)-[:K]->(c:W) WHERE c.id > a.t RETURN c.id AS cid",
        &["2", "2", "2", "NULL", "NULL"],
    );
}

#[test]
fn nested_optional_matches_chain_through_the_inner_row() {
    expect(
        "MATCH (a:W) OPTIONAL MATCH (a)-[:K]->(b:W) OPTIONAL MATCH (b)-[:K]->(c:W) \
         RETURN a.id AS aid, b.id AS bid, c.id AS cid",
        &[
            "1,2,3",
            "1,2,3",
            "2,3,1",
            "2,3,2",
            "3,1,2",
            "3,1,2",
            "3,2,3",
            "4,NULL,NULL",
        ],
    );
}

#[test]
fn not_pattern_predicate_drops_the_outer_rows_the_pattern_matches() {
    // The outer rows are (1,2) twice, (2,3), (3,1) and (3,2). `3 -> 2` and
    // `2 -> 3` match the reverse pattern, so those two rows go; `a.w` is read
    // above the predicate only.
    expect(
        "MATCH (a:W)-[:K]->(b:W) WHERE NOT (b)-[:K]->(a) RETURN a.w AS w, b.id AS bid",
        &["11,2", "11,2", "13,1"],
    );
}

#[test]
fn pattern_predicate_keeps_the_outer_rows_the_pattern_matches() {
    expect(
        "MATCH (a:W)-[:K]->(b:W) WHERE (b)-[:K]->(a) RETURN a.w AS w, b.id AS bid",
        &["12,3", "13,2"],
    );
}

#[test]
fn optional_match_follows_a_pattern_predicate() {
    // The predicate keeps (1,2) twice and (3,1); node 2 reaches 3 and node 1
    // reaches 2 twice.
    expect(
        "MATCH (a:W)-[:K]->(b:W) WHERE NOT (b)-[:K]->(a) \
         OPTIONAL MATCH (b)-[:K]->(c:W) RETURN a.w AS w, b.id AS bid, c.id AS cid",
        &["11,2,3", "11,2,3", "13,1,2", "13,1,2"],
    );
}

// ---------------------------------------------------------------------------
// Plan shape
// ---------------------------------------------------------------------------

/// Keeps the outer rows (1,2) twice and (3,1); `2 -> 3` and `3 -> 2` close a
/// cycle, so those rows go.
const NOT_PREDICATE: &str = "MATCH (a:W)-[:K]->(b:W) WHERE NOT (b)-[:K]->(a) RETURN a.id AS id";

/// The same predicate with a filter on `w` and `t` below it. Both properties
/// pass every node, so the rows equal `NOT_PREDICATE`'s; the parent reads
/// neither.
const NOT_PREDICATE_AFTER_FILTER: &str = "MATCH (a:W)-[:K]->(b:W) \
     WHERE a.w > 10 AND a.t < 5 AND NOT (b)-[:K]->(a) RETURN a.id AS id";

/// `NOT_PREDICATE` with the parent reading `x0` to `x{count - 1}` of `a` too.
fn predicate_reading_properties(count: usize) -> String {
    let columns: String = (0..count).map(|i| format!(", a.x{i} AS v{i}")).collect();
    format!("MATCH (a:W)-[:K]->(b:W) WHERE NOT (b)-[:K]->(a) RETURN a.id AS id{columns}")
}

fn physical(forge: &GraphForge, query: &str) -> Vec<String> {
    let explain = forge
        .explain(query)
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    explain
        .split_once("PhysicalPlan\n------------\n")
        .expect("physical-plan section")
        .1
        .lines()
        .map(str::to_owned)
        .collect()
}

/// The number after `key` on the first line containing `operator`.
fn number_after(plan: &[String], operator: &str, key: &str) -> usize {
    let line = plan
        .iter()
        .find(|line| line.contains(operator))
        .unwrap_or_else(|| panic!("no `{operator}` in the plan:\n{}", plan.join("\n")));
    let (_, tail) = line
        .split_once(key)
        .unwrap_or_else(|| panic!("`{key}` not in `{line}`"));
    tail.chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or_else(|_| panic!("no number after `{key}` in `{line}`"))
}

/// How many columns the pattern predicate's seed is asked for.
fn seed_demand(plan: &[String]) -> usize {
    number_after(plan, "CorrelatedSeedExec", "projection={")
}

/// How many columns the outer expansion produces.
fn outer_expansion_width(plan: &[String]) -> usize {
    number_after(plan, "ExpandExec", "projection=")
}

/// The predicate's seed is asked for the identities the pattern names, however
/// many properties of `a` the parent reads: the outer row grows, the seed does
/// not.
#[test]
fn pattern_predicate_seed_reads_the_pattern_variables_and_not_the_whole_row() {
    let forge = forge(20);
    let narrow = physical(&forge, &predicate_reading_properties(0));
    let wide = physical(&forge, &predicate_reading_properties(20));
    assert!(
        outer_expansion_width(&wide) > outer_expansion_width(&narrow),
        "the parent's extra reads should widen the outer expansion:\n{}",
        wide.join("\n")
    );
    assert_eq!(
        seed_demand(&narrow),
        seed_demand(&wide),
        "the seed's demand grew with the properties the parent reads:\n{}",
        wide.join("\n")
    );
}

/// A property read only below the predicate (by a filter on the scan) is not
/// carried through the outer expansion: the expansion produces what the
/// predicate and the parent read, and the join keys stay the identities.
#[test]
fn pattern_predicate_outer_plan_does_not_carry_properties_read_only_below_it() {
    let forge = forge(0);
    let plain = physical(&forge, NOT_PREDICATE);
    let filtered = physical(&forge, NOT_PREDICATE_AFTER_FILTER);
    assert_eq!(
        number_after(&plain, "OptionalMatchExec", "keys="),
        number_after(&filtered, "OptionalMatchExec", "keys="),
        "join keys grew with the properties read below the predicate"
    );
    assert_eq!(
        outer_expansion_width(&plain),
        outer_expansion_width(&filtered),
        "the outer expansion carries properties nothing above it reads:\n{}",
        filtered.join("\n")
    );
}
