//! A pattern predicate whose pattern mentions only variables the outer query
//! already bound is evaluated for the outer rows, not over the whole graph
//! (#1984, cause 4 of #1982).
//!
//! The binder emits a node scan for every node element of a predicate's pattern,
//! including the elements that name an outer variable. The lowerer read those
//! scans as the predicate producing its own rows, so it planned the pattern over
//! the whole graph and anti-joined the result afterwards: every
//! `NOT (a)-[:T]-(b)` paid for an expansion from every node. Each query here
//! runs through the public path, and the expected rows are computed by hand from
//! the fixture, so they do not depend on which lowering answers.

use std::collections::HashMap;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;
use graphforge_exec::demand::{self, DemandSnapshot};
use graphforge_ir::IrLiteral;

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

/// Six `P` nodes (`id` 1 to 6; node 4 is also a `Q`) and these `K`
/// relationships:
///
/// ```text
/// 1 -> 2   (twice: a parallel pair)
/// 2 -> 3
/// 2 -> 5
/// 3 -> 1   (closes the cycle 1 -> 2 -> 3 -> 1)
/// 3 -> 3   (a self-loop)
/// 4 -> 1
/// ```
///
/// Node 6 is isolated.
const FIXTURE: &str = "CREATE (n1:P {id: 1}), (n2:P {id: 2}), (n3:P {id: 3}), \
     (n4:P:Q {id: 4}), (n5:P {id: 5}), (n6:P {id: 6}), \
     (n1)-[:K]->(n2), (n1)-[:K]->(n2), (n2)-[:K]->(n3), (n2)-[:K]->(n5), \
     (n3)-[:K]->(n1), (n3)-[:K]->(n3), (n4)-[:K]->(n1)";

fn forge() -> GraphForge {
    let forge = GraphForge::new(None).expect("in-memory forge");
    forge.execute(FIXTURE).expect("fixture creates");
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

fn answer(forge: &GraphForge, query: &str) -> Vec<String> {
    rows(
        &forge
            .execute_with_params(query, &HashMap::<String, IrLiteral>::new())
            .unwrap_or_else(|e| panic!("{query}: {e:?}"))
            .batches,
    )
}

fn expect(forge: &GraphForge, query: &str, expected: &[&str]) {
    let expected: Vec<String> = expected.iter().map(|row| (*row).to_owned()).collect();
    assert_eq!(answer(forge, query), expected, "{query}");
}

/// Run `predicate` as written and negated over `outer`, and expect `positive`
/// and `negative`.
fn both_forms(
    forge: &GraphForge,
    outer: &str,
    predicate: &str,
    ret: &str,
    positive: &[&str],
    negative: &[&str],
) {
    expect(
        forge,
        &format!("{outer} WHERE {predicate} RETURN {ret}"),
        positive,
    );
    expect(
        forge,
        &format!("{outer} WHERE NOT {predicate} RETURN {ret}"),
        negative,
    );
}

const FROM_ONE: &str = "MATCH (a:P {id: 1})-[:K]-(b)";

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

#[test]
fn closing_a_cycle_back_to_the_anchor() {
    // 1 -> 2 -> 3 and 1 -> 2 -> 5, each twice (the parallel pair). 3 -> 1
    // closes a cycle; nothing joins 5 to 1.
    let forge = forge();
    both_forms(
        &forge,
        "MATCH (a:P {id: 1})-[:K]->()-[:K]->(c)",
        "(c)-[:K]-(a)",
        "c.id AS id",
        &["3", "3"],
        &["5", "5"],
    );
}

#[test]
fn a_directed_predicate_respects_its_direction() {
    // The neighbours of 1 are 2 (twice), 3 and 4. Of those, 3 -> 1 and 4 -> 1
    // point at 1; 1 -> 2 is the only edge that leaves it.
    let forge = forge();
    both_forms(
        &forge,
        FROM_ONE,
        "(b)-[:K]->(a)",
        "b.id AS id",
        &["3", "4"],
        &["2", "2"],
    );
    both_forms(
        &forge,
        FROM_ONE,
        "(a)-[:K]->(b)",
        "b.id AS id",
        &["2", "2"],
        &["3", "4"],
    );
}

#[test]
fn a_predicate_never_multiplies_the_outer_rows() {
    // 1 and 2 are joined by a parallel pair, so the predicate matches twice for
    // each row with b = 2. The outer row survives once per outer match.
    let forge = forge();
    expect(
        &forge,
        &format!("{FROM_ONE} WHERE (a)-[:K]->(b) RETURN b.id AS id"),
        &["2", "2"],
    );
}

#[test]
fn a_self_loop_matches_a_predicate_that_returns_to_its_start() {
    let forge = forge();
    both_forms(
        &forge,
        "MATCH (a:P)",
        "(a)-[:K]->(a)",
        "a.id AS id",
        &["3"],
        &["1", "2", "4", "5", "6"],
    );
    both_forms(
        &forge,
        "MATCH (a:P)",
        "(a)-[:K]-(a)",
        "a.id AS id",
        &["3"],
        &["1", "2", "4", "5", "6"],
    );
}

#[test]
fn a_zero_length_predicate_matches_every_start() {
    let forge = forge();
    both_forms(
        &forge,
        "MATCH (a:P)",
        "(a)-[:K*0..1]->(a)",
        "a.id AS id",
        &["1", "2", "3", "4", "5", "6"],
        &[],
    );
}

#[test]
fn null_outer_bindings_match_nothing() {
    // OPTIONAL MATCH leaves x NULL for a = 2, 3 and 4: no Q node points at them.
    // A pattern over a NULL binding matches nothing, so NOT keeps those rows
    // and the positive form drops them.
    let forge = forge();
    let outer = "MATCH (a:P) WHERE a.id <= 4 OPTIONAL MATCH (a)<-[:K]-(x:Q) WITH a, x";
    let ret = "a.id AS a, x.id AS x";
    both_forms(
        &forge,
        outer,
        "(a)-[:K]-(x)",
        ret,
        &["1,4"],
        &["2,NULL", "3,NULL", "4,NULL"],
    );
    both_forms(
        &forge,
        outer,
        "(x)-[:K]-(a)",
        ret,
        &["1,4"],
        &["2,NULL", "3,NULL", "4,NULL"],
    );
}

#[test]
fn labels_and_properties_on_a_rebound_node_are_checked_against_its_value() {
    // The neighbours of 1 are 2 (twice), 3 and 4; only 4 is a Q.
    let forge = forge();
    both_forms(
        &forge,
        FROM_ONE,
        "(b:Q)-[:K]-(a)",
        "b.id AS id",
        &["4"],
        &["2", "2", "3"],
    );
    both_forms(
        &forge,
        FROM_ONE,
        "(b {id: 3})-[:K]-(a)",
        "b.id AS id",
        &["3"],
        &["2", "2", "4"],
    );
    // The anchor is not a Q, so the label rules every neighbour out.
    both_forms(
        &forge,
        FROM_ONE,
        "(b)-[:K]-(a:Q)",
        "b.id AS id",
        &[],
        &["2", "2", "3", "4"],
    );
}

#[test]
fn a_predicate_with_one_bound_node_and_anonymous_neighbours() {
    // Of the neighbours of 1 (2 twice, 3, 4), 2 and 3 have an incoming K
    // relationship and 4 has none.
    let forge = forge();
    both_forms(
        &forge,
        FROM_ONE,
        "(b)<-[:K]-()",
        "b.id AS id",
        &["2", "2", "3"],
        &["4"],
    );
}

#[test]
fn a_two_hop_predicate() {
    // From 2: 2 -> 3 -> 1. From 3: 3 -> 3 -> 1 through the self-loop. From 4:
    // 4 -> 1 and then nothing returns to 1.
    let forge = forge();
    expect(
        &forge,
        &format!("{FROM_ONE} WHERE EXISTS {{ (b)-[:K]->()-[:K]->(a) }} RETURN b.id AS id"),
        &["2", "2", "3"],
    );
}

#[test]
fn a_variable_length_predicate() {
    // Exactly three directed hops back to 1: 2 -> 3 -> 3 -> 1. From 3 the three
    // hop walks end at 2, 3 or 5; from 4 at 3 or 5.
    let forge = forge();
    both_forms(
        &forge,
        FROM_ONE,
        "(b)-[:K*3..3]->(a)",
        "b.id AS id",
        &["2", "2"],
        &["3", "4"],
    );
    both_forms(
        &forge,
        FROM_ONE,
        "(b)-[:K*2..2]-(a)",
        "b.id AS id",
        &["2", "2", "3"],
        &["4"],
    );
}

#[test]
fn relationship_uniqueness_does_not_reach_across_a_predicate() {
    // 4 -> 1 is the only relationship at 4. The predicate may use the same
    // relationship the outer pattern bound as r.
    let forge = forge();
    expect(
        &forge,
        "MATCH (a:P {id: 4})-[r:K]-(b) WHERE (b)-[:K]-(a) RETURN b.id AS id",
        &["1"],
    );
}

#[test]
fn an_exists_subquery_over_bound_nodes() {
    // The binder accepts EXISTS only as written; there is no NOT EXISTS form.
    let forge = forge();
    expect(
        &forge,
        &format!("{FROM_ONE} WHERE EXISTS {{ (b)-[:K]->(a) }} RETURN b.id AS id"),
        &["3", "4"],
    );
}

#[test]
fn a_subquery_that_reads_an_outer_property_is_unchanged() {
    // Only node 4 is a Q, and the subquery compares it with the outer id. The
    // subquery does not name an outer node in a pattern.
    let forge = forge();
    expect(
        &forge,
        "MATCH (a:P) WHERE EXISTS { MATCH (x:Q) WHERE x.id > a.id RETURN x.id } RETURN a.id AS id",
        &["1", "2", "3"],
    );
}

#[test]
fn a_subquery_that_rebinds_an_outer_relationship_is_unchanged() {
    // r is the one relationship from 4 to 1. Rebinding it in the subquery
    // matches that relationship and no other.
    let forge = forge();
    expect(
        &forge,
        "MATCH (a:P {id: 4})-[r:K]->(b) WHERE EXISTS { MATCH ()-[r]->() RETURN r } \
         RETURN b.id AS id",
        &["1"],
    );
}

#[test]
fn relationship_type_alternatives_are_unchanged() {
    let forge = forge();
    forge
        .execute("MATCH (a:P {id: 6}), (b:P {id: 1}) CREATE (a)-[:L]->(b)")
        .expect("adds L");
    both_forms(
        &forge,
        "MATCH (a:P {id: 1})-[:K|L]-(b)",
        "(b)-[:L]->(a)",
        "b.id AS id",
        &["6"],
        &["2", "2", "3", "4"],
    );
}

// ---------------------------------------------------------------------------
// Plan shape
// ---------------------------------------------------------------------------

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

fn operators(plan: &[String], name: &str) -> usize {
    plan.iter().filter(|line| line.contains(name)).count()
}

/// The predicate must add `expansions` expansions seeded by the outer rows and
/// no storage scan: every scan in the plan of `with_predicate` is a scan of
/// `without_predicate`.
fn assert_correlated(
    forge: &GraphForge,
    without_predicate: &str,
    with_predicate: &str,
    expansions: usize,
) {
    let base = physical(forge, without_predicate);
    let plan = physical(forge, with_predicate);
    let text = plan.join("\n");
    assert_eq!(
        operators(&plan, "GraphForgeParquetExec"),
        operators(&base, "GraphForgeParquetExec"),
        "the predicate scans storage:\n{text}"
    );
    assert_eq!(
        operators(&plan, "ExpandExec"),
        operators(&base, "ExpandExec") + expansions,
        "the predicate should add {expansions} expansion(s):\n{text}"
    );
    assert_eq!(
        operators(&plan, "CorrelatedSeedExec"),
        1,
        "the predicate should be seeded by the outer rows:\n{text}"
    );
    assert_eq!(operators(&base, "CorrelatedSeedExec"), 0, "{base:?}");
}

#[test]
fn a_predicate_over_bound_nodes_adds_no_scan() {
    let forge = forge();
    let base = "MATCH (a:P {id: 1})-[:K]-(b) RETURN b.id AS id";
    assert_correlated(
        &forge,
        base,
        "MATCH (a:P {id: 1})-[:K]-(b) WHERE NOT (b)-[:K]-(a) RETURN b.id AS id",
        1,
    );
    assert_correlated(
        &forge,
        base,
        "MATCH (a:P {id: 1})-[:K]-(b) WHERE (b)-[:K]->(a) RETURN b.id AS id",
        1,
    );
    assert_correlated(
        &forge,
        base,
        "MATCH (a:P {id: 1})-[:K]-(b) WHERE EXISTS { (b)-[:K]->()-[:K]->(a) } RETURN b.id AS id",
        2,
    );
}

#[test]
fn a_predicate_that_names_the_same_nodes_as_a_longer_match() {
    // The shape of a friend-recommendation query: a two-hop reach, a join to a
    // second pattern, and a predicate over the two bound ends.
    let forge = forge();
    assert_correlated(
        &forge,
        "MATCH (person:P {id: 1})-[:K*2..2]-(friend), (friend)-[:K]->(other:P) \
         WHERE NOT friend = person \
         RETURN friend.id AS id",
        "MATCH (person:P {id: 1})-[:K*2..2]-(friend), (friend)-[:K]->(other:P) \
         WHERE NOT friend = person AND NOT (friend)-[:K]-(person) \
         RETURN friend.id AS id",
        1,
    );
}

// ---------------------------------------------------------------------------
// The seed carries only what the predicate names
// ---------------------------------------------------------------------------

/// The number after `key=` on `line`.
fn number_after(line: &str, key: &str) -> usize {
    let (_, tail) = line
        .split_once(key)
        .unwrap_or_else(|| panic!("`{key}` not in `{line}`"));
    tail.chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or_else(|_| panic!("no number after `{key}` in `{line}`"))
}

/// The expansion the predicate's seed feeds: the nearest one above the last
/// seed leaf. Earlier clauses that are themselves correlated print their own
/// seeds first; the predicate filters their rows, so its seed comes last.
fn seeded_expansion(plan: &[String]) -> &str {
    let seed = plan
        .iter()
        .rposition(|line| line.contains("CorrelatedSeedExec"))
        .unwrap_or_else(|| panic!("no seed in the plan:\n{}", plan.join("\n")));
    plan[..seed]
        .iter()
        .rev()
        .find(|line| line.contains("ExpandExec"))
        .unwrap_or_else(|| panic!("no seeded expansion:\n{}", plan.join("\n")))
}

/// The columns the predicate's matches join back to the outer rows on: the
/// outermost join, since the predicate filters what earlier clauses produce.
fn join_keys(plan: &[String]) -> usize {
    let join = plan
        .iter()
        .find(|line| line.contains("OptionalMatchExec"))
        .unwrap_or_else(|| panic!("no join in the plan:\n{}", plan.join("\n")));
    number_after(join, "keys=")
}

/// What `assert_narrow` expects of a predicate's plan.
struct Narrow {
    /// Columns the predicate's matches join back to the outer rows on.
    keys: usize,
    /// Most columns the expansion the seed feeds may carry: the identities of
    /// the seed's node and the anchor, the identity of the node it reaches, and
    /// the edge id a longer pattern keeps for relationship uniqueness.
    columns: usize,
    /// Also check the `NOT` form; the binder has no `NOT EXISTS { .. }`.
    negated: bool,
}

/// `predicate` over `outer` joins back and expands from the seed as `narrow`
/// says, so the seed never carries an outer row's other columns.
fn assert_narrow(forge: &GraphForge, outer: &str, predicate: &str, narrow: Narrow) {
    let mut queries = vec![format!("{outer} WHERE {predicate} RETURN a.id AS id")];
    if narrow.negated {
        queries.push(format!("{outer} WHERE NOT {predicate} RETURN a.id AS id"));
    }
    for query in queries {
        let plan = physical(forge, &query);
        let text = plan.join("\n");
        assert_eq!(join_keys(&plan), narrow.keys, "{query}\n{text}");
        let width = number_after(seeded_expansion(&plan), "projection=");
        assert!(
            width <= narrow.columns,
            "the seeded expansion carries {width} columns, not at most {}: {query}\n{text}",
            narrow.columns
        );
    }
}

#[test]
fn the_correlated_seed_carries_only_the_identity_of_the_named_nodes() {
    // The outer rows are 82 columns wide on a real graph. The predicate names b
    // and a, so the seed is their identities and the matches join back on two.
    let forge = forge();
    let outer = "MATCH (a:P {id: 1})-[:K]-(b)";
    let plan = physical(
        &forge,
        &format!("{outer} WHERE NOT (b)-[:K]-(a) RETURN b.id AS id"),
    );
    let text = plan.join("\n");
    assert_eq!(join_keys(&plan), 2, "{text}");
    let seeded = seeded_expansion(&plan);
    let main_path = plan
        .iter()
        .map(String::as_str)
        .find(|line| line.contains("ExpandExec") && !std::ptr::eq(*line, seeded))
        .expect("the outer expansion");
    assert!(
        number_after(seeded, "projection=") < number_after(main_path, "projection="),
        "the seeded expansion should carry fewer columns than the outer one:\n{text}"
    );
    let seed = plan
        .iter()
        .position(|line| line.contains("CorrelatedSeedExec"))
        .expect("seed");
    assert!(
        plan[seed - 1].matches(" as ").count() <= 8,
        "the seed should project the identities only:\n{text}"
    );
}

#[test]
fn a_narrow_seed_serves_every_shape_of_predicate_over_bound_nodes() {
    let forge = forge();
    let outer = "MATCH (a:P {id: 1})-[:K]-(b)";
    let single = |keys| Narrow {
        keys,
        columns: 3,
        negated: true,
    };
    // One variable named: one key.
    assert_narrow(&forge, outer, "(b)-[:K]->()", single(1));
    // A label on a re-bound node moves onto the seed beside its identity.
    assert_narrow(&forge, outer, "(b:Q)-[:K]-(a)", single(2));
    assert_narrow(&forge, outer, "(b)-[:K]-(a:Q)", single(2));
    // Two traversals; the middle node is the predicate's own.
    assert_narrow(
        &forge,
        outer,
        "EXISTS { (b)-[:K]->()-[:K]->(a) }",
        Narrow {
            keys: 2,
            columns: 5,
            negated: false,
        },
    );
    // A property on a re-bound node is read from the node, not from the seed.
    assert_narrow(&forge, outer, "(b {id: 3})-[:K]-(a)", single(2));
    // A NULL outer binding (an unmatched OPTIONAL MATCH) joins back null-safe.
    assert_narrow(
        &forge,
        "MATCH (a:P) WHERE a.id <= 4 OPTIONAL MATCH (a)<-[:K]-(x:Q) WITH a, x",
        "(a)-[:K]-(x)",
        single(2),
    );
}

#[test]
fn a_predicate_that_reads_an_outer_property_seeds_that_variables_columns() {
    // The subquery compares what it reaches with a.id, so a rides through whole
    // while b is only its identity.
    let forge = forge();
    let outer = "MATCH (a:P {id: 1})-[:K]-(b)";
    let predicate = "EXISTS { MATCH (b)-[:K]->(c) WHERE c.id > a.id RETURN c.id }";
    // b is 2 (twice), 3 or 4; 2 reaches 3 and 5, 3 reaches 3, and 4 reaches
    // only a itself.
    expect(
        &forge,
        &format!("{outer} WHERE {predicate} RETURN b.id AS id"),
        &["2", "2", "3"],
    );
    let plan = physical(
        &forge,
        &format!("{outer} WHERE {predicate} RETURN b.id AS id"),
    );
    assert_eq!(join_keys(&plan), 2, "{}", plan.join("\n"));
    assert_eq!(operators(&plan, "CorrelatedSeedExec"), 1, "{plan:?}");
}

// ---------------------------------------------------------------------------
// Growth
// ---------------------------------------------------------------------------

/// The rows the expansions of one execution pulled, selected, examined and
/// emitted.
fn hop_work(snapshot: &DemandSnapshot) -> u64 {
    snapshot
        .hops
        .values()
        .map(|hop| {
            hop.input_rows
                + hop.candidates_generated
                + hop.adjacency_rows_examined
                + hop.rows_emitted
        })
        .sum()
}

/// The fixture plus `bulk` disconnected pairs of nodes. None of them is
/// reachable from the anchor, so they change the size of the graph and nothing
/// the outer rows can see.
fn forge_with_bulk(bulk: usize) -> GraphForge {
    let forge = forge();
    forge
        .execute(&format!(
            "UNWIND range(1000, {}) AS i CREATE (:P {{id: i}})-[:K]->(:P {{id: -i}})",
            1000 + bulk
        ))
        .expect("bulk creates");
    forge
}

/// The expansion work the predicate adds to `base` on a graph with `bulk`
/// extra pairs of nodes.
fn predicate_work(bulk: usize, base: &str, with_predicate: &str) -> (u64, Vec<String>) {
    let forge = forge_with_bulk(bulk);
    let (plain, plain_work) = demand::capture(|| forge.execute(base));
    let (filtered, filtered_work) = demand::capture(|| forge.execute(with_predicate));
    let plain = plain.expect("base executes");
    let filtered = filtered.expect("predicate executes");
    let answer = rows(&filtered.batches);
    assert!(
        answer.len() <= rows(&plain.batches).len(),
        "a predicate cannot add rows"
    );
    (
        hop_work(&filtered_work)
            .checked_sub(hop_work(&plain_work))
            .expect("the predicate cannot remove expansion work"),
        answer,
    )
}

/// Growing the graph far beyond the outer rows' neighbourhoods must not change
/// the expansion work the predicate adds: it is bounded by what the outer rows
/// can reach. The old lowering expanded from every node, so its work grew with
/// the graph.
fn assert_work_independent_of_graph_size(base: &str, with_predicate: &str) {
    let (small, small_rows) = predicate_work(0, base, with_predicate);
    let (large, large_rows) = predicate_work(1500, base, with_predicate);
    assert!(small > 0, "the predicate should expand: {with_predicate}");
    assert_eq!(small_rows, large_rows, "{with_predicate}");
    assert_eq!(
        large, small,
        "the predicate's expansion work depends on the graph size: {with_predicate}"
    );
}

#[test]
fn predicate_work_does_not_grow_with_the_graph() {
    assert_work_independent_of_graph_size(
        "MATCH (a:P {id: 1})-[:K]-(b) RETURN b.id AS id",
        "MATCH (a:P {id: 1})-[:K]-(b) WHERE NOT (b)-[:K]->(a) RETURN b.id AS id",
    );
    assert_work_independent_of_graph_size(
        "MATCH (a:P {id: 1})-[:K]-(b) RETURN b.id AS id",
        "MATCH (a:P {id: 1})-[:K]-(b) WHERE (b)-[:K]->(a) RETURN b.id AS id",
    );
    assert_work_independent_of_graph_size(
        "MATCH (a:P {id: 1})-[:K]-(b) RETURN b.id AS id",
        "MATCH (a:P {id: 1})-[:K]-(b) WHERE EXISTS { (b)-[:K]->()-[:K]->(a) } RETURN b.id AS id",
    );
}

/// A ring of 64 `Entity` nodes built through bulk construction, where node `i`
/// links to the next two nodes: a compact generation that carries the ordinal
/// identities the identity-only expansion reads. Nothing in a project built by
/// Cypher `CREATE` does.
#[test]
fn the_seeded_expansion_reads_identities_without_touching_edges_or_nodes() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("state");
    bulk_fixture::generate_bulk_graph_with_properties(&path, 64, 2, false);
    let forge = GraphForge::new(Some(path.to_str().expect("UTF-8 path"))).expect("opens");
    let outer = format!(
        "MATCH (a:Entity {{name: '{}'}})-[:LINK]->(b)",
        bulk_fixture::fixture_node_name(0)
    );
    let base = format!("{outer} RETURN b.node_uuid AS id");
    let query = format!("{outer} WHERE NOT (b)-[:LINK]->(a) RETURN b.node_uuid AS id");

    let plan = physical(&forge, &query);
    let text = plan.join("\n");
    let seeded = seeded_expansion(&plan);
    assert!(seeded.contains("identity=v4"), "{seeded}\n{text}");
    assert_eq!(number_after(seeded, "projection="), 3, "{text}");
    assert_eq!(join_keys(&plan), 2, "{text}");

    let (plain, plain_work) = demand::capture(|| forge.execute(&base));
    let (filtered, work) = demand::capture(|| forge.execute(&query));
    // Node 0 links to nodes 1 and 2, and neither links back.
    let expected: Vec<String> = [1, 2]
        .iter()
        .map(|&node| {
            bulk_fixture::fixture_node_uuid(node)
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        })
        .collect();
    assert_eq!(rows(&plain.expect("base executes").batches), expected);
    assert_eq!(
        rows(&filtered.expect("predicate executes").batches),
        expected
    );

    let seeded_hops: Vec<_> = work
        .hops
        .iter()
        .filter(|(hop, _)| !plain_work.hops.contains_key(hop))
        .collect();
    assert_eq!(seeded_hops.len(), 1, "{:?}", work.hops);
    let hop = seeded_hops[0].1;
    assert_eq!(hop.input_rows, 2, "{hop:?}");
    assert_eq!(hop.candidates_generated, 4, "{hop:?}");
    assert_eq!(hop.edge_reads_started, 0, "{hop:?}");
    assert_eq!(hop.edge_full_reads, 0, "{hop:?}");
    assert_eq!(hop.node_reads_started, 0, "{hop:?}");
    assert_eq!(hop.node_full_reads, 0, "{hop:?}");
    assert_eq!(hop.edge_projected_columns, 0, "{hop:?}");
    assert_eq!(hop.node_projected_columns, 0, "{hop:?}");
    // The identities come from the ordinal ranges the base expansion already
    // read, or from one read of their own.
    assert!(hop.identity_ranges_selected > 0, "{hop:?}");
}
