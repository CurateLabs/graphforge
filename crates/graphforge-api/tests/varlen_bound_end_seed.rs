//! A variable-length expansion whose end is already bound is searched from that
//! end (#1984).
//!
//! `MATCH (x:N {id: 5}) MATCH (o:N)-[r:E*1..3]->(x)` used to seed the search
//! from every `:N` node and filter the paths that reach `x` at the end, so the
//! work followed the population of the label rather than the neighbourhood of
//! `x`. The lowerer now seeds the search from `x` in the opposite direction and
//! reverses each emitted edge list, so the rows (including the order of the
//! relationship list and the direction of a path) are exactly those of the
//! unreversed search.
//!
//! The baseline of every comparison is the same statement with a trivially true
//! `WHERE o.id >= 0`: a predicate on the scanned variable keeps the lowerer on
//! the unreversed plan, which the physical plan shows. Every test also asserts
//! that its subject statement is the reversed plan and its baseline is not, so
//! a test cannot pass by comparing the unreversed plan with itself.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use graphforge_api::{ExecutionResourcePolicy, ExplainStage, GraphForge, GraphForgeOptions};
use graphforge_exec::demand::{self, DemandSnapshot};

const PARTITIONS: [usize; 2] = [1, 4];
const NODES: i64 = 10;
/// The bound end of most statements.
const END: i64 = 5;

/// `(source, destination, key)`: the key identifies the edge, so a relationship
/// list reads as the sequence of keys. A ring with chords, two parallel edges
/// from 2 to 3, a self-loop on 4 and an edge back from 5 to 4, so that trails
/// revisit nodes, share endpoints and close on themselves.
fn edges() -> Vec<(i64, i64, i64)> {
    let mut edges = Vec::new();
    for source in 0..NODES {
        for step in [1, 3] {
            let destination = (source + step) % NODES;
            edges.push((source, destination, 10 * source + destination));
        }
    }
    edges.push((2, 3, 900));
    edges.push((4, 4, 901));
    edges.push((5, 4, 902));
    edges
}

fn forge(partitions: usize, bulk: usize) -> GraphForge {
    let forge = GraphForge::new_with_options(
        None,
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(partitions),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .expect("in-memory forge");
    // Even nodes also carry `:M`.
    forge
        .execute("UNWIND range(0, 9) AS i WITH i WHERE i % 2 = 0 CREATE (:N:M {id: i, name: 'n' + toString(i)})")
        .expect("even nodes");
    forge
        .execute("UNWIND range(0, 9) AS i WITH i WHERE i % 2 = 1 CREATE (:N {id: i, name: 'n' + toString(i)})")
        .expect("odd nodes");
    for (source, destination, key) in edges() {
        forge
            .execute(&format!(
                "MATCH (a:N {{id: {source}}}), (b:N {{id: {destination}}}) \
                 CREATE (a)-[:E {{k: {key}}}]->(b)"
            ))
            .expect("edge");
    }
    // The statement shape that binds the end through another expansion.
    forge.execute("CREATE (:Person {id: 1})").expect("person");
    forge
        .execute("MATCH (p:Person {id: 1}), (x:N) WHERE x.id IN [5, 7] CREATE (p)-[:OWN]->(x)")
        .expect("owned nodes");
    if bulk > 0 {
        // Disconnected pairs: nothing reaches the bound end from them.
        forge
            .execute(&format!(
                "UNWIND range(0, {}) AS i CREATE (:N {{id: 1000 + 2 * i, name: 'bulk'}})\
                 -[:E {{k: 100000 + i}}]->(:N {{id: 1001 + 2 * i, name: 'bulk'}})",
                bulk - 1
            ))
            .expect("bulk");
    }
    forge
}

/// Every cell rendered, nulls as `NULL`, one entry per row; sorted, since none
/// of the statements orders its result.
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
    rows(&forge.execute(query).expect(query).batches)
}

fn is_reversed(forge: &GraphForge, query: &str) -> bool {
    forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .expect("physical plan")
        .contains("edge_list=reversed")
}

/// The statement with the bound end `prefix` binds, and the same statement with
/// a predicate on `o` that keeps the lowerer on the unreversed plan.
fn pair(prefix: &str, pattern: &str, ret: &str) -> (String, String) {
    (
        format!("{prefix} MATCH {pattern} RETURN {ret}"),
        format!("{prefix} MATCH {pattern} WHERE o.id >= 0 RETURN {ret}"),
    )
}

const BIND_END: &str = "MATCH (x:N {id: 5})";

/// Run both forms, require the subject reversed and the baseline not, and
/// require equal non-empty answers. Returns the answer.
fn assert_same(forge: &GraphForge, context: &str, subject: &str, baseline: &str) -> Vec<String> {
    assert!(
        is_reversed(forge, subject),
        "{context}: the subject must be searched from the bound end\n{subject}"
    );
    assert!(
        !is_reversed(forge, baseline),
        "{context}: the baseline must stay unreversed\n{baseline}"
    );
    let reversed = answer(forge, subject);
    let unreversed = answer(forge, baseline);
    assert!(
        !reversed.is_empty(),
        "{context}: the statement must return rows"
    );
    assert_eq!(reversed, unreversed, "{context}");
    reversed
}

/// Every trail of `min..=max` edges that follows the edge directions and ends
/// at `end`, as `(start, keys along the path)`; the start must satisfy
/// `starts`.
fn trails(min: usize, max: usize, end: i64, starts: impl Fn(i64) -> bool) -> Vec<String> {
    fn walk(
        edges: &[(i64, i64, i64)],
        at: i64,
        end: i64,
        (min, max): (usize, usize),
        used: &mut Vec<usize>,
        keys: &mut Vec<i64>,
        start: i64,
        out: &mut Vec<String>,
    ) {
        if keys.len() >= min && at == end {
            let list = keys
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            out.push(format!("{start},[{list}]"));
        }
        if keys.len() == max {
            return;
        }
        for (index, &(source, destination, key)) in edges.iter().enumerate() {
            if source == at && !used.contains(&index) {
                used.push(index);
                keys.push(key);
                walk(edges, destination, end, (min, max), used, keys, start, out);
                keys.pop();
                used.pop();
            }
        }
    }
    let edges = edges();
    let mut out = Vec::new();
    for start in (0..NODES).filter(|n| starts(*n)) {
        walk(
            &edges,
            start,
            end,
            (min, max),
            &mut Vec::new(),
            &mut Vec::new(),
            start,
            &mut out,
        );
    }
    out.sort();
    out
}

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

#[test]
fn the_work_follows_the_bound_end_and_not_the_label() {
    let subject = "MATCH (x:N {id: 5}) MATCH (o:N)-[r:E*1..3]->(x) RETURN count(*)";
    let baseline =
        "MATCH (x:N {id: 5}) MATCH (o:N)-[r:E*1..3]->(x) WHERE o.id >= 0 RETURN count(*)";
    for partitions in PARTITIONS {
        let mut subject_work = Vec::new();
        let mut baseline_work = Vec::new();
        for bulk in [0, 400] {
            let forge = forge(partitions, bulk);
            assert!(is_reversed(&forge, subject));
            assert!(!is_reversed(&forge, baseline));
            let (reversed, reversed_snapshot) = demand::capture(|| forge.execute(subject));
            let (unreversed, unreversed_snapshot) = demand::capture(|| forge.execute(baseline));
            assert_eq!(
                rows(&reversed.expect("subject").batches),
                rows(&unreversed.expect("baseline").batches),
                "{partitions} partitions, bulk {bulk}"
            );
            subject_work.push(hop_work(&reversed_snapshot));
            baseline_work.push(hop_work(&unreversed_snapshot));
        }
        eprintln!("partitions {partitions}: subject {subject_work:?} baseline {baseline_work:?}");
        assert_eq!(
            subject_work[0], subject_work[1],
            "{partitions} partitions: unreachable nodes must add no expansion work"
        );
        assert!(subject_work[0] > 0, "the expansion did work");
        assert!(
            baseline_work[1] > baseline_work[0] + 400,
            "{partitions} partitions: the unreversed search seeds from the label \
             population ({baseline_work:?}), so growing the label must grow its work"
        );
        assert!(subject_work[1] < baseline_work[0]);
    }
}

#[test]
fn an_unnamed_relationship_returns_the_unreversed_rows() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        for (min, max) in [(1, 3), (0, 3)] {
            let (subject, baseline) = pair(
                BIND_END,
                &format!("(o:N)-[:E*{min}..{max}]->(x)"),
                "o.id AS start, x.id AS end",
            );
            let rows = assert_same(
                &forge,
                &format!("{partitions} partitions, *{min}..{max}"),
                &subject,
                &baseline,
            );
            // One row per trail, so the multiset is the trail count per start.
            let expected: Vec<String> = trails(min, max, END, |_| true)
                .iter()
                .map(|trail| format!("{},{END}", trail.split(',').next().expect("start")))
                .collect();
            let mut expected = expected;
            expected.sort();
            assert_eq!(rows, expected, "{partitions} partitions, *{min}..{max}");
        }
    }
}

#[test]
fn a_named_relationship_keeps_its_edge_list_order() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        for (min, max) in [(1, 3), (0, 3), (2, 4), (1, 2)] {
            let (subject, baseline) = pair(
                BIND_END,
                &format!("(o:N)-[r:E*{min}..{max}]->(x)"),
                "o.id AS start, [e IN r | e.k] AS keys",
            );
            let rows = assert_same(
                &forge,
                &format!("{partitions} partitions, *{min}..{max}"),
                &subject,
                &baseline,
            );
            // The edge list reads from `o` to `x`, not from the seed.
            assert_eq!(
                rows,
                trails(min, max, END, |_| true),
                "{partitions} partitions, *{min}..{max}"
            );
        }
    }
}

#[test]
fn a_named_path_keeps_its_node_and_edge_order() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        for returned in [
            "p",
            "nodes(p) AS ns, relationships(p) AS rs",
            "[n IN nodes(p) | n.id] AS ns, [e IN relationships(p) | e.k] AS ks",
            "o.id AS start, length(p) AS hops, [n IN nodes(p) | n.id] AS ns",
        ] {
            let (subject, baseline) = pair(BIND_END, "p = (o:N)-[r:E*1..3]->(x)", returned);
            assert_same(
                &forge,
                &format!("{partitions} partitions, {returned}"),
                &subject,
                &baseline,
            );
        }
        // Spelled out: the path starts at `o`, ends at `x` and its nodes are
        // the walk of the edge list.
        let (subject, _) = pair(
            BIND_END,
            "p = (o:N)-[r:E*1..3]->(x)",
            "[n IN nodes(p) | n.id] AS ns, [e IN r | e.k] AS ks",
        );
        let rows = answer(&forge, &subject);
        assert!(rows.contains(&"[1, 2, 5],[12, 25]".to_owned()), "{rows:?}");
        for row in &rows {
            let nodes = row.split(",[").next().expect("nodes");
            assert!(
                nodes
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .ends_with(", 5"),
                "{row}"
            );
        }
    }
}

#[test]
fn the_length_of_a_path_and_a_relationship_list_agree_with_the_unreversed_rows() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        for (pattern, returned) in [
            (
                "p = (o:N)-[r:E*1..3]->(x)",
                "o.id AS start, length(p) AS hops, count(*) AS trails",
            ),
            (
                "(o:N)-[r:E*0..3]->(x)",
                "length(r) AS hops, count(*) AS trails",
            ),
            (
                "(o:N)-[r:E*2..4]->(x)",
                "size(r) AS hops, count(*) AS trails",
            ),
        ] {
            let (subject, baseline) = pair(BIND_END, pattern, returned);
            assert_same(
                &forge,
                &format!("{partitions} partitions, {pattern}"),
                &subject,
                &baseline,
            );
        }
    }
}

#[test]
fn an_undirected_relationship_returns_the_unreversed_rows() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        // The ring has parallel edges, a self-loop and a two-cycle, so the
        // trails cross both orientations of the same edge.
        for (min, max) in [(1, 3), (0, 2), (2, 3)] {
            for returned in [
                "o.id AS start, count(*) AS trails",
                "o.id AS start, [e IN r | e.k] AS keys",
                "o.id AS start, [n IN nodes(p) | n.id] AS ns, [e IN relationships(p) | e.k] AS ks",
            ] {
                let pattern = if returned.contains("nodes(p)") {
                    format!("p = (o:N)-[r:E*{min}..{max}]-(x)")
                } else {
                    format!("(o:N)-[r:E*{min}..{max}]-(x)")
                };
                let (subject, baseline) = pair(BIND_END, &pattern, returned);
                assert_same(
                    &forge,
                    &format!("{partitions} partitions, {pattern}, {returned}"),
                    &subject,
                    &baseline,
                );
            }
        }
    }
}

#[test]
fn a_pattern_written_against_the_edge_direction_returns_the_unreversed_rows() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        for returned in ["o.id AS start, [e IN r | e.k] AS keys", "p"] {
            let pattern = if returned == "p" {
                "p = (o:N)<-[r:E*1..3]-(x)"
            } else {
                "(o:N)<-[r:E*1..3]-(x)"
            };
            let (subject, baseline) = pair(BIND_END, pattern, returned);
            assert_same(
                &forge,
                &format!("{partitions} partitions, {returned}"),
                &subject,
                &baseline,
            );
        }
    }
}

#[test]
fn the_label_and_properties_of_the_scanned_variable_still_apply() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        let (subject, baseline) = pair(
            BIND_END,
            "(o:M)-[r:E*1..3]->(x)",
            "o.id AS start, o.name AS name, [e IN r | e.k] AS keys",
        );
        let rows = assert_same(
            &forge,
            &format!("{partitions} partitions"),
            &subject,
            &baseline,
        );
        assert_eq!(rows, {
            let mut expected: Vec<String> = trails(1, 3, END, |n| n % 2 == 0)
                .iter()
                .map(|trail| {
                    let (start, keys) = trail.split_once(',').expect("trail");
                    format!("{start},n{start},{keys}")
                })
                .collect();
            expected.sort();
            expected
        });
        assert!(rows.iter().all(|row| {
            row.split(',')
                .next()
                .expect("start")
                .parse::<i64>()
                .expect("id")
                % 2
                == 0
        }));
    }
}

#[test]
fn the_end_may_be_bound_by_an_expansion_and_may_repeat() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        // The shape of the benchmark statement: the end is a node a person
        // owns, two of them here.
        let owned = "MATCH (person:Person {id: 1})-[:OWN]->(x:N)";
        for returned in [
            "x.id AS end, o.id AS start, [e IN r | e.k] AS keys",
            "x.id AS end, o.id AS start, [n IN nodes(p) | n.id] AS ns",
        ] {
            let pattern = if returned.contains("nodes(p)") {
                "p = (o:N)-[r:E*1..3]->(x)"
            } else {
                "(o:N)-[r:E*1..3]->(x)"
            };
            let (subject, baseline) = pair(owned, pattern, returned);
            let rows = assert_same(
                &forge,
                &format!("{partitions} partitions, owned"),
                &subject,
                &baseline,
            );
            assert!(rows.iter().any(|row| row.starts_with("5,")));
            assert!(rows.iter().any(|row| row.starts_with("7,")));
        }
        // The same end three times, one of them absent from the graph.
        let repeated = "UNWIND [5, 5, 7, 99] AS wanted MATCH (x:N {id: wanted})";
        let (subject, baseline) = pair(
            repeated,
            "(o:N)-[r:E*1..3]->(x)",
            "x.id AS end, o.id AS start, [e IN r | e.k] AS keys",
        );
        let rows = assert_same(
            &forge,
            &format!("{partitions} partitions, repeated"),
            &subject,
            &baseline,
        );
        let fives = rows.iter().filter(|row| row.starts_with("5,")).count();
        assert_eq!(fives, 2 * trails(1, 3, 5, |_| true).len());
        assert!(rows.iter().all(|row| !row.starts_with("99,")));
    }
}

#[test]
fn a_missing_or_null_end_returns_no_rows() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        for prefix in ["MATCH (x:N {id: 99})", "OPTIONAL MATCH (x:N {id: 99})"] {
            let (subject, baseline) = pair(
                prefix,
                "(o:N)-[r:E*0..3]->(x)",
                "o.id AS start, [e IN r | e.k] AS keys",
            );
            assert!(
                is_reversed(&forge, &subject),
                "{prefix}: subject is reversed"
            );
            assert!(!is_reversed(&forge, &baseline), "{prefix}: baseline is not");
            assert_eq!(answer(&forge, &subject), Vec::<String>::new(), "{prefix}");
            assert_eq!(answer(&forge, &baseline), Vec::<String>::new(), "{prefix}");
        }
    }
}

#[test]
fn shapes_the_search_does_not_reverse_keep_the_unreversed_plan() {
    let forge = forge(1, 0);
    for (name, query) in [
        // The scanned variable is filtered in the pattern, so it is anchored.
        (
            "inline filter on the scanned variable",
            "MATCH (x:N {id: 5}) MATCH (o:N {id: 1})-[r:E*1..3]->(x) RETURN o.id, [e IN r | e.k]",
        ),
        // A later predicate on the scanned variable narrows its scan.
        (
            "predicate on the scanned variable",
            "MATCH (x:N {id: 5}) MATCH (o:N)-[r:E*1..3]->(x) WHERE o.id = 1 RETURN o.id, [e IN r | e.k]",
        ),
        // A pattern without a bound end is not reversed.
        (
            "unbound end",
            "MATCH (o:N)-[r:E*1..3]->(x:N) RETURN o.id, x.id, [e IN r | e.k]",
        ),
        // Fixed hops are not variable-length expansions.
        (
            "fixed hop",
            "MATCH (x:N {id: 5}) MATCH (o:N)-[r:E]->(x) RETURN o.id, r.k",
        ),
    ] {
        assert!(!is_reversed(&forge, query), "{name}");
        assert!(!answer(&forge, query).is_empty(), "{name}");
    }
}

/// The trails of [`trails`] whose every edge key is below `limit`.
fn trails_below(min: usize, max: usize, limit: i64) -> Vec<String> {
    trails(min, max, END, |_| true)
        .into_iter()
        .filter(|trail| {
            let (_, keys) = trail.split_once(',').expect("trail");
            keys.trim_matches(|c| c == '[' || c == ']')
                .split(", ")
                .filter(|key| !key.is_empty())
                .all(|key| key.parse::<i64>().expect("key") < limit)
        })
        .collect()
}

#[test]
fn a_per_edge_predicate_keeps_its_meaning_in_both_modes() {
    for partitions in PARTITIONS {
        let forge = forge(partitions, 0);
        let bound = "MATCH (x:N {id: 5}) MATCH (o:N)-[r:E*1..3]->(x)";
        let all = "ALL(e IN r WHERE e.k < 900)";

        // The relationship list is returned, so the search enumerates trails
        // and the predicate filters their lists: the search runs from the end.
        let returned = "o.id AS start, [e IN r | e.k] AS keys";
        let subject = format!("{bound} WHERE {all} RETURN {returned}");
        let baseline = format!("{bound} WHERE {all} AND o.id >= 0 RETURN {returned}");
        let context = format!("{partitions} partitions, enumerated");
        let rows = assert_same(&forge, &context, &subject, &baseline);
        let mut expected = trails_below(1, 3, 900);
        expected.sort();
        assert_eq!(rows, expected, "{context}");
        assert!(
            expected.len() < trails(1, 3, END, |_| true).len(),
            "the predicate removes trails"
        );

        // Only the set of starts is returned, so the search is a reachability
        // search that applies the predicate to each edge itself. That mode is
        // not reversed, and its answer does not change.
        let subject = format!("{bound} WHERE {all} RETURN DISTINCT o.id AS start");
        let plan = forge
            .explain_stage(&subject, ExplainStage::PhysicalPlan)
            .expect("plan");
        assert!(
            plan.contains("mode=reachability"),
            "{partitions} partitions:\n{plan}"
        );
        assert!(
            !plan.contains("edge_list=reversed"),
            "{partitions} partitions:\n{plan}"
        );
        let mut starts: Vec<String> = trails_below(1, 3, 900)
            .iter()
            .map(|trail| trail.split(',').next().expect("start").to_owned())
            .collect();
        starts.sort();
        starts.dedup();
        assert_eq!(
            answer(&forge, &subject),
            starts,
            "{partitions} partitions, reachability"
        );
    }
}
