//! A variable-length expansion copies only the input columns it needs (#1984,
//! cause of #1982).
//!
//! Every path an expansion emits repeats its input row. When the rows carry
//! many columns (the properties a query projected with `WITH`) and the parent
//! reads two of them, copying the rest into every emitted path is the dominant
//! cost, and demanding them all from the input forces the operators below to
//! read them. The expansion now demands its source node, its bound destination,
//! and the columns its parent reads, and carries every other nullable column as
//! nulls. The relationship list is built only when the parent reads it.
//!
//! The plan-shape tests run the same expansion over rows that carry one
//! projected column and rows that carry forty: the columns the expansion copies
//! must not grow with the columns nobody asked for. The result tests compute
//! their rows by hand from the fixture (a ring), so they do not depend on how
//! the plan was pruned, and each also asserts the `copied=` the plan prints, so
//! a plan that copies everything cannot pass them.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use graphforge_api::{ExecutionResourcePolicy, ExplainStage, GraphForge, GraphForgeOptions};

/// `W` nodes `1..=NODES`, each with `id`, `w = id + 10` and the properties
/// `x0..x39` (`xj = id * 100 + j`), and one `K` relationship from every node to
/// the next, the last back to the first:
/// `id -> id % NODES + 1`. A relationship's `t` is 1, except for the one that
/// leaves [`CUT`], whose `t` is 0.
const NODES: i64 = 200;
const PROPERTIES: usize = 40;
const CUT: i64 = 199;
/// Rows per batch. The planner spreads a plan over partitions only when it
/// estimates more than one batch of rows, so a small batch makes the fixture
/// span several.
const BATCH_SIZE: usize = 8;
/// How many projected columns the rows entering the expansion carry.
const CARRIED: [usize; 2] = [1, PROPERTIES];
const PARTITIONS: [usize; 2] = [1, 4];

fn forge(target_partitions: usize) -> GraphForge {
    let forge = GraphForge::new_with_options(
        None,
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(target_partitions),
                batch_size: Some(BATCH_SIZE),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .expect("in-memory forge");
    let properties: String = (0..PROPERTIES)
        .map(|j| format!(", x{j}: i * 100 + {j}"))
        .collect();
    forge
        .execute(&format!(
            "UNWIND range(1, {NODES}) AS i CREATE (:W {{id: i, w: i + 10{properties}}})"
        ))
        .expect("nodes");
    forge
        .execute(&format!(
            "MATCH (a:W), (b:W) WHERE b.id = a.id % {NODES} + 1 \
             CREATE (a)-[:K {{t: CASE WHEN a.id = {CUT} THEN 0 ELSE 1 END}}]->(b)"
        ))
        .expect("relationships");
    forge
}

/// `WITH <node>, <node>.w AS <w>, <node>.x0 AS c0, ..` projecting `carried`
/// columns, so the rows entering the next `MATCH` are `carried + 1` columns
/// wider than the node's identity.
fn with(node: &str, w: &str, carried: usize) -> String {
    let columns: String = (0..carried)
        .map(|j| format!(", {node}.x{j} AS c{j}"))
        .collect();
    format!("WITH {node}, {node}.w AS {w}{columns}")
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

fn sorted(mut expected: Vec<String>) -> Vec<String> {
    expected.sort();
    expected
}

/// The `VarLenExpandExec` line of the physical plan.
fn expansion_line(forge: &GraphForge, query: &str) -> String {
    let plan = forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    plan.lines()
        .map(str::trim_start)
        .find(|line| line.starts_with("VarLenExpandExec:"))
        .unwrap_or_else(|| panic!("no `VarLenExpandExec` in the plan:\n{plan}"))
        .to_owned()
}

/// The `N` of the `copied=N` the expansion prints: how many input columns it
/// copies into every path it emits. Absent when it copies them all.
fn copied_columns(forge: &GraphForge, query: &str) -> usize {
    let line = expansion_line(forge, query);
    let (_, tail) = line
        .split_once("copied=")
        .unwrap_or_else(|| panic!("`VarLenExpandExec` prints no copied columns: {line}"));
    tail.chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or_else(|_| panic!("no number after `copied=` in `{line}`"))
}

/// The query's rows must equal `expected` however many columns the input
/// carries and however many partitions run it, and its expansion must copy
/// exactly `copied(carried)` input columns: pruning the unread columns cannot
/// change an answer.
fn expect(
    query: impl Fn(usize) -> String,
    expected: &[String],
    copied: impl Fn(usize) -> usize,
) -> Vec<(usize, usize, String)> {
    let mut plans = Vec::new();
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        for carried in CARRIED {
            let query = query(carried);
            assert_eq!(
                rows(
                    &forge
                        .execute(&query)
                        .unwrap_or_else(|e| panic!("{query}: {e:?}"))
                        .batches
                ),
                expected,
                "{query} at {partitions} partitions carrying {carried} columns"
            );
            assert_eq!(
                copied_columns(&forge, &query),
                copied(carried),
                "{query} at {partitions} partitions carrying {carried} columns"
            );
            plans.push((partitions, carried, expansion_line(&forge, &query)));
        }
    }
    plans
}

/// The node reached after `hops` relationships from `id`.
fn walk(id: i64, hops: i64) -> i64 {
    (id - 1 + hops) % NODES + 1
}

/// Every `(source, hops)` of the variable-length pattern `*1..3` on the ring.
fn paths() -> impl Iterator<Item = (i64, i64)> {
    (1..=NODES).flat_map(|source| (1..=3).map(move |hops| (source, hops)))
}

/// Whether every relationship of the path has `t = 1`: none leaves [`CUT`].
fn passes(source: i64, hops: i64) -> bool {
    (0..hops).all(|step| walk(source, step) != CUT)
}

// ---------------------------------------------------------------------------
// Results and the columns copied
// ---------------------------------------------------------------------------

#[test]
fn a_column_read_only_above_the_expansion_survives_it() {
    // `w` and `c0` are not read by the expansion: the parent reads them.
    let expected = sorted(
        paths()
            .map(|(a, h)| format!("{},{},{}", a + 10, a * 100, walk(a, h)))
            .collect(),
    );
    expect(
        |carried| {
            format!(
                "MATCH (a:W) {} MATCH (a)-[:K*1..3]->(b:W) RETURN w, c0, b.id AS b",
                with("a", "w", carried)
            )
        },
        &expected,
        // Six identity columns the node always has, `w` and `c0`.
        |_| 8,
    );
}

#[test]
fn the_last_carried_column_survives_the_expansion() {
    let expected = sorted(
        paths()
            .map(|(a, h)| {
                format!(
                    "{},{},{}",
                    a + 10,
                    a * 100 + PROPERTIES as i64 - 1,
                    walk(a, h)
                )
            })
            .collect(),
    );
    // Only the widest input carries `c39`; the narrow one has just `c0`.
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        let query = format!(
            "MATCH (a:W) {} MATCH (a)-[:K*1..3]->(b:W) RETURN w, c{} AS last, b.id AS b",
            with("a", "w", PROPERTIES),
            PROPERTIES - 1
        );
        assert_eq!(
            rows(&forge.execute(&query).expect(&query).batches),
            expected,
            "{query} at {partitions} partitions"
        );
        assert_eq!(copied_columns(&forge, &query), 8, "{query}");
    }
}

#[test]
fn an_unread_column_is_not_copied_into_the_paths() {
    let expected = sorted(
        paths()
            .map(|(a, h)| format!("{},{}", a + 10, walk(a, h)))
            .collect(),
    );
    expect(
        |carried| {
            format!(
                "MATCH (a:W) {} MATCH (a)-[:K*1..3]->(b:W) RETURN w, b.id AS b",
                with("a", "w", carried)
            )
        },
        &expected,
        // Six identity columns and `w`: `c0` and the rest are not copied.
        |_| 7,
    );
}

#[test]
fn relationship_lists_survive_the_expansion() {
    let expected = sorted(paths().map(|(a, h)| format!("{},{h}", a + 10)).collect());
    expect(
        |carried| {
            format!(
                "MATCH (a:W) {} MATCH (a)-[r:K*1..3]->(b:W) RETURN w, size(r) AS hops",
                with("a", "w", carried)
            )
        },
        &expected,
        |_| 7,
    );
}

#[test]
fn a_per_relationship_predicate_reads_the_relationship_list() {
    // Counting the paths keeps trail enumeration, so the predicate is applied
    // to the list the expansion emits and the list must be built. The path
    // from `a` of `h` hops passes when none of its relationships leaves `CUT`.
    let expected = sorted(
        paths()
            .filter(|(a, h)| passes(*a, *h))
            .map(|(a, h)| format!("{},{},{}", a + 10, a * 100, walk(a, h)))
            .collect(),
    );
    assert!(
        expected.len() < 3 * NODES as usize,
        "the predicate must reject paths"
    );
    let plans = expect(
        |carried| {
            format!(
                "MATCH (a:W) {} MATCH (a)-[r:K*1..3]->(b:W) \
                 WHERE ALL(e IN r WHERE e.t = 1) RETURN w, c0, b.id AS b",
                with("a", "w", carried)
            )
        },
        &expected,
        |_| 8,
    );
    for (_, _, line) in plans {
        assert!(!line.contains("mode=reachability"), "{line}");
    }
}

#[test]
fn named_paths_and_their_nodes_survive_the_expansion() {
    let expected = sorted(
        paths()
            .map(|(a, h)| {
                let nodes: Vec<String> = (0..=h).map(|step| walk(a, step).to_string()).collect();
                format!("{},{h},[{}]", a + 10, nodes.join(", "))
            })
            .collect(),
    );
    expect(
        |carried| {
            format!(
                "MATCH (a:W) {} MATCH p = (a)-[:K*1..3]->(b:W) \
                 RETURN w, length(p) AS len, [n IN nodes(p) | n.id] AS ids",
                with("a", "w", carried)
            )
        },
        &expected,
        |_| 7,
    );
}

#[test]
fn a_reachability_search_copies_only_what_the_parent_reads() {
    // The shortest passing path from `a` to any node is its first relationship,
    // which passes unless it leaves `CUT`.
    let expected = sorted(
        (1..=NODES)
            .filter(|a| passes(*a, 1))
            .map(|a| format!("{},1", a + 10))
            .collect(),
    );
    assert_eq!(expected.len(), NODES as usize - 1);
    let plans = expect(
        |carried| {
            format!(
                "MATCH (a:W) {} MATCH (a)-[r:K*1..3]->(b:W) \
                 WHERE ALL(e IN r WHERE e.t = 1) RETURN w, coalesce(min(size(r)), -1) AS n",
                with("a", "w", carried)
            )
        },
        &expected,
        |_| 7,
    );
    for (_, _, line) in plans {
        assert!(line.contains("mode=reachability"), "{line}");
        assert!(line.contains("edge_filter="), "{line}");
    }
}

/// The nodes that reach `2` within three relationships, and in how many:
/// `1` in one, `200` in two, `199` in three.
fn reaching_two() -> [(i64, i64); 3] {
    [(1, 1), (NODES, 2), (NODES - 1, 3)]
}

#[test]
fn a_bound_destination_is_kept_while_wide_input_is_not_copied() {
    let expected = sorted(
        reaching_two()
            .iter()
            .map(|(a, hops)| format!("{},{hops}", a + 10))
            .collect(),
    );
    // The destination `b` is bound by an earlier clause: its identity columns
    // and `w` come along, and the carried columns do not.
    expect(
        |carried| {
            format!(
                "MATCH (a:W) {} MATCH (b:W {{id: 2}}) MATCH (a)-[r:K*1..3]->(b) \
                 RETURN w, size(r) AS n",
                with("a", "w", carried)
            )
        },
        &expected,
        // `a` and `b` have six identity columns each, and `w`.
        |_| 13,
    );
}

#[test]
fn a_bound_destination_with_a_per_relationship_predicate_is_searched_by_reachability() {
    // The path from `199` crosses the relationship that leaves it, whose `t`
    // is 0, so only the paths from `1` and `200` remain.
    let expected = sorted(
        reaching_two()
            .iter()
            .filter(|(a, hops)| passes(*a, *hops))
            .map(|(a, hops)| format!("{},{hops}", a + 10))
            .collect(),
    );
    assert_eq!(expected, ["11,1", "210,2"]);
    let plans = expect(
        |carried| {
            format!(
                "MATCH (a:W) {} MATCH (b:W {{id: 2}}) MATCH (a)-[r:K*1..3]->(b) \
                 WHERE ALL(e IN r WHERE e.t = 1) RETURN w, coalesce(min(size(r)), -1) AS n",
                with("a", "w", carried)
            )
        },
        &expected,
        |_| 13,
    );
    for (_, _, line) in plans {
        assert!(line.contains("mode=reachability"), "{line}");
    }
}

/// The nodes that reach `5` within three relationships, and in how many.
fn reaching_five() -> [(i64, i64); 3] {
    [(4, 1), (3, 2), (2, 3)]
}

#[test]
fn a_bound_end_searched_from_the_end_copies_only_what_the_parent_reads() {
    let expected = sorted(
        reaching_five()
            .iter()
            .map(|(o, hops)| format!("15,{o},{hops}"))
            .collect(),
    );
    let plans = expect(
        |carried| {
            format!(
                "MATCH (x:W {{id: 5}}) {} MATCH (o:W)-[r:K*1..3]->(x) \
                 RETURN xw, o.id AS o, size(r) AS n",
                with("x", "xw", carried)
            )
        },
        &expected,
        // The bound node's six identity columns and `xw`.
        |_| 7,
    );
    for (_, _, line) in plans {
        assert!(line.contains("edge_list=reversed"), "{line}");
    }
}

#[test]
fn a_bound_end_searched_from_the_label_returns_the_same_rows() {
    // A predicate on `o` keeps the search on the label, so `x` is a
    // destination column of the expansion's input and not its seed.
    let expected = sorted(
        reaching_five()
            .iter()
            .map(|(o, hops)| format!("15,{o},{hops}"))
            .collect(),
    );
    let plans = expect(
        |carried| {
            format!(
                "MATCH (x:W {{id: 5}}) {} MATCH (o:W)-[r:K*1..3]->(x) WHERE o.id >= 0 \
                 RETURN xw, o.id AS o, size(r) AS n",
                with("x", "xw", carried)
            )
        },
        &expected,
        // `x` and `o` have six identity columns each, plus `xw` and `o.id`.
        |_| 14,
    );
    for (_, _, line) in plans {
        assert!(!line.contains("edge_list=reversed"), "{line}");
    }
}

// ---------------------------------------------------------------------------
// Plan shape
// ---------------------------------------------------------------------------

/// A column the parent reads is one more column copied, however wide the input.
#[test]
fn each_column_the_parent_reads_is_one_more_column_copied() {
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        let copied = |reads: &str| {
            let counts: Vec<usize> = CARRIED
                .iter()
                .map(|carried| {
                    copied_columns(
                        &forge,
                        &format!(
                            "MATCH (a:W) {} MATCH (a)-[:K*1..3]->(b:W) RETURN {reads}",
                            with("a", "w", *carried)
                        ),
                    )
                })
                .collect();
            assert_eq!(
                counts[0], counts[1],
                "`{reads}` over {partitions} partitions: the columns copied grew with the \
                 columns nobody read"
            );
            counts[1]
        };
        let none = copied("b.id AS b");
        assert_eq!(copied("w, b.id AS b"), none + 1);
        assert_eq!(copied("w, c0, b.id AS b"), none + 2);
    }
}

/// The columns the expansion copies are bounded by the columns the parent
/// reads and the identity columns, however wide the input: the forty carried
/// columns are not copied. At four partitions the expansion runs on every
/// partition of a four-way input, with no exchange above it fanning a single
/// stream out, so the partitioned results above are the results of a
/// multi-partition expansion.
#[test]
fn the_expansion_copies_a_bounded_number_of_columns_over_wide_input() {
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        let query = format!(
            "MATCH (a:W) {} MATCH (a)-[:K*1..3]->(b:W) RETURN w, b.id AS b",
            with("a", "w", PROPERTIES)
        );
        let copied = copied_columns(&forge, &query);
        assert!(
            copied <= 7,
            "the expansion copies {copied} columns for a parent that reads one projected column \
             at {partitions} partitions"
        );
        if partitions == 1 {
            continue;
        }
        let plan = forge
            .explain_stage(&query, ExplainStage::PhysicalPlan)
            .expect("plan");
        let lines: Vec<&str> = plan.lines().collect();
        let expansion = lines
            .iter()
            .position(|line| line.trim_start().starts_with("VarLenExpandExec:"))
            .unwrap_or_else(|| panic!("no expansion in the plan:\n{plan}"));
        let depth = |line: &str| line.len() - line.trim_start().len();
        assert!(
            lines[expansion + 1..]
                .iter()
                .take_while(|line| depth(line) > depth(lines[expansion]))
                .any(|line| line.contains(&format!("RoundRobinBatch({partitions})"))),
            "the expansion's input is not {partitions}-way:\n{plan}"
        );
        assert!(
            !lines[..expansion].iter().any(|line| {
                line.contains("RepartitionExec") && line.contains("input_partitions=1")
            }),
            "an exchange above the expansion fans a single stream out:\n{plan}"
        );
    }
}
