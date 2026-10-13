//! Reachability-mode variable-length expansion against full trail enumeration.
//!
//! A variable-length pattern whose result depends only on which nodes are
//! reached, and how soon, runs as a level-synchronous search with a visited set
//! (`VarLenMode::Reachability`) instead of enumerating every edge-distinct trail.
//! Each query here runs twice over the same random graph: once as written, which
//! the lowerer proves reachability-only, and once with `WHERE size(r) >= 0`
//! added, which always holds but reads the relationship list and so keeps trail
//! enumeration. The two must return the same rows.
//!
//! The graphs are small and dense enough to contain every shape the proof has to
//! cover: cycles, self-loops, parallel relationships, relationships in both
//! directions between two nodes, isolated nodes, and a second relation type.
//!
//! A top-level `ALL(e IN r WHERE p)` that reads only the relationship `e` itself
//! is pushed into the search as a per-relationship filter (#1984), so a window
//! over a property such as TCR3's timestamp runs as a search over the
//! relationships that pass it. The same differential applies: the enumerating form
//! evaluates the quantifier over every trail, and both must agree on every graph,
//! including where the property varies, is `NULL`, or is stored on no
//! relationship at all.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use tempfile::TempDir;

use graphforge_exec::demand::{self, DemandSnapshot, HopSnapshot};
use graphforge_exec::{ExecutionResult, ExecutionSession};
use graphforge_ir::{Binder, GraphPlan, IrLiteral, OntologyMode, RuntimeCatalog};
use graphforge_storage::GraphCatalog;
use graphforge_storage::adjacency::build_adjacency_index;

const TS: i64 = 1_700_000_000_000_000;

fn bind(query: &str, rc: &Arc<Mutex<RuntimeCatalog>>) -> GraphPlan {
    let binder = Binder::new(None, Arc::clone(rc), OntologyMode::Advisory);
    let ast = graphforge_cypher::parse(query).expect("parse");
    binder
        .bind(&ast)
        .unwrap_or_else(|e| panic!("bind {query:?}: {e:?}"))
}

fn session(dir: &Path, rc: &Arc<Mutex<RuntimeCatalog>>) -> ExecutionSession {
    let catalog = GraphCatalog::open(dir, None, &rc.lock().unwrap()).unwrap();
    ExecutionSession::new_with_target(catalog, None, dir.to_path_buf(), OntologyMode::Advisory)
        .unwrap()
}

/// One relationship: `(source, target, type)`.
type Edge = (usize, usize, &'static str);

/// A graph of `nodes` nodes labelled `N` with `id` 0..`nodes`, and relationships
/// `(source, target, type)`. A relationship may carry an integer property `w`.
struct Graph {
    nodes: usize,
    edges: Vec<Edge>,
    /// `w` of each relationship, by position in `edges`; a missing or `None`
    /// entry stores no `w`, which reads as `NULL`.
    weights: Vec<Option<i64>>,
}

impl Graph {
    fn new(nodes: usize, edges: Vec<Edge>) -> Self {
        Self {
            nodes,
            edges,
            weights: Vec::new(),
        }
    }

    /// The same graph with `w` of each relationship (by position) from `weight`.
    fn weighted(mut self, weight: impl Fn(usize, &Edge) -> Option<i64>) -> Self {
        self.weights = self
            .edges
            .iter()
            .enumerate()
            .map(|(index, edge)| weight(index, edge))
            .collect();
        self
    }

    fn create_statement(&self) -> String {
        let mut parts: Vec<String> = (0..self.nodes)
            .map(|id| format!("(n{id}:N {{id: {id}}})"))
            .collect();
        parts.extend(self.edges.iter().enumerate().map(|(index, (s, t, ty))| {
            match self.weights.get(index).copied().flatten() {
                Some(w) => format!("(n{s})-[:{ty} {{w: {w}}}]->(n{t})"),
                None => format!("(n{s})-[:{ty}]->(n{t})"),
            }
        }));
        format!("CREATE {}", parts.join(", "))
    }

    /// Pseudo-random edges over `nodes` nodes from `seed`: self-loops and
    /// parallel relationships occur by chance, and `T` and `U` relationships mix.
    fn random(seed: u64, nodes: usize, t_edges: usize, u_edges: usize) -> Self {
        let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = |bound: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            usize::try_from(state % bound as u64).unwrap()
        };
        let mut edges = Vec::new();
        for _ in 0..t_edges {
            edges.push((next(nodes), next(nodes), "T"));
        }
        for _ in 0..u_edges {
            edges.push((next(nodes), next(nodes), "U"));
        }
        Self::new(nodes, edges)
    }

    /// Every shape the proof has to cover, on purpose rather than by chance:
    /// a parallel pair `0 => 1`, a 2-cycle `1 <-> 2`, a self-loop on `3`, a
    /// triangle `3 -> 4 -> 5 -> 3`, a tail `5 -> 6` and an isolated node `7`.
    fn shapes() -> Self {
        Self::new(
            8,
            vec![
                (0, 1, "T"),
                (0, 1, "T"),
                (1, 2, "T"),
                (2, 1, "T"),
                (2, 3, "T"),
                (3, 3, "T"),
                (3, 4, "T"),
                (4, 5, "T"),
                (5, 3, "T"),
                (5, 6, "T"),
                (4, 4, "U"),
                (6, 7, "U"),
            ],
        )
    }
}

impl Graph {
    /// Closed trails of several lengths through shared nodes: a 5-cycle
    /// `0 -> 1 -> 2 -> 3 -> 4 -> 0` and a 6-cycle `0 -> 5 -> ... -> 9 -> 0`
    /// sharing node `0`, with a chord `2 -> 4` that makes a triangle through `3`
    /// and a 4-cycle through `0`. Closed trails to the seed that are longer
    /// than a triangle, and joins of two cycles at the seed, are the cases the
    /// seed's own result has to get right.
    fn cycles() -> Self {
        let mut edges: Vec<Edge> = vec![
            (0, 1, "T"),
            (1, 2, "T"),
            (2, 3, "T"),
            (3, 4, "T"),
            (4, 0, "T"),
        ];
        edges.extend([(0, 5, "T"), (5, 6, "T"), (6, 7, "T"), (7, 8, "T")]);
        edges.extend([(8, 9, "T"), (9, 0, "T"), (2, 4, "T")]);
        Self::new(10, edges)
    }
}

impl Graph {
    /// A source node `0` and `depth` layers of two nodes each, the source joined
    /// to both nodes of the first layer and every node of a layer to both nodes
    /// of the next (layer `k` holds nodes `2k - 1` and `2k`). A node of layer
    /// `k` is reached by `2^(k-1)` distinct trails, so enumerating trails is
    /// exponential in `depth` while the graph has `4 * depth - 2` relationships.
    fn ladder(depth: usize) -> Self {
        let layer = |k: usize| [2 * k - 1, 2 * k];
        let mut edges = Vec::new();
        for to in layer(1) {
            edges.push((0, to, "T"));
        }
        for k in 1..depth {
            for from in layer(k) {
                for to in layer(k + 1) {
                    edges.push((from, to, "T"));
                }
            }
        }
        Self::new(1 + 2 * depth, edges)
    }
}

/// Canonical multiset of a result's rows: column-name-sorted `name=value`
/// rendering per row, rows sorted.
fn canonical_rows(result: &ExecutionResult) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in &result.batches {
        let mut cols: Vec<(String, arrow::array::ArrayRef)> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), batch.column(i).clone()))
            .collect();
        cols.sort_by(|a, b| a.0.cmp(&b.0));
        for row in 0..batch.num_rows() {
            let parts: Vec<String> = cols
                .iter()
                .map(|(name, col)| {
                    let rendered = if col.is_null(row) {
                        "NULL".to_owned()
                    } else {
                        arrow::util::display::array_value_to_string(col, row)
                            .unwrap_or_else(|e| panic!("render {name} at row {row}: {e}"))
                    };
                    format!("{name}={rendered}")
                })
                .collect();
            rows.push(parts.join(","));
        }
    }
    rows.sort();
    rows
}

/// `query` with a condition that always holds but reads the relationship list
/// `r`, so the expansion keeps trail enumeration.
fn enumerating(query: &str) -> String {
    if query.contains(" WHERE ") {
        query.replacen(" WHERE ", " WHERE size(r) >= 0 AND ", 1)
    } else {
        query.replacen(" RETURN ", " WHERE size(r) >= 0 RETURN ", 1)
    }
}

/// A query bound twice: as written, and in its enumerating form.
struct Pair {
    query: String,
    reach: GraphPlan,
    trails: GraphPlan,
}

/// Bind every query before any statement runs: a read session only sees the
/// properties interned before it opened (the property-visibility rule
/// `read_execution.rs` documents).
fn bind_pairs(rc: &Arc<Mutex<RuntimeCatalog>>, queries: &[String]) -> Vec<Pair> {
    queries
        .iter()
        .map(|query| Pair {
            query: query.clone(),
            reach: bind(query, rc),
            trails: bind(&enumerating(query), rc),
        })
        .collect()
}

/// Run `pair` as written and enumerating and assert both return the same rows;
/// returns how many. With `check_plans`, first assert that the written form plans
/// exactly one reachability expansion and the enumerating form plans none. The
/// plan depends on the query, not the data, so one pass over a corpus is enough.
async fn compare(session: &ExecutionSession, pair: &Pair, check_plans: bool) -> Vec<String> {
    let query = &pair.query;
    if check_plans {
        let reach_plan = session
            .explain_physical(&pair.reach)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e:?}"));
        let trail_plan = session
            .explain_physical(&pair.trails)
            .await
            .unwrap_or_else(|e| panic!("{query} (enumerating): {e:?}"));
        assert!(
            reach_plan.contains("VarLenExpandExec") && reach_plan.contains("mode=reachability"),
            "{query}: expected a reachability expansion:\n{reach_plan}"
        );
        assert_eq!(
            reach_plan.matches("mode=reachability").count(),
            1,
            "{query}: one expansion in reachability mode:\n{reach_plan}"
        );
        assert!(
            trail_plan.contains("VarLenExpandExec") && !trail_plan.contains("mode=reachability"),
            "{query}: the enumerating form must keep trails:\n{trail_plan}"
        );
        // A `WHERE ALL(e IN r WHERE ...)` that reads only `e` is the search's own
        // filter; the trail form evaluates it over each trail instead.
        let pushed = query.contains("ALL(e IN r WHERE");
        assert_eq!(
            reach_plan.matches("edge_filter=").count(),
            usize::from(pushed),
            "{query}: the quantifier is pushed into the search exactly when written:\n{reach_plan}"
        );
        assert!(
            !trail_plan.contains("edge_filter="),
            "{query}: trails carry no search filter:\n{trail_plan}"
        );
    }

    let from_reach = canonical_rows(
        &session
            .execute_plan(&pair.reach)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e:?}")),
    );
    let from_trails = canonical_rows(
        &session
            .execute_plan(&pair.trails)
            .await
            .unwrap_or_else(|e| panic!("{query} (enumerating): {e:?}")),
    );
    assert_eq!(
        from_reach, from_trails,
        "{query}: reachability and trail enumeration differ"
    );
    from_reach
}

/// Direction and bound combinations, over the relation type `T`, the wildcard,
/// and the relation `U`.
fn patterns() -> Vec<String> {
    let mut patterns = Vec::new();
    for bounds in ["*1..2", "*1..3", "*0..2", "*0..3", "*1..", "*0.."] {
        for (open, close) in [("-", "->"), ("<-", "-"), ("-", "-")] {
            patterns.push(format!("(a:N){open}[r:T{bounds}]{close}(b:N)"));
        }
    }
    patterns.push("(a:N)-[r*1..3]->(b:N)".to_owned());
    patterns.push("(a:N)-[r*0..2]-(b:N)".to_owned());
    patterns.push("(a:N)-[r:U*1..3]-(b:N)".to_owned());
    patterns
}

/// The consumers that ignore row multiplicity, over a pattern with relationship
/// variable `r`.
fn consumers(pattern: &str) -> Vec<String> {
    vec![
        // Shortest distance per pair (IC1, TCR3's `min(size(r))`).
        format!("MATCH {pattern} RETURN a.id AS a, b.id AS b, min(length(r)) AS d"),
        format!("MATCH {pattern} RETURN a.id AS a, b.id AS b, min(size(r)) AS d"),
        format!("MATCH {pattern} RETURN min(length(r)) AS d"),
        // Distinct destinations (IC3, IC5, IC9, IC11, TCR8, TCR11).
        format!("MATCH {pattern} RETURN DISTINCT a.id AS a, b.id AS b"),
        format!("MATCH {pattern} WHERE NOT a = b WITH DISTINCT a, b RETURN a.id AS a, b.id AS b"),
        format!("MATCH {pattern} RETURN a.id AS a, count(DISTINCT b) AS reached"),
        format!("MATCH {pattern} RETURN a.id AS a, collect(DISTINCT b.id) AS ids"),
        format!("MATCH {pattern} RETURN a.id AS a, max(b.id) AS highest"),
    ]
}

/// Build `graph` and compare every query on it. Returns the rows each query
/// returned, so a corpus that quietly matches nothing fails its caller's floor.
async fn compare_graph(
    graph: &Graph,
    build_index: bool,
    check_plans: bool,
    queries: &[String],
) -> Vec<Vec<String>> {
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let ids = bind("MATCH (n:N) RETURN n.id AS id", &rc);
    let pairs = bind_pairs(&rc, queries);
    let create = bind(&graph.create_statement(), &rc);
    session(dir.path(), &rc)
        .execute_create(&create)
        .await
        .unwrap();
    if build_index {
        build_adjacency_index(dir.path(), TS).unwrap();
    }
    let session = session(dir.path(), &rc);

    // The ids the queries group and pin on must be real: unreadable properties
    // read as NULL and would make every comparison degenerate.
    let expected: Vec<String> = (0..graph.nodes).map(|id| format!("id={id}")).collect();
    let mut actual = canonical_rows(&session.execute_plan(&ids).await.unwrap());
    actual.sort_by_key(|row| row.trim_start_matches("id=").parse::<usize>().unwrap());
    assert_eq!(actual, expected, "node ids must be readable");

    let mut rows = Vec::new();
    for pair in &pairs {
        rows.push(compare(&session, pair, check_plans).await);
    }
    rows
}

fn total(rows: &[Vec<String>]) -> usize {
    rows.iter().map(Vec::len).sum()
}

/// A `w` for each relationship that varies along any path, with a `NULL` every
/// fifth one: on `Graph::shapes` it is 0, 2, 4, 1, NULL, 0, 2, 4, 1, NULL, 0, 2.
fn varied(index: usize, _: &Edge) -> Option<i64> {
    (index % 5 != 4).then(|| i64::try_from(index * 7 % 5).unwrap())
}

/// A pseudo-random `w` in `0..5`, `NULL` about one time in seven.
fn scattered(seed: u64) -> impl Fn(usize, &Edge) -> Option<i64> {
    move |index, _| {
        let h = (u64::try_from(index).unwrap() * 2_654_435_761 + seed * 40_503) % 7;
        (h != 6).then(|| i64::try_from(h % 5).unwrap())
    }
}

/// Predicates over the relationship `e` alone: comparisons, boolean connectives
/// (also over `NULL`), arithmetic, a function, and a property no relationship
/// stores. Each is a conjunct of a `WHERE` once wrapped by [`with_all`].
const WINDOWS: [&str; 12] = [
    "e.w > 1",
    "e.w >= 2 AND e.w <= 3",
    "e.w < 3",
    "e.w <> 2",
    "NOT e.w = 2",
    "e.w IS NULL",
    "e.w IS NOT NULL",
    "coalesce(e.w, 0) > 0",
    "e.w > 1 OR e.w IS NULL",
    "e.w + 1 > 2",
    "e.absent > 0",
    "e.absent IS NULL",
];

/// `query` with `ALL(e IN r WHERE predicate)` as a top-level conjunct of its
/// `WHERE`, added to one if there is one.
fn with_all(query: &str, predicate: &str) -> String {
    let all = format!("ALL(e IN r WHERE {predicate})");
    if query.contains(" WHERE ") {
        query.replacen(" WHERE ", &format!(" WHERE {all} AND "), 1)
    } else {
        query.replacen(" RETURN ", &format!(" WHERE {all} RETURN "), 1)
    }
}

/// Every pattern under the given consumers of [`consumers`], unfiltered.
fn plain_queries(consumer_indices: &[usize]) -> Vec<String> {
    let mut queries = Vec::new();
    for pattern in patterns() {
        let all = consumers(&pattern);
        queries.extend(consumer_indices.iter().map(|&index| all[index].clone()));
    }
    queries
}

#[tokio::test]
async fn an_all_predicate_over_each_relationship_matches_trail_enumeration() {
    // Each unfiltered query, then the same query under each predicate: the
    // search runs over the relationships that pass, the enumeration over every
    // trail that is made only of them. The plan checks make sure the first is
    // a reachability expansion carrying the predicate.
    let plain = plain_queries(&[0, 3, 5, 7]);
    let mut queries = plain.clone();
    for query in &plain {
        for predicate in WINDOWS {
            queries.push(with_all(query, predicate));
        }
    }
    let graph = Graph::shapes().weighted(varied);
    let indexed = compare_graph(&graph, true, true, &queries).await;
    let scanned = compare_graph(&graph, false, false, &queries).await;
    assert_eq!(indexed, scanned, "indexed and scan-built adjacency agree");

    let (unfiltered, filtered) = indexed.split_at(plain.len());
    let filtered_total = total(filtered);
    assert!(
        filtered_total > 1_000,
        "the corpus must return rows: {filtered_total}"
    );
    // The predicates must change the answers, or the corpus proves nothing.
    let changed = filtered
        .chunks(WINDOWS.len())
        .zip(unfiltered)
        .flat_map(|(rows, plain)| rows.iter().filter(move |rows| *rows != plain))
        .count();
    assert!(
        changed > filtered.len() / 4,
        "only {changed} of {} filtered queries differ from the unfiltered one",
        filtered.len()
    );
}

#[tokio::test]
async fn an_all_predicate_matches_trail_enumeration_on_random_graphs() {
    let plain = plain_queries(&[0, 3]);
    let mut rows = 0;
    for seed in 1..=8_u64 {
        // A different window per pattern and seed, so every predicate meets
        // every graph over the run.
        let queries: Vec<String> = plain
            .iter()
            .enumerate()
            .map(|(index, query)| {
                with_all(
                    query,
                    WINDOWS[(index + usize::try_from(seed).unwrap()) % WINDOWS.len()],
                )
            })
            .collect();
        let graph = Graph::random(seed, 6, 8, 2).weighted(scattered(seed));
        rows += total(&compare_graph(&graph, seed % 2 == 0, false, &queries).await);
    }
    assert!(rows > 500, "the corpus must return rows: {rows}");
}

#[tokio::test]
async fn an_all_predicate_decides_which_closed_trails_reach_the_seed() {
    // The seed is a result of its own only through a closed trail, and under a
    // filter only through a closed trail of passing relationships.
    let mut queries = Vec::new();
    for bound in ["*1..", "*1..5", "*0.."] {
        for (open, close) in [("-", "->"), ("<-", "-"), ("-", "-")] {
            // Nodes on the 5-cycle only, on the 6-cycle only, and on both.
            for node in [0_usize, 2, 3, 7] {
                for predicate in [WINDOWS[3], WINDOWS[8]] {
                    queries.push(with_all(
                        &format!(
                            "MATCH (a:N {{id: {node}}}){open}[r:T{bound}]{close}(b:N {{id: {node}}}) \
                             RETURN coalesce(min(size(r)), -1) AS shortest"
                        ),
                        predicate,
                    ));
                }
            }
        }
    }
    let graph = Graph::cycles().weighted(varied);
    let rows = compare_graph(&graph, true, true, &queries).await;
    assert_eq!(rows.len(), queries.len());
    let reaching = rows.iter().filter(|rows| *rows != &["shortest=-1"]).count();
    assert!(
        reaching > 10 && reaching < rows.len(),
        "{reaching} of {} seeds reach themselves: the filter must both keep and cut cycles",
        rows.len()
    );
}

#[tokio::test]
async fn a_predicate_false_on_the_short_path_is_met_by_a_longer_one() {
    // `0 -> 3` is one relationship with `w = 1`; `0 -> 1 -> 2 -> 3` is three
    // with `w = 5`; `3 -> 4` has `w = 9`.
    let graph = Graph::new(
        5,
        vec![
            (0, 3, "T"),
            (0, 1, "T"),
            (1, 2, "T"),
            (2, 3, "T"),
            (3, 4, "T"),
        ],
    )
    .weighted(|index, _| Some([1, 5, 5, 5, 9][index]));
    let shortest = |bound: &str, source: usize, target: usize, predicate: &str| {
        with_all(
            &format!(
                "MATCH (a:N {{id: {source}}})-[r:T{bound}]->(b:N {{id: {target}}}) \
                 RETURN coalesce(min(size(r)), -1) AS d"
            ),
            predicate,
        )
    };
    let reached = |source: usize, predicate: &str| {
        with_all(
            &format!(
                "MATCH (a:N {{id: {source}}})-[r:T*1..]->(b:N) \
                 RETURN b.id AS b, min(length(r)) AS d"
            ),
            predicate,
        )
    };
    let cases = [
        (shortest("*1..", 0, 3, "e.w > 0"), vec!["d=1"]),
        (shortest("*1..", 0, 3, "e.w > 1"), vec!["d=3"]),
        (shortest("*1..", 0, 4, "e.w > 1"), vec!["d=4"]),
        (shortest("*1..", 0, 4, "e.w > 5"), vec!["d=-1"]),
        (shortest("*1..2", 0, 3, "e.w > 1"), vec!["d=-1"]),
        (shortest("*1..3", 0, 3, "e.w > 1"), vec!["d=3"]),
        // The empty list satisfies every `ALL`: the seed reaches itself.
        (shortest("*0..", 0, 0, "e.w > 9"), vec!["d=0"]),
        (shortest("*1..", 0, 0, "e.w > 0"), vec!["d=-1"]),
        (
            reached(0, "e.w > 1"),
            vec!["b=1,d=1", "b=2,d=2", "b=3,d=3", "b=4,d=4"],
        ),
        (
            reached(0, "e.w > 0"),
            vec!["b=1,d=1", "b=2,d=2", "b=3,d=1", "b=4,d=2"],
        ),
        // Only `3 -> 4` passes, so nothing is reachable from `0`.
        (reached(0, "e.w > 5"), vec![]),
        (reached(3, "e.w > 5"), vec!["b=4,d=1"]),
    ];
    let queries: Vec<String> = cases.iter().map(|(query, _)| query.clone()).collect();
    let rows = compare_graph(&graph, true, true, &queries).await;
    for ((query, expected), rows) in cases.iter().zip(rows) {
        let mut expected: Vec<String> = expected.iter().map(|row| (*row).to_owned()).collect();
        expected.sort();
        assert_eq!(rows, expected, "{query}");
    }
}

#[tokio::test]
async fn reachability_matches_trail_enumeration_on_every_shape() {
    let queries: Vec<String> = patterns()
        .iter()
        .flat_map(|pattern| consumers(pattern))
        .collect();
    let graph = Graph::shapes();
    // Indexed and scan-built adjacency answer the same.
    let indexed = compare_graph(&graph, true, true, &queries).await;
    let scanned = compare_graph(&graph, false, false, &queries).await;
    assert_eq!(indexed, scanned);
    let indexed = total(&indexed);
    assert!(indexed > 1_000, "the corpus must return rows: {indexed}");
}

#[tokio::test]
async fn reachability_matches_trail_enumeration_on_random_graphs() {
    // Two consumers per pattern keep the corpus quick (the enumerating form is
    // exponential in the relationships); the shapes test above runs every
    // consumer and checks every plan.
    let queries: Vec<String> = patterns()
        .iter()
        .flat_map(|pattern| {
            [
                format!("MATCH {pattern} RETURN a.id AS a, b.id AS b, min(length(r)) AS d"),
                format!("MATCH {pattern} RETURN DISTINCT a.id AS a, b.id AS b"),
            ]
        })
        .collect();
    let mut rows = 0;
    for seed in 1..=10 {
        let graph = Graph::random(seed, 6, 8, 2);
        rows += total(&compare_graph(&graph, seed % 2 == 0, false, &queries).await);
    }
    assert!(rows > 1_000, "the corpus must return rows: {rows}");
}

#[tokio::test]
async fn a_required_destination_stops_at_the_first_level_that_reaches_it() {
    // TCR3's shape: both ends pinned, `min(size(r))` over the trails between
    // them. The unbounded form scans the destination first and passes it to the
    // search; the bounded form keeps the join after the expansion.
    let mut queries = Vec::new();
    for bounds in ["*1..", "*0..", "*1..3"] {
        for (open, close) in [("-", "->"), ("<-", "-"), ("-", "-")] {
            for (source, target) in [(0, 1), (1, 0), (3, 3), (4, 6), (5, 5), (0, 7)] {
                queries.push(format!(
                    "MATCH (a:N {{id: {source}}}){open}[r:T{bounds}]{close}(b:N {{id: {target}}}) \
                     RETURN coalesce(min(size(r)), -1) AS shortest"
                ));
                queries.push(format!(
                    "MATCH (a:N {{id: {source}}}){open}[r:T{bounds}]{close}(b:N {{id: {target}}}) \
                     RETURN count(DISTINCT b) AS reached"
                ));
            }
        }
    }
    let graph = Graph::shapes();
    let rows = total(&compare_graph(&graph, true, true, &queries).await);
    assert!(rows >= queries.len(), "every aggregate returns a row");
    // The plan does not depend on the data: the random graphs run only the
    // `min(size(r))` form, without the plan checks.
    let shortest: Vec<String> = queries.iter().step_by(2).cloned().collect();
    for seed in 20..22 {
        let graph = Graph::random(seed, 6, 8, 2);
        compare_graph(&graph, seed % 2 == 0, false, &shortest).await;
    }
}

#[tokio::test]
async fn the_seed_is_reached_by_its_shortest_closed_trail() {
    // `a = b` with at least one relationship: the seed is a result of its own
    // only through a closed trail, whose length is the shortest cycle through
    // it. Every node of the cycles graph is such a seed, under bounds that cut
    // the cycles at different lengths.
    let bounds = [("*1..", None), ("*1..4", Some(4)), ("*1..5", Some(5))];
    let directions = [("-", "->", true), ("<-", "-", true), ("-", "-", false)];
    let mut queries = Vec::new();
    let mut expected = Vec::new();
    for (bound, max) in bounds {
        for (open, close, directed) in directions {
            for node in 0..10_usize {
                queries.push(format!(
                    "MATCH (a:N {{id: {node}}}){open}[r:T{bound}]{close}(b:N {{id: {node}}}) \
                     RETURN coalesce(min(size(r)), -1) AS shortest"
                ));
                // By hand from the drawing on `Graph::cycles`: the shortest
                // cycle through each node, following the direction or not.
                let shortest: i64 = match (node, directed) {
                    (0 | 1, _) | (2 | 4, true) => 4,
                    (2..=4, false) => 3,
                    (3, true) => 5,
                    _ => 6,
                };
                let shortest = if max.is_some_and(|max| shortest > max) {
                    -1
                } else {
                    shortest
                };
                expected.push(vec![format!("shortest={shortest}")]);
            }
        }
    }
    let rows = compare_graph(&Graph::cycles(), true, true, &queries).await;
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn an_unbounded_search_with_a_pinned_destination_scans_the_destination_first() {
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let unbounded = bind(
        "MATCH (a:N {id: 0})-[r:T*1..]->(b:N {id: 5}) RETURN min(size(r)) AS d",
        &rc,
    );
    let bounded = bind(
        "MATCH (a:N {id: 0})-[r:T*1..6]->(b:N {id: 5}) RETURN min(size(r)) AS d",
        &rc,
    );
    let create = bind(&Graph::shapes().create_statement(), &rc);
    session(dir.path(), &rc)
        .execute_create(&create)
        .await
        .unwrap();
    let session = session(dir.path(), &rc);

    for plan in [&unbounded, &bounded] {
        let text = session.explain_physical(plan).await.unwrap();
        assert!(text.contains("mode=reachability"), "{text}");
    }
    let rows = canonical_rows(&session.execute_plan(&unbounded).await.unwrap());
    assert_eq!(rows, vec!["d=5".to_owned()], "0 -> 1 -> 2 -> 3 -> 4 -> 5");
}

/// Run `plan` once with demand capture on, returning its rows and counters.
fn captured(
    runtime: &tokio::runtime::Runtime,
    session: &ExecutionSession,
    plan: &GraphPlan,
) -> (Vec<String>, DemandSnapshot) {
    let (result, snapshot) = demand::capture(|| runtime.block_on(session.execute_plan(plan)));
    (canonical_rows(&result.unwrap()), snapshot)
}

/// What one run of a query over a ladder measured, in both forms.
struct LadderRun {
    reach_rows: Vec<String>,
    trail_rows: Vec<String>,
    reach: HopSnapshot,
    trails: HopSnapshot,
    edges: usize,
}

/// The level of relationship `index` of [`Graph::ladder`]: the two from the
/// source are level 1, and each later layer adds four.
fn ladder_level(index: usize) -> usize {
    if index < 2 { 1 } else { 2 + (index - 2) / 4 }
}

/// The rows of `b.id AS b, min(length(r)) AS d` over a ladder when every node of
/// the first `levels` layers is reached: layer `k` is `k` relationships away.
fn ladder_rows(levels: usize) -> Vec<String> {
    let mut rows: Vec<String> = (1..=levels)
        .flat_map(|k| [2 * k - 1, 2 * k].map(|id| format!("b={id},d={k}")))
        .collect();
    rows.sort();
    rows
}

/// Run `query` and its enumerating form over a ladder of `depth` layers whose
/// relationship `w` comes from `weight`, with demand capture on.
fn run_ladder(
    runtime: &tokio::runtime::Runtime,
    depth: usize,
    query: &str,
    weight: impl Fn(usize, &Edge) -> Option<i64>,
) -> LadderRun {
    let graph = Graph::ladder(depth).weighted(weight);
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let reach = bind(query, &rc);
    let trails = bind(&enumerating(query), &rc);
    let create = bind(&graph.create_statement(), &rc);
    runtime.block_on(async {
        session(dir.path(), &rc)
            .execute_create(&create)
            .await
            .unwrap();
    });
    let session = session(dir.path(), &rc);

    let (reach_rows, reach_demand) = captured(runtime, &session, &reach);
    let (trail_rows, trail_demand) = captured(runtime, &session, &trails);
    assert_eq!(reach_demand.hops.len(), 1, "one expansion");
    assert_eq!(trail_demand.hops.len(), 1, "one expansion");
    let reach_hop = reach_demand.hops.values().next().unwrap().clone();
    let trail_hop = trail_demand.hops.values().next().unwrap().clone();
    assert_eq!(reach_hop.input_rows, 1, "one seed");
    LadderRun {
        reach_rows,
        trail_rows,
        reach: reach_hop,
        trails: trail_hop,
        edges: graph.edges.len(),
    }
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Run `query` over ladders of `depth` 6 and 12 whose relationships all store
/// `w = 1`, and assert the search scans each relationship once per seed while
/// the enumeration returns one row per trail, however deep the ladder is.
fn assert_work_grows_with_the_graph(query: &str) {
    let runtime = current_thread_runtime();
    let mut grown = Vec::new();
    for depth in [6, 12] {
        let run = run_ladder(&runtime, depth, query, |_, _| Some(1));
        let expected = ladder_rows(depth);
        assert_eq!(run.reach_rows, expected, "depth {depth}: reachability rows");
        assert_eq!(run.trail_rows, expected, "depth {depth}: trail rows");
        // One row per reached node, however many trails reach it.
        assert_eq!(run.reach.rows_emitted, 2 * depth as u64);
        // Each relationship is scanned at most once per seed.
        assert!(
            run.reach.candidates_generated <= run.edges as u64,
            "depth {depth}: {} candidates for {} relationships",
            run.reach.candidates_generated,
            run.edges
        );
        // The trail form returns one row per trail: 2^(depth + 1) - 2.
        assert_eq!(run.trails.rows_emitted, (1_u64 << (depth + 1)) - 2);
        grown.push((run.reach, run.trails));
    }

    // Doubling the depth doubles the search and squares the enumeration.
    let ((shallow_reach, shallow_trails), (deep_reach, deep_trails)) = (&grown[0], &grown[1]);
    assert!(deep_reach.candidates_generated <= 2 * shallow_reach.candidates_generated + 4);
    assert!(deep_trails.candidates_generated >= 32 * shallow_trails.candidates_generated);
}

#[test]
fn reachability_work_grows_with_the_graph_not_with_the_trails() {
    // `MATCH (a)-[r:T*1..]->(b) RETURN b, min(length(r))` over the ladder: the
    // search scans each relationship once, while enumerating trails scans every
    // trail's prefix and returns one row per trail. The counters are the
    // operator's own work, not a timing.
    assert_work_grows_with_the_graph(
        "MATCH (a:N {id: 0})-[r:T*1..]->(b:N) RETURN b.id AS b, min(length(r)) AS d",
    );
}

#[test]
fn reachability_work_with_an_all_predicate_grows_with_the_graph_not_with_the_trails() {
    // The same, under a predicate every relationship passes: the search still
    // evaluates it once per relationship, and the enumeration still runs over
    // every trail. Without the pushdown this query enumerates trails.
    assert_work_grows_with_the_graph(
        "MATCH (a:N {id: 0})-[r:T*1..]->(b:N) WHERE ALL(e IN r WHERE e.w > 0) \
         RETURN b.id AS b, min(length(r)) AS d",
    );
}

#[test]
fn a_predicate_bounds_the_search_to_the_relationships_that_pass() {
    // Only the relationships of the first `CUT` levels pass `e.w > 0`. The search
    // scans no relationship past the first one it may not cross, however deep the
    // ladder goes; the enumeration still builds every trail of the whole ladder
    // before the quantifier discards the ones that fail.
    const CUT: usize = 3;
    let runtime = current_thread_runtime();
    let query = "MATCH (a:N {id: 0})-[r:T*1..]->(b:N) WHERE ALL(e IN r WHERE e.w > 0) \
                 RETURN b.id AS b, min(length(r)) AS d";
    let mut scanned = Vec::new();
    for depth in [6, 12] {
        let run = run_ladder(&runtime, depth, query, |index, _| {
            Some(i64::from(ladder_level(index) <= CUT))
        });
        let expected = ladder_rows(CUT);
        assert_eq!(run.reach_rows, expected, "depth {depth}: reachability rows");
        assert_eq!(run.trail_rows, expected, "depth {depth}: trail rows");
        assert_eq!(run.reach.rows_emitted, 2 * CUT as u64);
        // The relationships of levels 1..=CUT + 1: those of level CUT + 1 are
        // evaluated and not crossed.
        let reachable = 2 + 4 * CUT;
        assert!(
            run.reach.candidates_generated <= reachable as u64,
            "depth {depth}: {} candidates for {reachable} reachable relationships",
            run.reach.candidates_generated
        );
        assert_eq!(run.trails.rows_emitted, (1_u64 << (depth + 1)) - 2);
        scanned.push(run.reach.candidates_generated);
    }
    assert_eq!(scanned[0], scanned[1], "depth does not change the work");
}

/// A small graph for plan tests, with the properties the benchmark shapes read.
const PLAN_GRAPH: &str = "CREATE (a1:Account {id: 1}), (a2:Account {id: 2}), \
     (a1)-[:transfer {timestamp: 50}]->(a2), \
     (p1:Person {id: 1, firstName: 'Ada', lastName: 'L'}), \
     (p2:Person {id: 2, firstName: 'Bo', lastName: 'M'}), (p1)-[:KNOWS {since: 2020}]->(p2)";

/// The physical plan of each query over `PLAN_GRAPH`.
async fn plans(queries: &[&str]) -> Vec<String> {
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let bound: Vec<GraphPlan> = queries.iter().map(|query| bind(query, &rc)).collect();
    let create = bind(PLAN_GRAPH, &rc);
    session(dir.path(), &rc)
        .execute_create(&create)
        .await
        .unwrap();
    let session = session(dir.path(), &rc);
    let mut plans = Vec::new();
    for (query, plan) in queries.iter().zip(&bound) {
        let text = session
            .explain_physical(plan)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e:?}"));
        assert!(
            text.contains("VarLenExpandExec"),
            "{query}: expected a variable-length expansion:\n{text}"
        );
        plans.push(text);
    }
    plans
}

#[tokio::test]
async fn reachability_is_chosen_for_the_benchmark_shapes() {
    // Each query with whether its plan carries a pushed per-relationship filter.
    let queries = [
        // Shortest transfer chain between two accounts (TCR3 without its window).
        (
            "MATCH (src:Account {id: 1})-[r:transfer*1..]->(dst:Account {id: 2}) \
             RETURN coalesce(min(size(r)), -1) AS shortestPathLength",
            false,
        ),
        // TCR3: the same, over the transfers inside a time window.
        (
            "MATCH (src:Account {id: 1})-[r:transfer*1..]->(dst:Account {id: 2}) \
             WHERE ALL(e IN r WHERE 1 < e.timestamp AND e.timestamp < 100) \
             RETURN coalesce(min(size(r)), -1) AS shortestPathLength",
            true,
        ),
        // Friends by distance (IC1).
        (
            "MATCH path = (p:Person {id: 1})-[:KNOWS*1..3]-(friend:Person {firstName: 'Bo'}) \
             WHERE NOT p = friend \
             WITH friend, min(length(path)) AS distance \
             ORDER BY distance ASC, friend.lastName ASC LIMIT 20 \
             RETURN friend.id AS friendId, distance",
            false,
        ),
        // Distinct people within reach (IC3, IC5, IC9, IC11).
        (
            "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(friend:Person) \
             WHERE NOT p = friend \
             RETURN DISTINCT friend.id AS friendId",
            false,
        ),
        // Distinct accounts reached (TCR8, TCR11).
        (
            "MATCH (src:Account {id: 1})-[:transfer*1..3]->(other:Account) \
             RETURN count(DISTINCT other) AS reached",
            false,
        ),
        // A filter that reads the relationship only, with the relationship bound.
        (
            "MATCH (p:Person {id: 1})-[r:KNOWS*1..3]-(friend:Person) \
             WHERE ALL(e IN r WHERE e.since > 0) RETURN DISTINCT friend.id AS friendId",
            true,
        ),
        // The filter beside other conjuncts: only the quantifier is lifted.
        (
            "MATCH (p:Person {id: 1})-[r:KNOWS*1..3]-(friend:Person) \
             WHERE NOT p = friend AND ALL(e IN r WHERE e.since > 0 AND e.since < 3000) \
             RETURN DISTINCT friend.id AS friendId",
            true,
        ),
    ];
    let texts: Vec<&str> = queries.iter().map(|(query, _)| *query).collect();
    for ((query, pushed), plan) in queries.iter().zip(plans(&texts).await) {
        assert_eq!(
            plan.matches("mode=reachability").count(),
            1,
            "{query}: expected one reachability expansion:\n{plan}"
        );
        assert_eq!(
            plan.matches("edge_filter=").count(),
            usize::from(*pushed),
            "{query}: expected the filter to be pushed: {pushed}\n{plan}"
        );
    }
}

#[tokio::test]
async fn trails_are_kept_when_the_trail_is_observed() {
    let queries = [
        // The relationship list or the path is returned or used beyond its length.
        "MATCH (p:Person {id: 1})-[r:KNOWS*1..3]-(friend:Person) RETURN r",
        "MATCH path = (p:Person {id: 1})-[:KNOWS*1..3]-(friend:Person) RETURN path",
        "MATCH path = (p:Person {id: 1})-[:KNOWS*1..3]-(friend:Person) \
         RETURN DISTINCT nodes(path) AS visited",
        "MATCH (p:Person {id: 1})-[r:KNOWS*1..3]-(friend:Person) \
         RETURN friend.id AS friendId, min(size(r)) AS distance, collect(r) AS trails",
        // Every trail is a row: multiplicity is observed.
        "MATCH (p:Person {id: 1})-[:KNOWS*1..3]-(friend:Person) RETURN friend.id AS friendId",
        "MATCH (p:Person {id: 1})-[:KNOWS*1..3]-(friend:Person) RETURN count(friend) AS trails",
        "MATCH (p:Person {id: 1})-[r:KNOWS*1..3]-(friend:Person) \
         RETURN friend.id AS friendId, sum(length(r)) AS total",
        // A minimum of two relationships can need a trail longer than the
        // shortest path to a node.
        "MATCH (p:Person {id: 1})-[:KNOWS*2..3]-(friend:Person) RETURN DISTINCT friend.id AS friendId",
    ];
    for (query, plan) in queries.iter().zip(plans(&queries).await) {
        assert!(
            !plan.contains("mode=reachability"),
            "{query}: the trail is observed and must be enumerated:\n{plan}"
        );
    }
}

#[tokio::test]
async fn a_predicate_that_is_not_about_the_relationship_alone_keeps_trails() {
    // Only `ALL(e IN r WHERE p)` with `p` reading the properties of `e`, parameters
    // and literals alone, as a top-level conjunct, is a property of each
    // relationship by itself. Everything else observes the trail and keeps it.
    let head = "MATCH (src:Account {id: 1})-[r:transfer*1..]->(dst:Account {id: 2}) ";
    let tail = "RETURN coalesce(min(size(r)), -1) AS shortestPathLength";
    let filters = [
        // Other quantifiers do not hold for each relationship separately.
        "WHERE ANY(e IN r WHERE e.timestamp > 0)",
        "WHERE NONE(e IN r WHERE e.timestamp < 0)",
        "WHERE SINGLE(e IN r WHERE e.timestamp > 0)",
        // The predicate reads something other than the relationship.
        "WHERE ALL(e IN r WHERE e.timestamp > size(r))",
        "WHERE ALL(e IN r WHERE e.timestamp > rand())",
        "WHERE ALL(e IN r WHERE type(e) = 'transfer')",
        "WHERE ALL(e IN r WHERE e = e)",
        // Not a top-level conjunct.
        "WHERE ALL(e IN r WHERE e.timestamp > 0) OR src.id = 1",
        "WHERE NOT ALL(e IN r WHERE e.timestamp > 0)",
        // One conjunct that cannot be lifted keeps the trail for the rest.
        "WHERE ALL(e IN r WHERE e.timestamp > 0) AND ANY(e IN r WHERE e.timestamp > 1)",
    ];
    let queries: Vec<String> = filters
        .iter()
        .map(|filter| format!("{head}{filter} {tail}"))
        .collect();
    let texts: Vec<&str> = queries.iter().map(String::as_str).collect();
    for (query, plan) in queries.iter().zip(plans(&texts).await) {
        assert!(
            !plan.contains("mode=reachability") && !plan.contains("edge_filter="),
            "{query}: must stay on trails:\n{plan}"
        );
    }

    // The predicate reads another variable of the pattern. That case is decided
    // from the bound plan: planning its trail form is a separate matter, as a
    // quantifier that reads two variables' like-named columns is ambiguous there.
    let reads = |filter: &str| {
        let plan = bind(
            &format!("{head}{filter} {tail}"),
            &Arc::new(Mutex::new(RuntimeCatalog::new())),
        );
        graphforge_ir::reachability_expansions(&plan.ops, &plan.exprs)
    };
    let alone = reads("WHERE ALL(e IN r WHERE e.timestamp < 100)");
    assert!(
        !alone.is_empty(),
        "the control, a predicate about the relationship alone, is lifted"
    );
    let other = reads("WHERE ALL(e IN r WHERE e.timestamp < dst.tag)");
    assert!(
        other.is_empty(),
        "a predicate that reads another variable stays on trails: {other:?}"
    );

    // A predicate that depends on a relationship's position is refused when the
    // query is bound, so no plan, reachability or trails, ever holds it.
    let positional =
        format!("{head}WHERE ALL(i IN range(0, size(r) - 1) WHERE r[i].timestamp > 0) {tail}");
    let ast = graphforge_cypher::parse(&positional).expect("parse");
    let refused = Binder::new(
        None,
        Arc::new(Mutex::new(RuntimeCatalog::new())),
        OntologyMode::Advisory,
    )
    .bind(&ast)
    .expect_err("a position-dependent predicate over a relationship list must not bind");
    assert!(
        format!("{refused:?}").contains("IndexedPathRelationshipPredicate"),
        "{refused:?}"
    );

    // Over the nodes of a path, not its relationships.
    let path = "MATCH p = (src:Account {id: 1})-[:transfer*1..]->(dst:Account {id: 2}) \
                WHERE ALL(n IN nodes(p) WHERE n.id > 0) RETURN min(length(p)) AS d";
    for plan in plans(&[path]).await {
        assert!(
            !plan.contains("mode=reachability") && !plan.contains("edge_filter="),
            "{path}: must stay on trails:\n{plan}"
        );
    }
}

#[test]
fn a_pinned_destination_still_ends_the_search_under_a_predicate() {
    // TCR3's shape: the destination is pinned by its own filter, and the quantifier
    // follows it. The search ends at the level that reaches the destination, so a
    // long tail beyond it is never scanned.
    let runtime = current_thread_runtime();
    let mut edges: Vec<Edge> = vec![(0, 1, "T"), (1, 2, "T")];
    edges.extend((2..20).map(|from| (from, from + 1, "T")));
    let graph = Graph::new(21, edges).weighted(|_, _| Some(1));
    let query = "MATCH (a:N {id: 0})-[r:T*1..]->(b:N {id: 2}) WHERE ALL(e IN r WHERE e.w > 0) \
                 RETURN min(size(r)) AS d";

    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let plan = bind(query, &rc);
    let create = bind(&graph.create_statement(), &rc);
    runtime.block_on(async {
        session(dir.path(), &rc)
            .execute_create(&create)
            .await
            .unwrap();
    });
    let session = session(dir.path(), &rc);
    let text = runtime.block_on(session.explain_physical(&plan)).unwrap();
    assert!(text.contains("mode=reachability"), "{text}");
    assert!(text.contains("edge_filter="), "{text}");

    let (rows, snapshot) = captured(&runtime, &session, &plan);
    assert_eq!(rows, vec!["d=2".to_owned()]);
    let hop = snapshot.hops.values().next().unwrap();
    // Two levels: `0 -> 1`, then `1 -> 2`. The 18 relationships past node 2 are
    // not scanned.
    assert!(
        hop.candidates_generated <= 3,
        "{} candidates for a destination two relationships away",
        hop.candidates_generated
    );
}

#[tokio::test]
async fn a_parameter_in_the_predicate_is_bound_before_the_search_runs() {
    let graph = Graph::shapes().weighted(varied);
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let query = "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w > $floor) \
                 RETURN a.id AS a, b.id AS b, min(length(r)) AS d";
    let pair = bind_pairs(&rc, &[query.to_owned()]).remove(0);
    let literal_pairs: Vec<(i64, Pair)> = (-1..=4)
        .map(|floor| {
            let literal = query.replace("$floor", &floor.to_string());
            (floor, bind_pairs(&rc, &[literal]).remove(0))
        })
        .collect();
    let create = bind(&graph.create_statement(), &rc);
    session(dir.path(), &rc)
        .execute_create(&create)
        .await
        .unwrap();
    let session = session(dir.path(), &rc);

    let text = session.explain_physical(&literal_pairs[0].1.reach).await;
    assert!(text.unwrap().contains("edge_filter="));

    // Unbound, the plan cannot run.
    assert!(session.execute_plan(&pair.reach).await.is_err());

    let mut distinct = std::collections::BTreeSet::new();
    for (floor, literal) in &literal_pairs {
        let params = HashMap::from([("floor".to_owned(), IrLiteral::Int(*floor))]);
        let reach = canonical_rows(
            &session
                .execute_plan_with_params(&pair.reach, &params)
                .await
                .unwrap(),
        );
        // The trail form cannot take the parameter: a quantifier keeps its
        // predicate inside an opaque function that parameter binding does not
        // reach. The enumeration is run on the literal written out instead.
        let trails = canonical_rows(&session.execute_plan(&literal.trails).await.unwrap());
        let written = canonical_rows(&session.execute_plan(&literal.reach).await.unwrap());
        assert_eq!(reach, trails, "floor {floor}: search and enumeration");
        assert_eq!(reach, written, "floor {floor}: parameter and literal");
        distinct.insert(reach);
    }
    assert!(
        distinct.len() >= 4,
        "the parameter must change the answer: {} distinct results",
        distinct.len()
    );
}
