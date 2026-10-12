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

use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use tempfile::TempDir;

use graphforge_exec::demand::{self, DemandSnapshot};
use graphforge_exec::{ExecutionResult, ExecutionSession};
use graphforge_ir::{Binder, GraphPlan, OntologyMode, RuntimeCatalog};
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

/// A graph of `nodes` nodes labelled `N` with `id` 0..`nodes`, and relationships
/// `(source, target, type)`.
struct Graph {
    nodes: usize,
    edges: Vec<(usize, usize, &'static str)>,
}

impl Graph {
    fn create_statement(&self) -> String {
        let mut parts: Vec<String> = (0..self.nodes)
            .map(|id| format!("(n{id}:N {{id: {id}}})"))
            .collect();
        parts.extend(
            self.edges
                .iter()
                .map(|(source, target, ty)| format!("(n{source})-[:{ty}]->(n{target})")),
        );
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
        Self { nodes, edges }
    }

    /// Every shape the proof has to cover, on purpose rather than by chance:
    /// a parallel pair `0 => 1`, a 2-cycle `1 <-> 2`, a self-loop on `3`, a
    /// triangle `3 -> 4 -> 5 -> 3`, a tail `5 -> 6` and an isolated node `7`.
    fn shapes() -> Self {
        Self {
            nodes: 8,
            edges: vec![
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
        }
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
        let mut edges: Vec<(usize, usize, &'static str)> = vec![
            (0, 1, "T"),
            (1, 2, "T"),
            (2, 3, "T"),
            (3, 4, "T"),
            (4, 0, "T"),
        ];
        edges.extend([(0, 5, "T"), (5, 6, "T"), (6, 7, "T"), (7, 8, "T")]);
        edges.extend([(8, 9, "T"), (9, 0, "T"), (2, 4, "T")]);
        Self { nodes: 10, edges }
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
        Self {
            nodes: 1 + 2 * depth,
            edges,
        }
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
        let reach_plan = session.explain_physical(&pair.reach).await.unwrap();
        let trail_plan = session.explain_physical(&pair.trails).await.unwrap();
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
    }

    let from_reach = canonical_rows(&session.execute_plan(&pair.reach).await.unwrap());
    let from_trails = canonical_rows(&session.execute_plan(&pair.trails).await.unwrap());
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

#[test]
fn reachability_work_grows_with_the_graph_not_with_the_trails() {
    // `MATCH (a)-[r:T*1..]->(b) RETURN b, min(length(r))` over the ladder: the
    // search scans each relationship once, while enumerating trails scans every
    // trail's prefix and returns one row per trail. The counters are the
    // operator's own work, not a timing.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let query = "MATCH (a:N {id: 0})-[r:T*1..]->(b:N) RETURN b.id AS b, min(length(r)) AS d";

    let mut grown = Vec::new();
    for depth in [6, 12] {
        let graph = Graph::ladder(depth);
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

        let (reach_rows, reach_demand) = captured(&runtime, &session, &reach);
        let (trail_rows, trail_demand) = captured(&runtime, &session, &trails);

        // Layer `k` is `k` relationships from the source.
        let mut expected: Vec<String> = (1..=depth)
            .flat_map(|k| [2 * k - 1, 2 * k].map(|id| format!("b={id},d={k}")))
            .collect();
        expected.sort();
        assert_eq!(reach_rows, expected, "depth {depth}: reachability rows");
        assert_eq!(trail_rows, expected, "depth {depth}: trail rows");

        assert_eq!(reach_demand.hops.len(), 1, "one expansion");
        assert_eq!(trail_demand.hops.len(), 1, "one expansion");
        let reach_hop = reach_demand.hops.values().next().unwrap();
        let trail_hop = trail_demand.hops.values().next().unwrap();
        assert_eq!(reach_hop.input_rows, 1, "one seed");
        // One row per reached node, however many trails reach it.
        assert_eq!(reach_hop.rows_emitted, 2 * depth as u64);
        // Each relationship is scanned at most once per seed.
        assert!(
            reach_hop.candidates_generated <= graph.edges.len() as u64,
            "depth {depth}: {} candidates for {} relationships",
            reach_hop.candidates_generated,
            graph.edges.len()
        );
        // The trail form returns one row per trail: 2^(depth + 1) - 2.
        assert_eq!(trail_hop.rows_emitted, (1_u64 << (depth + 1)) - 2);
        grown.push((reach_hop.clone(), trail_hop.clone()));
    }

    // Doubling the depth doubles the search and squares the enumeration.
    let ((shallow_reach, shallow_trails), (deep_reach, deep_trails)) = (&grown[0], &grown[1]);
    assert!(deep_reach.candidates_generated <= 2 * shallow_reach.candidates_generated + 4);
    assert!(deep_trails.candidates_generated >= 32 * shallow_trails.candidates_generated);
}

/// A small graph for plan tests, with the properties the benchmark shapes read.
const PLAN_GRAPH: &str = "CREATE (a1:Account {id: 1}), (a2:Account {id: 2}), \
     (a1)-[:transfer]->(a2), \
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
        let text = session.explain_physical(plan).await.unwrap();
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
    let queries = [
        // Shortest transfer chain between two accounts (TCR3, without the
        // per-hop timestamp window, which waits for edge-property pushdown).
        "MATCH (src:Account {id: 1})-[r:transfer*1..]->(dst:Account {id: 2}) \
         RETURN coalesce(min(size(r)), -1) AS shortestPathLength",
        // Friends by distance (IC1).
        "MATCH path = (p:Person {id: 1})-[:KNOWS*1..3]-(friend:Person {firstName: 'Bo'}) \
         WHERE NOT p = friend \
         WITH friend, min(length(path)) AS distance \
         ORDER BY distance ASC, friend.lastName ASC LIMIT 20 \
         RETURN friend.id AS friendId, distance",
        // Distinct people within reach (IC3, IC5, IC9, IC11).
        "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(friend:Person) \
         WHERE NOT p = friend \
         RETURN DISTINCT friend.id AS friendId",
        // Distinct accounts reached (TCR8, TCR11).
        "MATCH (src:Account {id: 1})-[:transfer*1..3]->(other:Account) \
         RETURN count(DISTINCT other) AS reached",
    ];
    for (query, plan) in queries.iter().zip(plans(&queries).await) {
        assert_eq!(
            plan.matches("mode=reachability").count(),
            1,
            "{query}: expected one reachability expansion:\n{plan}"
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
        "MATCH (p:Person {id: 1})-[r:KNOWS*1..3]-(friend:Person) \
         WHERE ALL(e IN r WHERE e.since > 0) RETURN DISTINCT friend.id AS friendId",
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
