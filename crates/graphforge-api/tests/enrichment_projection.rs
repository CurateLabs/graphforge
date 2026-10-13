//! A node enrichment projects only the properties the query reads for that
//! variable (#1984, the LDBC SNB Interactive IC5 shape).
//!
//! Plan-shape tests read the physical EXPLAIN of a durable project, where the
//! node property route is a `PropertyOverlayExec`. The physical plan names every
//! property column that survives into a join projection or an aggregate, so the
//! set of property names it mentions is the set the enrichments project.
//! Result tests run the same query matrix against the durable project and an
//! in-memory graph built from the same data, and compare with the rows the data
//! defines: pruning a column must never change a result, a NULL, or a count.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions,
};
use graphforge_core::uuid::Uuid;

/// Every property column in the fixture. `id` is shared by all three labels;
/// the others are each carried by one label only.
const PROPERTY_COLUMNS: [&str; 4] = ["id", "firstName", "title", "content"];

/// `(label, id, firstName, title, content)`.
type NodeRow = (
    &'static str,
    i64,
    Option<&'static str>,
    Option<&'static str>,
    Option<&'static str>,
);

const NODES: [NodeRow; 9] = [
    ("Person", 1, Some("ann"), None, None),
    ("Person", 2, Some("bob"), None, None),
    ("Person", 3, Some("cat"), None, None),
    ("Forum", 10, None, Some("alpha"), None),
    ("Forum", 11, None, Some("beta"), None),
    ("Forum", 12, None, Some("gamma"), None),
    ("Post", 100, None, None, Some("c1")),
    ("Post", 101, None, None, Some("c2")),
    ("Post", 102, None, None, Some("c3")),
];

/// `(type, source index, target index, joinDate)`. Forums alpha, beta and
/// gamma are nodes 3, 4 and 5; gamma has no members and no posts.
const EDGES: [(&str, usize, usize, Option<i64>); 11] = [
    ("KNOWS", 0, 1, None),
    ("KNOWS", 1, 2, None),
    ("HAS_MEMBER", 3, 1, Some(10)),
    ("HAS_MEMBER", 3, 2, Some(20)),
    ("HAS_MEMBER", 4, 1, Some(5)),
    ("CONTAINER_OF", 3, 6, None),
    ("CONTAINER_OF", 3, 7, None),
    ("CONTAINER_OF", 4, 8, None),
    ("HAS_CREATOR", 6, 1, None),
    ("HAS_CREATOR", 7, 2, None),
    ("HAS_CREATOR", 8, 1, None),
];

/// The same data as [`NODES`] and [`EDGES`], as one Cypher statement.
const MEMORY_SCRIPT: &str = "CREATE \
    (p1:Person {id: 1, firstName: 'ann'}), \
    (p2:Person {id: 2, firstName: 'bob'}), \
    (p3:Person {id: 3, firstName: 'cat'}), \
    (fa:Forum {id: 10, title: 'alpha'}), \
    (fb:Forum {id: 11, title: 'beta'}), \
    (fg:Forum {id: 12, title: 'gamma'}), \
    (q1:Post {id: 100, content: 'c1'}), \
    (q2:Post {id: 101, content: 'c2'}), \
    (q3:Post {id: 102, content: 'c3'}), \
    (p1)-[:KNOWS]->(p2), (p2)-[:KNOWS]->(p3), \
    (fa)-[:HAS_MEMBER {joinDate: 10}]->(p2), \
    (fa)-[:HAS_MEMBER {joinDate: 20}]->(p3), \
    (fb)-[:HAS_MEMBER {joinDate: 5}]->(p2), \
    (fa)-[:CONTAINER_OF]->(q1), (fa)-[:CONTAINER_OF]->(q2), (fb)-[:CONTAINER_OF]->(q3), \
    (q1)-[:HAS_CREATOR]->(p2), (q2)-[:HAS_CREATOR]->(p3), (q3)-[:HAS_CREATOR]->(p2)";

fn uuid(tag: u128, index: usize) -> Uuid {
    Uuid::from_u128((tag << 120) | (index as u128 + 1))
}

/// A published durable project holding the fixture, reopened read-only.
struct Durable {
    graph: GraphForge,
    // Keeps the project directory alive for the test's duration.
    _dir: tempfile::TempDir,
}

fn durable() -> Durable {
    let dir = tempfile::tempdir().expect("tempdir");
    let project = dir.path().join("project");
    let path = project.to_str().expect("utf-8 path");
    let forge = GraphForge::new(Some(path)).expect("create project");
    let mut construction = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .expect("begin construction");

    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    for (name, ty) in [
        ("id", DataType::Int64),
        ("firstName", DataType::Utf8),
        ("title", DataType::Utf8),
        ("content", DataType::Utf8),
    ] {
        fields.push(Arc::new(Field::new(name, ty, true)));
    }
    let mut node_ids = FixedSizeBinaryBuilder::new(16);
    for index in 0..NODES.len() {
        node_ids
            .append_value(uuid(1, index).as_bytes())
            .expect("node uuid");
    }
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(node_ids.finish()) as ArrayRef,
            Arc::new(StringArray::from_iter_values(NODES.iter().map(|n| n.0))),
            Arc::new(Int64Array::from_iter(NODES.iter().map(|n| Some(n.1)))),
            Arc::new(StringArray::from_iter(NODES.iter().map(|n| n.2))),
            Arc::new(StringArray::from_iter(NODES.iter().map(|n| n.3))),
            Arc::new(StringArray::from_iter(NODES.iter().map(|n| n.4))),
        ],
    )
    .expect("node batch");
    construction.append_nodes("nodes", &nodes).expect("nodes");

    let mut fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("joinDate", DataType::Int64, true)));
    let mut edge_ids = FixedSizeBinaryBuilder::new(16);
    let mut sources = FixedSizeBinaryBuilder::new(16);
    let mut targets = FixedSizeBinaryBuilder::new(16);
    for (index, edge) in EDGES.iter().enumerate() {
        edge_ids
            .append_value(uuid(2, index).as_bytes())
            .expect("edge uuid");
        sources
            .append_value(uuid(1, edge.1).as_bytes())
            .expect("source uuid");
        targets
            .append_value(uuid(1, edge.2).as_bytes())
            .expect("target uuid");
    }
    let edges = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(edge_ids.finish()) as ArrayRef,
            Arc::new(StringArray::from_iter_values(EDGES.iter().map(|e| e.0))),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
            Arc::new(Int64Array::from_iter(EDGES.iter().map(|e| e.3))),
        ],
    )
    .expect("edge batch");
    construction.append_edges("edges", &edges).expect("edges");
    construction.seal_and_publish().expect("publish");
    drop(forge);

    Durable {
        graph: GraphForge::new(Some(path)).expect("reopen project"),
        _dir: dir,
    }
}

fn in_memory() -> GraphForge {
    let graph = GraphForge::new(None).expect("in-memory instance");
    graph.execute(MEMORY_SCRIPT).expect("load fixture");
    graph
}

fn rows(graph: &GraphForge, query: &str) -> Vec<Vec<String>> {
    let result = graph
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "null".to_owned()
                        } else {
                            array_value_to_string(column, row).expect("render")
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|cell| (*cell).to_owned()).collect())
        .collect()
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

fn physical_plan(graph: &GraphForge, query: &str) -> String {
    graph
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
}

/// The fixture property columns the plan mentions anywhere.
fn property_columns_in(plan: &str) -> BTreeSet<&'static str> {
    let words: BTreeSet<&str> = plan
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|word| !word.is_empty())
        .collect();
    PROPERTY_COLUMNS
        .into_iter()
        .filter(|column| words.contains(column))
        .collect()
}

/// Every `var_<n>.<column>` the plan references for a fixture property column,
/// which is how an aggregate or a distinct carries a node variable's property.
fn qualified_references_in(plan: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for column in PROPERTY_COLUMNS {
        let mut rest = plan;
        while let Some(at) = rest.find(&format!(".{column}")) {
            let before = &rest[..at];
            let after = &rest[at + 1 + column.len()..];
            let var_start = before
                .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .map_or(0, |i| i + 1);
            let var = &before[var_start..];
            let ends_word = after
                .chars()
                .next()
                .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
            if var.starts_with("var_") && ends_word {
                found.insert(format!("{var}.{column}"));
            }
            rest = &rest[at + 1..];
        }
    }
    found
}

fn set(columns: &[&'static str]) -> BTreeSet<&'static str> {
    columns.iter().copied().collect()
}

/// LDBC SNB Interactive IC5 with the benchmark's names and its parameters
/// inlined: `personId` = 1 and `minDate` = 0.
const IC5: &str = "MATCH (person:Person {id: 1})-[:KNOWS*1..2]-(friend) \
    WHERE NOT person = friend \
    WITH DISTINCT friend \
    MATCH (friend)<-[membership:HAS_MEMBER]-(forum) \
    WHERE membership.joinDate > 0 \
    WITH forum, collect(friend) AS friends \
    OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) \
    WHERE friend IN friends \
    WITH forum, count(post) AS postCount \
    RETURN forum.title AS forumName, postCount \
    ORDER BY postCount DESC, forum.id ASC LIMIT 20";

#[test]
fn ic5_projects_the_forum_title_and_id_and_nothing_of_the_posts_or_friends() {
    let durable = durable();
    let plan = physical_plan(&durable.graph, IC5);
    // `title` and `id` are read for the forum, `id` also anchors the person.
    // `firstName` (person) and `content` (post) are read by nothing.
    assert_eq!(
        property_columns_in(&plan),
        set(&["id", "title"]),
        "IC5 projects properties nothing reads:\n{plan}"
    );
    // Only the forum variable carries properties through its aggregates.
    assert_eq!(
        qualified_references_in(&plan),
        ["var_5.id", "var_5.title"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "only the forum carries properties:\n{plan}"
    );
    assert_eq!(
        rows(&durable.graph, IC5),
        table(&[&["alpha", "2"], &["beta", "1"]])
    );
}

#[test]
fn a_distinct_node_that_is_only_counted_projects_no_property() {
    let durable = durable();
    let query = "MATCH (a)-[:KNOWS]->(b) WITH DISTINCT b \
        OPTIONAL MATCH (b)-[:KNOWS]->(c) RETURN count(c) AS c";
    let plan = physical_plan(&durable.graph, query);
    assert_eq!(
        property_columns_in(&plan),
        set(&[]),
        "identity-only nodes project no property:\n{plan}"
    );
    assert_eq!(rows(&durable.graph, query), table(&[&["1"]]));
}

#[test]
fn a_distinct_node_projects_only_the_property_read_after_it() {
    let durable = durable();
    let query = "MATCH (n:Person) WITH DISTINCT n \
        OPTIONAL MATCH (n)-[:KNOWS]->(m) \
        RETURN n.firstName AS x, count(m) AS c ORDER BY x";
    let plan = physical_plan(&durable.graph, query);
    assert_eq!(
        property_columns_in(&plan),
        set(&["firstName"]),
        "only firstName is read:\n{plan}"
    );
    assert_eq!(
        rows(&durable.graph, query),
        table(&[&["ann", "1"], &["bob", "1"], &["cat", "0"]])
    );
}

#[test]
fn an_unlabelled_optional_source_projects_only_the_property_read() {
    let durable = durable();
    let query = "MATCH (a:Person)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:KNOWS]->(c) \
        RETURN b.firstName AS n, count(c) AS k ORDER BY n";
    let plan = physical_plan(&durable.graph, query);
    assert_eq!(
        property_columns_in(&plan),
        set(&["firstName"]),
        "only b.firstName is read:\n{plan}"
    );
    assert_eq!(
        rows(&durable.graph, query),
        table(&[&["bob", "1"], &["cat", "0"]])
    );
}

#[test]
fn an_optional_match_node_that_is_only_counted_projects_no_property() {
    let durable = durable();
    let query = "MATCH (f:Forum) OPTIONAL MATCH (f)-[:CONTAINER_OF]->(p) \
        RETURN f.title AS t, count(p) AS c ORDER BY t";
    let plan = physical_plan(&durable.graph, query);
    assert_eq!(
        property_columns_in(&plan),
        set(&["title"]),
        "only the forum title is read:\n{plan}"
    );
    assert_eq!(
        rows(&durable.graph, query),
        table(&[&["alpha", "2"], &["beta", "1"], &["gamma", "0"]])
    );
}

#[test]
fn a_node_returned_whole_keeps_every_property() {
    let durable = durable();
    for (query, expected) in [
        (
            "MATCH (n) WITH DISTINCT n RETURN n",
            set(&["id", "firstName", "title", "content"]),
        ),
        (
            "MATCH (n:Person) WITH DISTINCT n RETURN n",
            set(&["id", "firstName"]),
        ),
        (
            "MATCH (n:Forum) WITH DISTINCT n RETURN keys(n) AS k",
            set(&["id", "title"]),
        ),
        (
            "MATCH (n:Post) WITH DISTINCT n RETURN properties(n) AS p",
            set(&["id", "content"]),
        ),
        (
            "MATCH (n) WITH DISTINCT n WITH n AS m RETURN m",
            set(&["id", "firstName", "title", "content"]),
        ),
    ] {
        let plan = physical_plan(&durable.graph, query);
        assert!(
            property_columns_in(&plan).is_superset(&expected),
            "{query} dropped a property of a whole node: {:?} vs {expected:?}\n{plan}",
            property_columns_in(&plan)
        );
    }
}

/// The result matrix. Every case is `(query, expected rows)`, with the rows in
/// the query's own order, or compared sorted when it has none.
fn matrix() -> Vec<(&'static str, Vec<Vec<String>>, bool)> {
    let ordered = |query, expected| (query, expected, true);
    let unordered = |query, expected| (query, expected, false);
    vec![
        // The IC5 shape, including a minDate that drops the beta forum.
        ordered(IC5, table(&[&["alpha", "2"], &["beta", "1"]])),
        ordered(IC5_RECENT_MEMBERS, table(&[&["alpha", "1"]])),
        // Unlabelled nodes read in WHERE only.
        unordered(
            "MATCH (n) WHERE n.firstName = 'ann' RETURN count(*) AS c",
            table(&[&["1"]]),
        ),
        // Labelled nodes read in WHERE only.
        unordered(
            "MATCH (n:Person) WHERE n.firstName STARTS WITH 'b' RETURN count(n) AS c",
            table(&[&["1"]]),
        ),
        // A property read only in ORDER BY: the second person by firstName is
        // bob, who has two KNOWS neighbours.
        unordered(
            "MATCH (p:Person) WITH p ORDER BY p.firstName SKIP 1 LIMIT 1 \
             MATCH (p)-[:KNOWS]-(q) RETURN count(q) AS c",
            table(&[&["2"]]),
        ),
        // The same for unlabelled nodes, where NULL firstNames sort last.
        unordered(
            "MATCH (p) WITH p ORDER BY p.firstName SKIP 1 LIMIT 1 \
             MATCH (p)-[:KNOWS]-(q) RETURN count(q) AS c",
            table(&[&["2"]]),
        ),
        // A property read only inside an aggregate.
        unordered("MATCH (n:Person) RETURN max(n.id) AS m", table(&[&["3"]])),
        unordered("MATCH (n) RETURN sum(n.id) AS s", table(&[&["342"]])),
        // A property read after WITH renames the variable.
        unordered(
            "MATCH (n:Person) WITH n AS m WHERE m.id > 1 RETURN count(m) AS c",
            table(&[&["2"]]),
        ),
        ordered(
            "MATCH (n) WITH n AS m WHERE m.title IS NOT NULL RETURN m.title AS t ORDER BY t",
            table(&[&["alpha"], &["beta"], &["gamma"]]),
        ),
        ordered(
            "MATCH (n:Forum) WITH DISTINCT n AS m RETURN m.title AS t ORDER BY t",
            table(&[&["alpha"], &["beta"], &["gamma"]]),
        ),
        // count(n) skips OPTIONAL MATCH null rows, count(*) does not.
        ordered(
            "MATCH (f:Forum) OPTIONAL MATCH (f)-[:CONTAINER_OF]->(p) \
             RETURN f.title AS t, count(p) AS posts, count(*) AS entries ORDER BY t",
            table(&[
                &["alpha", "2", "2"],
                &["beta", "1", "1"],
                &["gamma", "0", "1"],
            ]),
        ),
        // The same with an unlabelled optional node and a property of it.
        ordered(
            "MATCH (f:Forum) OPTIONAL MATCH (f)-[:CONTAINER_OF]->(p) \
             RETURN f.title AS t, p.content AS c ORDER BY t, c",
            table(&[
                &["alpha", "c1"],
                &["alpha", "c2"],
                &["beta", "c3"],
                &["gamma", "null"],
            ]),
        ),
        // Identity comparison and counting of unlabelled nodes.
        unordered(
            "MATCH (a)-[:KNOWS]->(b) WHERE a <> b RETURN count(*) AS c",
            table(&[&["2"]]),
        ),
        unordered(
            "MATCH (a)-[:KNOWS]->(b) WHERE a = b RETURN count(*) AS c",
            table(&[&["0"]]),
        ),
        unordered(
            "MATCH (a:Person)-[:KNOWS]-(b) WITH DISTINCT b RETURN count(b) AS c",
            table(&[&["3"]]),
        ),
        // collect() of a node that only has its size read, and of one whose
        // elements are read.
        ordered(
            "MATCH (f:Forum)-[:CONTAINER_OF]->(p) WITH f, collect(p) AS ps \
             RETURN f.title AS t, size(ps) AS n ORDER BY t",
            table(&[&["alpha", "2"], &["beta", "1"]]),
        ),
        ordered(
            "MATCH (f:Forum)-[:CONTAINER_OF]->(p) WITH f, collect(p) AS ps \
             RETURN f.title AS t, [m IN ps | m.content] AS cs ORDER BY t",
            table(&[&["alpha", "[c1, c2]"], &["beta", "[c3]"]]),
        ),
        // A node grouped on while another is read whole.
        ordered(
            "MATCH (f:Forum)-[:CONTAINER_OF]->(p) WITH f, count(p) AS c \
             RETURN f.title AS t, c ORDER BY t",
            table(&[&["alpha", "2"], &["beta", "1"]]),
        ),
        // UNION branches compute their demand separately, even when they
        // reuse a variable name.
        unordered(
            "MATCH (n:Person {id: 1}) RETURN n.firstName AS x \
             UNION ALL MATCH (n:Forum) WHERE n.id = 10 RETURN n.title AS x",
            table(&[&["ann"], &["alpha"]]),
        ),
        // Correlated EXISTS reading, or not reading, a property of the inner node.
        ordered(
            "MATCH (f:Forum) WHERE EXISTS { (f)-[:CONTAINER_OF]->(p:Post) \
             WHERE p.content = 'c3' } RETURN f.title AS t ORDER BY t",
            table(&[&["beta"]]),
        ),
        ordered(
            "MATCH (f:Forum) WHERE EXISTS { (f)-[:CONTAINER_OF]->(p) } \
             RETURN f.title AS t ORDER BY t",
            table(&[&["alpha"], &["beta"]]),
        ),
        ordered(
            "MATCH (f:Forum) WHERE NOT (f)-[:CONTAINER_OF]->() \
             RETURN f.title AS t ORDER BY t",
            table(&[&["gamma"]]),
        ),
        ordered(
            "MATCH (f:Forum) WHERE (f)-[:CONTAINER_OF]->() RETURN f.title AS t ORDER BY t",
            table(&[&["alpha"], &["beta"]]),
        ),
        // A whole node alongside a counted one.
        ordered(
            "MATCH (f:Forum)-[:CONTAINER_OF]->(p) WITH f, count(p) AS c \
             RETURN f.id AS id, c, labels(f) AS l ORDER BY id",
            table(&[&["10", "2", "[Forum]"], &["11", "1", "[Forum]"]]),
        ),
        ordered(
            "MATCH (n:Person) WHERE n.id = 2 \
             RETURN n.id AS id, n.firstName AS name, size(keys(n)) AS k",
            table(&[&["2", "bob", "2"]]),
        ),
    ]
}

const IC5_RECENT_MEMBERS: &str = "MATCH (person:Person {id: 1})-[:KNOWS*1..2]-(friend) \
    WHERE NOT person = friend \
    WITH DISTINCT friend \
    MATCH (friend)<-[membership:HAS_MEMBER]-(forum) \
    WHERE membership.joinDate > 15 \
    WITH forum, collect(friend) AS friends \
    OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) \
    WHERE friend IN friends \
    WITH forum, count(post) AS postCount \
    RETURN forum.title AS forumName, postCount \
    ORDER BY postCount DESC, forum.id ASC LIMIT 20";

fn check_matrix(graph: &GraphForge) {
    for (query, expected, ordered) in matrix() {
        let actual = rows(graph, query);
        let (actual, expected) = if ordered {
            (actual, expected)
        } else {
            (sorted(actual), sorted(expected))
        };
        assert_eq!(actual, expected, "{query}");
    }
}

#[test]
fn durable_results_are_unchanged_by_property_pruning() {
    check_matrix(&durable().graph);
}

/// A distinct `UNION` of two labelled branches fails to plan on an in-memory
/// graph, with or without property pruning (a physical/logical schema-metadata
/// mismatch on `graphforge.property_live_schema`), so only the durable graph
/// covers it.
#[test]
fn durable_distinct_union_branches_prune_independently() {
    let durable = durable();
    let graph = &durable.graph;
    let query = "MATCH (n:Person) RETURN n.firstName AS x \
        UNION MATCH (n:Forum) RETURN n.title AS x";
    assert_eq!(
        sorted(rows(graph, query)),
        sorted(table(&[
            &["ann"],
            &["bob"],
            &["cat"],
            &["alpha"],
            &["beta"],
            &["gamma"]
        ]))
    );
}

#[test]
fn in_memory_results_are_unchanged_by_property_pruning() {
    check_matrix(&in_memory());
}

#[test]
fn whole_node_values_still_carry_their_properties() {
    for graph in [durable().graph, in_memory()] {
        let person = rows(&graph, "MATCH (n:Person {id: 1}) RETURN n");
        assert_eq!(person.len(), 1);
        let rendered = &person[0][0];
        assert!(
            rendered.contains("firstName: ann") && rendered.contains("id: 1"),
            "{rendered}"
        );

        // A grouped, then returned whole, node.
        let forums = rows(
            &graph,
            "MATCH (f:Forum)-[:CONTAINER_OF]->(p) RETURN f, count(p) AS c ORDER BY c DESC",
        );
        assert_eq!(forums.len(), 2);
        assert!(forums[0][0].contains("title: alpha"), "{:?}", forums[0]);
        assert_eq!(forums[0][1], "2");
        assert!(forums[1][0].contains("title: beta"), "{:?}", forums[1]);

        // One node counted, the other returned whole.
        let posts = rows(
            &graph,
            "MATCH (f:Forum)-[:CONTAINER_OF]->(p) WHERE f.title = 'beta' RETURN p",
        );
        assert_eq!(posts.len(), 1);
        assert!(posts[0][0].contains("content: c3"), "{:?}", posts[0]);

        // properties() and a collected node list.
        assert_eq!(
            rows(
                &graph,
                "MATCH (n:Forum {id: 11}) RETURN size(keys(properties(n))) AS k"
            ),
            table(&[&["2"]])
        );
        let properties = rows(&graph, "MATCH (n:Forum {id: 11}) RETURN properties(n) AS p");
        assert!(properties[0][0].contains("beta"), "{properties:?}");
        let collected = rows(
            &graph,
            "MATCH (f:Forum)-[:CONTAINER_OF]->(p) WITH f, collect(p) AS ps \
             WHERE f.title = 'alpha' RETURN ps",
        );
        assert_eq!(collected.len(), 1);
        assert!(
            collected[0][0].contains("content: c1") && collected[0][0].contains("content: c2"),
            "{:?}",
            collected[0]
        );
    }
}

/// The `PropertyOverlayExec` nodes in a physical plan: one per node property
/// join, whether or not it projects a column.
fn overlays_in(plan: &str) -> usize {
    plan.matches("PropertyOverlayExec").count()
}

/// A node variable that nothing reads a property of is enriched by no join at
/// all, however many times a later clause matches it again. A join that
/// projects no column is not free: it still scans the route's whole key column
/// and, having no payload, nothing can narrow that scan to the frontier.
#[test]
fn a_node_matched_again_without_a_property_read_adds_no_enrichment() {
    let durable = durable();
    let graph = &durable.graph;
    let cases: [(&str, usize, Vec<Vec<String>>); 4] = [
        // Distinct, then an OPTIONAL MATCH anchored on it.
        (
            "MATCH (a)-[:KNOWS]->(b) WITH DISTINCT b \
             OPTIONAL MATCH (b)-[:KNOWS]->(c) RETURN count(c) AS c",
            0,
            table(&[&["1"]]),
        ),
        // The OPTIONAL MATCH anchor is matched a second time.
        (
            "MATCH (a:Person)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:KNOWS]->(c) \
             RETURN count(c) AS k",
            0,
            table(&[&["1"]]),
        ),
        // A variable forwarded through WITH and matched again by a plain MATCH.
        (
            "MATCH (a:Person)-[:KNOWS]->(b) WITH b MATCH (b)-[:KNOWS]->(c) \
             RETURN count(c) AS k",
            0,
            table(&[&["1"]]),
        ),
        // The one variable that is read is enriched once, not once per match.
        (
            "MATCH (a:Person)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:KNOWS]->(c) \
             RETURN b.firstName AS n, count(c) AS k ORDER BY n",
            1,
            table(&[&["bob", "1"], &["cat", "0"]]),
        ),
    ];
    for (query, overlays, expected) in cases {
        let plan = physical_plan(graph, query);
        assert_eq!(overlays_in(&plan), overlays, "{query}\n{plan}");
        assert_eq!(rows(graph, query), expected, "{query}");
    }
}

/// A published durable project of three people (`id`, `firstName`) chained by
/// `KNOWS`, whose properties are owned by an adopted ontology. Its node property
/// routes are semantic: they carry the composition fingerprint that
/// authenticates the rows they join.
fn semantic_durable() -> Durable {
    use graphforge_api::{
        AdoptOntologyRequest, CompositionChangeRequest, CompositionDataDisposition, OntologyMode,
        OperationId, WriteContext,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let project = dir.path().join("project");
    let path = project.to_str().expect("utf-8 path");
    let ontology = dir.path().join("people.yaml");
    std::fs::write(
        &ontology,
        "ontology_id: https://example.test/people\nversion: \"1\"\n\
         entity_types:\n  - name: Person\n    abstract: false\n\
         relation_types:\n  - name: KNOWS\n    src: Person\n    dst: Person\n\
         properties:\n\
         \x20 - owner: Person\n    name: id\n    type: int64\n    nullable: true\n\
         \x20 - owner: Person\n    name: firstName\n    type: utf8\n    nullable: true\n",
    )
    .expect("write ontology");
    let context = || WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    };

    let mut forge = GraphForge::new(Some(path)).expect("create project");
    forge
        .adopt_ontology(AdoptOntologyRequest {
            context: context(),
            path: ontology,
            mode: OntologyMode::Advisory,
        })
        .expect("adopt ontology");
    // Construction binds the published composition: publish the adopted
    // ontology's, then reopen so the facade loads it.
    drop(forge);
    let mut adopted = GraphForge::new(Some(path)).expect("reopen adopted project");
    let candidate = adopted
        .workspace_ontology_composition()
        .expect("composition")
        .expect("adopted composition");
    let request = CompositionChangeRequest {
        context: context(),
        expected_project_generation_uuid: graphforge_storage::resolve_project_generation(&project)
            .expect("generation")
            .generation_uuid(),
        expected_composition_fingerprint: Some(candidate.composition_fingerprint.clone()),
        candidate,
        data_disposition: CompositionDataDisposition::RequireConforming,
    };
    let preview = adopted
        .preview_ontology_composition_change(&request, None)
        .expect("preview");
    assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
    adopted
        .publish_ontology_composition_change(&request, &preview, None)
        .expect("publish composition");
    drop(adopted);

    let forge = GraphForge::new(Some(path)).expect("reopen published project");
    let mut construction = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .expect("begin construction");
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("id", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("firstName", DataType::Utf8, true)));
    let mut node_ids = FixedSizeBinaryBuilder::new(16);
    for index in 0..3 {
        node_ids
            .append_value(uuid(1, index).as_bytes())
            .expect("node uuid");
    }
    let nodes = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(node_ids.finish()) as ArrayRef,
            Arc::new(StringArray::from_iter_values(["Person"; 3])),
            Arc::new(Int64Array::from_iter_values([1, 2, 3])),
            Arc::new(StringArray::from_iter_values(["ann", "bob", "cat"])),
        ],
    )
    .expect("node batch");
    construction.append_nodes("nodes", &nodes).expect("nodes");
    let mut edge_ids = FixedSizeBinaryBuilder::new(16);
    let mut sources = FixedSizeBinaryBuilder::new(16);
    let mut targets = FixedSizeBinaryBuilder::new(16);
    for (index, (source, target)) in [(0, 1), (1, 2)].into_iter().enumerate() {
        edge_ids
            .append_value(uuid(2, index).as_bytes())
            .expect("edge uuid");
        sources
            .append_value(uuid(1, source).as_bytes())
            .expect("source uuid");
        targets
            .append_value(uuid(1, target).as_bytes())
            .expect("target uuid");
    }
    let edges = RecordBatch::try_new(
        Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
        vec![
            Arc::new(edge_ids.finish()) as ArrayRef,
            Arc::new(StringArray::from_iter_values(["KNOWS"; 2])),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
        ],
    )
    .expect("edge batch");
    construction.append_edges("edges", &edges).expect("edges");
    construction.seal_and_publish().expect("publish");
    drop(forge);

    Durable {
        graph: GraphForge::new(Some(path)).expect("reopen project"),
        _dir: dir,
    }
}

/// Only a node variable whose property is read joins the semantic route that
/// owns it. `a` and `c` are matched, counted and traversed, never read, so they
/// carry no enrichment; `b.firstName` is the one read, so `b` carries the one
/// join, and re-matching `b` in the OPTIONAL MATCH adds no second.
#[test]
fn an_authenticated_semantic_node_matched_again_is_not_joined_twice() {
    let durable = semantic_durable();
    let query = "MATCH (a:Person)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:KNOWS]->(c) \
                 RETURN b.firstName AS n, count(c) AS k ORDER BY n";

    assert_eq!(
        rows(&durable.graph, query),
        vec![
            vec!["bob".to_string(), "1".to_string()],
            vec!["cat".to_string(), "0".to_string()],
        ]
    );
    let plan = physical_plan(&durable.graph, query);
    assert_eq!(overlays_in(&plan), 1, "{plan}");
}

/// Statements that leave a project holding every shape of node-property row the
/// enrichment's key could meet: a node in two labels, a node whose properties
/// a newer mutation fragment replaced, a node whose property was removed, a
/// node deleted outright, and a node that never had a property row.
const NON_SEMANTIC_MUTATIONS: [&str; 7] = [
    "CREATE (:Person:Employee {id: 4, firstName: 'dan'})",
    "CREATE (:Person)",
    "MATCH (p:Person {id: 2}) SET p.firstName = 'bobby'",
    "MATCH (p:Person {id: 3}) REMOVE p.firstName",
    "MATCH (p:Post {id: 102}) DETACH DELETE p",
    "MATCH (a:Person {id: 4}), (b:Person {id: 1}) CREATE (a)-[:KNOWS]->(b)",
    // A second update of the same node: three fragments now name it.
    "MATCH (p:Person {id: 2}) SET p.firstName = 'bobbie'",
];

/// One shape of enrichment: a query that reads no property of `n` next to the
/// same query forced to read one, and the row and node counts the data defines.
struct Shape {
    pruned: &'static str,
    forced: &'static str,
    rows: &'static str,
    nodes: &'static str,
}

const NON_SEMANTIC_SHAPES: [Shape; 7] = [
    Shape {
        pruned: "MATCH (n:Person) RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (n:Person) RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, \
                 size(collect(n)) AS k",
        rows: "5",
        nodes: "5",
    },
    // The second label of a node whose properties are stored under its first.
    Shape {
        pruned: "MATCH (n:Employee) RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (n:Employee) RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, \
                 size(collect(n)) AS k",
        rows: "1",
        nodes: "1",
    },
    Shape {
        pruned: "MATCH (n) RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (n) RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, \
                 size(collect(n)) AS k",
        rows: "10",
        nodes: "10",
    },
    Shape {
        pruned: "MATCH (:Person)-[:KNOWS]->(n) RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (:Person)-[:KNOWS]->(n) RETURN count(*) AS r, count(n) AS c, \
                 count(n.id) AS p, size(collect(n)) AS k",
        rows: "3",
        nodes: "3",
    },
    // The deleted post is gone from every join the enrichment could add.
    Shape {
        pruned: "MATCH (n:Post) RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (n:Post) RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, \
                 size(collect(n)) AS k",
        rows: "2",
        nodes: "2",
    },
    // OPTIONAL: a null row stays one null row, enriched or not.
    Shape {
        pruned: "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(n) \
                 RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(n) \
                 RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, size(collect(n)) AS k",
        rows: "5",
        nodes: "3",
    },
    Shape {
        pruned: "MATCH (f:Forum) OPTIONAL MATCH (f)-[:CONTAINER_OF]->(n) \
                 RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (f:Forum) OPTIONAL MATCH (f)-[:CONTAINER_OF]->(n) \
                 RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, size(collect(n)) AS k",
        rows: "4",
        nodes: "2",
    },
];

/// Apply `statements` to a published project, then close it.
fn mutated(durable: Durable, statements: &[&str]) -> (tempfile::TempDir, String) {
    let Durable { graph, _dir: dir } = durable;
    for statement in statements {
        graph
            .execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    drop(graph);
    let path = dir
        .path()
        .join("project")
        .to_str()
        .expect("utf-8 path")
        .to_owned();
    (dir, path)
}

fn open_with_partitions(path: &str, partitions: usize) -> GraphForge {
    GraphForge::new_with_options(
        Some(path),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(partitions),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .expect("open project")
}

/// The first row of a query that returns one.
fn only_row(graph: &GraphForge, query: &str) -> Vec<String> {
    let mut found = rows(graph, query);
    assert_eq!(found.len(), 1, "{query}: {found:?}");
    found.remove(0)
}

/// Dropping an enrichment that projects no property is exact only if the join
/// it replaces neither drops nor repeats a row: the property route must hold at
/// most one row per `node_uuid`. This is the invariant, tested over every row
/// shape the route can hold, with the enrichment present and absent: counting a
/// node, counting rows and collecting nodes agree between the statement that
/// reads a property (so keeps the join), the statement that reads none (so has
/// none), the in-memory engine, and the counts the data defines.
fn check_enrichment_multiplicity(
    path: &str,
    memory: Option<&GraphForge>,
    shapes: &[Shape],
    partitions: &[usize],
) {
    for &partition_count in partitions {
        let graph = open_with_partitions(path, partition_count);
        for shape in shapes {
            let context = format!("partitions={partition_count}: {}", shape.pruned);
            let pruned = physical_plan(&graph, shape.pruned);
            assert_eq!(overlays_in(&pruned), 0, "{context}\n{pruned}");
            let forced = physical_plan(&graph, shape.forced);
            assert!(overlays_in(&forced) >= 1, "{context}\n{forced}");

            let without = only_row(&graph, shape.pruned);
            assert_eq!(without, [shape.rows, shape.nodes], "{context}");
            let with = only_row(&graph, shape.forced);
            assert_eq!(with[..2], without[..], "{context}: enriched counts");
            assert_eq!(with[3], shape.nodes, "{context}: collect(n)");
            if let Some(memory) = memory {
                assert_eq!(only_row(memory, shape.pruned), without, "{context}");
                assert_eq!(only_row(memory, shape.forced), with, "{context}");
            }
        }
    }
}

#[test]
fn dropping_a_property_free_enrichment_keeps_every_row_and_count() {
    let (_dir, path) = mutated(durable(), &NON_SEMANTIC_MUTATIONS);
    let memory = in_memory();
    for statement in NON_SEMANTIC_MUTATIONS {
        memory.execute(statement).expect("mirror mutation");
    }
    check_enrichment_multiplicity(&path, Some(&memory), &NON_SEMANTIC_SHAPES, &[1, 4]);
}

/// The statements of [`NON_SEMANTIC_MUTATIONS`] on the semantic project: a node
/// in a second label, a node rewritten twice, a node whose property was removed,
/// a node deleted with its edges, a node with no property row, and a new edge.
const SEMANTIC_MUTATIONS: [&str; 8] = [
    "MATCH (p:Person {id: 1}) SET p.firstName = 'annie'",
    "MATCH (p:Person {id: 2}) REMOVE p.firstName",
    "MATCH (p:Person {id: 3}) DETACH DELETE p",
    "CREATE (:Person {id: 4, firstName: 'dan'})",
    "CREATE (:Person:Employee {id: 5, firstName: 'eve'})",
    "CREATE (:Person)",
    "MATCH (a:Person {id: 4}), (b:Person {id: 1}) CREATE (a)-[:KNOWS]->(b)",
    "MATCH (p:Person {id: 1}) SET p.firstName = 'anne'",
];

/// The semantic project's starting data as one Cypher statement, for the
/// in-memory mirror.
const SEMANTIC_MEMORY_SCRIPT: &str = "CREATE \
    (p1:Person {id: 1, firstName: 'ann'}), \
    (p2:Person {id: 2, firstName: 'bob'}), \
    (p3:Person {id: 3, firstName: 'cat'}), \
    (p1)-[:KNOWS]->(p2), (p2)-[:KNOWS]->(p3)";

const SEMANTIC_SHAPES: [Shape; 5] = [
    Shape {
        pruned: "MATCH (n:Person) RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (n:Person) RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, \
                 size(collect(n)) AS k",
        rows: "5",
        nodes: "5",
    },
    Shape {
        pruned: "MATCH (n:Employee) RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (n:Employee) RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, \
                 size(collect(n)) AS k",
        rows: "1",
        nodes: "1",
    },
    Shape {
        pruned: "MATCH (n) RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (n) RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, \
                 size(collect(n)) AS k",
        rows: "5",
        nodes: "5",
    },
    // The edge that went with the deleted node is gone; the new one is there.
    Shape {
        pruned: "MATCH (:Person)-[:KNOWS]->(n) RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (:Person)-[:KNOWS]->(n) RETURN count(*) AS r, count(n) AS c, \
                 count(n.id) AS p, size(collect(n)) AS k",
        rows: "2",
        nodes: "2",
    },
    Shape {
        pruned: "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(n) \
                 RETURN count(*) AS r, count(n) AS c",
        forced: "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(n) \
                 RETURN count(*) AS r, count(n) AS c, count(n.id) AS p, size(collect(n)) AS k",
        rows: "5",
        nodes: "2",
    },
];

/// [`dropping_a_property_free_enrichment_keeps_every_row_and_count`] for a route
/// that carries the composition fingerprint: dropping the join that
/// authenticated it must not change a row or a count.
#[test]
fn dropping_a_property_free_semantic_enrichment_keeps_every_row_and_count() {
    let (_dir, path) = mutated(semantic_durable(), &SEMANTIC_MUTATIONS);
    let memory = GraphForge::new(None).expect("in-memory instance");
    memory
        .execute(SEMANTIC_MEMORY_SCRIPT)
        .expect("load semantic fixture");
    for statement in SEMANTIC_MUTATIONS {
        memory.execute(statement).expect("mirror mutation");
    }
    check_enrichment_multiplicity(&path, Some(&memory), &SEMANTIC_SHAPES, &[1, 4]);
}
