//! A pattern comprehension whose pattern starts or ends at variables the outer
//! rows already bind expands from those nodes instead of scanning the
//! relationship type (#1984, the LDBC SNB Interactive IC7 shape
//! `size([(liker)-[:KNOWS]-(person) | 1]) = 0`).
//!
//! The comprehension yields one list per outer row. Seeding it from the outer
//! rows must keep that contract exactly: an empty list for a row without a
//! match, one value per outer row however often the row repeats, relationship
//! uniqueness inside the pattern, both directions of an undirected relationship,
//! and the empty list for an outer variable that is NULL. Each result test
//! asserts those rows first and the seeded plan second, so a lowering that
//! returned other rows fails on the rows and one that stopped seeding fails on
//! the plan.
//!
//! The io gate is deterministic and counts the edge reads of the expansion
//! operators and of the storage layer, never time: the same comprehension over
//! the same bound nodes reads the same relationships whether the graph holds a
//! few KNOWS relationships or ten times as many.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, RecordBatch, StringArray,
    cast::AsArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions,
};
use graphforge_core::uuid::Uuid;

// ---------------------------------------------------------------------------
// Result semantics, on an in-memory graph.
// ---------------------------------------------------------------------------

fn open(project: Option<&Path>, target_partitions: usize) -> GraphForge {
    GraphForge::new_with_options(
        project.map(|path| path.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(target_partitions),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

fn graph(partitions: usize, statements: &[&str]) -> GraphForge {
    let graph = open(None, partitions);
    for statement in statements {
        graph
            .execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    graph
}

/// One rendered cell; a list renders its elements in sorted order, because a
/// comprehension does not promise an order for the matches of one row.
fn cell(column: &ArrayRef, row: usize) -> String {
    if column
        .logical_nulls()
        .is_some_and(|nulls| nulls.is_null(row))
    {
        return "null".to_owned();
    }
    if matches!(column.data_type(), DataType::List(_)) {
        let values = column.as_list::<i32>().value(row);
        let mut items: Vec<String> = (0..values.len())
            .map(|index| array_value_to_string(&values, index).expect("render"))
            .collect();
        items.sort();
        return format!("[{}]", items.join(", "));
    }
    array_value_to_string(column, row).expect("render")
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
                    .map(|column| cell(column, row))
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

/// At least `seeds` comprehensions are seeded from the outer rows: the plan
/// binds the rows into a correlated seed rather than lowering the pattern
/// against a scan of the whole relationship type.
fn assert_seeded(graph: &GraphForge, query: &str, seeds: usize) {
    let plan = graph
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let found = plan.matches("CorrelatedSeedExec").count();
    assert!(
        found >= seeds,
        "{seeds} comprehensions must be seeded from the outer rows, found {found}:\n{plan}"
    );
}

/// Runs `query` at one and at four partitions on the graph `statements` build,
/// asserts the rows, then asserts the plan holds at least `seeds` correlated
/// seeds.
fn assert_rows_and_seeded(statements: &[&str], query: &str, seeds: usize, expected: &[&[&str]]) {
    let expected = table(expected);
    for partitions in [1, 4] {
        let graph = graph(partitions, statements);
        assert_eq!(
            rows(&graph, query),
            expected,
            "{partitions} partitions: {query}"
        );
        assert_seeded(&graph, query, seeds);
    }
}

/// Persons 0 to 6 and their KNOWS relationships. Person 0 is the start person.
///
/// ```text
/// 0 -> 1            liker 1 knows 0 once, from 0
/// 2 -> 0            liker 2 knows 0 once, to 0
/// 0 -> 3, 3 -> 0    liker 3 knows 0 twice, in both directions
/// (4)               liker 4 knows nobody
/// 5 -> 6            liker 5 knows someone, but not 0
/// ```
const PEOPLE: &[&str] = &[
    "CREATE (:Person {id: 0}), (:Person {id: 1}), (:Person {id: 2}), (:Person {id: 3}), \
            (:Person {id: 4}), (:Person {id: 5}), (:Person {id: 6})",
    "MATCH (a:Person {id: 0}), (b:Person {id: 1}) CREATE (a)-[:KNOWS]->(b)",
    "MATCH (a:Person {id: 2}), (b:Person {id: 0}) CREATE (a)-[:KNOWS]->(b)",
    "MATCH (a:Person {id: 0}), (b:Person {id: 3}) CREATE (a)-[:KNOWS]->(b)",
    "MATCH (a:Person {id: 3}), (b:Person {id: 0}) CREATE (a)-[:KNOWS]->(b)",
    "MATCH (a:Person {id: 5}), (b:Person {id: 6}) CREATE (a)-[:KNOWS]->(b)",
];

const LIKERS: &str = "MATCH (person:Person {id: 0})
     MATCH (liker:Person) WHERE liker.id IN [1, 2, 3, 4, 5]";

#[test]
fn a_comprehension_over_a_bound_pair_yields_one_list_per_outer_row() {
    // An undirected relationship matches both directions, so liker 3 matches
    // twice; likers 4 and 5 match nothing and still get their row with `[]`.
    assert_rows_and_seeded(
        PEOPLE,
        &format!(
            "{LIKERS}
             RETURN liker.id AS liker, [(liker)-[:KNOWS]-(person) | person.id] AS known
             ORDER BY liker"
        ),
        1,
        &[
            &["1", "[0]"],
            &["2", "[0]"],
            &["3", "[0, 0]"],
            &["4", "[]"],
            &["5", "[]"],
        ],
    );
}

#[test]
fn size_zero_over_a_bound_pair_marks_the_rows_without_a_match() {
    // The IC7 projection.
    assert_rows_and_seeded(
        PEOPLE,
        &format!(
            "{LIKERS}
             RETURN liker.id AS liker, size([(liker)-[:KNOWS]-(person) | 1]) = 0 AS isNew,
                    size([(liker)-[:KNOWS]-(person) | 1]) AS matches
             ORDER BY liker"
        ),
        2,
        &[
            &["1", "false", "1"],
            &["2", "false", "1"],
            &["3", "false", "2"],
            &["4", "true", "0"],
            &["5", "true", "0"],
        ],
    );
}

#[test]
fn a_directed_comprehension_matches_only_its_direction() {
    assert_rows_and_seeded(
        PEOPLE,
        &format!(
            "{LIKERS}
             RETURN liker.id AS liker,
                    size([(liker)-[:KNOWS]->(person) | 1]) AS out,
                    size([(liker)<-[:KNOWS]-(person) | 1]) AS inward
             ORDER BY liker"
        ),
        2,
        &[
            &["1", "0", "1"],
            &["2", "1", "0"],
            &["3", "1", "1"],
            &["4", "0", "0"],
            &["5", "0", "0"],
        ],
    );
}

#[test]
fn a_comprehension_starting_at_the_second_bound_node_is_seeded_too() {
    // The pattern is written from `person`, whose single row repeats five
    // times: the seed holds each distinct pair once and every outer row still
    // gets its own list.
    assert_rows_and_seeded(
        PEOPLE,
        &format!(
            "{LIKERS}
             RETURN liker.id AS liker, size([(person)-[:KNOWS]-(liker) | 1]) AS matches
             ORDER BY liker"
        ),
        1,
        &[
            &["1", "1"],
            &["2", "1"],
            &["3", "2"],
            &["4", "0"],
            &["5", "0"],
        ],
    );
}

#[test]
fn a_repeated_outer_row_gets_its_own_list() {
    // The same liker three times, and twice again after another UNWIND: the
    // multiplicity of the outer rows is the multiplicity of the output.
    assert_rows_and_seeded(
        PEOPLE,
        "MATCH (person:Person {id: 0})
         UNWIND [3, 3, 4, 3] AS wanted
         MATCH (liker:Person {id: wanted})
         RETURN wanted, size([(liker)-[:KNOWS]-(person) | 1]) AS matches
         ORDER BY wanted",
        1,
        &[&["3", "2"], &["3", "2"], &["3", "2"], &["4", "0"]],
    );
}

#[test]
fn a_comprehension_cannot_use_a_relationship_twice() {
    // `(liker)-[a]-(m)-[b]-(liker)` closes on the bound `liker`. Liker 1 has
    // one KNOWS relationship, which the two hops would have to share; liker 3
    // has two parallel ones, which pair up in either order.
    assert_rows_and_seeded(
        PEOPLE,
        "MATCH (liker:Person) WHERE liker.id IN [1, 3, 4]
         RETURN liker.id AS liker, size([(liker)-[:KNOWS]-(m)-[:KNOWS]-(liker) | m.id]) AS closed
         ORDER BY liker",
        1,
        &[&["1", "0"], &["3", "2"], &["4", "0"]],
    );
}

#[test]
fn a_null_outer_variable_yields_the_empty_list() {
    // `ghost` is NULL on the only row: nothing matches, and the row is kept.
    assert_rows_and_seeded(
        PEOPLE,
        "MATCH (person:Person {id: 0})
         OPTIONAL MATCH (ghost:Person {id: 99})
         RETURN person.id AS person, ghost.id AS ghost,
                [(ghost)-[:KNOWS]-(person) | 1] AS known,
                size([(ghost)-[:KNOWS]-(person) | 1]) = 0 AS isNew",
        2,
        &[&["0", "null", "[]", "true"]],
    );
}

#[test]
fn a_comprehension_nested_in_a_comprehension_keeps_its_rows_and_seeds_the_inner_one() {
    // For each liker, the people the liker knows, each with the count of its
    // KNOWS relationships back to that same liker. The inner pattern closes on
    // `liker`, bound two scopes out, and on `friend`, bound by the outer
    // comprehension. The outer comprehension's value is itself a comprehension,
    // so it keeps its whole-graph lowering; the inner one is seeded from the
    // rows the outer produces.
    assert_rows_and_seeded(
        PEOPLE,
        &format!(
            "{LIKERS}
             RETURN liker.id AS liker,
                    [(liker)-[:KNOWS]-(friend) | size([(friend)-[:KNOWS]-(liker) | 1])] AS nested
             ORDER BY liker"
        ),
        1,
        &[
            &["1", "[1]"],
            &["2", "[1]"],
            &["3", "[2, 2]"],
            &["4", "[]"],
            &["5", "[1]"],
        ],
    );
}

#[test]
fn a_comprehension_per_list_element_is_seeded_from_the_unwound_elements() {
    // `n` is bound by the list comprehension, not by a pattern: one count per
    // list element.
    assert_rows_and_seeded(
        PEOPLE,
        "MATCH (person:Person {id: 0})
         MATCH (liker:Person) WHERE liker.id IN [1, 2, 3, 4, 5]
         WITH person, collect(liker) AS likers
         RETURN [n IN likers | size([(n)-[:KNOWS]-(person) | 1])] AS knows",
        1,
        &[&["[0, 0, 1, 1, 2]"]],
    );
}

// ---------------------------------------------------------------------------
// The edge reads, on a durable project.
// ---------------------------------------------------------------------------

/// The people of the fixture: persons `0..=7` carry their own index as `ident`;
/// the noise persons that follow carry `100 + n`.
const NOISE_IDENT: i64 = 100;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// The graph as plain data: nodes as `(label, ident)` and relationships as
/// `(type, source node index, target node index)`.
struct Fixture {
    nodes: Vec<(&'static str, i64)>,
    edges: Vec<(&'static str, usize, usize)>,
}

/// The start person `0`, the likers `1..=5` with the KNOWS relationships of the
/// semantic fixture, and `noise` further persons joined in a KNOWS ring that
/// touches none of them. Only the ring grows with `noise`.
fn fixture(noise: usize) -> Fixture {
    let mut nodes: Vec<(&'static str, i64)> = (0..8).map(|ident| ("Person", ident)).collect();
    let mut edges = vec![
        ("KNOWS", 0, 1),
        ("KNOWS", 2, 0),
        ("KNOWS", 0, 3),
        ("KNOWS", 3, 0),
        ("KNOWS", 5, 6),
        ("KNOWS", 6, 7),
    ];
    let first = nodes.len();
    for n in 0..noise {
        nodes.push(("Person", NOISE_IDENT + n as i64));
        edges.push(("KNOWS", first + n, first + (n + 1) % noise));
    }
    Fixture { nodes, edges }
}

fn construct(dir: &Path, fixture: &Fixture) {
    let mut node_fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    node_fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(node_fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("weight", DataType::Int64, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();

    let mut ids = FixedSizeBinaryBuilder::with_capacity(fixture.nodes.len(), 16);
    let mut idents = Int64Builder::with_capacity(fixture.nodes.len());
    for (index, &(_, ident)) in fixture.nodes.iter().enumerate() {
        ids.append_value(node_uuid(index).as_bytes()).unwrap();
        idents.append_value(ident);
    }
    let labels: Vec<&str> = fixture.nodes.iter().map(|&(label, _)| label).collect();
    let batch = RecordBatch::try_new(
        node_schema,
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(labels)),
            Arc::new(idents.finish()),
        ],
    )
    .unwrap();
    session.append_nodes("nodes", &batch).unwrap();

    let rows = fixture.edges.len();
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut weights = Int64Builder::with_capacity(rows);
    for (index, &(_, source, target)) in fixture.edges.iter().enumerate() {
        ids.append_value(edge_uuid(index).as_bytes()).unwrap();
        sources.append_value(node_uuid(source).as_bytes()).unwrap();
        targets.append_value(node_uuid(target).as_bytes()).unwrap();
        weights.append_value(index as i64);
    }
    let types: Vec<&str> = fixture.edges.iter().map(|&(rel, _, _)| rel).collect();
    let batch = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(types)),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
            Arc::new(weights.finish()),
        ],
    )
    .unwrap();
    session.append_edges("edges", &batch).unwrap();
    session.seal_and_publish().unwrap();
}

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug)]
struct Measured {
    batches: Vec<RecordBatch>,
    /// What the expansion operators themselves report, summed over the hops:
    /// the rows entering them, the candidates generated from those rows, the
    /// adjacency entries examined, and the rows they emit.
    hop_work: [u64; 4],
}

impl Measured {
    fn int_column(&self, name: &str) -> Vec<Option<i64>> {
        let mut values = Vec::new();
        for batch in &self.batches {
            let column = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name}"));
            let column = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("{name} is Int64"));
            values.extend(
                (0..column.len()).map(|row| column.is_valid(row).then(|| column.value(row))),
            );
        }
        values
    }
}

/// Runs the statement and reports the work of its expansion operators.
///
/// The counters are the operators' own process-wide ones, exact at every
/// partition count, so the caller holds `serial()` while measuring.
fn measured(forge: &GraphForge, query: &str) -> Measured {
    let (result, demand) = graphforge_exec::demand::capture(|| forge.execute(query));
    let result = result.expect("statement executes");
    let hop_work = demand.hops.values().fold([0_u64; 4], |sum, hop| {
        [
            sum[0] + hop.input_rows,
            sum[1] + hop.candidates_generated,
            sum[2] + hop.adjacency_rows_examined,
            sum[3] + hop.rows_emitted,
        ]
    });
    Measured {
        batches: result.batches,
        hop_work,
    }
}

/// The statement after one warm-up run, which admits the footers, the
/// adjacency index and the node topology once per session.
fn warm_measured(forge: &GraphForge, query: &str) -> Measured {
    let _ = measured(forge, query);
    measured(forge, query)
}

/// IC7's last stage over a bound start person and five likers: how many
/// KNOWS relationships join each liker to the start person. The pattern is
/// written from either end.
fn knows_query(pattern: &str) -> String {
    format!(
        "MATCH (person:Person {{ident: 0}})
         MATCH (liker:Person) WHERE liker.ident IN [1, 2, 3, 4, 5]
         RETURN liker.ident AS liker, size([{pattern} | 1]) AS knows
         ORDER BY liker"
    )
}

impl Fixture {
    /// The relationships that touch the node, counted once per end.
    fn degree(&self, index: usize) -> u64 {
        self.edges
            .iter()
            .map(|&(_, source, target)| u64::from(source == index) + u64::from(target == index))
            .sum()
    }
}

#[test]
fn a_comprehension_over_a_bound_pair_reads_the_degree_of_the_bound_nodes_not_the_relationship_type()
{
    let _serial = serial();
    let small = fixture(200);
    let large = fixture(2000);
    // The start person and the five likers are the bound nodes; the ring
    // among the noise persons touches none of them. Each of the five outer rows
    // may expand both of its bound nodes, so a row costs at most the degree of
    // the start person plus that of its liker.
    let bound_degree = |fixture: &Fixture| -> u64 {
        (1..=5)
            .map(|liker| fixture.degree(0) + fixture.degree(liker))
            .sum()
    };
    let bound_degree = bound_degree(&small);
    assert_eq!(bound_degree, 5 * 4 + 5);
    assert!(
        large.edges.len() as u64 > 50 * bound_degree,
        "the relationship type dwarfs the bound nodes"
    );

    for pattern in ["(liker)-[:KNOWS]-(person)", "(person)-[:KNOWS]-(liker)"] {
        let query = knows_query(pattern);
        for partitions in [1, 4] {
            let mut work = Vec::new();
            for fixture in [&small, &large] {
                let root = tempfile::tempdir().expect("project directory");
                let project = root.path().join("project");
                construct(&project, fixture);
                let forge = open(Some(&project), partitions);
                let run = warm_measured(&forge, &query);
                assert_eq!(
                    run.int_column("liker"),
                    [Some(1), Some(2), Some(3), Some(4), Some(5)],
                    "{pattern} at {partitions} partitions"
                );
                assert_eq!(
                    run.int_column("knows"),
                    [Some(1), Some(1), Some(2), Some(0), Some(0)],
                    "{pattern} at {partitions} partitions"
                );
                work.push(run.hop_work);
            }
            assert_eq!(
                work[0], work[1],
                "{pattern} at {partitions} partitions: the same bound nodes in a graph ten \
                 times as large expand the same relationships"
            );
            for counter in work[1] {
                assert!(
                    counter <= bound_degree,
                    "{pattern} at {partitions} partitions: {:?} exceeds the degree {bound_degree} \
                     of the bound nodes",
                    work[1]
                );
            }
        }
    }
}
