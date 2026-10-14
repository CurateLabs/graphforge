//! Two inline-map node patterns keyed off one unwound map plan and execute
//! (#2084).
//!
//! `UNWIND [{a: 1, b: 2}] AS e MATCH (a:N {id: e.a}), (b:N {id: e.b})` joins two
//! node scans to the unwound row. Each shape below ran into a planning error in
//! the `optimize_projections` rule before the fix, so every shape is checked
//! on an in-memory and a durable project at one and four target partitions,
//! reading both the rows and `count(*)`.
//!
//! The row-returning forms failed on every run; the `count(*)` and `CREATE` forms
//! failed on only some runs, so those execute [`REPEATS`] times per project. The
//! optimized plans are checked directly, without repetition, in the
//! `graphforge-rel` `struct_field_join_keys` test.
//!
//! Patterns whose second condition is not an equality on the map field, and a map
//! built by `WITH` rather than `UNWIND`, share the join key that triggered the
//! failure, so they run the same checks.

use arrow::array::{Array, RecordBatch};
use arrow::util::display::array_value_to_string;
use graphforge_api::{ExecutionResourcePolicy, GraphForge, GraphForgeOptions};
use tempfile::TempDir;

/// The unwound pairs: `{1, 2}` and `{3, 4}` resolve, `{2, 9}` has no node `9`
/// and `{7, 1}` has no node `7`, so neither produces a row.
const UNWOUND: &str = "UNWIND [{a: 1, b: 2}, {a: 3, b: 4}, {a: 2, b: 9}, {a: 7, b: 1}] AS e";

struct Fixture {
    forge: GraphForge,
    label: String,
    // Keeps a durable project's directory alive for the fixture's lifetime.
    _dir: Option<TempDir>,
}

fn options(target_partitions: usize) -> GraphForgeOptions {
    GraphForgeOptions {
        resource: ExecutionResourcePolicy {
            target_partitions: Some(target_partitions),
            ..ExecutionResourcePolicy::default()
        },
        ..GraphForgeOptions::default()
    }
}

fn fixtures() -> Vec<Fixture> {
    let mut fixtures = Vec::new();
    for partitions in [1, 4] {
        for durable in [false, true] {
            let dir = durable.then(|| tempfile::tempdir().expect("project directory"));
            let path = dir
                .as_ref()
                .map(|dir| dir.path().to_str().expect("utf-8 path").to_owned());
            let forge = GraphForge::new_with_options(path.as_deref(), options(partitions))
                .expect("project opens");
            forge
                .execute(
                    "CREATE (:N {id: 1, name: 'one'}), (:N {id: 2, name: 'two'}), \
                     (:N {id: 3, name: 'three'}), (:N {id: 4, name: 'four'}), \
                     (:M {id: 9, name: 'wrong label'})",
                )
                .expect("nodes are created");
            fixtures.push(Fixture {
                forge,
                label: format!(
                    "{} project, {partitions} partitions",
                    if durable { "durable" } else { "in-memory" }
                ),
                _dir: dir,
            });
        }
    }
    fixtures
}

fn render(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(row) {
                            "null".to_owned()
                        } else {
                            array_value_to_string(column, row).expect("renders")
                        }
                    })
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

fn read(fixture: &Fixture, query: &str) -> Vec<Vec<String>> {
    let result = fixture
        .forge
        .execute(query)
        .unwrap_or_else(|error| panic!("{}: {query}: {error}", fixture.label));
    render(&result.batches)
}

fn expect_rows(fixture: &Fixture, query: &str, expected: &[&[&str]]) {
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|value| (*value).to_owned()).collect())
        .collect();
    assert_eq!(read(fixture, query), expected, "{}: {query}", fixture.label);
}

const PAIRS: &[&[&str]] = &[&["one", "two"], &["three", "four"]];

/// Executions of a form that failed on only some runs: four to seven of every
/// eight to twelve. Twenty passes in a row are not a plausible outcome for a
/// plan that still fails.
const REPEATS: usize = 20;

fn expect_count_every_time(fixture: &Fixture, query: &str, expected: &str) {
    for _ in 0..REPEATS {
        expect_rows(fixture, query, &[&[expected]]);
    }
}

#[test]
fn two_inline_map_patterns_in_one_match_return_their_rows() {
    let query =
        format!("{UNWOUND} MATCH (a:N {{id: e.a}}), (b:N {{id: e.b}}) RETURN a.name, b.name");
    for fixture in fixtures() {
        expect_rows(&fixture, &query, PAIRS);
    }
}

#[test]
fn two_inline_map_patterns_in_one_match_count_their_rows() {
    let query =
        format!("{UNWOUND} MATCH (a:N {{id: e.a}}), (b:N {{id: e.b}}) RETURN count(*) AS rows");
    for fixture in fixtures() {
        expect_count_every_time(&fixture, &query, "2");
    }
}

#[test]
fn two_inline_map_patterns_in_separate_matches_return_their_rows() {
    let query =
        format!("{UNWOUND} MATCH (a:N {{id: e.a}}) MATCH (b:N {{id: e.b}}) RETURN a.name, b.name");
    for fixture in fixtures() {
        expect_rows(&fixture, &query, PAIRS);
    }
}

#[test]
fn two_inline_map_patterns_in_separate_matches_count_their_rows() {
    let query = format!(
        "{UNWOUND} MATCH (a:N {{id: e.a}}) MATCH (b:N {{id: e.b}}) RETURN count(*) AS rows"
    );
    for fixture in fixtures() {
        expect_count_every_time(&fixture, &query, "2");
    }
}

#[test]
fn two_patterns_matched_by_where_equalities_return_their_rows() {
    let query = format!(
        "{UNWOUND} MATCH (a:N), (b:N) WHERE a.id = e.a AND b.id = e.b RETURN a.name, b.name"
    );
    for fixture in fixtures() {
        expect_rows(&fixture, &query, PAIRS);
    }
}

#[test]
fn two_patterns_matched_by_where_equalities_count_their_rows() {
    let query = format!(
        "{UNWOUND} MATCH (a:N), (b:N) WHERE a.id = e.a AND b.id = e.b RETURN count(*) AS rows"
    );
    for fixture in fixtures() {
        expect_count_every_time(&fixture, &query, "2");
    }
}

/// Pairs for `b.id > e.b` over `{1, 2}`, `{3, 4}`, `{2, 9}`, `{7, 1}`: only `{1, 2}`
/// resolves `a`, with `b` above `2`, and `{3, 4}` has nothing above `4`.
const GREATER_PAIRS: &[&[&str]] = &[&["one", "four"], &["one", "three"]];

/// Pairs for `b.id = e.b OR b.id = e.a`: `a` is `e.a`, so `b` is `a` itself or the
/// node at `e.b`; `{2, 9}` has no `N` node `9` and `{7, 1}` has no `a`.
const EITHER_PAIRS: &[&[&str]] = &[
    &["one", "one"],
    &["one", "two"],
    &["three", "four"],
    &["three", "three"],
    &["two", "two"],
];

#[test]
fn a_second_pattern_ordered_by_the_map_returns_its_rows() {
    let query =
        format!("{UNWOUND} MATCH (a:N {{id: e.a}}), (b:N) WHERE b.id > e.b RETURN a.name, b.name");
    for fixture in fixtures() {
        expect_rows(&fixture, &query, GREATER_PAIRS);
    }
}

#[test]
fn a_second_pattern_ordered_by_the_map_counts_its_rows() {
    let query = format!(
        "{UNWOUND} MATCH (a:N {{id: e.a}}), (b:N) WHERE b.id > e.b RETURN count(*) AS rows"
    );
    for fixture in fixtures() {
        expect_count_every_time(&fixture, &query, "2");
    }
}

#[test]
fn a_second_pattern_equal_to_either_map_field_returns_its_rows() {
    let query = format!(
        "{UNWOUND} MATCH (a:N {{id: e.a}}), (b:N) WHERE b.id = e.b OR b.id = e.a \
         RETURN a.name, b.name"
    );
    for fixture in fixtures() {
        expect_rows(&fixture, &query, EITHER_PAIRS);
    }
}

#[test]
fn a_second_pattern_equal_to_either_map_field_counts_its_rows() {
    let query = format!(
        "{UNWOUND} MATCH (a:N {{id: e.a}}), (b:N) WHERE b.id = e.b OR b.id = e.a \
         RETURN count(*) AS rows"
    );
    for fixture in fixtures() {
        expect_count_every_time(&fixture, &query, "5");
    }
}

#[test]
fn a_map_built_by_with_feeds_two_inline_map_patterns() {
    let rows =
        "WITH {a: 1, b: 2} AS e MATCH (a:N {id: e.a}), (b:N {id: e.b}) RETURN a.name, b.name";
    let count =
        "WITH {a: 1, b: 2} AS e MATCH (a:N {id: e.a}), (b:N {id: e.b}) RETURN count(*) AS rows";
    for fixture in fixtures() {
        expect_rows(&fixture, rows, &[&["one", "two"]]);
        expect_count_every_time(&fixture, count, "1");
    }
}

#[test]
fn two_inline_map_patterns_create_a_relationship_between_the_matches() {
    let writes = [
        (
            "E",
            format!("{UNWOUND} MATCH (a:N {{id: e.a}}), (b:N {{id: e.b}}) CREATE (a)-[:E]->(b)"),
        ),
        (
            "F",
            format!(
                "{UNWOUND} MATCH (a:N {{id: e.a}}) MATCH (b:N {{id: e.b}}) CREATE (a)-[:F]->(b)"
            ),
        ),
        (
            "G",
            format!(
                "{UNWOUND} MATCH (a:N), (b:N) WHERE a.id = e.a AND b.id = e.b CREATE (a)-[:G]->(b)"
            ),
        ),
    ];
    for fixture in fixtures() {
        for (kind, write) in &writes {
            for run in 1..=REPEATS {
                fixture.forge.execute(write).unwrap_or_else(|error| {
                    panic!("{} (run {run}): {write}: {error}", fixture.label)
                });
                // Each execution adds exactly the two resolved pairs.
                let created = (2 * run).to_string();
                expect_rows(
                    &fixture,
                    &format!("MATCH (:N)-[r:{kind}]->(:N) RETURN count(r) AS rows"),
                    &[&[created.as_str()]],
                );
            }
            let pairs = read(
                &fixture,
                &format!("MATCH (a:N)-[r:{kind}]->(b:N) RETURN DISTINCT a.name, b.name"),
            );
            let mut expected: Vec<Vec<String>> = PAIRS
                .iter()
                .map(|row| row.iter().map(|value| (*value).to_owned()).collect())
                .collect();
            expected.sort();
            assert_eq!(pairs, expected, "{}: {kind} pairs", fixture.label);
        }
    }
}
