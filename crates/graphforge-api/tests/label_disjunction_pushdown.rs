//! A label disjunction in a filter must not block the pushable conjuncts
//! beside it (#1988).
//!
//! `(m:Post OR m:Comment) AND m.id = $id` is the shape the GDC harness gives
//! `:Message`. The disjunction used to make the whole filter non-total, so the
//! id comparison stayed above the expansion and the expansion started from
//! every node.

use std::collections::{BTreeSet, HashMap};

use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;
use graphforge_ir::IrLiteral;

const SHAPE: &str = "MATCH (m)-[:HAS_CREATOR]->(p:Person) \
                     WHERE (m:Post OR m:Comment) AND m.id = $id \
                     RETURN m.id AS id, p.name AS creator";

/// The same match with the id comparison left out, so that the test applies it
/// itself: the answer a plan that never moved the predicate would give.
const UNFILTERED: &str = "MATCH (m)-[:HAS_CREATOR]->(p:Person) \
                          WHERE (m:Post OR m:Comment) \
                          RETURN m.id AS id, p.name AS creator";

fn fixture() -> GraphForge {
    let forge = GraphForge::new(None).expect("in-memory forge");
    forge
        .execute(
            "CREATE (alice:Person {name: 'alice'}), (bob:Person {name: 'bob'}), \
             (:Post {id: 1})-[:HAS_CREATOR]->(alice), \
             (:Comment {id: 2})-[:HAS_CREATOR]->(alice), \
             (:Post:Comment {id: 3})-[:HAS_CREATOR]->(bob), \
             (:Other {id: 4})-[:HAS_CREATOR]->(bob), \
             (:Post {id: 5})-[:HAS_CREATOR]->(alice), \
             (:Comment {id: 5})-[:HAS_CREATOR]->(bob), \
             (:Other {id: 5})-[:HAS_CREATOR]->(bob), \
             (:Post {name: 'no id'})-[:HAS_CREATOR]->(bob)",
        )
        .expect("fixture creates");
    forge
}

fn rows(batches: &[RecordBatch]) -> BTreeSet<(String, String)> {
    let mut out = BTreeSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let id = array_value_to_string(batch.column(0), row).expect("id renders");
            let creator = array_value_to_string(batch.column(1), row).expect("creator renders");
            assert!(out.insert((id, creator)), "unexpected duplicate row");
        }
    }
    out
}

fn answer(forge: &GraphForge, query: &str, id: i64) -> BTreeSet<(String, String)> {
    let params = HashMap::from([("id".to_owned(), IrLiteral::Int(id))]);
    rows(
        &forge
            .execute_with_params(query, &params)
            .expect("executes")
            .batches,
    )
}

fn physical(explain: &str) -> Vec<&str> {
    explain
        .split_once("PhysicalPlan\n------------\n")
        .expect("physical-plan section")
        .1
        .lines()
        .collect()
}

#[test]
fn the_id_predicate_is_planned_below_the_expansion() {
    let forge = fixture();
    let explain = forge.explain(&SHAPE.replace("$id", "1")).expect("plan");
    let plan = physical(&explain);
    let expand = plan
        .iter()
        .position(|line| line.trim_start().starts_with("ExpandExec"))
        .unwrap_or_else(|| panic!("no expansion in\n{explain}"));
    let id = plan
        .iter()
        .position(|line| line.contains("id@") && line.contains("= 1"))
        .unwrap_or_else(|| panic!("no id predicate in\n{explain}"));
    assert!(
        id > expand,
        "the id predicate sits above the expansion:\n{explain}"
    );
    // Only the destination label test, which reads the expansion's output,
    // may remain above it.
    assert!(
        plan[..expand]
            .iter()
            .filter(|line| line.trim_start().starts_with("FilterExec"))
            .all(|line| !line.contains("cypher_or")),
        "the label disjunction sits above the expansion:\n{explain}"
    );
}

#[test]
fn pushing_the_conjuncts_does_not_change_an_answer() {
    let forge = fixture();
    let all = answer(&forge, UNFILTERED, 0);
    let rendered = |id: i64| id.to_string();
    for id in 0..=6 {
        let expected: BTreeSet<_> = all
            .iter()
            .filter(|(value, _)| *value == rendered(id))
            .cloned()
            .collect();
        assert_eq!(answer(&forge, SHAPE, id), expected, "id {id}");
    }
    // Post, Comment, both labels, neither label, and a shared id, in the
    // unfiltered answer.
    let ids: BTreeSet<_> = all.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, BTreeSet::from(["1", "2", "3", "5", ""]), "{all:?}");
    assert_eq!(
        answer(&forge, SHAPE, 5),
        BTreeSet::from([
            ("5".to_owned(), "alice".to_owned()),
            ("5".to_owned(), "bob".to_owned())
        ])
    );
    assert!(answer(&forge, SHAPE, 4).is_empty());
}
