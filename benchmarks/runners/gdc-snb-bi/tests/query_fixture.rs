//! Every runnable SNB BI read, executed through the public API on the
//! committed fixture and compared with rows derived independently from the
//! fixture CSV files (#1879).

use graphforge_api::GraphForge;
use graphforge_benchmark_gdc_snb_bi::queries::{BI_QUERIES, BiQuery, REFUSED_READS, bi_query};
use graphforge_benchmark_gdc_snb_bi::query_fixture::{
    compare_rows, execute_query, load_expected_rows, load_query_graph, load_query_parameters,
    run_query_fixture,
};
use graphforge_benchmark_gdc_snb_bi::{Category, MappingOutcome, Operation, map_operation};
use std::path::PathBuf;
use std::sync::OnceLock;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/gdc/snb-bi-queries")
}

fn loaded() -> &'static GraphForge {
    static FORGE: OnceLock<GraphForge> = OnceLock::new();
    FORGE.get_or_init(|| {
        let forge = GraphForge::new(None).unwrap();
        load_query_graph(&forge, &fixture().join("graph")).unwrap();
        forge
    })
}

fn rows_of(query: &BiQuery) -> Vec<Vec<serde_json::Value>> {
    let parameters = load_query_parameters(&fixture()).unwrap();
    execute_query(loaded(), query, &parameters[&query.operation]).unwrap()
}

fn query(operation: Operation) -> &'static BiQuery {
    bi_query(operation).unwrap()
}

#[test]
fn every_runnable_read_matches_its_independent_expectation() {
    let parameters = load_query_parameters(&fixture()).unwrap();
    let mut failures = Vec::new();
    for query in &BI_QUERIES {
        let expected = load_expected_rows(&fixture(), query.operation).unwrap();
        assert!(!expected.is_empty(), "{} expects no rows", query.operation);
        let outcome = execute_query(loaded(), query, &parameters[&query.operation])
            .and_then(|rows| compare_rows(query.operation, &expected, &rows));
        if let Err(error) = outcome {
            failures.push(error.to_string());
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

#[test]
fn fixture_lane_reports_every_read_once() {
    let evidence = run_query_fixture(&fixture()).unwrap();
    assert_eq!(evidence["status"], "passed", "{evidence:#}");
    assert_eq!(evidence["certification"], false);
    let operations = evidence["operations"].as_array().unwrap();
    assert_eq!(operations.len(), 20);
    let passed = operations
        .iter()
        .filter(|entry| entry["status"] == "passed")
        .count();
    assert_eq!(passed, BI_QUERIES.len());
    // The evidence carries each read's variance label for the scorecard.
    for entry in operations
        .iter()
        .filter(|entry| entry["status"] == "passed")
    {
        let operation: Operation = entry["operation"].as_str().unwrap().parse().unwrap();
        assert_eq!(
            entry["rewrite"].as_str(),
            query(operation).rewrite,
            "{operation}"
        );
    }
    for refusal in &REFUSED_READS {
        assert!(operations.iter().any(|entry| {
            entry["operation"] == refusal.operation.code()
                && entry["status"] == "semantic_incompatibility"
                && entry["cause"] == refusal.cause
        }));
    }
}

#[test]
fn every_analytical_read_is_runnable_or_refused_exactly_once() {
    for operation in Operation::ALL {
        if operation.category() != Category::AnalyticalRead {
            assert!(bi_query(operation).is_none());
            continue;
        }
        let runnable = BI_QUERIES
            .iter()
            .filter(|query| query.operation == operation)
            .count();
        let refused = REFUSED_READS
            .iter()
            .filter(|refusal| refusal.operation == operation)
            .count();
        assert_eq!(runnable + refused, 1, "{operation}");
        match map_operation(operation) {
            MappingOutcome::Compatible(mapping) => {
                assert_eq!(mapping.cypher_shape, query(operation).cypher);
            }
            MappingOutcome::SemanticIncompatibility { cause, .. } => {
                assert_eq!(runnable, 0, "{operation}");
                assert!(!cause.is_empty());
            }
        }
    }
}

/// Every departure from the LDBC text is labelled as a variance, and one caused
/// by a GraphForge defect cites its tracking issue (#1887, #1888).
#[test]
fn rewrites_are_labelled_variances_that_cite_their_cause() {
    let rewritten: Vec<Operation> = BI_QUERIES
        .iter()
        .filter(|query| query.rewrite.is_some())
        .map(|query| query.operation)
        .collect();
    assert_eq!(
        rewritten,
        [
            Operation::Bi1,
            Operation::Bi2,
            Operation::Bi4,
            Operation::Bi8,
            Operation::Bi10,
            Operation::Bi13,
            Operation::Bi14,
            Operation::Bi16,
            Operation::Bi17,
        ]
    );
    for query in &BI_QUERIES {
        let Some(rewrite) = query.rewrite else {
            continue;
        };
        assert!(
            rewrite.starts_with("rewrite: LDBC text "),
            "{}",
            query.operation
        );
        let cites_defect = rewrite.contains("#1887 D") || rewrite.contains("#1888 D");
        let not_a_defect = matches!(query.operation, Operation::Bi10 | Operation::Bi14);
        assert_eq!(cites_defect, !not_a_defect, "{}", query.operation);
    }
}

#[test]
fn declared_parameters_are_exactly_the_ones_the_text_uses() {
    for query in &BI_QUERIES {
        let mut used: Vec<&str> = query
            .cypher
            .split('$')
            .skip(1)
            .map(|rest| {
                let end = rest
                    .find(|character: char| !character.is_ascii_alphanumeric())
                    .unwrap_or(rest.len());
                &rest[..end]
            })
            .collect();
        used.sort_unstable();
        used.dedup();
        let mut declared: Vec<&str> = query.parameters.iter().map(|p| p.name).collect();
        declared.sort_unstable();
        assert_eq!(used, declared, "{}", query.operation);
    }
}

#[test]
fn bi10_refuses_path_distances_other_than_the_specification() {
    let mut document: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(fixture().join("parameters.json")).unwrap())
            .unwrap();
    document["queries"]["BI10"]["maxPathDistance"]["value"] = serde_json::json!(5);
    let directory = std::env::temp_dir().join(format!("gdc-snb-bi-bi10-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("parameters.json"), document.to_string()).unwrap();
    let error = load_query_parameters(&directory).unwrap_err().to_string();
    std::fs::remove_dir_all(&directory).unwrap();
    assert!(
        error.contains("parameter_not_fixed_specification_value"),
        "{error}"
    );
}

/// Each mutant drops one clause of the specification. The fixture and its
/// independent expectations must tell every mutant apart from the real query.
#[test]
fn semantic_mutants_do_not_match_the_expectations() {
    let mutants: [(Operation, &str, &str); 5] = [
        // BI10 without the minimum path distance.
        (
            Operation::Bi10,
            "WHERE $minPathDistance <= distance AND distance <= $maxPathDistance",
            "WHERE $minPathDistance <= distance + 2 AND distance <= $maxPathDistance",
        ),
        // BI16 with the friends limit loosened by one.
        (
            Operation::Bi16,
            "WHERE cp2 <= $maxKnowsLimit",
            "WHERE cp2 <= $maxKnowsLimit + 1",
        ),
        // BI17 without the delta.
        (
            Operation::Bi17,
            "message1CreationDate.epochMillis + $delta * 3600000",
            "message1CreationDate.epochMillis + $delta * 0",
        ),
        // BI11 without the edge date window on the closing edge.
        (
            Operation::Bi11,
            "MATCH (c)-[k3:KNOWS]-(a)\nWHERE $startDate <= k3.creationDate AND k3.creationDate <= $endDate",
            "MATCH (c)-[k3:KNOWS]-(a)\nWHERE $startDate <= $endDate",
        ),
        // BI13 counting every like rather than likes by zombies.
        (
            Operation::Bi13,
            "count(CASE WHEN likerZombie.id IN zombieIds THEN likerZombie END)",
            "count(likerZombie)",
        ),
    ];
    for (operation, original, mutated) in mutants {
        let base = query(operation);
        assert!(base.cypher.contains(original), "{operation} mutant anchor");
        let text = base.cypher.replace(original, mutated);
        let mutant = BiQuery {
            cypher: Box::leak(text.into_boxed_str()),
            ..*base
        };
        let expected = load_expected_rows(&fixture(), operation).unwrap();
        let rows = rows_of(&mutant);
        assert!(
            compare_rows(operation, &expected, &rows).is_err(),
            "{operation} mutant matched the expectation"
        );
    }
}
