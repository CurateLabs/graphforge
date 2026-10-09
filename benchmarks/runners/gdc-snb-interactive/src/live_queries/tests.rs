use std::sync::OnceLock;

use super::*;
use crate::queries::query_definition;
use crate::{Category, MappingOutcome, map_operation};

fn forge() -> &'static GraphForge {
    static FORGE: OnceLock<GraphForge> = OnceLock::new();
    FORGE.get_or_init(|| load_committed_query_fixture().expect("committed fixture loads"))
}

fn expected() -> BTreeMap<Operation, Vec<ExpectedCase>> {
    parse_expected(QUERY_EXPECTED).expect("committed expected results parse")
}

fn definition(operation: Operation) -> &'static QueryDefinition {
    query_definition(operation).expect("runnable read")
}

/// The definition with `from` replaced by `to` in its Cypher text.
fn mutated(operation: Operation, from: &str, to: &str) -> QueryDefinition {
    let original = definition(operation);
    let text = original.cypher().expect("cypher definition");
    assert!(text.contains(from), "{operation} text lacks {from:?}");
    let leaked: &'static str = Box::leak(text.replace(from, to).into_boxed_str());
    QueryDefinition {
        interface: QueryInterface::Cypher(leaked),
        ..*original
    }
}

#[test]
fn definitions_cover_exactly_the_mapped_reads() {
    let runnable: Vec<Operation> = query_definitions()
        .iter()
        .map(|definition| definition.operation)
        .collect();
    let reads: Vec<Operation> = Operation::ALL
        .into_iter()
        .filter(|operation| {
            operation.category() != Category::Update && *operation != Operation::Ic14
        })
        .collect();
    assert_eq!(runnable, reads);
    for definition in query_definitions() {
        assert!(!definition.columns.is_empty(), "{}", definition.operation);
        assert!(!definition.notes.is_empty(), "{}", definition.operation);
        // #1919 is fixed, so no note cites it as a reason to rewrite.
        assert!(
            !definition.notes.contains("#1919"),
            "{}",
            definition.operation
        );
        if let Some(variance) = definition.spec_variance {
            assert!(
                variance.starts_with("reference behaviour, differs from spec prose: "),
                "{}",
                definition.operation
            );
        }
        for column in definition.unordered_list_columns {
            assert!(
                definition.columns.contains(column),
                "{}",
                definition.operation
            );
        }
        let Some(text) = definition.cypher() else {
            assert_eq!(definition.operation, Operation::Ic13);
            continue;
        };
        // The declared parameters are exactly the $names the text uses.
        let used: BTreeSet<&str> = text
            .split('$')
            .skip(1)
            .map(|rest| {
                let end = rest
                    .find(|character: char| !character.is_ascii_alphanumeric())
                    .unwrap_or(rest.len());
                &rest[..end]
            })
            .collect();
        let declared: BTreeSet<&str> = definition.parameter_names().into_iter().collect();
        assert_eq!(used, declared, "{}", definition.operation);
        if let Some(limit) = definition.limit {
            assert!(
                text.contains(&format!("LIMIT {limit}\n"))
                    || text.ends_with(&format!("LIMIT {limit}")),
                "{}",
                definition.operation
            );
        }
        match map_operation(definition.operation) {
            MappingOutcome::Compatible(mapping) => {
                assert_eq!(mapping.interface, "cypher");
                assert_eq!(mapping.cypher_shape, text);
            }
            MappingOutcome::SemanticIncompatibility { .. } => panic!("{}", definition.operation),
        }
    }
}

#[test]
fn every_runnable_read_matches_its_independent_expected_result() {
    let expected = expected();
    let mut failures = Vec::new();
    for definition in query_definitions() {
        if let Err(error) = check_definition(forge(), definition, &expected[&definition.operation])
        {
            failures.push(format!("{}: {error}", definition.operation));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn expected_results_exercise_limits_and_non_empty_rows() {
    let expected = expected();
    for definition in query_definitions() {
        let cases = &expected[&definition.operation];
        assert!(
            cases.iter().any(|case| !case.rows.is_empty()),
            "{} has only empty expected results",
            definition.operation
        );
        if let Some(limit) = definition.limit {
            assert!(
                cases.iter().all(|case| case.rows.len() <= limit),
                "{} exceeds its LIMIT",
                definition.operation
            );
        }
    }
    // At least the long-tail reads reach their LIMIT on the fixture.
    for operation in [
        Operation::Ic1,
        Operation::Ic2,
        Operation::Ic4,
        Operation::Ic5,
        Operation::Ic6,
        Operation::Ic7,
        Operation::Ic8,
        Operation::Ic9,
        Operation::Ic10,
        Operation::Ic11,
        Operation::Is2,
    ] {
        let limit = definition(operation).limit.unwrap();
        assert!(
            expected[&operation]
                .iter()
                .any(|case| case.rows.len() == limit),
            "{operation} never reaches LIMIT {limit}"
        );
    }
}

/// Each mutation changes the query's semantics in a way the fixture is built
/// to expose (a tie, a boundary value, a quirk of the reference text); the
/// validation must reject every one.
#[test]
fn semantic_mutations_are_rejected() {
    let expected = expected();
    let mutations = [
        // Tie-breakers.
        (Operation::Is2, "messageId ASC", "messageId DESC"),
        (
            Operation::Is3,
            "toInteger(personId) ASC",
            "toInteger(personId) DESC",
        ),
        (
            Operation::Ic2,
            "toInteger(postOrCommentId) ASC",
            "toInteger(postOrCommentId) DESC",
        ),
        (Operation::Ic7, "min(message.id)", "max(message.id)"),
        // Boundary comparisons.
        (
            Operation::Ic2,
            "message.creationDate <= $maxDate",
            "message.creationDate < $maxDate",
        ),
        (
            Operation::Ic9,
            "message.creationDate < $maxDate",
            "message.creationDate <= $maxDate",
        ),
        // Traversal depth and limits.
        (Operation::Ic1, "[:KNOWS*1..3]", "[:KNOWS*1..2]"),
        (Operation::Ic8, "LIMIT 20", "LIMIT 21"),
        // The reference's simple-CASE null tests never match (as in Neo4j);
        // the spec-prose reading `IS NULL` must disagree with the reference.
        (
            Operation::Is7,
            "CASE r\n        WHEN null THEN false",
            "CASE WHEN r IS NULL THEN false",
        ),
        (
            Operation::Ic1,
            "CASE uni.name WHEN null THEN null",
            "CASE WHEN uni IS NULL THEN null",
        ),
        // The reference IC12 tag-name disjunct.
        (
            Operation::Ic12,
            "WHERE tag.name = $tagClassName OR baseTagClass.name",
            "WHERE baseTagClass.name",
        ),
        // Calendar arithmetic for the birthday window.
        (Operation::Ic10, "($month % 12) + 1", "$month + 1"),
        (Operation::Ic10, "birthday.day < 22", "birthday.day < 21"),
        // The Message label disjunction (a University shares a Post id).
        (
            Operation::Is4,
            "WHERE (m:Post OR m:Comment) AND m.id",
            "WHERE m.id",
        ),
    ];
    for (operation, from, to) in mutations {
        let mutant = mutated(operation, from, to);
        let result = check_definition(forge(), &mutant, &expected[&operation]);
        assert!(
            result.is_err(),
            "{operation} mutation {from:?} -> {to:?} still matched the expected result"
        );
    }
}

#[test]
fn ic13_reports_minus_one_zero_and_hop_counts() {
    let ic13 = definition(Operation::Ic13);
    let run = |first: i64, second: i64| {
        let parameters = QueryParameters::from([
            ("person1Id".into(), Value::from(first)),
            ("person2Id".into(), Value::from(second)),
        ]);
        execute_query(forge(), ic13, &parameters).unwrap().rows
    };
    assert_eq!(run(2048, 2048), vec![vec![Value::from(0)]]);
    assert_eq!(run(2048, 2039), vec![vec![Value::from(-1)]]);
    assert_eq!(run(2048, 2021), vec![vec![Value::from(4)]]);
    // Undirected: the reverse direction has the same length.
    assert_eq!(run(2021, 2048), vec![vec![Value::from(4)]]);
}

#[test]
fn parameters_must_match_the_definition_exactly() {
    let is1 = definition(Operation::Is1);
    let good = QueryParameters::from([("personId".into(), Value::from(2048))]);
    bind_parameters(is1, &good).unwrap();
    for bad in [
        QueryParameters::new(),
        QueryParameters::from([("personId".into(), Value::from("2048"))]),
        QueryParameters::from([
            ("personId".into(), Value::from(2048)),
            ("extra".into(), Value::from(1)),
        ]),
    ] {
        assert!(matches!(
            bind_parameters(is1, &bad),
            Err(SuiteError::InvalidDocument(_))
        ));
    }
}

#[test]
fn heterogeneous_list_encodings_fail_closed() {
    let result = forge().execute("RETURN [1, 'a'] AS mixed").unwrap();
    let error = arrow_rows(&result.batches, &result.schema).unwrap_err();
    assert!(error.to_string().contains("heterogeneous"), "{error}");
}

#[test]
fn unordered_list_columns_compare_as_sets_but_rows_stay_ordered() {
    let ic12 = definition(Operation::Ic12);
    let row = |id: i64, tags: &[&str]| {
        vec![
            Value::from(id),
            Value::from("A"),
            Value::from("B"),
            Value::from(tags.to_vec()),
            Value::from(1),
        ]
    };
    validate_rows(ic12, &[row(1, &["x", "y"])], &[row(1, &["y", "x"])]).unwrap();
    assert!(validate_rows(ic12, &[row(1, &["x"])], &[row(1, &["y"])]).is_err());
    assert!(
        validate_rows(
            ic12,
            &[row(1, &["x"]), row(2, &["x"])],
            &[row(2, &["x"]), row(1, &["x"])]
        )
        .is_err()
    );
    // Integers and floats are distinct values.
    let is4 = definition(Operation::Is4);
    assert!(
        validate_rows(
            is4,
            &[vec![Value::from(1), Value::from("a")]],
            &[vec![serde_json::json!(1.0), Value::from("a")]]
        )
        .is_err()
    );
}

#[test]
fn lane_passes_every_read_and_keeps_refusals_typed() {
    let evidence = run_live_queries().unwrap();
    assert_eq!(evidence.lane, EvidenceLane::LiveQueryFixture);
    assert_eq!(evidence.status, OperationStatus::Passed);
    assert!(!evidence.certification);
    assert!(evidence.live_context.is_none());
    let context = evidence.query_context.as_ref().unwrap();
    assert_eq!(context.runnable_operations, 20);
    assert!(context.cases >= 20);
    for definition in query_definitions() {
        let outcome = outcome_for(&evidence, definition.operation).unwrap();
        assert_eq!(
            outcome.status,
            OperationStatus::Passed,
            "{:?}",
            outcome.cause
        );
        assert_eq!(outcome.validation_mode, "exact");
    }
    let ic14 = outcome_for(&evidence, Operation::Ic14).unwrap();
    assert_eq!(ic14.status, OperationStatus::SemanticIncompatibility);
    assert!(
        ic14.cause
            .as_deref()
            .unwrap()
            .starts_with("weighted_interaction_path_enumeration_not_exposed")
    );
    for operation in Operation::ALL {
        if operation.category() == Category::Update {
            let outcome = outcome_for(&evidence, operation).unwrap();
            assert_eq!(outcome.status, OperationStatus::SemanticIncompatibility);
            assert!(
                outcome
                    .cause
                    .as_deref()
                    .unwrap()
                    .starts_with("interactive_update_stream_not_exposed")
            );
        }
    }
}
