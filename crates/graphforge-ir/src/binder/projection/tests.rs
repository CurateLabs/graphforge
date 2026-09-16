use super::super::BindErrorKind;
use super::super::patterns::{
    expr_contains_pattern_comprehension, expr_contains_pattern_predicate,
};
use super::super::tests::{expect_bind_error, make_binder, parsed_return_expr};
use super::{
    collect_grouping_refs, expr_contains_aggregate_inside_aggregate,
    expr_contains_volatile_function, extract_int_constant, extract_non_negative_int_constant,
    extract_parameter_name, is_float_constant, rewrite_grouping_refs,
    rewrite_projection_alias_refs, row_count_expr_is_integer, same_expr_shape, same_grouping_expr,
};
use crate::plan::{GraphOp, OntologyMode};
use crate::{AggFunc, VarId};
use graphforge_ast::{Expr, Literal, ReturnItem};
use graphforge_core::Span;
use graphforge_cypher::parse;

#[test]
fn expression_rewriters_traverse_every_public_ast_container() {
    let expressions = [
        "RETURN a + 1",
        "RETURN NOT a",
        "RETURN (a)",
        "RETURN a.name",
        "RETURN coalesce(a, 1)",
        "RETURN [a, 1]",
        "RETURN {k: a}",
        "RETURN CASE a WHEN 1 THEN a ELSE 0 END",
        "RETURN [x IN a WHERE x > 0 | x]",
        "RETURN all(x IN a WHERE x > 0)",
        "RETURN a IS NULL",
        "RETURN a IN [1]",
        "RETURN a STARTS WITH 'x'",
        "RETURN a =~ 'x'",
        "RETURN a:Person",
        "RETURN [(a)-->(b) WHERE a.name = 'x' | a]",
        "RETURN exists { (a)-->(b) WHERE a.name = 'x' }",
    ]
    .map(parsed_return_expr);
    let alias_projection = ReturnItem {
        expr: Expr::Literal(Literal::Int(7, Span::new(0, 1))),
        alias: Some("a".into()),
        display: None,
        span: Span::new(0, 1),
    };
    let grouping = parsed_return_expr("RETURN a");
    let bindings = [(grouping, "group_a".into(), VarId(0))];

    for expression in expressions {
        let mut refs = Vec::new();
        collect_grouping_refs(&expression, &mut refs);
        let alias_rewritten = rewrite_projection_alias_refs(
            expression.clone(),
            std::slice::from_ref(&alias_projection),
        );
        let grouping_rewritten = rewrite_grouping_refs(expression.clone(), &bindings);
        assert_eq!(alias_rewritten.span(), expression.span());
        assert_eq!(grouping_rewritten.span(), expression.span());
        let _ = expr_contains_pattern_comprehension(&expression);
        let _ = expr_contains_pattern_predicate(&expression);
        let _ = expr_contains_volatile_function(&expression);
        let _ = expr_contains_aggregate_inside_aggregate(&expression, false);
    }
}

#[test]
fn with_nested_aggregate_lowers_to_aggregate_then_scope_reset() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast =
        parse("MATCH (me)--(you) WITH me.age AS age, me.age + count(you.age) AS agg RETURN *")
            .unwrap();
    let plan = binder.bind(&ast).expect("nested WITH aggregate binds");
    let aggregate = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::Aggregate { .. }))
        .expect("aggregate op");
    let with = plan
        .ops
        .iter()
        .skip(aggregate + 1)
        .position(|op| matches!(op, GraphOp::With { .. }))
        .expect("post-aggregate scope reset");
    assert_eq!(with, 0);
}

#[test]
fn with_rejects_ambiguous_and_nested_aggregation() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ambiguous =
        parse("MATCH (me)--(you) WITH me.age + count(you.age) AS agg RETURN agg").unwrap();
    let error = binder
        .bind(&ambiguous)
        .expect_err("ambiguous grouping must fail");
    assert!(
        error
            .iter()
            .any(|error| error.message.contains("ambiguous aggregation expression"))
    );

    let nested = parse("MATCH (n) WITH count(count(*)) AS c RETURN c").unwrap();
    let error = binder
        .bind(&nested)
        .expect_err("nested aggregates must fail");
    assert!(
        error
            .iter()
            .any(|error| { error.message.contains("may not contain another aggregate") })
    );
}

#[test]
fn return_rejects_nested_and_volatile_aggregation() {
    for (query, message) in [
        (
            "RETURN count(count(*))",
            "may not contain another aggregate",
        ),
        (
            "RETURN count(rand())",
            "non-deterministic functions are not allowed",
        ),
    ] {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse(query).unwrap();
        let errors = binder.bind(&ast).expect_err("query must fail at bind");
        assert!(
            errors.iter().any(|error| error.message.contains(message)),
            "missing {message:?} for {query}: {errors:?}"
        );
    }
}

#[test]
fn aggregate_in_where_and_duplicate_alias_are_rejected() {
    expect_bind_error(
        "MATCH (a) WHERE count(a) > 10 RETURN a",
        BindErrorKind::InvalidArgument,
    );
    expect_bind_error("RETURN 1 AS a, 2 AS a", BindErrorKind::InvalidArgument);
    expect_bind_error(
        "WITH 1 AS a, 2 AS a RETURN a",
        BindErrorKind::InvalidArgument,
    );
}

#[test]
fn skip_limit_reject_negative_and_non_parameter_arguments() {
    for q in [
        "RETURN 1 SKIP -1",
        "RETURN 1 LIMIT -1",
        "RETURN 1 SKIP (-1)",
        "WITH 1 AS x SKIP -1 RETURN x",
        "WITH 1 AS x, count(*) AS c SKIP -1 RETURN x, c",
        "MATCH (n) RETURN n SKIP n.count",
        "MATCH (n) RETURN n LIMIT rand()",
        "MATCH (n) WITH n SKIP n.count RETURN n",
        "MATCH (n) WITH n, count(*) AS c LIMIT rand() RETURN n, c",
    ] {
        expect_bind_error(q, BindErrorKind::InvalidArgument);
    }
}

#[test]
fn skip_limit_accept_non_negative_integer_constants_and_parameters() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    for q in [
        "RETURN 1 SKIP 0 LIMIT 1",
        "WITH 1 AS x SKIP (1) LIMIT 2 RETURN x",
        "WITH 1 AS x, count(*) AS c SKIP 0 LIMIT 1 RETURN x, c",
        "RETURN 1 SKIP $s LIMIT $l",
        "WITH 1 AS x SKIP ($s) LIMIT ($l) RETURN x",
    ] {
        let ast = parse(q).expect("parse");
        binder
            .bind(&ast)
            .unwrap_or_else(|e| panic!("expected clean bind for {q}, got {e:?}"));
    }
}

#[test]
fn return_clause_emits_project_op() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person) RETURN a.name").unwrap();
    let plan = binder.bind(&ast).unwrap();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::Project { .. }))
    );
}

#[test]
fn return_with_count_emits_aggregate() {
    // `RETURN count(n)` lowers to an Aggregate (one Count agg, no group keys),
    // not a Project (#729).
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (n:Person) RETURN count(n) AS total").unwrap();
    let plan = binder.bind(&ast).unwrap();
    let agg = plan.ops.iter().find_map(|op| match op {
        GraphOp::Aggregate { group_by, aggs, .. } => Some((group_by, aggs)),
        _ => None,
    });
    let (group_by, aggs) = agg.expect("count should emit an Aggregate");
    assert!(
        group_by.is_empty(),
        "no non-aggregate items → no group keys"
    );
    assert_eq!(aggs.len(), 1);
    assert_eq!(aggs[0].func, AggFunc::Count);
    assert_eq!(aggs[0].alias, "total");
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::Project { .. })),
        "an aggregate RETURN must not also emit a Project"
    );
}

#[test]
fn return_with_grouping_keys_and_aggregate() {
    // `RETURN n.name, count(n)` groups by the non-aggregate item.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (n:Person) RETURN n.name AS name, count(n) AS total").unwrap();
    let plan = binder.bind(&ast).unwrap();
    let (group_by, aggs) = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Aggregate { group_by, aggs, .. } => Some((group_by, aggs)),
            _ => None,
        })
        .expect("Aggregate");
    assert_eq!(group_by.len(), 1, "n.name is the group key");
    assert_eq!(aggs.len(), 1);
}

#[test]
fn return_wildcard_requires_a_variable_in_scope() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let errors = binder
        .bind(&parse("RETURN *").unwrap())
        .expect_err("empty-scope RETURN wildcard must fail");
    assert!(errors.iter().any(|error| {
        error
            .message
            .contains("wildcard requires at least one variable")
    }));
    assert_ne!(errors[0].span, Span::default());
}

#[test]
fn with_wildcard_preserves_an_empty_named_scope() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let plan = binder
        .bind(&parse("CREATE () WITH * CREATE ()").unwrap())
        .expect("empty-scope WITH wildcard should preserve pipeline rows");
    assert_eq!(plan.ops.len(), 3);
    assert!(matches!(&plan.ops[1], GraphOp::With { items, .. } if items.is_empty()));
}

#[test]
fn nested_aggregate_rewrite_covers_scalar_container_shapes() {
    let cases = [
        "MATCH (n:Person) RETURN count(*) + 1 AS value",
        "MATCH (n:Person) RETURN -count(*) AS value",
        "MATCH (n:Person) RETURN (count(*)) AS value",
        "MATCH (n:Person) RETURN coalesce(count(*), 0) AS value",
        "MATCH (n:Person) RETURN [count(*), 1] AS value",
        "MATCH (n:Person) RETURN {total: count(*), fallback: 0} AS value",
        "MATCH (n:Person) RETURN CASE count(*) WHEN 0 THEN 1 ELSE count(*) END AS value",
        "MATCH (n:Person) RETURN count(*) IS NULL AS value",
        "MATCH (n:Person) RETURN count(*) IN [0, 1] AS value",
        "MATCH (n:Person) RETURN toString(count(*)) STARTS WITH '1' AS value",
        "MATCH (n:Person) RETURN toString(count(*)) =~ '.*' AS value",
        "MATCH (n:Person) RETURN [x IN collect(n.name) | x] AS value",
        "MATCH (n:Person) RETURN all(x IN collect(n.name) WHERE x IS NOT NULL) AS value",
    ];

    for query in cases {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let plan = binder
            .bind(&parse(query).unwrap())
            .unwrap_or_else(|errors| panic!("query={query} errors={errors:?}"));
        let aggregate_count = plan
            .ops
            .iter()
            .filter(|op| matches!(op, GraphOp::Aggregate { .. }))
            .count();
        assert_eq!(aggregate_count, 1, "query={query} ops={:?}", plan.ops);
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, GraphOp::Project { .. })),
            "nested aggregate needs a final projection: query={query} ops={:?}",
            plan.ops
        );
    }
}

#[test]
fn expression_shape_comparison_covers_scalar_and_container_variants() {
    let equal = [
        ("1", "1"),
        ("1.5", "1.5"),
        ("'x'", "'x'"),
        ("true", "true"),
        ("null", "null"),
        ("$value", "$value"),
        ("a", "a"),
        ("a.name", "a.name"),
        ("a + 1", "a + 1"),
        ("NOT a", "NOT a"),
        ("coalesce(a, 1)", "COALESCE(a, 1)"),
    ];
    for (left, right) in equal {
        assert!(
            same_expr_shape(
                &parsed_return_expr(&format!("RETURN {left}")),
                &parsed_return_expr(&format!("RETURN {right}")),
            ),
            "{left} should have the same shape as {right}"
        );
    }

    let unequal = [
        ("1", "2"),
        ("1.5", "2.5"),
        ("'x'", "'y'"),
        ("true", "false"),
        ("$left", "$right"),
        ("a", "b"),
        ("a.name", "a.age"),
        ("a + 1", "a - 1"),
        ("NOT a", "-a"),
        ("coalesce(a, 1)", "coalesce(a, 2)"),
        ("[a, 1]", "[a]"),
        ("{a: 1}", "{a: 2}"),
        ("CASE WHEN true THEN 1 END", "1"),
    ];
    for (left, right) in unequal {
        assert!(
            !same_expr_shape(
                &parsed_return_expr(&format!("RETURN {left}")),
                &parsed_return_expr(&format!("RETURN {right}")),
            ),
            "{left} should differ from {right}"
        );
    }

    assert!(same_grouping_expr(
        &parsed_return_expr("RETURN [a, 1]"),
        &parsed_return_expr("RETURN [a, 1]"),
    ));
    assert!(!same_grouping_expr(
        &parsed_return_expr("RETURN [a, 1]"),
        &parsed_return_expr("RETURN [a]"),
    ));
    assert!(same_grouping_expr(
        &parsed_return_expr("RETURN {a: 1, b: true}"),
        &parsed_return_expr("RETURN {b: true, a: 1}"),
    ));
    assert!(!same_grouping_expr(
        &parsed_return_expr("RETURN {a: 1}"),
        &parsed_return_expr("RETURN {a: 2}"),
    ));
}

#[test]
fn row_count_constant_classification_covers_every_ast_shape() {
    for source in ["1", "(1)", "-1", "$n", "1 + 2", "toInteger(1.5)"] {
        assert!(
            row_count_expr_is_integer(&parsed_return_expr(&format!("RETURN {source}"))),
            "{source}"
        );
    }
    assert!(!row_count_expr_is_integer(&parsed_return_expr(
        "RETURN 1.5"
    )));

    for source in ["1.5", "(1.5)", "-1.5"] {
        assert!(
            is_float_constant(&parsed_return_expr(&format!("RETURN {source}"))),
            "{source}"
        );
    }
    assert!(!is_float_constant(&parsed_return_expr("RETURN 1")));

    assert_eq!(
        extract_parameter_name(&parsed_return_expr("RETURN ($rows)")),
        Some("rows".into())
    );
    assert_eq!(
        extract_parameter_name(&parsed_return_expr("RETURN 1")),
        None
    );
    assert_eq!(
        extract_int_constant(&parsed_return_expr("RETURN -(-1)")),
        Some(1)
    );
    assert_eq!(
        extract_int_constant(&parsed_return_expr("RETURN true")),
        None
    );
    assert_eq!(
        extract_non_negative_int_constant(&parsed_return_expr("RETURN -1")),
        None
    );
}
