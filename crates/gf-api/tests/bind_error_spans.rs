//! Observability (#606): binder failures surface as the span-rich
//! [`GfError::Bind`] variant, so callers (and the Python/Node bindings) can
//! point at the offending token rather than just receiving a flat string.

use gf_api::{GfError, GraphForge};

/// `RETURN <undeclared>` is syntactically valid but fails to bind — the result
/// must be `GfError::Bind` whose span pinpoints the undeclared variable.
#[test]
fn undeclared_variable_yields_bind_error_with_accurate_span() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let query = "RETURN missingVar";

    let err = gf
        .execute(query)
        .expect_err("undeclared variable should fail to bind");

    let GfError::Bind { msg, span } = err else {
        panic!("expected GfError::Bind, got {err:?}");
    };

    assert!(span.start < span.end, "span must be non-empty: {span:?}");
    assert!(
        span.end <= query.len(),
        "span {span:?} out of bounds for {query:?}"
    );
    assert_eq!(
        &query[span.start..span.end],
        "missingVar",
        "span should cover the undeclared variable; msg = {msg:?}"
    );
}

/// A bind error reaching the public API must NOT be the span-less
/// `GfError::Plan` variant anymore (regression guard for #606).
#[test]
fn bind_failures_are_not_plain_plan_errors() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let err = gf
        .execute("RETURN undeclaredThing")
        .expect_err("should fail to bind");
    assert!(
        matches!(err, GfError::Bind { .. }),
        "bind failures should be GfError::Bind, got {err:?}"
    );
}
