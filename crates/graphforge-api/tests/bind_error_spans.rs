//! Observability (#606): binder failures surface as the span-rich
//! [`GfError::Bind`] variant, so callers (and the Python/Node bindings) can
//! point at the offending token rather than just receiving a flat string.

use graphforge_api::{GfError, GraphForge};

/// `RETURN <undeclared>` is syntactically valid but fails to bind — the result
/// must be `GfError::Bind` whose span pinpoints the undeclared variable.
#[test]
fn undeclared_variable_yields_bind_error_with_accurate_span() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let query = "RETURN missingVar";

    let err = gf
        .execute(query)
        .expect_err("undeclared variable should fail to bind");

    let GfError::Bind { msg, span, .. } = err else {
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

/// One fixture is executed by Rust, Python and Node; adapters must not invent a
/// separate taxonomy. The type correction is deliberate; all other rows freeze
/// the existing entry-point-specific presentation.
#[test]
fn structured_stage_error_entry_point_matrix() {
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("stage_error_matrix.json")).unwrap();
    let gf = GraphForge::new(None).unwrap();
    for case in cases {
        let query = case["query"].as_str().unwrap();
        let modes: &[&str] = if case["operation"] == "analyze" {
            &["analyze"]
        } else {
            &["execute", "params", "stream", "stream_params", "explain"]
        };
        for &mode in modes {
            let error = match mode {
                "analyze" => {
                    gf.execute(query).unwrap();
                    gf.analyze(
                        None,
                        graphforge_api::AnalyzeOptions {
                            by: graphforge_api::AnalyzeAlgorithm::EulerCircuit,
                            directed: false,
                            ..Default::default()
                        },
                    )
                    .err()
                }
                "execute" => gf.execute(query).err(),
                "params" => gf
                    .execute_with_params(query, &std::collections::HashMap::new())
                    .err(),
                "stream" => gf.execute_stream(query).err(),
                "stream_params" => gf
                    .execute_stream_with_params(query, &std::collections::HashMap::new())
                    .err(),
                "explain" => gf.explain(query).err(),
                _ => unreachable!(),
            }
            .unwrap_or_else(|| panic!("{mode} {query} unexpectedly succeeded"));
            let expected_code = if mode.starts_with("stream") {
                case.get("stream_rust").unwrap_or(&case["rust"])
            } else if mode == "explain" {
                case.get("explain_rust").unwrap_or(&case["rust"])
            } else {
                &case["rust"]
            };
            assert_eq!(
                error.code(),
                expected_code.as_str().unwrap(),
                "{mode} {query}: {error:?}"
            );
            let expected_message = if mode == "explain" {
                case.get("explain_message").unwrap_or(&case["message"])
            } else {
                &case["message"]
            }
            .as_str()
            .unwrap();
            assert!(
                error.to_string().ends_with(expected_message),
                "{mode} {query}: {error}"
            );
            assert_stage_diagnostic(error, &case, expected_message);
        }
    }
}

fn assert_stage_diagnostic(error: GfError, case: &serde_json::Value, expected_message: &str) {
    use graphforge_core::{BindErrorKind, LoweringError, ParseErrorKind};
    match case["kind"].as_str().unwrap() {
        "UnexpectedEof" => {
            let GfError::Parse {
                diagnostic,
                span,
                msg,
            } = error
            else {
                panic!("parser diagnostic lost")
            };
            let diagnostic = diagnostic.expect("parser source");
            assert!(
                matches!(diagnostic.kind, ParseErrorKind::UnexpectedEof { ref expected } if !expected.is_empty())
            );
            assert_eq!(diagnostic.span, span);
            assert_eq!(diagnostic.message, case["message"]);
            assert_eq!(msg, expected_message);
            assert_eq!(span, graphforge_core::Span::new(8, 8));
        }
        "UndeclaredVariable" => {
            let GfError::Bind {
                diagnostics, span, ..
            } = error
            else {
                panic!("binder diagnostic lost")
            };
            assert_eq!(diagnostics.len(), 1);
            assert_eq!(diagnostics[0].kind, BindErrorKind::UndeclaredVariable);
            assert_eq!(diagnostics[0].span, span);
            assert_eq!(span, graphforge_core::Span::new(7, 17));
        }
        "InvalidType" => assert!(
            matches!(error, GfError::Lowering(LoweringError::InvalidType(_))),
            "{error:?}"
        ),
        "UnsupportedExpr" => assert!(
            matches!(
                error,
                GfError::Lowering(LoweringError::UnsupportedExpr(_))
                    | GfError::LoweringExecution(LoweringError::UnsupportedExpr(_))
            ),
            "{error:?}"
        ),
        "UndefinedEulerCircuit" => assert!(matches!(
            error,
            GfError::Algorithm(graphforge_api::AlgorithmError::UndefinedEulerCircuit)
        )),
        "Foreign" => assert!(matches!(error, GfError::Execution(_) | GfError::Plan(_))),
        other => panic!("unhandled fixture kind {other}"),
    }
}

#[test]
fn multiple_real_binder_diagnostics_keep_each_kind_and_span() {
    let gf = GraphForge::new(None).unwrap();
    let query = "RETURN firstMissing, secondMissing";
    let error = gf.execute(query).unwrap_err();
    let GfError::Bind {
        diagnostics, span, ..
    } = error
    else {
        panic!("binder diagnostic lost")
    };
    assert_eq!(diagnostics.len(), 2);
    assert_eq!(span, diagnostics[0].span);
    for (diagnostic, name) in diagnostics.iter().zip(["firstMissing", "secondMissing"]) {
        assert_eq!(
            diagnostic.kind,
            graphforge_core::BindErrorKind::UndeclaredVariable
        );
        assert_eq!(&query[diagnostic.span.start..diagnostic.span.end], name);
    }
}

#[test]
fn explanation_stages_share_binder_codes_payloads_and_all_spans() {
    use graphforge_api::ExplainStage;
    let gf = GraphForge::new(None).unwrap();
    let query = "RETURN firstMissing, secondMissing";
    let expected = binder_signature(gf.execute(query).unwrap_err());
    let params = std::collections::HashMap::new();
    for error in [
        gf.execute_with_params(query, &params).err().unwrap(),
        gf.execute_stream(query).err().unwrap(),
        gf.execute_stream_with_params(query, &params).err().unwrap(),
        gf.explain(query).unwrap_err(),
        gf.explain_stage(query, ExplainStage::GraphIr).unwrap_err(),
        gf.explain_stage(query, ExplainStage::LogicalPlan)
            .unwrap_err(),
        gf.explain_stage(query, ExplainStage::PhysicalPlan)
            .unwrap_err(),
    ] {
        assert_eq!(binder_signature(error), expected);
    }
    // AST inspection is deliberately syntax-only; it does not run the binder.
    let ast = gf.explain_stage(query, ExplainStage::Ast).unwrap();
    let ast: serde_json::Value = serde_json::from_str(&ast).unwrap();
    assert!(ast.get("clauses").is_some());
    assert!(matches!(
        gf.explain_stage(query, ExplainStage::BoundAst),
        Err(GfError::NotImplemented(_))
    ));
}

fn binder_signature(
    error: GfError,
) -> (
    String,
    graphforge_core::Span,
    Vec<graphforge_core::BindError>,
) {
    assert_eq!(error.code(), "GF_PARSE");
    let GfError::Bind {
        msg,
        span,
        diagnostics,
    } = error
    else {
        panic!("binder rejection lost its typed facade diagnostic")
    };
    assert_eq!(diagnostics.len(), 2);
    assert_eq!(span, graphforge_core::Span::new(7, 19));
    assert_eq!(diagnostics[1].span, graphforge_core::Span::new(21, 34));
    assert!(
        diagnostics
            .iter()
            .all(|error| error.kind == graphforge_core::BindErrorKind::UndeclaredVariable)
    );
    (msg, span, diagnostics)
}

#[test]
fn explanation_stage_selection_keeps_typed_parameter_and_lowering_boundaries() {
    use graphforge_api::{ExplainStage, LoweringError};
    let gf = GraphForge::new(None).unwrap();
    for query in ["RETURN $missing", "RETURN 1.foo"] {
        gf.explain_stage(query, ExplainStage::Ast).unwrap();
        gf.explain_stage(query, ExplainStage::GraphIr).unwrap();
        let expected = gf.explain(query).unwrap_err();
        if query == "RETURN $missing" {
            // Logical planning can retain an unresolved placeholder; physical
            // planning requires its value, just like the full explanation.
            let logical = gf.explain_stage(query, ExplainStage::LogicalPlan).unwrap();
            assert!(logical.contains("$missing"));
            let error = gf
                .explain_stage(query, ExplainStage::PhysicalPlan)
                .unwrap_err();
            assert_eq!(error.code(), "GF_PLAN");
            assert_eq!(error.to_string(), expected.to_string());
        } else {
            for stage in [ExplainStage::LogicalPlan, ExplainStage::PhysicalPlan] {
                let error = gf.explain_stage(query, stage).unwrap_err();
                assert_eq!(error.code(), "GF_VALIDATION");
                assert_eq!(error.to_string(), expected.to_string());
                assert!(matches!(
                    error,
                    GfError::Lowering(LoweringError::InvalidType(_))
                ));
            }
        }
    }
}
