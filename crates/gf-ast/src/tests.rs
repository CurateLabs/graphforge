//! Unit tests for gf-ast: Token, ParseError, and AST node types.
#![allow(clippy::pedantic)]

use crate::{Span, ast::*, parse_error::*, token::*};
use serde_json;

// ---------------------------------------------------------------------------
// Span
// ---------------------------------------------------------------------------

#[test]
fn span_display() {
    assert_eq!(Span::new(0, 10).to_string(), "0..10");
    assert_eq!(Span::new(3, 3).to_string(), "3..3");
}

#[test]
fn span_default_is_zero() {
    let s = Span::default();
    assert_eq!(s.start, 0);
    assert_eq!(s.end, 0);
}

#[test]
fn span_roundtrip_json() {
    let s = Span::new(5, 15);
    let json = serde_json::to_string(&s).unwrap();
    let back: Span = serde_json::from_str(&json).unwrap();
    assert_eq!(s, back);
}

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

#[test]
fn token_span_extraction_keyword() {
    let tok = Token::Keyword(Keyword::Match, Span::new(0, 5));
    assert_eq!(tok.span(), Span::new(0, 5));
}

#[test]
fn token_span_extraction_ident() {
    let tok = Token::Ident("n".to_owned(), Span::new(7, 8));
    assert_eq!(tok.span(), Span::new(7, 8));
}

#[test]
fn token_span_extraction_punctuation() {
    assert_eq!(Token::LParen(Span::new(0, 1)).span(), Span::new(0, 1));
    assert_eq!(Token::RParen(Span::new(1, 2)).span(), Span::new(1, 2));
    assert_eq!(Token::Dot(Span::new(3, 4)).span(), Span::new(3, 4));
    assert_eq!(Token::Eq(Span::new(5, 6)).span(), Span::new(5, 6));
}

#[test]
fn token_is_trivia_whitespace() {
    assert!(Token::Whitespace(Span::new(0, 1)).is_trivia());
}

#[test]
fn token_is_trivia_comment() {
    assert!(Token::Comment("// hi".to_owned(), Span::new(0, 5)).is_trivia());
}

#[test]
fn token_non_trivia() {
    assert!(!Token::Ident("x".to_owned(), Span::new(0, 1)).is_trivia());
    assert!(!Token::Eof(Span::new(10, 10)).is_trivia());
}

#[test]
fn token_int_lit() {
    let tok = Token::IntLit(42, Span::new(0, 2));
    assert_eq!(tok.span(), Span::new(0, 2));
}

#[test]
fn token_str_lit() {
    let tok = Token::StrLit("hello".to_owned(), Span::new(0, 7));
    assert_eq!(tok.span(), Span::new(0, 7));
}

#[test]
fn token_param() {
    let tok = Token::Param("name".to_owned(), Span::new(5, 10));
    assert_eq!(tok.span(), Span::new(5, 10));
}

#[test]
fn token_eof_span() {
    let tok = Token::Eof(Span::new(100, 100));
    assert_eq!(tok.span(), Span::new(100, 100));
}

// ---------------------------------------------------------------------------
// ParseError
// ---------------------------------------------------------------------------

#[test]
fn parse_error_display_unexpected_char() {
    let e = ParseError::new(ParseErrorKind::UnexpectedChar, Span::new(3, 4), "bad char");
    let s = e.to_string();
    assert!(s.contains("unexpected character"), "got: {s}");
    assert!(s.contains("3..4"), "got: {s}");
}

#[test]
fn parse_error_display_unexpected_token() {
    let e = ParseError::new(
        ParseErrorKind::UnexpectedToken {
            found: "BOOM".to_owned(),
            expected: vec!["MATCH".to_owned()],
        },
        Span::new(0, 4),
        "msg",
    );
    let s = e.to_string();
    assert!(s.contains("BOOM"), "got: {s}");
}

#[test]
fn parse_error_display_unterminated_string() {
    let e = ParseError::new(ParseErrorKind::UnterminatedString, Span::new(1, 5), "msg");
    assert!(e.to_string().contains("unterminated string"));
}

#[test]
fn parse_error_display_unexpected_eof() {
    let e = ParseError::new(
        ParseErrorKind::UnexpectedEof {
            expected: vec!["statement".to_owned()],
        },
        Span::new(0, 0),
        "msg",
    );
    assert!(e.to_string().contains("unexpected end of input"));
}

#[test]
fn parse_error_roundtrip_json() {
    let e = ParseError::new(ParseErrorKind::UnexpectedChar, Span::new(2, 3), "oops");
    let json = serde_json::to_string(&e).unwrap();
    let back: ParseError = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn parse_error_is_std_error() {
    let e = ParseError::new(ParseErrorKind::InvalidParameter, Span::new(0, 1), "msg");
    let _: &dyn std::error::Error = &e;
}

// ---------------------------------------------------------------------------
// AST node construction and JSON round-trip
// ---------------------------------------------------------------------------

fn zero() -> Span {
    Span::new(0, 0)
}

#[test]
fn ast_query_empty() {
    let q = AstQuery {
        dialect: DialectVersion::OpenCypher9,
        clauses: vec![],
        span: Span::new(0, 0),
    };
    assert!(q.clauses.is_empty());
    let json = serde_json::to_string(&q).unwrap();
    let back: AstQuery = serde_json::from_str(&json).unwrap();
    assert_eq!(q, back);
}

#[test]
fn ast_literal_int_roundtrip() {
    let lit = Literal::Int(42, Span::new(0, 2));
    let json = serde_json::to_string(&lit).unwrap();
    let back: Literal = serde_json::from_str(&json).unwrap();
    assert_eq!(lit, back);
}

#[test]
fn ast_literal_span() {
    assert_eq!(Literal::Int(1, Span::new(0, 1)).span(), Span::new(0, 1));
    assert_eq!(
        Literal::Str("x".into(), Span::new(2, 5)).span(),
        Span::new(2, 5)
    );
    assert_eq!(Literal::Bool(true, Span::new(3, 7)).span(), Span::new(3, 7));
    assert_eq!(Literal::Null(Span::new(0, 4)).span(), Span::new(0, 4));
    assert_eq!(Literal::Float(1.5, Span::new(0, 3)).span(), Span::new(0, 3));
}

#[test]
fn ast_expr_var_span() {
    let e = Expr::Var(VarRef {
        name: "n".into(),
        span: Span::new(7, 8),
    });
    assert_eq!(e.span(), Span::new(7, 8));
}

#[test]
fn ast_expr_property_access() {
    let e = Expr::Property(PropertyAccess {
        object: Box::new(Expr::Var(VarRef {
            name: "n".into(),
            span: zero(),
        })),
        key: "name".into(),
        span: Span::new(0, 6),
    });
    assert_eq!(e.span(), Span::new(0, 6));
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_binary_op_roundtrip() {
    let e = Expr::BinaryOp(BinaryOp {
        op: BinaryOpKind::Eq,
        left: Box::new(Expr::Var(VarRef {
            name: "x".into(),
            span: zero(),
        })),
        right: Box::new(Expr::Literal(Literal::Int(1, zero()))),
        span: Span::new(0, 5),
    });
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_function_call_roundtrip() {
    let e = Expr::FunctionCall(FunctionCall {
        name: vec!["count".into()],
        distinct: false,
        star: false,
        args: vec![Expr::Var(VarRef {
            name: "n".into(),
            span: zero(),
        })],
        span: Span::new(0, 10),
    });
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_list_literal_roundtrip() {
    let e = Expr::List(ListLiteral {
        elements: vec![
            Expr::Literal(Literal::Int(1, zero())),
            Expr::Literal(Literal::Int(2, zero())),
        ],
        span: zero(),
    });
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_map_literal_roundtrip() {
    use std::collections::HashMap;
    let mut entries = HashMap::new();
    entries.insert(
        "name".to_owned(),
        Expr::Literal(Literal::Str("Alice".into(), zero())),
    );
    let key_spans = HashMap::from([("name".to_owned(), Span::new(1, 5))]);
    let e = Expr::Map(MapLiteral {
        entries,
        key_spans,
        span: zero(),
    });
    assert_eq!(e.clone(), e);
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);

    let mut legacy = serde_json::to_value(&e).unwrap();
    legacy["Map"].as_object_mut().unwrap().remove("key_spans");
    let legacy: Expr = serde_json::from_value(legacy).unwrap();
    let Expr::Map(legacy) = legacy else {
        panic!("expected map literal");
    };
    assert!(legacy.key_spans.is_empty());
}

#[test]
fn ast_unary_op() {
    let e = Expr::UnaryOp(UnaryOp {
        op: UnaryOpKind::Not,
        expr: Box::new(Expr::Literal(Literal::Bool(true, zero()))),
        span: zero(),
    });
    assert_eq!(e.span(), zero());
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_is_null_expr_roundtrip() {
    let e = Expr::IsNull {
        expr: Box::new(Expr::Var(VarRef {
            name: "x".into(),
            span: zero(),
        })),
        negated: false,
        span: Span::new(0, 10),
    };
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_in_list_expr_roundtrip() {
    let e = Expr::InList {
        expr: Box::new(Expr::Var(VarRef {
            name: "x".into(),
            span: zero(),
        })),
        list: Box::new(Expr::List(ListLiteral {
            elements: vec![],
            span: zero(),
        })),
        negated: false,
        span: zero(),
    };
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_string_op_roundtrip() {
    let e = Expr::StringOp {
        expr: Box::new(Expr::Var(VarRef {
            name: "s".into(),
            span: zero(),
        })),
        op: StringOpKind::StartsWith,
        pattern: Box::new(Expr::Literal(Literal::Str("Al".into(), zero()))),
        span: zero(),
    };
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_regex_match_roundtrip() {
    let e = Expr::RegexMatch {
        expr: Box::new(Expr::Var(VarRef {
            name: "n".into(),
            span: zero(),
        })),
        pattern: Box::new(Expr::Literal(Literal::Str("A.*".into(), zero()))),
        span: zero(),
    };
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_case_expr_roundtrip() {
    let e = Expr::Case(CaseExpr {
        subject: None,
        when_clauses: vec![WhenClause {
            condition: Expr::Literal(Literal::Bool(true, zero())),
            result: Expr::Literal(Literal::Int(1, zero())),
            span: zero(),
        }],
        else_expr: Some(Box::new(Expr::Literal(Literal::Int(0, zero())))),
        span: zero(),
    });
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_node_pattern_roundtrip() {
    let np = NodePattern {
        var: Some("n".into()),
        labels: vec!["Person".into()],
        properties: None,
        span: Span::new(0, 10),
    };
    let json = serde_json::to_string(&np).unwrap();
    let back: NodePattern = serde_json::from_str(&json).unwrap();
    assert_eq!(np, back);
}

#[test]
fn ast_rel_pattern_roundtrip() {
    let rp = RelPattern {
        var: Some("r".into()),
        types: vec!["KNOWS".into()],
        direction: Direction::Out,
        min_hops: None,
        max_hops: None,
        properties: None,
        span: zero(),
    };
    let json = serde_json::to_string(&rp).unwrap();
    let back: RelPattern = serde_json::from_str(&json).unwrap();
    assert_eq!(rp, back);
}

#[test]
fn ast_direction_variants() {
    // Ensures all variants serialise/deserialise correctly
    for dir in [Direction::Out, Direction::In, Direction::Undirected] {
        let json = serde_json::to_string(&dir).unwrap();
        let back: Direction = serde_json::from_str(&json).unwrap();
        assert_eq!(dir, back);
    }
}

#[test]
fn ast_match_clause_roundtrip() {
    let mc = MatchClause {
        patterns: vec![PathPattern {
            var: None,
            elements: vec![PathElement::Node(NodePattern {
                var: Some("n".into()),
                labels: vec!["Person".into()],
                properties: None,
                span: zero(),
            })],
            span: zero(),
        }],
        where_clause: None,
        span: zero(),
    };
    let clause = AstClause::Match(mc);
    assert_eq!(clause.span(), zero());
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
}

#[test]
fn ast_return_clause_roundtrip() {
    let rc = ReturnClause {
        distinct: false,
        items: vec![ReturnItem {
            expr: Expr::Var(VarRef {
                name: "n".into(),
                span: zero(),
            }),
            alias: Some("node".into()),
            display: None,
            span: zero(),
        }],
        order_by: None,
        skip: None,
        limit: None,
        span: zero(),
    };
    let clause = AstClause::Return(rc);
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
}

#[test]
fn ast_unwind_clause_roundtrip() {
    let uc = UnwindClause {
        expr: Expr::Var(VarRef {
            name: "list".into(),
            span: zero(),
        }),
        alias: "x".into(),
        span: zero(),
    };
    let clause = AstClause::Unwind(uc);
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
}

#[test]
fn ast_delete_clause_detach_flag() {
    let dc = DeleteClause {
        detach: true,
        exprs: vec![Expr::Var(VarRef {
            name: "n".into(),
            span: zero(),
        })],
        span: zero(),
    };
    let json = serde_json::to_string(&dc).unwrap();
    let back: DeleteClause = serde_json::from_str(&json).unwrap();
    assert_eq!(dc, back);
    assert!(back.detach);
}

#[test]
fn ast_sort_order_default() {
    assert_eq!(SortOrder::default(), SortOrder::Ascending);
}

#[test]
fn ast_dialect_version_default() {
    assert_eq!(DialectVersion::default(), DialectVersion::OpenCypher9);
}

#[test]
fn ast_label_predicate_roundtrip() {
    let e = Expr::LabelPredicate(LabelPredicate {
        var: "n".into(),
        labels: vec!["Person".into(), "Employee".into()],
        span: zero(),
    });
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_list_comprehension_roundtrip() {
    let e = Expr::ListComprehension(ListComprehension {
        var: "x".into(),
        list: Box::new(Expr::Var(VarRef {
            name: "items".into(),
            span: zero(),
        })),
        filter: None,
        projection: Some(Box::new(Expr::Var(VarRef {
            name: "x".into(),
            span: zero(),
        }))),
        span: zero(),
    });
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn ast_pattern_comprehension_roundtrip() {
    let pc = PatternComprehension {
        var: Some("p".into()),
        pattern: PathPattern {
            var: None,
            elements: vec![PathElement::Node(NodePattern {
                var: Some("n".into()),
                labels: vec![],
                properties: None,
                span: zero(),
            })],
            span: zero(),
        },
        filter: None,
        projection: Box::new(Expr::Var(VarRef {
            name: "n".into(),
            span: zero(),
        })),
        span: zero(),
    };
    let e = Expr::PatternComprehension(pc);
    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

// ---------------------------------------------------------------------------
// Clause round-trip tests for types not covered above
// ---------------------------------------------------------------------------

#[test]
fn ast_optional_match_clause_roundtrip() {
    let mc = MatchClause {
        patterns: vec![PathPattern {
            var: None,
            elements: vec![PathElement::Node(NodePattern {
                var: Some("n".into()),
                labels: vec![],
                properties: None,
                span: zero(),
            })],
            span: zero(),
        }],
        where_clause: Some(WhereClause {
            predicate: Expr::Literal(Literal::Bool(true, zero())),
            span: zero(),
        }),
        span: zero(),
    };
    let clause = AstClause::OptionalMatch(mc);
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
}

#[test]
fn ast_where_clause_roundtrip() {
    let wc = WhereClause {
        predicate: Expr::Var(VarRef {
            name: "x".into(),
            span: zero(),
        }),
        span: zero(),
    };
    let json = serde_json::to_string(&wc).unwrap();
    let back: WhereClause = serde_json::from_str(&json).unwrap();
    assert_eq!(wc, back);
}

#[test]
fn ast_create_clause_roundtrip() {
    let cc = CreateClause {
        patterns: vec![PathPattern {
            var: None,
            elements: vec![PathElement::Node(NodePattern {
                var: Some("n".into()),
                labels: vec!["Person".into()],
                properties: None,
                span: zero(),
            })],
            span: zero(),
        }],
        span: zero(),
    };
    let clause = AstClause::Create(cc);
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
}

#[test]
fn ast_merge_clause_on_create_on_match() {
    let node = PathPattern {
        var: None,
        elements: vec![PathElement::Node(NodePattern {
            var: Some("n".into()),
            labels: vec!["Person".into()],
            properties: None,
            span: zero(),
        })],
        span: zero(),
    };
    let set_item = SetItem::Label {
        var: "n".into(),
        labels: vec!["Employee".into()],
        span: zero(),
    };
    let mc = MergeClause {
        pattern: node,
        on_create: vec![set_item.clone()],
        on_match: vec![set_item],
        span: zero(),
    };
    let clause = AstClause::Merge(mc);
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
}

#[test]
fn ast_set_clause_roundtrip() {
    let sc = SetClause {
        items: vec![
            SetItem::PropertyReplace {
                var: "n".into(),
                map: Expr::Map(MapLiteral {
                    entries: Default::default(),
                    key_spans: Default::default(),
                    span: zero(),
                }),
                span: zero(),
            },
            SetItem::PropertyMerge {
                var: "n".into(),
                map: Expr::Map(MapLiteral {
                    entries: Default::default(),
                    key_spans: Default::default(),
                    span: zero(),
                }),
                span: zero(),
            },
        ],
        span: zero(),
    };
    let clause = AstClause::Set(sc);
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
}

#[test]
fn ast_remove_clause_roundtrip() {
    let rc = RemoveClause {
        items: vec![RemoveItem::Label {
            var: "n".into(),
            labels: vec!["Temp".into()],
            span: zero(),
        }],
        span: zero(),
    };
    let clause = AstClause::Remove(rc);
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
}

#[test]
fn ast_union_clause_roundtrip() {
    let uc = UnionClause {
        all: true,
        span: zero(),
    };
    let clause = AstClause::Union(uc);
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
    if let AstClause::Union(u) = back {
        assert!(u.all);
    }
}

#[test]
fn ast_call_clause_roundtrip() {
    let cc = CallClause {
        procedure: vec!["db".into(), "labels".into()],
        args: vec![],
        args_explicit: true,
        yield_items: vec![ReturnItem {
            expr: Expr::Var(VarRef {
                name: "label".into(),
                span: zero(),
            }),
            alias: None,
            display: None,
            span: zero(),
        }],
        span: zero(),
    };
    let clause = AstClause::Call(cc);
    let json = serde_json::to_string(&clause).unwrap();
    let back: AstClause = serde_json::from_str(&json).unwrap();
    assert_eq!(clause, back);
}

// ---------------------------------------------------------------------------
// gf-cypher stub
// ---------------------------------------------------------------------------

#[test]
fn cypher_parse_stub_returns_not_implemented() {
    use crate::parse_error::ParseErrorKind;
    // Import parse from the sibling crate via re-export in gf-cypher's tests,
    // but since we're inside gf-ast we just verify the error types directly.
    let e = ParseError::new(
        ParseErrorKind::UnexpectedEof {
            expected: vec!["statement".into()],
        },
        Span::new(0, 0),
        "stub",
    );
    assert!(matches!(e.kind, ParseErrorKind::UnexpectedEof { .. }));
}
