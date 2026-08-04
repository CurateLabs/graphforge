use crate::lexer::Tok;
use graphforge_ast::{
    BinaryOp, BinaryOpKind, CaseExpr, ExistentialSubquery, ExistentialSubqueryBody, Expr,
    FunctionCall, LabelPredicate, ListComprehension, ListLiteral, Literal, MapLiteral, ParamRef,
    ParseError, ParseErrorKind, PatternComprehension, PatternPredicate, PropertyAccess, Quantifier,
    QuantifierKind, StringOpKind, UnaryOp, UnaryOpKind, VarRef, WhenClause,
};
use graphforge_core::Span;
use std::collections::HashMap;

use super::TokenStream;
use super::patterns::{parse_node_pattern, parse_pattern};

// ---------------------------------------------------------------------------
// Binding powers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum InfixOp {
    Or,
    Xor,
    And,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    RegexMatch,
    IsNull,
    IsNotNull,
    In,
    NotIn,
    StartsWith,
    EndsWith,
    Contains,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Dot,
    Subscript,
    LabelPredicate,
}

fn peek_infix_op(ts: &TokenStream) -> Option<InfixOp> {
    match ts.peek()? {
        Tok::Or => Some(InfixOp::Or),
        Tok::Xor => Some(InfixOp::Xor),
        Tok::And => Some(InfixOp::And),
        Tok::Eq => Some(InfixOp::Eq),
        Tok::Neq => Some(InfixOp::Neq),
        Tok::Lt => Some(InfixOp::Lt),
        Tok::Lte => Some(InfixOp::Lte),
        Tok::Gt => Some(InfixOp::Gt),
        Tok::Gte => Some(InfixOp::Gte),
        Tok::RegexMatch => Some(InfixOp::RegexMatch),
        Tok::IsNull => Some(InfixOp::IsNull),
        Tok::IsNotNull => Some(InfixOp::IsNotNull),
        Tok::In => Some(InfixOp::In),
        Tok::NotIn => Some(InfixOp::NotIn),
        Tok::StartsWith => Some(InfixOp::StartsWith),
        Tok::EndsWith => Some(InfixOp::EndsWith),
        Tok::Contains => Some(InfixOp::Contains),
        Tok::Plus => Some(InfixOp::Add),
        Tok::Minus => Some(InfixOp::Sub),
        Tok::Star => Some(InfixOp::Mul),
        Tok::Slash => Some(InfixOp::Div),
        Tok::Percent => Some(InfixOp::Mod),
        Tok::Caret => Some(InfixOp::Pow),
        Tok::Dot => Some(InfixOp::Dot),
        Tok::LBracket => Some(InfixOp::Subscript),
        Tok::Colon => Some(InfixOp::LabelPredicate),
        _ => None,
    }
}

/// Returns `(left_bp, right_bp)`.
fn infix_binding_power(op: &InfixOp) -> (u8, u8) {
    match op {
        InfixOp::Or => (10, 11),
        InfixOp::Xor => (20, 21),
        InfixOp::And => (30, 31),
        InfixOp::Eq
        | InfixOp::Neq
        | InfixOp::Lt
        | InfixOp::Lte
        | InfixOp::Gt
        | InfixOp::Gte
        | InfixOp::RegexMatch => (40, 41),
        // postfix / non-assoc predicates. openCypher predicates bind tighter
        // than comparison: `false = true IS NULL` is
        // `false = (true IS NULL)`, not `(false = true) IS NULL`.
        InfixOp::IsNull
        | InfixOp::IsNotNull
        | InfixOp::In
        | InfixOp::NotIn
        | InfixOp::StartsWith
        | InfixOp::EndsWith
        | InfixOp::Contains => (45, 46),
        InfixOp::Add | InfixOp::Sub => (50, 51),
        InfixOp::Mul | InfixOp::Div | InfixOp::Mod => (60, 61),
        InfixOp::Pow => (70, 71),
        InfixOp::Dot => (80, 81),
        InfixOp::Subscript => (80, 81),
        InfixOp::LabelPredicate => (80, 81),
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn parse_expr(ts: &mut TokenStream, min_bp: u8) -> Result<Expr, ParseError> {
    let mut lhs = parse_prefix(ts)?;
    loop {
        let Some(op) = peek_infix_op(ts) else { break };
        let (l_bp, r_bp) = infix_binding_power(&op);
        if l_bp < min_bp {
            break;
        }
        lhs = parse_infix(ts, lhs, op, r_bp)?;
    }
    Ok(lhs)
}

// ---------------------------------------------------------------------------
// Prefix (nud)
// ---------------------------------------------------------------------------

fn parse_prefix(ts: &mut TokenStream) -> Result<Expr, ParseError> {
    let start = ts.current_pos();

    match ts.peek().cloned() {
        // --- Literals ---
        Some(Tok::IntLit(n)) => {
            let (l, _, r) = ts.advance().unwrap();
            let span = Span::new(l, r);
            Ok(Expr::Literal(Literal::Int(to_i64(n, span, ts)?, span)))
        }
        Some(Tok::FloatLit(f)) => {
            let (l, _, r) = ts.advance().unwrap();
            Ok(Expr::Literal(Literal::Float(f, Span::new(l, r))))
        }
        Some(Tok::StrLit(s)) => {
            let (l, _, r) = ts.advance().unwrap();
            Ok(Expr::Literal(Literal::Str(s, Span::new(l, r))))
        }
        Some(Tok::True) => {
            let (l, _, r) = ts.advance().unwrap();
            Ok(Expr::Literal(Literal::Bool(true, Span::new(l, r))))
        }
        Some(Tok::False) => {
            let (l, _, r) = ts.advance().unwrap();
            Ok(Expr::Literal(Literal::Bool(false, Span::new(l, r))))
        }
        Some(Tok::Null) => {
            let (l, _, r) = ts.advance().unwrap();
            Ok(Expr::Literal(Literal::Null(Span::new(l, r))))
        }

        // --- Parameter ---
        Some(Tok::Param(name)) => {
            let (l, _, r) = ts.advance().unwrap();
            Ok(Expr::Param(ParamRef {
                name,
                span: Span::new(l, r),
            }))
        }

        // --- Identifier: variable, function call, or namespaced function call ---
        Some(Tok::Ident(name)) => {
            ts.advance();
            if ts.eat_if(&Tok::LParen) {
                parse_function_call_args(ts, vec![name], false, start)
            } else if let Some(segments) = namespaced_call_segments(ts, &name) {
                // `date.truncate(…)`, `datetime.fromepoch(…)`: a dotted name
                // followed by `(` is a qualified function call (the whole path is
                // the name). A dotted name NOT followed by `(` stays a property
                // access, handled by the infix `.` operator below.
                for _ in 1..segments.len() {
                    ts.advance(); // `.`
                    ts.advance(); // segment ident
                }
                ts.eat(&Tok::LParen)?;
                parse_function_call_args(ts, segments, false, start)
            } else {
                Ok(Expr::Var(VarRef {
                    name,
                    span: ts.span_from(start),
                }))
            }
        }

        // --- Quantifier predicates: all/any/none/single(var IN list WHERE pred) ---
        Some(Tok::All | Tok::Any | Tok::None | Tok::Single) => {
            let kind = match ts.peek().unwrap() {
                Tok::All => QuantifierKind::All,
                Tok::Any => QuantifierKind::Any,
                Tok::None => QuantifierKind::None,
                _ => QuantifierKind::Single,
            };
            let name = tok_keyword_name(ts.peek().unwrap());
            ts.advance();
            ts.eat(&Tok::LParen)?;
            // The quantifier form is `var IN …`; otherwise fall back to a plain
            // function call (preserves any non-quantifier use of the name).
            if matches!(ts.peek(), Some(Tok::Ident(_))) && matches!(ts.peek_n(1), Some(Tok::In)) {
                parse_quantifier(ts, kind, start)
            } else {
                parse_function_call_args(ts, vec![name], false, start)
            }
        }

        // --- Block-form existential subquery or exists(...) function ---
        Some(Tok::Exists) => {
            ts.advance();
            if ts.eat_if(&Tok::LBrace) {
                let body = if matches!(
                    ts.peek(),
                    Some(
                        Tok::Match
                            | Tok::Optional
                            | Tok::With
                            | Tok::Return
                            | Tok::Unwind
                            | Tok::Create
                            | Tok::Merge
                            | Tok::Set
                            | Tok::Remove
                            | Tok::Delete
                            | Tok::Detach
                            | Tok::Call
                    )
                ) {
                    ExistentialSubqueryBody::Full(Box::new(super::clauses::parse_subquery(ts)?))
                } else {
                    let pattern = parse_pattern(ts)?;
                    let filter = if ts.eat_if(&Tok::Where) {
                        Some(Box::new(parse_expr(ts, 0)?))
                    } else {
                        None
                    };
                    ExistentialSubqueryBody::Simple { pattern, filter }
                };
                ts.eat(&Tok::RBrace)?;
                Ok(Expr::ExistentialSubquery(ExistentialSubquery {
                    body,
                    span: ts.span_from(start),
                }))
            } else {
                ts.eat(&Tok::LParen)?;
                parse_function_call_args(ts, vec!["exists".into()], false, start)
            }
        }

        // --- Keyword-named functions ---
        Some(
            Tok::Count
            | Tok::Filter
            | Tok::Extract
            | Tok::Reduce
            | Tok::ShortestPath
            | Tok::AllShortestPaths,
        ) => {
            let name = tok_keyword_name(ts.peek().unwrap());
            ts.advance();
            if ts.eat_if(&Tok::LParen) {
                parse_function_call_args(ts, vec![name], false, start)
            } else {
                Ok(Expr::Var(VarRef {
                    name,
                    span: ts.span_from(start),
                }))
            }
        }

        // --- NOT prefix ---
        Some(Tok::Not) => {
            ts.advance();
            let expr = parse_expr(ts, 35)?;
            Ok(Expr::UnaryOp(UnaryOp {
                op: UnaryOpKind::Not,
                expr: Box::new(expr),
                span: ts.span_from(start),
            }))
        }

        // --- Unary minus ---
        Some(Tok::Minus) => {
            ts.advance();
            if let Some(Tok::IntLit(n)) = ts.peek().cloned() {
                let (l, _, r) = ts.advance().unwrap();
                let span = Span::new(start, r);
                return Ok(Expr::Literal(Literal::Int(
                    negate_i64_magnitude(n, Span::new(l, r), ts)?,
                    span,
                )));
            }
            let expr = parse_expr(ts, 75)?;
            Ok(Expr::UnaryOp(UnaryOp {
                op: UnaryOpKind::Neg,
                expr: Box::new(expr),
                span: ts.span_from(start),
            }))
        }

        // --- Parenthesized expression ---
        Some(Tok::LParen) => {
            if let Some(pattern_predicate) = try_parse_pattern_predicate(ts, start)? {
                return Ok(pattern_predicate);
            }
            ts.advance();
            let inner = parse_expr(ts, 0)?;
            ts.eat(&Tok::RParen)?;
            Ok(Expr::Parenthesized {
                inner: Box::new(inner),
                span: ts.span_from(start),
            })
        }

        // --- List literal or list comprehension ---
        Some(Tok::LBracket) => {
            ts.advance();
            parse_list_or_comprehension(ts, start)
        }

        // --- Map literal ---
        Some(Tok::LBrace) => {
            ts.advance();
            parse_map_literal(ts, start)
        }

        // --- CASE expression ---
        Some(Tok::Case) => {
            ts.advance();
            parse_case_expr(ts, start)
        }

        // --- Unexpected ---
        Some(_) => Err(ts.err("expected expression")),
        None => Err(ParseError::new(
            ParseErrorKind::UnexpectedEof {
                expected: vec!["expression".into()],
            },
            ts.current_span(),
            "expected expression, found end of input",
        )),
    }
}

fn try_parse_pattern_predicate(
    ts: &mut TokenStream,
    start: usize,
) -> Result<Option<Expr>, ParseError> {
    let mut probe = ts.clone();
    if parse_node_pattern(&mut probe).is_err() || !is_relationship_start(probe.peek()) {
        return Ok(None);
    }

    let mut pattern_probe = ts.clone();
    let pattern = parse_pattern(&mut pattern_probe)?;
    *ts = pattern_probe;
    Ok(Some(Expr::PatternPredicate(PatternPredicate {
        pattern,
        span: ts.span_from(start),
    })))
}

fn is_relationship_start(tok: Option<&Tok>) -> bool {
    matches!(tok, Some(Tok::RelOpen | Tok::Minus | Tok::LeftArrow))
}

// ---------------------------------------------------------------------------
// Infix / postfix (led)
// ---------------------------------------------------------------------------

fn parse_infix(ts: &mut TokenStream, lhs: Expr, op: InfixOp, r_bp: u8) -> Result<Expr, ParseError> {
    let start = lhs.span().start as usize;

    match op {
        // --- Binary logical / arithmetic / comparison ---
        InfixOp::Or
        | InfixOp::Xor
        | InfixOp::And
        | InfixOp::Eq
        | InfixOp::Neq
        | InfixOp::Lt
        | InfixOp::Lte
        | InfixOp::Gt
        | InfixOp::Gte
        | InfixOp::Add
        | InfixOp::Sub
        | InfixOp::Mul
        | InfixOp::Div
        | InfixOp::Mod
        | InfixOp::Pow => {
            ts.advance(); // consume the operator token
            let rhs = parse_expr(ts, r_bp)?;
            let span = Span::new(start, rhs.span().end as usize);
            let comparison_left = comparison_chain_tail(&lhs, &op);
            let binary = Expr::BinaryOp(BinaryOp {
                op: infix_op_to_binary_kind(&op),
                left: Box::new(
                    comparison_left
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| lhs.clone()),
                ),
                right: Box::new(rhs),
                span,
            });
            if comparison_left.is_some() {
                Ok(Expr::BinaryOp(BinaryOp {
                    op: BinaryOpKind::And,
                    left: Box::new(lhs),
                    right: Box::new(binary),
                    span,
                }))
            } else {
                Ok(binary)
            }
        }

        // --- Regex match ---
        InfixOp::RegexMatch => {
            ts.advance();
            let rhs = parse_expr(ts, r_bp)?;
            let span = Span::new(start, rhs.span().end as usize);
            Ok(Expr::RegexMatch {
                expr: Box::new(lhs),
                pattern: Box::new(rhs),
                span,
            })
        }

        // --- IS NULL / IS NOT NULL ---
        InfixOp::IsNull => {
            ts.advance();
            let span = Span::new(start, ts.current_pos());
            Ok(Expr::IsNull {
                expr: Box::new(lhs),
                negated: false,
                span,
            })
        }
        InfixOp::IsNotNull => {
            ts.advance();
            let span = Span::new(start, ts.current_pos());
            Ok(Expr::IsNull {
                expr: Box::new(lhs),
                negated: true,
                span,
            })
        }

        // --- IN / NOT IN ---
        InfixOp::In => {
            ts.advance();
            let rhs = parse_expr(ts, r_bp)?;
            let span = Span::new(start, rhs.span().end as usize);
            Ok(Expr::InList {
                expr: Box::new(lhs),
                list: Box::new(rhs),
                negated: false,
                span,
            })
        }
        InfixOp::NotIn => {
            ts.advance();
            let rhs = parse_expr(ts, r_bp)?;
            let span = Span::new(start, rhs.span().end as usize);
            Ok(Expr::InList {
                expr: Box::new(lhs),
                list: Box::new(rhs),
                negated: true,
                span,
            })
        }

        // --- String predicates ---
        InfixOp::StartsWith | InfixOp::EndsWith | InfixOp::Contains => {
            let string_op = match op {
                InfixOp::StartsWith => StringOpKind::StartsWith,
                InfixOp::EndsWith => StringOpKind::EndsWith,
                InfixOp::Contains => StringOpKind::Contains,
                _ => unreachable!(),
            };
            ts.advance();
            let rhs = parse_expr(ts, r_bp)?;
            let span = Span::new(start, rhs.span().end as usize);
            Ok(Expr::StringOp {
                expr: Box::new(lhs),
                op: string_op,
                pattern: Box::new(rhs),
                span,
            })
        }

        // --- Property access ---
        InfixOp::Dot => {
            ts.advance();
            let key_start = ts.current_pos();
            let key = eat_ident(ts)?;
            let span = Span::new(start, ts.current_pos());
            // If the identifier is immediately followed by `(`, this is a
            // method/function call — not supported in openCypher core; treat as
            // property access (callers can wrap if needed).
            let _ = key_start;
            Ok(Expr::Property(PropertyAccess {
                object: Box::new(lhs),
                key,
                span,
            }))
        }

        // --- Label/type predicate expression ---
        InfixOp::LabelPredicate => {
            ts.advance();
            let label = eat_ident(ts)?;
            let span = Span::new(start, ts.current_pos());
            match lhs {
                Expr::Var(VarRef { name, .. }) => Ok(Expr::LabelPredicate(LabelPredicate {
                    var: name,
                    labels: vec![label],
                    span,
                })),
                Expr::LabelPredicate(mut pred) => {
                    pred.labels.push(label);
                    pred.span = span;
                    Ok(Expr::LabelPredicate(pred))
                }
                other => Err(ParseError::new(
                    ParseErrorKind::UnexpectedToken {
                        expected: vec!["variable before `:Label` predicate".into()],
                        found: format!("{other:?}"),
                    },
                    span,
                    "`:Label` predicates require a variable",
                )),
            }
        }

        // --- Subscript / slice ---
        InfixOp::Subscript => {
            ts.advance(); // consume `[`
            // Slice: [lo..hi], [..hi], [lo..]
            // Subscript: [expr]
            // Distinguish: if the first non-trivial thing is `..` it's a slice
            // with no lower bound.
            if ts.eat_if(&Tok::DotDot) {
                // [..hi]
                let hi = parse_expr(ts, 0)?;
                ts.eat(&Tok::RBracket)?;
                let span = Span::new(start, ts.current_pos());
                // Encode as BinaryOp Slice — use a special representation via
                // a FunctionCall to internal `slice` until AST gets a Slice node.
                // For now represent as Subscript via InList trick — actually the
                // AST has no dedicated Slice node. We model `a[lo..hi]` as a
                // FunctionCall to a synthetic `_slice` node. Users of the AST
                // (the IR binder) will interpret it.
                return Ok(Expr::FunctionCall(FunctionCall {
                    name: vec!["_slice_from_start".into()],
                    distinct: false,
                    star: false,
                    args: vec![lhs, hi],
                    span,
                }));
            }
            let idx = parse_expr(ts, 0)?;
            if ts.eat_if(&Tok::DotDot) {
                // [lo..hi] or [lo..]
                let (name, args) = if ts.at(&Tok::RBracket) {
                    ("_slice_to_end", vec![lhs, idx])
                } else {
                    ("_slice", vec![lhs, idx, parse_expr(ts, 0)?])
                };
                ts.eat(&Tok::RBracket)?;
                let span = Span::new(start, ts.current_pos());
                return Ok(Expr::FunctionCall(FunctionCall {
                    name: vec![name.into()],
                    distinct: false,
                    star: false,
                    args,
                    span,
                }));
            }
            ts.eat(&Tok::RBracket)?;
            let span = Span::new(start, ts.current_pos());
            Ok(Expr::FunctionCall(FunctionCall {
                name: vec!["_subscript".into()],
                distinct: false,
                star: false,
                args: vec![lhs, idx],
                span,
            }))
        }
    }
}

fn comparison_chain_tail(lhs: &Expr, next: &InfixOp) -> Option<Expr> {
    if !matches!(
        next,
        InfixOp::Eq | InfixOp::Neq | InfixOp::Lt | InfixOp::Lte | InfixOp::Gt | InfixOp::Gte
    ) {
        return None;
    }
    let Expr::BinaryOp(binary) = lhs else {
        return None;
    };
    match binary.op {
        BinaryOpKind::Eq
        | BinaryOpKind::Neq
        | BinaryOpKind::Lt
        | BinaryOpKind::Lte
        | BinaryOpKind::Gt
        | BinaryOpKind::Gte => Some((*binary.right).clone()),
        BinaryOpKind::And => comparison_chain_tail(&binary.right, next),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Function call argument list
// ---------------------------------------------------------------------------

/// Detect a qualified function-call name at the cursor (just past the first
/// identifier `first`): one or more `.<ident>` segments terminated by `(`.
/// Returns the full dotted path when it is a call (e.g. `date.truncate`,
/// `datetime.fromepoch`), or `None` for a plain property access (`a.b`), which
/// the infix `.` operator handles. Peeks only; the caller consumes.
fn namespaced_call_segments(ts: &TokenStream, first: &str) -> Option<Vec<String>> {
    let mut segments = vec![first.to_string()];
    let mut i = 0;
    while let (Some(Tok::Dot), Some(Tok::Ident(seg))) = (ts.peek_n(2 * i), ts.peek_n(2 * i + 1)) {
        segments.push(seg.clone());
        i += 1;
    }
    (i > 0 && matches!(ts.peek_n(2 * i), Some(Tok::LParen))).then_some(segments)
}

fn parse_function_call_args(
    ts: &mut TokenStream,
    name: Vec<String>,
    _pre_distinct: bool,
    start: usize,
) -> Result<Expr, ParseError> {
    // COUNT(*) special case
    if ts.eat_if(&Tok::Star) {
        ts.eat(&Tok::RParen)?;
        return Ok(Expr::FunctionCall(FunctionCall {
            name,
            distinct: false,
            star: true,
            args: vec![],
            span: ts.span_from(start),
        }));
    }

    let distinct = ts.eat_if(&Tok::Distinct);
    let mut args = Vec::new();
    if !ts.at(&Tok::RParen) {
        args.push(parse_expr(ts, 0)?);
        while ts.eat_if(&Tok::Comma) {
            args.push(parse_expr(ts, 0)?);
        }
    }
    ts.eat(&Tok::RParen)?;
    Ok(Expr::FunctionCall(FunctionCall {
        name,
        distinct,
        star: false,
        args,
        span: ts.span_from(start),
    }))
}

// ---------------------------------------------------------------------------
// List literal or list comprehension
// ---------------------------------------------------------------------------

fn parse_list_or_comprehension(ts: &mut TokenStream, start: usize) -> Result<Expr, ParseError> {
    // Empty list
    if ts.eat_if(&Tok::RBracket) {
        return Ok(Expr::List(ListLiteral {
            elements: vec![],
            span: ts.span_from(start),
        }));
    }

    if let Some(pattern_comprehension) = try_parse_pattern_comprehension(ts, start)? {
        return Ok(pattern_comprehension);
    }

    // List comprehension: [var IN list_expr WHERE? filter | projection]
    // Disambiguate: Ident at pos 0 AND In at pos 1
    if matches!(ts.peek(), Some(Tok::Ident(_))) && matches!(ts.peek_n(1), Some(Tok::In)) {
        let var = match ts.advance() {
            Some((_, Tok::Ident(name), _)) => name,
            _ => unreachable!(),
        };
        ts.eat(&Tok::In)?;
        let list = parse_expr(ts, 0)?;
        let filter = if ts.eat_if(&Tok::Where) {
            Some(Box::new(parse_expr(ts, 0)?))
        } else {
            None
        };
        let projection = if ts.eat_if(&Tok::Pipe) {
            Some(Box::new(parse_expr(ts, 0)?))
        } else {
            None
        };
        ts.eat(&Tok::RBracket)?;
        return Ok(Expr::ListComprehension(ListComprehension {
            var,
            list: Box::new(list),
            filter,
            projection,
            span: ts.span_from(start),
        }));
    }

    // List literal
    let mut elements = vec![parse_expr(ts, 0)?];
    while ts.eat_if(&Tok::Comma) {
        elements.push(parse_expr(ts, 0)?);
    }
    ts.eat(&Tok::RBracket)?;
    Ok(Expr::List(ListLiteral {
        elements,
        span: ts.span_from(start),
    }))
}

fn try_parse_pattern_comprehension(
    ts: &mut TokenStream,
    start: usize,
) -> Result<Option<Expr>, ParseError> {
    let mut shape_probe = ts.clone();
    if matches!(shape_probe.peek(), Some(Tok::Ident(_))) && shape_probe.peek_n(1) == Some(&Tok::Eq)
    {
        shape_probe.advance();
        shape_probe.advance();
    }
    if parse_node_pattern(&mut shape_probe).is_err() || !is_relationship_start(shape_probe.peek()) {
        return Ok(None);
    }

    let mut probe = ts.clone();
    let mut pattern = parse_pattern(&mut probe)?;
    let var = pattern.var.take();
    let filter = if probe.eat_if(&Tok::Where) {
        Some(Box::new(parse_expr(&mut probe, 0)?))
    } else {
        None
    };
    probe.eat(&Tok::Pipe)?;
    let projection = Box::new(parse_expr(&mut probe, 0)?);
    probe.eat(&Tok::RBracket)?;
    *ts = probe;

    Ok(Some(Expr::PatternComprehension(PatternComprehension {
        var,
        pattern,
        filter,
        projection,
        span: ts.span_from(start),
    })))
}

/// Parse a quantifier predicate body `var IN list WHERE pred )` (the keyword and
/// `(` are already consumed). Modeled on the list-comprehension production.
fn parse_quantifier(
    ts: &mut TokenStream,
    kind: QuantifierKind,
    start: usize,
) -> Result<Expr, ParseError> {
    let var = match ts.advance() {
        Some((_, Tok::Ident(name), _)) => name,
        _ => unreachable!("dispatch checked `Ident IN`"),
    };
    ts.eat(&Tok::In)?;
    let list = parse_expr(ts, 0)?;
    ts.eat(&Tok::Where)?;
    let predicate = parse_expr(ts, 0)?;
    ts.eat(&Tok::RParen)?;
    Ok(Expr::Quantifier(Quantifier {
        kind,
        var,
        list: Box::new(list),
        predicate: Box::new(predicate),
        span: ts.span_from(start),
    }))
}

// ---------------------------------------------------------------------------
// Map literal  { key: expr, ... }
// ---------------------------------------------------------------------------

fn parse_map_literal(ts: &mut TokenStream, start: usize) -> Result<Expr, ParseError> {
    let mut entries = HashMap::new();
    let mut key_spans = HashMap::new();
    if !ts.at(&Tok::RBrace) {
        loop {
            let (key, key_span) = eat_ident_with_span(ts)?;
            ts.eat(&Tok::Colon)?;
            let val = parse_expr(ts, 0)?;
            if entries.insert(key.clone(), val).is_some() {
                return Err(ts.err(format!("duplicate key '{key}' in map literal")));
            }
            key_spans.insert(key, key_span);
            if !ts.eat_if(&Tok::Comma) {
                break;
            }
        }
    }
    ts.eat(&Tok::RBrace)?;
    Ok(Expr::Map(MapLiteral {
        entries,
        key_spans,
        span: ts.span_from(start),
    }))
}

// ---------------------------------------------------------------------------
// CASE expression
// ---------------------------------------------------------------------------

fn parse_case_expr(ts: &mut TokenStream, start: usize) -> Result<Expr, ParseError> {
    // Simple CASE: CASE expr WHEN val THEN result ... END
    // Searched CASE: CASE WHEN pred THEN result ... END
    // Distinguish: if next token is WHEN it's searched, else it's simple.
    let subject = if ts.at(&Tok::When) {
        None
    } else {
        Some(Box::new(parse_expr(ts, 0)?))
    };

    let mut when_clauses = Vec::new();
    while ts.eat_if(&Tok::When) {
        let when_start = ts.current_pos();
        let condition = parse_expr(ts, 0)?;
        ts.eat(&Tok::Then)?;
        let result = parse_expr(ts, 0)?;
        when_clauses.push(WhenClause {
            condition,
            result,
            span: ts.span_from(when_start),
        });
    }

    let else_expr = if ts.eat_if(&Tok::Else) {
        Some(Box::new(parse_expr(ts, 0)?))
    } else {
        None
    };

    ts.eat(&Tok::End)?;

    Ok(Expr::Case(CaseExpr {
        subject,
        when_clauses,
        else_expr,
        span: ts.span_from(start),
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn eat_ident(ts: &mut TokenStream) -> Result<String, ParseError> {
    eat_ident_with_span(ts).map(|(name, _)| name)
}

fn eat_ident_with_span(ts: &mut TokenStream) -> Result<(String, Span), ParseError> {
    // Identifiers can also be keyword tokens in property key position
    let token = ts.advance();
    match token {
        Some((l, Tok::Ident(name), r)) => Ok((name, Span::new(l, r))),
        // Allow keywords as property keys (common in Cypher: n.end, n.type, etc.)
        Some((l, tok, r)) => {
            if let Some(kw) = tok_as_keyword_str(&tok) {
                let raw = ts.text(Span::new(l, r));
                if raw.is_empty() {
                    Ok((kw.to_ascii_lowercase(), Span::new(l, r)))
                } else {
                    Ok((raw.to_owned(), Span::new(l, r)))
                }
            } else {
                Err(ts.err_at(
                    Span::new(l, r),
                    ParseErrorKind::UnexpectedToken {
                        found: format!("{tok:?}"),
                        expected: vec!["identifier".into()],
                    },
                    "expected identifier",
                ))
            }
        }
        None => Err(ts.err("expected identifier, found end of input")),
    }
}

fn infix_op_to_binary_kind(op: &InfixOp) -> BinaryOpKind {
    match op {
        InfixOp::Or => BinaryOpKind::Or,
        InfixOp::Xor => BinaryOpKind::Xor,
        InfixOp::And => BinaryOpKind::And,
        InfixOp::Eq => BinaryOpKind::Eq,
        InfixOp::Neq => BinaryOpKind::Neq,
        InfixOp::Lt => BinaryOpKind::Lt,
        InfixOp::Lte => BinaryOpKind::Lte,
        InfixOp::Gt => BinaryOpKind::Gt,
        InfixOp::Gte => BinaryOpKind::Gte,
        InfixOp::Add => BinaryOpKind::Add,
        InfixOp::Sub => BinaryOpKind::Sub,
        InfixOp::Mul => BinaryOpKind::Mul,
        InfixOp::Div => BinaryOpKind::Div,
        InfixOp::Mod => BinaryOpKind::Mod,
        InfixOp::Pow => BinaryOpKind::Pow,
        _ => unreachable!("not a binary op kind"),
    }
}

fn to_i64(n: i128, span: Span, ts: &TokenStream<'_>) -> Result<i64, ParseError> {
    i64::try_from(n).map_err(|_| {
        ts.err_at(
            span,
            ParseErrorKind::InvalidNumericLiteral,
            format!("integer literal out of range for i64: {n}"),
        )
    })
}

fn negate_i64_magnitude(n: i128, span: Span, ts: &TokenStream<'_>) -> Result<i64, ParseError> {
    let min_magnitude = (i64::MAX as i128) + 1;
    if n == min_magnitude {
        Ok(i64::MIN)
    } else {
        let positive = to_i64(n, span, ts)?;
        Ok(-positive)
    }
}

fn tok_keyword_name(tok: &Tok) -> String {
    match tok {
        Tok::Count => "count".into(),
        Tok::Exists => "exists".into(),
        Tok::All => "all".into(),
        Tok::Any => "any".into(),
        Tok::None => "none".into(),
        Tok::Single => "single".into(),
        Tok::Filter => "filter".into(),
        Tok::Extract => "extract".into(),
        Tok::Reduce => "reduce".into(),
        Tok::ShortestPath => "shortestPath".into(),
        Tok::AllShortestPaths => "allShortestPaths".into(),
        _ => unreachable!(),
    }
}

fn tok_as_keyword_str(tok: &Tok) -> Option<&'static str> {
    match tok {
        Tok::Match => Some("match"),
        Tok::Return => Some("return"),
        Tok::Where => Some("where"),
        Tok::With => Some("with"),
        Tok::As => Some("as"),
        Tok::Distinct => Some("distinct"),
        Tok::Union => Some("union"),
        Tok::All => Some("all"),
        Tok::Create => Some("create"),
        Tok::Merge => Some("merge"),
        Tok::On => Some("on"),
        Tok::Set => Some("set"),
        Tok::Remove => Some("remove"),
        Tok::Delete => Some("delete"),
        Tok::Detach => Some("detach"),
        Tok::Call => Some("call"),
        Tok::Yield => Some("yield"),
        Tok::Unwind => Some("unwind"),
        Tok::Order => Some("order"),
        Tok::By => Some("by"),
        Tok::Skip => Some("skip"),
        Tok::Limit => Some("limit"),
        Tok::Is => Some("is"),
        Tok::In => Some("in"),
        Tok::Starts => Some("starts"),
        Tok::Ends => Some("ends"),
        Tok::Contains => Some("contains"),
        Tok::Case => Some("case"),
        Tok::When => Some("when"),
        Tok::Then => Some("then"),
        Tok::Else => Some("else"),
        Tok::End => Some("end"),
        Tok::True => Some("true"),
        Tok::False => Some("false"),
        Tok::Null => Some("null"),
        Tok::Count => Some("count"),
        Tok::Exists => Some("exists"),
        Tok::Not => Some("not"),
        Tok::And => Some("and"),
        Tok::Or => Some("or"),
        Tok::Xor => Some("xor"),
        Tok::Optional => Some("optional"),
        Tok::Any => Some("any"),
        Tok::None => Some("none"),
        Tok::Single => Some("single"),
        Tok::Filter => Some("filter"),
        Tok::Extract => Some("extract"),
        Tok::Reduce => Some("reduce"),
        Tok::ShortestPath => Some("shortestPath"),
        Tok::AllShortestPaths => Some("allShortestPaths"),
        Tok::Asc => Some("asc"),
        Tok::Desc => Some("desc"),
        Tok::Ascending => Some("ascending"),
        Tok::Descending => Some("descending"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::TokenStream;

    fn expr(input: &str) -> Expr {
        let mut ts = TokenStream::new(input).expect("lex failed");
        parse_expr(&mut ts, 0).expect("parse failed")
    }

    // --- Literals ---

    #[test]
    fn integer_literal() {
        assert!(matches!(expr("42"), Expr::Literal(Literal::Int(42, _))));
    }

    #[test]
    fn float_literal() {
        assert!(matches!(expr("3.14"), Expr::Literal(Literal::Float(_, _))));
    }

    #[test]
    fn leading_dot_float_literal() {
        assert!(matches!(
            expr(".1e-5"),
            Expr::Literal(Literal::Float(f, _)) if (f - 0.000001).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn signed_min_integer_literal() {
        assert!(matches!(
            expr("-9223372036854775808"),
            Expr::Literal(Literal::Int(i64::MIN, _))
        ));
        assert!(matches!(
            expr("-0x8000000000000000"),
            Expr::Literal(Literal::Int(i64::MIN, _))
        ));
        assert!(matches!(
            expr("-0o1000000000000000000000"),
            Expr::Literal(Literal::Int(i64::MIN, _))
        ));
    }

    #[test]
    fn string_literal() {
        assert!(matches!(
            expr("'hello'"),
            Expr::Literal(Literal::Str(ref s, _)) if s == "hello"
        ));
    }

    #[test]
    fn bool_true() {
        assert!(matches!(
            expr("true"),
            Expr::Literal(Literal::Bool(true, _))
        ));
    }

    #[test]
    fn bool_false() {
        assert!(matches!(
            expr("false"),
            Expr::Literal(Literal::Bool(false, _))
        ));
    }

    #[test]
    fn null_literal() {
        assert!(matches!(expr("null"), Expr::Literal(Literal::Null(_))));
    }

    #[test]
    fn param() {
        assert!(matches!(
            expr("$name"),
            Expr::Param(ParamRef { ref name, .. }) if name == "name"
        ));
    }

    // --- Variable ---

    #[test]
    fn variable() {
        assert!(matches!(
            expr("n"),
            Expr::Var(VarRef { ref name, .. }) if name == "n"
        ));
    }

    // --- Arithmetic precedence ---

    #[test]
    fn left_assoc_add() {
        // 1 + 2 + 3 => (1 + 2) + 3
        let e = expr("1 + 2 + 3");
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::Add,
            left,
            ..
        }) = e
        else {
            panic!("expected BinaryOp Add");
        };
        assert!(matches!(
            *left,
            Expr::BinaryOp(BinaryOp {
                op: BinaryOpKind::Add,
                ..
            })
        ));
    }

    #[test]
    fn left_assoc_pow_matches_tck_precedence() {
        // 2 ^ 3 ^ 2 => (2 ^ 3) ^ 2
        let e = expr("2 ^ 3 ^ 2");
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::Pow,
            left,
            ..
        }) = e
        else {
            panic!("expected BinaryOp Pow");
        };
        assert!(matches!(
            *left,
            Expr::BinaryOp(BinaryOp {
                op: BinaryOpKind::Pow,
                ..
            })
        ));
    }

    #[test]
    fn null_predicate_binds_tighter_than_comparison() {
        let e = expr("false = true IS NULL");
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::Eq,
            right,
            ..
        }) = e
        else {
            panic!("expected equality at top level");
        };
        assert!(matches!(*right, Expr::IsNull { negated: false, .. }));
    }

    #[test]
    fn list_predicate_binds_tighter_than_comparison() {
        let e = expr("false = true IN [true, false]");
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::Eq,
            right,
            ..
        }) = e
        else {
            panic!("expected equality at top level");
        };
        assert!(matches!(*right, Expr::InList { negated: false, .. }));
    }

    #[test]
    fn list_predicate_binds_tighter_than_not() {
        let e = expr("NOT a IN b");
        let Expr::UnaryOp(UnaryOp {
            op: UnaryOpKind::Not,
            expr,
            ..
        }) = e
        else {
            panic!("expected NOT at top level");
        };
        assert!(matches!(*expr, Expr::InList { negated: false, .. }));
    }

    #[test]
    fn list_predicate_binds_tighter_than_boolean_operator() {
        let e = expr("a OR b IN c");
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::Or,
            right,
            ..
        }) = e
        else {
            panic!("expected OR at top level");
        };
        assert!(matches!(*right, Expr::InList { negated: false, .. }));
    }

    #[test]
    fn string_predicate_binds_tighter_than_boolean_operator() {
        let e = expr("true OR null STARTS WITH 'abc'");
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::Or,
            right,
            ..
        }) = e
        else {
            panic!("expected OR at top level");
        };
        assert!(matches!(
            *right,
            Expr::StringOp {
                op: StringOpKind::StartsWith,
                ..
            }
        ));
    }

    #[test]
    fn mul_before_add() {
        // 1 + 2 * 3 => 1 + (2 * 3)
        let e = expr("1 + 2 * 3");
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::Add,
            right,
            ..
        }) = e
        else {
            panic!("expected BinaryOp Add at top level");
        };
        assert!(matches!(
            *right,
            Expr::BinaryOp(BinaryOp {
                op: BinaryOpKind::Mul,
                ..
            })
        ));
    }

    // --- Comparison ---

    #[test]
    fn comparison_eq() {
        let e = expr("n.age = 30");
        assert!(matches!(
            e,
            Expr::BinaryOp(BinaryOp {
                op: BinaryOpKind::Eq,
                ..
            })
        ));
    }

    #[test]
    fn chained_comparisons_become_adjacent_conjunctions() {
        let e = expr("1 < n.num <= 3");
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::And,
            left,
            right,
            ..
        }) = e
        else {
            panic!("expected comparison conjunction");
        };
        assert!(matches!(
            *left,
            Expr::BinaryOp(BinaryOp {
                op: BinaryOpKind::Lt,
                ..
            })
        ));
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::Lte,
            left: second_left,
            ..
        }) = *right
        else {
            panic!("expected upper-bound comparison");
        };
        assert!(matches!(*second_left, Expr::Property(_)));
    }

    // --- Logical ---

    #[test]
    fn logical_not_and_or() {
        // NOT a AND b OR c  =>  (NOT a AND b) OR c  (due to precedence)
        let e = expr("NOT a AND b OR c");
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::Or,
            left,
            ..
        }) = e
        else {
            panic!("expected OR at top level");
        };
        let Expr::BinaryOp(BinaryOp {
            op: BinaryOpKind::And,
            left: not_a,
            ..
        }) = *left
        else {
            panic!("expected AND");
        };
        assert!(matches!(
            *not_a,
            Expr::UnaryOp(UnaryOp {
                op: UnaryOpKind::Not,
                ..
            })
        ));
    }

    #[test]
    fn prefix_not() {
        let e = expr("NOT true");
        assert!(matches!(
            e,
            Expr::UnaryOp(UnaryOp {
                op: UnaryOpKind::Not,
                ..
            })
        ));
    }

    #[test]
    fn unary_neg() {
        let e = expr("-x");
        assert!(matches!(
            e,
            Expr::UnaryOp(UnaryOp {
                op: UnaryOpKind::Neg,
                ..
            })
        ));
    }

    // --- Compound predicate tokens ---

    #[test]
    fn is_null() {
        let e = expr("n.x IS NULL");
        assert!(matches!(e, Expr::IsNull { negated: false, .. }));
    }

    #[test]
    fn is_not_null() {
        let e = expr("n.x IS NOT NULL");
        assert!(matches!(e, Expr::IsNull { negated: true, .. }));
    }

    #[test]
    fn in_list() {
        let e = expr("n.x IN [1, 2, 3]");
        assert!(matches!(e, Expr::InList { negated: false, .. }));
    }

    #[test]
    fn not_in_list() {
        let e = expr("n.x NOT IN [1, 2]");
        assert!(matches!(e, Expr::InList { negated: true, .. }));
    }

    #[test]
    fn starts_with() {
        let e = expr("n.name STARTS WITH 'Al'");
        assert!(matches!(
            e,
            Expr::StringOp {
                op: StringOpKind::StartsWith,
                ..
            }
        ));
    }

    #[test]
    fn ends_with() {
        let e = expr("n.name ENDS WITH 'ce'");
        assert!(matches!(
            e,
            Expr::StringOp {
                op: StringOpKind::EndsWith,
                ..
            }
        ));
    }

    #[test]
    fn contains() {
        let e = expr("n.name CONTAINS 'li'");
        assert!(matches!(
            e,
            Expr::StringOp {
                op: StringOpKind::Contains,
                ..
            }
        ));
    }

    // --- Property access and subscript ---

    #[test]
    fn property_access() {
        let e = expr("n.name");
        assert!(matches!(
            e,
            Expr::Property(PropertyAccess { ref key, .. }) if key == "name"
        ));
    }

    #[test]
    fn subscript() {
        let e = expr("list[0]");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall { ref name, .. }) if name[0] == "_subscript"
        ));
    }

    #[test]
    fn slice_lo_hi() {
        let e = expr("list[1..3]");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall { ref name, .. }) if name[0] == "_slice"
        ));
    }

    #[test]
    fn slice_no_lower() {
        let e = expr("list[..3]");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall { ref name, ref args, .. })
                if name[0] == "_slice_from_start" && args.len() == 2
        ));
    }

    #[test]
    fn slice_no_upper() {
        let e = expr("list[1..]");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall { ref name, ref args, .. })
                if name[0] == "_slice_to_end" && args.len() == 2
        ));
    }

    #[test]
    fn slice_explicit_null_bounds_are_not_omitted_bounds() {
        let e = expr("list[null..null]");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall { ref name, ref args, .. })
                if name[0] == "_slice" && args.len() == 3
        ));
    }

    // --- Function calls ---

    #[test]
    fn function_call() {
        let e = expr("toUpper(n.name)");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall { ref name, .. }) if name[0] == "toUpper"
        ));
    }

    #[test]
    fn namespaced_function_call() {
        // `date.truncate(...)` is a qualified function call: the whole dotted
        // path is the name (the binder joins it as `date.truncate`), not a
        // property access on a `date` variable.
        let e = expr("date.truncate('year', d)");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall { ref name, .. })
                if name.as_slice() == ["date", "truncate"]
        ));
    }

    #[test]
    fn namespaced_function_call_three_segments() {
        let e = expr("a.b.c(1)");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall { ref name, .. })
                if name.as_slice() == ["a", "b", "c"]
        ));
    }

    #[test]
    fn dotted_name_without_parens_is_property_access() {
        // A dotted name NOT followed by `(` stays a property access, so the
        // namespaced-call path must not steal `a.b`.
        let e = expr("a.b");
        assert!(matches!(
            e,
            Expr::Property(PropertyAccess { ref key, .. }) if key == "b"
        ));
    }

    #[test]
    fn count_star() {
        let e = expr("count(*)");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall { star: true, ref name, .. }) if name[0] == "count"
        ));
    }

    #[test]
    fn count_distinct() {
        let e = expr("count(DISTINCT x)");
        assert!(matches!(
            e,
            Expr::FunctionCall(FunctionCall {
                distinct: true,
                star: false,
                ..
            })
        ));
    }

    #[test]
    fn keyword_named_function_can_be_a_variable() {
        let e = expr("count");
        assert!(matches!(e, Expr::Var(VarRef { ref name, .. }) if name == "count"));
    }

    // --- Collection literals ---

    #[test]
    fn list_literal() {
        let e = expr("[1, 2, 3]");
        let Expr::List(ListLiteral { elements, .. }) = e else {
            panic!("expected list literal");
        };
        assert_eq!(elements.len(), 3);
    }

    #[test]
    fn empty_list() {
        let e = expr("[]");
        let Expr::List(ListLiteral { elements, .. }) = e else {
            panic!("expected list literal");
        };
        assert!(elements.is_empty());
    }

    #[test]
    fn map_literal() {
        let source = "{name: 'Alice', return: 30, `odd key`: true}";
        let e = expr(source);
        let Expr::Map(MapLiteral {
            entries, key_spans, ..
        }) = e
        else {
            panic!("expected map literal");
        };
        assert_eq!(entries.len(), 3);
        assert!(entries.contains_key("name"));
        for (key, source_key) in [
            ("name", "name"),
            ("return", "return"),
            ("odd key", "`odd key`"),
        ] {
            let span = key_spans[key];
            assert_eq!(&source[span.start..span.end], source_key);
        }
    }

    // --- CASE expressions ---

    #[test]
    fn case_simple() {
        let e = expr("CASE n.status WHEN 'A' THEN 1 ELSE 0 END");
        let Expr::Case(CaseExpr {
            subject,
            when_clauses,
            else_expr,
            ..
        }) = e
        else {
            panic!("expected CASE");
        };
        assert!(subject.is_some());
        assert_eq!(when_clauses.len(), 1);
        assert!(else_expr.is_some());
    }

    #[test]
    fn case_searched() {
        let e = expr("CASE WHEN n.age > 30 THEN 'senior' END");
        let Expr::Case(CaseExpr {
            subject,
            when_clauses,
            else_expr,
            ..
        }) = e
        else {
            panic!("expected CASE");
        };
        assert!(subject.is_none());
        assert_eq!(when_clauses.len(), 1);
        assert!(else_expr.is_none());
    }

    // --- List comprehension ---

    #[test]
    fn list_comprehension() {
        let e = expr("[x IN [1,2,3] WHERE x > 0 | x * 2]");
        let Expr::ListComprehension(ListComprehension {
            ref var,
            filter,
            projection,
            ..
        }) = e
        else {
            panic!("expected list comprehension");
        };
        assert_eq!(var, "x");
        assert!(filter.is_some());
        assert!(projection.is_some());
    }

    #[test]
    fn list_comprehension_no_filter() {
        let e = expr("[x IN list | x]");
        assert!(matches!(e, Expr::ListComprehension(_)));
    }

    #[test]
    fn named_pattern_comprehension() {
        let e = expr("[p = (n)-[:REL]->() | p]");
        let Expr::PatternComprehension(PatternComprehension {
            var,
            pattern,
            filter,
            projection,
            ..
        }) = e
        else {
            panic!("expected pattern comprehension");
        };
        assert_eq!(var.as_deref(), Some("p"));
        assert_eq!(pattern.elements.len(), 3);
        assert!(filter.is_none());
        assert!(matches!(*projection, Expr::Var(VarRef { ref name, .. }) if name == "p"));
    }

    #[test]
    fn anonymous_filtered_pattern_comprehension() {
        let e = expr("[(n)-[r:REL*1..3]->(m) WHERE m.ok | r.weight]");
        let Expr::PatternComprehension(PatternComprehension {
            var,
            pattern,
            filter,
            projection,
            ..
        }) = e
        else {
            panic!("expected pattern comprehension");
        };
        assert!(var.is_none());
        assert_eq!(pattern.elements.len(), 3);
        assert!(filter.is_some());
        assert!(matches!(*projection, Expr::Property(_)));
        let graphforge_ast::PathElement::Rel(rel) = &pattern.elements[1] else {
            panic!("expected relationship pattern");
        };
        assert_eq!(rel.var.as_deref(), Some("r"));
        assert_eq!(rel.min_hops, Some(1));
        assert_eq!(rel.max_hops, Some(3));
    }

    #[test]
    fn simple_existential_subquery() {
        let e = expr("exists { (n)-[r:REL]->(m) WHERE type(r) = 'REL' }");
        let Expr::ExistentialSubquery(ExistentialSubquery {
            body: ExistentialSubqueryBody::Simple { pattern, filter },
            ..
        }) = e
        else {
            panic!("expected existential subquery");
        };
        assert_eq!(pattern.elements.len(), 3);
        assert!(filter.is_some());
    }

    #[test]
    fn full_existential_subquery_parses_read_pipeline() {
        let e = expr("exists { MATCH (n)-->(m) WITH n, count(*) AS c WHERE c > 1 RETURN true }");
        let Expr::ExistentialSubquery(ExistentialSubquery {
            body: ExistentialSubqueryBody::Full(query),
            ..
        }) = e
        else {
            panic!("expected full existential subquery");
        };
        assert_eq!(query.clauses.len(), 3);
        assert!(matches!(
            query.clauses[0],
            graphforge_ast::AstClause::Match(_)
        ));
        assert!(matches!(
            query.clauses[1],
            graphforge_ast::AstClause::With(_)
        ));
        assert!(matches!(
            query.clauses[2],
            graphforge_ast::AstClause::Return(_)
        ));
    }

    #[test]
    fn exists_function_form_is_preserved() {
        assert!(matches!(expr("exists(n.name)"), Expr::FunctionCall(_)));
    }

    #[test]
    fn pattern_comprehension_requires_projection_pipe() {
        let mut ts = TokenStream::new("[(n)-[:REL]->()]").expect("lex failed");
        assert!(parse_expr(&mut ts, 0).is_err());
        assert!(matches!(expr("[(n)]"), Expr::List(_)));
    }

    // --- Parenthesized ---

    #[test]
    fn parenthesized() {
        let e = expr("(1 + 2)");
        assert!(matches!(e, Expr::Parenthesized { .. }));
    }

    #[test]
    fn pattern_predicate_expression() {
        let e = expr("(n)-[:REL]->(m)");
        let Expr::PatternPredicate(pred) = e else {
            panic!("expected pattern predicate");
        };
        assert_eq!(pred.pattern.elements.len(), 3);
        assert!(matches!(
            pred.pattern.elements[1],
            graphforge_ast::PathElement::Rel(_)
        ));
    }

    #[test]
    fn parenthesized_var_is_not_pattern_predicate() {
        let e = expr("(n)");
        assert!(matches!(e, Expr::Parenthesized { .. }));
    }

    #[test]
    fn label_predicate_expression() {
        let e = expr("n:Person");
        let Expr::LabelPredicate(pred) = e else {
            panic!("expected label predicate");
        };
        assert_eq!(pred.var, "n");
        assert_eq!(pred.labels, vec!["Person"]);
    }

    #[test]
    fn conjunctive_label_predicate_expression() {
        let e = expr("n:A:B");
        let Expr::LabelPredicate(pred) = e else {
            panic!("expected label predicate");
        };
        assert_eq!(pred.var, "n");
        assert_eq!(pred.labels, vec!["A", "B"]);
    }

    #[test]
    fn every_keyword_expression_name_has_a_stable_spelling() {
        let cases = [
            (Tok::Match, "match"),
            (Tok::Return, "return"),
            (Tok::Where, "where"),
            (Tok::With, "with"),
            (Tok::As, "as"),
            (Tok::Distinct, "distinct"),
            (Tok::Union, "union"),
            (Tok::All, "all"),
            (Tok::Create, "create"),
            (Tok::Merge, "merge"),
            (Tok::On, "on"),
            (Tok::Set, "set"),
            (Tok::Remove, "remove"),
            (Tok::Delete, "delete"),
            (Tok::Detach, "detach"),
            (Tok::Call, "call"),
            (Tok::Yield, "yield"),
            (Tok::Unwind, "unwind"),
            (Tok::Order, "order"),
            (Tok::By, "by"),
            (Tok::Skip, "skip"),
            (Tok::Limit, "limit"),
            (Tok::Is, "is"),
            (Tok::In, "in"),
            (Tok::Starts, "starts"),
            (Tok::Ends, "ends"),
            (Tok::Contains, "contains"),
            (Tok::Case, "case"),
            (Tok::When, "when"),
            (Tok::Then, "then"),
            (Tok::Else, "else"),
            (Tok::End, "end"),
            (Tok::True, "true"),
            (Tok::False, "false"),
            (Tok::Null, "null"),
            (Tok::Count, "count"),
            (Tok::Exists, "exists"),
            (Tok::Not, "not"),
            (Tok::And, "and"),
            (Tok::Or, "or"),
            (Tok::Xor, "xor"),
            (Tok::Optional, "optional"),
            (Tok::Any, "any"),
            (Tok::None, "none"),
            (Tok::Single, "single"),
            (Tok::Filter, "filter"),
            (Tok::Extract, "extract"),
            (Tok::Reduce, "reduce"),
            (Tok::ShortestPath, "shortestPath"),
            (Tok::AllShortestPaths, "allShortestPaths"),
            (Tok::Asc, "asc"),
            (Tok::Desc, "desc"),
            (Tok::Ascending, "ascending"),
            (Tok::Descending, "descending"),
        ];
        for (token, spelling) in cases {
            assert_eq!(tok_as_keyword_str(&token), Some(spelling));
        }
        assert_eq!(tok_as_keyword_str(&Tok::Comma), None);

        for (token, spelling) in [
            (Tok::Count, "count"),
            (Tok::Exists, "exists"),
            (Tok::All, "all"),
            (Tok::Any, "any"),
            (Tok::None, "none"),
            (Tok::Single, "single"),
            (Tok::Filter, "filter"),
            (Tok::Extract, "extract"),
            (Tok::Reduce, "reduce"),
            (Tok::ShortestPath, "shortestPath"),
            (Tok::AllShortestPaths, "allShortestPaths"),
        ] {
            assert_eq!(tok_keyword_name(&token), spelling);
        }
    }
}
