use crate::lexer::Tok;
use graphforge_ast::{
    AstClause, AstQuery, CallClause, CreateClause, DeleteClause, DialectVersion, Expr, MatchClause,
    MergeClause, OrderByClause, ParseError, ParseErrorKind, PathElement, PropertyAccess,
    RemoveClause, RemoveItem, ReturnClause, ReturnItem, SetClause, SetItem, SortItem, SortOrder,
    UnionClause, UnwindClause, VarRef, WhereClause, WithClause,
};
use graphforge_core::Span;

use super::TokenStream;
use super::expr::parse_expr;
use super::patterns::{parse_pattern, parse_pattern_list};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Parse a full Cypher query into an [`AstQuery`].
pub fn parse_query(ts: &mut TokenStream) -> Result<AstQuery, ParseError> {
    parse_query_until(ts, false)
}

pub(super) fn parse_subquery(ts: &mut TokenStream) -> Result<AstQuery, ParseError> {
    parse_query_until(ts, true)
}

fn parse_query_until(
    ts: &mut TokenStream,
    stop_at_closing_brace: bool,
) -> Result<AstQuery, ParseError> {
    let start = ts.current_pos();
    let mut clauses = Vec::new();

    loop {
        match ts.peek() {
            None => break,
            Some(Tok::RBrace) if stop_at_closing_brace => break,
            Some(Tok::Match) => {
                clauses.push(AstClause::Match(parse_match_clause(ts, false)?));
            }
            Some(Tok::Optional) => {
                clauses.push(AstClause::OptionalMatch(parse_match_clause(ts, true)?));
            }
            Some(Tok::With) => {
                clauses.push(AstClause::With(parse_with_clause(ts)?));
            }
            Some(Tok::Return) => {
                clauses.push(AstClause::Return(parse_return_clause(ts)?));
            }
            Some(Tok::Unwind) => {
                clauses.push(AstClause::Unwind(parse_unwind_clause(ts)?));
            }
            Some(Tok::Union) => {
                clauses.push(AstClause::Union(parse_union_clause(ts)?));
            }
            Some(Tok::Create) => {
                clauses.push(AstClause::Create(parse_create_clause(ts)?));
            }
            Some(Tok::Merge) => {
                clauses.push(AstClause::Merge(parse_merge_clause(ts)?));
            }
            Some(Tok::Set) => {
                clauses.push(AstClause::Set(parse_set_clause(ts)?));
            }
            Some(Tok::Remove) => {
                clauses.push(AstClause::Remove(parse_remove_clause(ts)?));
            }
            Some(Tok::Delete) => {
                clauses.push(AstClause::Delete(parse_delete_clause(ts, false)?));
            }
            Some(Tok::Detach) => {
                clauses.push(AstClause::Delete(parse_delete_clause(ts, true)?));
            }
            Some(Tok::Call) => {
                clauses.push(AstClause::Call(parse_call_clause(ts)?));
            }
            _ => return Err(ts.err("expected a Cypher clause (MATCH, RETURN, WITH, …)")),
        }
    }

    Ok(AstQuery {
        dialect: DialectVersion::OpenCypher9,
        clauses,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// MATCH / OPTIONAL MATCH
// ---------------------------------------------------------------------------

fn parse_match_clause(ts: &mut TokenStream, optional: bool) -> Result<MatchClause, ParseError> {
    let start = ts.current_pos();

    if optional {
        ts.eat(&Tok::Optional)?;
    }
    ts.eat(&Tok::Match)?;

    let patterns = parse_pattern_list(ts)?;

    let where_clause = if ts.at(&Tok::Where) {
        Some(parse_where_clause(ts)?)
    } else {
        None
    };

    Ok(MatchClause {
        patterns,
        where_clause,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// WHERE (inline sub-parse; not a top-level clause)
// ---------------------------------------------------------------------------

fn parse_where_clause(ts: &mut TokenStream) -> Result<WhereClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::Where)?;
    let predicate = parse_expr(ts, 0)?;
    Ok(WhereClause {
        predicate,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// RETURN
// ---------------------------------------------------------------------------

fn parse_return_clause(ts: &mut TokenStream) -> Result<ReturnClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::Return)?;

    let distinct = ts.eat_if(&Tok::Distinct);
    let items = parse_return_items(ts)?;
    let order_by = parse_opt_order_by(ts)?;
    let skip = if ts.at(&Tok::Skip) {
        ts.advance();
        Some(parse_expr(ts, 0)?)
    } else {
        None
    };
    let limit = if ts.at(&Tok::Limit) {
        ts.advance();
        Some(parse_expr(ts, 0)?)
    } else {
        None
    };

    Ok(ReturnClause {
        distinct,
        items,
        order_by,
        skip,
        limit,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// WITH
// ---------------------------------------------------------------------------

fn parse_with_clause(ts: &mut TokenStream) -> Result<WithClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::With)?;

    let distinct = ts.eat_if(&Tok::Distinct);
    let items = parse_return_items(ts)?;
    let order_by = parse_opt_order_by(ts)?;
    let skip = if ts.at(&Tok::Skip) {
        ts.advance();
        Some(parse_expr(ts, 0)?)
    } else {
        None
    };
    let limit = if ts.at(&Tok::Limit) {
        ts.advance();
        Some(parse_expr(ts, 0)?)
    } else {
        None
    };
    let where_clause = if ts.at(&Tok::Where) {
        Some(parse_where_clause(ts)?)
    } else {
        None
    };

    Ok(WithClause {
        distinct,
        items,
        order_by,
        skip,
        limit,
        where_clause,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// UNWIND
// ---------------------------------------------------------------------------

fn parse_unwind_clause(ts: &mut TokenStream) -> Result<UnwindClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::Unwind)?;
    let expr = parse_expr(ts, 0)?;
    ts.eat(&Tok::As)?;
    let alias = eat_ident(ts)?;
    Ok(UnwindClause {
        expr,
        alias,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// UNION [ALL]
// ---------------------------------------------------------------------------

fn parse_union_clause(ts: &mut TokenStream) -> Result<UnionClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::Union)?;
    let all = ts.eat_if(&Tok::All);
    Ok(UnionClause {
        all,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// CREATE
// ---------------------------------------------------------------------------

fn parse_create_clause(ts: &mut TokenStream) -> Result<CreateClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::Create)?;
    let patterns = parse_pattern_list(ts)?;
    // A CREATE relationship must name exactly one type — `:A|B` (a type
    // disjunction) is only meaningful when matching. The pattern grammar is
    // shared with MATCH (which does allow `:A|B`), so the constraint is enforced
    // here, where the CREATE context is known.
    for pat in &patterns {
        for elem in &pat.elements {
            if let PathElement::Rel(rel) = elem {
                if rel.types.len() > 1 {
                    return Err(ts.err_at(
                        rel.span,
                        ParseErrorKind::UnexpectedToken {
                            found: "|".to_string(),
                            expected: vec!["a single relationship type".to_string()],
                        },
                        "a CREATE relationship must have exactly one type; \
                         a type disjunction `:A|B` is only valid in MATCH",
                    ));
                }
            }
        }
    }
    Ok(CreateClause {
        patterns,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// MERGE
// ---------------------------------------------------------------------------

fn parse_merge_clause(ts: &mut TokenStream) -> Result<MergeClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::Merge)?;
    let pattern = parse_pattern(ts)?;

    let mut on_create: Vec<SetItem> = Vec::new();
    let mut on_match: Vec<SetItem> = Vec::new();

    // Optional ON CREATE SET and/or ON MATCH SET (either order, both optional)
    loop {
        if ts.at(&Tok::On) {
            ts.advance(); // consume ON
            match ts.peek() {
                Some(Tok::Create) => {
                    ts.advance(); // consume CREATE
                    ts.eat(&Tok::Set)?;
                    on_create.extend(parse_set_items(ts)?);
                }
                Some(Tok::Match) => {
                    ts.advance(); // consume MATCH
                    ts.eat(&Tok::Set)?;
                    on_match.extend(parse_set_items(ts)?);
                }
                _ => return Err(ts.err("expected CREATE or MATCH after ON")),
            }
        } else {
            break;
        }
    }

    Ok(MergeClause {
        pattern,
        on_create,
        on_match,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// SET
// ---------------------------------------------------------------------------

fn parse_set_clause(ts: &mut TokenStream) -> Result<SetClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::Set)?;
    let items = parse_set_items(ts)?;
    Ok(SetClause {
        items,
        span: ts.span_from(start),
    })
}

fn parse_set_items(ts: &mut TokenStream) -> Result<Vec<SetItem>, ParseError> {
    let mut items = vec![parse_set_item(ts)?];
    while ts.eat_if(&Tok::Comma) {
        items.push(parse_set_item(ts)?);
    }
    Ok(items)
}

/// Parse a single SET item:
/// - `n.prop = expr`  — property assignment
/// - `n = expr`       — full property replace
/// - `n += expr`      — property merge
/// - `n:Label`        — add label
fn parse_set_item(ts: &mut TokenStream) -> Result<SetItem, ParseError> {
    let start = ts.current_pos();
    let parenthesized = ts.eat_if(&Tok::LParen);
    let (var, var_span) = eat_ident_with_span(ts)?;
    if parenthesized {
        ts.eat(&Tok::RParen)?;
    }

    match ts.peek() {
        // n.prop = expr
        Some(Tok::Dot) => {
            ts.advance(); // consume dot
            let key = eat_ident(ts)?;
            let prop_span = ts.span_from(start);
            ts.eat(&Tok::Eq)?;
            let value = parse_expr(ts, 0)?;
            let span = ts.span_from(start);
            Ok(SetItem::Property {
                target: PropertyAccess {
                    object: Box::new(Expr::Var(VarRef {
                        name: var,
                        span: var_span,
                    })),
                    key,
                    span: prop_span,
                },
                value,
                span,
            })
        }
        // n = expr (full replace)
        Some(Tok::Eq) => {
            ts.advance(); // consume =
            let map = parse_expr(ts, 0)?;
            Ok(SetItem::PropertyReplace {
                var,
                map,
                span: ts.span_from(start),
            })
        }
        // n += expr (merge)
        Some(Tok::PlusEq) => {
            ts.advance(); // consume +=
            let map = parse_expr(ts, 0)?;
            Ok(SetItem::PropertyMerge {
                var,
                map,
                span: ts.span_from(start),
            })
        }
        // n:Label (add label)
        Some(Tok::Colon) => {
            let labels = eat_label_names(ts)?;
            Ok(SetItem::Label {
                var,
                labels,
                span: ts.span_from(start),
            })
        }
        _ => Err(ts.err("expected `.`, `=`, `+=`, or `:` after variable in SET")),
    }
}

// ---------------------------------------------------------------------------
// REMOVE
// ---------------------------------------------------------------------------

fn parse_remove_clause(ts: &mut TokenStream) -> Result<RemoveClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::Remove)?;
    let items = parse_remove_items(ts)?;
    Ok(RemoveClause {
        items,
        span: ts.span_from(start),
    })
}

fn parse_remove_items(ts: &mut TokenStream) -> Result<Vec<RemoveItem>, ParseError> {
    let mut items = vec![parse_remove_item(ts)?];
    while ts.eat_if(&Tok::Comma) {
        items.push(parse_remove_item(ts)?);
    }
    Ok(items)
}

/// Parse a single REMOVE item:
/// - `n.prop`   — remove property
/// - `n:Label`  — remove label
fn parse_remove_item(ts: &mut TokenStream) -> Result<RemoveItem, ParseError> {
    let start = ts.current_pos();
    let (var, var_span) = eat_ident_with_span(ts)?;

    match ts.peek() {
        Some(Tok::Dot) => {
            ts.advance(); // consume dot
            let key = eat_ident(ts)?;
            let span = ts.span_from(start);
            Ok(RemoveItem::Property(
                PropertyAccess {
                    object: Box::new(Expr::Var(VarRef {
                        name: var,
                        span: var_span,
                    })),
                    key,
                    span,
                },
                span,
            ))
        }
        Some(Tok::Colon) => {
            let labels = eat_label_names(ts)?;
            Ok(RemoveItem::Label {
                var,
                labels,
                span: ts.span_from(start),
            })
        }
        _ => Err(ts.err("expected `.` or `:` after variable in REMOVE")),
    }
}

// ---------------------------------------------------------------------------
// DELETE / DETACH DELETE
// ---------------------------------------------------------------------------

fn parse_delete_clause(ts: &mut TokenStream, detach: bool) -> Result<DeleteClause, ParseError> {
    let start = ts.current_pos();
    if detach {
        ts.eat(&Tok::Detach)?;
    }
    ts.eat(&Tok::Delete)?;
    let mut exprs = vec![parse_expr(ts, 0)?];
    while ts.eat_if(&Tok::Comma) {
        exprs.push(parse_expr(ts, 0)?);
    }
    Ok(DeleteClause {
        detach,
        exprs,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// CALL
// ---------------------------------------------------------------------------

/// Parse a CALL clause. Handles two forms:
/// - `CALL proc.name(args) [YIELD items]` — named procedure call
/// - `CALL { query } [YIELD items]`       — subquery (procedure is empty)
fn parse_call_clause(ts: &mut TokenStream) -> Result<CallClause, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::Call)?;

    if ts.at(&Tok::LBrace) {
        // CALL { ... } subquery form
        ts.advance(); // consume {
        // Consume tokens until matching } (depth-aware)
        let mut depth = 1usize;
        while !ts.is_empty() {
            match ts.peek() {
                Some(Tok::LBrace) => {
                    depth += 1;
                    ts.advance();
                }
                Some(Tok::RBrace) => {
                    depth -= 1;
                    ts.advance();
                    if depth == 0 {
                        break;
                    }
                }
                _ => {
                    ts.advance();
                }
            }
        }
        if depth != 0 {
            return Err(ts.err("unterminated CALL subquery: expected `}`"));
        }
        let yield_items = parse_opt_yield(ts)?;
        Ok(CallClause {
            procedure: vec![],
            args: vec![],
            args_explicit: false,
            yield_items,
            span: ts.span_from(start),
        })
    } else {
        // CALL proc.name(args) form
        let procedure = parse_procedure_name(ts)?;
        let args_explicit = ts.at(&Tok::LParen);
        let args = if args_explicit {
            ts.advance(); // consume (
            if ts.eat_if(&Tok::RParen) {
                vec![]
            } else {
                let mut args = vec![parse_expr(ts, 0)?];
                while ts.eat_if(&Tok::Comma) {
                    args.push(parse_expr(ts, 0)?);
                }
                ts.eat(&Tok::RParen)?;
                args
            }
        } else {
            vec![]
        };
        let yield_items = parse_opt_yield(ts)?;
        Ok(CallClause {
            procedure,
            args,
            args_explicit,
            yield_items,
            span: ts.span_from(start),
        })
    }
}

/// Parse `proc.name` as `["proc", "name"]`.
fn parse_procedure_name(ts: &mut TokenStream) -> Result<Vec<String>, ParseError> {
    let mut parts = vec![eat_ident(ts)?];
    while ts.at(&Tok::Dot) {
        ts.advance(); // consume dot
        parts.push(eat_ident(ts)?);
    }
    Ok(parts)
}

/// Parse optional `YIELD item (, item)*` — returns empty vec if absent.
fn parse_opt_yield(ts: &mut TokenStream) -> Result<Vec<ReturnItem>, ParseError> {
    if !ts.at(&Tok::Yield) {
        return Ok(vec![]);
    }
    ts.advance(); // consume YIELD
    parse_return_items(ts)
}

// ---------------------------------------------------------------------------
// Shared label-list helper
// ---------------------------------------------------------------------------

/// Consume `:Label1:Label2…` — at least one label required.
fn eat_label_names(ts: &mut TokenStream) -> Result<Vec<String>, ParseError> {
    let mut labels = Vec::new();
    while ts.eat_if(&Tok::Colon) {
        labels.push(eat_label_name(ts)?);
    }
    if labels.is_empty() {
        return Err(ts.err("expected `:Label` after variable"));
    }
    Ok(labels)
}

/// Consume a single label/type name (identifier or keyword-as-label).
fn eat_label_name(ts: &mut TokenStream) -> Result<String, ParseError> {
    match ts.peek().cloned() {
        Some(Tok::Ident(name)) => {
            ts.advance();
            Ok(name)
        }
        Some(tok) => {
            if let Some(kw) = tok_as_alias_str(&tok) {
                ts.advance();
                Ok(kw.to_owned())
            } else {
                Err(ts.err("expected label name"))
            }
        }
        None => Err(ts.err("expected label name, found end of input")),
    }
}

// ---------------------------------------------------------------------------
// Shared sub-parsers
// ---------------------------------------------------------------------------

/// Parse a comma-separated list of return items (`expr [AS alias]` or `*`).
fn parse_return_items(ts: &mut TokenStream) -> Result<Vec<ReturnItem>, ParseError> {
    let mut items = vec![parse_one_return_item(ts)?];
    while ts.eat_if(&Tok::Comma) {
        items.push(parse_one_return_item(ts)?);
    }
    Ok(items)
}

fn parse_one_return_item(ts: &mut TokenStream) -> Result<ReturnItem, ParseError> {
    let start = ts.current_pos();

    // `RETURN *` wildcard
    if ts.at(&Tok::Star) {
        let (l, _, r) = ts.advance().unwrap();
        return Ok(ReturnItem {
            expr: graphforge_ast::Expr::Var(VarRef {
                name: "*".to_string(),
                span: Span::new(l, r),
            }),
            alias: None,
            display: None,
            span: Span::new(l, r),
        });
    }

    let expr = parse_expr(ts, 0)?;
    // Capture the expression's verbatim source text (before any `AS`) as the
    // default column name for an un-aliased item — openCypher names a projection
    // column by the expression as written (`n.prop`, `count(*)`, `a.x IS NULL`).
    let display = Some(ts.text(expr.span()).trim().to_string());
    let alias = if ts.eat_if(&Tok::As) {
        Some(eat_alias(ts)?)
    } else {
        None
    };

    Ok(ReturnItem {
        expr,
        alias,
        display,
        span: ts.span_from(start),
    })
}

/// Parse optional `ORDER BY sort_item (, sort_item)*`.
fn parse_opt_order_by(ts: &mut TokenStream) -> Result<Option<OrderByClause>, ParseError> {
    if !ts.at(&Tok::Order) {
        return Ok(None);
    }
    let start = ts.current_pos();
    ts.advance(); // consume ORDER
    ts.eat(&Tok::By)?;

    let mut items = vec![parse_sort_item(ts)?];
    while ts.eat_if(&Tok::Comma) {
        items.push(parse_sort_item(ts)?);
    }

    Ok(Some(OrderByClause {
        items,
        span: ts.span_from(start),
    }))
}

fn parse_sort_item(ts: &mut TokenStream) -> Result<SortItem, ParseError> {
    let start = ts.current_pos();
    let expr = parse_expr(ts, 0)?;
    let order = match ts.peek() {
        Some(Tok::Asc) | Some(Tok::Ascending) => {
            ts.advance();
            SortOrder::Ascending
        }
        Some(Tok::Desc) | Some(Tok::Descending) => {
            ts.advance();
            SortOrder::Descending
        }
        _ => SortOrder::Ascending,
    };
    Ok(SortItem {
        expr,
        order,
        span: ts.span_from(start),
    })
}

// ---------------------------------------------------------------------------
// Identifier helpers
// ---------------------------------------------------------------------------

fn eat_ident(ts: &mut TokenStream) -> Result<String, ParseError> {
    match ts.peek().cloned() {
        Some(Tok::Ident(name)) => {
            ts.advance();
            Ok(name)
        }
        _ => Err(ts.err("expected identifier")),
    }
}

/// Like `eat_ident` but also returns the exact token span.
fn eat_ident_with_span(ts: &mut TokenStream) -> Result<(String, Span), ParseError> {
    match ts.peek().cloned() {
        Some(Tok::Ident(name)) => {
            let (l, _, r) = ts.advance().unwrap();
            Ok((name, Span::new(l, r)))
        }
        _ => Err(ts.err("expected identifier")),
    }
}

/// Consume an alias name — identifiers or keywords (e.g. `AS count`, `AS type`).
fn eat_alias(ts: &mut TokenStream) -> Result<String, ParseError> {
    match ts.peek().cloned() {
        Some(Tok::Ident(name)) => {
            ts.advance();
            Ok(name)
        }
        Some(tok) => {
            if let Some(kw) = tok_as_alias_str(&tok) {
                ts.advance();
                Ok(kw.to_owned())
            } else {
                Err(ts.err("expected alias name after AS"))
            }
        }
        None => Err(ts.err("expected alias name, found end of input")),
    }
}

/// Keyword tokens that may appear as alias names (e.g. `RETURN x AS count`).
fn tok_as_alias_str(tok: &Tok) -> Option<&'static str> {
    match tok {
        Tok::Count => Some("count"),
        Tok::Exists => Some("exists"),
        Tok::Filter => Some("filter"),
        Tok::Reduce => Some("reduce"),
        Tok::Extract => Some("extract"),
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
        Tok::Not => Some("not"),
        Tok::And => Some("and"),
        Tok::Or => Some("or"),
        Tok::Xor => Some("xor"),
        Tok::Optional => Some("optional"),
        Tok::Any => Some("any"),
        Tok::None => Some("none"),
        Tok::Single => Some("single"),
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
    use graphforge_ast::{AstClause, Expr, Literal, RemoveItem, SetItem, SortOrder};

    fn ts(input: &str) -> TokenStream<'_> {
        TokenStream::new(input).expect("lex failed")
    }

    fn query(input: &str) -> AstQuery {
        parse_query(&mut ts(input)).expect("parse_query failed")
    }

    // --- MATCH ---

    #[test]
    fn match_return() {
        let q = query("MATCH (n:Person) RETURN n");
        assert_eq!(q.clauses.len(), 2);
        assert!(matches!(q.clauses[0], AstClause::Match(_)));
        assert!(matches!(q.clauses[1], AstClause::Return(_)));
    }

    #[test]
    fn match_single_node() {
        let q = query("MATCH (n:Person) RETURN n");
        let AstClause::Match(m) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(m.patterns.len(), 1);
        assert!(m.where_clause.is_none());
    }

    #[test]
    fn optional_match_with_where() {
        let q = query("OPTIONAL MATCH (n) WHERE n.age > 18 RETURN n");
        assert!(matches!(q.clauses[0], AstClause::OptionalMatch(_)));
        let AstClause::OptionalMatch(m) = &q.clauses[0] else {
            panic!()
        };
        assert!(m.where_clause.is_some());
    }

    #[test]
    fn match_multi_pattern() {
        let q = query("MATCH (a), (b)-[:K]->(c) RETURN a");
        let AstClause::Match(m) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(m.patterns.len(), 2);
    }

    // --- WHERE inside MATCH ---

    #[test]
    fn match_with_where_predicate() {
        let q = query("MATCH (n) WHERE n.age > 18 RETURN n");
        let AstClause::Match(m) = &q.clauses[0] else {
            panic!()
        };
        assert!(m.where_clause.is_some());
    }

    // --- WITH ---

    #[test]
    fn with_and_where() {
        let q = query("MATCH (n) WITH n WHERE n.x > 0 RETURN n");
        assert_eq!(q.clauses.len(), 3);
        let AstClause::With(w) = &q.clauses[1] else {
            panic!()
        };
        assert!(w.where_clause.is_some());
        assert!(!w.distinct);
    }

    #[test]
    fn with_distinct() {
        let q = query("MATCH (n) WITH DISTINCT n RETURN n");
        let AstClause::With(w) = &q.clauses[1] else {
            panic!()
        };
        assert!(w.distinct);
    }

    // --- RETURN ---

    #[test]
    fn return_distinct_with_alias() {
        let q = query("RETURN DISTINCT n.name AS name");
        let AstClause::Return(r) = &q.clauses[0] else {
            panic!()
        };
        assert!(r.distinct);
        assert_eq!(r.items.len(), 1);
        assert_eq!(r.items[0].alias.as_deref(), Some("name"));
    }

    #[test]
    fn return_order_by_multi() {
        let q = query("RETURN n ORDER BY n.age DESC, n.name ASC");
        let AstClause::Return(r) = &q.clauses[0] else {
            panic!()
        };
        let ob = r.order_by.as_ref().expect("order_by");
        assert_eq!(ob.items.len(), 2);
        assert_eq!(ob.items[0].order, SortOrder::Descending);
        assert_eq!(ob.items[1].order, SortOrder::Ascending);
    }

    #[test]
    fn return_skip_limit() {
        let q = query("RETURN n SKIP 10 LIMIT 5");
        let AstClause::Return(r) = &q.clauses[0] else {
            panic!()
        };
        assert!(matches!(r.skip, Some(Expr::Literal(Literal::Int(10, _)))));
        assert!(matches!(r.limit, Some(Expr::Literal(Literal::Int(5, _)))));
    }

    #[test]
    fn return_star_wildcard() {
        let q = query("MATCH (n) RETURN *");
        let AstClause::Return(r) = &q.clauses[1] else {
            panic!()
        };
        assert_eq!(r.items.len(), 1);
        match &r.items[0].expr {
            Expr::Var(v) => assert_eq!(v.name, "*"),
            other => panic!("expected star var, got {other:?}"),
        }
    }

    // --- UNWIND ---

    #[test]
    fn unwind_list_literal() {
        let q = query("UNWIND [1, 2, 3] AS x RETURN x");
        let AstClause::Unwind(u) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(u.alias, "x");
        assert!(matches!(u.expr, Expr::List(_)));
    }

    // --- UNION ---

    #[test]
    fn union_all() {
        let q = query("MATCH (n) RETURN n UNION ALL MATCH (m) RETURN m");
        assert_eq!(q.clauses.len(), 5); // M, R, UNION ALL, M, R
        let AstClause::Union(u) = &q.clauses[2] else {
            panic!()
        };
        assert!(u.all);
    }

    #[test]
    fn union_non_all() {
        let q = query("RETURN n UNION RETURN m");
        let AstClause::Union(u) = &q.clauses[1] else {
            panic!()
        };
        assert!(!u.all);
    }

    // --- parse_query wires up correctly ---

    #[test]
    fn empty_query_produces_empty_clauses() {
        let q = query("");
        assert!(q.clauses.is_empty());
    }

    // --- CREATE ---

    #[test]
    fn create_single_node() {
        let q = query("CREATE (n:Person {name: 'Alice'})");
        assert_eq!(q.clauses.len(), 1);
        let AstClause::Create(c) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(c.patterns.len(), 1);
    }

    #[test]
    fn create_relationship() {
        let q = query("CREATE (a)-[:KNOWS]->(b)");
        let AstClause::Create(c) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(c.patterns.len(), 1);
        assert_eq!(c.patterns[0].elements.len(), 3); // N R N
    }

    #[test]
    fn create_multi_type_relationship_rejected() {
        // A type disjunction `:KNOWS|LIKES` is invalid in CREATE — a created
        // relationship must have exactly one type.
        let err = parse_query(&mut ts("CREATE (a:Person)-[:KNOWS|LIKES]->(b:Person)"))
            .expect_err("multi-type CREATE rel must be rejected");
        assert!(
            matches!(err.kind, ParseErrorKind::UnexpectedToken { .. }),
            "expected UnexpectedToken, got {:?}",
            err.kind
        );
        assert!(
            err.message.contains("exactly one type"),
            "message should explain the constraint, got: {}",
            err.message
        );
    }

    #[test]
    fn match_multi_type_relationship_still_allowed() {
        // The same disjunction is valid in MATCH and must continue to parse.
        let q = query("MATCH (a)-[:KNOWS|LIKES]->(b) RETURN a");
        let AstClause::Match(m) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(m.patterns[0].elements.len(), 3); // N R N
    }

    // --- MERGE ---

    #[test]
    fn merge_no_actions() {
        let q = query("MERGE (n:Person {email: $e})");
        let AstClause::Merge(m) = &q.clauses[0] else {
            panic!()
        };
        assert!(m.on_create.is_empty());
        assert!(m.on_match.is_empty());
    }

    #[test]
    fn merge_with_on_create_and_on_match() {
        let q = query(
            "MERGE (n:Person {email: $e}) ON CREATE SET n.created = 1 ON MATCH SET n.seen = 2",
        );
        let AstClause::Merge(m) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(m.on_create.len(), 1);
        assert_eq!(m.on_match.len(), 1);
    }

    // --- SET ---

    #[test]
    fn set_property_assignment() {
        let q = query("SET n.age = 30");
        let AstClause::Set(s) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(s.items.len(), 1);
        assert!(matches!(s.items[0], SetItem::Property { .. }));
    }

    #[test]
    fn set_multiple_items() {
        let q = query("SET n.age = 30, n.name = 'Bob'");
        let AstClause::Set(s) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(s.items.len(), 2);
    }

    #[test]
    fn set_full_replace() {
        let q = query("SET n = {age: 30}");
        let AstClause::Set(s) = &q.clauses[0] else {
            panic!()
        };
        assert!(matches!(s.items[0], SetItem::PropertyReplace { .. }));
    }

    #[test]
    fn set_label_addition() {
        let q = query("SET n:Employee");
        let AstClause::Set(s) = &q.clauses[0] else {
            panic!()
        };
        assert!(matches!(s.items[0], SetItem::Label { .. }));
        if let SetItem::Label { labels, .. } = &s.items[0] {
            assert_eq!(labels, &["Employee"]);
        }
    }

    // --- REMOVE ---

    #[test]
    fn remove_property_and_label() {
        let q = query("REMOVE n.age, n:TempLabel");
        let AstClause::Remove(r) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(r.items.len(), 2);
        assert!(matches!(r.items[0], RemoveItem::Property(..)));
        assert!(matches!(r.items[1], RemoveItem::Label { .. }));
    }

    // --- DELETE ---

    #[test]
    fn delete_multiple() {
        let q = query("DELETE n, r");
        let AstClause::Delete(d) = &q.clauses[0] else {
            panic!()
        };
        assert!(!d.detach);
        assert_eq!(d.exprs.len(), 2);
    }

    #[test]
    fn detach_delete() {
        let q = query("DETACH DELETE n");
        let AstClause::Delete(d) = &q.clauses[0] else {
            panic!()
        };
        assert!(d.detach);
        assert_eq!(d.exprs.len(), 1);
    }

    // --- CALL ---

    #[test]
    fn call_subquery() {
        let q = query("CALL { MATCH (n) RETURN n } YIELD n");
        let AstClause::Call(c) = &q.clauses[0] else {
            panic!()
        };
        assert!(c.procedure.is_empty()); // empty = subquery form
        assert_eq!(c.yield_items.len(), 1);
    }

    #[test]
    fn call_procedure() {
        let q = query("CALL db.labels() YIELD label");
        let AstClause::Call(c) = &q.clauses[0] else {
            panic!()
        };
        assert_eq!(c.procedure, vec!["db", "labels"]);
        assert!(c.args_explicit);
        assert_eq!(c.yield_items.len(), 1);
    }

    #[test]
    fn call_procedure_without_parentheses_is_implicit() {
        let q = query("CALL db.labels YIELD label");
        let AstClause::Call(c) = &q.clauses[0] else {
            panic!()
        };
        assert!(!c.args_explicit);
    }

    // --- all 13 clause types reachable ---

    #[test]
    fn all_clause_types_dispatch_without_panic() {
        // Read-only (tested individually above, just confirm no panic)
        query("MATCH (n) RETURN n");
        query("OPTIONAL MATCH (n) RETURN n");
        query("WITH n RETURN n");
        query("RETURN n");
        query("UNWIND [1] AS x RETURN x");
        query("RETURN n UNION RETURN m");
        // Write
        query("CREATE (n)");
        query("MERGE (n:P)");
        query("SET n.x = 1");
        query("REMOVE n.x");
        query("DELETE n");
        query("DETACH DELETE n");
        query("CALL { MATCH (n) RETURN n }");
    }

    #[test]
    fn every_reserved_word_has_a_stable_alias_spelling() {
        let cases = [
            (Tok::Count, "count"),
            (Tok::Exists, "exists"),
            (Tok::Filter, "filter"),
            (Tok::Reduce, "reduce"),
            (Tok::Extract, "extract"),
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
            (Tok::Not, "not"),
            (Tok::And, "and"),
            (Tok::Or, "or"),
            (Tok::Xor, "xor"),
            (Tok::Optional, "optional"),
            (Tok::Any, "any"),
            (Tok::None, "none"),
            (Tok::Single, "single"),
            (Tok::ShortestPath, "shortestPath"),
            (Tok::AllShortestPaths, "allShortestPaths"),
            (Tok::Asc, "asc"),
            (Tok::Desc, "desc"),
            (Tok::Ascending, "ascending"),
            (Tok::Descending, "descending"),
        ];
        for (token, spelling) in cases {
            assert_eq!(tok_as_alias_str(&token), Some(spelling));
        }
        assert_eq!(tok_as_alias_str(&Tok::Comma), None);
    }
}
