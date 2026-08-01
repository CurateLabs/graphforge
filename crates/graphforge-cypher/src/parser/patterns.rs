use crate::lexer::Tok;
use graphforge_ast::{
    Direction, Expr, NodePattern, ParseError, ParseErrorKind, PathElement, PathPattern, RelPattern,
};
use graphforge_core::Span;

use super::TokenStream;
use super::expr::parse_expr;

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Parse a single path pattern (optionally named: `p = (...)`).
pub fn parse_pattern(ts: &mut TokenStream) -> Result<PathPattern, ParseError> {
    let start = ts.current_pos();

    // Named path: `p = (...)`
    let var = if matches!(ts.peek(), Some(Tok::Ident(_))) && ts.peek_n(1) == Some(&Tok::Eq) {
        let name = eat_ident(ts)?;
        ts.eat(&Tok::Eq)?;
        Some(name)
    } else {
        None
    };

    let first = parse_node_pattern(ts)?;
    let mut elements: Vec<PathElement> = vec![PathElement::Node(first)];

    loop {
        // Try to parse a relationship pattern
        match ts.peek() {
            Some(Tok::RelOpen) | Some(Tok::Minus) | Some(Tok::LeftArrow) => {
                let rel = parse_rel_pattern(ts)?;
                elements.push(PathElement::Rel(rel));
                let node = parse_node_pattern(ts)?;
                elements.push(PathElement::Node(node));
            }
            _ => break,
        }
    }

    Ok(PathPattern {
        var,
        elements,
        span: ts.span_from(start),
    })
}

/// Parse a comma-separated list of path patterns.
pub fn parse_pattern_list(ts: &mut TokenStream) -> Result<Vec<PathPattern>, ParseError> {
    let mut patterns = vec![parse_pattern(ts)?];
    while ts.eat_if(&Tok::Comma) {
        patterns.push(parse_pattern(ts)?);
    }
    Ok(patterns)
}

/// Parse a node pattern: `( [var] [:Label]* [{props}] )`.
pub fn parse_node_pattern(ts: &mut TokenStream) -> Result<NodePattern, ParseError> {
    let start = ts.current_pos();
    ts.eat(&Tok::LParen)?;

    // Optional variable
    let var = match ts.peek() {
        Some(Tok::Ident(_)) => Some(eat_ident(ts)?),
        _ => None,
    };

    // Zero or more labels: `:Label`
    let mut labels = Vec::new();
    while ts.eat_if(&Tok::Colon) {
        labels.push(eat_label_name(ts)?);
    }

    // Optional property map
    let properties = if ts.peek() == Some(&Tok::LBrace) {
        Some(parse_expr(ts, 0)?)
    } else {
        None
    };

    let (_, r) = ts.eat(&Tok::RParen)?;

    Ok(NodePattern {
        var,
        labels,
        properties,
        span: Span::new(start, r),
    })
}

// ---------------------------------------------------------------------------
// Relationship pattern
// ---------------------------------------------------------------------------

fn parse_rel_pattern(ts: &mut TokenStream) -> Result<RelPattern, ParseError> {
    let start = ts.current_pos();

    // Determine leading token to decide direction and whether bracket is present.
    // Possible openings:
    //   RelOpen   (-[)      → Out or Undirected (need to check closing)
    //   LeftArrow (<-)      → In or Both
    //   Minus     (-)       → Out (-->) or Undirected (--) — short forms without bracket

    match ts.peek().cloned() {
        Some(Tok::RelOpen) => {
            // -[ ... ]-> or -[ ... ]-
            ts.advance(); // consume RelOpen (-[)
            let (var, types, min_hops, max_hops, properties) = parse_rel_detail(ts)?;
            ts.eat(&Tok::RBracket)?;
            let direction = if ts.eat_if(&Tok::RightArrow) {
                Direction::Out
            } else if ts.eat_if(&Tok::Minus) {
                Direction::Undirected
            } else {
                return Err(ts.err("expected `->` or `-` after `]`"));
            };
            Ok(RelPattern {
                var,
                types,
                direction,
                min_hops,
                max_hops,
                properties,
                span: Span::new(start, ts.current_pos()),
            })
        }
        Some(Tok::LeftArrow) => {
            // <-[...]-  or  <-[...]->  or  <--
            ts.advance(); // consume LeftArrow (<-)
            if ts.eat_if(&Tok::LBracket) {
                // <-[ ... ]-> or <-[ ... ]-
                let (var, types, min_hops, max_hops, properties) = parse_rel_detail(ts)?;
                let (_, _r_brace) = ts.eat(&Tok::RBracket)?;
                let direction = if ts.eat_if(&Tok::Minus) {
                    Direction::In
                } else if ts.eat_if(&Tok::RightArrow) {
                    Direction::Undirected
                } else {
                    return Err(ts.err("expected `-` after `]` in incoming relationship pattern"));
                };
                Ok(RelPattern {
                    var,
                    types,
                    direction,
                    min_hops,
                    max_hops,
                    properties,
                    span: Span::new(start, ts.current_pos()),
                })
            } else {
                // <-- (anonymous incoming, no bracket) or <--> (undirected)
                let direction = if ts.eat_if(&Tok::RightArrow) {
                    Direction::Undirected
                } else {
                    ts.eat(&Tok::Minus)?;
                    Direction::In
                };
                Ok(RelPattern {
                    var: None,
                    types: vec![],
                    direction,
                    min_hops: None,
                    max_hops: None,
                    properties: None,
                    span: Span::new(start, ts.current_pos()),
                })
            }
        }
        Some(Tok::Minus) => {
            // --> or --
            ts.advance(); // consume Minus
            if ts.eat_if(&Tok::RightArrow) {
                // -->
                Ok(RelPattern {
                    var: None,
                    types: vec![],
                    direction: Direction::Out,
                    min_hops: None,
                    max_hops: None,
                    properties: None,
                    span: Span::new(start, ts.current_pos()),
                })
            } else if ts.eat_if(&Tok::Minus) {
                // --
                Ok(RelPattern {
                    var: None,
                    types: vec![],
                    direction: Direction::Undirected,
                    min_hops: None,
                    max_hops: None,
                    properties: None,
                    span: Span::new(start, ts.current_pos()),
                })
            } else {
                Err(ts.err("expected `->` or `-` after `-`"))
            }
        }
        _ => Err(ts.err("expected relationship pattern (`-`, `<-`, or `-[`)")),
    }
}

/// Parse the interior of a bracketed relationship pattern:
/// `[var :TYPE|OTHER *min..max {props}]` — everything between `[` and `]`.
fn parse_rel_detail(
    ts: &mut TokenStream,
) -> Result<
    (
        Option<String>,
        Vec<String>,
        Option<u32>,
        Option<u32>,
        Option<Expr>,
    ),
    ParseError,
> {
    // Optional variable
    let var = match ts.peek() {
        Some(Tok::Ident(_)) => Some(eat_ident(ts)?),
        _ => None,
    };

    // Optional type list: `:TYPE1|TYPE2`
    let mut types = Vec::new();
    if ts.eat_if(&Tok::Colon) {
        types.push(eat_label_name(ts)?);
        while ts.eat_if(&Tok::Pipe) {
            ts.eat_if(&Tok::Colon);
            types.push(eat_label_name(ts)?);
        }
    }

    // Optional variable-length: `*`, `*2`, `*1..3`, `*..5`, `*3..`
    let (min_hops, max_hops) = if ts.eat_if(&Tok::Star) {
        parse_hop_range(ts)?
    } else {
        (None, None)
    };

    // Optional property map
    let properties = if ts.peek() == Some(&Tok::LBrace) {
        Some(parse_expr(ts, 0)?)
    } else {
        None
    };

    Ok((var, types, min_hops, max_hops, properties))
}

/// Parse optional hop-range after `*`: ``, `2`, `1..3`, `..5`, `3..`
/// Returns `(min_hops, max_hops)`.
/// Bare `*` → `(Some(1), None)` per openCypher semantics (min 1 hop, unbounded).
fn parse_hop_range(ts: &mut TokenStream) -> Result<(Option<u32>, Option<u32>), ParseError> {
    match ts.peek().cloned() {
        // `*..N` — no lower bound
        Some(Tok::DotDot) => {
            ts.advance();
            match ts.peek() {
                Some(Tok::IntLit(_)) => {
                    let max = parse_u32_lit(ts)?;
                    Ok((None, Some(max)))
                }
                _ => Ok((Some(1), None)),
            }
        }
        // `*N` or `*N..` or `*N..M`
        Some(Tok::IntLit(n)) => {
            let (l, _, r) = ts.advance().unwrap();
            let min = to_u32(n, Span::new(l, r))?;
            if ts.eat_if(&Tok::DotDot) {
                match ts.peek() {
                    Some(Tok::IntLit(_)) => {
                        let max = parse_u32_lit(ts)?;
                        Ok((Some(min), Some(max)))
                    }
                    _ => Ok((Some(min), None)), // `*N..` — no upper bound
                }
            } else {
                // bare `*N` — exactly N hops
                Ok((Some(min), Some(min)))
            }
        }
        // bare `*` — min 1 hop, unbounded
        _ => Ok((Some(1), None)),
    }
}

fn parse_u32_lit(ts: &mut TokenStream) -> Result<u32, ParseError> {
    match ts.peek().cloned() {
        Some(Tok::IntLit(n)) => {
            let (l, _, r) = ts.advance().unwrap();
            to_u32(n, Span::new(l, r))
        }
        _ => Err(ts.err("expected integer for hop count")),
    }
}

fn to_u32(n: i128, span: Span) -> Result<u32, ParseError> {
    u32::try_from(n).map_err(|_| {
        ParseError::new(
            ParseErrorKind::InvalidNumericLiteral,
            span,
            format!("hop count {n} out of range for u32"),
        )
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Consume an identifier or keyword-as-identifier.
fn eat_ident(ts: &mut TokenStream) -> Result<String, ParseError> {
    match ts.peek().cloned() {
        Some(Tok::Ident(name)) => {
            ts.advance();
            Ok(name)
        }
        _ => Err(ts.err("expected identifier")),
    }
}

/// Consume a label or type name: identifiers or keyword-as-label.
/// In Cypher, `:Match`, `:Return`, etc. are valid label/type names.
fn eat_label_name(ts: &mut TokenStream) -> Result<String, ParseError> {
    match ts.peek().cloned() {
        Some(Tok::Ident(name)) => {
            ts.advance();
            Ok(name)
        }
        Some(tok) => {
            if let Some(kw) = tok_as_label_str(&tok) {
                ts.advance();
                Ok(kw.to_owned())
            } else {
                Err(ts.err("expected label or type name"))
            }
        }
        None => Err(ts.err("expected label or type name, found end of input")),
    }
}

/// Map keyword tokens to their string form for use as label/type names.
fn tok_as_label_str(tok: &Tok) -> Option<&'static str> {
    match tok {
        Tok::Match => Some("Match"),
        Tok::Return => Some("Return"),
        Tok::Where => Some("Where"),
        Tok::With => Some("With"),
        Tok::As => Some("As"),
        Tok::Distinct => Some("Distinct"),
        Tok::Union => Some("Union"),
        Tok::All => Some("All"),
        Tok::Create => Some("Create"),
        Tok::Merge => Some("Merge"),
        Tok::On => Some("On"),
        Tok::Set => Some("Set"),
        Tok::Remove => Some("Remove"),
        Tok::Delete => Some("Delete"),
        Tok::Detach => Some("Detach"),
        Tok::Call => Some("Call"),
        Tok::Yield => Some("Yield"),
        Tok::Unwind => Some("Unwind"),
        Tok::Order => Some("Order"),
        Tok::By => Some("By"),
        Tok::Skip => Some("Skip"),
        Tok::Limit => Some("Limit"),
        Tok::Is => Some("Is"),
        Tok::In => Some("In"),
        Tok::Starts => Some("Starts"),
        Tok::Ends => Some("Ends"),
        Tok::Contains => Some("Contains"),
        Tok::Case => Some("Case"),
        Tok::When => Some("When"),
        Tok::Then => Some("Then"),
        Tok::Else => Some("Else"),
        Tok::End => Some("End"),
        Tok::True => Some("True"),
        Tok::False => Some("False"),
        Tok::Null => Some("Null"),
        Tok::Count => Some("Count"),
        Tok::Exists => Some("Exists"),
        Tok::Not => Some("Not"),
        Tok::And => Some("And"),
        Tok::Or => Some("Or"),
        Tok::Xor => Some("Xor"),
        Tok::Optional => Some("Optional"),
        Tok::Any => Some("Any"),
        Tok::None => Some("None"),
        Tok::Single => Some("Single"),
        Tok::Filter => Some("Filter"),
        Tok::Extract => Some("Extract"),
        Tok::Reduce => Some("Reduce"),
        Tok::ShortestPath => Some("ShortestPath"),
        Tok::AllShortestPaths => Some("AllShortestPaths"),
        Tok::Asc => Some("Asc"),
        Tok::Desc => Some("Desc"),
        Tok::Ascending => Some("Ascending"),
        Tok::Descending => Some("Descending"),
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
    use graphforge_ast::Expr;

    fn ts(input: &str) -> TokenStream<'_> {
        TokenStream::new(input).expect("lex failed")
    }

    fn node(input: &str) -> NodePattern {
        parse_node_pattern(&mut ts(input)).expect("parse_node_pattern failed")
    }

    fn pattern(input: &str) -> PathPattern {
        parse_pattern(&mut ts(input)).expect("parse_pattern failed")
    }

    fn pattern_list(input: &str) -> Vec<PathPattern> {
        parse_pattern_list(&mut ts(input)).expect("parse_pattern_list failed")
    }

    // --- NodePattern tests ---

    #[test]
    fn anonymous_node() {
        let n = node("()");
        assert_eq!(n.var, None);
        assert!(n.labels.is_empty());
        assert!(n.properties.is_none());
    }

    #[test]
    fn variable_only() {
        let n = node("(n)");
        assert_eq!(n.var.as_deref(), Some("n"));
        assert!(n.labels.is_empty());
    }

    #[test]
    fn label_only() {
        let n = node("(:Person)");
        assert_eq!(n.var, None);
        assert_eq!(n.labels, vec!["Person"]);
    }

    #[test]
    fn variable_and_label() {
        let n = node("(n:Person)");
        assert_eq!(n.var.as_deref(), Some("n"));
        assert_eq!(n.labels, vec!["Person"]);
    }

    #[test]
    fn multi_label() {
        let n = node("(n:Person:Employee)");
        assert_eq!(n.labels, vec!["Person", "Employee"]);
    }

    #[test]
    fn node_with_properties() {
        let n = node("(n {age: 30})");
        assert!(n.properties.is_some());
    }

    #[test]
    fn full_node_pattern() {
        let n = node("(n:Person {name: $p})");
        assert_eq!(n.var.as_deref(), Some("n"));
        assert_eq!(n.labels, vec!["Person"]);
        assert!(n.properties.is_some());
    }

    #[test]
    fn properties_delegate_to_parse_expr() {
        let n = node("(n {age: 1 + 2})");
        match n.properties {
            Some(Expr::Map(_)) => {}
            other => panic!("expected MapLiteral, got {other:?}"),
        }
    }

    // --- RelPattern (via parse_pattern) tests ---

    #[test]
    fn anon_out() {
        let p = pattern("(a)-->(b)");
        assert_eq!(p.elements.len(), 3);
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.direction, Direction::Out);
            assert!(r.var.is_none());
            assert!(r.types.is_empty());
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn anon_in() {
        let p = pattern("(a)<--(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.direction, Direction::In);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn anon_bidirectional_segment() {
        let p = pattern("(a)<-->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.direction, Direction::Undirected);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn anon_undirected() {
        let p = pattern("(a)--(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.direction, Direction::Undirected);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn bracketed_out_with_type() {
        let p = pattern("(a)-[:KNOWS]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.direction, Direction::Out);
            assert_eq!(r.types, vec!["KNOWS"]);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn bracketed_in_with_type() {
        let p = pattern("(a)<-[:KNOWS]-(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.direction, Direction::In);
            assert_eq!(r.types, vec!["KNOWS"]);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn bracketed_undirected() {
        let p = pattern("(a)-[r]-(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.direction, Direction::Undirected);
            assert_eq!(r.var.as_deref(), Some("r"));
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn type_list() {
        let p = pattern("(a)-[:KNOWS|LIKES]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.types, vec!["KNOWS", "LIKES"]);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn type_list_accepts_repeated_colons() {
        let p = pattern("(a)-[:KNOWS|:LIKES]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.types, vec!["KNOWS", "LIKES"]);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn bracketed_bidirectional_segment_is_undirected() {
        let p = pattern("(a)<-[r]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.direction, Direction::Undirected);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn variable_length_star() {
        // bare * → min 1 hop, unbounded (openCypher semantics)
        let p = pattern("(a)-[*]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.min_hops, Some(1));
            assert_eq!(r.max_hops, None);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn variable_length_exact() {
        let p = pattern("(a)-[*2]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.min_hops, Some(2));
            assert_eq!(r.max_hops, Some(2));
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn variable_length_range() {
        let p = pattern("(a)-[*1..3]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.min_hops, Some(1));
            assert_eq!(r.max_hops, Some(3));
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn variable_length_upper_only() {
        let p = pattern("(a)-[*..5]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.min_hops, None);
            assert_eq!(r.max_hops, Some(5));
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn variable_length_explicit_unbounded() {
        let p = pattern("(a)-[*..]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.min_hops, Some(1));
            assert_eq!(r.max_hops, None);
        } else {
            panic!("expected Rel");
        }
    }

    #[test]
    fn variable_length_lower_only() {
        let p = pattern("(a)-[*3..]->(b)");
        if let PathElement::Rel(r) = &p.elements[1] {
            assert_eq!(r.min_hops, Some(3));
            assert_eq!(r.max_hops, None);
        } else {
            panic!("expected Rel");
        }
    }

    // --- PathPattern tests ---

    #[test]
    fn chain_three_nodes() {
        let p = pattern("(a)-[:K]->(b)-[:L]->(c)");
        assert_eq!(p.elements.len(), 5); // N R N R N
    }

    #[test]
    fn named_path() {
        let p = pattern("p = (a)-[:KNOWS]->(b)");
        assert_eq!(p.var.as_deref(), Some("p"));
        assert_eq!(p.elements.len(), 3);
    }

    // --- parse_pattern_list ---

    #[test]
    fn pattern_list_multiple() {
        let patterns = pattern_list("(a), (b)-[:K]->(c)");
        assert_eq!(patterns.len(), 2);
        assert_eq!(patterns[0].elements.len(), 1);
        assert_eq!(patterns[1].elements.len(), 3);
    }

    // --- Span tests ---

    #[test]
    fn node_span_covers_parens() {
        let n = node("(n:Person)");
        assert_eq!(n.span.start, 0);
        assert!(n.span.end > n.span.start);
    }
}
