//! Public partial parsers must never expose deferred placeholders as valid ASTs.
use graphforge_core::{ParseErrorKind, UnsupportedCypherFeature};
use graphforge_cypher::parser::{
    TokenStream, parse_expr, parse_node_pattern, parse_pattern, parse_pattern_list,
};

#[test]
fn expression_entry_point_refuses_reduce_and_count() {
    for (input, feature) in [
        (
            "reduce(total = 0, x IN [1,2,3] | total + x)",
            UnsupportedCypherFeature::Reduce,
        ),
        (
            "COUNT { MATCH (n) RETURN n }",
            UnsupportedCypherFeature::CountSubquery,
        ),
    ] {
        let error = parse_expr(&mut TokenStream::new(input).unwrap(), 0).unwrap_err();
        assert_eq!(error.kind, ParseErrorKind::UnsupportedFeature(feature));
    }
}

#[test]
fn path_entry_points_refuse_shortest_path_and_nested_reduce() {
    let mut stream = TokenStream::new("p = shortestPath((a)-[:transfer*]->(b))").unwrap();
    assert_eq!(
        parse_pattern(&mut stream).unwrap_err().kind,
        ParseErrorKind::UnsupportedFeature(UnsupportedCypherFeature::ShortestPath)
    );
    let mut stream = TokenStream::new("(a {value: reduce(t = 0, x IN [1] | t + x)})").unwrap();
    assert_eq!(
        parse_node_pattern(&mut stream).unwrap_err().kind,
        ParseErrorKind::UnsupportedFeature(UnsupportedCypherFeature::Reduce)
    );
    let mut stream = TokenStream::new("(a), p = shortestPath((b)-[:transfer*]->(c))").unwrap();
    assert_eq!(
        parse_pattern_list(&mut stream).unwrap_err().kind,
        ParseErrorKind::UnsupportedFeature(UnsupportedCypherFeature::ShortestPath)
    );
}

#[test]
fn partial_parsers_validate_enclosing_grammar_before_refusing() {
    for input in [
        "(a {value: reduce(t = 0, x IN [1] | t + x)}",
        "(a), p = shortestPath((b)-[:transfer*]->(c)) , broken",
    ] {
        let error = parse_pattern_list(&mut TokenStream::new(input).unwrap()).unwrap_err();
        assert!(
            !matches!(error.kind, ParseErrorKind::UnsupportedFeature(_)),
            "{input}: {error:?}"
        );
    }
}
