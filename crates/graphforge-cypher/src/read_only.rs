//! Syntax-only admission of read queries and their parameter references.

use std::collections::BTreeSet;

use graphforge_ast::{
    AstClause, AstQuery, ExistentialSubqueryBody, Expr, PathElement, PathPattern,
};
use graphforge_core::GfError;

/// Parse a read-only query and collect its exact, case-sensitive parameter names.
///
/// This does not bind against a catalog or execute the query. Procedure calls
/// are refused because syntax alone cannot establish their side effects.
///
/// # Errors
/// Returns a validation error for invalid syntax, mutations, procedure calls,
/// or AST forms whose read-only behavior is not supported.
pub fn read_only_query_parameters(query: &str) -> Result<BTreeSet<String>, GfError> {
    let parsed = crate::parse(query)
        .map_err(|error| GfError::Validation(format!("invalid query syntax: {error}")))?;
    let mut parameters = BTreeSet::new();
    visit_query(&parsed, &mut parameters)?;
    Ok(parameters)
}

fn refused() -> GfError {
    GfError::Validation("saved queries require read-only Cypher without procedure calls".into())
}

fn visit_query(query: &AstQuery, parameters: &mut BTreeSet<String>) -> Result<(), GfError> {
    if query.clauses.is_empty() {
        return Err(GfError::Validation(
            "saved queries require a nonempty query".into(),
        ));
    }
    for clause in &query.clauses {
        match clause {
            AstClause::Match(clause) | AstClause::OptionalMatch(clause) => {
                for pattern in &clause.patterns {
                    visit_pattern(pattern, parameters)?;
                }
                if let Some(predicate) = &clause.where_clause {
                    visit_expr(&predicate.predicate, parameters)?;
                }
            }
            AstClause::Where(clause) => visit_expr(&clause.predicate, parameters)?,
            AstClause::With(clause) => {
                visit_projection(
                    &clause.items,
                    clause.order_by.as_ref(),
                    clause.skip.as_ref(),
                    clause.limit.as_ref(),
                    parameters,
                )?;
                if let Some(predicate) = &clause.where_clause {
                    visit_expr(&predicate.predicate, parameters)?;
                }
            }
            AstClause::Return(clause) => visit_projection(
                &clause.items,
                clause.order_by.as_ref(),
                clause.skip.as_ref(),
                clause.limit.as_ref(),
                parameters,
            )?,
            AstClause::Unwind(clause) => visit_expr(&clause.expr, parameters)?,
            AstClause::Union(_) => {}
            _ => return Err(refused()),
        }
    }
    Ok(())
}

fn visit_projection(
    items: &[graphforge_ast::ReturnItem],
    order_by: Option<&graphforge_ast::OrderByClause>,
    skip: Option<&Expr>,
    limit: Option<&Expr>,
    parameters: &mut BTreeSet<String>,
) -> Result<(), GfError> {
    for item in items {
        visit_expr(&item.expr, parameters)?;
    }
    if let Some(order) = order_by {
        for item in &order.items {
            visit_expr(&item.expr, parameters)?;
        }
    }
    for expr in skip.into_iter().chain(limit) {
        visit_expr(expr, parameters)?;
    }
    Ok(())
}

fn visit_pattern(pattern: &PathPattern, parameters: &mut BTreeSet<String>) -> Result<(), GfError> {
    for element in &pattern.elements {
        let properties = match element {
            PathElement::Node(node) => &node.properties,
            PathElement::Rel(rel) => &rel.properties,
        };
        if let Some(properties) = properties {
            visit_expr(properties, parameters)?;
        }
    }
    Ok(())
}

fn visit_expr(expr: &Expr, parameters: &mut BTreeSet<String>) -> Result<(), GfError> {
    match expr {
        Expr::Literal(_) | Expr::Var(_) | Expr::LabelPredicate(_) => {}
        Expr::Param(reference) => {
            parameters.insert(reference.name.clone());
        }
        Expr::Property(property) => visit_expr(&property.object, parameters)?,
        Expr::BinaryOp(binary) => {
            visit_expr(&binary.left, parameters)?;
            visit_expr(&binary.right, parameters)?;
        }
        Expr::UnaryOp(unary) => visit_expr(&unary.expr, parameters)?,
        Expr::FunctionCall(function) => {
            for arg in &function.args {
                visit_expr(arg, parameters)?;
            }
        }
        Expr::List(list) => {
            for element in &list.elements {
                visit_expr(element, parameters)?;
            }
        }
        Expr::Map(map) => {
            for value in map.entries.values() {
                visit_expr(value, parameters)?;
            }
        }
        Expr::Case(case) => {
            for expr in case.subject.iter().chain(&case.else_expr) {
                visit_expr(expr, parameters)?;
            }
            for branch in &case.when_clauses {
                visit_expr(&branch.condition, parameters)?;
                visit_expr(&branch.result, parameters)?;
            }
        }
        Expr::ListComprehension(list) => {
            visit_expr(&list.list, parameters)?;
            for expr in list.filter.iter().chain(&list.projection) {
                visit_expr(expr, parameters)?;
            }
        }
        Expr::Quantifier(quantifier) => {
            visit_expr(&quantifier.list, parameters)?;
            visit_expr(&quantifier.predicate, parameters)?;
        }
        Expr::PatternComprehension(pattern) => {
            visit_pattern(&pattern.pattern, parameters)?;
            if let Some(filter) = &pattern.filter {
                visit_expr(filter, parameters)?;
            }
            visit_expr(&pattern.projection, parameters)?;
        }
        Expr::PatternPredicate(pattern) => visit_pattern(&pattern.pattern, parameters)?,
        Expr::ExistentialSubquery(subquery) => match &subquery.body {
            ExistentialSubqueryBody::Simple { pattern, filter } => {
                visit_pattern(pattern, parameters)?;
                if let Some(filter) = filter {
                    visit_expr(filter, parameters)?;
                }
            }
            ExistentialSubqueryBody::Full(query) => visit_query(query, parameters)?,
        },
        Expr::IsNull { expr, .. } => visit_expr(expr, parameters)?,
        Expr::InList { expr, list, .. } => {
            visit_expr(expr, parameters)?;
            visit_expr(list, parameters)?;
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            visit_expr(expr, parameters)?;
            visit_expr(pattern, parameters)?;
        }
        Expr::Parenthesized { inner, .. } => visit_expr(inner, parameters)?,
        _ => return Err(refused()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_comment_only_queries_are_refused() {
        for query in ["", "   ", "// no query", "/* no query */"] {
            assert!(read_only_query_parameters(query).is_err());
        }
    }

    #[test]
    fn parameters_cover_patterns_projections_and_nested_expressions() {
        let cases = [
            (
                "MATCH (n:Missing {value: $node})-[r:R {value: $rel}]->(m) WHERE n.x = $where WITH n, $with AS x ORDER BY $sort SKIP $skip LIMIT $limit WHERE x = $after RETURN $out",
                vec![
                    "node", "rel", "where", "with", "sort", "skip", "limit", "after", "out",
                ],
            ),
            (
                "RETURN CASE $subject WHEN $condition THEN [$yes, {value: $map}] ELSE $no END",
                vec!["subject", "condition", "yes", "map", "no"],
            ),
            (
                "RETURN [x IN $list WHERE x > $filter | x + $projection], any(x IN $quantified WHERE x = $predicate)",
                vec!["list", "filter", "projection", "quantified", "predicate"],
            ),
            (
                "MATCH (n) RETURN [(n)-[r:R {x: $pattern}]->(m) WHERE r.x > $filter | $projection], exists { MATCH (n)-[:R]->(m) WHERE m.x = $nested RETURN $result }",
                vec!["pattern", "filter", "projection", "nested", "result"],
            ),
            (
                "UNWIND [$input] AS x RETURN ($unary + size([$function])) IS NULL, $left IN [$right], $text STARTS WITH $prefix, $text =~ $regex",
                vec![
                    "input", "unary", "function", "left", "right", "text", "prefix", "regex",
                ],
            ),
            (
                "RETURN $first UNION ALL RETURN $second",
                vec!["first", "second"],
            ),
            (
                "RETURN '$ignored', {create: $real}, '$real' /* $comment */",
                vec!["real"],
            ),
        ];
        for (query, expected) in cases {
            assert_eq!(
                read_only_query_parameters(query).unwrap(),
                expected.into_iter().map(String::from).collect(),
                "{query}"
            );
        }
    }

    #[test]
    fn syntax_only_admission_refuses_mutations_and_calls_including_nested_queries() {
        for query in [
            "CREATE (n)",
            "MERGE (n)",
            "MATCH (n) SET n.x = 1",
            "MATCH (n) REMOVE n.x",
            "MATCH (n) DELETE n",
            "MATCH (n) DETACH DELETE n",
            "CALL unsafe.procedure() RETURN 1",
            "RETURN 1 UNION CREATE (n) RETURN n",
            "RETURN exists { CREATE (n) RETURN n }",
            "RETURN exists { CALL unsafe.procedure() RETURN 1 }",
            "RETURN",
        ] {
            assert!(read_only_query_parameters(query).is_err(), "{query}");
        }
        assert!(
            read_only_query_parameters(
                "MATCH (n:Create {delete: 1}) RETURN n.set, 'CALL unsafe()'"
            )
            .is_ok()
        );
    }
}
