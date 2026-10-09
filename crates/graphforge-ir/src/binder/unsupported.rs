//! Specific diagnoses for recognized workload constructs without execution support.

use std::collections::{HashMap, HashSet};

use graphforge_ast::{AstClause, AstQuery, Expr, FunctionCall, PathElement};
use graphforge_core::{BindError, BindErrorKind, UnsupportedCypherFeature};

use super::patterns::strip_parens;
use super::{BinderState, VarKind, is_function_named};

pub(super) fn diagnostic(
    feature: UnsupportedCypherFeature,
    span: graphforge_core::Span,
) -> BindError {
    BindError::new(
        BindErrorKind::UnsupportedFeature(feature),
        span,
        format!("{} are not supported", feature.description()),
    )
}

/// Called only after the supported bound endpoint rewrite has been considered.
pub(super) fn function(call: &FunctionCall) -> Option<UnsupportedCypherFeature> {
    if call.args.len() != 1 || call.star || call.distinct {
        return None;
    }
    if is_function_named(call, "id") {
        return Some(UnsupportedCypherFeature::IdentityFunction);
    }
    if is_function_named(call, "startNode") || is_function_named(call, "endNode") {
        return Some(UnsupportedCypherFeature::UnboundRelationshipEndpoint);
    }
    if is_function_named(call, "duration")
        && let Expr::Map(map) = strip_parens(&call.args[0])
        && map
            .entries
            .values()
            .any(|value| !matches!(strip_parens(value), Expr::Literal(_)))
    {
        return Some(UnsupportedCypherFeature::DynamicDuration);
    }
    None
}

/// Restrict the diagnosis to a property of an indexed relationship list. Scalar
/// lists, map lists, fixed-hop relationships, and plain ALL remain supported.
pub(super) fn indexed_relationship_property(expr: &Expr, state: &BinderState) -> bool {
    any_expression(expr, &|expr, locals| {
        let Expr::Property(property) = expr else {
            return false;
        };
        let Expr::FunctionCall(call) = strip_parens(&property.object) else {
            return false;
        };
        if !is_function_named(call, "_subscript") || call.args.len() != 2 {
            return false;
        }
        let Expr::Var(var) = strip_parens(&call.args[0]) else {
            return false;
        };
        !locals.contains(&var.name.as_str())
            && state.vars.get(&var.name).is_some_and(|id| {
                state.var_kinds.get(id) == Some(&VarKind::Relationship)
                    && !state.edge_vars.contains_key(id)
            })
    })
}

/// Parameter row values currently lack the independent owner projections needed
/// by property-constrained MATCH scans across different labels. Track only row
/// aliases introduced by UNWIND parameters and their property-dependent scans.
pub(super) fn parameter_rows_across_labels(query: &AstQuery) -> Option<BindError> {
    let mut rows: HashMap<String, HashSet<String>> = HashMap::new();
    for clause in &query.clauses {
        match clause {
            AstClause::Unwind(unwind) => {
                rows.remove(&unwind.alias);
                if matches!(strip_parens(&unwind.expr), Expr::Param(_)) {
                    rows.insert(unwind.alias.clone(), HashSet::new());
                }
            }
            AstClause::With(with) => {
                let mut forwarded = if with.items.iter().any(
                    |item| matches!(strip_parens(&item.expr), Expr::Var(var) if var.name == "*"),
                ) {
                    rows.clone()
                } else {
                    HashMap::new()
                };
                for item in &with.items {
                    if let Expr::Var(var) = strip_parens(&item.expr)
                        && let Some(labels) = rows.get(&var.name)
                    {
                        forwarded.insert(
                            item.alias.as_ref().unwrap_or(&var.name).clone(),
                            labels.clone(),
                        );
                    }
                }
                rows = forwarded;
            }
            AstClause::Match(matched) | AstClause::OptionalMatch(matched) => {
                for pattern in &matched.patterns {
                    for element in &pattern.elements {
                        let PathElement::Node(node) = element else {
                            continue;
                        };
                        let Some(properties) = &node.properties else {
                            continue;
                        };
                        for (alias, labels) in &mut rows {
                            let depends_on_row = any_expression(properties, &|expr, locals| {
                                !locals.contains(&alias.as_str())
                                    && matches!(expr, Expr::Property(property)
                                    if matches!(strip_parens(&property.object), Expr::Var(var) if &var.name == alias))
                            });
                            if depends_on_row {
                                labels.extend(node.labels.iter().cloned());
                                if labels.len() > 1 {
                                    return Some(diagnostic(
                                        UnsupportedCypherFeature::ParameterRowsAcrossLabels,
                                        node.span,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            AstClause::Union(_) => rows.clear(),
            _ => {}
        }
    }
    None
}

fn any_expression(expr: &Expr, predicate: &impl Fn(&Expr, &[&str]) -> bool) -> bool {
    scoped_expression(expr, predicate, &mut Vec::new())
}

fn scoped_expression<'a>(
    expr: &'a Expr,
    predicate: &impl Fn(&Expr, &[&str]) -> bool,
    locals: &mut Vec<&'a str>,
) -> bool {
    if predicate(expr, locals) {
        return true;
    }
    match expr {
        Expr::Property(property) => scoped_expression(&property.object, predicate, locals),
        Expr::BinaryOp(binary) => {
            scoped_expression(&binary.left, predicate, locals)
                || scoped_expression(&binary.right, predicate, locals)
        }
        Expr::UnaryOp(unary) => scoped_expression(&unary.expr, predicate, locals),
        Expr::Parenthesized { inner, .. } | Expr::IsNull { expr: inner, .. } => {
            scoped_expression(inner, predicate, locals)
        }
        Expr::FunctionCall(call) => call
            .args
            .iter()
            .any(|arg| scoped_expression(arg, predicate, locals)),
        Expr::List(list) => list
            .elements
            .iter()
            .any(|item| scoped_expression(item, predicate, locals)),
        Expr::Map(map) => map
            .entries
            .values()
            .any(|value| scoped_expression(value, predicate, locals)),
        Expr::Case(case) => {
            case.subject
                .as_deref()
                .is_some_and(|expr| scoped_expression(expr, predicate, locals))
                || case.when_clauses.iter().any(|when| {
                    scoped_expression(&when.condition, predicate, locals)
                        || scoped_expression(&when.result, predicate, locals)
                })
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(|expr| scoped_expression(expr, predicate, locals))
        }
        Expr::ListComprehension(list) => {
            if scoped_expression(&list.list, predicate, locals) {
                return true;
            }
            locals.push(&list.var);
            let found = list
                .filter
                .as_deref()
                .is_some_and(|expr| scoped_expression(expr, predicate, locals))
                || list
                    .projection
                    .as_deref()
                    .is_some_and(|expr| scoped_expression(expr, predicate, locals));
            locals.pop();
            found
        }
        Expr::Quantifier(quantifier) => {
            if scoped_expression(&quantifier.list, predicate, locals) {
                return true;
            }
            locals.push(&quantifier.var);
            let found = scoped_expression(&quantifier.predicate, predicate, locals);
            locals.pop();
            found
        }
        Expr::InList { expr, list, .. } => {
            scoped_expression(expr, predicate, locals) || scoped_expression(list, predicate, locals)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            scoped_expression(expr, predicate, locals)
                || scoped_expression(pattern, predicate, locals)
        }
        _ => false,
    }
}
