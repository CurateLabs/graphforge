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
    any_expression(expr, &|expr| {
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
        state.vars.get(&var.name).is_some_and(|id| {
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
                let mut forwarded = HashMap::new();
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
                            let depends_on_row = any_expression(properties, &|expr| {
                                matches!(expr, Expr::Property(property)
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

fn any_expression(expr: &Expr, predicate: &impl Fn(&Expr) -> bool) -> bool {
    if predicate(expr) {
        return true;
    }
    match expr {
        Expr::Property(property) => any_expression(&property.object, predicate),
        Expr::BinaryOp(binary) => {
            any_expression(&binary.left, predicate) || any_expression(&binary.right, predicate)
        }
        Expr::UnaryOp(unary) => any_expression(&unary.expr, predicate),
        Expr::Parenthesized { inner, .. } | Expr::IsNull { expr: inner, .. } => {
            any_expression(inner, predicate)
        }
        Expr::FunctionCall(call) => call.args.iter().any(|arg| any_expression(arg, predicate)),
        Expr::List(list) => list
            .elements
            .iter()
            .any(|item| any_expression(item, predicate)),
        Expr::Map(map) => map
            .entries
            .values()
            .any(|value| any_expression(value, predicate)),
        Expr::Case(case) => {
            case.subject
                .as_deref()
                .is_some_and(|expr| any_expression(expr, predicate))
                || case.when_clauses.iter().any(|when| {
                    any_expression(&when.condition, predicate)
                        || any_expression(&when.result, predicate)
                })
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(|expr| any_expression(expr, predicate))
        }
        Expr::ListComprehension(list) => {
            any_expression(&list.list, predicate)
                || list
                    .filter
                    .as_deref()
                    .is_some_and(|expr| any_expression(expr, predicate))
                || list
                    .projection
                    .as_deref()
                    .is_some_and(|expr| any_expression(expr, predicate))
        }
        Expr::Quantifier(quantifier) => {
            any_expression(&quantifier.list, predicate)
                || any_expression(&quantifier.predicate, predicate)
        }
        Expr::InList { expr, list, .. } => {
            any_expression(expr, predicate) || any_expression(list, predicate)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            any_expression(expr, predicate) || any_expression(pattern, predicate)
        }
        _ => false,
    }
}
