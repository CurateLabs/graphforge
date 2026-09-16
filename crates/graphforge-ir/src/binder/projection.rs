//! Projection, aggregation, grouping rewrites, and row-count binding.

use super::patterns::{expr_contains_pattern_predicate, strip_parens};
use super::{
    BindError, BindErrorKind, Binder, BinderState, PathBinding, VarKind, alloc_anon_var,
    ensure_var_name, is_function_named,
};
use crate::expr::IrExpr;
use crate::plan::{GraphOp, SortKey};
use crate::{AggExpr, AggFunc, ExprId, ProjectItem, SortOrder, VarId};
use graphforge_ast::{
    ExistentialSubqueryBody, Expr, Literal, PathElement, PathPattern, ReturnItem, SortItem,
    SortOrder as AstSortOrder, UnaryOpKind as AstUnOp, VarRef, WithClause,
};
use graphforge_core::Span;
use std::collections::HashSet;

impl Binder {
    // Item classification (aggregate/node-forward/path/scalar) + scope reset +
    // ORDER BY/SKIP/LIMIT make this long but linear, like the RETURN lowering.
    #[allow(clippy::too_many_lines)]
    pub(super) fn lower_with(&self, w: &WithClause, s: &mut BinderState) {
        type ForwardedEdge = (VarId, Option<String>, Option<(VarId, VarId)>);

        let items_ast = expand_projection_wildcard(&w.items, s);
        check_duplicate_aliases(&items_ast, s);
        if w.order_by
            .as_ref()
            .is_some_and(|order_by| has_unprojected_order_aggregate(&items_ast, &order_by.items))
        {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                w.order_by.as_ref().expect("checked above").span,
                "an aggregate function in ORDER BY must also appear in the WITH projection",
            ));
            return;
        }
        // WITH projects/renames the pipeline and then *resets the scope*: only
        // the projected aliases are visible to subsequent clauses (#814).
        //
        // Each item's expression is lowered in the CURRENT scope (it references
        // upstream vars), then a fresh `out_var` is minted for its alias and
        // becomes the downstream scope. The lowerer maps `out_var` to the
        // projected column.
        //
        // Aggregation in WITH (#958): a WITH with a top-level aggregate
        // (`WITH a.name AS name, count(*) AS c`) lowers to a `GraphOp::Aggregate`
        // (mirroring RETURN's implicit grouping) instead of a `Project`. A NESTED
        // aggregate inside a larger expression (`count(n) + 1 AS c`) uses an
        // aggregate→project decomposition with a re-bound scope. An aggregate
        // inside another aggregate's argument (`sum(count(*))`) is invalid.
        let has_nested_agg = items_ast.iter().any(|i| match agg_func_of(&i.expr) {
            // A top-level aggregate is fine only if its arguments hold no further
            // aggregate (Cypher forbids nesting, so this is a malformed shape).
            Some(_) => match &i.expr {
                Expr::FunctionCall(call) => call.args.iter().any(expr_contains_aggregate),
                _ => false,
            },
            // A non-aggregate item must contain no aggregate at all.
            None => expr_contains_aggregate(&i.expr),
        });
        let has_aggregate_inside_aggregate = items_ast
            .iter()
            .any(|item| expr_contains_aggregate_inside_aggregate(&item.expr, false));
        if has_aggregate_inside_aggregate {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                w.span,
                "an aggregate function may not contain another aggregate function",
            ));
            return;
        }
        if has_nested_agg && items_ast.iter().any(|i| expr_contains_aggregate(&i.expr)) {
            self.lower_with_aggregate_arith(w, &items_ast, s);
            return;
        }
        if !has_nested_agg && items_ast.iter().any(|i| agg_func_of(&i.expr).is_some()) {
            self.lower_with_aggregate(w, &items_ast, s);
            return;
        }
        let mut items: Vec<ProjectItem> = Vec::with_capacity(items_ast.len());
        let mut new_scope: Vec<(String, VarId)> = Vec::new();
        // Entity variables forwarded WHOLE (`WITH n`, `WITH r`): restore the
        // binder-side metadata after the scope reset, so a later `RETURN n` /
        // `n.x` or `RETURN r` / `r.x` still treats them as entities.
        let mut forwarded_nodes: Vec<(VarId, Option<String>)> = Vec::new();
        let mut forwarded_edges: Vec<ForwardedEdge> = Vec::new();
        let mut forwarded_paths: Vec<(String, PathBinding)> = Vec::new();
        let mut forwarded_path_vars = HashSet::new();
        for item in &items_ast {
            // Reject (rather than silently mishandle) the parts not yet
            // supported in WITH — these would otherwise produce wrong results.
            // The aggregate check is recursive: `WITH count(n) + 1 AS c` must be
            // caught too, not only a top-level `WITH count(n) AS c`.
            if expr_contains_aggregate(&item.expr) {
                s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    item.span,
                    "aggregation in WITH is not yet supported (#814 follow-up)".to_string(),
                ));
            }

            // A bare entity variable (`WITH n` or `WITH n AS m`) is carried
            // through whole. Each output alias gets a fresh var while the
            // source expression retains the incoming var, allowing the lowerer
            // to re-qualify every entity column independently. Forwarding a
            // path is still deferred.
            let node_forward = match &item.expr {
                Expr::Var(VarRef { name, .. }) => s
                    .vars
                    .get(name)
                    .copied()
                    .filter(|v| s.node_vars.contains_key(v)),
                _ => None,
            };
            let edge_forward = match &item.expr {
                Expr::Var(VarRef { name, .. }) => s
                    .vars
                    .get(name)
                    .copied()
                    .filter(|v| s.edge_rel_names.contains_key(v)),
                _ => None,
            };
            let computed_node_label = match &item.expr {
                Expr::FunctionCall(call)
                    if is_function_named(call, "coalesce") && !call.args.is_empty() =>
                {
                    call.args
                        .iter()
                        .map(|arg| match arg {
                            Expr::Var(VarRef { name, .. }) => s
                                .vars
                                .get(name)
                                .and_then(|var| s.node_vars.get(var))
                                .cloned(),
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>()
                        .map(|labels| labels.into_iter().flatten().next())
                }
                _ => None,
            };
            let path_forward = match &item.expr {
                Expr::Var(VarRef { name, .. }) => s.path_vars.get(name).cloned(),
                _ => None,
            };
            let expr_id = if computed_node_label.is_some() || path_forward.is_some() {
                self.lower_return_item_expr(&item.expr, item.span, s, true)
            } else {
                self.lower_expr(&item.expr, item.span, s)
            };

            if let Some(v) = node_forward {
                // Each alias gets its own output var. Reusing `v` for both
                // `WITH n AS a, n AS b` would project the same qualified entity
                // columns twice and make the downstream schema ambiguous.
                let label = s.node_vars.get(&v).cloned().flatten();
                let expr_name = match &item.expr {
                    Expr::Var(VarRef { name, .. }) => name.clone(),
                    _ => unreachable!("node_forward is Some only for a Var item"),
                };
                let name = item.alias.clone().unwrap_or(expr_name);
                let out_var = alloc_anon_var(s);
                forwarded_nodes.push((out_var, label));
                new_scope.push((name.clone(), out_var));
                items.push(ProjectItem {
                    expr: expr_id,
                    alias: Some(name),
                    out_var: Some(out_var),
                });
                continue;
            }
            if let Some(v) = edge_forward {
                let rel_name = s.edge_rel_names.get(&v).cloned().flatten();
                let endpoints = s.edge_vars.get(&v).copied();
                let expr_name = match &item.expr {
                    Expr::Var(VarRef { name, .. }) => name.clone(),
                    _ => unreachable!("edge_forward is Some only for a Var item"),
                };
                let name = item.alias.clone().unwrap_or(expr_name);
                let out_var = alloc_anon_var(s);
                forwarded_edges.push((out_var, rel_name, endpoints));
                new_scope.push((name.clone(), out_var));
                items.push(ProjectItem {
                    expr: expr_id,
                    alias: Some(name),
                    out_var: Some(out_var),
                });
                continue;
            }

            // Alias: explicit (`expr AS name`), or the variable's own name for a
            // bare scalar-variable pass-through (`WITH x`).
            let alias = item.alias.clone().or_else(|| match &item.expr {
                Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                _ => None,
            });
            let Some(alias) = alias else {
                s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    item.span,
                    "a non-variable WITH item must be aliased (`expr AS name`)".to_string(),
                ));
                continue;
            };
            let out_var = alloc_anon_var(s);
            if matches!(item.expr, Expr::Literal(Literal::Null(_))) {
                s.var_kinds.insert(out_var, VarKind::Unknown);
            }
            if let Some(label) = computed_node_label {
                forwarded_nodes.push((out_var, label));
            }
            if let Some(binding) = path_forward {
                s.var_kinds.insert(out_var, VarKind::Unknown);
                forwarded_path_vars.extend(binding.nodes.iter().copied());
                forwarded_path_vars.extend(binding.segments.iter().map(|segment| segment.edge));
                forwarded_paths.push((alias.clone(), binding));
            }
            if let Expr::List(graphforge_ast::ListLiteral { elements, .. }) = &item.expr
                && !elements.is_empty()
                && elements.iter().all(|element| {
                    matches!(element, Expr::Var(VarRef { name, .. })
                        if s.vars.get(name).is_some_and(|var| s.edge_rel_names.contains_key(var)))
                })
            {
                forwarded_edges.push((out_var, None, None));
            }
            new_scope.push((alias.clone(), out_var));
            items.push(ProjectItem {
                expr: expr_id,
                alias: Some(alias),
                out_var: Some(out_var),
            });
        }

        let mut forwarded_path_vars = forwarded_path_vars.into_iter().collect::<Vec<_>>();
        forwarded_path_vars.sort_by_key(|var| var.0);
        for var in forwarded_path_vars {
            let expr = s.builder.push_expr(IrExpr::VarRef(var));
            items.push(ProjectItem {
                expr,
                alias: Some(format!("__gf_path_component_{}", var.0)),
                out_var: Some(var),
            });
            if let Some(label) = s.node_vars.get(&var).cloned() {
                forwarded_nodes.push((var, label));
            }
            if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                forwarded_edges.push((var, rel_name, s.edge_vars.get(&var).copied()));
            }
        }

        // A scalar WITH WHERE can be evaluated against the incoming row after
        // substituting projected aliases back to their source expressions. This
        // keeps hidden incoming columns available for both WHERE and ORDER BY,
        // then lets the WITH operator perform only the final scope reset.
        if let Some(wc) = w
            .where_clause
            .as_ref()
            .filter(|wc| !expr_contains_pattern_predicate(&wc.predicate))
        {
            let predicate = rewrite_projection_alias_refs(wc.predicate.clone(), &items_ast);
            let predicate = self.lower_expr(&predicate, wc.span, s);
            s.builder.push_op_mut(GraphOp::Filter { predicate });
        }
        let where_pattern_predicate = w
            .where_clause
            .as_ref()
            .filter(|wc| expr_contains_pattern_predicate(&wc.predicate))
            .cloned();

        let projection_bindings: Vec<(Expr, String, VarId)> = items_ast
            .iter()
            .filter_map(|item| {
                let alias = item.alias.clone().or_else(|| match &item.expr {
                    Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                    _ => None,
                })?;
                let var = new_scope
                    .iter()
                    .find_map(|(name, var)| (name == &alias).then_some(*var))?;
                Some((item.expr.clone(), alias, var))
            })
            .collect();

        // A non-DISTINCT ORDER BY may use incoming expressions that are not
        // projected. Bind it while that scope still exists and execute it before
        // WITH drops the hidden columns. Projected aliases are substituted back
        // to their source expressions for this pre-projection sort.
        let projected_names = projection_bindings
            .iter()
            .map(|(_, alias, _)| alias.as_str())
            .collect::<std::collections::HashSet<_>>();
        let sort_fits_projection = w.order_by.as_ref().is_none_or(|order_by| {
            order_by.items.iter().all(|item| {
                let rewritten = rewrite_grouping_refs(item.expr.clone(), &projection_bindings);
                let mut refs = Vec::new();
                collect_grouping_refs(&rewritten, &mut refs);
                refs.iter().all(|reference| {
                    grouping_ref_root_name(reference)
                        .is_some_and(|name| projected_names.contains(name))
                })
            })
        });
        let sort_before_projection = w.order_by.is_some() && !w.distinct && !sort_fits_projection;
        if sort_before_projection {
            self.push_sort_before_projection(
                &w.order_by.as_ref().expect("checked above").items,
                &items_ast,
                s,
            );
        }

        // Scope reset: drop everything, then introduce only the WITH aliases
        // (and re-register forwarded whole-node vars as nodes).
        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (name, v) in new_scope {
            s.vars.insert(name, v);
        }
        for (v, label) in forwarded_nodes {
            s.node_vars.insert(v, label);
        }
        for (v, rel_name, endpoints) in forwarded_edges {
            s.edge_rel_names.insert(v, rel_name);
            if let Some(endpoints) = endpoints {
                s.edge_vars.insert(v, endpoints);
            }
        }
        for (name, binding) in forwarded_paths {
            s.path_vars.insert(name, binding);
        }

        s.builder.push_op_mut(GraphOp::With {
            items,
            distinct: w.distinct,
            where_predicate: None,
        });
        if let Some(wc) = where_pattern_predicate {
            self.lower_where_predicate(&wc.predicate, wc.span, s);
        }
        if let Some(ob) = &w.order_by
            && !sort_before_projection
        {
            self.push_sort_rewritten(&ob.items, &projection_bindings, s);
        }
        push_skip_limit(self, w.skip.as_ref(), w.limit.as_ref(), s);
    }

    /// Lower a WITH containing a top-level aggregate (#958) into a
    /// [`GraphOp::Aggregate`] that also introduces the post-aggregate scope:
    /// non-aggregate items become group-by keys, aggregates become
    /// [`AggExpr`]s, and every output column is bound to a fresh `out_var` so
    /// the lowerer's decomposed-aggregate path resets the scope to exactly the
    /// projected aliases (the same scope-reset contract a plain WITH enforces).
    /// A following `WHERE` becomes a `Filter` over those aliases; `ORDER BY` /
    /// `SKIP` / `LIMIT` follow as for a non-aggregate WITH.
    ///
    /// Whole node, relationship, and path keys retain their original variables;
    /// the relational lowerer groups their qualified physical columns and keeps
    /// those variables available to downstream graph operators.
    #[allow(clippy::too_many_lines)]
    fn lower_with_aggregate(&self, w: &WithClause, items: &[ReturnItem], s: &mut BinderState) {
        let mut group_by: Vec<ExprId> = Vec::new();
        let mut group_aliases: Vec<Option<String>> = Vec::new();
        let mut group_vars: Vec<Option<VarId>> = Vec::new();
        let mut aggs: Vec<AggExpr> = Vec::new();
        let mut new_scope: Vec<(String, VarId)> = Vec::new();
        let mut forwarded_nodes: Vec<(VarId, Option<String>)> = Vec::new();
        let mut forwarded_edges: Vec<ForwardedEdgeBinding> = Vec::new();
        let mut forwarded_paths: Vec<(String, PathBinding)> = Vec::new();
        let mut projection_bindings: Vec<(Expr, String, VarId)> = Vec::new();

        for item in items {
            // Output column name: explicit `AS alias`, or a bare variable's name.
            let alias = item.alias.clone().or_else(|| match &item.expr {
                Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                _ => None,
            });

            if let Some(func) = agg_func_of(&item.expr) {
                let Expr::FunctionCall(call) = &item.expr else {
                    unreachable!("agg_func_of only matches FunctionCall");
                };
                let Some(alias) = alias else {
                    s.errors.push(BindError::new(
                        BindErrorKind::UnsupportedClause,
                        item.span,
                        "an aggregate in WITH must be aliased (`count(*) AS name`)".to_string(),
                    ));
                    continue;
                };
                let out_var = alloc_anon_var(s);
                aggs.push(self.build_agg(call, func, alias.clone(), Some(out_var), s));
                projection_bindings.push((item.expr.clone(), alias.clone(), out_var));
                new_scope.push((alias, out_var));
            } else {
                if let Expr::Var(VarRef { name, .. }) = &item.expr
                    && let Some(binding) = s.path_vars.get(name).cloned()
                {
                    let Some(alias) = alias else {
                        unreachable!("a bare path always has its variable name as alias")
                    };
                    let mut vars = binding.nodes.clone();
                    vars.extend(binding.segments.iter().map(|segment| segment.edge));
                    vars.sort_by_key(|var| var.0);
                    vars.dedup();
                    for var in vars {
                        let expr = s.builder.push_expr(IrExpr::VarRef(var));
                        group_by.push(expr);
                        group_aliases.push(None);
                        group_vars.push(Some(var));
                        if let Some(label) = s.node_vars.get(&var).cloned() {
                            forwarded_nodes.push((var, label));
                        }
                        if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                            forwarded_edges.push(ForwardedEdgeBinding {
                                var,
                                rel_name,
                                endpoints: s.edge_vars.get(&var).copied(),
                            });
                        }
                    }
                    forwarded_paths.push((alias, binding));
                    continue;
                }

                let entity_var = match &item.expr {
                    Expr::Var(VarRef { name, .. }) => s.vars.get(name).copied().filter(|var| {
                        s.node_vars.contains_key(var) || s.edge_rel_names.contains_key(var)
                    }),
                    Expr::FunctionCall(call) => Self::resolve_endpoint_node(call, s),
                    _ => None,
                };
                if let Some(var) = entity_var {
                    let Some(alias) = alias else {
                        unreachable!("a graph variable always has an output alias")
                    };
                    let expr = s.builder.push_expr(IrExpr::VarRef(var));
                    group_by.push(expr);
                    group_aliases.push(None);
                    group_vars.push(Some(var));
                    projection_bindings.push((item.expr.clone(), alias.clone(), var));
                    new_scope.push((alias, var));
                    if let Some(label) = s.node_vars.get(&var).cloned() {
                        forwarded_nodes.push((var, label));
                    }
                    if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                        forwarded_edges.push(ForwardedEdgeBinding {
                            var,
                            rel_name,
                            endpoints: s.edge_vars.get(&var).copied(),
                        });
                    }
                    continue;
                }
                let Some(alias) = alias else {
                    s.errors.push(BindError::new(
                        BindErrorKind::UnsupportedClause,
                        item.span,
                        "a non-variable WITH item must be aliased (`expr AS name`)".to_string(),
                    ));
                    continue;
                };
                let expr_id = self.lower_expr(&item.expr, item.span, s);
                let out_var = alloc_anon_var(s);
                group_by.push(expr_id);
                group_aliases.push(Some(alias.clone()));
                group_vars.push(Some(out_var));
                projection_bindings.push((item.expr.clone(), alias.clone(), out_var));
                new_scope.push((alias, out_var));
            }
        }
        // Scope reset: WITH exposes only its projected aliases downstream.
        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (name, v) in new_scope {
            s.vars.insert(name, v);
        }
        for (var, label) in forwarded_nodes {
            s.node_vars.insert(var, label);
        }
        for edge in forwarded_edges {
            s.edge_rel_names.insert(edge.var, edge.rel_name);
            if let Some(endpoints) = edge.endpoints {
                s.edge_vars.insert(edge.var, endpoints);
            }
        }
        for (name, binding) in forwarded_paths {
            s.path_vars.insert(name, binding);
        }

        s.builder.push_op_mut(GraphOp::Aggregate {
            group_by,
            group_aliases,
            group_vars,
            aggs,
        });

        // WHERE over the post-aggregate aliases cannot inline into the Aggregate,
        // so it becomes a Filter over the new scope.
        if let Some(wc) = &w.where_clause {
            let pred = self.lower_expr(&wc.predicate, wc.span, s);
            s.builder.push_op_mut(GraphOp::Filter { predicate: pred });
        }
        if let Some(ob) = &w.order_by {
            self.push_sort_rewritten(&ob.items, &projection_bindings, s);
        }
        push_skip_limit(self, w.skip.as_ref(), w.limit.as_ref(), s);
    }

    /// Lower aggregate calls nested inside WITH expressions as Aggregate -> Project.
    /// Non-aggregate WITH items define the only legal implicit grouping leaves.
    #[allow(clippy::too_many_lines)]
    fn lower_with_aggregate_arith(
        &self,
        w: &WithClause,
        items: &[ReturnItem],
        s: &mut BinderState,
    ) {
        let group_items: Vec<&ReturnItem> = items
            .iter()
            .filter(|item| !expr_contains_aggregate(&item.expr))
            .collect();
        let mut group_by = Vec::with_capacity(group_items.len());
        let mut group_aliases = Vec::with_capacity(group_items.len());
        let mut group_vars = Vec::with_capacity(group_items.len());
        let mut group_bindings: Vec<(Expr, String, VarId)> = Vec::with_capacity(group_items.len());
        let mut grouped_nodes: Vec<(String, VarId, Option<String>)> = Vec::new();
        let mut grouped_edges: Vec<GroupedEdgeBinding> = Vec::new();
        let mut grouped_paths: Vec<(String, PathBinding)> = Vec::new();
        let mut grouped_path_nodes: Vec<(VarId, Option<String>)> = Vec::new();
        let mut grouped_path_edges: Vec<ForwardedEdgeBinding> = Vec::new();

        for item in &group_items {
            let alias = item.alias.clone().or_else(|| match &item.expr {
                Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                _ => None,
            });
            let Some(alias) = alias else {
                s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    item.span,
                    "a non-variable WITH item must be aliased (`expr AS name`)",
                ));
                continue;
            };
            if let Expr::Var(VarRef { name, .. }) = &item.expr
                && let Some(binding) = s.path_vars.get(name).cloned()
            {
                let mut vars = binding.nodes.clone();
                vars.extend(binding.segments.iter().map(|segment| segment.edge));
                vars.sort_by_key(|var| var.0);
                vars.dedup();
                for var in vars {
                    group_by.push(s.builder.push_expr(IrExpr::VarRef(var)));
                    group_aliases.push(None);
                    group_vars.push(Some(var));
                    if let Some(label) = s.node_vars.get(&var).cloned() {
                        grouped_path_nodes.push((var, label));
                    }
                    if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                        grouped_path_edges.push(ForwardedEdgeBinding {
                            var,
                            rel_name,
                            endpoints: s.edge_vars.get(&var).copied(),
                        });
                    }
                }
                grouped_paths.push((alias, binding));
                continue;
            }
            let entity_var = match &item.expr {
                Expr::Var(VarRef { name, .. }) => s.vars.get(name).copied().filter(|var| {
                    s.node_vars.contains_key(var) || s.edge_rel_names.contains_key(var)
                }),
                Expr::FunctionCall(call) => Self::resolve_endpoint_node(call, s),
                _ => None,
            };
            if let Some(var) = entity_var {
                group_by.push(s.builder.push_expr(IrExpr::VarRef(var)));
                group_aliases.push(None);
                group_vars.push(Some(var));
                group_bindings.push((item.expr.clone(), alias.clone(), var));
                if let Some(label) = s.node_vars.get(&var).cloned() {
                    grouped_nodes.push((alias, var, label));
                } else if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                    grouped_edges.push(GroupedEdgeBinding {
                        alias,
                        var,
                        rel_name,
                        endpoints: s.edge_vars.get(&var).copied(),
                    });
                }
                continue;
            }

            let out_var = alloc_anon_var(s);
            group_by.push(self.lower_expr(&item.expr, item.span, s));
            group_aliases.push(Some(alias.clone()));
            group_vars.push(Some(out_var));
            group_bindings.push((item.expr.clone(), alias, out_var));
        }

        let mut aggs = Vec::new();
        let mut aggregate_bindings = Vec::new();
        let mut rewritten_items = Vec::with_capacity(items.len());
        for item in items {
            let alias = item.alias.clone().or_else(|| match &item.expr {
                Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                _ => None,
            });
            let Some(alias) = alias else {
                s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    item.span,
                    "an aggregate expression in WITH must be aliased (`expr AS name`)",
                ));
                continue;
            };

            if expr_contains_aggregate(&item.expr) {
                let mut refs = Vec::new();
                collect_grouping_refs(&item.expr, &mut refs);
                let ambiguous = refs.iter().any(|reference| {
                    !group_bindings.iter().any(|(group, _, _)| {
                        is_atomic_grouping_expr(group) && same_grouping_expr(reference, group)
                    })
                });
                if ambiguous {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        item.span,
                        "ambiguous aggregation expression: every variable or property outside an \
                         aggregate must be projected as its own WITH grouping key",
                    ));
                }
                let first_agg = aggs.len();
                let rewritten = self.rewrite_aggs(&item.expr, &mut aggs, s);
                for (index, agg) in aggs.iter().enumerate().skip(first_agg) {
                    if let Some(var) = agg.out_var {
                        aggregate_bindings.push((format!("__agg_{index}"), var));
                    }
                }
                if aggs.len() == first_agg + 1 {
                    aggs[first_agg].alias.clone_from(&alias);
                }
                rewritten_items.push((rewritten, alias, item.span));
            } else {
                rewritten_items.push((
                    Expr::Var(VarRef {
                        name: alias.clone(),
                        span: item.span,
                    }),
                    alias,
                    item.span,
                ));
            }
        }

        let aggregate_scope: Vec<(String, VarId)> = group_bindings
            .iter()
            .map(|(_, alias, var)| (alias.clone(), *var))
            .chain(aggregate_bindings)
            .collect();

        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (name, var) in &aggregate_scope {
            s.vars.insert(name.clone(), *var);
        }
        for (_, var, label) in &grouped_nodes {
            s.node_vars.insert(*var, label.clone());
        }
        for edge in &grouped_edges {
            s.edge_rel_names.insert(edge.var, edge.rel_name.clone());
            if let Some(endpoints) = edge.endpoints {
                s.edge_vars.insert(edge.var, endpoints);
            }
        }
        for (var, label) in &grouped_path_nodes {
            s.node_vars.insert(*var, label.clone());
        }
        for edge in &grouped_path_edges {
            s.edge_rel_names.insert(edge.var, edge.rel_name.clone());
            if let Some(endpoints) = edge.endpoints {
                s.edge_vars.insert(edge.var, endpoints);
            }
        }
        for (alias, binding) in &grouped_paths {
            s.path_vars.insert(alias.clone(), binding.clone());
        }

        s.builder.push_op_mut(GraphOp::Aggregate {
            group_by,
            group_aliases,
            group_vars,
            aggs,
        });

        let mut project = Vec::with_capacity(rewritten_items.len());
        let mut final_scope = Vec::with_capacity(rewritten_items.len());
        let mut final_nodes = Vec::new();
        let mut final_edges = Vec::new();
        let mut final_paths = Vec::new();
        for (expr, alias, span) in rewritten_items {
            let expr = rewrite_grouping_refs(expr, &group_bindings);
            let expr = self.lower_expr(&expr, span, s);
            let node_binding = grouped_nodes
                .iter()
                .find(|(group_alias, _, _)| group_alias == &alias);
            let edge_binding = grouped_edges.iter().find(|binding| binding.alias == alias);
            let out_var = node_binding
                .map(|(_, var, _)| *var)
                .or_else(|| edge_binding.map(|binding| binding.var))
                .unwrap_or_else(|| alloc_anon_var(s));
            project.push(ProjectItem {
                expr,
                alias: Some(alias.clone()),
                out_var: Some(out_var),
            });
            if let Some((_, _, label)) = node_binding {
                final_nodes.push((out_var, label.clone()));
            }
            if let Some(binding) = edge_binding {
                final_edges.push((out_var, binding.rel_name.clone(), binding.endpoints));
            }
            if let Some((_, binding)) = grouped_paths
                .iter()
                .find(|(group_alias, _)| group_alias == &alias)
            {
                final_paths.push((alias.clone(), binding.clone()));
            }
            final_scope.push((alias, out_var));
        }
        // Keep path components as hidden WITH outputs. They are not installed
        // as named scope entries, so `RETURN *` exposes only declared aliases,
        // while downstream path functions can still rebuild the path value.
        let mut hidden_path_vars = grouped_paths
            .iter()
            .flat_map(|(_, binding)| {
                binding
                    .nodes
                    .iter()
                    .copied()
                    .chain(binding.segments.iter().map(|segment| segment.edge))
            })
            .collect::<Vec<_>>();
        hidden_path_vars.sort_by_key(|var| var.0);
        hidden_path_vars.dedup();
        for var in hidden_path_vars {
            project.push(ProjectItem {
                expr: s.builder.push_expr(IrExpr::VarRef(var)),
                alias: Some(format!("__path_{}", var.0)),
                out_var: Some(var),
            });
        }
        s.builder.push_op_mut(GraphOp::With {
            items: project,
            distinct: w.distinct,
            where_predicate: None,
        });

        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (name, var) in final_scope {
            s.vars.insert(name, var);
        }
        for (var, label) in final_nodes {
            s.node_vars.insert(var, label);
        }
        for (var, rel_name, endpoints) in final_edges {
            s.edge_rel_names.insert(var, rel_name);
            if let Some(endpoints) = endpoints {
                s.edge_vars.insert(var, endpoints);
            }
        }
        for (name, binding) in final_paths {
            s.path_vars.insert(name, binding);
        }
        for (var, label) in grouped_path_nodes {
            s.node_vars.insert(var, label);
        }
        for edge in grouped_path_edges {
            s.edge_rel_names.insert(edge.var, edge.rel_name);
            if let Some(endpoints) = edge.endpoints {
                s.edge_vars.insert(edge.var, endpoints);
            }
        }
        if let Some(wc) = &w.where_clause {
            self.lower_where_predicate(&wc.predicate, wc.span, s);
        }
        if let Some(ob) = &w.order_by {
            self.push_sort(&ob.items, s);
        }
        push_skip_limit(self, w.skip.as_ref(), w.limit.as_ref(), s);
    }

    pub(super) fn lower_return(&self, r: &graphforge_ast::ReturnClause, s: &mut BinderState) {
        if reject_empty_projection_wildcard(&r.items, s) {
            return;
        }
        check_duplicate_aliases(&r.items, s);
        // Expand a `RETURN *` wildcard to one item per in-scope named variable.
        let items = expand_projection_wildcard(&r.items, s);
        if r.order_by
            .as_ref()
            .is_some_and(|order_by| has_unprojected_order_aggregate(&items, &order_by.items))
        {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                r.order_by.as_ref().expect("checked above").span,
                "an aggregate function in ORDER BY must also appear in the RETURN projection",
            ));
            return;
        }
        // A NESTED aggregate (`count(*) + 1`, `count(a) + count(b)`) is one that
        // is not the whole item. Reuse WITH's aggregate-to-project path: it
        // validates implicit grouping leaves and supports aggregates nested in
        // maps, lists, and arithmetic expressions.
        let has_nested = items
            .iter()
            .any(|i| agg_func_of(&i.expr).is_none() && expr_contains_aggregate(&i.expr));
        if items
            .iter()
            .any(|item| expr_contains_aggregate_inside_aggregate(&item.expr, false))
        {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                r.span,
                "an aggregate function may not contain another aggregate function",
            ));
            return;
        }
        let mut aggregate_exprs = Vec::new();
        for item in &items {
            collect_aggregate_exprs(&item.expr, &mut aggregate_exprs);
        }
        if aggregate_exprs.iter().any(|expr| match expr {
            Expr::FunctionCall(call) => call.args.iter().any(expr_contains_volatile_function),
            _ => false,
        }) {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                r.span,
                "non-deterministic functions are not allowed inside aggregate arguments",
            ));
            return;
        }
        // If any item is a top-level aggregate (`count(...)`, `sum(...)`, …),
        // emit an `Aggregate` rather than a `Project`: the non-aggregate items
        // become group-by keys and the aggregates become `AggExpr`s. openCypher
        // implicit grouping = group by every non-aggregated return expression.
        if has_nested {
            self.lower_nested_return(r, &items, s);
        } else if items.iter().any(|i| agg_func_of(&i.expr).is_some()) {
            let bindings = self.lower_return_aggregate(&items, s);
            if let Some(ob) = &r.order_by {
                self.push_sort_rewritten(&ob.items, &bindings, s);
            }
            push_skip_limit(self, r.skip.as_ref(), r.limit.as_ref(), s);
        } else {
            let sort_before_projection = r.order_by.is_some() && !r.distinct;
            let (lowered, projection_scope) =
                self.lower_return_items(&items, s, true, r.order_by.is_some());
            if sort_before_projection {
                self.push_sort_before_projection(
                    &r.order_by.as_ref().expect("checked above").items,
                    &items,
                    s,
                );
            }
            let projection_bindings: Vec<(Expr, String, VarId)> = items
                .iter()
                .zip(&projection_scope)
                .map(|(item, (alias, var))| (item.expr.clone(), alias.clone(), *var))
                .collect();
            s.builder.push_op_mut(GraphOp::Project {
                items: lowered,
                distinct: r.distinct,
            });
            if r.distinct {
                s.vars.clear();
                s.node_vars.clear();
                s.edge_vars.clear();
                s.edge_rel_names.clear();
                s.path_vars.clear();
            }
            for (name, v) in &projection_scope {
                s.vars.insert(name.clone(), *v);
            }
            if let Some(ob) = &r.order_by
                && !sort_before_projection
            {
                self.push_sort_rewritten(&ob.items, &projection_bindings, s);
            }
            push_skip_limit(self, r.skip.as_ref(), r.limit.as_ref(), s);
        }
    }

    fn lower_nested_return(
        &self,
        r: &graphforge_ast::ReturnClause,
        items: &[ReturnItem],
        s: &mut BinderState,
    ) {
        let aggregate_items = items
            .iter()
            .cloned()
            .map(|mut item| {
                if item.alias.is_none() {
                    item.alias.clone_from(&item.display);
                }
                item
            })
            .collect::<Vec<_>>();
        let aggregate_return = WithClause {
            distinct: r.distinct,
            items: aggregate_items.clone(),
            order_by: r.order_by.clone(),
            skip: r.skip.clone(),
            limit: r.limit.clone(),
            where_clause: None,
            span: r.span,
        };
        self.lower_with_aggregate_arith(&aggregate_return, &aggregate_items, s);
        let terminal_items = aggregate_items
            .iter()
            .filter_map(|item| {
                let name = item.alias.clone()?;
                Some(ReturnItem {
                    expr: Expr::Var(VarRef {
                        name: name.clone(),
                        span: item.span,
                    }),
                    alias: Some(name.clone()),
                    display: Some(name),
                    span: item.span,
                })
            })
            .collect();
        self.lower_return(
            &graphforge_ast::ReturnClause {
                distinct: false,
                items: terminal_items,
                order_by: None,
                skip: None,
                limit: None,
                span: r.span,
            },
            s,
        );
    }

    fn push_sort(&self, items: &[SortItem], s: &mut BinderState) {
        let keys: Vec<SortKey> = items
            .iter()
            .map(|item| {
                let expr = self.lower_expr(&item.expr, item.span, s);
                SortKey {
                    expr,
                    order: match item.order {
                        AstSortOrder::Ascending => SortOrder::Asc,
                        AstSortOrder::Descending => SortOrder::Desc,
                    },
                    nulls_first: false,
                }
            })
            .collect();
        if !keys.is_empty() {
            s.builder.push_op_mut(GraphOp::Sort { keys });
        }
    }

    fn push_sort_rewritten(
        &self,
        items: &[SortItem],
        bindings: &[(Expr, String, VarId)],
        s: &mut BinderState,
    ) {
        let keys = items
            .iter()
            .map(|item| {
                let rewritten = rewrite_grouping_refs(item.expr.clone(), bindings);
                SortKey {
                    expr: self.lower_expr(&rewritten, item.span, s),
                    order: match item.order {
                        AstSortOrder::Ascending => SortOrder::Asc,
                        AstSortOrder::Descending => SortOrder::Desc,
                    },
                    nulls_first: false,
                }
            })
            .collect::<Vec<_>>();
        if !keys.is_empty() {
            s.builder.push_op_mut(GraphOp::Sort { keys });
        }
    }

    fn push_sort_before_projection(
        &self,
        items: &[SortItem],
        projections: &[ReturnItem],
        s: &mut BinderState,
    ) {
        let rewritten = items
            .iter()
            .map(|item| {
                let expr = rewrite_projection_alias_refs(item.expr.clone(), projections);
                SortKey {
                    expr: self.lower_expr(&expr, item.span, s),
                    order: match item.order {
                        AstSortOrder::Ascending => SortOrder::Asc,
                        AstSortOrder::Descending => SortOrder::Desc,
                    },
                    nulls_first: false,
                }
            })
            .collect::<Vec<_>>();
        if !rewritten.is_empty() {
            s.builder.push_op_mut(GraphOp::Sort { keys: rewritten });
        }
    }

    fn lower_return_items(
        &self,
        items: &[ReturnItem],
        s: &mut BinderState,
        materialize_nodes: bool,
        bind_projection_scope: bool,
    ) -> (Vec<ProjectItem>, Vec<(String, VarId)>) {
        let mut projection_scope = Vec::new();
        let out = items
            .iter()
            .map(|item| {
                let expr = self.lower_return_item_expr(&item.expr, item.span, s, materialize_nodes);
                // Column name: an explicit `AS alias` wins. Otherwise a bare path
                // (`RETURN p`, #754) or — in a terminal RETURN — node (`RETURN n`,
                // #785) variable rewrites to a composed expression whose generated
                // column name would be the expression display string, so alias it
                // back to the variable name the query wrote. Every other un-aliased
                // item (`n.prop`, `count(*)`, `a.x IS NULL`) is named by its verbatim
                // source text (`item.display`), matching openCypher (#598).
                let alias = item.alias.clone().or_else(|| match &item.expr {
                    Expr::Var(VarRef { name, .. })
                        if s.path_vars.contains_key(name)
                            || (materialize_nodes
                                && s.vars.get(name).is_some_and(|v| {
                                    s.node_vars.contains_key(v) || s.edge_rel_names.contains_key(v)
                                })) =>
                    {
                        Some(name.clone())
                    }
                    _ => item.display.clone(),
                });
                let out_var = if bind_projection_scope {
                    alias.as_ref().map(|name| {
                        let v = alloc_anon_var(s);
                        projection_scope.push((name.clone(), v));
                        v
                    })
                } else {
                    None
                };
                ProjectItem {
                    expr,
                    alias,
                    out_var,
                }
            })
            .collect();
        (out, projection_scope)
    }

    /// Lower a single RETURN item's expression. A bare node variable (`RETURN n`)
    /// over a `NodeScan`-bound var rewrites to a `_node_struct` call so the
    /// lowerer materializes a whole node value (identity + labels + properties) —
    /// #785. A bare fixed-hop relationship variable (`RETURN r`) rewrites to a
    /// `_rel_struct` call so it materializes as a whole relationship value —
    /// identity + type + properties (#889). A property access (`n.name`,
    /// `r.since`) keeps its `VarRef` base unchanged.
    fn lower_return_item_expr(
        &self,
        expr: &Expr,
        span: Span,
        s: &mut BinderState,
        materialize_nodes: bool,
    ) -> ExprId {
        if let Some(id) = self.lower_projection_value_expr(expr, span, s, materialize_nodes) {
            return id;
        }
        self.lower_expr(expr, span, s)
    }

    pub(super) fn lower_projection_value_expr(
        &self,
        expr: &Expr,
        span: Span,
        s: &mut BinderState,
        materialize_nodes: bool,
    ) -> Option<ExprId> {
        if materialize_nodes {
            // A bare node var (`RETURN n`, #785) or a relationship endpoint
            // (`RETURN startNode(r)` / `endNode(r)`, #753) materializes a whole
            // node value. Both resolve to a node var bound to a `NodeScan`.
            let node_var = match expr {
                Expr::Var(VarRef { name, .. }) => s
                    .vars
                    .get(name)
                    .copied()
                    .filter(|v| s.node_vars.contains_key(v)),
                Expr::FunctionCall(call) => Self::resolve_endpoint_node(call, s),
                _ => None,
            };
            if let Some(v) = node_var {
                return Some(Self::node_struct_expr(v, s));
            }
        }

        match expr {
            Expr::Var(VarRef { name, .. }) => {
                let var = s
                    .vars
                    .get(name)
                    .copied()
                    .filter(|v| s.edge_rel_names.contains_key(v))?;
                Some(if s.scalar_list_edges.contains(&var) {
                    Self::relationship_struct_list_expr(var, s)
                } else {
                    Self::relationship_struct_expr(var, s)
                })
            }
            Expr::FunctionCall(call)
                if is_function_named(call, "type")
                    && call.args.len() == 1
                    && !Self::invalid_direct_graph_function_argument(call, s) =>
            {
                let arg = self
                    .lower_projection_value_expr(&call.args[0], call.span, s, materialize_nodes)
                    .unwrap_or_else(|| self.lower_expr(&call.args[0], call.span, s));
                Some(s.builder.push_expr(IrExpr::FunctionCall {
                    name: "type".into(),
                    args: vec![arg],
                }))
            }
            Expr::FunctionCall(call)
                if materialize_nodes
                    && is_function_named(call, "coalesce")
                    && !call.args.is_empty() =>
            {
                let args = call
                    .args
                    .iter()
                    .map(|arg| self.lower_projection_value_expr(arg, span, s, true))
                    .collect::<Option<Vec<_>>>()?;
                Some(s.builder.push_expr(IrExpr::FunctionCall {
                    name: "coalesce".into(),
                    args,
                }))
            }
            Expr::List(graphforge_ast::ListLiteral { elements, .. }) => {
                let ids: Vec<ExprId> = elements
                    .iter()
                    .map(|e| {
                        self.lower_projection_value_expr(e, span, s, materialize_nodes)
                            .unwrap_or_else(|| self.lower_expr(e, span, s))
                    })
                    .collect();
                Some(s.builder.push_expr(IrExpr::ListLiteral(ids)))
            }
            _ => None,
        }
    }

    /// Lower a RETURN that contains at least one aggregate into a
    /// [`GraphOp::Aggregate`]. Non-aggregate items become group-by keys;
    /// aggregate items become [`AggExpr`]s.
    fn lower_return_aggregate(
        &self,
        items: &[ReturnItem],
        s: &mut BinderState,
    ) -> Vec<(Expr, String, VarId)> {
        let mut group_by: Vec<ExprId> = Vec::new();
        let mut group_aliases: Vec<Option<String>> = Vec::new();
        let mut group_vars: Vec<Option<VarId>> = Vec::new();
        let mut aggs: Vec<AggExpr> = Vec::new();
        let mut bindings = Vec::with_capacity(items.len());
        for (idx, item) in items.iter().enumerate() {
            if let Some(func) = agg_func_of(&item.expr) {
                let Expr::FunctionCall(call) = &item.expr else {
                    unreachable!("agg_func_of only matches FunctionCall");
                };
                // openCypher names an un-aliased aggregate column by its source
                // text (`count(*)`, `min(x)`); fall back to the verbatim `display`
                // before the synthetic `agg_N` (#598/#599). Matches the non-
                // aggregate naming in `lower_return_items`.
                let alias = item
                    .alias
                    .clone()
                    .or_else(|| item.display.clone())
                    .unwrap_or_else(|| format!("agg_{idx}"));
                let out_var = alloc_anon_var(s);
                aggs.push(self.build_agg(call, func, alias.clone(), Some(out_var), s));
                bindings.push((item.expr.clone(), alias, out_var));
            } else {
                // Materialize whole-node / path group keys (`RETURN n, count(*)`)
                // the same way terminal RETURN items do — a bare node var rewrites
                // to `_node_struct`, not a bare `var_N` (which is not a real column
                // and would lower against an unbound reference). Non-node exprs
                // (`n.name`) fall through to the same `lower_expr` inside.
                group_by.push(self.lower_return_item_expr(&item.expr, item.span, s, true));
                // Name the group-key column by the RETURN item's source text
                // (or its `AS` alias) so a mixed `RETURN n.name, count(*)` yields
                // the `n.name` header openCypher expects (#599). A bare variable
                // (`RETURN n, count(*)`) keeps its lowered name — `display` is the
                // var name, which is the right column name anyway.
                let alias = item
                    .alias
                    .clone()
                    .or_else(|| item.display.clone())
                    .unwrap_or_else(|| format!("group_{idx}"));
                let out_var = alloc_anon_var(s);
                group_aliases.push(Some(alias.clone()));
                group_vars.push(Some(out_var));
                bindings.push((item.expr.clone(), alias, out_var));
            }
        }
        s.builder.push_op_mut(GraphOp::Aggregate {
            group_by,
            group_aliases,
            group_vars,
            aggs,
        });
        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (_, alias, var) in &bindings {
            s.vars.insert(alias.clone(), *var);
        }
        bindings
    }

    /// Build one [`AggExpr`] from an aggregate call. Only `count(*)` has no
    /// argument: explicit variables may be null after OPTIONAL MATCH or scalar
    /// projection and must retain their null-sensitive count semantics.
    /// Distinct aggregate calls are preserved for lowering. `out_var`, when set,
    /// binds the result column for a following `Project` (#599 nested aggregates).
    fn build_agg(
        &self,
        call: &graphforge_ast::FunctionCall,
        func: AggFunc,
        alias: String,
        out_var: Option<VarId>,
        s: &mut BinderState,
    ) -> AggExpr {
        let is_percentile = matches!(func, AggFunc::PercentileDisc | AggFunc::PercentileCont);
        if is_percentile {
            if call.star || call.args.len() != 2 {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    call.span,
                    "percentile aggregate functions require value and percentile arguments",
                ));
            }
            if call.distinct {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    call.span,
                    "percentile aggregate functions do not support DISTINCT",
                ));
            }
        }
        let arg = if !is_percentile && call.star {
            None
        } else {
            call.args.first().map(|a| {
                if func == AggFunc::Collect {
                    self.lower_projection_value_expr(a, call.span, s, true)
                        .unwrap_or_else(|| self.lower_expr(a, call.span, s))
                } else {
                    self.lower_expr(a, call.span, s)
                }
            })
        };
        let percentile = if is_percentile {
            call.args.get(1).map(|p| self.lower_expr(p, call.span, s))
        } else {
            None
        };
        let func = match (call.distinct, func, arg.is_some()) {
            (true, AggFunc::Count, true) => AggFunc::CountDistinct,
            (true, AggFunc::Sum, true) => AggFunc::SumDistinct,
            (true, AggFunc::Avg, true) => AggFunc::AvgDistinct,
            (true, AggFunc::Collect, true) => AggFunc::CollectDistinct,
            _ => func,
        };
        AggExpr {
            func,
            arg,
            percentile,
            alias,
            out_var,
        }
    }

    /// Lower a RETURN where some item NESTS an aggregate inside a larger
    /// Rewrite an expression, replacing each aggregate call with a fresh synthetic
    /// variable (registered in scope, bound to the aggregate's output column) and
    /// recording the [`AggExpr`]. Non-aggregate container nodes are rebuilt with
    /// rewritten children; leaves are cloned. Used by
    /// [`lower_return_aggregate_arith`]. (#599)
    #[allow(clippy::too_many_lines)]
    fn rewrite_aggs(&self, expr: &Expr, aggs: &mut Vec<AggExpr>, s: &mut BinderState) -> Expr {
        if let Some(func) = agg_func_of(expr) {
            let Expr::FunctionCall(call) = expr else {
                unreachable!("agg_func_of only matches FunctionCall");
            };
            let name = format!("__agg_{}", aggs.len());
            let out_var = ensure_var_name(&name, s);
            let agg = self.build_agg(call, func, name.clone(), Some(out_var), s);
            aggs.push(agg);
            return Expr::Var(VarRef {
                name,
                span: call.span,
            });
        }
        match expr {
            Expr::BinaryOp(b) => Expr::BinaryOp(graphforge_ast::BinaryOp {
                op: b.op,
                left: Box::new(self.rewrite_aggs(&b.left, aggs, s)),
                right: Box::new(self.rewrite_aggs(&b.right, aggs, s)),
                span: b.span,
            }),
            Expr::UnaryOp(u) => Expr::UnaryOp(graphforge_ast::UnaryOp {
                op: u.op,
                expr: Box::new(self.rewrite_aggs(&u.expr, aggs, s)),
                span: u.span,
            }),
            Expr::Parenthesized { inner, span } => Expr::Parenthesized {
                inner: Box::new(self.rewrite_aggs(inner, aggs, s)),
                span: *span,
            },
            Expr::ListComprehension(lc) => {
                Expr::ListComprehension(graphforge_ast::ListComprehension {
                    var: lc.var.clone(),
                    list: Box::new(self.rewrite_aggs(&lc.list, aggs, s)),
                    // Aggregation inside a comprehension body is invalid Cypher. Keep
                    // those children intact so lower_expr emits the established error.
                    filter: lc.filter.clone(),
                    projection: lc.projection.clone(),
                    span: lc.span,
                })
            }
            Expr::FunctionCall(c) => Expr::FunctionCall(graphforge_ast::FunctionCall {
                name: c.name.clone(),
                distinct: c.distinct,
                star: c.star,
                args: c
                    .args
                    .iter()
                    .map(|a| self.rewrite_aggs(a, aggs, s))
                    .collect(),
                span: c.span,
            }),
            Expr::List(l) => Expr::List(graphforge_ast::ListLiteral {
                elements: l
                    .elements
                    .iter()
                    .map(|e| self.rewrite_aggs(e, aggs, s))
                    .collect(),
                span: l.span,
            }),
            Expr::Map(m) => Expr::Map(graphforge_ast::MapLiteral {
                entries: m
                    .entries
                    .iter()
                    .map(|(key, value)| (key.clone(), self.rewrite_aggs(value, aggs, s)))
                    .collect(),
                key_spans: m.key_spans.clone(),
                span: m.span,
            }),
            Expr::Case(c) => Expr::Case(graphforge_ast::CaseExpr {
                subject: c
                    .subject
                    .as_deref()
                    .map(|subject| Box::new(self.rewrite_aggs(subject, aggs, s))),
                when_clauses: c
                    .when_clauses
                    .iter()
                    .map(|when| graphforge_ast::WhenClause {
                        condition: self.rewrite_aggs(&when.condition, aggs, s),
                        result: self.rewrite_aggs(&when.result, aggs, s),
                        span: when.span,
                    })
                    .collect(),
                else_expr: c
                    .else_expr
                    .as_deref()
                    .map(|else_expr| Box::new(self.rewrite_aggs(else_expr, aggs, s))),
                span: c.span,
            }),
            Expr::Quantifier(q) => Expr::Quantifier(graphforge_ast::Quantifier {
                kind: q.kind,
                var: q.var.clone(),
                list: Box::new(self.rewrite_aggs(&q.list, aggs, s)),
                predicate: Box::new(self.rewrite_aggs(&q.predicate, aggs, s)),
                span: q.span,
            }),
            Expr::PatternComprehension(pc) => {
                Expr::PatternComprehension(graphforge_ast::PatternComprehension {
                    var: pc.var.clone(),
                    pattern: pc.pattern.clone(),
                    filter: pc.filter.clone(),
                    projection: pc.projection.clone(),
                    span: pc.span,
                })
            }
            Expr::ExistentialSubquery(es) => Expr::ExistentialSubquery(es.clone()),
            Expr::Property(p) => Expr::Property(graphforge_ast::PropertyAccess {
                object: Box::new(self.rewrite_aggs(&p.object, aggs, s)),
                key: p.key.clone(),
                span: p.span,
            }),
            Expr::IsNull {
                expr,
                negated,
                span,
            } => Expr::IsNull {
                expr: Box::new(self.rewrite_aggs(expr, aggs, s)),
                negated: *negated,
                span: *span,
            },
            Expr::InList {
                expr,
                list,
                negated,
                span,
            } => Expr::InList {
                expr: Box::new(self.rewrite_aggs(expr, aggs, s)),
                list: Box::new(self.rewrite_aggs(list, aggs, s)),
                negated: *negated,
                span: *span,
            },
            Expr::StringOp {
                expr,
                op,
                pattern,
                span,
            } => Expr::StringOp {
                expr: Box::new(self.rewrite_aggs(expr, aggs, s)),
                op: *op,
                pattern: Box::new(self.rewrite_aggs(pattern, aggs, s)),
                span: *span,
            },
            Expr::RegexMatch {
                expr,
                pattern,
                span,
            } => Expr::RegexMatch {
                expr: Box::new(self.rewrite_aggs(expr, aggs, s)),
                pattern: Box::new(self.rewrite_aggs(pattern, aggs, s)),
                span: *span,
            },
            other => other.clone(),
        }
    }
}

/// Classify a return expression as a top-level aggregate function call.
///
/// Returns the [`AggFunc`] when `expr` is a bare `count(...)`/`sum(...)`/…
/// call (the openCypher aggregating functions). `DISTINCT` variants are refined
/// by the caller using the call's `distinct` flag. Names are matched
/// case-insensitively and unqualified.
fn agg_func_of(expr: &Expr) -> Option<AggFunc> {
    let Expr::FunctionCall(call) = expr else {
        return None;
    };
    // Only bare (unqualified) names are aggregates here.
    let [name] = call.name.as_slice() else {
        return None;
    };
    match name.to_ascii_lowercase().as_str() {
        "count" => Some(AggFunc::Count),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        "collect" => Some(AggFunc::Collect),
        "percentiledisc" => Some(AggFunc::PercentileDisc),
        "percentilecont" => Some(AggFunc::PercentileCont),
        _ => None,
    }
}

pub(super) fn expr_contains_aggregate(expr: &Expr) -> bool {
    if agg_func_of(expr).is_some() {
        return true;
    }
    match expr {
        Expr::BinaryOp(b) => expr_contains_aggregate(&b.left) || expr_contains_aggregate(&b.right),
        Expr::UnaryOp(u) => expr_contains_aggregate(&u.expr),
        Expr::Parenthesized { inner, .. } => expr_contains_aggregate(inner),
        Expr::FunctionCall(c) => c.args.iter().any(expr_contains_aggregate),
        Expr::Property(p) => expr_contains_aggregate(&p.object),
        Expr::List(l) => l.elements.iter().any(expr_contains_aggregate),
        Expr::Map(m) => m.entries.values().any(expr_contains_aggregate),
        Expr::Case(c) => {
            c.subject.as_deref().is_some_and(expr_contains_aggregate)
                || c.when_clauses.iter().any(|when| {
                    expr_contains_aggregate(&when.condition)
                        || expr_contains_aggregate(&when.result)
                })
                || c.else_expr.as_deref().is_some_and(expr_contains_aggregate)
        }
        Expr::ListComprehension(lc) => {
            expr_contains_aggregate(&lc.list)
                || lc.filter.as_deref().is_some_and(expr_contains_aggregate)
                || lc
                    .projection
                    .as_deref()
                    .is_some_and(expr_contains_aggregate)
        }
        Expr::Quantifier(q) => {
            expr_contains_aggregate(&q.list) || expr_contains_aggregate(&q.predicate)
        }
        Expr::PatternComprehension(pc) => {
            pc.filter.as_deref().is_some_and(expr_contains_aggregate)
                || expr_contains_aggregate(&pc.projection)
        }
        Expr::ExistentialSubquery(es) => match &es.body {
            ExistentialSubqueryBody::Simple { filter, .. } => {
                filter.as_deref().is_some_and(expr_contains_aggregate)
            }
            ExistentialSubqueryBody::Full(_) => false,
        },
        Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(list)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(pattern)
        }
        _ => false,
    }
}

fn collect_aggregate_exprs(expr: &Expr, out: &mut Vec<Expr>) {
    if agg_func_of(expr).is_some() {
        if !out.iter().any(|existing| same_expr_shape(existing, expr)) {
            out.push(expr.clone());
        }
        return;
    }
    match expr {
        Expr::BinaryOp(binary) => {
            collect_aggregate_exprs(&binary.left, out);
            collect_aggregate_exprs(&binary.right, out);
        }
        Expr::UnaryOp(unary) => collect_aggregate_exprs(&unary.expr, out),
        Expr::Parenthesized { inner, .. } => collect_aggregate_exprs(inner, out),
        Expr::FunctionCall(call) => {
            for arg in &call.args {
                collect_aggregate_exprs(arg, out);
            }
        }
        Expr::Property(property) => collect_aggregate_exprs(&property.object, out),
        Expr::List(list) => {
            for element in &list.elements {
                collect_aggregate_exprs(element, out);
            }
        }
        Expr::Map(map) => {
            for value in map.entries.values() {
                collect_aggregate_exprs(value, out);
            }
        }
        Expr::Case(case) => {
            if let Some(subject) = &case.subject {
                collect_aggregate_exprs(subject, out);
            }
            for when in &case.when_clauses {
                collect_aggregate_exprs(&when.condition, out);
                collect_aggregate_exprs(&when.result, out);
            }
            if let Some(else_expr) = &case.else_expr {
                collect_aggregate_exprs(else_expr, out);
            }
        }
        Expr::IsNull { expr, .. } => collect_aggregate_exprs(expr, out),
        Expr::InList { expr, list, .. } => {
            collect_aggregate_exprs(expr, out);
            collect_aggregate_exprs(list, out);
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            collect_aggregate_exprs(expr, out);
            collect_aggregate_exprs(pattern, out);
        }
        _ => {}
    }
}

fn has_unprojected_order_aggregate(projections: &[ReturnItem], order_by: &[SortItem]) -> bool {
    let mut projected = Vec::new();
    for item in projections {
        collect_aggregate_exprs(&item.expr, &mut projected);
    }
    let mut ordered = Vec::new();
    for item in order_by {
        collect_aggregate_exprs(&item.expr, &mut ordered);
    }
    ordered
        .iter()
        .any(|order_agg| !projected.iter().any(|agg| same_expr_shape(agg, order_agg)))
}

fn expr_contains_aggregate_inside_aggregate(expr: &Expr, inside_aggregate: bool) -> bool {
    let is_aggregate = agg_func_of(expr).is_some();
    if is_aggregate && inside_aggregate {
        return true;
    }
    let inside_aggregate = inside_aggregate || is_aggregate;
    match expr {
        Expr::BinaryOp(b) => {
            expr_contains_aggregate_inside_aggregate(&b.left, inside_aggregate)
                || expr_contains_aggregate_inside_aggregate(&b.right, inside_aggregate)
        }
        Expr::UnaryOp(u) => expr_contains_aggregate_inside_aggregate(&u.expr, inside_aggregate),
        Expr::Parenthesized { inner, .. } => {
            expr_contains_aggregate_inside_aggregate(inner, inside_aggregate)
        }
        Expr::FunctionCall(c) => c
            .args
            .iter()
            .any(|arg| expr_contains_aggregate_inside_aggregate(arg, inside_aggregate)),
        Expr::Property(p) => expr_contains_aggregate_inside_aggregate(&p.object, inside_aggregate),
        Expr::List(l) => l
            .elements
            .iter()
            .any(|element| expr_contains_aggregate_inside_aggregate(element, inside_aggregate)),
        Expr::Map(m) => m
            .entries
            .values()
            .any(|value| expr_contains_aggregate_inside_aggregate(value, inside_aggregate)),
        Expr::Case(c) => {
            c.subject.as_deref().is_some_and(|subject| {
                expr_contains_aggregate_inside_aggregate(subject, inside_aggregate)
            }) || c.when_clauses.iter().any(|when| {
                expr_contains_aggregate_inside_aggregate(&when.condition, inside_aggregate)
                    || expr_contains_aggregate_inside_aggregate(&when.result, inside_aggregate)
            }) || c.else_expr.as_deref().is_some_and(|else_expr| {
                expr_contains_aggregate_inside_aggregate(else_expr, inside_aggregate)
            })
        }
        Expr::ListComprehension(lc) => {
            expr_contains_aggregate_inside_aggregate(&lc.list, inside_aggregate)
                || lc.filter.as_deref().is_some_and(|filter| {
                    expr_contains_aggregate_inside_aggregate(filter, inside_aggregate)
                })
                || lc.projection.as_deref().is_some_and(|projection| {
                    expr_contains_aggregate_inside_aggregate(projection, inside_aggregate)
                })
        }
        Expr::Quantifier(q) => {
            expr_contains_aggregate_inside_aggregate(&q.list, inside_aggregate)
                || expr_contains_aggregate_inside_aggregate(&q.predicate, inside_aggregate)
        }
        Expr::PatternComprehension(pc) => {
            pc.filter.as_deref().is_some_and(|filter| {
                expr_contains_aggregate_inside_aggregate(filter, inside_aggregate)
            }) || expr_contains_aggregate_inside_aggregate(&pc.projection, inside_aggregate)
        }
        Expr::ExistentialSubquery(es) => match &es.body {
            ExistentialSubqueryBody::Simple { filter, .. } => {
                filter.as_deref().is_some_and(|filter| {
                    expr_contains_aggregate_inside_aggregate(filter, inside_aggregate)
                })
            }
            ExistentialSubqueryBody::Full(_) => false,
        },
        Expr::IsNull { expr, .. } => {
            expr_contains_aggregate_inside_aggregate(expr, inside_aggregate)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate_inside_aggregate(expr, inside_aggregate)
                || expr_contains_aggregate_inside_aggregate(list, inside_aggregate)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_aggregate_inside_aggregate(expr, inside_aggregate)
                || expr_contains_aggregate_inside_aggregate(pattern, inside_aggregate)
        }
        _ => false,
    }
}

fn expr_contains_volatile_function(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall(call) => {
            is_function_named(call, "rand") || call.args.iter().any(expr_contains_volatile_function)
        }
        Expr::BinaryOp(binary) => {
            expr_contains_volatile_function(&binary.left)
                || expr_contains_volatile_function(&binary.right)
        }
        Expr::UnaryOp(unary) => expr_contains_volatile_function(&unary.expr),
        Expr::Parenthesized { inner, .. } => expr_contains_volatile_function(inner),
        Expr::Property(property) => expr_contains_volatile_function(&property.object),
        Expr::List(list) => list.elements.iter().any(expr_contains_volatile_function),
        Expr::Map(map) => map.entries.values().any(expr_contains_volatile_function),
        Expr::Case(case) => {
            case.subject
                .as_deref()
                .is_some_and(expr_contains_volatile_function)
                || case.when_clauses.iter().any(|when| {
                    expr_contains_volatile_function(&when.condition)
                        || expr_contains_volatile_function(&when.result)
                })
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(expr_contains_volatile_function)
        }
        Expr::IsNull { expr, .. } => expr_contains_volatile_function(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_volatile_function(expr) || expr_contains_volatile_function(list)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_volatile_function(expr) || expr_contains_volatile_function(pattern)
        }
        _ => false,
    }
}

fn is_atomic_grouping_expr(expr: &Expr) -> bool {
    matches!(strip_parens(expr), Expr::Var(_) | Expr::Property(_))
}

fn grouping_ref_root_name(expr: &Expr) -> Option<&str> {
    match strip_parens(expr) {
        Expr::Var(var) => Some(var.name.as_str()),
        Expr::Property(property) => grouping_ref_root_name(&property.object),
        _ => None,
    }
}

fn same_grouping_expr(left: &Expr, right: &Expr) -> bool {
    match (strip_parens(left), strip_parens(right)) {
        (Expr::Var(a), Expr::Var(b)) => a.name == b.name,
        (Expr::Property(a), Expr::Property(b)) => {
            a.key == b.key && same_grouping_expr(&a.object, &b.object)
        }
        (Expr::FunctionCall(a), Expr::FunctionCall(b)) => {
            a.name
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .eq(b.name.iter().map(|part| part.to_ascii_lowercase()))
                && a.distinct == b.distinct
                && a.star == b.star
                && a.args.len() == b.args.len()
                && a.args
                    .iter()
                    .zip(&b.args)
                    .all(|(left, right)| same_expr_shape(left, right))
        }
        (Expr::List(a), Expr::List(b)) => {
            a.elements.len() == b.elements.len()
                && a.elements
                    .iter()
                    .zip(&b.elements)
                    .all(|(left, right)| same_expr_shape(left, right))
        }
        (Expr::Map(a), Expr::Map(b)) => {
            a.entries.len() == b.entries.len()
                && a.entries.iter().all(|(key, left)| {
                    b.entries
                        .get(key)
                        .is_some_and(|right| same_expr_shape(left, right))
                })
        }
        _ => false,
    }
}

fn same_expr_shape(left: &Expr, right: &Expr) -> bool {
    match (strip_parens(left), strip_parens(right)) {
        (Expr::Literal(Literal::Int(a, _)), Expr::Literal(Literal::Int(b, _))) => a == b,
        (Expr::Literal(Literal::Float(a, _)), Expr::Literal(Literal::Float(b, _))) => {
            a.to_bits() == b.to_bits()
        }
        (Expr::Literal(Literal::Str(a, _)), Expr::Literal(Literal::Str(b, _))) => a == b,
        (Expr::Literal(Literal::Bool(a, _)), Expr::Literal(Literal::Bool(b, _))) => a == b,
        (Expr::Literal(Literal::Null(_)), Expr::Literal(Literal::Null(_))) => true,
        (Expr::Param(a), Expr::Param(b)) => a.name == b.name,
        (Expr::Var(a), Expr::Var(b)) => a.name == b.name,
        (Expr::Property(a), Expr::Property(b)) => {
            a.key == b.key && same_expr_shape(&a.object, &b.object)
        }
        (Expr::BinaryOp(a), Expr::BinaryOp(b)) => {
            a.op == b.op && same_expr_shape(&a.left, &b.left) && same_expr_shape(&a.right, &b.right)
        }
        (Expr::UnaryOp(a), Expr::UnaryOp(b)) => a.op == b.op && same_expr_shape(&a.expr, &b.expr),
        (Expr::FunctionCall(a), Expr::FunctionCall(b)) => {
            a.name
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .eq(b.name.iter().map(|part| part.to_ascii_lowercase()))
                && a.distinct == b.distinct
                && a.star == b.star
                && a.args.len() == b.args.len()
                && a.args
                    .iter()
                    .zip(&b.args)
                    .all(|(left, right)| same_expr_shape(left, right))
        }
        _ => false,
    }
}

pub(super) fn rewrite_projection_alias_refs(expr: Expr, projections: &[ReturnItem]) -> Expr {
    rewrite_projection_alias_refs_except(expr, projections, &[])
}

#[allow(clippy::too_many_lines)]
fn rewrite_projection_alias_refs_except(
    expr: Expr,
    projections: &[ReturnItem],
    hidden: &[String],
) -> Expr {
    if let Expr::Var(var) = &expr
        && !hidden.contains(&var.name)
        && let Some(projection) = projections
            .iter()
            .find(|projection| projection.alias.as_deref() == Some(var.name.as_str()))
    {
        return projection.expr.clone();
    }
    match expr {
        Expr::BinaryOp(binary) => Expr::BinaryOp(graphforge_ast::BinaryOp {
            op: binary.op,
            left: Box::new(rewrite_projection_alias_refs_except(
                *binary.left,
                projections,
                hidden,
            )),
            right: Box::new(rewrite_projection_alias_refs_except(
                *binary.right,
                projections,
                hidden,
            )),
            span: binary.span,
        }),
        Expr::UnaryOp(unary) => Expr::UnaryOp(graphforge_ast::UnaryOp {
            op: unary.op,
            expr: Box::new(rewrite_projection_alias_refs_except(
                *unary.expr,
                projections,
                hidden,
            )),
            span: unary.span,
        }),
        Expr::Parenthesized { inner, span } => Expr::Parenthesized {
            inner: Box::new(rewrite_projection_alias_refs_except(
                *inner,
                projections,
                hidden,
            )),
            span,
        },
        Expr::Property(mut property) => {
            property.object = Box::new(rewrite_projection_alias_refs_except(
                *property.object,
                projections,
                hidden,
            ));
            Expr::Property(property)
        }
        Expr::FunctionCall(mut call) => {
            call.args = call
                .args
                .into_iter()
                .map(|arg| rewrite_projection_alias_refs_except(arg, projections, hidden))
                .collect();
            Expr::FunctionCall(call)
        }
        Expr::List(mut list) => {
            list.elements = list
                .elements
                .into_iter()
                .map(|element| rewrite_projection_alias_refs_except(element, projections, hidden))
                .collect();
            Expr::List(list)
        }
        Expr::Map(mut map) => {
            map.entries = map
                .entries
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        rewrite_projection_alias_refs_except(value, projections, hidden),
                    )
                })
                .collect();
            Expr::Map(map)
        }
        Expr::Case(mut case) => {
            case.subject = case.subject.map(|subject| {
                Box::new(rewrite_projection_alias_refs_except(
                    *subject,
                    projections,
                    hidden,
                ))
            });
            for when in &mut case.when_clauses {
                when.condition = rewrite_projection_alias_refs_except(
                    when.condition.clone(),
                    projections,
                    hidden,
                );
                when.result =
                    rewrite_projection_alias_refs_except(when.result.clone(), projections, hidden);
            }
            case.else_expr = case.else_expr.map(|else_expr| {
                Box::new(rewrite_projection_alias_refs_except(
                    *else_expr,
                    projections,
                    hidden,
                ))
            });
            Expr::Case(case)
        }
        Expr::ListComprehension(mut comprehension) => {
            comprehension.list = Box::new(rewrite_projection_alias_refs_except(
                *comprehension.list,
                projections,
                hidden,
            ));
            let mut body_hidden = hidden.to_vec();
            body_hidden.push(comprehension.var.clone());
            comprehension.filter = comprehension.filter.map(|filter| {
                Box::new(rewrite_projection_alias_refs_except(
                    *filter,
                    projections,
                    &body_hidden,
                ))
            });
            comprehension.projection = comprehension.projection.map(|projection| {
                Box::new(rewrite_projection_alias_refs_except(
                    *projection,
                    projections,
                    &body_hidden,
                ))
            });
            Expr::ListComprehension(comprehension)
        }
        Expr::Quantifier(mut quantifier) => {
            quantifier.list = Box::new(rewrite_projection_alias_refs_except(
                *quantifier.list,
                projections,
                hidden,
            ));
            let mut body_hidden = hidden.to_vec();
            body_hidden.push(quantifier.var.clone());
            quantifier.predicate = Box::new(rewrite_projection_alias_refs_except(
                *quantifier.predicate,
                projections,
                &body_hidden,
            ));
            Expr::Quantifier(quantifier)
        }
        Expr::IsNull {
            expr,
            negated,
            span,
        } => Expr::IsNull {
            expr: Box::new(rewrite_projection_alias_refs_except(
                *expr,
                projections,
                hidden,
            )),
            negated,
            span,
        },
        Expr::InList {
            expr,
            list,
            negated,
            span,
        } => Expr::InList {
            expr: Box::new(rewrite_projection_alias_refs_except(
                *expr,
                projections,
                hidden,
            )),
            list: Box::new(rewrite_projection_alias_refs_except(
                *list,
                projections,
                hidden,
            )),
            negated,
            span,
        },
        Expr::StringOp {
            expr,
            op,
            pattern,
            span,
        } => Expr::StringOp {
            expr: Box::new(rewrite_projection_alias_refs_except(
                *expr,
                projections,
                hidden,
            )),
            op,
            pattern: Box::new(rewrite_projection_alias_refs_except(
                *pattern,
                projections,
                hidden,
            )),
            span,
        },
        Expr::RegexMatch {
            expr,
            pattern,
            span,
        } => Expr::RegexMatch {
            expr: Box::new(rewrite_projection_alias_refs_except(
                *expr,
                projections,
                hidden,
            )),
            pattern: Box::new(rewrite_projection_alias_refs_except(
                *pattern,
                projections,
                hidden,
            )),
            span,
        },
        other => other,
    }
}

fn collect_grouping_refs(expr: &Expr, out: &mut Vec<Expr>) {
    collect_grouping_refs_except(expr, out, &[]);
}

#[allow(clippy::too_many_lines)]
fn collect_grouping_refs_except(expr: &Expr, out: &mut Vec<Expr>, hidden: &[String]) {
    if agg_func_of(expr).is_some() {
        return;
    }
    match expr {
        Expr::Var(_) | Expr::Property(_) if !grouping_ref_is_hidden(expr, hidden) => {
            out.push(expr.clone());
        }
        Expr::BinaryOp(binary) => {
            collect_grouping_refs_except(&binary.left, out, hidden);
            collect_grouping_refs_except(&binary.right, out, hidden);
        }
        Expr::UnaryOp(unary) => collect_grouping_refs_except(&unary.expr, out, hidden),
        Expr::Parenthesized { inner, .. } => collect_grouping_refs_except(inner, out, hidden),
        Expr::FunctionCall(call) => {
            for arg in &call.args {
                collect_grouping_refs_except(arg, out, hidden);
            }
        }
        Expr::List(list) => {
            for element in &list.elements {
                collect_grouping_refs_except(element, out, hidden);
            }
        }
        Expr::Map(map) => {
            for value in map.entries.values() {
                collect_grouping_refs_except(value, out, hidden);
            }
        }
        Expr::Case(case) => {
            if let Some(subject) = &case.subject {
                collect_grouping_refs_except(subject, out, hidden);
            }
            for when in &case.when_clauses {
                collect_grouping_refs_except(&when.condition, out, hidden);
                collect_grouping_refs_except(&when.result, out, hidden);
            }
            if let Some(else_expr) = &case.else_expr {
                collect_grouping_refs_except(else_expr, out, hidden);
            }
        }
        Expr::ListComprehension(lc) => {
            collect_grouping_refs_except(&lc.list, out, hidden);
            let mut body_hidden = hidden.to_vec();
            body_hidden.push(lc.var.clone());
            if let Some(filter) = &lc.filter {
                collect_grouping_refs_except(filter, out, &body_hidden);
            }
            if let Some(projection) = &lc.projection {
                collect_grouping_refs_except(projection, out, &body_hidden);
            }
        }
        Expr::Quantifier(q) => {
            collect_grouping_refs_except(&q.list, out, hidden);
            let mut predicate_hidden = hidden.to_vec();
            predicate_hidden.push(q.var.clone());
            collect_grouping_refs_except(&q.predicate, out, &predicate_hidden);
        }
        Expr::PatternComprehension(pc) => {
            let body_hidden = hidden_with_pattern_vars(hidden, &pc.pattern, pc.var.as_ref());
            if let Some(filter) = &pc.filter {
                collect_grouping_refs_except(filter, out, &body_hidden);
            }
            collect_grouping_refs_except(&pc.projection, out, &body_hidden);
        }
        Expr::ExistentialSubquery(es) => {
            if let ExistentialSubqueryBody::Simple { pattern, filter } = &es.body {
                let body_hidden = hidden_with_pattern_vars(hidden, pattern, None);
                if let Some(filter) = filter {
                    collect_grouping_refs_except(filter, out, &body_hidden);
                }
            }
        }
        Expr::LabelPredicate(label) if !hidden.contains(&label.var) => {
            out.push(Expr::Var(VarRef {
                name: label.var.clone(),
                span: label.span,
            }));
        }
        Expr::IsNull { expr, .. } => collect_grouping_refs_except(expr, out, hidden),
        Expr::InList { expr, list, .. } => {
            collect_grouping_refs_except(expr, out, hidden);
            collect_grouping_refs_except(list, out, hidden);
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            collect_grouping_refs_except(expr, out, hidden);
            collect_grouping_refs_except(pattern, out, hidden);
        }
        _ => {}
    }
}

fn grouping_ref_is_hidden(expr: &Expr, hidden: &[String]) -> bool {
    match strip_parens(expr) {
        Expr::Var(var) => hidden.contains(&var.name),
        Expr::Property(property) => grouping_ref_is_hidden(&property.object, hidden),
        _ => false,
    }
}

fn hidden_with_pattern_vars(
    hidden: &[String],
    pattern: &PathPattern,
    path_var: Option<&String>,
) -> Vec<String> {
    let mut body_hidden = hidden.to_vec();
    body_hidden.extend(pattern.elements.iter().filter_map(|element| match element {
        PathElement::Node(node) => node.var.clone(),
        PathElement::Rel(rel) => rel.var.clone(),
    }));
    body_hidden.extend(path_var.cloned());
    body_hidden
}

#[allow(clippy::too_many_lines)]
fn rewrite_grouping_refs(expr: Expr, bindings: &[(Expr, String, VarId)]) -> Expr {
    rewrite_grouping_refs_except(expr, bindings, &[])
}

#[allow(clippy::too_many_lines)]
fn rewrite_grouping_refs_except(
    expr: Expr,
    bindings: &[(Expr, String, VarId)],
    hidden: &[String],
) -> Expr {
    if !grouping_ref_is_hidden(&expr, hidden)
        && let Some((_, alias, _)) = bindings
            .iter()
            .find(|(group, _, _)| same_grouping_expr(&expr, group))
    {
        return Expr::Var(VarRef {
            name: alias.clone(),
            span: expr.span(),
        });
    }
    match expr {
        Expr::BinaryOp(binary) => Expr::BinaryOp(graphforge_ast::BinaryOp {
            op: binary.op,
            left: Box::new(rewrite_grouping_refs_except(*binary.left, bindings, hidden)),
            right: Box::new(rewrite_grouping_refs_except(
                *binary.right,
                bindings,
                hidden,
            )),
            span: binary.span,
        }),
        Expr::UnaryOp(unary) => Expr::UnaryOp(graphforge_ast::UnaryOp {
            op: unary.op,
            expr: Box::new(rewrite_grouping_refs_except(*unary.expr, bindings, hidden)),
            span: unary.span,
        }),
        Expr::Parenthesized { inner, span } => Expr::Parenthesized {
            inner: Box::new(rewrite_grouping_refs_except(*inner, bindings, hidden)),
            span,
        },
        Expr::FunctionCall(call) => Expr::FunctionCall(graphforge_ast::FunctionCall {
            name: call.name,
            distinct: call.distinct,
            star: call.star,
            args: call
                .args
                .into_iter()
                .map(|arg| rewrite_grouping_refs_except(arg, bindings, hidden))
                .collect(),
            span: call.span,
        }),
        Expr::Property(mut property) => {
            property.object = Box::new(rewrite_grouping_refs_except(
                *property.object,
                bindings,
                hidden,
            ));
            Expr::Property(property)
        }
        Expr::List(list) => Expr::List(graphforge_ast::ListLiteral {
            elements: list
                .elements
                .into_iter()
                .map(|element| rewrite_grouping_refs_except(element, bindings, hidden))
                .collect(),
            span: list.span,
        }),
        Expr::Map(mut map) => {
            map.entries = map
                .entries
                .into_iter()
                .map(|(key, value)| (key, rewrite_grouping_refs_except(value, bindings, hidden)))
                .collect();
            Expr::Map(map)
        }
        Expr::Case(mut case) => {
            case.subject = case
                .subject
                .map(|subject| Box::new(rewrite_grouping_refs_except(*subject, bindings, hidden)));
            for when in &mut case.when_clauses {
                when.condition =
                    rewrite_grouping_refs_except(when.condition.clone(), bindings, hidden);
                when.result = rewrite_grouping_refs_except(when.result.clone(), bindings, hidden);
            }
            case.else_expr = case.else_expr.map(|else_expr| {
                Box::new(rewrite_grouping_refs_except(*else_expr, bindings, hidden))
            });
            Expr::Case(case)
        }
        Expr::ListComprehension(mut lc) => {
            lc.list = Box::new(rewrite_grouping_refs_except(*lc.list, bindings, hidden));
            let mut body_hidden = hidden.to_vec();
            body_hidden.push(lc.var.clone());
            lc.filter = lc.filter.map(|filter| {
                Box::new(rewrite_grouping_refs_except(
                    *filter,
                    bindings,
                    &body_hidden,
                ))
            });
            lc.projection = lc.projection.map(|projection| {
                Box::new(rewrite_grouping_refs_except(
                    *projection,
                    bindings,
                    &body_hidden,
                ))
            });
            Expr::ListComprehension(lc)
        }
        Expr::Quantifier(mut q) => {
            q.list = Box::new(rewrite_grouping_refs_except(*q.list, bindings, hidden));
            let mut predicate_hidden = hidden.to_vec();
            predicate_hidden.push(q.var.clone());
            q.predicate = Box::new(rewrite_grouping_refs_except(
                *q.predicate,
                bindings,
                &predicate_hidden,
            ));
            Expr::Quantifier(q)
        }
        Expr::PatternComprehension(mut pc) => {
            let body_hidden = hidden_with_pattern_vars(hidden, &pc.pattern, pc.var.as_ref());
            pc.filter = pc.filter.map(|filter| {
                Box::new(rewrite_grouping_refs_except(
                    *filter,
                    bindings,
                    &body_hidden,
                ))
            });
            pc.projection = Box::new(rewrite_grouping_refs_except(
                *pc.projection,
                bindings,
                &body_hidden,
            ));
            Expr::PatternComprehension(pc)
        }
        Expr::ExistentialSubquery(mut es) => {
            if let ExistentialSubqueryBody::Simple { pattern, filter } = &mut es.body {
                let body_hidden = hidden_with_pattern_vars(hidden, pattern, None);
                *filter = filter.take().map(|filter| {
                    Box::new(rewrite_grouping_refs_except(
                        *filter,
                        bindings,
                        &body_hidden,
                    ))
                });
            }
            Expr::ExistentialSubquery(es)
        }
        Expr::LabelPredicate(mut label) => {
            let reference = Expr::Var(VarRef {
                name: label.var.clone(),
                span: label.span,
            });
            if !hidden.contains(&label.var)
                && let Some((_, alias, _)) = bindings
                    .iter()
                    .find(|(group, _, _)| same_grouping_expr(&reference, group))
            {
                label.var.clone_from(alias);
            }
            Expr::LabelPredicate(label)
        }
        Expr::IsNull {
            expr,
            negated,
            span,
        } => Expr::IsNull {
            expr: Box::new(rewrite_grouping_refs_except(*expr, bindings, hidden)),
            negated,
            span,
        },
        Expr::InList {
            expr,
            list,
            negated,
            span,
        } => Expr::InList {
            expr: Box::new(rewrite_grouping_refs_except(*expr, bindings, hidden)),
            list: Box::new(rewrite_grouping_refs_except(*list, bindings, hidden)),
            negated,
            span,
        },
        Expr::StringOp {
            expr,
            op,
            pattern,
            span,
        } => Expr::StringOp {
            expr: Box::new(rewrite_grouping_refs_except(*expr, bindings, hidden)),
            op,
            pattern: Box::new(rewrite_grouping_refs_except(*pattern, bindings, hidden)),
            span,
        },
        Expr::RegexMatch {
            expr,
            pattern,
            span,
        } => Expr::RegexMatch {
            expr: Box::new(rewrite_grouping_refs_except(*expr, bindings, hidden)),
            pattern: Box::new(rewrite_grouping_refs_except(*pattern, bindings, hidden)),
            span,
        },
        other => other,
    }
}

/// Expand a `RETURN *` wildcard into one item per in-scope NAMED variable
/// (#598). `*` parses as a `Var` named `"*"`; here it becomes the current
/// variables (e.g. `a, b` for `MATCH (a)-->(b) RETURN *`). The TCK compares
/// result columns by header name, so order is immaterial — emit them sorted for
/// a deterministic plan. Non-wildcard items pass through unchanged; a `*` with
/// no in-scope variables expands to nothing (the projection then errors, as
/// Cypher requires at least one).
///
/// Anonymous pattern elements (no user name) are not in scope and so are not
/// returned. Named path bindings live outside `vars`, so include both maps.
fn expand_projection_wildcard(items: &[ReturnItem], s: &BinderState) -> Vec<ReturnItem> {
    let is_star = |e: &Expr| matches!(e, Expr::Var(VarRef { name, .. }) if name == "*");
    if !items.iter().any(|i| is_star(&i.expr)) {
        return items.to_vec();
    }
    let mut names: Vec<String> = s.vars.keys().chain(s.path_vars.keys()).cloned().collect();
    names.sort();
    names.dedup();
    let mut out = Vec::new();
    for item in items {
        if is_star(&item.expr) {
            for name in &names {
                out.push(ReturnItem {
                    expr: Expr::Var(VarRef {
                        name: name.clone(),
                        span: item.span,
                    }),
                    alias: None,
                    // `RETURN *` names each column by its variable.
                    display: Some(name.clone()),
                    span: item.span,
                });
            }
        } else {
            out.push(item.clone());
        }
    }
    out
}

fn reject_empty_projection_wildcard(items: &[ReturnItem], s: &mut BinderState) -> bool {
    let Some(star) = items
        .iter()
        .find(|item| matches!(&item.expr, Expr::Var(VarRef { name, .. }) if name == "*"))
    else {
        return false;
    };
    if !s.vars.is_empty() || !s.path_vars.is_empty() {
        return false;
    }
    s.errors.push(BindError::new(
        BindErrorKind::InvalidArgument,
        star.span,
        "projection wildcard requires at least one variable in scope",
    ));
    true
}

struct ForwardedEdgeBinding {
    var: VarId,
    rel_name: Option<String>,
    endpoints: Option<(VarId, VarId)>,
}

struct GroupedEdgeBinding {
    alias: String,
    var: VarId,
    rel_name: Option<String>,
    endpoints: Option<(VarId, VarId)>,
}

#[derive(Clone, Copy)]
enum RowCountOp {
    Skip,
    Limit,
}

impl RowCountOp {
    fn keyword(self) -> &'static str {
        match self {
            Self::Skip => "SKIP",
            Self::Limit => "LIMIT",
        }
    }

    fn graph_op(self, count: u64) -> GraphOp {
        match self {
            Self::Skip => GraphOp::Skip { count },
            Self::Limit => GraphOp::Limit { count },
        }
    }

    fn graph_param_op(self, name: String) -> GraphOp {
        match self {
            Self::Skip => GraphOp::SkipParam { name },
            Self::Limit => GraphOp::LimitParam { name },
        }
    }

    fn graph_expr_op(self, expr: ExprId) -> GraphOp {
        match self {
            Self::Skip => GraphOp::SkipExpr { expr },
            Self::Limit => GraphOp::LimitExpr { expr },
        }
    }
}

fn push_skip_limit(
    binder: &Binder,
    skip: Option<&Expr>,
    limit: Option<&Expr>,
    s: &mut BinderState,
) {
    if let Some(expr) = skip {
        push_row_count_op(binder, RowCountOp::Skip, expr, s);
    }
    if let Some(expr) = limit {
        push_row_count_op(binder, RowCountOp::Limit, expr, s);
    }
}

fn push_row_count_op(binder: &Binder, kind: RowCountOp, expr: &Expr, s: &mut BinderState) {
    if binder.typed_uuid_param_in(expr).is_some() {
        binder.lower_expr(expr, expr.span(), s);
        return;
    }
    if let Some(n) = extract_non_negative_int_constant(expr) {
        s.builder.push_op_mut(kind.graph_op(n));
        return;
    }
    if let Some(name) = extract_parameter_name(expr) {
        s.builder.push_op_mut(kind.graph_param_op(name));
        return;
    }
    if extract_int_constant(expr).is_some() || is_float_constant(expr) {
        s.errors.push(BindError::new(
            BindErrorKind::InvalidArgument,
            expr.span(),
            format!("{} requires a non-negative integer value", kind.keyword()),
        ));
        return;
    }
    let mut refs = Vec::new();
    collect_grouping_refs(expr, &mut refs);
    if refs.is_empty() && !expr_contains_aggregate(expr) && row_count_expr_is_integer(expr) {
        let expr = binder.lower_expr(expr, expr.span(), s);
        s.builder.push_op_mut(kind.graph_expr_op(expr));
        return;
    }
    s.errors.push(BindError::new(
        BindErrorKind::InvalidArgument,
        expr.span(),
        format!(
            "{} requires a non-negative variable-independent integer expression",
            kind.keyword()
        ),
    ));
}

fn row_count_expr_is_integer(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Literal::Int(_, _)) | Expr::Param(_) => true,
        Expr::Parenthesized { inner, .. } => row_count_expr_is_integer(inner),
        Expr::UnaryOp(graphforge_ast::UnaryOp {
            op: AstUnOp::Neg,
            expr,
            ..
        }) => row_count_expr_is_integer(expr),
        Expr::BinaryOp(binary) => {
            row_count_expr_is_integer(&binary.left) && row_count_expr_is_integer(&binary.right)
        }
        Expr::FunctionCall(call) => is_function_named(call, "toInteger"),
        _ => false,
    }
}

fn is_float_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Literal::Float(_, _)) => true,
        Expr::Parenthesized { inner, .. } => is_float_constant(inner),
        Expr::UnaryOp(graphforge_ast::UnaryOp {
            op: AstUnOp::Neg,
            expr,
            ..
        }) => is_float_constant(expr),
        _ => false,
    }
}

fn extract_parameter_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Param(graphforge_ast::ParamRef { name, .. }) => Some(name.clone()),
        Expr::Parenthesized { inner, .. } => extract_parameter_name(inner),
        _ => None,
    }
}

fn extract_non_negative_int_constant(expr: &Expr) -> Option<u64> {
    let n = extract_int_constant(expr)?;
    u64::try_from(n).ok()
}

fn extract_int_constant(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(Literal::Int(n, _)) => Some(*n),
        Expr::Parenthesized { inner, .. } => extract_int_constant(inner),
        Expr::UnaryOp(graphforge_ast::UnaryOp {
            op: AstUnOp::Neg,
            expr,
            ..
        }) => extract_int_constant(expr)?.checked_neg(),
        _ => None,
    }
}

fn check_duplicate_aliases(items: &[ReturnItem], s: &mut BinderState) {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for item in items {
        if let Some(a) = item.alias.as_deref()
            && !seen.insert(a)
        {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                item.span,
                format!("multiple result columns with the same name `{a}`"),
            ));
        }
    }
}

#[cfg(test)]
mod tests;
