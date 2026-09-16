//! Pattern, path, and existential binding with shared parent scope state.

use super::projection::expr_contains_aggregate;
use super::{
    BindError, BindErrorKind, Binder, BinderState, BoundPropertyOwner, PathBinding, PathSegment,
    VarKind, alloc_anon_var, ensure_pattern_var, property_owner_for_var,
};
use crate::composition_binding::SymbolBinding;
use crate::expr::{BinaryOpKind, IrExpr, IrLiteral};
use crate::plan::{GraphOp, GraphPlan, OntologyMode, PATTERN_COMPREHENSION_VALUE_ALIAS};
use crate::{Direction, ExprId, ProjectItem, VarId};
use graphforge_ast::{
    AstClause, BinaryOpKind as AstBinOp, ExistentialSubqueryBody, Expr, FunctionCall, PathElement,
    PathPattern, PatternPredicate, UnaryOpKind as AstUnOp, VarRef, WhereClause,
};
use graphforge_core::Span;
use graphforge_value::{EntityTypeId, RelationTypeId};
use std::collections::HashSet;

impl Binder {
    pub(super) fn lower_match(
        &self,
        m: &graphforge_ast::MatchClause,
        optional: bool,
        s: &mut BinderState,
    ) {
        if optional {
            // Collect ops into a sub-builder, then wrap in Optional.
            let mut sub_state = BinderState {
                vars: s.vars.clone(),
                path_vars: s.path_vars.clone(),
                node_vars: s.node_vars.clone(),
                edge_vars: s.edge_vars.clone(),
                edge_rel_names: s.edge_rel_names.clone(),
                scalar_list_edges: s.scalar_list_edges.clone(),
                var_kinds: s.var_kinds.clone(),
                next_var: s.next_var,
                builder: GraphPlan::builder("openCypher").ontology_mode(self.mode),
                errors: Vec::new(),
                warnings: Vec::new(),
                captured_pattern_comprehensions: None,
                existential_depth: s.existential_depth,
                standalone_call: false,
            };
            for pat in &m.patterns {
                self.lower_path_pattern(pat, &mut sub_state);
            }
            if let Some(w) = &m.where_clause {
                self.lower_where(w, &mut sub_state);
            }
            // Propagate state back up
            s.next_var = sub_state.next_var;
            // Merge any newly introduced vars (do not overwrite existing)
            for (name, id) in sub_state.vars {
                s.vars.entry(name).or_insert(id);
            }
            for (name, binding) in sub_state.path_vars {
                s.path_vars.entry(name).or_insert(binding);
            }
            for (v, label) in sub_state.node_vars {
                s.node_vars.entry(v).or_insert(label);
            }
            for (v, rel_name) in sub_state.edge_rel_names {
                s.edge_rel_names.entry(v).or_insert(rel_name);
            }
            for (v, kind) in sub_state.var_kinds {
                s.var_kinds.entry(v).or_insert(kind);
            }
            // Deliberately do NOT propagate `edge_vars` out of an OPTIONAL MATCH.
            // After an unmatched optional row `r` is null, and Cypher requires
            // `startNode(r)`/`endNode(r)` to be null — but the rewrite resolves
            // to the endpoint node var, which on such a row is the *outer*,
            // non-null var (e.g. `a` in `MATCH (a) OPTIONAL MATCH (a)-[r]->(b)`).
            // Without an edge-uuid null gate on endpoint materialization that
            // would return a wrong non-null value, so optional edges stay
            // unresolved here and `startNode`/`endNode` fall through to
            // UnknownFunction. (Edges still resolve inside the optional's own
            // WHERE, where matched rows have a non-null `r`.) Null-gated endpoint
            // values are a node-value-completeness follow-up (#889).
            s.errors.extend(sub_state.errors);
            s.warnings.extend(sub_state.warnings);
            let mut child = sub_state.builder.build();
            let referenced_vars = (0..child.exprs.len())
                .filter_map(|index| {
                    let index = u32::try_from(index).ok()?;
                    match child.exprs.get(ExprId(index)) {
                        IrExpr::VarRef(var) => Some(*var),
                        _ => None,
                    }
                })
                .collect::<HashSet<_>>();
            let bound_vars = child
                .ops
                .iter()
                .flat_map(graph_op_bound_vars)
                .collect::<HashSet<_>>();
            let mut correlated_scans = referenced_vars
                .difference(&bound_vars)
                .filter(|var| s.node_vars.contains_key(var))
                .copied()
                .collect::<Vec<_>>();
            correlated_scans.sort_by_key(|var| var.0);
            for var in correlated_scans.into_iter().rev() {
                child.ops.insert(0, GraphOp::NodeScan { var, ty: None });
            }
            let mut correlated_edges = referenced_vars
                .difference(&bound_vars)
                .filter(|var| s.edge_rel_names.contains_key(var))
                .copied()
                .collect::<Vec<_>>();
            correlated_edges.sort_by_key(|var| var.0);
            for var in correlated_edges.into_iter().rev() {
                child.ops.insert(0, GraphOp::EdgeScan { var, ty: None });
            }
            s.builder.push_op_mut(GraphOp::Optional {
                child: Box::new(child),
            });
        } else {
            for pat in &m.patterns {
                self.lower_path_pattern(pat, s);
            }
            if let Some(w) = &m.where_clause {
                self.lower_where(w, s);
            }
        }
    }

    /// Finish independent name diagnostics after a pattern has failed admission.
    /// Resolution records its errors in `s`; no scan or expansion is emitted.
    fn diagnose_remaining_pattern_names<'a>(
        &self,
        elements: impl Iterator<Item = &'a PathElement>,
        s: &mut BinderState,
    ) {
        for element in elements {
            match element {
                PathElement::Node(node) => {
                    for label in &node.labels {
                        let _ = self.resolve_label(label, node.span, s);
                    }
                }
                PathElement::Rel(rel) => {
                    for name in &rel.types {
                        let _ = self.resolve_relation_type(name, rel.span, s);
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn lower_path_pattern(&self, pat: &PathPattern, s: &mut BinderState) {
        let mut prev_node_var: Option<VarId> = None;
        let mut iter = pat.elements.iter().peekable();
        // A node that follows a relationship IS that relationship's destination:
        // reuse the `dst` var the Rel arm already computed (carried in
        // `pending_dst`) so the trailing `NodeScan` binds to the SAME var as the
        // `Expand`'s `dst`. Otherwise an anonymous destination would mint a fresh
        // var here (`ensure_var(None)`), leaving the scan disconnected from the
        // expansion — the lowerer would then drop or cross-join it (#718/#598).
        let mut pending_dst: Option<VarId> = None;
        let mut path_nodes: Vec<VarId> = Vec::new();
        let mut path_segments: Vec<PathSegment> = Vec::new();
        let mut path_edges: Vec<VarId> = Vec::new();

        while let Some(elem) = iter.next() {
            match elem {
                PathElement::Node(node) => {
                    let var = pending_dst.take().unwrap_or_else(|| {
                        ensure_pattern_var(node.var.as_deref(), VarKind::Node, node.span, s)
                    });
                    let ty = match node.labels.first() {
                        Some(label) => {
                            let Some(id) = self.resolve_label(label, node.span, s) else {
                                for label in &node.labels[1..] {
                                    let _ = self.resolve_label(label, node.span, s);
                                }
                                self.diagnose_remaining_pattern_names(iter, s);
                                return;
                            };
                            Some(id)
                        }
                        None => None,
                    };
                    s.builder.push_op_mut(GraphOp::NodeScan { var, ty });
                    s.node_vars.insert(var, node.labels.first().cloned());
                    if node.labels.len() > 1 {
                        let predicate =
                            self.lower_node_label_predicate(var, &node.labels[1..], node.span, s);
                        s.builder.push_op_mut(GraphOp::Filter { predicate });
                    }
                    // An inline property map `(a:Person {name:'x'})` is an
                    // exact-equality constraint — lower it to a Filter over the
                    // just-scanned rows (#748).
                    self.lower_inline_property_filter(var, node.properties.as_ref(), node.span, s);
                    prev_node_var = Some(var);
                }
                PathElement::Rel(rel) => {
                    let edge_var =
                        ensure_pattern_var(rel.var.as_deref(), VarKind::Relationship, rel.span, s);

                    // Peek ahead for the destination node. When it exists, hand
                    // its var to the next loop iteration via `pending_dst` so the
                    // trailing NodeScan binds to this same dst (see top of fn).
                    let dst_var = if let Some(PathElement::Node(dst)) = iter.peek() {
                        let v = ensure_pattern_var(dst.var.as_deref(), VarKind::Node, dst.span, s);
                        pending_dst = Some(v);
                        v
                    } else {
                        alloc_anon_var(s)
                    };

                    let src_var = prev_node_var.unwrap_or_else(|| alloc_anon_var(s));
                    let dir = lower_direction(rel.direction);
                    let rel_name = (rel.types.len() == 1)
                        .then(|| rel.types.first().cloned())
                        .flatten();
                    let rel_ty = match rel_name.as_ref() {
                        Some(name) => {
                            let Some(id) = self.resolve_relation_type(name, rel.span, s) else {
                                self.diagnose_remaining_pattern_names(iter, s);
                                return;
                            };
                            Some(id)
                        }
                        None => None,
                    };
                    let is_var_hop = rel.min_hops.is_some() || rel.max_hops.is_some();

                    // Both fixed (`-[:R]->`) and variable-length (`-[:R*1..3]->`)
                    // hops lower to a single `Expand`: it is the only op that
                    // carries the src/dst node vars, so the relational layer can
                    // connect `(a)` and `(b)` with a join. A bare `TypedEdgeScan`
                    // / `EdgeScan` would drop those vars and leave the pattern's
                    // node scans disconnected (#718). A fixed hop is encoded as
                    // `min_hops == 1 && max_hops == Some(1)`, which the lowerer
                    // routes to a relational join; any other bound goes to the
                    // BFS Extension.
                    let (min_hops, max_hops) = if is_var_hop {
                        let min = u16::try_from(rel.min_hops.unwrap_or(1)).unwrap_or(u16::MAX);
                        let max = rel.max_hops.map(|h| u16::try_from(h).unwrap_or(u16::MAX));
                        (min, max)
                    } else {
                        (1, Some(1))
                    };
                    let is_scalar_hop = min_hops == 1 && max_hops == Some(1);
                    if is_var_hop && is_scalar_hop {
                        s.scalar_list_edges.insert(edge_var);
                    }
                    let bound_rel_type_conflict =
                        bound_rel_type_conflict(edge_var, rel_name.as_deref(), is_scalar_hop, s);
                    s.builder.push_op_mut(GraphOp::Expand {
                        src: src_var,
                        edge: edge_var,
                        dst: dst_var,
                        rel_ty,
                        dir,
                        min_hops,
                        max_hops,
                    });
                    if rel.types.len() > 1 {
                        if is_scalar_hop {
                            s.edge_rel_names.insert(edge_var, None);
                        }
                        let predicate = self
                            .lower_relationship_type_predicate(edge_var, &rel.types, rel.span, s);
                        s.builder.push_op_mut(GraphOp::Filter { predicate });
                    }
                    let prior_edges = path_edges.clone();
                    if path_edges.contains(&edge_var) {
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            rel.span,
                            "RelationshipUniquenessViolation: a relationship variable may not be reused within one pattern",
                        ));
                    }
                    if !prior_edges.is_empty() {
                        s.builder.push_op_mut(GraphOp::RelationshipUnique {
                            edge: edge_var,
                            prior_edges,
                        });
                    }
                    path_edges.push(edge_var);
                    if bound_rel_type_conflict {
                        push_false_filter(s);
                    }

                    // Record the relationship's endpoints so `startNode(r)` /
                    // `endNode(r)` can return the start/end node value (#753).
                    // `src_var`/`dst_var` are the pattern's traversal-left/right
                    // vars; the relationship's true start/end follow its
                    // *direction*: an outgoing `(a)-[r]->(b)` starts at `a`, an
                    // incoming `(a)<-[r]-(b)` starts at `b`. An undirected edge's
                    // orientation is per matched row, so a static left/right
                    // rewrite could pick the wrong endpoint — skip it (startNode/
                    // endNode then fall through to UnknownFunction). Only scalar
                    // single hops qualify: a true variable-length edge var binds
                    // to a *list*, not one relationship.
                    if is_scalar_hop {
                        s.edge_rel_names.insert(edge_var, rel_name.clone());
                        let endpoints = match dir {
                            Direction::Out => Some((src_var, dst_var)),
                            Direction::In => Some((dst_var, src_var)),
                            Direction::Undirected => None,
                        };
                        if let Some(endpoints) = endpoints {
                            s.edge_vars.insert(edge_var, endpoints);
                        }
                    }

                    if path_nodes.is_empty() {
                        path_nodes.push(src_var);
                    }
                    path_nodes.push(dst_var);
                    path_segments.push(PathSegment {
                        edge: edge_var,
                        // Mirror the lowerer's routing, not the syntax: an
                        // explicit `*1..1` goes to the relational join like a
                        // fixed hop, so its edge var binds to scalar edge
                        // columns — only true BFS bounds get the list column.
                        var_len: !(min_hops == 1 && max_hops == Some(1)),
                        rel_name: rel_name.clone(),
                    });

                    // An inline relationship-property map `-[r:KNOWS {since:2020}]->`
                    // is an exact-equality constraint on the edge — lower it to a
                    // Filter over the just-expanded rows, mirroring the node arm
                    // (#748/#750). The read side already materialises
                    // `var_<edge>.<prop>` for a scalar hop (`join_edge_properties`,
                    // #784). A true variable-length edge var binds to a *list*
                    // column, not scalar props, so an inline filter there has
                    // nothing to resolve against (that's #755).
                    if is_scalar_hop {
                        self.lower_inline_property_filter(
                            edge_var,
                            rel.properties.as_ref(),
                            rel.span,
                            s,
                        );
                    } else {
                        self.lower_varlen_inline_property_filter(
                            edge_var,
                            rel_name.as_deref(),
                            rel.properties.as_ref(),
                            rel.span,
                            s,
                        );
                    }

                    prev_node_var = Some(dst_var);
                }
            }
        }

        if let Some(name) = &pat.var {
            if path_nodes.is_empty()
                && let Some(node) = prev_node_var
            {
                path_nodes.push(node);
            }
            Self::bind_path_var(name, pat.span, path_nodes, path_segments, s);
        }
    }

    /// Register a named path variable (`MATCH p = (a)-[*]->(b)`, #754).
    ///
    /// The binding retains every node and segment in traversal order so path
    /// functions can compose fixed and variable-length segments later.
    pub(super) fn bind_path_var(
        name: &str,
        span: Span,
        nodes: Vec<VarId>,
        segments: Vec<PathSegment>,
        s: &mut BinderState,
    ) {
        if s.vars.contains_key(name) || s.path_vars.contains_key(name) {
            s.errors.push(BindError::new(
                BindErrorKind::DuplicateVariable,
                span,
                format!("path variable `{name}` conflicts with an existing variable"),
            ));
            return;
        }
        s.path_vars
            .insert(name.to_owned(), PathBinding { nodes, segments });
    }

    /// Lower an inline node-property map (`(a {name:'x', age:30})`) into a
    /// [`GraphOp::Filter`] of AND-ed equality predicates over `var` (#748).
    ///
    /// openCypher treats inline node properties as exact-equality conjunction,
    /// so `{k1:v1, k2:v2}` becomes `var.k1 = v1 AND var.k2 = v2`. A no-property
    /// node (or an empty `{}`) adds no filter.
    fn lower_inline_property_filter(
        &self,
        var: VarId,
        properties: Option<&Expr>,
        span: Span,
        s: &mut BinderState,
    ) {
        let Some(Expr::Map(map)) = properties else {
            return;
        };
        // `HashMap` iteration order is non-deterministic; sort by key so a
        // multi-property filter produces a stable predicate order (reproducible
        // plans).
        let mut keys: Vec<&String> = map.entries.keys().collect();
        keys.sort();
        let mut combined: Option<ExprId> = None;
        for key in keys {
            let value = self.lower_expr(&map.entries[key], span, s);
            let owner = property_owner_for_var(var, s);
            let prop_span = map.key_spans.get(key).copied().unwrap_or(span);
            let Some(prop) = self.resolve_property(key, prop_span, owner, s) else {
                continue;
            };
            let base = s.builder.push_expr(IrExpr::VarRef(var));
            let access = s.builder.push_expr(IrExpr::PropertyAccess { base, prop });
            let eq = s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Eq,
                left: access,
                right: value,
            });
            combined = Some(match combined {
                None => eq,
                Some(acc) => s.builder.push_expr(IrExpr::BinaryOp {
                    op: BinaryOpKind::And,
                    left: acc,
                    right: eq,
                }),
            });
        }
        if let Some(predicate) = combined {
            s.builder.push_op_mut(GraphOp::Filter { predicate });
        }
    }

    fn lower_varlen_inline_property_filter(
        &self,
        edge_var: VarId,
        rel_name: Option<&str>,
        properties: Option<&Expr>,
        span: Span,
        s: &mut BinderState,
    ) {
        let Some(Expr::Map(map)) = properties else {
            return;
        };
        let mut keys: Vec<&String> = map.entries.keys().collect();
        keys.sort();
        let mut combined = None;
        for key in keys {
            let value = self.lower_expr(&map.entries[key], span, s);
            let owner = BoundPropertyOwner::Relationship(rel_name.map(str::to_owned));
            let prop_span = map.key_spans.get(key).copied().unwrap_or(span);
            let Some(prop) = self.resolve_property(key, prop_span, owner, s) else {
                continue;
            };
            let loop_var = alloc_anon_var(s);
            let element = s.builder.push_expr(IrExpr::VarRef(loop_var));
            let access = s.builder.push_expr(IrExpr::PropertyAccess {
                base: element,
                prop,
            });
            let predicate = s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Eq,
                left: access,
                right: value,
            });
            let list = s.builder.push_expr(IrExpr::VarRef(edge_var));
            let all = s.builder.push_expr(IrExpr::Quantifier {
                kind: graphforge_ast::QuantifierKind::All,
                loop_var,
                list,
                predicate,
            });
            combined = Some(match combined {
                None => all,
                Some(acc) => s.builder.push_expr(IrExpr::BinaryOp {
                    op: BinaryOpKind::And,
                    left: acc,
                    right: all,
                }),
            });
        }
        if let Some(predicate) = combined {
            s.builder.push_op_mut(GraphOp::Filter { predicate });
        }
    }

    pub(super) fn lower_where(&self, w: &WhereClause, s: &mut BinderState) {
        // An aggregate in WHERE is invalid — aggregation happens in WITH/RETURN,
        // and a predicate filtering on `count(...)` must use a WITH first
        // (openCypher `InvalidAggregation`, #956).
        if expr_contains_aggregate(&w.predicate) {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                w.span,
                "an aggregate function may not be used in WHERE".to_string(),
            ));
        }
        self.lower_where_predicate(&w.predicate, w.span, s);
    }

    pub(super) fn lower_where_predicate(
        &self,
        expr: &Expr,
        parent_span: Span,
        s: &mut BinderState,
    ) {
        match expr {
            Expr::Parenthesized { inner, .. } => self.lower_where_predicate(inner, parent_span, s),
            Expr::PatternPredicate(pp) => self.lower_pattern_predicate(pp, false, s),
            Expr::ExistentialSubquery(es) => self.lower_existential_subquery(es, s),
            Expr::UnaryOp(graphforge_ast::UnaryOp {
                op: AstUnOp::Not,
                expr: inner,
                ..
            }) if matches_pattern_predicate(inner) => {
                let Expr::PatternPredicate(pp) = strip_parens(inner) else {
                    unreachable!("matches_pattern_predicate ensured the inner shape");
                };
                self.lower_pattern_predicate(pp, true, s);
            }
            Expr::BinaryOp(graphforge_ast::BinaryOp {
                op: AstBinOp::And,
                left,
                right,
                ..
            }) => {
                self.lower_where_predicate(left, parent_span, s);
                self.lower_where_predicate(right, parent_span, s);
            }
            Expr::BinaryOp(graphforge_ast::BinaryOp {
                op: AstBinOp::Or, ..
            }) => {
                let mut alternatives = Vec::new();
                if collect_pattern_disjunction(expr, &mut alternatives) {
                    self.lower_pattern_predicate_alternatives(&alternatives, false, s);
                } else if expr_contains_pattern_predicate(expr) {
                    let mut branches = Vec::new();
                    if collect_mixed_pattern_disjunction(expr, &mut branches) {
                        self.lower_mixed_pattern_predicate_alternatives(&branches, s);
                    } else {
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            expr.span(),
                            "each OR branch containing a pattern predicate must contain exactly one pattern alternative",
                        ));
                    }
                } else {
                    Self::reject_bare_graph_value_predicate(expr, parent_span, s);
                    let pred = self.lower_expr(expr, parent_span, s);
                    s.builder.push_op_mut(GraphOp::Filter { predicate: pred });
                }
            }
            other if expr_contains_pattern_predicate(other) => {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    other.span(),
                    "pattern predicates are currently supported only as single-relationship \
                     WHERE predicates, with optional NOT and AND",
                ));
            }
            other => {
                Self::reject_bare_graph_value_predicate(other, parent_span, s);
                let pred = self.lower_expr(other, parent_span, s);
                s.builder.push_op_mut(GraphOp::Filter { predicate: pred });
            }
        }
    }

    fn lower_pattern_predicate(&self, pp: &PatternPredicate, negated: bool, s: &mut BinderState) {
        self.lower_pattern_predicate_alternatives(&[pp], negated, s);
    }

    fn lower_existential_subquery(
        &self,
        es: &graphforge_ast::ExistentialSubquery,
        s: &mut BinderState,
    ) {
        let prior_error_count = s.errors.len();
        let outer_vars = s.vars.values().copied().collect::<HashSet<_>>();

        let mut sub_state = BinderState {
            vars: s.vars.clone(),
            path_vars: s.path_vars.clone(),
            node_vars: s.node_vars.clone(),
            edge_vars: s.edge_vars.clone(),
            edge_rel_names: s.edge_rel_names.clone(),
            scalar_list_edges: s.scalar_list_edges.clone(),
            var_kinds: s.var_kinds.clone(),
            next_var: s.next_var,
            builder: GraphPlan::builder("openCypher").ontology_mode(self.mode),
            errors: Vec::new(),
            warnings: Vec::new(),
            captured_pattern_comprehensions: None,
            existential_depth: s.existential_depth + 1,
            standalone_call: false,
        };

        match &es.body {
            ExistentialSubqueryBody::Simple { pattern, filter } => {
                self.lower_path_pattern(pattern, &mut sub_state);
                if let Some(filter) = filter {
                    if expr_contains_aggregate(filter) {
                        sub_state.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            es.span,
                            "an aggregate function may not be used in a simple existential subquery",
                        ));
                    }
                    self.lower_where_predicate(filter, es.span, &mut sub_state);
                }
            }
            ExistentialSubqueryBody::Full(query) => {
                let last = query.clauses.len().saturating_sub(1);
                for (index, clause) in query.clauses.iter().enumerate() {
                    let allowed = matches!(
                        clause,
                        AstClause::Match(_)
                            | AstClause::OptionalMatch(_)
                            | AstClause::With(_)
                            | AstClause::Unwind(_)
                    ) || matches!(clause, AstClause::Return(_)) && index == last;
                    if !allowed {
                        sub_state.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            clause.span(),
                            "a full existential subquery must contain only read clauses and end in RETURN",
                        ));
                        continue;
                    }
                    self.lower_clause(clause, &mut sub_state);
                }
                if !matches!(query.clauses.last(), Some(AstClause::Return(_))) {
                    sub_state.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        es.span,
                        "a full existential subquery must end in RETURN",
                    ));
                }
            }
        }

        let child = sub_state.builder.build();
        let references_outer = plan_references_any_var(&child, &outer_vars);
        s.next_var = s.next_var.max(sub_state.next_var);
        s.errors.extend(sub_state.errors);
        s.warnings.extend(sub_state.warnings);
        if !references_outer {
            s.errors.push(BindError::new(
                BindErrorKind::UndeclaredVariable,
                es.span,
                "existential subquery must reference at least one outer variable",
            ));
        }
        if s.errors.len() == prior_error_count {
            s.builder.push_op_mut(GraphOp::Exists {
                child: Box::new(child),
                negated: false,
            });
        }
    }

    fn lower_pattern_predicate_alternatives(
        &self,
        alternatives: &[&PatternPredicate],
        negated: bool,
        s: &mut BinderState,
    ) {
        let prior_error_count = s.errors.len();
        let mut children = Vec::new();
        for pp in alternatives {
            if let Some(name) = pp.pattern.var.as_deref() {
                s.errors.push(BindError::new(
                    BindErrorKind::UndeclaredVariable,
                    pp.span,
                    format!("path variable `{name}` is not bound in this pattern predicate scope"),
                ));
                continue;
            }
            if !is_single_relationship_pattern(&pp.pattern) && s.existential_depth < 2 {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    pp.span,
                    "multi-relationship pattern predicates are supported only in nested existential subqueries",
                ));
                continue;
            }
            if pattern_has_var_length_relationship_properties(&pp.pattern) {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    pp.span,
                    "variable-length relationships in pattern predicates cannot have property maps",
                ));
                continue;
            }
            for pattern in relationship_type_alternatives(&pp.pattern) {
                if let Some(child) = self.bind_pattern_predicate_child(&pattern, pp.span, s) {
                    children.push(child);
                }
            }
        }

        if children.is_empty() || s.errors.len() > prior_error_count {
            return;
        }
        let child = if children.len() == 1 {
            children.pop().expect("one child remains")
        } else {
            GraphPlan::builder("openCypher")
                .push_op(GraphOp::Union {
                    all: true,
                    inputs: children,
                })
                .build()
        };
        s.builder.push_op_mut(GraphOp::Exists {
            child: Box::new(child),
            negated,
        });
    }

    fn lower_mixed_pattern_predicate_alternatives(
        &self,
        branches: &[MixedPatternBranch<'_>],
        s: &mut BinderState,
    ) {
        let prior_error_count = s.errors.len();
        let mut children = Vec::new();
        for branch in branches {
            for pattern in relationship_type_alternatives(&branch.pattern.pattern) {
                if let Some(child) = self.bind_pattern_predicate_child_with_filters(
                    &pattern,
                    branch.pattern.span,
                    &branch.scalar_filters,
                    s,
                ) {
                    children.push(child);
                }
            }
        }
        if children.is_empty() || s.errors.len() > prior_error_count {
            return;
        }
        let child = if children.len() == 1 {
            children.pop().expect("one child remains")
        } else {
            GraphPlan::builder("openCypher")
                .push_op(GraphOp::Union {
                    all: true,
                    inputs: children,
                })
                .build()
        };
        s.builder.push_op_mut(GraphOp::Exists {
            child: Box::new(child),
            negated: false,
        });
    }

    fn bind_pattern_predicate_child(
        &self,
        pattern: &PathPattern,
        span: Span,
        s: &mut BinderState,
    ) -> Option<GraphPlan> {
        self.bind_pattern_predicate_child_with_filters(pattern, span, &[], s)
    }

    fn bind_pattern_predicate_child_with_filters(
        &self,
        pattern: &PathPattern,
        span: Span,
        scalar_filters: &[&Expr],
        s: &mut BinderState,
    ) -> Option<GraphPlan> {
        let prior_error_count = s.errors.len();
        let mut sub_state = BinderState {
            vars: s.vars.clone(),
            path_vars: s.path_vars.clone(),
            node_vars: s.node_vars.clone(),
            edge_vars: s.edge_vars.clone(),
            edge_rel_names: s.edge_rel_names.clone(),
            scalar_list_edges: s.scalar_list_edges.clone(),
            var_kinds: s.var_kinds.clone(),
            next_var: s.next_var,
            builder: GraphPlan::builder("openCypher").ontology_mode(self.mode),
            errors: Vec::new(),
            warnings: Vec::new(),
            captured_pattern_comprehensions: None,
            existential_depth: s.existential_depth,
            standalone_call: false,
        };
        self.lower_path_pattern(pattern, &mut sub_state);
        for filter in scalar_filters {
            self.lower_where_predicate(filter, filter.span(), &mut sub_state);
        }

        for name in sub_state.vars.keys() {
            if !s.vars.contains_key(name) {
                sub_state.errors.push(BindError::new(
                    BindErrorKind::UndeclaredVariable,
                    span,
                    format!("variable `{name}` is not bound in this pattern predicate scope"),
                ));
            }
        }

        if !pattern_references_bound_var(pattern, s) {
            sub_state.errors.push(BindError::new(
                BindErrorKind::UndeclaredVariable,
                span,
                "pattern predicate must reference at least one bound variable",
            ));
        }

        s.errors.extend(sub_state.errors);
        s.warnings.extend(sub_state.warnings);
        if s.errors.len() > prior_error_count {
            return None;
        }

        Some(sub_state.builder.build())
    }

    pub(super) fn lower_pattern_comprehension(
        &self,
        pc: &graphforge_ast::PatternComprehension,
        s: &mut BinderState,
    ) -> ExprId {
        let prior_error_count = s.errors.len();
        let output = VarId(s.next_var);
        s.next_var += 1;

        let mut pattern = pc.pattern.clone();
        pattern.var.clone_from(&pc.var);
        let alternatives = relationship_type_alternatives(&pattern);
        let mut children = Vec::with_capacity(alternatives.len());
        for pattern in alternatives {
            let mut sub_state = BinderState {
                vars: s.vars.clone(),
                path_vars: s.path_vars.clone(),
                node_vars: s.node_vars.clone(),
                edge_vars: s.edge_vars.clone(),
                edge_rel_names: s.edge_rel_names.clone(),
                scalar_list_edges: s.scalar_list_edges.clone(),
                var_kinds: s.var_kinds.clone(),
                next_var: s.next_var,
                builder: GraphPlan::builder("openCypher").ontology_mode(self.mode),
                errors: Vec::new(),
                warnings: Vec::new(),
                captured_pattern_comprehensions: None,
                existential_depth: s.existential_depth,
                standalone_call: false,
            };
            self.lower_path_pattern(&pattern, &mut sub_state);

            if let Some(filter) = &pc.filter {
                if expr_contains_aggregate(filter) {
                    sub_state.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        pc.span,
                        "an aggregate function may not be used in a pattern comprehension filter",
                    ));
                }
                self.lower_where_predicate(filter, pc.span, &mut sub_state);
            }
            if expr_contains_aggregate(&pc.projection) {
                sub_state.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    pc.span,
                    "an aggregate function may not be used in a pattern comprehension projection",
                ));
            }

            let projection = self
                .lower_projection_value_expr(&pc.projection, pc.span, &mut sub_state, true)
                .unwrap_or_else(|| self.lower_expr(&pc.projection, pc.span, &mut sub_state));
            sub_state.builder.push_op_mut(GraphOp::Project {
                items: vec![ProjectItem {
                    expr: projection,
                    alias: Some(PATTERN_COMPREHENSION_VALUE_ALIAS.into()),
                    out_var: None,
                }],
                distinct: false,
            });

            s.next_var = s.next_var.max(sub_state.next_var);
            s.errors.extend(sub_state.errors);
            s.warnings.extend(sub_state.warnings);
            children.push(sub_state.builder.build());
        }
        if s.errors.len() > prior_error_count {
            return s.builder.push_expr(IrExpr::Literal(IrLiteral::Null));
        }

        let mut outputs = Vec::with_capacity(children.len());
        for (index, child) in children.into_iter().enumerate() {
            let child_output = if index == 0 {
                output
            } else {
                let next = VarId(s.next_var);
                s.next_var += 1;
                next
            };
            let child = Box::new(child);
            if let Some(captured) = s.captured_pattern_comprehensions.as_mut() {
                captured.push((child, child_output));
            } else {
                s.builder.push_op_mut(GraphOp::PatternComprehension {
                    child,
                    output: child_output,
                });
            }
            outputs.push(s.builder.push_expr(IrExpr::VarRef(child_output)));
        }
        let mut outputs = outputs.into_iter();
        let Some(mut combined) = outputs.next() else {
            return s.builder.push_expr(IrExpr::ListLiteral(Vec::new()));
        };
        for next in outputs {
            combined = s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Add,
                left: combined,
                right: next,
            });
        }
        combined
    }

    fn reject_bare_graph_value_predicate(expr: &Expr, span: Span, s: &mut BinderState) {
        let Expr::Var(VarRef { name, .. }) = strip_parens(expr) else {
            return;
        };
        if s.path_vars.contains_key(name) {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                span,
                format!("WHERE predicate `{name}` must be a boolean expression"),
            ));
            return;
        }
        let Some(var_id) = s.vars.get(name).copied() else {
            return;
        };
        if s.node_vars.contains_key(&var_id) || s.edge_rel_names.contains_key(&var_id) {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                span,
                format!("WHERE predicate `{name}` must be a boolean expression"),
            ));
        }
    }

    /// Rewrite `nodes(p)` / `relationships(p)` / `length(p)` over a named path
    /// variable onto the path's constituent variables (#754).
    ///
    /// Returns `None` when the call is not a path function — including these
    /// names over a non-path argument (`length(r)` on an edge list keeps its
    /// generic lowering) — so the caller falls through unchanged.
    pub(super) fn lower_path_function(call: &FunctionCall, s: &mut BinderState) -> Option<ExprId> {
        let [fn_name] = call.name.as_slice() else {
            return None;
        };
        let fn_name = fn_name.to_ascii_lowercase();
        if !matches!(fn_name.as_str(), "nodes" | "relationships" | "length") {
            return None;
        }
        let [Expr::Var(VarRef { name, .. })] = call.args.as_slice() else {
            return None;
        };
        let binding = s.path_vars.get(name)?.clone();
        let id = match fn_name.as_str() {
            "nodes" => Self::path_nodes_expr(&binding, s),
            "relationships" => Self::path_rels_expr(&binding, s),
            "length" => Self::path_length_expr(&binding, s),
            _ => unreachable!("guarded by the name match above"),
        };
        Some(id)
    }

    /// Resolve `startNode(r)` / `endNode(r)` over a fixed-hop matched
    /// relationship to the relationship's src / dst node variable (#753).
    ///
    /// Returns `None` for anything else — a non-matching name, a non-variable
    /// argument, an unbound name, or an edge var that is not a recorded
    /// fixed-hop endpoint (variable-length edges bind to a list, not one
    /// relationship) — so the caller falls through to generic lowering, where
    /// the function name surfaces as the usual `UnknownFunction` error.
    pub(super) fn resolve_endpoint_node(call: &FunctionCall, s: &BinderState) -> Option<VarId> {
        let [fn_name] = call.name.as_slice() else {
            return None;
        };
        let start = match fn_name.to_ascii_lowercase().as_str() {
            "startnode" => true,
            "endnode" => false,
            _ => return None,
        };
        let [Expr::Var(VarRef { name, .. })] = call.args.as_slice() else {
            return None;
        };
        let edge_var = s.vars.get(name)?;
        let (src, dst) = s.edge_vars.get(edge_var)?;
        Some(if start { *src } else { *dst })
    }

    /// Build a `_node_struct` call that materializes the given node var as a
    /// whole node value — identity + labels + properties (#785). Shared by a
    /// bare `RETURN n` and by `RETURN startNode(r)` / `endNode(r)` (#753).
    pub(super) fn node_struct_expr(var: VarId, s: &mut BinderState) -> ExprId {
        // The pattern's label name, passed through because the lowerer's
        // ontology map is empty in exploratory mode; absent for an unlabelled
        // match (one arg).
        let label_opt = s.node_vars.get(&var).cloned().flatten();
        let var_ref = s.builder.push_expr(IrExpr::VarRef(var));
        let mut args = vec![var_ref];
        if let Some(label) = label_opt {
            let lit = s.builder.push_expr(IrExpr::Literal(IrLiteral::Str(label)));
            args.push(lit);
        }
        s.builder.push_expr(IrExpr::FunctionCall {
            name: "_node_struct".to_string(),
            args,
        })
    }

    pub(super) fn relationship_struct_expr(var: VarId, s: &mut BinderState) -> ExprId {
        let rel_name = s.edge_rel_names.get(&var).cloned().flatten();
        let edge = s.builder.push_expr(IrExpr::VarRef(var));
        let rel_name = s.builder.push_expr(IrExpr::Literal(match rel_name {
            Some(name) => IrLiteral::Str(name),
            None => IrLiteral::Null,
        }));
        s.builder.push_expr(IrExpr::FunctionCall {
            name: "_rel_struct".into(),
            args: vec![edge, rel_name],
        })
    }

    pub(super) fn relationship_struct_list_expr(var: VarId, s: &mut BinderState) -> ExprId {
        let edge = s.builder.push_expr(IrExpr::VarRef(var));
        let rel_name = s.builder.push_expr(IrExpr::Literal(
            match s.edge_rel_names.get(&var).cloned().flatten() {
                Some(name) => IrLiteral::Str(name),
                None => IrLiteral::Null,
            },
        ));
        s.builder.push_expr(IrExpr::FunctionCall {
            name: "_rel_struct_list".into(),
            args: vec![edge, rel_name],
        })
    }

    /// IR for `nodes(p)`: the traversal node sequence as a
    /// `List<Struct{node_uuid}>` value.
    ///
    /// Variable-length: recovered at runtime by walking the edge list from the
    /// start node (`_path_nodes`, graphforge-rel). Fixed single hop: the two endpoint
    /// node vars directly (`_node_struct_list`) — traversal order comes from
    /// the binder's pattern walk, so `(a)<-[r]-(b)` yields `[a, b]` regardless
    /// of storage orientation.
    fn path_nodes_expr(binding: &PathBinding, s: &mut BinderState) -> ExprId {
        if binding.segments.is_empty() {
            let node = Self::node_struct_expr(binding.nodes[0], s);
            return s.builder.push_expr(IrExpr::ListLiteral(vec![node]));
        }
        if binding.segments.len() > 1 {
            let mut combined = None;
            for (index, segment) in binding.segments.iter().enumerate() {
                let part = PathBinding {
                    nodes: binding.nodes[index..=index + 1].to_vec(),
                    segments: vec![segment.clone()],
                };
                let mut nodes = Self::path_nodes_expr(&part, s);
                if index > 0 {
                    nodes = s.builder.push_expr(IrExpr::FunctionCall {
                        name: "tail".into(),
                        args: vec![nodes],
                    });
                }
                combined = Some(match combined {
                    None => nodes,
                    Some(left) => s.builder.push_expr(IrExpr::BinaryOp {
                        op: BinaryOpKind::Add,
                        left,
                        right: nodes,
                    }),
                });
            }
            return combined.expect("multi-segment path has node parts");
        }
        let seg = &binding.segments[0];
        if seg.var_len {
            let start = s.builder.push_expr(IrExpr::VarRef(binding.nodes[0]));
            let rels = s.builder.push_expr(IrExpr::VarRef(seg.edge));
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "_path_nodes".into(),
                args: vec![start, rels],
            })
        } else {
            // The trailing edge VarRef is the null gate: an unmatched
            // OPTIONAL MATCH row's path is Cypher null, and graphforge-rel keys that
            // on the edge's `edge_uuid` being null.
            let a = s.builder.push_expr(IrExpr::VarRef(binding.nodes[0]));
            let b = s.builder.push_expr(IrExpr::VarRef(binding.nodes[1]));
            let edge = s.builder.push_expr(IrExpr::VarRef(seg.edge));
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "_node_struct_list".into(),
                args: vec![a, b, edge],
            })
        }
    }

    /// IR for `relationships(p)`: the relationship sequence.
    ///
    /// Variable-length: the #709 relationship-list column verbatim. Fixed
    /// single hop: a one-element list built from the edge var's scalar columns
    /// (`_rel_struct_list`), with the bind-time relation name as `rel_type` —
    /// topology fields only, no edge properties (a documented gap vs the
    /// var-length list; see #754 follow-ups).
    fn path_rels_expr(binding: &PathBinding, s: &mut BinderState) -> ExprId {
        if binding.segments.is_empty() {
            return s.builder.push_expr(IrExpr::ListLiteral(Vec::new()));
        }
        if binding.segments.len() > 1 {
            let mut combined = None;
            for (index, segment) in binding.segments.iter().enumerate() {
                let part = PathBinding {
                    nodes: binding.nodes[index..=index + 1].to_vec(),
                    segments: vec![segment.clone()],
                };
                let rels = Self::path_rels_expr(&part, s);
                combined = Some(match combined {
                    None => rels,
                    Some(left) => s.builder.push_expr(IrExpr::BinaryOp {
                        op: BinaryOpKind::Add,
                        left,
                        right: rels,
                    }),
                });
            }
            return combined.expect("multi-segment path has relationship parts");
        }
        let seg = &binding.segments[0];
        if seg.var_len {
            s.builder.push_expr(IrExpr::VarRef(seg.edge))
        } else {
            let edge = s.builder.push_expr(IrExpr::VarRef(seg.edge));
            let rel_name = s.builder.push_expr(IrExpr::Literal(match &seg.rel_name {
                Some(n) => IrLiteral::Str(n.clone()),
                None => IrLiteral::Null,
            }));
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "_rel_struct_list".into(),
                args: vec![edge, rel_name],
            })
        }
    }

    /// IR for `length(p)`: the relationship count (openCypher path length).
    ///
    /// Variable-length: the edge list's element count (0-hop → 0). Fixed
    /// single hop: the constant 1 when the hop matched, null otherwise
    /// (`_path_fixed_length` over the edge VarRef gate; UInt64 so both forms
    /// agree on the output type).
    fn path_length_expr(binding: &PathBinding, s: &mut BinderState) -> ExprId {
        if binding.segments.is_empty() {
            return s.builder.push_expr(IrExpr::Literal(IrLiteral::Int(0)));
        }
        if binding.segments.len() > 1 {
            let mut total = None;
            for (index, segment) in binding.segments.iter().enumerate() {
                let part = PathBinding {
                    nodes: binding.nodes[index..=index + 1].to_vec(),
                    segments: vec![segment.clone()],
                };
                let length = Self::path_length_expr(&part, s);
                total = Some(match total {
                    None => length,
                    Some(left) => s.builder.push_expr(IrExpr::BinaryOp {
                        op: BinaryOpKind::Add,
                        left,
                        right: length,
                    }),
                });
            }
            return total.expect("multi-segment path has lengths");
        }
        let seg = &binding.segments[0];
        let rels = s.builder.push_expr(IrExpr::VarRef(seg.edge));
        if seg.var_len {
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "length".into(),
                args: vec![rels],
            })
        } else {
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "_path_fixed_length".into(),
                args: vec![rels],
            })
        }
    }

    /// IR for a bare path value (`RETURN p`): a
    /// `Struct{nodes, relationships}` assembled from the same expressions the
    /// path functions use (`_path_struct` → `named_struct` in graphforge-rel).
    pub(super) fn path_struct_expr(binding: &PathBinding, s: &mut BinderState) -> ExprId {
        let nodes = Self::path_nodes_expr(binding, s);
        let rels = Self::path_rels_expr(binding, s);
        s.builder.push_expr(IrExpr::FunctionCall {
            name: "_path_struct".into(),
            args: vec![nodes, rels],
        })
    }

    // -----------------------------------------------------------------------
    // Type resolution
    // -----------------------------------------------------------------------

    pub(super) fn resolve_label(
        &self,
        name: &str,
        span: Span,
        s: &mut BinderState,
    ) -> Option<EntityTypeId> {
        let resolved = if let Some(composition) = &self.composition {
            match composition.resolve(graphforge_ontology::SymbolKind::Entity, name) {
                Ok((binding, receipt)) => {
                    s.builder.push_binding_receipt(receipt);
                    match binding {
                        SymbolBinding::Qualified(symbol) => composition
                            .semantic_id(&symbol)
                            .map_err(|error| error.to_string())
                            .and_then(|id| {
                                EntityTypeId::ontology(id).map_err(|error| error.to_string())
                            }),
                        SymbolBinding::Runtime { local_id, .. } => self
                            .catalog
                            .lock()
                            .unwrap()
                            .intern_label(&local_id)
                            .map(EntityTypeId::runtime)
                            .map_err(|error| error.to_string()),
                    }
                }
                Err(diagnostic) => {
                    Self::push_composition_error(&diagnostic, span, s);
                    return None;
                }
            }
        } else if let Some(id) = self
            .ontology
            .as_ref()
            .and_then(|handle| handle.entity_type_id(name))
        {
            EntityTypeId::ontology(id).map_err(|error| error.to_string())
        } else {
            match self.mode {
                OntologyMode::Strict => {
                    s.errors.push(BindError::new(
                        BindErrorKind::UnknownLabel,
                        span,
                        format!("unknown label `{name}` (strict mode)"),
                    ));
                    return None;
                }
                OntologyMode::Advisory => {
                    s.warnings.push(BindError::new(
                        BindErrorKind::UnknownLabel,
                        span,
                        format!("unknown label `{name}` — using runtime catalog"),
                    ));
                }
                OntologyMode::Exploratory => {}
            }
            self.catalog
                .lock()
                .unwrap()
                .intern_label(name)
                .map(EntityTypeId::runtime)
                .map_err(|error| error.to_string())
        };
        match resolved {
            Ok(id) => Some(id),
            Err(error) => {
                s.errors
                    .push(BindError::new(BindErrorKind::InvalidArgument, span, error));
                None
            }
        }
    }

    pub(super) fn resolve_relation_type(
        &self,
        name: &str,
        span: Span,
        s: &mut BinderState,
    ) -> Option<RelationTypeId> {
        let resolved = if let Some(composition) = &self.composition {
            match composition.resolve(graphforge_ontology::SymbolKind::Relation, name) {
                Ok((binding, receipt)) => {
                    s.builder.push_binding_receipt(receipt);
                    match binding {
                        SymbolBinding::Qualified(symbol) => composition
                            .semantic_id(&symbol)
                            .map_err(|error| error.to_string())
                            .and_then(|id| {
                                RelationTypeId::ontology(id).map_err(|error| error.to_string())
                            }),
                        SymbolBinding::Runtime { local_id, .. } => self
                            .catalog
                            .lock()
                            .unwrap()
                            .intern_relation_type(&local_id)
                            .map(RelationTypeId::runtime)
                            .map_err(|error| error.to_string()),
                    }
                }
                Err(diagnostic) => {
                    Self::push_composition_error(&diagnostic, span, s);
                    return None;
                }
            }
        } else if let Some(id) = self
            .ontology
            .as_ref()
            .and_then(|handle| handle.relation_type_id(name))
        {
            RelationTypeId::ontology(id).map_err(|error| error.to_string())
        } else {
            match self.mode {
                OntologyMode::Strict => {
                    s.errors.push(BindError::new(
                        BindErrorKind::UnknownRelationType,
                        span,
                        format!("unknown relation type `{name}` (strict mode)"),
                    ));
                    return None;
                }
                OntologyMode::Advisory => {
                    s.warnings.push(BindError::new(
                        BindErrorKind::UnknownRelationType,
                        span,
                        format!("unknown relation type `{name}` — using runtime catalog"),
                    ));
                }
                OntologyMode::Exploratory => {}
            }
            self.catalog
                .lock()
                .unwrap()
                .intern_relation_type(name)
                .map(RelationTypeId::runtime)
                .map_err(|error| error.to_string())
        };
        match resolved {
            Ok(id) => Some(id),
            Err(error) => {
                s.errors
                    .push(BindError::new(BindErrorKind::InvalidArgument, span, error));
                None
            }
        }
    }
}

pub(super) fn expr_contains_pattern_comprehension(expr: &Expr) -> bool {
    match expr {
        Expr::PatternComprehension(_) => true,
        Expr::Parenthesized { inner, .. } => expr_contains_pattern_comprehension(inner),
        Expr::BinaryOp(b) => {
            expr_contains_pattern_comprehension(&b.left)
                || expr_contains_pattern_comprehension(&b.right)
        }
        Expr::UnaryOp(u) => expr_contains_pattern_comprehension(&u.expr),
        Expr::IsNull { expr, .. } => expr_contains_pattern_comprehension(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_pattern_comprehension(expr) || expr_contains_pattern_comprehension(list)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_pattern_comprehension(expr)
                || expr_contains_pattern_comprehension(pattern)
        }
        Expr::FunctionCall(call) => call.args.iter().any(expr_contains_pattern_comprehension),
        Expr::Property(property) => expr_contains_pattern_comprehension(&property.object),
        Expr::List(list) => list
            .elements
            .iter()
            .any(expr_contains_pattern_comprehension),
        Expr::Map(map) => map
            .entries
            .values()
            .any(expr_contains_pattern_comprehension),
        Expr::Case(case) => {
            case.subject
                .as_deref()
                .is_some_and(expr_contains_pattern_comprehension)
                || case.when_clauses.iter().any(|when| {
                    expr_contains_pattern_comprehension(&when.condition)
                        || expr_contains_pattern_comprehension(&when.result)
                })
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(expr_contains_pattern_comprehension)
        }
        Expr::ListComprehension(lc) => {
            expr_contains_pattern_comprehension(&lc.list)
                || lc
                    .filter
                    .as_deref()
                    .is_some_and(expr_contains_pattern_comprehension)
                || lc
                    .projection
                    .as_deref()
                    .is_some_and(expr_contains_pattern_comprehension)
        }
        Expr::Quantifier(q) => {
            expr_contains_pattern_comprehension(&q.list)
                || expr_contains_pattern_comprehension(&q.predicate)
        }
        _ => false,
    }
}

pub(super) fn strip_parens(expr: &Expr) -> &Expr {
    match expr {
        Expr::Parenthesized { inner, .. } => strip_parens(inner),
        other => other,
    }
}

fn matches_pattern_predicate(expr: &Expr) -> bool {
    matches!(strip_parens(expr), Expr::PatternPredicate(_))
}

pub(super) fn expr_contains_pattern_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::PatternPredicate(_) => true,
        Expr::Parenthesized { inner, .. } => expr_contains_pattern_predicate(inner),
        Expr::BinaryOp(b) => {
            expr_contains_pattern_predicate(&b.left) || expr_contains_pattern_predicate(&b.right)
        }
        Expr::UnaryOp(u) => expr_contains_pattern_predicate(&u.expr),
        Expr::IsNull { expr, .. } => expr_contains_pattern_predicate(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_pattern_predicate(expr) || expr_contains_pattern_predicate(list)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_pattern_predicate(expr) || expr_contains_pattern_predicate(pattern)
        }
        Expr::FunctionCall(call) => call.args.iter().any(expr_contains_pattern_predicate),
        Expr::List(list) => list.elements.iter().any(expr_contains_pattern_predicate),
        Expr::Map(map) => map.entries.values().any(expr_contains_pattern_predicate),
        Expr::Case(case) => {
            case.subject
                .as_deref()
                .is_some_and(expr_contains_pattern_predicate)
                || case.when_clauses.iter().any(|when| {
                    expr_contains_pattern_predicate(&when.condition)
                        || expr_contains_pattern_predicate(&when.result)
                })
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(expr_contains_pattern_predicate)
        }
        Expr::ListComprehension(lc) => {
            expr_contains_pattern_predicate(&lc.list)
                || lc
                    .filter
                    .as_deref()
                    .is_some_and(expr_contains_pattern_predicate)
                || lc
                    .projection
                    .as_deref()
                    .is_some_and(expr_contains_pattern_predicate)
        }
        Expr::Quantifier(q) => {
            expr_contains_pattern_predicate(&q.list)
                || expr_contains_pattern_predicate(&q.predicate)
        }
        Expr::PatternComprehension(pc) => {
            pc.filter
                .as_deref()
                .is_some_and(expr_contains_pattern_predicate)
                || expr_contains_pattern_predicate(&pc.projection)
        }
        _ => false,
    }
}

fn relationship_type_alternatives(pattern: &PathPattern) -> Vec<PathPattern> {
    let Some(PathElement::Rel(rel)) = pattern.elements.get(1) else {
        return vec![pattern.clone()];
    };
    if rel.types.len() <= 1 {
        return vec![pattern.clone()];
    }

    rel.types
        .iter()
        .map(|rel_type| {
            let mut alternative = pattern.clone();
            let PathElement::Rel(rel) = &mut alternative.elements[1] else {
                unreachable!("single-relationship pattern shape was checked")
            };
            rel.types = vec![rel_type.clone()];
            alternative
        })
        .collect()
}

fn is_single_relationship_pattern(pattern: &PathPattern) -> bool {
    matches!(
        pattern.elements.as_slice(),
        [
            PathElement::Node(_),
            PathElement::Rel(_),
            PathElement::Node(_)
        ]
    )
}

fn pattern_has_var_length_relationship_properties(pattern: &PathPattern) -> bool {
    pattern.elements.iter().any(|element| {
        let PathElement::Rel(rel) = element else {
            return false;
        };
        (rel.min_hops.is_some() || rel.max_hops.is_some()) && rel.properties.is_some()
    })
}

fn collect_pattern_disjunction<'a>(
    expr: &'a Expr,
    alternatives: &mut Vec<&'a PatternPredicate>,
) -> bool {
    match expr {
        Expr::Parenthesized { inner, .. } => collect_pattern_disjunction(inner, alternatives),
        Expr::PatternPredicate(pp) => {
            alternatives.push(pp);
            true
        }
        Expr::BinaryOp(graphforge_ast::BinaryOp {
            op: AstBinOp::Or,
            left,
            right,
            ..
        }) => {
            collect_pattern_disjunction(left, alternatives)
                && collect_pattern_disjunction(right, alternatives)
        }
        _ => false,
    }
}

struct MixedPatternBranch<'a> {
    pattern: &'a PatternPredicate,
    scalar_filters: Vec<&'a Expr>,
}

fn collect_mixed_pattern_disjunction<'a>(
    expr: &'a Expr,
    branches: &mut Vec<MixedPatternBranch<'a>>,
) -> bool {
    match expr {
        Expr::Parenthesized { inner, .. } => collect_mixed_pattern_disjunction(inner, branches),
        Expr::BinaryOp(graphforge_ast::BinaryOp {
            op: AstBinOp::Or,
            left,
            right,
            ..
        }) => {
            collect_mixed_pattern_disjunction(left, branches)
                && collect_mixed_pattern_disjunction(right, branches)
        }
        branch => {
            let mut conjuncts = Vec::new();
            collect_conjuncts(branch, &mut conjuncts);
            let mut pattern = None;
            let mut scalar_filters = Vec::new();
            for conjunct in conjuncts {
                let conjunct = strip_parens(conjunct);
                if let Expr::PatternPredicate(found) = conjunct {
                    if pattern.replace(found).is_some() {
                        return false;
                    }
                } else if expr_contains_pattern_predicate(conjunct) {
                    return false;
                } else {
                    scalar_filters.push(conjunct);
                }
            }
            let Some(pattern) = pattern else {
                return false;
            };
            branches.push(MixedPatternBranch {
                pattern,
                scalar_filters,
            });
            true
        }
    }
}

fn collect_conjuncts<'a>(expr: &'a Expr, conjuncts: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Parenthesized { inner, .. } => collect_conjuncts(inner, conjuncts),
        Expr::BinaryOp(graphforge_ast::BinaryOp {
            op: AstBinOp::And,
            left,
            right,
            ..
        }) => {
            collect_conjuncts(left, conjuncts);
            collect_conjuncts(right, conjuncts);
        }
        other => conjuncts.push(other),
    }
}

fn plan_references_any_var(plan: &GraphPlan, vars: &HashSet<VarId>) -> bool {
    let expression_reference = (0..plan.exprs.len()).any(|index| {
        let index = u32::try_from(index).expect("ExprArena length is capped at u32::MAX");
        matches!(
            plan.exprs.get(ExprId(index)),
            IrExpr::VarRef(var) if vars.contains(var)
        )
    });
    expression_reference
        || plan.ops.iter().any(|op| match op {
            GraphOp::NodeScan { var, .. }
            | GraphOp::EdgeScan { var, .. }
            | GraphOp::TypedEdgeScan { var, .. } => vars.contains(var),
            GraphOp::Expand { src, edge, dst, .. } => {
                vars.contains(src) || vars.contains(edge) || vars.contains(dst)
            }
            GraphOp::Optional { child }
            | GraphOp::Exists { child, .. }
            | GraphOp::PatternComprehension { child, .. }
            | GraphOp::ListElementPatternComprehension { child, .. } => {
                plan_references_any_var(child, vars)
            }
            GraphOp::Union { inputs, .. } => inputs
                .iter()
                .any(|input| plan_references_any_var(input, vars)),
            _ => false,
        })
}

fn graph_op_bound_vars(op: &GraphOp) -> Vec<VarId> {
    match op {
        GraphOp::NodeScan { var, .. }
        | GraphOp::EdgeScan { var, .. }
        | GraphOp::TypedEdgeScan { var, .. } => vec![*var],
        GraphOp::Expand { src, edge, dst, .. } => vec![*src, *edge, *dst],
        _ => Vec::new(),
    }
}

fn pattern_references_bound_var(pattern: &PathPattern, s: &BinderState) -> bool {
    pattern.elements.iter().any(|element| match element {
        PathElement::Node(node) => node
            .var
            .as_deref()
            .is_some_and(|name| s.vars.contains_key(name)),
        PathElement::Rel(rel) => rel
            .var
            .as_deref()
            .is_some_and(|name| s.vars.contains_key(name)),
    })
}

fn bound_rel_type_conflict(
    edge_var: VarId,
    rel_name: Option<&str>,
    is_scalar_hop: bool,
    s: &BinderState,
) -> bool {
    is_scalar_hop
        && matches!(
            (s.edge_rel_names.get(&edge_var).and_then(Option::as_deref), rel_name),
            (Some(prev), Some(next)) if prev != next
        )
}

fn push_false_filter(s: &mut BinderState) {
    let predicate = s.builder.push_expr(IrExpr::Literal(IrLiteral::Bool(false)));
    s.builder.push_op_mut(GraphOp::Filter { predicate });
}

pub(super) fn push_conjunction(mut predicates: Vec<ExprId>, s: &mut BinderState) -> ExprId {
    let Some(mut acc) = predicates.pop() else {
        return s.builder.push_expr(IrExpr::Literal(IrLiteral::Bool(true)));
    };
    while let Some(next) = predicates.pop() {
        acc = s.builder.push_expr(IrExpr::BinaryOp {
            op: BinaryOpKind::And,
            left: next,
            right: acc,
        });
    }
    acc
}

pub(super) fn lower_direction(dir: graphforge_ast::Direction) -> Direction {
    match dir {
        graphforge_ast::Direction::Out => Direction::Out,
        graphforge_ast::Direction::In => Direction::In,
        graphforge_ast::Direction::Undirected => Direction::Undirected,
    }
}

#[cfg(test)]
mod tests;
