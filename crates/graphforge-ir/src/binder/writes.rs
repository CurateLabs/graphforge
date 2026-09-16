//! CREATE, MERGE, DELETE, SET, and REMOVE clause binding.

use super::patterns::lower_direction;
use super::projection::rewrite_projection_alias_refs;
use super::{
    BindError, BindErrorKind, Binder, BinderState, PathSegment, VarKind, alloc_anon_var,
    bind_var_kind, ensure_var, property_owner_for_var,
};
use crate::expr::IrExpr;
use crate::plan::GraphOp;
use crate::{
    CreateEdgeSpec, CreateNodeSpec, CreatePattern, Direction, ExprId, MergeSetItem, RemovePropItem,
    SetMapItem, SetPropItem, VarId,
};
use graphforge_ast::{
    CreateClause, Expr, MapLiteral, PathElement, PropertyAccess, RemoveClause, RemoveItem,
    ReturnItem, SetClause, SetItem, VarRef,
};
use graphforge_core::Span;
use graphforge_value::PropertyId;
use std::collections::HashMap;

impl Binder {
    /// Lower a `CREATE` clause into a single [`GraphOp::Create`].
    ///
    /// Walks each path pattern's node/relationship elements (mirroring
    /// [`lower_path_pattern`](Self::lower_path_pattern)) and accumulates the
    /// node and edge specs.  Variables are shared across the clause's patterns
    /// via the binder scope, so an edge in one pattern may reference a node
    /// bound in another.  Property maps are lowered into the [`ExprArena`] and
    /// referenced by [`ExprId`]; the relational lowering layer resolves them to
    /// literals at write time.
    pub(super) fn lower_create(&self, c: &CreateClause, s: &mut BinderState) {
        let pattern = self.bind_create_patterns(&c.patterns, false, s);
        s.builder.push_op_mut(GraphOp::Create { pattern });
    }

    pub(super) fn lower_merge(&self, m: &graphforge_ast::MergeClause, s: &mut BinderState) {
        let pattern = self.bind_create_patterns(std::slice::from_ref(&m.pattern), true, s);
        let on_create = self.lower_merge_actions(&m.on_create, s);
        let on_match = self.lower_merge_actions(&m.on_match, s);
        s.builder.push_op_mut(GraphOp::Merge {
            pattern,
            on_create,
            on_match,
        });
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one stateful walk keeps node, relationship, and named-path bindings aligned"
    )]
    fn bind_create_patterns(
        &self,
        patterns: &[graphforge_ast::PathPattern],
        allow_undirected_relationship: bool,
        s: &mut BinderState,
    ) -> CreatePattern {
        let mut pattern = CreatePattern::default();
        let mut created_property_bindings: Vec<ReturnItem> = Vec::new();

        // Variables bound BEFORE this CREATE (by a preceding MATCH/WITH/…). A
        // node spec for such a var is a *reference* (resolve the matched node per
        // row), not a mint (#703). Snapshot before `ensure_var` introduces any
        // of this clause's new vars. `ensure_var_name` reuses an existing VarId
        // for a re-named var, so membership here is exactly "came from earlier".
        let bound_before: std::collections::HashSet<VarId> = s.vars.values().copied().collect();

        for pat in patterns {
            let mut path_nodes = Vec::new();
            let mut path_segments = Vec::new();
            // A standalone single-node pattern whose variable is already bound
            // (`MATCH (a) CREATE (a)`) re-declares it — invalid even without a
            // new shape (`VariableAlreadyBound`, #956). A bound var used as an
            // edge endpoint (a multi-element pattern) is a valid reference.
            if let [PathElement::Node(node)] = pat.elements.as_slice()
                && let Some(name) = node.var.as_deref()
                && s.vars.get(name).is_some_and(|v| bound_before.contains(v))
            {
                s.errors.push(BindError::new(
                    BindErrorKind::VariableAlreadyBound,
                    node.span,
                    format!("variable `{name}` is already bound and cannot be re-created"),
                ));
            }
            let mut prev_node_var: Option<VarId> = None;
            // A node following a relationship is that relationship's destination:
            // reuse the `dst` var the Rel arm computed (mirrors lower_path_pattern,
            // #895) so an anonymous dst node's create-spec matches the edge spec's
            // `dst`. Otherwise `ensure_var(None)` mints a fresh var and the edge
            // references an unbound dst (`CREATE (:A)-[:R]->(:B)`).
            let mut pending_dst: Option<VarId> = None;
            let mut iter = pat.elements.iter().peekable();

            while let Some(elem) = iter.next() {
                match elem {
                    PathElement::Node(node) => {
                        let var = pending_dst
                            .take()
                            .unwrap_or_else(|| ensure_var(node.var.as_ref(), s));
                        if let Some(name) = node.var.as_deref() {
                            bind_var_kind(var, VarKind::Node, name, node.span, s);
                        }
                        // Re-declaring an already-bound node (from an earlier
                        // clause or earlier in this CREATE) with NEW labels or
                        // properties is invalid — `MATCH (a) CREATE (a {…})`,
                        // `CREATE (n:Foo) CREATE (n:Bar)…` (`VariableAlreadyBound`,
                        // #956). A bare reference (endpoint, no shape) is fine.
                        let already_specced = pattern.nodes.iter().any(|n| n.var == var);
                        let has_new_shape = !node.labels.is_empty() || node.properties.is_some();
                        if (bound_before.contains(&var) || already_specced) && has_new_shape {
                            s.errors.push(BindError::new(
                                BindErrorKind::VariableAlreadyBound,
                                node.span,
                                format!(
                                    "variable `{}` is already bound and cannot be re-declared \
                                     with new labels or properties",
                                    node.var.as_deref().unwrap_or("?")
                                ),
                            ));
                        }
                        // Only emit a create-spec the first time a variable
                        // appears in this CREATE; a repeated var (e.g.
                        // `CREATE (a), (a)-[:R]->(b)`) references the same node
                        // rather than creating a duplicate.
                        if !pattern.nodes.iter().any(|n| n.var == var) {
                            let labels = node
                                .labels
                                .iter()
                                .filter_map(|label| self.resolve_label(label, node.span, s))
                                .collect();
                            let resolved_properties = node.properties.as_ref().map(|expr| {
                                rewrite_projection_alias_refs(
                                    expr.clone(),
                                    &created_property_bindings,
                                )
                            });
                            let properties = resolved_properties
                                .as_ref()
                                .map(|expr| self.lower_expr(expr, node.span, s));
                            let is_reference = bound_before.contains(&var);
                            pattern.nodes.push(CreateNodeSpec {
                                var,
                                labels,
                                properties,
                                is_reference,
                            });
                            // A freshly-created node var is node-valued, so a
                            // trailing `RETURN n` / `n.prop` treats it as a node
                            // (write-result RETURN, #814). A reference var is
                            // already registered by the preceding MATCH — don't
                            // clobber its label.
                            if !is_reference {
                                s.node_vars
                                    .entry(var)
                                    .or_insert_with(|| node.labels.first().cloned());
                                if let Some(name) = node.var.as_deref() {
                                    created_property_bindings.push(ReturnItem {
                                        expr: resolved_properties.unwrap_or_else(|| {
                                            Expr::Map(MapLiteral {
                                                entries: HashMap::new(),
                                                key_spans: HashMap::new(),
                                                span: node.span,
                                            })
                                        }),
                                        alias: Some(name.to_owned()),
                                        display: Some(name.to_owned()),
                                        span: node.span,
                                    });
                                }
                            }
                        }
                        prev_node_var = Some(var);
                        if path_nodes.is_empty() {
                            path_nodes.push(var);
                        }
                    }
                    PathElement::Rel(rel) => {
                        validate_created_rel(rel, &bound_before, allow_undirected_relationship, s);
                        let edge_var = ensure_var(rel.var.as_ref(), s);
                        if let Some(name) = rel.var.as_deref() {
                            bind_var_kind(edge_var, VarKind::Relationship, name, rel.span, s);
                        }
                        let dst_var = if let Some(PathElement::Node(dst)) = iter.peek() {
                            let v = ensure_var(dst.var.as_ref(), s);
                            pending_dst = Some(v);
                            v
                        } else {
                            alloc_anon_var(s)
                        };
                        let src_var = prev_node_var.unwrap_or_else(|| alloc_anon_var(s));
                        let rel_type = match rel.types.first() {
                            Some(name) => {
                                let Some(id) = self.resolve_relation_type(name, rel.span, s) else {
                                    return pattern;
                                };
                                Some(id)
                            }
                            None => None,
                        };
                        let properties = rel
                            .properties
                            .as_ref()
                            .map(|expr| self.lower_expr(expr, rel.span, s));
                        pattern.edges.push(CreateEdgeSpec {
                            var: edge_var,
                            src: src_var,
                            dst: dst_var,
                            rel_type,
                            direction: lower_direction(rel.direction),
                            properties,
                        });
                        s.edge_rel_names
                            .insert(edge_var, rel.types.first().cloned());
                        let endpoints = match lower_direction(rel.direction) {
                            Direction::In => (dst_var, src_var),
                            Direction::Out | Direction::Undirected => (src_var, dst_var),
                        };
                        s.edge_vars.insert(edge_var, endpoints);
                        path_nodes.push(dst_var);
                        path_segments.push(PathSegment {
                            edge: edge_var,
                            var_len: false,
                            rel_name: rel.types.first().cloned(),
                        });
                        prev_node_var = Some(dst_var);
                    }
                }
            }
            if let Some(name) = &pat.var {
                Self::bind_path_var(name, pat.span, path_nodes, path_segments, s);
            }
        }

        pattern
    }

    fn lower_merge_actions(&self, actions: &[SetItem], s: &mut BinderState) -> Vec<MergeSetItem> {
        let mut lowered = Vec::with_capacity(actions.len());
        for action in actions {
            match action {
                SetItem::Property {
                    target,
                    value,
                    span,
                } => {
                    if let Some((var, prop, prop_name)) = self.resolve_write_target(target, s) {
                        lowered.push(MergeSetItem::Property(SetPropItem {
                            target: var,
                            prop,
                            prop_name,
                            value: self.lower_expr(value, *span, s),
                        }));
                    }
                }
                SetItem::PropertyMerge { var, map, span }
                | SetItem::PropertyReplace { var, map, span } => {
                    if let Some(&target) = s.vars.get(var) {
                        lowered.push(MergeSetItem::Map(SetMapItem {
                            target,
                            map: self.lower_set_map_source(map, *span, s),
                            replace: matches!(action, SetItem::PropertyReplace { .. }),
                        }));
                    } else {
                        s.errors.push(BindError::new(
                            BindErrorKind::UndeclaredVariable,
                            *span,
                            format!("undefined variable `{var}`"),
                        ));
                    }
                }
                SetItem::Label { var, labels, span } => {
                    if let Some(&target) = s.vars.get(var) {
                        lowered.push(MergeSetItem::AddLabels {
                            target,
                            labels: labels
                                .iter()
                                .filter_map(|label| self.resolve_label(label, *span, s))
                                .collect(),
                        });
                    } else {
                        s.errors.push(BindError::new(
                            BindErrorKind::UndeclaredVariable,
                            *span,
                            format!("undefined variable `{var}`"),
                        ));
                    }
                }
                _ => unreachable!("future SET forms are rejected by the parser contract"),
            }
        }
        lowered
    }

    /// Lower a `DELETE` / `DETACH DELETE` clause into a [`GraphOp::Delete`].
    ///
    /// Direct entity variables retain their identity-column fast path. Named
    /// paths expand to their constituent variables, while list/map access and
    /// other runtime value expressions are evaluated by the statement driver.
    pub(super) fn lower_delete(&self, d: &graphforge_ast::DeleteClause, s: &mut BinderState) {
        let mut vars: Vec<VarId> = Vec::with_capacity(d.exprs.len());
        let mut exprs = Vec::new();
        for expr in &d.exprs {
            match expr {
                Expr::Var(VarRef { name, span }) => match s.vars.get(name) {
                    Some(&var_id) => vars.push(var_id),
                    None => {
                        if let Some(path) = s.path_vars.get(name) {
                            vars.extend(path.nodes.iter().copied());
                            vars.extend(path.segments.iter().map(|segment| segment.edge));
                        } else {
                            s.errors.push(BindError::new(
                                BindErrorKind::UndeclaredVariable,
                                *span,
                                format!("DELETE target `{name}` is not a bound variable"),
                            ));
                        }
                    }
                },
                Expr::Property(_) => exprs.push(self.lower_expr(expr, expr.span(), s)),
                Expr::FunctionCall(call) if call.name.as_slice() == ["_subscript"] => {
                    exprs.push(self.lower_expr(expr, expr.span(), s));
                }
                Expr::Parenthesized { inner, .. } => {
                    if matches!(inner.as_ref(), Expr::Property(_))
                        || matches!(inner.as_ref(), Expr::FunctionCall(call) if call.name.as_slice() == ["_subscript"])
                    {
                        exprs.push(self.lower_expr(inner, inner.span(), s));
                    } else {
                        if self.typed_uuid_param_in(inner).is_some() {
                            self.lower_expr(inner, inner.span(), s);
                        }
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidDeleteTarget,
                            expr.span(),
                            "DELETE target must be a node, relationship, or path value",
                        ));
                    }
                }
                other => {
                    if self.typed_uuid_param_in(other).is_some() {
                        self.lower_expr(other, other.span(), s);
                    }
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidDeleteTarget,
                        other.span(),
                        "DELETE target must be a node, relationship, or path value",
                    ));
                }
            }
        }
        vars.sort_unstable_by_key(|var| var.0);
        vars.dedup();
        s.builder.push_op_mut(GraphOp::Delete {
            vars,
            exprs,
            detach: d.detach,
        });
    }

    /// Lower a `SET` clause into a [`GraphOp::Set`] (#791).
    ///
    /// Property assignments and bulk map assignments carry full runtime
    /// expressions through to lowering; label additions carry resolved type ids.
    pub(super) fn lower_set(&self, st: &SetClause, s: &mut BinderState) {
        let mut items: Vec<SetPropItem> = Vec::with_capacity(st.items.len());
        let mut map_items = Vec::new();
        let mut label_items = Vec::new();
        for item in &st.items {
            match item {
                SetItem::Property {
                    target,
                    value,
                    span,
                } => {
                    let Some((var, prop, prop_name)) = self.resolve_write_target(target, s) else {
                        continue;
                    };
                    let value = self.lower_expr(value, *span, s);
                    items.push(SetPropItem {
                        target: var,
                        prop,
                        prop_name,
                        value,
                    });
                }
                SetItem::PropertyMerge { var, map, span }
                | SetItem::PropertyReplace { var, map, span } => {
                    let Some(&target) = s.vars.get(var) else {
                        s.errors.push(BindError::new(
                            BindErrorKind::UndeclaredVariable,
                            *span,
                            format!("undefined variable `{var}`"),
                        ));
                        continue;
                    };
                    map_items.push(SetMapItem {
                        target,
                        map: self.lower_set_map_source(map, *span, s),
                        replace: matches!(item, SetItem::PropertyReplace { .. }),
                    });
                }
                SetItem::Label { var, labels, span } => match s.vars.get(var).copied() {
                    Some(target) => label_items.push(crate::LabelItem {
                        target,
                        labels: labels
                            .iter()
                            .filter_map(|label| self.resolve_label(label, *span, s))
                            .collect(),
                    }),
                    None => s.errors.push(BindError::new(
                        BindErrorKind::UndeclaredVariable,
                        *span,
                        format!("undefined variable `{var}`"),
                    )),
                },
                _ => unreachable!("future SET forms rejected above"),
            }
        }
        s.builder.push_op_mut(GraphOp::Set {
            items,
            map_items,
            label_items,
        });
    }

    fn lower_set_map_source(&self, map: &Expr, span: Span, s: &mut BinderState) -> ExprId {
        if let Expr::Var(VarRef { name, .. }) = map
            && let Some(&var) = s.vars.get(name)
            && (s.node_vars.contains_key(&var) || s.edge_rel_names.contains_key(&var))
        {
            let value = s.builder.push_expr(IrExpr::VarRef(var));
            return s.builder.push_expr(IrExpr::FunctionCall {
                name: "properties".into(),
                args: vec![value],
            });
        }
        self.lower_expr(map, span, s)
    }

    /// Lower a `REMOVE` clause into a [`GraphOp::Remove`] (#791).
    ///
    /// Property removals carry resolved property ids; label removals carry
    /// resolved type ids for statement-driver execution.
    pub(super) fn lower_remove(&self, r: &RemoveClause, s: &mut BinderState) {
        let mut items: Vec<RemovePropItem> = Vec::with_capacity(r.items.len());
        let mut label_items = Vec::new();
        for item in &r.items {
            match item {
                RemoveItem::Property(target, _span) => {
                    let Some((var, prop, prop_name)) = self.resolve_write_target(target, s) else {
                        continue;
                    };
                    items.push(RemovePropItem {
                        target: var,
                        prop,
                        prop_name,
                    });
                }
                RemoveItem::Label { var, labels, span } => match s.vars.get(var).copied() {
                    Some(target) => label_items.push(crate::LabelItem {
                        target,
                        labels: labels
                            .iter()
                            .filter_map(|label| self.resolve_label(label, *span, s))
                            .collect(),
                    }),
                    None => s.errors.push(BindError::new(
                        BindErrorKind::UndeclaredVariable,
                        *span,
                        format!("undefined variable `{var}`"),
                    )),
                },
                _ => s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    r.span,
                    "this form of REMOVE is not yet supported",
                )),
            }
        }
        s.builder
            .push_op_mut(GraphOp::Remove { items, label_items });
    }

    /// Resolve a `SET`/`REMOVE` property target (`n.prop`) into its bound
    /// variable, [`PropId`], and property name.
    ///
    /// The target's object must be a bound variable (`Expr::Var`); a property
    /// access on anything else — a literal, an expression, a nested property —
    /// is an [`InvalidDeleteTarget`](BindErrorKind::InvalidDeleteTarget) (the
    /// kind means "write target must be a bound variable"). Returns `None` (and
    /// records an error) when the target is malformed or the variable unbound,
    /// so the caller skips that item.
    pub(super) fn resolve_write_target(
        &self,
        target: &PropertyAccess,
        s: &mut BinderState,
    ) -> Option<(VarId, PropertyId, String)> {
        if matches!(target.key.as_str(), "node_uuid" | "edge_uuid") {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                target.span,
                format!("structural identity field `{}` is read-only", target.key),
            ));
            return None;
        }
        let Expr::Var(VarRef { name, span }) = target.object.as_ref() else {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidDeleteTarget,
                target.span,
                "write target must be a bound variable's property \
                 (e.g. `SET n.prop = …`)",
            ));
            return None;
        };
        let Some(&var) = s.vars.get(name) else {
            s.errors.push(BindError::new(
                BindErrorKind::UndeclaredVariable,
                *span,
                format!("write target `{name}` is not a bound variable"),
            ));
            return None;
        };
        let owner = property_owner_for_var(var, s);
        let prop = self.resolve_property(&target.key, target.span, owner, s)?;
        Some((var, prop, target.key.clone()))
    }
}

/// Reject a duplicate EXPLICIT output column name in a RETURN/WITH projection
/// (`RETURN 1 AS a, 2 AS a` → openCypher `ColumnNameConflict`, #956). Only
/// explicit `AS` aliases are checked; implicit column names mirror the source
/// text, where a collision is both rare and self-inflicted.
/// Validate a relationship element in a CREATE/MERGE pattern (#956): exactly
/// one type (`NoSingleRelationshipType`), fixed-length (`CreatingVarLength`),
/// directed (`RequiresDirectedRelationship`), and not a reused already-bound
/// variable (`VariableAlreadyBound`). `bound_before` is the set of variables
/// bound before this clause.
fn validate_created_rel(
    rel: &graphforge_ast::RelPattern,
    bound_before: &std::collections::HashSet<VarId>,
    allow_undirected: bool,
    s: &mut BinderState,
) {
    let mut err = |m: &str| {
        s.errors.push(BindError::new(
            BindErrorKind::InvalidArgument,
            rel.span,
            m.to_string(),
        ));
    };
    if rel.types.len() != 1 {
        err("a created relationship must have exactly one type");
    }
    if rel.min_hops.is_some() || rel.max_hops.is_some() {
        err("cannot create a variable-length relationship");
    }
    if !allow_undirected && matches!(rel.direction, graphforge_ast::Direction::Undirected) {
        err("a created relationship must be directed");
    }
    if let Some(name) = rel.var.as_deref()
        && s.vars.get(name).is_some_and(|v| bound_before.contains(v))
    {
        s.errors.push(BindError::new(
            BindErrorKind::VariableAlreadyBound,
            rel.span,
            format!("relationship variable `{name}` is already bound"),
        ));
    }
}

#[cfg(test)]
mod tests;
