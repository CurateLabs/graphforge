//! Scalar expression, function, property, and typed-UUID binding.

use super::patterns::{expr_contains_pattern_comprehension, push_conjunction};
use super::projection::expr_contains_aggregate;
use super::{
    BindError, BindErrorKind, Binder, BinderState, BoundPropertyOwner, UuidParamClass, VarKind,
    is_function_named, property_owner_for_expr,
};
use crate::composition_binding::{BindingDiagnosticCode, SymbolBinding};
use crate::expr::{BinaryOpKind, CaseArm, IrExpr, IrLiteral, UnaryOpKind};
use crate::plan::{GraphOp, GraphPlan, OntologyMode};
use crate::{ExprId, VarId};
use graphforge_ast::{
    BinaryOpKind as AstBinOp, CaseExpr, Expr, FunctionCall, LabelPredicate, Literal, MapLiteral,
    PropertyAccess, StringOpKind, UnaryOpKind as AstUnOp, VarRef,
};
use graphforge_core::{PropId, Span};
use graphforge_value::PropertyId;

impl Binder {
    // -----------------------------------------------------------------------
    // Expression lowering
    // -----------------------------------------------------------------------

    pub(super) fn typed_uuid_param_in<'a>(&self, expr: &'a Expr) -> Option<&'a str> {
        match expr {
            Expr::Param(param) if self.typed_uuid_params.contains_key(&param.name) => {
                Some(&param.name)
            }
            Expr::Parenthesized { inner, .. }
            | Expr::UnaryOp(graphforge_ast::UnaryOp { expr: inner, .. }) => {
                self.typed_uuid_param_in(inner)
            }
            Expr::List(list) => list
                .elements
                .iter()
                .find_map(|item| self.typed_uuid_param_in(item)),
            Expr::Map(map) => map
                .entries
                .values()
                .find_map(|value| self.typed_uuid_param_in(value)),
            Expr::FunctionCall(call) => call
                .args
                .iter()
                .find_map(|arg| self.typed_uuid_param_in(arg)),
            Expr::BinaryOp(binary) => self
                .typed_uuid_param_in(&binary.left)
                .or_else(|| self.typed_uuid_param_in(&binary.right)),
            Expr::Case(case) => case
                .subject
                .as_deref()
                .and_then(|subject| self.typed_uuid_param_in(subject))
                .or_else(|| {
                    case.when_clauses.iter().find_map(|when| {
                        self.typed_uuid_param_in(&when.condition)
                            .or_else(|| self.typed_uuid_param_in(&when.result))
                    })
                })
                .or_else(|| {
                    case.else_expr
                        .as_deref()
                        .and_then(|otherwise| self.typed_uuid_param_in(otherwise))
                }),
            Expr::ListComprehension(comprehension) => self
                .typed_uuid_param_in(&comprehension.list)
                .or_else(|| {
                    comprehension
                        .filter
                        .as_deref()
                        .and_then(|filter| self.typed_uuid_param_in(filter))
                })
                .or_else(|| {
                    comprehension
                        .projection
                        .as_deref()
                        .and_then(|projection| self.typed_uuid_param_in(projection))
                }),
            Expr::Quantifier(quantifier) => self
                .typed_uuid_param_in(&quantifier.list)
                .or_else(|| self.typed_uuid_param_in(&quantifier.predicate)),
            Expr::PatternComprehension(comprehension) => comprehension
                .filter
                .as_deref()
                .and_then(|filter| self.typed_uuid_param_in(filter))
                .or_else(|| self.typed_uuid_param_in(&comprehension.projection)),
            Expr::IsNull { expr, .. } => self.typed_uuid_param_in(expr),
            Expr::InList { expr, list, .. } => self
                .typed_uuid_param_in(expr)
                .or_else(|| self.typed_uuid_param_in(list)),
            Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => self
                .typed_uuid_param_in(expr)
                .or_else(|| self.typed_uuid_param_in(pattern)),
            _ => None,
        }
    }

    fn direct_typed_uuid_identity_parameter(
        &self,
        property_expr: &Expr,
        value_expr: &Expr,
        s: &BinderState,
    ) -> Option<String> {
        let mut value_expr = value_expr;
        while let Expr::Parenthesized { inner, .. } = value_expr {
            value_expr = inner;
        }
        let Expr::Param(param) = value_expr else {
            return None;
        };
        if self.typed_uuid_params.get(&param.name) != Some(&UuidParamClass::ExactUuid) {
            return None;
        }
        let mut property_expr = property_expr;
        while let Expr::Parenthesized { inner, .. } = property_expr {
            property_expr = inner;
        }
        let Expr::Property(PropertyAccess { object, key, .. }) = property_expr else {
            return None;
        };
        let mut object = object.as_ref();
        while let Expr::Parenthesized { inner, .. } = object {
            object = inner;
        }
        let actual = match object {
            Expr::Var(VarRef { name, .. }) => s
                .vars
                .get(name)
                .and_then(|var| s.var_kinds.get(var))
                .copied(),
            _ => None,
        };
        let compatible = matches!(
            (key.as_str(), actual),
            ("node_uuid", Some(VarKind::Node)) | ("edge_uuid", Some(VarKind::Relationship))
        );
        compatible.then(|| param.name.clone())
    }

    fn lower_expr_with_direct_uuid_allowed(
        &self,
        expr: &Expr,
        parent_span: Span,
        allowed: Option<&str>,
        s: &mut BinderState,
    ) -> ExprId {
        let mut direct = expr;
        while let Expr::Parenthesized { inner, .. } = direct {
            direct = inner;
        }
        if let Expr::Param(param) = direct
            && allowed == Some(param.name.as_str())
        {
            return s.builder.push_expr(IrExpr::Parameter(param.name.clone()));
        }
        self.lower_expr(expr, parent_span, s)
    }

    #[allow(clippy::only_used_in_recursion, clippy::too_many_lines)]
    pub(super) fn lower_expr(&self, expr: &Expr, parent_span: Span, s: &mut BinderState) -> ExprId {
        match expr {
            Expr::Literal(lit) => {
                if let Literal::Float(f, span) = lit
                    && !f.is_finite()
                {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        *span,
                        "float literal is outside the supported f64 range",
                    ));
                }
                s.builder.push_expr(IrExpr::Literal(lower_literal(lit)))
            }

            Expr::Var(VarRef { name, span }) => {
                if let Some(&var_id) = s.vars.get(name) {
                    s.builder.push_expr(IrExpr::VarRef(var_id))
                } else if let Some(binding) = s.path_vars.get(name).cloned() {
                    // A bare path value (`RETURN p`): a Struct{nodes,
                    // relationships} assembled from the path's constituents.
                    Self::path_struct_expr(&binding, s)
                } else {
                    s.errors.push(BindError::new(
                        BindErrorKind::UndeclaredVariable,
                        *span,
                        format!("variable `{name}` used before it was introduced"),
                    ));
                    s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
                }
            }

            Expr::Property(graphforge_ast::PropertyAccess { object, key, span }) => {
                if matches!(object.as_ref(), Expr::Var(VarRef { name, .. }) if s.path_vars.contains_key(name))
                {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        *span,
                        "property access is not valid on a path value",
                    ));
                    return s.builder.push_expr(IrExpr::Literal(IrLiteral::Null));
                }
                if matches!(key.as_str(), "node_uuid" | "edge_uuid") {
                    let expected = if key == "node_uuid" {
                        VarKind::Node
                    } else {
                        VarKind::Relationship
                    };
                    let actual = match object.as_ref() {
                        Expr::Var(VarRef { name, .. }) => s
                            .vars
                            .get(name)
                            .and_then(|var| s.var_kinds.get(var))
                            .copied(),
                        _ => None,
                    };
                    if actual != Some(expected) {
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            *span,
                            format!(
                                "structural identity field `{key}` is valid only on {expected}"
                            ),
                        ));
                        return s.builder.push_expr(IrExpr::Literal(IrLiteral::Null));
                    }
                }
                let owner = property_owner_for_expr(object, s);
                let base = self.lower_expr(object, *span, s);
                let Some(prop) = self.resolve_property(key, *span, owner, s) else {
                    return s.builder.push_expr(IrExpr::Literal(IrLiteral::Null));
                };
                s.builder.push_expr(IrExpr::PropertyAccess { base, prop })
            }

            Expr::BinaryOp(graphforge_ast::BinaryOp {
                op,
                left,
                right,
                span,
            }) => {
                let right_allowed = (*op == AstBinOp::Eq)
                    .then(|| self.direct_typed_uuid_identity_parameter(left, right, s))
                    .flatten();
                let left_allowed = (*op == AstBinOp::Eq)
                    .then(|| self.direct_typed_uuid_identity_parameter(right, left, s))
                    .flatten();
                match op {
                    AstBinOp::Concat => {
                        let a = self.lower_expr_with_direct_uuid_allowed(
                            left,
                            *span,
                            left_allowed.as_deref(),
                            s,
                        );
                        let b = self.lower_expr_with_direct_uuid_allowed(
                            right,
                            *span,
                            right_allowed.as_deref(),
                            s,
                        );
                        s.builder.push_expr(IrExpr::FunctionCall {
                            name: "string.concat".into(),
                            args: vec![a, b],
                        })
                    }
                    other => {
                        let l = self.lower_expr_with_direct_uuid_allowed(
                            left,
                            *span,
                            left_allowed.as_deref(),
                            s,
                        );
                        let r = self.lower_expr_with_direct_uuid_allowed(
                            right,
                            *span,
                            right_allowed.as_deref(),
                            s,
                        );
                        s.builder.push_expr(IrExpr::BinaryOp {
                            op: lower_binop(*other),
                            left: l,
                            right: r,
                        })
                    }
                }
            }

            Expr::UnaryOp(graphforge_ast::UnaryOp {
                op,
                expr: inner,
                span,
            }) => {
                if let Some(name) = self.typed_uuid_param_in(inner) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        *span,
                        format!(
                            "typed UUID parameter `${name}` is only supported as a direct node_uuid or edge_uuid identity equality predicate"
                        ),
                    ));
                }
                let ir_op = match op {
                    AstUnOp::Not => UnaryOpKind::Not,
                    AstUnOp::Neg => UnaryOpKind::Neg,
                };
                let e = self.lower_expr(inner, *span, s);
                s.builder.push_expr(IrExpr::UnaryOp { op: ir_op, expr: e })
            }

            Expr::IsNull {
                expr: inner,
                negated,
                span,
            } => {
                let op = if *negated {
                    UnaryOpKind::IsNotNull
                } else {
                    UnaryOpKind::IsNull
                };
                let e = self.lower_expr(inner, *span, s);
                s.builder.push_expr(IrExpr::UnaryOp { op, expr: e })
            }

            Expr::InList {
                expr: lhs,
                list: rhs,
                negated,
                span,
            } => {
                let l = self.lower_expr(lhs, *span, s);
                let r = self.lower_expr(rhs, *span, s);
                let in_id = s.builder.push_expr(IrExpr::BinaryOp {
                    op: BinaryOpKind::In,
                    left: l,
                    right: r,
                });
                if *negated {
                    s.builder.push_expr(IrExpr::UnaryOp {
                        op: UnaryOpKind::Not,
                        expr: in_id,
                    })
                } else {
                    in_id
                }
            }

            Expr::StringOp {
                expr: lhs,
                op,
                pattern: rhs,
                span,
            } => {
                let ir_op = match op {
                    StringOpKind::StartsWith => BinaryOpKind::StartsWith,
                    StringOpKind::EndsWith => BinaryOpKind::EndsWith,
                    StringOpKind::Contains => BinaryOpKind::Contains,
                };
                let l = self.lower_expr(lhs, *span, s);
                let r = self.lower_expr(rhs, *span, s);
                s.builder.push_expr(IrExpr::BinaryOp {
                    op: ir_op,
                    left: l,
                    right: r,
                })
            }

            Expr::RegexMatch {
                expr: lhs,
                pattern: rhs,
                span,
            } => {
                let l = self.lower_expr(lhs, *span, s);
                let r = self.lower_expr(rhs, *span, s);
                s.builder.push_expr(IrExpr::BinaryOp {
                    op: BinaryOpKind::RegexMatch,
                    left: l,
                    right: r,
                })
            }

            Expr::Parenthesized { inner, .. } => self.lower_expr(inner, parent_span, s),

            Expr::FunctionCall(call) => {
                if !is_known_cypher_function(call) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        call.span,
                        format!("unknown function `{}`", call.name.join(".")),
                    ));
                    s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
                } else if Self::invalid_direct_graph_function_argument(call, s) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        call.span,
                        format!(
                            "{}() does not accept this graph value type",
                            call.name.join(".")
                        ),
                    ));
                    s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
                } else if Self::is_size_of_path_variable(call, s) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        call.span,
                        "size() is not valid for paths; use length(path) instead",
                    ));
                    s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
                } else if is_function_named(call, "type") && call.args.len() == 1 {
                    let arg = match &call.args[0] {
                        Expr::Var(VarRef { name, .. }) => {
                            let edge_var = s
                                .vars
                                .get(name)
                                .copied()
                                .filter(|var| s.edge_rel_names.contains_key(var));
                            if let Some(var) = edge_var {
                                Self::relationship_struct_expr(var, s)
                            } else {
                                self.lower_expr(&call.args[0], parent_span, s)
                            }
                        }
                        other => self.lower_expr(other, parent_span, s),
                    };
                    s.builder.push_expr(IrExpr::FunctionCall {
                        name: "type".into(),
                        args: vec![arg],
                    })
                } else if let Some(id) = Self::lower_path_function(call, s) {
                    id
                } else if let Some(node_var) = Self::resolve_endpoint_node(call, s) {
                    // `startNode(r)` / `endNode(r)` over a matched relationship
                    // is the src / dst node var (#753). As a bare value it is a
                    // node reference; property access (`startNode(r).name`)
                    // resolves against that var's columns. A terminal RETURN
                    // upgrades it to a whole node value in `lower_return_item_expr`.
                    s.builder.push_expr(IrExpr::VarRef(node_var))
                } else {
                    let fn_name = call.name.join(".");
                    let ir_args: Vec<ExprId> = call
                        .args
                        .iter()
                        .map(|a| self.lower_expr(a, parent_span, s))
                        .collect();
                    s.builder.push_expr(IrExpr::FunctionCall {
                        name: fn_name,
                        args: ir_args,
                    })
                }
            }

            Expr::Param(graphforge_ast::ParamRef { name, span }) => {
                if self.typed_uuid_params.contains_key(name) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        *span,
                        format!(
                            "typed UUID parameter `${name}` is only supported as a direct node_uuid or edge_uuid identity equality predicate"
                        ),
                    ));
                }
                s.builder.push_expr(IrExpr::Parameter(name.clone()))
            }

            Expr::Case(CaseExpr {
                subject,
                when_clauses,
                else_expr,
                ..
            }) => {
                let operand = subject
                    .as_deref()
                    .map(|e| self.lower_expr(e, parent_span, s));
                let arms: Vec<CaseArm> = when_clauses
                    .iter()
                    .map(|w| {
                        let when = self.lower_expr(&w.condition, w.span, s);
                        let when = operand.map_or(when, |subject| {
                            s.builder.push_expr(IrExpr::BinaryOp {
                                op: BinaryOpKind::Eq,
                                left: subject,
                                right: when,
                            })
                        });
                        CaseArm {
                            when,
                            then: self.lower_expr(&w.result, w.span, s),
                        }
                    })
                    .collect();
                let else_id = else_expr
                    .as_deref()
                    .map(|e| self.lower_expr(e, parent_span, s));
                s.builder.push_expr(IrExpr::Case {
                    operand: None,
                    arms,
                    else_expr: else_id,
                })
            }

            Expr::List(graphforge_ast::ListLiteral { elements, .. }) => {
                let ids: Vec<ExprId> = elements
                    .iter()
                    .map(|e| self.lower_expr(e, parent_span, s))
                    .collect();
                s.builder.push_expr(IrExpr::ListLiteral(ids))
            }

            Expr::Map(MapLiteral { entries, .. }) => {
                let mut pairs: Vec<(&String, &Expr)> = entries.iter().collect();
                pairs.sort_by_key(|(k, _)| k.as_str());
                let ids: Vec<(String, ExprId)> = pairs
                    .into_iter()
                    .map(|(k, v)| (k.clone(), self.lower_expr(v, parent_span, s)))
                    .collect();
                s.builder.push_expr(IrExpr::MapLiteral(ids))
            }

            // `[var IN list WHERE filter | projection]` (#955): the list is
            // lowered with the loop var OUT of scope; the loop var is then bound
            // (shadowing) only while lowering the filter + projection, and
            // restored after — mirrors `Quantifier` below.
            Expr::ListComprehension(lc) => {
                if lc.filter.as_deref().is_some_and(expr_contains_aggregate)
                    || lc
                        .projection
                        .as_deref()
                        .is_some_and(expr_contains_aggregate)
                {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        lc.span,
                        "an aggregate function may not be used inside a list \
                         comprehension filter or projection",
                    ));
                }
                let list = self.lower_expr(&lc.list, parent_span, s);
                let has_nested_pattern = lc
                    .filter
                    .as_deref()
                    .is_some_and(expr_contains_pattern_comprehension)
                    || lc
                        .projection
                        .as_deref()
                        .is_some_and(expr_contains_pattern_comprehension);
                let prev = s.vars.get(&lc.var).copied();
                let loop_var = VarId(s.next_var);
                s.next_var += 1;
                s.vars.insert(lc.var.clone(), loop_var);
                let previous_node = s.node_vars.get(&loop_var).cloned();
                let previous_kind = s.var_kinds.get(&loop_var).copied();
                if has_nested_pattern {
                    // The currently supported graph-valued source is `nodes(path)`,
                    // whose elements are whole node structs at execution time.
                    s.node_vars.insert(loop_var, None);
                    s.var_kinds.insert(loop_var, VarKind::Node);
                }
                let previous_capture = if has_nested_pattern {
                    s.captured_pattern_comprehensions.replace(Vec::new())
                } else {
                    None
                };
                let filter = lc
                    .filter
                    .as_ref()
                    .map(|f| self.lower_expr(f, parent_span, s));
                let projection = lc
                    .projection
                    .as_ref()
                    .map(|p| self.lower_expr(p, parent_span, s));
                let captured = if has_nested_pattern {
                    let captured = s.captured_pattern_comprehensions.take().unwrap_or_default();
                    s.captured_pattern_comprehensions = previous_capture;
                    captured
                } else {
                    Vec::new()
                };
                match prev {
                    Some(v) => {
                        s.vars.insert(lc.var.clone(), v);
                    }
                    None => {
                        s.vars.remove(&lc.var);
                    }
                }
                match previous_node {
                    Some(shape) => {
                        s.node_vars.insert(loop_var, shape);
                    }
                    None => {
                        s.node_vars.remove(&loop_var);
                    }
                }
                match previous_kind {
                    Some(kind) => {
                        s.var_kinds.insert(loop_var, kind);
                    }
                    None => {
                        s.var_kinds.remove(&loop_var);
                    }
                }
                if has_nested_pattern {
                    let [(child, pattern_output)] = captured.try_into().unwrap_or_else(|captured: Vec<_>| {
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            lc.span,
                            format!(
                                "a list comprehension currently supports exactly one nested pattern comprehension, found {}",
                                captured.len()
                            ),
                        ));
                        [(Box::new(GraphPlan::builder("openCypher").build()), loop_var)]
                    });
                    let output = VarId(s.next_var);
                    s.next_var += 1;
                    s.builder
                        .push_op_mut(GraphOp::ListElementPatternComprehension {
                            list_expr: list,
                            loop_var,
                            child,
                            pattern_output,
                            filter,
                            projection,
                            output,
                        });
                    return s.builder.push_expr(IrExpr::VarRef(output));
                }
                s.builder.push_expr(IrExpr::ListComprehension {
                    loop_var,
                    list,
                    filter,
                    projection,
                })
            }

            // `all/any/none/single(var IN list WHERE pred)` (#955): the list is
            // lowered with the loop var OUT of scope; the loop var is then bound
            // (shadowing) only while lowering the predicate, and restored after.
            Expr::Quantifier(q) => {
                let list = self.lower_expr(&q.list, parent_span, s);
                let prev = s.vars.get(&q.var).copied();
                let loop_var = VarId(s.next_var);
                s.next_var += 1;
                s.vars.insert(q.var.clone(), loop_var);
                let predicate = self.lower_expr(&q.predicate, parent_span, s);
                match prev {
                    Some(v) => {
                        s.vars.insert(q.var.clone(), v);
                    }
                    None => {
                        s.vars.remove(&q.var);
                    }
                }
                s.builder.push_expr(IrExpr::Quantifier {
                    kind: q.kind,
                    loop_var,
                    list,
                    predicate,
                })
            }

            Expr::PatternPredicate(pp) => {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    pp.span,
                    "pattern predicates are invalid outside WHERE",
                ));
                s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
            }

            Expr::ExistentialSubquery(es) => {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    es.span,
                    "existential subqueries are currently valid only as WHERE predicates",
                ));
                s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
            }

            Expr::PatternComprehension(pc) => self.lower_pattern_comprehension(pc, s),

            Expr::LabelPredicate(lp) => self.lower_label_predicate(lp, s),

            // Expr is #[non_exhaustive] — catch future variants gracefully.
            _ => s.builder.push_expr(IrExpr::Literal(IrLiteral::Null)),
        }
    }

    fn lower_label_predicate(&self, lp: &LabelPredicate, s: &mut BinderState) -> ExprId {
        let Some(&var_id) = s.vars.get(&lp.var) else {
            s.errors.push(BindError::new(
                BindErrorKind::UndeclaredVariable,
                lp.span,
                format!("variable `{}` used before it was introduced", lp.var),
            ));
            return s.builder.push_expr(IrExpr::Literal(IrLiteral::Null));
        };

        let kind = s.var_kinds.get(&var_id).copied().or_else(|| {
            if s.node_vars.contains_key(&var_id) {
                Some(VarKind::Node)
            } else if s.edge_rel_names.contains_key(&var_id) {
                Some(VarKind::Relationship)
            } else {
                None
            }
        });

        match kind {
            Some(VarKind::Node) => self.lower_node_label_predicate(var_id, &lp.labels, lp.span, s),
            Some(VarKind::Relationship) => {
                self.lower_relationship_type_predicate(var_id, &lp.labels, lp.span, s)
            }
            Some(VarKind::Unknown) | None => {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    lp.span,
                    format!(
                        "`{}:{}` requires a node or relationship variable",
                        lp.var,
                        lp.labels.join(":")
                    ),
                ));
                s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
            }
        }
    }

    pub(super) fn lower_node_label_predicate(
        &self,
        var_id: VarId,
        labels: &[String],
        span: Span,
        s: &mut BinderState,
    ) -> ExprId {
        let var_expr = s.builder.push_expr(IrExpr::VarRef(var_id));
        let node_labels_expr = s.builder.push_expr(IrExpr::FunctionCall {
            name: "labels".into(),
            args: vec![var_expr],
        });
        let mut predicates = Vec::with_capacity(labels.len());
        for label in labels {
            self.resolve_label(label, span, s);
            let expected_label_expr = s
                .builder
                .push_expr(IrExpr::Literal(IrLiteral::Str(label.clone())));
            predicates.push(s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::In,
                left: expected_label_expr,
                right: node_labels_expr,
            }));
        }
        push_conjunction(predicates, s)
    }

    pub(super) fn lower_relationship_type_predicate(
        &self,
        var_id: VarId,
        types: &[String],
        span: Span,
        s: &mut BinderState,
    ) -> ExprId {
        let rel_expr = if s.edge_rel_names.contains_key(&var_id) {
            Self::relationship_struct_expr(var_id, s)
        } else {
            s.builder.push_expr(IrExpr::VarRef(var_id))
        };
        let type_expr = s.builder.push_expr(IrExpr::FunctionCall {
            name: "type".into(),
            args: vec![rel_expr],
        });
        let mut predicates = Vec::with_capacity(types.len());
        for rel_type in types {
            self.resolve_relation_type(rel_type, span, s);
            let type_lit = s
                .builder
                .push_expr(IrExpr::Literal(IrLiteral::Str(rel_type.clone())));
            predicates.push(s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Eq,
                left: type_expr,
                right: type_lit,
            }));
        }
        let mut predicates = predicates.into_iter();
        let Some(mut predicate) = predicates.next() else {
            return s.builder.push_expr(IrExpr::Literal(IrLiteral::Bool(false)));
        };
        for next in predicates {
            predicate = s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Or,
                left: predicate,
                right: next,
            });
        }
        predicate
    }

    pub(super) fn invalid_direct_graph_function_argument(
        call: &FunctionCall,
        s: &BinderState,
    ) -> bool {
        let [name] = call.name.as_slice() else {
            return false;
        };
        let [Expr::Var(VarRef { name: var_name, .. })] = call.args.as_slice() else {
            return false;
        };
        let function = name.to_ascii_lowercase();
        if s.path_vars.contains_key(var_name) {
            return matches!(function.as_str(), "labels" | "type");
        }
        let Some(var) = s.vars.get(var_name) else {
            return false;
        };
        let is_node = s.node_vars.contains_key(var);
        let is_relationship = s.edge_rel_names.contains_key(var);
        matches!(function.as_str(), "type" | "length") && is_node
            || matches!(function.as_str(), "labels" | "length") && is_relationship
    }

    fn is_size_of_path_variable(call: &FunctionCall, s: &BinderState) -> bool {
        let [fn_name] = call.name.as_slice() else {
            return false;
        };
        if !fn_name.eq_ignore_ascii_case("size") {
            return false;
        }
        let [Expr::Var(VarRef { name, .. })] = call.args.as_slice() else {
            return false;
        };
        s.path_vars.contains_key(name)
    }

    fn runtime_property(
        &self,
        name: &str,
        owner: Option<&str>,
        span: Span,
        s: &mut BinderState,
    ) -> Option<PropertyId> {
        match self.catalog.lock().unwrap().intern_property(name, owner) {
            Ok(id) => Some(PropertyId::runtime(id)),
            Err(error) => {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    span,
                    error.to_string(),
                ));
                None
            }
        }
    }

    pub(super) fn resolve_property(
        &self,
        name: &str,
        span: Span,
        owner: BoundPropertyOwner,
        s: &mut BinderState,
    ) -> Option<PropertyId> {
        // Structural fields retain their existing runtime catalog name routing.
        if matches!(name, "node_uuid" | "edge_uuid") {
            return self.runtime_property(name, None, span, s);
        }
        if let Some(composition) = &self.composition {
            let scoped_owner = match &owner {
                BoundPropertyOwner::Entity(Some(owner)) => {
                    Some((graphforge_ontology::SymbolKind::Entity, owner.as_str()))
                }
                BoundPropertyOwner::Relationship(Some(owner)) => {
                    Some((graphforge_ontology::SymbolKind::Relation, owner.as_str()))
                }
                _ => None,
            };
            let resolution = scoped_owner.map_or_else(
                || composition.resolve(graphforge_ontology::SymbolKind::Property, name),
                |(kind, owner)| composition.resolve_owned_property(kind, owner, name),
            );
            return match resolution {
                Ok((binding, receipt)) => {
                    s.builder.push_binding_receipt(receipt);
                    match binding {
                        SymbolBinding::Qualified(symbol) => match composition
                            .semantic_id(&symbol)
                            .map_err(|error| error.to_string())
                            .and_then(|id| {
                                PropertyId::ontology(PropId(id.0))
                                    .map_err(|error| error.to_string())
                            }) {
                            Ok(id) => Some(id),
                            Err(error) => {
                                s.errors.push(BindError::new(
                                    BindErrorKind::InvalidArgument,
                                    span,
                                    error,
                                ));
                                None
                            }
                        },
                        SymbolBinding::Runtime { local_id, .. } => {
                            self.runtime_property(&local_id, None, span, s)
                        }
                    }
                }
                Err(diagnostic) => {
                    Self::push_composition_error(&diagnostic, span, s);
                    None
                }
            };
        }
        match self.mode {
            OntologyMode::Strict => self.resolve_strict_property(name, span, owner, s),
            OntologyMode::Advisory => {
                s.warnings.push(BindError::new(
                    BindErrorKind::UnknownProperty,
                    span,
                    format!("unknown property `{name}` — using runtime catalog"),
                ));
                self.runtime_property(name, None, span, s)
            }
            OntologyMode::Exploratory => self.runtime_property(name, None, span, s),
        }
    }

    pub(super) fn push_composition_error(
        diagnostic: &crate::BindingDiagnostic,
        span: Span,
        s: &mut BinderState,
    ) {
        if diagnostic.code == BindingDiagnosticCode::WrongOwnerProperty
            && let Some((owner, property)) = diagnostic.subject.split_once('.')
            && !owner.contains(':')
        {
            s.errors.push(BindError::new(
                BindErrorKind::UnknownProperty,
                span,
                format!("property `{property}` is not declared for entity `{owner}`"),
            ));
            return;
        }
        let kind = match diagnostic.code {
            BindingDiagnosticCode::AmbiguousSymbol => BindErrorKind::AmbiguousComposedSymbol,
            BindingDiagnosticCode::UnknownSymbol => BindErrorKind::UnknownLabel,
            _ => BindErrorKind::CompositionConflict,
        };
        let candidates = if diagnostic.candidates.is_empty() {
            String::new()
        } else {
            format!("; candidates: {}", diagnostic.candidates.join(", "))
        };
        s.errors.push(BindError::new(
            kind,
            span,
            format!(
                "{}: {}{}; remediation: {}",
                serde_json::to_value(diagnostic.code)
                    .ok()
                    .and_then(|value| value.as_str().map(ToOwned::to_owned))
                    .unwrap_or_else(|| "composition_error".to_owned()),
                diagnostic.subject,
                candidates,
                diagnostic.remediation
            ),
        ));
    }

    fn resolve_strict_property(
        &self,
        name: &str,
        span: Span,
        owner: BoundPropertyOwner,
        s: &mut BinderState,
    ) -> Option<PropertyId> {
        if owner == BoundPropertyOwner::Value {
            return self.runtime_property(name, None, span, s);
        }
        let Some(handle) = &self.ontology else {
            s.errors.push(BindError::new(
                BindErrorKind::UnknownProperty,
                span,
                format!("unknown property `{name}` (strict mode has no ontology)"),
            ));
            return None;
        };

        let (declarations, description, runtime_owner, entity_owner) = match owner {
            BoundPropertyOwner::Entity(Some(owner)) => (
                handle
                    .entity_type_id(&owner)
                    .map(|id| handle.entity_property_declarations(id, name))
                    .unwrap_or_default(),
                format!("entity `{owner}`"),
                Some(owner),
                true,
            ),
            BoundPropertyOwner::Entity(None) => (
                handle.all_entity_property_declarations(name),
                "unlabeled entity".to_owned(),
                None,
                true,
            ),
            BoundPropertyOwner::Relationship(Some(owner)) => (
                handle
                    .relation_type_id(&owner)
                    .map(|id| handle.relation_property_declarations(id, name))
                    .unwrap_or_default(),
                format!("relationship `{owner}`"),
                Some(owner),
                false,
            ),
            BoundPropertyOwner::Relationship(None) => (
                handle.all_relation_property_declarations(name),
                "untyped relationship".to_owned(),
                None,
                false,
            ),
            BoundPropertyOwner::Value => unreachable!("value properties returned above"),
        };

        if declarations.len() == 1 {
            return self.runtime_property(name, runtime_owner.as_deref(), span, s);
        }
        let (kind, message) = if declarations.is_empty() {
            (
                BindErrorKind::UnknownProperty,
                format!("property `{name}` is not declared for {description} (strict mode)"),
            )
        } else {
            let owners = declarations
                .iter()
                .filter_map(|(id, _)| {
                    if entity_owner {
                        handle.entity_type_name(*id)
                    } else {
                        handle.relation_type_name(*id)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            (
                BindErrorKind::AmbiguousProperty,
                format!("property `{name}` is ambiguous for {description}; declarations: {owners}"),
            )
        };
        s.errors.push(BindError::new(kind, span, message));
        None
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "flat compile-time registry mirrors the scalar-function dispatch table"
)]
fn is_known_cypher_function(call: &FunctionCall) -> bool {
    let name = call.name.join(".").to_ascii_lowercase();
    matches!(
        name.as_str(),
        "abs"
            | "allshortestpaths"
            | "avg"
            | "ceil"
            | "char_length"
            | "character_length"
            | "coalesce"
            | "collect"
            | "concat"
            | "count"
            | "date"
            | "datetime"
            | "datetime.fromepoch"
            | "datetime.fromepochmillis"
            | "date.truncate"
            | "datetime.truncate"
            | "duration"
            | "duration.between"
            | "duration.indays"
            | "duration.inmonths"
            | "duration.inseconds"
            | "distance"
            | "elementid"
            | "endnode"
            | "exp"
            | "exists"
            | "extract"
            | "filter"
            | "floor"
            | "head"
            | "id"
            | "keys"
            | "labels"
            | "last"
            | "length"
            | "localdatetime"
            | "localdatetime.truncate"
            | "localtime"
            | "localtime.truncate"
            | "log"
            | "lower"
            | "ltrim"
            | "max"
            | "min"
            | "nodes"
            | "percentilecont"
            | "percentiledisc"
            | "point"
            | "power"
            | "properties"
            | "rand"
            | "range"
            | "reduce"
            | "relationships"
            | "replace"
            | "reverse"
            | "round"
            | "rtrim"
            | "shortestpath"
            | "sign"
            | "size"
            | "split"
            | "sqrt"
            | "startnode"
            | "string.concat"
            | "substring"
            | "sum"
            | "tail"
            | "time"
            | "time.truncate"
            | "toboolean"
            | "tofloat"
            | "tointeger"
            | "tolower"
            | "tostring"
            | "toupper"
            | "trim"
            | "type"
            | "upper"
            | "timestamp"
            | "_slice"
            | "_slice_from_start"
            | "_slice_to_end"
            | "_subscript"
    ) || matches!(
        name.as_str(),
        "date.transaction"
            | "date.statement"
            | "date.realtime"
            | "datetime.transaction"
            | "datetime.statement"
            | "datetime.realtime"
            | "localdatetime.transaction"
            | "localdatetime.statement"
            | "localdatetime.realtime"
            | "localtime.transaction"
            | "localtime.statement"
            | "localtime.realtime"
            | "time.transaction"
            | "time.statement"
            | "time.realtime"
    )
}

fn lower_literal(lit: &Literal) -> IrLiteral {
    match lit {
        Literal::Int(n, _) => IrLiteral::Int(*n),
        Literal::Float(f, _) => IrLiteral::Float(*f),
        Literal::Str(s, _) => IrLiteral::Str(s.clone()),
        Literal::Bool(b, _) => IrLiteral::Bool(*b),
        Literal::Null(_) => IrLiteral::Null,
    }
}

fn lower_binop(op: AstBinOp) -> BinaryOpKind {
    match op {
        AstBinOp::Eq => BinaryOpKind::Eq,
        AstBinOp::Neq => BinaryOpKind::Neq,
        AstBinOp::Lt => BinaryOpKind::Lt,
        AstBinOp::Lte => BinaryOpKind::Lte,
        AstBinOp::Gt => BinaryOpKind::Gt,
        AstBinOp::Gte => BinaryOpKind::Gte,
        AstBinOp::And => BinaryOpKind::And,
        AstBinOp::Or => BinaryOpKind::Or,
        AstBinOp::Xor => BinaryOpKind::Xor,
        AstBinOp::Add => BinaryOpKind::Add,
        AstBinOp::Sub => BinaryOpKind::Sub,
        AstBinOp::Mul => BinaryOpKind::Mul,
        AstBinOp::Div => BinaryOpKind::Div,
        AstBinOp::Mod => BinaryOpKind::Mod,
        AstBinOp::Pow => BinaryOpKind::Pow,
        AstBinOp::Concat => unreachable!("handled separately"),
    }
}

#[cfg(test)]
mod tests;
