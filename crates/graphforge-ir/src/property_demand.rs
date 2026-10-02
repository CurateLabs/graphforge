//! Whether a read plan needs any property value schema to compile.
//!
//! Compiling a plan against stored property schemas admits each property
//! route's content first (a footer is a payload fact). A plan that reads no
//! property value needs no schema, so it can compile without admitting a route
//! it never reads values from. The analysis is conservative: anything it cannot
//! prove value-free demands every schema.

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;

use crate::arrow_schema::TOPOLOGY_NODES_SCHEMA;
use crate::{ExprArena, ExprId, GraphOp, GraphPlan, IrExpr, PropertyId, VarId};

/// The property schemas a plan needs at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyDemand {
    /// The plan reads no property value: it binds only graph topology, node
    /// topology columns, literals and parameters.
    None,
    /// The plan may read property values, or the analysis cannot prove it does
    /// not; every property schema must be captured.
    Complete,
}

/// The property demand of `plan`, given the catalog's `PropertyId → name` map.
///
/// A plan demands nothing only when every expression it evaluates is built from
/// literals, parameters and functions over them, plus reads of a node
/// variable's topology columns (`n.node_uuid`). Any entity used as a value
/// (`RETURN n`, `keys(n)`, `count(n)`, `WITH n AS m`, a path), any other
/// property read, an unresolved property identity, a write, a procedure call or
/// an operator this analysis does not model demands every schema.
#[must_use]
pub fn property_demand<S: BuildHasher>(
    plan: &GraphPlan,
    property_names: &HashMap<PropertyId, String, S>,
) -> PropertyDemand {
    let analysis = DemandAnalysis { property_names };
    if analysis.plan_is_value_free(plan, &HashSet::new()) {
        PropertyDemand::None
    } else {
        PropertyDemand::Complete
    }
}

/// Node variables an `OPTIONAL` child binds into the enclosing plan.
fn collect_node_vars(plan: &GraphPlan, vars: &mut HashSet<VarId>) {
    for op in &plan.ops {
        bind_node_vars(op, vars);
    }
}

/// Add the node variables `op` binds to `vars`. An `EXISTS` or
/// pattern-comprehension child's variables stay in that child, and a `UNION`
/// branch is bound afresh, so none of them enter the enclosing scope.
fn bind_node_vars(op: &GraphOp, vars: &mut HashSet<VarId>) {
    match op {
        GraphOp::NodeScan { var, .. } => {
            vars.insert(*var);
        }
        GraphOp::Expand { src, dst, .. } => {
            vars.insert(*src);
            vars.insert(*dst);
        }
        GraphOp::Optional { child } => collect_node_vars(child, vars),
        _ => {}
    }
}

struct DemandAnalysis<'a, S> {
    property_names: &'a HashMap<PropertyId, String, S>,
}

impl<S: BuildHasher> DemandAnalysis<'_, S> {
    /// `inherited` holds the enclosing plan's node variables in scope where
    /// `plan` runs. A variable is in scope from the operator that binds it
    /// onward: an expression or child plan only references variables bound
    /// before it, so a number the binder reuses for a later variable, or one a
    /// child scope reused, never reaches it.
    fn plan_is_value_free(&self, plan: &GraphPlan, inherited: &HashSet<VarId>) -> bool {
        let mut node_vars = inherited.clone();
        let exprs = &plan.exprs;
        for op in &plan.ops {
            let free = |id: &ExprId| self.expr_is_value_free(exprs, &node_vars, *id);
            let value_free = match op {
                GraphOp::NodeScan { .. }
                | GraphOp::EdgeScan { .. }
                | GraphOp::TypedEdgeScan { .. }
                | GraphOp::Expand { .. }
                | GraphOp::RelationshipUnique { .. }
                | GraphOp::Limit { .. }
                | GraphOp::LimitParam { .. }
                | GraphOp::Skip { .. }
                | GraphOp::SkipParam { .. } => true,
                GraphOp::Filter { predicate } => free(predicate),
                GraphOp::Project { items, .. } => items.iter().all(|item| free(&item.expr)),
                GraphOp::With {
                    items,
                    where_predicate,
                    ..
                } => items.iter().all(|item| free(&item.expr)) && where_predicate.iter().all(free),
                GraphOp::Aggregate { group_by, aggs, .. } => {
                    group_by.iter().all(free)
                        && aggs
                            .iter()
                            .all(|agg| agg.arg.iter().all(free) && agg.percentile.iter().all(free))
                }
                GraphOp::Sort { keys } => keys.iter().all(|key| free(&key.expr)),
                GraphOp::LimitExpr { expr } | GraphOp::SkipExpr { expr } => free(expr),
                GraphOp::Unwind { list_expr, .. } => free(list_expr),
                GraphOp::Optional { child }
                | GraphOp::Exists { child, .. }
                | GraphOp::PatternComprehension { child, .. } => {
                    self.plan_is_value_free(child, &node_vars)
                }
                // Each branch numbers its own variables; none is this plan's.
                GraphOp::Union { inputs, .. } => inputs
                    .iter()
                    .all(|input| self.plan_is_value_free(input, &HashSet::new())),
                // Writes, procedure calls, graph-valued list comprehensions and
                // any operator added later read or write entities whole.
                _ => false,
            };
            if !value_free {
                return false;
            }
            bind_node_vars(op, &mut node_vars);
        }
        true
    }

    fn expr_is_value_free(
        &self,
        exprs: &ExprArena,
        node_vars: &HashSet<VarId>,
        id: ExprId,
    ) -> bool {
        let free = |id: &ExprId| self.expr_is_value_free(exprs, node_vars, *id);
        match exprs.get(id) {
            IrExpr::Literal(_) | IrExpr::Parameter(_) => true,
            // An entity, path or bound value used whole.
            IrExpr::VarRef(_) => false,
            IrExpr::PropertyAccess { base, prop } => {
                matches!(exprs.get(*base), IrExpr::VarRef(var) if node_vars.contains(var))
                    && self
                        .property_names
                        .get(prop)
                        .is_some_and(|name| TOPOLOGY_NODES_SCHEMA.field_with_name(name).is_ok())
            }
            IrExpr::BinaryOp { left, right, .. } => free(left) && free(right),
            IrExpr::UnaryOp { expr, .. } => free(expr),
            IrExpr::FunctionCall { args, .. } => args.iter().all(free),
            IrExpr::Case {
                operand,
                arms,
                else_expr,
            } => {
                operand.iter().all(free)
                    && arms.iter().all(|arm| free(&arm.when) && free(&arm.then))
                    && else_expr.iter().all(free)
            }
            IrExpr::ListLiteral(items) => items.iter().all(free),
            IrExpr::MapLiteral(entries) => entries.iter().all(|(_, value)| free(value)),
            IrExpr::Quantifier {
                list, predicate, ..
            } => free(list) && free(predicate),
            IrExpr::ListComprehension {
                list,
                filter,
                projection,
                ..
            } => free(list) && filter.iter().all(free) && projection.iter().all(free),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AggExpr, AggFunc, IrLiteral, ProjectItem, RemovePropItem, SetPropItem, SortKey, SortOrder,
    };

    const NODE_UUID: u32 = 1;
    const NAME: u32 = 2;
    const UNRESOLVED: u32 = 3;
    const UPDATED_AT: u32 = 4;

    fn property(id: u32) -> PropertyId {
        PropertyId::runtime(graphforge_value::RuntimePropId::new(id).unwrap())
    }

    fn names() -> HashMap<PropertyId, String> {
        HashMap::from([
            (property(NODE_UUID), "node_uuid".to_owned()),
            (property(NAME), "name".to_owned()),
        ])
    }

    fn scan(var: u32) -> GraphOp {
        GraphOp::NodeScan {
            var: VarId(var),
            ty: None,
        }
    }

    /// `MATCH (n) <ops built by `tail`>` over a fresh arena.
    fn demand(tail: impl FnOnce(&mut ExprArena) -> Vec<GraphOp>) -> PropertyDemand {
        let mut exprs = ExprArena::new();
        let mut ops = vec![scan(0)];
        ops.extend(tail(&mut exprs));
        let mut plan = GraphPlan::builder("openCypher").build();
        plan.ops = ops;
        plan.exprs = exprs;
        property_demand(&plan, &names())
    }

    fn read(exprs: &mut ExprArena, var: u32, prop: u32) -> ExprId {
        let base = exprs.push(IrExpr::VarRef(VarId(var)));
        exprs.push(IrExpr::PropertyAccess {
            base,
            prop: property(prop),
        })
    }

    fn returning(expr: ExprId) -> GraphOp {
        GraphOp::Project {
            items: vec![ProjectItem {
                expr,
                alias: Some("v".into()),
                out_var: None,
            }],
            distinct: false,
        }
    }

    fn count(arg: Option<ExprId>) -> GraphOp {
        GraphOp::Aggregate {
            group_by: Vec::new(),
            group_aliases: Vec::new(),
            group_vars: Vec::new(),
            aggs: vec![AggExpr {
                func: AggFunc::Count,
                arg,
                percentile: None,
                alias: "value".into(),
                out_var: None,
            }],
        }
    }

    #[test]
    fn value_free_plans_demand_no_property_schema() {
        // MATCH (n) RETURN count(*)
        assert_eq!(demand(|_| vec![count(None)]), PropertyDemand::None);
        // MATCH (n) RETURN n.node_uuid ORDER BY n.node_uuid LIMIT 3
        assert_eq!(
            demand(|exprs| {
                let id = read(exprs, 0, NODE_UUID);
                vec![
                    GraphOp::Sort {
                        keys: vec![SortKey {
                            expr: id,
                            order: SortOrder::Asc,
                            nulls_first: false,
                        }],
                    },
                    returning(id),
                    GraphOp::Limit { count: 3 },
                ]
            }),
            PropertyDemand::None
        );
        // MATCH (n) WHERE n.node_uuid = $uuid RETURN 1 + 2
        assert_eq!(
            demand(|exprs| {
                let id = read(exprs, 0, NODE_UUID);
                let param = exprs.push(IrExpr::Parameter("uuid".into()));
                let predicate = exprs.push(IrExpr::BinaryOp {
                    op: crate::BinaryOpKind::Eq,
                    left: id,
                    right: param,
                });
                let one = exprs.push(IrExpr::Literal(IrLiteral::Int(1)));
                let two = exprs.push(IrExpr::Literal(IrLiteral::Int(2)));
                let sum = exprs.push(IrExpr::BinaryOp {
                    op: crate::BinaryOpKind::Add,
                    left: one,
                    right: two,
                });
                vec![GraphOp::Filter { predicate }, returning(sum)]
            }),
            PropertyDemand::None
        );
    }

    #[test]
    fn property_values_and_whole_entities_demand_every_schema() {
        let cases: Vec<(&str, Box<dyn FnOnce(&mut ExprArena) -> Vec<GraphOp>>)> = vec![
            (
                "RETURN n.name",
                Box::new(|exprs| vec![returning(read(exprs, 0, NAME))]),
            ),
            (
                "unresolved property identity",
                Box::new(|exprs| vec![returning(read(exprs, 0, UNRESOLVED))]),
            ),
            (
                "topology name on a non-node variable",
                Box::new(|exprs| vec![returning(read(exprs, 9, NODE_UUID))]),
            ),
            (
                "WHERE n.name = 'x' RETURN count(*)",
                Box::new(|exprs| {
                    let name = read(exprs, 0, NAME);
                    let literal = exprs.push(IrExpr::Literal(IrLiteral::Str("x".into())));
                    let predicate = exprs.push(IrExpr::BinaryOp {
                        op: crate::BinaryOpKind::Eq,
                        left: name,
                        right: literal,
                    });
                    vec![GraphOp::Filter { predicate }, count(None)]
                }),
            ),
            (
                "RETURN n",
                Box::new(|exprs| vec![returning(exprs.push(IrExpr::VarRef(VarId(0))))]),
            ),
            (
                "RETURN keys(n)",
                Box::new(|exprs| {
                    let node = exprs.push(IrExpr::VarRef(VarId(0)));
                    vec![returning(exprs.push(IrExpr::FunctionCall {
                        name: "keys".into(),
                        args: vec![node],
                    }))]
                }),
            ),
            (
                "RETURN count(n)",
                Box::new(|exprs| vec![count(Some(exprs.push(IrExpr::VarRef(VarId(0)))))]),
            ),
            (
                "WITH n AS m",
                Box::new(|exprs| {
                    vec![GraphOp::With {
                        items: vec![ProjectItem {
                            expr: exprs.push(IrExpr::VarRef(VarId(0))),
                            alias: Some("m".into()),
                            out_var: Some(VarId(1)),
                        }],
                        distinct: false,
                        where_predicate: None,
                    }]
                }),
            ),
            (
                "SET n.name = 'x'",
                Box::new(|exprs| {
                    vec![GraphOp::Set {
                        items: vec![SetPropItem {
                            target: VarId(0),
                            prop: property(NAME),
                            prop_name: "name".into(),
                            value: exprs.push(IrExpr::Literal(IrLiteral::Str("x".into()))),
                        }],
                        map_items: Vec::new(),
                        label_items: Vec::new(),
                    }]
                }),
            ),
            (
                "REMOVE n.name",
                Box::new(|_| {
                    vec![GraphOp::Remove {
                        items: vec![RemovePropItem {
                            target: VarId(0),
                            prop: property(NAME),
                            prop_name: "name".into(),
                        }],
                        label_items: Vec::new(),
                    }]
                }),
            ),
        ];
        for (name, tail) in cases {
            assert_eq!(demand(tail), PropertyDemand::Complete, "{name}");
        }
    }

    #[test]
    fn nested_plans_carry_their_demand_outward() {
        let child = |demanding: bool| {
            let mut exprs = ExprArena::new();
            let mut ops = vec![scan(1)];
            if demanding {
                ops.push(returning(read(&mut exprs, 1, NAME)));
            }
            let mut plan = GraphPlan::builder("openCypher").build();
            plan.ops = ops;
            plan.exprs = exprs;
            plan
        };
        for demanding in [false, true] {
            let expected = if demanding {
                PropertyDemand::Complete
            } else {
                PropertyDemand::None
            };
            for op in [
                GraphOp::Optional {
                    child: Box::new(child(demanding)),
                },
                GraphOp::Exists {
                    child: Box::new(child(demanding)),
                    negated: false,
                },
                GraphOp::Union {
                    all: true,
                    inputs: vec![child(false), child(demanding)],
                },
            ] {
                assert_eq!(demand(|_| vec![op.clone()]), expected, "{op:?}");
            }
        }
    }

    /// Each `UNION` branch is bound afresh, so its variable numbers collide with
    /// another branch's. An edge variable that shares a number with a node
    /// variable elsewhere must not borrow that node's topology columns.
    #[test]
    fn union_branches_do_not_share_node_variables() {
        let mut names = names();
        names.insert(property(UPDATED_AT), "updated_at".to_owned());
        let nodes_only = {
            let mut plan = GraphPlan::builder("openCypher").build();
            plan.ops = vec![scan(0), scan(1), scan(2), count(None)];
            plan
        };
        let edge_property = {
            let mut exprs = ExprArena::new();
            let read = read(&mut exprs, 1, UPDATED_AT);
            let mut plan = GraphPlan::builder("openCypher").build();
            plan.ops = vec![
                scan(0),
                GraphOp::Expand {
                    src: VarId(0),
                    edge: VarId(1),
                    dst: VarId(2),
                    rel_ty: None,
                    dir: crate::Direction::Out,
                    min_hops: 1,
                    max_hops: Some(1),
                },
                returning(read),
            ];
            plan.exprs = exprs;
            plan
        };
        let mut union = GraphPlan::builder("openCypher").build();
        union.ops = vec![GraphOp::Union {
            all: true,
            inputs: vec![nodes_only, edge_property.clone()],
        }];
        assert_eq!(property_demand(&union, &names), PropertyDemand::Complete);
        assert_eq!(
            property_demand(&edge_property, &names),
            PropertyDemand::Complete
        );
    }

    /// A pattern predicate binds its anonymous variables in a child scope, and
    /// the binder may number later variables of the enclosing plan the same
    /// way. An edge bound after the predicate must not borrow the predicate's
    /// node, so its topology-named property still demands every schema.
    #[test]
    fn pattern_predicate_variables_stay_in_their_scope() {
        let mut names = names();
        names.insert(property(UPDATED_AT), "updated_at".to_owned());
        let expand = |src: u32, edge: u32, dst: u32| GraphOp::Expand {
            src: VarId(src),
            edge: VarId(edge),
            dst: VarId(dst),
            rel_ty: None,
            dir: crate::Direction::Out,
            min_hops: 1,
            max_hops: Some(1),
        };
        let mut predicate = GraphPlan::builder("openCypher").build();
        predicate.ops = vec![expand(0, 1, 2)];
        let mut exprs = ExprArena::new();
        let read = read(&mut exprs, 2, UPDATED_AT);
        let mut plan = GraphPlan::builder("openCypher").build();
        plan.ops = vec![
            scan(0),
            GraphOp::Exists {
                child: Box::new(predicate),
                negated: false,
            },
            scan(1),
            expand(1, 2, 0),
            returning(read),
        ];
        plan.exprs = exprs;
        assert_eq!(property_demand(&plan, &names), PropertyDemand::Complete);
    }

    /// A predicate's anonymous edge may share its number with a node the
    /// enclosing plan binds later. Inside the predicate that node is not yet in
    /// scope, so the edge's topology-named property still demands every schema,
    /// and a value-free enclosing plan stays value-free.
    #[test]
    fn variables_bound_later_are_not_in_scope_of_an_earlier_predicate() {
        let mut names = names();
        names.insert(property(UPDATED_AT), "updated_at".to_owned());
        let mut predicate_exprs = ExprArena::new();
        let edge_read = read(&mut predicate_exprs, 1, UPDATED_AT);
        let mut predicate = GraphPlan::builder("openCypher").build();
        predicate.ops = vec![
            GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: crate::Direction::Out,
                min_hops: 1,
                max_hops: Some(1),
            },
            GraphOp::Filter {
                predicate: edge_read,
            },
        ];
        predicate.exprs = predicate_exprs;
        let mut plan = GraphPlan::builder("openCypher").build();
        plan.ops = vec![
            scan(0),
            GraphOp::Exists {
                child: Box::new(predicate),
                negated: false,
            },
            scan(1),
            count(None),
        ];
        assert_eq!(property_demand(&plan, &names), PropertyDemand::Complete);

        let mut topology_exprs = ExprArena::new();
        let node_read = read(&mut topology_exprs, 2, NODE_UUID);
        let mut topology_predicate = GraphPlan::builder("openCypher").build();
        topology_predicate.ops = vec![
            GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: crate::Direction::Out,
                min_hops: 1,
                max_hops: Some(1),
            },
            GraphOp::Filter {
                predicate: node_read,
            },
        ];
        topology_predicate.exprs = topology_exprs;
        let mut value_free = GraphPlan::builder("openCypher").build();
        value_free.ops = vec![
            scan(0),
            GraphOp::Exists {
                child: Box::new(topology_predicate),
                negated: false,
            },
            scan(1),
            count(None),
        ];
        assert_eq!(property_demand(&value_free, &names), PropertyDemand::None);
    }

    #[test]
    fn unmodelled_operators_demand_every_schema() {
        assert_eq!(
            demand(|exprs| {
                let list = exprs.push(IrExpr::ListLiteral(Vec::new()));
                vec![GraphOp::ListElementPatternComprehension {
                    list_expr: list,
                    loop_var: VarId(2),
                    child: Box::new(GraphPlan::builder("openCypher").build()),
                    pattern_output: VarId(3),
                    filter: None,
                    projection: None,
                    output: VarId(4),
                }]
            }),
            PropertyDemand::Complete
        );
    }
}
