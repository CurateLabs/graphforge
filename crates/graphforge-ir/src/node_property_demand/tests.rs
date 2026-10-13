use super::*;
use crate::{
    AggExpr, AggFunc, BinaryOpKind, IrLiteral, ProjectItem, SortKey, SortOrder, UnaryOpKind,
};

const NODE_UUID: u32 = 1;
const NAME: u32 = 2;
const TITLE: u32 = 3;
const UNRESOLVED: u32 = 4;

const N: VarId = VarId(0);
const M: VarId = VarId(1);

fn property(id: u32) -> PropertyId {
    PropertyId::runtime(graphforge_value::RuntimePropId::new(id).unwrap())
}

fn names() -> HashMap<PropertyId, String> {
    HashMap::from([
        (property(NODE_UUID), "node_uuid".to_owned()),
        (property(NAME), "name".to_owned()),
        (property(TITLE), "title".to_owned()),
    ])
}

fn scan(var: VarId) -> GraphOp {
    GraphOp::NodeScan { var, ty: None }
}

fn plan_of(ops: Vec<GraphOp>, exprs: ExprArena) -> GraphPlan {
    let mut plan = GraphPlan::builder("openCypher").build();
    plan.ops = ops;
    plan.exprs = exprs;
    plan
}

/// `MATCH (n), (m) <ops built by `tail`>` over a fresh arena.
fn demand(tail: impl FnOnce(&mut ExprArena) -> Vec<GraphOp>) -> NodePropertyDemand {
    let mut exprs = ExprArena::new();
    let mut ops = vec![scan(N), scan(M)];
    ops.extend(tail(&mut exprs));
    node_property_demand(&plan_of(ops, exprs), &names())
}

fn var(exprs: &mut ExprArena, var: VarId) -> ExprId {
    exprs.push(IrExpr::VarRef(var))
}

fn read(exprs: &mut ExprArena, v: VarId, prop: u32) -> ExprId {
    let base = var(exprs, v);
    exprs.push(IrExpr::PropertyAccess {
        base,
        prop: property(prop),
    })
}

fn call(exprs: &mut ExprArena, name: &str, args: Vec<ExprId>) -> ExprId {
    exprs.push(IrExpr::FunctionCall {
        name: name.into(),
        args,
    })
}

fn binary(exprs: &mut ExprArena, op: BinaryOpKind, left: ExprId, right: ExprId) -> ExprId {
    exprs.push(IrExpr::BinaryOp { op, left, right })
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

fn aggregate(func: AggFunc, arg: Option<ExprId>) -> GraphOp {
    GraphOp::Aggregate {
        group_by: Vec::new(),
        group_aliases: Vec::new(),
        group_vars: Vec::new(),
        aggs: vec![AggExpr {
            func,
            arg,
            percentile: None,
            alias: "value".into(),
            out_var: None,
        }],
    }
}

fn keeping(expr: ExprId, out_var: Option<VarId>) -> GraphOp {
    GraphOp::With {
        items: vec![ProjectItem {
            expr,
            alias: Some("kept".into()),
            out_var,
        }],
        distinct: false,
        where_predicate: None,
    }
}

/// The columns of `v` the demand asks for, out of `name`, `title`, `other`.
fn needed(demand: &NodePropertyDemand, v: VarId) -> Vec<&'static str> {
    ["name", "title", "other"]
        .into_iter()
        .filter(|column| demand.needs(v, column))
        .collect()
}

#[test]
fn identity_only_uses_demand_no_property() {
    let cases: Vec<(&str, Box<dyn FnOnce(&mut ExprArena) -> Vec<GraphOp>>)> = vec![
        (
            "count(n)",
            Box::new(|exprs| {
                let n = var(exprs, N);
                vec![aggregate(AggFunc::Count, Some(n))]
            }),
        ),
        (
            "count(DISTINCT n)",
            Box::new(|exprs| {
                let n = var(exprs, N);
                vec![aggregate(AggFunc::CountDistinct, Some(n))]
            }),
        ),
        (
            "count(*)",
            Box::new(|_| vec![aggregate(AggFunc::Count, None)]),
        ),
        (
            "collect(n) narrowed to identity",
            Box::new(|exprs| {
                let n = var(exprs, N);
                vec![aggregate(AggFunc::Collect, Some(n))]
            }),
        ),
        (
            "n IN list",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let list = exprs.push(IrExpr::Parameter("list".into()));
                let predicate = binary(exprs, BinaryOpKind::In, n, list);
                vec![GraphOp::Filter { predicate }]
            }),
        ),
        (
            "n = m",
            Box::new(|exprs| {
                let (n, m) = (var(exprs, N), var(exprs, M));
                let predicate = binary(exprs, BinaryOpKind::Eq, n, m);
                vec![GraphOp::Filter { predicate }]
            }),
        ),
        (
            "n <> m",
            Box::new(|exprs| {
                let (n, m) = (var(exprs, N), var(exprs, M));
                let predicate = binary(exprs, BinaryOpKind::Neq, n, m);
                vec![GraphOp::Filter { predicate }]
            }),
        ),
        (
            "'Label' IN labels(n)",
            Box::new(|exprs| {
                let label = exprs.push(IrExpr::Literal(IrLiteral::Str("Label".into())));
                let n = var(exprs, N);
                let labels = call(exprs, "labels", vec![n]);
                let predicate = binary(exprs, BinaryOpKind::In, label, labels);
                vec![GraphOp::Filter { predicate }]
            }),
        ),
        (
            "n IS NULL",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let predicate = exprs.push(IrExpr::UnaryOp {
                    op: UnaryOpKind::IsNull,
                    expr: n,
                });
                vec![GraphOp::Filter { predicate }]
            }),
        ),
        (
            "n IS NOT NULL",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let predicate = exprs.push(IrExpr::UnaryOp {
                    op: UnaryOpKind::IsNotNull,
                    expr: n,
                });
                vec![GraphOp::Filter { predicate }]
            }),
        ),
        (
            "WITH n (kept under its own name)",
            Box::new(|exprs| {
                let n = var(exprs, N);
                vec![keeping(n, None)]
            }),
        ),
        (
            "group by n, keeping the variable",
            Box::new(|exprs| {
                let n = var(exprs, N);
                vec![GraphOp::Aggregate {
                    group_by: vec![n],
                    group_aliases: vec![Some("n".into())],
                    group_vars: vec![Some(N)],
                    aggs: Vec::new(),
                }]
            }),
        ),
    ];
    for (name, tail) in cases {
        let demand = demand(tail);
        for v in [N, M] {
            assert_eq!(needed(&demand, v), Vec::<&str>::new(), "{name}: {v:?}");
        }
    }
}

#[test]
fn a_property_read_demands_exactly_that_column() {
    let cases: Vec<(&str, Box<dyn FnOnce(&mut ExprArena) -> Vec<GraphOp>>)> = vec![
        (
            "WHERE n.name = $x",
            Box::new(|exprs| {
                let name = read(exprs, N, NAME);
                let x = exprs.push(IrExpr::Parameter("x".into()));
                let predicate = binary(exprs, BinaryOpKind::Eq, name, x);
                vec![GraphOp::Filter { predicate }]
            }),
        ),
        (
            "ORDER BY n.name",
            Box::new(|exprs| {
                let key = read(exprs, N, NAME);
                vec![GraphOp::Sort {
                    keys: vec![SortKey {
                        expr: key,
                        order: SortOrder::Asc,
                        nulls_first: false,
                    }],
                }]
            }),
        ),
        (
            "RETURN n.name",
            Box::new(|exprs| {
                let name = read(exprs, N, NAME);
                vec![returning(name)]
            }),
        ),
        (
            "max(n.name)",
            Box::new(|exprs| {
                let name = read(exprs, N, NAME);
                vec![aggregate(AggFunc::Max, Some(name))]
            }),
        ),
        (
            "group by n.name",
            Box::new(|exprs| {
                let name = read(exprs, N, NAME);
                vec![GraphOp::Aggregate {
                    group_by: vec![name],
                    group_aliases: vec![Some("name".into())],
                    group_vars: vec![None],
                    aggs: Vec::new(),
                }]
            }),
        ),
        (
            "WITH 1 AS one WHERE n.name IS NOT NULL",
            Box::new(|exprs| {
                let one = exprs.push(IrExpr::Literal(IrLiteral::Int(1)));
                let name = read(exprs, N, NAME);
                let predicate = exprs.push(IrExpr::UnaryOp {
                    op: UnaryOpKind::IsNotNull,
                    expr: name,
                });
                vec![GraphOp::With {
                    items: vec![ProjectItem {
                        expr: one,
                        alias: Some("one".into()),
                        out_var: None,
                    }],
                    distinct: false,
                    where_predicate: Some(predicate),
                }]
            }),
        ),
        (
            "CASE WHEN n.name = 'a' THEN 1 END",
            Box::new(|exprs| {
                let name = read(exprs, N, NAME);
                let a = exprs.push(IrExpr::Literal(IrLiteral::Str("a".into())));
                let when = binary(exprs, BinaryOpKind::Eq, name, a);
                let then = exprs.push(IrExpr::Literal(IrLiteral::Int(1)));
                let case = exprs.push(IrExpr::Case {
                    operand: None,
                    arms: vec![crate::CaseArm { when, then }],
                    else_expr: None,
                });
                vec![returning(case)]
            }),
        ),
        (
            "[n.name, 1]",
            Box::new(|exprs| {
                let name = read(exprs, N, NAME);
                let one = exprs.push(IrExpr::Literal(IrLiteral::Int(1)));
                let list = exprs.push(IrExpr::ListLiteral(vec![name, one]));
                vec![returning(list)]
            }),
        ),
        (
            "{k: n.name}",
            Box::new(|exprs| {
                let name = read(exprs, N, NAME);
                let map = exprs.push(IrExpr::MapLiteral(vec![("k".into(), name)]));
                vec![returning(map)]
            }),
        ),
        (
            "toUpper(n.name)",
            Box::new(|exprs| {
                let name = read(exprs, N, NAME);
                let upper = call(exprs, "toUpper", vec![name]);
                vec![returning(upper)]
            }),
        ),
    ];
    for (name, tail) in cases {
        let demand = demand(tail);
        assert_eq!(needed(&demand, N), vec!["name"], "{name}");
        assert_eq!(needed(&demand, M), Vec::<&str>::new(), "{name}: m");
    }
}

#[test]
fn each_variable_and_column_is_tracked_separately() {
    // MATCH (n), (m) WHERE n.name = m.title RETURN n.title
    let demand = demand(|exprs| {
        let (name, title) = (read(exprs, N, NAME), read(exprs, M, TITLE));
        let predicate = binary(exprs, BinaryOpKind::Eq, name, title);
        let returned = read(exprs, N, TITLE);
        vec![GraphOp::Filter { predicate }, returning(returned)]
    });
    assert_eq!(needed(&demand, N), vec!["name", "title"]);
    assert_eq!(needed(&demand, M), vec!["title"]);
    // A variable the plan never mentions demands nothing.
    assert_eq!(needed(&demand, VarId(9)), Vec::<&str>::new());
}

#[test]
fn whole_node_uses_demand_every_column() {
    let cases: Vec<(&str, Box<dyn FnOnce(&mut ExprArena) -> Vec<GraphOp>>)> = vec![
        (
            "RETURN n",
            Box::new(|exprs| {
                let n = var(exprs, N);
                vec![returning(n)]
            }),
        ),
        (
            "ORDER BY n",
            Box::new(|exprs| {
                let n = var(exprs, N);
                vec![GraphOp::Sort {
                    keys: vec![SortKey {
                        expr: n,
                        order: SortOrder::Asc,
                        nulls_first: false,
                    }],
                }]
            }),
        ),
        (
            "properties(n)",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let properties = call(exprs, "properties", vec![n]);
                vec![returning(properties)]
            }),
        ),
        (
            "keys(n)",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let keys = call(exprs, "keys", vec![n]);
                vec![returning(keys)]
            }),
        ),
        (
            "id(n)",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let id = call(exprs, "id", vec![n]);
                vec![returning(id)]
            }),
        ),
        (
            "collect(n) observed as a value",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let node = call(exprs, "_node_struct", vec![n]);
                vec![aggregate(AggFunc::Collect, Some(node))]
            }),
        ),
        (
            "n = $node",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let node = exprs.push(IrExpr::Parameter("node".into()));
                let predicate = binary(exprs, BinaryOpKind::Eq, n, node);
                vec![GraphOp::Filter { predicate }]
            }),
        ),
        (
            "n + 1",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let one = exprs.push(IrExpr::Literal(IrLiteral::Int(1)));
                let sum = binary(exprs, BinaryOpKind::Add, n, one);
                vec![returning(sum)]
            }),
        ),
        (
            "x IN n",
            Box::new(|exprs| {
                let x = exprs.push(IrExpr::Parameter("x".into()));
                let n = var(exprs, N);
                let predicate = binary(exprs, BinaryOpKind::In, x, n);
                vec![GraphOp::Filter { predicate }]
            }),
        ),
        (
            "labels(n, n)",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let all = call(exprs, "labels", vec![n, n]);
                vec![returning(all)]
            }),
        ),
        (
            "sum(n)",
            Box::new(|exprs| {
                let n = var(exprs, N);
                vec![aggregate(AggFunc::Sum, Some(n))]
            }),
        ),
        (
            "{k: n}",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let map = exprs.push(IrExpr::MapLiteral(vec![("k".into(), n)]));
                vec![returning(map)]
            }),
        ),
        (
            "[n]",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let list = exprs.push(IrExpr::ListLiteral(vec![n]));
                vec![returning(list)]
            }),
        ),
        (
            "a property the catalog cannot name",
            Box::new(|exprs| {
                let unresolved = read(exprs, N, UNRESOLVED);
                vec![returning(unresolved)]
            }),
        ),
        (
            "group by n under another variable",
            Box::new(|exprs| {
                let n = var(exprs, N);
                vec![GraphOp::Aggregate {
                    group_by: vec![n],
                    group_aliases: vec![Some("n".into())],
                    group_vars: vec![Some(M)],
                    aggs: Vec::new(),
                }]
            }),
        ),
        (
            "count(head(n))",
            Box::new(|exprs| {
                let n = var(exprs, N);
                let node = call(exprs, "head", vec![n]);
                vec![aggregate(AggFunc::Count, Some(node))]
            }),
        ),
    ];
    for (name, tail) in cases {
        let demand = demand(tail);
        assert!(
            demand.needs(N, "name") && demand.needs(N, "title") && demand.needs(N, "other"),
            "{name}"
        );
    }
}

#[test]
fn an_unmodelled_operator_demands_every_column_of_every_variable() {
    let demand = demand(|_| {
        vec![GraphOp::Delete {
            vars: vec![N],
            exprs: Vec::new(),
            detach: false,
        }]
    });
    assert!(demand.needs(N, "name") && demand.needs(M, "title") && demand.needs(VarId(9), "x"));
    assert_eq!(demand, NodePropertyDemand::everything());
    assert_eq!(
        NodePropertyDemand::default(),
        NodePropertyDemand::everything()
    );
}

#[test]
fn a_node_kept_under_another_name_shares_its_demand() {
    // MATCH (n) WITH n AS m RETURN m.name
    let demand = demand(|exprs| {
        let n = var(exprs, N);
        let name = read(exprs, M, NAME);
        vec![keeping(n, Some(M)), returning(name)]
    });
    assert_eq!(needed(&demand, N), vec!["name"]);
    assert_eq!(needed(&demand, M), vec!["name"]);

    // MATCH (n) WITH n AS m RETURN m
    let demand = demand_whole_after_rename();
    assert!(demand.needs(N, "other") && demand.needs(M, "other"));

    // MATCH (n) WITH n AS m WHERE n.title IS NOT NULL: both names, one class.
    let demand = self::demand(|exprs| {
        let n = var(exprs, N);
        let title = read(exprs, N, TITLE);
        vec![GraphOp::With {
            items: vec![ProjectItem {
                expr: n,
                alias: Some("m".into()),
                out_var: Some(M),
            }],
            distinct: false,
            where_predicate: Some(title),
        }]
    });
    assert_eq!(needed(&demand, N), vec!["title"]);
    assert_eq!(needed(&demand, M), vec!["title"]);
}

fn demand_whole_after_rename() -> NodePropertyDemand {
    demand(|exprs| {
        let n = var(exprs, N);
        let m = var(exprs, M);
        vec![keeping(n, Some(M)), returning(m)]
    })
}

#[test]
fn a_rename_chain_shares_one_demand() {
    // MATCH (n) WITH n AS m WITH m AS k RETURN k.name
    let k = VarId(2);
    let mut exprs = ExprArena::new();
    let n = var(&mut exprs, N);
    let m = var(&mut exprs, M);
    let name = read(&mut exprs, k, NAME);
    let ops = vec![
        scan(N),
        keeping(n, Some(M)),
        keeping(m, Some(k)),
        returning(name),
    ];
    let demand = node_property_demand(&plan_of(ops, exprs), &names());
    for v in [N, M, k] {
        assert_eq!(needed(&demand, v), vec!["name"], "{v:?}");
    }
}

#[test]
fn nested_plans_contribute_their_demand() {
    let child = |ops: fn(&mut ExprArena) -> Vec<GraphOp>| {
        let mut exprs = ExprArena::new();
        let mut child_ops = vec![scan(N)];
        child_ops.extend(ops(&mut exprs));
        plan_of(child_ops, exprs)
    };
    let reading_title: fn(&mut ExprArena) -> Vec<GraphOp> = |exprs| {
        let title = read(exprs, N, TITLE);
        vec![returning(title)]
    };
    let counting: fn(&mut ExprArena) -> Vec<GraphOp> = |exprs| {
        let n = var(exprs, N);
        vec![aggregate(AggFunc::Count, Some(n))]
    };
    for (kind, wrap) in [
        (
            "OPTIONAL",
            (|child| GraphOp::Optional {
                child: Box::new(child),
            }) as fn(GraphPlan) -> GraphOp,
        ),
        ("EXISTS", |child| GraphOp::Exists {
            child: Box::new(child),
            negated: false,
        }),
        ("UNION", |child| GraphOp::Union {
            all: true,
            inputs: vec![child],
        }),
    ] {
        let mut outer = ExprArena::new();
        let outer_name = read(&mut outer, N, NAME);
        let ops = vec![scan(N), wrap(child(reading_title)), returning(outer_name)];
        let demand = node_property_demand(&plan_of(ops, outer), &names());
        assert_eq!(needed(&demand, N), vec!["name", "title"], "{kind}");

        let ops = vec![scan(N), wrap(child(counting))];
        let demand = node_property_demand(&plan_of(ops, ExprArena::new()), &names());
        assert_eq!(needed(&demand, N), Vec::<&str>::new(), "{kind} identity");
    }
}

/// The IC5 shape: a friend collected and tested with `IN`, a post counted, a
/// forum read for `title` and ordered by `id`.
#[test]
fn a_counted_and_collected_node_demands_nothing_while_its_group_key_demands_what_is_read() {
    let (person, friend, forum, post) = (VarId(0), VarId(1), VarId(2), VarId(3));
    let friends = VarId(4);
    let mut exprs = ExprArena::new();
    let friend_ref = var(&mut exprs, friend);
    let forum_ref = var(&mut exprs, forum);
    let post_ref = var(&mut exprs, post);
    let list_ref = var(&mut exprs, friends);
    let member = binary(&mut exprs, BinaryOpKind::In, friend_ref, list_ref);
    let person_id = read(&mut exprs, person, NODE_UUID);
    let title = read(&mut exprs, forum, TITLE);
    let forum_name = read(&mut exprs, forum, NAME);
    let ops = vec![
        scan(person),
        GraphOp::Filter {
            predicate: person_id,
        },
        scan(friend),
        scan(forum),
        GraphOp::Aggregate {
            group_by: vec![forum_ref],
            group_aliases: vec![Some("forum".into())],
            group_vars: vec![Some(forum)],
            aggs: vec![AggExpr {
                func: AggFunc::Collect,
                arg: Some(friend_ref),
                percentile: None,
                alias: "friends".into(),
                out_var: Some(friends),
            }],
        },
        GraphOp::Optional {
            child: Box::new({
                let mut child = ExprArena::new();
                let friend_ref = var(&mut child, friend);
                let list_ref = var(&mut child, friends);
                let predicate = binary(&mut child, BinaryOpKind::In, friend_ref, list_ref);
                plan_of(
                    vec![scan(post), scan(friend), GraphOp::Filter { predicate }],
                    child,
                )
            }),
        },
        GraphOp::Aggregate {
            group_by: vec![forum_ref],
            group_aliases: vec![Some("forum".into())],
            group_vars: vec![Some(forum)],
            aggs: vec![AggExpr {
                func: AggFunc::Count,
                arg: Some(post_ref),
                percentile: None,
                alias: "postCount".into(),
                out_var: None,
            }],
        },
        returning(title),
        returning(forum_name),
        GraphOp::Filter { predicate: member },
    ];
    let demand = node_property_demand(&plan_of(ops, exprs), &names());
    assert_eq!(needed(&demand, forum), vec!["name", "title"]);
    assert_eq!(needed(&demand, friend), Vec::<&str>::new());
    assert_eq!(needed(&demand, post), Vec::<&str>::new());
    assert_eq!(needed(&demand, person), Vec::<&str>::new());
}
