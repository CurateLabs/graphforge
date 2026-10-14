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

// ---------------------------------------------------------------------------
// Late columns: read only by the final projection, after the Limit.
// ---------------------------------------------------------------------------

const OTHER: u32 = 5;
const LIMIT: GraphOp = GraphOp::Limit { count: 20 };

fn names_with_other() -> HashMap<PropertyId, String> {
    let mut names = names();
    names.insert(property(OTHER), "other".to_owned());
    names
}

fn projecting(exprs: Vec<ExprId>) -> GraphOp {
    GraphOp::Project {
        items: exprs
            .into_iter()
            .enumerate()
            .map(|(index, expr)| ProjectItem {
                expr,
                alias: Some(format!("c{index}")),
                out_var: None,
            })
            .collect(),
        distinct: false,
    }
}

fn sorted_by(expr: ExprId) -> GraphOp {
    GraphOp::Sort {
        keys: vec![SortKey {
            expr,
            order: SortOrder::Asc,
            nulls_first: false,
        }],
    }
}

fn filtering(predicate: ExprId) -> GraphOp {
    GraphOp::Filter { predicate }
}

/// `(early, late)` columns of `v` out of `name`, `title`, `other`.
fn split(demand: &NodePropertyDemand, v: VarId) -> (Vec<&'static str>, Vec<&'static str>) {
    let pick = |test: fn(&NodePropertyDemand, VarId, &str) -> bool| {
        ["name", "title", "other"]
            .into_iter()
            .filter(|column| test(demand, v, column))
            .collect::<Vec<_>>()
    };
    (
        pick(NodePropertyDemand::needs_early),
        pick(NodePropertyDemand::needs_late),
    )
}

fn deferred(ops: Vec<GraphOp>, exprs: ExprArena) -> NodePropertyDemand {
    node_property_demand(&plan_of(ops, exprs), &names_with_other())
}

#[test]
fn a_column_only_the_final_projection_reads_is_late() {
    // MATCH (n), (m) ORDER BY n.name RETURN n.name, n.title, m.name LIMIT 20
    let mut exprs = ExprArena::new();
    let sort_name = read(&mut exprs, N, NAME);
    let (n_name, n_title, m_name) = (
        read(&mut exprs, N, NAME),
        read(&mut exprs, N, TITLE),
        read(&mut exprs, M, NAME),
    );
    let ops = vec![
        scan(N),
        scan(M),
        sorted_by(sort_name),
        projecting(vec![n_name, n_title, m_name]),
        LIMIT,
    ];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, N), (vec!["name"], vec!["title"]));
    assert_eq!(split(&demand, M), (vec![], vec!["name"]));
    assert!(demand.defers(N) && demand.defers(M));
    assert_eq!(demand.deferred_projection(), Some(3));
    // `needs` is the union, so a lowerer that ignores the split reads everything.
    assert_eq!(needed(&demand, N), vec!["name", "title"]);
}

#[test]
fn a_column_a_filter_also_reads_is_early() {
    // MATCH (n) WHERE n.title = n.name RETURN n.title LIMIT 20
    let mut exprs = ExprArena::new();
    let (a, b) = (read(&mut exprs, N, TITLE), read(&mut exprs, N, NAME));
    let predicate = binary(&mut exprs, BinaryOpKind::Eq, a, b);
    let title = read(&mut exprs, N, TITLE);
    let ops = vec![
        scan(N),
        filtering(predicate),
        projecting(vec![title]),
        LIMIT,
    ];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, N), (vec!["name", "title"], vec![]));
    assert!(!demand.defers(N));
    assert_eq!(demand.deferred_projection(), None);
}

#[test]
fn a_column_a_sort_key_reads_through_a_function_is_early() {
    // MATCH (n) RETURN n.name, n.title ORDER BY toInteger(n.name) LIMIT 20
    let mut exprs = ExprArena::new();
    let key = read(&mut exprs, N, NAME);
    let key = call(&mut exprs, "toInteger", vec![key]);
    let (name, title) = (read(&mut exprs, N, NAME), read(&mut exprs, N, TITLE));
    let ops = vec![
        scan(N),
        sorted_by(key),
        projecting(vec![name, title]),
        LIMIT,
    ];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, N), (vec!["name"], vec!["title"]));
}

#[test]
fn a_skip_before_the_limit_keeps_the_deferral() {
    let mut exprs = ExprArena::new();
    let title = read(&mut exprs, N, TITLE);
    let ops = vec![
        scan(N),
        projecting(vec![title]),
        GraphOp::Skip { count: 3 },
        LIMIT,
    ];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, N), (vec![], vec!["title"]));
    assert_eq!(demand.deferred_projection(), Some(1));
}

#[test]
fn a_coalesce_over_late_columns_stays_late() {
    // RETURN coalesce(n.name, n.title) LIMIT 20
    let mut exprs = ExprArena::new();
    let (a, b) = (read(&mut exprs, N, NAME), read(&mut exprs, N, TITLE));
    let value = call(&mut exprs, "coalesce", vec![a, b]);
    let ops = vec![scan(N), projecting(vec![value]), LIMIT];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, N), (vec![], vec!["name", "title"]));
}

#[test]
fn a_node_the_projection_returns_whole_has_no_late_column() {
    // RETURN n, n.name LIMIT 20
    let mut exprs = ExprArena::new();
    let (whole, name) = (var(&mut exprs, N), read(&mut exprs, N, NAME));
    let ops = vec![scan(N), projecting(vec![whole, name]), LIMIT];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, N), (vec!["name", "title", "other"], vec![]));
    assert_eq!(demand.deferred_projection(), None);
}

#[test]
fn a_node_a_sort_observes_whole_has_no_late_column() {
    // ORDER BY n RETURN n.name LIMIT 20
    let mut exprs = ExprArena::new();
    let key = var(&mut exprs, N);
    let name = read(&mut exprs, N, NAME);
    let ops = vec![scan(N), sorted_by(key), projecting(vec![name]), LIMIT];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, N), (vec!["name", "title", "other"], vec![]));
    assert_eq!(demand.deferred_projection(), None);
}

#[test]
fn a_property_the_catalog_cannot_name_is_not_late() {
    // RETURN n.<unresolved>, n.name LIMIT 20
    let mut exprs = ExprArena::new();
    let unresolved = read(&mut exprs, N, UNRESOLVED);
    let name = read(&mut exprs, N, NAME);
    let ops = vec![scan(N), projecting(vec![unresolved, name]), LIMIT];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, N), (vec!["name", "title", "other"], vec![]));
    assert_eq!(demand.deferred_projection(), None);
}

/// Plans whose final projection must be read at bind time: no `LIMIT`, a
/// `LIMIT` the planner cannot count, a `DISTINCT` projection, or an operator
/// between the scan and the projection that reads, groups or renames the node.
#[test]
fn a_plan_that_cannot_defer_reads_every_column_at_bind_time() {
    type Build = Box<dyn FnOnce(&mut ExprArena) -> Vec<GraphOp>>;
    let projected = |exprs: &mut ExprArena| read(exprs, N, TITLE);
    let cases: Vec<(&str, Build)> = vec![
        (
            "no LIMIT",
            Box::new(move |exprs| vec![scan(N), projecting(vec![projected(exprs)])]),
        ),
        (
            "SKIP without LIMIT",
            Box::new(move |exprs| {
                vec![
                    scan(N),
                    projecting(vec![projected(exprs)]),
                    GraphOp::Skip { count: 1 },
                ]
            }),
        ),
        (
            "LIMIT $param",
            Box::new(move |exprs| {
                vec![
                    scan(N),
                    projecting(vec![projected(exprs)]),
                    GraphOp::LimitParam {
                        name: "limit".into(),
                    },
                ]
            }),
        ),
        (
            "LIMIT expression",
            Box::new(move |exprs| {
                let count = exprs.push(IrExpr::Literal(IrLiteral::Int(3)));
                vec![
                    scan(N),
                    projecting(vec![projected(exprs)]),
                    GraphOp::LimitExpr { expr: count },
                ]
            }),
        ),
        (
            "SKIP $param before LIMIT",
            Box::new(move |exprs| {
                vec![
                    scan(N),
                    projecting(vec![projected(exprs)]),
                    GraphOp::SkipParam {
                        name: "skip".into(),
                    },
                    LIMIT,
                ]
            }),
        ),
        (
            "DISTINCT projection",
            Box::new(move |exprs| {
                let title = projected(exprs);
                vec![
                    scan(N),
                    GraphOp::Project {
                        items: vec![ProjectItem {
                            expr: title,
                            alias: Some("t".into()),
                            out_var: None,
                        }],
                        distinct: true,
                    },
                    LIMIT,
                ]
            }),
        ),
        (
            "aggregate below the projection",
            Box::new(move |exprs| {
                let n = var(exprs, N);
                vec![
                    scan(N),
                    aggregate(AggFunc::Count, Some(n)),
                    projecting(vec![projected(exprs)]),
                    LIMIT,
                ]
            }),
        ),
        (
            "WITH below the projection",
            Box::new(move |exprs| {
                let n = var(exprs, N);
                vec![
                    scan(N),
                    keeping(n, None),
                    projecting(vec![projected(exprs)]),
                    LIMIT,
                ]
            }),
        ),
        (
            "rename",
            Box::new(move |exprs| {
                let n = var(exprs, N);
                let title = read(exprs, M, TITLE);
                vec![
                    scan(N),
                    keeping(n, Some(M)),
                    scan(M),
                    projecting(vec![title]),
                    LIMIT,
                ]
            }),
        ),
        (
            "scanned twice",
            Box::new(move |exprs| {
                vec![
                    scan(N),
                    scan(N),
                    projecting(vec![projected(exprs)]),
                    LIMIT,
                ]
            }),
        ),
        (
            "scanned again in an OPTIONAL MATCH",
            Box::new(move |exprs| {
                vec![
                    scan(N),
                    GraphOp::Optional {
                        child: Box::new(plan_of(vec![scan(N)], ExprArena::new())),
                    },
                    projecting(vec![projected(exprs)]),
                    LIMIT,
                ]
            }),
        ),
    ];
    for (case, build) in cases {
        let mut exprs = ExprArena::new();
        let ops = build(&mut exprs);
        let demand = deferred(ops, exprs);
        assert!(!demand.defers(N), "{case}");
        assert_eq!(demand.deferred_projection(), None, "{case}");
        assert!(!demand.needs_late(N, "title"), "{case}");
    }
}

#[test]
fn a_node_scanned_after_an_unwind_defers_while_the_collected_one_does_not() {
    // MATCH (n) WITH collect(n) AS ns UNWIND ns AS m MATCH (m) RETURN n.title?, m.title LIMIT 20
    // The collect holds the node whole, so `n` reads every column; the
    // unwound alias is a fresh scan that only the projection reads.
    let mut exprs = ExprArena::new();
    let list = VarId(3);
    let node = var(&mut exprs, N);
    let ns = var(&mut exprs, list);
    let m_title = read(&mut exprs, M, TITLE);
    let ops = vec![
        scan(N),
        GraphOp::Aggregate {
            group_by: Vec::new(),
            group_aliases: Vec::new(),
            group_vars: Vec::new(),
            aggs: vec![AggExpr {
                func: AggFunc::Collect,
                arg: Some(node),
                percentile: None,
                alias: "ns".into(),
                out_var: Some(list),
            }],
        },
        GraphOp::Unwind {
            list_expr: ns,
            alias: M,
        },
        scan(M),
        projecting(vec![m_title]),
        LIMIT,
    ];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, M), (vec![], vec!["title"]));
    assert!(!demand.defers(N));
    assert_eq!(demand.deferred_projection(), Some(4));
}

#[test]
fn a_later_use_of_the_node_in_an_expansion_does_not_make_its_columns_early() {
    // Expansions and relationship uniqueness carry identity, not properties.
    let mut exprs = ExprArena::new();
    let title = read(&mut exprs, M, TITLE);
    let ops = vec![
        scan(N),
        scan(M),
        GraphOp::RelationshipUnique {
            edge: VarId(7),
            prior_edges: Vec::new(),
        },
        projecting(vec![title]),
        LIMIT,
    ];
    let demand = deferred(ops, exprs);
    assert_eq!(split(&demand, M), (vec![], vec!["title"]));
}

#[test]
fn an_unmodelled_operator_defeats_the_deferral() {
    // A procedure call (or any operator the analysis does not model) can read
    // any node whole.
    let mut exprs = ExprArena::new();
    let title = read(&mut exprs, N, TITLE);
    let ops = vec![
        scan(N),
        GraphOp::Delete {
            vars: vec![N],
            exprs: Vec::new(),
            detach: false,
        },
        projecting(vec![title]),
        LIMIT,
    ];
    let demand = deferred(ops, exprs);
    assert_eq!(demand.deferred_projection(), None);
    assert!(demand.needs_early(N, "title"));
    assert!(!demand.needs_late(N, "title"));
}

#[test]
fn everything_has_no_late_column() {
    let demand = NodePropertyDemand::everything();
    assert!(demand.needs_early(N, "title"));
    assert!(!demand.needs_late(N, "title"));
    assert!(!demand.defers(N));
    assert_eq!(demand.deferred_projection(), None);
}

// ---------------------------------------------------------------------------
// The LDBC interactive shapes, bound from Cypher.
// ---------------------------------------------------------------------------

mod interactive_shapes {
    use super::*;
    use crate::{Binder, OntologyMode, RuntimeCatalog};
    use std::sync::{Arc, Mutex};

    const COLUMNS: [&str; 9] = [
        "id",
        "firstName",
        "lastName",
        "content",
        "imageFile",
        "creationDate",
        "name",
        "workFrom",
        "title",
    ];

    struct Bound {
        plan: GraphPlan,
        demand: NodePropertyDemand,
    }

    fn bind(query: &str) -> Bound {
        let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
        let binder = Binder::new(None, Arc::clone(&catalog), OntologyMode::Exploratory);
        let ast = graphforge_cypher::parse(query).expect("query parses");
        let plan = binder.bind(&ast).expect("query binds");
        let names: HashMap<PropertyId, String> = catalog
            .lock()
            .unwrap()
            .property_names()
            .map(|(id, name)| (PropertyId::runtime(id), name.to_owned()))
            .collect();
        let demand = node_property_demand(&plan, &names);
        Bound { plan, demand }
    }

    /// The variable of the `index`th top-level node scan.
    fn scanned(plan: &GraphPlan, index: usize) -> VarId {
        plan.ops
            .iter()
            .filter_map(|op| match op {
                GraphOp::NodeScan { var, .. } => Some(*var),
                _ => None,
            })
            .nth(index)
            .expect("node scan")
    }

    fn early(demand: &NodePropertyDemand, v: VarId) -> Vec<&'static str> {
        COLUMNS
            .into_iter()
            .filter(|column| demand.needs_early(v, column))
            .collect()
    }

    fn late(demand: &NodePropertyDemand, v: VarId) -> Vec<&'static str> {
        COLUMNS
            .into_iter()
            .filter(|column| demand.needs_late(v, column))
            .collect()
    }

    #[test]
    fn friend_names_and_message_text_are_late_for_the_recent_messages_query() {
        let Bound { plan, demand } = bind(
            "MATCH (:Person {id: 17592186052613})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message) \
             WHERE (message:Post OR message:Comment) AND message.creationDate <= 1354060800000 \
             RETURN friend.id AS personId, friend.firstName AS personFirstName, \
             friend.lastName AS personLastName, message.id AS postOrCommentId, \
             coalesce(message.content, message.imageFile) AS postOrCommentContent, \
             message.creationDate AS postOrCommentCreationDate \
             ORDER BY postOrCommentCreationDate DESC, toInteger(postOrCommentId) ASC LIMIT 20",
        );
        let (person, friend, message) = (
            scanned(&plan, 0),
            scanned(&plan, 1),
            scanned(&plan, 2),
        );
        // The start person is filtered by id: nothing left to defer.
        assert_eq!(late(&demand, person), Vec::<&str>::new());
        // The friend's columns are all returned and none is a sort key.
        assert_eq!(late(&demand, friend), vec!["id", "firstName", "lastName"]);
        assert_eq!(early(&demand, friend), Vec::<&str>::new());
        // The message id and creation date are filter or sort keys.
        assert_eq!(early(&demand, message), vec!["id", "creationDate"]);
        assert_eq!(late(&demand, message), vec!["content", "imageFile"]);
        assert_eq!(demand.deferred_projection(), Some(plan.ops.len() - 2));
    }

    #[test]
    fn a_collected_and_unwound_friend_defers_its_columns() {
        let Bound { plan, demand } = bind(
            "MATCH (root:Person {id: 13194139542834})-[:KNOWS*1..2]-(friend:Person) \
             WHERE NOT friend = root WITH collect(DISTINCT friend) AS friends \
             UNWIND friends AS friend MATCH (friend)<-[:HAS_CREATOR]-(message) \
             WHERE (message:Post OR message:Comment) AND message.creationDate < 1324080000000 \
             RETURN friend.id AS personId, friend.firstName AS personFirstName, \
             friend.lastName AS personLastName, message.id AS commentOrPostId, \
             coalesce(message.content, message.imageFile) AS commentOrPostContent, \
             message.creationDate AS commentOrPostCreationDate \
             ORDER BY commentOrPostCreationDate DESC, commentOrPostId ASC LIMIT 20",
        );
        // The friend the collect holds is read at bind time, as it always was.
        let collected = scanned(&plan, 1);
        assert!(!demand.defers(collected));
        // The unwound friend, matched again, is read only by the projection.
        let unwound = scanned(&plan, 2);
        assert_eq!(late(&demand, unwound), vec!["id", "firstName", "lastName"]);
        assert_eq!(early(&demand, unwound), Vec::<&str>::new());
        let message = scanned(&plan, 3);
        assert_eq!(early(&demand, message), vec!["id", "creationDate"]);
        assert_eq!(late(&demand, message), vec!["content", "imageFile"]);
    }

    #[test]
    fn a_sort_key_column_stays_early_for_the_recent_employment_query() {
        let Bound { plan, demand } = bind(
            "MATCH (person:Person {id: 30786325583618})-[:KNOWS*1..2]-(friend:Person) \
             WHERE NOT(person = friend) WITH DISTINCT friend \
             MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(:Country {name: 'Laos'}) \
             WHERE workAt.workFrom < 2010 \
             RETURN friend.id AS personId, friend.firstName AS personFirstName, \
             friend.lastName AS personLastName, company.name AS organizationName, \
             workAt.workFrom AS organizationWorkFromYear \
             ORDER BY organizationWorkFromYear ASC, toInteger(personId) ASC, organizationName DESC LIMIT 10",
        );
        let company = scanned(&plan, 3);
        // `company.name` is an ORDER BY key, so it is read before the Limit.
        assert_eq!(early(&demand, company), vec!["name"]);
        assert_eq!(late(&demand, company), Vec::<&str>::new());
        // The friend passes through `WITH DISTINCT`, which shares its demand
        // across the alias, so none of its columns is deferred.
        for index in [1, 2] {
            let friend = scanned(&plan, index);
            assert_eq!(late(&demand, friend), Vec::<&str>::new());
        }
    }

    #[test]
    fn a_plan_that_orders_an_aggregate_defers_nothing() {
        let Bound { plan, demand } = bind(
            "MATCH (person:Person {id: 1})-[:KNOWS*1..2]-(friend) WHERE NOT person = friend \
             WITH DISTINCT friend MATCH (friend)<-[membership:HAS_MEMBER]-(forum) \
             WHERE membership.joinDate > 5 WITH forum, collect(friend) AS friends \
             OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) \
             WHERE friend IN friends WITH forum, count(post) AS postCount \
             RETURN forum.title AS forumName, postCount ORDER BY postCount DESC, forum.id ASC LIMIT 20",
        );
        assert_eq!(demand.deferred_projection(), None);
        for index in 0..4 {
            assert_eq!(late(&demand, scanned(&plan, index)), Vec::<&str>::new());
        }
    }
}
