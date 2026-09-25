use super::*;
use crate::tests::empty_write_input;
use datafusion::logical_expr::Extension;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::SessionContext;
use graphforge_core::OntologyMode;
use graphforge_ir::ExprArena;
use graphforge_ir::ExprId;
use graphforge_ir::GraphOp;
use graphforge_ir::GraphPlan;
use graphforge_ir::IrExpr;
use graphforge_ir::IrLiteral;
use graphforge_ir::RuntimeCatalog;
use graphforge_ir::VarId;
use graphforge_plan::GraphCreateNode;
use graphforge_plan::GraphDeleteNode;
use graphforge_plan::GraphRemoveNode;
use graphforge_plan::GraphSetNode;
use graphforge_plan::OptionalMatchNode;
use graphforge_plan::VarLenExpandNode;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;

fn make_session() -> ExecutionSession {
    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    ExecutionSession::new(catalog, None).unwrap()
}

#[test]
fn session_uses_sound_row_estimates_for_partition_planning() {
    let session = make_session();
    assert!(
        session
            .ctx
            .state()
            .config_options()
            .execution
            .use_row_number_estimates_to_optimize_partitioning
    );
}

#[test]
fn persisted_read_detection_recurses_through_every_nested_plan_shape() {
    let scan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(1),
            ty: None,
        })
        .build();
    let empty = GraphPlan::builder("openCypher").build();
    assert!(plan_reads_persisted_data(&scan));
    assert!(!plan_reads_persisted_data(&empty));

    let nested = [
        GraphOp::Optional {
            child: Box::new(scan.clone()),
        },
        GraphOp::Exists {
            child: Box::new(scan.clone()),
            negated: false,
        },
        GraphOp::PatternComprehension {
            child: Box::new(scan.clone()),
            output: VarId(2),
        },
        GraphOp::ListElementPatternComprehension {
            list_expr: ExprId(0),
            loop_var: VarId(3),
            child: Box::new(scan.clone()),
            pattern_output: VarId(4),
            filter: None,
            projection: None,
            output: VarId(5),
        },
        GraphOp::Union {
            all: true,
            inputs: vec![empty.clone(), scan.clone()],
        },
    ];
    for op in nested {
        let plan = GraphPlan::builder("openCypher").push_op(op).build();
        assert!(plan_reads_persisted_data(&plan));
    }
    let union = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Union {
            all: false,
            inputs: vec![empty],
        })
        .build();
    assert!(!plan_reads_persisted_data(&union));
}

#[tokio::test]
async fn query_planner_dispatches_every_write_extension_to_its_physical_exec() {
    let dir = TempDir::new().unwrap();
    let (input, _) = empty_write_input();
    let logical_nodes: Vec<(Arc<dyn UserDefinedLogicalNode>, &str)> = vec![
        (
            Arc::new(GraphCreateNode::new(input.clone(), vec![], vec![])),
            "GraphCreateExec",
        ),
        (
            Arc::new(GraphDeleteNode::new(input.clone(), vec![], false)),
            "GraphDeleteExec",
        ),
        (
            Arc::new(GraphSetNode::new(input.clone(), vec![])),
            "GraphSetExec",
        ),
        (
            Arc::new(GraphRemoveNode::new(input, vec![])),
            "GraphRemoveExec",
        ),
    ];
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new_with_target(
        catalog,
        None,
        dir.path().to_path_buf(),
        OntologyMode::Exploratory,
    )
    .unwrap();
    let state = session.context().state();
    let missing = SessionContext::new().state();
    let readonly = session.restrict_to_reads().context().state();
    let planner = GraphForgeQueryPlanner;

    for (node, expected_name) in logical_nodes {
        let logical = LogicalPlan::Extension(Extension { node });
        let physical = planner
            .create_physical_plan(&logical, &state)
            .await
            .unwrap();
        assert_eq!(physical.name(), expected_name);
        for (state, code) in [
            (&missing, "GF_WRITE_RESOURCE_MISSING"),
            (&readonly, "GF_WRITE_RESOURCE_READ_ONLY"),
        ] {
            let error = planner
                .create_physical_plan(&logical, state)
                .await
                .unwrap_err();
            assert!(error.to_string().contains(code), "{error}");
        }
    }
}

#[tokio::test]
async fn extension_planner_rejects_missing_write_inputs_and_declines_unknown_nodes() {
    let (input, _) = empty_write_input();
    let nodes: Vec<Arc<dyn UserDefinedLogicalNode>> = vec![
        Arc::new(GraphCreateNode::new(input.clone(), vec![], vec![])),
        Arc::new(GraphDeleteNode::new(input.clone(), vec![], false)),
        Arc::new(GraphSetNode::new(input.clone(), vec![])),
        Arc::new(GraphRemoveNode::new(input, vec![])),
    ];
    let state = SessionContext::new().state();
    let physical_planner = DefaultPhysicalPlanner::default();

    for node in nodes {
        let error = GraphForgeExtensionPlanner
            .plan_extension(&physical_planner, node.as_ref(), &[], &[], &state)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("requires one physical input"));
    }

    let unknown = graphforge_plan::GraphMergeNode::new();
    assert!(
        GraphForgeExtensionPlanner
            .plan_extension(&physical_planner, &unknown, &[], &[], &state)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn extension_planner_builds_scan_backed_expand_variants_and_optional_join() {
    let dir = TempDir::new().unwrap();
    let (input, physical) = empty_write_input();
    let expand = graphforge_plan::ExpandNode::new(
        input.clone(),
        "KNOWS",
        1,
        2,
        3,
        graphforge_ir::Direction::Out,
        None,
        vec![],
        vec![],
        vec![],
    );
    let var_len = VarLenExpandNode::new(
        input.clone(),
        "KNOWS",
        1,
        Some(2),
        1,
        2,
        3,
        graphforge_ir::Direction::Out,
        None,
        vec![],
        graphforge_plan::var_len_edge_list_field(&[]),
    );
    let optional = OptionalMatchNode::new(input.clone(), input, vec![], vec![]);
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new_with_target(
        catalog,
        None,
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    )
    .unwrap();
    let state = session.context().state();
    let planner = DefaultPhysicalPlanner::default();

    let unbound = SessionContext::new().state();
    for node in [
        &expand as &dyn UserDefinedLogicalNode,
        &var_len as &dyn UserDefinedLogicalNode,
    ] {
        let error = GraphForgeExtensionPlanner
            .plan_extension(
                &planner,
                node,
                &[],
                std::slice::from_ref(&physical),
                &unbound,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("GF_READ_RESOURCE_MISSING"));
    }

    let physical_expand = GraphForgeExtensionPlanner
        .plan_extension(
            &planner,
            &expand,
            &[],
            std::slice::from_ref(&physical),
            &state,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(physical_expand.name(), "ExpandExec");
    let physical_var_len = GraphForgeExtensionPlanner
        .plan_extension(
            &planner,
            &var_len,
            &[],
            std::slice::from_ref(&physical),
            &state,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(physical_var_len.name(), "VarLenExpandExec");
    let physical_optional = GraphForgeExtensionPlanner
        .plan_extension(
            &planner,
            &optional,
            &[],
            &[physical.clone(), physical.clone()],
            &state,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(physical_optional.name(), "OptionalMatchExec");

    for node in [
        &expand as &dyn UserDefinedLogicalNode,
        &var_len as &dyn UserDefinedLogicalNode,
    ] {
        assert!(
            GraphForgeExtensionPlanner
                .plan_extension(&planner, node, &[], &[], &state)
                .await
                .unwrap_err()
                .to_string()
                .contains("requires one physical input")
        );
    }
    assert!(
        GraphForgeExtensionPlanner
            .plan_extension(&planner, &optional, &[], &[physical], &state)
            .await
            .unwrap_err()
            .to_string()
            .contains("requires two physical inputs")
    );
}

#[tokio::test]
async fn wave11_merge_requires_an_explicit_write_target() {
    let session = make_session();
    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Merge {
            pattern: graphforge_ir::CreatePattern::default(),
            on_create: vec![],
            on_match: vec![],
        })
        .build();
    let error = session.execute_create(&plan).await.unwrap_err();
    assert!(error.to_string().contains("requires a write target"));
}

#[test]
fn deleted_entity_expression_walkers_cover_every_ir_container() {
    let deleted_var = VarId(7);
    let deleted = HashSet::from([deleted_var]);
    let mut arena = ExprArena::new();
    let var = arena.push(IrExpr::VarRef(deleted_var));
    let literal = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let parameter = arena.push(IrExpr::Parameter("value".into()));
    let property = arena.push(IrExpr::PropertyAccess {
        base: var,
        prop: graphforge_value::PropertyId::ontology(graphforge_core::PropId(0)).unwrap(),
    });
    let binary = arena.push(IrExpr::BinaryOp {
        op: graphforge_ir::BinaryOpKind::Add,
        left: literal,
        right: property,
    });
    let unary = arena.push(IrExpr::UnaryOp {
        op: graphforge_ir::UnaryOpKind::Neg,
        expr: property,
    });
    let function = arena.push(IrExpr::FunctionCall {
        name: "properties".into(),
        args: vec![var],
    });
    let type_function = arena.push(IrExpr::FunctionCall {
        name: "type".into(),
        args: vec![var],
    });
    let list = arena.push(IrExpr::ListLiteral(vec![literal, property]));
    let map = arena.push(IrExpr::MapLiteral(vec![("key".into(), property)]));
    let case = arena.push(IrExpr::Case {
        operand: Some(literal),
        arms: vec![graphforge_ir::CaseArm {
            when: literal,
            then: var,
        }],
        else_expr: Some(parameter),
    });
    let quantifier = arena.push(IrExpr::Quantifier {
        kind: graphforge_ir::QuantifierKind::Any,
        loop_var: VarId(8),
        list,
        predicate: var,
    });
    let comprehension = arena.push(IrExpr::ListComprehension {
        loop_var: VarId(9),
        list,
        filter: Some(literal),
        projection: Some(var),
    });

    for id in [
        var,
        property,
        binary,
        unary,
        function,
        list,
        map,
        case,
        quantifier,
        comprehension,
    ] {
        assert!(ExecutionSession::expr_references_vars(&arena, id, &deleted));
    }
    assert!(!ExecutionSession::expr_references_vars(
        &arena, parameter, &deleted
    ));
    assert!(ExecutionSession::expr_accesses_deleted_vars(
        &arena, property, &deleted
    ));
    assert!(ExecutionSession::expr_accesses_deleted_vars(
        &arena, function, &deleted
    ));
    assert!(ExecutionSession::expr_accesses_deleted_vars(
        &arena, binary, &deleted
    ));
    assert!(ExecutionSession::expr_accesses_deleted_vars(
        &arena, unary, &deleted
    ));
    assert!(ExecutionSession::expr_accesses_deleted_vars(
        &arena, list, &deleted
    ));
    assert!(ExecutionSession::expr_accesses_deleted_vars(
        &arena, map, &deleted
    ));
    assert!(!ExecutionSession::expr_accesses_deleted_vars(
        &arena,
        type_function,
        &deleted
    ));
}

#[test]
fn execution_session_constructs_without_panic() {
    let _session = make_session();
}

#[tokio::test]
async fn execute_plan_empty_plan_yields_unit_row() {
    // A plan with no source op lowers over the single "unit" row of
    // relational algebra (so `RETURN 1` / `UNWIND [..]` produce output);
    // an entirely empty plan therefore executes to one zero-column row.
    let session = make_session();
    let plan = GraphPlan::builder("openCypher").build();
    let result = session.execute_plan(&plan).await.expect("execute_plan");
    assert_eq!(result.stats.rows_produced, 1);
}

#[tokio::test]
async fn row_count_expressions_resolve_inside_union_inputs() {
    let session = make_session();
    let mut child = GraphPlan::builder("openCypher");
    let one = child.push_expr(IrExpr::Literal(graphforge_ir::IrLiteral::Int(1)));
    let two = child.push_expr(IrExpr::BinaryOp {
        op: graphforge_ir::BinaryOpKind::Add,
        left: one,
        right: one,
    });
    child.push_op_mut(GraphOp::SkipExpr { expr: two });
    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Union {
            all: true,
            inputs: vec![child.build()],
        })
        .build();

    let resolved = session
        .resolve_row_count_expressions(&plan, &HashMap::new())
        .await
        .expect("resolve nested row-count expression");
    let [GraphOp::Union { inputs, .. }] = resolved.ops.as_slice() else {
        panic!("expected union");
    };
    assert!(matches!(
        inputs[0].ops.as_slice(),
        [GraphOp::Skip { count: 2 }]
    ));
}

#[tokio::test]
async fn public_write_wrappers_preserve_no_target_and_empty_plan_errors() {
    let session = make_session();
    let plan = GraphPlan::builder("openCypher").build();
    let params = HashMap::from([("value".to_owned(), graphforge_ir::IrLiteral::Int(1))]);

    for result in [
        session.execute_delete(&plan).await,
        session.execute_delete_with_params(&plan, &params).await,
        session.execute_set(&plan).await,
        session.execute_set_with_params(&plan, &params).await,
        session.execute_remove(&plan).await,
        session.execute_remove_with_params(&plan, &params).await,
    ] {
        let error = result.expect_err("write wrappers require a write target");
        assert!(
            error.to_string().contains("write target"),
            "unexpected error: {error}"
        );
    }
}

#[tokio::test]
async fn public_create_set_remove_params_persist_across_session_reopen() {
    use graphforge_ir::{
        CreateNodeSpec, CreatePattern, LabelItem, RemovePropItem, SetMapItem, SetPropItem,
    };

    let dir = TempDir::new().unwrap();
    let open_session = || {
        let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
        ExecutionSession::new_with_target(
            catalog,
            None,
            dir.path().to_path_buf(),
            OntologyMode::Exploratory,
        )
        .unwrap()
    };

    let create = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Create {
            pattern: CreatePattern {
                nodes: vec![CreateNodeSpec {
                    var: VarId(0),
                    labels: vec![],
                    properties: None,
                    is_reference: false,
                }],
                edges: vec![],
            },
        })
        .build();
    let session = open_session();
    let created = session.execute_create(&create).await.unwrap();
    assert_eq!(created.side_effects.unwrap().nodes_created, 1);
    drop(session);

    let mut map_set = GraphPlan::builder("openCypher");
    let name = map_set.push_expr(IrExpr::Literal(IrLiteral::Str("Ada".into())));
    let active = map_set.push_expr(IrExpr::Literal(IrLiteral::Bool(true)));
    let map = map_set.push_expr(IrExpr::MapLiteral(vec![
        ("name".into(), name),
        ("active".into(), active),
    ]));
    let map_set = map_set
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Set {
            items: vec![],
            map_items: vec![SetMapItem {
                target: VarId(0),
                map,
                replace: false,
            }],
            label_items: vec![LabelItem {
                target: VarId(0),
                labels: vec![graphforge_value::EntityTypeId::decode(7).unwrap()],
            }],
        })
        .build();
    let session = open_session();
    let map_result = session.execute_set(&map_set).await.unwrap();
    let map_effects = map_result.side_effects.unwrap();
    assert_eq!(map_effects.properties_set, 2);
    assert_eq!(map_effects.labels_added, 1);
    drop(session);

    let mut set = GraphPlan::builder("openCypher");
    let score = set.push_expr(IrExpr::Parameter("score".into()));
    let set = set
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Set {
            items: vec![SetPropItem {
                target: VarId(0),
                prop: graphforge_value::PropertyId::ontology(graphforge_core::PropId(0)).unwrap(),
                prop_name: "score".into(),
                value: score,
            }],
            map_items: vec![],
            label_items: vec![],
        })
        .build();
    let session = open_session();
    let set_result = session
        .execute_set_with_params(&set, &HashMap::from([("score".into(), IrLiteral::Int(42))]))
        .await
        .unwrap();
    assert_eq!(set_result.side_effects.unwrap().properties_set, 1);
    drop(session);

    let remove = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Remove {
            items: vec![RemovePropItem {
                target: VarId(0),
                prop: graphforge_value::PropertyId::ontology(graphforge_core::PropId(0)).unwrap(),
                prop_name: "score".into(),
            }],
            label_items: vec![LabelItem {
                target: VarId(0),
                labels: vec![graphforge_value::EntityTypeId::decode(7).unwrap()],
            }],
        })
        .build();
    let session = open_session();
    let removed = session.execute_remove(&remove).await.unwrap();
    let removed = removed.side_effects.unwrap();
    assert_eq!(removed.properties_removed, 1);
    assert_eq!(removed.labels_removed, 1);
}

#[test]
fn context_exposes_graph_catalog() {
    let session = make_session();
    assert!(
        session.context().catalog("graph").is_some(),
        "graph catalog should be registered"
    );
}

#[test]
fn graph_schema_has_topology_nodes_table() {
    let session = make_session();
    let catalog = session.context().catalog("graph").unwrap();
    let schema = catalog.schema("graph").unwrap();
    assert!(
        schema.table_exist("topology_nodes"),
        "topology_nodes table should be registered"
    );
}

/// Two `FixedSizeBinary(16)` sort keys, the shape of `a.node_uuid, b.node_uuid`,
/// in `batch_rows`-row input batches: one constant (a star hub) and one
/// pseudo-random (its leaves).
fn uuid_pair_batches(rows: usize, batch_rows: usize) -> Vec<arrow::record_batch::RecordBatch> {
    use arrow::array::FixedSizeBinaryArray;
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::FixedSizeBinary(16), false),
        Field::new("d", DataType::FixedSizeBinary(16), false),
    ]));
    (0..rows)
        .step_by(batch_rows)
        .map(|start| {
            let end = (start + batch_rows).min(rows);
            let hub = FixedSizeBinaryArray::try_from_iter((start..end).map(|_| [7_u8; 16]))
                .expect("hub keys");
            let leaves = FixedSizeBinaryArray::try_from_iter((start..end).map(|row| {
                let mut key = [0_u8; 16];
                let mixed = (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                key[..8].copy_from_slice(&mixed.to_be_bytes());
                key[8..].copy_from_slice(&(row as u64).to_be_bytes());
                key
            }))
            .expect("leaf keys");
            arrow::record_batch::RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(hub), Arc::new(leaves)],
            )
            .expect("uuid pair batch")
        })
        .collect()
}

fn sort_spill_count(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> usize {
    let own = if plan
        .downcast_ref::<datafusion::physical_plan::sorts::sort::SortExec>()
        .is_some()
    {
        plan.metrics()
            .and_then(|metrics| metrics.spill_count())
            .unwrap_or(0)
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(sort_spill_count)
        .sum::<usize>()
}

/// #1591: a full `ORDER BY` whose input exceeds the query memory budget must
/// spill and complete. Before the fix every buffered input batch was a single
/// merge chunk, so merging them for the first spill needed the buffered data
/// plus its row-format keys (74 bytes a row against 64 reserved) and failed
/// with `Resources exhausted ... ExternalSorterMerge` once the budget was large
/// enough that the fixed 10 MiB merge headroom no longer covered the excess
/// (about 70 MiB). 128 MiB is past that point; 2.5M rows reserve 160 MiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_spills_when_input_exceeds_query_memory_budget() {
    use arrow::array::FixedSizeBinaryArray;
    use datafusion::datasource::MemTable;

    const ROWS: usize = 2_500_000;
    let spill = TempDir::new().unwrap();
    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let resources = SessionResourceConfig {
        target_partitions: 1,
        memory_budget_bytes: 128 * 1024 * 1024,
        spill_enabled: true,
        spill_directory: Some(spill.path().to_path_buf()),
        ..SessionResourceConfig::default()
    };
    let session = ExecutionSession::build(
        catalog,
        None,
        PathBuf::new(),
        OntologyMode::Exploratory,
        None,
        OrdinalIdentityConfig::default(),
        &resources,
    );
    let batches = uuid_pair_batches(ROWS, resources.batch_size);
    let table = MemTable::try_new(batches[0].schema(), vec![batches]).unwrap();
    session.ctx.register_table("t", Arc::new(table)).unwrap();

    let plan = session
        .ctx
        .sql("SELECT s, d FROM t ORDER BY s, d")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let sorted = datafusion::physical_plan::collect(Arc::clone(&plan), session.ctx.task_ctx())
        .await
        .expect("ORDER BY over the budget must spill, not exhaust the pool");

    assert_eq!(
        sorted
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        ROWS
    );
    assert!(
        sort_spill_count(&plan) > 0,
        "the input exceeds the budget, so the sort must have spilled"
    );
    let mut previous: Option<Vec<u8>> = None;
    for batch in &sorted {
        let leaves = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = leaves.value(row).to_vec();
            assert!(
                previous.as_ref().is_none_or(|p| *p < key),
                "keys out of order"
            );
            previous = Some(key);
        }
    }
}

/// Every full sort a Cypher query plans gets coalesced input runs; top-k sorts
/// (`LIMIT`) do not, because they never spill and the ordered fast paths match
/// their shape.
#[tokio::test]
async fn cypher_full_sorts_get_coalesced_input_runs() {
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(std::sync::Mutex::new(RuntimeCatalog::new()));
    let bind = |query: &str| {
        let ast = graphforge_cypher::parse(query).expect("parse");
        graphforge_ir::Binder::new(None, Arc::clone(&rc), OntologyMode::Exploratory)
            .bind(&ast)
            .expect("bind")
    };
    let session = |dir: &std::path::Path| {
        let catalog = GraphCatalog::open(dir, None, &rc.lock().unwrap()).unwrap();
        ExecutionSession::new_with_target(
            catalog,
            None,
            dir.to_path_buf(),
            OntologyMode::Exploratory,
        )
        .unwrap()
    };
    let create = bind("CREATE (:N {k: 2})-[:E]->(:N {k: 1}), (:N {k: 3})-[:E]->(:N {k: 0})");
    session(dir.path())
        .execute_write_statement(&create)
        .await
        .unwrap();

    let full = bind("MATCH (a)-[r]->(b) RETURN a.k AS s, b.k AS d ORDER BY s, d");
    let explained = session(dir.path()).explain_physical(&full).await.unwrap();
    let sort = explained
        .lines()
        .position(|line| line.contains("SortExec"))
        .unwrap_or_else(|| panic!("full ORDER BY plans a SortExec:\n{explained}"));
    assert!(
        explained
            .lines()
            .nth(sort + 1)
            .is_some_and(|line| line.contains("SortRunCoalesceExec")),
        "the sort's input must be coalesced into runs:\n{explained}"
    );
    let result = session(dir.path()).execute_plan(&full).await.unwrap();
    let rows: Vec<String> = result
        .batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                (0..batch.num_columns())
                    .map(|col| {
                        arrow::util::display::array_value_to_string(batch.column(col), row).unwrap()
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
        })
        .collect();
    assert_eq!(rows, ["2,1", "3,0"]);

    let top_k = bind("MATCH (a)-[r]->(b) RETURN a.k AS s ORDER BY s LIMIT 1");
    let explained = session(dir.path()).explain_physical(&top_k).await.unwrap();
    assert!(
        !explained.contains("SortRunCoalesceExec"),
        "top-k sorts keep their input unchanged:\n{explained}"
    );
}

#[test]
fn sort_run_target_scales_with_the_partition_share_within_bounds() {
    use crate::sort_runs::sort_run_bytes;
    const MIB: usize = 1024 * 1024;
    assert_eq!(sort_run_bytes(512 * MIB, 8), 4 * MIB);
    assert_eq!(sort_run_bytes(512 * MIB, 1), 32 * MIB);
    assert_eq!(sort_run_bytes(16 * MIB, 8), MIB);
    assert_eq!(sort_run_bytes(usize::MAX, 1), 64 * MIB);
    assert_eq!(sort_run_bytes(512 * MIB, 0), 32 * MIB);
}
