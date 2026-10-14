//! Joins keyed off a field of a struct column must optimize and keep their schemas.
//!
//! `UNWIND [{a: 1, b: 2}] AS e MATCH (a:N {id: e.a}), (b:N {id: e.b})` lowers to
//! cross joins under filters on `get_field(e, 'a')`. DataFusion turns those into
//! joins whose condition reads the struct field, and its leaf-extraction pair then
//! pushes a projection of the field below the lower join. That rewrites the lower
//! join's schema while the upper join keeps the schema it had before, so
//! `optimize_projections` splits the upper join's required columns at the wrong
//! offset and prunes a column the query reads (#2084).
//!
//! The vendored `datafusion-optimizer` refreshes the parent schema after a child
//! is rewritten (see `vendor/datafusion-optimizer/GRAPHFORGE.md`). These tests
//! check both halves of that contract on plans with the lowerer's shapes: the
//! optimization succeeds with the right output columns, and no node of the plan
//! keeps a schema that differs from the one its own inputs produce.
//!
//! The end-to-end `unwind_inline_map_anchors` test in `graphforge-api` runs the
//! same shapes against real catalogs.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::functions::core::expr_fn::{get_field, named_struct};
use datafusion::functions_aggregate::count::count;
use datafusion::functions_nested::expr_fn::{array_has, make_array};
use datafusion::logical_expr::logical_plan::LogicalTableSource;
use datafusion::logical_expr::{
    Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder, col, lit,
};
use datafusion::optimizer::{Optimizer, OptimizerContext, OptimizerRule};
use graphforge_rel::input_predicates::optimizer_rules;

type Rules = Vec<Arc<dyn OptimizerRule + Send + Sync>>;

fn source(fields: Vec<Field>) -> Arc<LogicalTableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(Schema::new(fields))))
}

/// A node scan the way the lowerer builds one: the typed node table left-joined to
/// its properties and projected under the pattern variable's qualifier.
fn node(variable: &str) -> LogicalPlan {
    let nodes = LogicalPlanBuilder::scan(
        variable,
        source(vec![
            Field::new("node_uuid", DataType::Utf8, false),
            Field::new("node_id", DataType::UInt64, false),
            Field::new("type_id", DataType::UInt32, false),
            Field::new(
                "type_ids",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                false,
            ),
            Field::new("created_at", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, false),
        ]),
        None,
    )
    .unwrap()
    .filter(array_has(col(format!("{variable}.type_ids")), lit(1u32)))
    .unwrap()
    .build()
    .unwrap();
    let properties_name = format!("{variable}__props");
    let properties = LogicalPlanBuilder::scan(
        properties_name.clone(),
        source(vec![
            Field::new("node_uuid", DataType::Utf8, false),
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let joined = LogicalPlanBuilder::from(nodes)
        .join_with_expr_keys(
            properties,
            JoinType::Left,
            (
                vec![col(format!("{variable}.node_uuid"))],
                vec![col(format!("{properties_name}.node_uuid"))],
            ),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
    let mut columns: Vec<Expr> = [
        "node_uuid",
        "node_id",
        "type_id",
        "type_ids",
        "created_at",
        "updated_at",
    ]
    .iter()
    .map(|name| col(format!("{variable}.{name}")))
    .collect();
    for name in ["id", "name"] {
        columns
            .push(col(format!("{properties_name}.{name}")).alias_qualified(Some(variable), name));
    }
    LogicalPlanBuilder::from(joined)
        .project(columns)
        .unwrap()
        .build()
        .unwrap()
}

fn map_row() -> Expr {
    named_struct(vec![lit("a"), lit(1i64), lit("b"), lit(2i64)])
}

/// One row holding the map in `var_0`, from a projection over an empty relation.
fn rows_from_projection() -> LogicalPlan {
    LogicalPlanBuilder::empty(true)
        .project(vec![map_row().alias("var_0")])
        .unwrap()
        .build()
        .unwrap()
}

/// One row holding the map in `var_0`, from the planner's `UnwindNode`.
fn rows_from_unwind() -> LogicalPlan {
    let map_type = DataType::Struct(Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let unwind = graphforge_plan::UnwindNode::new(
        Arc::new(LogicalPlanBuilder::empty(true).build().unwrap()),
        make_array(vec![map_row()]),
        "var_0",
        &Field::new("var_0", map_type, true),
    );
    LogicalPlan::Extension(Extension {
        node: Arc::new(unwind),
    })
}

/// Both patterns joined on a `get_field` key, as DataFusion forms the joins.
fn keyed_joins(rows: LogicalPlan) -> LogicalPlan {
    let first = LogicalPlanBuilder::from(rows)
        .join_with_expr_keys(
            node("var_1"),
            JoinType::Inner,
            (vec![get_field(col("var_0"), "a")], vec![col("var_1.id")]),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
    LogicalPlanBuilder::from(first)
        .join_with_expr_keys(
            node("var_2"),
            JoinType::Inner,
            (vec![get_field(col("var_0"), "b")], vec![col("var_2.id")]),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
}

/// Both patterns as the lowerer emits them: cross joins under filters.
fn filtered_cross_joins(rows: LogicalPlan, second: Expr) -> LogicalPlan {
    LogicalPlanBuilder::from(rows)
        .cross_join(node("var_1"))
        .unwrap()
        .filter(col("var_1.id").eq(get_field(col("var_0"), "a")))
        .unwrap()
        .cross_join(node("var_2"))
        .unwrap()
        .filter(second)
        .unwrap()
        .build()
        .unwrap()
}

/// Both patterns under one `WHERE` over unfiltered cross joins.
fn where_over_cross_joins(rows: LogicalPlan, condition: Expr) -> LogicalPlan {
    LogicalPlanBuilder::from(rows)
        .cross_join(node("var_1"))
        .unwrap()
        .cross_join(node("var_2"))
        .unwrap()
        .filter(condition)
        .unwrap()
        .build()
        .unwrap()
}

fn first_equality() -> Expr {
    col("var_1.id").eq(get_field(col("var_0"), "a"))
}

fn second_equality() -> Expr {
    col("var_2.id").eq(get_field(col("var_0"), "b"))
}

/// `(name, plan)` for every shape the tests run.
fn shapes() -> Vec<(&'static str, LogicalPlan)> {
    vec![
        (
            "keyed joins, projected map",
            keyed_joins(rows_from_projection()),
        ),
        ("keyed joins, unwound map", keyed_joins(rows_from_unwind())),
        (
            "filtered cross joins",
            filtered_cross_joins(rows_from_unwind(), second_equality()),
        ),
        (
            "where equalities",
            where_over_cross_joins(rows_from_unwind(), first_equality().and(second_equality())),
        ),
        (
            "second pattern ordered by the map",
            filtered_cross_joins(
                rows_from_unwind(),
                col("var_2.id").gt(get_field(col("var_0"), "b")),
            ),
        ),
        (
            "second pattern equal to either map field",
            filtered_cross_joins(
                rows_from_unwind(),
                second_equality().or(col("var_2.id").eq(get_field(col("var_0"), "a"))),
            ),
        ),
        (
            "where ordered by the map",
            where_over_cross_joins(
                rows_from_unwind(),
                first_equality().and(col("var_2.id").gt(get_field(col("var_0"), "b"))),
            ),
        ),
        (
            "where equal to either map field",
            where_over_cross_joins(
                rows_from_projection(),
                first_equality()
                    .and(second_equality().or(col("var_2.id").eq(get_field(col("var_0"), "a")))),
            ),
        ),
    ]
}

fn returning_names(plan: LogicalPlan) -> LogicalPlan {
    LogicalPlanBuilder::from(plan)
        .project(vec![
            col("var_1.name").alias("a.name"),
            col("var_2.name").alias("b.name"),
        ])
        .unwrap()
        .build()
        .unwrap()
}

fn counting_rows(plan: LogicalPlan) -> LogicalPlan {
    LogicalPlanBuilder::from(plan)
        .aggregate(Vec::<Expr>::new(), vec![count(lit(1i64)).alias("count")])
        .unwrap()
        .build()
        .unwrap()
}

fn optimize_with(rules: Rules, plan: LogicalPlan) -> Result<LogicalPlan, String> {
    Optimizer::with_rules(rules)
        .optimize(plan, &OptimizerContext::new(), |_, _| {})
        .map_err(|error| error.to_string())
}

fn output_names(plan: &LogicalPlan) -> Vec<String> {
    plan.schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

/// DataFusion's default rules, which a stock session runs.
fn stock_rules() -> Rules {
    Optimizer::new().rules
}

/// The rule lists the tests run: DataFusion's own and GraphForge's.
fn rule_lists() -> Vec<(&'static str, Rules)> {
    vec![
        ("stock rules", stock_rules()),
        ("graphforge rules", optimizer_rules()),
    ]
}

/// The qualified column names of a schema, in order. Nullability is left out: a rule
/// may legitimately tighten it without rebuilding the parent.
fn column_names(schema: &DFSchema) -> Vec<String> {
    schema
        .iter()
        .map(|(qualifier, field)| match qualifier {
            Some(qualifier) => format!("{qualifier}.{}", field.name()),
            None => field.name().clone(),
        })
        .collect()
}

/// Every node whose cached columns differ from the columns its inputs produce.
fn stale_schemas(plan: &LogicalPlan) -> Vec<String> {
    let mut stale = Vec::new();
    plan.apply(|node| {
        if !node.inputs().is_empty() {
            let rebuilt = node.clone().recompute_schema().unwrap();
            let cached = column_names(node.schema());
            let produced = column_names(rebuilt.schema());
            if cached != produced {
                stale.push(format!(
                    "{}: cached {cached:?}, inputs produce {produced:?}",
                    node.display()
                ));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    stale
}

/// Runs `rules` over `plan` and returns the nodes that, after some rule application,
/// kept a schema their inputs no longer produce, with the rule's name, plus the
/// optimizer's own result. Looking after each rule matters: a later pass of the same
/// rules rebuilds some nodes and would hide a node left stale earlier.
fn stale_after_each_rule(rules: Rules, plan: LogicalPlan) -> (Vec<String>, Result<(), String>) {
    let mut stale = Vec::new();
    let outcome = Optimizer::with_rules(rules)
        .optimize(plan, &OptimizerContext::new(), |after, rule| {
            for node in stale_schemas(after) {
                stale.push(format!("after {}: {node}", rule.name()));
            }
        })
        .map(|_| ())
        .map_err(|error| error.to_string());
    (stale, outcome)
}

#[test]
fn rewritten_join_inputs_leave_no_node_with_a_stale_schema() {
    for (rules_name, rules) in rule_lists() {
        for (shape, plan) in shapes() {
            let (stale, outcome) = stale_after_each_rule(rules.clone(), returning_names(plan));
            assert_eq!(
                stale,
                Vec::<String>::new(),
                "{rules_name}, {shape}: a node kept the schema from before its input was \
                 rewritten"
            );
            assert_eq!(outcome, Ok(()), "{rules_name}, {shape}");
        }
    }
}

#[test]
fn joins_keyed_off_a_struct_field_return_their_columns() {
    for (rules_name, rules) in rule_lists() {
        for (shape, plan) in shapes() {
            let optimized = optimize_with(rules.clone(), returning_names(plan))
                .unwrap_or_else(|error| panic!("{rules_name}, {shape}: {error}"));
            assert_eq!(
                output_names(&optimized),
                ["a.name", "b.name"],
                "{rules_name}, {shape}:\n{}",
                optimized.display_indent()
            );
        }
    }
}

#[test]
fn joins_keyed_off_a_struct_field_can_be_counted() {
    for (rules_name, rules) in rule_lists() {
        for (shape, plan) in shapes() {
            let optimized = optimize_with(rules.clone(), counting_rows(plan))
                .unwrap_or_else(|error| panic!("{rules_name}, {shape}: {error}"));
            assert_eq!(
                output_names(&optimized),
                ["count"],
                "{rules_name}, {shape}:\n{}",
                optimized.display_indent()
            );
        }
    }
}

#[test]
fn optimizing_an_optimized_plan_changes_nothing() {
    for (shape, plan) in shapes() {
        let optimized = optimize_with(optimizer_rules(), returning_names(plan))
            .unwrap_or_else(|error| panic!("{shape}: {error}"));
        let again = optimize_with(optimizer_rules(), optimized.clone())
            .unwrap_or_else(|error| panic!("{shape}, second pass: {error}"));
        assert_eq!(
            optimized.display_indent().to_string(),
            again.display_indent().to_string(),
            "{shape}"
        );
    }
}
