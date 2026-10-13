//! Conservative predicate movement across required fixed graph expansion.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    Expr, ExprSchemable, Extension, Filter, LogicalPlan, Operator, UserDefinedLogicalNode,
};
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use graphforge_plan::{ExpandNode, VarLenExpandNode};

/// Move total, deterministic input predicates before required fixed expansion.
///
/// DataFusion's extension hook uses unqualified column names. This rule instead
/// requires qualified references to unchanged input fields and never moves an
/// expression that can introduce row-dependent errors or volatile evaluation.
#[derive(Debug)]
pub struct FixedExpandInputPredicates;

/// Preserve DataFusion defaults and place graph input filtering before projection pruning.
#[must_use]
pub fn optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules = datafusion::optimizer::Optimizer::new().rules;
    let position = rules
        .iter()
        .position(|rule| rule.name() == "push_down_filter")
        .expect("pinned DataFusion optimizer includes filter pushdown");
    rules.insert(position + 1, Arc::new(FixedExpandInputPredicates));
    rules.insert(position + 2, Arc::new(VarLenExpandInputPredicates));
    rules.insert(position + 3, Arc::new(FixedExpandEdgePredicates));
    // Last, so every filter has reached its final place before it is offered
    // to the property scan that can use it.
    rules.push(Arc::new(crate::stored_equality::StoredEqualityHints));
    rules
}

fn atom_type(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    match expr {
        Expr::Column(column) if column.relation.is_some() => expr.get_type(schema).ok(),
        Expr::Literal(value, _) if !value.is_null() => Some(value.data_type()),
        _ => None,
    }
}

fn primitive(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::FixedSizeBinary(_)
    )
}

pub(crate) fn total_predicate(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) =>
        {
            let left = atom_type(&binary.left, schema);
            left.as_ref().is_some_and(primitive) && left == atom_type(&binary.right, schema)
        }
        Expr::IsNull(value) | Expr::IsNotNull(value) => atom_type(value, schema).is_some(),
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            total_predicate(&binary.left, schema) && total_predicate(&binary.right, schema)
        }
        Expr::ScalarFunction(function) if crate::expr::is_comparison_predicate(function) => {
            function.args.len() == 3
                && matches!(
                    &function.args[2],
                    Expr::Literal(
                        datafusion::common::ScalarValue::Int8(Some(0..=3))
                            | datafusion::common::ScalarValue::Int64(Some(0..=3)),
                        _
                    )
                )
                && atom_type(&function.args[0], schema)
                    .as_ref()
                    .is_some_and(primitive)
                && atom_type(&function.args[0], schema) == atom_type(&function.args[1], schema)
        }
        // A label test is `array_has(<node>.type_ids, <type id>)`: it yields
        // true, false or NULL for every row and cannot fail, so a disjunction
        // of such tests is as total as its operands. `OR` evaluates both
        // operands and fails only through them.
        Expr::ScalarFunction(function) if label_membership(function, schema) => true,
        _ => {
            crate::expr::is_total_relationship_disjoint(expr, schema)
                || matches!(
                    expr,
                    Expr::ScalarFunction(function)
                        if crate::expr::cypher_or_operands(function).is_some_and(
                            |(left, right)| {
                                total_predicate(left, schema) && total_predicate(right, schema)
                            }
                        )
                )
        }
    }
}

/// `array_has(column, literal)` where the column is a list of the literal's
/// primitive type, so that the call cannot fail on any row.
fn label_membership(
    function: &datafusion::logical_expr::expr::ScalarFunction,
    schema: &DFSchema,
) -> bool {
    use datafusion::functions_nested::array_has::ArrayHas;
    let [list, needle] = function.args.as_slice() else {
        return false;
    };
    function.func.inner().downcast_ref::<ArrayHas>().is_some()
        && matches!(list, Expr::Column(column) if column.relation.is_some())
        && match (list.get_type(schema), atom_type(needle, schema)) {
            (Ok(DataType::List(element)), Some(needle)) => {
                needle.is_integer() && element.data_type() == &needle
            }
            _ => false,
        }
}

/// Move the total conjuncts of the filter `plan` that read only `input`'s
/// columns onto `input`, and rebuild the expansion over that filtered input with
/// `rebuild`. The remaining conjuncts stay in a filter above the expansion.
///
/// With `settle`, the moved filter is then carried on down through the plan
/// that supplies `input` (see [`settle_filter`]).
fn push_input_predicates(
    plan: LogicalPlan,
    input: &Arc<LogicalPlan>,
    settle: Option<&dyn OptimizerConfig>,
    rebuild: impl FnOnce(Arc<LogicalPlan>) -> LogicalPlan,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Filter(filter) = &plan else {
        return Ok(Transformed::no(plan));
    };
    // Do not alter which rows reach a potentially failing or volatile
    // residual expression in the same filter.
    if !total_predicate(&filter.predicate, filter.input.schema()) {
        return Ok(Transformed::no(plan));
    }
    let (push, keep): (Vec<_>, Vec<_>) = split_conjunction(&filter.predicate)
        .into_iter()
        .cloned()
        .partition(|expr| {
            let columns = expr.column_refs();
            // A relationship-uniqueness test relates relationships the
            // expansion produces; it is never a seed predicate.
            !crate::expr::is_total_relationship_disjoint(expr, filter.input.schema())
                && !columns.is_empty()
                && columns
                    .iter()
                    .all(|column| column.relation.is_some() && input.schema().has_column(column))
        });
    let Some(predicate) = conjunction(push) else {
        return Ok(Transformed::no(plan));
    };
    let moved = LogicalPlan::Filter(Filter::try_new(predicate, Arc::clone(input))?);
    let moved = match settle {
        Some(config) => settle_filter(moved, config)?,
        None => moved,
    };
    let replacement = rebuild(Arc::new(moved));
    let replacement = match conjunction(keep) {
        Some(predicate) => LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(replacement))?),
        None => replacement,
    };
    Ok(Transformed::yes(replacement))
}

/// Carry a filter that was just moved onto an expansion input on down through
/// the plan that supplies that input, in one traversal.
///
/// A seed predicate on the first node of a pattern has to cross every later
/// hop, and between two hops the plan holds projections, the left joins that
/// hydrate node properties, and the label filters of the nodes that were
/// matched. DataFusion moves a filter one such operator per optimizer pass and
/// the optimizer runs a fixed number of passes, so the depth of the pattern
/// would decide whether the predicate reaches the scan. This applies the same
/// rules down the whole subtree instead: DataFusion's own filter pushdown at
/// each operator, then the expansion rules at each expansion. It makes no
/// move those rules would not make, only all of them at once, and it is one
/// finite traversal of the subtree (#1984).
///
/// A variable-length expansion ends the traversal. Its seed is where a seed
/// predicate belongs, and the filter is left there, directly above the plan
/// that supplies the seed, for the stored-equality hints to read before any
/// further pushdown folds it into the scans.
fn settle_filter(plan: LogicalPlan, config: &dyn OptimizerConfig) -> Result<LogicalPlan> {
    plan.transform_down(|node| {
        let node = PushDownFilter::new()
            .rewrite(node, config)?
            .transform_data(|node| FixedExpandInputPredicates::push(node, None))?;
        let sunk = VarLenExpandInputPredicates::push(node.data)?;
        Ok(Transformed::new(
            sunk.data,
            sunk.transformed || node.transformed,
            if sunk.transformed {
                TreeNodeRecursion::Jump
            } else {
                TreeNodeRecursion::Continue
            },
        ))
    })
    .map(|transformed| transformed.data)
}

fn extension(node: impl UserDefinedLogicalNode + 'static) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    })
}

impl FixedExpandInputPredicates {
    fn push(
        plan: LogicalPlan,
        settle: Option<&dyn OptimizerConfig>,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::Extension(node) = filter.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let Some(expand) = node.node.as_any().downcast_ref::<ExpandNode>() else {
            return Ok(Transformed::no(plan));
        };
        let mut replacement = expand.clone();
        let input = Arc::clone(&expand.input);
        push_input_predicates(plan, &input, settle, move |filtered| {
            replacement.input = filtered;
            extension(replacement)
        })
    }
}

impl OptimizerRule for FixedExpandInputPredicates {
    fn name(&self) -> &'static str {
        "graphforge_fixed_expand_input_predicates"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        Self::push(plan, Some(config))
    }
}

/// Copy the total, deterministic conjuncts of a filter over an adjacency-backed
/// single-hop expansion that read only the hop's own relationship properties
/// into the expansion, so it tests them on each traversed edge before it reads
/// the edge's destination.
///
/// The filter above the expansion tests the same conjuncts on the rows that
/// survive the destination read. Testing them earlier selects the same edges,
/// so the filter stays where it is: this rule only changes how many destinations
/// the expansion reads, never which rows leave it. Every conjunct moved is
/// total (see [`total_predicate`]) and so is the whole filter, which is what
/// lets an edge be dropped early without hiding an error a later conjunct would
/// have raised on it.
#[derive(Debug)]
pub struct FixedExpandEdgePredicates;

impl FixedExpandEdgePredicates {
    fn push(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::Extension(node) = filter.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let Some(expand) = node.node.as_any().downcast_ref::<ExpandNode>() else {
            return Ok(Transformed::no(plan));
        };
        if !total_predicate(&filter.predicate, filter.input.schema()) {
            return Ok(Transformed::no(plan));
        }
        let properties: Vec<datafusion::common::Column> = expand
            .edge_property_fields()
            .into_iter()
            .map(|(qualifier, field)| {
                datafusion::common::Column::new(Some(qualifier), field.name())
            })
            .collect();
        let mut pushed = expand
            .edge_predicate
            .as_ref()
            .map(|predicate| {
                split_conjunction(predicate)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let before = pushed.len();
        for conjunct in split_conjunction(&filter.predicate) {
            let columns = conjunct.column_refs();
            if !columns.is_empty()
                && columns.iter().all(|column| properties.contains(column))
                && !pushed.contains(conjunct)
            {
                pushed.push(conjunct.clone());
            }
        }
        if pushed.len() == before {
            return Ok(Transformed::no(plan));
        }
        let replacement = expand.clone().with_edge_predicate(conjunction(pushed));
        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
            filter.predicate.clone(),
            Arc::new(extension(replacement)),
        )?)))
    }
}

impl OptimizerRule for FixedExpandEdgePredicates {
    fn name(&self) -> &'static str {
        "graphforge_fixed_expand_edge_predicates"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        Self::push(plan)
    }
}

/// Move total, deterministic input predicates before a variable-length
/// expansion.
///
/// The expansion runs once per input row and its output extends that row, so a
/// predicate over the input's own columns selects the same seed rows before the
/// expansion as after it. Moving it lets a stored-property equality on the seed
/// reach the scan that supplies the seed, instead of the expansion starting
/// from every node of the graph (#1984).
#[derive(Debug)]
pub struct VarLenExpandInputPredicates;

impl VarLenExpandInputPredicates {
    /// The moved filter is not carried on into the seed: see [`settle_filter`].
    fn push(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::Extension(node) = filter.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let Some(expand) = node.node.as_any().downcast_ref::<VarLenExpandNode>() else {
            return Ok(Transformed::no(plan));
        };
        let mut replacement = expand.clone();
        let input = Arc::clone(&expand.input);
        push_input_predicates(plan, &input, None, move |filtered| {
            replacement.input = filtered;
            extension(replacement)
        })
    }
}

impl OptimizerRule for VarLenExpandInputPredicates {
    fn name(&self) -> &'static str {
        "graphforge_var_len_expand_input_predicates"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        Self::push(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::{Column, TableReference};
    use datafusion::logical_expr::{EmptyRelation, col, lit};
    use datafusion::optimizer::OptimizerContext;
    use graphforge_ir::Direction;

    fn expanded() -> LogicalPlan {
        let schema = DFSchema::new_with_metadata(
            vec![(
                Some(TableReference::bare("var_0")),
                Arc::new(Field::new("score", DataType::Int64, true)),
            )],
            Default::default(),
        )
        .unwrap();
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(schema),
        });
        LogicalPlan::Extension(Extension {
            node: Arc::new(ExpandNode::new(
                Arc::new(input),
                "*",
                0,
                1,
                2,
                Direction::Out,
                None,
                vec![Arc::new(Field::new("edge_id", DataType::UInt64, false))],
                vec![],
                vec![Arc::new(Field::new("score", DataType::Int64, true))],
            )),
        })
    }

    fn column(var: &str) -> Expr {
        Expr::Column(Column::new(Some(TableReference::bare(var)), "score"))
    }

    fn rewrite(predicate: Expr) -> Transformed<LogicalPlan> {
        FixedExpandInputPredicates
            .rewrite(
                LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(expanded())).unwrap()),
                &OptimizerContext::new(),
            )
            .unwrap()
    }

    #[test]
    fn same_named_destination_stays_residual() {
        let result = rewrite(
            column("var_0")
                .eq(lit(7_i64))
                .and(column("var_1").gt(lit(2_i64))),
        );
        assert!(result.transformed);
        let LogicalPlan::Filter(residual) = result.data else {
            panic!("destination predicate must remain");
        };
        assert_eq!(residual.predicate, column("var_1").gt(lit(2_i64)));
        let LogicalPlan::Extension(extension) = residual.input.as_ref() else {
            panic!();
        };
        let expand = extension
            .node
            .as_any()
            .downcast_ref::<ExpandNode>()
            .unwrap();
        let LogicalPlan::Filter(input) = expand.input.as_ref() else {
            panic!("input predicate must precede expansion");
        };
        assert_eq!(input.predicate, column("var_0").eq(lit(7_i64)));
    }

    #[test]
    fn potentially_failing_residual_blocks_entire_filter() {
        let predicate = column("var_0")
            .eq(lit(7_i64))
            .and((column("var_1") / lit(0_i64)).gt(lit(0_i64)));
        assert!(!rewrite(predicate).transformed);
    }

    #[test]
    fn unqualified_or_mixed_disjunction_is_not_moved() {
        assert!(!total_predicate(
            &col("score").eq(lit(7_i64)),
            expanded().schema()
        ));
        assert!(
            !rewrite(
                column("var_0")
                    .eq(lit(7_i64))
                    .or(column("var_1").eq(lit(7_i64)))
            )
            .transformed
        );
        assert!(!rewrite(column("var_1").eq(lit(7_i64))).transformed);
    }

    #[test]
    fn volatile_residual_blocks_entire_filter() {
        let random = datafusion::functions::math::random().call(vec![]);
        assert!(!rewrite(column("var_0").eq(lit(7_i64)).and(random.gt(lit(0.5)))).transformed);
    }

    #[test]
    fn input_column_comparison_preserves_qualified_membership() {
        assert!(rewrite(column("var_0").eq(column("var_0"))).transformed);
        assert!(!rewrite(column("var_0").eq(column("var_1"))).transformed);
    }

    #[test]
    fn nullable_input_checks_can_precede_expansion() {
        assert!(rewrite(column("var_0").is_null()).transformed);
        assert!(rewrite(column("var_0").is_not_null()).transformed);
        assert!(
            rewrite(
                column("var_0")
                    .gt_eq(lit(-7_i64))
                    .and(column("var_0").lt_eq(lit(7_i64)))
            )
            .transformed
        );
    }

    fn labelled() -> DFSchema {
        DFSchema::new_with_metadata(
            vec![
                (
                    Some(TableReference::bare("var_0")),
                    Arc::new(Field::new(
                        "type_ids",
                        DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                        true,
                    )),
                ),
                (
                    Some(TableReference::bare("var_0")),
                    Arc::new(Field::new("score", DataType::Int64, true)),
                ),
            ],
            Default::default(),
        )
        .unwrap()
    }

    fn has_label(id: u32) -> Expr {
        datafusion::functions_nested::expr_fn::array_has(
            Expr::Column(Column::new(Some(TableReference::bare("var_0")), "type_ids")),
            lit(id),
        )
    }

    #[test]
    fn label_disjunction_is_total_only_through_total_operands() {
        let schema = labelled();
        let or = |left, right| crate::expr::cypher_or(left, right);
        assert!(total_predicate(&has_label(1), &schema));
        assert!(total_predicate(&or(has_label(1), has_label(2)), &schema));
        assert!(total_predicate(
            &or(has_label(1), or(has_label(2), has_label(3))),
            &schema
        ));
        let failing = (column("var_0") / lit(0_i64)).gt(lit(0_i64));
        assert!(!total_predicate(&or(has_label(1), failing), &schema));
        // A needle of another type than the list's elements is not admitted.
        let wrong = datafusion::functions_nested::expr_fn::array_has(
            Expr::Column(Column::new(Some(TableReference::bare("var_0")), "type_ids")),
            lit(1_i64),
        );
        assert!(!total_predicate(&wrong, &schema));
    }

    fn var_len() -> VarLenExpandNode {
        let LogicalPlan::Extension(expanded) = expanded() else {
            unreachable!();
        };
        let fixed = expanded.node.as_any().downcast_ref::<ExpandNode>().unwrap();
        VarLenExpandNode::new(
            Arc::clone(&fixed.input),
            "*",
            0,
            None,
            0,
            1,
            2,
            Direction::Out,
            None,
            vec![Arc::new(Field::new("score", DataType::Int64, true))],
            graphforge_plan::var_len_edge_list_field(&[]),
        )
    }

    fn rewrite_var_len(node: VarLenExpandNode, predicate: Expr) -> Transformed<LogicalPlan> {
        VarLenExpandInputPredicates
            .rewrite(
                LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(extension(node))).unwrap()),
                &OptimizerContext::new(),
            )
            .unwrap()
    }

    fn pushed_below(result: &Transformed<LogicalPlan>) -> (Option<Expr>, Expr, VarLenExpandNode) {
        let (residual, expansion) = match &result.data {
            LogicalPlan::Filter(residual) => {
                (Some(residual.predicate.clone()), (*residual.input).clone())
            }
            other => (None, other.clone()),
        };
        let LogicalPlan::Extension(extension) = &expansion else {
            panic!("the expansion must remain");
        };
        let node = extension
            .node
            .as_any()
            .downcast_ref::<VarLenExpandNode>()
            .unwrap()
            .clone();
        let LogicalPlan::Filter(input) = node.input.as_ref() else {
            panic!("the input predicate must precede the expansion");
        };
        (residual, input.predicate.clone(), node)
    }

    #[test]
    fn variable_length_seed_predicate_precedes_the_expansion() {
        let result = rewrite_var_len(
            var_len(),
            column("var_0")
                .eq(lit(7_i64))
                .and(column("var_1").gt(lit(2_i64))),
        );
        assert!(result.transformed);
        let (residual, input, node) = pushed_below(&result);
        assert_eq!(residual, Some(column("var_1").gt(lit(2_i64))));
        assert_eq!(input, column("var_0").eq(lit(7_i64)));
        assert_eq!(
            node.schema(),
            var_len().schema(),
            "moving a filter below the expansion leaves its output schema alone"
        );
    }

    #[test]
    fn variable_length_expansion_keeps_everything_but_its_input() {
        let original = var_len()
            .with_bound_destination(Some("var_0".to_owned()))
            .into_reachability();
        let result = rewrite_var_len(original.clone(), column("var_0").eq(lit(7_i64)));
        assert!(result.transformed);
        let (residual, _, node) = pushed_below(&result);
        assert_eq!(residual, None);
        assert_eq!(node.mode, original.mode);
        assert_eq!(node.bound_destination, original.bound_destination);
        assert_eq!(node.min_hops, original.min_hops);
        assert_eq!(node.max_hops, original.max_hops);
        assert_eq!(node.edge_var, original.edge_var);
        assert_eq!(node.schema(), original.schema());
    }

    #[test]
    fn variable_length_destination_and_failing_predicates_stay_above() {
        assert!(!rewrite_var_len(var_len(), column("var_1").eq(lit(7_i64))).transformed);
        assert!(
            !rewrite_var_len(
                var_len(),
                column("var_0")
                    .eq(lit(7_i64))
                    .and((column("var_1") / lit(0_i64)).gt(lit(0_i64)))
            )
            .transformed
        );
        let random = datafusion::functions::math::random().call(vec![]);
        assert!(
            !rewrite_var_len(
                var_len(),
                column("var_0").eq(lit(7_i64)).and(random.gt(lit(0.5)))
            )
            .transformed
        );
    }

    #[test]
    fn fixed_relationship_disjointness_stays_total() {
        let edge = |var: &str| {
            (
                Some(TableReference::bare(var)),
                Arc::new(Field::new("edge_id", DataType::UInt64, false)),
            )
        };
        let schema =
            DFSchema::new_with_metadata(vec![edge("var_1"), edge("var_3")], Default::default())
                .unwrap();
        let disjoint = crate::expr::relationship_disjoint(
            Expr::Column(Column::new(Some(TableReference::bare("var_1")), "edge_id")),
            Expr::Column(Column::new(Some(TableReference::bare("var_3")), "edge_id")),
        );
        assert!(total_predicate(&disjoint, &schema));
    }

    fn fields_of(columns: &[(&str, &str, DataType)]) -> DFSchema {
        DFSchema::new_with_metadata(
            columns
                .iter()
                .map(|(var, name, data_type)| {
                    (
                        Some(TableReference::bare(*var)),
                        Arc::new(Field::new(*name, data_type.clone(), true)),
                    )
                })
                .collect(),
            Default::default(),
        )
        .unwrap()
    }

    fn named(var: &str, name: &str) -> Expr {
        Expr::Column(Column::new(Some(TableReference::bare(var)), name))
    }

    #[test]
    fn relationship_disjointness_is_total_by_type_not_by_column_name() {
        let relationship = DataType::Struct(
            vec![
                Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("rel_type", DataType::Utf8, true),
            ]
            .into(),
        );
        let trail = DataType::List(Arc::new(Field::new("item", relationship, true)));
        let schema = fields_of(&[
            ("var_1", "rels", trail.clone()),
            ("var_3", "edge_id", DataType::UInt64),
            ("var_5", "edge_id", DataType::UInt64),
        ]);
        let disjoint = |left: Expr, right: Expr| crate::expr::relationship_disjoint(left, right);
        // A trail of relationships against one relationship, as a
        // variable-length hop followed by a fixed hop produces.
        assert!(total_predicate(
            &disjoint(named("var_1", "rels"), named("var_3", "edge_id")),
            &schema
        ));
        assert!(total_predicate(
            &disjoint(named("var_3", "edge_id"), named("var_5", "edge_id")),
            &schema
        ));
        // A column the schema does not hold cannot be typed.
        assert!(!total_predicate(
            &disjoint(named("var_1", "rels"), named("var_9", "edge_id")),
            &schema
        ));
        // An argument that is not a column is not proven by its type.
        assert!(!total_predicate(
            &disjoint(named("var_1", "rels"), lit(1_u64)),
            &schema
        ));
    }

    #[test]
    fn relationship_disjointness_over_heterogeneous_values_is_not_total() {
        // Properties carried in the heterogeneous encoding are validated when
        // the test reads them, and malformed wire data makes the call fail.
        let heterogeneous = DataType::Struct(graphforge_value::heterogeneous::scalar_fields());
        let holds = |data_type: DataType| {
            let schema = fields_of(&[
                ("var_1", "rels", data_type),
                ("var_3", "edge_id", DataType::UInt64),
            ]);
            total_predicate(
                &crate::expr::relationship_disjoint(
                    named("var_1", "rels"),
                    named("var_3", "edge_id"),
                ),
                &schema,
            )
        };
        assert!(!holds(heterogeneous.clone()));
        assert!(!holds(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Field::new("props", heterogeneous, true)].into()),
            true
        )))));
        assert!(holds(DataType::UInt64));
    }

    fn seed() -> Arc<LogicalPlan> {
        let LogicalPlan::Extension(expanded) = expanded() else {
            unreachable!();
        };
        let fixed = expanded.node.as_any().downcast_ref::<ExpandNode>().unwrap();
        Arc::clone(&fixed.input)
    }

    fn var_len_from(
        input: Arc<LogicalPlan>,
        source: u32,
        destination: u32,
        edge: u32,
    ) -> LogicalPlan {
        extension(VarLenExpandNode::new(
            input,
            "*",
            0,
            None,
            source,
            destination,
            edge,
            Direction::Out,
            None,
            vec![Arc::new(Field::new("score", DataType::Int64, true))],
            graphforge_plan::var_len_edge_list_field(&[]),
        ))
    }

    fn expand_from(input: LogicalPlan, source: u32, destination: u32, edge: u32) -> LogicalPlan {
        extension(ExpandNode::new(
            Arc::new(input),
            "*",
            source,
            destination,
            edge,
            Direction::Out,
            None,
            vec![Arc::new(Field::new("edge_id", DataType::UInt64, false))],
            vec![],
            vec![Arc::new(Field::new("score", DataType::Int64, true))],
        ))
    }

    fn keep_scores(input: LogicalPlan, variables: &[&str]) -> LogicalPlan {
        LogicalPlan::Projection(
            datafusion::logical_expr::Projection::try_new(
                variables.iter().map(|variable| column(variable)).collect(),
                Arc::new(input),
            )
            .unwrap(),
        )
    }

    /// The operators of a single-input plan from the top down, one per line.
    fn outline(plan: &LogicalPlan) -> Vec<String> {
        let mut lines = Vec::new();
        let mut current = plan;
        loop {
            lines.push(match current {
                LogicalPlan::Filter(filter) => format!("Filter {}", filter.predicate),
                LogicalPlan::Projection(_) => "Projection".to_owned(),
                LogicalPlan::Extension(extension) => extension.node.name().to_owned(),
                LogicalPlan::EmptyRelation(_) => "Empty".to_owned(),
                other => panic!("unexpected operator {}", other.display()),
            });
            match current.inputs().as_slice() {
                [only] => current = only,
                _ => return lines,
            }
        }
    }

    fn filter_over(predicate: Expr, input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(input)).unwrap())
    }

    #[test]
    fn a_seed_predicate_sinks_through_the_chain_to_a_variable_length_seed() {
        // The first node of `(a)-[*]->(b)-[]->(c)`: the predicate is on `a`, and
        // between it and the scan lie a projection and the variable-length
        // expansion that `a` seeds.
        let chain = expand_from(
            keep_scores(var_len_from(seed(), 0, 1, 2), &["var_0", "var_1"]),
            1,
            3,
            4,
        );
        let selected = column("var_0").eq(lit(7_i64));
        let result = FixedExpandInputPredicates
            .rewrite(
                filter_over(selected.clone(), chain),
                &OptimizerContext::new(),
            )
            .unwrap();
        assert!(result.transformed);
        assert_eq!(
            outline(&result.data),
            [
                "Expand".to_owned(),
                "Projection".to_owned(),
                "VarLenExpand".to_owned(),
                format!("Filter {selected}"),
                "Empty".to_owned(),
            ],
            "the predicate must end directly above the plan that seeds the \
             variable-length expansion, with nothing left above the hop"
        );
    }

    #[test]
    fn the_sink_stops_at_the_seed_of_a_variable_length_expansion() {
        // Two variable-length expansions in sequence. The predicate belongs on
        // the seed of the later one; it stays above the plan that supplies that
        // seed, where the stored-equality hints read it, and is not carried into
        // the earlier expansion's input.
        let earlier = keep_scores(var_len_from(seed(), 0, 1, 2), &["var_0", "var_1"]);
        let later = var_len_from(Arc::new(earlier), 1, 3, 4);
        let selected = column("var_0").eq(lit(7_i64));
        let result = FixedExpandInputPredicates
            .rewrite(
                filter_over(selected.clone(), expand_from(later, 3, 5, 6)),
                &OptimizerContext::new(),
            )
            .unwrap();
        assert!(result.transformed);
        assert_eq!(
            outline(&result.data),
            [
                "Expand".to_owned(),
                "VarLenExpand".to_owned(),
                format!("Filter {selected}"),
                "Projection".to_owned(),
                "VarLenExpand".to_owned(),
                "Empty".to_owned(),
            ]
        );
    }

    #[test]
    fn a_relationship_disjointness_test_is_never_a_seed_predicate() {
        // Both relationships are columns of the input, so the test could be
        // evaluated there; it still relates relationships and stays above.
        let edges = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(fields_of(&[
                ("var_0", "score", DataType::Int64),
                ("var_5", "edge_id", DataType::UInt64),
                ("var_6", "edge_id", DataType::UInt64),
            ])),
        });
        let disjoint = crate::expr::relationship_disjoint(
            named("var_5", "edge_id"),
            named("var_6", "edge_id"),
        );
        let selected = column("var_0").eq(lit(7_i64));
        let result = rewrite_var_len(
            VarLenExpandNode::new(
                Arc::new(edges),
                "*",
                0,
                None,
                0,
                1,
                2,
                Direction::Out,
                None,
                vec![Arc::new(Field::new("score", DataType::Int64, true))],
                graphforge_plan::var_len_edge_list_field(&[]),
            ),
            selected.clone().and(disjoint.clone()),
        );
        assert!(result.transformed);
        let (residual, moved, _) = pushed_below(&result);
        assert_eq!(moved, selected);
        assert_eq!(residual, Some(disjoint));
    }
}
