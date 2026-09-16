//! Nested queries lowering.

use super::{
    Arc, DfExpr, ExprArena, ExprFunctionExt, ExprId, ExprSchemable, Extension, GraphOp, GraphPlan,
    GraphPlanLowerer, IrExpr, JoinType, LogicalPlan, LogicalPlanBuilder, LoweringError,
    MapUnsupportedExpr, OptionalMatchNode, PATTERN_COMPREHENSION_VALUE_ALIAS, UnwindNode, VarId,
    VarMap, array_agg, list_index_range, lower_filter, var_alias,
};

impl GraphPlanLowerer {
    pub(super) fn lower_union_op(
        &self,
        all: bool,
        inputs: &[GraphPlan],
    ) -> Result<LogicalPlan, LoweringError> {
        if inputs.len() < 2 {
            return Err(LoweringError::UnsupportedExpr(
                "UNION requires at least two branch plans".into(),
            ));
        }
        let mut plans = inputs
            .iter()
            .map(|branch| {
                let mut branch_vars = VarMap::new();
                self.lower_pipeline(&branch.ops, &branch.exprs, &mut branch_vars)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter();
        let first = plans.next().expect("UNION branch count checked above");
        let union = plans.try_fold(first, |left, right| {
            LogicalPlanBuilder::from(left)
                .union(right)
                .and_then(LogicalPlanBuilder::build)
                .map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))
        })?;
        if all {
            Ok(union)
        } else {
            LogicalPlanBuilder::from(union)
                .distinct()
                .and_then(LogicalPlanBuilder::build)
                .map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))
        }
    }

    pub(super) fn lower_exists_op(
        &self,
        child: &GraphPlan,
        negated: bool,
        input: LogicalPlan,
        var_map: &VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        if let [GraphOp::Union { inputs, .. }] = child.ops.as_slice() {
            return self.lower_exists_alternatives(inputs, negated, input, var_map);
        }
        // A full existential subquery's terminal RETURN controls syntax and
        // inner evaluation only; its projected values are not exposed. Keep the
        // pre-projection relation so correlated outer keys remain available for
        // the semi-join.
        let (child_ops, is_full_subquery) = match child.ops.last() {
            Some(GraphOp::Project { .. }) => (&child.ops[..child.ops.len() - 1], true),
            _ => (child.ops.as_slice(), false),
        };
        let seed_outer_input = is_full_subquery && full_subquery_needs_outer_input(child, var_map);
        let mut child_vm = if seed_outer_input {
            var_map.clone()
        } else {
            VarMap::new()
        };
        let child_plan = if seed_outer_input {
            self.lower_pipeline_from(child_ops, &child.exprs, &mut child_vm, input.clone(), None)?
        } else {
            self.lower_pipeline(child_ops, &child.exprs, &mut child_vm)?
        };
        let (join_keys, _) = optional_join_keys(&input, &child_plan, var_map, &child_vm);
        if join_keys.is_empty() {
            return Err(LoweringError::UnsupportedExpr(
                "pattern predicate must share at least one bound variable".into(),
            ));
        }
        let left_keys = join_keys
            .iter()
            .map(|(outer_idx, _)| schema_join_column(input.schema(), *outer_idx))
            .collect::<Vec<_>>();
        let right_keys = join_keys
            .iter()
            .map(|(_, inner_idx)| schema_join_column(child_plan.schema(), *inner_idx))
            .collect::<Vec<_>>();
        let join_type = if negated {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        LogicalPlanBuilder::from(input)
            .join(child_plan, join_type, (left_keys, right_keys), None)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()
    }

    pub(super) fn lower_exists_alternatives(
        &self,
        children: &[GraphPlan],
        negated: bool,
        input: LogicalPlan,
        var_map: &VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        use datafusion::common::Column;

        let mut expected_outer_keys: Option<Vec<usize>> = None;
        let mut key_union: Option<LogicalPlan> = None;

        for child in children {
            let mut child_vm = VarMap::new();
            let child_plan = self.lower_pipeline(&child.ops, &child.exprs, &mut child_vm)?;
            let (mut join_keys, _) = optional_join_keys(&input, &child_plan, var_map, &child_vm);
            if join_keys.is_empty() {
                return Err(LoweringError::UnsupportedExpr(
                    "pattern predicate must share at least one bound variable".into(),
                ));
            }
            join_keys.sort_unstable_by_key(|(outer_idx, _)| *outer_idx);

            let outer_keys = join_keys
                .iter()
                .map(|(outer_idx, _)| *outer_idx)
                .collect::<Vec<_>>();
            if expected_outer_keys
                .as_ref()
                .is_some_and(|expected| expected != &outer_keys)
            {
                return Err(LoweringError::UnsupportedExpr(
                    "OR pattern predicates must correlate on the same bound variables".into(),
                ));
            }
            expected_outer_keys.get_or_insert(outer_keys);

            let key_projection = join_keys
                .iter()
                .enumerate()
                .map(|(key_idx, (_, inner_idx))| {
                    DfExpr::Column(schema_join_column(child_plan.schema(), *inner_idx))
                        .alias(format!("__exists_key_{key_idx}"))
                })
                .collect::<Vec<_>>();
            let key_plan = LogicalPlanBuilder::from(child_plan)
                .project(key_projection)
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()?;
            key_union = Some(match key_union {
                None => key_plan,
                Some(union) => LogicalPlanBuilder::from(union)
                    .union(key_plan)
                    .and_then(LogicalPlanBuilder::build)
                    .map_unsupported_expr()?,
            });
        }

        let outer_keys = expected_outer_keys.ok_or_else(|| {
            LoweringError::UnsupportedExpr("pattern predicate has no alternatives".into())
        })?;
        let key_union = key_union.expect("outer keys imply a key union");
        let left_keys = outer_keys
            .iter()
            .map(|idx| schema_join_column(input.schema(), *idx))
            .collect::<Vec<_>>();
        let right_keys = (0..left_keys.len())
            .map(|idx| Column::from_name(format!("__exists_key_{idx}")))
            .collect::<Vec<_>>();
        let join_type = if negated {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        LogicalPlanBuilder::from(input)
            .join(key_union, join_type, (left_keys, right_keys), None)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()
    }

    pub(super) fn lower_pattern_comprehension_op(
        &self,
        child: &GraphPlan,
        output: VarId,
        input: LogicalPlan,
        var_map: &mut VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        use datafusion::common::Column;
        use datafusion::functions::core::expr_fn::coalesce;
        use datafusion::scalar::ScalarValue;

        let (terminal, match_ops) = child.ops.split_last().ok_or_else(|| {
            LoweringError::UnsupportedExpr("pattern comprehension child is empty".into())
        })?;
        let GraphOp::Project { items, distinct } = terminal else {
            return Err(LoweringError::UnsupportedExpr(
                "pattern comprehension child must end in a value projection".into(),
            ));
        };
        if *distinct || items.len() != 1 {
            return Err(LoweringError::UnsupportedExpr(
                "pattern comprehension child must project exactly one non-distinct value".into(),
            ));
        }
        let item = &items[0];
        if item.alias.as_deref() != Some(PATTERN_COMPREHENSION_VALUE_ALIAS) {
            return Err(LoweringError::UnsupportedExpr(
                "pattern comprehension child has an invalid value projection".into(),
            ));
        }

        let outer_columns = input
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                DfExpr::Column(Column::new(qualifier.cloned(), field.name().to_owned()))
            })
            .collect::<Vec<_>>();
        let mut child_vm = VarMap::new();
        let child_plan = self.lower_pipeline(match_ops, &child.exprs, &mut child_vm)?;
        let (mut join_keys, _) = optional_join_keys(&input, &child_plan, var_map, &child_vm);
        if join_keys.is_empty() {
            return Err(LoweringError::UnsupportedExpr(
                "pattern comprehension must share at least one bound node variable".into(),
            ));
        }
        join_keys.sort_unstable_by_key(|(outer_idx, _)| *outer_idx);

        let value = self
            .expr_lowerer(&child.exprs, &child_vm)
            .with_input_schema(child_plan.schema().clone())
            .lower(item.expr)?;
        let element_type = value.get_type(child_plan.schema()).map_unsupported_expr()?;
        let key_aliases = (0..join_keys.len())
            .map(|idx| format!("__gf_pattern_key_{idx}"))
            .collect::<Vec<_>>();
        let group_exprs = join_keys
            .iter()
            .zip(&key_aliases)
            .map(|((_, inner_idx), alias)| {
                DfExpr::Column(schema_join_column(child_plan.schema(), *inner_idx)).alias(alias)
            })
            .collect::<Vec<_>>();
        let output_alias = format!("{PATTERN_COMPREHENSION_VALUE_ALIAS}_{}", output.0);
        let aggregate_exprs = vec![array_agg(value).alias(&output_alias)];
        let collected = LogicalPlanBuilder::from(child_plan)
            .aggregate(group_exprs, aggregate_exprs)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;

        let left_keys = join_keys
            .iter()
            .map(|(outer_idx, _)| schema_join_column(input.schema(), *outer_idx))
            .collect::<Vec<_>>();
        let right_keys = key_aliases
            .iter()
            .map(|alias| Column::from_name(alias.clone()))
            .collect::<Vec<_>>();
        let joined = LogicalPlanBuilder::from(input)
            .join(collected, JoinType::Left, (left_keys, right_keys), None)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;

        let empty = ScalarValue::List(ScalarValue::new_list(&[], &element_type, true));
        let mut projection = outer_columns;
        projection.push(
            coalesce(vec![
                DfExpr::Column(Column::from_name(output_alias.clone())),
                DfExpr::Literal(empty, None),
            ])
            .alias(&output_alias),
        );
        let result = LogicalPlanBuilder::from(joined)
            .project(projection)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        var_map.insert(output, output_alias);
        Ok(result)
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "ordinal unwind, child correlation, and ordered regroup form one lowering operation"
    )]
    pub(super) fn lower_list_element_pattern_comprehension_op(
        &self,
        list_expr: ExprId,
        loop_var: VarId,
        child: &GraphPlan,
        pattern_output: VarId,
        filter: Option<ExprId>,
        projection: Option<ExprId>,
        output: VarId,
        input: LogicalPlan,
        exprs: &ExprArena,
        var_map: &mut VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        use datafusion::arrow::datatypes::{DataType, Field};
        use datafusion::common::Column;
        use datafusion::functions::core::expr_fn::{coalesce, get_field};
        use datafusion::functions_nested::expr_fn::array_element;
        use datafusion::scalar::ScalarValue;

        const LIST: &str = "__gf_list_source";
        const INDICES: &str = "__gf_list_indices";
        const INDEX: &str = "__gf_list_index";

        let outer_columns = plan_columns(&input);
        let row_keys = input
            .schema()
            .iter()
            .filter(|(_, field)| matches!(field.name().as_str(), "node_id" | "edge_id"))
            .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name().to_owned()))
            .collect::<Vec<_>>();
        if row_keys.is_empty() {
            return Err(LoweringError::UnsupportedExpr(
                "graph-valued list comprehension requires an outer entity identity".into(),
            ));
        }
        let row_key_aliases = (0..row_keys.len())
            .map(|index| format!("__gf_list_row_key_{index}"))
            .collect::<Vec<_>>();
        let list = self
            .expr_lowerer(exprs, var_map)
            .with_input_schema(input.schema().clone())
            .lower(list_expr)?;
        let DataType::List(item) = list.get_type(input.schema()).map_unsupported_expr()? else {
            return Err(LoweringError::InvalidType(
                "nested pattern comprehension source must be a list".into(),
            ));
        };
        let DataType::Struct(node_fields) = item.data_type() else {
            return Err(LoweringError::InvalidType(
                "nested pattern comprehension elements must be node values".into(),
            ));
        };

        let mut indexed_projection = outer_columns.clone();
        indexed_projection.push(list.alias(LIST));
        let indexed = LogicalPlanBuilder::from(input)
            .project(indexed_projection)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;

        let mut index_projection = plan_columns(&indexed);
        index_projection
            .push(list_index_range(DfExpr::Column(Column::from_name(LIST))).alias(INDICES));
        let with_indices = LogicalPlanBuilder::from(indexed.clone())
            .project(index_projection)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        let index_field = Field::new(INDEX, DataType::Int64, true);
        let expanded = LogicalPlan::Extension(Extension {
            node: Arc::new(UnwindNode::new(
                Arc::new(with_indices),
                DfExpr::Column(Column::from_name(INDICES)),
                INDEX,
                &index_field,
            )),
        });

        let element = array_element(
            DfExpr::Column(Column::from_name(LIST)),
            DfExpr::Column(Column::from_name(INDEX)) + datafusion::logical_expr::lit(1_i64),
        );
        let loop_alias = var_alias(loop_var);
        let mut element_projection = plan_columns(&expanded);
        element_projection.extend(node_fields.iter().map(|field| {
            get_field(element.clone(), field.name())
                .alias_qualified(Some(loop_alias.as_str()), field.name())
        }));
        let expanded = LogicalPlanBuilder::from(expanded)
            .project(element_projection)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        var_map.insert(loop_var, loop_alias.clone());

        let matched =
            self.lower_pattern_comprehension_op(child, pattern_output, expanded, var_map)?;
        let clause_lowerer = self
            .expr_lowerer(exprs, var_map)
            .with_input_schema(matched.schema().clone());
        let filtered = if let Some(predicate) = filter {
            lower_filter(predicate, matched, &clause_lowerer)?
        } else {
            matched
        };
        let value = match projection {
            Some(expr) => self
                .expr_lowerer(exprs, var_map)
                .with_input_schema(filtered.schema().clone())
                .lower(expr)?,
            None => DfExpr::Column(Column::new(Some(loop_alias.as_str()), "node_uuid")),
        };
        let element_type = value.get_type(filtered.schema()).map_unsupported_expr()?;
        let output_alias = format!("__gf_list_pattern_{}", output.0);
        let ordered = array_agg(value)
            .order_by(vec![
                DfExpr::Column(Column::from_name(INDEX)).sort(true, true),
            ])
            .build()
            .map_unsupported_expr()?
            .alias(&output_alias);
        let collected = LogicalPlanBuilder::from(filtered)
            .aggregate(
                row_keys
                    .iter()
                    .zip(&row_key_aliases)
                    .map(|(column, alias)| DfExpr::Column(column.clone()).alias(alias))
                    .collect::<Vec<_>>(),
                vec![ordered],
            )
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        let joined = LogicalPlanBuilder::from(indexed)
            .join(
                collected,
                JoinType::Left,
                (
                    row_keys,
                    row_key_aliases
                        .iter()
                        .map(|alias| Column::from_name(alias.clone()))
                        .collect::<Vec<_>>(),
                ),
                None,
            )
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;

        let empty = ScalarValue::List(ScalarValue::new_list(&[], &element_type, true));
        let null = ScalarValue::new_null_list(element_type, true, 1);
        let result_list = datafusion::logical_expr::when(
            DfExpr::Column(Column::from_name(LIST)).is_null(),
            DfExpr::Literal(null, None),
        )
        .otherwise(coalesce(vec![
            DfExpr::Column(Column::from_name(output_alias.clone())),
            DfExpr::Literal(empty, None),
        ]))
        .map_unsupported_expr()?
        .alias(&output_alias);
        let mut final_projection = outer_columns;
        final_projection.push(result_list);
        let result = LogicalPlanBuilder::from(joined)
            .project(final_projection)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        var_map.insert(output, output_alias);
        Ok(result)
    }

    pub(super) fn lower_optional_op(
        &self,
        child: &GraphPlan,
        input: LogicalPlan,
        var_map: &mut VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        let mut child_vm = VarMap::new();
        let child_plan = self.lower_pipeline(&child.ops, &child.exprs, &mut child_vm)?;
        let (join_keys, inner_keep_idx) =
            optional_join_keys(&input, &child_plan, var_map, &child_vm);
        merge_optional_child_vars(&child_vm, var_map);
        promote_optional_entity_vars(&input, &child_plan, &child_vm, var_map);
        let node = OptionalMatchNode::new(
            Arc::new(input),
            Arc::new(child_plan),
            join_keys,
            inner_keep_idx,
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }
}

/// Compute an `OPTIONAL MATCH`'s join keys and the inner output field list.
///
/// The join keys are the variables bound in **both** the outer scope and the
/// optional sub-plan (the implicit shared variables of the pattern, e.g. `a` in
/// `MATCH (a) OPTIONAL MATCH (a)-[:R]->(b)`). For each such variable, the key is
/// the pair of `node_id` column indices `(outer_idx, inner_idx)` resolved
/// against the respective qualified schemas.
///
/// The inner output fields are the child plan's columns with **every** column of
/// a shared variable removed — not just its `node_id` join key. Once fixed
/// single-hop hops lower to a real join (#718), the inner plan carries all of a
/// shared variable's topology columns (`node_uuid`, `type_id`, …), and those
/// fully duplicate the outer side; appending them would build a schema with
/// duplicate `var_<shared>` qualified fields. The remaining (non-shared) inner
/// columns get appended nullable to form the node's output.
/// Register the optional-side variables the child introduced (those NOT
/// already in the outer scope) so a downstream `RETURN m.x` resolves.
///
/// Each keeps the column name the child registered — usually the bare
/// `var_<v>` alias, but a var-length edge var registers fully qualified
/// `var_<v>.rels` (#709), and the OptionalMatch node appends the child's
/// columns under their child qualifiers either way (see `inner_keep_idx`).
/// Shared vars keep their existing outer alias.
fn merge_optional_child_vars(child_vm: &VarMap, var_map: &mut VarMap) {
    for v in child_vm.var_ids() {
        if var_map.get(v).is_none() {
            let child_col = child_vm
                .get(v)
                .expect("var_ids yields only registered vars")
                .to_owned();
            var_map.insert(v, child_col);
        }
    }
}

fn promote_optional_entity_vars(
    outer: &LogicalPlan,
    inner: &LogicalPlan,
    child_vm: &VarMap,
    var_map: &mut VarMap,
) {
    use datafusion::common::TableReference;

    for var in child_vm.var_ids() {
        let Some(outer_alias) = var_map.get(var) else {
            continue;
        };
        let Some(inner_alias) = child_vm.get(var) else {
            continue;
        };
        let outer_qual = TableReference::bare(outer_alias);
        let inner_qual = TableReference::bare(inner_alias);
        let outer_is_entity = ["node_uuid", "edge_uuid"].iter().any(|name| {
            outer
                .schema()
                .index_of_column_by_name(Some(&outer_qual), name)
                .is_some()
        });
        let inner_is_entity = ["node_uuid", "edge_uuid"].iter().any(|name| {
            inner
                .schema()
                .index_of_column_by_name(Some(&inner_qual), name)
                .is_some()
        });
        if !outer_is_entity && inner_is_entity {
            var_map.insert(var, inner_alias.to_owned());
        }
    }
}

fn full_subquery_needs_outer_input(child: &GraphPlan, outer_vm: &VarMap) -> bool {
    outer_vm.var_ids().any(|var| {
        plan_references_var(child, var) && !child.ops.iter().any(|op| graph_op_binds_var(op, var))
    })
}

fn plan_references_var(plan: &GraphPlan, var: VarId) -> bool {
    let expression_reference = (0..plan.exprs.len()).any(|index| {
        let index = u32::try_from(index).expect("ExprArena length is capped at u32::MAX");
        matches!(plan.exprs.get(ExprId(index)), IrExpr::VarRef(found) if *found == var)
    });
    expression_reference
        || plan.ops.iter().any(|op| {
            graph_op_binds_var(op, var)
                || match op {
                    GraphOp::Optional { child }
                    | GraphOp::Exists { child, .. }
                    | GraphOp::PatternComprehension { child, .. }
                    | GraphOp::ListElementPatternComprehension { child, .. } => {
                        plan_references_var(child, var)
                    }
                    GraphOp::Union { inputs, .. } => {
                        inputs.iter().any(|input| plan_references_var(input, var))
                    }
                    _ => false,
                }
        })
}

fn graph_op_binds_var(op: &GraphOp, var: VarId) -> bool {
    match op {
        GraphOp::NodeScan { var: found, .. }
        | GraphOp::EdgeScan { var: found, .. }
        | GraphOp::TypedEdgeScan { var: found, .. } => *found == var,
        GraphOp::Expand { src, edge, dst, .. } => *src == var || *edge == var || *dst == var,
        _ => false,
    }
}

fn optional_join_keys(
    outer: &LogicalPlan,
    inner: &LogicalPlan,
    outer_vm: &VarMap,
    inner_vm: &VarMap,
) -> (Vec<(usize, usize)>, Vec<usize>) {
    use datafusion::common::TableReference;

    let outer_schema = outer.schema();
    let inner_schema = inner.schema();

    let shared_cols = outer_vm
        .var_ids()
        .filter_map(|var| {
            let outer_col = outer_vm.get(var)?;
            let inner_col = inner_vm.get(var)?;
            Some((outer_col.to_owned(), inner_col.to_owned()))
        })
        .collect::<Vec<_>>();

    // Qualifiers of the variables shared between the outer scope and the
    // optional sub-plan; their columns are sourced entirely from the outer side.
    let shared_quals: std::collections::HashSet<String> = shared_cols
        .iter()
        .filter_map(|(outer_col, inner_col)| {
            let outer_qual = TableReference::bare(outer_col.clone());
            ["node_id", "node_uuid", "edge_uuid"]
                .iter()
                .any(|identity| {
                    outer_schema
                        .index_of_column_by_name(Some(&outer_qual), identity)
                        .is_some()
                })
                .then(|| inner_col.clone())
        })
        .collect();

    let mut join_keys: Vec<(usize, usize)> = Vec::new();
    for (outer_col, inner_col) in shared_cols {
        let outer_qual = TableReference::bare(outer_col.clone());
        let inner_qual = TableReference::bare(inner_col);
        for identity in ["node_id", "node_uuid", "edge_uuid"] {
            if let (Some(o), Some(i)) = (
                outer_schema.index_of_column_by_name(Some(&outer_qual), identity),
                inner_schema.index_of_column_by_name(Some(&inner_qual), identity),
            ) {
                join_keys.push((o, i));
                break;
            }
        }
        if !join_keys.iter().any(|(_, inner)| {
            ["node_id", "node_uuid", "edge_uuid"]
                .iter()
                .any(|identity| {
                    inner_schema.index_of_column_by_name(Some(&inner_qual), identity)
                        == Some(*inner)
                })
        }) && let Some(o) = outer_schema.index_of_column_by_name(None, &outer_col)
            && let Some(i) = inner_schema
                .index_of_column_by_name(Some(&inner_qual), "node_uuid")
                .or_else(|| inner_schema.index_of_column_by_name(Some(&inner_qual), "edge_uuid"))
        {
            join_keys.push((o, i));
        }
    }

    // Keep the inner columns whose qualifier is NOT a shared variable (those are
    // carried by the outer side); the node makes the remainder nullable. A field
    // with no qualifier (e.g. a computed column) is kept.
    let inner_keep_idx = inner_schema
        .iter()
        .enumerate()
        .filter(|(_, (q, _))| q.is_none_or(|q| !shared_quals.contains(q.table())))
        .map(|(i, _)| i)
        .collect();

    (join_keys, inner_keep_idx)
}

fn schema_join_column(
    schema: &datafusion::common::DFSchema,
    index: usize,
) -> datafusion::common::Column {
    let (qualifier, field) = schema
        .iter()
        .nth(index)
        .expect("join key index must point at a schema field");
    datafusion::common::Column::new(qualifier.cloned(), field.name().to_owned())
}

fn plan_columns(plan: &LogicalPlan) -> Vec<DfExpr> {
    use datafusion::common::Column;
    plan.schema()
        .iter()
        .map(|(qualifier, field)| {
            DfExpr::Column(Column::new(qualifier.cloned(), field.name().to_owned()))
        })
        .collect()
}

#[cfg(test)]
mod tests;
