//! Writes lowering.

use super::{
    Arc, CreatePattern, DeleteTarget, DfExpr, ExprArena, ExprId, Extension, GfError,
    GraphCreateNode, GraphDeleteNode, GraphOp, GraphPlanLowerer, GraphRemoveNode, GraphSetNode,
    IrExpr, IrLiteral, LogicalPlan, LoweringError, LoweringSnapshot, MapUnsupportedExpr,
    RecordBatch, RemovePropItem, RemoveTarget, ResolvedEdgeSpec, ResolvedNodeSpec, SetPropItem,
    SetTarget, VarId, VarMap, var_alias,
};

impl GraphPlanLowerer {
    /// Lower a statement-local relational segment with buffered node topology
    /// visible alongside persisted nodes.
    pub fn lower_write_segment(
        &self,
        ops: &[GraphOp],
        exprs: &ExprArena,
        var_map: &mut VarMap,
        input_schema: datafusion::common::DFSchemaRef,
        pending_nodes: &RecordBatch,
    ) -> Result<LogicalPlan, GfError> {
        let input = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: true,
            schema: input_schema,
        });
        self.lower_pipeline_from(ops, exprs, var_map, input, Some(pending_nodes))
            .and_then(|plan| self.attach_graph_contract(plan))
            .map_err(GfError::from)
    }

    /// Resolve a `CREATE` pattern to its executable node/edge specs (label and
    /// relation names resolved, property maps evaluated to literals).
    ///
    /// The same resolution `lower_create` bakes into a [`GraphCreateNode`];
    /// exposed so the statement driver can run the create phase without a
    /// logical plan node.
    ///
    /// # Errors
    /// Returns [`GfError::Plan`] when a property map cannot be evaluated.
    pub fn resolve_create_pattern(
        &self,
        pattern: &CreatePattern,
        exprs: &ExprArena,
        var_map: &VarMap,
        input_schema: &datafusion::common::DFSchemaRef,
    ) -> Result<(Vec<ResolvedNodeSpec>, Vec<ResolvedEdgeSpec>), GfError> {
        self.create_specs(pattern, exprs, var_map, Some(input_schema))
            .map_err(GfError::from)
    }

    /// Register freshly-created node shapes in the write driver so a
    /// terminal `RETURN n` can materialize a same-statement created node value.
    pub fn register_created_node_shapes(&self, nodes: &[ResolvedNodeSpec]) {
        let mut shapes = self.node_shapes.write().expect("node shapes lock poisoned");
        for spec in nodes.iter().filter(|n| !n.is_reference) {
            let prop_names = spec
                .properties
                .iter()
                .map(|(k, _)| k.clone())
                .chain(spec.computed_properties.iter().map(|(k, _)| k.clone()))
                .collect();
            shapes.insert(spec.var, crate::expr::NodeShape { prop_names });
        }
    }

    /// Extend a bound node's same-statement value shape after a dynamic write.
    pub fn register_node_property_shape(&self, var: VarId, name: &str) {
        let mut shapes = self.node_shapes.write().expect("node shapes lock poisoned");
        let shape = shapes
            .entry(var.0)
            .or_insert_with(|| crate::expr::NodeShape { prop_names: vec![] });
        if !shape.prop_names.iter().any(|existing| existing == name) {
            shape.prop_names.push(name.to_owned());
            shape.prop_names.sort();
        }
    }

    /// Lower a [`GraphOp::Create`] into a self-contained [`GraphCreateNode`]
    /// wrapped as a DataFusion [`Extension`].
    ///
    /// Resolves each spec's label/relation-type name (from the ontology maps)
    /// and evaluates its property map here: constant values bake to literals,
    /// row-dependent values lower to DataFusion `Expr`s over the input columns
    /// (evaluated per row by the execution layer) — so the executor needs no
    /// access to the IR arena or ontology.  The write target (directory + mode)
    /// must have been supplied via [`new_for_writes`](Self::new_for_writes).
    pub(super) fn lower_create(
        &self,
        pattern: &CreatePattern,
        exprs: &ExprArena,
        input: LogicalPlan,
        var_map: &mut VarMap,
        feeds_read: bool,
    ) -> Result<LogicalPlan, LoweringError> {
        let () = self.write_target.then_some(()).ok_or_else(|| {
            LoweringError::UnsupportedExpr(
                "CREATE requires a write target; lower via new_for_writes".into(),
            )
        })?;

        let (nodes, edges) = self.create_specs(pattern, exprs, var_map, Some(input.schema()))?;

        // Write-result RETURN (#814): when a read clause follows CREATE, build
        // an output relation so trailing RETURN/WITH can read each created node's
        // `var_<n>`-qualified columns. MATCH/WITH-bound reference nodes and
        // edges are supported here: references arrive via input passthrough,
        // edges are minted but project no columns (so `RETURN r` remains a loud
        // unresolved-var error rather than a false pass).
        if feeds_read {
            let out_schema = Self::created_rows_schema(&nodes, input.schema())?;
            self.register_created_node_shapes(&nodes);
            for spec in &nodes {
                // Reference vars are already registered by the preceding
                // MATCH/WITH. Re-registering would clobber their node shape and
                // make `RETURN a` / `a.prop` lose data.
                if spec.is_reference {
                    continue;
                }
                let v = VarId(spec.var);
                var_map.insert(v, var_alias(v));
            }
            let node = GraphCreateNode::new_emitting(Arc::new(input), nodes, edges, out_schema)
                .with_semantic_composition_fingerprint(
                    self.catalog
                        .as_ref()
                        .and_then(LoweringSnapshot::semantic_composition_fingerprint),
                );
            return Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            }));
        }

        let node = GraphCreateNode::new(Arc::new(input), nodes, edges)
            .with_semantic_composition_fingerprint(
                self.catalog
                    .as_ref()
                    .and_then(LoweringSnapshot::semantic_composition_fingerprint),
            );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// Build the created-entity output schema for emit-rows mode (#814): the
    /// input columns (passed through), then per freshly-minted node spec its
    /// `var_<n>`-qualified `node_uuid`/`node_id`/`type_id` identity columns and
    /// one column per property (a literal's type via its `ScalarValue`, a
    /// computed value's via the expr's logical type).
    fn created_rows_schema(
        nodes: &[ResolvedNodeSpec],
        input_schema: &datafusion::common::DFSchemaRef,
    ) -> Result<datafusion::common::DFSchemaRef, LoweringError> {
        use std::collections::HashMap;

        use datafusion::arrow::datatypes::{DataType, Field};
        use datafusion::common::{DFSchema, TableReference};
        use datafusion::logical_expr::ExprSchemable;

        let mut qualified: Vec<(Option<TableReference>, Arc<Field>)> = input_schema
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();

        for spec in nodes {
            // Reference vars are not minted; their `var_<n>` columns already
            // arrive via the input passthrough above. Re-adding them would build
            // a duplicate-qualified schema that DataFusion rejects.
            if spec.is_reference {
                continue;
            }
            let qual = TableReference::bare(var_alias(VarId(spec.var)));
            let mut push = |name: &str, ty: DataType, nullable: bool| {
                qualified.push((Some(qual.clone()), Arc::new(Field::new(name, ty, nullable))));
            };
            push("node_uuid", DataType::FixedSizeBinary(16), false);
            push("node_id", DataType::UInt64, false);
            push("type_id", DataType::UInt32, false);
            push(
                "type_ids",
                DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new(
                    "item",
                    DataType::UInt32,
                    false,
                ))),
                false,
            );
            for (name, lit) in &spec.properties {
                Self::ensure_created_node_emit_property_name(name)?;
                let scalar = crate::expr::ir_literal_to_scalar(lit);
                push(name, scalar.data_type(), true);
            }
            for (name, expr) in &spec.computed_properties {
                Self::ensure_created_node_emit_property_name(name)?;
                let ty = expr.get_type(input_schema).map_unsupported_expr()?;
                push(name, ty, true);
            }
        }
        let schema =
            DFSchema::new_with_metadata(qualified, HashMap::new()).map_unsupported_expr()?;
        Ok(Arc::new(schema))
    }

    fn ensure_created_node_emit_property_name(name: &str) -> Result<(), LoweringError> {
        if matches!(name, "node_uuid" | "node_id" | "type_id" | "type_ids") {
            return Err(LoweringError::UnsupportedExpr(format!(
                "CREATE property `{name}` collides with a reserved node topology field"
            )));
        }
        Ok(())
    }

    /// Resolve a `CREATE` pattern's node and edge specs: label / relation
    /// names from the catalog maps, property maps evaluated to literals.
    fn create_specs(
        &self,
        pattern: &CreatePattern,
        exprs: &ExprArena,
        var_map: &VarMap,
        input_schema: Option<&datafusion::common::DFSchemaRef>,
    ) -> Result<(Vec<ResolvedNodeSpec>, Vec<ResolvedEdgeSpec>), LoweringError> {
        let nodes: Vec<ResolvedNodeSpec> = pattern
            .nodes
            .iter()
            .map(|n| {
                let (properties, computed_properties) =
                    eval_map_literal(self, n.properties, exprs, var_map, input_schema)?;
                Ok(ResolvedNodeSpec {
                    var: n.var.0,
                    label_ids: n.labels.clone(),
                    label_names: n
                        .labels
                        .iter()
                        .filter_map(|t| self.type_id_to_entity_name.get(t).cloned())
                        .collect(),
                    properties,
                    computed_properties,
                    is_reference: n.is_reference,
                })
            })
            .collect::<Result<_, LoweringError>>()?;

        let edges: Vec<ResolvedEdgeSpec> = pattern
            .edges
            .iter()
            .map(|e| {
                let (properties, computed_properties) =
                    eval_map_literal(self, e.properties, exprs, var_map, input_schema)?;
                Ok(ResolvedEdgeSpec {
                    var: e.var.0,
                    src: e.src.0,
                    dst: e.dst.0,
                    rel_type_id: e.rel_type,
                    rel_type_name: e
                        .rel_type
                        .and_then(|t| self.type_id_to_rel_name.get(&t).cloned()),
                    direction: e.direction,
                    properties,
                    computed_properties,
                })
            })
            .collect::<Result<_, LoweringError>>()?;
        Ok((nodes, edges))
    }

    /// Lower a [`GraphOp::Delete`] into a self-contained [`GraphDeleteNode`]
    /// wrapped as a DataFusion [`Extension`] (#740).
    ///
    /// Each target var's node-vs-edge kind is resolved from `input`'s schema:
    /// a node var carries a `var_<n>.node_uuid` column, an edge var a
    /// `var_<n>.edge_uuid` column (the qualified topology columns the scans
    /// produce). A var whose identity column is absent — i.e. it was not bound
    /// by a preceding scan — is a lowering error. The write target (directory +
    /// mode) must have been supplied via [`new_for_writes`](Self::new_for_writes).
    pub(super) fn lower_delete_op(
        &self,
        vars: &[VarId],
        detach: bool,
        input: LogicalPlan,
    ) -> Result<LogicalPlan, LoweringError> {
        use datafusion::common::TableReference;

        let () = self.write_target.then_some(()).ok_or_else(|| {
            LoweringError::UnsupportedExpr(
                "DELETE requires a write target; lower via new_for_writes".into(),
            )
        })?;

        let schema = input.schema();
        let targets: Vec<DeleteTarget> = vars
            .iter()
            .map(|var| {
                let qual = TableReference::bare(var_alias(*var));
                let is_node = schema
                    .index_of_column_by_name(Some(&qual), "node_uuid")
                    .is_some();
                let is_edge = schema
                    .index_of_column_by_name(Some(&qual), "edge_uuid")
                    .is_some();
                match (is_node, is_edge) {
                    (true, _) => Ok(DeleteTarget {
                        var: var.0,
                        is_edge: false,
                    }),
                    (false, true) => Ok(DeleteTarget {
                        var: var.0,
                        is_edge: true,
                    }),
                    (false, false) => Err(LoweringError::UnsupportedExpr(format!(
                        "DELETE target var_{} has no node_uuid/edge_uuid column in the \
                         input — it must be bound by a preceding MATCH",
                        var.0
                    ))),
                }
            })
            .collect::<Result<_, LoweringError>>()?;

        let node = GraphDeleteNode::new(Arc::new(input), targets, detach);
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// Resolve a write-target variable's node-vs-edge kind from `schema`.
    ///
    /// A node var carries a `var_<n>.node_uuid` column; an edge var a
    /// `var_<n>.edge_uuid` column. An edge var must also carry the
    /// `var_<n>.rel_type_name` column — the per-row file stem for edge property
    /// writes (stored for exploratory routes and projected from the authenticated
    /// catalog route for typed scans). Returns
    /// `is_edge` or a lowering error.
    fn resolve_write_kind(
        schema: &datafusion::common::DFSchemaRef,
        var: VarId,
        clause: &str,
    ) -> Result<bool, LoweringError> {
        use datafusion::common::TableReference;

        let qual = TableReference::bare(var_alias(var));
        let is_node = schema
            .index_of_column_by_name(Some(&qual), "node_uuid")
            .is_some();
        let is_edge = schema
            .index_of_column_by_name(Some(&qual), "edge_uuid")
            .is_some();
        match (is_node, is_edge) {
            (true, _) => Ok(false),
            (false, true) => {
                if schema
                    .index_of_column_by_name(Some(&qual), "rel_type_name")
                    .is_some()
                {
                    Ok(true)
                } else {
                    Err(LoweringError::UnsupportedExpr(format!(
                        "{clause} on an edge requires a known relation type \
                         (e.g. `-[r:KNOWS]->`); an untyped edge write is not yet \
                         supported (follow-up to #791)"
                    )))
                }
            }
            (false, false) => Err(LoweringError::UnsupportedExpr(format!(
                "{clause} target var_{} has no node_uuid/edge_uuid column in the \
                 input — it must be bound by a preceding MATCH",
                var.0
            ))),
        }
    }

    /// Lower a [`GraphOp::Set`] into a [`GraphSetNode`] Extension (#791).
    ///
    /// Each item's value expression is lowered to a DataFusion `Expr` against
    /// the input schema (so `var_<n>.prop` and cross-var columns resolve) and
    /// evaluated per matched row by the execution layer — values are **not**
    /// coerced to literals here. Node/edge kind is resolved from the input
    /// schema; an untyped edge target is rejected.
    pub(super) fn lower_set_op(
        &self,
        items: &[SetPropItem],
        input: LogicalPlan,
        exprs: &ExprArena,
        var_map: &VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        let () = self.write_target.then_some(()).ok_or_else(|| {
            LoweringError::UnsupportedExpr(
                "SET requires a write target; lower via new_for_writes".into(),
            )
        })?;

        // Lower each value expr against the matched-row schema so `var_<n>.prop`
        // and cross-var columns resolve; evaluated per row in the exec layer.
        let expr_lowerer = self.expr_lowerer(exprs, var_map);
        let schema = input.schema();
        let targets: Vec<SetTarget> = items
            .iter()
            .map(|item| {
                let is_edge = Self::resolve_write_kind(schema, item.target, "SET")?;
                let value = expr_lowerer.lower(item.value)?;
                Ok(SetTarget {
                    var: item.target.0,
                    is_edge,
                    prop_name: item.prop_name.clone(),
                    value,
                })
            })
            .collect::<Result<_, LoweringError>>()?;

        let node = GraphSetNode::new(Arc::new(input), targets);
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// Lower a [`GraphOp::Remove`] into a [`GraphRemoveNode`] Extension (#791) —
    /// the value-less dual of [`lower_set_op`](Self::lower_set_op).
    pub(super) fn lower_remove_op(
        &self,
        items: &[RemovePropItem],
        input: LogicalPlan,
    ) -> Result<LogicalPlan, LoweringError> {
        let () = self.write_target.then_some(()).ok_or_else(|| {
            LoweringError::UnsupportedExpr(
                "REMOVE requires a write target; lower via new_for_writes".into(),
            )
        })?;

        let schema = input.schema();
        let targets: Vec<RemoveTarget> = items
            .iter()
            .map(|item| {
                let is_edge = Self::resolve_write_kind(schema, item.target, "REMOVE")?;
                Ok(RemoveTarget {
                    var: item.target.0,
                    is_edge,
                    prop_name: item.prop_name.clone(),
                })
            })
            .collect::<Result<_, LoweringError>>()?;

        let node = GraphRemoveNode::new(Arc::new(input), targets);
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }
}

/// Evaluate an optional `CREATE` property map, splitting each value into either
/// a **constant** literal or a **row-dependent** DataFusion `Expr`.
///
/// The expression must be an [`IrExpr::MapLiteral`]. A plain [`IrExpr::Literal`]
/// value is taken directly (the fast path). A computed value (e.g. `date({...})`,
/// `1 + 2`) is **constant-folded**: lowered and, if it reduces to a literal
/// scalar, converted back to an [`IrLiteral`] (#814 Slice 1a). A *row-dependent*
/// value (a variable reference like `{n: x}` from a driving `UNWIND`/`MATCH`)
/// does not fold; it is lowered against `var_map` to an `Expr` over the input
/// columns and returned in the second vec, for per-row evaluation by the
/// execution layer (#814 Slice 1b).
///
/// Returns `(constant literals, row-dependent exprs)`.
type EvaluatedProps = (Vec<(String, IrLiteral)>, Vec<(String, DfExpr)>);

/// Constant-fold a lowered value expression to a scalar: a literal is taken
/// directly; anything else is evaluated against a single empty row (so a pure
/// constant like `1 + 2` folds, while a column/variable reference fails to
/// resolve against the empty schema and yields `None` — kept deferred). (#814)
pub(super) fn const_eval_scalar(df: &DfExpr) -> Option<datafusion::scalar::ScalarValue> {
    use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::execution::context::ExecutionProps;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion::scalar::ScalarValue;

    if let DfExpr::Literal(scalar, _) = df {
        return Some(scalar.clone());
    }
    let schema = datafusion::common::DFSchema::empty();
    let phys = create_physical_expr(df, &schema, &ExecutionProps::new()).ok()?;
    let batch = RecordBatch::try_new_with_options(
        std::sync::Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .ok()?;
    let array = phys.evaluate(&batch).ok()?.into_array(1).ok()?;
    ScalarValue::try_from_array(&array, 0).ok()
}

pub(super) fn eval_map_literal(
    lowerer: &GraphPlanLowerer,
    id: Option<ExprId>,
    exprs: &ExprArena,
    var_map: &VarMap,
    input_schema: Option<&datafusion::common::DFSchemaRef>,
) -> Result<EvaluatedProps, LoweringError> {
    let Some(id) = id else {
        return Ok((Vec::new(), Vec::new()));
    };
    let IrExpr::MapLiteral(pairs) = exprs.get(id) else {
        return Err(LoweringError::UnsupportedExpr(format!(
            "CREATE properties must be a map literal, got: {:?}",
            exprs.get(id)
        )));
    };
    let mut literals = Vec::new();
    let mut computed = Vec::new();
    for (k, vexpr) in pairs {
        if let IrExpr::Literal(lit) = exprs.get(*vexpr) {
            reject_map_property_value(k, lit)?;
            literals.push((k.clone(), lit.clone()));
            continue;
        }
        // Lower against the matched-row scope so a column/variable reference
        // resolves; a genuinely constant expression is column-free and still
        // constant-folds (slice 1a), a row-dependent one is deferred to exec.
        let mut expr_lowerer = lowerer.expr_lowerer(exprs, var_map);
        if let Some(schema) = input_schema {
            expr_lowerer = expr_lowerer.with_input_schema(schema.clone());
        }
        let df = expr_lowerer.lower(*vexpr)?;
        match const_eval_scalar(&df) {
            // Constant-foldable AND storable → bake as a literal (slice 1a). A
            // constant the storage layer can't represent yet keeps its error.
            Some(scalar) => {
                let lit = crate::expr::scalar_to_ir_literal(&scalar)?;
                reject_map_property_value(k, &lit)?;
                literals.push((k.clone(), lit));
            }
            // Row-dependent: evaluated per minted row by the execution layer.
            None => computed.push((k.clone(), df)),
        }
    }
    Ok((literals, computed))
}

pub(super) fn reject_map_property_value(
    prop_name: &str,
    lit: &IrLiteral,
) -> Result<(), LoweringError> {
    if contains_map_literal(lit) {
        return Err(LoweringError::UnsupportedExpr(format!(
            "CREATE property `{prop_name}` cannot store map values"
        )));
    }
    Ok(())
}

pub(super) fn contains_map_literal(lit: &IrLiteral) -> bool {
    match lit {
        IrLiteral::Map(_) => true,
        IrLiteral::List(items) => items.iter().any(contains_map_literal),
        _ => false,
    }
}

#[cfg(test)]
mod tests;
