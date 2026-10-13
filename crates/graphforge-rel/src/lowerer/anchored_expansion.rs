//! Decorrelation of a pattern that restarts at a node the input already bound.
//!
//! `WITH person, liker, max(..) AS t MATCH (person)<-[:HAS_CREATOR]-(m)<-[:LIKES]-(liker)`
//! lowers, in the per-row form, to an expansion of `person`'s whole relationship
//! set once for every input row: the intermediate is `rows x relationships`
//! although the expansion depends only on `person`. This module recognises that
//! shape and lowers it as an [`AnchoredExpansionNode`] instead: the pattern is
//! expanded once per distinct anchor and hash-joined back to the input.
//!
//! The rewrite applies to a run of operators that follows an anchor `NodeScan`
//! whose input is already materialized by an aggregation or `DISTINCT` barrier:
//!
//! * `Expand` of one fixed hop from the anchor or from a node the run bound;
//! * `NodeScan` of a node the run bound;
//! * `RelationshipUnique` over relationships the run bound;
//! * `Filter`, split into conjuncts.
//!
//! A conjunct that reads only variables the run bound is applied inside the
//! expansion. A top-level equality between such an expression and an input
//! column becomes an additional join key. Every other conjunct stays above the
//! join. A node the pattern reaches that the input already bound (a `liker`
//! carried through the `WITH`) is joined on its identity instead of being
//! checked afterwards.
//!
//! The run ends at the first operator it cannot take; everything after it
//! lowers on the joined plan exactly as before. Operators that write, call
//! procedures, or depend on input order are never part of a run.

use super::{
    AnchoredExpansionNode, Arc, CorrelatedSeedNode, DfExpr, Direction, ExprArena, ExprId,
    Extension, GraphOp, GraphPlanLowerer, HashMap, HashSet, IrExpr, LogicalPlan,
    LogicalPlanBuilder, LoweringError, MapUnsupportedExpr, VarId, VarMap, var_alias,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, TableReference};
use graphforge_ir::{BinaryOpKind, calls_volatile_function, conjuncts, referenced_vars};

impl GraphPlanLowerer {
    /// Whether the pipeline `ordered` may contain anchored expansions at all.
    ///
    /// Reachability expansions keep their own operator order, writes and
    /// procedure calls must observe the per-row order, and the relational
    /// reference form exists to be the unrewritten baseline.
    pub(super) fn may_decorrelate(&self, ordered: &[&GraphOp], reachability_empty: bool) -> bool {
        reachability_empty
            && self.read_snapshot().is_some()
            && !self.write_target
            && !self.relational_reference()
            && !ordered.iter().any(|op| {
                matches!(
                    op,
                    GraphOp::Create { .. }
                        | GraphOp::Merge { .. }
                        | GraphOp::Delete { .. }
                        | GraphOp::Set { .. }
                        | GraphOp::Remove { .. }
                        | GraphOp::Call { .. }
                )
            })
    }

    /// The anchor of an expansion run that would start after `ordered[at]`.
    ///
    /// `ordered[at]` must scan a node the input already bound, and the nearest
    /// operator that shapes the rows before it must be an aggregation or a
    /// `DISTINCT`: only then is the input already materialized, so retaining it
    /// for the join costs no early termination. A sort, limit or skip is not
    /// accepted because the join does not preserve the input order. A `LIMIT`
    /// the rows stream into is not accepted either: the per-row expansion stops
    /// at the limit, the joined one would not (see [`limit_streams_from`]).
    pub(super) fn anchored_expansion_anchor(
        ordered: &[&GraphOp],
        at: usize,
        var_map: &VarMap,
    ) -> Option<VarId> {
        let GraphOp::NodeScan { var, .. } = ordered.get(at)? else {
            return None;
        };
        var_map.get(*var)?;
        for op in ordered[..at].iter().rev() {
            match op {
                GraphOp::Filter { .. }
                | GraphOp::Project {
                    distinct: false, ..
                }
                | GraphOp::With {
                    distinct: false, ..
                } => {}
                GraphOp::Aggregate { .. }
                | GraphOp::Project { distinct: true, .. }
                | GraphOp::With { distinct: true, .. } => {
                    return (!limit_streams_from(&ordered[at + 1..])).then_some(*var);
                }
                _ => return None,
            }
        }
        None
    }

    /// Lower the longest run of `rest` that can be expanded once per distinct
    /// `anchor` and joined back to `input`.
    ///
    /// Returns the joined plan and the number of operators of `rest` it
    /// consumed, or `None` when no run applies. `None` leaves `var_map`
    /// untouched; the caller lowers `rest` per row as usual. An operator the
    /// decorrelated lowering cannot take ends the run rather than failing: the
    /// per-row lowering then meets and reports it.
    pub(super) fn try_lower_anchored_expansion(
        &self,
        rest: &[&GraphOp],
        exprs: &ExprArena,
        anchor: VarId,
        input: &LogicalPlan,
        var_map: &mut VarMap,
    ) -> Result<Option<(LogicalPlan, usize)>, LoweringError> {
        let anchor_alias = var_alias(anchor);
        if var_map.get(anchor) != Some(anchor_alias.as_str()) {
            return Ok(None);
        }
        if !matches!(rest.first(), Some(GraphOp::Expand { src, .. }) if *src == anchor) {
            return Ok(None);
        }
        let input_schema = input.schema();
        let anchor_id = Column::new(Some(TableReference::bare(anchor_alias.clone())), "node_id");
        let Some(anchor_input_col) = unique_column_index(input_schema, &anchor_id) else {
            return Ok(None);
        };

        // The distinct, non-null anchors, read from the rows the node binds.
        let leaf = CorrelatedSeedNode::new(Arc::clone(input_schema));
        let seed = leaf.id();
        let body = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(leaf),
        }))
        .project(vec![DfExpr::Column(anchor_id.clone())])
        .and_then(|builder| builder.filter(DfExpr::Column(anchor_id.clone()).is_not_null()))
        .and_then(LogicalPlanBuilder::distinct)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()?;

        let mut body_map = VarMap::new();
        body_map.insert(anchor, anchor_alias.clone());
        let mut run = Run {
            lowerer: self,
            exprs,
            real_map: var_map.clone(),
            input_schema: Arc::clone(input_schema),
            anchor,
            input_vars: var_map.var_ids().collect(),
            body_vars: HashSet::new(),
            rebound: Vec::new(),
            body_edges: HashSet::new(),
            postponed: Vec::new(),
            keys: Vec::new(),
            residual: Vec::new(),
            body,
            body_map,
            consumed: 0,
        };
        for (offset, op) in rest.iter().enumerate() {
            if !run.step(op, offset) {
                break;
            }
            run.consumed = offset + 1;
        }
        run.finish(input, rest, seed, &anchor_alias, anchor_input_col, var_map)
    }
}

/// Whether a `LIMIT` is reached from `ops` through operators that pass rows on
/// as they arrive.
///
/// The physical planner stops a limited traversal once the limit is met, and
/// bounds each hop's batches by the limit, only while every operator between
/// the hop and the limit streams. Expanding once per distinct anchor and
/// joining back would read the expansion unbounded, so such a pipeline keeps
/// its per-row expansion. A sort, an aggregation or any other blocking
/// operator in between consumes all its input first and ends the search.
fn limit_streams_from(ops: &[&GraphOp]) -> bool {
    for op in ops {
        match op {
            GraphOp::Limit { .. } | GraphOp::LimitParam { .. } | GraphOp::LimitExpr { .. } => {
                return true;
            }
            GraphOp::NodeScan { .. }
            | GraphOp::Expand { .. }
            | GraphOp::RelationshipUnique { .. }
            | GraphOp::Filter { .. }
            | GraphOp::Skip { .. }
            | GraphOp::SkipParam { .. }
            | GraphOp::SkipExpr { .. }
            | GraphOp::Project {
                distinct: false, ..
            }
            | GraphOp::With {
                distinct: false, ..
            } => {}
            _ => return false,
        }
    }
    false
}

/// An equality `body = input` that may become a join key.
struct KeyCandidate {
    /// Column of the input the equality reads.
    input_col: usize,
    /// Column of the expansion the equality reads.
    body_column: Column,
    /// Variables the input side reads.
    input_vars: Vec<VarId>,
    /// The conjunct, kept for when the key cannot be used.
    conjunct: ExprId,
}

/// The state of one run while it grows.
///
/// Every step computes into locals and commits only on success, so a step that
/// returns `false` leaves the run exactly as the previous step left it.
struct Run<'a> {
    lowerer: &'a GraphPlanLowerer,
    exprs: &'a ExprArena,
    /// The caller's variable map when the run began; not changed by the run.
    real_map: VarMap,
    input_schema: DFSchemaRef,
    anchor: VarId,
    /// Variables the input binds, anchor included.
    input_vars: HashSet<VarId>,
    /// Nodes and relationships the run bound that the input did not.
    body_vars: HashSet<VarId>,
    /// Input nodes the pattern reaches again; joined on identity.
    rebound: Vec<VarId>,
    /// Relationships the run bound.
    body_edges: HashSet<VarId>,
    /// Offsets in `rest` of scans of rebound nodes, lowered after the join.
    postponed: Vec<usize>,
    keys: Vec<KeyCandidate>,
    /// Conjuncts that stay above the join.
    residual: Vec<ExprId>,
    /// The expansion built so far, rooted at the distinct anchors.
    body: LogicalPlan,
    body_map: VarMap,
    /// Operators of `rest` fully taken.
    consumed: usize,
}

impl Run<'_> {
    /// Take `op` into the run. `false` ends the run before `op`.
    fn step(&mut self, op: &GraphOp, offset: usize) -> bool {
        match op {
            GraphOp::NodeScan { var, .. } => self.node_scan(op, *var, offset),
            GraphOp::Expand { .. } => self.expand(op),
            GraphOp::RelationshipUnique { edge, prior_edges } => {
                self.relationship_unique(op, *edge, prior_edges)
            }
            GraphOp::Filter { predicate } => self.filter(*predicate),
            _ => false,
        }
    }

    /// Lower `op` over the expansion with `map`; `None` when it cannot be.
    fn lower_body_op(&self, op: &GraphOp, map: &mut VarMap) -> Option<LogicalPlan> {
        self.lowerer
            .lower_op_with_arena(
                op,
                self.body.clone(),
                self.exprs,
                map,
                false,
                false,
                None,
                None,
            )
            .ok()
    }

    fn node_scan(&mut self, op: &GraphOp, var: VarId, offset: usize) -> bool {
        if self.body_vars.contains(&var) {
            let mut map = self.body_map.clone();
            let Some(next) = self.lower_body_op(op, &mut map) else {
                return false;
            };
            self.body = next;
            self.body_map = map;
            true
        } else if self.rebound.contains(&var) {
            // The input already carries the node's columns; the scan runs on
            // the joined plan, where it adds a label check or missing columns.
            self.postponed.push(offset);
            true
        } else {
            false
        }
    }

    fn expand(&mut self, op: &GraphOp) -> bool {
        let GraphOp::Expand {
            src,
            edge,
            dst,
            rel_ty,
            dir,
            min_hops,
            max_hops,
        } = op
        else {
            return false;
        };
        if *min_hops != 1 || *max_hops != Some(1) || matches!(dir, Direction::Undirected) {
            return false;
        }
        if *src != self.anchor && !self.body_vars.contains(src) {
            return false;
        }
        if *dst == self.anchor
            || *edge == *dst
            || self.input_vars.contains(edge)
            || self.body_vars.contains(edge)
        {
            return false;
        }
        if rel_ty
            .as_ref()
            .is_some_and(|ty| !self.lowerer.type_id_to_rel_name.contains_key(ty))
        {
            return false;
        }
        let rebinds = self.input_vars.contains(dst);
        if rebinds {
            if self.rebound.contains(dst) {
                return false;
            }
            let alias = var_alias(*dst);
            if self.real_map.get(*dst) != Some(alias.as_str()) {
                return false;
            }
            let identity = Column::new(Some(TableReference::bare(alias)), "node_uuid");
            if unique_column_index(&self.input_schema, &identity).is_none() {
                return false;
            }
        }

        // A rebound node is absent from the expansion's map, so the expansion
        // creates it fresh; its identity is joined to the input afterwards.
        let mut map = self.body_map.clone();
        let before = expand_node_count(&self.body);
        let Some(next) = self.lower_body_op(op, &mut map) else {
            return false;
        };
        if expand_node_count(&next) <= before {
            return false;
        }
        self.body = next;
        self.body_map = map;
        self.body_edges.insert(*edge);
        self.body_vars.insert(*edge);
        if rebinds {
            self.rebound.push(*dst);
        } else {
            self.body_vars.insert(*dst);
        }
        true
    }

    fn relationship_unique(&mut self, op: &GraphOp, edge: VarId, prior: &[VarId]) -> bool {
        if prior.is_empty() {
            return true;
        }
        if !self.body_edges.contains(&edge) || !prior.iter().all(|p| self.body_edges.contains(p)) {
            return false;
        }
        let mut map = self.body_map.clone();
        let Some(next) = self.lower_body_op(op, &mut map) else {
            return false;
        };
        self.body = next;
        self.body_map = map;
        true
    }

    fn filter(&mut self, predicate: ExprId) -> bool {
        let mut in_body = Vec::new();
        let mut keys = Vec::new();
        let mut residual = Vec::new();
        for conjunct in conjuncts(self.exprs, predicate) {
            let vars = referenced_vars(self.exprs, conjunct);
            if vars
                .iter()
                .any(|v| !self.input_vars.contains(v) && !self.body_vars.contains(v))
            {
                return false;
            }
            if calls_volatile_function(self.exprs, conjunct) {
                // Evaluated once per joined pair, as the per-row form does.
                residual.push(conjunct);
            } else if vars.iter().all(|v| self.body_vars.contains(v)) {
                in_body.push(conjunct);
            } else if let Some(key) = self.key_candidate(conjunct) {
                keys.push(key);
            } else {
                residual.push(conjunct);
            }
        }
        if !in_body.is_empty() {
            let lowered = self
                .lowerer
                .expr_lowerer(self.exprs, &self.body_map)
                .with_input_schema(Arc::clone(self.body.schema()))
                .lower_conjunction(&in_body);
            let Ok(lowered) = lowered else {
                return false;
            };
            let Ok(next) = LogicalPlanBuilder::from(self.body.clone())
                .filter(lowered)
                .and_then(LogicalPlanBuilder::build)
            else {
                return false;
            };
            self.body = next;
        }
        self.keys.extend(keys);
        self.residual.extend(residual);
        true
    }

    /// `conjunct` as a join key, when it equates an expression over the run's
    /// own variables with a plain column of the input.
    fn key_candidate(&self, conjunct: ExprId) -> Option<KeyCandidate> {
        let IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left,
            right,
        } = self.exprs.get(conjunct)
        else {
            return None;
        };
        self.key_oriented(conjunct, *left, *right)
            .or_else(|| self.key_oriented(conjunct, *right, *left))
    }

    fn key_oriented(
        &self,
        conjunct: ExprId,
        body_side: ExprId,
        input_side: ExprId,
    ) -> Option<KeyCandidate> {
        let body_reads = referenced_vars(self.exprs, body_side);
        let input_reads = referenced_vars(self.exprs, input_side);
        if body_reads.is_empty() || !body_reads.iter().all(|v| self.body_vars.contains(v)) {
            return None;
        }
        if input_reads.is_empty() || !input_reads.iter().all(|v| self.input_vars.contains(v)) {
            return None;
        }
        let DfExpr::Column(input_column) = self
            .lowerer
            .expr_lowerer(self.exprs, &self.real_map)
            .with_input_schema(Arc::clone(&self.input_schema))
            .lower(input_side)
            .ok()?
        else {
            return None;
        };
        let DfExpr::Column(body_column) = self
            .lowerer
            .expr_lowerer(self.exprs, &self.body_map)
            .with_input_schema(Arc::clone(self.body.schema()))
            .lower(body_side)
            .ok()?
        else {
            return None;
        };
        if is_entity_identity(&input_column) || is_entity_identity(&body_column) {
            return None;
        }
        let input_col = unique_column_index(&self.input_schema, &input_column)?;
        Some(KeyCandidate {
            input_col,
            body_column,
            input_vars: input_reads,
            conjunct,
        })
    }

    /// The equi-join between the input and the expansion, and the conjuncts
    /// that could not become part of it. `None` when no sound join exists.
    fn join_plan(
        &self,
        input_schema: &DFSchema,
        anchor_alias: &str,
        anchor_input_col: usize,
    ) -> Option<JoinPlan> {
        let body_schema = self.body.schema();
        let anchor_body = Column::new(Some(TableReference::bare(anchor_alias)), "node_id");
        let anchor_body_col = unique_column_index(body_schema, &anchor_body)?;
        let mut join_keys = vec![key_pair(
            input_schema,
            anchor_input_col,
            body_schema,
            anchor_body_col,
        )?];
        for var in &self.rebound {
            let identity = Column::new(Some(TableReference::bare(var_alias(*var))), "node_uuid");
            let input_col = unique_column_index(input_schema, &identity)?;
            let body_col = unique_column_index(body_schema, &identity)?;
            join_keys.push(key_pair(input_schema, input_col, body_schema, body_col)?);
        }

        // A candidate that cannot be a key is an ordinary conjunct again. One
        // that reads a rebound node's columns is not trusted: that node's scan
        // may still add the columns it reads.
        let mut residual = self.residual.clone();
        for key in &self.keys {
            let pair = if key.input_vars.iter().any(|v| self.rebound.contains(v)) {
                None
            } else {
                unique_column_index(body_schema, &key.body_column).and_then(|body_col| {
                    key_pair(input_schema, key.input_col, body_schema, body_col)
                })
            };
            match pair {
                Some(pair) if join_keys.contains(&pair) => {}
                Some(pair) => join_keys.push(pair),
                None => residual.push(key.conjunct),
            }
        }
        Some((join_keys, residual))
    }

    /// The expansion columns the joined plan adds to the input: the ones the
    /// run bound. The anchor's and the rebound nodes' columns are the input's
    /// own. `None` when the joined schema would be ambiguous.
    fn kept_columns(&self, input_schema: &DFSchema, anchor_alias: &str) -> Option<Vec<usize>> {
        let body_schema = self.body.schema();
        let rebound_aliases: Vec<String> = self.rebound.iter().map(|v| var_alias(*v)).collect();
        let mut keep = Vec::new();
        for index in 0..body_schema.fields().len() {
            let (qualifier, field) = body_schema.qualified_field(index);
            let table = qualifier?.table();
            if table == anchor_alias || rebound_aliases.iter().any(|alias| alias == table) {
                continue;
            }
            if input_schema.has_column_with_qualified_name(qualifier?, field.name()) {
                return None;
            }
            keep.push(index);
        }
        let mut fields: Vec<(Option<TableReference>, Arc<_>)> = Vec::new();
        fields.extend(
            input_schema
                .iter()
                .map(|(q, f)| (q.cloned(), Arc::clone(f))),
        );
        fields.extend(keep.iter().map(|&index| {
            let (q, f) = body_schema.qualified_field(index);
            (q.cloned(), Arc::clone(f))
        }));
        DFSchema::new_with_metadata(fields, HashMap::new()).ok()?;
        Some(keep)
    }

    /// Join the expansion to the input and lower what the run left for the
    /// joined plan. `None` when the run cannot be expressed as a join.
    fn finish(
        &self,
        input: &LogicalPlan,
        rest: &[&GraphOp],
        seed: u64,
        anchor_alias: &str,
        anchor_input_col: usize,
        var_map: &mut VarMap,
    ) -> Result<Option<(LogicalPlan, usize)>, LoweringError> {
        if self.body_edges.is_empty() {
            return Ok(None);
        }
        let Some((join_keys, residual)) =
            self.join_plan(input.schema(), anchor_alias, anchor_input_col)
        else {
            return Ok(None);
        };
        let Some(keep) = self.kept_columns(input.schema(), anchor_alias) else {
            return Ok(None);
        };

        let node = AnchoredExpansionNode::new(
            Arc::new(input.clone()),
            Arc::new(self.body.clone()),
            join_keys,
            keep,
            seed,
        );
        let mut plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });

        let registered: Vec<(VarId, String)> = self
            .body_map
            .var_ids()
            .filter(|var| var_map.get(*var).is_none())
            .filter_map(|var| self.body_map.get(var).map(|alias| (var, alias.to_owned())))
            .collect();
        for (var, alias) in registered {
            var_map.insert(var, alias);
        }
        for &offset in &self.postponed {
            plan = self.lowerer.lower_op_with_arena(
                rest[offset],
                plan,
                self.exprs,
                var_map,
                false,
                false,
                None,
                None,
            )?;
        }
        if !residual.is_empty() {
            let predicate = self
                .lowerer
                .expr_lowerer(self.exprs, var_map)
                .with_input_schema(Arc::clone(plan.schema()))
                .lower_conjunction(&residual)?;
            plan = LogicalPlanBuilder::from(plan)
                .filter(predicate)
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()?;
        }
        Ok(Some((plan, self.consumed)))
    }
}

/// Equi-join key pairs (input column, expansion column) and the conjuncts the
/// join does not absorb.
type JoinPlan = (Vec<(usize, usize)>, Vec<ExprId>);

/// The index of the one field of `schema` that `column` names, `None` when it
/// names no field or more than one.
fn unique_column_index(schema: &DFSchema, column: &Column) -> Option<usize> {
    let mut found = None;
    for index in 0..schema.fields().len() {
        let (qualifier, field) = schema.qualified_field(index);
        let named = field.name() == &column.name
            && column
                .relation
                .as_ref()
                .is_none_or(|wanted| qualifier == Some(wanted));
        if named {
            if found.is_some() {
                return None;
            }
            found = Some(index);
        }
    }
    found
}

/// A column that is an entity's identity rather than a value: equating two of
/// them is an identity comparison, not a value join.
fn is_entity_identity(column: &Column) -> bool {
    column.name.ends_with("node_uuid") || column.name.ends_with("edge_uuid")
}

/// The pair of columns as a join key, when both have the same type and it is a
/// type whose Arrow equality is the Cypher equality (floats are not: NaN).
fn key_pair(
    input_schema: &DFSchema,
    input_col: usize,
    body_schema: &DFSchema,
    body_col: usize,
) -> Option<(usize, usize)> {
    let input_type = input_schema.field(input_col).data_type();
    let body_type = body_schema.field(body_col).data_type();
    (input_type == body_type && is_join_key_type(input_type)).then_some((input_col, body_col))
}

fn is_join_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Boolean
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::FixedSizeBinary(_)
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
    )
}

/// How many relationship expansions `plan` contains.
fn expand_node_count(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Extension(Extension { node }) = node
            && node
                .as_any()
                .downcast_ref::<graphforge_plan::ExpandNode>()
                .is_some()
        {
            count += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    });
    count
}
