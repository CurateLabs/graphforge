//! [`GraphPlanLowerer`] — converts a [`GraphPlan`] operator pipeline into a
//! DataFusion [`LogicalPlan`].
//!
//! ## Scope (logical-plan lowering #575 + #576)
//!
//! | [`GraphOp`] | DataFusion node |
//! |---|---|
//! | `Filter { predicate }` | `Filter` |
//! | `Project { items, distinct }` | `Projection` (+ optional `Distinct`) |
//! | `Aggregate { group_by, aggs }` | `Aggregate` |
//! | `Sort { keys }` | `Sort` |
//! | `Limit { count }` | `Limit` (skip=0, fetch=count) |
//! | `Skip { count }` | `Limit` (skip=count, fetch=None) |
//! | `NodeScan { var, ty }` | `TableScan("var_N")` + optional `Filter(type_id)` |
//! | `TypedEdgeScan { var, rel_ty }` | `TableScan("edges_NAME")` or exploratory fallback |
//! | `EdgeScan { var, ty }` | `TableScan("edges__exploratory")` + optional filter |
//! | `Expand { .. min_hops=1, max_hops=Some(1) }` | provider-backed `ExpandNode` (relational fallback for schema-only/bound-edge plans) |
//! | `Expand { variable-length }` | `VarLenExpandNode` (`graphforge-plan` Extension stub) |
//! | `Optional { child }` | `OptionalMatchNode` (`graphforge-plan` Extension stub) |
//! | `Exists { child, .. }` | `LeftSemi` / `LeftAnti` join; correlated key union for alternatives |
//! | `PatternComprehension { child, .. }` | correlated aggregate + left join |
//! | `ListElementPatternComprehension { .. }` | ordinal unwind + correlated aggregate + regroup |
//! | `Unwind { list_expr, alias }` | `UnwindNode` (`graphforge-plan` Extension stub) |
//!
//! Graph-native operators (#578) lower to `graphforge-plan` logical stub nodes wrapped
//! as [`LogicalPlan::Extension`]; their physical execution is deferred to physical execution.

mod nested_queries;
mod primary_property_value;
mod scans;
mod semantic_node_properties;
mod traversal;
mod writes;

use scans::{
    enrich_bound_node_identity, filter_node_by_type, lower_edge_scan, lower_node_scan,
    lower_typed_edge_scan,
};
use traversal::lower_expand;

use graphforge_ir::LoweringSnapshot;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::functions_aggregate::count::count_all;
use datafusion::functions_aggregate::expr_fn::{
    array_agg, avg, avg_distinct, count, count_distinct, max, min, sum, sum_distinct,
};
use datafusion::logical_expr::{
    Expr as DfExpr, ExprFunctionExt, ExprSchemable, Extension, JoinType, LogicalPlanBuilder,
    SortExpr,
};

use graphforge_core::{GfError, OntologyMode};
use graphforge_ir::arrow_schema::{
    EXPLORATORY_EDGE_SCHEMA, TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA,
};
use graphforge_ir::plan::PATTERN_COMPREHENSION_VALUE_ALIAS;
use graphforge_ir::{
    AggExpr, AggFunc, CreatePattern, Direction, ExprArena, ExprId, GraphOp, GraphPlan, IrExpr,
    IrLiteral, ProjectItem, RemovePropItem, SetPropItem, SortOrder, VarId,
};
use graphforge_ontology::OntologyHandle;
use graphforge_plan::{
    DeleteTarget, GraphCreateNode, GraphDeleteNode, GraphRemoveNode, GraphSetNode,
    OptionalMatchNode, RemoveTarget, ResolvedEdgeSpec, ResolvedNodeSpec, SetTarget, UnwindNode,
    VarLenExpandNode,
};
use graphforge_value::{EntityTypeId, PropertyId, RelationTypeId};

use crate::LogicalPlan;
use crate::expr::{ExprLowerer, LoweringError, VarMap, list_index_range};

const INPUT_ORDER_COLUMN_PREFIX: &str = "__gf_input_order_";

/// Convert an underlying planner error into the relational facade's stable
/// unsupported-expression diagnostic without duplicating formatting logic at
/// every builder call site.
trait MapUnsupportedExpr<T> {
    fn map_unsupported_expr(self) -> Result<T, LoweringError>;
}

impl<T, E: std::fmt::Display> MapUnsupportedExpr<T> for Result<T, E> {
    fn map_unsupported_expr(self) -> Result<T, LoweringError> {
        self.map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))
    }
}

// ---------------------------------------------------------------------------
// GraphPlanLowerer
// ---------------------------------------------------------------------------

/// Converts a [`GraphPlan`] operator pipeline into a DataFusion
/// [`LogicalPlan`].
///
/// Construct via [`GraphPlanLowerer::new`] and call either
/// [`lower_plan`](Self::lower_plan) (processes the full pipeline) or
/// [`lower_op`](Self::lower_op) (processes a single operator given an
/// existing input plan).
pub struct GraphPlanLowerer {
    /// The catalog used by scan operators. `None` when called from
    /// `lower()` without a catalog (scan ops return an error). Also the source
    /// of the `PropId → name` map used to resolve property accesses.
    catalog: Option<LoweringSnapshot>,
    /// Reverse map: `TypeId.0` → relation type name.
    /// Populated at construction from the ontology; empty in exploratory mode.
    type_id_to_rel_name: HashMap<RelationTypeId, String>,
    /// Reverse map: `TypeId.0` → entity (label) name.
    /// Populated at construction from the ontology; empty in exploratory mode.
    type_id_to_entity_name: HashMap<EntityTypeId, String>,
    /// Whether compilation permits write operators; execution separately admits authority.
    write_target: bool,
    /// Dataset schema facts and compile-time layout policy. No source path is retained.
    read_snapshot: Option<(LoweringSnapshot, OntologyMode)>,
    /// `VarId.0 → NodeShape`, seeded once per `lower_plan` from the plan's
    /// `NodeScan`s for bare-node-value materialization (#785). Interior
    /// mutability so the `&self` lowering pass can populate it.
    node_shapes: std::sync::RwLock<HashMap<u32, crate::expr::NodeShape>>,
    /// `TypeId.0 → [(rule_id, confidence_model)]` for relations carrying
    /// inference semantics (transitive/symmetric), built from the ontology at
    /// construction (#605). Empty in exploratory mode — the TCK-safety gate: a
    /// var-len expand only wraps in `OntologyInferNode` when this lookup is
    /// non-empty, so the no-ontology TCK plan is byte-identical.
    inference_rules: HashMap<RelationTypeId, Vec<(String, String)>>,
    /// Test-only escape hatch that retains the relational fixed-hop lowering
    /// as an independent semantic oracle. Absent from ordinary builds.
    #[cfg(feature = "differential-testing")]
    relational_fixed_hop_reference: bool,
}

impl GraphPlanLowerer {
    /// Create a new lowerer for read/query plans.
    ///
    /// - `catalog`: the [`LoweringSnapshot`] used by scan operators, or `None`
    ///   when no catalog is available (scan ops will return an error).
    /// - `ontology`: the compiled ontology, or `None` in exploratory mode.
    ///
    /// Lowering a `CREATE` plan with this constructor errors — use
    /// [`new_for_writes`](Self::new_for_writes) instead.
    /// Returns a validation error if a supplied runtime carries an invalid declared identity.
    pub fn new(
        catalog: Option<&LoweringSnapshot>,
        ontology: Option<&OntologyHandle>,
    ) -> Result<Self, GfError> {
        Self::build(catalog, ontology, None, None)
    }

    /// Compile read/write plans from immutable dataset facts.
    /// This permits write syntax, not execution against a destination.
    /// Returns a validation error for an invalid declared identity.
    pub fn new_for_writes(
        snapshot: &LoweringSnapshot,
        ontology: Option<&OntologyHandle>,
        mode: OntologyMode,
    ) -> Result<Self, GfError> {
        Self::build(
            Some(snapshot),
            ontology,
            Some((snapshot, mode)),
            Some((snapshot, mode)),
        )
    }

    /// Compile read plans from immutable dataset facts. Write syntax is rejected.
    /// Returns a validation error for an invalid declared identity.
    pub fn new_for_reads(
        snapshot: &LoweringSnapshot,
        ontology: Option<&OntologyHandle>,
        mode: OntologyMode,
    ) -> Result<Self, GfError> {
        Self::build(Some(snapshot), ontology, None, Some((snapshot, mode)))
    }

    fn build(
        catalog: Option<&LoweringSnapshot>,
        ontology: Option<&OntologyHandle>,
        write_target: Option<(&LoweringSnapshot, OntologyMode)>,
        read_snapshot: Option<(&LoweringSnapshot, OntologyMode)>,
    ) -> Result<Self, GfError> {
        // Relation-name map: ontology IDs and tagged runtime-catalog IDs occupy
        // disjoint plan key spaces. This is essential in advisory mode, where
        // both source ID spaces begin at zero and an unknown relation must not
        // resolve through a colliding ontology ID.
        let mut type_id_to_rel_name = build_type_id_map(ontology)?;
        if let Some(c) = catalog {
            type_id_to_rel_name.extend(c.semantic_rel_routes().clone());
            for (id, name) in c.rel_names() {
                let plan_id = RelationTypeId::runtime(*id);
                type_id_to_rel_name.insert(plan_id, name.clone());
            }
        }
        Ok(Self {
            catalog: catalog.cloned(),
            type_id_to_rel_name,
            // Ontology-only: this map drives property-table routing
            // (`node_prop_cols` / `join_node_properties`) and write specs, where
            // an exploratory node's properties live in `_untyped` (not a
            // per-label table). The runtime-catalog labels are merged in
            // separately for node-value rendering only — see `expr_lowerer`.
            type_id_to_entity_name: {
                let mut names = build_entity_id_map(ontology)?;
                if let Some(c) = catalog {
                    names.extend(c.semantic_label_routes().clone());
                }
                names
            },
            write_target: write_target.is_some(),
            read_snapshot: read_snapshot.map(|(snapshot, mode)| (snapshot.clone(), mode)),
            node_shapes: std::sync::RwLock::new(HashMap::new()),
            inference_rules: build_inference_rules(ontology)?,
            #[cfg(feature = "differential-testing")]
            relational_fixed_hop_reference: false,
        })
    }

    /// Select the legacy relational fixed-hop lowering as a differential-test
    /// oracle. Available only with the non-default `differential-testing` feature.
    #[cfg(feature = "differential-testing")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_relational_fixed_hop_reference(mut self) -> Self {
        self.relational_fixed_hop_reference = true;
        self
    }

    /// The dataset facts available to read operators. Execution binds providers. `None` for pure logical/explain
    /// lowering, where scans use a schema-only source.
    fn read_snapshot(&self) -> Option<&LoweringSnapshot> {
        self.read_snapshot.as_ref().map(|(d, _)| d)
    }

    /// The ontology mode for read operators. Defaults to `Exploratory` for pure
    /// logical/explain lowering (`read_snapshot` is `None`), where scans use a
    /// schema-only source and the mode is never consulted.
    fn read_mode(&self) -> OntologyMode {
        self.read_snapshot
            .as_ref()
            .map_or(OntologyMode::Exploratory, |(_, m)| *m)
    }

    /// The `PropId.0 → name` map for resolving `PropertyAccess`.
    ///
    /// Sourced from the catalog (built from the runtime catalog at `open` time).
    /// With no catalog (pure logical/explain lowering) the map is empty, so
    /// property accesses fall back to `"prop_<id>"` — those paths render plans,
    /// not data.
    fn prop_names(&self) -> HashMap<PropertyId, String> {
        match self.catalog.as_ref() {
            Some(c) => c.prop_names().clone(),
            None => HashMap::new(),
        }
    }

    /// Build an `ExprLowerer` for the current plan, seeded with the resolved
    /// `PropId → name` map.
    fn expr_lowerer<'b>(&self, arena: &'b ExprArena, var_map: &'b VarMap) -> ExprLowerer<'b> {
        // Label-name map for node-value rendering (#889): the ontology entity
        // names plus tagged runtime-catalog labels (exploratory/advisory). Built
        // here, separate from `type_id_to_entity_name` (which must stay
        // ontology-only for property-table routing), so an unlabelled
        // `MATCH (n) RETURN n` can resolve a node's stored `type_id` to its
        // label name without colliding with ontology type 0 (#702).
        let mut node_label_names = self.type_id_to_entity_name.clone();
        if let Some(c) = self.catalog.as_ref() {
            node_label_names.extend(c.semantic_label_names().clone());
            for (id, name) in c.label_names() {
                let plan_id = EntityTypeId::runtime(*id);
                node_label_names
                    .entry(plan_id)
                    .or_insert_with(|| name.clone());
            }
        }
        let mut lowerer = ExprLowerer::with_prop_names_and_nodes(
            arena,
            var_map,
            self.prop_names(),
            self.node_shapes
                .read()
                .expect("node shapes lock poisoned")
                .clone(),
            node_label_names,
            // Authoritative property lists only when a backing dataset is present:
            // `node_prop_cols` reads each node's columns from the property table
            // under `read_snapshot`. With no dir (schema-only/explain lowering) an empty
            // `prop_names` means "unknown", not "absent", so the missing-property→
            // null rewrite (#598) must NOT fire — gate it on having the dataset.
            self.read_snapshot().is_some(),
        );
        // With a dataset attached, `nodes(p)` hydrates its elements (#1024).
        if let Some(dir) = self.read_snapshot() {
            lowerer = lowerer.with_read_target(dir.clone());
        }
        lowerer
    }

    /// Build the `VarId.0 → NodeShape` map for a plan from its `NodeScan`s (#785):
    /// each node var's resolved label + the property columns its scan joins in.
    /// Read-side only; empty in schema-only lowering. Optional child pipelines bind
    /// variables that later projections can read, so include their scans too.
    fn build_node_shapes(&self, ops: &[GraphOp]) -> HashMap<u32, crate::expr::NodeShape> {
        let mut shapes = HashMap::new();
        self.collect_node_shapes(ops, &mut shapes);
        shapes
    }

    fn collect_node_shapes(
        &self,
        ops: &[GraphOp],
        shapes: &mut HashMap<u32, crate::expr::NodeShape>,
    ) {
        for op in ops {
            match op {
                GraphOp::NodeScan { var, ty } => {
                    let prop_names = self.node_prop_cols(*ty);
                    shapes.insert(var.0, crate::expr::NodeShape { prop_names });
                }
                GraphOp::Optional { child }
                | GraphOp::Exists { child, .. }
                | GraphOp::PatternComprehension { child, .. }
                | GraphOp::ListElementPatternComprehension { child, .. } => {
                    self.collect_node_shapes(&child.ops, shapes);
                }
                GraphOp::Union { inputs, .. } => {
                    for input in inputs {
                        self.collect_node_shapes(&input.ops, shapes);
                    }
                }
                _ => {}
            }
        }
    }

    /// Lower a complete [`GraphPlan`] to a DataFusion [`LogicalPlan`].
    ///
    /// Processes the operator pipeline in order.  Scan operators
    /// (`NodeScan`, `TypedEdgeScan`, `Expand`) are not yet implemented (#576)
    /// and return [`GfError::NotImplemented`].
    ///
    /// The starting base for the pipeline is an `EmptyRelation` (zero rows,
    /// no columns).  #576 will replace this with a real table scan.
    ///
    /// # Errors
    ///
    /// Returns [`GfError`] if any operator in the pipeline cannot be lowered.
    pub fn lower_plan(&self, plan: &GraphPlan) -> Result<LogicalPlan, GfError> {
        // Seed node shapes for bare-node-value materialization (#785) from the
        // plan's NodeScans before lowering its expressions.
        *self.node_shapes.write().expect("node shapes lock poisoned") =
            self.build_node_shapes(&plan.ops);
        let mut var_map = VarMap::new();
        self.lower_pipeline(&plan.ops, &plan.exprs, &mut var_map)
            .and_then(|plan| self.attach_graph_contract(plan))
            .map_err(GfError::from)
    }

    /// The semantic assumptions for rebinding this query's current graph.
    #[must_use]
    pub fn read_contract(&self) -> graphforge_plan::GraphReadContract {
        let mut labels = self.type_id_to_entity_name.clone();
        if let Some(catalog) = self.catalog.as_ref() {
            labels.extend(catalog.semantic_label_names().clone());
            labels.extend(
                catalog
                    .label_names()
                    .iter()
                    .map(|(id, name)| (EntityTypeId::runtime(*id), name.clone())),
            );
        }
        let mut labels: Vec<_> = labels.into_iter().collect();
        labels.sort_by_key(|(id, _)| id.encode());
        let mut relations: Vec<_> = self
            .type_id_to_rel_name
            .iter()
            .map(|(id, name)| (*id, name.clone()))
            .collect();
        relations.sort_by_key(|(id, _)| id.encode());
        graphforge_plan::GraphReadContract {
            labels,
            relations,
            composition: self
                .catalog
                .as_ref()
                .and_then(LoweringSnapshot::semantic_composition_fingerprint)
                .map(str::to_owned),
        }
    }

    fn attach_graph_contract(&self, plan: LogicalPlan) -> Result<LogicalPlan, LoweringError> {
        use datafusion::common::tree_node::Transformed;
        let contract = self.read_contract();
        plan.transform_up_with_subqueries(|mut plan| {
            match &mut plan {
                LogicalPlan::TableScan(scan) => {
                    if let Some(source) = scan
                        .source
                        .downcast_ref::<graphforge_plan::GraphReadSource>()
                    {
                        let mut source = source.clone();
                        source.contract = Some(contract.clone());
                        scan.source = Arc::new(source);
                    }
                }
                LogicalPlan::Extension(extension) => {
                    if let Some(node) = extension.node.as_any().downcast_ref::<GraphCreateNode>() {
                        extension.node =
                            Arc::new(node.clone().with_write_contract(Some(contract.clone())));
                    }
                    if let Some(node) = extension.node.as_any().downcast_ref::<GraphDeleteNode>() {
                        extension.node =
                            Arc::new(node.clone().with_write_contract(Some(contract.clone())));
                    }
                    if let Some(node) = extension.node.as_any().downcast_ref::<GraphSetNode>() {
                        extension.node =
                            Arc::new(node.clone().with_write_contract(Some(contract.clone())));
                    }
                    if let Some(node) = extension.node.as_any().downcast_ref::<GraphRemoveNode>() {
                        extension.node =
                            Arc::new(node.clone().with_write_contract(Some(contract.clone())));
                    }
                    if let Some(node) = extension
                        .node
                        .as_any()
                        .downcast_ref::<graphforge_plan::ExpandNode>()
                    {
                        extension.node =
                            Arc::new(node.clone().with_read_contract(Some(contract.clone())));
                    } else if let Some(node) =
                        extension.node.as_any().downcast_ref::<VarLenExpandNode>()
                    {
                        extension.node =
                            Arc::new(node.clone().with_read_contract(Some(contract.clone())));
                    }
                }
                _ => {}
            }
            Ok(Transformed::yes(plan))
        })
        .map(|result| result.data)
        .map_unsupported_expr()
    }

    // -----------------------------------------------------------------------
    // Unified write statement driver support (#792, #817)
    //
    // The driver in graphforge-exec runs one read prefix and then applies the write
    // clauses itself (phase loop), so it needs the lowering building blocks
    // individually: the prefix plan WITH its variable registrations, a CREATE
    // pattern's resolved specs, and per-expression value lowering against a
    // driver-extended VarMap.
    // -----------------------------------------------------------------------

    /// Lower a statement's read prefix (`ops`), exposing the [`VarMap`] the
    /// write phases resolve identity columns and value expressions against.
    ///
    /// Identical to [`lower_plan`](Self::lower_plan) except the caller owns
    /// the map. An empty `ops` lowers to the implicit one-row unit relation
    /// (the standalone-`CREATE` prefix).
    ///
    /// # Errors
    /// Returns [`GfError::Plan`] if any prefix operator cannot be lowered.
    pub fn lower_prefix(
        &self,
        ops: &[GraphOp],
        exprs: &ExprArena,
        var_map: &mut VarMap,
    ) -> Result<LogicalPlan, GfError> {
        *self.node_shapes.write().expect("node shapes lock poisoned") = self.build_node_shapes(ops);
        self.lower_pipeline(ops, exprs, var_map)
            .and_then(|plan| self.attach_graph_contract(plan))
            .map_err(GfError::from)
    }

    /// Lower a terminal read suffix over an already-materialized input schema.
    /// The returned plan has a synthetic empty leaf that the executor replaces
    /// with the statement driver's final frontier.
    pub fn lower_terminal_suffix(
        &self,
        ops: &[GraphOp],
        exprs: &ExprArena,
        var_map: &mut VarMap,
        input_schema: datafusion::common::DFSchemaRef,
    ) -> Result<LogicalPlan, GfError> {
        let input = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: true,
            schema: input_schema,
        });
        self.lower_pipeline_from(ops, exprs, var_map, input, None)
            .and_then(|plan| self.attach_graph_contract(plan))
            .map_err(GfError::from)
    }

    /// Lower one IR value expression against `var_map` (the driver's frontier
    /// registrations, including variables created earlier in the statement).
    ///
    /// # Errors
    /// Returns [`GfError::Plan`] when the expression cannot be lowered.
    pub fn lower_value_expr(
        &self,
        exprs: &ExprArena,
        var_map: &VarMap,
        id: graphforge_ir::ExprId,
    ) -> Result<DfExpr, GfError> {
        self.expr_lowerer(exprs, var_map)
            .lower(id)
            .map_err(GfError::from)
    }

    /// Lower one IR value expression against `var_map` and an existing input
    /// schema. The write driver uses this for a terminal read suffix over
    /// its materialized frontier, where temporal/map accessors need the
    /// frontier's Arrow types (#814).
    ///
    /// # Errors
    /// Returns [`GfError::Plan`] when the expression cannot be lowered.
    pub fn lower_value_expr_with_input(
        &self,
        exprs: &ExprArena,
        var_map: &VarMap,
        id: graphforge_ir::ExprId,
        input_schema: datafusion::common::DFSchemaRef,
    ) -> Result<DfExpr, GfError> {
        self.expr_lowerer(exprs, var_map)
            .with_input_schema(input_schema)
            .lower(id)
            .map_err(GfError::from)
    }

    /// The `TypeId.0 → entity name` map (from the ontology), for per-row
    /// property-file stem resolution in the statement driver.
    #[must_use]
    pub fn entity_name_map(&self) -> HashMap<EntityTypeId, String> {
        self.type_id_to_entity_name.clone()
    }

    /// Lower an operator pipeline starting from an `EmptyRelation` base.
    ///
    /// Shared by [`lower_plan`](Self::lower_plan) and the `Optional` arm of
    /// [`lower_op_with_arena`](Self::lower_op_with_arena) (which lowers a
    /// nested child pipeline). Operators are folded in binder-emitted order;
    /// projection shaping depends on whether ORDER BY was deliberately placed
    /// before or after the projection boundary.
    fn lower_pipeline(
        &self,
        ops: &[GraphOp],
        exprs: &ExprArena,
        var_map: &mut VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        let ordered: Vec<&GraphOp> = ops.iter().collect();

        // Base relation. Only the FIRST op consumes this base: a scan/source op
        // (NodeScan/EdgeScan/Expand) builds its own plan and ignores it, so a
        // zero-row base is fine there. Any other leading op FOLDS on the base —
        // `RETURN 1`, `UNWIND [..]`, a leading `WITH 1 AS x` (then `MATCH`), or a
        // bare `CREATE (n)` — and must see the single implicit "unit" row, or it
        // projects/creates over zero rows. Keying off `ops.first()` (not "any
        // source op present") is what makes `WITH … MATCH …` cross-join instead
        // of collapsing to empty (#920 temporal arithmetic; corpus-wide).
        let produce_one_row = !ordered.first().copied().is_some_and(is_source_op);
        let mut current = LogicalPlanBuilder::empty(produce_one_row)
            .build()
            .map_unsupported_expr()?;

        for (i, op) in ordered.iter().enumerate() {
            // A `CREATE` with any clause after it feeds a read (RETURN/WITH/…),
            // so it must emit the created-entity rows rather than the one-row
            // summary (#814 write-result RETURN).
            let create_feeds_read = matches!(op, GraphOp::Create { .. }) && i + 1 < ordered.len();
            current = self.lower_op_with_arena(
                op,
                current,
                exprs,
                var_map,
                create_feeds_read,
                false,
                None,
            )?;
        }
        Ok(current)
    }

    /// Lower an operator pipeline over an existing correlated input.
    fn lower_pipeline_from(
        &self,
        ops: &[GraphOp],
        exprs: &ExprArena,
        var_map: &mut VarMap,
        mut current: LogicalPlan,
        pending_nodes: Option<&RecordBatch>,
    ) -> Result<LogicalPlan, LoweringError> {
        let ordered: Vec<&GraphOp> = ops.iter().collect();
        for (i, op) in ordered.iter().enumerate() {
            let create_feeds_read = matches!(op, GraphOp::Create { .. }) && i + 1 < ordered.len();
            current = self.lower_op_with_arena(
                op,
                current,
                exprs,
                var_map,
                create_feeds_read,
                true,
                pending_nodes,
            )?;
        }
        Ok(current)
    }

    /// Lower a single [`GraphOp`] given an `input` plan (convenience wrapper).
    ///
    /// Scan operators (`NodeScan`, `TypedEdgeScan`, `EdgeScan`) ignore `input`
    /// and produce a fresh plan.  All other operators fold on top of `input`.
    ///
    /// # Errors
    ///
    /// Returns [`LoweringError`] if the operator cannot be lowered.
    /// Lower a single relational [`GraphOp`] given an `input` plan.
    ///
    /// This method handles non-scan operators only.  Scan operators
    /// (`NodeScan`, `TypedEdgeScan`, `EdgeScan`, `Expand`) are handled by
    /// [`lower_op_with_arena`](Self::lower_op_with_arena) which manages `VarMap`
    /// mutation separately.
    ///
    /// # Errors
    ///
    /// Returns [`LoweringError`] if the operator cannot be lowered.
    pub fn lower_op(
        &self,
        op: &GraphOp,
        input: LogicalPlan,
        exprs: &ExprArena,
        _var_map: &VarMap,
        expr_lowerer: &ExprLowerer<'_>,
    ) -> Result<LogicalPlan, LoweringError> {
        lower_relational_op(op, input, exprs, expr_lowerer)
    }

    /// Internal helper: creates a fresh [`ExprLowerer`] per op so that
    /// `var_map` can be borrowed mutably for scan operators in the same loop.
    // A flat dispatch over every `GraphOp` kind — long by nature, like the
    // binder's `lower_expr`.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn lower_op_with_arena(
        &self,
        op: &GraphOp,
        input: LogicalPlan,
        exprs: &ExprArena,
        var_map: &mut VarMap,
        create_feeds_read: bool,
        preserve_empty_input: bool,
        pending_nodes: Option<&RecordBatch>,
    ) -> Result<LogicalPlan, LoweringError> {
        // Scan ops don't use the expression lowerer — handle them first.
        match op {
            GraphOp::NodeScan { var, ty } => {
                // A NodeScan for a variable already bound upstream (e.g. the
                // destination of a preceding Expand/var-length Expand) must not
                // re-scan — that would discard the current plan (Extension stubs
                // included). But the destination's label, if any, lives only on
                // this trailing scan (the binder emits `Expand` then
                // `NodeScan{dst, ty}`), so apply it as a filter on the already-
                // bound `var_<dst>.type_id` rather than dropping it (#718).
                //
                // The label also selects the property table: join it here so a
                // downstream `RETURN/WHERE/ORDER BY` (or inline `{prop:val}`
                // filter) on the destination's properties resolves — the fresh
                // scan below does this via `join_node_properties`, but a bound
                // var never hit it before, so `var_<dst>.<prop>` was unresolved
                // and the query failed to plan (#789). `join_node_properties`
                // preserves the existing multi-var (src + edge + dst) columns.
                if let Some(alias) = var_map.get(*var) {
                    let qualifier = datafusion::common::TableReference::bare(alias);
                    let input = if input
                        .schema()
                        .index_of_column_by_name(Some(&qualifier), "node_uuid")
                        .is_some()
                        && input
                            .schema()
                            .index_of_column_by_name(Some(&qualifier), "node_id")
                            .is_none()
                    {
                        enrich_bound_node_identity(&input, alias, self.read_snapshot())?
                    } else {
                        input
                    };
                    return match ty {
                        Some(type_id) => {
                            let filtered = filter_node_by_type(input, alias, *type_id)?;
                            self.join_node_properties(*var, *ty, filtered)
                        }
                        // An already-bound but UNLABELLED node (e.g. the dst of
                        // `(n)-[r]->(x)`) still needs its properties joined so a
                        // later `x.prop` / `WHERE x.p = …` resolves. In
                        // exploratory mode `join_node_properties` routes to
                        // `_untyped` (#889); a no-op otherwise.
                        None => self.join_node_properties(*var, None, input),
                    };
                }
                let scan =
                    lower_node_scan(*var, *ty, var_map, self.read_snapshot(), pending_nodes)?;
                let scan = self.join_node_properties(*var, *ty, scan)?;
                // Multi-pattern MATCH: comma-separated patterns (`MATCH (a), (b)`)
                // lower to consecutive *fresh* NodeScans. The first replaces the
                // column-less base; a later disconnected scan is a CROSS PRODUCT
                // with the rows built so far, not a replacement — without this the
                // earlier pattern's columns were dropped (`No field named var_0`).
                // A connected pattern's trailing node binds to the Expand's dst
                // (the bound-var path above), so a fresh scan over a non-empty
                // input is always a genuinely disconnected component.
                if input.schema().fields().is_empty() && !preserve_empty_input {
                    return Ok(scan);
                }
                return LogicalPlanBuilder::from(input)
                    .cross_join(scan)
                    .and_then(LogicalPlanBuilder::build)
                    .map_unsupported_expr();
            }
            GraphOp::TypedEdgeScan { var, rel_ty } => {
                return lower_typed_edge_scan(
                    *var,
                    *rel_ty,
                    var_map,
                    self.catalog.as_ref(),
                    &self.type_id_to_rel_name,
                    self.read_snapshot(),
                    self.read_mode(),
                );
            }
            GraphOp::EdgeScan { var, ty } => {
                return lower_edge_scan(
                    *var,
                    *ty,
                    var_map,
                    &self.type_id_to_rel_name,
                    self.read_snapshot(),
                    self.read_mode(),
                );
            }
            GraphOp::Expand {
                src,
                edge,
                dst,
                rel_ty,
                dir,
                min_hops,
                max_hops,
            } => {
                return lower_expand(
                    *src,
                    *edge,
                    *dst,
                    *rel_ty,
                    *dir,
                    *min_hops,
                    *max_hops,
                    input,
                    var_map,
                    self.catalog.as_ref(),
                    &self.type_id_to_rel_name,
                    &self.inference_rules,
                    self.read_snapshot.as_ref().map(|(s, m)| (s, *m)),
                    #[cfg(feature = "differential-testing")]
                    self.relational_fixed_hop_reference,
                );
            }
            GraphOp::RelationshipUnique { edge, prior_edges } => {
                use datafusion::logical_expr::col;
                let edge_alias = var_map
                    .get(*edge)
                    .ok_or(LoweringError::UnboundVar(edge.0))?;
                let mut predicates = prior_edges.iter().map(|prior| {
                    let prior_alias = var_map
                        .get(*prior)
                        .ok_or(LoweringError::UnboundVar(prior.0))?;
                    let edge_is_list =
                        edge_alias.ends_with(graphforge_plan::VAR_LEN_EDGE_LIST_FIELD);
                    let prior_is_list =
                        prior_alias.ends_with(graphforge_plan::VAR_LEN_EDGE_LIST_FIELD);
                    // Fixed-hop adjacency already carries the exact edge_id.
                    // Use that internal identity when both operands are fixed
                    // hops so uniqueness never forces an edge-Parquet UUID
                    // hydration. Mixed fixed/variable-length comparisons must
                    // remain on public UUID identity because path lists contain
                    // edge UUIDs rather than storage ordinals.
                    let value = |alias: &str, is_list: bool| {
                        if is_list {
                            col(alias)
                        } else if !edge_is_list && !prior_is_list {
                            col(format!("{alias}.edge_id"))
                        } else {
                            col(format!("{alias}.edge_uuid"))
                        }
                    };
                    Ok(crate::expr::relationship_disjoint(
                        value(edge_alias, edge_is_list),
                        value(prior_alias, prior_is_list),
                    ))
                });
                let Some(mut predicate) = predicates.next().transpose()? else {
                    return Ok(input);
                };
                for next in predicates {
                    predicate = predicate.and(next?);
                }
                return LogicalPlanBuilder::from(input)
                    .filter(predicate)
                    .and_then(LogicalPlanBuilder::build)
                    .map_unsupported_expr();
            }
            // UNWIND — emit the graphforge-plan stub Extension node (physical execution
            // deferred to physical execution).  Needs the expression lowerer for `list_expr`.
            GraphOp::Unwind { list_expr, alias } => {
                let expr_lowerer = self
                    .expr_lowerer(exprs, var_map)
                    .with_input_schema(input.schema().clone());
                let df_expr = expr_lowerer.lower(*list_expr)?;
                // The output adds one column of the list's element type; resolve
                // it from df_expr's type against the input schema, defaulting to
                // a nullable Int64 when it can't be determined pre-execution.
                let element_field =
                    unwind_element_field(&df_expr, input.schema(), self.prop_names().values());
                let node =
                    UnwindNode::new(Arc::new(input), df_expr, var_alias(*alias), &element_field);
                // Register the unwound variable so downstream ops can refer to it.
                var_map.insert(*alias, var_alias(*alias));
                return Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }));
            }
            GraphOp::Call {
                procedure,
                args,
                yields,
            } => {
                return self.lower_call_op(procedure, args, yields, input, exprs, var_map);
            }
            GraphOp::Union { all, inputs } => {
                return self.lower_union_op(*all, inputs);
            }
            // OPTIONAL MATCH — lower the nested child pipeline and wrap both the
            // outer input and the optional sub-plan in the OptionalMatch node,
            // computing the join keys (the variables shared between the outer
            // scope and the optional sub-plan) so the physical node can left-join
            // and null-shape correctly.
            GraphOp::Optional { child } => {
                return self.lower_optional_op(child, input, var_map);
            }
            GraphOp::Exists { child, negated } => {
                return self.lower_exists_op(child, *negated, input, var_map);
            }
            GraphOp::PatternComprehension { child, output } => {
                return self.lower_pattern_comprehension_op(child, *output, input, var_map);
            }
            GraphOp::ListElementPatternComprehension {
                list_expr,
                loop_var,
                child,
                pattern_output,
                filter,
                projection,
                output,
            } => {
                return self.lower_list_element_pattern_comprehension_op(
                    *list_expr,
                    *loop_var,
                    child,
                    *pattern_output,
                    *filter,
                    *projection,
                    *output,
                    input,
                    exprs,
                    var_map,
                );
            }
            GraphOp::Create { pattern } => {
                // The create node is input-driven: it runs one write per input
                // row (#703). Fold the accumulated pipeline (`current`) in as
                // that input — so a preceding `MATCH` drives the CREATE per
                // matched row, referencing MATCH-bound vars' identities. For a
                // standalone CREATE, `current` is the implicit single unit-row
                // base (no source op), so the create runs exactly once.
                return self.lower_create(pattern, exprs, input, var_map, create_feeds_read);
            }
            GraphOp::Delete { vars, detach, .. } => {
                // Input-driven like CREATE: the preceding MATCH supplies the
                // matched entities' identities, one delete per matched row.
                return self.lower_delete_op(vars, *detach, input);
            }
            GraphOp::Set {
                items,
                map_items,
                label_items,
            } => {
                if !map_items.is_empty() || !label_items.is_empty() {
                    return Err(LoweringError::UnsupportedExpr(
                        "SET map and label assignments execute through the statement driver".into(),
                    ));
                }
                return self.lower_set_op(items, input, exprs, var_map);
            }
            GraphOp::Remove { items, label_items } => {
                if !label_items.is_empty() {
                    return Err(LoweringError::UnsupportedExpr(
                        "REMOVE label assignments execute through the statement driver".into(),
                    ));
                }
                return self.lower_remove_op(items, input);
            }
            GraphOp::Project { items, distinct }
                if items.iter().any(|item| item.out_var.is_some()) =>
            {
                let plan = {
                    let lowerer = self
                        .expr_lowerer(exprs, var_map)
                        .with_input_schema(input.schema().clone());
                    lower_project(items, *distinct, input, &lowerer)?
                };
                for item in items {
                    if let (Some(v), Some(alias)) = (item.out_var, item.alias.as_ref()) {
                        var_map.insert(v, alias.clone());
                    }
                }
                return Ok(plan);
            }
            GraphOp::With {
                items,
                distinct,
                where_predicate,
            } => {
                return self.lower_with_op(
                    items,
                    *distinct,
                    *where_predicate,
                    input,
                    exprs,
                    var_map,
                );
            }
            // Aggregate is handled here (not only in `lower_relational_op`) because
            // a DECOMPOSED aggregate (#599 nested aggregates) is followed by a
            // `Project` that references the aggregate's outputs via synthetic
            // variables — so the aggregate must reset the scope to those outputs.
            // A plain top-level aggregate has no synthetic vars and leaves the
            // scope untouched (so `RETURN count(*) AS c ORDER BY c` still resolves
            // `c` against the unchanged map, exactly as before).
            GraphOp::Aggregate {
                group_by,
                group_aliases,
                group_vars,
                aggs,
            } => {
                let plan = {
                    let lowerer = self
                        .expr_lowerer(exprs, var_map)
                        .with_input_schema(input.schema().clone());
                    lower_aggregate(
                        group_by,
                        group_aliases,
                        aggs,
                        input,
                        exprs,
                        &lowerer,
                        Some((group_vars, var_map)),
                    )?
                };
                let decomposed = !group_vars.is_empty() || aggs.iter().any(|a| a.out_var.is_some());
                if decomposed {
                    // The aggregate's output columns are its group-key aliases and
                    // its agg aliases; bind each synthetic var to its column.
                    let passthrough: Vec<(VarId, String)> = group_by
                        .iter()
                        .zip(group_vars)
                        .filter_map(|(&expr, group_var)| {
                            let group_var = (*group_var)?;
                            match exprs.get(expr) {
                                IrExpr::VarRef(source) if *source == group_var => var_map
                                    .get(group_var)
                                    .map(|alias| (group_var, alias.to_owned())),
                                _ => None,
                            }
                        })
                        .collect();
                    var_map.clear();
                    for (var, alias) in passthrough {
                        var_map.insert(var, alias);
                    }
                    for (i, gv) in group_vars.iter().enumerate() {
                        if let (Some(v), Some(Some(alias))) = (gv, group_aliases.get(i)) {
                            var_map.insert(*v, alias.clone());
                        }
                    }
                    for a in aggs {
                        if let Some(v) = a.out_var {
                            var_map.insert(v, a.alias.clone());
                        }
                    }
                }
                return Ok(plan);
            }
            _ => {}
        }
        // Relational ops need the expression lowerer; var_map is immutable here.
        // They do not register new variables so &mut is not needed. The input
        // schema is attached so a `PropertyAccess` can resolve a temporal-
        // component accessor by the base column's type (#920).
        let expr_lowerer = self
            .expr_lowerer(exprs, var_map)
            .with_input_schema(input.schema().clone());
        lower_relational_op(op, input, exprs, &expr_lowerer)
    }

    fn lower_call_op(
        &self,
        procedure: &graphforge_ir::ProcedureDefinition,
        args: &[ExprId],
        yields: &[graphforge_ir::ProcedureYield],
        input: LogicalPlan,
        exprs: &ExprArena,
        var_map: &mut VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::{Column, DFSchema};
        use datafusion::logical_expr::{EmptyRelation, lit};

        let width = procedure.inputs.len() + procedure.outputs.len();
        let names: Vec<String> = (0..width)
            .map(|index| format!("column{}", index + 1))
            .collect();
        let fields = procedure
            .inputs
            .iter()
            .chain(&procedure.outputs)
            .zip(&names)
            .map(|(field, name)| {
                let data_type = match field.type_name.to_ascii_uppercase().as_str() {
                    "BOOLEAN" => DataType::Boolean,
                    "INTEGER" => DataType::Int64,
                    "FLOAT" | "NUMBER" => DataType::Float64,
                    _ => DataType::Utf8,
                };
                Field::new(name, data_type, field.nullable)
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(
            DFSchema::try_from(Schema::new(fields))
                .map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))?,
        );

        let fixture = if width == 0 {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: !procedure.rows.is_empty(),
                schema,
            })
        } else if procedure.rows.is_empty() {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            })
        } else {
            let rows = procedure
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|value| lit(crate::expr::ir_literal_to_scalar(value)))
                        .collect()
                })
                .collect();
            LogicalPlanBuilder::values_with_schema(rows, &schema)
                .and_then(LogicalPlanBuilder::build)
                .map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))?
        };

        let input_columns: Vec<Column> = input
            .schema()
            .iter()
            .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
            .collect();
        let lowerer = self
            .expr_lowerer(exprs, var_map)
            .with_input_schema(input.schema().clone());
        let predicates = args
            .iter()
            .enumerate()
            .map(|(index, arg)| {
                Ok(DfExpr::BinaryExpr(
                    datafusion::logical_expr::BinaryExpr::new(
                        Box::new(lowerer.lower(*arg)?),
                        datafusion::logical_expr::Operator::IsNotDistinctFrom,
                        Box::new(DfExpr::Column(Column::from_name(names[index].clone()))),
                    ),
                ))
            })
            .collect::<Result<Vec<_>, LoweringError>>()?;
        let joined = LogicalPlanBuilder::from(input)
            .join_on(fixture, JoinType::Inner, predicates)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))?;

        let mut projection: Vec<DfExpr> = input_columns.into_iter().map(DfExpr::Column).collect();
        for yielded in yields {
            let output_index = procedure
                .outputs
                .iter()
                .position(|field| field.name == yielded.field)
                .expect("binder only emits registered procedure outputs");
            let alias = yielded.alias.clone();
            projection.push(
                DfExpr::Column(Column::from_name(
                    names[procedure.inputs.len() + output_index].clone(),
                ))
                .alias(alias.clone()),
            );
            var_map.insert(yielded.var, alias);
        }
        LogicalPlanBuilder::from(joined)
            .project(projection)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))
    }

    /// Lower a [`GraphOp::With`] (#814): a mid-pipeline projection that also
    /// *introduces a new scope*. Each item is projected to its alias column; the
    /// item's `out_var` is then registered in the [`VarMap`] so a later clause
    /// referencing the alias resolves. An optional `WHERE` filters over both the
    /// incoming scope and the projected aliases (#1028), then the operator drops
    /// back to exactly the projected aliases for downstream clauses.
    #[allow(clippy::too_many_lines)]
    fn lower_with_op(
        &self,
        items: &[ProjectItem],
        distinct: bool,
        where_predicate: Option<ExprId>,
        input: LogicalPlan,
        exprs: &ExprArena,
        var_map: &mut VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        let input_order = input_sort_exprs(&input);
        // 1. Project the items against the CURRENT scope (their expressions
        //    reference upstream vars). A scalar item projects its expression
        //    aliased to its name; a whole-node item (`WITH n`) forwards ALL of
        //    the node's columns (a node var spans `var_<v>.node_uuid` + props)
        //    so a downstream `RETURN n` / `n.x` still resolves. `new_scope`
        //    collects each out_var's resulting column (or column prefix).
        let keep_input_for_where = where_predicate.is_some();
        let incoming_scope = var_map.clone();
        let node_shapes = self
            .node_shapes
            .read()
            .expect("node shapes lock poisoned")
            .clone();
        // A forwarded entity keeps qualified property columns such as
        // `var_0.name`. Give a scalar alias named `name` an internal physical
        // column so DataFusion never has to resolve an ambiguous unqualified
        // `name`; the terminal projection still restores the public alias.
        let forwarded_prefixes: HashSet<String> = items
            .iter()
            .filter_map(|item| {
                let IrExpr::VarRef(v) = exprs.get(item.expr) else {
                    return None;
                };
                let prefix = var_map.get(*v)?;
                let is_relationship = input.schema().iter().any(|(qualifier, field)| {
                    qualifier.is_some_and(|q| q.table() == prefix) && field.name() == "edge_uuid"
                });
                (node_shapes.contains_key(&v.0) || is_relationship).then(|| prefix.to_owned())
            })
            .collect();
        let physical_names: HashMap<VarId, String> = items
            .iter()
            .filter_map(|item| {
                let out_var = item.out_var?;
                let name = item.alias.as_ref()?;
                let conflicts_with_forwarded_property =
                    input.schema().iter().any(|(qualifier, field)| {
                        qualifier.as_ref().is_some_and(|q| {
                            forwarded_prefixes.contains(q.table()) && field.name() == name
                        })
                    });
                let shadows_input = input.schema().iter().any(|(_, field)| field.name() == name);
                (conflicts_with_forwarded_property || shadows_input)
                    .then(|| (out_var, format!("__gf_with_{}", out_var.0)))
            })
            .collect();
        let mut select: Vec<DfExpr> = Vec::new();
        let mut new_scope: Vec<(VarId, String)> = Vec::new();
        let mut predicate_scope: Vec<(VarId, String)> = Vec::new();
        let mut forwarded_node_shapes = Vec::new();
        if keep_input_for_where {
            for (qualifier, field) in input.schema().iter() {
                select.push(DfExpr::Column(datafusion::common::Column::new(
                    qualifier.cloned(),
                    field.name(),
                )));
            }
        }
        {
            // Attach the input schema so a `PropertyAccess` on a map/temporal-typed
            // column resolves by the base column's Arrow type (`input.list` on an
            // UNWIND-bound map → `get_field`, #1017; `d.year` → temporal accessor,
            // #920) and `is_list_typed` can type a computed `+` operand for list
            // append. Relational ops already do this (see `lower_relational_op`).
            let lowerer = self
                .expr_lowerer(exprs, var_map)
                .with_input_schema(input.schema().clone());
            for item in items {
                let name = item.alias.as_deref().ok_or_else(|| {
                    LoweringError::UnsupportedExpr(
                        "WITH item without an alias (binder should reject)".into(),
                    )
                })?;
                // Whole-entity forwarding: a bare VarRef to a node or relationship
                // var. Forward every input column under that var's qualifier,
                // unchanged, so downstream projections can still materialize the
                // entity value.
                if let IrExpr::VarRef(v) = exprs.get(item.expr) {
                    let prefix = var_map
                        .get(*v)
                        .ok_or(LoweringError::UnboundVar(v.0))?
                        .to_string();
                    let is_relationship = input.schema().iter().any(|(qualifier, field)| {
                        qualifier.is_some_and(|q| q.table() == prefix)
                            && field.name() == "edge_uuid"
                    });
                    if node_shapes.contains_key(&v.0) || is_relationship {
                        let output_var = item.out_var.unwrap_or(*v);
                        let output_prefix = if output_var == *v {
                            prefix.clone()
                        } else {
                            var_alias(output_var)
                        };
                        if output_var != *v
                            && let Some(shape) = node_shapes.get(&v.0).cloned()
                        {
                            forwarded_node_shapes.push((output_var.0, shape));
                        }
                        if !keep_input_for_where || output_prefix != prefix {
                            for (qualifier, field) in input.schema().iter() {
                                if qualifier.is_some_and(|q| q.table() == prefix) {
                                    let column = DfExpr::Column(datafusion::common::Column::new(
                                        qualifier.cloned(),
                                        field.name(),
                                    ));
                                    select.push(if output_prefix == prefix {
                                        column
                                    } else {
                                        column.alias_qualified(
                                            Some(output_prefix.as_str()),
                                            field.name(),
                                        )
                                    });
                                }
                            }
                        }
                        new_scope.push((output_var, output_prefix.clone()));
                        predicate_scope.push((output_var, output_prefix));
                    } else {
                        let projected_name = if keep_input_for_where {
                            item.out_var.map_or_else(
                                || format!("__gf_with_{}", predicate_scope.len()),
                                |v| format!("__gf_with_{}", v.0),
                            )
                        } else {
                            item.out_var
                                .and_then(|v| physical_names.get(&v).cloned())
                                .unwrap_or_else(|| name.to_string())
                        };
                        select.push(lowerer.lower(item.expr)?.alias(projected_name.as_str()));
                        if let Some(v) = item.out_var {
                            let output_name = physical_names
                                .get(&v)
                                .cloned()
                                .unwrap_or_else(|| name.to_string());
                            new_scope.push((v, output_name));
                            predicate_scope.push((v, projected_name));
                        }
                    }
                } else {
                    use datafusion::logical_expr::ExprSchemable;

                    let projected_name = if keep_input_for_where {
                        item.out_var.map_or_else(
                            || format!("__gf_with_{}", predicate_scope.len()),
                            |v| format!("__gf_with_{}", v.0),
                        )
                    } else {
                        item.out_var
                            .and_then(|v| physical_names.get(&v).cloned())
                            .unwrap_or_else(|| name.to_string())
                    };
                    let value_expr = lowerer.lower(item.expr)?;
                    if let Some(output_var) = item.out_var
                        && let Ok(datafusion::arrow::datatypes::DataType::Struct(fields)) =
                            value_expr.get_type(input.schema().as_ref())
                        && fields.iter().any(|field| field.name() == "node_uuid")
                    {
                        let output_prefix = var_alias(output_var);
                        for field in &fields {
                            select.push(
                                datafusion::functions::core::expr_fn::get_field(
                                    value_expr.clone(),
                                    field.name(),
                                )
                                .alias_qualified(
                                    Some(output_prefix.as_str()),
                                    field.name().as_str(),
                                ),
                            );
                        }
                        new_scope.push((output_var, output_prefix.clone()));
                        predicate_scope.push((output_var, output_prefix));
                        continue;
                    }
                    select.push(value_expr.alias(projected_name.as_str()));
                    if let Some(v) = item.out_var {
                        let output_name = physical_names
                            .get(&v)
                            .cloned()
                            .unwrap_or_else(|| name.to_string());
                        new_scope.push((v, output_name));
                        predicate_scope.push((v, projected_name));
                    }
                }
            }
        }
        if !distinct {
            select.extend(input_order.iter().enumerate().map(|(index, sort)| {
                sort.expr
                    .clone()
                    .alias(format!("{INPUT_ORDER_COLUMN_PREFIX}{index}"))
            }));
        }
        let projected = LogicalPlanBuilder::from(input)
            .project(select)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        drop(node_shapes);
        if !forwarded_node_shapes.is_empty() {
            self.node_shapes
                .write()
                .expect("node shapes lock poisoned")
                .extend(forwarded_node_shapes);
        }
        // 2. Apply WITH's WHERE over the incoming columns plus projected aliases.
        let filtered = match where_predicate {
            Some(pred) => {
                let mut filter_scope = incoming_scope;
                for (v, col) in &predicate_scope {
                    filter_scope.insert(*v, col.clone());
                }
                let lowerer = self
                    .expr_lowerer(exprs, &filter_scope)
                    .with_input_schema(projected.schema().clone());
                let df_pred = lowerer.lower(pred)?;
                LogicalPlanBuilder::from(projected)
                    .filter(df_pred)
                    .and_then(LogicalPlanBuilder::build)
                    .map_unsupported_expr()?
            }
            None => projected,
        };

        // 3. When WHERE needed incoming columns, project back down to the true
        //    WITH output so pre-WITH variables do not leak downstream.
        let mut output = if keep_input_for_where {
            let node_shapes = self
                .node_shapes
                .read()
                .expect("node shapes lock poisoned")
                .clone();
            let lowerer = self
                .expr_lowerer(exprs, var_map)
                .with_input_schema(filtered.schema().clone());
            let mut final_select: Vec<DfExpr> = Vec::new();
            for item in items {
                let name = item.alias.as_deref().ok_or_else(|| {
                    LoweringError::UnsupportedExpr(
                        "WITH item without an alias (binder should reject)".into(),
                    )
                })?;
                if let IrExpr::VarRef(v) = exprs.get(item.expr) {
                    let prefix = var_map.get(*v).ok_or(LoweringError::UnboundVar(v.0))?;
                    let is_relationship = filtered.schema().iter().any(|(qualifier, field)| {
                        qualifier.is_some_and(|q| q.table() == prefix)
                            && field.name() == "edge_uuid"
                    });
                    if node_shapes.contains_key(&v.0) || is_relationship {
                        let output_var = item.out_var.unwrap_or(*v);
                        let output_prefix = if output_var == *v {
                            prefix.to_owned()
                        } else {
                            var_alias(output_var)
                        };
                        for (qualifier, field) in filtered.schema().iter() {
                            if qualifier.is_some_and(|q| q.table() == output_prefix) {
                                final_select.push(DfExpr::Column(datafusion::common::Column::new(
                                    qualifier.cloned(),
                                    field.name(),
                                )));
                            }
                        }
                    } else {
                        let projected_name = item
                            .out_var
                            .and_then(|v| physical_names.get(&v))
                            .map_or(name, String::as_str);
                        final_select.push(lowerer.lower(item.expr)?.alias(projected_name));
                    }
                } else {
                    let projected_name = item
                        .out_var
                        .and_then(|v| physical_names.get(&v))
                        .map_or(name, String::as_str);
                    final_select.push(lowerer.lower(item.expr)?.alias(projected_name));
                }
            }
            if !distinct {
                final_select.extend(input_order.iter().enumerate().map(|(index, _)| {
                    DfExpr::Column(datafusion::common::Column::from_name(format!(
                        "{INPUT_ORDER_COLUMN_PREFIX}{index}"
                    )))
                }));
            }
            LogicalPlanBuilder::from(filtered)
                .project(final_select)
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()?
        } else {
            filtered
        };

        if distinct {
            output = LogicalPlanBuilder::from(output)
                .distinct()
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()?;
        }

        // 4. Install the new scope: WITH resets it to exactly its aliases, so
        //    drop every pre-WITH variable (mirroring the binder) before mapping
        //    each out_var to its projected column / prefix.
        var_map.clear();
        for (v, col) in new_scope {
            var_map.insert(v, col);
        }
        Ok(output)
    }
}

// ---------------------------------------------------------------------------
// Per-operator lowering functions
// ---------------------------------------------------------------------------

/// Dispatch for relational operators that do not mutate the `VarMap`.
fn lower_relational_op(
    op: &GraphOp,
    input: LogicalPlan,
    exprs: &ExprArena,
    expr_lowerer: &ExprLowerer<'_>,
) -> Result<LogicalPlan, LoweringError> {
    match op {
        GraphOp::Filter { predicate } => lower_filter(*predicate, input, expr_lowerer),
        GraphOp::Project { items, distinct } => {
            lower_project(items, *distinct, input, expr_lowerer)
        }
        GraphOp::Aggregate {
            group_by,
            group_aliases,
            aggs,
            ..
        } => lower_aggregate(
            group_by,
            group_aliases,
            aggs,
            input,
            exprs,
            expr_lowerer,
            None,
        ),
        GraphOp::Sort { keys } => lower_sort(keys, input, expr_lowerer),
        GraphOp::Limit { count } => lower_limit(*count, input),
        GraphOp::Skip { count } => lower_skip(*count, input),
        other => Err(LoweringError::UnsupportedExpr(format!(
            "operator not yet lowered (deferred to #577+): {other:?}"
        ))),
    }
}

fn lower_filter(
    predicate: ExprId,
    input: LogicalPlan,
    lowerer: &ExprLowerer<'_>,
) -> Result<LogicalPlan, LoweringError> {
    let df_pred = lowerer.lower(predicate)?;
    LogicalPlanBuilder::from(input)
        .filter(df_pred)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

fn lower_project(
    items: &[ProjectItem],
    distinct: bool,
    input: LogicalPlan,
    lowerer: &ExprLowerer<'_>,
) -> Result<LogicalPlan, LoweringError> {
    let select_exprs: Vec<DfExpr> = items
        .iter()
        .map(|item| {
            let e = lowerer.lower(item.expr)?;
            Ok::<_, LoweringError>(match &item.alias {
                Some(alias) => e.alias(alias),
                None => e,
            })
        })
        .collect::<Result<_, _>>()?;

    let plan = LogicalPlanBuilder::from(input)
        .project(select_exprs)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()?;

    if distinct {
        LogicalPlanBuilder::from(plan)
            .distinct()
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()
    } else {
        Ok(plan)
    }
}

fn input_sort_exprs(input: &LogicalPlan) -> Vec<SortExpr> {
    match input {
        LogicalPlan::Sort(sort) => sort.expr.clone(),
        LogicalPlan::Projection(projection) => input_sort_exprs(&projection.input)
            .into_iter()
            .enumerate()
            .filter_map(|(index, sort)| {
                let name = format!("{INPUT_ORDER_COLUMN_PREFIX}{index}");
                projection
                    .schema
                    .fields()
                    .iter()
                    .any(|field| field.name() == &name)
                    .then(|| {
                        SortExpr::new(
                            DfExpr::Column(datafusion::common::Column::from_name(name)),
                            sort.asc,
                            sort.nulls_first,
                        )
                    })
            })
            .collect(),
        LogicalPlan::Filter(filter) => input_sort_exprs(&filter.input),
        _ => Vec::new(),
    }
}

fn preserve_collect_order(
    func: AggFunc,
    aggregate: DfExpr,
    input_order: &[SortExpr],
) -> Result<DfExpr, LoweringError> {
    if func != AggFunc::Collect || input_order.is_empty() {
        return Ok(aggregate);
    }
    aggregate
        .order_by(input_order.to_vec())
        .build()
        .map_unsupported_expr()
}

#[allow(
    clippy::too_many_lines,
    reason = "group shaping, row markers, aggregate lowering, and aliases form one operation"
)]
fn lower_aggregate(
    group_by: &[ExprId],
    group_aliases: &[Option<String>],
    aggs: &[AggExpr],
    input: LogicalPlan,
    exprs: &ExprArena,
    lowerer: &ExprLowerer<'_>,
    passthrough_groups: Option<(&[Option<VarId>], &VarMap)>,
) -> Result<LogicalPlan, LoweringError> {
    let input_order = input_sort_exprs(&input);
    // Each group key may carry an output-column alias (its RETURN source text, so
    // a mixed `RETURN n.name, count(*)` produces the `n.name` header — #599).
    let mut group_exprs = Vec::new();
    let mut row_marker_aliases = Vec::new();
    for (i, &id) in group_by.iter().enumerate() {
        let passthrough = passthrough_groups.and_then(|(group_vars, var_map)| {
            let group_var = group_vars.get(i).copied().flatten()?;
            match exprs.get(id) {
                IrExpr::VarRef(source) if *source == group_var => {
                    var_map.get(group_var).map(|alias| (group_var, alias))
                }
                _ => None,
            }
        });
        if let Some((_var, alias)) = passthrough {
            let qualifier = datafusion::common::TableReference::bare(alias);
            let columns: Vec<DfExpr> = input
                .schema()
                .iter()
                .filter(|(q, _)| q.as_ref().is_some_and(|q| **q == qualifier))
                .map(|(q, field)| {
                    DfExpr::Column(datafusion::common::Column::new(q.cloned(), field.name()))
                })
                .collect();
            if !columns.is_empty() {
                group_exprs.extend(columns);
                continue;
            }
        }
        let e = lowerer.lower(id)?;
        if matches!(
            &e,
            DfExpr::Literal(datafusion::scalar::ScalarValue::Null, _)
        ) && let Some((qualifier, field)) = input
            .schema()
            .iter()
            .find(|(_, field)| {
                matches!(
                    field.name().as_str(),
                    "node_uuid" | "edge_uuid" | "node_id" | "edge_id" | "src_id" | "dst_id"
                )
            })
            .or_else(|| input.schema().iter().next())
        {
            let present = DfExpr::Column(datafusion::common::Column::new(
                qualifier.cloned(),
                field.name(),
            ));
            // DataFusion drops a literal-null group key and turns an empty
            // grouped aggregate into a one-row global aggregate. This
            // row-dependent tautology keeps the grouping set non-empty while
            // remaining one value for both present and null identities.
            let marker_alias = format!("__gf_row_marker_{i}");
            group_exprs.push(
                crate::expr::CYPHER_ROW_MARKER
                    .call(vec![present])
                    .alias(&marker_alias),
            );
            row_marker_aliases.push(marker_alias);
        }
        group_exprs.push(match group_aliases.get(i).and_then(Option::as_ref) {
            Some(alias) => e.alias(alias),
            None => e,
        });
    }

    let aggr_exprs: Result<Vec<DfExpr>, LoweringError> = aggs
        .iter()
        .map(|a| {
            let mut arg = a.arg.map(|id| lowerer.lower(id)).transpose()?;
            if a.func == AggFunc::Count
                && arg.is_none()
                && let Some((qualifier, field)) = input
                    .schema()
                    .iter()
                    .filter(|(_, field)| {
                        matches!(
                            field.name().as_str(),
                            "node_uuid" | "edge_uuid" | "node_id" | "edge_id" | "src_id" | "dst_id"
                        )
                    })
                    .last()
                    .or_else(|| input.schema().iter().last())
            {
                // The marker is true even for null OPTIONAL rows. Its value
                // does not need a property payload when an identity is in scope.
                let column = DfExpr::Column(datafusion::common::Column::new(
                    qualifier.cloned(),
                    field.name(),
                ));
                arg = Some(crate::expr::CYPHER_ROW_MARKER.call(vec![column]));
            }
            let percentile = a.percentile.map(|id| lowerer.lower(id)).transpose()?;
            // Resolve the argument's type against the input schema so min/max can
            // detect a heterogeneous (tagged) column (ADR 0011).
            let arg_type = arg.as_ref().and_then(|e| {
                use datafusion::logical_expr::ExprSchemable;
                e.get_type(input.schema()).ok()
            });
            let df_agg = lower_agg_func(a.func, arg, percentile, arg_type.as_ref())?;
            let df_agg = preserve_collect_order(a.func, df_agg, &input_order)?;
            Ok(df_agg.alias(&a.alias))
        })
        .collect();

    let aggregate = LogicalPlanBuilder::from(input)
        .aggregate(group_exprs, aggr_exprs?)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()?;
    if row_marker_aliases.is_empty() {
        return Ok(aggregate);
    }

    let visible_columns = aggregate
        .schema()
        .iter()
        .filter(|(_, field)| !row_marker_aliases.iter().any(|alias| alias == field.name()))
        .map(|(qualifier, field)| {
            DfExpr::Column(datafusion::common::Column::new(
                qualifier.cloned(),
                field.name(),
            ))
        })
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(aggregate)
        .project(visible_columns)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

fn lower_sort(
    keys: &[graphforge_ir::SortKey],
    input: LogicalPlan,
    lowerer: &ExprLowerer<'_>,
) -> Result<LogicalPlan, LoweringError> {
    let sort_exprs: Result<Vec<SortExpr>, LoweringError> = keys
        .iter()
        .map(|k| {
            let e = lowerer.lower(k.expr)?;
            let e = match e.get_type(input.schema()) {
                Ok(dt) if crate::expr::needs_cypher_order_key_type(&dt) => {
                    crate::expr::CYPHER_ORDER_KEY.call(vec![e])
                }
                Err(_) => crate::expr::CYPHER_ORDER_KEY.call(vec![e]),
                _ => e,
            };
            Ok(SortExpr::new(e, k.order == SortOrder::Asc, k.nulls_first))
        })
        .collect();

    LogicalPlanBuilder::from(input)
        .sort(sort_exprs?)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

fn lower_limit(count: u64, input: LogicalPlan) -> Result<LogicalPlan, LoweringError> {
    let fetch = usize::try_from(count).map_err(|_| {
        LoweringError::UnsupportedExpr(format!("LIMIT count {count} exceeds platform usize::MAX"))
    })?;
    LogicalPlanBuilder::from(input)
        .limit(0, Some(fetch))
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

fn lower_skip(count: u64, input: LogicalPlan) -> Result<LogicalPlan, LoweringError> {
    let skip = usize::try_from(count).map_err(|_| {
        LoweringError::UnsupportedExpr(format!("SKIP count {count} exceeds platform usize::MAX"))
    })?;
    LogicalPlanBuilder::from(input)
        .limit(skip, None)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

fn lower_agg_func(
    func: AggFunc,
    arg: Option<DfExpr>,
    percentile: Option<DfExpr>,
    arg_type: Option<&datafusion::arrow::datatypes::DataType>,
) -> Result<DfExpr, LoweringError> {
    // `min`/`max` over a heterogeneous (tagged) list column (ADR 0011) need Cypher
    // orderability — native min/max would order by the struct's first field.
    let het = crate::expr::is_het_struct_type(arg_type);
    match func {
        AggFunc::Count => Ok(match arg {
            Some(e) => count(e),
            None => count_all(),
        }),
        AggFunc::CountDistinct => Ok(count_distinct(arg.ok_or_else(|| {
            LoweringError::UnsupportedExpr("COUNT DISTINCT requires an argument".into())
        })?)),
        AggFunc::Sum => Ok(sum(arg.ok_or_else(|| {
            LoweringError::UnsupportedExpr("SUM requires an argument".into())
        })?)),
        AggFunc::SumDistinct => Ok(sum_distinct(arg.ok_or_else(|| {
            LoweringError::UnsupportedExpr("SUM DISTINCT requires an argument".into())
        })?)),
        AggFunc::Avg => {
            let arg = arg
                .ok_or_else(|| LoweringError::UnsupportedExpr("AVG requires an argument".into()))?;
            let arg = if matches!(arg_type, Some(datafusion::arrow::datatypes::DataType::Null)) {
                datafusion::logical_expr::expr_fn::cast(
                    arg,
                    datafusion::arrow::datatypes::DataType::Float64,
                )
            } else {
                arg
            };
            Ok(avg(arg))
        }
        AggFunc::AvgDistinct => {
            let arg = arg.ok_or_else(|| {
                LoweringError::UnsupportedExpr("AVG DISTINCT requires an argument".into())
            })?;
            let arg = if matches!(arg_type, Some(datafusion::arrow::datatypes::DataType::Null)) {
                datafusion::logical_expr::expr_fn::cast(
                    arg,
                    datafusion::arrow::datatypes::DataType::Float64,
                )
            } else {
                arg
            };
            Ok(avg_distinct(arg))
        }
        AggFunc::Min => {
            let a = arg
                .ok_or_else(|| LoweringError::UnsupportedExpr("MIN requires an argument".into()))?;
            Ok(if het {
                crate::expr::CYPHER_MIN.call(vec![a])
            } else {
                min(a)
            })
        }
        AggFunc::Max => {
            let a = arg
                .ok_or_else(|| LoweringError::UnsupportedExpr("MAX requires an argument".into()))?;
            Ok(if het {
                crate::expr::CYPHER_MAX.call(vec![a])
            } else {
                max(a)
            })
        }
        AggFunc::Collect => Ok(crate::expr::CYPHER_COLLECT.call(vec![arg.ok_or_else(|| {
            LoweringError::UnsupportedExpr("COLLECT requires an argument".into())
        })?])),
        AggFunc::CollectDistinct => {
            Ok(
                crate::expr::CYPHER_COLLECT_DISTINCT.call(vec![arg.ok_or_else(|| {
                    LoweringError::UnsupportedExpr("COLLECT requires an argument".into())
                })?]),
            )
        }
        AggFunc::PercentileDisc => Ok(crate::expr::CYPHER_PERCENTILE_DISC.call(vec![
            arg.ok_or_else(|| {
                LoweringError::UnsupportedExpr("percentileDisc requires a value argument".into())
            })?,
            percentile.ok_or_else(|| {
                LoweringError::UnsupportedExpr(
                    "percentileDisc requires a percentile argument".into(),
                )
            })?,
        ])),
        AggFunc::PercentileCont => Ok(crate::expr::CYPHER_PERCENTILE_CONT.call(vec![
            arg.ok_or_else(|| {
                LoweringError::UnsupportedExpr("percentileCont requires a value argument".into())
            })?,
            percentile.ok_or_else(|| {
                LoweringError::UnsupportedExpr(
                    "percentileCont requires a percentile argument".into(),
                )
            })?,
        ])),
    }
}

// ---------------------------------------------------------------------------
// TypeId → relation name reverse map
// ---------------------------------------------------------------------------

/// Build a `TypeId.0 → relation_name` map from the ontology at construction
/// time so that scan lowering can resolve `TypeId`s without repeated iteration.
fn build_type_id_map(
    ontology: Option<&OntologyHandle>,
) -> Result<HashMap<RelationTypeId, String>, GfError> {
    let mut map = HashMap::new();
    if let Some(h) = ontology {
        for name in h.relation_type_names() {
            if let Some(type_id) = h.relation_type_id(name) {
                map.insert(
                    RelationTypeId::ontology(type_id)
                        .map_err(|error| GfError::Validation(error.to_string()))?,
                    name.to_owned(),
                );
            }
        }
    }
    Ok(map)
}

/// Build the `TypeId.0 → [(rule_id, confidence_model)]` inference-rule map from
/// the ontology (#605): a relation flagged `transitive`/`symmetric` gets a rule
/// (`transitive:NAME` / `symmetric:NAME`) with the `conservative_min` model.
/// Empty when no ontology is loaded (exploratory) — the TCK-safety gate.
fn build_inference_rules(
    ontology: Option<&OntologyHandle>,
) -> Result<HashMap<RelationTypeId, Vec<(String, String)>>, GfError> {
    let mut map = HashMap::new();
    if let Some(h) = ontology {
        for name in h.relation_type_names() {
            if let Some(type_id) = h.relation_type_id(name) {
                let relation_id = RelationTypeId::ontology(type_id)
                    .map_err(|error| GfError::Validation(error.to_string()))?;
                let flags = h.semantic_flags(type_id);
                let mut rules = Vec::new();
                if flags.transitive {
                    rules.push((format!("transitive:{name}"), "conservative_min".to_owned()));
                }
                if flags.symmetric {
                    rules.push((format!("symmetric:{name}"), "conservative_min".to_owned()));
                }
                if !rules.is_empty() {
                    map.insert(relation_id, rules);
                }
            }
        }
    }
    Ok(map)
}

/// Build a `TypeId.0 → entity (label) name` map from the ontology, mirroring
/// [`build_type_id_map`] for relation types.  Empty in exploratory mode.
fn build_entity_id_map(
    ontology: Option<&OntologyHandle>,
) -> Result<HashMap<EntityTypeId, String>, GfError> {
    let mut map = HashMap::new();
    if let Some(h) = ontology {
        for name in h.entity_type_names() {
            if let Some(type_id) = h.entity_type_id(name) {
                map.insert(
                    EntityTypeId::ontology(type_id)
                        .map_err(|error| GfError::Validation(error.to_string()))?,
                    name.to_owned(),
                );
            }
        }
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// Scan operator lowering (#576)
// ---------------------------------------------------------------------------

/// Produce a plan alias string for a `VarId`.
fn var_alias(var: VarId) -> String {
    format!("var_{}", var.0)
}

/// Whether `op` produces rows from the store (a scan or expand) and therefore
/// supplies its own base relation, as opposed to transforming an existing one.
fn is_source_op(op: &GraphOp) -> bool {
    matches!(
        op,
        GraphOp::NodeScan { .. }
            | GraphOp::EdgeScan { .. }
            | GraphOp::TypedEdgeScan { .. }
            | GraphOp::Expand { .. }
    )
}

/// Resolve the element column **type** for an `UNWIND <list_expr>` from the
/// lowered expression's type.
///
/// If `list_expr` types to a `List`/`LargeList`/`FixedSizeList`, the element
/// field's data type is used. Otherwise (e.g. a `$param` whose type is unknown
/// pre-execution) it defaults to a nullable `Int64` — the physical node uses
/// the actual `ListArray`'s element type at run time regardless. The field name
/// is a placeholder; `UnwindNode::new` renames it to the alias so a bare
/// `RETURN x` resolves.
fn unwind_element_field(
    list_expr: &DfExpr,
    input_schema: &datafusion::common::DFSchemaRef,
    property_names: impl IntoIterator<Item = impl AsRef<str>>,
) -> datafusion::arrow::datatypes::Field {
    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use datafusion::logical_expr::ExprSchemable;

    let element_type =
        if let Ok(DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _)) =
            list_expr.get_type(input_schema.as_ref())
        {
            f.data_type().clone()
        } else {
            // Parameters are untyped until DataFusion substitutes their values.
            // Give an unknown UNWIND element a map-shaped schema containing the
            // query's interned property names so downstream `item.key` expressions
            // lower to `get_field(item, key)`. UnwindNode retypes this field from
            // the bound parameter before physical planning.
            let mut names: Vec<String> = property_names
                .into_iter()
                .map(|name| name.as_ref().to_owned())
                .collect();
            names.sort();
            names.dedup();
            if names.is_empty() {
                DataType::Int64
            } else {
                DataType::Struct(Fields::from(
                    names
                        .into_iter()
                        .map(|name| Field::new(name, DataType::Null, true))
                        .collect::<Vec<_>>(),
                ))
            }
        };
    Field::new("elem", element_type, true)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
