//! [`GraphPlanLowerer`] — converts a [`GraphPlan`] operator pipeline into a
//! DataFusion [`LogicalPlan`].
//!
//! ## Scope (M12 #575 + #576)
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
//! as [`LogicalPlan::Extension`]; their physical execution is deferred to M13.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::functions_aggregate::count::count_all;
use datafusion::functions_aggregate::expr_fn::{
    array_agg, avg, avg_distinct, count, count_distinct, max, min, sum, sum_distinct,
};
use datafusion::logical_expr::{
    Expr as DfExpr, ExprFunctionExt, ExprSchemable, Extension, JoinType, LogicalPlanBuilder,
    SortExpr, logical_plan::LogicalTableSource,
};

use graphforge_core::{GfError, OntologyMode, TypeId};
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
use graphforge_storage::{
    EXPLORATORY_EDGE_SCHEMA, GraphCatalog, TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA,
};

use crate::LogicalPlan;
use crate::expr::{ExprLowerer, LoweringError, VarMap, list_index_range};

const INPUT_ORDER_COLUMN_PREFIX: &str = "__gf_input_order_";

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
pub struct GraphPlanLowerer<'a> {
    /// The catalog used by scan operators. `None` when called from
    /// `lower()` without a catalog (scan ops return an error). Also the source
    /// of the `PropId → name` map used to resolve property accesses.
    catalog: Option<&'a GraphCatalog>,
    /// Reverse map: `TypeId.0` → relation type name.
    /// Populated at construction from the ontology; empty in exploratory mode.
    type_id_to_rel_name: HashMap<u32, String>,
    /// Reverse map: `TypeId.0` → entity (label) name.
    /// Populated at construction from the ontology; empty in exploratory mode.
    type_id_to_entity_name: HashMap<u32, String>,
    /// Write target for `CREATE`/`MERGE` lowering: the project directory and
    /// ontology mode.  Set **only** by [`new_for_writes`](Self::new_for_writes)
    /// — this is the gate that authorizes the write path.  `None` for read
    /// lowering (`CREATE` then errors).
    write_target: Option<(&'a Path, OntologyMode)>,
    /// Read-side project directory + mode, for read operators that must touch
    /// the on-disk store — variable-length `Expand` bakes this into its
    /// physical node to read edges.  Set by [`new_with_dir`](Self::new_with_dir)
    /// **and** implied by a write target.  Crucially this does **not** authorize
    /// writes, keeping the read/write separation intact.
    read_dir: Option<(&'a Path, OntologyMode)>,
    /// `VarId.0 → NodeShape`, seeded once per `lower_plan` from the plan's
    /// `NodeScan`s for bare-node-value materialization (#785). Interior
    /// mutability so the `&self` lowering pass can populate it.
    node_shapes: std::sync::RwLock<HashMap<u32, crate::expr::NodeShape>>,
    /// `TypeId.0 → [(rule_id, confidence_model)]` for relations carrying
    /// inference semantics (transitive/symmetric), built from the ontology at
    /// construction (#605). Empty in exploratory mode — the TCK-safety gate: a
    /// var-len expand only wraps in `OntologyInferNode` when this lookup is
    /// non-empty, so the no-ontology TCK plan is byte-identical.
    inference_rules: HashMap<u32, Vec<(String, String)>>,
    /// Test-only escape hatch that retains the relational fixed-hop lowering
    /// as an independent semantic oracle. Production constructors leave this
    /// disabled, so provider-backed expansion remains the sole default.
    relational_fixed_hop_reference: bool,
}

impl<'a> GraphPlanLowerer<'a> {
    /// Create a new lowerer for read/query plans.
    ///
    /// - `catalog`: the [`GraphCatalog`] used by scan operators, or `None`
    ///   when no catalog is available (scan ops will return an error).
    /// - `ontology`: the compiled ontology, or `None` in exploratory mode.
    ///
    /// Lowering a `CREATE` plan with this constructor errors — use
    /// [`new_for_writes`](Self::new_for_writes) instead.
    #[must_use]
    pub fn new(catalog: Option<&'a GraphCatalog>, ontology: Option<&'a OntologyHandle>) -> Self {
        Self::build(catalog, ontology, None, None)
    }

    /// Create a lowerer that can lower `CREATE` plans, given the project
    /// directory and ontology mode the write should target.
    ///
    /// This is the **only** constructor that authorizes the write path.  The
    /// same directory is also exposed read-side, so read operators that need it
    /// (variable-length `Expand`) work in mixed read/write pipelines.
    #[must_use]
    pub fn new_for_writes(
        catalog: Option<&'a GraphCatalog>,
        ontology: Option<&'a OntologyHandle>,
        dir: &'a Path,
        mode: OntologyMode,
    ) -> Self {
        Self::build(catalog, ontology, Some((dir, mode)), Some((dir, mode)))
    }

    /// Create a read/query lowerer that also knows the project directory.
    ///
    /// Required for queries containing variable-length `Expand`, whose physical
    /// node reads edges directly from `dir`.  Equivalent to [`new`](Self::new)
    /// for all other read operators.  Unlike [`new_for_writes`], this does
    /// **not** authorize the write path — `CREATE` still errors.
    #[must_use]
    pub fn new_with_dir(
        catalog: Option<&'a GraphCatalog>,
        ontology: Option<&'a OntologyHandle>,
        dir: &'a Path,
        mode: OntologyMode,
    ) -> Self {
        Self::build(catalog, ontology, None, Some((dir, mode)))
    }

    fn build(
        catalog: Option<&'a GraphCatalog>,
        ontology: Option<&'a OntologyHandle>,
        write_target: Option<(&'a Path, OntologyMode)>,
        read_dir: Option<(&'a Path, OntologyMode)>,
    ) -> Self {
        // Relation-name map: ontology IDs and tagged runtime-catalog IDs occupy
        // disjoint plan key spaces. This is essential in advisory mode, where
        // both source ID spaces begin at zero and an unknown relation must not
        // resolve through a colliding ontology ID.
        let mut type_id_to_rel_name = build_type_id_map(ontology);
        if let Some(c) = catalog {
            for (id, name) in c.rel_names() {
                let plan_id =
                    graphforge_ir::runtime_relation_type_id(graphforge_ir::RuntimeTypeId(*id));
                type_id_to_rel_name.insert(plan_id.0, name.clone());
            }
        }
        Self {
            catalog,
            type_id_to_rel_name,
            // Ontology-only: this map drives property-table routing
            // (`node_prop_cols` / `join_node_properties`) and write specs, where
            // an exploratory node's properties live in `_untyped` (not a
            // per-label table). The runtime-catalog labels are merged in
            // separately for node-value rendering only — see `expr_lowerer`.
            type_id_to_entity_name: build_entity_id_map(ontology),
            write_target,
            read_dir,
            node_shapes: std::sync::RwLock::new(HashMap::new()),
            inference_rules: build_inference_rules(ontology),
            relational_fixed_hop_reference: false,
        }
    }

    /// Select the legacy relational fixed-hop lowering as a differential-test
    /// oracle. This is deliberately doc-hidden and never enabled by production
    /// API paths.
    #[doc(hidden)]
    #[must_use]
    pub fn with_relational_fixed_hop_reference(mut self) -> Self {
        self.relational_fixed_hop_reference = true;
        self
    }

    /// The project directory available to read operators (scans bind their real
    /// Parquet-backed providers from it). `None` for pure logical/explain
    /// lowering, where scans use a schema-only source.
    fn read_dir(&self) -> Option<&'a Path> {
        self.read_dir.map(|(d, _)| d)
    }

    /// The ontology mode for read operators. Defaults to `Exploratory` for pure
    /// logical/explain lowering (`read_dir` is `None`), where scans use a
    /// schema-only source and the mode is never consulted.
    fn read_mode(&self) -> OntologyMode {
        self.read_dir.map_or(OntologyMode::Exploratory, |(_, m)| m)
    }

    /// The `PropId.0 → name` map for resolving `PropertyAccess`.
    ///
    /// Sourced from the catalog (built from the runtime catalog at `open` time).
    /// With no catalog (pure logical/explain lowering) the map is empty, so
    /// property accesses fall back to `"prop_<id>"` — those paths render plans,
    /// not data.
    fn prop_names(&self) -> HashMap<u32, String> {
        match self.catalog {
            Some(c) => c.prop_names().clone(),
            None => HashMap::new(),
        }
    }

    /// Build an `ExprLowerer` for the current plan, seeded with the resolved
    /// `PropId → name` map.
    fn expr_lowerer<'b>(&self, arena: &'b ExprArena, var_map: &'b VarMap) -> ExprLowerer<'b> {
        // Label-name map for node-value rendering (#889): the ontology entity
        // names plus the runtime catalog's labels (exploratory). Built here,
        // separate from `type_id_to_entity_name` (which must stay ontology-only
        // for property-table routing), so an unlabelled `MATCH (n) RETURN n` can
        // resolve a node's stored `type_id` to its label name.
        let mut node_label_names = self.type_id_to_entity_name.clone();
        if let Some(c) = self.catalog {
            for (id, name) in c.label_names() {
                node_label_names.entry(*id).or_insert_with(|| name.clone());
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
            // under `read_dir`. With no dir (schema-only/explain lowering) an empty
            // `prop_names` means "unknown", not "absent", so the missing-property→
            // null rewrite (#598) must NOT fire — gate it on having the dataset.
            self.read_dir().is_some(),
        );
        // With a dataset attached, `nodes(p)` hydrates its elements (#1024).
        if let Some(dir) = self.read_dir() {
            lowerer = lowerer.with_read_target(dir.to_path_buf());
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

    /// The property-file stem for a node of type `ty` — the table
    /// `join_node_properties` reads `var_N.<prop>` from, and `node_prop_cols`
    /// reads the column names from.
    ///
    /// - labelled: the ontology entity name, or `_untyped` when the label is only
    ///   known to the runtime catalog (exploratory mode);
    /// - unlabelled in **exploratory** mode: `_untyped`, where all exploratory
    ///   properties are written — so `MATCH (n) RETURN n` carries its props (#889);
    /// - unlabelled in an **ontology** mode: `None` — properties are spread across
    ///   per-entity tables with no single stem (a multi-table union; out of scope).
    fn prop_table_stem(&self, ty: Option<TypeId>) -> Option<String> {
        match ty {
            Some(type_id) => Some(
                self.type_id_to_entity_name
                    .get(&type_id.0)
                    .cloned()
                    .unwrap_or_else(|| "_untyped".to_owned()),
            ),
            None if matches!(self.read_mode(), OntologyMode::Exploratory) => {
                Some("_untyped".to_owned())
            }
            None => None,
        }
    }

    /// The persisted property column names a node of type `ty` carries — the
    /// columns `join_node_properties` materializes as `var_N.<name>`. Empty in
    /// schema-only lowering or when no single property table applies (see
    /// [`prop_table_stem`](Self::prop_table_stem)).
    fn node_prop_cols(&self, ty: Option<TypeId>) -> Vec<String> {
        let Some(dir) = self.read_dir() else {
            return Vec::new();
        };
        let Some(stem) = self.prop_table_stem(ty) else {
            return Vec::new();
        };
        let prop_table = graphforge_storage::PropertyTable::open_discovered(dir, &stem);
        prop_table
            .schema_ref()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| TOPOLOGY_NODES_SCHEMA.field_with_name(n).is_err())
            .collect()
    }

    /// LEFT-join the property table for a labelled node `var` onto `scan`, so a
    /// later `PropertyAccess` (`var_N.<prop>`) resolves to a real column (#704).
    ///
    /// The property values live in `properties/<Entity>.parquet` (strict /
    /// advisory) or `properties/_untyped.parquet` (exploratory), keyed by
    /// `node_uuid`. We join on `var_N.node_uuid = <props>.node_uuid`, pass **every
    /// column already in `scan` through unchanged**, then append each property
    /// column (except the duplicate `node_uuid` key) **re-qualified** under
    /// `var_N` so it resolves as `var_N.<prop>`.
    ///
    /// Preserving all of `scan`'s existing columns (rather than just the joined
    /// node's topology columns) is what lets this run on **both** a fresh
    /// single-var node scan *and* an already-joined multi-var plan — e.g. the
    /// destination of a fixed single-hop `Expand`, whose plan carries the source
    /// and edge columns too (#789). On a fresh single-var scan the preserved set
    /// is exactly that node's topology columns, so the result is unchanged.
    ///
    /// No-ops (returns `scan` unchanged) when:
    /// - there is no project directory (schema-only explain/golden lowering),
    /// - no single property table applies — an unlabelled node in an ontology
    ///   mode, whose properties are spread across per-entity tables (see
    ///   [`prop_table_stem`](Self::prop_table_stem); an unlabelled node in
    ///   *exploratory* mode resolves to `_untyped` and DOES join), or
    /// - the property table has no columns beyond `node_uuid`.
    ///
    /// A LEFT join preserves nodes that have no property row written yet.
    fn join_node_properties(
        &self,
        var: VarId,
        ty: Option<TypeId>,
        scan: LogicalPlan,
    ) -> Result<LogicalPlan, LoweringError> {
        use datafusion::common::Column;
        use datafusion::logical_expr::col;

        let Some(dir) = self.read_dir() else {
            return Ok(scan); // schema-only lowering: no real provider to join
        };
        let Some(stem) = self.prop_table_stem(ty) else {
            return Ok(scan); // no single property table applies (see prop_table_stem)
        };

        let prop_table = graphforge_storage::PropertyTable::open_discovered(dir, &stem);
        let prop_schema = prop_table.schema_ref();
        let node_alias = var_alias(var);

        // Property columns already present under this var's qualifier. A var whose
        // properties were joined upstream and then re-matched — e.g. forwarded
        // through `WITH a` and matched again in `MATCH (a)-[…]->(b)` — must NOT
        // have them joined a second time: that re-qualifies a second `var_N.<prop>`
        // and a later `RETURN *` projects two columns of the same name, which
        // DataFusion rejects ("Projections require unique expression names").
        let existing: std::collections::HashSet<String> = scan
            .schema()
            .iter()
            .filter(|(q, _)| q.is_some_and(|t| t.table() == node_alias.as_str()))
            .map(|(_, f)| f.name().clone())
            .collect();

        // The non-key property columns to ADD. Exclude every node-topology column
        // name (not just the `node_uuid` join key): a property that happens to
        // share a topology column name — `node_id`, `type_id`, `created_at`,
        // `updated_at` — would re-qualify to a second `var_N.<name>` field and
        // build a duplicate-qualified schema DataFusion rejects. Dropping the
        // collision keeps the topology column authoritative. Also exclude any
        // property already present under this var (idempotent re-join, above). If
        // nothing remains, the join would add nothing — skip it.
        let prop_cols: Vec<String> = prop_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| TOPOLOGY_NODES_SCHEMA.field_with_name(n).is_err())
            .filter(|n| !existing.contains(n))
            .collect();
        if prop_cols.is_empty() {
            return Ok(scan);
        }

        let prop_alias = format!("{node_alias}__props");
        let prop_src = datafusion::datasource::provider_as_source(Arc::new(prop_table));
        let prop_scan = LogicalPlanBuilder::scan(prop_alias.clone(), prop_src, None)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

        // Snapshot the input's existing qualified columns BEFORE the join, so the
        // projection preserves exactly what `scan` carried (one var for a fresh
        // node scan; src + edge + dst for an already-joined Expand plan) — and
        // never accidentally re-projects the joined-in `<prop>` columns by their
        // bare names.
        let input_cols: Vec<Column> = scan
            .schema()
            .iter()
            .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
            .collect();

        // LEFT join: node ⟕ props ON node.node_uuid = props.node_uuid.
        let join_pred =
            col(format!("{node_alias}.node_uuid")).eq(col(format!("{prop_alias}.node_uuid")));
        let joined = LogicalPlanBuilder::from(scan)
            .join_on(prop_scan, JoinType::Left, vec![join_pred])
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

        // Project: every pre-join input column through unchanged, then each
        // property column re-qualified under var_N so `var_N.<prop>` resolves.
        let mut projections: Vec<DfExpr> = input_cols.into_iter().map(DfExpr::Column).collect();
        for name in &prop_cols {
            projections.push(
                crate::expr::qualified_col(&prop_alias, name)
                    .alias_qualified(Some(node_alias.as_str()), name.as_str()),
            );
        }

        LogicalPlanBuilder::from(joined)
            .project(projections)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
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
            .map_err(|e| GfError::Plan(e.to_string()))
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
            .map_err(|e| GfError::Plan(e.to_string()))
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
            .map_err(|e| GfError::Plan(e.to_string()))
    }

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
            .map_err(|e| GfError::Plan(e.to_string()))
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
            .map_err(|e| GfError::Plan(e.to_string()))
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
            .map_err(|e| GfError::Plan(e.to_string()))
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
            .map_err(|e| GfError::Plan(e.to_string()))
    }

    /// The `TypeId.0 → entity name` map (from the ontology), for per-row
    /// property-file stem resolution in the statement driver.
    #[must_use]
    pub fn entity_name_map(&self) -> HashMap<u32, String> {
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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

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
                        enrich_bound_node_identity(&input, alias, self.read_dir())?
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
                let scan = lower_node_scan(*var, *ty, var_map, self.read_dir(), pending_nodes)?;
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
                    .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
                    .build()
                    .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()));
            }
            GraphOp::TypedEdgeScan { var, rel_ty } => {
                return lower_typed_edge_scan(
                    *var,
                    *rel_ty,
                    var_map,
                    self.catalog,
                    &self.type_id_to_rel_name,
                    self.read_dir(),
                    self.read_mode(),
                );
            }
            GraphOp::EdgeScan { var, ty } => {
                return lower_edge_scan(
                    *var,
                    *ty,
                    var_map,
                    &self.type_id_to_rel_name,
                    self.read_dir(),
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
                    self.catalog,
                    &self.type_id_to_rel_name,
                    &self.inference_rules,
                    self.read_dir,
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
                    let value = |alias: &str| {
                        if alias.ends_with(graphforge_plan::VAR_LEN_EDGE_LIST_FIELD) {
                            col(alias)
                        } else {
                            col(format!("{alias}.edge_uuid"))
                        }
                    };
                    Ok(crate::expr::relationship_disjoint(
                        value(edge_alias),
                        value(prior_alias),
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
                    .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()));
            }
            // UNWIND — emit the graphforge-plan stub Extension node (physical execution
            // deferred to M13).  Needs the expression lowerer for `list_expr`.
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
                return Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }));
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

    fn lower_union_op(
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

    fn lower_exists_op(
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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
    }

    fn lower_exists_alternatives(
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
                .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
            key_union = Some(match key_union {
                None => key_plan,
                Some(union) => LogicalPlanBuilder::from(union)
                    .union(key_plan)
                    .and_then(LogicalPlanBuilder::build)
                    .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?,
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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
    }

    fn lower_pattern_comprehension_op(
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
        let element_type = value
            .get_type(child_plan.schema())
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
        var_map.insert(output, output_alias);
        Ok(result)
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "ordinal unwind, child correlation, and ordered regroup form one lowering operation"
    )]
    fn lower_list_element_pattern_comprehension_op(
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
        let DataType::List(item) = list
            .get_type(input.schema())
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        else {
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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

        let mut index_projection = plan_columns(&indexed);
        index_projection
            .push(list_index_range(DfExpr::Column(Column::from_name(LIST))).alias(INDICES));
        let with_indices = LogicalPlanBuilder::from(indexed.clone())
            .project(index_projection)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
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
        let element_type = value
            .get_type(filtered.schema())
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
        let output_alias = format!("__gf_list_pattern_{}", output.0);
        let ordered = array_agg(value)
            .order_by(vec![
                DfExpr::Column(Column::from_name(INDEX)).sort(true, true),
            ])
            .build()
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

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
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        .alias(&output_alias);
        let mut final_projection = outer_columns;
        final_projection.push(result_list);
        let result = LogicalPlanBuilder::from(joined)
            .project(final_projection)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
        var_map.insert(output, output_alias);
        Ok(result)
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
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
            .build()
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
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
                    .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
                    .build()
                    .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
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
                .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
                .build()
                .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        } else {
            filtered
        };

        if distinct {
            output = LogicalPlanBuilder::from(output)
                .distinct()
                .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
                .build()
                .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
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

    /// Lower a [`GraphOp::Create`] into a self-contained [`GraphCreateNode`]
    /// wrapped as a DataFusion [`Extension`].
    ///
    /// Resolves each spec's label/relation-type name (from the ontology maps)
    /// and evaluates its property map here: constant values bake to literals,
    /// row-dependent values lower to DataFusion `Expr`s over the input columns
    /// (evaluated per row by the execution layer) — so the executor needs no
    /// access to the IR arena or ontology.  The write target (directory + mode)
    /// must have been supplied via [`new_for_writes`](Self::new_for_writes).
    fn lower_create(
        &self,
        pattern: &CreatePattern,
        exprs: &ExprArena,
        input: LogicalPlan,
        var_map: &mut VarMap,
        feeds_read: bool,
    ) -> Result<LogicalPlan, LoweringError> {
        let (dir, mode) = self.write_target.ok_or_else(|| {
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
            let node = GraphCreateNode::new_emitting(
                Arc::new(input),
                nodes,
                edges,
                dir.to_path_buf(),
                mode,
                out_schema,
            );
            return Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            }));
        }

        let node = GraphCreateNode::new(Arc::new(input), nodes, edges, dir.to_path_buf(), mode);
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
                let ty = expr
                    .get_type(input_schema)
                    .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
                push(name, ty, true);
            }
        }
        let schema = DFSchema::new_with_metadata(qualified, HashMap::new())
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
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
                    label_ids: n.labels.iter().map(|t| t.0).collect(),
                    label_names: n
                        .labels
                        .iter()
                        .filter_map(|t| self.type_id_to_entity_name.get(&t.0).cloned())
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
                    rel_type_id: e.rel_type.map(|t| t.0),
                    rel_type_name: e
                        .rel_type
                        .and_then(|t| self.type_id_to_rel_name.get(&t.0).cloned()),
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
    fn lower_delete_op(
        &self,
        vars: &[VarId],
        detach: bool,
        input: LogicalPlan,
    ) -> Result<LogicalPlan, LoweringError> {
        use datafusion::common::TableReference;

        let (dir, mode) = self.write_target.ok_or_else(|| {
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

        let node = GraphDeleteNode::new(Arc::new(input), targets, detach, dir.to_path_buf(), mode);
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// Resolve a write-target variable's node-vs-edge kind from `schema`.
    ///
    /// A node var carries a `var_<n>.node_uuid` column; an edge var a
    /// `var_<n>.edge_uuid` column. An edge var must also carry the
    /// `var_<n>.rel_type_name` column — the per-row file stem for edge property
    /// writes (present in Exploratory mode; a typed-only Strict-mode edge has no
    /// such column and is rejected here, a documented #791 follow-up). Returns
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
    fn lower_set_op(
        &self,
        items: &[SetPropItem],
        input: LogicalPlan,
        exprs: &ExprArena,
        var_map: &VarMap,
    ) -> Result<LogicalPlan, LoweringError> {
        let (dir, mode) = self.write_target.ok_or_else(|| {
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

        let node = GraphSetNode::new(
            Arc::new(input),
            targets,
            self.type_id_to_entity_name.clone(),
            dir.to_path_buf(),
            mode,
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// Lower a [`GraphOp::Remove`] into a [`GraphRemoveNode`] Extension (#791) —
    /// the value-less dual of [`lower_set_op`](Self::lower_set_op).
    fn lower_remove_op(
        &self,
        items: &[RemovePropItem],
        input: LogicalPlan,
    ) -> Result<LogicalPlan, LoweringError> {
        let (dir, mode) = self.write_target.ok_or_else(|| {
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

        let node = GraphRemoveNode::new(
            Arc::new(input),
            targets,
            self.type_id_to_entity_name.clone(),
            dir.to_path_buf(),
            mode,
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
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
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
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
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

    if distinct {
        LogicalPlanBuilder::from(plan)
            .distinct()
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
            .build()
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
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
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
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
                && let Some((qualifier, field)) = input.schema().iter().last()
            {
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
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
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
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
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
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
}

fn lower_limit(count: u64, input: LogicalPlan) -> Result<LogicalPlan, LoweringError> {
    let fetch = usize::try_from(count).map_err(|_| {
        LoweringError::UnsupportedExpr(format!("LIMIT count {count} exceeds platform usize::MAX"))
    })?;
    LogicalPlanBuilder::from(input)
        .limit(0, Some(fetch))
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
}

fn lower_skip(count: u64, input: LogicalPlan) -> Result<LogicalPlan, LoweringError> {
    let skip = usize::try_from(count).map_err(|_| {
        LoweringError::UnsupportedExpr(format!("SKIP count {count} exceeds platform usize::MAX"))
    })?;
    LogicalPlanBuilder::from(input)
        .limit(skip, None)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
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
fn build_type_id_map(ontology: Option<&OntologyHandle>) -> HashMap<u32, String> {
    let mut map = HashMap::new();
    if let Some(h) = ontology {
        for name in h.relation_type_names() {
            if let Some(type_id) = h.relation_type_id(name) {
                map.insert(type_id.0, name.to_owned());
            }
        }
    }
    map
}

/// Build the `TypeId.0 → [(rule_id, confidence_model)]` inference-rule map from
/// the ontology (#605): a relation flagged `transitive`/`symmetric` gets a rule
/// (`transitive:NAME` / `symmetric:NAME`) with the `conservative_min` model.
/// Empty when no ontology is loaded (exploratory) — the TCK-safety gate.
fn build_inference_rules(ontology: Option<&OntologyHandle>) -> HashMap<u32, Vec<(String, String)>> {
    let mut map = HashMap::new();
    if let Some(h) = ontology {
        for name in h.relation_type_names() {
            if let Some(type_id) = h.relation_type_id(name) {
                let flags = h.semantic_flags(type_id);
                let mut rules = Vec::new();
                if flags.transitive {
                    rules.push((format!("transitive:{name}"), "conservative_min".to_owned()));
                }
                if flags.symmetric {
                    rules.push((format!("symmetric:{name}"), "conservative_min".to_owned()));
                }
                if !rules.is_empty() {
                    map.insert(type_id.0, rules);
                }
            }
        }
    }
    map
}

/// Build a `TypeId.0 → entity (label) name` map from the ontology, mirroring
/// [`build_type_id_map`] for relation types.  Empty in exploratory mode.
fn build_entity_id_map(ontology: Option<&OntologyHandle>) -> HashMap<u32, String> {
    let mut map = HashMap::new();
    if let Some(h) = ontology {
        for name in h.entity_type_names() {
            if let Some(type_id) = h.entity_type_id(name) {
                map.insert(type_id.0, name.to_owned());
            }
        }
    }
    map
}

/// Constant-fold a lowered value expression to a scalar: a literal is taken
/// directly; anything else is evaluated against a single empty row (so a pure
/// constant like `1 + 2` folds, while a column/variable reference fails to
/// resolve against the empty schema and yields `None` — kept deferred). (#814)
fn const_eval_scalar(df: &DfExpr) -> Option<datafusion::scalar::ScalarValue> {
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

fn eval_map_literal(
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

fn reject_map_property_value(prop_name: &str, lit: &IrLiteral) -> Result<(), LoweringError> {
    if contains_map_literal(lit) {
        return Err(LoweringError::UnsupportedExpr(format!(
            "CREATE property `{prop_name}` cannot store map values"
        )));
    }
    Ok(())
}

fn contains_map_literal(lit: &IrLiteral) -> bool {
    match lit {
        IrLiteral::Map(_) => true,
        IrLiteral::List(items) => items.iter().any(contains_map_literal),
        _ => false,
    }
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

/// Wrap a schema in a [`LogicalTableSource`] suitable for
/// [`LogicalPlanBuilder::scan`].
fn table_source(schema: datafusion::arrow::datatypes::SchemaRef) -> Arc<LogicalTableSource> {
    Arc::new(LogicalTableSource::new(schema))
}

/// The data source for a node scan.
///
/// With a project `dir`, build a real Parquet-backed [`TopologyNodeTable`]
/// provider (wrapped via `provider_as_source`) so the scan reads actual rows at
/// execution time. Without a `dir` (pure logical/explain lowering, e.g. golden
/// tests), fall back to the schema-only [`LogicalTableSource`].
fn node_scan_source(dir: Option<&Path>) -> Arc<dyn datafusion::logical_expr::TableSource> {
    use datafusion::datasource::provider_as_source;
    match dir {
        Some(d) => {
            let path = d.join("topology").join("nodes.parquet");
            provider_as_source(Arc::new(graphforge_storage::TopologyNodeTable::new(path)))
        }
        None => table_source(TOPOLOGY_NODES_SCHEMA.clone()),
    }
}

/// The data source for an edge scan over `stem` (a relation name for the typed
/// table, or `"_exploratory"`). Real provider when `dir` is set; otherwise the
/// schema-only source (`schema` chooses typed vs exploratory shape).
///
/// In a typed project (Strict/Advisory) there is no `_exploratory.parquet`, so
/// an untyped edge scan over the `"_exploratory"` stem reads the **union** of
/// every per-relation file via [`graphforge_storage::UnionEdgeTable`] (#823) — the same
/// `EXPLORATORY_EDGE_SCHEMA`-shaped, `rel_type_name`-tagged rows the shared
/// exploratory file would have carried, so the untyped single-hop join path is
/// unchanged otherwise. Exploratory mode keeps reading its shared file.
fn edge_scan_source(
    dir: Option<&Path>,
    stem: &str,
    schema: datafusion::arrow::datatypes::SchemaRef,
    mode: OntologyMode,
) -> Arc<dyn datafusion::logical_expr::TableSource> {
    use datafusion::datasource::provider_as_source;
    match dir {
        Some(d)
            if stem == "_exploratory"
                && matches!(mode, OntologyMode::Strict | OntologyMode::Advisory) =>
        {
            provider_as_source(Arc::new(graphforge_storage::UnionEdgeTable::open(d)))
        }
        Some(d) => provider_as_source(Arc::new(graphforge_storage::TypedEdgeTable::open(d, stem))),
        None => table_source(schema),
    }
}

/// Filter an already-bound node variable by its label type.
///
/// Used when a destination node's `NodeScan` is a no-op (the var was bound by a
/// preceding `Expand`) but still carries a label: the label predicate is applied
/// against the already-present `<alias>.type_id` column.
fn filter_node_by_type(
    input: LogicalPlan,
    alias: &str,
    type_id: TypeId,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::functions_nested::expr_fn::array_has;
    use datafusion::logical_expr::{col, lit};
    LogicalPlanBuilder::from(input)
        .filter(array_has(col(format!("{alias}.type_ids")), lit(type_id.0)))
        .and_then(LogicalPlanBuilder::build)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
}

fn enrich_bound_node_identity(
    input: &LogicalPlan,
    alias: &str,
    dir: Option<&Path>,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::common::Column;
    use datafusion::logical_expr::col;

    let identity_alias = format!("__gf_identity_{alias}");
    let identity = LogicalPlanBuilder::scan(identity_alias.clone(), node_scan_source(dir), None)
        .and_then(LogicalPlanBuilder::build)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
    let joined = LogicalPlanBuilder::from(input.clone())
        .join(
            identity,
            datafusion::logical_expr::JoinType::Inner,
            (
                vec![Column::from_qualified_name(format!("{alias}.node_uuid"))],
                vec![Column::from_qualified_name(format!(
                    "{identity_alias}.node_uuid"
                ))],
            ),
            None,
        )
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
    let mut projection = input
        .schema()
        .iter()
        .map(|(qualifier, field)| DfExpr::Column(Column::new(qualifier.cloned(), field.name())))
        .collect::<Vec<_>>();
    for name in ["node_id", "type_id", "type_ids"] {
        projection.push(col(format!("{identity_alias}.{name}")).alias_qualified(Some(alias), name));
    }
    LogicalPlanBuilder::from(joined)
        .project(projection)
        .and_then(LogicalPlanBuilder::build)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
}

fn lower_node_scan(
    var: VarId,
    ty: Option<TypeId>,
    var_map: &mut VarMap,
    dir: Option<&Path>,
    pending_nodes: Option<&RecordBatch>,
) -> Result<LogicalPlan, LoweringError> {
    let alias = var_alias(var);
    var_map.insert(var, alias.clone());

    let src = if let Some(batch) = pending_nodes.filter(|batch| batch.num_rows() > 0) {
        use datafusion::datasource::{MemTable, provider_as_source};
        let mut batches = dir
            .map(graphforge_storage::read_nodes)
            .transpose()
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?
            .unwrap_or_default();
        batches.push(batch.clone());
        let table = MemTable::try_new(batch.schema(), vec![batches])
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
        provider_as_source(Arc::new(table))
    } else {
        node_scan_source(dir)
    };
    let mut builder = LogicalPlanBuilder::scan(alias.clone(), src, None)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

    if let Some(type_id) = ty {
        use datafusion::functions_nested::expr_fn::array_has;
        use datafusion::logical_expr::{col, lit};
        builder = builder
            .filter(array_has(col(format!("{alias}.type_ids")), lit(type_id.0)))
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
    }

    builder
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
}

fn lower_typed_edge_scan(
    var: VarId,
    rel_ty: TypeId,
    var_map: &mut VarMap,
    catalog: Option<&GraphCatalog>,
    type_id_to_rel_name: &HashMap<u32, String>,
    dir: Option<&Path>,
    mode: OntologyMode,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::catalog::CatalogProvider;
    use datafusion::logical_expr::{col, lit};

    let alias = var_alias(var);
    var_map.insert(var, alias.clone());

    // Require a known relation name — silently falling back to _exploratory
    // would change query semantics (wrong table / over-scan).
    let rel_name = type_id_to_rel_name.get(&rel_ty.0).ok_or_else(|| {
        LoweringError::UnsupportedExpr(format!(
            "TypedEdgeScan: TypeId({}) has no known relation name; \
             ontology may be incomplete or stale",
            rel_ty.0
        ))
    })?;

    // Check if the catalog has a typed edge table for this relation.
    let use_exploratory = catalog
        .and_then(|c| c.schema("graph"))
        .is_none_or(|s| !s.table_exist(&format!("edges_{rel_name}")));

    if use_exploratory {
        let src = edge_scan_source(dir, "_exploratory", EXPLORATORY_EDGE_SCHEMA.clone(), mode);
        let filter_expr = col("rel_type_name").eq(lit(rel_name.as_str()));
        // Use alias as the scan qualifier so var_map column refs resolve correctly.
        LogicalPlanBuilder::scan(alias, src, None)
            .and_then(|b| b.filter(filter_expr))
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
    } else {
        let src = edge_scan_source(dir, rel_name, TYPED_EDGE_SCHEMA.clone(), mode);
        // Use alias as the scan qualifier so downstream join predicates
        // (var_map.get(edge) → "var_N") can resolve edge columns correctly.
        LogicalPlanBuilder::scan(alias, src, None)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
    }
}

fn lower_edge_scan(
    var: VarId,
    ty: Option<TypeId>,
    var_map: &mut VarMap,
    type_id_to_rel_name: &HashMap<u32, String>,
    dir: Option<&Path>,
    mode: OntologyMode,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::logical_expr::{col, lit};

    let alias = var_alias(var);
    var_map.insert(var, alias.clone());

    let src = edge_scan_source(dir, "_exploratory", EXPLORATORY_EDGE_SCHEMA.clone(), mode);
    let mut builder = LogicalPlanBuilder::scan(alias, src, None)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

    if let Some(type_id) = ty
        && let Some(name) = type_id_to_rel_name.get(&type_id.0)
    {
        builder = builder
            .filter(col("rel_type_name").eq(lit(name.as_str())))
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
    }

    builder
        .build()
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
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

/// Lower a variable-length `Expand` (`min_hops != 1 || max_hops != Some(1)`)
/// into the `VarLenExpandNode` Extension whose physical execution (M13) runs an
/// iterative BFS over the edge table.
#[allow(clippy::too_many_arguments)]
#[allow(
    clippy::too_many_lines,
    reason = "property discovery, bound-variable correlation, and extension shaping are one lowering operation"
)]
fn lower_var_len_expand(
    src: VarId,
    edge: VarId,
    dst: VarId,
    rel_ty: Option<TypeId>,
    dir: Direction,
    min_hops: u16,
    max_hops: Option<u16>,
    input: LogicalPlan,
    var_map: &mut VarMap,
    type_id_to_rel_name: &HashMap<u32, String>,
    inference_rules: &HashMap<u32, Vec<(String, String)>>,
    target: Option<(&Path, OntologyMode)>,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::logical_expr::col;

    let bound_edge_list = var_map.get(edge).map(str::to_owned);
    let bound_dst = var_map.get(dst).map(str::to_owned);
    let traversal_dst = bound_dst
        .as_ref()
        .map_or(dst, |_| VarId(u32::MAX.saturating_sub(dst.0)));
    let rel_name = match rel_ty {
        Some(rt) => type_id_to_rel_name.get(&rt.0).cloned().ok_or_else(|| {
            LoweringError::UnsupportedExpr(format!(
                "VarLenExpand: TypeId({}) has no known relation name; \
                 ontology may be incomplete or stale",
                rt.0
            ))
        })?,
        None => "*".to_owned(),
    };
    // Ontology inference (#605): if this relation carries semantic rules
    // (transitive/symmetric), wrap the var-len traversal in OntologyInferNode(s)
    // so the closure is auditable. Empty in exploratory mode (no ontology) → the
    // TCK-safety gate. Captured here before `rel_name` is moved into the node.
    let infer_rules: Vec<(String, String)> = rel_ty
        .and_then(|rt| inference_rules.get(&rt.0))
        .cloned()
        .unwrap_or_default();
    let rel_for_infer = rel_name.clone();
    // The physical node reads edges directly from the project directory, so the
    // dir/mode must be threaded through at lowering time (the ExtensionPlanner
    // only sees DataFusion session state).
    let (dir_path, mode) = target.ok_or_else(|| {
        LoweringError::UnsupportedExpr(
            "variable-length expand requires a project directory; \
             lower via new_for_writes or new_with_dir"
                .into(),
        )
    })?;
    // Discover the relation's persisted edge-property columns (#755) so the
    // edge-list struct carries them and `r[i].<prop>` resolves. Mirrors the
    // fixed-hop `join_edge_properties`: read the dynamic on-disk schema and drop
    // the `edge_uuid` key + any name colliding with the struct's four topology
    // fields (`edge_uuid`/`src_uuid`/`dst_uuid`/`rel_type`). A wildcard (`*`)
    // has no single property file, so it unions EVERY relation's fields (#1023)
    // — sorted stem order for determinism, first occurrence of a name wins,
    // forced nullable since an edge from a relation without the column is NULL
    // (the exec coalesces each edge's values from its own relation's file).
    let topology_names = ["edge_uuid", "src_uuid", "dst_uuid", "rel_type"];
    let prop_fields: Vec<datafusion::arrow::datatypes::Field> = if rel_name == "*" {
        let mut seen = std::collections::HashSet::new();
        let mut fields = Vec::new();
        for stem in graphforge_storage::list_edge_property_stems(dir_path) {
            let prop_table =
                graphforge_storage::EdgePropertyTable::open_discovered(dir_path, &stem);
            for f in prop_table.schema_ref().fields() {
                if topology_names.contains(&f.name().as_str()) {
                    continue;
                }
                if seen.insert(f.name().clone()) {
                    fields.push(f.as_ref().clone().with_nullable(true));
                }
            }
        }
        fields
    } else {
        let prop_table =
            graphforge_storage::EdgePropertyTable::open_discovered(dir_path, &rel_name);
        prop_table
            .schema_ref()
            .fields()
            .iter()
            .filter(|f| !topology_names.contains(&f.name().as_str()))
            .map(|f| f.as_ref().clone())
            .collect()
    };

    // The output extends the input with the destination node's columns,
    // qualified `var_<dst>` (mirrors a `NodeScan(dst)`), then a trailing
    // edge-list column qualified `var_<edge>` (#709).  Pass the field lists in
    // so graphforge-plan need not depend on graphforge-storage; the edge-list field type is the
    // shared single source of truth in graphforge-plan.
    let dst_fields = TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect();
    let node = VarLenExpandNode::new(
        Arc::new(input),
        rel_name,
        min_hops,
        max_hops,
        src.0,
        traversal_dst.0,
        edge.0,
        dir,
        rel_ty.map(|t| t.0),
        dir_path.to_path_buf(),
        mode,
        dst_fields,
        graphforge_plan::var_len_edge_list_field(&prop_fields),
    );
    // Register the destination var so the binder's trailing NodeScan(dst)
    // recognises it as already bound (and stays a no-op, preserving this
    // Extension node as the plan root).
    if bound_dst.is_none() {
        var_map.insert(dst, var_alias(dst));
    }
    // Register the edge var bound to the single `List<Struct>` relationship-list
    // column.  Unlike a node var (which stores the bare `var_<n>` qualifier and
    // resolves only via `n.<field>`), the edge var maps to one concrete column,
    // so it is registered fully-qualified: `RETURN r` lowers to
    // `col("var_<edge>.rels")` and `length(r)` to `array_length` over it.
    var_map.insert(
        edge,
        format!(
            "var_{}.{}",
            edge.0,
            graphforge_plan::VAR_LEN_EDGE_LIST_FIELD
        ),
    );
    let mut base = LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    });
    if let Some(bound_dst) = bound_dst {
        let traversal_alias = var_alias(traversal_dst);
        base = LogicalPlanBuilder::from(base)
            .filter(
                col(format!("{traversal_alias}.node_uuid"))
                    .eq(col(format!("{bound_dst}.node_uuid"))),
            )
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
        let traversal_ref = datafusion::common::TableReference::bare(traversal_alias);
        let projection = base
            .schema()
            .iter()
            .filter(|(qualifier, _)| {
                qualifier
                    .as_ref()
                    .is_none_or(|qualifier| **qualifier != traversal_ref)
            })
            .map(|(qualifier, field)| {
                DfExpr::Column(datafusion::common::Column::new(
                    qualifier.cloned(),
                    field.name(),
                ))
            })
            .collect::<Vec<_>>();
        base = LogicalPlanBuilder::from(base)
            .project(projection)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
    }
    if let Some(bound_edge_list) = bound_edge_list {
        let produced = format!(
            "var_{}.{}",
            edge.0,
            graphforge_plan::VAR_LEN_EDGE_LIST_FIELD
        );
        base = LogicalPlanBuilder::from(base)
            .filter(col(bound_edge_list).eq(col(produced)))
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
    }
    // Wrap in one OntologyInferNode per applicable rule (#605); the execution
    // session records a kind="inference" provenance event per rule. Pass-through
    // physically — VarLenExpand already computes the closure.
    let plan = infer_rules
        .into_iter()
        .fold(base, |acc, (rule_id, conf_model)| {
            LogicalPlan::Extension(Extension {
                node: Arc::new(graphforge_plan::OntologyInferNode::new(
                    Arc::new(acc),
                    rel_for_infer.clone(),
                    rule_id,
                    conf_model,
                )),
            })
        });
    Ok(plan)
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn lower_expand(
    src: VarId,
    edge: VarId,
    dst: VarId,
    rel_ty: Option<TypeId>,
    dir: Direction,
    min_hops: u16,
    max_hops: Option<u16>,
    input: LogicalPlan,
    var_map: &mut VarMap,
    catalog: Option<&GraphCatalog>,
    type_id_to_rel_name: &HashMap<u32, String>,
    inference_rules: &HashMap<u32, Vec<(String, String)>>,
    target: Option<(&Path, OntologyMode)>,
    relational_reference: bool,
) -> Result<LogicalPlan, LoweringError> {
    // Variable-length expand cannot be expressed in relational algebra; emit
    // the graphforge-plan Extension node whose physical execution (M13) performs an
    // iterative BFS over the edge table.
    if min_hops != 1 || max_hops != Some(1) {
        return lower_var_len_expand(
            src,
            edge,
            dst,
            rel_ty,
            dir,
            min_hops,
            max_hops,
            input,
            var_map,
            type_id_to_rel_name,
            inference_rules,
            target,
        );
    }

    // The source must be bound BEFORE either single-hop strategy: the
    // adjacency path would otherwise reach execution with no
    // `var_<src>.node_id` and silently seed from column 0.
    let src_alias = var_map
        .get(src)
        .ok_or(LoweringError::UnboundVar(src.0))?
        .to_owned();

    // A project-backed fixed hop always uses the provider-backed Extension
    // node (#1248). The provider owns hit/miss/building fallback, keeping the
    // physical shape stable so a terminal LIMIT can cancel traversal work.
    if !relational_reference
        && let Some(plan) = try_lower_provider_expand(
            src,
            edge,
            dst,
            rel_ty,
            dir,
            &input,
            var_map,
            type_id_to_rel_name,
            target,
        )?
    {
        return Ok(plan);
    }

    match dir {
        Direction::Out => expand_single_dir(
            src,
            edge,
            dst,
            rel_ty,
            &src_alias,
            true,
            input,
            var_map,
            catalog,
            type_id_to_rel_name,
            target,
        ),
        Direction::In => expand_single_dir(
            src,
            edge,
            dst,
            rel_ty,
            &src_alias,
            false,
            input,
            var_map,
            catalog,
            type_id_to_rel_name,
            target,
        ),
        Direction::Undirected => {
            use datafusion::common::Column;
            use datafusion::logical_expr::col;

            // Undirected = Out ∪ In. Build both legs with independent VarMaps
            // (both register the same `var_<n>` aliases), then copy the
            // edge/dst registrations back into the outer map.
            let mut vm_out = var_map.clone();
            let mut vm_in = var_map.clone();
            let out_plan = expand_single_dir(
                src,
                edge,
                dst,
                rel_ty,
                &src_alias,
                true,
                input.clone(),
                &mut vm_out,
                catalog,
                type_id_to_rel_name,
                target,
            )?;
            let in_plan = expand_single_dir(
                src,
                edge,
                dst,
                rel_ty,
                &src_alias,
                false,
                input,
                &mut vm_in,
                catalog,
                type_id_to_rel_name,
                target,
            )?;
            // Copy the newly registered edge/dst vars from the Out map.
            if let Some(edge_col) = vm_out.get(edge) {
                var_map.insert(edge, edge_col.to_owned());
            }
            if let Some(dst_col) = vm_out.get(dst) {
                var_map.insert(dst, dst_col.to_owned());
            }

            // The ONLY full-row duplicate between the two legs is a self-loop:
            // an edge with `src_id == dst_id` is matched by BOTH the Out join
            // (`src = edge.src_id`) and the In join (`src = edge.dst_id`) for the
            // same bound node, yielding an identical row; every other edge
            // matches exactly one leg per bound node. Drop self-loops from the
            // In leg so the union is duplicate-free WITHOUT a `Distinct` — a
            // wrapping `Distinct` over the merged multi-`var_<n>` schema trips
            // DataFusion's duplicate-field-name check at physical planning (the
            // #825 failure). This mirrors the adjacency path, which likewise
            // collapses only the self-loop's double entry.
            let edge_alias = var_alias(edge);
            let in_plan = LogicalPlanBuilder::from(in_plan)
                .filter(
                    col(format!("{edge_alias}.src_id")).not_eq(col(format!("{edge_alias}.dst_id"))),
                )
                .and_then(LogicalPlanBuilder::build)
                .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

            // DataFusion's `union` DROPS all relation qualifiers and, for
            // same-named columns (`var_0.node_id` and `var_2.node_id` both become
            // bare `node_id`), appends positional suffixes (`node_id_1`). Those
            // synthetic names are UNSTABLE under the `optimize_projections` pass
            // (pruning a collision shifts the suffix), so a projection that
            // references them post-union breaks the optimizer. Instead, rename
            // each leg's columns to collision-free positional names (`__u{i}`)
            // BEFORE the union — so the union output has no collisions to
            // disambiguate — then restore the original `var_<n>` qualifiers AFTER
            // it from the captured leg schema, referencing only the stable names
            // (mirroring `join_*_properties`' `alias_qualified` re-qualification).
            // Without this, downstream `b.<prop>` / `r.<prop>` refs and the
            // trailing `NodeScan{dst}` property join cannot resolve their columns.
            let leg_schema = out_plan.schema().clone();
            let stable = |plan: LogicalPlan| -> Result<LogicalPlan, LoweringError> {
                let proj: Vec<DfExpr> = plan
                    .schema()
                    .iter()
                    .enumerate()
                    .map(|(i, (q, f))| {
                        DfExpr::Column(Column::new(q.cloned(), f.name())).alias(format!("__u{i}"))
                    })
                    .collect();
                LogicalPlanBuilder::from(plan)
                    .project(proj)
                    .and_then(LogicalPlanBuilder::build)
                    .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
            };
            let out_plan = stable(out_plan)?;
            let in_plan = stable(in_plan)?;
            let unioned = LogicalPlanBuilder::from(out_plan)
                .union(in_plan)
                .and_then(LogicalPlanBuilder::build)
                .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
            let projections: Vec<DfExpr> = leg_schema
                .iter()
                .enumerate()
                .map(|(i, (q, f))| {
                    col(format!("__u{i}")).alias_qualified(q.cloned(), f.name().as_str())
                })
                .collect();
            LogicalPlanBuilder::from(unioned)
                .project(projections)
                .and_then(LogicalPlanBuilder::build)
                .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
        }
    }
}

/// Emit the provider-backed [`graphforge_plan::ExpandNode`] for a project-backed fixed
/// hop (#1248).
///
/// The provider, not lowering, owns hit/miss/building fallback. Schema-only
/// lowering and already-bound relationship variables keep the relational path;
/// the latter is already a cheap row-local constraint rather than a graph
/// expansion. A repeated destination is expanded under a private binding and
/// filtered back to the existing node, preserving cyclic-pattern semantics.
#[allow(clippy::too_many_arguments)]
#[allow(
    clippy::too_many_lines,
    reason = "schema discovery, repeated-destination correlation, and extension shaping are one lowering operation"
)]
fn try_lower_provider_expand(
    src: VarId,
    edge: VarId,
    dst: VarId,
    rel_ty: Option<TypeId>,
    dir: Direction,
    input: &LogicalPlan,
    var_map: &mut VarMap,
    type_id_to_rel_name: &HashMap<u32, String>,
    target: Option<(&Path, OntologyMode)>,
) -> Result<Option<LogicalPlan>, LoweringError> {
    let Some((dir_path, mode)) = target else {
        return Ok(None); // schema-only lowering has no execution provider
    };
    if var_map.get(edge).is_some() {
        return Ok(None); // already-bound relationship: row-local filter path
    }
    let rel_name = match rel_ty {
        Some(rt) => {
            let Some(name) = type_id_to_rel_name.get(&rt.0) else {
                return Ok(None); // relational path reports the unknown TypeId
            };
            name.clone()
        }
        None => "*".to_owned(),
    };

    let bound_dst = var_map.get(dst).map(str::to_owned);
    let traversal_dst = bound_dst
        .as_ref()
        .map_or(dst, |_| VarId(u32::MAX.saturating_sub(dst.0)));

    // Edge property fields, discovered exactly like `join_edge_properties`:
    // wildcard traversal unions every relation's fields, first occurrence wins.
    let edge_schema = if rel_ty.is_none() || matches!(mode, OntologyMode::Exploratory) {
        &*EXPLORATORY_EDGE_SCHEMA
    } else {
        &*TYPED_EDGE_SCHEMA
    };
    let edge_fields: Vec<Arc<datafusion::arrow::datatypes::Field>> =
        edge_schema.fields().iter().cloned().collect();
    let base_names: HashSet<&str> = edge_fields.iter().map(|f| f.name().as_str()).collect();
    let mut stems = if rel_name == "*" {
        graphforge_storage::list_edge_property_stems(dir_path)
    } else {
        vec![rel_name.clone()]
    };
    stems.sort();
    let mut seen = HashSet::new();
    let mut edge_prop_fields = Vec::new();
    for stem in stems {
        let prop_table = graphforge_storage::EdgePropertyTable::open_discovered(dir_path, &stem);
        for field in prop_table.schema_ref().fields() {
            if field.name() != "edge_uuid"
                && !base_names.contains(field.name().as_str())
                && seen.insert(field.name().clone())
            {
                edge_prop_fields.push(Arc::clone(field));
            }
        }
    }
    let dst_fields: Vec<Arc<datafusion::arrow::datatypes::Field>> =
        TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect();

    let node = graphforge_plan::ExpandNode::new(
        Arc::new(input.clone()),
        rel_name,
        src.0,
        traversal_dst.0,
        edge.0,
        dir,
        rel_ty.map(|rt| rt.0),
        dir_path.to_path_buf(),
        mode,
        edge_fields,
        edge_prop_fields,
        dst_fields,
    );
    // Register new bindings exactly like the join path. A repeated destination
    // keeps its existing registration and is correlated below.
    var_map.insert(edge, var_alias(edge));
    if bound_dst.is_none() {
        var_map.insert(dst, var_alias(dst));
    }

    let mut base = LogicalPlan::Extension(datafusion::logical_expr::Extension {
        node: Arc::new(node),
    });
    if let Some(bound_dst) = bound_dst {
        use datafusion::logical_expr::col;

        let traversal_alias = var_alias(traversal_dst);
        base = LogicalPlanBuilder::from(base)
            .filter(
                col(format!("{traversal_alias}.node_uuid"))
                    .eq(col(format!("{bound_dst}.node_uuid"))),
            )
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
        let traversal_ref = datafusion::common::TableReference::bare(traversal_alias);
        let projection = base
            .schema()
            .iter()
            .filter(|(qualifier, _)| {
                qualifier
                    .as_ref()
                    .is_none_or(|qualifier| **qualifier != traversal_ref)
            })
            .map(|(qualifier, field)| {
                DfExpr::Column(datafusion::common::Column::new(
                    qualifier.cloned(),
                    field.name(),
                ))
            })
            .collect::<Vec<_>>();
        base = LogicalPlanBuilder::from(base)
            .project(projection)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
    }
    Ok(Some(base))
}

/// Lower a single-direction (Out or In) fixed-hop expand.
///
/// `out_direction = true`  → join on `src.node_id = edge.src_id` (Out)
/// `out_direction = false` → join on `src.node_id = edge.dst_id` (In)
#[allow(clippy::too_many_arguments)]
fn expand_single_dir(
    _src: VarId,
    edge: VarId,
    dst: VarId,
    rel_ty: Option<TypeId>,
    src_alias: &str,
    out_direction: bool,
    input: LogicalPlan,
    var_map: &mut VarMap,
    catalog: Option<&GraphCatalog>,
    type_id_to_rel_name: &HashMap<u32, String>,
    target: Option<(&Path, OntologyMode)>,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::logical_expr::col;

    let bound_dst = var_map.get(dst).and_then(|alias| {
        let qualifier = datafusion::common::TableReference::bare(alias);
        if input
            .schema()
            .index_of_column_by_name(Some(&qualifier), "node_id")
            .is_some()
        {
            Some((alias.to_owned(), "node_id", false))
        } else {
            input
                .schema()
                .index_of_column_by_name(Some(&qualifier), "node_uuid")
                .is_some()
                .then(|| (alias.to_owned(), "node_uuid", true))
        }
    });
    let dir = target.map(|(d, _)| d);
    let mode = target.map_or(OntologyMode::Exploratory, |(_, m)| m);
    if let Some(edge_alias) = var_map.get(edge).map(str::to_owned) {
        return expand_bound_edge_single_dir(
            dst,
            rel_ty,
            src_alias,
            &edge_alias,
            out_direction,
            input,
            var_map,
            type_id_to_rel_name,
            target,
        );
    }

    // Produce the edge scan (registers edge var in var_map).
    let edge_plan = match rel_ty {
        Some(rt) => {
            lower_typed_edge_scan(edge, rt, var_map, catalog, type_id_to_rel_name, dir, mode)?
        }
        None => lower_edge_scan(edge, None, var_map, type_id_to_rel_name, dir, mode)?,
    };
    let edge_alias = var_map
        .get(edge)
        .ok_or(LoweringError::UnboundVar(edge.0))?
        .to_owned();

    // Enrich the edge scan with its persisted properties (#784) so a downstream
    // `RETURN r.<prop>` resolves to a real column. A wildcard edge scan
    // (`rel_ty == None`) has no single relation file, so no property join.
    let edge_plan = join_edge_properties(&edge_alias, rel_ty, type_id_to_rel_name, dir, edge_plan)?;

    // Join input (has src) with edge scan.
    let src_col = col(format!("{src_alias}.node_id"));
    let edge_src_col = col(if out_direction {
        format!("{edge_alias}.src_id")
    } else {
        format!("{edge_alias}.dst_id")
    });

    // Join input (has src) with edge scan using ON expression.
    let join_pred = src_col.eq(edge_src_col);
    let joined = LogicalPlanBuilder::from(input)
        .join_on(edge_plan, JoinType::Inner, vec![join_pred])
        .and_then(LogicalPlanBuilder::build)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

    let edge_dst_col = col(
        match (out_direction, bound_dst.as_ref().map(|(_, _, uuid)| *uuid)) {
            (true, Some(true)) => format!("{edge_alias}.dst_uuid"),
            (false, Some(true)) => format!("{edge_alias}.src_uuid"),
            (true, _) => format!("{edge_alias}.dst_id"),
            (false, _) => format!("{edge_alias}.src_id"),
        },
    );
    // Reusing a node variable constrains this hop to the node already present
    // in the row; it must not append a second scan with the same qualifier.
    if let Some((dst_alias, dst_field, _)) = bound_dst {
        return LogicalPlanBuilder::from(joined)
            .filter(edge_dst_col.eq(col(format!("{dst_alias}.{dst_field}"))))
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()));
    }

    // Produce the dst node scan.
    let dst_plan = lower_node_scan(dst, None, var_map, dir, None)?;
    let dst_alias = var_map
        .get(dst)
        .ok_or(LoweringError::UnboundVar(dst.0))?
        .to_owned();

    let dst_col = col(format!("{dst_alias}.node_id"));

    let join_pred2 = edge_dst_col.eq(dst_col);
    LogicalPlanBuilder::from(joined)
        .join_on(dst_plan, JoinType::Inner, vec![join_pred2])
        .and_then(LogicalPlanBuilder::build)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
}

#[allow(clippy::too_many_arguments)]
fn expand_bound_edge_single_dir(
    dst: VarId,
    rel_ty: Option<TypeId>,
    src_alias: &str,
    edge_alias: &str,
    out_direction: bool,
    input: LogicalPlan,
    var_map: &mut VarMap,
    type_id_to_rel_name: &HashMap<u32, String>,
    target: Option<(&Path, OntologyMode)>,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::common::TableReference;
    use datafusion::logical_expr::{col, lit};

    let edge_src_field = if out_direction { "src_id" } else { "dst_id" };
    let edge_dst_field = if out_direction { "dst_id" } else { "src_id" };
    let mut predicate =
        col(format!("{src_alias}.node_id")).eq(col(format!("{edge_alias}.{edge_src_field}")));

    if let Some(rt) = rel_ty {
        let rel_name = type_id_to_rel_name.get(&rt.0).ok_or_else(|| {
            LoweringError::UnsupportedExpr(format!(
                "bound edge TypeId({}) has no known relation name; ontology may be incomplete or stale",
                rt.0
            ))
        })?;
        let qual = TableReference::bare(edge_alias);
        if input
            .schema()
            .index_of_column_by_name(Some(&qual), "rel_type_name")
            .is_some()
        {
            predicate = predicate
                .and(col(format!("{edge_alias}.rel_type_name")).eq(lit(rel_name.as_str())));
        }
    }

    let filtered = LogicalPlanBuilder::from(input)
        .filter(predicate)
        .and_then(LogicalPlanBuilder::build)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

    let edge_dst = col(format!("{edge_alias}.{edge_dst_field}"));
    if let Some(dst_alias) = var_map.get(dst).map(str::to_owned) {
        return LogicalPlanBuilder::from(filtered)
            .filter(edge_dst.eq(col(format!("{dst_alias}.node_id"))))
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()));
    }

    let dir = target.map(|(d, _)| d);
    let dst_plan = lower_node_scan(dst, None, var_map, dir, None)?;
    let dst_alias = var_map
        .get(dst)
        .ok_or(LoweringError::UnboundVar(dst.0))?
        .to_owned();
    LogicalPlanBuilder::from(filtered)
        .join_on(
            dst_plan,
            JoinType::Inner,
            vec![edge_dst.eq(col(format!("{dst_alias}.node_id")))],
        )
        .and_then(LogicalPlanBuilder::build)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
}

/// LEFT-join an edge scan with its persisted properties (#784), the edge
/// analogue of [`GraphPlanLowerer::join_node_properties`].
///
/// Returns `scan` unchanged when:
/// - there is no read directory (schema-only lowering),
/// - `rel_ty` is `None` (a wildcard edge scan has no single relation file), or
/// - the edge-property table has no columns beyond the `edge_uuid` join key.
///
/// Otherwise it opens `edge_properties/<rel>.parquet`, LEFT-joins on `edge_uuid`
/// (preserving edges with no property row yet), and re-qualifies each property
/// column under the edge var alias so `var_<edge>.<prop>` resolves. The base
/// topology columns are read from the edge `scan`'s own schema (typed and
/// exploratory edge files differ in width), keeping the projection in lock-step
/// with whichever edge file the scan reads.
fn join_edge_properties(
    edge_alias: &str,
    rel_ty: Option<TypeId>,
    type_id_to_rel_name: &HashMap<u32, String>,
    dir: Option<&Path>,
    scan: LogicalPlan,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::logical_expr::{col, lit};
    use std::collections::{HashMap as StdHashMap, HashSet};

    let Some(dir) = dir else {
        return Ok(scan); // schema-only lowering: no real provider to join
    };

    // Base topology columns come from the edge scan's own schema (typed: 9 cols;
    // exploratory: + `rel_type_name`), so the projection matches the live file.
    let base_cols: Vec<String> = scan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let mut prop_sources = Vec::new();
    let mut prop_order = Vec::new();
    let mut seen_props = HashSet::new();
    let mut push_source = |stem: String| {
        let table = graphforge_storage::EdgePropertyTable::open_discovered(dir, &stem);
        let prop_cols: Vec<String> = table
            .schema_ref()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| !base_cols.contains(n))
            .collect();
        if prop_cols.is_empty() {
            return;
        }
        for name in &prop_cols {
            if seen_props.insert(name.clone()) {
                prop_order.push(name.clone());
            }
        }
        prop_sources.push((stem, table, prop_cols));
    };
    if let Some(rel_ty) = rel_ty {
        let Some(rel_name) = type_id_to_rel_name.get(&rel_ty.0) else {
            return Ok(scan); // unknown relation name: nothing to resolve
        };
        push_source(rel_name.clone());
    } else {
        for stem in graphforge_storage::list_edge_property_stems(dir) {
            push_source(stem);
        }
    }
    if prop_sources.is_empty() {
        return Ok(scan);
    }

    let wildcard = rel_ty.is_none();
    let mut joined = scan;
    let mut prop_refs: StdHashMap<String, Vec<DfExpr>> = StdHashMap::new();
    for (idx, (stem, prop_table, prop_cols)) in prop_sources.into_iter().enumerate() {
        let prop_alias = format!("{edge_alias}__eprops_{idx}");
        let prop_src = datafusion::datasource::provider_as_source(Arc::new(prop_table));
        let prop_scan = LogicalPlanBuilder::scan(prop_alias.clone(), prop_src, None)
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;

        // LEFT join: edge ⟕ props ON edge.edge_uuid = props.edge_uuid. For a
        // wildcard edge scan, constrain each property table to its relation so a
        // relation-specific property file never contributes to another relation.
        let mut join_pred =
            col(format!("{edge_alias}.edge_uuid")).eq(col(format!("{prop_alias}.edge_uuid")));
        if wildcard {
            join_pred = join_pred.and(col(format!("{edge_alias}.rel_type_name")).eq(lit(stem)));
        }
        joined = LogicalPlanBuilder::from(joined)
            .join_on(prop_scan, JoinType::Left, vec![join_pred])
            .and_then(LogicalPlanBuilder::build)
            .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))?;
        for name in prop_cols {
            prop_refs
                .entry(name.clone())
                .or_default()
                .push(crate::expr::qualified_col(&prop_alias, &name));
        }
    }

    // Project: all edge topology columns (qualified var_N) unchanged, then each
    // property column re-qualified under var_N so `var_N.<prop>` resolves.
    let mut projections: Vec<DfExpr> = base_cols
        .iter()
        .map(|name| col(format!("{edge_alias}.{name}")))
        .collect();
    for name in prop_order {
        let mut refs = prop_refs.remove(&name).unwrap_or_default();
        let value = if refs.len() == 1 {
            refs.remove(0)
        } else {
            datafusion::functions::core::expr_fn::coalesce(refs)
        };
        projections.push(value.alias_qualified(Some(edge_alias), name.as_str()));
    }

    LogicalPlanBuilder::from(joined)
        .project(projections)
        .and_then(LogicalPlanBuilder::build)
        .map_err(|e| LoweringError::UnsupportedExpr(e.to_string()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
    use graphforge_core::TypeId;
    use graphforge_ir::expr::{IrExpr, IrLiteral};
    use graphforge_ir::{Direction, ExprArena, GraphPlan, VarId};

    fn empty_base() -> LogicalPlan {
        LogicalPlanBuilder::empty(false).build().unwrap()
    }

    fn make_catalog_and_lowerer() -> (
        tempfile::TempDir,
        graphforge_storage::GraphCatalog,
        graphforge_ir::RuntimeCatalog,
    ) {
        let dir = tempfile::TempDir::new().unwrap();
        let rc = graphforge_ir::RuntimeCatalog::new();
        let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
        (dir, catalog, rc)
    }

    fn node_scan_with_alias(alias: &str) -> LogicalPlan {
        let schema = std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "node_id",
                datafusion::arrow::datatypes::DataType::UInt64,
                false,
            ),
        ]));
        LogicalPlanBuilder::scan(alias, table_source(schema), None)
            .and_then(LogicalPlanBuilder::build)
            .unwrap()
    }

    #[test]
    fn optional_join_keys_use_var_map_qualifiers() {
        let outer = node_scan_with_alias("projected_node");
        let inner = node_scan_with_alias("pattern_node");
        let mut outer_vm = VarMap::new();
        outer_vm.insert(VarId(7), "projected_node");
        let mut inner_vm = VarMap::new();
        inner_vm.insert(VarId(7), "pattern_node");

        let (join_keys, inner_keep_idx) = optional_join_keys(&outer, &inner, &outer_vm, &inner_vm);

        assert_eq!(join_keys, vec![(0, 0)]);
        assert_eq!(inner_keep_idx, Vec::<usize>::new());
    }

    #[test]
    fn filter_lowers_predicate() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let mut arena = ExprArena::new();
        let lit = arena.push(IrExpr::Literal(IrLiteral::Bool(true)));
        let var_map = VarMap::new();
        let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

        let result = lowerer
            .lower_op(
                &GraphOp::Filter { predicate: lit },
                empty_base(),
                &arena,
                &var_map,
                &expr_lowerer,
            )
            .unwrap();

        assert!(
            matches!(result, DfLogicalPlan::Filter(_)),
            "expected Filter, got {result:?}"
        );
    }

    #[test]
    fn project_lowers_columns() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let mut arena = ExprArena::new();
        let lit = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let item = ProjectItem {
            expr: lit,
            alias: Some("x".into()),
            out_var: None,
        };
        let var_map = VarMap::new();
        let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

        let result = lowerer
            .lower_op(
                &GraphOp::Project {
                    items: vec![item],
                    distinct: false,
                },
                empty_base(),
                &arena,
                &var_map,
                &expr_lowerer,
            )
            .unwrap();

        assert!(
            matches!(result, DfLogicalPlan::Projection(_)),
            "expected Projection, got {result:?}"
        );
    }

    #[test]
    fn project_with_distinct_wraps_in_distinct() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let mut arena = ExprArena::new();
        let lit = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let item = ProjectItem {
            expr: lit,
            alias: None,
            out_var: None,
        };
        let var_map = VarMap::new();
        let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

        let result = lowerer
            .lower_op(
                &GraphOp::Project {
                    items: vec![item],
                    distinct: true,
                },
                empty_base(),
                &arena,
                &var_map,
                &expr_lowerer,
            )
            .unwrap();

        assert!(
            matches!(result, DfLogicalPlan::Distinct(_)),
            "expected Distinct, got {result:?}"
        );
    }

    #[test]
    fn aggregate_count_star() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let arena = ExprArena::new();
        let agg = AggExpr {
            func: AggFunc::Count,
            arg: None,
            percentile: None,
            alias: "total".into(),
            out_var: None,
        };
        let var_map = VarMap::new();
        let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

        let result = lowerer
            .lower_op(
                &GraphOp::Aggregate {
                    group_by: vec![],
                    group_aliases: vec![],
                    group_vars: vec![],
                    aggs: vec![agg],
                },
                empty_base(),
                &arena,
                &var_map,
                &expr_lowerer,
            )
            .unwrap();

        assert!(
            matches!(result, DfLogicalPlan::Aggregate(_)),
            "expected Aggregate, got {result:?}"
        );
    }

    #[test]
    fn aggregate_function_argument_contract_matrix_and_specialized_paths() {
        use datafusion::arrow::datatypes::{DataType, Field, Fields};
        use datafusion::logical_expr::lit;

        for (function, expected) in [
            (
                AggFunc::CountDistinct,
                "COUNT DISTINCT requires an argument",
            ),
            (AggFunc::Sum, "SUM requires an argument"),
            (AggFunc::SumDistinct, "SUM DISTINCT requires an argument"),
            (AggFunc::Avg, "AVG requires an argument"),
            (AggFunc::AvgDistinct, "AVG DISTINCT requires an argument"),
            (AggFunc::Min, "MIN requires an argument"),
            (AggFunc::Max, "MAX requires an argument"),
            (AggFunc::Collect, "COLLECT requires an argument"),
            (AggFunc::CollectDistinct, "COLLECT requires an argument"),
            (
                AggFunc::PercentileDisc,
                "percentileDisc requires a value argument",
            ),
            (
                AggFunc::PercentileCont,
                "percentileCont requires a value argument",
            ),
        ] {
            assert_eq!(
                lower_agg_func(function, None, None, None)
                    .unwrap_err()
                    .to_string(),
                format!("unsupported expression: {expected}")
            );
        }

        for function in [AggFunc::PercentileDisc, AggFunc::PercentileCont] {
            assert!(
                lower_agg_func(function, Some(lit(1_i64)), None, Some(&DataType::Int64))
                    .unwrap_err()
                    .to_string()
                    .contains("percentile argument")
            );
        }

        for function in [
            AggFunc::Count,
            AggFunc::CountDistinct,
            AggFunc::Sum,
            AggFunc::SumDistinct,
            AggFunc::Avg,
            AggFunc::AvgDistinct,
            AggFunc::Min,
            AggFunc::Max,
            AggFunc::Collect,
            AggFunc::CollectDistinct,
        ] {
            let arg = (function != AggFunc::Count).then(|| lit(1_i64));
            assert!(lower_agg_func(function, arg, None, Some(&DataType::Int64)).is_ok());
        }
        assert!(
            lower_agg_func(
                AggFunc::Avg,
                Some(lit(datafusion::scalar::ScalarValue::Null)),
                None,
                Some(&DataType::Null),
            )
            .is_ok()
        );
        assert!(
            lower_agg_func(
                AggFunc::AvgDistinct,
                Some(lit(datafusion::scalar::ScalarValue::Null)),
                None,
                Some(&DataType::Null),
            )
            .is_ok()
        );

        let heterogeneous = DataType::Struct(Fields::from(vec![
            Field::new("__het_tag", DataType::Int8, false),
            Field::new("__het_value_0", DataType::Int64, true),
        ]));
        for function in [AggFunc::Min, AggFunc::Max] {
            let expression = lower_agg_func(
                function,
                Some(lit(datafusion::scalar::ScalarValue::Null)),
                None,
                Some(&heterogeneous),
            )
            .unwrap();
            assert!(format!("{expression}").contains("cypher_"));
        }
    }

    #[test]
    fn sort_lowers_keys() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        // Use a literal rather than a VarRef to avoid schema validation on
        // the empty base relation (DataFusion rejects unknown column names).
        let mut arena = ExprArena::new();
        let lit = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let var_map = VarMap::new();
        let key = graphforge_ir::SortKey {
            expr: lit,
            order: SortOrder::Desc,
            nulls_first: false,
        };
        let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

        let result = lowerer
            .lower_op(
                &GraphOp::Sort { keys: vec![key] },
                empty_base(),
                &arena,
                &var_map,
                &expr_lowerer,
            )
            .unwrap();

        assert!(
            matches!(result, DfLogicalPlan::Sort(_)),
            "expected Sort, got {result:?}"
        );
    }

    #[test]
    fn limit_lowers_correctly() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let arena = ExprArena::new();
        let var_map = VarMap::new();
        let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

        let result = lowerer
            .lower_op(
                &GraphOp::Limit { count: 10 },
                empty_base(),
                &arena,
                &var_map,
                &expr_lowerer,
            )
            .unwrap();

        assert!(
            matches!(result, DfLogicalPlan::Limit(_)),
            "expected Limit, got {result:?}"
        );
    }

    #[test]
    fn skip_lowers_correctly() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let arena = ExprArena::new();
        let var_map = VarMap::new();
        let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

        let result = lowerer
            .lower_op(
                &GraphOp::Skip { count: 5 },
                empty_base(),
                &arena,
                &var_map,
                &expr_lowerer,
            )
            .unwrap();

        // Skip is implemented as Limit with a non-zero skip offset
        assert!(
            matches!(result, DfLogicalPlan::Limit(_)),
            "expected Limit (skip), got {result:?}"
        );
    }

    #[test]
    fn unsupported_op_returns_error() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let arena = ExprArena::new();
        let var_map = VarMap::new();
        let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

        let result = lowerer.lower_op(
            &GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            },
            empty_base(),
            &arena,
            &var_map,
            &expr_lowerer,
        );
        assert!(
            matches!(result, Err(LoweringError::UnsupportedExpr(_))),
            "expected UnsupportedExpr for NodeScan"
        );
    }

    #[test]
    fn statement_driver_only_write_forms_are_rejected_by_relational_lowering() {
        use graphforge_ir::{LabelItem, RemovePropItem, SetMapItem};

        let dir = tempfile::tempdir().unwrap();
        let lowerer = GraphPlanLowerer::new_for_writes(
            None,
            None,
            dir.path(),
            graphforge_core::OntologyMode::Exploratory,
        );

        let mut set_builder = GraphPlan::builder("openCypher");
        let map = set_builder.push_expr(IrExpr::MapLiteral(vec![]));
        let set_map = set_builder
            .push_op(GraphOp::Set {
                items: vec![],
                map_items: vec![SetMapItem {
                    target: VarId(1),
                    map,
                    replace: false,
                }],
                label_items: vec![],
            })
            .build();
        assert!(
            lowerer
                .lower_plan(&set_map)
                .unwrap_err()
                .to_string()
                .contains("statement driver")
        );

        let set_labels = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Set {
                items: vec![],
                map_items: vec![],
                label_items: vec![LabelItem {
                    target: VarId(1),
                    labels: vec![TypeId(7)],
                }],
            })
            .build();
        assert!(
            lowerer
                .lower_plan(&set_labels)
                .unwrap_err()
                .to_string()
                .contains("statement driver")
        );

        let remove_labels = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Remove {
                items: Vec::<RemovePropItem>::new(),
                label_items: vec![LabelItem {
                    target: VarId(1),
                    labels: vec![TypeId(7)],
                }],
            })
            .build();
        assert!(
            lowerer
                .lower_plan(&remove_labels)
                .unwrap_err()
                .to_string()
                .contains("statement driver")
        );
    }

    #[test]
    fn correlated_subquery_shapes_fail_with_precise_contract_errors() {
        let lowerer = GraphPlanLowerer::new(None, None);

        let no_alternatives = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Exists {
                child: Box::new(
                    GraphPlan::builder("openCypher")
                        .push_op(GraphOp::Union {
                            all: true,
                            inputs: vec![],
                        })
                        .build(),
                ),
                negated: false,
            })
            .build();
        assert!(
            lowerer
                .lower_plan(&no_alternatives)
                .unwrap_err()
                .to_string()
                .contains("no alternatives")
        );

        let uncorrelated = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Exists {
                child: Box::new(GraphPlan::builder("openCypher").build()),
                negated: true,
            })
            .build();
        assert!(
            lowerer
                .lower_plan(&uncorrelated)
                .unwrap_err()
                .to_string()
                .contains("share at least one bound variable")
        );

        let empty_comprehension = GraphPlan::builder("openCypher")
            .push_op(GraphOp::PatternComprehension {
                child: Box::new(GraphPlan::builder("openCypher").build()),
                output: VarId(10),
            })
            .build();
        assert!(
            lowerer
                .lower_plan(&empty_comprehension)
                .unwrap_err()
                .to_string()
                .contains("child is empty")
        );

        let wrong_terminal = GraphPlan::builder("openCypher")
            .push_op(GraphOp::PatternComprehension {
                child: Box::new(
                    GraphPlan::builder("openCypher")
                        .push_op(GraphOp::Limit { count: 1 })
                        .build(),
                ),
                output: VarId(10),
            })
            .build();
        assert!(
            lowerer
                .lower_plan(&wrong_terminal)
                .unwrap_err()
                .to_string()
                .contains("must end in a value projection")
        );
    }

    #[test]
    fn pattern_comprehension_projection_contract_is_strict() {
        let lowerer = GraphPlanLowerer::new(None, None);
        let cases = [
            (
                true,
                PATTERN_COMPREHENSION_VALUE_ALIAS,
                "exactly one non-distinct",
            ),
            (false, "wrong_alias", "invalid value projection"),
        ];
        for (distinct, alias, expected) in cases {
            let mut child = GraphPlan::builder("openCypher");
            let value = child.push_expr(IrExpr::Literal(IrLiteral::Int(1)));
            child.push_op_mut(GraphOp::Project {
                items: vec![ProjectItem {
                    expr: value,
                    alias: Some(alias.into()),
                    out_var: None,
                }],
                distinct,
            });
            let plan = GraphPlan::builder("openCypher")
                .push_op(GraphOp::PatternComprehension {
                    child: Box::new(child.build()),
                    output: VarId(11),
                })
                .build();
            assert!(
                lowerer
                    .lower_plan(&plan)
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
    }

    #[test]
    fn terminal_suffix_uses_supplied_schema_and_preserves_scope() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::DFSchema;

        let lowerer = GraphPlanLowerer::new(None, None);
        let schema = Arc::new(
            DFSchema::try_from(Schema::new(vec![Field::new(
                "seed",
                DataType::Int64,
                false,
            )]))
            .unwrap(),
        );
        let mut arena = ExprArena::new();
        let literal = arena.push(IrExpr::Literal(IrLiteral::Int(9)));
        let mut vars = VarMap::new();
        vars.insert(VarId(4), "seed");
        let suffix = lowerer
            .lower_terminal_suffix(
                &[GraphOp::Project {
                    items: vec![ProjectItem {
                        expr: literal,
                        alias: Some("answer".into()),
                        out_var: Some(VarId(5)),
                    }],
                    distinct: false,
                }],
                &arena,
                &mut vars,
                schema,
            )
            .unwrap();
        assert!(matches!(suffix, DfLogicalPlan::Projection(_)));
        assert_eq!(vars.get(VarId(5)), Some("answer"));
    }

    #[test]
    fn lower_plan_empty_ops_succeeds() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let plan = GraphPlan::builder("openCypher").build();
        let result = lowerer.lower_plan(&plan);
        assert!(result.is_ok(), "empty op pipeline should succeed");
    }

    #[test]
    fn union_requires_two_branch_plans() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Union {
                all: true,
                inputs: vec![GraphPlan::builder("openCypher").build()],
            })
            .build();

        let error = lowerer
            .lower_plan(&plan)
            .expect_err("one branch is invalid");
        assert!(error.to_string().contains("at least two branch plans"));
    }

    #[test]
    fn integration_filter_project_limit_pipeline() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        // Build the plan using GraphPlanBuilder so expressions live in plan.exprs.
        let mut builder = GraphPlan::builder("openCypher");
        let pred = builder.push_expr(IrExpr::Literal(IrLiteral::Bool(true)));
        let col_expr = builder.push_expr(IrExpr::Literal(IrLiteral::Str("n".into())));

        let plan = builder
            .push_op(GraphOp::Filter { predicate: pred })
            .push_op(GraphOp::Project {
                items: vec![ProjectItem {
                    expr: col_expr,
                    alias: Some("name".into()),
                    out_var: None,
                }],
                distinct: false,
            })
            .push_op(GraphOp::Limit { count: 10 })
            .build();

        let lp = lowerer.lower_plan(&plan).unwrap();
        // Outermost plan node should be Limit
        assert!(
            matches!(lp, DfLogicalPlan::Limit(_)),
            "expected Limit at top, got {lp:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Scan operator tests (#576)
    // -----------------------------------------------------------------------

    #[test]
    fn node_scan_no_type_produces_table_scan() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        assert!(
            matches!(lp, DfLogicalPlan::TableScan(_)),
            "expected TableScan, got {lp:?}"
        );
    }

    #[test]
    fn node_scan_with_type_produces_filter_over_scan() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: Some(TypeId(1)),
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        assert!(
            matches!(lp, DfLogicalPlan::Filter(_)),
            "expected Filter over TableScan, got {lp:?}"
        );
    }

    #[test]
    fn typed_edge_scan_unknown_type_id_returns_error() {
        // TypeId(42) is not in the type_id_to_rel_name map (no ontology),
        // so lower_plan must return an error rather than silently falling back.
        let lowerer = GraphPlanLowerer::new(None, None);

        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::TypedEdgeScan {
                var: VarId(0),
                rel_ty: TypeId(42),
            })
            .build();
        let result = lowerer.lower_plan(&plan);
        assert!(
            result.is_err(),
            "unknown TypeId should return an error, not silently fall back"
        );
    }

    #[test]
    fn edge_scan_wildcard_produces_table_scan() {
        let lowerer = GraphPlanLowerer::new(None, None);

        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::EdgeScan {
                var: VarId(0),
                ty: None,
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        assert!(
            matches!(lp, DfLogicalPlan::TableScan(_)),
            "expected TableScan, got {lp:?}"
        );
    }

    fn var_len_plan() -> GraphPlan {
        GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: Direction::Out,
                min_hops: 1,
                max_hops: Some(3), // variable-length — emits VarLenExpandNode
            })
            .build()
    }

    #[test]
    fn expand_var_len_produces_extension_node() {
        use datafusion::logical_expr::UserDefinedLogicalNodeCore;
        use graphforge_plan::VarLenExpandNode;

        let (dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer =
            GraphPlanLowerer::new_with_dir(Some(&catalog), None, dir.path(), OntologyMode::Strict);

        let lp = lowerer.lower_plan(&var_len_plan()).unwrap();
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected Extension (VarLenExpandNode), got {lp:?}");
        };
        let node = ext
            .node
            .as_any()
            .downcast_ref::<VarLenExpandNode>()
            .expect("VarLenExpandNode");

        // Baked execution context + pattern fields.
        assert_eq!(node.src_var, 0);
        assert_eq!(node.dst_var, 2);
        assert_eq!(node.direction, Direction::Out);
        assert_eq!(node.min_hops, 1);
        assert_eq!(node.max_hops, Some(3));
        assert_eq!(node.dir, dir.path());
        assert_eq!(node.mode, OntologyMode::Strict);

        // Output schema carries the destination node's columns, qualified
        // `var_2`, so a downstream `RETURN b.node_id` can resolve them.
        let schema = UserDefinedLogicalNodeCore::schema(node);
        let dst = datafusion::common::TableReference::bare("var_2");
        assert!(schema.field_with_qualified_name(&dst, "node_id").is_ok());
    }

    #[test]
    fn expand_var_len_without_dir_errors() {
        // The read-only constructor has no project directory, so a
        // variable-length expand cannot bake its edge-read path.
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);
        let err = lowerer.lower_plan(&var_len_plan()).unwrap_err();
        assert!(
            err.to_string().contains("project directory"),
            "expected a project-directory error, got: {err}"
        );
    }

    #[test]
    fn unwind_produces_extension_node() {
        let lowerer = GraphPlanLowerer::new(None, None);

        let mut builder = GraphPlan::builder("openCypher");
        let list_expr = builder.push_expr(IrExpr::Literal(IrLiteral::Int(1)));
        let plan = builder
            .push_op(GraphOp::Unwind {
                list_expr,
                alias: VarId(0),
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        assert!(
            matches!(lp, DfLogicalPlan::Extension(_)),
            "expected Extension (UnwindNode), got {lp:?}"
        );
    }

    #[test]
    fn optional_produces_extension_node() {
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        let child = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(1),
                ty: None,
            })
            .build();
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Optional {
                child: Box::new(child),
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        assert!(
            matches!(lp, DfLogicalPlan::Extension(_)),
            "expected Extension (OptionalMatchNode), got {lp:?}"
        );
    }

    #[test]
    fn optional_node_output_schema_appends_nullable_inner() {
        use datafusion::logical_expr::UserDefinedLogicalNodeCore;
        use graphforge_plan::OptionalMatchNode;

        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);

        // Outer binds var_0; the optional child binds a fresh, unshared var_1,
        // so `join_keys` is empty and all 5 inner columns are kept. This test
        // pins the *schema* contract (outer ++ nullable inner); the shared-var
        // exclusion (non-empty join keys) is covered by
        // `optional_child_with_shared_var_excludes_outer_columns` below.
        let child = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(1),
                ty: None,
            })
            .build();
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Optional {
                child: Box::new(child),
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected Extension (OptionalMatchNode), got {lp:?}");
        };
        let node = ext
            .node
            .as_any()
            .downcast_ref::<OptionalMatchNode>()
            .expect("OptionalMatchNode");

        // Output = outer (6 topology cols) ++ inner (6 cols, all nullable).
        let schema = UserDefinedLogicalNodeCore::schema(node);
        assert_eq!(schema.fields().len(), 12, "outer(6) + inner(6)");
        for i in 0..6 {
            assert!(
                !schema.field(i).is_nullable(),
                "outer col {i} stays non-null"
            );
        }
        for i in 6..12 {
            assert!(
                schema.field(i).is_nullable(),
                "inner col {i} must be nullable for null-shaping"
            );
        }
    }

    #[test]
    fn expand_single_hop_out_produces_join() {
        let lowerer = GraphPlanLowerer::new(None, None);

        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: Direction::Out,
                min_hops: 1,
                max_hops: Some(1),
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        // Top-level should be an inner join (dst scan joined onto edge scan)
        assert!(
            matches!(lp, DfLogicalPlan::Join(_)),
            "expected Join, got {lp:?}"
        );
    }

    #[test]
    fn fixed_hop_dst_label_is_preserved_as_filter() {
        // A binder emits `NodeScan{a} → Expand → NodeScan{b, ty}` for
        // `(a)-[:R]->(b:Label)`. The trailing `NodeScan{b}` is a no-op (b is
        // already bound by Expand), but b's label must still filter the result
        // rather than being dropped (#718). The optimizer leaves a `Filter` on
        // `var_2.type_id` somewhere in the tree.
        let lowerer = GraphPlanLowerer::new(None, None);
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: Direction::Out,
                min_hops: 1,
                max_hops: Some(1),
            })
            .push_op(GraphOp::NodeScan {
                var: VarId(2),
                ty: Some(TypeId(7)),
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        let rendered = lp.display_indent_schema().to_string();
        assert!(
            rendered.contains("array_has(var_2.type_ids, UInt32(7))"),
            "destination label filter must be applied, got:\n{rendered}"
        );
    }

    #[test]
    fn optional_child_with_shared_var_excludes_outer_columns() {
        use datafusion::logical_expr::UserDefinedLogicalNodeCore;
        use graphforge_plan::OptionalMatchNode;

        // `MATCH (a) OPTIONAL MATCH (a)-[:R]->(b)`: the optional child now lowers
        // to a real join that re-binds the shared `a` (var_0). Its columns must
        // NOT be appended again (they live on the outer side) — otherwise the
        // node's schema would carry duplicate `var_0` fields (#718).
        let (_dir, catalog, _rc) = make_catalog_and_lowerer();
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);
        let child = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: Direction::Out,
                min_hops: 1,
                max_hops: Some(1),
            })
            .build();
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Optional {
                child: Box::new(child),
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected Extension (OptionalMatchNode), got {lp:?}");
        };
        let node = ext
            .node
            .as_any()
            .downcast_ref::<OptionalMatchNode>()
            .expect("OptionalMatchNode");

        // The shared var_0 is a join key, so the inner output drops all 6 of its
        // columns — keeping only the exploratory edge (var_1, 8 cols) and dst
        // (var_2, 6).
        assert_eq!(node.join_keys.len(), 1, "var_0 is the shared join key");
        let schema = UserDefinedLogicalNodeCore::schema(node);
        // outer var_0 (6) ++ inner kept (edge 8 + var_2 6 = 14) = 20.
        assert_eq!(schema.fields().len(), 20, "no duplicate var_0 columns");
        // Building the schema would have panicked on a duplicate qualified field
        // if var_0's columns were appended, so reaching here proves exclusion.
    }

    // -----------------------------------------------------------------------
    // CREATE lowering (#700)
    // -----------------------------------------------------------------------

    fn create_plan_with_props() -> GraphPlan {
        use graphforge_ir::{CreateNodeSpec, CreatePattern};
        let mut builder = GraphPlan::builder("openCypher");
        // Property map {name: 'Alice'} in the arena.
        let name_lit = builder.push_expr(IrExpr::Literal(IrLiteral::Str("Alice".into())));
        let map = builder.push_expr(IrExpr::MapLiteral(vec![("name".into(), name_lit)]));
        builder
            .push_op(GraphOp::Create {
                pattern: CreatePattern {
                    nodes: vec![CreateNodeSpec {
                        var: VarId(0),
                        labels: vec![TypeId(0)],
                        properties: Some(map),
                        is_reference: false,
                    }],
                    edges: vec![],
                },
            })
            .build()
    }

    #[test]
    fn create_lowers_to_extension_with_write_target() {
        let dir = tempfile::TempDir::new().unwrap();
        let lowerer = GraphPlanLowerer::new_for_writes(
            None,
            None,
            dir.path(),
            graphforge_core::OntologyMode::Exploratory,
        );
        let plan = create_plan_with_props();
        let lp = lowerer.lower_plan(&plan).unwrap();
        assert!(
            matches!(lp, DfLogicalPlan::Extension(_)),
            "expected Extension (GraphCreateNode), got {lp:?}"
        );
    }

    #[test]
    fn create_without_write_target_errors() {
        // The read-only `new` constructor has no write target.
        let lowerer = GraphPlanLowerer::new(None, None);
        let plan = create_plan_with_props();
        let result = lowerer.lower_plan(&plan);
        assert!(
            result.is_err(),
            "CREATE without a write target should error"
        );
    }

    #[test]
    fn new_with_dir_does_not_authorize_writes() {
        // `new_with_dir` grants read-side directory access (for var-length
        // Expand) but must NOT open the write path — only `new_for_writes`
        // authorizes CREATE.
        let dir = tempfile::TempDir::new().unwrap();
        let lowerer = GraphPlanLowerer::new_with_dir(
            None,
            None,
            dir.path(),
            graphforge_core::OntologyMode::Exploratory,
        );
        let result = lowerer.lower_plan(&create_plan_with_props());
        assert!(
            result.is_err(),
            "new_with_dir must not authorize CREATE; only new_for_writes does"
        );
    }

    #[test]
    fn create_non_literal_property_lowers_as_computed() {
        use graphforge_ir::{CreateNodeSpec, CreatePattern};
        let dir = tempfile::TempDir::new().unwrap();
        let lowerer = GraphPlanLowerer::new_for_writes(
            None,
            None,
            dir.path(),
            graphforge_core::OntologyMode::Exploratory,
        );
        let mut builder = GraphPlan::builder("openCypher");
        // A non-literal property value (here a parameter) is no longer rejected
        // (#814): it lowers to a row-dependent computed `Expr` on the create node,
        // evaluated per row by the execution layer.
        let param = builder.push_expr(IrExpr::Parameter("p".into()));
        let map = builder.push_expr(IrExpr::MapLiteral(vec![("name".into(), param)]));
        let plan = builder
            .push_op(GraphOp::Create {
                pattern: CreatePattern {
                    nodes: vec![CreateNodeSpec {
                        var: VarId(0),
                        labels: vec![],
                        properties: Some(map),
                        is_reference: false,
                    }],
                    edges: vec![],
                },
            })
            .build();
        let logical = lowerer
            .lower_plan(&plan)
            .expect("a non-literal CREATE property lowers to a computed expr");
        let datafusion::logical_expr::LogicalPlan::Extension(ext) = &logical else {
            panic!("CREATE lowers to an Extension node");
        };
        let create = ext
            .node
            .as_any()
            .downcast_ref::<graphforge_plan::GraphCreateNode>()
            .expect("a GraphCreateNode");
        assert!(
            create.nodes[0].properties.is_empty(),
            "the parameter value is not a baked literal"
        );
        assert_eq!(
            create.nodes[0].computed_properties.len(),
            1,
            "the parameter value is a row-dependent computed property"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn created_rows_schema_preserves_input_skips_references_and_types_minted_nodes() {
        use std::collections::HashMap;

        use datafusion::arrow::datatypes::{DataType, Field};
        use datafusion::common::{DFSchema, TableReference};
        use datafusion::logical_expr::{col, lit};
        use graphforge_plan::ResolvedNodeSpec;

        let input = Arc::new(
            DFSchema::new_with_metadata(
                vec![(
                    Some(TableReference::bare("input")),
                    Arc::new(Field::new("seed", DataType::Int64, false)),
                )],
                HashMap::new(),
            )
            .unwrap(),
        );
        let reference = ResolvedNodeSpec {
            var: 1,
            label_ids: vec![7],
            label_names: vec!["Existing".into()],
            properties: vec![("ignored".into(), IrLiteral::Int(1))],
            computed_properties: vec![],
            is_reference: true,
        };
        let minted = ResolvedNodeSpec {
            var: 2,
            label_ids: vec![8, 9],
            label_names: vec!["New".into(), "Tagged".into()],
            properties: vec![("active".into(), IrLiteral::Bool(true))],
            computed_properties: vec![("copied_seed".into(), col("seed") + lit(1_i64))],
            is_reference: false,
        };

        let schema = GraphPlanLowerer::created_rows_schema(&[reference, minted], &input).unwrap();
        let fields: Vec<_> = schema
            .iter()
            .map(|(qualifier, field)| {
                (
                    qualifier.map(ToString::to_string),
                    field.name().clone(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();

        assert_eq!(
            fields.len(),
            7,
            "one input plus four identity and two property fields"
        );
        assert_eq!(
            fields[0],
            (Some("input".into()), "seed".into(), DataType::Int64, false)
        );
        assert_eq!(
            fields[1..5]
                .iter()
                .map(|(q, name, ty, nullable)| { (q.clone(), name.clone(), ty.clone(), *nullable) })
                .collect::<Vec<_>>(),
            vec![
                (
                    Some("var_2".into()),
                    "node_uuid".into(),
                    DataType::FixedSizeBinary(16),
                    false
                ),
                (
                    Some("var_2".into()),
                    "node_id".into(),
                    DataType::UInt64,
                    false,
                ),
                (
                    Some("var_2".into()),
                    "type_id".into(),
                    DataType::UInt32,
                    false,
                ),
                (
                    Some("var_2".into()),
                    "type_ids".into(),
                    DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                    false,
                ),
            ]
        );
        assert_eq!(
            fields[5],
            (
                Some("var_2".into()),
                "active".into(),
                DataType::Boolean,
                true
            )
        );
        assert_eq!(
            fields[6],
            (
                Some("var_2".into()),
                "copied_seed".into(),
                DataType::Int64,
                true,
            )
        );
        assert!(
            fields
                .iter()
                .all(|(q, _, _, _)| q.as_deref() != Some("var_1")),
            "reference nodes are passed through only and never duplicated"
        );
    }

    #[test]
    fn created_rows_schema_rejects_reserved_and_unbound_computed_properties() {
        use graphforge_plan::ResolvedNodeSpec;

        let input = Arc::new(datafusion::common::DFSchema::empty());
        for reserved in ["node_uuid", "node_id", "type_id", "type_ids"] {
            let spec = ResolvedNodeSpec {
                var: 3,
                label_ids: vec![],
                label_names: vec![],
                properties: vec![(reserved.into(), IrLiteral::Null)],
                computed_properties: vec![],
                is_reference: false,
            };
            let error = GraphPlanLowerer::created_rows_schema(&[spec], &input).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!(
                    "unsupported expression: CREATE property `{reserved}` collides with a reserved node topology field"
                )
            );
        }

        let unbound = ResolvedNodeSpec {
            var: 4,
            label_ids: vec![],
            label_names: vec![],
            properties: vec![],
            computed_properties: vec![("value".into(), datafusion::logical_expr::col("missing"))],
            is_reference: false,
        };
        let error = GraphPlanLowerer::created_rows_schema(&[unbound], &input).unwrap_err();
        assert!(
            error.to_string().contains("No field named missing"),
            "unbound computed properties must retain the DataFusion schema error: {error}"
        );
    }

    // -----------------------------------------------------------------------
    // DELETE lowering (#740)
    // -----------------------------------------------------------------------

    /// `MATCH (n) DELETE n` over a real read dir, so the NodeScan binds
    /// `var_0.node_uuid` for the delete-target kind resolution.
    fn match_delete_plan(detach: bool) -> GraphPlan {
        GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Delete {
                vars: vec![VarId(0)],
                exprs: vec![],
                detach,
            })
            .build()
    }

    #[test]
    fn delete_lowers_to_extension_with_write_target() {
        let dir = tempfile::TempDir::new().unwrap();
        let rc = graphforge_ir::RuntimeCatalog::new();
        let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
        let lowerer = GraphPlanLowerer::new_for_writes(
            Some(&catalog),
            None,
            dir.path(),
            graphforge_core::OntologyMode::Exploratory,
        );
        let lp = lowerer.lower_plan(&match_delete_plan(false)).unwrap();
        assert!(
            matches!(lp, DfLogicalPlan::Extension(_)),
            "expected Extension (GraphDeleteNode), got {lp:?}"
        );
    }

    #[test]
    fn delete_without_write_target_errors() {
        let dir = tempfile::TempDir::new().unwrap();
        let rc = graphforge_ir::RuntimeCatalog::new();
        let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
        // `new_with_dir` grants read access but not the write path.
        let lowerer = GraphPlanLowerer::new_with_dir(
            Some(&catalog),
            None,
            dir.path(),
            graphforge_core::OntologyMode::Exploratory,
        );
        assert!(
            lowerer.lower_plan(&match_delete_plan(false)).is_err(),
            "DELETE without a write target should error"
        );
    }

    #[test]
    fn delete_edge_variable_resolves_is_edge_flag() {
        use graphforge_plan::GraphDeleteNode;

        // `MATCH ()-[r]->() DELETE r`: the edge var must resolve to a DeleteTarget
        // with is_edge=true (the input schema carries var_1.edge_uuid, not
        // node_uuid), so the executor reads the edge identity column.
        let dir = tempfile::TempDir::new().unwrap();
        let rc = graphforge_ir::RuntimeCatalog::new();
        let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
        let lowerer = GraphPlanLowerer::new_for_writes(
            Some(&catalog),
            None,
            dir.path(),
            graphforge_core::OntologyMode::Exploratory,
        );
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: Direction::Out,
                min_hops: 1,
                max_hops: Some(1),
            })
            .push_op(GraphOp::Delete {
                vars: vec![VarId(1)],
                exprs: vec![],
                detach: false,
            })
            .build();

        let lp = lowerer.lower_plan(&plan).unwrap();
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected Extension (GraphDeleteNode), got {lp:?}");
        };
        let node = ext
            .node
            .as_any()
            .downcast_ref::<GraphDeleteNode>()
            .expect("GraphDeleteNode");
        assert_eq!(node.targets.len(), 1, "one delete target");
        assert_eq!(node.targets[0].var, 1);
        assert!(
            node.targets[0].is_edge,
            "the edge var must resolve to is_edge=true"
        );
    }

    #[test]
    fn write_kind_resolution_covers_node_typed_edge_and_invalid_targets() {
        use std::collections::HashMap;

        use datafusion::arrow::datatypes::{DataType, Field};
        use datafusion::common::{DFSchema, TableReference};

        let schema = |names: &[&str]| {
            Arc::new(
                DFSchema::new_with_metadata(
                    names
                        .iter()
                        .map(|name| {
                            (
                                Some(TableReference::bare("var_7")),
                                Arc::new(Field::new(*name, DataType::Utf8, true)),
                            )
                        })
                        .collect(),
                    HashMap::new(),
                )
                .unwrap(),
            )
        };

        assert!(
            !GraphPlanLowerer::resolve_write_kind(&schema(&["node_uuid"]), VarId(7), "SET")
                .unwrap()
        );
        assert!(
            GraphPlanLowerer::resolve_write_kind(
                &schema(&["edge_uuid", "rel_type_name"]),
                VarId(7),
                "REMOVE"
            )
            .unwrap()
        );

        let untyped_edge =
            GraphPlanLowerer::resolve_write_kind(&schema(&["edge_uuid"]), VarId(7), "SET")
                .unwrap_err();
        assert!(untyped_edge.to_string().contains("known relation type"));

        let unbound =
            GraphPlanLowerer::resolve_write_kind(&schema(&[]), VarId(7), "REMOVE").unwrap_err();
        assert!(unbound.to_string().contains("must be bound"));

        // A malformed schema carrying both identities resolves as a node, the
        // same precedence used by DELETE target classification.
        assert!(
            !GraphPlanLowerer::resolve_write_kind(
                &schema(&["node_uuid", "edge_uuid", "rel_type_name"]),
                VarId(7),
                "SET"
            )
            .unwrap()
        );
    }

    // -----------------------------------------------------------------------
    // SET / REMOVE lowering (#791)
    // -----------------------------------------------------------------------

    fn writes_lowerer<'a>(
        catalog: &'a graphforge_storage::GraphCatalog,
        dir: &'a std::path::Path,
    ) -> GraphPlanLowerer<'a> {
        GraphPlanLowerer::new_for_writes(
            Some(catalog),
            None,
            dir,
            graphforge_core::OntologyMode::Exploratory,
        )
    }

    #[test]
    fn set_literal_value_lowers_to_extension() {
        use graphforge_ir::SetPropItem;
        let dir = tempfile::TempDir::new().unwrap();
        let rc = graphforge_ir::RuntimeCatalog::new();
        let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
        let lowerer = writes_lowerer(&catalog, dir.path());

        let mut builder = GraphPlan::builder("openCypher");
        let value = builder.push_expr(IrExpr::Literal(IrLiteral::Int(42)));
        let plan = builder
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Set {
                items: vec![SetPropItem {
                    target: VarId(0),
                    prop: graphforge_core::PropId(0),
                    prop_name: "age".into(),
                    value,
                }],
                map_items: vec![],
                label_items: vec![],
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected Extension (GraphSetNode), got {lp:?}");
        };
        let node = ext.node.as_any().downcast_ref::<GraphSetNode>().unwrap();
        assert_eq!(node.targets.len(), 1);
        assert_eq!(node.targets[0].prop_name, "age");
        assert!(!node.targets[0].is_edge, "node target");
    }

    #[test]
    fn set_runtime_expr_value_lowers_against_input_schema() {
        // `SET n.age = n.age + 1` — the value references the matched-row column
        // `var_0.age`; it must lower without error (the topology scan carries the
        // bound var, and the value expr resolves against the input schema).
        use graphforge_ir::SetPropItem;
        let dir = tempfile::TempDir::new().unwrap();
        let rc = graphforge_ir::RuntimeCatalog::new();
        let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
        let lowerer = writes_lowerer(&catalog, dir.path());

        let mut builder = GraphPlan::builder("openCypher");
        // n.age + 1
        let var = builder.push_expr(IrExpr::VarRef(VarId(0)));
        let age = builder.push_expr(IrExpr::PropertyAccess {
            base: var,
            prop: graphforge_core::PropId(0),
        });
        let one = builder.push_expr(IrExpr::Literal(IrLiteral::Int(1)));
        let sum = builder.push_expr(IrExpr::BinaryOp {
            op: graphforge_ir::expr::BinaryOpKind::Add,
            left: age,
            right: one,
        });
        let plan = builder
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Set {
                items: vec![SetPropItem {
                    target: VarId(0),
                    prop: graphforge_core::PropId(0),
                    prop_name: "age".into(),
                    value: sum,
                }],
                map_items: vec![],
                label_items: vec![],
            })
            .build();
        // The runtime value expr is carried onto the SET node, not collapsed.
        let lp = lowerer.lower_plan(&plan).unwrap();
        assert!(matches!(lp, DfLogicalPlan::Extension(_)));
    }

    #[test]
    fn remove_lowers_to_extension() {
        use graphforge_ir::RemovePropItem;
        let dir = tempfile::TempDir::new().unwrap();
        let rc = graphforge_ir::RuntimeCatalog::new();
        let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
        let lowerer = writes_lowerer(&catalog, dir.path());

        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Remove {
                items: vec![RemovePropItem {
                    target: VarId(0),
                    prop: graphforge_core::PropId(0),
                    prop_name: "age".into(),
                }],
                label_items: vec![],
            })
            .build();
        let lp = lowerer.lower_plan(&plan).unwrap();
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected Extension (GraphRemoveNode), got {lp:?}");
        };
        let node = ext.node.as_any().downcast_ref::<GraphRemoveNode>().unwrap();
        assert_eq!(node.targets.len(), 1);
        assert_eq!(node.targets[0].prop_name, "age");
    }

    #[test]
    fn set_without_write_target_errors() {
        use graphforge_ir::SetPropItem;
        let dir = tempfile::TempDir::new().unwrap();
        let rc = graphforge_ir::RuntimeCatalog::new();
        let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
        // Read-side dir access only — no write authorization.
        let lowerer = GraphPlanLowerer::new_with_dir(
            Some(&catalog),
            None,
            dir.path(),
            graphforge_core::OntologyMode::Exploratory,
        );
        let mut builder = GraphPlan::builder("openCypher");
        let value = builder.push_expr(IrExpr::Literal(IrLiteral::Int(1)));
        let plan = builder
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Set {
                items: vec![SetPropItem {
                    target: VarId(0),
                    prop: graphforge_core::PropId(0),
                    prop_name: "age".into(),
                    value,
                }],
                map_items: vec![],
                label_items: vec![],
            })
            .build();
        assert!(
            lowerer.lower_plan(&plan).is_err(),
            "SET without a write target should error"
        );
    }

    // -----------------------------------------------------------------------
    // Provider-backed fixed-hop lowering (#763, #1248)
    // -----------------------------------------------------------------------

    /// Single-hop typed plan over an interned KNOWS relation; returns the
    /// fixture pieces plus the relation TypeId.
    fn typed_single_hop_fixture(
        dir: Direction,
    ) -> (
        tempfile::TempDir,
        graphforge_storage::GraphCatalog,
        GraphPlan,
    ) {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut rc = graphforge_ir::RuntimeCatalog::new();
        let rel = graphforge_ir::runtime_relation_type_id(rc.intern_relation_type("KNOWS"));
        let catalog = graphforge_storage::GraphCatalog::open(tmp.path(), None, &rc).unwrap();
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: Some(rel),
                dir,
                min_hops: 1,
                max_hops: Some(1),
            })
            .build();
        (tmp, catalog, plan)
    }

    #[test]
    fn project_backed_single_hop_emits_expand_extension_node() {
        use datafusion::logical_expr::UserDefinedLogicalNodeCore;

        let (tmp, catalog, plan) = typed_single_hop_fixture(Direction::Out);
        let lowerer =
            GraphPlanLowerer::new_with_dir(Some(&catalog), None, tmp.path(), OntologyMode::Strict);

        let lp = lowerer.lower_plan(&plan).unwrap();
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected Extension (ExpandNode), got {lp:?}");
        };
        let node = ext
            .node
            .as_any()
            .downcast_ref::<graphforge_plan::ExpandNode>()
            .expect("ExpandNode");
        assert_eq!(node.rel_type_name, "KNOWS");
        assert_eq!(node.src_var, 0);
        assert_eq!(node.edge_var, 1);
        assert_eq!(node.dst_var, 2);
        assert_eq!(node.direction, Direction::Out);
        assert_eq!(node.mode, OntologyMode::Strict);
        assert_eq!(node.edge_prop_count, 0, "no edge_properties file on disk");

        // Schema parity essentials: edge topology under var_1, dst under var_2.
        let schema = UserDefinedLogicalNodeCore::schema(node);
        let edge = datafusion::common::TableReference::bare("var_1");
        let dst = datafusion::common::TableReference::bare("var_2");
        for name in ["edge_uuid", "src_id", "dst_id", "edge_id"] {
            assert!(
                schema.field_with_qualified_name(&edge, name).is_ok(),
                "{name}"
            );
        }
        assert!(schema.field_with_qualified_name(&dst, "node_id").is_ok());
    }

    #[test]
    fn project_backed_undirected_single_hop_emits_plain_extension() {
        let (tmp, catalog, plan) = typed_single_hop_fixture(Direction::Undirected);
        let lowerer =
            GraphPlanLowerer::new_with_dir(Some(&catalog), None, tmp.path(), OntologyMode::Strict);

        let lp = lowerer.lower_plan(&plan).unwrap();
        // No DISTINCT wrapper: the self-loop dedup happens inside ExpandExec
        // (a wrapping Distinct trips DataFusion's duplicate-field-name
        // disambiguation against the extension's multi-var schema).
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected Extension (ExpandNode), got {lp:?}");
        };
        let node = ext
            .node
            .as_any()
            .downcast_ref::<graphforge_plan::ExpandNode>()
            .expect("ExpandNode");
        assert_eq!(node.direction, Direction::Undirected);
    }

    #[test]
    fn schema_only_single_hop_keeps_join_path() {
        let (_tmp, catalog, plan) = typed_single_hop_fixture(Direction::Out);
        let lowerer = GraphPlanLowerer::new(Some(&catalog), None);
        let lp = lowerer.lower_plan(&plan).unwrap();
        assert!(
            matches!(lp, DfLogicalPlan::Join(_)),
            "expected Join chain, got {lp:?}"
        );
    }

    #[test]
    fn exploratory_mode_emits_expand_with_dynamic_edge_schema() {
        use datafusion::logical_expr::UserDefinedLogicalNodeCore;

        let (tmp, catalog, plan) = typed_single_hop_fixture(Direction::Out);
        let lowerer = GraphPlanLowerer::new_with_dir(
            Some(&catalog),
            None,
            tmp.path(),
            OntologyMode::Exploratory,
        );
        let lp = lowerer.lower_plan(&plan).unwrap();
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected exploratory ExpandNode, got {lp:?}");
        };
        let node = ext
            .node
            .as_any()
            .downcast_ref::<graphforge_plan::ExpandNode>()
            .expect("ExpandNode");
        let edge = datafusion::common::TableReference::bare("var_1");
        assert!(
            UserDefinedLogicalNodeCore::schema(node)
                .field_with_qualified_name(&edge, "rel_type_name")
                .is_ok()
        );
    }

    #[test]
    fn wildcard_single_hop_emits_expand_extension() {
        let tmp = tempfile::TempDir::new().unwrap();
        let rc = graphforge_ir::RuntimeCatalog::new();
        let catalog = graphforge_storage::GraphCatalog::open(tmp.path(), None, &rc).unwrap();
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: Direction::Out,
                min_hops: 1,
                max_hops: Some(1),
            })
            .build();
        let lowerer =
            GraphPlanLowerer::new_with_dir(Some(&catalog), None, tmp.path(), OntologyMode::Strict);
        let lp = lowerer.lower_plan(&plan).unwrap();
        let DfLogicalPlan::Extension(ext) = &lp else {
            panic!("expected wildcard ExpandNode, got {lp:?}");
        };
        let node = ext
            .node
            .as_any()
            .downcast_ref::<graphforge_plan::ExpandNode>()
            .expect("ExpandNode");
        assert_eq!(node.rel_type_name, "*");
        assert_eq!(node.rel_ty, None);
    }

    #[test]
    fn unbound_source_errors_on_provider_path() {
        // The adjacency path must not bypass the source-binding invariant: an
        // Expand whose src var was never bound is a lowering error, not a
        // silently column-0-seeded ExpandNode.
        let tmp = tempfile::TempDir::new().unwrap();
        let mut rc = graphforge_ir::RuntimeCatalog::new();
        let rel = graphforge_ir::runtime_relation_type_id(rc.intern_relation_type("KNOWS"));
        let catalog = graphforge_storage::GraphCatalog::open(tmp.path(), None, &rc).unwrap();
        // No NodeScan: src VarId(0) is never registered.
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: Some(rel),
                dir: Direction::Out,
                min_hops: 1,
                max_hops: Some(1),
            })
            .build();
        let lowerer =
            GraphPlanLowerer::new_with_dir(Some(&catalog), None, tmp.path(), OntologyMode::Strict);
        let err = lowerer.lower_plan(&plan).unwrap_err();
        assert!(
            err.to_string().contains("unbound") || err.to_string().contains("Unbound"),
            "expected an unbound-variable error, got {err:?}"
        );
    }
}
