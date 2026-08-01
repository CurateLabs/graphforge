//! GraphForge DataFusion integration, optimizer rules, and custom plan nodes.
//!
//! # Custom logical plan nodes (#577)
//!
//! Six graph-native operators cannot be expressed in relational algebra.  This
//! module defines their **logical plan stubs** — enough for the lowering layer
//! to produce a valid [`LogicalPlan`] that DataFusion accepts without panicking.
//! Physical implementations come in milestone 13 (Execution Baseline).
//!
//! | Node | Triggered by |
//! |---|---|
//! | [`VarLenExpandNode`] | `Expand` with `max_hops != Some(1)` |
//! | [`OptionalMatchNode`] | `Optional { child }` |
//! | [`PathUniqueNode`] | `Expand` with path-uniqueness flag |
//! | [`OntologyInferNode`] | `Expand` on transitive/symmetric relation |
//! | [`GraphMergeNode`] | `Merge { pattern }` |
//! | [`UnwindNode`] | `Unwind { list_expr, alias }` |
#![forbid(unsafe_code)]
// The `name()` methods return string literals but the trait signature requires
// `&str` tied to `&self`; the lint fires because the bound is unnecessary for
// the literal values.
#![allow(clippy::unnecessary_literal_bound)]

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::common::{DFSchema, DFSchemaRef, Result as DfResult, TableReference};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};

use graphforge_core::OntologyMode;
use graphforge_ir::{Direction, IrLiteral};

/// Implement `PartialOrd` for custom plan nodes that contain `DFSchemaRef`.
///
/// `DFSchemaRef` (`Arc<DFSchema>`) does not implement `PartialOrd`, so we
/// cannot derive it.  Returning `None` (incomparable) is the safe default
/// used by DataFusion's own test nodes.
macro_rules! impl_partial_ord {
    ($t:ty) => {
        impl PartialOrd for $t {
            fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
                None
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// A placeholder output schema that mirrors the input schema.  Used by stubs
/// whose real output schema is determined at physical planning time.
fn passthrough_schema(inputs: &[&LogicalPlan]) -> DFSchemaRef {
    inputs
        .first()
        .map_or_else(|| Arc::new(DFSchema::empty()), |p| p.schema().clone())
}

/// Arrow field name of the variable-length edge-list column (qualified
/// `var_<edge_var>`).  openCypher binds the edge variable `r` in
/// `(a)-[r:KNOWS*1..3]->(b)` to the **list of relationships** along each path.
///
/// Lowercase ASCII so the dotted reference `col("var_<edge>.rels")` resolves —
/// DataFusion lowercases unquoted identifiers.
pub const VAR_LEN_EDGE_LIST_FIELD: &str = "rels";

/// The Arrow [`Field`] for the variable-length edge-list column: a nullable
/// `List<Struct<{edge_uuid, src_uuid, dst_uuid, rel_type, <props…>}>>`.
///
/// This is the **single source of truth** for the column's type, shared by the
/// lowerer (which puts it in [`VarLenExpandNode`]'s schema) and the physical
/// `VarLenExpandExec` (which produces it).  They must be byte-identical or the
/// positional `RecordBatch::try_new` at execution time fails the schema check.
///
/// The struct carries only **public** UUID identity + the relation type — never
/// the surrogate `edge_id`/`src_id`/`dst_id` — so the UUID-only output contract
/// holds (the graphforge-api Shaper passes the whole column through untouched).
///
/// `prop_fields` are the relation's persisted edge-property columns (#755),
/// appended after the four topology fields in order. They are discovered at
/// lowering time from `edge_properties/<REL>.parquet` (see
/// `lower_var_len_expand`); pass an empty slice for a topology-only struct
/// (wildcard `*`, or a relation with no persisted properties), which reproduces
/// the original four-field layout byte-for-byte.
#[must_use]
pub fn var_len_edge_list_field(prop_fields: &[Field]) -> Arc<Field> {
    let mut fields = vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("rel_type", DataType::Utf8, true),
    ];
    fields.extend(prop_fields.iter().cloned());
    let struct_fields = Fields::from(fields);
    // The item is NULLABLE even though the exec never emits a null element:
    // DataFusion's list kernels (`array_pop_front` for `tail(r)`, #1023) return
    // item-nullable lists, and the runtime asserts the kernel's output type
    // matches the type promised at planning time — a non-null item fails that
    // assertion the moment any list function touches the column.
    let item = Arc::new(Field::new("item", DataType::Struct(struct_fields), true));
    Arc::new(Field::new(
        VAR_LEN_EDGE_LIST_FIELD,
        DataType::List(item),
        true,
    ))
}

// ---------------------------------------------------------------------------
// VarLenExpandNode
// ---------------------------------------------------------------------------

/// Physical node for variable-length path expansion.
///
/// Triggered when `Expand` has `max_hops != Some(1)` — patterns like
/// `(a)-[:KNOWS*1..3]->(b)`.  These cannot be expressed as a finite join
/// sequence, so the physical layer ([`VarLenExpandExec`](../graphforge_exec) in M13)
/// performs an iterative BFS over the Parquet edge table.
///
/// # Baked execution context
///
/// Like [`GraphCreateNode`], the project `dir` and ontology `mode` are baked in
/// at lowering time because the physical-planning `ExtensionPlanner` only sees
/// the DataFusion session state, not the GraphForge project path.  The physical
/// node reads edges directly from `dir` at execution time.
///
/// # Output schema
///
/// The output extends the input schema with the **destination** node's columns
/// (`var_<dst>`-qualified [`TOPOLOGY_NODES_SCHEMA`](../graphforge_storage)), then a
/// trailing edge-list column (`var_<edge>`-qualified [`var_len_edge_list_field`])
/// binding the edge variable `r` to the openCypher *list of relationships* along
/// each path (#709).  The edge column is **last**; the physical node produces
/// columns in this exact order.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VarLenExpandNode {
    /// Input plan (the source node scan / prior pipeline).
    pub input: Arc<LogicalPlan>,
    /// Name of the relation type to expand along (or `"*"` for wildcard).
    pub rel_type_name: String,
    /// Minimum number of hops.
    pub min_hops: u16,
    /// Maximum number of hops (`None` = unbounded).
    pub max_hops: Option<u16>,
    /// Source node pattern-variable id (the frontier seed in the input).
    pub src_var: u32,
    /// Destination node pattern-variable id (bound to the reached node).
    pub dst_var: u32,
    /// Edge pattern-variable id (bound to the per-path relationship list).
    pub edge_var: u32,
    /// Edge traversal direction.
    pub direction: Direction,
    /// Resolved relation type id (`TypeId.0`), or `None` for a wildcard.
    pub rel_ty: Option<u32>,
    /// Project directory the physical node reads edges from.
    pub dir: PathBuf,
    /// Ontology mode (drives typed vs exploratory edge-file routing).
    pub mode: OntologyMode,
    schema: DFSchemaRef,
}

impl VarLenExpandNode {
    /// Create a variable-length expand node.
    ///
    /// `dst_fields` is the destination node's column list (the storage layer's
    /// `TOPOLOGY_NODES_SCHEMA` fields), passed in by the lowerer so this crate
    /// need not depend on `graphforge-storage`.  They are qualified `var_<dst_var>` and
    /// appended to the input schema.  `edge_field` is the trailing edge-list
    /// column ([`var_len_edge_list_field`]), qualified `var_<edge_var>`.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        input: Arc<LogicalPlan>,
        rel_type_name: impl Into<String>,
        min_hops: u16,
        max_hops: Option<u16>,
        src_var: u32,
        dst_var: u32,
        edge_var: u32,
        direction: Direction,
        rel_ty: Option<u32>,
        dir: PathBuf,
        mode: OntologyMode,
        dst_fields: Vec<Arc<Field>>,
        edge_field: Arc<Field>,
    ) -> Self {
        let schema = Self::build_schema(&input, dst_var, edge_var, dst_fields, edge_field);
        Self {
            input,
            rel_type_name: rel_type_name.into(),
            min_hops,
            max_hops,
            src_var,
            dst_var,
            edge_var,
            direction,
            rel_ty,
            dir,
            mode,
            schema,
        }
    }

    /// Build the output [`DFSchema`]: the input's qualified fields, then the
    /// destination node's fields qualified `var_<dst_var>`, then the edge-list
    /// column qualified `var_<edge_var>` (last).
    fn build_schema(
        input: &Arc<LogicalPlan>,
        dst_var: u32,
        edge_var: u32,
        dst_fields: Vec<Arc<Field>>,
        edge_field: Arc<Field>,
    ) -> DFSchemaRef {
        let dst_qualifier = TableReference::bare(format!("var_{dst_var}"));
        let edge_qualifier = TableReference::bare(format!("var_{edge_var}"));
        let mut qualified: Vec<(Option<TableReference>, Arc<Field>)> = input
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();
        qualified.extend(
            dst_fields
                .into_iter()
                .map(|f| (Some(dst_qualifier.clone()), f)),
        );
        // The edge-list column is appended LAST; the physical node produces
        // columns in this order.
        qualified.push((Some(edge_qualifier), edge_field));
        // Fail fast: a construction error here means the node would advertise a
        // schema missing its `var_<dst>`/`var_<edge>` columns, breaking
        // downstream resolution.  This only fails on a malformed field set (a
        // programmer error), so panic with context rather than degrading
        // silently.
        Arc::new(
            DFSchema::new_with_metadata(qualified, std::collections::HashMap::new())
                .expect("VarLenExpandNode schema must include qualified destination + edge fields"),
        )
    }
}

impl_partial_ord!(VarLenExpandNode);

impl UserDefinedLogicalNodeCore for VarLenExpandNode {
    fn name(&self) -> &str {
        "VarLenExpand"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let max = self.max_hops.map_or("*".to_owned(), |h| h.to_string());
        let arrow = match self.direction {
            Direction::Out => "->",
            Direction::In => "<-",
            Direction::Undirected => "--",
        };
        write!(
            f,
            "VarLenExpand: rel={}, hops={}..{}, dir={arrow}, edge=var_{}",
            self.rel_type_name, self.min_hops, max, self.edge_var
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        // Reuse the already-computed destination fields (strip the qualifier);
        // the input schema is the only piece that can change here.
        let dst_qual = format!("var_{}", self.dst_var);
        let dst_fields: Vec<Arc<Field>> = self
            .schema
            .iter()
            .filter(|(q, _)| q.map(TableReference::table) == Some(dst_qual.as_str()))
            .map(|(_, f)| Arc::clone(f))
            .collect();
        // The trailing edge-list column (exactly one field qualified `var_<edge>`).
        let edge_qual = format!("var_{}", self.edge_var);
        let edge_field = self
            .schema
            .iter()
            .find(|(q, _)| q.map(TableReference::table) == Some(edge_qual.as_str()))
            .map_or_else(|| var_len_edge_list_field(&[]), |(_, f)| Arc::clone(f));
        Ok(Self::new(
            input,
            self.rel_type_name.clone(),
            self.min_hops,
            self.max_hops,
            self.src_var,
            self.dst_var,
            self.edge_var,
            self.direction,
            self.rel_ty,
            self.dir.clone(),
            self.mode,
            dst_fields,
            edge_field,
        ))
    }
}

// ---------------------------------------------------------------------------
// ExpandNode (#763, #1248)
// ---------------------------------------------------------------------------

/// Physical node for adjacency-backed single-hop expansion (#763).
///
/// Emitted by the lowerer **instead of** the two-join chain whenever a project
/// read target is available. The physical `ExpandExec` (graphforge-exec) probes the
/// session adjacency provider per frontier row; that provider owns the
/// hit/miss/building fallback policy, so plan shape is stable across index
/// state and downstream `LIMIT` can cancel traversal work (#1248).
///
/// # Output schema (join-path parity)
///
/// Exactly what the join chain would have produced, in the same order: the
/// input's qualified fields, then the edge topology fields (typed or
/// exploratory/wildcard) qualified `var_<edge_var>`, then persisted
/// edge-property fields (forced **nullable** — the join path LEFT-joins them)
/// also under `var_<edge_var>`, then the destination node's topology fields
/// qualified `var_<dst_var>`. Destination type filtering and property joining
/// stay downstream in the binder's trailing `NodeScan{dst, ty}` (#789),
/// exactly as on the join path.
///
/// For `Undirected`, `ExpandExec` collapses the provider's duplicate self-loop
/// entries per input row, matching the relational union shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExpandNode {
    /// Input plan (the source node scan / prior pipeline).
    pub input: Arc<LogicalPlan>,
    /// Name of the relation type to expand along (`"*"` for wildcard).
    pub rel_type_name: String,
    /// Source node pattern-variable id (the frontier seed in the input).
    pub src_var: u32,
    /// Destination node pattern-variable id (bound to the reached node).
    pub dst_var: u32,
    /// Edge pattern-variable id (bound per-column, like the join path).
    pub edge_var: u32,
    /// Edge traversal direction.
    pub direction: Direction,
    /// Resolved relation type id (`TypeId.0`), absent for wildcard expansion.
    pub rel_ty: Option<u32>,
    /// Project directory the physical node reads from.
    pub dir: PathBuf,
    /// Ontology mode controlling the persisted edge layout.
    pub mode: OntologyMode,
    /// How many of the `var_<edge_var>` fields are edge-property columns
    /// (the trailing ones); the rest are edge topology columns.
    pub edge_prop_count: usize,
    schema: DFSchemaRef,
}

impl ExpandNode {
    /// Create an adjacency-backed single-hop expand node.
    ///
    /// `edge_fields` are the typed edge table's topology columns and
    /// `edge_prop_fields` the relation's persisted property columns (the
    /// lowerer discovers them from `edge_properties/<REL>.parquet` and forces
    /// them nullable); `dst_fields` are the destination node's topology
    /// columns. Passed in so this crate need not depend on graphforge-storage.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        input: Arc<LogicalPlan>,
        rel_type_name: impl Into<String>,
        src_var: u32,
        dst_var: u32,
        edge_var: u32,
        direction: Direction,
        rel_ty: Option<u32>,
        dir: PathBuf,
        mode: OntologyMode,
        edge_fields: Vec<Arc<Field>>,
        edge_prop_fields: Vec<Arc<Field>>,
        dst_fields: Vec<Arc<Field>>,
    ) -> Self {
        let edge_prop_count = edge_prop_fields.len();
        let schema = Self::build_schema(
            &input,
            dst_var,
            edge_var,
            edge_fields,
            edge_prop_fields,
            dst_fields,
        );
        Self {
            input,
            rel_type_name: rel_type_name.into(),
            src_var,
            dst_var,
            edge_var,
            direction,
            rel_ty,
            dir,
            mode,
            edge_prop_count,
            schema,
        }
    }

    /// Build the output [`DFSchema`]: input qualified fields ++ edge topology
    /// fields (`var_<edge>`) ++ edge property fields (`var_<edge>`, forced
    /// nullable) ++ destination node fields (`var_<dst>`).
    fn build_schema(
        input: &Arc<LogicalPlan>,
        dst_var: u32,
        edge_var: u32,
        edge_fields: Vec<Arc<Field>>,
        edge_prop_fields: Vec<Arc<Field>>,
        dst_fields: Vec<Arc<Field>>,
    ) -> DFSchemaRef {
        let edge_qualifier = TableReference::bare(format!("var_{edge_var}"));
        let dst_qualifier = TableReference::bare(format!("var_{dst_var}"));
        let mut qualified: Vec<(Option<TableReference>, Arc<Field>)> = input
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();
        qualified.extend(
            edge_fields
                .into_iter()
                .map(|f| (Some(edge_qualifier.clone()), f)),
        );
        // Property columns are nullable on the join path (LEFT join); force it
        // here so the schemas agree regardless of the file's declared
        // nullability.
        qualified.extend(edge_prop_fields.into_iter().map(|f| {
            let f = Arc::new(f.as_ref().clone().with_nullable(true));
            (Some(edge_qualifier.clone()), f)
        }));
        qualified.extend(
            dst_fields
                .into_iter()
                .map(|f| (Some(dst_qualifier.clone()), f)),
        );
        Arc::new(
            DFSchema::new_with_metadata(qualified, std::collections::HashMap::new())
                .expect("ExpandNode schema must include qualified edge + destination fields"),
        )
    }
}

impl_partial_ord!(ExpandNode);

impl UserDefinedLogicalNodeCore for ExpandNode {
    fn name(&self) -> &str {
        "Expand"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let arrow = match self.direction {
            Direction::Out => "->",
            Direction::In => "<-",
            Direction::Undirected => "--",
        };
        write!(
            f,
            "Expand: rel={}, dir={arrow}, edge=var_{}",
            self.rel_type_name, self.edge_var
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        // Recover the edge/dst fields from this node's own schema by
        // qualifier; only the input can change under optimizer rewrites.
        let edge_qual = format!("var_{}", self.edge_var);
        let all_edge: Vec<Arc<Field>> = self
            .schema
            .iter()
            .filter(|(q, _)| q.map(TableReference::table) == Some(edge_qual.as_str()))
            .map(|(_, f)| Arc::clone(f))
            .collect();
        let topo_count = all_edge.len().saturating_sub(self.edge_prop_count);
        let edge_fields = all_edge[..topo_count].to_vec();
        let edge_prop_fields = all_edge[topo_count..].to_vec();
        let dst_qual = format!("var_{}", self.dst_var);
        let dst_fields: Vec<Arc<Field>> = self
            .schema
            .iter()
            .filter(|(q, _)| q.map(TableReference::table) == Some(dst_qual.as_str()))
            .map(|(_, f)| Arc::clone(f))
            .collect();
        Ok(Self::new(
            input,
            self.rel_type_name.clone(),
            self.src_var,
            self.dst_var,
            self.edge_var,
            self.direction,
            self.rel_ty,
            self.dir.clone(),
            self.mode,
            edge_fields,
            edge_prop_fields,
            dst_fields,
        ))
    }
}

// ---------------------------------------------------------------------------
// OptionalMatchNode
// ---------------------------------------------------------------------------

/// Physical node for `OPTIONAL MATCH` (LEFT OUTER semantics with openCypher
/// null-shaping over a sub-plan).
///
/// The physical layer ([`OptionalMatchExec`](../graphforge_exec) in M13) left-joins the
/// `outer` (mandatory) input against the `optional` sub-plan on the shared
/// pattern variables ([`join_keys`](Self::join_keys)), preserving every outer
/// row and setting the optional-side columns to **null** when there is no match
/// — distinct from a SQL `LEFT JOIN` only in that the shared join-key columns
/// are not duplicated on the output (they belong to the outer side).
///
/// # Output schema
///
/// `outer` fields, followed by the `optional` fields that are **not** join keys,
/// each made nullable (an unmatched outer row nulls them all).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OptionalMatchNode {
    /// The outer (mandatory) input.
    pub outer: Arc<LogicalPlan>,
    /// The optional sub-plan.
    pub optional: Arc<LogicalPlan>,
    /// Shared-variable join keys as `(outer_col_idx, inner_col_idx)` pairs,
    /// resolved against the qualified input schemas at lowering time.
    pub join_keys: Vec<(usize, usize)>,
    /// Inner (optional-side) column indices to append to the output, in order.
    /// Excludes **every** shared-variable column — not merely the join-key
    /// columns — because a shared variable's whole node (e.g. `a` in
    /// `MATCH (a) OPTIONAL MATCH (a)-[:R]->(b)`) is carried by the outer side;
    /// appending its columns again would duplicate the `var_<shared>` fields.
    pub inner_keep_idx: Vec<usize>,
    schema: DFSchemaRef,
}

impl OptionalMatchNode {
    /// Create an optional-match node.
    ///
    /// `inner_keep_idx` lists the optional plan's column indices to append to
    /// the output (every column of a shared variable already excluded — those
    /// live on the outer side). Passed by the lowerer, which resolves them
    /// against the qualified inner schema, so this crate needs no
    /// `graphforge-rel`/`graphforge-storage` dependency. The node appends the corresponding
    /// fields to the outer schema as **nullable** columns (an unmatched outer
    /// row nulls them all).
    #[must_use]
    pub fn new(
        outer: Arc<LogicalPlan>,
        optional: Arc<LogicalPlan>,
        join_keys: Vec<(usize, usize)>,
        inner_keep_idx: Vec<usize>,
    ) -> Self {
        let schema = Self::build_schema(&outer, &optional, &inner_keep_idx);
        Self {
            outer,
            optional,
            join_keys,
            inner_keep_idx,
            schema,
        }
    }

    /// Build the output [`DFSchema`]: outer fields, then the kept optional
    /// fields (by [`inner_keep_idx`](Self::inner_keep_idx)), each made nullable.
    fn build_schema(
        outer: &Arc<LogicalPlan>,
        optional: &Arc<LogicalPlan>,
        inner_keep_idx: &[usize],
    ) -> DFSchemaRef {
        let mut qualified: Vec<(Option<TableReference>, Arc<Field>)> = outer
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();
        let inner: Vec<(Option<TableReference>, Arc<Field>)> = optional
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();
        qualified.extend(inner_keep_idx.iter().map(|&i| {
            let (q, f) = &inner[i];
            // Unmatched outer rows null these columns, so they must be nullable
            // regardless of the inner plan's nullability (idempotent).
            let nullable = Arc::new(f.as_ref().clone().with_nullable(true));
            (q.clone(), nullable)
        }));
        Arc::new(
            DFSchema::new_with_metadata(qualified, std::collections::HashMap::new())
                .expect("OptionalMatchNode schema must be constructible from outer + inner fields"),
        )
    }
}

impl_partial_ord!(OptionalMatchNode);

impl UserDefinedLogicalNodeCore for OptionalMatchNode {
    fn name(&self) -> &str {
        "OptionalMatch"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.outer, &self.optional]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OptionalMatch: keys={}", self.join_keys.len())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        // Only the inputs can change here; the join keys and the kept-inner
        // column indices are preserved (the schema is rebuilt from them).
        let optional = Arc::new(inputs.pop().unwrap_or_else(|| (*self.optional).clone()));
        let outer = Arc::new(inputs.pop().unwrap_or_else(|| (*self.outer).clone()));
        Ok(Self::new(
            outer,
            optional,
            self.join_keys.clone(),
            self.inner_keep_idx.clone(),
        ))
    }
}

// ---------------------------------------------------------------------------
// PathUniqueNode
// ---------------------------------------------------------------------------

/// Logical stub for path-uniqueness filtering (eliminates paths that visit
/// the same node or edge more than once).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathUniqueNode {
    /// Input plan.
    pub input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
}

impl PathUniqueNode {
    /// Create a new path-uniqueness stub.
    #[must_use]
    pub fn new(input: Arc<LogicalPlan>) -> Self {
        let schema = passthrough_schema(&[&input]);
        Self { input, schema }
    }
}

impl_partial_ord!(PathUniqueNode);

impl UserDefinedLogicalNodeCore for PathUniqueNode {
    fn name(&self) -> &str {
        "PathUnique"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PathUnique")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        Ok(Self::new(input))
    }
}

// ---------------------------------------------------------------------------
// OntologyInferNode
// ---------------------------------------------------------------------------

/// Logical stub for ontology-driven semantic inference (transitive /
/// symmetric closure expansion).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OntologyInferNode {
    /// Input plan.
    pub input: Arc<LogicalPlan>,
    /// The relation type whose semantic flags trigger inference.
    pub rel_type_name: String,
    /// The ontology rule that triggered inference, e.g. `transitive:KNOWS` (#605).
    pub rule_id: String,
    /// The confidence policy for the derived facts, e.g. `conservative_min` (#605).
    pub confidence_model: String,
    schema: DFSchemaRef,
}

impl OntologyInferNode {
    /// Create a new ontology-inference node carrying its rule (#605).
    #[must_use]
    pub fn new(
        input: Arc<LogicalPlan>,
        rel_type_name: impl Into<String>,
        rule_id: impl Into<String>,
        confidence_model: impl Into<String>,
    ) -> Self {
        let schema = passthrough_schema(&[&input]);
        Self {
            input,
            rel_type_name: rel_type_name.into(),
            rule_id: rule_id.into(),
            confidence_model: confidence_model.into(),
            schema,
        }
    }
}

impl_partial_ord!(OntologyInferNode);

impl UserDefinedLogicalNodeCore for OntologyInferNode {
    fn name(&self) -> &str {
        "OntologyInfer"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OntologyInfer: rel={} rule_id={}",
            self.rel_type_name, self.rule_id
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        Ok(Self::new(
            input,
            self.rel_type_name.clone(),
            self.rule_id.clone(),
            self.confidence_model.clone(),
        ))
    }
}

// ---------------------------------------------------------------------------
// GraphMergeNode
// ---------------------------------------------------------------------------

/// Logical stub for `MERGE` (match-or-create write semantics).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraphMergeNode {
    schema: DFSchemaRef,
}

impl GraphMergeNode {
    /// Create a new graph-merge stub.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: Arc::new(DFSchema::empty()),
        }
    }
}

impl_partial_ord!(GraphMergeNode);

impl Default for GraphMergeNode {
    fn default() -> Self {
        Self::new()
    }
}

impl UserDefinedLogicalNodeCore for GraphMergeNode {
    fn name(&self) -> &str {
        "GraphMerge"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GraphMerge")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        Ok(Self::new())
    }
}

// ---------------------------------------------------------------------------
// GraphCreateNode
// ---------------------------------------------------------------------------

/// A node to create, fully resolved (no expression-arena coupling).
///
/// Built by the relational lowering layer from a `graphforge_ir::CreateNodeSpec`:
/// label IDs are paired with their resolved names and property maps are
/// evaluated to literal key/value pairs, so the execution layer needs no
/// access to the IR arena or ontology.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedNodeSpec {
    /// Pattern variable id (`VarId.0`).
    pub var: u32,
    /// Complete resolved label type-id set, in pattern order.
    pub label_ids: Vec<u32>,
    /// Complete resolved label-name set, in pattern order.
    pub label_names: Vec<String>,
    /// Literal property key/value pairs (constant or constant-folded values).
    pub properties: Vec<(String, IrLiteral)>,
    /// Row-dependent property values that could not be folded to a literal
    /// (e.g. `{n: x}` from a driving `UNWIND`/`MATCH`): each is a DataFusion
    /// `Expr` over the input columns, evaluated per minted row by the execution
    /// layer (#814). Surfaced through the node's `expressions()` so the
    /// optimizer rewrites the columns they reference.
    pub computed_properties: Vec<(String, Expr)>,
    /// `true` when this var was bound by a preceding `MATCH`/`WITH` — the
    /// executor **references** the matched node (its identity comes from the
    /// input row) rather than minting a new one (#703).
    pub is_reference: bool,
}

/// An edge to create, fully resolved.  See [`ResolvedNodeSpec`].
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedEdgeSpec {
    /// Pattern variable id of the edge.
    pub var: u32,
    /// Source node variable id.
    pub src: u32,
    /// Destination node variable id.
    pub dst: u32,
    /// Resolved relation type id, if any.
    pub rel_type_id: Option<u32>,
    /// Resolved relation type name (for file routing), if known.
    pub rel_type_name: Option<String>,
    /// Edge direction.
    pub direction: Direction,
    /// Literal property key/value pairs (constant or constant-folded values).
    ///
    /// Persisted as of #784: the execution layer writes these via
    /// `GraphWriter::set_edge_properties` to `edge_properties/<REL_TYPE>.parquet`
    /// (keyed by `edge_uuid`), and the read side joins them back. An edge with
    /// properties but no relation type is rejected (the props would be
    /// unreadable), so a non-empty map implies `rel_type_name` is `Some`.
    pub properties: Vec<(String, IrLiteral)>,
    /// Row-dependent property values evaluated per minted row (#814); see
    /// [`ResolvedNodeSpec::computed_properties`].
    pub computed_properties: Vec<(String, Expr)>,
}

/// Logical node for `CREATE`: a write specification driven by an input plan.
///
/// Carries the resolved node/edge specs plus the project directory and ontology
/// mode needed to drive a writer.  The directory is baked in at lowering time
/// because the physical-planning layer (an `ExtensionPlanner`) only sees the
/// DataFusion session state, not the GraphForge project path.
///
/// # Input
///
/// The CREATE runs **once per input row** (#703): a standalone `CREATE (:X)`
/// lowers over the implicit single unit row (so it creates exactly once), while
/// a mixed `MATCH … CREATE …` runs the write once per matched row, referencing
/// MATCH-bound vars' identities (`ResolvedNodeSpec::is_reference`) and minting
/// the rest. The input columns are **consumed** (for cardinality + referenced
/// identities), not projected — the node still emits only the write summary.
///
/// Output schema is a one-row write summary: `nodes_created` / `edges_created`
/// (`UInt64`).
#[derive(Debug, Clone)]
pub struct GraphCreateNode {
    /// Input plan whose rows drive the writes (the implicit unit row for a
    /// standalone CREATE; the MATCH results for a mixed pipeline).
    pub input: Arc<LogicalPlan>,
    /// Nodes to create, in pattern order.
    pub nodes: Vec<ResolvedNodeSpec>,
    /// Edges to create, in pattern order.
    pub edges: Vec<ResolvedEdgeSpec>,
    /// Target project directory.
    pub dir: PathBuf,
    /// Ontology mode (drives writer edge / property routing).
    pub mode: OntologyMode,
    /// Output schema: the one-row write summary in summary mode, or the
    /// created-entity row schema in emit-rows mode (#814).
    schema: DFSchemaRef,
    /// `true` when the node emits created-entity rows (write-result RETURN);
    /// `false` for the terminal one-row summary.
    emit_rows: bool,
}

impl GraphCreateNode {
    /// Build the write-summary Arrow schema
    /// (`nodes_created`, `edges_created`, `properties_set`, `labels_added`).
    #[must_use]
    pub fn summary_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("nodes_created", DataType::UInt64, false),
            Field::new("edges_created", DataType::UInt64, false),
            Field::new("properties_set", DataType::UInt64, false),
            Field::new("labels_added", DataType::UInt64, false),
        ]))
    }

    /// Create a new graph-create node over `input` in **summary** mode: the node
    /// emits the one-row write summary (a terminal `CREATE`).
    #[must_use]
    pub fn new(
        input: Arc<LogicalPlan>,
        nodes: Vec<ResolvedNodeSpec>,
        edges: Vec<ResolvedEdgeSpec>,
        dir: PathBuf,
        mode: OntologyMode,
    ) -> Self {
        let schema = Arc::new(
            DFSchema::try_from(Self::summary_schema())
                .expect("write-summary schema is always valid"),
        );
        Self {
            input,
            nodes,
            edges,
            dir,
            mode,
            schema,
            emit_rows: false,
        }
    }

    /// Create a node in **emit-rows** mode (write-result RETURN, #814): instead
    /// of the summary, it emits one row per input row carrying the input columns
    /// plus the created variables' `var_<n>`-qualified identity + property columns
    /// (`output_schema`), so a following `RETURN`/`WITH` can read them. Side
    /// effects are reported out of band (the exec's tally), since a relation
    /// cannot also carry the summary.
    #[must_use]
    pub fn new_emitting(
        input: Arc<LogicalPlan>,
        nodes: Vec<ResolvedNodeSpec>,
        edges: Vec<ResolvedEdgeSpec>,
        dir: PathBuf,
        mode: OntologyMode,
        output_schema: DFSchemaRef,
    ) -> Self {
        Self {
            input,
            nodes,
            edges,
            dir,
            mode,
            schema: output_schema,
            emit_rows: true,
        }
    }

    /// Whether this node emits created-entity rows (`true`) rather than the
    /// one-row write summary (`false`).
    #[must_use]
    pub fn emits_rows(&self) -> bool {
        self.emit_rows
    }
}

impl_partial_ord!(GraphCreateNode);

// `IrLiteral` is `PartialEq` (so we derive `PartialEq`) but not `Eq`/`Hash`
// (it carries `f64`).  `UserDefinedLogicalNodeCore` requires `Eq + Hash`, so we
// provide them by hand: `Eq` is the empty marker (literal property values are
// never NaN-reflexivity-sensitive in practice — matching DataFusion's own
// float-bearing nodes), and `Hash` normalises each literal (floats via
// `to_bits`).  `schema` is excluded from both (it is derived from the rest).
impl PartialEq for GraphCreateNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.nodes == other.nodes
            && self.edges == other.edges
            && self.dir == other.dir
            && self.mode == other.mode
            && self.emit_rows == other.emit_rows
    }
}

impl Eq for GraphCreateNode {}

fn hash_literal<H: Hasher>(lit: &IrLiteral, state: &mut H) {
    match lit {
        IrLiteral::Null => 0u8.hash(state),
        IrLiteral::Bool(b) => {
            1u8.hash(state);
            b.hash(state);
        }
        IrLiteral::Int(i) => {
            2u8.hash(state);
            i.hash(state);
        }
        IrLiteral::Float(f) => {
            3u8.hash(state);
            f.to_bits().hash(state);
        }
        IrLiteral::Str(s) => {
            4u8.hash(state);
            s.hash(state);
        }
        IrLiteral::Uuid(uuid) => {
            14u8.hash(state);
            uuid.hash(state);
        }
        IrLiteral::Duration {
            months,
            days,
            seconds,
            nanos,
        } => {
            5u8.hash(state);
            months.hash(state);
            days.hash(state);
            seconds.hash(state);
            nanos.hash(state);
        }
        IrLiteral::DateTime(t) => {
            6u8.hash(state);
            t.hash(state);
        }
        IrLiteral::Date(d) => {
            7u8.hash(state);
            d.hash(state);
        }
        IrLiteral::LocalDateTime { days, nanos } => {
            8u8.hash(state);
            days.hash(state);
            nanos.hash(state);
        }
        IrLiteral::Time(n) => {
            9u8.hash(state);
            n.hash(state);
        }
        IrLiteral::ZonedTime { nanos, offset } => {
            10u8.hash(state);
            nanos.hash(state);
            offset.hash(state);
        }
        IrLiteral::ZonedDateTime {
            days,
            nanos,
            offset,
            zone,
        } => {
            11u8.hash(state);
            days.hash(state);
            nanos.hash(state);
            offset.hash(state);
            zone.hash(state);
        }
        IrLiteral::List(items) => {
            12u8.hash(state);
            items.len().hash(state);
            for it in items {
                hash_literal(it, state);
            }
        }
        IrLiteral::Map(entries) => {
            13u8.hash(state);
            entries.len().hash(state);
            for (key, value) in entries {
                key.hash(state);
                hash_literal(value, state);
            }
        }
    }
}

fn hash_props<H: Hasher>(props: &[(String, IrLiteral)], state: &mut H) {
    props.len().hash(state);
    for (k, v) in props {
        k.hash(state);
        hash_literal(v, state);
    }
}

// Computed-property values are `Expr` (not `Hash`); hash the count and keys and
// let `PartialEq` disambiguate the exprs (matching `GraphSetNode::hash`).
fn hash_computed<H: Hasher>(props: &[(String, Expr)], state: &mut H) {
    props.len().hash(state);
    for (k, _) in props {
        k.hash(state);
    }
}

impl Hash for GraphCreateNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.nodes.len().hash(state);
        for n in &self.nodes {
            n.var.hash(state);
            n.label_ids.hash(state);
            n.label_names.hash(state);
            n.is_reference.hash(state);
            hash_props(&n.properties, state);
            hash_computed(&n.computed_properties, state);
        }
        self.edges.len().hash(state);
        for e in &self.edges {
            e.var.hash(state);
            e.src.hash(state);
            e.dst.hash(state);
            e.rel_type_id.hash(state);
            e.rel_type_name.hash(state);
            e.direction.hash(state);
            hash_props(&e.properties, state);
            hash_computed(&e.computed_properties, state);
        }
        self.dir.hash(state);
        self.mode.hash(state);
        self.emit_rows.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for GraphCreateNode {
    fn name(&self) -> &str {
        "GraphCreate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // Surface every row-dependent property value expr (nodes then edges, in
        // spec order) so the optimizer sees the columns they reference and
        // round-trips them through `with_exprs_and_inputs`.
        self.nodes
            .iter()
            .flat_map(|n| n.computed_properties.iter())
            .chain(self.edges.iter().flat_map(|e| e.computed_properties.iter()))
            .map(|(_, e)| e.clone())
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GraphCreate: nodes={}, edges={}",
            self.nodes.len(),
            self.edges.len()
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        // Reinstall the computed-property exprs positionally in the same
        // nodes-then-edges order `expressions()` emits; a short/empty `exprs`
        // (e.g. a probe call) keeps the originals.
        let mut nodes = self.nodes.clone();
        let mut edges = self.edges.clone();
        let total: usize = nodes
            .iter()
            .map(|n| n.computed_properties.len())
            .sum::<usize>()
            + edges
                .iter()
                .map(|e| e.computed_properties.len())
                .sum::<usize>();
        if exprs.len() == total {
            let mut it = exprs.into_iter();
            for n in &mut nodes {
                for (_, e) in &mut n.computed_properties {
                    *e = it.next().expect("count checked above");
                }
            }
            for e in &mut edges {
                for (_, x) in &mut e.computed_properties {
                    *x = it.next().expect("count checked above");
                }
            }
        }
        // Preserve emit-rows mode + its output schema (a plain `new` would reset
        // to summary mode and drop the created-rows schema).
        if self.emit_rows {
            Ok(Self::new_emitting(
                input,
                nodes,
                edges,
                self.dir.clone(),
                self.mode,
                self.schema.clone(),
            ))
        } else {
            Ok(Self::new(input, nodes, edges, self.dir.clone(), self.mode))
        }
    }
}

// ---------------------------------------------------------------------------
// GraphDeleteNode
// ---------------------------------------------------------------------------

/// One resolved `DELETE` target: a bound variable and whether it is an edge.
///
/// The binder records only the `VarId`; the lowering layer resolves whether the
/// var is a node or an edge by inspecting the input schema (which identity
/// column — `node_uuid` vs `edge_uuid` — it carries), so the execution layer
/// reads the right identity column without re-deriving it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeleteTarget {
    /// Pattern variable id (`VarId.0`).
    pub var: u32,
    /// `true` if the variable is an edge (delete the edge row); `false` for a
    /// node (delete the node row, and incident edges when `detach`).
    pub is_edge: bool,
}

/// Logical node for `DELETE` / `DETACH DELETE` (#740): a delete specification
/// driven by an input plan (the preceding `MATCH`).
///
/// Like [`GraphCreateNode`], the project directory and ontology mode are baked
/// in at lowering time (the `ExtensionPlanner` sees only the DataFusion session
/// state). The input rows supply the identities of the entities to delete; the
/// node emits a one-row write summary `nodes_deleted` / `edges_deleted`.
#[derive(Debug, Clone)]
pub struct GraphDeleteNode {
    /// Input plan whose rows carry the matched entities' identities.
    pub input: Arc<LogicalPlan>,
    /// The variables to delete, with their resolved node/edge kind.
    pub targets: Vec<DeleteTarget>,
    /// `true` for `DETACH DELETE` (also remove a node's incident edges).
    pub detach: bool,
    /// Target project directory.
    pub dir: PathBuf,
    /// Ontology mode (drives writer edge / property routing).
    pub mode: OntologyMode,
    schema: DFSchemaRef,
}

impl GraphDeleteNode {
    /// Build the write-summary Arrow schema (`nodes_deleted`, `edges_deleted`).
    #[must_use]
    pub fn summary_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("nodes_deleted", DataType::UInt64, false),
            Field::new("edges_deleted", DataType::UInt64, false),
        ]))
    }

    /// Create a new graph-delete node over `input`.
    #[must_use]
    pub fn new(
        input: Arc<LogicalPlan>,
        targets: Vec<DeleteTarget>,
        detach: bool,
        dir: PathBuf,
        mode: OntologyMode,
    ) -> Self {
        let schema = Arc::new(
            DFSchema::try_from(Self::summary_schema())
                .expect("write-summary schema is always valid"),
        );
        Self {
            input,
            targets,
            detach,
            dir,
            mode,
            schema,
        }
    }
}

impl_partial_ord!(GraphDeleteNode);

// `schema` is derived from the rest, so it is excluded from `PartialEq`/`Hash`
// (it is not part of the node's logical identity).
impl PartialEq for GraphDeleteNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.targets == other.targets
            && self.detach == other.detach
            && self.dir == other.dir
            && self.mode == other.mode
    }
}

impl Eq for GraphDeleteNode {}

impl Hash for GraphDeleteNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.targets.hash(state);
        self.detach.hash(state);
        self.dir.hash(state);
        self.mode.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for GraphDeleteNode {
    fn name(&self) -> &str {
        "GraphDelete"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GraphDelete: targets={}, detach={}",
            self.targets.len(),
            self.detach
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        Ok(Self::new(
            input,
            self.targets.clone(),
            self.detach,
            self.dir.clone(),
            self.mode,
        ))
    }
}

// ---------------------------------------------------------------------------
// GraphSetNode / GraphRemoveNode
// ---------------------------------------------------------------------------

/// One resolved `SET n.prop = <expr>` target (#791).
///
/// Like [`DeleteTarget`], the node/edge kind (`is_edge`) is resolved by the
/// lowering layer from the input schema. `value` is the lowered DataFusion
/// expression for the assigned value — evaluated per matched row at execution
/// time (mirrors how [`UnwindNode`] carries its `list_expr`).
///
/// The property-file **stem** is resolved per row in the exec layer, not baked
/// here: a node uses its `type_id` (→ `_untyped` in Exploratory mode, else the
/// entity name from [`GraphSetNode::type_id_to_entity_name`]); an edge uses its
/// `rel_type_name` column (edge property files are keyed by relation name in
/// every mode).
#[derive(Debug, Clone, PartialEq)]
pub struct SetTarget {
    /// Pattern variable id (`VarId.0`).
    pub var: u32,
    /// `true` if the variable is an edge (write to `edge_properties/`); `false`
    /// for a node (write to `properties/`).
    pub is_edge: bool,
    /// The property name being assigned.
    pub prop_name: String,
    /// The value expression, evaluated per matched row in the exec layer.
    pub value: Expr,
}

/// One resolved `REMOVE n.prop` target (#791) — the value-less dual of
/// [`SetTarget`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemoveTarget {
    /// Pattern variable id (`VarId.0`).
    pub var: u32,
    /// `true` if the variable is an edge.
    pub is_edge: bool,
    /// The property name being removed.
    pub prop_name: String,
}

/// Logical node for `SET <prop> = <expr>` (#791): a property-write driven by an
/// input plan (the preceding `MATCH`).
///
/// Mirrors [`GraphDeleteNode`] — project directory and ontology mode are baked
/// in at lowering time. Each target's value expression is evaluated per matched
/// row by the physical layer, the resulting per-row literal written to the
/// entity's property file. Node-target file stems are resolved per row from the
/// row's `type_id` via [`type_id_to_entity_name`](Self::type_id_to_entity_name)
/// (handles an untyped `MATCH (n)` whose rows span several entity types). The
/// node emits a one-row summary `properties_set`.
#[derive(Debug, Clone)]
pub struct GraphSetNode {
    /// Input plan whose rows carry the matched entities' identities + columns.
    pub input: Arc<LogicalPlan>,
    /// The property assignments, with resolved node/edge kind + value expr.
    pub targets: Vec<SetTarget>,
    /// Maps a node `type_id` to its property-file entity stem, for per-row node
    /// stem resolution (an empty map / missing id falls back to `_untyped`).
    pub type_id_to_entity_name: HashMap<u32, String>,
    /// Target project directory.
    pub dir: PathBuf,
    /// Ontology mode (drives node property-file routing).
    pub mode: OntologyMode,
    schema: DFSchemaRef,
}

impl GraphSetNode {
    /// Build the write-summary Arrow schema (`properties_set`).
    #[must_use]
    pub fn summary_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "properties_set",
            DataType::UInt64,
            false,
        )]))
    }

    /// Create a new graph-set node over `input`.
    #[must_use]
    pub fn new(
        input: Arc<LogicalPlan>,
        targets: Vec<SetTarget>,
        type_id_to_entity_name: HashMap<u32, String>,
        dir: PathBuf,
        mode: OntologyMode,
    ) -> Self {
        let schema = Arc::new(
            DFSchema::try_from(Self::summary_schema())
                .expect("write-summary schema is always valid"),
        );
        Self {
            input,
            targets,
            type_id_to_entity_name,
            dir,
            mode,
            schema,
        }
    }
}

impl_partial_ord!(GraphSetNode);

// `schema` is derived; excluded from `PartialEq`/`Hash` (not part of logical
// identity). The value exprs ARE part of identity, so they are included.
impl PartialEq for GraphSetNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.targets == other.targets
            && self.dir == other.dir
            && self.mode == other.mode
            && self.type_id_to_entity_name == other.type_id_to_entity_name
    }
}

impl Eq for GraphSetNode {}

impl Hash for GraphSetNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        // `SetTarget` is not `Hash` (it holds an `Expr`, which is not `Hash`);
        // hash the stable scalar fields and let `PartialEq` disambiguate the
        // value exprs. Plan-node hashing only needs to be consistent with `Eq`
        // for buckets, not collision-free.
        for t in &self.targets {
            t.var.hash(state);
            t.is_edge.hash(state);
            t.prop_name.hash(state);
        }
        self.dir.hash(state);
        self.mode.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for GraphSetNode {
    fn name(&self) -> &str {
        "GraphSet"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // Surface each target's value expr so the optimizer sees the columns it
        // references (and round-trips them through `with_exprs_and_inputs`).
        self.targets.iter().map(|t| t.value.clone()).collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GraphSet: targets={}", self.targets.len())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        // Reinstall value exprs positionally; an empty/short `exprs` (e.g. a
        // probe call) keeps the originals.
        let mut targets = self.targets.clone();
        if exprs.len() == targets.len() {
            for (t, e) in targets.iter_mut().zip(exprs) {
                t.value = e;
            }
        }
        Ok(Self::new(
            input,
            targets,
            self.type_id_to_entity_name.clone(),
            self.dir.clone(),
            self.mode,
        ))
    }
}

/// Logical node for `REMOVE <prop>` (#791): the value-less dual of
/// [`GraphSetNode`]. Emits a one-row summary `properties_removed`.
#[derive(Debug, Clone)]
pub struct GraphRemoveNode {
    /// Input plan whose rows carry the matched entities' identities + columns.
    pub input: Arc<LogicalPlan>,
    /// The properties to remove, with resolved node/edge kind.
    pub targets: Vec<RemoveTarget>,
    /// Maps a node `type_id` to its property-file entity stem (see
    /// [`GraphSetNode::type_id_to_entity_name`]).
    pub type_id_to_entity_name: HashMap<u32, String>,
    /// Target project directory.
    pub dir: PathBuf,
    /// Ontology mode (drives node property-file routing).
    pub mode: OntologyMode,
    schema: DFSchemaRef,
}

impl GraphRemoveNode {
    /// Build the write-summary Arrow schema (`properties_removed`).
    #[must_use]
    pub fn summary_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "properties_removed",
            DataType::UInt64,
            false,
        )]))
    }

    /// Create a new graph-remove node over `input`.
    #[must_use]
    pub fn new(
        input: Arc<LogicalPlan>,
        targets: Vec<RemoveTarget>,
        type_id_to_entity_name: HashMap<u32, String>,
        dir: PathBuf,
        mode: OntologyMode,
    ) -> Self {
        let schema = Arc::new(
            DFSchema::try_from(Self::summary_schema())
                .expect("write-summary schema is always valid"),
        );
        Self {
            input,
            targets,
            type_id_to_entity_name,
            dir,
            mode,
            schema,
        }
    }
}

impl_partial_ord!(GraphRemoveNode);

impl PartialEq for GraphRemoveNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.targets == other.targets
            && self.dir == other.dir
            && self.mode == other.mode
            && self.type_id_to_entity_name == other.type_id_to_entity_name
    }
}

impl Eq for GraphRemoveNode {}

impl Hash for GraphRemoveNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.targets.hash(state);
        self.dir.hash(state);
        self.mode.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for GraphRemoveNode {
    fn name(&self) -> &str {
        "GraphRemove"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GraphRemove: targets={}", self.targets.len())
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        Ok(Self::new(
            input,
            self.targets.clone(),
            self.type_id_to_entity_name.clone(),
            self.dir.clone(),
            self.mode,
        ))
    }
}

// ---------------------------------------------------------------------------
// UnwindNode
// ---------------------------------------------------------------------------

/// Physical node for `UNWIND` — explodes a list expression into one row per
/// element, binding each element to an alias variable.
///
/// The physical layer ([`UnwindExec`](../graphforge_exec) in M13) evaluates `list_expr`
/// per input row and emits one output row per list element (null/empty list →
/// zero rows). The output extends the input schema with a single `alias`-named,
/// `alias`-qualified column of the element type (nullable — elements may be
/// null).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnwindNode {
    /// Input plan.
    pub input: Arc<LogicalPlan>,
    /// The list expression to iterate.
    pub list_expr: Expr,
    /// The alias variable qualifier bound to each element (e.g. `var_0`).
    pub alias: String,
    element_field: Arc<Field>,
    schema: DFSchemaRef,
}

impl UnwindNode {
    /// Create an unwind node.
    ///
    /// `element_field` is the unwound element's column (its type comes from the
    /// list element type, resolved by the lowerer so this crate needs no
    /// `graphforge-rel` dependency). It is appended to the input schema qualified by
    /// `alias` and forced nullable (UNWIND elements may be null).
    #[must_use]
    pub fn new(
        input: Arc<LogicalPlan>,
        list_expr: Expr,
        alias: impl Into<String>,
        element_field: &Field,
    ) -> Self {
        let alias = alias.into();
        // The element column is named after the alias and forced nullable
        // (UNWIND elements may be null); build_schema appends it unqualified so a
        // bare `RETURN x` (lowered to `col(alias)`) resolves it.
        let element_field = Arc::new(element_field.clone().with_name(&alias).with_nullable(true));
        let schema = Self::build_schema(&input, &alias, Arc::clone(&element_field));
        Self {
            input,
            list_expr,
            alias,
            element_field,
            schema,
        }
    }

    /// Build the output [`DFSchema`]: the input's qualified fields followed by
    /// the element column (already named after the alias, nullable), appended
    /// **unqualified** so a bare `UNWIND … AS x RETURN x` — which lowers `x` to
    /// `col(alias)`, an unqualified `Column` — resolves it.
    fn build_schema(
        input: &Arc<LogicalPlan>,
        alias: &str,
        element_field: Arc<Field>,
    ) -> DFSchemaRef {
        let mut qualified: Vec<(Option<TableReference>, Arc<Field>)> = input
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();
        if let DataType::Struct(fields) = element_field.data_type()
            && fields.iter().any(|field| {
                matches!(
                    field.name().as_str(),
                    "node_uuid" | "edge_uuid" | "src_uuid" | "dst_uuid" | "nodes" | "relationships"
                )
            })
        {
            let alias_ref = TableReference::bare(alias.to_owned());
            qualified.extend(fields.iter().map(|field| {
                (
                    Some(alias_ref.clone()),
                    Arc::new(field.as_ref().clone().with_nullable(true)),
                )
            }));
        } else {
            qualified.push((None, element_field));
        }
        Arc::new(
            DFSchema::new_with_metadata(qualified, std::collections::HashMap::new())
                .expect("UnwindNode schema must include the element column"),
        )
    }

    /// Recover the appended element field from the output schema (last column),
    /// for reconstructing the node in [`with_exprs_and_inputs`].
    fn element_field(&self) -> Arc<Field> {
        Arc::clone(&self.element_field)
    }

    fn bound_element_field(&self, list_expr: &Expr, input: &LogicalPlan) -> Arc<Field> {
        match list_expr.get_type(input.schema().as_ref()) {
            Ok(
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _),
            ) => Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(&self.alias)
                    .with_nullable(true),
            ),
            _ => self.element_field(),
        }
    }
}

impl_partial_ord!(UnwindNode);

impl UserDefinedLogicalNodeCore for UnwindNode {
    fn name(&self) -> &str {
        "Unwind"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.list_expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Unwind: alias={}", self.alias)
    }

    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        let list_expr = exprs.pop().unwrap_or_else(|| self.list_expr.clone());
        let input = Arc::new(
            inputs
                .into_iter()
                .next()
                .unwrap_or_else(|| (*self.input).clone()),
        );
        let element_field = self.bound_element_field(&list_expr, &input);
        Ok(Self::new(
            input,
            list_expr,
            self.alias.clone(),
            &element_field,
        ))
    }
}

// ---------------------------------------------------------------------------
// Prevent-predicate-push-down override for all stubs
// ---------------------------------------------------------------------------

// All stubs inherit the default (block all push-down) which is safe.

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{Extension, LogicalPlan};

    fn empty_plan() -> Arc<LogicalPlan> {
        use datafusion::logical_expr::LogicalPlanBuilder;
        Arc::new(LogicalPlanBuilder::empty(false).build().unwrap())
    }

    fn wrap(node: impl UserDefinedLogicalNodeCore) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })
    }

    /// Two representative destination-node fields (mirrors the storage layer's
    /// `TOPOLOGY_NODES_SCHEMA`, kept minimal for tests).
    fn dst_node_fields() -> Vec<Arc<Field>> {
        vec![
            Arc::new(Field::new("node_id", DataType::UInt64, false)),
            Arc::new(Field::new("type_id", DataType::UInt32, false)),
        ]
    }

    /// Build a `VarLenExpandNode` with test defaults (src=0, dst=1, edge=2, Out).
    fn var_len(rel: &str, min_hops: u16, max_hops: Option<u16>) -> VarLenExpandNode {
        VarLenExpandNode::new(
            empty_plan(),
            rel,
            min_hops,
            max_hops,
            0,
            1,
            2,
            Direction::Out,
            Some(7),
            PathBuf::from("/tmp/gf"),
            OntologyMode::Strict,
            dst_node_fields(),
            var_len_edge_list_field(&[]),
        )
    }

    /// A `LogicalPlan` scan over `fields` qualified `var_<var>`, for tests that
    /// need an optional sub-plan with real columns.
    fn scan_plan(var: u32, fields: Vec<Field>) -> Arc<LogicalPlan> {
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::logical_expr::logical_plan::LogicalTableSource;
        let schema = Arc::new(Schema::new(fields));
        Arc::new(
            LogicalPlanBuilder::scan(
                format!("var_{var}"),
                Arc::new(LogicalTableSource::new(schema)),
                None,
            )
            .unwrap()
            .build()
            .unwrap(),
        )
    }

    /// Build an `OptionalMatchNode` over an empty outer and the given optional
    /// sub-plan, keeping the inner columns named by `inner_keep_idx`, for tests.
    fn opt_match(
        optional: Arc<LogicalPlan>,
        join_keys: Vec<(usize, usize)>,
        inner_keep_idx: Vec<usize>,
    ) -> OptionalMatchNode {
        OptionalMatchNode::new(empty_plan(), optional, join_keys, inner_keep_idx)
    }

    /// Build an `UnwindNode` with the given list expr + alias, defaulting the
    /// element column to a nullable Int64 named `elem`.
    fn unwind(list_expr: datafusion::logical_expr::Expr, alias: &str) -> UnwindNode {
        let element_field = Field::new("elem", DataType::Int64, true);
        UnwindNode::new(empty_plan(), list_expr, alias, &element_field)
    }

    #[test]
    fn var_len_expand_name() {
        let n = var_len("KNOWS", 1, Some(3));
        assert_eq!(UserDefinedLogicalNodeCore::name(&n), "VarLenExpand");
        assert_eq!(UserDefinedLogicalNodeCore::inputs(&n).len(), 1);
    }

    #[test]
    fn var_len_expand_schema_extends_input_with_dst_and_edge_fields() {
        use datafusion::common::TableReference;
        // empty input has 0 fields; output = 0 + 2 dst fields + 1 edge-list col.
        let n = var_len("KNOWS", 1, Some(2));
        let schema = UserDefinedLogicalNodeCore::schema(&n);
        assert_eq!(schema.fields().len(), 3);
        // Destination columns are qualified `var_1` and resolvable by name.
        let dst = TableReference::bare("var_1");
        assert!(schema.field_with_qualified_name(&dst, "node_id").is_ok());
        assert!(schema.field_with_qualified_name(&dst, "type_id").is_ok());
        // The edge-list column is qualified `var_2` (edge_var) and is a List.
        let edge = TableReference::bare("var_2");
        let edge_field = schema
            .field_with_qualified_name(&edge, VAR_LEN_EDGE_LIST_FIELD)
            .expect("edge-list column present");
        assert!(matches!(edge_field.data_type(), DataType::List(_)));
    }

    #[test]
    fn optional_match_name() {
        let n = opt_match(empty_plan(), vec![], vec![]);
        assert_eq!(UserDefinedLogicalNodeCore::name(&n), "OptionalMatch");
        assert_eq!(UserDefinedLogicalNodeCore::inputs(&n).len(), 2);
    }

    #[test]
    fn optional_match_schema_appends_nullable_inner_fields() {
        // empty outer (0 fields) + an optional sub-plan with two `var_1` columns,
        // both kept, no join keys → 2 nullable output cols.
        let optional = scan_plan(
            1,
            vec![
                Field::new("node_id", DataType::UInt64, false),
                Field::new("type_id", DataType::UInt32, false),
            ],
        );
        let n = opt_match(optional, vec![], vec![0, 1]);
        let schema = UserDefinedLogicalNodeCore::schema(&n);
        assert_eq!(schema.fields().len(), 2);
        // Inner columns are made nullable for null-shaping.
        assert!(schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        let var1 = TableReference::bare("var_1");
        assert!(schema.field_with_qualified_name(&var1, "node_id").is_ok());
    }

    #[test]
    fn optional_match_schema_excludes_shared_var_columns() {
        // Regression (#718): a shared variable's columns are carried by the outer
        // side, so `inner_keep_idx` excludes them — only the genuinely new inner
        // column (here index 2) is appended, avoiding duplicate `var_0` fields.
        let optional = scan_plan(
            7,
            vec![
                Field::new("node_id", DataType::UInt64, false),
                Field::new("type_id", DataType::UInt32, false),
                Field::new("payload", DataType::UInt64, false),
            ],
        );
        let n = opt_match(optional, vec![(0, 0)], vec![2]);
        let schema = UserDefinedLogicalNodeCore::schema(&n);
        assert_eq!(schema.fields().len(), 1, "only the non-shared col is kept");
        assert_eq!(schema.field(0).name(), "payload");
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn path_unique_name() {
        let n = PathUniqueNode::new(empty_plan());
        assert_eq!(UserDefinedLogicalNodeCore::name(&n), "PathUnique");
        assert_eq!(UserDefinedLogicalNodeCore::inputs(&n).len(), 1);
    }

    #[test]
    fn ontology_infer_name() {
        let n = OntologyInferNode::new(
            empty_plan(),
            "MANAGES",
            "transitive:MANAGES",
            "conservative_min",
        );
        assert_eq!(UserDefinedLogicalNodeCore::name(&n), "OntologyInfer");
        assert_eq!(UserDefinedLogicalNodeCore::inputs(&n).len(), 1);
    }

    #[test]
    fn graph_merge_name() {
        let n = GraphMergeNode::new();
        assert_eq!(UserDefinedLogicalNodeCore::name(&n), "GraphMerge");
        assert_eq!(UserDefinedLogicalNodeCore::inputs(&n).len(), 0);
    }

    #[test]
    fn unwind_name() {
        use datafusion::logical_expr::lit;
        let n = unwind(lit(1i64), "x");
        assert_eq!(UserDefinedLogicalNodeCore::name(&n), "Unwind");
        assert_eq!(UserDefinedLogicalNodeCore::inputs(&n).len(), 1);
        assert_eq!(UserDefinedLogicalNodeCore::expressions(&n).len(), 1);
    }

    #[test]
    fn unwind_schema_appends_element_column() {
        use datafusion::logical_expr::lit;
        // empty input (0 fields) + 1 element column → 1 nullable col named `x`.
        let n = unwind(lit(1i64), "x");
        let schema = UserDefinedLogicalNodeCore::schema(&n);
        assert_eq!(schema.fields().len(), 1);
        assert!(
            schema.field(0).is_nullable(),
            "unwound element column is nullable"
        );
        // The element column is named after the alias and UNQUALIFIED, so a bare
        // `RETURN x` (lowered to `col("x")`) resolves it.
        assert_eq!(schema.field(0).name(), "x");
        assert!(schema.field_with_unqualified_name("x").is_ok());
    }

    #[test]
    fn graph_create_name_and_wrap() {
        let node = GraphCreateNode::new(
            empty_plan(),
            vec![ResolvedNodeSpec {
                var: 0,
                label_ids: vec![3],
                label_names: vec!["Person".to_owned()],
                properties: vec![("name".to_owned(), IrLiteral::Str("Alice".to_owned()))],
                computed_properties: vec![],
                is_reference: false,
            }],
            vec![],
            PathBuf::from("/tmp/gf"),
            OntologyMode::Strict,
        );
        assert_eq!(UserDefinedLogicalNodeCore::name(&node), "GraphCreate");
        // Input-driven now (one CREATE per input row).
        assert_eq!(UserDefinedLogicalNodeCore::inputs(&node).len(), 1);
        // Summary schema: nodes_created + edges_created + properties_set + labels_added.
        assert_eq!(UserDefinedLogicalNodeCore::schema(&node).fields().len(), 4);
        // Wraps as an Extension that DataFusion accepts.
        assert!(matches!(wrap(node), LogicalPlan::Extension(_)));
    }

    #[test]
    fn graph_create_eq_and_hash_are_consistent() {
        use std::collections::hash_map::DefaultHasher;

        let mk = || {
            GraphCreateNode::new(
                empty_plan(),
                vec![ResolvedNodeSpec {
                    var: 0,
                    label_ids: vec![],
                    label_names: vec![],
                    properties: vec![("score".to_owned(), IrLiteral::Float(1.5))],
                    computed_properties: vec![],
                    is_reference: false,
                }],
                vec![],
                PathBuf::from("/tmp/gf"),
                OntologyMode::Exploratory,
            )
        };
        let (a, b) = (mk(), mk());
        assert_eq!(a, b);
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }

    fn set_node(value: Expr) -> GraphSetNode {
        GraphSetNode::new(
            empty_plan(),
            vec![SetTarget {
                var: 0,
                is_edge: false,
                prop_name: "age".to_owned(),
                value,
            }],
            HashMap::new(),
            PathBuf::from("/tmp/gf"),
            OntologyMode::Exploratory,
        )
    }

    fn remove_node() -> GraphRemoveNode {
        GraphRemoveNode::new(
            empty_plan(),
            vec![RemoveTarget {
                var: 0,
                is_edge: false,
                prop_name: "age".to_owned(),
            }],
            HashMap::new(),
            PathBuf::from("/tmp/gf"),
            OntologyMode::Exploratory,
        )
    }

    #[test]
    fn graph_set_name_schema_and_expr_surface() {
        use datafusion::logical_expr::lit;
        let n = set_node(lit(42i64));
        assert_eq!(UserDefinedLogicalNodeCore::name(&n), "GraphSet");
        assert_eq!(UserDefinedLogicalNodeCore::inputs(&n).len(), 1);
        // Summary schema: properties_set.
        assert_eq!(UserDefinedLogicalNodeCore::schema(&n).fields().len(), 1);
        // The value expr is surfaced so the optimizer sees its referenced cols.
        assert_eq!(UserDefinedLogicalNodeCore::expressions(&n).len(), 1);
        assert!(matches!(wrap(n), LogicalPlan::Extension(_)));
    }

    #[test]
    fn graph_set_round_trips_value_expr() {
        use datafusion::logical_expr::{col, lit};
        // A runtime value expr (`var_0.age + 1`) must survive the expr round-trip
        // through `with_exprs_and_inputs` (used by the optimizer).
        let value = col("var_0.age") + lit(1i64);
        let n = set_node(value.clone());
        let exprs = UserDefinedLogicalNodeCore::expressions(&n);
        assert_eq!(exprs, vec![value.clone()]);
        let rebuilt = UserDefinedLogicalNodeCore::with_exprs_and_inputs(&n, exprs, vec![]).unwrap();
        assert_eq!(rebuilt.targets[0].value, value);
        assert_eq!(n, rebuilt);
    }

    #[test]
    fn graph_set_eq_distinguishes_value_expr() {
        use datafusion::logical_expr::lit;
        // Two SET nodes differing only in the value expr must NOT compare equal
        // (the value is part of logical identity).
        assert_ne!(set_node(lit(1i64)), set_node(lit(2i64)));
    }

    #[test]
    fn graph_remove_name_schema_and_no_exprs() {
        let n = remove_node();
        assert_eq!(UserDefinedLogicalNodeCore::name(&n), "GraphRemove");
        assert_eq!(UserDefinedLogicalNodeCore::inputs(&n).len(), 1);
        assert_eq!(UserDefinedLogicalNodeCore::schema(&n).fields().len(), 1);
        // REMOVE carries no value expression.
        assert!(UserDefinedLogicalNodeCore::expressions(&n).is_empty());
        assert!(matches!(wrap(n), LogicalPlan::Extension(_)));
    }

    #[test]
    fn graph_remove_eq_and_hash_are_consistent() {
        use std::collections::hash_map::DefaultHasher;
        let (a, b) = (remove_node(), remove_node());
        assert_eq!(a, b);
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }

    #[test]
    fn all_stubs_wrap_as_extension() {
        use datafusion::logical_expr::lit;
        let plans = vec![
            wrap(var_len("*", 1, None)),
            wrap(opt_match(empty_plan(), vec![], vec![])),
            wrap(PathUniqueNode::new(empty_plan())),
            wrap(OntologyInferNode::new(
                empty_plan(),
                "REL",
                "transitive:REL",
                "conservative_min",
            )),
            wrap(GraphMergeNode::new()),
            wrap(unwind(lit(1i64), "x")),
            wrap(set_node(lit(1i64))),
            wrap(remove_node()),
        ];
        for p in &plans {
            assert!(
                matches!(p, LogicalPlan::Extension(_)),
                "expected Extension, got {p:?}"
            );
        }
    }

    #[test]
    fn fmt_for_explain_is_non_empty() {
        use datafusion::logical_expr::lit;

        // Thin Display adapter that calls fmt_for_explain directly.
        struct ExplainWrapper<'a, T: UserDefinedLogicalNodeCore>(&'a T);
        impl<T: UserDefinedLogicalNodeCore> fmt::Display for ExplainWrapper<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt_for_explain(f)
            }
        }
        fn explain<T: UserDefinedLogicalNodeCore>(n: &T) -> String {
            ExplainWrapper(n).to_string()
        }

        assert!(!explain(&var_len("KNOWS", 1, Some(3))).is_empty());
        assert!(!explain(&opt_match(empty_plan(), vec![], vec![])).is_empty());
        assert!(!explain(&PathUniqueNode::new(empty_plan())).is_empty());
        assert!(
            !explain(&OntologyInferNode::new(
                empty_plan(),
                "MANAGES",
                "transitive:MANAGES",
                "conservative_min"
            ))
            .is_empty()
        );
        assert!(!explain(&GraphMergeNode::new()).is_empty());
        assert!(!explain(&unwind(lit(1i64), "x")).is_empty());

        // Spot-check actual explain output content.
        assert!(explain(&var_len("KNOWS", 1, Some(3))).contains("KNOWS"));
        assert!(
            explain(&OntologyInferNode::new(
                empty_plan(),
                "MANAGES",
                "transitive:MANAGES",
                "conservative_min"
            ))
            .contains("MANAGES")
        );
        assert!(explain(&unwind(lit(1i64), "x")).contains("x"));
    }

    // -----------------------------------------------------------------------
    // ExpandNode (#763)
    // -----------------------------------------------------------------------

    fn expand_node(edge_prop_fields: Vec<Arc<Field>>) -> ExpandNode {
        let edge_fields = vec![
            Arc::new(Field::new(
                "edge_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )),
            Arc::new(Field::new("edge_id", DataType::UInt64, false)),
            Arc::new(Field::new("src_id", DataType::UInt64, false)),
            Arc::new(Field::new("dst_id", DataType::UInt64, false)),
        ];
        ExpandNode::new(
            empty_plan(),
            "KNOWS",
            0,
            2,
            1,
            Direction::Out,
            Some(7),
            PathBuf::from("/tmp/p"),
            OntologyMode::Strict,
            edge_fields,
            edge_prop_fields,
            dst_node_fields(),
        )
    }

    #[test]
    fn expand_node_schema_order_qualifiers_and_prop_nullability() {
        use datafusion::common::TableReference;

        // A non-nullable property field on disk must come out NULLABLE
        // (LEFT-join parity with the join path).
        let node = expand_node(vec![Arc::new(Field::new("since", DataType::Int64, false))]);
        let schema = UserDefinedLogicalNodeCore::schema(&node);
        // Order: (empty input) ++ var_1 edge topology ++ var_1 props ++ var_2 dst.
        let names: Vec<String> = schema
            .iter()
            .map(|(q, f)| {
                format!(
                    "{}.{}",
                    q.map(TableReference::table).unwrap_or("?"),
                    f.name()
                )
            })
            .collect();
        assert_eq!(
            names,
            [
                "var_1.edge_uuid",
                "var_1.edge_id",
                "var_1.src_id",
                "var_1.dst_id",
                "var_1.since",
                "var_2.node_id",
                "var_2.type_id",
            ]
        );
        let edge = TableReference::bare("var_1");
        let since = schema.field_with_qualified_name(&edge, "since").unwrap();
        assert!(since.is_nullable(), "prop columns are LEFT-join nullable");
        let uuid = schema
            .field_with_qualified_name(&edge, "edge_uuid")
            .unwrap();
        assert!(!uuid.is_nullable(), "topology columns keep non-null");
        assert_eq!(node.edge_prop_count, 1);
    }

    #[test]
    fn expand_node_with_exprs_and_inputs_round_trips() {
        let node = expand_node(vec![Arc::new(Field::new("since", DataType::Int64, true))]);
        let rebuilt = UserDefinedLogicalNodeCore::with_exprs_and_inputs(
            &node,
            vec![],
            vec![(*empty_plan()).clone()],
        )
        .unwrap();
        assert_eq!(
            UserDefinedLogicalNodeCore::schema(&rebuilt).as_arrow(),
            UserDefinedLogicalNodeCore::schema(&node).as_arrow(),
            "schema survives optimizer-style input replacement"
        );
        assert_eq!(rebuilt.edge_prop_count, 1);
        assert_eq!(rebuilt.rel_type_name, "KNOWS");
    }
}
