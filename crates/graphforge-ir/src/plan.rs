//! [`GraphPlan`] envelope and [`GraphOp`] operator enum.
//!
//! `GraphPlan` is the stable semantic contract between the compiler (binder)
//! and the execution layer.  It wraps a flat sequence of [`GraphOp`] operators
//! together with an [`ExprArena`] holding all referenced expressions, plus
//! version and ontology metadata.

use serde::{Deserialize, Serialize};

use crate::{
    AggExpr, CreatePattern, Direction, ExprArena, ExprId, IrVersion, OntologyVersion,
    ProcedureDefinition, ProcedureYield, ProjectItem, RemovePropItem, SetMapItem, SetPropItem,
    SortOrder, TypeId, VarId,
};

/// Child projection column consumed by pattern-comprehension lowering.
pub const PATTERN_COMPREHENSION_VALUE_ALIAS: &str = "__gf_pattern_comprehension_value";

// ---------------------------------------------------------------------------
// OntologyMode
// ---------------------------------------------------------------------------

/// The active ontology enforcement mode for this query plan.
///
/// Defined in [`graphforge_core`] (shared with the project manifest) and re-exported
/// here so existing `graphforge_ir::OntologyMode` paths keep resolving.
pub use graphforge_core::OntologyMode;

// ---------------------------------------------------------------------------
// SortKey
// ---------------------------------------------------------------------------

/// A single key in a [`GraphOp::Sort`] operator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortKey {
    /// The expression to sort by.
    pub expr: ExprId,
    /// Sort direction.
    pub order: SortOrder,
    /// If `true`, `NULL` values appear before non-`NULL` values.
    pub nulls_first: bool,
}

// ---------------------------------------------------------------------------
// GraphOp
// ---------------------------------------------------------------------------

/// A single operator node in a [`GraphPlan`].
///
/// Plans are represented as a flat `Vec<GraphOp>` (operators in pipeline
/// order).  Recursive structure — e.g. optional matches, unions — is
/// expressed by variants that embed a child [`GraphPlan`].
///
/// This enum is `#[non_exhaustive]` so that adding new operators in a minor
/// version does not constitute a breaking change for downstream crates that
/// match on it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GraphOp {
    /// Scan all nodes, optionally filtered to a single label type.
    ///
    /// `ty: None` scans every node regardless of label.
    NodeScan {
        /// The variable that receives each scanned node.
        var: VarId,
        /// The label type to filter on, or `None` for a full scan.
        ty: Option<TypeId>,
    },

    /// Wildcard edge scan across all relation types.
    ///
    /// Prefer [`TypedEdgeScan`](Self::TypedEdgeScan) when the relation type is
    /// statically known; this variant exists for queries that match any edge.
    EdgeScan {
        /// The variable that receives each scanned edge.
        var: VarId,
        /// Optional relation type filter.
        ty: Option<TypeId>,
    },

    /// Scan `topology/edges/TYPENAME.parquet` for a specific relation type.
    ///
    /// This is the primary edge-scan operator emitted by the binder whenever a
    /// MATCH pattern specifies a concrete relation type.  The relational
    /// lowering layer maps `rel_ty → relation_name` via the ontology (or the
    /// `RuntimeCatalog` in exploratory mode) to determine the Parquet file
    /// path.  In exploratory mode, if `rel_ty` is a `RuntimeTypeId`, the
    /// storage layer routes the scan to `topology/edges/_exploratory.parquet`.
    TypedEdgeScan {
        /// The variable that receives each scanned edge.
        var: VarId,
        /// The concrete relation type to scan.
        rel_ty: TypeId,
    },

    /// Expand from a source node along edges to destination nodes.
    Expand {
        /// Source node variable.
        src: VarId,
        /// Edge variable.
        edge: VarId,
        /// Destination node variable.
        dst: VarId,
        /// Relation type filter (`None` = any relation type).
        rel_ty: Option<TypeId>,
        /// Traversal direction.
        dir: Direction,
        /// Minimum number of hops (1 for a single hop).
        min_hops: u16,
        /// Maximum number of hops (`None` = unbounded).
        max_hops: Option<u16>,
    },

    /// Enforce relationship isomorphism within one path pattern.
    RelationshipUnique {
        /// Newly bound relationship variable.
        edge: VarId,
        /// Relationships bound earlier in the same path pattern.
        prior_edges: Vec<VarId>,
    },

    /// Retain only rows where `predicate` evaluates to `true`.
    Filter {
        /// The filter predicate expression.
        predicate: ExprId,
    },

    /// Project a set of expressions into named output columns.
    Project {
        /// The output columns.
        items: Vec<ProjectItem>,
        /// If `true`, duplicate rows are eliminated.
        distinct: bool,
    },

    /// Group rows and compute aggregates.
    Aggregate {
        /// Expressions to group by.
        group_by: Vec<ExprId>,
        /// Output column name for each group-by key (parallel to `group_by`):
        /// `Some(name)` aliases that key's result column (openCypher names it by
        /// the RETURN item's source text), `None` leaves the lowered expr's
        /// default name. Empty or shorter-than-`group_by` ⇒ no aliasing (#599).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_aliases: Vec<Option<String>>,
        /// Synthetic output variable for each group-by key (parallel to
        /// `group_by`), bound to that key's result column so a following `Project`
        /// can reference it when a RETURN item nests an aggregate (`n.x +
        /// count(*)`). Empty unless the aggregate was decomposed (#599).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_vars: Vec<Option<VarId>>,
        /// Aggregate expressions.
        aggs: Vec<AggExpr>,
    },

    /// Sort the row stream by one or more keys.
    Sort {
        /// Ordered list of sort keys.
        keys: Vec<SortKey>,
    },

    /// Retain at most `count` rows.
    Limit {
        /// Maximum number of rows to return.
        count: u64,
    },

    /// Retain at most the row count supplied by a query parameter.
    LimitParam {
        /// Parameter name without the leading `$`.
        name: String,
    },

    /// Retain at most the row count produced by a variable-independent expression.
    LimitExpr {
        /// Expression evaluated once at query execution time.
        expr: ExprId,
    },

    /// Skip the first `count` rows.
    Skip {
        /// Number of rows to skip.
        count: u64,
    },

    /// Skip the row count supplied by a query parameter.
    SkipParam {
        /// Parameter name without the leading `$`.
        name: String,
    },

    /// Skip the row count produced by a variable-independent expression.
    SkipExpr {
        /// Expression evaluated once at query execution time.
        expr: ExprId,
    },

    /// Execute a child plan and emit its results, substituting `NULL` for
    /// unmatched rows (LEFT OPTIONAL semantics).
    Optional {
        /// The optional sub-plan.
        child: Box<GraphPlan>,
    },

    /// Retain or reject rows according to whether a correlated child plan has
    /// at least one match (pattern-predicate semantics).
    Exists {
        /// The correlated sub-plan.
        child: Box<GraphPlan>,
        /// `true` for `NOT` existential predicates.
        negated: bool,
    },

    /// Evaluate a correlated pattern and collect one projected value per match.
    PatternComprehension {
        /// The correlated match, optional filter, and terminal value projection.
        child: Box<GraphPlan>,
        /// Synthetic variable receiving the collected list in the outer plan.
        output: VarId,
    },

    /// Evaluate a pattern comprehension once per graph-valued list element.
    ListElementPatternComprehension {
        /// Source list whose elements bind `loop_var` in lexical order.
        list_expr: ExprId,
        /// Lexical list-comprehension element variable.
        loop_var: VarId,
        /// Correlated pattern-comprehension child plan.
        child: Box<GraphPlan>,
        /// Synthetic variable receiving the child collection per element.
        pattern_output: VarId,
        /// Optional list-comprehension filter.
        filter: Option<ExprId>,
        /// Optional list-comprehension projection.
        projection: Option<ExprId>,
        /// Synthetic variable receiving the reassembled outer list.
        output: VarId,
    },

    /// Combine results from multiple sub-plans.
    Union {
        /// If `false`, duplicates are eliminated (UNION).
        /// If `true`, all rows are kept (UNION ALL).
        all: bool,
        /// The sub-plans whose results are combined.
        inputs: Vec<GraphPlan>,
    },

    /// Iterate over a list expression, binding each element to `alias`.
    Unwind {
        /// The expression producing the list to iterate.
        list_expr: ExprId,
        /// The variable bound to each list element.
        alias: VarId,
    },

    /// Invoke a registered procedure for every input row.
    Call {
        /// Frozen signature and deterministic fixture rows.
        procedure: ProcedureDefinition,
        /// Explicit expressions or implicit parameters, in signature order.
        args: Vec<ExprId>,
        /// Procedure outputs introduced into query scope.
        yields: Vec<ProcedureYield>,
    },

    /// Create nodes and/or edges described by `pattern`.
    ///
    /// Write-path execution semantics are deferred to Milestone 13; this
    /// variant serialises and deserialises but is not covered by execution
    /// tests yet.
    Create {
        /// The pattern to create.
        pattern: CreatePattern,
    },

    /// Match-or-create semantics for `pattern`.
    ///
    /// Same execution-deferral note as [`Create`](Self::Create).
    Merge {
        /// The pattern to match or create.
        pattern: CreatePattern,
        /// Actions applied only when the pattern is created.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        on_create: Vec<crate::MergeSetItem>,
        /// Actions applied only when the pattern already exists.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        on_match: Vec<crate::MergeSetItem>,
    },

    /// Delete graph entities referenced directly or produced by expressions.
    ///
    /// Direct variables use identity columns; runtime expressions may produce
    /// nodes, relationships, paths, or null through list/map access. When
    /// `detach` is `false`, deleting a node that still has relationships is an
    /// execution error; `detach = true` also removes incident edges.
    Delete {
        /// The bound variables to delete (nodes and/or edges).
        vars: Vec<VarId>,
        /// Runtime expressions yielding nodes, relationships, or paths.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        exprs: Vec<ExprId>,
        /// `true` for `DETACH DELETE` — remove incident edges with the node.
        detach: bool,
    },

    /// Set properties on bound graph entities (#791).
    ///
    /// Each item assigns one property of a bound node or edge variable to a
    /// value expression evaluated per matched row. Whether a target is a node
    /// or an edge is resolved at lowering from the input row's identity columns
    /// (`node_uuid` vs `edge_uuid`). Label and bulk-map writes execute through
    /// the statement driver.
    Set {
        /// The property assignments, in clause order.
        items: Vec<SetPropItem>,
        /// Bulk map merge/replacement assignments, in clause order.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        map_items: Vec<SetMapItem>,
        /// Node-label additions, in clause order.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        label_items: Vec<crate::LabelItem>,
    },

    /// Remove properties from bound graph entities (#791).
    ///
    /// The dual of [`Set`](Self::Set). Removing an absent property or label is
    /// a no-op (openCypher).
    Remove {
        /// The properties to remove, in clause order.
        items: Vec<RemovePropItem>,
        /// Node-label removals, in clause order.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        label_items: Vec<crate::LabelItem>,
    },

    /// Project a set of columns and optionally apply a WHERE predicate, then
    /// pass results to subsequent operators (WITH clause semantics).
    With {
        /// The output columns.
        items: Vec<ProjectItem>,
        /// If `true`, duplicate projected rows are eliminated.
        #[serde(default, skip_serializing_if = "is_false")]
        distinct: bool,
        /// Optional WHERE predicate applied after projection.
        where_predicate: Option<ExprId>,
    },
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(value: &bool) -> bool {
    !*value
}

// ---------------------------------------------------------------------------
// GraphPlan
// ---------------------------------------------------------------------------

/// A versioned, serialisable graph query plan.
///
/// `GraphPlan` is the stable contract between the binder and the execution
/// engine.  It carries the full operator pipeline, all referenced expressions
/// in a flat [`ExprArena`], and the ontology metadata needed by the relational
/// lowering layer.
///
/// # Construction
///
/// Use [`GraphPlan::builder`] rather than constructing this directly:
///
/// ```
/// use graphforge_ir::{GraphPlan, GraphOp, VarId};
///
/// let plan = GraphPlan::builder("openCypher")
///     .push_op(GraphOp::NodeScan { var: VarId(0), ty: None })
///     .build();
///
/// assert_eq!(plan.ops.len(), 1);
/// assert_eq!(plan.dialect, "openCypher");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphPlan {
    /// IR format version for forward-compatibility checks.
    pub ir_version: IrVersion,
    /// Query dialect (e.g. `"openCypher"`).
    pub dialect: String,
    /// Ontology version used when the plan was compiled.
    ///
    /// `None` in `Exploratory` mode (no formal ontology present).
    pub ontology_version: Option<OntologyVersion>,
    /// Active ontology enforcement mode.
    pub ontology_mode: OntologyMode,
    /// Optional feature flags that affect plan behaviour.
    pub feature_flags: Vec<String>,
    /// The operator pipeline (in execution order).
    pub ops: Vec<GraphOp>,
    /// All expressions referenced by operators in this plan.
    pub exprs: ExprArena,
}

impl GraphPlan {
    /// Create a [`GraphPlanBuilder`] for constructing a new plan.
    ///
    /// `dialect` identifies the query language (e.g. `"openCypher"`).
    /// The builder defaults to `ir_version = IrVersion::CURRENT` and
    /// `ontology_mode = OntologyMode::Exploratory`.
    #[must_use]
    pub fn builder(dialect: impl Into<String>) -> GraphPlanBuilder {
        GraphPlanBuilder {
            ir_version: IrVersion::CURRENT,
            dialect: dialect.into(),
            ontology_version: None,
            ontology_mode: OntologyMode::default(),
            feature_flags: Vec::new(),
            ops: Vec::new(),
            exprs: ExprArena::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// GraphPlanBuilder
// ---------------------------------------------------------------------------

/// Builder for [`GraphPlan`].
///
/// Obtain via [`GraphPlan::builder`].
#[derive(Debug)]
pub struct GraphPlanBuilder {
    ir_version: IrVersion,
    dialect: String,
    ontology_version: Option<OntologyVersion>,
    ontology_mode: OntologyMode,
    feature_flags: Vec<String>,
    ops: Vec<GraphOp>,
    exprs: ExprArena,
}

impl GraphPlanBuilder {
    /// Override the IR version (default: [`IrVersion::CURRENT`]).
    #[must_use]
    pub fn ir_version(mut self, v: IrVersion) -> Self {
        self.ir_version = v;
        self
    }

    /// Set the ontology version.
    #[must_use]
    pub fn ontology_version(mut self, v: impl Into<OntologyVersion>) -> Self {
        self.ontology_version = Some(v.into());
        self
    }

    /// Set the ontology mode (default: [`OntologyMode::Exploratory`]).
    #[must_use]
    pub fn ontology_mode(mut self, m: OntologyMode) -> Self {
        self.ontology_mode = m;
        self
    }

    /// Add a feature flag string.
    #[must_use]
    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    /// Append an operator to the pipeline (owned chaining form).
    #[must_use]
    pub fn push_op(mut self, op: GraphOp) -> Self {
        self.ops.push(op);
        self
    }

    /// Append an operator to the pipeline (mutable-reference form).
    ///
    /// Prefer this inside loops or helper functions where owning `self` is
    /// inconvenient.
    pub fn push_op_mut(&mut self, op: GraphOp) {
        self.ops.push(op);
    }

    /// Push an expression into the arena and return its [`ExprId`].
    pub fn push_expr(&mut self, expr: crate::IrExpr) -> ExprId {
        self.exprs.push(expr)
    }

    /// Consume the builder and produce a [`GraphPlan`].
    #[must_use]
    pub fn build(self) -> GraphPlan {
        GraphPlan {
            ir_version: self.ir_version,
            dialect: self.dialect,
            ontology_version: self.ontology_version,
            ontology_mode: self.ontology_mode,
            feature_flags: self.feature_flags,
            ops: self.ops,
            exprs: self.exprs,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggFunc, BinaryOpKind, ExprArena, IrExpr, IrLiteral, PropId};

    fn make_type_id(n: u32) -> TypeId {
        TypeId(n)
    }

    // NodeScan → TypedEdgeScan → Filter → Project
    #[test]
    fn plan_node_typed_edge_filter_project_roundtrip() {
        let mut exprs = ExprArena::new();
        let var_n = VarId(0);
        let var_e = VarId(1);
        let var_r = exprs.push(IrExpr::VarRef(VarId(0)));
        let lit = exprs.push(IrExpr::Literal(IrLiteral::Bool(true)));
        let pred = exprs.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left: var_r,
            right: lit,
        });

        let plan = GraphPlan {
            ir_version: IrVersion::CURRENT,
            dialect: "openCypher".into(),
            ontology_version: Some(OntologyVersion::from("v1")),
            ontology_mode: OntologyMode::Advisory,
            feature_flags: vec![],
            ops: vec![
                GraphOp::NodeScan {
                    var: var_n,
                    ty: Some(make_type_id(1)),
                },
                GraphOp::TypedEdgeScan {
                    var: var_e,
                    rel_ty: make_type_id(10),
                },
                GraphOp::Filter { predicate: pred },
                GraphOp::Project {
                    items: vec![ProjectItem {
                        expr: var_r,
                        alias: Some("n".into()),
                        out_var: None,
                    }],
                    distinct: false,
                },
            ],
            exprs,
        };

        let json = serde_json::to_string(&plan).unwrap();
        let restored: GraphPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(plan, restored);
    }

    // NodeScan → Expand → Filter → Project (architecture docs example)
    #[test]
    fn plan_node_expand_filter_project_roundtrip() {
        let mut exprs = ExprArena::new();
        let var_a = VarId(0);
        let var_e = VarId(1);
        let var_b = VarId(2);
        let ref_b = exprs.push(IrExpr::VarRef(var_b));
        let prop = exprs.push(IrExpr::PropertyAccess {
            base: ref_b,
            prop: PropId(5),
        });
        let lit = exprs.push(IrExpr::Literal(IrLiteral::Str("Bob".into())));
        let pred = exprs.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left: prop,
            right: lit,
        });

        let plan = GraphPlan {
            ir_version: IrVersion::CURRENT,
            dialect: "openCypher".into(),
            ontology_version: None,
            ontology_mode: OntologyMode::Exploratory,
            feature_flags: vec![],
            ops: vec![
                GraphOp::NodeScan {
                    var: var_a,
                    ty: None,
                },
                GraphOp::Expand {
                    src: var_a,
                    edge: var_e,
                    dst: var_b,
                    rel_ty: None,
                    dir: Direction::Out,
                    min_hops: 1,
                    max_hops: Some(1),
                },
                GraphOp::Filter { predicate: pred },
                GraphOp::Project {
                    items: vec![ProjectItem {
                        expr: ref_b,
                        alias: Some("b".into()),
                        out_var: None,
                    }],
                    distinct: false,
                },
            ],
            exprs,
        };

        let json = serde_json::to_string(&plan).unwrap();
        let restored: GraphPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(plan, restored);
    }

    #[test]
    fn typed_edge_scan_roundtrip() {
        let op = GraphOp::TypedEdgeScan {
            var: VarId(3),
            rel_ty: make_type_id(42),
        };
        let json = serde_json::to_string(&op).unwrap();
        let back: GraphOp = serde_json::from_str(&json).unwrap();
        assert_eq!(op, back);
    }

    #[test]
    fn exploratory_plan_no_ontology_version_roundtrip() {
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .build();

        assert!(plan.ontology_version.is_none());
        assert_eq!(plan.ontology_mode, OntologyMode::Exploratory);

        let json = serde_json::to_string(&plan).unwrap();
        let restored: GraphPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(plan, restored);
    }

    #[test]
    fn ontology_mode_serialises_as_lowercase() {
        assert_eq!(
            serde_json::to_string(&OntologyMode::Exploratory).unwrap(),
            "\"exploratory\""
        );
        assert_eq!(
            serde_json::to_string(&OntologyMode::Advisory).unwrap(),
            "\"advisory\""
        );
        assert_eq!(
            serde_json::to_string(&OntologyMode::Strict).unwrap(),
            "\"strict\""
        );

        // round-trip all three
        for mode in [
            OntologyMode::Exploratory,
            OntologyMode::Advisory,
            OntologyMode::Strict,
        ] {
            let json = serde_json::to_string(&mode).unwrap();
            let back: OntologyMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, back);
        }
    }

    #[test]
    fn builder_produces_correct_plan() {
        let plan = GraphPlan::builder("openCypher")
            .ontology_mode(OntologyMode::Strict)
            .ontology_version("sha256:abc")
            .feature_flag("experimental_join")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: Some(make_type_id(1)),
            })
            .push_op(GraphOp::Limit { count: 100 })
            .build();

        assert_eq!(plan.dialect, "openCypher");
        assert_eq!(plan.ir_version, IrVersion::CURRENT);
        assert_eq!(plan.ontology_mode, OntologyMode::Strict);
        assert_eq!(
            plan.ontology_version,
            Some(OntologyVersion::from("sha256:abc"))
        );
        assert_eq!(plan.feature_flags, vec!["experimental_join"]);
        assert_eq!(plan.ops.len(), 2);
    }

    #[test]
    fn sort_key_roundtrip() {
        let key = SortKey {
            expr: ExprId(0),
            order: SortOrder::Desc,
            nulls_first: true,
        };
        let json = serde_json::to_string(&key).unwrap();
        let back: SortKey = serde_json::from_str(&json).unwrap();
        assert_eq!(key, back);
    }

    #[test]
    fn all_graphop_variants_roundtrip() {
        let mut exprs = ExprArena::new();
        let e0 = exprs.push(IrExpr::Literal(IrLiteral::Null));
        let v0 = VarId(0);

        let ops: Vec<GraphOp> = vec![
            GraphOp::NodeScan { var: v0, ty: None },
            GraphOp::EdgeScan {
                var: v0,
                ty: Some(make_type_id(1)),
            },
            GraphOp::TypedEdgeScan {
                var: v0,
                rel_ty: make_type_id(2),
            },
            GraphOp::Expand {
                src: v0,
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: Direction::Undirected,
                min_hops: 1,
                max_hops: None,
            },
            GraphOp::Filter { predicate: e0 },
            GraphOp::Project {
                items: vec![ProjectItem {
                    expr: e0,
                    alias: None,
                    out_var: None,
                }],
                distinct: true,
            },
            GraphOp::Aggregate {
                group_by: vec![e0],
                group_aliases: vec![Some("g".into())],
                group_vars: vec![None],
                aggs: vec![AggExpr {
                    func: AggFunc::Count,
                    arg: None,
                    percentile: None,
                    alias: "cnt".into(),
                    out_var: None,
                }],
            },
            GraphOp::Sort {
                keys: vec![SortKey {
                    expr: e0,
                    order: SortOrder::Asc,
                    nulls_first: false,
                }],
            },
            GraphOp::Limit { count: 10 },
            GraphOp::LimitParam { name: "l".into() },
            GraphOp::LimitExpr { expr: e0 },
            GraphOp::Skip { count: 5 },
            GraphOp::SkipParam { name: "s".into() },
            GraphOp::SkipExpr { expr: e0 },
            GraphOp::Optional {
                child: Box::new(
                    GraphPlan::builder("openCypher")
                        .push_op(GraphOp::NodeScan { var: v0, ty: None })
                        .build(),
                ),
            },
            GraphOp::Exists {
                child: Box::new(
                    GraphPlan::builder("openCypher")
                        .push_op(GraphOp::NodeScan { var: v0, ty: None })
                        .build(),
                ),
                negated: false,
            },
            GraphOp::PatternComprehension {
                child: Box::new(
                    GraphPlan::builder("openCypher")
                        .push_op(GraphOp::NodeScan { var: v0, ty: None })
                        .build(),
                ),
                output: v0,
            },
            GraphOp::ListElementPatternComprehension {
                list_expr: e0,
                loop_var: v0,
                child: Box::new(GraphPlan::builder("openCypher").build()),
                pattern_output: v0,
                filter: None,
                projection: Some(e0),
                output: v0,
            },
            GraphOp::Union {
                all: true,
                inputs: vec![GraphPlan::builder("openCypher").build()],
            },
            GraphOp::Unwind {
                list_expr: e0,
                alias: v0,
            },
            GraphOp::Create {
                pattern: CreatePattern::default(),
            },
            GraphOp::Merge {
                pattern: CreatePattern::default(),
                on_create: vec![],
                on_match: vec![],
            },
            GraphOp::Delete {
                vars: vec![v0],
                exprs: vec![],
                detach: true,
            },
            GraphOp::Set {
                items: vec![SetPropItem {
                    target: v0,
                    prop: PropId(7),
                    prop_name: "age".into(),
                    value: e0,
                }],
                map_items: vec![],
                label_items: vec![],
            },
            GraphOp::Remove {
                items: vec![RemovePropItem {
                    target: v0,
                    prop: PropId(7),
                    prop_name: "age".into(),
                }],
                label_items: vec![],
            },
            GraphOp::With {
                items: vec![ProjectItem {
                    expr: e0,
                    alias: Some("x".into()),
                    out_var: None,
                }],
                distinct: true,
                where_predicate: Some(e0),
            },
        ];

        for op in &ops {
            let json = serde_json::to_string(op).unwrap();
            let back: GraphOp = serde_json::from_str(&json).unwrap();
            assert_eq!(op, &back, "round-trip failed for {op:?}");
        }
    }
}
