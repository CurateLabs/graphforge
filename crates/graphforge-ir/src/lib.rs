//! GraphForge Graph IR — semantic, graph-native, semver-versioned.
//!
//! The Graph IR is the stable contract between the parser/binder and the
//! execution engine.
//!
//! # Milestone status
//!
//! - phase-11 #565 — Newtype IDs and core IR primitives
//! - phase-11 #566 — Expression arena
//! - phase-11 #567 — GraphOp enum and GraphPlan envelope
//! - phase-11 #568 — Binder (AST → Graph IR) ← **this issue**
//! - phase-11 #681 — RuntimeCatalog
#![forbid(unsafe_code)]

pub mod binder;
pub use binder::{BindError, BindErrorKind, Binder};

pub mod catalog;
pub use catalog::{
    RUNTIME_ENTITY_TYPE_TAG, RUNTIME_RELATION_TYPE_TAG, RuntimeCatalog, RuntimePropId,
    RuntimeTypeId, is_runtime_entity_type_id, runtime_entity_type_id, runtime_relation_type_id,
    runtime_type_id_from_entity_plan_id,
};

pub mod expr;
pub use expr::{BinaryOpKind, CaseArm, ExprArena, IrExpr, IrLiteral, UnaryOpKind};
pub use graphforge_ast::QuantifierKind;

pub mod plan;
pub use plan::{GraphOp, GraphPlan, GraphPlanBuilder, OntologyMode, SortKey};

pub mod procedure;
pub use procedure::{ProcedureDefinition, ProcedureField, ProcedureRegistry, ProcedureYield};

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub use graphforge_core::{GfError, PropId, TypeId};

// ---------------------------------------------------------------------------
// Core newtypes
// ---------------------------------------------------------------------------

/// Identifies a bound variable within a query plan (e.g. the `n` in `(n:Person)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VarId(pub u32);

/// Identifies an expression node within an [`ExprArena`](crate::ExprArena).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExprId(pub u32);

// ---------------------------------------------------------------------------
// IrVersion
// ---------------------------------------------------------------------------

/// Semver-like version for the Graph IR wire format.
///
/// Breaking changes bump `major`; new operators bump `minor`; bug fixes bump
/// `patch`.  Serialised as `"major.minor.patch"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IrVersion {
    /// Major version — incremented on breaking IR changes.
    pub major: u16,
    /// Minor version — incremented when new operators are added.
    pub minor: u16,
    /// Patch version — incremented for bug fixes.
    pub patch: u16,
}

impl IrVersion {
    /// The current IR version shipped with this build.
    pub const CURRENT: Self = Self {
        major: 0,
        minor: 3,
        patch: 0,
    };
}

impl fmt::Display for IrVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl From<&str> for IrVersion {
    /// Parse `"major.minor.patch"`.  Returns `0.0.0` on any parse failure.
    fn from(s: &str) -> Self {
        let parts: Vec<&str> = s.splitn(3, '.').collect();
        let parse = |p: &&str| p.parse::<u16>().unwrap_or(0);
        Self {
            major: parts.first().map_or(0, parse),
            minor: parts.get(1).map_or(0, parse),
            patch: parts.get(2).map_or(0, parse),
        }
    }
}

impl FromStr for IrVersion {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

// ---------------------------------------------------------------------------
// OntologyVersion
// ---------------------------------------------------------------------------

/// Wraps an ontology checksum or semver string for embedding in a `GraphPlan`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OntologyVersion(pub String);

impl fmt::Display for OntologyVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<S: Into<String>> From<S> for OntologyVersion {
    fn from(s: S) -> Self {
        Self(s.into())
    }
}

// ---------------------------------------------------------------------------
// Direction and SortOrder
// ---------------------------------------------------------------------------

/// Edge traversal direction in a pattern or expand operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Direction {
    /// Outgoing edge: `(a)-[:R]->(b)`.
    Out,
    /// Incoming edge: `(a)<-[:R]-(b)`.
    In,
    /// Either direction: `(a)-[:R]-(b)`.
    Undirected,
}

/// Sort direction for ORDER BY keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SortOrder {
    /// Ascending (smallest first).
    Asc,
    /// Descending (largest first).
    Desc,
}

// ---------------------------------------------------------------------------
// Projection and aggregation primitives
// ---------------------------------------------------------------------------

/// A single item in a PROJECT or WITH operator: an expression plus an optional alias.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectItem {
    /// The expression to project.
    pub expr: ExprId,
    /// Optional output column name.
    pub alias: Option<String>,
    /// For a `WITH` item only: the variable this item introduces into the
    /// downstream scope (#814). The lowerer maps it to the projected column so a
    /// later clause referencing the alias resolves. `None` for terminal `RETURN`
    /// items, which introduce no scope.
    #[serde(default)]
    pub out_var: Option<VarId>,
}

/// An aggregation expression inside an AGGREGATE operator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggExpr {
    /// The aggregation function.
    pub func: AggFunc,
    /// The argument expression (`None` for `COUNT(*)`).
    pub arg: Option<ExprId>,
    /// The percentile argument for `percentileDisc(expr, percentile)` /
    /// `percentileCont(expr, percentile)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percentile: Option<ExprId>,
    /// Output column name.
    pub alias: String,
    /// When this aggregate is computed as a sub-expression of a larger RETURN
    /// item (`count(*) + 1`), the synthetic variable bound to its output column
    /// so a following `Project` can reference it. `None` for a terminal
    /// top-level aggregate (no `Project` follows). (#599 nested aggregates.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub out_var: Option<VarId>,
}

/// Aggregation function kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggFunc {
    /// `count(expr)` or `count(*)`.
    Count,
    /// `count(DISTINCT expr)`.
    CountDistinct,
    /// `sum(expr)`.
    Sum,
    /// `sum(DISTINCT expr)`.
    SumDistinct,
    /// `avg(expr)`.
    Avg,
    /// `avg(DISTINCT expr)`.
    AvgDistinct,
    /// `min(expr)`.
    Min,
    /// `max(expr)`.
    Max,
    /// `collect(expr)` — gathers values into a list.
    Collect,
    /// `collect(DISTINCT expr)` — gathers distinct non-null values into a list.
    CollectDistinct,
    /// `percentileDisc(expr, percentile)` — discrete percentile.
    PercentileDisc,
    /// `percentileCont(expr, percentile)` — continuous percentile.
    PercentileCont,
}

// ---------------------------------------------------------------------------
// CreatePattern
// ---------------------------------------------------------------------------

/// A node to create, as bound from a `CREATE` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateNodeSpec {
    /// The pattern variable bound to this node.
    pub var: VarId,
    /// The node's complete label set, resolved to [`TypeId`] values.
    pub labels: Vec<TypeId>,
    /// Property map expression (an [`IrExpr::MapLiteral`](crate::IrExpr)), or
    /// `None` when no `{…}` was given.
    pub properties: Option<ExprId>,
    /// `true` when this variable was already bound by a preceding clause
    /// (`MATCH`/`WITH`) — the node is **referenced** per matched row, not minted
    /// (#703). `false` (the default) for a node the `CREATE` introduces.
    #[serde(default)]
    pub is_reference: bool,
}

/// An edge to create, as bound from a `CREATE` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateEdgeSpec {
    /// The pattern variable bound to this edge.
    pub var: VarId,
    /// Source node variable.
    pub src: VarId,
    /// Destination node variable.
    pub dst: VarId,
    /// The relation type, resolved to a [`TypeId`] (or `None` if untyped).
    pub rel_type: Option<TypeId>,
    /// Edge direction.
    pub direction: Direction,
    /// Property map expression, or `None`.
    pub properties: Option<ExprId>,
}

/// The nodes and edges described by a `CREATE` (or `MERGE`) clause.
///
/// Populated by the binder from the AST pattern; consumed by the relational
/// lowering layer, which resolves property expressions and type names before
/// handing a self-contained write spec to the execution layer.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CreatePattern {
    /// Nodes to create, in pattern order.
    #[serde(default)]
    pub nodes: Vec<CreateNodeSpec>,
    /// Edges to create, in pattern order.
    #[serde(default)]
    pub edges: Vec<CreateEdgeSpec>,
}

// ---------------------------------------------------------------------------
// SET / REMOVE
// ---------------------------------------------------------------------------

/// A single `SET n.prop = <expr>` assignment, as bound from a `SET` clause
/// (#791).
///
/// The write **target** is always a bound variable's property; bulk forms
/// (`n += {…}`, `n = {…}`) and label writes (`n:Label`) are rejected at bind.
/// `value` is a full runtime expression (not coerced to a literal) — the
/// lowering layer turns it into a DataFusion expression evaluated per matched
/// row at execution time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetPropItem {
    /// The bound variable whose property is written.
    pub target: VarId,
    /// The property, resolved to a [`PropId`].
    pub prop: PropId,
    /// The property name (carried so the lowering layer needs no `PropId →
    /// name` catalog round-trip — mirrors how [`CreateNodeSpec`] carries
    /// resolved names alongside ids).
    pub prop_name: String,
    /// The value expression, evaluated per matched row.
    pub value: ExprId,
}

/// A bulk map assignment from `SET n += map` or `SET n = map` (#800).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetMapItem {
    /// The bound node or relationship variable whose properties are written.
    pub target: VarId,
    /// The map expression, evaluated per matched row.
    pub map: ExprId,
    /// `true` for replacement (`=`), `false` for merge (`+=`).
    pub replace: bool,
}

/// Labels added to or removed from a bound node variable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabelItem {
    /// Bound node variable.
    pub target: VarId,
    /// Labels resolved through the runtime/ontology catalog.
    pub labels: Vec<TypeId>,
}

/// One branch-specific action attached to a `MERGE` clause (#959).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MergeSetItem {
    /// Assign one property from a runtime expression.
    Property(SetPropItem),
    /// Merge or replace a complete property map.
    Map(SetMapItem),
    /// Add labels to the matched or newly-created node.
    AddLabels {
        /// Bound node variable.
        target: VarId,
        /// Labels resolved through the runtime/ontology catalog.
        labels: Vec<TypeId>,
    },
}

/// A single `REMOVE n.prop` deletion, as bound from a `REMOVE` clause (#791).
///
/// Label removals are represented separately by [`LabelItem`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemovePropItem {
    /// The bound variable whose property is removed.
    pub target: VarId,
    /// The property, resolved to a [`PropId`].
    pub prop: PropId,
    /// The property name (see [`SetPropItem::prop_name`]).
    pub prop_name: String,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn ir_version_display() {
        let v = IrVersion {
            major: 1,
            minor: 2,
            patch: 3,
        };
        assert_eq!(v.to_string(), "1.2.3");
    }

    #[test]
    fn ir_version_from_str() {
        let v = IrVersion::from("2.0.1");
        assert_eq!(v.major, 2);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 1);
    }

    #[test]
    fn ir_version_from_str_partial() {
        let v = IrVersion::from("3.1");
        assert_eq!(v.major, 3);
        assert_eq!(v.minor, 1);
        assert_eq!(v.patch, 0);
    }

    #[test]
    fn ir_version_from_str_invalid_falls_back() {
        let v = IrVersion::from("not.a.version");
        assert_eq!(
            v,
            IrVersion {
                major: 0,
                minor: 0,
                patch: 0
            }
        );
    }

    #[test]
    fn newtype_hash_keys() {
        let mut map: HashMap<VarId, &str> = HashMap::new();
        map.insert(VarId(0), "a");
        map.insert(VarId(1), "b");
        assert_eq!(map[&VarId(0)], "a");

        let mut pmap: HashMap<PropId, u32> = HashMap::new();
        pmap.insert(PropId(5), 42);
        assert_eq!(pmap[&PropId(5)], 42);

        let mut emap: HashMap<ExprId, bool> = HashMap::new();
        emap.insert(ExprId(10), true);
        assert!(emap[&ExprId(10)]);
    }

    #[test]
    fn serde_roundtrip_ir_version() {
        let v = IrVersion {
            major: 1,
            minor: 2,
            patch: 3,
        };
        let json = serde_json::to_string(&v).unwrap();
        let back: IrVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn serde_roundtrip_ontology_version() {
        let v = OntologyVersion::from("abc123checksum");
        let json = serde_json::to_string(&v).unwrap();
        let back: OntologyVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn serde_roundtrip_direction() {
        for d in [Direction::Out, Direction::In, Direction::Undirected] {
            let json = serde_json::to_string(&d).unwrap();
            let back: Direction = serde_json::from_str(&json).unwrap();
            assert_eq!(d, back);
        }
    }

    #[test]
    fn serde_roundtrip_sort_order() {
        for s in [SortOrder::Asc, SortOrder::Desc] {
            let json = serde_json::to_string(&s).unwrap();
            let back: SortOrder = serde_json::from_str(&json).unwrap();
            assert_eq!(s, back);
        }
    }

    #[test]
    fn type_id_reexported() {
        // TypeId must come from graphforge-core, not a local definition
        let id = TypeId(42);
        assert_eq!(id.0, 42);
    }

    #[test]
    fn create_pattern_roundtrip() {
        let p = CreatePattern {
            nodes: vec![CreateNodeSpec {
                var: VarId(0),
                labels: vec![TypeId(3)],
                properties: Some(ExprId(1)),
                is_reference: false,
            }],
            edges: vec![CreateEdgeSpec {
                var: VarId(1),
                src: VarId(0),
                dst: VarId(2),
                rel_type: Some(TypeId(4)),
                direction: Direction::Out,
                properties: None,
            }],
        };
        let json = serde_json::to_string(&p).unwrap();
        let back: CreatePattern = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn create_pattern_default_is_empty() {
        let p = CreatePattern::default();
        assert!(p.nodes.is_empty() && p.edges.is_empty());
    }
}
