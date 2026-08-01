//! Syntax-faithful, span-rich AST for openCypher queries.
//!
//! Produced by `graphforge-cypher`; consumed by `graphforge-ir` (the binder). No other crate
//! should depend on this module directly.
//!
//! Every public type is `Clone + Debug + PartialEq + Serialize + Deserialize`
//! so the differential test harness can round-trip ASTs through JSON and
//! compare parser outputs.
//!
//! Struct fields carry `span: Span` to locate source positions — doc comments
//! on these repetitive span fields are omitted intentionally.
#![allow(missing_docs)]

use graphforge_core::Span;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Top-level query
// ---------------------------------------------------------------------------

/// openCypher dialect version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DialectVersion {
    /// openCypher 9 (the baseline target for GraphForge).
    #[default]
    OpenCypher9,
}

/// A fully parsed Cypher query.
///
/// At the skeleton stage `clauses` may be empty; the real parser populates it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AstQuery {
    /// Cypher dialect in use.
    pub dialect: DialectVersion,
    /// Top-level clauses in source order.
    pub clauses: Vec<AstClause>,
    /// Span covering the full query source.
    pub span: Span,
}

// ---------------------------------------------------------------------------
// Clause variants
// ---------------------------------------------------------------------------

/// A top-level clause in a Cypher query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AstClause {
    Match(MatchClause),
    OptionalMatch(MatchClause),
    Where(WhereClause),
    With(WithClause),
    Return(ReturnClause),
    Create(CreateClause),
    Merge(MergeClause),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
    Unwind(UnwindClause),
    Call(CallClause),
    Union(UnionClause),
}

impl AstClause {
    /// Span of the clause keyword plus its body.
    #[must_use]
    pub fn span(&self) -> Span {
        match self {
            Self::Match(c) | Self::OptionalMatch(c) => c.span,
            Self::Where(c) => c.span,
            Self::With(c) => c.span,
            Self::Return(c) => c.span,
            Self::Create(c) => c.span,
            Self::Merge(c) => c.span,
            Self::Set(c) => c.span,
            Self::Remove(c) => c.span,
            Self::Delete(c) => c.span,
            Self::Unwind(c) => c.span,
            Self::Call(c) => c.span,
            Self::Union(c) => c.span,
        }
    }
}

// ---------------------------------------------------------------------------
// MATCH
// ---------------------------------------------------------------------------

/// A `MATCH` or `OPTIONAL MATCH` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchClause {
    /// The path patterns being matched.
    pub patterns: Vec<PathPattern>,
    /// Optional inline `WHERE` predicate.
    pub where_clause: Option<WhereClause>,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// WHERE
// ---------------------------------------------------------------------------

/// A `WHERE` clause (also used inline in MATCH/WITH).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhereClause {
    pub predicate: Expr,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// WITH
// ---------------------------------------------------------------------------

/// A `WITH` clause (pipeline stage with optional ORDER/SKIP/LIMIT/WHERE).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WithClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Option<OrderByClause>,
    pub skip: Option<Expr>,
    pub limit: Option<Expr>,
    pub where_clause: Option<WhereClause>,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// RETURN
// ---------------------------------------------------------------------------

/// A `RETURN` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Option<OrderByClause>,
    pub skip: Option<Expr>,
    pub limit: Option<Expr>,
    pub span: Span,
}

/// A single item in a `RETURN` or `WITH` projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReturnItem {
    pub expr: Expr,
    /// Optional `AS alias` name.
    pub alias: Option<String>,
    /// The verbatim source text of `expr` (no `AS` part), captured by the parser
    /// for an un-aliased item. openCypher names an un-aliased projection column by
    /// the expression as written (`n.prop`, `count(*)`, `a.x IS NULL`); the binder
    /// uses this as the default `RETURN` column name when there is no `alias`.
    /// `None` for `RETURN *` and for items built outside the parser.
    #[serde(default)]
    pub display: Option<String>,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

/// An `ORDER BY` sub-clause carrying one or more sort keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByClause {
    pub items: Vec<SortItem>,
    pub span: Span,
}

/// A single sort key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortItem {
    pub expr: Expr,
    pub order: SortOrder,
    pub span: Span,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SortOrder {
    #[default]
    Ascending,
    Descending,
}

// ---------------------------------------------------------------------------
// CREATE / MERGE
// ---------------------------------------------------------------------------

/// A `CREATE` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateClause {
    pub patterns: Vec<PathPattern>,
    pub span: Span,
}

/// A `MERGE` clause (with optional ON CREATE / ON MATCH SET actions).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeClause {
    pub pattern: PathPattern,
    pub on_create: Vec<SetItem>,
    pub on_match: Vec<SetItem>,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// SET / REMOVE / DELETE
// ---------------------------------------------------------------------------

/// A `SET` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetClause {
    pub items: Vec<SetItem>,
    pub span: Span,
}

/// A single item inside a SET clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SetItem {
    /// `n.prop = expr`
    Property {
        target: PropertyAccess,
        value: Expr,
        span: Span,
    },
    /// `n += {map}` (property merge)
    PropertyMerge { var: String, map: Expr, span: Span },
    /// `n = {map}` (property replace)
    PropertyReplace { var: String, map: Expr, span: Span },
    /// `n:Label` (add label)
    Label {
        var: String,
        labels: Vec<String>,
        span: Span,
    },
}

/// A `REMOVE` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoveClause {
    pub items: Vec<RemoveItem>,
    pub span: Span,
}

/// A single item inside a `REMOVE` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum RemoveItem {
    /// `n.prop`
    Property(PropertyAccess, Span),
    /// `n:Label`
    Label {
        var: String,
        labels: Vec<String>,
        span: Span,
    },
}

/// A `DELETE` or `DETACH DELETE` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteClause {
    pub detach: bool,
    pub exprs: Vec<Expr>,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// UNWIND
// ---------------------------------------------------------------------------

/// An `UNWIND` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnwindClause {
    pub expr: Expr,
    pub alias: String,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// CALL
// ---------------------------------------------------------------------------

/// A `CALL procedure YIELD ...` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallClause {
    pub procedure: Vec<String>,
    pub args: Vec<Expr>,
    /// Whether the call included an explicit parenthesized argument list.
    #[serde(default)]
    pub args_explicit: bool,
    pub yield_items: Vec<ReturnItem>,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// UNION
// ---------------------------------------------------------------------------

/// A `UNION` or `UNION ALL` clause joining two query halves.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnionClause {
    pub all: bool,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// Path patterns
// ---------------------------------------------------------------------------

/// A path pattern: one or more alternating node/relationship patterns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathPattern {
    /// Optional `variable = ...` path binding.
    pub var: Option<String>,
    /// Elements: must start and end with a `NodePattern`, with
    /// `RelPattern` in between.
    pub elements: Vec<PathElement>,
    pub span: Span,
}

/// One element in a path pattern.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PathElement {
    Node(NodePattern),
    Rel(RelPattern),
}

/// A node pattern: `(var:Label {props})`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodePattern {
    /// Optional variable binding.
    pub var: Option<String>,
    /// Label constraints.
    pub labels: Vec<String>,
    /// Property constraints (map literal).
    pub properties: Option<Expr>,
    pub span: Span,
}

/// A relationship pattern: `-[var:TYPE*min..max {props}]->`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelPattern {
    pub var: Option<String>,
    /// Relationship type constraints.
    pub types: Vec<String>,
    pub direction: Direction,
    /// Minimum hops for variable-length patterns (`None` = exactly 1).
    pub min_hops: Option<u32>,
    /// Maximum hops (`None` = unbounded or exactly 1 if `min_hops` is also None).
    pub max_hops: Option<u32>,
    pub properties: Option<Expr>,
    pub span: Span,
}

/// Edge direction in a relationship pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// `-[r]->` — outgoing.
    Out,
    /// `<-[r]-` — incoming.
    In,
    /// `-[r]-` — undirected.
    Undirected,
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

/// A Cypher expression.
///
/// `Expr` is recursive: `Box<Expr>` is used wherever a child expression is
/// needed to keep the enum `Sized`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Expr {
    /// An integer, float, string, boolean, or null literal.
    Literal(Literal),
    /// A variable reference: `n`.
    Var(VarRef),
    /// A property access: `n.prop`.
    Property(PropertyAccess),
    /// A binary operation: `left op right`.
    BinaryOp(BinaryOp),
    /// A unary operation: `op expr` (e.g. `NOT x`, `-x`).
    UnaryOp(UnaryOp),
    /// A function call: `name(args)` or `name(DISTINCT arg)`.
    FunctionCall(FunctionCall),
    /// A list literal: `[1, 2, 3]`.
    List(ListLiteral),
    /// A map literal: `{key: expr}`.
    Map(MapLiteral),
    /// A Cypher parameter: `$name`.
    Param(ParamRef),
    /// A `CASE` expression.
    Case(CaseExpr),
    /// A list comprehension: `[x IN list WHERE pred | expr]`.
    ListComprehension(ListComprehension),
    /// A quantifier predicate: `all/any/none/single(x IN list WHERE pred)`.
    Quantifier(Quantifier),
    /// A pattern comprehension: `[(n)-[r]->(m) | r.weight]`.
    PatternComprehension(PatternComprehension),
    /// A pattern predicate: `(n)-[:KNOWS]->(m)`.
    PatternPredicate(PatternPredicate),
    /// A simple or full existential subquery: `exists { (n)-->(m) }` or
    /// `exists { MATCH (n)-->(m) RETURN true }`.
    ExistentialSubquery(ExistentialSubquery),
    /// A label predicate: `n:Person`.
    LabelPredicate(LabelPredicate),
    /// `IS NULL` / `IS NOT NULL`.
    IsNull {
        expr: Box<Expr>,
        negated: bool,
        span: Span,
    },
    /// `x IN list` / `x NOT IN list`.
    InList {
        expr: Box<Expr>,
        list: Box<Expr>,
        negated: bool,
        span: Span,
    },
    /// `x STARTS WITH y` / `x ENDS WITH y` / `x CONTAINS y`.
    StringOp {
        expr: Box<Expr>,
        op: StringOpKind,
        pattern: Box<Expr>,
        span: Span,
    },
    /// `x =~ pattern` — regular expression match.
    RegexMatch {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        span: Span,
    },
    /// A subexpression wrapped in parentheses (preserved for span accuracy).
    Parenthesized { inner: Box<Expr>, span: Span },
}

impl Expr {
    /// Return the span of this expression.
    #[must_use]
    pub fn span(&self) -> Span {
        match self {
            Self::Literal(l) => l.span(),
            Self::Var(v) => v.span,
            Self::Property(p) => p.span,
            Self::BinaryOp(b) => b.span,
            Self::UnaryOp(u) => u.span,
            Self::FunctionCall(f) => f.span,
            Self::List(l) => l.span,
            Self::Map(m) => m.span,
            Self::Param(p) => p.span,
            Self::Case(c) => c.span,
            Self::ListComprehension(lc) => lc.span,
            Self::Quantifier(q) => q.span,
            Self::PatternComprehension(pc) => pc.span,
            Self::PatternPredicate(pp) => pp.span,
            Self::ExistentialSubquery(es) => es.span,
            Self::LabelPredicate(lp) => lp.span,
            Self::IsNull { span, .. }
            | Self::InList { span, .. }
            | Self::StringOp { span, .. }
            | Self::RegexMatch { span, .. }
            | Self::Parenthesized { span, .. } => *span,
        }
    }
}

// ---------------------------------------------------------------------------
// Literal
// ---------------------------------------------------------------------------

/// A scalar literal value: integer, float, string, boolean, or null.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Int(i64, Span),
    Float(f64, Span),
    Str(String, Span),
    Bool(bool, Span),
    Null(Span),
}

impl Literal {
    #[must_use]
    pub fn span(&self) -> Span {
        match self {
            Self::Int(_, s)
            | Self::Float(_, s)
            | Self::Str(_, s)
            | Self::Bool(_, s)
            | Self::Null(s) => *s,
        }
    }
}

// ---------------------------------------------------------------------------
// Leaf expression types
// ---------------------------------------------------------------------------

/// A variable reference: `n`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VarRef {
    pub name: String,
    pub span: Span,
}

/// A property access: `n.prop`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyAccess {
    pub object: Box<Expr>,
    pub key: String,
    pub span: Span,
}

/// A query parameter reference: `$name`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParamRef {
    pub name: String,
    pub span: Span,
}

/// A label predicate: `n:Person` used as a boolean expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabelPredicate {
    pub var: String,
    pub labels: Vec<String>,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// Compound expression types
// ---------------------------------------------------------------------------

/// A binary infix expression: `left op right`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinaryOp {
    pub op: BinaryOpKind,
    pub left: Box<Expr>,
    pub right: Box<Expr>,
    pub span: Span,
}

/// All binary operators in openCypher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOpKind {
    // Comparison
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    // Logical
    And,
    Or,
    Xor,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    // String
    Concat,
}

/// A unary prefix expression: `NOT expr` or `-expr`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnaryOp {
    pub op: UnaryOpKind,
    pub expr: Box<Expr>,
    pub span: Span,
}

/// The operator in a unary expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOpKind {
    Not,
    Neg,
}

/// A function call: `name(args)`, `name(DISTINCT arg)`, or `name(*)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    /// Namespace-qualified name, e.g. `["apoc", "coll", "sum"]`.
    pub name: Vec<String>,
    pub distinct: bool,
    /// `true` when called as `f(*)` — `args` is empty in this case.
    pub star: bool,
    pub args: Vec<Expr>,
    pub span: Span,
}

/// A list literal: `[1, 2, 3]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListLiteral {
    pub elements: Vec<Expr>,
    pub span: Span,
}

/// A map literal: `{key: expr, …}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MapLiteral {
    pub entries: HashMap<String, Expr>,
    /// Exact source span for each parsed key token. Synthetic and legacy
    /// deserialized maps may omit entries here.
    #[serde(default)]
    pub key_spans: HashMap<String, Span>,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// CASE expression
// ---------------------------------------------------------------------------

/// A `CASE` expression — simple (`CASE expr WHEN … THEN …`) or searched (`CASE WHEN … THEN …`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseExpr {
    /// `CASE expr WHEN ...` — the subject is `Some`; simple `CASE WHEN` is `None`.
    pub subject: Option<Box<Expr>>,
    pub when_clauses: Vec<WhenClause>,
    pub else_expr: Option<Box<Expr>>,
    pub span: Span,
}

/// A single `WHEN condition THEN result` arm inside a `CASE` expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
    pub span: Span,
}

// ---------------------------------------------------------------------------
// List / pattern comprehensions
// ---------------------------------------------------------------------------

/// A list comprehension: `[x IN list WHERE pred | projection]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListComprehension {
    pub var: String,
    pub list: Box<Expr>,
    pub filter: Option<Box<Expr>>,
    pub projection: Option<Box<Expr>>,
    pub span: Span,
}

/// Which quantifier a [`Quantifier`] predicate applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QuantifierKind {
    /// `all(x IN list WHERE pred)` — true iff every element satisfies `pred`.
    All,
    /// `any(x IN list WHERE pred)` — true iff some element satisfies `pred`.
    Any,
    /// `none(x IN list WHERE pred)` — true iff no element satisfies `pred`.
    None,
    /// `single(x IN list WHERE pred)` — true iff exactly one element satisfies `pred`.
    Single,
}

/// A quantifier predicate: `all/any/none/single(var IN list WHERE pred)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quantifier {
    pub kind: QuantifierKind,
    pub var: String,
    pub list: Box<Expr>,
    pub predicate: Box<Expr>,
    pub span: Span,
}

/// A pattern comprehension: `[(n)-[r]->(m) WHERE pred | r.weight]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PatternComprehension {
    pub var: Option<String>,
    pub pattern: PathPattern,
    pub filter: Option<Box<Expr>>,
    pub projection: Box<Expr>,
    pub span: Span,
}

/// A pattern predicate used as a boolean expression, e.g. `(n)-->()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PatternPredicate {
    pub pattern: PathPattern,
    pub span: Span,
}

/// A simple or full existential subquery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExistentialSubquery {
    pub body: ExistentialSubqueryBody,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExistentialSubqueryBody {
    Simple {
        pattern: PathPattern,
        filter: Option<Box<Expr>>,
    },
    Full(Box<AstQuery>),
}

// ---------------------------------------------------------------------------
// String operations
// ---------------------------------------------------------------------------

/// The operation in a `STARTS WITH` / `ENDS WITH` / `CONTAINS` string predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StringOpKind {
    StartsWith,
    EndsWith,
    Contains,
}
