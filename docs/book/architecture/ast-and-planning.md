# AST & Planning

**Status:** v0.5.0 — Cypher compiler pipeline shipped
**Last Updated:** 2026-07-27

---

## Overview

The Cypher compiler is a four-stage pipeline: **parse → bind → Graph IR → DataFusion logical plan**. Each stage has a distinct representation; they are not interchangeable.

```text
OpenCypher text
      ↓
┌─────────────────────────┐
│  Hand-written Tok lexer │
│  Recursive-descent +    │
│  Pratt expression parser│  ← gf-cypher
│  → AST with spans       │
└─────────────────────────┘
      ↓
┌─────────────────────────┐
│  Binder                 │
│  Ontology resolution    │  ← gf-ontology
│  Scope validation       │
└─────────────────────────┘
      ↓
┌─────────────────────────┐
│  Graph IR               │  ← gf-ir
│  Semantic graph ops     │
│  Versioned envelope     │
└─────────────────────────┘
      ↓
┌─────────────────────────┐
│  Relational lowering    │  ← gf-rel
│  DataFusion LogicalPlan │  ← gf-plan
│  Optimizer              │
│  Physical plan          │
└─────────────────────────┘
      ↓
  Arrow RecordBatch stream
```

---

## Parser: Recursive Descent + Pratt

The parser is a **hand-written recursive-descent clause parser** with a **Pratt expression parser** subroutine — the architecture used by Neo4j's production Cypher parser, PostgreSQL, SQLite, and virtually every other production SQL/query-language parser. It is conflict-free by construction.

See [ADR 0002](../../adr/0002-lr1-grammar.md) for the full rationale. LALRPOP was the original approach; 50 structural shift/reduce conflicts in the full openCypher grammar proved unresolvable within LALRPOP's state-merging model.

Key design decisions:

- **TokenStream** — tokens collected eagerly upfront from the `Tok` lexer; the parser walks a clean `Vec` with no further lexer interaction
- **One token of lookahead** — sufficient for the entire openCypher grammar at the clause dispatch level
- **Pratt binding powers** — `OR(10)` → `XOR(20)` → `AND(30)` → `NOT(35)` → comparisons `(40)` → `+/-` (50) → `*/%` (60) → `^` (70) → postfix `.[]` (80)
- **Compound lexer tokens** — `NOT IN`, `IS NULL`, `IS NOT NULL`, `STARTS WITH`, `ENDS WITH` are emitted as single tokens by the lexer; the Pratt parser treats them as single-token operators

The openCypher grammar artifacts (ISO WG3 BNF, ANTLR grammar, EBNF, TCK) are the specification source. The TCK is the conformance gate.

### Key parser files

- `crates/gf-cypher/src/parser/mod.rs` — `TokenStream` struct; public `parse()` entry point
- `crates/gf-cypher/src/parser/expr.rs` — Pratt expression parser
- `crates/gf-cypher/src/parser/clauses.rs` — recursive descent clause rules
- `crates/gf-cypher/src/parser/patterns.rs` — node, relationship, and path pattern parsing
- `crates/gf-cypher/src/lexer.rs` — hand-written `Tok` lexer; emits compound tokens for multi-word predicates

---

## AST

The AST is **syntax-faithful and span-rich**. It is an internal representation with no compatibility guarantee across versions.

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AstQuery {
    pub dialect: DialectVersion,   // e.g. "opencypher-9"
    pub clauses: Vec<AstClause>,
    pub span: Span,
}
```

Key invariants:
- Every node carries a `Span` for error reporting and diagnostics
- AST nodes are immutable (`frozen` equivalent in Rust via owned fields)
- The AST is not serialized or shared across language boundaries — Graph IR is the stable contract

---

## Binder and Ontology Resolution

The binder resolves AST identifiers against the runtime ontology before producing the Graph IR:

- Node labels, relationship types, and property names are resolved to typed integer IDs
- Variable scope is validated (variables referenced in WHERE/RETURN must be introduced in MATCH)
- Semantic flags from the ontology (transitive, symmetric, inverse) are recorded on IR operators for planner use

**Unknown label/type/property behaviour depends on the project's ontology mode** (see [ADR 0003](../../adr/0003-progressive-ontology.md)):

| Mode | Unknown label/type/property behaviour |
|---|---|
| `exploratory` | RuntimeCatalog assigns a runtime TypeId; binding continues; no error |
| `advisory` | RuntimeCatalog assigns a runtime TypeId; a warning is recorded; binding continues |
| `strict` | Graph properties must be declared for the bound owner; unknown, wrong-owner, or ambiguous declarations produce a span-rich `BindError` |

Strict property binding keeps two identity spaces separate. Ontology `PropId`
values validate declarations only; they never enter executable Graph IR.
After owner validation succeeds, the binder interns the property name through
`RuntimeCatalog` and emits that runtime property ID for relational lowering and
execution.

For node variables, the binder accepts a property declared directly by the
bound entity type or inherited through its single-entity-parent chain. Multiple
visible declarations of the same name are ambiguous and fail closed rather
than choosing one. Relationship variables accept properties declared directly
by their bound relation type; relation inheritance is not part of the ontology
model. Entity and relation owner IDs occupy independent numeric namespaces, so
compiled property lookup includes the owner kind and cannot collide when those
numeric IDs are equal.

Catalog interning is transactional at the bind boundary. The binder stages all
catalog observations, publishes them only with a successful plan, and discards
them on any bind error. A failed read or mutation bind therefore cannot alter
the shared runtime catalog, staging area, graph, or published generation.

The default mode is `exploratory` when no ontology is present and `advisory` when `ontology.yaml` exists. `strict` must be explicitly declared in `graphforge.yaml`.

The ontology is a **runtime-loadable metadata model**, not compiled into Rust types. See [Storage](storage.md) for the ontology representation and [ADR 0003](../../adr/0003-progressive-ontology.md) for the progressive ontology decision.

---

## Graph IR

The Graph IR is the **stable semantic contract** between the compiler and the execution layer. It is semver-versioned.

```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphPlan {
    pub ir_version: IrVersion,
    pub dialect: String,
    pub ontology_version: Option<OntologyVersion>,  // None in exploratory mode
    pub ontology_mode: OntologyMode,               // exploratory | advisory | strict
    pub ops: Vec<GraphOp>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GraphOp {
    NodeScan { var: VarId, ty: TypeId },
    EdgeScan { var: VarId, ty: TypeId },
    Expand {
        src: VarId,
        edge: VarId,
        dst: VarId,
        rel_ty: TypeId,
        dir: Direction,
        min_hops: u16,
        max_hops: Option<u16>,
    },
    Filter { expr: ExprId },
    Project { items: Vec<ProjectItem> },
    Aggregate { group_by: Vec<ExprId>, aggs: Vec<AggExpr> },
    Optional { child: Box<GraphPlan> },
    Union { all: bool, inputs: Vec<GraphPlan> },
    Create { pattern: CreatePattern },
    Merge { pattern: CreatePattern },
}
```

### IR versioning

The IR envelope records:

| Field | Purpose |
|---|---|
| `ir_version` | Semver-like; backward-compatible when possible |
| `dialect` | e.g. `"opencypher-9"` |
| `ontology_version` | Exact runtime ontology checksum or semantic version |
| `feature_flags` | Required optional features for execution |

The AST version is internal only. The IR version is the cross-layer contract. Substrait can optionally be used as a serialization format for relational plans *after* graph lowering — not as the graph IR itself, because Substrait does not cover the full range of graph-native operators.

---

## Relational Lowering

The relational lowering stage (`gf-rel`) translates graph IR operators into DataFusion `LogicalPlan` nodes:

- Simple scans, filters, projections, and aggregations lower cleanly to DataFusion relational operators
- DataFusion then optimizes projections and filters and chooses physical scan strategies
- Graph-native operators that cannot be expressed as relational algebra remain as **custom DataFusion nodes**

### Example lowering

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.name = $name
RETURN b
```

Graph IR:
```json
{
  "ir_version": "1.0.0",
  "ops": [
    { "kind": "NodeScan", "var": "a", "ty": "Person" },
    { "kind": "Expand", "src": "a", "edge": "e", "dst": "b",
      "rel_ty": "KNOWS", "dir": "out", "min_hops": 1, "max_hops": 1 },
    { "kind": "Filter", "expr": "a.name = $name" },
    { "kind": "Project", "items": ["b"] }
  ]
}
```

Relational lowering produces:
1. `NodeScan` → `TableScan` on `node_facts` filtered to `type_id = Person`
2. `Expand` → join against `edge_facts` and second `node_facts` scan
3. `Filter` → DataFusion `Filter` node with predicate pushdown
4. `Project` → DataFusion `Projection`

---

## Custom Graph Operators

The following operators remain as GraphForge-owned custom DataFusion nodes because their semantics cannot be faithfully expressed in plain relational algebra:

| Operator | Why custom |
|---|---|
| Variable-length path expansion | Requires recursive or iterative traversal — not a finite join sequence |
| Optional-match nullability | Semantic null-shaping distinct from SQL LEFT JOIN |
| Path uniqueness / distinctness | Cypher path semantics (isomorphism vs. homomorphism) |
| Provenance-aware semijoins | Confidence propagation during join |
| Ontology-aware inference | Transitive/symmetric closure — not a static join |
| MERGE | Graph upsert semantics with write-path locking |

---

## Operator Ordering (Cypher Semantics)

The planner must preserve openCypher clause ordering semantics:

1. MATCH / OPTIONAL MATCH (NodeScan, Expand, LeftJoin)
2. WHERE (Filter) — applied as early as possible via predicate pushdown
3. WITH / aggregation
4. ORDER BY (Sort) — must occur before RETURN to access all bound variables
5. RETURN (Project or Aggregate)
6. SKIP / LIMIT

---

## Diagnostics and `explain`

Every AST node carries a `Span`. Bind errors and parse errors report the span alongside a structured message. The `explain` API exposes the plan at each stage:

```rust
pub enum ExplainStage {
    Ast,
    BoundAst,
    GraphIr,
    LogicalPlan,
    PhysicalPlan,
}
```

This enables `db.explain(cypher, stage=ExplainStage::GraphIr)` to return a human-readable or JSON representation of the plan at any compiler stage.

---

## References

- [Architecture Overview](overview.md) — workspace layout and high-level pipeline
- [Execution Model](execution-model.md) — DataFusion integration and Arrow result streams
- [ADR 0001: Rust Core](../../adr/0001-rust-core.md) — parser tool choice rationale
- [ADR 0002: LR(1) Grammar](../../adr/0002-lr1-grammar.md) — LR(1) vs LALR(1) mode decision
