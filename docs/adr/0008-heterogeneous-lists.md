# ADR 0008: Heterogeneous List Values

**Status:** Accepted
**Date:** 2026-06-27
**Build target:** v0.5.0 (conformance hardening); openCypher heterogeneous-list cluster (`List5`/IN, `Precedence3`, `Aggregation2`, `Literals7`, …)
**Related:** (mixed-numeric `max`/`min` returns `Float64`, not the integer element); the histogram "heterogeneous list literals" cluster (`docs/reference/tck-failure-histogram.md`)

---

## Context

openCypher lists are **heterogeneous**: `[1, 2.0, 5]` is a list whose elements keep their individual
types (two integers, one float). Operations preserve those types — `max([1, 2.0, 5])` returns the
**integer** `5`, while `min([1, 2.0, 5, 0.1])` returns the **float** `0.1`.

GraphForge lowers a list literal in `lower_list_literal` (`crates/graphforge-rel/src/expr.rs`). A homogeneous
list const-folds via `ScalarValue::new_list`; a **heterogeneous** one fails the homogeneity guard and
falls back to DataFusion's `make_array`, which **coerces every element to one common Arrow type**
(widening `[1, 2.0, 5]` → `List<Float64>`). The per-element int/float distinction is destroyed *at
construction*, before `UNWIND` or `max`/`min` ever run — so `max` faithfully returns `Float64` `5.0`
and renders `"5.0"` (≠ expected `"5"`). This is ; the same coercion (surfacing as a `make_array`
"failed to unify argument types" error for non-numeric mixes) blocks ~36 TCK scenarios.

Arrow enforces **one DataType per column**, and `UNWIND`'s output is a column — so preserving
per-element type requires a value representation that is itself a single Arrow type yet carries mixed
contents. Two candidates were evaluated in depth against DataFusion 53.1.0 / arrow 58.3.0:

- **Arrow `Union`** — faithfully tags each element, but is **hostile to every aggregate/compare path**:
  `min`/`max` has *no* Union arm (`internal_err!("Min/Max accumulator not implemented for type Union")`);
  every Union ordering/equality primitive (arrow-ord `compare_union`, `ScalarValue::partial_cmp`,
  arrow-row encoding) is **type-id-first**, which segregates ints from floats instead of interleaving
  by magnitude — *silently wrong* Cypher semantics; and you cannot even *build* a Union (`make_array`
  never emits one, arrow-cast refuses `(_, Union)`). ~1500–2500 LOC, high risk. **Rejected.**
- **`ScalarValue`-typed-element list** — inherits the same type-id-first / `internal_err` pathologies
  downstream and discards the plain primitive column for the common homogeneous case. **Rejected.**

## Decision

**Represent a heterogeneous list as a homogeneous Arrow `List<Struct{tag, …value fields}>`
("tagged-struct"), with exactly one value field populated per element, and interpret the `tag` in
custom Cypher UDFs/UDAFs for the three semantic surfaces DataFusion cannot get right (min/max,
ORDER BY, equality).** Homogeneous lists keep their existing primitive `new_list` path untouched.

### Why tagged-struct wins (grounded in DataFusion 53.1.0)

A mixed list becomes a **genuinely homogeneous** `List<Struct<…>>`, so most machinery is native:

| Surface | Tagged-struct | Union |
|---|---|---|
| list construction | native — const-folds via `new_list` (homogeneous struct) | custom kernel (`make_array` can't emit Union; can't cast to Union) |
| UNWIND explode | native — `concat`/`take` handle Struct | native |
| group-by / DISTINCT | native — arrow-row recurses Struct | native (but type-id-first keys) |
| render | extend the existing `Struct` arms | **panics** (no Union arm) |
| min / max | custom UDAF (native struct min/max is field-order = tag-first → wrong, but never *errors*) | **impossible** (`internal_err`) |
| ORDER BY | custom sort surrogate (native struct ordering is tag-first) | simple-sort errors; row-path wrong |
| equality / IN | **extend the already-wired `cypher_value_eq`** | new `(Union, _)` arm + override |

Crucially, the tagged-struct never *hard-errors* on any operator (the worst failure mode — silently
wrong results — is avoided), and equality reuses the existing, tested `cypher_value_eq`/`cypher_seq_eq`
cross-type path. The custom comparison surfaces (min/max, sort, eq) are needed either way; Union
*additionally* needs custom construction and rendering and still fights type-id-first semantics.

### Representation

Start numeric-only, widen later:

```
List<Struct{ tag: Int8, int: Int64, float: Float64 }>      // slice 1 (numeric)
List<Struct{ tag: Int8, int: Int64, float: Float64,
             str: Utf8, bool: Boolean, … }>                // slice 4 (full Any)
```

`tag` selects the live field; the others are null for that element. The struct layout is fixed and
shared (one schema constant), so a mixed list is a homogeneous `List<Struct>` and const-folds cleanly.

## Consequences

**Custom code (~700–1000 LOC net, sliced):**
- Shared tagged-struct schema constant + `ScalarValue → tagged-struct` builder (~150).
- Mixed-list branch in `lower_list_literal`, gated on "elements differ in kind" (~120).
- Custom Cypher **min/max UDAF** — decode `tag`, order by numeric value (tag as deterministic
  tiebreaker), return the tagged struct (~200). Unavoidable: native struct min/max is tag-first.
- Custom **ORDER BY sort surrogate** (Float64 magnitude + tag) (~120). Unavoidable: native struct
  ordering is tag-first.
- **Equality** extension in `cypher_value_eq` tagged-struct arm; IN reuses it (~80).
- **render** tagged-struct arm, factored so every result surface (TCK harness + API/bindings) shares
  one tag→cell dispatch (~80).

**Stays native:** UNWIND explode, group-by/DISTINCT, SortExec plumbing, homogeneous-list const-fold.

**Risks & mitigations:**
- **R1 — field-order-as-sort-key trap.** Never rely on native struct ordering; the min/max UDAF and
  sort surrogate are mandatory and ship in the same slice as any feature exercising that ordering.
- **R2 — regressing the passing baseline.** The tagged-struct path triggers *only* for
  provably-heterogeneous literals; homogeneous lists keep the primitive `new_list` path. A guard test
  asserts `[1, 2, 3]` still lowers to `List<Int64>`.
- **R3 — DISTINCT over-splitting `1` vs `1.0`.** arrow-row keys treat them as distinct; Cypher treats
  `1 = 1.0`. Deferred to slice 4 (canonicalization pass); not in 's critical path. Documented.
- **R4 — render fan-out.** The tag arm must exist on every surface that materializes values, or
  results diverge. Mitigation: one shared dispatch function.

## Limitation discovered during implementation: NESTED heterogeneous lists (amendment, 2026-06-27)

The tagged-struct represents a heterogeneous **flat-scalar** list. It **cannot** represent a list whose
elements are themselves lists or maps — e.g. `[1, [1, 2]]` or `RETURN [1, 2] IN [1, [1, 2]]`. Such an
element would need a `__het_list` field of type `List<Struct{…the same tagged struct…}>` — a **recursive
Arrow type**, which Arrow cannot express (a `DataType` is a finite tree). No fixed struct schema can hold
an arbitrarily-nested heterogeneous value.

Measuring the cluster confirmed the split: of the 38 `make_array` "failed to unify" failures, **only 3
are flat** (scalar-only mixes); **35 contain a nested list or map element** (`List5`/IN ~19,
`Precedence3` ~9, `String8` `[…,[],{}]`, `Aggregation2 [11]/[12]` `max` over `[1,'a',[1,2],…]`, etc.).
So this representation tops out at the flat cases; the nested 35 need a **different value model** — a
recursive/boxed representation (e.g. a `CypherValue` enum carried opaquely, or a canonical-string
encoding for structural `IN`/equality) — which is a separate ADR. **Out of scope here.**

## Vertical-slice plan (revised)

| Slice | Scope | Scenarios | Status |
|---|---|---|---|
| **1** | flip : mixed-**numeric** list → UNWIND → native min/max → render | (`Aggregation2 [5]`) | ✅, +1 |
| **2** | full-`Any` **flat-scalar** lists (int/float/str/bool/null) — construct + render | `Literals7 [16]` | ✅ this PR, +1 |
| — | ~~equality/IN, ORDER BY over mixed~~ | ~~`List5`/`Precedence3`~~ | **blocked** — all nested (see above) |
| future | NESTED heterogeneous lists (~35) | ~35 | needs a new value-model ADR |

The custom min/max UDAF the original plan called for proved **unnecessary**: putting `__het_key` (the
numeric value) FIRST lets DataFusion's native lexicographic `Struct` min/max return the correct element.
**Net result of this ADR: +2 scenarios** (the flat cases); the ~35 nested cases are deferred to a
follow-up value-model ADR. Homogeneous lists stay on the primitive `new_list` path; the baseline must
stay green before each re-bless.
