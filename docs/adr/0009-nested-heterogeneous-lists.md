# ADR 0009: Nested Heterogeneous List Values

**Status:** Accepted
**Date:** 2026-06-28
**Build target:** v0.5.0 (conformance hardening); openCypher `List5`/IN, `Precedence3`, `String8`, `Aggregation2 [11]/[12]`
**Related:** supersedes the "nested" limitation amendment of [ADR 0008](0008-heterogeneous-lists.md); closes the heterogeneous-list cluster of (residue of / the histogram)

---

## Context

ADR 0008 represents a **flat-scalar** heterogeneous list as a tagged struct
`List<Struct{__het_key, __het_tag, __het_int, __het_float, __het_str, __het_bool}>`. Its amendment
concluded that **nested** heterogeneous lists (`[1, [1, 2]]`, `RETURN [1, 2] IN [1, [1, 2]]`) were
infeasible because "an Arrow type cannot be recursive," and deferred ~35 TCK scenarios.

**That conclusion was too strong.** It is true for a *single fixed shared schema* (what ADR 0008 chose)
and for runtime lists whose shape varies per row. But **a list literal's depth and shape are known at
lowering time**, so the engine can build a *concrete, finite, per-literal* Arrow type. `[1, [1, 2]]`
needs `List<Struct{…, __het_list: List<Struct{…}>}>` built to exactly that literal's depth — recursion
**by value**, not a recursive **type**. Every targeted scenario uses list **literals**, so all are
feasible.

This matters because the downstream comparison machinery is **already nested-capable**: `cypher_value_eq`
→ `cypher_seq_eq`/`cypher_struct_eq` (`crates/graphforge-rel/src/expr.rs:2059`, `:3275`, `:3296`) recurse over
`List`/`Struct` `ScalarValue`s with three-valued, cross-type semantics. The only missing piece was a
well-formed nested value to feed them. So nested support needs **no serde, no canonical-string encoding,
and no second comparator** — just construction, IN routing, rendering, and (for `max`/`min` over mixed)
orderability.

## Decision

**Extend the ADR-0008 tagged element with two recursive-by-value fields, and build the struct type to the
literal's actual nesting depth:**

```
__het_list:  List<Struct{__het_*}>   // a nested-list element; its elements are themselves tagged
__het_map:   Struct{…}               // a map element (a named_struct)
```

`__het_tag` gains `4` = list and `5` = map. `__het_key` stays first so native `Struct` min/max ordering
for the flat-numeric case is unchanged (ADR 0008). The fields are generated **per literal**
(`het_fields(depth)`), where the `__het_list` element type at level *k* is the depth-*(k-1)* struct — a
distinct, shallower, finite type at each level, terminating at the scalar struct (no `__het_list`). This
is the one departure from ADR 0008's single-shared-schema choice.

Comparison reuses the existing recursive comparator: a het-tagged element decodes its live field to a
plain `ScalarValue` and recurses through `cypher_value_eq`, so a normal `1: Int64` and a tagged `int = 1`
compare equal, and a normal list compares equal to a het list element-by-element.

## Consequences

- **`IN`** currently lowers to DataFusion's native `in_list` (`expr.rs:1082`), which cannot do structural
  cross-type membership. For a het/nested list operand it is rerouted to a new `CYPHER_IN` ScalarUDF that
  decodes the het list and runs three-valued membership via `cypher_value_eq`. Homogeneous `IN` keeps
  `in_list` (no regression, preserves pushdown).
- **`=`/`<>`** need only one new arm in `cypher_value_eq` (decode-and-recurse for a het-tagged operand).
- **Rendering** extends the existing `__het_tag` arm to recurse into `__het_list`/`__het_map`
  (`render_cell` already recurses `List`/`Struct`).
- **`max`/`min` over fully-mixed lists** need Cypher cross-type orderability (number < string < bool <
  list …), which native min/max lacks — a custom UDAF, isolated as the final slice.
- **Runtime graph-value extension:** ADR 0011 supersedes the original boundary
  for non-constant mixed lists by using a finite per-expression payload schema.

## Slice plan

| # | Scope | Scenarios |
|---|---|---|
| 1 | nested-list construction + `CYPHER_IN` + eq decode arm + render recursion | ~19 (`List5`) |
| 2 | map elements (`__het_map`) | `String8 [8]`, `Map1`, `Literals7 [17]` |
| 3 | list element-access / slicing over het lists | `Precedence3 [1-5]` |
| 4 | `<`/`>`/`<=`/`>=` over lists → `null` | `Precedence3 [6]` |
| 5 | cross-type orderability UDAF for `max`/`min` | `Aggregation2 [11]/[12]` |

Each slice is its own PR; the advisory baseline must stay green and re-blessed; no false passes
(every flip — including three-valued `null` results — verified against the TCK canonical string).
