# ADR 0011: Dynamic Heterogeneous Value Lists

**Status:** Accepted
**Date:** 2026-07-10
**Build target:** v0.5.0 (conformance hardening)
**Related:** extends ADR 0008/0009; 

## Context

ADR 0009 supports finite nested heterogeneous literals whose values are known at
lowering time, but excludes runtime expressions and graph values. A list such as
`[n, r, p, 1.5, ['list'], null, {a: 'map'}]` therefore falls through to
DataFusion `make_array`, which cannot unify the different Arrow types.

Converting graph values to strings or a bespoke result wrapper would lose value
semantics and violate GraphForge's Arrow result contract. A single recursive
shared Arrow type is also impossible, but the expressions and their Arrow types
are known when the relational plan is built.

## Decision

Represent a non-constant mixed-type list with a finite per-expression payload
schema:

```text
List<Struct{
  __het_tag: Int8,
  __het_value_0: <type of expression 0>,
  __het_value_1: <type of expression 1>,
  ...
}>
```

Each list element sets `__het_tag` to its expression index and populates exactly
that payload field. Null input values become null struct elements. A variadic
scalar UDF builds the list per input row using Arrow `take`, so node,
relationship, path, temporal, nested list, map, and scalar arrays remain in
their original Arrow representation.

The existing tagged-value decoder selects `__het_value_<tag>` and returns the
original `ScalarValue`. Equality, access, conversion, and Cypher ordering then
reuse their existing decode-then-operate paths. Result rendering selects the
same payload and recursively uses the normal Arrow renderer.

Homogeneous lists and constant ADR-0008/0009 lists keep their existing layouts.
The dynamic path is selected only when all expression types are known and at
least two differ.

## Consequences

- Mixed runtime graph values remain complete Arrow values rather than strings.
- `UNWIND`, equality, conversion errors, and ORDER BY share one representation.
- The schema is finite and exact for each expression; no recursive Arrow type or
  serde payload is required.
- The Int8 tag limits one dynamic list literal to 127 elements. Larger literals
  fail planning explicitly rather than truncating or wrapping tags.
- Because payload field names are internal and selected by tag, they do not
  alter public result column names.
