# ADR 0007: Runtime Temporal Values

**Status:** Accepted
**Date:** 2026-06-24
**Build target:** v0.5.0 (conformance hardening); openCypher `Temporal3`–`Temporal10`
**Related:** builds on the lowering-time construction of temporal values (ADR-less, `gf-rel::temporal`)

---

## Context

GraphForge gained temporal **construction** in earlier work — `date`/`localtime`/`time`/
`localdatetime`/`datetime`/`duration` built from a constant ISO string or a literal map are
parsed and canonicalised **at lowering time** and emitted as the quoted-ISO `Utf8` literal the
openCypher TCK renders. That cleared the two construction features — `Temporal2` (from a string,
53/53) and `Temporal1` (from a map, 206) — and lifted the corpus to **946** passing.

That approach has a hard ceiling: it only works when the argument is a **compile-time constant**.
The remaining temporal corpus operates on **runtime** temporal values:

- **`Temporal3`** — project/modify from an existing value: `WITH date({…}) AS d RETURN date({date: d, year: 28})`.
- **`Temporal4`** — selecting components into new temporals from a runtime value.
- **`Temporal5`–`Temporal10`** — component **accessors** (`d.year`, `d.epochMillis`), **truncation**
  (`date.truncate('month', d)`), temporal **arithmetic** (`d + duration(…)`), and **comparison/ordering**.

A `Utf8` string carries no type and no component structure, so none of these are expressible on it
without re-parsing strings inside ad-hoc UDFs — which is the "string shortcut" generalised, and does
not compose (mixing a literal `Utf8` temporal with a runtime one is type-inconsistent, equality and
ordering are lexical not temporal, and there is nowhere to hang `.year`).

## Decision

**Represent every Cypher temporal as a typed runtime value backed by an Arrow type, and render it to
the canonical Cypher string only at the result boundary.** Construction (the – path) is
migrated to emit these typed values instead of `Utf8`; the lowering-time *parsing* is retained, only
its *output* changes.

### Value representation

| Cypher type | Arrow representation | Notes |
|-----------------|-------------------------------------------------------|-------|
| `date` | `Date32` (days since epoch) | native |
| `localtime` | `Time64(Nanosecond)` | native |
| `time` | `Struct { nanos: Time64(ns), offset_seconds: Int32 }` | time-of-day + UTC offset (no native Arrow type) |
| `localdatetime` | `Timestamp(Nanosecond, None)` | native |
| `datetime` | `Struct { utc: Timestamp(ns, "UTC"), zone: Utf8 }` | instant + original zone string (offset *or* IANA name) |
| `duration` | `Interval(MonthDayNano)` | signed months/days/nanos — exactly Cypher's model |

`time`/`datetime` use a struct because Arrow has no offset-bearing time and a `Timestamp`'s single
`tz` string cannot carry *both* a resolved offset and the original IANA label needed to render
`…+02:00[Europe/Stockholm]`.

### Rendering is a pure function of the value

The TCK's rendered precision is **derived from the value, not from how it was constructed**. Evidence
(`Temporal3` [2]): `datetime({hour: 12, …})` → `localtime(…)` renders `'12:00'`, while
`localtime({time: other, second: 42})` renders `'12:00:42'`. So the canonical renderer is:

- **time-of-day:** show `HH:MM`; add `:SS` iff seconds ≠ 0 *or* subseconds ≠ 0; add `.fff`
  (trailing-zero-trimmed) iff subseconds ≠ 0.
- **offset:** `Z` for zero; else `±HH:MM` (`:SS` only when the offset has nonzero seconds).
- **duration:** `P[nY][nM][nD]T[nH][nM][nS]`, **each component carrying its own sign**
  (`P12Y5M-14DT16H`), years split from months, whole days folded out of the seconds field,
  subseconds trimmed.

This rule reproduces every one of the 946 currently-passing construction strings (verified against
the `Temporal2`/`Temporal1` examples), so the migration carries **no rendering regression** — the
committed `passing_baseline.txt` gate is the proof obligation at each step.

The existing string formatters in `gf-rel::temporal` (`format_time`, `format_offset`,
`format_duration`, `format_date`) already implement these rules and are reused verbatim by the
renderer; only their *inputs* change from parsed-literal structs to Arrow-value decompositions.

### Where each concern lives

- **gf-rel** (`temporal.rs`, `expr.rs`): construction emits Arrow temporal scalars; new static
  `ScalarUDF`s (alongside `CYPHER_SIZE`) implement accessors, truncation, projection-from-value, and
  duration arithmetic; comparison/ordering need **no** code — Arrow kernels handle native temporal
  types, and the struct-backed `time`/`datetime` compare by their instant fields.
- **gf-api** (result shaper + `tck_steps.rs::render_cell`): add render arms for the six Arrow shapes
  above, delegating to the shared `gf-rel::temporal` formatters.
- **gf-ir**: no new expression-level type system. Temporal functions are **monomorphic by name**
  (`datetime.year`, `date.truncate`) exactly as the corpus spells them, so dispatch needs no
  inference. `IrLiteral::DateTime`/`Duration` (already present) are retained for parameter binding.

## Consequences

- **Composability.** Accessors, arithmetic, comparison, ordering, and `toString`/round-trip
  (`date(toString(d)) = d`) all work because values are typed and decomposable.
- **No regression by construction.** Every phase is gated by `passing_baseline.txt`; a phase that
  changes a rendered string fails CI before merge.
- **Cost.** `time`/`datetime` structs and the duration `Interval` need bespoke decompose/render code;
  named-zone datetimes carry the zone label alongside the UTC instant.
- **Negative & overflowing durations** become representable (they are not in the string-only path).
- **Heterogeneous equality must be solved first (discovered in phase 1).** The corpus compares
  values of different types — e.g. `Temporal7` [6] `WITH duration({…}) AS x, date({…}) AS d RETURN x = d`
  expects `false`. With everything `Utf8` this "worked" by string inequality; the moment one operand
  becomes a typed Arrow value (`Date32`) while the other is still `Utf8`, DataFusion rejects the
  mismatched `=` at planning and the query errors instead of yielding `false`. Cypher requires
  `=` between different types to be `false` (never an error). So **a type-tolerant Cypher equality**
  (a `cypher_eq` that returns `false` on incompatible types and element-equality otherwise) is a
  prerequisite of phase 1, not a later concern — a single-type partial migration regresses
  cross-type comparisons. This also means the migration is closer to "all types at once, behind a
  Cypher-equality shim" than a clean per-type rollout; the phases still stage the *value* work, but
  the equality shim lands first.

## Phased plan (each phase one PR, gated by the 946 baseline)

1. **Date end-to-end.** Migrate `date` construction to `Date32`; render `Date32`; add `date.*`
   accessors (`year`/`month`/`day`/`week`/`ordinalDay`/`quarter`/`dayOfWeek`) and `Temporal3`/`4`
   date projection (`date(value)`, `{date: value, …overrides}`). Proves the architecture.
2. **Local time & local datetime.** `Time64`/`Timestamp(None)`; their accessors; time/datetime
   projection.
3. **Time & datetime with zones.** The struct representations, offset/named-zone handling, zone
   accessors (`offset`, `timezone`, `epochMillis`).
4. **Duration.** `Interval(MonthDayNano)`, signed-component rendering, duration accessors and
   `temporal ± duration` / `duration ± duration` arithmetic.
5. **Truncation & comparison sweep.** `*.truncate(unit, value)`, cross-type comparison/ordering
   conformance, and any remaining `Temporal7`–`Temporal10` tail.

## Alternatives considered

- **Keep `Utf8` + parse inside every UDF.** Rejected: no type identity, lexical (not temporal)
  comparison, type-inconsistent when literals and runtime values mix, and `.year` has nowhere to
  dispatch. It is the string shortcut wearing a UDF.
- **One opaque `Struct` for all six types with a type tag.** Rejected: loses Arrow's native
  comparison/sort/arithmetic kernels and forces a custom kernel for every operation; the per-type
  Arrow mapping above gets ordering and arithmetic largely for free.
