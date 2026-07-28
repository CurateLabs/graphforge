# ADR 0010: Full-range dates (proleptic-Gregorian calendar) and a wider duration model

**Status:** Accepted
**Date:** 2026-06-30
**Build target:** v0.5.0 (conformance hardening); openCypher `Temporal10` large-value cases
**Related:** builds on ADR 0007 (runtime temporal values)

---

## Context

openCypher dates span years **−999,999,999 … +999,999,999** and `duration.between`/`inSeconds`
can produce spans of billions of years. ADR 0007 represented a `date` as Arrow `Date32` (signed
**i32 days** since the Unix epoch) and did all date arithmetic via chrono `NaiveDate`. Both cap far
short of the spec:

- chrono `NaiveDate` is limited to roughly year **±262,143**.
- `Date32` is **i32 days** ≈ year **±5.8M**.
- The duration model `{months:i32, days:i32, nanos:i64}` overflowed: ~24e9 months don't fit `i32`,
  and a billion-year span in total sub-day nanoseconds (~6.3e25) doesn't fit `i64`.

So `date('-999999999-01-01')` parsed to `null`, and Temporal10 **[9]/[10]** returned `null` — the
last two temporal scenarios. ADR 0007's `(Date32, Time64)` split for `localdatetime`/`datetime` was
chosen to span "1…9999+ at nanosecond precision"; it left no headroom for the full year range.

## Decision

1. **A custom proleptic-Gregorian calendar on `i64` days** (`crates/gf-rel/src/calendar.rs`) replaces
   chrono for all DATE math — Howard Hinnant's `days_from_civil`/`civil_from_days`, plus ISO-week,
   ordinal, weekday, day-clamped `add_months`, and signed-year ISO formatting. Exact for any `i64`
   year; cross-checked against chrono for the in-range era in unit tests.

2. **The `date` Arrow representation widens** from `Date32` (i32 days) to **i64 days**, self-describing
   so storage decode can still tell a date from an integer:
   - standalone `date` → `Struct{epoch_day: Int64}` (a bare `Int64` is indistinguishable from an
     integer property; a one-field struct is self-describing and orders chronologically, consistent
     with how `duration`/`localdatetime`/`datetime` are already struct-typed);
   - the `date` child of the `localdatetime`/`datetime` structs → `Int64` days (the parent struct
     already self-describes).

3. **The `duration` model widens** to `{months:i64, days:i64, seconds:i64, nanos:i64}` in the
   Neo4j/openCypher **floor-canonical** form: `seconds` carries the sign and `nanos` is the
   non-negative nanoseconds-of-second `[0, 1e9)`; rendering reconstructs sign-coherent components.
   Splitting `seconds` from `nanos` lets a ~6.3e16-second span fit `i64`; `i64` months hold ~24e9.

4. **Out of scope (unchanged):** time-of-day stays `Time64` (nanoseconds-of-day — year-independent);
   named-zone resolution stays `chrono_tz`. No corpus scenario combines a named zone with an extreme
   year (extreme dates are offset-only / unzoned), so `chrono_tz`'s ±262k limit is not a constraint.

## Consequences

- **Ordering preserved.** Native `Int64` is chronological; the date-first `localdatetime`/`datetime`
  structs keep row-format lexicographic = chronological order.
- **Storage layout changes** for `date`, `duration`, `localdatetime`, `datetime` columns. Pre-1.0,
  no on-disk format guarantee, so no migration is provided; the struct/field-name decode dispatch
  distinguishes each type.
- **Rendering:** `calendar::format_date` emits signed years for the extreme range (chrono's `%Y`
  cannot); in-range output is byte-identical to chrono.
- **Risk** concentrates in calendar correctness (ISO-week-year boundaries, the proleptic year 0,
  leap years, day-clamped month add) — covered by dedicated unit tests + an in-range chrono
  cross-check, and ultimately by the whole-corpus regression gate.

## Phasing

- **Phase 1 (foundation):** the `calendar` module + the duration-model widening. Behavior-preserving
  on the in-range corpus (0 new/lost scenarios); the calendar lands validated but not yet wired.
- **Phase 2:** migrate the date math off chrono to `calendar` **and** widen the date Arrow
  representation together (each date function touched once). Delivers Temporal10 [9]/[10] → the
  temporal corpus reaches 1004/1004 and closes.
