# Release Tracking — v0.3.x Series

**Updated:** 2026-05-02
**Strategy:** Patch-level releases focusing on correctness, then performance
**TCK baseline (v0.3.8):** 3,885/3,885 passing (100%) — locked

---

## Released

| Release | Date | TCK | Highlights | Issue |
|---------|------|-----|------------|-------|
| v0.3.0 | 2026-02-17 | 638/3,837 | Baseline, error validation | |
| v0.3.6 | 2026-03-xx | 2,507/3,885 | Core clause coverage | |
| v0.3.7 | 2026-04-07 | 3,235/3,885 | Sorting, aggregation, temporals | |
| **v0.3.8** | **2026-05-02** | **3,885/3,885** | **Full TCK compliance** | **** |

---

## v0.3.8 — Full TCK Compliance (2026-05-02)

**Goal achieved:** 3,885/3,885 TCK scenarios passing (100%).

### What shipped

**Correctness fixes (6 TCK failures → 0)**
- Aggregate detection in `QuantifierExpression` (`ALL(x IN collect(...) WHERE x)`)
- OPTIONAL MATCH multi-WHERE placement bug (second WHERE overwrote first)
- OPTIONAL MATCH multi-hop WHERE placement (variables not yet bound)
- `coalesce()` type inference for node/relationship arguments
- Non-deterministic aggregate arguments (`rand()`, `timestamp()`, `randomUUID()`)

**Nanosecond precision (17 scenarios)**
- `_ns_residue` field on `CypherDateTime` / `CypherTime` for sub-microsecond storage
- `_components` tuple extended to 8 elements (added `nanoseconds`)
- Construction, accessors, arithmetic, comparison, serialization all updated

**Statement clock caching (4 scenarios)**
- `now()` / temporal functions without args now return consistent values per query

**Extreme year support (2 scenarios)**
- `_WideDate` / `_WideDateTime` for years outside Python's 1–9999 range
- `_BigDuration` for durations that overflow `timedelta`
- Julian Day Number arithmetic for cross-extreme-year differences

**IANA timezone preservation (1 scenario)**
- Store `_iana_tz_name` on `CypherDateTime`
- `.timezone` accessor returns zone name (e.g. `Europe/Stockholm`) not offset

### All 23 xfail markers removed
Previously-xfailed tests were all fixable — none were true CPython limitations.

---

## Released (continued)

| Release | Date | Highlights |
|---------|------|------------|
| v0.3.9 | 2026-05-02 | LALR(1) parser, property indexing, LIMIT short-circuit, bulk ingest |
| v0.3.10 | 2026-05-xx | NetworkX/igraph export, parse/plan LRU cache, LangChain ingestion |
| v0.4.0 | 2026-05-07 | Three-surface API: `db.gds` (8 algorithms), `db.search` (FTS5 + vector), `graphforge.recipes` |

## Planned

| Release | Goal | Focus |
|---------|------|-------|
| v0.4.1 | Beta stabilisation | Bug fixes, API polish |
| v0.5.0 | Rust core | Recursive-descent + Pratt parser, DataFusion execution, Arrow result streams, Parquet storage, Python + Node + Swift + Kotlin bindings — active development on `main` |
