# openCypher TCK Failure Histogram

**Measured:** 2026-06-26, against the whole vendored TCK corpus (tag 2024.3).
**Snapshot:** 1181 passing / 2716 failing of the scenarios the advisory runner executes.

> A point-in-time measurement to pick conformance targets from **data, not estimates**. Authoritative
> per-feature status stays in `tests/tck/coverage_matrix.json`; this doc ranks *what to build next*.

## Method

The BDD runner records only the *passing* set, so failure reasons were unavailable. We added an
env-gated dump (`TCK_DUMP_FAILURES=<path>`, see `crates/gf-api/tests/bdd/main.rs`) that writes one
JSON record per failing scenario — failing step, error message, query — **before** the baseline gate.
All 2716 failures were then bucketed by cause, refined to construct-level clusters by reading the
queries, and each candidate target was **adversarially verified**: would implementing *only* it flip
the scenarios, or do they have secondary blockers?

Re-run with:

```bash
TCK_DUMP_FAILURES=/tmp/tck_failures.jsonl cargo test -p gf-api --test bdd
```

## A. Histogram by primary cause (all 2716)

| Count | Cause | Notes |
|------:|-------|-------|
| 746 | Parse error (unsupported syntax) | **652 = quantifier predicate `WHERE`** alone |
| 457 | Missing built-in function | **414 temporal** (`*.truncate` 271, `duration.*` 143); 20 cheap non-temporal |
| 341 | Negative scenario we can't run (capability gap) | error-tests; **low direct value** |
| 282 | Binder/schema bug ("No field named") | **correctness bugs**, not missing features |
| 202 | Unsupported feature ("not yet supported") | agg-in-WITH 42, heterog-list 34, MERGE 27, label 23… |
| 183 | **Wrong result** (runs, wrong output) | **~104 are the TCK-harness renderer, not the engine** |
| 162 | Missing test-harness step vocabulary | params 62, CALL 50, list-order 20, control-q 11, graphs 19 |
| 86 | Negative scenario: wrong/no error raised | type-checks the binder doesn't do |
| 85 | Setup (`having executed:`) failure | 65 = can't store temporal/list values as properties |
| 73 | Binder/scoping bug | used-before-introduced / path-var / WITH-* |
| ~99 | misc (rendering 15, side-effects 10, runtime 5, other 19, tail) | |

## B. Ranked targets by **verified** flip (not raw cluster size)

| Rank | Target | Claimed | **Verified** | Conf | Fix surface |
|-----:|--------|--------:|-------------:|------|-------------|
| 1 | **Temporal `*.truncate()`** | 271 | **271** | med | function-lib (`gf-rel/expr.rs`): 4 typed returns + override logic. Infra proven; sole gate. |
| 2 | **Quantifier predicate `WHERE`** | 442–652 | **~368** | med | parser **+ binder + evaluator** (element-scoped predicate; shares machinery w/ list-comprehension). NOT a parser one-liner. |
| 3 | **TCK harness renderer fix** | ~104 | **~100** | high | **test-harness only** (`tck_steps.rs`). No engine change. Cheapest win. |
| 4 | Temporal `duration.*` | 122 | ~51 | med | function-lib + **typed Duration value type** |
| 5 | Binder: identifier **case-folding** | 77 | (bug) | — | binder: stop lowercasing identifiers |
| 6 | Parameterized-query harness | 62 | ~31 | med | harness step + write-path params + SKIP/LIMIT-as-expr + list/map IrLiteral |
| 7 | Typed/list property **storage** | 65 | ~49 | low | executor/storage; overlaps renderer + list-orderability |
| 8 | Binder: composite-var reference after projection | 71 | (bug) | — | `gf-rel/lowerer.rs` |
| 9 | SET/REMOVE `<label>` | 25 | 25\* | high | \*needs full multi-label storage+exec+ledger (4 layers) |
| 10 | Aggregation in `WITH` | 60 | ~15 | med | binder; blocked by read-after-write for write-area scenarios |
| 11 | MERGE (node only) | 32 | ~5 | high | binder stub + IR; ON-clauses & rel-MERGE are separate |
| 12 | CALL procedures | 52 | **0** | high | greenfield: harness registry + CALL/YIELD exec + error codes; every scenario double-blocked |
| — | "ORDER BY wrong result" | 58 | **0** | high | **mislabeled** — ORDER BY works; folds into finding #3 (renderer) |

## C. The sleeper: the harness renderer is a hidden ~100-scenario blocker

In `crates/gf-api/tests/bdd/tck_steps.rs`, scenarios the engine **computes correctly** fail on
formatting/comparison:

- `render_node_struct` sorts props with an alphabetical `BTreeMap` — TCK expects **insertion order**;
  needs a **structural** node/map comparison (order-insensitive keys), not string compare.
- Label-less nodes render as `( {…})` (leading space) vs expected `({…})`.
- `Float64` renders via `f64::to_string()` → `1.0` becomes `"1"`; TCK expects `"1.0"`.
- String values aren't re-escaped (quotes / backslashes / newlines).

Pure test-harness work, no engine risk — the highest ROI on the board, and the entire mislabeled
"ORDER BY" set folds into it.

## D. What the adversarial pass corrected

- **CALL (52 → 0):** double-blocked (no harness fixture step *and* CALL is a binder stub).
- **ORDER BY (58 → 0):** ORDER BY is implemented; failures are renderer formatting.
- **Agg-in-WITH (60 → 15):** write-area scenarios blocked by read-after-write.
- **MERGE (32 → 5):** binder stub; ON-clauses dropped; rel-MERGE separate.
- **duration (122 → 51), params (62 → 31), typed-prop (65 → 49):** each hides a value-model gap.

## E. Recommended sequencing

1. **Harness renderer fix** — ~100 scenarios, no engine risk.
2. **Temporal `*.truncate`** — 271, self-contained function-lib work.
3. **Quantifier `WHERE`** — ~368, but budget for the binder/evaluator (also unlocks list-comprehension).
4. **Binder case-folding** + **composite-var ref** — focused correctness bugs.
5. Then duration / params / typed-prop storage as value-model investments.

**Do not** scope CALL, MERGE, or agg-in-WITH as quick single-target wins — verified leverage is a
fraction of the raw count.
