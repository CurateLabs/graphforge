# GraphForge TCK Compliance — Rust v0.5 core

**Status:** in progress (M17 Conformance Hardening). Authoritative per-feature status lives in
`tests/tck/coverage_matrix.json` (at the repo root, outside this docs tree) and is CI-enforced.

## The corpus and the target

GraphForge vendors the openCypher Technology Compatibility Kit (TCK) Gherkin corpus at tag **2024.3** under `tests/tck/features/`: **220 feature files** holding **1,615 authored
`Scenario`/`Scenario Outline` blocks**, which expand (each Scenario Outline × its `Examples` rows) to
**3,897 runnable scenarios** — the count cucumber actually executes, and the **denominator of the
100% gate** (reported on every BDD run as "N passing of 3897 scenarios"). An earlier
`tck_coverage.rs` figure of 3,880 under-counted a handful of Scenario-Outline expansions; the BDD
harness's expanded count is authoritative.

The v0.5.0 bar (release-enforced at the v0.5.0 publication close-out) is **100% of
the vendored corpus passing**. GraphForge implements every openCypher feature the
corpus exercises — **including `CALL … YIELD` procedures** via the TCK
mock-procedure registry (analyst verbs are an additional surface, not a Cypher
`CALL` substitute) — so the corpus has **no deliberate xfails**: "100%" means
**every scenario actually passes**.

## Current status

| | scenarios (cucumber-expanded) |
|---|---|
| **Passing** | **3,897** |
| Not yet passing | 0 |
| **Total** | **3,897** |

The whole corpus runs on every CI build; all **3,897** scenarios pass. The gate denominator is the
cucumber-expanded corpus — the count the BDD harness runs and gates on every CI build.

## How it works — advisory whole-corpus run

- **Runner.** The cucumber-rs harness in `crates/graphforge-api/tests/bdd/` runs **every** scenario of the
  vendored corpus — no `@skip-rust` filter — via cucumber `run()`, which does **not** exit on
  failures. So an unsupported scenario failing does **not** break CI (the run is *advisory*).
- **The gate is per-scenario (set-based, not a count).** `tests/tck/passing_baseline.txt` lists the
  scenarios that pass, one per line, keyed `<feature-name>:<line>:<scenario-name>`. A custom cucumber
  writer records the set that actually passes this run, then the harness:
  - **fails** the `BDD — Rust` job if **any** baseline scenario stops passing — a regression — even
    if a *different* scenario newly passes (a count would let an XPASS mask a regression; a set does
    not);
  - emits a **`TCK XPASS`** warning listing scenarios that newly pass and aren't yet in the baseline.
- **Catching XPASS.** As engine features land, scenarios start passing on their own — no manual
  un-skipping. The CI warning surfaces exactly which ones; lock them in by re-blessing the baseline.

Running the whole TCK advisorily surfaces XPASS at scenario granularity, so a
two-pass / one-regression change is correctly flagged as a regression, not a
net gain.

## Regression floor

No scenario in `passing_baseline.txt` may stop passing. The advisory run checks the live passing
**set** against it every build, so a regression fails CI automatically — and cannot be offset by an
unrelated improvement. Removing a scenario from the baseline is an explicit, review-visible edit.

## Performance monitoring

The same Rust runner measures every GraphForge API BDD and openCypher TCK scenario once during its
normal execution. A concurrency-safe writer observes raw cucumber `Scenario::Started` and
`Scenario::Finished` events with `std::time::Instant`; timing after cucumber's normalization buffer
would measure event replay rather than execution for queued scenarios.

TCK timing uses the versioned `pooled-isolated-serial-v1` fixture profile. One in-memory
`GraphForge` is reused across scenarios, but `clear()` removes graph storage and resets the runtime
catalog, procedure registry, and adjacency cache before every lease. The serial measurement
boundary prevents a genuine outlier from inflating other concurrently active scenarios. It is also
faster for this corpus than cucumber's default 64-way execution, which creates 128 Tokio worker
threads and causes severe temporary-storage contention. API BDD timing remains informational and
keeps its normal runner behavior.

Each run writes `report.json`, `summary.md`, and `tck-baseline-candidate.json` under
`BDD_TIMING_DIR` (default `target/bdd-timings`). CI uploads the directory for 14 days and appends the
Markdown summary to the job summary. Reports contain only the suite, public
`<feature>:<line>:<scenario>` identity, outcome, and elapsed time — never query/fixture payloads,
graph values, UUIDs, parameters, temporary directories, or local paths.

Both API BDD and TCK suites report count, summed scenario time, minimum, maximum, mean, median, p90,
p95, p99, feature aggregates, and their slowest scenarios. API timing is informational. Only
passing, baseline-matched TCK scenarios can emit performance warnings:

```text
scenario: current > max(2 × baseline, baseline + 250 ms)
aggregate: current total > max(1.25 × baseline total, baseline total + 15 seconds)
```

The scenario comparison also uses the committed absolute slow threshold, warning at the earlier of
the relative or absolute boundary. New or renamed scenarios are reported as unbaselined without a
warning on their first observation. At most 25 GitHub annotations are emitted; every finding remains
available in the artifacts. Performance findings are warning-only for v0.5.0. Correctness
regressions—including API BDD failures—malformed required configuration, and artifact failures
remain blocking. Artifacts are written before those assertions so failures retain timing evidence.
The policy and baseline also record the fixture-profile name and TCK concurrency. A profile mismatch
is a blocking configuration error rather than an invitation to compare incompatible measurements.

### Updating the performance baseline

Refresh `tests/tck/performance_baseline.json` only from a complete, passing `BDD — Rust` run on the
standard `ubuntu-latest` CI runner:

1. Download and inspect `bdd-rust-timings/tck-baseline-candidate.json`.
2. Confirm `partial` is false, `scenario_count` is 3,897, the fixture profile is
   `pooled-isolated-serial-v1` with concurrency 1, and the correctness gate has zero regressions or
   XPASS.
3. Review p95, p99, maximum, the slowest 25 scenarios, and changes to every baseline duration.
4. Copy the candidate to `tests/tck/performance_baseline.json`.
5. Set `baseline_required` to `true` and set `absolute_slow_ms` to the candidate's suggested value,
   calculated as `ceil_100ms(max(2 × p99, max + 250 ms))`.
6. Re-run CI and require a clean comparison before merging.

Never baseline a local, partial, failing, or pre-fix run. Wall-clock CI job duration remains useful
context but is not a threshold; the aggregate policy uses summed scenario durations.

### Initial fixture validation and pre-fix baseline

a prior pull request's first `ubuntu-latest` candidate was rejected during review. Under cucumber's default
concurrency of 64, it reported a 7,948.747-second summed TCK duration, 30.240-second p99, and
37.457-second maximum. The supposedly slow `Comparison1` scenarios measured only 129–142 ms when
that feature ran alone and 0.5–34 ms serially, proving the full-corpus values were fixture
contention rather than scenario cost.

The repaired local `pooled-isolated-serial-v1` run preserved **3,897 / 3,897** scenarios, created one
reusable engine, and completed in 71.79 seconds. Its summed TCK duration was 53.738 seconds, median
1.135 ms, p95 18.651 ms, p99 37.421 ms, and maximum 15.187 seconds. The actual long scenarios were
the large XOR expression, the many-`CREATE` query, and the quantifier-invariant cases. Local results
are validation evidence only.

The reviewed complete `ubuntu-latest` candidate used the same fixture profile, preserved API BDD at
102 passed/4 skipped with no failures, and preserved **3,897 / 3,897** TCK scenarios with zero
regressions or XPASS:

| Suite | Count | Sum | Min | Mean | Median | p90 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| API BDD | 106 | 9.547 s | 22.592 ms | 90.062 ms | 116.245 ms | 143.964 ms | 149.438 ms | 165.048 ms | 165.514 ms |
| openCypher TCK | 3,897 | 137.581 s | 0.190 ms | 35.304 ms | 4.279 ms | 35.637 ms | 53.572 ms | 102.345 ms | 32,966.692 ms |

The real slow tail is the large XOR expression (32.967 s), many-`CREATE` query (11.967 s), and eight
quantifier-invariant scenarios (5.068–5.274 s). The absolute formula selects **33.3 seconds**. The
aggregate warning boundary is **171.976 seconds** (1.25× the 137.581-second baseline; the relative
allowance is larger than the +15-second floor). The Rust BDD job completed in 3m51s, comparable to
the rejected concurrent run while producing attributable scenario timings. This measurement is the
reviewed pre-fix reference; the post-M17 baseline below supersedes it for warnings.

### XOR chain correction

The `Boolean3:90:[3]` outlier was a compiler-tree problem, not expensive boolean evaluation. The
binder represented each XOR as `(a OR b) AND NOT(a AND b)` while sharing the `a` expression ID.
Relational lowering recursively rebuilt that shared subtree twice at every XOR, so an 11-operand
source chain became 3,069 lowered AND/OR UDF nodes. XOR is now a first-class Graph IR operator and
lowers to one immutable, three-valued `cypher_xor` UDF node per source operator.

The following release-mode stage profile uses five measured runs after warm-up on the same local
machine and exact pre-fix/fixed commits. It is diagnostic evidence; the committed deterministic gate
uses node counts rather than wall time.

| Stage | 11 operands, before | 11 operands, after | 22 operands, after |
|---|---:|---:|---:|
| Parse | 13.709 us | 7.375 us | 16.292 us |
| Bind + Graph IR | 9.334 us | 2.750 us | 2.916 us |
| Relational lowering | 219.610 ms | 0.115 ms | 0.693 ms |
| DataFusion optimization | 454.073 ms | 0.246 ms | 1.308 ms |
| Physical planning | 0.133 ms | 0.042 ms | 0.046 ms |
| Execution | 0.018 ms | 0.011 ms | 0.011 ms |
| Lowered boolean/XOR nodes | 3,069 | 10 | 21 |

At 11 operands, the fix reduces relational lowering by approximately **1,909x**, optimization by
approximately **1,844x**, and lowered expression work by **307x**. Doubling the source operands from
11 to 22 grows the deterministic work counter from 10 to 21 nodes (**2.1x**, within the 3x gate).
The public-API test covers both null-containing and non-null chains, while the UDF unit test covers
all nine `true`/`false`/`null` input pairs.

A focused local `Boolean3` run preserved all 30 scenarios with no warning and reduced the affected
scenario to **15.823 ms**. The subsequent complete local run preserved **3,897 / 3,897**, reported
zero regressions, XPASS, or performance warnings, and measured the scenario at **11.236 ms**. The
reviewed `ubuntu-latest` baseline is **32,966.692 ms**; because cross-machine wall times are
evidence rather than an assertion, the PR's complete Ubuntu timing run is the authoritative 10x
acceptance comparison.

Run the deterministic scaling gate with:

```bash
cargo test -p graphforge-api --test xor_scaling
```

### Dependent CREATE correction

The `Create4:580:[2]` outlier already buffered all graph effects and committed them atomically in one
batch; storage persistence was not repeated per clause. A sampled 760-clause synthetic equivalent
spent 3,250 of 3,404 active samples in `run_create_phase`. Frontier extension accounted for 3,248
of those samples, split almost evenly between created nodes and relationships. Each extension rebuilt
an ever-wider DataFusion schema and rechecked every qualified column name. Parsing took about 9 ms
and binding about 53 ms, while execution took 4.716 seconds.

The statement driver now computes backward variable liveness for a contiguous terminal CREATE-only
suffix. A created binding is materialized into the relational frontier only when a later CREATE
clause reads it; all graph effects remain in the shared writer and still reach storage through the
same single atomic commit. Mixed writes, relational segments, and terminal projections retain the
conservative path. Property expressions participate in liveness, so later clauses can still read
properties from earlier created entities.

The permanent public-API gate compares 25 and 250 dependent clauses. Both sizes perform exactly one
rewrite commit and two zero-row node-file reads, with no edge-file reads; 10x more clauses therefore
produce 1x commit and full-read work. It also verifies exact node, relationship, property, and label
side effects, final persisted counts, and an execution-time failure in the last clause that leaves no
partial graph.

On the same local debug build, the 760-clause synthetic query fell from **4.716 seconds** to
**82.307 ms** (about **57x**). The focused vendored `Create4` scenario fell from **8,078.304 ms** to
**192.446 ms** (about **42x**) while both feature scenarios continued to pass. The reviewed 
`ubuntu-latest` baseline is **11,967.445 ms**; the PR's complete Ubuntu timing artifact is the
authoritative 5x acceptance comparison. A complete local run preserved **3,897 / 3,897** scenarios
with zero regressions, XPASS, or performance warnings and measured the scenario at **207.301 ms**.

Run the deterministic scaling and atomicity gate with:

```bash
cargo test -p graphforge-api --test create_scaling
```

### Quantifier-invariant correction

The eight slow `Quantifier9`–`Quantifier12` scenarios shared an expression-pipeline bottleneck,
not an invalid TCK fixture. An exact pre-fix local profile spent 2.311 seconds in execution while
parse and bind took 1.424 ms and 0.751 ms. Sampling found 687 active samples in the list
comprehension's per-row `filter_record_batch` path and another 323 in `cypher_list_plus`; only 29
samples reached the terminal quantifier. The dominant cost was repeatedly decoding, filtering, and
recursively rebuilding the same deeply nested heterogeneous Arrow values across the three `UNWIND`
boundaries.

The exact scenario's deterministic boundary profile is:

| Boundary | Output rows | Logical element/predicate rows |
|---|---:|---:|
| First `UNWIND` | 13 | — |
| First list comprehension / `WITH` | 13 | 169 |
| First list-plus `WITH` | 13 | 13 |
| Second `UNWIND` | 169 | — |
| Second list comprehension / `WITH` | 169 | 2,197 |
| Second list-plus `WITH` | 169 | 169 |
| Third `UNWIND` | 2,197 | — |
| Third list comprehension / `WITH` | 2,197 | 28,561 |
| Third list-plus and non-empty filter | 2,197 | 2,197 |
| Invariant quantifier / aggregation | 1 | 2,197 input rows, zero predicate evaluations after the fix |

The three list-comprehension boundaries therefore evaluate 30,927 logical elements and the three
list-plus boundaries shape 2,379 rows; neither count is removed or reordered. Post-fix internal
timers attributed a five-run median of 148.7 ms to the three batched list comprehensions and 87.1 ms
to the three range-assembled list-plus calls. The remaining 226.9 ms covered `UNWIND`, volatile
`CASE`/`rand()`, filtering, aggregation, planning, and result shaping.

Relational lowering now recognizes statically true, false, and null quantifier predicates and folds
them from list nullability and cardinality. Uncorrelated list comprehensions flatten all valid list
rows into one element batch, evaluate a volatile predicate exactly once per logical element, and
reassemble the original row boundaries. Tagged list-plus-element evaluation assembles Arrow ranges
directly and recursively promotes only nested list values, retaining Cypher's runtime rule that a
list-valued element concatenates rather than nests. Correlated comprehensions retain the
conservative per-row path.

Deterministic unit gates cover all four quantifiers over empty, singleton, 1x, and 10x
heterogeneous/null-containing lists. Their work counter advances once per list row rather than once
per element. An injected volatile zero-argument UDF verifies one batched invocation and exactly one
evaluation per non-null logical element; list order, null-list rows, empty rows, dynamic nested-list
concatenation, and output cardinality are asserted separately.

The exact local scenario fell from **2.311 seconds** to a five-run median of **462.714 ms** (about
**5.0x**). A focused serial run preserved all **604 / 604** quantifier scenarios with no warnings;
the affected eight measured **465.561–476.402 ms**. Compared with the reviewed Ubuntu
baseline of 5.068–5.274 seconds, that is at least a 10.6x reduction, although the PR's complete
`ubuntu-latest` timing artifact remains the authoritative cross-machine acceptance evidence. The
subsequent complete local run preserved **3,897 / 3,897** scenarios with zero regressions, XPASS, or
performance warnings; its TCK sum was 21.215 seconds and the affected eight measured
468.353–481.613 ms.

Run the deterministic and focused gates with:

```bash
cargo test -p graphforge-rel --lib
TCK_ONLY=Quantifier cargo test -p graphforge-api --test bdd -- --no-fail-fast
```

### Multi-label predicate correction

After the XOR, CREATE, and quantifier corrections, `Match3:137:[7]` remained the unexplained
slow tail: **988.414 ms** on the standard Ubuntu runner and **401.949 ms** in a focused local run,
while the next focused `Match3` scenario took **42.479 ms**. Its graph contains only two nodes and
one relationship, so data volume did not explain the difference.

The binder represents every pattern label after the first as `'<label>' IN labels(node)`. The
generic relational path lowered `labels(node)` by constructing a string list across every known
runtime-catalog label, then applied `cypher_in`. With 17 secondary predicates and 19 catalog labels,
the TCK query expanded filtering into 323 catalog-wide membership branches before accounting for
the two projected node values.

Known literal label membership now resolves the label name once and lowers directly to
`array_has(var.type_ids, type_id)`. The fast path matches the raw Graph IR before either operand is
lowered, so the complete label list is never constructed solely for filtering. Unknown literals,
dynamic expressions, and non-label membership retain the generic three-valued `cypher_in` path;
whole-node and explicit `labels()` projections still materialize the complete string label set.

The deterministic public-API gate compares 16- and 32-label predicates. It requires exactly one
direct membership check per requested label, no `cypher_in` or `array_concat` in the filter plan,
and no more than 3x work when label count doubles. Separate result cases cover the exact TCK shape,
known-present, known-absent, unmapped, dynamic, duplicate, and optional-null membership.

The focused local `Match3` run preserved all **30 / 30** scenarios with no warning and reduced the
affected case from **401.949 ms** to **38.712 ms** (about **10.4x**). The complete post-fix Ubuntu
run reduced it from **988.414 ms** to **90.922 ms** (about **10.87x**), exceeding the required 5x
evidence gate. A subsequent complete local run preserved **3,897 / 3,897** with zero regressions,
XPASS, or timing warnings; its TCK sum was **27.962 seconds** and the affected scenario measured
**43.319 ms**.

Run the deterministic and focused gates with:

```bash
cargo test -p graphforge-api --test multi_label_scaling
TCK_ONLY=Match3 cargo test -p graphforge-api --test bdd -- --no-fail-fast
```

### Post-M17 v0.5.0 performance baseline

The committed baseline comes from the final complete `BDD — Rust` artifact for the independently
merged multi-label correction. It reports `partial: false`, runner `ubuntu-latest`, fixture profile
`pooled-isolated-serial-v1`, concurrency 1, and exactly **3,897 / 3,897** passing TCK identities with
no regressions, missing or unbaselined scenarios, XPASS, or performance findings.

| Suite | Count | Sum | Min | Mean | Median | p90 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| API BDD | 106 | 11.665 s | 28.856 ms | 110.046 ms | 141.441 ms | 175.487 ms | 179.322 ms | 199.013 ms | 199.534 ms |
| openCypher TCK | 3,897 | 66.505 s | 0.205 ms | 17.066 ms | 5.841 ms | 41.160 ms | 60.468 ms | 113.515 ms | 975.693 ms |

The slowest eight scenarios are the already-corrected quantifier-invariant cases at
**950.833–975.693 ms**; the next scenario is the large CREATE query at **442.489 ms**. Review of all
3,897 scenario durations and all 192 feature aggregates found no relative scenario or aggregate
policy breach. Summed TCK time fell from **137.581 seconds** to **66.505 seconds**. The maximum fell
from **32,966.692 ms** to **975.693 ms**, and the many-label case fell to **90.922 ms**.

The candidate generated the exact absolute boundary:

```text
ceil_100ms(max(2 × 113.515 ms, 975.693 ms + 250 ms)) = 1,300 ms
```

`tests/tck/performance_policy.json` therefore warns at **1.3 seconds** instead of **33.3 seconds**.
The existing `2x` / `+250 ms` scenario formula, `1.25x` / `+15 seconds` aggregate formula,
25-annotation cap, and warning-only behavior remain unchanged. The refreshed candidate is accepted
only after another complete Ubuntu run compares cleanly against it.

## Updating the baseline

When the advisory run reports XPASS, lock the new passes in by re-blessing:

```bash
BLESS_TCK_BASELINE=1 cargo test -p graphforge-api --test bdd
```

This rewrites `tests/tck/passing_baseline.txt` to the current passing set. `_meta.scenarios_passing`
in `coverage_matrix.json` is informational (the baseline line count). `@skip-rust` tags in the corpus
are now **vestigial** for the Rust run (it runs all); `@skip-node` still gates the Node binding's BDD
suite.

### Fast local iteration (`TCK_ONLY`)

The full corpus is ~3,900 scenarios. While iterating on one feature, restrict the run to feature
files whose path contains a substring:

```bash
TCK_ONLY=Temporal cargo test -p graphforge-api --test bdd   # only tests/tck/features/**/Temporal*.feature
TCK_ONLY=Comparison1 cargo test -p graphforge-api --test bdd # one feature → seconds (after compile)
```

When `TCK_ONLY` is set the whole-corpus **baseline gate is skipped** (a subset can't satisfy it) —
it reports `N passing of M` and writes a `partial_not_compared` timing artifact. Always do a final
unfiltered run before pushing. CI never sets `TCK_ONLY`, so it always runs and gates the whole corpus.

## Documented xfails

**None.** GraphForge targets 100% of the vendored corpus with every exercised feature implemented as
Cypher — `CALL … YIELD` procedures included. The analyst verbs (`rank`, `cluster`,
`paths`, `analyze`, `similar`, `find`) are an *additional* surface, not a
replacement for Cypher `CALL`, so no TCK scenario is excluded by design.
If a future feature is ever deemed out-of-scope it would be enumerated here; the set is currently empty.

---

*Conformance is measured against the vendored openCypher 2024.3 corpus on the
v0.5.0 Rust core.*
