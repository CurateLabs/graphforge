# GDC / LDBC suite index (benchmark workspace)

This index is owned by the isolated `benchmarks/` workspace. It inventories the
Graph Data Council (GDC, formerly LDBC) portfolio as independently selectable
suites. Product crates never embed generators, drivers, or SPARQL approximations.

Authoritative product-facing specification remains
[`docs/guide/datasets/ldbc.md`](../docs/guide/datasets/ldbc.md). This page is the
**harness index**: which suites exist, how they are selected, and which are
executable versus inventory-only.

## Suites

| Suite id | Declaration | Disposition | Notes |
|---|---|---|---|
| `graphalytics` | `suites/gdc-graphalytics.json` | executable | Six-algorithm analytics via `gdc-graphalytics` (#961) |
| `snb-interactive` | `suites/gdc-snb-interactive.json` | executable | SNB Interactive operations via `gdc-snb-interactive` (#962) |
| `snb-bi` | `suites/gdc-snb-bi.json` | executable | SNB Business Intelligence via `gdc-snb-bi` (#963) |
| `finbench-transaction` | `suites/gdc-finbench-transaction.json` | executable | FinBench Transaction operations via `gdc-finbench-transaction` (#964) |
| `spb` | `suites/gdc-spb.json` | **inventory_only** | Semantic Publishing Benchmark (RDF/SPARQL) |

Shared identity and acquisition contracts live in
`graphforge_bench.gdc_contracts` (#960). Suite adapters share those contracts
without sharing workload semantics.

## Graphalytics

**Home:** [ldbcouncil.org/benchmarks/graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/)

| Item | Value |
|---|---|
| Algorithms | BFS, PR, WCC, CDLP, LCC, SSSP |
| Runner | `graphforge-benchmark-gdc-graphalytics` (`suites/gdc-graphalytics.json`) |
| Ladder | `profiles/gdc/graphalytics-ladder.json` (begins with bounded `ga-tiny`) |
| Validation | exact (BFS/CDLP), equivalence (WCC), epsilon=1e-4 (PR/LCC/SSSP) |
| Execution | Explicit `run-live` in-memory public API proof; `run-suite` is static replay |
| Unsupported semantics | Typed `semantic_incompatibility` (fixed-iteration PR; synchronous CDLP; directed LCC normalization) |
| Identity | Distinct pins: `profiles/gdc/graphalytics-static-identity.json` (historical `wiki-Talk` markers) and `profiles/gdc/graphalytics-live-identity.json` (`ga-tiny` proof). Suite/acquisition select the profile; cross-use is `identity_drift`. |
| Scorecard | `profiles/gdc/graphalytics-scorecard-identity.json` pins the `wiki-Talk`, `cit-Patents`, `datagen-7_5-fb` and `graph500-22` archives; `profiles/gdc/graphalytics-scorecard-ladder.json` carries the LDBC-published counts, load mappings and each archive's `.properties` facts; `profiles/gdc/graphalytics-scorecard-ladder-spec.json` is the rung runner's ladder (see Scorecard ladders). Acquisition and conversion are described in `README.md` (GDC dataset acquisition and conversion). |
| Authority | Synthetic `ga-tiny` engineering evidence only (edges-only fixture; isolated vertices out of scope); `certification=false`; legacy `wiki-Talk` stub excluded |

Profiles, validation, and evidence stay under the GDC Graphalytics suite and are
not shared with Graph500 orchestration.

```bash
CARGO_TARGET_DIR=target cargo build --locked -p graphforge-benchmark-gdc-graphalytics
PYTHONPATH=harness GRAPHFORGE_GDC_GRAPHALYTICS_BIN=target/debug/graphforge-benchmark-gdc-graphalytics \
  uv run --locked python -m unittest tests.test_gdc_graphalytics
```

## SNB Interactive

**Home:** [ldbcouncil.org/benchmarks/snb](https://ldbcouncil.org/benchmarks/snb/)

| Item | Value |
|---|---|
| Operations | Complex reads IC1–IC14, short reads IS1–IS7, updates IU1–IU8 (29 total) |
| Runner | `graphforge-benchmark-gdc-snb-interactive` (`suites/gdc-snb-interactive.json`) |
| Phases | Separate `load`, `warmup`, `execution`, `validation` with per-phase status/detail |
| Fixtures | `snb-interactive-static-synthetic-v1` is static replay; `snb-interactive-live-is1-synthetic-v1` and `snb-interactive-query-synthetic-v1` (`fixtures/gdc/snb-interactive-queries/`) are synthetic engineering graphs, not official Datagen output |
| Queries | `src/queries.rs` defines IC1–IC13 and IS1–IS7 as data (operation, Cypher text or IC13 `bfs` invocation, typed parameters, columns, `LIMIT`, rewrites); `list-queries` prints them as JSON |
| Validation | Live IS1 normalizes real Arrow rows and uses the same exact Rust reference validator; `run-live-queries` runs all 20 reads on the query fixture and compares typed rows in order with results `graphforge_bench.gdc_snb_interactive_reference` derives from the fixture without GraphForge |
| Unsupported semantics | Typed `semantic_incompatibility`: `interactive_update_stream_not_exposed` (IU1–IU8); `weighted_interaction_path_enumeration_not_exposed` (IC14) |
| Scorecard | `profiles/gdc/snb-interactive-scorecard-identity.json` pins the v1 `CsvComposite-LongDateFormatter` SF1/SF10 archives, their substitution parameters and the Neo4j validation parameters; `profiles/gdc/snb-interactive-load-mapping.json` and `snb-interactive-scorecard-ladder.json` carry the mapping and the LDBC-published counts. The mapping keeps the v1 reference data model the queries assume (#952 decision 2026-10-07): the archives' epoch-millisecond dates load as `int64`, one label per node. See `README.md` (LDBC CSV suite pins). |

Read-only complex/short reads follow the Cypher reference implementation at the
pinned driver commit (ordering, tie-breakers, `LIMIT`, parameter names); IC13
uses the public `bfs` path analyst verb. Where GraphForge evaluates a reference
construct differently (`shortestPath`, node-list `IN`,
`datetime({epochMillis})`, pattern predicates outside `WHERE`), the query uses
an exactly equivalent form recorded in its definition. Where the reference's
behaviour differs from the specification prose (for example IS7's
`CASE r WHEN null`, which never matches), the reference behaviour is kept,
because the v1 validation set was produced by it, and labelled `spec_variance`. Updates
require the official driver's transactional update-stream semantics,
dependency-time ordering, and write validation, which the public property-graph +
Cypher surface does not expose, so they fail closed with a typed cause. IC14
requires all-shortest-path enumeration with a dynamically computed interaction
weight and likewise fails closed. Profiles, validation, and evidence stay under
the GDC SNB Interactive suite and are not shared with Graph500, Graphalytics,
SNB BI, or FinBench. Evidence records `certification: false`: these are
engineering runs and never masquerade as an audited GDC certification.

```bash
CARGO_TARGET_DIR=target cargo build --locked -p graphforge-benchmark-gdc-snb-interactive
PYTHONPATH=harness GRAPHFORGE_GDC_SNB_INTERACTIVE_BIN=target/debug/graphforge-benchmark-gdc-snb-interactive \
  uv run --locked python -m unittest tests.test_gdc_snb_interactive
```

## SNB BI

**Home:** [LDBC Social Network Benchmark](https://ldbcouncil.org/benchmarks/snb/).

| Item | Value |
|---|---|
| Official focus | LDBC SNB Business Intelligence: analytical read queries over the social-network graph plus a batch maintenance stream |
| Queries | 20 analytical reads `BI1`..`BI20` |
| Maintenance | Batch inserts `INS1`..`INS8` and batch deletes `DEL1`..`DEL8` |
| Runner | `runners/gdc-snb-bi` (`gdc-snb-bi`), executes reads through the public `graphforge-api` |
| GraphForge disposition | `executable` (bounded tiny fixture only) |
| Certification | **false** — engineering evidence only; never masquerades as an audited GDC certification |
| Scorecard | `profiles/gdc/snb-bi-scorecard-identity.json` pins the `composite-projected-fk` SF1/SF10 archives, LDBC's member md5 lists, the SF1–SF30000 parameters and the Umbra SF10 validation output; `profiles/gdc/snb-bi-load-mapping.json` and `snb-bi-scorecard-ladder.json` carry the mapping and the LDBC-published counts. See `README.md` (LDBC CSV suite pins). |

### Query mapping

Seventeen analytical reads run as Cypher through the public API. Their texts
live in `runners/gdc-snb-bi/src/queries.rs` and follow the LDBC SNB BI
reference queries (`neo4j/queries/bi-N.cypher`, with semantics cross-checked
against `umbra/queries/bi-N.sql`). Where GraphForge cannot run the upstream text
as written, the definition records the exact rewrite and why the result is
unchanged. `run-queries` executes them on the committed `snb-bi-queries`
fixture and compares every result with rows derived independently from the
fixture CSV files.

### Unsupported policy (fail closed)

The runner never approximates semantics the public property-graph + Cypher
surface does not expose. It fails closed with a typed cause instead:

- `weighted_shortest_path_not_exposed` — `BI15`, `BI19`, and `BI20` require a
  weighted shortest-path search over a dynamically computed edge-weight
  function; the public surface exposes only unweighted single-path analyst
  verbs and pattern matching.
- `bi_batch_update_stream_not_exposed` — the entire `INS*`/`DEL*` maintenance
  stream requires the official driver's transactional insert/delete semantics,
  dependency-time ordering, and cascading deletes.

### Validation

Compatible reads are validated against pinned references either exactly
(row-for-row for a spec-mandated total order) or order-insensitively
(`normalized`) for grouped/set-shaped aggregations without a total tie-break.

### Resource vs. correctness separation

Evidence records per-phase resource metrics (`load`, `query`, `spill`, `rss`,
`io`) in a distinct top-level `resources` section, kept separate from the
per-operation correctness `operations`. Resource evidence is never mixed into a
correctness verdict.

### Opt-in large scale factors

The default suite runs only the bounded `snb-bi-sf0.003` tiny fixture. Any
larger scale factor is opt-in / external and never baked into the default
declaration or pinned identity.

```bash
CARGO_TARGET_DIR=target cargo build --locked -p graphforge-benchmark-gdc-snb-bi
PYTHONPATH=harness GRAPHFORGE_GDC_SNB_BI_BIN=target/debug/graphforge-benchmark-gdc-snb-bi \
  uv run --locked python -m unittest tests.test_gdc_snb_bi
```

## FinBench Transaction

**Home:** [ldbcouncil.org/benchmarks/finbench](https://ldbcouncil.org/benchmarks/finbench/)

| Item | Value |
|---|---|
| Operations | Complex reads TCR1–TCR12, simple reads TSR1–TSR6, writes TW1–TW19, read-writes TRW1–TRW3 (40 total) |
| Runner | `graphforge-benchmark-gdc-finbench-transaction` (`suites/gdc-finbench-transaction.json`) |
| Phases | Separate `load`, `warmup`, `execution`, `validation` (see evidence `phases`) |
| Bounded fixture | `finbench-engineering-tiny-v1` (synthetic engineering data; not an official scale factor) |
| Live lane | Trusted Rust runner owns in-memory `GraphForge::new(None)` load; `run-queries` executes every read over `finbench-engineering-queries-v1`, `run-live` the pinned TCR10 seed |
| Query catalog | `list-queries`: per read, Cypher text, typed parameters, result columns, ordering and truncation (`queries.rs`) |
| Validation | exact (ordered rows) and normalized (order-insensitive multiset) reference comparison |
| Unsupported semantics | Typed `semantic_incompatibility`: `finbench_transaction_write_semantics_not_exposed` (TW1–TW19, TRW1–TRW3); `truncation_order_not_supported` for any `truncationOrder` other than `TIMESTAMP_DESCENDING` |
| Scorecard | `profiles/gdc/finbench-transaction-scorecard-identity.json` pins the SF1/SF10 archives and their read parameters (LDBC publishes no reference output); `profiles/gdc/finbench-transaction-load-mapping.json` maps `snapshot/` and `finbench-transaction-scorecard-ladder.json` carries the LDBC-published counts. See `README.md` (LDBC CSV suite pins). |

Every read (TCR1–TCR12, TSR1–TSR6) is exact public Cypher, including the
specification's `truncationLimit`. Time windows are open
(`startTime < timestamp < endTime`), amount thresholds strict, and calculated
floats rounded to three decimals. Truncation follows the FinBench specification
and the 2026-10-07 decision on #952: when a step expands from a vertex, only the
`truncationLimit` newest edges of that type and direction at the vertex are
traversed, before the window and amount filters, so truncation is a property of
the vertex rather than of the path. Ties on `timestamp` are broken by the far
endpoint's id ascending; the specification leaves ties undefined, so every
truncated definition labels this as an accepted variance (`tie_break_variance`).
The LDBC parameter generator emits limit 500 and `TIMESTAMP_DESCENDING`; other
orders are refused, never reordered. Each query definition names the steps it
truncates, and its `semantics` field states the specification reading it
implements. The scorecard checks these readings against the spec-derived SF1
reference (see "FinBench Transaction SF1 reference" below), not an LDBC
implementation; where GPStore reads the specification differently (TCR1, TCR2,
TCR5, TCR8, TCR9, TCR11), `reference_reading` says how, for information.
`workarounds` cites the GraphForge defect (#1887)
or unsupported construct (#1888) behind any Cypher that departs from the direct
form.

TCR1, TCR2 and TCR5 (monotonically increasing transfer timestamps along a 1–3
hop trace) use per-vertex admissible-edge lists plus a list predicate over each
path's hops. TCR3 (temporally filtered shortest path) is an unbounded
variable-length match with a window predicate; its minimum length is the
shortest path. TCR4 is plain
pattern matching and aggregation. TCR3 and TCR11 enumerate edge-distinct paths
of unbounded length, which is exact but may exceed the rung envelope on large
graphs; the scorecard lane reports that as a resource failure, never a wrong
answer. Write queries (TW1–TW19) and read-write transactions (TRW1–TRW3) require
the official driver's ACID transaction, insert/delete/in-place-update stream,
and read-before-write risk semantics, which the public property-graph + Cypher
surface does not expose, so they fail closed with
`finbench_transaction_write_semantics_not_exposed`.

The query fixture `fixtures/gdc/finbench-transaction-queries` holds a
FinBench-shaped graph, parameter bindings per read (including bindings where
truncation changes the answer), and `expected.json`, which
`graphforge_bench.gdc_finbench_transaction_reference` derives procedurally from
the graph without running GraphForge. The benchmark unittest regenerates it and
fails on drift, then runs every binding live and compares rows.

The suite pins the upstream FinBench specification tag `v0.1.0` at
`d3ec7036bf6919df8cd3eeaa3a986048e779ea02`, DataGen `0.1.0` at
`eddcc0551861eaefeb9b37497b10de1bb0f52672`, and driver `0.1.0` at
`27e5640f47e91c783112ca654f670d15863780a6`. The committed graph is a small
FinBench-shaped synthetic engineering fixture authored in this repository, not
an official `SF0.01` dataset. The Rust benchmark runner is an internal driver,
not the upstream FinBench driver and not a certification implementation.

The explicit Python `run_live_suite` lane only orchestrates the trusted Rust
`run-live` command. The runner itself opens in-memory `GraphForge`, loads the
committed seed, and executes official TCR10 (`pid1`, `pid2`, open
`startTime < timestamp < endTime` window, single `jaccardSimilarity` column
rounded to three decimals). A static JSON envelope or `.out` file cannot claim
live execution. The reference `0.667` is independently derived from the seed
(`|{10,11} ∩ {10,11,12}| / |union| = 2/3`). TW1 remains typed
unsupported in the same evidence document, and correctness, resource, and
harness lanes stay distinct. The older `run-suite` command is retained only as
an explicitly marked `static_replay` regression lane and cannot satisfy live
evidence. Its compatible TCR10 reference and system output use the same official
single-column `jaccardSimilarity` value `0.667`; the reference-mismatch TCR10
output is a deliberately wrong Jaccard (`0.500`) so normalized and exact
comparison both fail closed. Obsolete company-ID rows are not a valid TCR10
result schema.

Evidence separates three failure classes and never conflates them: a
**correctness** mismatch (`correctness_failed`), a **resource**-limit event
(`resource_exceeded`, surfaced in the dedicated `resource_events` section), and a
**harness/runner** error (`harness_error`, surfaced in the dedicated
`harness_failures` section, e.g. a malformed job or a missing system output).
Each class has a distinct per-operation status and its own top-level section, so
a correctness mismatch, a resource-limit event, and a runner failure remain
independently attributable. Profiles, validation, and evidence stay under the
GDC FinBench Transaction suite and are not shared with Graph500, Graphalytics,
SNB Interactive, or SNB BI. Evidence records `certification: false`: these are
engineering runs and never masquerade as an audited GDC certification.

```bash
CARGO_TARGET_DIR=target cargo build --locked --manifest-path Cargo.toml -p graphforge-benchmark-gdc-finbench-transaction
PYTHONPATH=harness GRAPHFORGE_GDC_FINBENCH_TRANSACTION_BIN=target/debug/graphforge-benchmark-gdc-finbench-transaction \
  uv run --locked python -m unittest tests.test_gdc_finbench_transaction
```

### FinBench Transaction SF1 reference

LDBC publishes no FinBench reference output, and the third-party validation
files interleave writes. Per the #952 decision (revised 2026-10-08), the SF1
reference comes from the spec-derived module
`graphforge_bench.gdc_finbench_transaction_reference`, which never runs
GraphForge. The card says "checked against a spec-derived reference, not an
LDBC implementation" (#1894).

```bash
make -C benchmarks gdc-finbench-reference DATASET_CACHE=/home/ubuntu/gdc-cache \
  OUTPUT=/home/ubuntu/gdc-cache/finbench-reference/sf1-reference.json
```

The target reads the `snapshot/` CSV and `sf1_read_params/` that `gdc-acquire`
extracted from the pinned archives. It writes a new
`graphforge-gdc-rung-reference/1` document, which is never overwritten, and
`OUTPUT.sha256` with the digests of the module, all 30 input files and the
output. The output stays out of the repository; its digest and the manifest
are recorded on #1894.

- **Bindings.** These are the twelve `complex_<n>_param.csv` files, TCR1–TCR12
  (10,689 bindings). They have no header: eleven start with a literal `...`
  line, which is not a binding. Binding ids are `line-<n>`, the line in the
  file. A scorecard workload must use `read_ldbc_parameters` so the ids agree.
  No simple-read (TSR) parameters are published, because the LDBC driver
  derives them during a run, so the reference covers the complex reads only.
  Every published binding uses threshold `0.0`, limit 500 and the window
  `1627020616747..1669690342640`.
- **Vertices** are keyed by label and id, because LDBC ids repeat across labels
  (Person and Company share 1,376; Account and Loan share 190). Timestamps are
  naive-UTC `createTime` values converted to epoch milliseconds.
- **Cells** use the query driver's form: Arrow display text. Float64 cells use
  ryu's shortest round-trip layout, as arrow-cast does. Matching is `exact`.
  Sums are exactly rounded (`math.fsum`) before half-up rounding to three
  decimals, so no answer depends on summation or hash order.
- **Pins.**

  | Item | SHA-256 |
  |---|---|
  | `sf1.tar.gz` (identity profile) | `598d82e0bc442150629f3db7b6c6942a3f1bf6414cbc2f4e8ac6d2da89884636` |
  | `sf1_read_params.zip` (identity profile) | `ca624fc25ef56b22819739e9ee34c49944fc902dd5a7eb8b6f24819e558c50bf` |
  | module at generation | `0e3af90f494faa5bd53d7b9696ec8bf057083d70ae7c5c9dd4528e0c97146241` |
  | `sf1-reference.json` (43,860,695 bytes) | `9c73fb5baa71136696fc598f42852505595b1f2b9ea25ae8ae8e5644ec0f822a` |

  The run used Python 3.13.12 from `uv.lock`. Two runs, each in a separate
  process with its own hash seed, produced byte-identical output. Each run took
  about 40 s of wall time on one core, with a peak RSS of 1.5 GiB.
- **Scope.** The reference covers `snapshot/` only. It excludes the
  `incremental/` rows, consistent with the load mapping.
- **Readings worth stating**, at SF1, for information only:
  - TCR6 counts "more than 3 transfer-ins" as transfer edges, not distinct
    source accounts. Galaxybase, GPStore and Ultipa read it the same way;
    counting distinct sources would change 901 of 905 bindings.
  - TCR11 follows guarantee chains of any length ("until end", as GPStore
    does). Galaxybase, TuGraph and Ultipa stop at 5 hops, which changes 27 of
    983 bindings (the deepest chain is 11). This is the reference's only
    disagreement with a third-party result.
  - Truncation changes 12 answers: TCR6 lines 99, 118, 262, 374 and 686, and
    TCR8 lines 101, 278, 360, 392, 779, 848 and 903. TCR6 lines 262 and 686
    also depend on truncating the adjacency before the window and amount
    filters. No vertex has a timestamp tie at the 500-edge cut-off, so the
    far-endpoint-id tie-break variance changes no answer. None of the 12 is in
    Ultipa's validation set, so the truncation reading has no independent
    corroboration at SF1. It does agree with the Neo4j truncation example,
    which computes cut-offs before filtering.
  - TCR1: GPStore reports each account once, at its first breadth-first
    distance. 6 of 737 bindings have an account at several trace lengths.
  - TCR5: GPStore keeps traces that revisit an account. 43 of 1,000 bindings
    have one.
  - TCR8: GPStore counts every in-window edge from an expanded account into the
    destination as inflow and expands each account once. This difference is
    not quantified.
  - GPStore's extra truncations of own, deposit, repay and apply edges cannot
    apply at SF1, because those adjacencies have at most 22 edges.
- **Independent review of #1906.** Against Ultipa's `validation_params.csv`
  (821 complex-read bindings, with its interleaved writes replayed), 818 match;
  the 3 misses are TCR11 bindings that the 5-hop cap explains exactly. A
  separate Decimal re-implementation of TCR1–5, 7, 9, 10 and 12 matched all
  7,801 of its bindings.

## SPB (Semantic Publishing Benchmark)

**Home:** listed under [GDC Benchmarks](https://ldbcouncil.org/benchmarks/).

| Item | Value |
|---|---|
| Official focus | RDF / SPARQL stores over a media-publishing ontology |
| Protocol | Official SPB driver + SPARQL query/update mix (upstream-owned) |
| GraphForge disposition | `inventory_only` |
| Semantic reason | `rdf_sparql_outside_property_graph_cypher_surface` |
| Harness behavior | Report inventory status only; never advertise an executable SPB profile; never approximate SPARQL with Cypher |

### Inventory-only rationale

GraphForge’s current product surface is property-graph persistence with Cypher
and analyst verbs. SPB requires RDF storage and SPARQL evaluation. Translating
SPARQL into Cypher, inventing a fake RDF binding, or shipping a partial
property-graph “SPB-like” runner would be an incompatible approximation and is
forbidden.

### Activation criteria (objective)

An executable SPB adapter may be added only when **all** of the following are
true and recorded in evidence:

1. `product_exposes_supported_rdf_or_sparql_binding` — a supported product
   binding can evaluate the SPB protocol without Cypher approximation.
2. `official_spb_spec_and_driver_pins_recorded` — immutable upstream spec and
   driver identities are pinned through the GDC contracts.
3. `reference_validation_path_exists_without_cypher_approximation` — official
   reference validation is available without inventing substitute semantics.

Until then, `suite_status("spb")` returns `disposition=inventory_only`,
`executable=false`, and the semantic reason above.

## Scorecard ladders

Each executable suite climbs its own scale ladder for the #952 scorecards with
`graphforge_bench.gdc_rung` (operator commands: `README.md`, GDC scorecard
ladders). A suite registers its ladder with one
`graphforge-gdc-scorecard-ladder-spec/1` document
(`schemas/gdc-scorecard-ladder-spec.json`): the identity profile to acquire
from, its count ladder (`profiles/gdc/*-scorecard-ladder.json`), the card's
one-line variance and attribution, and per rung the query workload, the
queries refused at mapping with their typed causes, the pinned reference
(`graphforge-gdc-rung-reference/1`, `schemas/gdc-rung-reference.json`) or a note
saying why there is none, and the variances (`rewrite`, `spec_variance`,
`reference_reading`, `discrepancy`, `scope`).

One rung:

1. **Admit.** A 60 s quiet-host window, then free space on the work root above
   the declared reserve, as for a Graph500 rung. A refusal is recorded as
   `not_admitted` and stops the ladder; a busy host launches nothing.
2. **Acquire** the rung's archive from the pinned dataset cache
   (`gdc_dataset_cache`); a pin mismatch is `checksum_mismatch`.
3. **Convert, load, query**: three BenchExec runs of the
   `graphforge-gdc-rung-phase-v1` definition, each executing
   `graphforge_bench.gdc_phase` on one task. `convert` runs the converter;
   `load` runs `gf import-session` begin, register, validate and commit;
   `query` runs `gf storage-attribution`, then the query driver, which reopens
   the project, reconciles every count and times each query. The three phases
   share the rung's four-hour wall.
4. **Expected counts** come from the count ladder: Graphalytics reconciles to
   `listed_edges`; SNB and FinBench reconcile every table to `listed`, the
   records of the pinned archive's loaded snapshot (the #952 reconciliation
   rule). A column-labelled node table's per-label split is the converter's,
   accepted only when it sums to the ladder's table count.
5. **Check** every referenced result against the reference: `exact` (every
   cell, or with key columns every reference cell in the row with the same
   key), `epsilon` (Graphalytics' `|r - s| <= epsilon * |r|` on numeric cells,
   rows paired by key columns) or `equivalence` (the same partition up to
   relabelling). A written result must reproduce its measured digest first.
6. **Tear down**: reclaim `workspace/gdc-<suite>-<rung>` by path and inventory
   the work root. The dataset cache is outside the work root and is kept.

A rung passes only with no failure and an empty inventory. Typed causes include
`rung_wall_exceeded`, `memory_limit_exceeded` (a phase's largest
single-process peak RSS above 4 GiB, or BenchExec's memory stop),
`host_swapped`, `convert_failed`, `load_failed`, `count_mismatch`,
`query_failed`, `reference_mismatch`, `result_digest_mismatch` and
`teardown_incomplete`. A query that fails at runtime or answers wrongly fails
the rung, but every query still runs, so the result lists every failure. A
refused query counts against coverage and is never checked.

Each rung publishes, under `<suite>-<rung>-`: three `*-benchexec.json`
documents (`graphforge-benchexec-run/1`), `expected-counts.json`,
`query-evidence.json`, `correctness.json`, `inventory.json` and
`result.json` (`graphforge-gdc-rung-result/1`, listing every document's
SHA-256). The card (`<suite>-card.json`, `schemas/gdc-scorecard-card.json`, and
`<suite>-card.txt`) headlines the largest passing rung and names the next
rung's outcome. Its numbers come from those documents only, and
`gdc_measurement_policy.assert_card_metric_sources` refuses any other
authority: load and conversion time are BenchExec wall time; latency and
throughput are the query driver clock; peak RSS is each phase's largest
process high-water mark under BenchExec (BenchExec's cgroup peak counts page
cache on the host's full-access mounts, as for the Graph500 ladder); on-disk
bytes are the storage-attribution receipt's allocated bytes; graph counts are
the driver's reconciliation after reopen. Graphalytics cards report `Tl`, `Tp`
(the mean of three driver-clock runs per algorithm) and EVPS instead of
throughput and latency; makespan is labelled not measured, with the reason
below.

`fixtures/gdc/rung-fixture/` is a three-rung CI ladder in the SNB Interactive
v1 CSV shape: sf0 passes, sf1 fails (one query fails at runtime and one
reference answer is wrong) and sf2 is never attempted.
`tests/test_gdc_rung.py` drives it with the real converter, `gf` and driver;
only BenchExec is replaced, because CI runners cannot delegate cgroups.

### Graphalytics scorecard ladder

`profiles/gdc/graphalytics-scorecard-ladder-spec.json` climbs wiki-Talk (2XS),
cit-Patents (XS), datagen-7_5-fb (S) and graph500-22 (S) from the pinned
archives (`graphforge_bench.gdc_graphalytics_scorecard`):

- **Archive check.** Before converting, the archive's `<graph>.properties` must
  agree with the count ladder (`graphalytics-scorecard-ladder.json`: vertices,
  published edges, direction, weight, the `algorithms` list and the BFS and
  SSSP source vertices) and with the rung's workload: every listed algorithm is
  run or refused, and each run is dispatched as the graph needs. Otherwise the
  rung fails with `archive_properties_mismatch`.
- **Algorithms** (`graphalytics-scorecard-workload-<graph>.json`), each run
  three times after one excluded warm-up: BFS as `paths(by=bfs)` and SSSP as
  `paths(by=dijkstra, weight=weight)`, each from the source vertex selected by
  `NodeSelector::Uuid` (the converter's node UUID for that vertex); WCC as
  `cluster(by=components)`; LCC, on undirected graphs only, as
  `rank(by=clustering_coefficient)`. Each variant keeps only its answer
  columns (`target_uuid, cost` or `id, <value>`); the call is timed whole.
  PR (`fixed_iteration_pagerank_not_exposed`), CDLP
  (`synchronous_cdlp_not_exposed`) and directed LCC
  (`directed_lcc_semantics_not_exposed`) are refused and count against
  coverage.
- **Reference.** The rung spec's `{"archive_outputs": "graphalytics"}` derives
  the reference at check time from the archive's `<graph>-<ALGORITHM>` files.
  Each must list every vertex exactly once (`reference_invalid` otherwise).
  BFS matches exactly and SSSP within epsilon 1e-4, keyed by target UUID;
  `paths` returns only reached vertices, so a vertex the reference marks
  unreachable (`9223372036854775807`, `infinity`) matches by its absence. WCC
  matches by equivalence and LCC within epsilon 1e-4, keyed by vertex id. The
  correctness record's `reference_sha256` covers the `.properties` file and
  every reference output used.
- **Metrics.** `Tp` is the mean of the three measured runs and EVPS is
  `(vertices + edges) / Tp`. Makespan is labelled not measured: Graphalytics
  defines it as the time from issuing one algorithm job to its output for a
  cold system started for that job, and the driver runs every job warm in one
  process after one project open, so no such interval exists; the query
  phase's BenchExec wall spans all jobs together.

`fixtures/gdc/graphalytics-rung-fixture/` holds three tiny archives in the same
format with hand-derived references: a directed graph (BFS, WCC), an
undirected weighted one (BFS, WCC, LCC, SSSP) and a directed one with one
wrong BFS depth, where the ladder stops with `reference_mismatch`.
`tests/test_gdc_graphalytics_scorecard.py` drives it through the rung runner.

## Operator status query

```bash
PYTHONPATH=harness uv run --locked python - <<'PY'
from graphforge_bench.gdc_contracts import suite_status, assert_no_executable_spb_profile
print(suite_status("spb"))
assert_no_executable_spb_profile()
PY
```
