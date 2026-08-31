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
| `snb-bi` | `suites/gdc-snb-bi.json` | executable | SNB Business Intelligence; runner `gdc-snb-bi` (see [SNB BI](#snb-bi)) |
| `finbench-transaction` | `suites/gdc-finbench-transaction.json` | executable | FinBench Transaction; blocked on suite issue #964 |
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
| Unsupported semantics | Typed `semantic_incompatibility` (fixed-iteration PR; synchronous CDLP) |

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
| Phases | Separate `load`, `warmup`, `execution`, `validation` (see evidence `phases`) |
| Bounded fixture | `snb-sf0.003` (every ladder/run begins on this tiny dataset) |
| Validation | exact (ordered rows) and normalized (order-insensitive multiset) reference comparison |
| Unsupported semantics | Typed `semantic_incompatibility`: `interactive_update_stream_not_exposed` (IU1–IU8); `weighted_interaction_path_enumeration_not_exposed` (IC14) |

Read-only complex/short reads that are ordinary graph traversals or aggregations
map to public Cypher; IC13 uses the public `bfs` path analyst verb. Updates
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
| Runner | `runners/gdc-snb-bi` (`gdc-snb-bi`), serde-only control plane; no product-crate dependency |
| GraphForge disposition | `executable` (bounded tiny fixture only) |
| Certification | **false** — engineering evidence only; never masquerades as an audited GDC certification |

### Query mapping

Analytical reads that are ordinary traversals, aggregations, grouped counts,
and top-k rankings map to the public GraphForge Cypher / analyst-verb surface.
The runner records the concrete Cypher/analyst-verb shape for each compatible
read.

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

## Operator status query

```bash
PYTHONPATH=harness uv run --locked python - <<'PY'
from graphforge_bench.gdc_contracts import suite_status, assert_no_executable_spb_profile
print(suite_status("spb"))
assert_no_executable_spb_profile()
PY
```
