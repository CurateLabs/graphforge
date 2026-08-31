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
| `snb-interactive` | `suites/gdc-snb-interactive.json` | executable | SNB Interactive via `gdc-snb-interactive` (#962) |
| `snb-bi` | `suites/gdc-snb-bi.json` | executable | SNB Business Intelligence; blocked on suite issue #963 |
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
| Catalog | Interactive v1: IC1–IC14, IS1–IS7, IU1–IU8 (29 ops) |
| Runner | `graphforge-benchmark-gdc-snb-interactive` (`suites/gdc-snb-interactive.json`) |
| Ladder | `profiles/gdc/snb-interactive-ladder.json` (begins with `snb-sf0.003`) |
| Phases | load → warmup → execution → validation (never skipped) |
| Supported engineering map | IS1, IS3, IS4 → public Cypher `query` |
| Completeness policy | `full_catalog_declare_gaps` (gaps reported, not omitted) |
| Certification | `run_class=engineering`, `audited_gdc_certification=false` only |

Unsupported complex reads and update-stream ops fail closed with typed
`semantic_incompatibility`. Engineering evidence never claims audited GDC
certification.

```bash
CARGO_TARGET_DIR=target cargo build --locked -p graphforge-benchmark-gdc-snb-interactive
PYTHONPATH=harness GRAPHFORGE_GDC_SNB_INTERACTIVE_BIN=target/debug/graphforge-benchmark-gdc-snb-interactive \
  uv run --locked python -m unittest tests.test_gdc_snb_interactive
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
