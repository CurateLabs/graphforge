# LDBC Full Suite (spec)

**Last updated:** 2026-08-05

> **Status:** Specification in GraphForge docs. Generators, drivers, and audited
> runs execute in an **external scale harness** — not GraphForge core CI.
> Convenience catalog loaders (`graphforge.datasets`) remain a planned extension
> ([Datasets overview](overview.md)); this page is the workload **contract**,
> not a shipped loader.

The [Graph Data Council (GDC)](https://ldbcouncil.org/) (formerly Linked Data
Benchmark Council / LDBC) maintains the **LDBC benchmark suite**. Workloads
remain branded “LDBC benchmarks” after the 2025 rename. This document inventories
the **full official suite** as published on [ldbcouncil.org/benchmarks](https://ldbcouncil.org/benchmarks/)
and defines how GraphForge treats it at **spec level**.

## Spec vs harness

| Concern | GraphForge (this repo) | External scale harness |
|---|---|---|
| Which benchmarks constitute the suite | This page | Follows this inventory |
| Dataset / SF / algorithm contracts | This page + links to GDC specs | Runs official generators/drivers |
| Pass criteria / validation / auditing concepts | Spec-level summary below | Implements drivers + validation |
| Size labeling | [GSI](../../reference/graph-scale-index.md) after load | Emits GSI on evidence |
| Normal CI | Must **not** run full suite | Optional dedicated runners |

Do **not** bulk-add LDBC Spark/Hadoop generators or JDBC/driver stacks into
GraphForge core.

---

## Suite inventory (authoritative portfolio)

Per GDC’s published benchmarks page (verified 2026-08-05):

| Benchmark | Focus | Workloads (current) | Primary data |
|---|---|---|---|
| **SNB** — Social Network Benchmark | Property-graph DBMS (OLTP + OLAP-style) | **Interactive** (v1 / v2) and **Business Intelligence (BI)** | Synthetic social network (Datagen) |
| **Graphalytics** | Graph analysis platforms | Six core algorithms + reference outputs | Standard graph datasets (+ optional Graph500-class graphs) |
| **FinBench** — Financial Benchmark | Distributed transactional graph DBMS | **Transaction** workload (Analytics workload: future work) | Synthetic financial graph |
| **SPB** — Semantic Publishing Benchmark | RDF / SPARQL stores | Media-publishing ontology workload | RDF datasets |

GraphForge’s product surface is **property-graph + Cypher / analyst verbs**. For
harness planning:

- **In scope for GraphForge-facing runs:** SNB Interactive, SNB BI, Graphalytics,
  FinBench Transaction (as the SUT allows).
- **Suite completeness still lists SPB**, but SPB is RDF/SPARQL — treat as
  **out of GraphForge product path** unless a separate RDF binding exists.
  Document SPB so “full suite” means the GDC portfolio, not a silent omission.

Historical note: graph-analytics work that once lived near early SNB drafts was
delegated to **Graphalytics** as the official LDBC analytics benchmark.

---

## 1. Social Network Benchmark (SNB)

**Home:** [ldbcouncil.org/benchmarks/snb](https://ldbcouncil.org/benchmarks/snb/)  
**Specification:** [LDBC SNB specification (PDF)](https://ldbcouncil.org/ldbc_snb_docs/ldbc-snb-specification.pdf)  
**Generator (current):** [ldbc_snb_datagen_spark](https://github.com/ldbc/ldbc_snb_datagen_spark)  
**Docs / query repos:** [ldbc_snb_docs](https://github.com/ldbc/ldbc_snb_docs), Interactive / BI impls under [github.com/ldbc](https://github.com/ldbc)

### Shared dataset

Interactive and BI share a social-network schema (Person, Message/Post/Comment,
Forum, Organisation, Place, Tag, TagClass, and typed relationships such as
`knows`, `likes`, `hasCreator`, …). Exact attribute sets and cardinality rules
are owned by the SNB specification — do not invent alternate schemas for
“LDBC-compatible” claims.

### Scale factors

SF is defined by **serialized CSV size in GiB** (not GSI Size Tags). The
specification publishes SF sets including small validation factors
(`0.003`, `0.1`, `0.3`) and production factors
(`1`, `3`, `10`, `30`, `100`, `300`, `1000`, `3000`, … — see current spec).

| Concern | Rule |
|---|---|
| Reproducibility | Same SF + generator version + serializer → same data |
| Initial vs updates | Interactive splits ~90% bulk load vs update streams (see spec) |
| GSI labeling | Count loaded entities → full GSI; see [SF → GSI crosswalk](../../reference/graph-scale-index.md#best-effort-snb-sf--gsi-total-entities--v) |

### Workloads

| Workload | Intent | Query / op classes (spec-level) |
|---|---|---|
| **Interactive** | Transactional neighborhood + updates | Complex reads, short reads, inserts/deletes; v1 and v2 driver workflows |
| **Business Intelligence (BI)** | Aggregation- and join-heavy analytics | Complex analytical queries over large graph fractions; microbatches of insert/delete |

### Generators and drivers (external)

- **Datagen (Spark):** produce CSV (or other serializers) for a chosen SF.
- **Interactive / BI drivers:** official LDBC driver repos execute the query mix,
  schedule updates, and collect latency/throughput.
- GraphForge may only expose thin ingest helpers later; the **driver remains
  outside core**.

### Pass criteria / validation (spec-level)

Official audited runs follow GDC auditing rules (member-commissioned auditors,
full disclosure report, fees — see GDC “Auditing process for LDBC SNB”). For
**GraphForge harness / engineering runs** (non-audited), require at minimum:

1. **Correctness:** driver validation / substitution parameters match expected
   results for the SF (or documented golden outputs from the LDBC tooling).
2. **Workload completeness:** the declared query set for Interactive and/or BI
   is executed (no silent subset presented as “full SNB”).
3. **Disclosure:** SF, generator commit/version, serializer, hardware, resource
   policy, and whether updates were enabled.
4. **Honest partial runs:** if only Interactive SF0.1 is green, say so — do not
   imply BI or audited certification.

Retrospective GDC reviews called out partial SNB compliance; GraphForge docs
must not encourage “SNB-like” marketing without naming the exact workload + SF.

---

## 2. Graphalytics

**Home:** [ldbcouncil.org/benchmarks/graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/)  
**Driver:** [ldbc_graphalytics](https://github.com/ldbc/ldbc_graphalytics)

Industrial-grade benchmark for **graph analysis platforms** (Giraph, GraphX,
GraphBLAS, …). Consists of:

- **Six core algorithms** (BFS, PageRank, weakly connected components, local
  clustering coefficient, community detection via label propagation, SSSP —
  exact names/parameters per the Graphalytics specification PDF).
- **Standard datasets** with **reference outputs** for validation.
- Optional large synthetic graphs (including Graph500-class sizes) where the
  Graphalytics rules allow.

### Pass criteria / validation (spec-level)

1. Algorithm outputs validate against reference outputs within the specification’s
   tolerances.
2. All six core algorithms run on each declared dataset for a “full Graphalytics”
   claim at that dataset.
3. Report platform, parallelism, and dataset ids exactly.

**Relation to GraphForge:** analyst verbs (PageRank, components, paths, …) may
implement kernels that *overlap* Graphalytics algorithms, but a Graphalytics
result claim requires the official driver/validation path in the external
harness — not a one-off Python script.

**Relation to GSI / Graph500:** Graphalytics dataset sizes and optional
Graph500-class graphs should be labeled with GSI after load. The Graph500
**size ladder** on GSI remains the progressive synthetic track; Graphalytics is
the **algorithm workload** track.

---

## 3. FinBench (Financial Benchmark)

**Home:** [ldbcouncil.org/benchmarks/finbench](https://ldbcouncil.org/benchmarks/finbench/)  
**Specification:** [FinBench specification (PDF)](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)

Targets financial scenarios (anti-fraud, risk control): high-degree hubs, edge
multiplicity, asymmetric directed graphs, time-window and recursive path
patterns.

| Workload | Status (per GDC / spec) |
|---|---|
| **Transaction** | Defined — OLTP-style complex reads + continuous insert/delete |
| **Analytics** | Future work (not yet a required GraphForge harness lane) |

### Pass criteria / validation (spec-level)

1. Use FinBench datagen + Transaction driver at a declared scale.
2. Meet driver validation for the Transaction query set.
3. Disclose scale parameters, hardware, and consistency/write mode.
4. Do not claim “full FinBench” if only a read subset ran, or if Analytics is
   implied before GDC publishes it.

Profile loaded graphs with `GD-…` GSI (directed). SF/scale parameters are
FinBench-specific — crosswalk to GSI by entity counts after load (best-effort;
document measured V/E).

---

## 4. Semantic Publishing Benchmark (SPB)

**Home:** listed under [GDC Benchmarks](https://ldbcouncil.org/benchmarks/)  
RDF/SPARQL workload based on a media-publishing ontology.

| GraphForge posture | Detail |
|---|---|
| Suite inventory | **Included** — part of the official LDBC portfolio |
| Product path | **Out of scope** for Cypher/property-graph GraphForge unless RDF support is explicitly added |
| Harness | May omit SPB from GraphForge SUT runs; must still name SPB when stating what “full GDC suite” means |

---

## Workload completeness vs Graph500 first-fail

Two independent control policies:

| Policy | Applies to | Rule |
|---|---|---|
| **Progressive / first-fail on GSI** | Graph500 SCALE notches | Stop at first red size notch ([GSI](../../reference/graph-scale-index.md#progressive--first-fail-policy-graph500--gsi)) |
| **Workload completeness** | LDBC benchmarks | At a declared SF/dataset, run the **full** query/algorithm set for that workload (or label the run as a partial engineering subset) |

A harness may:

1. Climb Graph500 notches under first-fail for **size** evidence, and
2. Separately require complete SNB Interactive / BI / Graphalytics / FinBench
   Transaction coverage at chosen SFs.

M4 close is **not** blocked on full LDBC audit completion unless a milestone
plan explicitly widens that gate.

---

## Disk-limited DataFusion framing

Large SNB/FinBench/Graphalytics loads are **disk-limited** under DataFusion +
Parquet: RAM holds working sets. Prefer SF0.003–SF0.1 (and small Graphalytics
datasets) for developer laptops; SF1+ and FinBench large scales belong on
dedicated harness machines with pre-declared envelopes.

---

## Planned convenience API (not the suite)

Future `graphforge.datasets` helpers may load small SNB slices for tutorials.
That is **not** an LDBC-compliant run. Example shape only:

```python
from graphforge import GraphForge
from graphforge.datasets import load_dataset  # planned — not shipped in v0.5.x

gf = GraphForge()
load_dataset(gf, "ldbc-snb-sf0003")  # illustrative name
```

Official compliance requires the external harness + LDBC drivers.

---

## References

- [Graph Data Council](https://ldbcouncil.org/)
- [Benchmarks portfolio](https://ldbcouncil.org/benchmarks/)
- [SNB](https://ldbcouncil.org/benchmarks/snb/) · [Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) · [FinBench](https://ldbcouncil.org/benchmarks/finbench/)
- [LDBC GitHub org](https://github.com/ldbc)
- [Graph Scale Index](../../reference/graph-scale-index.md) — size axis + SF crosswalk
- [Scale limits](../../reference/scale-limits.md) — product envelopes

## Related

- [Dataset overview](overview.md)
- [Graph500 × GSI ladder](../../reference/graph-scale-index.md#graph500-on-the-gsi-axis)
- [WDC Hyperlink Graphs](wdc-hyperlink-graph.md) — retired from the scale harness
