# LDBC Full Suite (spec)

**Last updated:** 2026-08-05

> **Status:** Specification in GraphForge docs. Generators, drivers, and audited
> runs execute in an **external scale harness** — not GraphForge core CI.
> Convenience catalog loaders (`graphforge.datasets`) remain a planned extension
> ([Datasets overview](overview.md)); this page is the workload **contract**,
> not a shipped loader.
>
> Shared evaluation policy (spec vs harness boundary, completeness vs first-fail,
> evidence schema, pinned identity):
> [Scale Evaluation](../../reference/scale-evaluation.md). Size labeling:
> [GSI](../../reference/graph-scale-index.md). Disk-limited framing:
> [Scale Limits](../../reference/scale-limits.md).

The [Graph Data Council (GDC)](https://ldbcouncil.org/) (formerly Linked Data
Benchmark Council / LDBC) maintains the **LDBC benchmark suite**. Workloads
remain branded “LDBC benchmarks” after the 2025 rename. This document inventories
the **full official suite** as published on [ldbcouncil.org/benchmarks](https://ldbcouncil.org/benchmarks/)
and defines how GraphForge treats it at **spec level**.

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

SF is defined by **serialized CSV size in GiB** (not GSI Size Tags). Authoritative
SF lists live in the current SNB specification PDF — disclose the **spec version**
used in evidence ([pinned identity](../../reference/scale-evaluation.md#pinned-generator--driver-identity)).

| Run class | Typical SF set | Notes |
|---|---|---|
| **Engineering / harness smoke** | `0.003`, `0.1`, `0.3` | Laptop-feasible; non-audited; preferred developer default |
| **Engineering / dedicated harness** | `1`, `3`, `10` | Common published comparison class (esp. SF1); still not GDC-audited unless commissioned |
| **Large harness-only** | `30`, `100`, `300`, `1000`, `3000`, … | Spec-published production factors; dedicated runners only |
| **GDC audited certification** | Per current auditing rules / auditor | Member-commissioned; full disclosure report — **distinct** from engineering green |

| Concern | Rule |
|---|---|
| Reproducibility | Same SF + generator version + serializer → same data |
| Initial vs updates | Interactive splits ~90% bulk load vs update streams (see spec) |
| GSI labeling | Count loaded entities → full GSI; see [SF → GSI crosswalk](../../reference/graph-scale-index.md#best-effort-snb-sf--gsi-total-entities--v) |
| Disclosure | Evidence must record SF, Datagen commit/release, driver commit/release, serializer, and whether the run is engineering vs audited |

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
- **Identity:** every engineering or audited claim **must disclose** Datagen and
  driver commit/release plus SNB **spec version** — see
  [Pinned generator / driver identity](../../reference/scale-evaluation.md#pinned-generator--driver-identity)
  and the [evidence schema](../../reference/scale-evaluation.md#evidence-artifact-schema).

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
- **Standard datasets** with **reference outputs** for validation
  ([SURF/CWI repository](https://repository.surfsara.nl/datasets/cwi/graphalytics)).
- Optional large synthetic graphs (including Graph500-class sizes) where the
  Graphalytics rules allow.

### GraphForge-facing dataset shortlist (ordered)

Harnesses targeting GraphForge should prefer this ordered shortlist unless a
full Graphalytics standard-benchmark job composition is explicitly claimed.
Sizes are approximate from the Graphalytics specification; **re-profile → GSI**
after load. Full catalog remains authoritative at the GDC/Graphalytics homes.

| Order | Dataset id | Class | ≈ n | ≈ m | Typical GSI band | Role |
|---:|---|---|---:|---:|---|---|
| 1 | `wiki-Talk` | Real R1 (2XS) | 2.39M | 5.02M | `GU-06-MD-…` | First engineering dataset |
| 2 | `kgs` | Real R2 (XS) | 0.83M | 17.9M | `GU-05-SM-…` | Gaming / denser |
| 3 | `cit-Patents` | Real R3 (XS) | 3.77M | 16.5M | `GU-06-MD-…` | Knowledge / citation |
| 4 | `dota-league` | Real R4 (S) | 0.06M | 50.9M | `GU-04-XS-…` (n) / high E | Dense gaming |
| 5 | `Graph500-22` | Synthetic G22 (S) | 2.4M | 64.2M | `GU-06-MD-D00` | Bridge to Official Graph500 SCALE 22 |
| 6 | `Datagen-7.9-fb` (or nearest Datagen-S) | Synthetic D7.9 (S) | ~1.4M | ~85.7M | `GU-06-MD-…` | Datagen family smoke |
| 7+ | `com-Friendster`, `Graph500-24`+, larger Datagen | XL+ | — | — | `07`+ | Harness-only; not default laptop |

For a **“full Graphalytics at dataset X”** claim: run all six core algorithms on
X with reference-output validation. For **engineering green** on the shortlist:
complete algorithms the SUT implements via the official Graphalytics driver path
and **label omissions** (do not imply six-algorithm coverage if only a subset
ran).

### Pass criteria / validation (spec-level)

1. Algorithm outputs validate against reference outputs within the specification’s
   tolerances.
2. All six core algorithms run on each declared dataset for a “full Graphalytics”
   claim at that dataset.
3. Report platform, parallelism, dataset ids, driver commit/release, and
   Graphalytics **spec version** exactly.

**Relation to GraphForge:** analyst verbs (PageRank, components, paths, …) may
implement kernels that *overlap* Graphalytics algorithms, but a Graphalytics
result claim requires the official driver/validation path in the external
harness — not a one-off Python script.

**Relation to GSI / Graph500:** Graphalytics dataset sizes and optional
Graph500-class graphs should be labeled with GSI after load. The **Official**
Graph500 size ladder and **Derived** density matrix are separate tracks — see
[Scale Evaluation](../../reference/scale-evaluation.md#graph500-on-the-gsi-axis).
Graphalytics is the **algorithm workload** track.

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

### Scale factors (Transaction)

Per FinBench specification (v0.2.0-alpha class; **disclose the exact spec
version** used). SF ≈ serialized CSV GiB; default temporal window three years
from 2020; default split **97%** initial bulk / **3%** incremental. Published SF
set:

| SF | ≈ CSV size (published datasets page) | Engineering default? | Notes |
|---|---|---|---|
| `0.01` | ~6 MB | Yes — smoke | Smallest published factor |
| `0.1` | ~66 MB | Yes — laptop/harness | |
| `0.3` | ~202 MB | Yes — harness | |
| `1` | ~679 MB | Dedicated harness | Spec validation-class scale |
| `3` | ~2 GB | Dedicated harness | |
| `10` | ~6 GB | Large harness | Spec notes audited Transaction runs at SF10 |

Entity counts per SF (accounts, companies, transfers, …) are in FinBench
Appendix B — re-count after load and emit `GD-…` GSI. Pre-built dataset tarballs:
[FinBench datasets](https://ldbcouncil.org/benchmarks/finbench/datasets/).

### Pass criteria / validation (spec-level)

1. Use FinBench datagen + Transaction driver at a declared SF from the table.
2. Meet driver validation for the Transaction query set.
3. Disclose SF, datagen/driver commit or release, FinBench **spec version**,
   hardware, and consistency/write mode.
4. Do not claim “full FinBench” if only a read subset ran, or if Analytics is
   implied before GDC publishes it.

Profile loaded graphs with `GD-…` GSI (directed).

---

## 4. Semantic Publishing Benchmark (SPB)

**Home:** listed under [GDC Benchmarks](https://ldbcouncil.org/benchmarks/)  
RDF/SPARQL workload based on a media-publishing ontology.

| GraphForge posture | Detail |
|---|---|
| Suite inventory | **Included** — part of the official LDBC portfolio (name it; do not omit silently) |
| Product path | **Out of scope** for Cypher/property-graph GraphForge — RDF/SPARQL only |
| Harness / SUT | **Inventory-only** — omit SPB from GraphForge SUT execution; no pass/fail lane |
| Evidence | When listing suite coverage, record `spb: "inventory_only"` (or equivalent) rather than green/red |

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
- [Scale Evaluation](../../reference/scale-evaluation.md) — harness contract, evidence schema, pinned identity, completeness vs first-fail
- [Scale limits](../../reference/scale-limits.md) — product envelopes

## Related

- [Dataset overview](overview.md)
- [Scale Evaluation — Graph500 tracks](../../reference/scale-evaluation.md#graph500-on-the-gsi-axis)
