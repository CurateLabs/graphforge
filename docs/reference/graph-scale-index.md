# Graph Scale Index (GSI)

**Last updated:** 2026-08-05

The Graph Scale Index (GSI) is a standardized alphanumeric identifier for
profiling graph datasets when benchmarking algorithmic performance and
operational boundaries. Use it to label fixtures, compare workload classes, and
frame performance investigations without ambiguous “N million nodes” claims.

GSI describes **dataset shape** (directedness + node band + density). It is
complementary to GraphForge’s product [scale limits](scale-limits.md) and the
release [load-matrix taxonomy](../development/release-load-matrix.md)
(`tests/contracts/load-dataset-taxonomy.json`).

**Nomenclature rule:** when docs discuss profiling, benchmarking, or performance
classes for a graph, prefer a full GSI (`GU-03-XS-D01` or `GD-05-SM-D00`). Do
not reuse load-matrix size letters (`S`, `M`, `L`) as if they were GSI Size Tags
— only GSI Size Tags are `XS`, `SM`, `MD`, `LG`, `XL`, `2XL`–`5XL`, and `BIG`.

---

## Spec vs execution (boundary)

This document is the **size-axis specification / contract** for GraphForge scale
work. Companion workload specs:

| Track | Spec home | Role |
|---|---|---|
| **GSI** | This page | Size axis (node band + density) |
| **Official Graph500** | [Official track](#1-official-graph500-gsi-size-ladder) | Standard ef=16 Kronecker/R-MAT notches for GSI size ladder / community comparability |
| **Graph500-derived matrix** | [Derived track](#2-graph500-derived-scale--density-matrix) | Same generator family, parameterized `edgefactor` to hit GSI density tiers — **not** official Graph500 submissions |
| **LDBC suite** | [LDBC full suite](../guide/datasets/ldbc.md) | Official workload completeness (SNB, Graphalytics, FinBench, SPB) |

**Execution is not a GraphForge core CI or Makefile product.** An **external
scale harness** (separate repository) owns generators, drivers, orchestration,
evidence packaging, and progressive / first-fail reporting for **both** Graph500
tracks. This repo may ship thin **reference clients** only when useful; it must
**not** bulk-add Graph500 or LDBC generators.

See [External scale harness](#external-scale-harness-contract).

**Retired track:** Web Data Commons (WDC) Hyperlink Graphs are **not** the
GraphForge scale-testing ladder. See
[WDC Hyperlink Graphs (not used for scale harness)](../guide/datasets/wdc-hyperlink-graph.md).

---

## Disk-limited DataFusion framing

With DataFusion execution over Parquet-backed projects, **graph size is
disk-limited**: RAM holds working sets (batches, CSR warm regions, UUID maps,
query state), not the full edge table. Failures and slowdowns at large GSI
bands are expected performance/optimization signals for the scale track — not
proof that the product claims interactive support at those bands.

Product notebook posture remains roughly GSI Levels **01–06** (`XS`–`MD`,
V &lt; 10M). Levels **07+** are stretch / progressive-scale territory under the
external harness.

---

## Identifier structure

Every profile identifier follows this hyphenated format:

```text
[GD|GU]-[Scale Code]-[Size Tag]-D[Density Integer]
```

| Component | Meaning |
|---|---|
| `GD` / `GU` | Graph **D**irected or Graph **U**ndirected — selects the density formula |
| Scale Code | Two-character code for the node-count band (`01`–`12`, or `**` overflow) |
| Size Tag | Structural capacity descriptor tied to infrastructure targets |
| `D` + density | Density prefix plus a zero-padded integer percent (`00`–`100`) |

**Examples**

| Identifier | Reading |
|---|---|
| `GU-03-XS-D07` | Undirected; 1,000 ≤ V &lt; 10,000; density 7% |
| `GD-05-SM-D15` | Directed; 100,000 ≤ V &lt; 1,000,000; density 15% |
| `GD-07-LG-D02` | Directed; 10M ≤ V &lt; 100M; density 2% |
| `GU-**-BIG-D01` | Undirected; V ≥ 10T (above Level 12); density 1% |

---

## Operational scale and size tags

| Scale Code | Size Tag | Node count range (V) | Infrastructure target |
|---|---|---|---|
| `01` | XS | V &lt; 100 | L1/L2 cache resident |
| `02` | XS | 100 ≤ V &lt; 1,000 | L3 cache / main memory resident |
| `03` | XS | 1,000 ≤ V &lt; 10,000 | In-memory (standard compute) |
| `04` | XS | 10,000 ≤ V &lt; 100,000 | In-memory (high compute matrix cap) |
| `05` | SM | 100,000 ≤ V &lt; 1,000,000 | Memory saturated (single node) |
| `06` | MD | 1,000,000 ≤ V &lt; 10,000,000 | Scale-up hardware saturated |
| `07` | LG | 10,000,000 ≤ V &lt; 100,000,000 | Scale-out / sharded storage |
| `08` | XL | 100,000,000 ≤ V &lt; 1,000,000,000 | Distributed memory clusters |
| `09` | 2XL | 1,000,000,000 ≤ V &lt; 10,000,000,000 | Distributed big-data tier |
| `10` | 3XL | 10,000,000,000 ≤ V &lt; 100,000,000,000 | High-performance fabric arrays |
| `11` | 4XL | 100,000,000,000 ≤ V &lt; 1,000,000,000,000 | Exascale network layers |
| `12` | 5XL | 1,000,000,000,000 ≤ V &lt; 10,000,000,000,000 | Cloud multi-region datastore |
| `**` | BIG | V ≥ 10,000,000,000,000 | Edge of compute limits (overflow: above Level 12) |

Bands are half-open except the overflow bucket: each numeric level covers up to
but not including the next decade boundary; `**` / BIG starts at
V ≥ 10,000,000,000,000 (10T).

---

## Density quantification

Density maps standard graph topologies into an integer percent in `00`–`100`.
Choose `GU` or `GD` first — that choice selects the formula below.

### Direct calculations

- **`GU` (undirected):** `density = 2|E| / (|V| × (|V| − 1))`
- **`GD` (directed):** `density = |E| / (|V| × (|V| − 1))`

Self-loops are excluded from the complete-graph denominator (same convention as
the load-matrix density formula).

### Integer normalization

1. Compute raw density as a floating-point value with the formula for `GD` or `GU`.
2. Clamp strictly to `[0.0, 1.0]`.
3. Multiply by 100 and round to the nearest whole integer.
4. Format with zero-padding for single-digit values (`7` → `07`); `100` stays
   three digits (`100`).

Examples: 0.07 → `D07`; 0.995 → `D100`; 0.0 → `D00`.

---

## Behavioral analysis reference

Use exact integer density thresholds with scale tiers to anticipate performance
ceilings and data-structure profiles:

| Density tier | Levels 01–04 (XS) | Levels 05–06 (SM/MD) | Levels 07–12+ (LG to BIG) |
|---|---|---|---|
| D00–D09 (very low) | List-driven cache peak | Sparse matrix crossover | Partitioned streaming only |
| D10–D29 (low) | Standard RAM traversals | RAM bottleneck matrix | Approximations required |
| D30–D69 (medium) | Matrix performance base | Out of memory danger | Intractable for exact math |
| D70–D89 (high) | Instant lookup matrix | Hardware saturation | Intractable for exact math |
| D90–D100 (very high) | Iteration peak performance | Hardware saturation | Intractable for exact math |

These cells are qualitative planning cues, not GraphForge SLOs. Product
guarantees and measured envelopes remain in [scale limits](scale-limits.md).

---

## Graph500 on the GSI axis

**One size axis (GSI).** Graph500 generators supply synthetic instances; they do
**not** invent a parallel size taxonomy. GSI still labels every instance.

There are **two distinct tracks**. Label evidence with the track name — never
treat a parameterized-`edgefactor` density cell as an official Graph500
submission.

| Track | Parameters | Purpose | Official Graph500? |
|---|---|---|---|
| **Official Graph500** | Spec-default generator (typically `edgefactor = 16`), undirected Kronecker / R-MAT | GSI size-ladder notches; comparability with Graph500 community / ranking classes | **Yes** (when run per [Graph500 spec](https://graph500.org/?page_id=12)) |
| **Graph500-derived SCALE×density matrix** | Same generator **family**, free `SCALE` + `edgefactor` to hit GSI density tiers | Probe GSI density bands (D00–09 … D90–100) at feasible SCALEs | **No** — derived only |

Shared generator math ([Graph500 specification](https://graph500.org/?page_id=12)):

- `V = 2^SCALE`
- `E = edgefactor × V` (denote `edgefactor` as `ef`)
- Kronecker / R-MAT-style undirected edge list
- Profile instances with `GU-…` (undirected)

**Harness-elsewhere:** both tracks are executed in the external scale harness.
This repo is **spec only** — no Graph500 generator as product surface.

### Density ↔ edgefactor (GU)

Undirected GSI density (same as [Density quantification](#density-quantification)):

- `d = 2|E| / (|V| × (|V| − 1))`

With Graph500’s `E = ef × V` and `V = 2^SCALE`:

- `ef ≈ d · (V − 1) / 2`

Exact when the generator emits exactly `ef × V` undirected edges after
dedup/self-loop policy; harnesses should re-profile `|E|` and emit the measured
GSI.

---

### 1. Official Graph500 (GSI size ladder)

Use **standard Graph500 parameters** — typically `ef = 16`, undirected
Kronecker/R-MAT as specified by Graph500 — for bottom→top **size** notches on
GSI and for community-comparable runs (including ranking classes when the
harness opts in).

#### Representative SCALE notches (bottom → top)

One power-of-two near the middle of each GSI node band. Density uses the
undirected formula; at large SCALE with `ef = 16` it rounds to `D00`.

| GSI Scale / Size | Node band (V) | Graph500 SCALE (rep) | V = 2^SCALE | E (ef=16) | Example GSI |
|---|---|---:|---:|---:|---|
| `01` / XS | V &lt; 100 | **6** | 64 | 1,024 | `GU-01-XS-D51` |
| `02` / XS | 100–1K | **8** | 256 | 4,096 | `GU-02-XS-D13` |
| `03` / XS | 1K–10K | **12** | 4,096 | 65,536 | `GU-03-XS-D01` |
| `04` / XS | 10K–100K | **15** | 32,768 | 524,288 | `GU-04-XS-D00` |
| `05` / SM | 100K–1M | **18** | 262,144 | 4,194,304 | `GU-05-SM-D00` |
| `06` / MD | 1M–10M | **22** | 4,194,304 | 67,108,864 | `GU-06-MD-D00` |
| `07` / LG | 10M–100M | **25** | 33,554,432 | 536,870,912 | `GU-07-LG-D00` |
| `08` / XL | 100M–1B | **28** | 268,435,456 | 4,294,967,296 | `GU-08-XL-D00` |
| `09` / 2XL | 1B–10B | **32** | 4,294,967,296 | 68,719,476,736 | `GU-09-2XL-D00` |
| `10` / 3XL | 10B–100B | **35** | 34,359,738,368 | 549,755,813,888 | `GU-10-3XL-D00` |
| `11` / 4XL | 100B–1T | **38** | 274,877,906,944 | ~4.40T | `GU-11-4XL-D00` |
| `12` / 5XL | 1T–10T | **42** | ~4.40T | ~70.4T | `GU-12-5XL-D00` |
| `**` / BIG | V ≥ 10T | **44+** | ≥ 2^44 | ≥ 16 × 2^44 | `GU-**-BIG-D00` |

SCALE ranges that land in each band (any integer SCALE with
`2^SCALE` in the band): `01`→1–6, `02`→7–9, `03`→10–13, `04`→14–16,
`05`→17–19, `06`→20–23, `07`→24–26, `08`→27–29, `09`→30–33, `10`→34–36,
`11`→37–39, `12`→40–43, `**`→44+.

Official Graph500 ranking classes (Toy/Mini/Small/Medium/Large/Huge at SCALE
26/29/32/36/39/42) map to GSI `07`/`08`/`09`/`10`/`11`/`12` respectively —
useful labels, but the table above is the full bottom-to-top GSI coverage.

#### Progressive / first-fail policy (Official Graph500 × GSI)

Applies when the harness escalates **Official** Graph500 SCALE notches on the
GSI axis:

1. Attempt representative notches in ascending GSI Scale Code order
   (`01`→`**`, or the subset declared for the run).
2. A larger notch may start only after every earlier attempted notch is green
   (or explicitly skipped as out-of-envelope with recorded rationale).
3. On the **first** red notch, **stop** — do not attempt larger notches.
4. Record failed SCALE, failure class, GSI, disk/RSS/time.
5. Official ranking classes (Toy+) remain on the ladder; first-fail — not
   pretend pass — governs whether they run.

Typical green signals for an Official Graph500 notch: generate/load + reopen
counts, fixed-hop Cypher with `LIMIT` (and/or Graph500 BFS kernel when the
harness implements it), resource ledger within the operator’s declared machine
envelope.

---

### 2. Graph500-derived SCALE × density matrix

**Not official Graph500.** Same generator family (Kronecker / R-MAT style,
`V = 2^SCALE`, `E = ef · V`), but `edgefactor` is chosen to land in GSI’s five
density tiers. Evidence must say **derived** / **density matrix** — never
“Graph500 submission” or ranking-class claims.

#### Recommended mid-bucket density targets

| Density tier | Mid-bucket target `d` | Role |
|---|---:|---|
| D00–D09 (very low) | **0.05** | Sparse / list-friendly |
| D10–D29 (low) | **0.20** | Low-density traversals |
| D30–D69 (medium) | **0.50** | Matrix / medium fill |
| D70–D89 (high) | **0.80** | High fill |
| D90–D100 (very high) | **0.95** | Near-complete |

Solve `ef ≈ d · (V − 1) / 2` with `V = 2^SCALE`, then re-profile after
generation.

#### Demo tables (small SCALE)

**SCALE 6** (`V = 64`, GSI Level `01` / XS) — harness smoke / density demos:

| Density tier | Target `d` | `ef ≈ d·(V−1)/2` | E ≈ ef·V | Example GSI |
|---|---:|---:|---:|---|
| D00–D09 | 0.05 | 1.575 | ~101 | `GU-01-XS-D05` |
| D10–D29 | 0.20 | 6.3 | ~403 | `GU-01-XS-D20` |
| D30–D69 | 0.50 | 15.75 | ~1,008 | `GU-01-XS-D50` |
| D70–D89 | 0.80 | 25.2 | ~1,613 | `GU-01-XS-D80` |
| D90–D100 | 0.95 | 29.925 | ~1,915 | `GU-01-XS-D95` |

**SCALE 12** (`V = 4,096`, GSI Level `03` / XS):

| Density tier | Target `d` | `ef ≈ d·(V−1)/2` | E ≈ ef·V | Example GSI |
|---|---:|---:|---:|---|
| D00–D09 | 0.05 | 102.375 | ~419K | `GU-03-XS-D05` |
| D10–D29 | 0.20 | 409.5 | ~1.68M | `GU-03-XS-D20` |
| D30–D69 | 0.50 | 1,023.75 | ~4.19M | `GU-03-XS-D50` |
| D70–D89 | 0.80 | 1,638 | ~6.71M | `GU-03-XS-D80` |
| D90–D100 | 0.95 | 1,945.125 | ~7.97M | `GU-03-XS-D95` |

Harnesses may round `ef` to convenient integers; always emit measured GSI.

#### Feasibility / in-scope cells

Edge count for a fixed density grows as Θ(V²). Official `ef = 16` already
yields `D00` by SCALE **15–18**. Hitting mid/high density at those SCALEs means
tens of millions to billions of edges — usually **out-of-scope** for the
progressive harness.

| SCALE band | Official ef=16 | Derived D00–09 (d≈0.05) | Derived D10+ (d≥0.20) |
|---|---|---|---|
| ≤ 12 (XS demos) | In-scope (size ladder) | **In-scope** (demo matrix) | **In-scope** (demo matrix) |
| 13–14 | In-scope | Marginal — declare disk/time envelope | Usually **out-of-scope** unless envelope allows |
| ≥ 15–18 | In-scope → `D00` size notches | Often impractical (Θ(V²)) | **Out-of-scope** by default |
| ≥ 22 (MD+) | Size ladder only | **Out-of-scope** for density matrix | **Out-of-scope** |

**Default harness posture for the derived matrix:** exercise the five density
tiers at **XS / small** SCALEs (e.g. 6 and 12, optionally a few neighbors). Do
**not** require a full SCALE×density cartesian product at SM+ bands. Document
any cell attempted outside that default as an explicit envelope exception.

The derived matrix does **not** use Official first-fail across SCALEs; operators
may run a fixed small set of `(SCALE, density-tier)` cells independently.

---

## LDBC suite vs GSI

LDBC (Graph Data Council) benchmarks are a **workload suite**, not a second size
axis. Size still uses GSI; LDBC **scale factors (SF)** and Graphalytics dataset
sizes are best-effort crosswalks onto GSI after counting loaded entities.

Full inventory, generators, workloads, and validation expectations:
[LDBC full suite](../guide/datasets/ldbc.md).

### Best-effort SNB SF → GSI (total entities ≈ V)

SNB SF is defined by **CSV GiB size**, not node count. Approximate total entity
counts (all node labels, Interactive-class generators) map roughly as follows —
**re-profile after load** and emit a full GSI; do not treat SF as a Size Tag.

| SNB SF (approx.) | Approx. total entities (V) | Typical GSI Scale / Size | Notes |
|---|---:|---|---|
| 0.003 | ~10K–30K | `03`–`04` / XS | Validation / smoke |
| 0.1 | ~0.3M–1M | `05` / SM | Early Interactive/BI |
| 0.3 | ~1M–3M | `06` / MD | |
| 1 | ~3M–10M | `06` / MD | Common published SF1 class |
| 3 | ~10M–30M | `07` / LG | Past notebook Levels 01–06 |
| 10 | ~30M–100M | `07` / LG | |
| 30 | ~100M–300M | `08` / XL | |
| 100+ | ≥ ~300M | `08`+ / XL+ | Harness-only |

Property-graph density for SNB is schema-driven (many labels/types); compute
`GD-…` or a projection-specific GSI after choosing which relationships count as
`E`. Graphalytics datasets are usually undirected algorithm graphs — profile
with `GU-…`.

LDBC workloads have their own **completeness** requirements (query sets,
throughput/latency rules, validation/auditing). Those are independent of
Official Graph500 first-fail: a harness may run progressive Official notches
**and** require full LDBC workload coverage at each declared SF. The Derived
density matrix is independent of that climb.

---

## External scale harness (contract)

**Owner:** an external scale harness repository (not GraphForge core). This
section is the interface the harness should implement against the docs in this
repo. No harness repo name is fixed here — treat the name as operator-local
until published.

### Responsibilities (external)

| Concern | External harness | GraphForge core |
|---|---|---|
| Orchestrate **Official** Graph500 SCALE notches (ef=16) on GSI | Yes | Spec only |
| Run **Derived** SCALE×density matrix cells (parameterized ef) | Yes | Spec only |
| Run LDBC generators / drivers / validation | Yes | Spec only ([LDBC](../guide/datasets/ldbc.md)) |
| Progressive / first-fail stop + evidence artifacts (Official track) | Yes | Spec + issue links for product claims |
| Dedicated runners / disk budgets | Yes | No |
| Normal GitHub Actions CI for Graph500 Toy+ / LDBC SF≥1 | No | Must not |
| Thin reference clients | May call | Optional only; no bulk generators |
| Chunked ingest / CSR / Cypher via GraphForge APIs | Invokes published APIs | Engine + thin bindings |

### Expected inputs

| Input | Role |
|---|---|
| GSI Scale Code / Size Tag or full GSI | Select band; label evidence |
| Track id: `official` \| `derived` | Separates community-comparable vs density-matrix runs |
| Graph500 `SCALE` (+ `edgefactor`; Official default 16) | Synthetic instance |
| Derived density tier or target `d` | Required for derived matrix cells |
| LDBC benchmark id + SF / dataset name | Workload suite instance |
| Machine envelope | Pre-declared disk/RSS/time stop conditions |

### Expected outputs (per attempted step)

- Track id (`official` / `derived`), SCALE / LDBC id, and approximate GSI
- For derived cells: target `d` / density tier and chosen `edgefactor`
- Green/red disposition and stop reason if red
- Node/edge (or entity) counts, project bytes, peak RSS, optional CSR timings
- Workload metrics required by the LDBC or Graph500 spec being executed
- Pointer to cached artifacts (checksums) used for the run

### What this repo must not do

- Add Graph500 or LDBC generators/drivers as product surface
- Wire full Graph500 / LDBC suites (Official or Derived) into normal CI
- Recommend WDC Hyperlink Graphs as the scale harness path
- Present Derived density-matrix cells as official Graph500 submissions

---

## Profiling practice

When investigating performance:

1. Decide directedness and choose the prefix (`GD` or `GU`).
2. Count live nodes `V` and live edges `E` (exclude deleted facts if your store
   distinguishes them).
3. Assign the Scale Code / Size Tag from the node band table.
4. Compute density with the matching formula, normalize to `Dxx`.
5. Record the full GSI on the dataset, benchmark run, and issue notes.
6. Cross-check the behavioral matrix for likely bottlenecks before changing
   algorithms or hardware assumptions.

---

## Relationship to GraphForge envelopes

GraphForge’s documented product posture is research / notebook work through
roughly GSI Levels **01–06** (`XS` through `MD`, V &lt; 10M). Level **07**
(`LG`, V ≥ 10M) and above are outside the primary interactive envelope; use a
production graph database for multi-tenant or concurrent-write deployments at
those bands. Measured LIMIT contracts and edge-bound full scans remain in
[scale limits](scale-limits.md).

### Load-matrix size-class crosswalk

The release load matrix uses its own size letters for synthetic CI fixtures.
Those IDs are **not** GSI Size Tags. Matrix density is directed, so profile
fixtures with a `GD-…` GSI. Approximate node-band mapping:

| Load-matrix size | Node band (taxonomy) | Typical GSI Scale Code / Size Tag |
|---|---|---|
| `XS` | 16–31 | `01` / `XS` |
| `S` | 64–127 | `01`–`02` / `XS` |
| `M` | 256–511 | `02` / `XS` |
| `L` | 1,024–2,047 | `03` / `XS` |
| `XL` | 4,096–8,191 | `03` / `XS` |

Matrix `sparse` (density ≤ 0.08) maps to GSI **D00–D08**; matrix `dense`
(density ≥ 0.10) maps to **D10+**. Always emit the full GSI when profiling a
concrete fixture (for example `GD-03-XS-D12`), not the bare matrix letter.

### Example profiles

| Dataset / fixture | V / E (approx.) | GSI |
|---|---|---|
| Zachary karate | 34 / 78 | `GU-01-XS-D14` |
| SNAP ego-facebook | 4,039 / 88,234 | `GU-03-XS-D01` |
| Fixed-hop LIMIT bench (1M edges) | 62,500 / 1,000,000 | `GD-04-XS-D00` |
| Fixed-hop LIMIT bench (10M edges) | 625,000 / 10,000,000 | `GD-05-SM-D00` |
| SNAP web-Google | 876K / 5.1M | `GD-05-SM-D00` |
| LiveJournal release bench | 4.0M / 34.7M | `GD-06-MD-D00` |
| Graph500 SCALE 22 (Official, ef=16) | 4.19M / 67.1M | `GU-06-MD-D00` |
| Graph500-derived SCALE 6, d=0.50 | 64 / ~1,008 | `GU-01-XS-D50` |
| LDBC SNB SF1 (approx. total entities) | ~3M–10M / schema-dependent | `GD-06-MD-D00` (re-profile) |

---

## Further reading

- [Scale Limits](scale-limits.md) — GraphForge product envelopes and fixed-hop LIMIT contract
- [LDBC full suite](../guide/datasets/ldbc.md) — SNB, Graphalytics, FinBench, SPB (spec-level)
- [WDC Hyperlink Graphs](../guide/datasets/wdc-hyperlink-graph.md) — **not** used for the scale harness (retired track)
- [Standardized Release Load Matrix](../development/release-load-matrix.md) — CI size/density taxonomy (distinct from GSI)
- [Load Matrix Results](load-matrix-results.md) — accepted matrix evidence
- [Datasets overview](../guide/datasets/overview.md) — planned public dataset catalogs
- [Graph500 benchmark specification](https://graph500.org/?page_id=12) — SCALE / edgefactor definition (Official track)
- [Official Graph500 notches](#1-official-graph500-gsi-size-ladder) · [Derived density matrix](#2-graph500-derived-scale--density-matrix)
- [Graph Data Council / LDBC](https://ldbcouncil.org/) — official benchmark suite home
