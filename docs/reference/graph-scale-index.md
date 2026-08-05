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

This document (with the [WDC Hyperlink Graphs](../guide/datasets/wdc-hyperlink-graph.md)
guide) is the **scale ladder specification / contract** for GraphForge:

- GSI size bands and identifier grammar
- Graph500 SCALE notches that sit on those bands
- WDC T0→T6 tier placement on the same axis
- First-fail policy, disk-limited DataFusion framing, and per-tier acceptance
- Artifact / fetch expectations (`GF_WDC_*` env vars, mirror layout)

**Execution of the ladder is not a GraphForge core CI or Makefile product.**
An **external scale harness** (separate repository) owns orchestration, runners,
evidence packaging, and first-fail reporting. This repo may ship thin
**reference clients** (for example `scripts/datasets/fetch_wdc_hyperlink.py` and
`make fetch-wdc-hyperlink`) so operators can retrieve artifacts against the
spec — those are not the harness.

See [External scale harness](#external-scale-harness-contract).

---

## Disk-limited DataFusion framing

With DataFusion execution over Parquet-backed projects, **graph size is
disk-limited**: RAM holds working sets (batches, CSR warm regions, UUID maps,
query state), not the full edge table. Failures and slowdowns at large GSI
bands are expected performance/optimization signals for the scale track — not
proof that the product claims Host/Page interactive support.

Product notebook posture remains roughly GSI Levels **01–06** (`XS`–`MD`,
V &lt; 10M). Levels **07+** are stretch / first-fail territory under the
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

## One ladder: GSI × Graph500 × WDC

**One axis (GSI Size Tag / Scale Code).** Graph500 and WDC are datasets that
**sit on** that axis — do not maintain three independent size taxonomies.

- **Graph500** — synthetic, reproducible Kronecker graphs (`SCALE`, default
  `edgefactor = 16`). Undirected → profile with `GU-…`.
- **WDC** — real directed web graphs (Index/Arc / WebGraph). Profile with
  `GD-…`. Tier **order** (T0→T6 first-fail) is not strictly increasing GSI;
  see notes below.

### Graph500 on the GSI ladder

Graph500 definition ([benchmark spec](https://graph500.org/?page_id=12)):

- `V = 2^SCALE`
- `E = edgefactor × V` (default `edgefactor = 16`)
- Kronecker / R-MAT-style generator (undirected edge list for BFS ranking)

Representative SCALE notches (one power-of-two near the middle of each GSI node
band). Density uses the undirected formula; at large SCALE it rounds to `D00`.

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

This repo does **not** ship a Graph500 generator as product surface. The
external harness may generate or cache edge lists; docs only define the SCALE
notches.

### WDC tiers on the GSI ladder

Counts from the [WDC Hyperlink Graphs](https://webdatacommons.org/hyperlinkgraph/)
overview (millions as published). Densities are directed and round to `D00` or
`D01` at these sizes. **Approximate** — capped T1/T2 samples depend on the
ingest tool’s cap policy.

| Ladder step | WDC tier | Dataset | Approx. V / E | GSI (approx.) | Notes |
|---|---|---|---|---|---|
| 1 | **T0** | Example Index/Arc | 106 / 141 | `GD-02-XS-D01` | Smoke |
| 2 | **T1** | 2012 PLD head sample | ~100K / ~0.5–2M | `GD-05-SM-D00` | Cap via ingest; lower edge of SM |
| 3 | **T2** | 2012 PLD larger shard | ~1M / ~5–15M | `GD-06-MD-D00` | LiveJournal class |
| 4 | **T3** | Full 2014 PLD | ~13M / ~56M | `GD-07-LG-D00` | WebGraph→Arc required |
| 5 | **T4** | Full 2012 PLD | ~43M / ~623M | `GD-07-LG-D00` | Same LG band as T3; more edges |
| 6 | **T5a** | Full 2014 Host | ~22M / ~123M | `GD-07-LG-D00` | **Smaller V than T4**; ladder order ≠ size |
| 7 | **T5b** | Full 2012 Host | ~101M / ~2,043M | `GD-08-XL-D00` | Just into XL |
| 8 | **T6a** | Full 2014 Page | ~1,727M / ~64,422M | `GD-09-2XL-D00` | |
| 9 | **T6b** | Full 2012 Page | ~3,563M / ~128,736M | `GD-09-2XL-D00` | Same 2XL band as T6a |

**Approximations / mismatches**

- T1/T2 are **policy caps**, not published WDC aggregations — GSI follows the
  intended sample size, not a fixed public file.
- T5a (Host 2014) has fewer nodes than T4 (PLD 2012); first-fail **order** is
  authoritative for escalation, GSI only labels shape.
- T3 and T4 share Size Tag `LG`; distinguish them by full GSI + tier id + edge
  cardinality in evidence, not by Size Tag alone.
- Page tiers sit in `2XL` by node count; edge cardinality is far beyond typical
  Graph500 SCALE 32 edge lists — compare carefully.

Authoritative tier order, formats, fetch, and acceptance:
[WDC Hyperlink Graphs](../guide/datasets/wdc-hyperlink-graph.md).

### Summary crosswalk (GSI → Graph500 → WDC)

| GSI Size | Graph500 SCALE (rep) | WDC tier (approx.) |
|---|---:|---|
| XS (`01`) | 6 | — |
| XS (`02`) | 8 | **T0** |
| XS (`03`–`04`) | 12, 15 | — |
| SM (`05`) | 18 | **T1** |
| MD (`06`) | 22 | **T2** |
| LG (`07`) | 25 | **T3**, **T4**, **T5a** |
| XL (`08`) | 28 | **T5b** |
| 2XL (`09`) | 32 | **T6a**, **T6b** |
| 3XL–5XL / BIG | 35, 38, 42, 44+ | — (Graph500 only on this ladder) |

---

## First-fail policy (contract)

Applies to WDC T0→T6 and to any harness that escalates Graph500 SCALE notches
on the same GSI axis:

1. Attempt steps in documented order (WDC: T0→T6 with T5a→T5b, T6a→T6b).
2. A step may start only after every earlier step is green.
3. On the **first** red step, **stop** — do not attempt larger steps.
4. Record failed step id, failure class, GSI (if known), disk/RSS/time.
5. Host/Page (and Graph500 Toy+) remain on the ladder; first-fail — not pretend
   pass — governs whether they run.

Green criteria for WDC tiers live in the WDC guide. Typical green signals:
ingest + reopen counts, fixed-hop Cypher with `LIMIT`, resource ledger within
the operator’s declared machine envelope.

---

## External scale harness (contract)

**Owner:** an external scale harness repository (not GraphForge core). This
section is the interface the harness should implement against the docs in this
repo. No harness repo name is fixed here — treat the name as operator-local
until published.

### Responsibilities (external)

| Concern | External harness | GraphForge core |
|---|---|---|
| Orchestrate T0→T6 / Graph500 SCALE ladder | Yes | Spec only |
| First-fail stop + evidence artifacts | Yes | Spec + issue links for product claims |
| Dedicated runners / disk budgets | Yes | No |
| Normal GitHub Actions CI for PLD+/Host/Page | No | Must not |
| Thin fetch / mirror sync reference clients | May call | May ship (`scripts/datasets/…`) |
| Graph500 edge-list generation | Yes (or cache) | Not required |
| Chunked ingest / CSR / Cypher via GraphForge APIs | Invokes published APIs | Engine + thin bindings |

### Expected inputs

| Input | Role |
|---|---|
| GSI Scale Code / Size Tag or full GSI | Select band; label evidence |
| WDC tier id (`T0`…`T6b`) or Graph500 `SCALE` (+ `edgefactor`) | Dataset instance |
| `GF_WDC_CACHE` | Local artifact cache root |
| `GF_WDC_MIRROR_BASE` | Controlled HTTPS mirror (recommended) |
| `GF_WDC_SOURCE` | `mirror-first` \| `mirror-only` \| `origin` |
| Machine envelope | Pre-declared disk/RSS/time stop conditions |

### Expected outputs (per attempted step)

- Tier / SCALE id and approximate GSI
- Green/red disposition and stop reason if red
- Node/edge counts, project bytes, peak RSS, optional CSR timings
- Fixed-hop `LIMIT 1000` shape metrics when the tier allows LIMIT work
- Pointer to cached artifacts (checksums) used for the run

### Reference clients in this repo

```bash
# Spec-aligned fetch only — not ladder orchestration
python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact example
make fetch-wdc-hyperlink ARTIFACT=example
```

See the WDC guide for mirror sync and env details. Do not wire these into
normal CI.

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
| Graph500 SCALE 22 | 4.19M / 67.1M | `GU-06-MD-D00` |
| WDC 2014 PLD (T3) | ~13M / ~56M | `GD-07-LG-D00` |

---

## Further reading

- [Scale Limits](scale-limits.md) — GraphForge product envelopes and fixed-hop LIMIT contract
- [WDC Hyperlink Graphs](../guide/datasets/wdc-hyperlink-graph.md) — T0→T6 first-fail web-graph ladder (spec + reference fetch)
- [Standardized Release Load Matrix](../development/release-load-matrix.md) — CI size/density taxonomy (distinct from GSI)
- [Load Matrix Results](load-matrix-results.md) — accepted matrix evidence
- [Datasets overview](../guide/datasets/overview.md) — planned public dataset catalogs
- [Graph500 benchmark specification](https://graph500.org/?page_id=12) — SCALE / edgefactor definition
