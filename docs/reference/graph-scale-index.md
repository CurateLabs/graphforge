# Graph Scale Index (GSI)

**Last updated:** 2026-08-14

The Graph Scale Index (GSI) is a standardized alphanumeric identifier for
profiling graph datasets when benchmarking algorithmic performance and
operational boundaries. Use it to label fixtures, compare workload classes, and
frame performance investigations without ambiguous “N million nodes” claims.

GSI describes **dataset shape** (directedness + node band + density). It is
complementary to GraphForge’s product [scale limits](scale-limits.md) and the
release [load-matrix taxonomy](../development/release-load-matrix.md)
(`tests/contracts/load-dataset-taxonomy.json`).

**Nomenclature rule:** when docs discuss profiling, benchmarking, or performance
classes for a graph, prefer a full GSI (`GU-03-XS-D01`, `GD-05-SM-D00`, or
`Gx-00-XS-D00`). Do not reuse load-matrix size letters (`S`, `M`, `L`) as if they
were GSI Size Tags — only GSI Size Tags are `XS`, `SM`, `MD`, `LG`, `XL`,
`2XL`–`5XL`, and `BIG`.

Live workspaces can be graded through the Rust-owned
[`profile_gsi`](api.md#profile_gsi--graphscaleindexprofile) facade (thin
Python/Node bindings). Scale **evaluation** (Official Graph500, Graph500-derived
density matrix, LDBC suite, external harness contract, evidence schema) lives in
[Scale Evaluation](scale-evaluation.md). Large-graph work is **disk-limited**
under DataFusion + Parquet — see [Scale Limits](scale-limits.md).

---

## Identifier structure

Every profile identifier follows this hyphenated format:

```text
[GD|GU|Gx]-[Scale Code]-[Size Tag]-D[Density Integer]
```

| Component | Meaning |
|---|---|
| `GD` / `GU` / `Gx` | Graph **D**irected, Graph **U**ndirected, or unknown (`Gx`) — selects the density formula |
| Scale Code | Two-character code for the node-count band (`00` empty, `01`–`12`, or `**` overflow) |
| Size Tag | Structural capacity descriptor tied to infrastructure targets |
| `D` + density | Density prefix plus a zero-padded integer percent (`00`–`100`) |

**Examples**

| Identifier | Reading |
|---|---|
| `Gx-00-XS-D00` | Unknown directedness; empty live graph (`V = 0`); density 0% |
| `GU-03-XS-D07` | Undirected; 1,000 ≤ V &lt; 10,000; density 7% |
| `GD-05-SM-D15` | Directed; 100,000 ≤ V &lt; 1,000,000; density 15% |
| `Gx-01-XS-D50` | Unknown directedness; V &lt; 100; density uses the directed formula |
| `GD-07-LG-D02` | Directed; 10M ≤ V &lt; 100M; density 2% |
| `GU-**-BIG-D01` | Undirected; V ≥ 10T (above Level 12); density 1% |

---

## Operational scale and size tags

| Scale Code | Size Tag | Node count range (V) | Infrastructure target |
|---|---|---|---|
| `00` | XS | V = 0 (empty live graph) | Empty / uninitialized workspace |
| `01` | XS | 1 ≤ V &lt; 100 | L1/L2 cache resident |
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

Bands are half-open except the empty and overflow buckets: `00` is exactly
`V = 0`; each numeric level `01`–`12` covers up to but not including the next
decade boundary; `**` / BIG starts at V ≥ 10,000,000,000,000 (10T).

Product notebook posture remains roughly GSI Levels **01–06** (`XS`–`MD`,
V &lt; 10M). Levels **07+** are stretch / progressive-scale territory under the
[native scale evaluation harness](scale-evaluation.md#evaluation-scope-and-execution).

---

## Density quantification

Density maps standard graph topologies into an integer percent in `00`–`100`.
Choose `GU`, `GD`, or `Gx` first — that choice selects the formula below.

### Direct calculations

- **`GU` (undirected):** `density = 2|E| / (|V| × (|V| − 1))`
- **`GD` (directed):** `density = |E| / (|V| × (|V| − 1))`
- **`Gx` (unknown):** use the **directed** density formula; structured profiler
  results report `directedness=unknown` so callers are not misled

When `V < 2` (including empty and singleton graphs), density is `D00` — there is
no complete-graph denominator.

Self-loops are excluded from the complete-graph denominator (same convention as
the load-matrix density formula).

### Integer normalization

1. Compute raw density as a floating-point value with the formula for `GD`, `GU`,
   or `Gx`.
2. Clamp strictly to `[0.0, 1.0]`.
3. Multiply by 100 and round to the nearest whole integer.
4. Format with zero-padding for single-digit values (`7` → `07`); `100` stays
   three digits (`100`).

Examples: 0.07 → `D07`; 0.995 → `D100`; 0.0 → `D00`.

---

## Project directedness configuration

`workspace_configuration@1` may include optional `graph_directedness` with
values `directed` or `undirected`. Absent / unset → GSI prefix `Gx`. The field
is additive: existing configuration records without it remain valid and omit the
key when serialized.

Use the public read/write path:

- Rust: `GraphForge::graph_directedness` / `set_graph_directedness`
- Python / Node: thin wrappers of the same methods

Unknown values are rejected fail-closed. Algorithm `directed=` options remain
call-scoped and do **not** infer or overwrite this project metadata.

---

## Profiler API

`GraphForge::profile_gsi` grades the **live** nodes `V` and live edges `E` in an
opened workspace (deleted facts excluded). Empty and tiny graphs succeed without
error:

| Fixture | Expected GSI |
|---|---|
| Empty, unset directedness | `Gx-00-XS-D00` |
| Empty, `graph_directedness=directed` | `GD-00-XS-D00` |
| Empty, `graph_directedness=undirected` | `GU-00-XS-D00` |
| Singleton (`V = 1`), unset | `Gx-01-XS-D00` |

The structured result includes at least: GSI string, `directedness`
(`directed` / `undirected` / `unknown`), `V`, `E`, raw density, Scale Code,
Size Tag, and density integer. See [`profile_gsi`](api.md#profile_gsi--graphscaleindexprofile).

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

## Graph500 SCALE → GSI band mapping

Pure size mapping for synthetic Graph500 instances (`V = 2^SCALE`, typical
`ef = 16`). Track semantics, first-fail policy, and density-matrix cells live in
[Scale Evaluation](scale-evaluation.md#graph500-on-the-gsi-axis).

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
`11`→37–39, `12`→40–43, `**`→44+. Empty workspaces use Scale Code `00` and are
outside the Graph500 SCALE ladder.

Official Graph500 ranking classes (Toy/Mini/Small/Medium/Large/Huge at SCALE
26/29/32/36/39/42) map to GSI `07`/`08`/`09`/`10`/`11`/`12` respectively —
useful labels; the table above is the full bottom-to-top GSI coverage.

---

## Best-effort SNB SF → GSI (total entities ≈ V)

SNB SF is defined by **CSV GiB size**, not node count. Approximate total entity
counts (all node labels, Interactive-class generators) map roughly as follows —
**re-profile after load** and emit a full GSI; do not treat SF as a Size Tag.
LDBC workload contracts: [LDBC full suite](../guide/datasets/ldbc.md).

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

---

## Profiling practice

When investigating performance:

1. Set project `graph_directedness` when known (`directed` / `undirected`), or
   leave it unset for `Gx`.
2. Call `profile_gsi` on the opened workspace (or count live nodes `V` and live
   edges `E` yourself, excluding deleted facts).
3. Confirm the Scale Code / Size Tag from the node band table (including `00`
   for empty graphs).
4. Confirm density used the matching formula (`Gx` uses directed math),
   normalized to `Dxx`.
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
[scale limits](scale-limits.md). Escalation past Levels 01–06 is a
**spec + external harness** track — see [Scale Evaluation](scale-evaluation.md).

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

- [Scale Evaluation](scale-evaluation.md) — Official Graph500 + Derived density matrix; harness contract; evidence schema
- [Official-parameter SCALE-20 client](../development/perf-g500-scale20.md) — in-tree public-facade engineering green (not Official-track)
- [Scale Limits](scale-limits.md) — GraphForge product envelopes and fixed-hop LIMIT contract
- [LDBC full suite](../guide/datasets/ldbc.md) — SNB, Graphalytics, FinBench, SPB (spec-level)
- [Standardized Release Load Matrix](../development/release-load-matrix.md) — CI size/density taxonomy (distinct from GSI)
- [Load Matrix Results](load-matrix-results.md) — accepted matrix evidence
- [Datasets overview](../guide/datasets/overview.md) — planned public dataset catalogs
- [Graph500 benchmark specification](https://graph500.org/?page_id=12) — SCALE / edgefactor definition
- [Graph Data Council / LDBC](https://ldbcouncil.org/) — official benchmark suite home
