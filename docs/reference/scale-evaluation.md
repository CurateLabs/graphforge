# Scale Evaluation (harness contract)

**Last updated:** 2026-09-05

This document distinguishes GraphForge lifecycle scale tests, derived input
experiments, and official benchmark performance claims. The current release
uses the in-tree `benchmarks/` harness on **OVHC-AGENCY**: **Graph500-compliant
generated input; GraphForge lifecycle measurements; no Graph500 performance/TEPS
claim**. See the [native host runbook](../../benchmarks/README.md).
Size labeling uses the [Graph Scale Index (GSI)](graph-scale-index.md). Product
envelopes and the canonical disk-limited DataFusion statement live in
[Scale Limits](scale-limits.md).

---

## Evaluation scope and execution

| Track | Spec home | Role |
|---|---|---|
| **GSI** | [Graph Scale Index](graph-scale-index.md) | Size axis (node band + density) |
| **GraphForge lifecycle with compliant Graph500 input** | [Size ladder](#1-graph500-compliant-input-for-the-gsi-size-ladder) | Current native EF16 input and public-lifecycle scale tests; no official performance claim |
| **Official Graph500 performance** | [Graph500 specification](https://graph500.org/?page_id=12) | Separate BFS/SSSP kernel, validation, timing, and reporting obligations; outside this release scope |
| **Graph500-derived matrix** | [Track 2](#2-graph500-derived-scale--density-matrix) | Same generator family, parameterized `edgefactor` to hit GSI density tiers — **not** official Graph500 submissions |
| **LDBC suite** | [LDBC full suite](../guide/datasets/ldbc.md) | Official workload completeness (SNB, Graphalytics, FinBench, SPB) |

The in-tree benchmark harness owns the streaming generator, lifecycle driver,
profiles, native host commands, and evidence consumption. Rust owns product
behavior; benchmark orchestration calls the public CLI/API. Ordinary CI runs a
bounded tiny lifecycle and generator regressions, while the large host ladder
runs separately. Benchmark tools are not a new engine or public product API.

The [historical external harness contract](#historical-external-scale-harness-contract)
below documents an older integration format; it does not override the active
native producer schemas or require another harness repository.

---

## Graph500 on the GSI axis

**One size axis (GSI).** Graph500 generators supply synthetic instances; they do
**not** invent a parallel size taxonomy. GSI still labels every instance.

The compliant-input lifecycle and derived density matrix are distinct. Label
evidence by its actual scope; never
treat a parameterized-`edgefactor` density cell as an official Graph500
submission.

| Track | Parameters | Purpose | Official Graph500? |
|---|---|---|---|
| **Compliant-input lifecycle** | Graph500 input rules with **`edgefactor = 16`**, undirected raw tuples and scrambled labels | GraphForge native lifecycle size notches | **No performance claim**; input compliance alone is insufficient |
| **Official Graph500 performance** | Compliant input plus the applicable benchmark kernels, validation, timing, and reporting | Separate benchmark performance evaluation | Only when those complete specification requirements are met; not this release ladder |
| **Graph500-derived SCALE×density matrix** | Same generator **family**, free `SCALE` + `edgefactor` to hit GSI density tiers | Probe GSI density bands (D00–09 … D90–100) at feasible SCALEs | **No** — derived only |

Shared generator math ([Graph500 specification](https://graph500.org/?page_id=12)):

- `V = 2^SCALE`
- `M = edgefactor × V` raw tuples (denote `edgefactor` as `ef`)
- Kronecker / R-MAT-style undirected edge list
- Raw tuples have undirected semantics; record the actual stored graph interpretation when assigning GSI

The current in-tree generator supports EF16 only. A parameterized density
matrix is a separate derived workload, not an alternate setting of the current
release generator.

### Density ↔ edgefactor (GU)

Undirected GSI density (see [Density quantification](graph-scale-index.md#density-quantification)):

- `d = 2|E| / (|V| × (|V| − 1))`

For a simple undirected graph with `E = ef × V` and `V = 2^SCALE`:

- `ef ≈ d · (V − 1) / 2`

This relation uses unique non-loop pairs, not raw Graph500 tuples. Since
`M = ef × V` retains duplicates and self-loops, do not substitute raw tuple or
stored multigraph counts into a simple-graph density claim. A derived
simple-graph projection must measure its own `|E|` and report that interpretation
separately; it must not modify or replenish the compliant raw input. Representative SCALE → GSI band mapping:
[Graph500 SCALE notches](graph-scale-index.md#graph500-scale--gsi-band-mapping).

---

## 1. Graph500-compliant input for the GSI size ladder

Use the Graph500 input rules, including **`ef = 16`**, to evaluate ascending
GraphForge size notches. A different edge factor belongs to a derived workload.
Neither EF16 alone nor successful lifecycle execution establishes an official
Graph500 benchmark performance result.

### Progressive / first-fail policy (native lifecycle)

The native release ladder uses canonical S18/S19/S20/S22/S24/S25/S26 inputs
and stops at the first failed rung:

1. Attempt representative notches in ascending GSI Scale Code order
   (`01`→`**`, or the subset declared for the run).
2. A larger notch may start only after every earlier canonical rung is green.
   A skipped or out-of-envelope rung does not authorize advancing.
3. On the **first** red notch, **stop** — do not attempt larger notches.
4. Record failed SCALE, failure class, GSI, disk/RSS/time.
5. Report the largest completed rung without implying a higher-scale or official
   benchmark performance result.

### GraphForge size-ladder lifecycle

The current release uses **Graph500-compliant generated input; GraphForge
lifecycle measurements; no Graph500 performance/TEPS claim**. Its
[streaming generator](../../benchmarks/runners/graph500-generator/README.md)
emits exactly `16 × 2^SCALE` raw tuples with globally scrambled labels and
retains duplicate endpoint pairs and self-loops. The adapter stores one
distinct edge per tuple; directed Cypher follows the emitted orientation.
No target-live replenishment or simple-graph deduplication is applied.

For each attempted SCALE notch, the harness completes this pipeline under the
declared machine envelope:

| Step | Required? | Contract |
|---|---|---|
| Generate edge list (Graph500 generator, `ef = 16`) | **Required** | Disclose generator identity ([Pinned identity](#pinned-generator--driver-identity)) |
| Ingest into a GraphForge project (Parquet/Arrow path) | **Required** | Via published GraphForge APIs |
| Reopen / recount | **Required** | Reconcile all generated nodes and raw edge tuples with stored records and clean-imported results; emit measured GSI |
| Fixed-hop Cypher with `LIMIT` | **Required** | At least one one-hop and one two-hop `MATCH … RETURN … LIMIT N` (N ≥ 1000 recommended) that finish green — GraphForge product scale signal |
| Graph500 **BFS/SSSP** kernels + validation | Outside this release's lifecycle scope | Required as applicable for a separate official Graph500 performance claim |
| TEPS / Graph500 performance result fields | Not claimed | Do not infer them from GraphForge lifecycle timings |

Lifecycle success means generate → ingest → reopen → required LIMIT Cypher,
with correct source/imported results under the host envelope. It establishes
GraphForge behavior on compliant input; it is not an official Graph500
performance result.

### Failure classes → red / stop (native lifecycle)

| `error_class` | Meaning | Notch disposition | Ladder policy |
|---|---|---|---|
| `oom` | Process killed / allocator failure / RSS exceeds envelope | **Red** | **First-fail stop** — do not attempt larger notches |
| `timeout` | Wall time exceeds declared envelope | **Red** | **First-fail stop** |
| `incorrect_validation` | Count mismatch, Cypher wrong results, or BFS validation fail (when BFS claimed) | **Red** | **First-fail stop** |
| `disk_exhaustion` | Project/cache disk full or write failure from space | **Red** | **First-fail stop** |
| `harness_error` | Orchestration bug, missing binary, misconfiguration (not SUT) | **Red** for the attempt; may **retry** after fix **without** advancing the ladder | Do **not** skip to a larger notch; re-run the same notch after the harness fix |
| `out_of_envelope_skip` | Operator pre-declares notch beyond machine envelope | **Skipped** (not green) with rationale | Larger notches remain blocked unless earlier notches are green |

Progressive first-fail on SCALE notches remains **authoritative** for the
native lifecycle size ladder.

---

## 2. Graph500-derived SCALE × density matrix

**Not official Graph500.** Same generator family (Kronecker / R-MAT style,
`V = 2^SCALE`, `E = ef · V`), but `edgefactor` is chosen to land in GSI’s five
density tiers. Evidence must say **derived** / **density matrix** — never
“Graph500 submission” or ranking-class claims.

### Recommended mid-bucket density targets

| Density tier | Mid-bucket target `d` | Role |
|---|---:|---|
| D00–D09 (very low) | **0.05** | Sparse / list-friendly |
| D10–D29 (low) | **0.20** | Low-density traversals |
| D30–D69 (medium) | **0.50** | Matrix / medium fill |
| D70–D89 (high) | **0.80** | High fill |
| D90–D100 (very high) | **0.95** | Near-complete |

Solve `ef ≈ d · (V − 1) / 2` with `V = 2^SCALE`, then re-profile after
generation.

### Demo tables (small SCALE)

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

### Feasibility / in-scope cells

Edge count for a fixed density grows as Θ(V²). Standard `ef = 16` already
yields `D00` by SCALE **15–18**. Hitting mid/high density at those SCALEs means
tens of millions to billions of edges — usually **out-of-scope** for the
progressive harness.

| SCALE band | Standard ef=16 | Derived D00–09 (d≈0.05) | Derived D10+ (d≥0.20) |
|---|---|---|---|
| ≤ 12 (XS demos) | In-scope (size ladder) | **In-scope** (demo matrix) | **In-scope** (demo matrix) |
| 13–14 | In-scope | Marginal — declare disk/time envelope | Usually **out-of-scope** unless envelope allows |
| ≥ 15–18 | In-scope → `D00` size notches | Often impractical (Θ(V²)) | **Out-of-scope** by default |
| ≥ 22 (MD+) | Size ladder only | **Out-of-scope** for density matrix | **Out-of-scope** |

**Default harness posture for the derived matrix:** exercise the five density
tiers at **XS / small** SCALEs (e.g. 6 and 12, optionally a few neighbors). Do
**not** require a full SCALE×density cartesian product at SM+ bands. Document
any cell attempted outside that default as an explicit envelope exception.

The derived matrix does **not** use the native ladder’s first-fail across SCALEs or across
density tiers. Each in-scope `(SCALE, density-tier)` cell is an **independent**
pass/fail unit.

### Derived cell — required workloads (green)

For each in-scope cell the harness declares (default: five mid-bucket tiers at
SCALE 6 and 12):

| Step | Required? | Contract |
|---|---|---|
| Generate with parameterized `edgefactor` for target `d` | **Required** | Same R-MAT family; disclose the distinct derived generator identity |
| Unique-edge / self-loop filtering | **Required** | Apply generator/harness dedup policy, then **re-profile** `|V|`, `|E|`, density → measured `Dxx` / full GSI (target `d` is aspirational) |
| Ingest + reopen | **Required** | Via GraphForge APIs; counts match post-filter edge list |
| Fixed-hop Cypher with `LIMIT` | **Required** | Same fixed-hop lifecycle checks as the size ladder |
| Graph500 BFS / TEPS | **Optional** | Never claim as official Graph500 submission |

### Derived cell — pass / fail

| Disposition | Rule |
|---|---|
| **Green** | All required steps succeed within that cell’s envelope; measured GSI recorded (may differ from target `Dxx` after unique-edge filtering) |
| **Red** | Any required step fails (`oom`, `timeout`, `incorrect_validation`, `disk_exhaustion`, or unrecoverable `harness_error`) |
| **Independent cells** | Red on one `(SCALE, density-tier)` does **not** automatically fail or block other density tiers or SCALEs |
| **Out-of-scope** | Cells marked out-of-scope in the feasibility table are not required; if attempted, label as envelope exception |

Evidence must label `track: "derived"` and never use ranking-class language.

---

## 3. LDBC suite

LDBC (Graph Data Council) benchmarks are a **workload suite**, not a second size
axis. Size still uses GSI; LDBC **scale factors (SF)** and Graphalytics dataset
sizes are best-effort crosswalks onto GSI after counting loaded entities — see
[SNB SF → GSI](graph-scale-index.md#best-effort-snb-sf--gsi-total-entities--v).

Full inventory, generators, workloads, and per-benchmark validation:
[LDBC full suite](../guide/datasets/ldbc.md).

### Workload completeness vs Graph500 first-fail

Two independent control policies:

| Policy | Applies to | Rule |
|---|---|---|
| **Progressive / first-fail on GSI** | Native lifecycle SCALE notches (ef=16) | Stop at first red size notch ([policy](#progressive--first-fail-policy-native-lifecycle)) |
| **Derived density matrix** | Graph500-derived `(SCALE, ef)` cells | Independent XS/small density probes — not official submissions ([matrix](#2-graph500-derived-scale--density-matrix)) |
| **Workload completeness** | LDBC benchmarks | At a declared SF/dataset, run the **full** query/algorithm set for that workload (or label the run as a partial engineering subset) |

A harness may:

1. Climb compliant-input GraphForge notches under first-fail for **size** evidence,
2. Optionally run the **Derived** SCALE×density matrix at feasible SCALEs, and
3. Separately require complete SNB Interactive / BI / Graphalytics / FinBench
   Transaction coverage at chosen SFs.

embedded-performance close is **not** blocked on full LDBC audit completion unless a milestone
plan explicitly widens that gate.

---

## Historical external scale harness contract

This section preserves the former external-harness proposal and its JSON
format for interpreting historical material. Its external ownership, spec-only
repository boundary, and `track: "official"` parameter label are superseded for
the native release lifecycle. That old label never substitutes for actual
Graph500 performance validation. Current host evidence is emitted by the
in-tree harness under `benchmarks/schemas/`; do not manufacture the legacy
example below or add another publication step to consume native results.

### Historical responsibility split

| Concern | External harness | GraphForge core |
|---|---|---|
| Orchestrate EF16 SCALE notches on GSI | Yes | Spec only |
| Run **Derived** SCALE×density matrix cells (parameterized ef) | Yes | Spec only |
| Run LDBC generators / drivers / validation | Yes | Spec only ([LDBC](../guide/datasets/ldbc.md)) |
| Progressive / first-fail stop + evidence artifacts (Official track) | Yes | Spec + issue links for product claims |
| Dedicated runners / disk budgets | Yes | No |
| Normal GitHub Actions CI for Graph500 Toy+ / LDBC SF≥1 | No | Must not |
| Thin reference clients | May call | Optional only; no bulk generators. In-tree Official-parameter client: [perf-g500-scale20.md](../development/perf-g500-scale20.md) (SCALE-6 CI smoke + ignored SCALE-20; not `track: official`) and bounded first-fail ladder [perf-g500-ladder.md](../development/perf-g500-ladder.md) (#736; SCALE-10 CI + ignored SCALE-20→26) |
| Chunked ingest / CSR / Cypher via GraphForge APIs | Invokes published APIs | Engine + thin bindings |

### Historical expected inputs

| Input | Role |
|---|---|
| GSI Scale Code / Size Tag or full GSI | Select band; label evidence |
| Track id: `official` \| `derived` \| `ldbc` | Separates community-comparable vs density-matrix vs LDBC runs |
| Graph500 `SCALE` (+ `edgefactor`; Official default 16) | Synthetic instance |
| Derived density tier or target `d` | Required for derived matrix cells |
| LDBC benchmark id + SF / dataset name | Workload suite instance |
| Machine envelope | Pre-declared disk/RSS/time stop conditions |

### Historical expected outputs (per attempted step)

See the [historical artifact schema](#historical-evidence-artifact-schema).
It is not the current native producer/consumer contract.

### Historical evidence artifact schema

The former proposal described one object per step and a harness-defined
layout. This example is historical, including its `official` label for an EF16
engineering workload. It is not an official Graph500 result or a template for
current native evidence.

```json
{
  "schema_version": "1",
  "track": "official",
  "gsi": "GU-06-MD-D00",
  "scale": 22,
  "edgefactor": 16,
  "density_tier": null,
  "target_density": null,
  "measured_density": 0.0,
  "density_code": "D00",
  "ldbc": null,
  "workloads_run": ["generate", "ingest", "reopen", "cypher_limit_1hop", "cypher_limit_2hop"],
  "pass": true,
  "error_class": null,
  "wall_time_s": 123.4,
  "rss_peak_bytes": 8589934592,
  "disk_used_bytes": 21474836480,
  "artifact_checksums": {
    "edges": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    "project": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
  },
  "generator": {
    "name": "graph500",
    "source": "https://github.com/graph500/graph500",
    "version": "3.0.0",
    "commit": "REPLACE_WITH_GIT_SHA"
  },
  "driver": null,
  "sut": {"name": "graphforge", "version": "0.5.x", "git_sha": "REPLACE_WITH_GIT_SHA"},
  "machine_envelope": {"disk_bytes": 1099511627776, "rss_bytes": 68719476736, "timeout_s": 3600},
  "teps": null,
  "notes": null
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `schema_version` | string | Yes | `"1"` for this contract |
| `track` | string | Yes | `official` \| `derived` \| `ldbc` |
| `gsi` | string | Yes | Measured full GSI after load (or best-effort before fail) |
| `scale` | int \| null | Official/Derived | Graph500 `SCALE`; null for pure LDBC |
| `edgefactor` | number \| null | Official/Derived | Official **must** be `16` |
| `density_tier` | string \| null | Derived | e.g. `D00-D09`; null otherwise |
| `target_density` | number \| null | Derived | Mid-bucket `d` (e.g. `0.05`) |
| `measured_density` | number \| null | Yes when loaded | After unique-edge filtering |
| `density_code` | string \| null | Yes when loaded | `Dxx` from measured density |
| `ldbc` | object \| null | LDBC | `{ "benchmark", "workload", "sf_or_dataset", "spec_version" }` |
| `workloads_run` | string[] | Yes | e.g. `generate`, `ingest`, `reopen`, `cypher_limit_*`, `graph500_bfs`, `snb_interactive`, … |
| `pass` | bool | Yes | Green/red for this step |
| `error_class` | string \| null | When `pass=false` | `oom` \| `timeout` \| `incorrect_validation` \| `disk_exhaustion` \| `harness_error` \| `out_of_envelope_skip` |
| `wall_time_s` | number \| null | Recommended | End-to-end step wall time |
| `rss_peak_bytes` | number \| null | Recommended | Peak RSS |
| `disk_used_bytes` | number \| null | Recommended | Project + cache bytes attributable to the step |
| `artifact_checksums` | object | Yes when artifacts retained | Map of logical name → `sha256:…` (or equivalent) |
| `generator` | object | Yes when generation ran | See [Pinned identity](#pinned-generator--driver-identity) |
| `driver` | object \| null | LDBC / BFS driver | Same disclosure shape as `generator` |
| `sut` | object | Yes | GraphForge version / git SHA |
| `machine_envelope` | object | Yes | Declared stop conditions for the run |
| `teps` | number \| null | When BFS TEPS claimed | Graph500 harmonic-mean TEPS or documented equivalent |
| `notes` | string \| null | Optional | Skips, envelope exceptions, partial LDBC labels |

**Where reports land:** harness-chosen path (e.g. `evidence/<run_id>/<step>.json`).
This repo does not prescribe object-storage layout — only the fields above.

## Pinned generator / driver identity

Native results record the actual generator source and executable hashes,
profile identities, and engine executable hashes automatically. Active profiles
pin the current generator source; historical receipts retain their original
identities. The [generator provenance](../../benchmarks/runners/graph500-generator/README.md#reference-provenance-and-verification)
identifies the copied Graph500 helper and the independently verified tuple
contract. No historical external manifest is required for native results.

When an evaluation uses upstream tools directly, disclose their actual release
or commit and relevant configuration:

| Tooling | Official home | Disclosure required in evidence |
|---|---|---|
| Graph500 reference impl | [github.com/graph500/graph500](https://github.com/graph500/graph500) · [graph500.org spec](https://graph500.org/?page_id=12) | `generator.name`, `source` URL, **tag or release** (e.g. `3.0.0`) **or** full git `commit`, plus configure flags if non-default |
| LDBC SNB Datagen | [ldbc_snb_datagen_spark](https://github.com/ldbc/ldbc_snb_datagen_spark) | commit **or** release tag; serializer; SF |
| LDBC SNB / BI drivers | [github.com/ldbc](https://github.com/ldbc) Interactive/BI driver repos | commit **or** release; workload (`interactive`/`bi`); SF |
| Graphalytics driver + datasets | [ldbc_graphalytics](https://github.com/ldbc/ldbc_graphalytics) · [datasets repo](https://repository.surfsara.nl/datasets/cwi/graphalytics) | driver commit/release; dataset id (e.g. `wiki-Talk`); Graphalytics spec version |
| FinBench datagen / driver | [FinBench home](https://ldbcouncil.org/benchmarks/finbench/) · GDC FinBench repos | commit/release; SF; Transaction workload |

Also disclose **GDC/LDBC specification version** (PDF or docs tag) for any LDBC
claim. Prefer tags when available; if only `main`, record the commit SHA.
Re-runs that change generator/driver identity are different evidence — do not
silently mix.

## Current product boundary

- Benchmark generators and drivers remain outside the public product API.
- Full Graph500 and LDBC scale runs remain outside ordinary CI; bounded tiny
  lifecycle and generator regressions run in CI.
- Derived density-matrix results are labeled derived, without official Graph500
  submission claims.

### Historical SCALE-20 reference client

[perf-g500-scale20.md](../development/perf-g500-scale20.md) describes the older client using Graph500
**parameters** (SCALE / ef=16, undirected Kronecker) through published
`GraphForge` bulk ingest, reopen, measured GSI, and `LIMIT 1000` Cypher. It is
**not** Official-track: the generator is bench-local, evidence must not set
`track: "official"`, and `teps` stays null. SCALE-6 is CI; SCALE-20 is
`make bench-g500-scale20` only.

### Historical bounded scale client (#736)

The historical #736 client in
[perf-g500-ladder.md](../development/perf-g500-ladder.md) extended the SCALE-20
client with a versioned, **bounded-memory** ladder
(SCALE-20 → SCALE-26). Unlike the SCALE-20 client it does **not** retain raw
tuples in memory: it spills sorted runs and k-way merges, so peak resident
edges are independent of total edge count. Every attempted rung reconciles
`raw_attempts == live_unique_edges + self_loops_rejected + duplicates_rejected`,
and the ladder stops at the **first** envelope (RSS / disk / time) violation
rather than making an unsupported SCALE-26 claim. Declared Linux cloud SKU
capacity is 128 GiB RSS / 1 TiB NVMe with a provisional **4 h** wall-clock
fail-safe (#745; not a laptop SLA). Still **not** Official-track and **not**
TEPS, and it does **not** certify one billion live edges (that is #745).
SCALE-10 is CI; larger rungs are provisioned cloud / `make bench-g500-ladder`
only. This external-sort/deduplication path is not the current #900 native
streaming generator; its discarded-edge accounting and cloud envelope must not
be applied to the current raw-tuple lifecycle.

---

## Further reading

- [Native lifecycle host runbook](../../benchmarks/README.md) — current in-tree execution and evidence
- [Graph500 input generator](../../benchmarks/runners/graph500-generator/README.md) — current raw tuples, scrambling, and independent vectors
- [Historical SCALE-20 client](../development/perf-g500-scale20.md) — public-facade engineering green (not Official-track)
- [Native host and historical scale clients](../development/perf-g500-ladder.md) — M5 #736 first-fail contract (bounded memory, not a billion-edge claim)
- [Graph Scale Index](graph-scale-index.md) — size axis (node band + density)
- [Scale Limits](scale-limits.md) — product envelopes; disk-limited DataFusion framing
- [LDBC full suite](../guide/datasets/ldbc.md) — SNB, Graphalytics, FinBench, SPB
- [Graph500 benchmark specification](https://graph500.org/?page_id=12)
- [Graph Data Council / LDBC](https://ldbcouncil.org/)
