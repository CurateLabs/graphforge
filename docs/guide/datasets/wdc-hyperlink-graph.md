# WDC Hyperlink Graphs

> **Status:** Research / scale-validation track — not a shipped `graphforge.datasets`
> catalog loader. Use the retrieval helper and construction APIs below. Related
> issues: [#399](https://github.com/CurateLabs/graphforge/issues/399) (this guide +
> retrieval), [#406](https://github.com/CurateLabs/graphforge/issues/406) (R2 mirror
> provision), [#407](https://github.com/CurateLabs/graphforge/issues/407) (scale-runner
> runbook), [#400](https://github.com/CurateLabs/graphforge/issues/400) (ingest),
> [#401](https://github.com/CurateLabs/graphforge/issues/401) (first-fail ladder spike),
> [#402](https://github.com/CurateLabs/graphforge/issues/402) (CSR/reopen costs),
> [#403](https://github.com/CurateLabs/graphforge/issues/403) (Host T5),
> [#404](https://github.com/CurateLabs/graphforge/issues/404) (Page T6).

[Web Data Commons (WDC) Hyperlink Graphs](https://webdatacommons.org/hyperlinkgraph/)
are public directed hyperlink graphs extracted from Common Crawl (2012 and 2014).
They are among the largest openly downloadable web graphs and are useful for
stressing ingest, adjacency/CSR construction, reopen, and neighborhood-proportional
Cypher with `LIMIT`.

GraphForge is an embedded notebook/research workbench with a
[Levels 01–06](../../reference/scale-limits.md) product posture (V &lt; 10M as the
primary interactive band). Proven fixed-hop `LIMIT` evidence on the release path
reaches LiveJournal (~4.0M nodes / ~34.7M edges). WDC is therefore an **external
escalation ladder**, not a claim that GraphForge is a billion-edge analytics
engine.

**Ladder policy:** run the full ordered ladder **T0 → T6** from the beginning.
Each tier must meet its acceptance criteria before the next is attempted. On the
**first failed tier**, stop escalation — do not proceed to larger tiers. Record
which tier failed and why. Host (T5) and Page (T6) are **active later steps** in
that ladder; first-fail is the control for resource risk, not a promise that
Host/Page will pass.

---

## Dataset inventory

Counts are from the [WDC Hyperlink Graphs overview](https://webdatacommons.org/hyperlinkgraph/)
(verified 2026-08-05). Millions are as published by WDC.

### 2012 (August 2012 Common Crawl)

| Aggregation | Nodes | Arcs | Notes |
|-------------|------:|-----:|-------|
| Page | 3,563M | 128,736M | Sharded Index/Arc + WebGraph |
| Host (subdomain) | 101M | 2,043M | Index/Arc published |
| PLD | 43M | 623M | Index/Arc + Pajek |

Download instructions: [2012-08 download page](https://webdatacommons.org/hyperlinkgraph/2012-08/download.html).

### 2014 (Spring 2014 Common Crawl)

| Aggregation | Nodes | Arcs | Notes |
|-------------|------:|-----:|-------|
| Page | 1,727M | 64,422M | Sharded Index/Arc + WebGraph |
| Host | 22M | 123M | Index + WebGraph (no Index/Arc arcs) |
| PLD | 13M | 56M | Index + WebGraph (no Index/Arc arcs) |

Download instructions: [2014-04 download page](https://webdatacommons.org/hyperlinkgraph/2014-04/download.html).
Artifact storage paths use the `2014-03` prefix on
`data.dws.informatik.uni-mannheim.de` (WDC’s published links).

WDC notes that 2012 is generally a better sample of Web connectivity (BFS crawl
with URL discovery); 2014 used a fixed seed list. Prefer **2014 PLD for size**
inside GraphForge’s stretch band; prefer **2012 Index/Arc** when you need
tab-separated arcs without a WebGraph conversion step.

---

## Formats and GraphForge support

| Format | What it is | GraphForge |
|--------|------------|------------|
| **Index/Arc** (TSV) | Index: `name\tid`; Arc: `src_id\tdst_id` (tab-delimited, often `.gz`) | **Recommended.** Offline convert → Arrow → chunked bulk publish. |
| **WebGraph** (BVGraph) | `.graph` / `.offsets` / `.properties` | **Not supported in-tree.** Convert externally to Index/Arc before ingest. |
| **Pajek NET** | Combined PLD file (2012) | **Not supported in-tree.** Prefer Index/Arc. |

### Verified format reality (important)

- **2012 PLD / Host:** Index/Arc files are published and are the Index/Arc-native
  ladder for GraphForge.
- **2014 Host / PLD:** WDC publishes **index** files and **WebGraph** topology;
  Index/Arc **arc** files for those aggregations are **not** linked on the 2014
  download page (verified against the live page). Full 2014 PLD edges require a
  WebGraph → arc-list conversion (out of tree) or a future ingest helper that
  understands BVGraph — see [#400](https://github.com/CurateLabs/graphforge/issues/400) /
  [#401](https://github.com/CurateLabs/graphforge/issues/401).
- **2014 / 2012 Page:** Index/Arc shards via `index.list.txt` / `arc.list.txt`
  (hundreds of parts). Full-Page ingest is ladder step **T6** and is attempted
  only after T0–T5 are green; expect extreme disk/RSS risk and likely first-fail
  stop ([#404](https://github.com/CurateLabs/graphforge/issues/404)).

Tiny format samples from WDC (106 nodes / 141 arcs):

- https://webdatacommons.org/hyperlinkgraph/data/example_index
- https://webdatacommons.org/hyperlinkgraph/data/example_arcs
- https://webdatacommons.org/hyperlinkgraph/data/example.net (Pajek; not used here)

---

## Tiered scale escalation strategy

### First-fail policy (authoritative)

1. **Order is fixed:** attempt tiers strictly **T0 → T1 → T2 → T3 → T4 → T5 → T6**
   (year sub-order inside T5/T6 is fixed below). Do not skip ahead.
2. **Prior green required:** a tier may start only after every earlier tier met its
   acceptance criteria (see [Validation](#validation--acceptance-per-tier)).
3. **Stop on first fail:** if a tier fails acceptance (ingest OOM, disk ceiling,
   CSR/reopen failure, LIMIT contract miss, missing conversion path, or explicit
   resource stop), **stop the ladder**. Do not attempt any larger tier.
4. **Record the stop:** cite the failed tier id, failure class, and measured
   disk/RSS/time (or the prerequisite that blocked start). Attach to
   [#401](https://github.com/CurateLabs/graphforge/issues/401).
5. **Honesty over ambition:** Host/Page remain on the ladder so escalation is not
   artificially capped at PLD, but first-fail — not pretend pass — governs
   whether they ever run. Resource envelopes below are guidance; machine budget
   may force an earlier stop.

Early- and mid-tier success means **neighborhood-proportional Cypher with
`LIMIT`**, project **reopen**, and **disk/RSS** evidence — not full-graph
PageRank as a pass criterion.

### Exact tier order

| Step | Tier | Dataset (exact variant) | Approx. size | Format notes |
|------|------|-------------------------|--------------|--------------|
| 1 | **T0** | WDC example Index/Arc | 106 / 141 | Index/Arc |
| 2 | **T1** | 2012 PLD Index/Arc **head sample** | ~100K nodes / ~0.5–2M arcs (cap) | Cap via ingest tool |
| 3 | **T2** | 2012 PLD Index/Arc **larger shard** | ~1M nodes / ~5–15M arcs (cap) | Cap via ingest tool |
| 4 | **T3** | Full **2014 PLD** | ~13M / ~56M | WebGraph → Arc conversion required |
| 5 | **T4** | Full **2012 PLD** | ~43M / ~623M | Index/Arc published |
| 6 | **T5a** | Full **2014 Host** | ~22M / ~123M | Index + WebGraph (no Index/Arc arcs) |
| 7 | **T5b** | Full **2012 Host** | ~101M / ~2,043M | Index/Arc published |
| 8 | **T6a** | Full **2014 Page** | ~1,727M / ~64,422M | Sharded Index/Arc (+ WebGraph) |
| 9 | **T6b** | Full **2012 Page** | ~3,563M / ~128,736M | Sharded Index/Arc (+ WebGraph) |

Treat **T5a → T5b** as ordered sub-steps of T5, and **T6a → T6b** as ordered
sub-steps of T6: failing T5a stops before T5b; failing T6a stops before T6b.
Failing any PLD tier (T0–T4) stops before Host/Page.

### Tier table (workloads and envelopes)

| Tier | Prerequisites | Supported workloads | Resource envelope (guidance) | Failure / stop examples |
|------|---------------|---------------------|------------------------------|-------------------------|
| **T0** | None | LIMIT Cypher; smoke ingest; reopen | Seconds; &lt; 1 GB disk/RSS | Skip to PLD without T0 green |
| **T1** | T0 green | LIMIT 1–2 hop; optional CSR warm | Low tens of GB disk headroom; RSS bounded by batching | Cap ingest fails; LIMIT/reopen miss |
| **T2** | T1 green LIMIT + reopen | LIMIT traversal; CSR warm; disk/RSS ledger | Mid LiveJournal class; multi-GB project | RSS/disk over machine budget |
| **T3** | T2 green; WebGraph→Arc path executed | LIMIT stretch; reopen; CSR cost ([#402](https://github.com/CurateLabs/graphforge/issues/402)) | Beyond V &lt; 10M band; multi-tens of GB likely | Conversion missing; CSR/reopen fail |
| **T4** | T3 green | LIMIT-only exploration | Arc download ~2.7 GiB compressed; far beyond Levels 01–06 | Disk/RSS/time ceiling |
| **T5a/b** | T4 green ([#403](https://github.com/CurateLabs/graphforge/issues/403)) | LIMIT + reopen if attempted | Extreme; 2012 Host arcs alone ~9 GiB compressed | First-fail stop expected if envelopes breach |
| **T6a/b** | T5b green ([#404](https://github.com/CurateLabs/graphforge/issues/404)) | LIMIT + reopen if attempted | Hundreds of shard files; hundreds of GB–TB class | First-fail stop expected; do not claim success without evidence |

### Workload classes

| Class | When allowed |
|-------|----------------|
| Fixed-hop Cypher + `LIMIT` | T0+ when adjacency is warm (or document miss) |
| Project close/reopen | T0+ |
| CSR build / inspect timing | T1+ ([#402](https://github.com/CurateLabs/graphforge/issues/402)) |
| Analyst verbs (PageRank, etc.) | Only after LIMIT+reopen green **and** within algorithm memory caps; never a T0–T2 pass criterion |
| Full-graph aggregations / unconstrained `ORDER BY` | Separate, tighter ceiling — not WDC early-tier success |

### Anchors from in-repo benches

From [scale limits](../../reference/scale-limits.md) (release fixed-hop `LIMIT 1000` on Apple Silicon warm runs — shape evidence, not SLO):

| Graph | Nodes / edges | 1-hop / 2-hop `LIMIT 1000` |
|-------|---------------|---------------------------|
| Deterministic | 625K / 10M | ~113 ms / ~111 ms |
| LiveJournal | 4.0M / 34.7M | ~66 ms / ~90 ms |

WDC T2 should look operationally similar to the LiveJournal class for LIMIT work
if ingest and CSR succeed. T3 is a deliberate stretch past the V &lt; 10M band.
T5–T6 are on the ladder for completeness under first-fail; they are not implied
product support.

---

## Retrieval (easy and reliable)

Use the checked-in helper. It resumes interrupted downloads (`curl -C -`),
verifies `Content-Length` when the server provides it, and checks published
MD5 sums where WDC publishes them.

**Not part of normal CI.** WDC PLD+/Host/Page packs are too large for GitHub
Actions / LFS / Release assets. Fetch + ladder runs happen on dedicated scale
runners (or maintainer machines) against a CurateLabs-controlled mirror when
available, with optional WDC origin fallback.

### Quick start

```bash
# Example graph only (T0) — default
make fetch-wdc-hyperlink

# Or explicitly:
python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact example

# 2012 PLD Index/Arc (large: ~297 MB + ~2.7 GB compressed)
python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact pld-2012

# 2014 PLD index + WebGraph topology (edges still need conversion)
python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact pld-2014-webgraph

# Verify catalog URLs with HEAD only (no download)
python3 scripts/datasets/fetch_wdc_hyperlink.py --list
python3 scripts/datasets/fetch_wdc_hyperlink.py --verify-urls
```

Default cache root: `${GF_WDC_CACHE:-$HOME/.cache/graphforge/wdc-hyperlink}`.

### Controlled mirror (recommended for scale runners)

WDC origin hosts are public research infrastructure: they can rate-limit, require
a Referer, or become slow for multi-GiB resumes. GraphForge/CurateLabs should
host a **flat object-storage mirror** of the ladder artifacts we actually run,
so first-fail spikes ([#401](https://github.com/CurateLabs/graphforge/issues/401))
have a reliable fetch path under our control.

**Provider:** prefer **Cloudflare R2** (S3-compatible API, **zero egress** to the
internet, public custom domain or `r2.dev` for anonymous `curl`). AWS S3 is a
fallback but egress (~$0.09/GB) dominates once Host (~9 GiB) or Page (tens–hundreds
of GB) are pulled repeatedly. Railway Buckets match R2 on storage (~$0.015/GB-month)
and free bucket egress, but are **private-only** (presigned URLs / proxy) — worse
fit for simple resume downloads on arbitrary runners. Do **not** put these blobs
in git, GitHub LFS, or Release assets.

**Bucket layout:** object keys mirror the local cache relpaths (no WDC hostname
prefix):

```text
s3://graphforge-wdc/wdc-hyperlink/
  example/example_index
  example/example_arcs
  2012-08/pld-index.gz
  2012-08/pld-arc.gz
  2014-03/webgraph/…
  page-2014/index.list.txt
  …
```

Public base URL example: `https://wdc.<your-domain>/wdc-hyperlink` → set as
`GF_WDC_MIRROR_BASE`.

**Checksum policy:** preserve WDC `Content-Length` and published MD5 values from
the helper catalog. Maintainers must verify locally (size/md5/line-count) **before**
upload; the sync helper refuses to plan an upload for failing files. After mirror
fetch, runners re-verify the same catalog checks.

**Access model:** public read on the mirror prefix is fine for these already-public
research corpora (still subject to Common Crawl / WDC terms). Prefer anonymous
HTTPS over signed URLs so scale runners need only `GF_WDC_MIRROR_BASE`. If the
bucket must stay private, generate short-lived signed URLs and pass each as an
override — not the default path.

#### Environment variables

| Variable | Role |
|----------|------|
| `GF_WDC_CACHE` | Local cache root (default `~/.cache/graphforge/wdc-hyperlink`) |
| `GF_WDC_MIRROR_BASE` | HTTPS base for the controlled mirror (keys = cache relpaths) |
| `GF_WDC_SOURCE` | `mirror-first` (default when mirror set), `mirror-only`, or `origin` |
| `GF_WDC_MIRROR_S3_URI` | Maintainer upload dest, e.g. `s3://graphforge-wdc/wdc-hyperlink` |
| `GF_WDC_MIRROR_ENDPOINT` | S3 API endpoint (R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`) |

#### Scale-runner fetch (mirror-only)

```bash
export GF_WDC_CACHE="${GF_WDC_CACHE:-$HOME/.cache/graphforge/wdc-hyperlink}"
export GF_WDC_MIRROR_BASE="https://wdc.<your-domain>/wdc-hyperlink"
export GF_WDC_SOURCE=mirror-only

# T0 smoke
python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact example

# Ladder PLD packs (only when the runner is executing those tiers)
python3 scripts/datasets/fetch_wdc_hyperlink.py \
  --artifact pld-2014-webgraph --artifact pld-2012

# HEAD-check mirror objects + expected Content-Length
python3 scripts/datasets/fetch_wdc_hyperlink.py --verify-urls \
  --artifact example --artifact pld-2012
```

`mirror-first` tries the mirror, then falls back to WDC origin (with Referer).
Use `mirror-only` on controlled scale runs so a missing mirror object fails loud
instead of silently pulling terabytes from Mannheim.

#### Maintainer sync (bootstrap once, refresh as needed)

Human ops — **not** automated in CI. Minimum bootstrap for T0–T3 ladder work:
`example`, `pld-2014-webgraph`, `pld-2012` (~3.3 GiB compressed). Host/Page packs
are later ladder steps; mirror them only when [#401](https://github.com/CurateLabs/graphforge/issues/401)
escalation requires them (bucket provision: [#406](https://github.com/CurateLabs/graphforge/issues/406);
runner runbook: [#407](https://github.com/CurateLabs/graphforge/issues/407)).

```bash
# 1) Pull from WDC origin into local cache (verify size/md5)
GF_WDC_SOURCE=origin python3 scripts/datasets/fetch_wdc_hyperlink.py --tier-min
# (or: --artifact example --artifact pld-2014-webgraph --artifact pld-2012)

# 2) Dry-run: re-verify cache, print aws/rclone commands
python3 scripts/datasets/sync_wdc_mirror.py --tier-min

# 3) Upload (requires R2 API token in AWS_* env vars)
export GF_WDC_MIRROR_S3_URI=s3://graphforge-wdc/wdc-hyperlink
export GF_WDC_MIRROR_ENDPOINT=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
python3 scripts/datasets/sync_wdc_mirror.py --tier-min --execute
```

Approximate compressed footprint / storage (R2 ~$0.015/GB-month; egress $0):

| Pack | Compressed | Notes |
|------|-----------:|-------|
| T0 example | ~2.5 KB | Always mirror |
| 2014 PLD WebGraph | ~321 MB | T3 |
| 2012 PLD Index/Arc | ~3.0 GiB | T4 (also used for T1/T2 capped samples) |
| 2014 Host | ~660 MB | T5a — mirror when ladder reaches it |
| 2012 Host | ~9.4 GiB | T5b |
| Page shards | tens–hundreds of GB | T6 — do not bootstrap “just in case” |

#### Cost / bandwidth notes

- **R2 / Railway bucket egress:** $0 for object reads. Storage for T0–T4 mirror
  ≈ 3.3 GiB → on the order of **$0.05/month**.
- **AWS S3 Standard egress:** ~$0.09/GB → one full 2012 Host pull ≈ **$0.85**;
  repeated Page pulls dominate — avoid for runner downloads.
- **Normal CI:** must not download WDC packs. Keep this track on dedicated runners.

### Recommended cache layout

```text
$GF_WDC_CACHE/
  example/
    example_index
    example_arcs
  2012-08/
    pld-index.gz
    pld-arc.gz
    sd-index.gz          # Host, optional
    sd-arc.gz
  2014-03/
    webgraph/
      index.pld.gz
      pldgraph.graph
      pldgraph.offsets
      pldgraph.properties
      README             # contains md5 lines
  page-2014/
    index.list.txt
    arc.list.txt
    # parts fetched on demand only
```

### Exact URLs (verified live)

**Referer:** send a WDC download-page Referer; some `data.dws…` hosts return **403**
without one. The helper sets this automatically.

#### Example (T0)

| File | URL | Verified |
|------|-----|----------|
| index | https://webdatacommons.org/hyperlinkgraph/data/example_index | 106 lines |
| arcs | https://webdatacommons.org/hyperlinkgraph/data/example_arcs | 141 lines |

#### 2012 Index/Arc (PLD / Host)

| Artifact | URL | Content-Length (bytes, HEAD) |
|----------|-----|------------------------------:|
| PLD index | https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/pld-index.gz | 311,068,910 |
| PLD arcs | https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/pld-arc.gz | 2,912,232,966 |
| Host index | https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/sd-index.gz | 871,791,708 |
| Host arcs | https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/sd-arc.gz | 9,216,059,662 |

Pages: [2012-08 download](https://webdatacommons.org/hyperlinkgraph/2012-08/download.html).

#### 2014 PLD (index + WebGraph)

| Artifact | URL | Content-Length (bytes, HEAD) | MD5 (from WDC README) |
|----------|-----|------------------------------:|------------------------|
| PLD index | https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/index.pld.gz | 168,635,660 | `ab13f50eb5ffb4b62c1a0cdd69a4f749` |
| pldgraph.graph | https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/pldgraph.graph | 139,534,900 | `8cafd7e62f198ad4cd13ce8dd1c0e5c4` |
| pldgraph.offsets | https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/pldgraph.offsets | 12,867,176 | `c6c8aaf950e2fbffd893fe00106aefab` |
| pldgraph.properties | https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/pldgraph.properties | 1,163 | `ab377a864493202923a617c755c81785` |
| README (md5 list) | https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/README | 1,890 | — |

Pages: [2014-04 download](https://webdatacommons.org/hyperlinkgraph/2014-04/download.html).

#### Page shard lists (T6; fetch only after T0–T5 green)

```bash
# 2014: 52 index parts, 479 arc parts
wget -i http://webdatacommons.org/hyperlinkgraph/2014-04/data/index.list.txt
wget -i http://webdatacommons.org/hyperlinkgraph/2014-04/data/arc.list.txt

# 2012: 83 index parts, 697 arc parts
wget -i http://webdatacommons.org/hyperlinkgraph/2012-08/data/index.list.txt
wget -i http://webdatacommons.org/hyperlinkgraph/2012-08/data/arc.list.txt
```

Or: `python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact page-lists-2014`.
Do not download full Page shards “just in case” before earlier tiers are green.

### Retry / resume

- Prefer the helper (`curl --continue-at -` under the hood).
- Re-run the same command after network failure; completed files with matching
  size/md5 are skipped.
- For page shards, `wget -c -i …` also resumes.
- Disk: keep ≥ 2× compressed size free before PLD downloads (decompress + Parquet).

### License / terms

Extracted data follows Common Crawl terms of use / disclaimer (see WDC download
pages). The WDC extraction framework is Apache-2.0; that does not replace corpus
terms for the graph files themselves.

---

## Ingest recipe (architecture-aligned)

There is **no** shipped WDC loader in `graphforge-io` today. Follow the public
bulk construction path ([graph construction](../graph-construction.md)):

1. **Retrieve** artifacts for the **current** ladder tier only (T0–T2: example /
   capped 2012 PLD Index/Arc; T3/T5a: WebGraph→Arc where Index/Arc arcs are
   unpublished; T4/T5b: 2012 Index/Arc; T6: page shard lists after T5 green).
2. **Offline convert** streaming TSV → Arrow `RecordBatch`es:
   - Nodes: label e.g. `PLD` / `Host` / `Page`; property `wdc_id` (int) and/or
     `name` (PLD string from index).
   - Edges: type e.g. `LINKS_TO`; endpoints as committed node UUIDs.
3. **UUID mapping:** GraphForge identity is UUIDv7 at the API. Maintain a
   durable `wdc_id → uuid` map (memory-mapped or on-disk) — this dominates RAM
   at PLD+ scale if held naïvely as a Python dict.
4. **Publish** chunked batches via `publish_bulk_nodes` / `publish_bulk_edges`
   (or Python `add_nodes` / `add_edges`) with stable UUIDv7 `operation_uuid`s.
5. **Ontology:** use **exploratory** mode for raw web graphs
   ([exploratory analyst](../exploratory-analyst.md)).
6. **CSR warm:** `index_adjacency()` / inspect before LIMIT benches.
7. **Reopen:** `GraphForge(project_dir)` and re-count nodes/edges.
8. **Escalate or stop:** apply the [first-fail policy](#first-fail-policy-authoritative)
   before fetching or ingesting the next tier.

Tracked implementation: [#400](https://github.com/CurateLabs/graphforge/issues/400).

Illustrative shape (not a shipped API):

```python
from graphforge import GraphForge

forge = GraphForge("wdc-t0/")  # directory must exist
# After offline conversion to Arrow batches:
# forge.publish_bulk_nodes(operation_uuid, node_batch)
# forge.publish_bulk_edges(operation_uuid, edge_batch)
forge.index_adjacency()
table = forge.execute("""
  MATCH (a)-[:LINKS_TO]->(b)
  RETURN a.name AS src, b.name AS dst
  LIMIT 1000
""")
```

---

## Validation / acceptance per tier

A tier is **green** only when all rows below for that tier succeed. A **red**
(fail) on any criterion ends the ladder at that tier (first-fail).

| Tier | Green (acceptance) criteria |
|------|-----------------------------|
| **T0** | Example fetched (106/141); ingest + reopen counts match; `LIMIT` query returns |
| **T1** | Capped 2012 PLD sample ingested with streaming batches; LIMIT 1-hop + 2-hop; RSS/disk recorded |
| **T2** | Larger shard; CSR warm completes **or** fails with explicit ceiling citation (#336) treated as tier fail; LIMIT evidence; reopen OK |
| **T3** | Full 2014 PLD after conversion path; LIMIT + reopen + resource ledger; counts match published scale (± documented sample policy) |
| **T4** | Full 2012 PLD; LIMIT + reopen + resource ledger within machine budget |
| **T5a** | Full 2014 Host after conversion path; LIMIT + reopen + resource ledger |
| **T5b** | Full 2012 Host Index/Arc; LIMIT + reopen + resource ledger |
| **T6a** | Full 2014 Page (all shards or documented complete ingest); LIMIT + reopen + resource ledger |
| **T6b** | Full 2012 Page; LIMIT + reopen + resource ledger |

**Red / stop examples (any one):** OOM or disk exhaustion; missing WebGraph→Arc
conversion when required; reopen count mismatch; LIMIT path unavailable after CSR
warm attempt; wall-time or RSS beyond the operator’s pre-declared machine
envelope for that run.

Attach per-tier green/red evidence to
[#401](https://github.com/CurateLabs/graphforge/issues/401) /
[#402](https://github.com/CurateLabs/graphforge/issues/402). Optional for M4 exit
[#345](https://github.com/CurateLabs/graphforge/issues/345); **not** required to
close [#335](https://github.com/CurateLabs/graphforge/issues/335). First-fail stop
at or before Host/Page is a valid outcome — not a documentation gap.

---

## Related documentation

- [Scale limits](../../reference/scale-limits.md) — LIMIT contract and LiveJournal anchors
- [Graph construction](../graph-construction.md) — bulk Arrow publish
- [Exploratory analyst](../exploratory-analyst.md) — exploratory ontology mode
- [Datasets overview](overview.md) — planned catalog sources
- [Traversal scaling](https://github.com/CurateLabs/graphforge/blob/main/benchmarks/traversal_scaling.md)

## References

- [WDC Hyperlink Graphs](https://webdatacommons.org/hyperlinkgraph/)
- [2012 download](https://webdatacommons.org/hyperlinkgraph/2012-08/download.html) ·
  [2014 download](https://webdatacommons.org/hyperlinkgraph/2014-04/download.html)
- Meusel et al., *Graph Structure in the Web — Revisited* (WWW 2014 Web Science)
- Lehmberg et al., *The Graph Structure of the Web aggregated by Pay-Level Domain* (WebSci 2014)
