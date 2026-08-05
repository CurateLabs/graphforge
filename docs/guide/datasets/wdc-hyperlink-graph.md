# WDC Hyperlink Graphs

> **Status:** Research / scale-validation track — not a shipped `graphforge.datasets`
> catalog loader. Use the retrieval helper and construction APIs below. Related
> issues: [#399](https://github.com/CurateLabs/graphforge/issues/399) (this guide +
> retrieval), [#400](https://github.com/CurateLabs/graphforge/issues/400) (ingest),
> [#401](https://github.com/CurateLabs/graphforge/issues/401) (escalation spike),
> [#402](https://github.com/CurateLabs/graphforge/issues/402) (CSR/reopen costs).
> Host/Page full success is deferred ([#403](https://github.com/CurateLabs/graphforge/issues/403),
> [#404](https://github.com/CurateLabs/graphforge/issues/404)).

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

**Primary stretch target:** 2014 Pay-Level-Domain (PLD) graph — ~13M nodes /
~56M arcs. Full Host and Page success are explicit non-goals for the current
posture.

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
  (hundreds of parts). Use only as **sampling** material — never as a full-Page
  success criterion ([#404](https://github.com/CurateLabs/graphforge/issues/404)).

Tiny format samples from WDC (106 nodes / 141 arcs):

- https://webdatacommons.org/hyperlinkgraph/data/example_index
- https://webdatacommons.org/hyperlinkgraph/data/example_arcs
- https://webdatacommons.org/hyperlinkgraph/data/example.net (Pajek; not used here)

---

## Tiered scale escalation strategy

Escalate only when the previous tier is green. Early-tier success means
**neighborhood-proportional Cypher with `LIMIT`**, project **reopen**, and
**disk/RSS** evidence — not full-graph PageRank or Host/Page completion.

| Tier | Dataset | Approx. size | Prerequisites / exit from prior | Supported workloads | Resource envelope (guidance) | Stop / non-goals |
|------|---------|--------------|----------------------------------|---------------------|------------------------------|------------------|
| **T0** | WDC example Index/Arc | 106 nodes / 141 arcs | None | Full LIMIT Cypher; smoke ingest; reopen | Seconds; &lt; 1 GB disk/RSS | Do not skip straight to PLD |
| **T1** | Head sample of 2012 PLD Index/Arc | ~100K nodes / ~0.5–2M arcs (cap) | T0 green | LIMIT 1–2 hop; optional CSR warm | Low tens of GB disk headroom; RSS bounded by batching | No analyst full-graph verbs required |
| **T2** | Larger 2012 PLD shard | ~1M nodes / ~5–15M arcs (cap) | T1 green LIMIT + reopen | LIMIT traversal; CSR warm; disk/RSS ledger | Comparable to mid LiveJournal path; expect multi-GB project | Stop if RSS/disk exceeds machine budget |
| **T3** | Full **2014 PLD** | ~13M nodes / ~56M arcs | T2 green; WebGraph→Arc conversion path documented/executed | LIMIT Cypher stretch; reopen; CSR cost ([#402](https://github.com/CurateLabs/graphforge/issues/402)) | Beyond Levels 01–06 node band; multi-tens of GB likely; UUID map + Parquet dominate | **No** Host/Page; **no** page-scale PageRank as pass |
| **T4** | Optional full **2012 PLD** | ~43M nodes / ~623M arcs | Explicit go after T3; #336/#338/#340 dispositions | LIMIT-only exploration if attempted | Far beyond current posture; arc download alone ~2.7 GiB compressed | Default **stop**; treat as research exception |
| **T5** | **Host** (2014 then 2012) | 22M/123M → 101M/2B | Deferred [#403](https://github.com/CurateLabs/graphforge/issues/403) | N/A until posture change | — | **Non-goal** now |
| **T6** | **Page** | 1.7B/64B → 3.5B/128B | Deferred [#404](https://github.com/CurateLabs/graphforge/issues/404) | Shard sampling only under T1-style caps | Page shard lists are multi-hundred files / hundreds of GB | **Non-goal** for full graph |

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

---

## Retrieval (easy and reliable)

Use the checked-in helper. It resumes interrupted downloads (`curl -C -`),
verifies `Content-Length` when the server provides it, and checks published
MD5 sums where WDC publishes them.

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

#### Page shard lists (sampling only)

```bash
# 2014: 52 index parts, 479 arc parts
wget -i http://webdatacommons.org/hyperlinkgraph/2014-04/data/index.list.txt
wget -i http://webdatacommons.org/hyperlinkgraph/2014-04/data/arc.list.txt

# 2012: 83 index parts, 697 arc parts
wget -i http://webdatacommons.org/hyperlinkgraph/2012-08/data/index.list.txt
wget -i http://webdatacommons.org/hyperlinkgraph/2012-08/data/arc.list.txt
```

Or: `python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact page-lists-2014`.

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

1. **Retrieve** Index/Arc (T0–T2 from example / 2012 PLD; T3 after WebGraph→Arc).
2. **Offline convert** streaming TSV → Arrow `RecordBatch`es:
   - Nodes: label e.g. `PLD` / `Host` / `Page`; property `wdc_id` (int) and/or
     `name` (PLD string from index).
   - Edges: type e.g. `LINKS_TO`; endpoints as committed node UUIDs.
3. **UUID mapping:** GraphForge identity is UUIDv7 at the API. Maintain a
   durable `wdc_id → uuid` map (memory-mapped or on-disk) — this dominates RAM
   at PLD scale if held naïvely as a Python dict.
4. **Publish** chunked batches via `publish_bulk_nodes` / `publish_bulk_edges`
   (or Python `add_nodes` / `add_edges`) with stable UUIDv7 `operation_uuid`s.
5. **Ontology:** use **exploratory** mode for raw web graphs
   ([exploratory analyst](../exploratory-analyst.md)).
6. **CSR warm:** `index_adjacency()` / inspect before LIMIT benches.
7. **Reopen:** `GraphForge(project_dir)` and re-count nodes/edges.

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

| Tier | Green means |
|------|-------------|
| T0 | Example fetched (106/141); ingest + reopen counts match; `LIMIT` query returns |
| T1 | Capped 2012 PLD sample ingested with streaming batches; LIMIT 1-hop + 2-hop; RSS/disk recorded |
| T2 | Larger shard; CSR warm completes or fails with explicit ceiling citation (#336); LIMIT evidence; reopen OK |
| T3 | Full 2014 PLD only after conversion path; LIMIT + reopen + resource ledger; **no** Host/Page |
| T4+ | Written exception + machine budget; default is **do not run** |

Attach evidence to [#401](https://github.com/CurateLabs/graphforge/issues/401) /
[#402](https://github.com/CurateLabs/graphforge/issues/402). Optional for M4 exit
[#345](https://github.com/CurateLabs/graphforge/issues/345); **not** required to
close [#335](https://github.com/CurateLabs/graphforge/issues/335).

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
