# v0.3.8 → v0.3.9 Performance Delta

**Date:** 2026-05-04
**Branch:** main
**Machine:** Apple Silicon (macOS 25.4, Python 3.13)

This document compares pre-optimisation baseline numbers (captured in
`perf-baseline-findings-v0.3.9.md` on 2026-05-02) against the current
v0.3.9 measurements, and extends them to L/XL-tier datasets to assess
the "small-to-medium graphs (<10M nodes)" claim in the README.

---

## Executive Summary

- **Parser overhead eliminated.** Switching from Earley to LALR(1) removed
  the dominant per-query constant — cold query latency on XS dropped from ~76 ms to
  ~0.2 ms (380×).
- **LIMIT short-circuit makes two-hop queries practical.** A two-hop query
  with `LIMIT 1000` on the M-tier (926k edges) now takes 44 ms instead of ~9 s;
  the same query scales to 660 ms at XL-tier (16.5M edges).
- **SQLite persistent backend unblocked.** Reload of the S-tier dataset
  previously hung indefinitely; it now completes in 0.17 s.
- **Full-scan queries don't scale beyond ~1M nodes.** `count_edges` at L-tier
  (34M edges) takes 260 s; `topn` (ORDER BY over 4M nodes) takes 155 s. These
  are structural bottlenecks not addressed by Phases 1–7.

---

## Node Count vs Edge Count: Why They Behave Differently

`count_nodes` and `count_edges` have fundamentally different performance
characteristics and should be read separately:

| Query | Data structure hit | Scaling |
|-------|--------------------|---------|
| `MATCH (n) RETURN count(n)` | In-memory dict scan | O(N nodes) |
| `MATCH ()-[r]->() RETURN count(r)` | Full adjacency-list iteration | O(N edges) |

For dense graphs (many edges per node), `count_edges` is dramatically slower.
At L-tier (34M edges vs 4M nodes), the edge scan takes 8.5× longer than the
node scan even in the warm case (218 s vs 2.4 s).

This distinction drives the entire scaling story: LIMIT-based queries are fast
at any tier because they stop early (edges never fully scanned), while
full-scan queries (`count_edges`, `aggregation`, `topn` with ORDER BY) are
gating bottlenecks at scale.

---

## Phase-by-Phase Improvements

| Phase | PR | What Changed |
|-------|----|--------------|
| 1 | | SQLite bulk I/O: `executemany` in save/load paths, `_save_graph_to_backend` wrapped in a single transaction. Fixed indefinitely-hanging reload at S/M tier. |
| 2 | | LIMIT short-circuit: `NodeScan` and `Expand` stop emitting rows once a downstream LIMIT is satisfied. Made one-hop, two-hop-with-LIMIT, and `topn` (when not ORDER BY) dramatically faster. |
| 3 | | Variable-length relationship predicate fixes and cross-hop edge uniqueness enforcement. Correctness improvements with negligible latency impact. |
| 4 | | Property equality index: O(1) node lookup by property value. Eliminated the cold-run penalty for `label_filter` on M-tier (300 ms → 227 ms cold). |
| 5 | | LALR(1) parser replacing Earley: O(n) parse time, near-zero constant overhead. Dropped XS cold latency from ~76 ms to ~0.2 ms across all query types. |
| 6–7 | — | Test and tooling quality improvements, correctness fixes. No measurable latency impact. |

---

## Full Comparison Tables

All times are wall-clock seconds. "Speedup" is `v0.3.8 / v0.3.9` (higher is better).
A value of `—` means the metric was not collected for that version.

### XS — karate (34 nodes / 78 edges)

| Metric | v0.3.8 | v0.3.9 | Delta | Speedup |
|--------|--------|--------|-------|---------|
| Ingest | 0.001 s | 0.001 s | — | 1× |
| count_nodes (cold) | 0.076 s | 0.0002 s | −0.076 s | **380×** |
| count_nodes (warm) | 0.076 s | 0.0002 s | −0.076 s | **380×** |
| count_edges (cold) | 0.095 s | 0.0002 s | −0.095 s | **475×** |
| count_edges (warm) | 0.095 s | 0.0002 s | −0.095 s | **475×** |
| one_hop (cold) | 0.129 s | 0.0018 s | −0.127 s | **72×** |
| one_hop (warm) | 0.110 s | 0.0004 s | −0.110 s | **275×** |
| topn (cold) | 0.127 s | 0.0005 s | −0.127 s | **254×** |
| topn (warm) | 0.107 s | 0.0004 s | −0.107 s | **268×** |
| two_hop_limit (cold) | — | 0.0014 s | — | — |
| SQLite write | 0.0004 s | 0.0004 s | — | 1× |
| SQLite reload | 0.0006 s | 0.0009 s | — | ~1× |

### S — facebook ego-net (4 039 nodes / 88 234 edges)

| Metric | v0.3.8 | v0.3.9 | Delta | Speedup |
|--------|--------|--------|-------|---------|
| Ingest | 0.19 s | 0.180 s | −0.010 s | 1.1× |
| count_nodes (cold) | 0.078 s | 0.0019 s | −0.076 s | **41×** |
| count_edges (cold) | 0.192 s | 0.081 s | −0.111 s | **2.4×** |
| count_edges (warm) | 0.193 s | 0.083 s | −0.110 s | **2.3×** |
| one_hop (cold) | 0.356 s | 0.0026 s | −0.353 s | **137×** |
| one_hop (warm) | 0.353 s | 0.0027 s | −0.350 s | **131×** |
| topn (cold) | 0.128 s | 0.021 s | −0.107 s | **6.1×** |
| two_hop_limit (cold) | — | 0.003 s | — | — |
| SQLite write | — (not tested) | 1.017 s | — | — |
| SQLite reload | — (was hanging) | 0.171 s | **fixed** | ∞ |

### M — amazon co-purchase (334 863 nodes / 925 872 edges)

| Metric | v0.3.8 | v0.3.9 | Delta | Speedup |
|--------|--------|--------|-------|---------|
| Ingest | 3.80 s | 4.07 s | +0.27 s | ~1× |
| count_nodes (cold) | 0.68 s | 0.191 s | −0.489 s | **3.6×** |
| count_nodes (warm) | 0.71 s | 0.622 s | −0.088 s | 1.1× |
| count_edges (cold) | 2.52 s | 2.445 s | −0.075 s | 1.0× |
| count_edges (warm) | 2.48 s | 2.509 s | +0.029 s | ~1× |
| label_filter (cold) | 0.30 s | 0.227 s | −0.073 s | 1.3× |
| one_hop (cold) | 4.24 s | 0.043 s | −4.20 s | **99×** |
| one_hop (warm) | 4.67 s | 0.042 s | −4.63 s | **110×** |
| aggregation (cold) | 1.51 s | 1.820 s | +0.310 s | 0.83× |
| topn (cold) | 3.31 s | 2.800 s | −0.510 s | 1.2× |
| topn (warm) | 2.87 s | 3.218 s | +0.348 s | 0.89× |
| two_hop_limit (cold) | — | 0.044 s | — | — |
| Peak RSS | — | ~794 MB | — | — |

### L — livejournal (3 997 962 nodes / 34 681 189 edges) — newly measured

First-ever run of L-tier. Measured on Apple Silicon, 2026-05-04.

| Metric | v0.3.9 | Notes |
|--------|--------|-------|
| Ingest | **139 s** | 34M `create_relationship` calls, no bulk path |
| Peak RSS | **+12 491 MB** (12.2 GB) | ~360 bytes/edge in Python dicts |
| count_nodes (cold) | 26.0 s | Allocates property equality index on first scan |
| count_nodes (warm) | 2.4 s | Index built; still O(N nodes) dict scan |
| count_edges (cold) | **260 s** | Full iteration of 34M adjacency entries |
| count_edges (warm) | **218 s** | No caching — repeated full scan |
| label_filter (cold) | 3.2 s | |
| one_hop (cold) | 0.74 s | LIMIT short-circuit works |
| one_hop (warm) | 0.56 s | |
| two_hop_limit (cold) | 0.76 s | LIMIT short-circuit works |
| two_hop_limit (warm) | 0.66 s | |
| aggregation (cold) | 17.5 s | Full node scan |
| topn (cold) | **155 s** | ORDER BY materialises all 4M nodes before sort |

### XL — cit-patents (3 774 768 nodes / 16 518 948 edges) — newly measured

Citation network; fewer edges per node than livejournal (~4.4 vs ~8.7 avg degree).

| Metric | v0.3.9 | Notes |
|--------|--------|-------|
| Ingest | **139 s** | Same bottleneck as L-tier |
| Peak RSS | **+5 757 MB** (5.6 GB) | Lower density → less memory |
| count_nodes (cold) | 3.4 s | Index build on first scan |
| count_nodes (warm) | 2.5 s | |
| count_edges (cold) | **197 s** | Full scan of 16.5M edges |
| count_edges (warm) | **56 s** | Some OS page caching helps |
| label_filter (cold) | 2.7 s | |
| one_hop (cold) | 0.50 s | LIMIT short-circuit works |
| two_hop_limit (cold) | 0.66 s | LIMIT short-circuit works |
| aggregation (cold) | 21 s | Full node scan |
| topn (cold) | **52 s** | ORDER BY over 3.77M nodes |

---

## Scaling Summary: Nodes vs Edges

This table shows how latency scales with data size, separated by node count vs edge count,
and distinguishes LIMIT-respecting queries from full-scan queries.

### LIMIT-respecting queries (stop early — scale well)

| Query | XS (78e) | S (88k e) | M (926k e) | L (34M e) | XL (16.5M e) |
|-------|----------|-----------|------------|-----------|--------------|
| one_hop LIMIT 1000 | 0.002 s | 0.003 s | 0.043 s | 0.74 s | 0.50 s |
| two_hop LIMIT 1000 | 0.001 s | 0.003 s | 0.044 s | 0.76 s | 0.66 s |

These are dominated by the cost of reaching 1000 results — not total graph size.
The L-tier one_hop is slower than XL despite having more edges because livejournal
has much higher average degree (8.7 vs 4.4), so each node expansion produces more candidates.

### Full-scan queries (must visit every node or edge)

| Query | XS (34n/78e) | S (4k n/88k e) | M (335k n/926k e) | L (4M n/34M e) | XL (3.8M n/16.5M e) |
|-------|--------------|----------------|-------------------|----------------|---------------------|
| count_nodes (warm) | <1 ms | 2 ms | **622 ms** | **2 400 ms** | **2 539 ms** |
| count_edges (warm) | <1 ms | 83 ms | **2 509 ms** | **218 000 ms** | **55 818 ms** |
| aggregation (warm) | <1 ms | 15 ms | **1 820 ms** | **39 000 ms** | **20 833 ms** |
| topn ORDER BY (cold) | <1 ms | 21 ms | **2 800 ms** | **155 000 ms** | **51 962 ms** |

`count_edges` is catastrophically slow at L-tier (218 s warm) because it iterates every
adjacency list entry — 34M entries with no index, no caching, no early exit.

---

## Two-Hop with LIMIT: LIMIT Short-Circuit Effectiveness

`MATCH (a:Node)-[]->(b)-[]->(c) RETURN id(c) AS cid LIMIT 1000`

| Tier | Without LIMIT | With LIMIT 1000 | Speedup |
|------|--------------|-----------------|---------|
| XS (78 edges) | 0.001 s | 0.001 s | ~1× |
| S (88k edges) | ~3.8 s | 0.003 s | **1 270×** |
| M (926k edges) | ~9 s | 0.044 s | **205×** |
| L (34M edges) | hours | 0.76 s | >>1 000× |
| XL (16.5M edges) | hours | 0.66 s | >>1 000× |

The short-circuit makes two-hop queries with LIMIT practical at every tested tier.

---

## SQLite Round-Trip: Before vs 

| Tier | Before | After | Notes |
|------|----------|-----------|-------|
| XS (78 edges) | write: 0.4 ms / read: 0.6 ms | write: 0.4 ms / read: 0.9 ms | Unchanged |
| S (88k edges) | write: — / read: **hangs** | write: 1.02 s / read: **0.17 s** | Fixed |
| M (926k edges) | — / — | not tested (roundtrip disabled) | Phase 8 scope |

---

## Assessment: "< 10M nodes" Claim

The README states GraphForge is designed for graphs with fewer than 10M nodes.
Based on these measurements:

| Operation | XS/S | M (~335k n) | L (~4M n) | Assessment |
|-----------|------|-------------|-----------|------------|
| Ingest | fast | 4 s | **139 s** | Slow at L-tier; batch ingest needed |
| Memory (RSS) | tiny | 794 MB | **12+ GB** | Requires 16+ GB machine for L-tier |
| LIMIT queries | <1 ms | <50 ms | <800 ms | ✅ Practical at all tested tiers |
| Full-scan (count_edges) | <1 ms | 2.5 s | **218 s** | ❌ Unacceptably slow above ~1M edges |
| ORDER BY + LIMIT | <1 ms | 2.8 s | **155 s** | ❌ Unacceptably slow above ~1M nodes |

**Conclusion:** The "< 10M nodes" claim is accurate for LIMIT-based traversal queries
(the primary interactive use case), but full-scan queries (`count_edges`, aggregation
without filters, ORDER BY over all nodes) break down above ~1M nodes. The README
should clarify which operations remain practical at scale. The core bottlenecks are:

1. **`count_edges` / full adjacency scan** — no edge count index; must iterate all
   adjacency lists. Needs an O(1) edge counter (trivially stored in a header field).
2. **`topn` with ORDER BY (no LIMIT push-down through sort)** — must materialise all
   nodes before sorting. Needs top-N heap sort.
3. **Ingest throughput** — single `create_node`/`create_relationship` per row; no
   batch path..
4. **Memory** — ~360 bytes/edge in Python dicts (livejournal: 34M edges → 12 GB).
   Needs a more compact adjacency representation.

---

## Notable Regressions / Anomalies

### count_nodes warm on M-tier: 0.71 s → 0.62 s (improved but still slow)

The warm-run remains at 622 ms despite the cold-run improving to 191 ms. The cold-run
improvement comes from building the property equality index on first access.
The warm-run cost is dominated by the full `NodeScan` over 334k entries.

### Warm-run slower than cold at L-tier (label_filter, aggregation)

At L-tier, several warm runs are *slower* than cold (e.g. label_filter 3.2 s cold →
16.9 s warm; aggregation 17.5 s cold → 39 s warm). This is likely caused by Python GC
pressure: the cold run produces objects that need to be collected before the warm run
can proceed. At 12 GB RSS the GC overhead is substantial. This would be mitigated by
a streaming/generator pipeline that avoids materialising all rows in memory.

### topn warm on M-tier: 2.87 s → 3.22 s (slight regression)

ORDER BY still materialises all nodes before sorting; LIMIT short-circuit does not
help when an ORDER BY is present. Addressed in Phase 9 (top-N heap sort).

---

## Outstanding Work

| Area | Current Pain | Phase |
|------|-------------|-------|
| `count_edges` full scan | 218 s at L-tier (34M edges) | Add O(1) edge counter |
| `topn` ORDER BY | 155 s at L-tier (4M nodes) | Top-N heap sort |
| Ingest throughput | 139 s for 4M nodes | batch ingest |
| Memory per edge | ~360 bytes (12 GB for 34M edges) | Compact adjacency repr |
| UNWIND + WITH LIMIT | ~4.5 s for `LIMIT 3000` from 1M range | Phase 8 |
| SQLite M-tier roundtrip | Not yet tested (likely slow) | Phase 8 scope |

---

## How to Reproduce

```bash
# XS/S/M tiers (no large downloads, ~30 s)
make test-perf

# L/XL tiers (requires 16+ GB RAM, large downloads, hours)
make test-perf-large

# Two-hop full-expansion baselines (minutes on S/M)
make test-perf-slow

# Compare a before/after snapshot
cp benchmarks/real_dataset_baseline_v0.3.9.json benchmarks/before.json
# ... make changes ...
make test-perf
python3 scripts/perf_report.py benchmarks/before.json benchmarks/real_dataset_baseline_v0.3.9.json
```
