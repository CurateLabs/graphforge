# v0.3.9 Performance Baseline Findings

**Date:** 2026-05-02
**Branch:** feature/417-real-dataset-perf-suite
**Machine:** Apple Silicon (macOS 25.4, Python 3.13)

This document records the findings from running the real-dataset QA suite (`tests/perf/`) ahead of v0.3.9 performance work. It establishes a pre-optimisation baseline and documents two blocking issues discovered during the run.

---

## Baseline Numbers

Measured with `make test-perf` (XS/S/M tiers, in-memory backend). All times are wall-clock seconds. Two-hop traversal is excluded from the automated suite for reasons described below.

| Metric | xs-karate (34n / 78e) | s-facebook (4k / 88k) | m-amazon (335k / 926k) |
|--------|----------------------|----------------------|----------------------|
| **Ingest** | 0.001s | 0.19s | 3.80s |
| count\_nodes (cold) | 0.076s | 0.078s | 0.68s |
| count\_nodes (warm) | 0.076s | 0.102s | 0.71s |
| count\_edges (cold) | 0.095s | 0.192s | 2.52s |
| count\_edges (warm) | 0.095s | 0.193s | 2.48s |
| label\_filter (cold) | 0.077s | 0.078s | 0.30s |
| label\_filter (warm) | 0.076s | 0.077s | 1.02s |
| one\_hop (cold) | 0.129s | 0.356s | 4.24s |
| one\_hop (warm) | 0.110s | 0.353s | 4.67s |
| aggregation (cold) | 0.147s | 0.179s | 1.51s |
| aggregation (warm) | 0.139s | 0.152s | 2.23s |
| topn (cold) | 0.127s | 0.128s | 3.31s |
| topn (warm) | 0.107s | 0.128s | 2.87s |
| SQLite write | 0.0004s | — | — |
| SQLite reload | 0.0006s | — | — |

Dashes in the SQLite columns reflect issue [](#issue-1-sqlite-bulk-reload-is-unusably-slow-392--409) described below.

---

## Findings

Two issues were discovered that have direct impact on correctness guarantees and the scope of v0.3.9 perf work.

### Issue 1: SQLite bulk reload is unusably slow

**Observation:** Loading `snap-ego-facebook` (4k nodes, 88k edges) into a `GraphForge("path.db")` instance and then calling `close()` followed by `GraphForge("path.db")` (reload) hangs indefinitely — confirmed >20s with no result.

**Root cause:** `_load_graph_from_backend()` reads every node and edge back from SQLite one row at a time and calls `graph.add_node()` / `graph.add_edge()` in a Python loop. There is no bulk fetch, no batch insert, and no streaming. At 88k edges this is already intractable. The architectural doc (`docs/book/architecture/storage-analysis.md`) projected SQLite inserts at 50–100k/sec with transactions; the reload path uses no batching and no WAL tuning, falling far short.

**Impact on v0.3.9 scope:** The SQLite round-trip correctness check in the perf suite is disabled for S/M/L/XL tiers until this is fixed. More critically, any user who loads a moderately-sized graph into a persistent `GraphForge` instance and closes/reopens it will experience this hang. This is a **correctness-affecting** regression path that should be prioritised alongside issue (delete persistence).

**Related issues:** (SQLite backend tuning) (delete persistence — the write-through fix should also batch writes).

**Suggested fix priority:** Implement `executemany` bulk insert in `_save_graph_to_backend()` and `_load_graph_from_backend()` before any other SQLite tuning. This is the single change most likely to unblock the persistent backend at real-world scale.

---

### Issue 2: LIMIT does not short-circuit traversal

**Observation:** `MATCH (a:Node)-[]->(b)-[]->(c) RETURN id(c) AS cid LIMIT 100` on `snap-ego-facebook` (88k edges) takes **~9 seconds** — identical to running the query without LIMIT. Adding `LIMIT 1000` to a one-hop query on the same dataset takes 0.35s; without the limit it would expand all 88k edges.

**Root cause:** LIMIT is applied as a post-filter in the final RETURN operator, after all rows have been fully materialised through the traversal pipeline. The executor does not propagate a "stop after N rows" signal upstream to the expand/scan operators. This is standard in naive Volcano-model implementations but means LIMIT provides no performance benefit on traversal-heavy queries.

**Impact on v0.3.9 scope:** Any query with `LIMIT n` on a large graph — including the common `MATCH (n) RETURN n LIMIT 10` exploratory pattern — pays full-graph traversal cost. This makes interactive use on the M-tier (334k nodes) feel slow even for simple exploration. The `topn` benchmark (`ORDER BY nid LIMIT 100`) takes 3.3s cold on M-tier for this reason.

**Related issue:** (memory management — LIMIT short-circuit).

**Suggested fix:** Thread a `row_limit` parameter from the LIMIT operator back to the NodeScan and Expand operators so they stop emitting rows once the limit is reached. This does not require full streaming/generator refactor — a simple counter passed through the execution context is sufficient.

---

## Implications for v0.3.9 Perf Work

The following ordering is recommended for the perf PRs based on these findings:

| Priority | Issue | Why |
|----------|-------|-----|
| 1 | fix + bulk reload ( partial) | Unblocks persistent backend correctness; `executemany` is a contained change |
| 2 | LIMIT short-circuit | High user-visible impact; contained executor change |
| 3 | Label/property indexes | Needed before `label_filter` warm-run regression is meaningful |
| 4 | Parser LALR migration | High latency win but large blast radius |
| 5 | Remaining tuning (WAL, adjacency cache) | Lower priority once bulk reload is fixed |

---

## How to Reproduce

```bash
# Regenerate baseline on your machine
make test-perf

# Compare after a perf change
cp benchmarks/real_dataset_baseline_v0.3.9.json benchmarks/before.json
# ... make changes ...
make test-perf
python3 scripts/perf_report.py benchmarks/before.json benchmarks/real_dataset_baseline_v0.3.9.json
```

The report script prints a Markdown table with a delta column for each metric.

---

## Known Exclusions

- **Two-hop traversal** (`MATCH (a)-[]->(b)-[]->(c)`) is defined in `SLOW_QUERIES` in the test file but excluded from the timed suite (LIMIT short-circuit) is fixed. On S-tier it takes ~4–9s regardless of LIMIT value.
- **SQLite round-trip** is only run on XS-tier (34 nodes) bulk reload is fixed.
- **L/XL tiers** (livejournal 4M, orkut 3M/117M) require `make test-perf-large` and large downloads. Not run for this baseline.
