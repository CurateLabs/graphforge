# Traversal Scaling Benchmarks (#767, #838, #1248, #1269, #1271)

## Fixed-hop terminal LIMIT

Issue #1248 adds an end-to-end public-API benchmark for fixed one- and two-hop
queries ending in `LIMIT 1000`. Both queries run through the same
provider-backed `ExpandExec` for typed/wildcard relationships and every index
state. On an index hit, storage counters must show zero full edge reads and
bounded filtered edge/node rows as the graph grows 10x.

The non-ignored CI fixture compares 262,144 and 2,621,440 edges. The release-only
fixture defaults to 1M and 10M edges, warms the CSR and filesystem caches, then
reports the median of five runs. Timing is evidence only; the deterministic I/O
ratio is the gate.

Release-mode structural smoke on Apple Silicon:

| edges | one-hop ms | two-hop ms | one-hop edge rows | two-hop edge rows | one-hop node rows | two-hop node rows |
|---:|---:|---:|---:|---:|---:|---:|
| 262,144 | 5.42 | 25.13 | 1,000 | 74,176 | 132 | 9,384 |
| 2,621,440 | 39.92 | 66.66 | 1,000 | 82,368 | 132 | 10,415 |

Filtered rows stay within 1.11x across the 10x growth, with zero full node or
edge reads. `*_scanned_rows` can move with Parquet page boundaries and is
reported separately by the test; it is not confused with rows materialized.

Default release fixture (median of five warmed runs):

| edges | nodes | one-hop ms | two-hop ms | one-hop edge rows | two-hop edge rows |
|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 62,500 | 9.76 | 13.08 | 1,000 | 1,288 |
| 10,000,000 | 625,000 | 113.39 | 111.35 | 1,000 | 1,288 |

The 10x graph moves wall time with file and page-index size, but materialized
traversal work is flat for both hops. Timing is not a correctness threshold.

```bash
make bench-fixed-hop-limit

# Optional quicker fixture while validating the harness
GF_FIXED_HOP_BENCH_N1=6250 GF_FIXED_HOP_BENCH_N2=62500 \
  make bench-fixed-hop-limit
```

## Multi-hop demand and cancellation (#1269)

DataFusion kept the hard fetch above relationship uniqueness, correctly, but
inserted a 10-way round-robin repartition below that filter. Each partition
could eagerly buffer one complete `ExpandExec` batch, so downstream `LIMIT`
did not bound the already-scheduled multi-hop reads. The final physical demand
rule now removes only non-order-preserving round-robin exchanges in the
whitelisted fixed-hop/filter pipeline, supplies resumable soft batch goals, and
cancels the query-scoped hop chain after the terminal demand is satisfied.

| Structural evidence | Before | After |
|---|---:|---:|
| Two-hop LiveJournal edge rows scanned, `LIMIT 1000` | 29,491,200 | 196,608 |
| Two-hop LiveJournal node rows scanned, `LIMIT 1000` | 116,126,720 | 5,964,042 |
| Filtered reads started after cancellation | not attributable | 0 |
| Maximum in-flight filtered reads | eager partition buffering | 1 |
| Round-robin partitions in bounded two-hop plan | 2 | 0 |

LiveJournal (3,997,962 nodes / 34,681,189 edges), one warm-up and five release
samples per case:

| Hops | Limit | Median ms | Materialized edge rows | Scanned edge rows | Scanned node rows | Full reads |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 10 | 59.21 | 10 | 65,536 | 262,144 | 0 |
| 1 | 100 | 66.88 | 100 | 65,536 | 524,554 | 0 |
| 1 | 1,000 | 90.16 | 1,000 | 65,536 | 3,080,458 | 0 |
| 2 | 10 | 64.70 | 20 | 131,072 | 327,680 | 0 |
| 2 | 100 | 73.27 | 200 | 131,072 | 852,234 | 0 |
| 2 | 1,000 | 127.70 | 2,000 | 196,608 | 5,964,042 | 0 |

The two-hop `LIMIT 1000` case is 12.6x faster than the 1,614.5 ms pre-#1269
median and stays well below the release ceilings of 2,949,120 edge rows and
11,612,672 node rows scanned. The timed command reported 2.58 GiB maximum RSS.
The one-hop query already performs exactly one edge and node read with no
post-cancellation scheduling; exact dense node-row selection addresses its
remaining 3.08M-node decode footprint below.

```bash
GF_LIVEJOURNAL_PROJECT=/path/to/cached/project \
  make bench-fixed-hop-livejournal

# macOS maximum RSS evidence
/usr/bin/time -l env GF_LIVEJOURNAL_PROJECT=/path/to/cached/project \
  make bench-fixed-hop-livejournal
```

## Exact scattered node pruning (#1271)

Canonical node files are dense and ascending, so a filtered read can prove from
row-group and page metadata that `node_id = row ordinal + 1`. Storage then gives
Parquet exact row selections for the destination set while retaining the
membership predicate as a guard. Missing indexes, gaps, legacy layouts, or an
output-key mismatch fail closed to the prior row-group reader.

The same cached LiveJournal fixture, one warm-up and five measured samples per
case, produced:

| Hops | Limit | Before node rows scanned | After node rows scanned | After median ms |
|---:|---:|---:|---:|---:|
| 1 | 10 | 262,144 | 10 | 59.28 |
| 1 | 100 | 524,554 | 97 | 60.10 |
| 1 | 1,000 | 3,080,458 | 968 | 66.25 |
| 2 | 10 | 327,680 | 20 | 69.34 |
| 2 | 100 | 852,234 | 190 | 74.55 |
| 2 | 1,000 | 5,964,042 | 1,946 | 90.32 |

One-hop `LIMIT 1000` decodes 3,182x fewer node rows and remains far below the
1,048,576-row release ceiling. Two-hop decodes 3,065x fewer. Every sample used
the dense strategy with zero metadata or validation fallback, zero full reads,
zero post-cancellation read starts, and maximum one in-flight read. The first
execution of the first case was 798.63 ms; later per-case first executions were
60.26–101.00 ms. These are first-facade observations, not claims about the OS
filesystem cache. The complete timed command reported 2.66 GiB maximum RSS.

No sidecar is built or refreshed, so derived-index build/refresh time is not
applicable and persistent storage growth is zero bytes.

### Counter methodology

Demand capture is opt-in and query-scoped. Stable edge-binding keys and filter
ordinals record input
batches/rows, generated and emitted candidates, scheduled/completed/failed
edge and node reads, returned/scanned/full rows, cancellation, maximum in-flight
reads, and rejected post-cancellation attempts. Transparent probes count only
aggregate filter input/output rows while capture is enabled. No query text,
parameters, properties, UUIDs, graph contents, or local paths are retained.

Node reads additionally report dense versus conservative strategy, row groups
and pages considered/selected, exact rows selected, and metadata or validation
fallback counts. A validation retry is counted as a separate physical read.

The legacy process-wide storage counters remain the cross-check for full versus
filtered reads. Tests reset them immediately before one serialized measurement;
the query-scoped counters prove that started reads quiesce before collection
returns, so a fresh runtime is no longer needed to avoid late-read contamination.

### Historical #1248 comparison

LiveJournal, after the initial provider-backed fixed-hop correction:

| Query | Before #1248 | After #1248 | Python 0.4.x | Filtered edge rows | Full reads |
|---|---:|---:|---:|---:|---:|
| One hop `LIMIT 1000` | 6.61 s | 0.098 s | 0.56 s | 1,000 | 0 |
| Two hop `LIMIT 1000` | >15 min, incomplete | 1.47 s | 0.76 s | ~269k | 0 |

The one-hop Rust path was about 67x faster than the pre-fix plan and 5.7x faster
than Python 0.4.x. The two-hop path now completes instead of overflowing/full-
joining the graph, but remained about 1.9x slower than Python. Both physical plans show `ExpandExec` with
`adjacency=hit`; the two-hop plan preserves `cypher_relationship_disjoint`
above the second expansion. The timed five-sample run peaked at 10.2 GB RSS.

## Variable-length localized traversal

**Claim:** with the adjacency index present, a **localized** k-hop traversal
reads — and decodes — work proportional to its visited neighborhood,
**independent of total graph size**, so latency is flat as the graph grows.

## Topology

A deterministic ring-successor graph: node `i` has a `KNOWS` edge to each of
`(i + 1) ..= (i + 16)` (mod `n`) — fixed fan-out 16, so `edges = 16 · nodes`.
The k-hop neighborhood of any node is structurally identical at every scale.
Traversal: `*1..3`, warm `PersistentAdjacencyProvider` (CSR view cached).
Counters via `gf_storage::io_stats`: `*_filtered_rows` = rows materialized;
`*_scanned_rows` = rows the pushdown predicate evaluated (= the data pages the
`edge_id`/`node_id` row-group + page index did **not** prune — the decode-cost
footprint). Files are written in 64 K-row row groups so a clustered id range
prunes to ~one group.

## Localized traversal (the realistic "explore from a node" case)

8 clustered seeds (a contiguous block). Rust core, release, single-threaded.

| edges | nodes | edge_filtered | edge_scanned | node_filtered | node_scanned | `*1..1` ms | `*1..2` ms | `*1..3` ms |
|---|---|---|---|---|---|---|---|---|
| 1,000,000 | 62,500 | 640 | 65,536 | 55 | 55 | 1.11 | 3.00 | 11.76 |
| 10,000,000 | 625,000 | 640 | 65,536 | 55 | 55 | 1.96 | 2.88 | 12.05 |

- **Node scanned rows equal the 55-row neighborhood at both scales; edge scans
  remain flat at one 65 K row group across the 10× growth** — decode cost is
  bounded by the neighborhood, not the total. This is the
  machine-independent proof, asserted by
  `scaling_localized_traversal_is_neighborhood_proportional`.
- **Wall-clock is ~flat** (`*1..3`: 11.8 → 12.1 ms across 10×), confirming the
  decode bound translates to latency. Reported, not asserted (CI timing is noisy).
- `node_full_reads == edge_full_reads == 0`: no full table scan (#838 / #830).

## Scattered traversal (bounded nodes, columnar edge limit)

64 seeds spread across the ring, with permuted edge insertion. The reached node
set spans the whole node-id space, but dense row selection addresses exact
ordinals. Permuted edge ids still span the edge file and remain page-scan bound.

| edges | nodes | edge_filtered | edge_scanned | node_filtered | node_scanned | `*1..1` ms | `*1..3` ms |
|---|---|---|---|---|---|---|---|
| 1,000,000 | 62,500 | 33,792 | 1,000,000 | 3,072 | 3,072 | 16.75 | 179.43 |
| 10,000,000 | 625,000 | 33,792 | 10,000,000 | 3,072 | 3,072 | 165.77 | 297.19 |

- Node rows decoded are exactly the 3,072-row reached set at both scales, meeting
  the 3× structural ceiling with a 1.0× ratio. Edge rows remain scan-bound under
  columnar predicate pushdown because their permuted ids touch the whole file.
  Neither table uses a full read; `scaling_scattered_node_ids_are_pruned`
  separately asserts the bounded node path and honest edge limitation.

_Generated by `make bench-traversal` (`cargo test -p gf-exec --release --test bench_traversal_scaling -- --ignored --nocapture --test-threads=1`). Wall-clock is hardware-dependent and illustrative; the `*_scanned_rows` assertions are the gate._
