# Scale and workload limits

GraphForge does not have a published universal maximum graph size or query-time
guarantee. Measure the operations your project needs: construction, reopening,
queries, analysis, and export can have different memory and disk requirements.
Edge count, density, properties, result size, query shape, and hardware all matter.

Every number below describes one measured workload on one host at one source
commit. None is a capacity, latency, or throughput promise for your graph, and
none is a comparison with another product. GraphForge is pre-v1; see
[status and limits](../guide/installation.md#status-and-limits).

## Claims and their evidence

Raw output lives on the linked issue or pull request, not in this tree. This
page records the method, the identities, and the limits. Candidate claims are
re-verified against the selected release candidate
([#1096](https://github.com/CurateLabs/graphforge/issues/1096)) before
publication.

**Host for every measured row below:** OVHC-AGENCY (Ryzen 7 3800X, 16 threads,
125 GiB, ext4 on NVMe RAID, Linux 7.0 kernel series), `local-linux-cgroups-v2`
profile. The exact kernel patch level differs between campaigns; each linked
source records its own.

| Claim | Version / source and workload | Configuration | Command and result | Limitations and reproducibility |
| ----- | ----------------------------- | ------------- | ------------------ | ------------------------------- |
| **Billion-edge lifecycle.** A project of 67,108,864 nodes and **1,073,741,824 live edges** completes all ten lifecycle phases correctly | Merged commit [`114b1964`](https://github.com/CurateLabs/graphforge/commit/114b1964874419b31d13820bfbdf72fe57286544). Graph500-compliant generated input (SCALE 26, edge factor 16, one seed); every generated tuple, including duplicates and self-loops, is one stored edge; 0 rows rejected | One generation per rung; native admission before each rung; 15% free-disk reserve; BenchExec 3.35; gf SHA-256 `427393c3…` | `make -C benchmarks progressive-host-ladder-run MAXIMUM_SCALE=26 RESERVED_HEADROOM_BYTES=112360324301`. S26: generate 475 s, ingest 2,315 s, reopen 18 s, recount 19 s, query 12 s, export 227 s, verify 74 s, clean import 487 s, reopen proof 50 s; **3,769 s total**. Source and imported content digests match; reopen agrees. Seven rungs passed in order: S18, S19, S20, S22, S24, S25, S26 | One host, one generator seed, one run per rung. No Graph500 performance (TEPS) claim. Duplicates and self-loops are stored as given, so these are not unique undirected edges. Results: [#900](https://github.com/CurateLabs/graphforge/issues/900#issuecomment-6280622796), [#745](https://github.com/CurateLabs/graphforge/issues/745#issuecomment-6280647961). Sanitized bundle (42 JSON files): [`benchmarks/fixtures/parity/ladder-bundle`](https://github.com/CurateLabs/graphforge/tree/main/benchmarks/fixtures/parity/ladder-bundle), landed in [#2186](https://github.com/CurateLabs/graphforge/pull/2186). Method: [certification](../development/g500-certification.md), [host runbook](https://github.com/CurateLabs/graphforge/blob/main/benchmarks/README.md) |
| **Memory.** Peak process memory during ingest grows with edge count, from 630 MiB (S18, 4,194,304 edges) to **31,040 MiB** (S26). The largest peak among the non-ingest phases is 154 MiB (S19) to 393 MiB (S26) at each rung | Same ladder and commit | The bulk builder budgets 3/5 of claimable memory (cgroup-aware); the budget, not a leak, sets the ingest peak | Peak RSS per rung: S18 630, S19 902, S20 1,394, S22 2,394, S24 8,062, S25 15,727, S26 31,040 MiB (ingest); results as above | Ingest memory is planned, not constant. S26 exceeds the 4 GiB envelope used for S18–S22 (accepted maintainer decision, 2026-10-05 and 2026-10-19). A smaller host needs a smaller graph. Same sources as the row above |
| **Storage.** S26 retains **216.3 GB** (source project 43.5 GB) with a 276.2 GB transient peak. | Same ladder and commit | Admission projected 276.4 GB transient under a 112.4 GB reserve | Retained / transient by rung: S18 0.7 / 0.9 GB, S22 12.1 / 15.4 GB, S24 49.5 / 62.8 GB, S25 105.1 / 134.0 GB, S26 216.3 / 276.2 GB | Disk need is dominated by the transient peak. The certified run's graph-file count is not recorded in its result comments; the only recorded count is an earlier, differently built run ([below](#fixed-project-size-limit-graph-files-per-generation)). Same sources as the first row |
| **Complete-ingest throughput.** Measured: **184,749 edges/s** for 67,108,864 edges in 363.244 s (S22). The 1,000,000 edges/s target is **not met** and no such claim is made | Source and S22 reference recorded in [#1572](https://github.com/CurateLabs/graphforge/issues/1572#issuecomment-5938404634) (adjudicated 2026-10-01). Two quiet-host reruns on the same binary: 184,098 and 184,564 edges/s | Same host and profile; the measurement predates the ladder commit above | Full phase definition, region table, and reopen proof: [#1623](https://github.com/CurateLabs/graphforge/issues/1623#issuecomment-5904730594) | The target is deferred past v0.6.0 and stays open in [#1387](https://github.com/CurateLabs/graphforge/issues/1387). The eight-core S18 curve measured 1.39× one-core throughput (1.19 effective cores): multicore scaling is not established. A deferral is not a performance pass. Do not extrapolate to other sizes or hosts |
| **Ingest phase at the ladder commit.** S26 ingest took 2,315 s for 1,073,741,824 edges (about 463,800 edges/s, derived as edges ÷ phase wall). S22 ingest took 143 s (about 469,300 edges/s, derived) | The ladder above | Ladder ingest phase only; not the complete-ingest definition of #1387 | Derived from the per-rung ingest walls in [#900](https://github.com/CurateLabs/graphforge/issues/900#issuecomment-6280622796) | A derived ratio, not a recorded #1387 measurement, and still below 1,000,000 edges/s. About 80% of ingest wall time is one single-threaded stage |
| **GDC Graphalytics.** Passed all four pinned rungs, **63/63 measured results checked and matched** (wiki-Talk 15/15, cit-Patents 15/15, datagen-7_5-fb 18/18, graph500-22 15/15) | Source [`15b354ce`](https://github.com/CurateLabs/graphforge/commit/15b354ceea79050eb3e90eb0fcdbd209eb1d8f46); one frozen campaign; gf SHA-256 `427393c3…`, driver SHA-256 `a9cda0fc…` | BenchExec phases with a 4 GiB native cap and 900 s Graphalytics timeout; 80 GiB disk reserve; sixty-second quiet windows | Results and the byte-verified packet: [#952](https://github.com/CurateLabs/graphforge/issues/952#issuecomment-6264547283); [raw packet](https://gist.github.com/DecisionNerd/98a42615b54419b90525d92c34f30b4e) (876,762-byte archive, SHA-256 `e2a499a0…`, 169 manifest digests verified) | **Unaudited engineering evidence, not an LDBC Benchmark Result, no certification.** Algorithms and rungs are the pinned set only. No sub-second or other latency claim. Per-query timings are in the cards in the packet |
| **GDC SNB Interactive SF1.** **Failed**: 540 of 600 mapped bindings measured, 540 answer comparisons, 540 matches; 60 calls (30 IC1, 30 IC3) were refused with `GF_RESOURCE_LIMIT` | Same campaign and source as above | Derived 2 GiB pool; the named allocation requested another 13.4 GB | Query-phase wall 400.952 s. [#952](https://github.com/CurateLabs/graphforge/issues/952#issuecomment-6264547283) | The 60 refused calls have no answers. IC14 is a declared unsupported query. Open defect work: [#1982](https://github.com/CurateLabs/graphforge/issues/1982). Unaudited. No passing claim |
| **GDC FinBench Transaction SF1.** **Out of memory** at the 4 GiB phase cap (signal 9). No final report; no answer checker ran | Same campaign and source | 10,689 calls selected; 4 GiB BenchExec cap | Reconstructed: 5,936 successful measured calls, 8 failure sentinels, 1 started without an end record (outcome unknown), 4,744 never started (sums to 10,689). Whole query-phase wall 2,000.250 s. [#952](https://github.com/CurateLabs/graphforge/issues/952#issuecomment-6264547283) | Coverage is partial. Returned results are unchecked (0/0 comparisons). The responsible operator is unproven. No passing claim |
| **GDC SNB BI SF1.** **Out of memory** at the 4 GiB phase cap (signal 9). No final report | Same campaign and source | 660 calls selected; 4 GiB BenchExec cap | Reconstructed: 390 successful measured calls, 150 failure sentinels, 1 started without an end record (outcome unknown), 119 never started (sums to 660); BI16b, BI17, BI18 entirely unrun. Whole query-phase wall 2,275.180 s. [#952](https://github.com/CurateLabs/graphforge/issues/952#issuecomment-6264547283) | Partial coverage; 0/0 answer comparisons and no pinned independent SF1 reference. No passing claim |
| **openCypher conformance baseline.** The vendored TCK corpus (3,897 upstream scenarios plus one GraphForge regression scenario, 3,898 identities) is a required regression baseline | [`tests/tck/passing_baseline.txt`](https://github.com/CurateLabs/graphforge/blob/main/tests/tck/passing_baseline.txt); openCypher 2024.3 TCK | Rust BDD runner in the PR gate (`CI Gate`) | `make test-tck`; see [TCK compliance](tck-compliance.md) | A baseline preserves what passes; it is not a per-feature support matrix and not a claim about every extension. Prose-only changes do not run the corpus. Re-check the count on the selected candidate |
| **Fixed-hop queries ending in `LIMIT` have a bounded-work regression test** | [`fixed_hop_limit.rs`](https://github.com/CurateLabs/graphforge/blob/main/crates/graphforge-api/tests/fixed_hop_limit.rs); deterministic graphs, one- and two-hop queries, indexed and fallback paths | Supported durable filesystem; test-defined graph sizes and resource policy | `make test-rust ARGS="-p graphforge-api --test fixed_hop_limit"`; the candidate's CI result is required | A structural check of results and I/O work, not machine speed. See the [execution policy](../development/execution-resource-policy.md) |
| **Saved projects reopen with their committed graph** | [`file_backed_graph_generation.rs`](https://github.com/CurateLabs/graphforge/blob/main/crates/graphforge-api/tests/file_backed_graph_generation.rs); small public-API persistence fixtures | Supported durable filesystem; fixture settings are in the test | `make test-rust ARGS="-p graphforge-api --test file_backed_graph_generation"` | Small-fixture correctness does not establish a maximum project size. [Storage and recovery contract](../book/architecture/concurrency-recovery.md) |

For a numerical claim, require **all** of: release/source commit, workload and
input identity, hardware, configuration, exact command, result, limitation,
and a reproducibility link. A timing without those fields is not used here.
The [scale evaluation method](scale-evaluation.md) explains the measurement
terms; the issue results own the host ladder rather than duplicating it here.

## Fixed project-size limit: graph files per generation

A committed graph generation can list at most **100,000 graph files**. The
limit is `GraphManifestLimits::max_entries` in
[`graph_manifest.rs`](https://github.com/CurateLabs/graphforge/blob/main/crates/graphforge-storage/src/graph_manifest.rs);
every caller uses the default, and it is not configurable. A generation over
the limit is refused when its manifest is resolved, with
`graph manifest declared entry limit exceeded` or
`graph manifest entry limit exceeded`. Construction resolves that manifest
when it encodes and publishes, so an import that would cross the limit fails
during ingest rather than producing an unreadable project.

The graph-file count grows with the graph, not just with its schema. The
Graph500 S26 project (67,108,864 nodes, 1,073,741,824 edges) committed 21,525
graph files in an earlier measurement run of 2026-10-07
([#900](https://github.com/CurateLabs/graphforge/issues/900#issuecomment-6042763476)),
about a fifth of the limit. That run was split across binaries and is not the
certified `114b1964` ladder, whose results do not record a file count. The count roughly doubles with each Graph500
scale step, which projects to about 86,000 files (86%) at S28 and over the
limit at S29. Those two figures are projections from that earlier run, not measurements. Raising the
limit means changing the default and any bound derived from it, then
re-checking manifest resolution memory and work. Construction's encoded
inventory is one such bound: 512 bytes per graph file, 51,200,000 bytes at the
default limit, enforced when the inventory is written and when it is read.

## Choose a workload you can measure

- Start with a [small graph](../guide/quickstart.md) and representative questions.
- For neighborhood questions, prefer bounded paths and a result limit. Sorting,
  aggregation, and `DISTINCT` may still need to consume their full input.
- Measure full scans and global algorithms separately from short traversals.
  A graph that loads successfully may still exceed the budget for an analysis.
- Measure construction, reopen, query, and export phases separately. Process
  memory, application I/O counters, and allocated disk bytes are different measures.
- Use [Graph Scale Index](graph-scale-index.md) to describe graph size and density,
  not to infer a performance promise.

## Storage and moving a project

Durable projects require the [supported local filesystems](../guide/installation.md#durable-storage).
Before v1.0, GraphForge opens supported current-format projects; it does not
promise backward compatibility or migration for earlier formats. Readability
of base files within the current format is not a promise to open older project
containers. See [project format compatibility](../book/architecture/project-format-compatibility.md).

Move a saved project through [portable export, verification, and import](../guide/portable-projects.md).
Do not copy live storage. Verification checks the package and its supported
format; it does not certify your workload's speed or the truth of its contents.
