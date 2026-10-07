# Scale and workload limits

GraphForge does not have a published universal maximum graph size or query-time
guarantee. Measure the operations your project needs: construction, reopening,
queries, analysis, and export can have different memory and disk requirements.
Edge count, density, properties, result size, query shape, and hardware all matter.

These docs target v0.6.0. Its [scale evidence](https://github.com/CurateLabs/graphforge/issues/735)
and [benchmark scorecards](https://github.com/CurateLabs/graphforge/issues/952)
are still release-readiness work. A target, a successful small test, or a
historical measurement is not a certified capacity for this release.

## Claims and their evidence

This table separates reproducible correctness checks from performance claims.
Results belong to the linked issue or CI run, with the source commit and run
configuration. Missing candidate evidence is shown explicitly; it is not a pass.

| Claim or question                                                       | Version / source and workload                                                                                                                                                                                                                     | Hardware and configuration                                                                                                                           | Command and result                                                                                                                      | Limitation and reproducibility                                                                                                                                                               |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Fixed-hop queries ending in `LIMIT` have a bounded-work regression test | Candidate source: [`fixed_hop_limit.rs`](https://github.com/CurateLabs/graphforge/blob/main/crates/graphforge-api/tests/fixed_hop_limit.rs); deterministic graphs, one- and two-hop queries, indexed and fallback paths                           | Supported durable filesystem; test-defined graph sizes, cache state, and resource policy. This is a structural check, not a machine-speed comparison | `make test-rust ARGS="-p graphforge-api --test fixed_hop_limit"`; the matching candidate's CI result is required before a release claim | Checks results and I/O work. It does not establish latency for your graph. See the [execution policy](../development/execution-resource-policy.md) and test source                           |
| Saved projects reopen with their committed graph                        | Candidate source: [`file_backed_graph_generation.rs`](https://github.com/CurateLabs/graphforge/blob/main/crates/graphforge-api/tests/file_backed_graph_generation.rs); small public-API persistence fixtures                                      | Supported durable filesystem; fixture settings are in the test. Large ignored fixtures require a separately recorded host and configuration          | `make test-rust ARGS="-p graphforge-api --test file_backed_graph_generation"`; inspect the candidate CI run for its result              | Small-fixture correctness does not establish a maximum project size. [Storage and recovery contract](../book/architecture/concurrency-recovery.md)                                           |
| Billion-edge lifecycle certification                                    | Exact measured commit, Graph500 input identity, and source/imported counts must be in [#735](https://github.com/CurateLabs/graphforge/issues/735) through [#745](https://github.com/CurateLabs/graphforge/issues/745)                             | Designated OVHC-AGENCY host and `local-linux-cgroups-v2` profile; each accepted run records hardware, memory, disk, and phase budgets                | `make -C benchmarks progressive-host-ladder-run` with the issue's approved arguments; final lifecycle acceptance remains outstanding    | The target is not a capacity claim. Stop at the first failed rung. Commands and host admission: [benchmark runbook](https://github.com/CurateLabs/graphforge/blob/main/benchmarks/README.md) |
| Load/query performance for the four GDC suites                          | Exact source, pinned dataset, scale factor, supported query set, and reference results belong to each [#952 scorecard](https://github.com/CurateLabs/graphforge/issues/952)                                                                       | OVHC-AGENCY, declared BenchExec profile and driver clock; per-run settings must accompany the card                                                   | Reproduce with the scorecard's exact command. Completed release scorecards are still required; no timings are asserted here             | Read-only, unaudited results must identify refused queries and the first failing rung. They are not LDBC Benchmark Results or cross-machine guarantees                                       |
| Complete-ingest throughput and multicore scaling                        | The measured source/workload and accepted release-scope decision are recorded in [#1572](https://github.com/CurateLabs/graphforge/issues/1572); further performance work remains in [#1387](https://github.com/CurateLabs/graphforge/issues/1387) | Use the hardware, worker count, and full phase definition attached to that measurement                                                               | The complete-ingest rate target was not met and was deferred beyond v0.6.0; multicore scaling is not established by that decision       | A deferral is not a performance pass. Use the original run commands and receipts linked from the issues rather than extrapolating an isolated phase                                          |

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
graph files ([#900](https://github.com/CurateLabs/graphforge/issues/900)),
about a fifth of the limit. The count roughly doubles with each Graph500
scale step, which projects to about 86,000 files (86%) at S28 and over the
limit at S29. Those two figures are projections, not measurements. Raising the
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
