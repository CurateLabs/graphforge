# Partition-run retirement at the shaping→encoding handoff (#1562)

Before this change, a shape that had retired staged input behind a progress
boundary (#1418) kept every family's sealed partition segments until the whole
shape completed. Those segments were the only authority for their rows, so they
could not be removed earlier without breaking resume. At the transient peak they
were the largest single component: 199.3 B/edge of a 454.7 B/edge peak at S18
(#1393 phase two).

A **finish stage** now hands that authority on. Each step of the finish pipeline
installs a `shape-stage-<index>.json` control naming the installed successor it
produced. Only then are the inputs that successor replaces unlinked:

| stage | successor recorded | retired behind it |
|---|---|---|
| `identities` | `staged-identities.run` | identity segments |
| `node-details` | `shaped-node-details.run` | node-detail segments |
| `edge-details` | `shaped-edge-details.run` | edge-detail segments |
| `endpoints` | `staged-endpoints.run` | endpoint segments |
| `assigned` | `shaped-identities.run` (+ new node/edge counts) | `staged-identities.run` |
| `resolved-routed` | the sealed `resolved` segments (+ per-partition rows) | `staged-endpoints.run` |
| `resolved` | `shaped-edge-endpoints.run` | resolved segments |

Stages are strictly ordered and append-only. Each is chained by body digest to
the one before it, and the first to the progress chain's head. A stage is only
installed once routing is complete. For that reason, once any boundary has
retired staged input, the last chunk now always closes a boundary. A resumed
shape therefore never re-routes rows into a family whose segments are already
gone. Boundary-less shapes still keep their staged input and are unchanged.

## Result: the transient peak, before and after

The same host, the same Graph500 inputs (edge factor 16, the ladder seed
`13907095936298285200`), and the same #1393 `peak-probe` driver and composition
instrumentation were used for both runs. "Before" is `main` at `82a30cb6`;
"after" is this change.

| rung | edges | before (bytes) | before B/edge | after (bytes) | after B/edge | reduction |
|---|---:|---:|---:|---:|---:|---:|
| S17 | 2,097,152 | 1,014,996,992 | 484.0 | 769,888,256 | 367.1 | 24.1% |
| S18 | 4,194,304 | 1,907,134,464 | 454.7 | 1,334,460,416 | 318.2 | 30.0% |
| S20 | 16,777,216 | 7,539,556,352 | 449.4 | 5,299,531,776 | 315.9 | 29.7% |
| S22 | 67,108,864 | 30,156,115,968 | 449.4 | 21,197,926,400 | 315.9 | 29.7% |

The before column reproduces the recorded rungs. The S18 peak is
byte-identical to the phase-two rung (1,907,134,464). S20 and S22 match the
`ed273d2b`/`b6ffb088` archives' 449.4 B/edge. After the change, the peak is
**flat at 315.9 B/edge from S20 to S22**, a fourfold range.

## Composition of the peak, before → after (B/edge)

| component | S17 | S18 | S20 | S22 |
|---|---:|---:|---:|---:|
| `shaped_partition_run` | 199.8 → 80.7 | 199.3 → 32.7 | 199.1 → 67.2 | 199.5 → 67.1 |
| `shaped_output` | 81.9 → 0.0 | 81.9 → 0.0 | 81.9 → 54.3 | 81.9 → 54.3 |
| `staged_domain_run` | 66.0 → 0.0 | 66.0 → 0.0 | 66.0 → 93.6 | 66.0 → 93.6 |
| `staged_chunk_run` | 25.3 → 137.3 | 4.2 → 137.3 | 0.5 → 0.0 | 0.3 → 0.0 |
| `staged_chunk_parquet` | 9.0 → 49.1 | 1.5 → 49.1 | 0.2 → 0.0 | 0.1 → 0.0 |
| `source_parquet` | 49.0 → 49.0 | 49.0 → 49.0 | 49.0 → 49.0 | 49.0 → 49.0 |
| `registered_source_copy` | 49.0 → 49.0 | 49.0 → 49.0 | 49.0 → 49.0 | 49.0 → 49.0 |
| `construction_control` | 3.9 → 2.0 | 3.7 → 1.0 | 3.6 → 2.7 | 3.5 → 2.8 |
| everything else | 0.01 → 0.01 | 0.01 → 0.01 | 0.00 → 0.00 | 0.00 → 0.00 |

Every composition sums exactly to its peak, and `unclassified` is 0 in all
eight receipts.

Three findings are load-bearing.

**1. At S20 and above, the peak is the endpoint finish, and one family's
segments are resident rather than all of them.** Partition runs fall from
199.5 to 67.1 B/edge. That is the endpoint family's own segments, 66.0 B/edge
(two 33-byte records per edge), plus about 1 B/edge. The issue's lower bound
predicted exactly this: "one partition's worth" at family granularity. The
staged domains at the same instant are `staged-endpoints.run` (66.0) and
`staged-identities.run` (27.6), 93.6 together. The endpoint segments retire as
soon as the `endpoints` stage is durable.

**2. `staged_domain_run` was evaluated under the same mechanism, as the issue
asked.** Before this change, the 66.0 B/edge at the peak was
`staged-endpoints.run`, coexisting with every family's segments during
endpoint resolution. The `resolved-routed` stage now retires it as soon as the
resolved segments are durable. Resolution itself no longer sets the peak,
because the identity, detail and endpoint segments are gone by then. What
remains at the new peak instant is the endpoint finish's own input and output
side by side. That is structural to materializing a sorted domain and is not
removed by retirement.

**3. At S17 and S18 the peak moved to routing, and that floor is not new.**
Below about 8M edges, almost no sealing boundary is crossed before routing
ends. The whole staged input (186.4 B/edge) is therefore still resident while
the segments fill. That floor existed before this change, hidden under the
higher finish peak. #1418 already bounds it: the live staged set saturates at
about 4.3 GB (`open_spills × 255 KiB`), so it falls per edge with scale. It
did not set the peak at S20 or S22 and cannot at S26. **Use S20 and S22, not
S17 or S18, for projection.**

## S26 admission margin, restated from the measured slope

The projection uses 315.9 B/edge, measured flat at S20 and S22, over
1,073,741,824 edges. It is made the same way as #1393 phase two: the envelope
is 598.0 GB (the recorded 739.3 GB bench-host sample minus the unchanged
141.3 GB reserve).

| | B/edge | projected S26 peak | margin | margin as % of peak |
|---|---:|---:|---:|---:|
| when #1393 was filed | 547.78 | 588.5 GB | 9.6 GB | 1.6% |
| #1393 phase two (before this change) | 449.4 | 482.5 GB | 115.5 GB | 23.9% |
| **after this change** | **315.9** | **339.2 GB** | **258.8 GB** | **76.3%** |

The reserve is unchanged and no disk was freed. Only the peak's composition
moved. The linearity caveat from #1393 still applies: nothing has been measured
past 67 million edges.

## Digests and application I/O stay reproducible

The five `gf import-session` commands were run with `--json` at S18 on both
builds, plus a second run of the after build:

- **Graph content is identical.** A full scan of every edge
  (`MATCH (a)-[r]->(b) RETURN a, r, b`, 4,194,304 rows) and every node
  (`MATCH (n) RETURN n`, 262,144 rows) was hashed after canonical row sorting.
  Before, after and the after repeat all give edges
  `700fb97f…6449d` and nodes `bb185a9b…8146e`. The sink's own
  `result_sha256` is not usable for this comparison: it covers file metadata,
  including a per-query `query_id`, and unordered scan order.
- **Every application-I/O byte counter is unchanged:** reads, writes and
  block, object and call counts in every phase. The only non-timing
  differences are the transient peak fields and `fsync_calls` (8,297 →
  8,295 in `validate`). Both are identical between the two after runs.
  `construction_staging.allocated_bytes` differs by one 4 KiB block between
  any two runs, the before build included. It is filesystem block
  allocation, not application I/O.

## Throughput

The #1476 ingest gate (`GF_INGEST_FLOOR_GATE=1 cargo bench -p
graphforge-storage --bench m6_storage_io`) was run on both trees, one after
the other, on the same host:

| edges | tree | edges/s | read B/edge | write B/edge | CPU µs/edge | peak MiB |
|---:|---|---:|---:|---:|---:|---:|
| 524,288 | before | 102,137 | 1,238 | 719 | 8.02 | 201.8 |
| 524,288 | after | 104,503 | 1,238 | 719 | 7.89 | 201.8 |
| 8,388,608 | before | 140,790 | 1,261 | 740 | 6.11 | 2,840.5 |
| 8,388,608 | after | 140,527 | 1,261 | 740 | 6.17 | 1,729.3 |

Bytes read and written per edge are identical. Throughput and CPU per edge
are within run-to-run noise (throughput +2.3% and -0.2%, CPU -1.6% and
+1.0%). No regression side of
any ratchet moves. Both trees report the same three **unbanked gains**
against the banked ceilings (read ceiling 2,500, CPU ceiling 14.0,
degradation ratio 1.20). That is pre-existing on `main`, not caused by this
change, which moves none of those metrics. It is not banked here.

The probe's wall time is also unchanged: S18 31 s before and after, S20
128 s before and after. S22 took 552 s before and 533 s after.

## Correctness evidence in the same change

- `finish_stage_crashes_resume_with_the_same_graph` kills the run at 15
  windows. They are: before and after each stage install and each segment
  retirement; mid-retirement (`shape.after_derived_unlink`); after an output
  is installed but before its stage is recorded; and after each staged-domain
  retirement. Each reopened run produces **byte-identical shape outputs**
  (name, length, SHA-256) to an uninterrupted run, publishes the full graph,
  and reconciles its `storage_current` ledger exactly. At each crash, the test
  also asserts that the families a stage retired are really gone from disk.
  At the resolution instant, only resolved and row segments remain.
- `finish_stage_resume_refuses_mutated_successors` (the #1269 class) covers
  four mutations: an adopted family output mutated in place, an adopted
  resolved segment mutated in place, a stage rewritten inside the chain, and a
  head stage whose recorded receipt no longer matches its writer's. Each is
  refused for its specific reason, and `CURRENT` never moves.
- `finish_stage_returned_errors_refuse_reuse_until_reopen` covers a returned
  (non-crash) error inside a staged finish. It is handled like every other
  interrupted shape: the live facade refuses reuse without touching its
  evidence, and a reopen resumes to the same graph.
- `resolved_stage_controls_at_the_partition_ceiling_round_trip`: a
  `resolved-routed` stage names one segment per partition, so at the 4096
  partition ceiling it exceeds the general 1 MiB control bound. The first S22
  run of this change found supersession refusing its own stage control. This
  test fails with that exact error on the pre-fix reader.
- The existing #1418/#1526 boundary and shape-end crash suites and the
  consumed-shape-root suites pass unmodified.

## Method

```text
# before: worktree at main 82a30cb6; after: this branch
cargo build --release -p graphforge-cli
cargo build --release -p graphforge-benchmark-graph500-generator \
    --manifest-path benchmarks/Cargo.toml
cp docs/development/evidence/transient-peak-1393/peak-probe.rs \
   crates/graphforge-storage/examples/peak-probe.rs
cargo build --release -p graphforge-storage --example peak-probe

graphforge-benchmark-graph500-generator --scale <S> --edge-factor 16 \
    --seed 13907095936298285200 --nodes nodes-s<S>.parquet --edges edges-s<S>.parquet
TMPDIR=<ext4 dir on the project volume> \
    peak-probe <gf> <project-dir> <nodes> <edges> <operation-uuid>
```

Receipts: `s<S>-{before,after}-composition.json` in this directory, in the
`transient-peak-composition/1` schema of #1393.
