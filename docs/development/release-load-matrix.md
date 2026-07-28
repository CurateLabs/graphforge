# Standardized Release Load Matrix

The release load matrix detects correctness and operational failures hidden by
small semantic fixtures. It is release-readiness evidence, not a benchmark,
service-level objective, competitive comparison, or universal pull-request
gate.

Published [scale limits](../reference/scale-limits.md) describe the product’s
small-to-medium posture and fixed-hop `LIMIT` guidance. This matrix proves
public-surface correctness across size×density fixtures inside that posture; it
does **not** certify the LIMIT wall-clock benches. Accepted run evidence lands
on [Release Load Matrix Results](../reference/load-matrix-results.md).

## Scale limits mapping

| Matrix size / density | Scale-limits claim under test | Notes |
|---|---|---|
| XS–XL (all) | Small-to-medium notebook graphs; per-size resource bounds | Finite synthetic envelope — not LiveJournal-scale LIMIT benches |
| Sparse vs dense at each size | Edge count matters more than node count | Dense raises live edges at similar node counts |
| `*-sparse-hub`, `*-sparse-path` | Neighborhood / adjacency-shaped interactive work | Correctness and cleanup; not LIMIT latency |
| `*-dense-clustered`, `*-dense-cyclic` | Edge-heavy workloads within the size band | Includes dense L (e.g. `l-dense-cyclic`) |
| Every case reopen | Project sharing via `GraphForge(path)` reopen | Fail-closed reopen equivalence |
| _(none)_ | Fixed-hop `LIMIT` shape / ms figures | Covered by scale-limits benches, not this matrix |

See [Scale Limits → Release load matrix coverage](../reference/scale-limits.md#release-load-matrix-coverage)
for the full claim↔scenario table.

## Versioned contracts

`tests/contracts/load-dataset-taxonomy.json` owns the deterministic seeds,
size thresholds, topology vocabulary, and directed density definition:

```text
density = live_edges / (live_nodes * (live_nodes - 1))
```

Self-loops do not increase the denominator. The checked-in generator emits
synthetic JSON fixtures with stable content SHA-256 values. Dataset manifests
record counts, density, property shape and distributions, components, degree
statistics and skew, loops, parallel edges, isolated nodes, hubs, a documented
diameter substitute, generation time, and persisted bytes. XS, S, and M each
contain two materially different sparse and two dense datasets; L and XL use
bounded representative fixtures so release runs remain finite.

Per-size machine-readable bounds cap peak RSS, persisted bytes, temporary
bytes, and case runtime. Runtime is only a hang boundary, not a latency target;
the accepted bundle retains actual elapsed time for diagnosis.

`tests/contracts/load-workload-matrix.json` resolves against the authoritative
non-Cypher surface inventory. It maps every release-tested public method, all
94 M18 registry entries, and every required M19 contract to XS through XL and
to Rust, Python, and Node. Adding or removing a public entry without a workload
assignment fails repository policy.

## Executor protocol

The repository-owned executor receives `--request PATH --output PATH`. For each
language it locates and hashes the built native artifact, runs the existing
exhaustive native public-surface suite once per SHA/artifact/inventory, and
retains that content-addressed preflight proof. Every one of the 144 cases then
loads its complete supplied fixture through the Rust, Python, or Node public
facade, executes the selected lifecycle, M18, or M19 load operation, closes and
reopens the project, and reports measured results. It cannot substitute a
smaller graph, committed binary, fallback implementation, network provider, or
proprietary data.

The exhaustive proof is not inferred from the representative load operation.
Rust runs the complete `gf-api` suite, including typed dispatch of all 94 M18
entries across rank, cluster, paths, analyze, and similar plus every public
checkpoint/M20/M21/M19 test reference. Python runs every shipped native-wheel
acceptance file, and Node runs the complete native `node:test` suite. The
authoritative surface manifest, adapter, language probe, command, artifact,
inventory, output, and source SHA are all hashed into each case's provenance.

Each output is a `graphforge-load-case/1` JSON object. It records:

- exact case, source SHA, fixture fingerprint, package artifact/version,
  platform, and toolchain identities;
- the complete inventory exercised, outcome, attempt number, and sanitized
  error (if any);
- elapsed time, peak RSS, output rows/bytes, file descriptors, threads/tasks,
  persisted/temporary bytes, cancellation cleanup, and reopen equivalence;
- canonical schema, row, ordering, and logical-result fingerprints.

Every executor must report `attempt: 1`. Failed cases remain failed; the runner
never retries, skips, quarantines, or converts a failure into success. Evidence
contains aggregate metadata and fingerprints, never graph content or secrets.

## Run and reproduce

First validate the finite contract locally:

```bash
make release-load-matrix-check
```

Build the Rust release example, install the native wheel into the active Python
environment, and build exactly one current-platform Node addon from the same
exact commit. Then run one command:

```bash
export GF_LOAD_SHA="$(git rev-parse HEAD)"
make release-load-matrix
```

`GF_LOAD_WORK` and `GF_LOAD_OUTPUT` optionally relocate generated fixtures,
individual reports, and the final bundle. Reproduce a failure by invoking the
recorded language executor with the retained request, whose dataset seed,
generator version, and fingerprint identify the input exactly.

The aggregator rejects a missing, extra, duplicate, retried, skipped, failed,
mixed-SHA, stale-fixture, incomplete-inventory, incomplete-resource, cleanup,
reopen, or cross-binding parity case. Elapsed time is diagnostic only. A slow
case passes when it remains correct and inside the release machine's declared
resource bounds; hangs, panics, corruption, nondeterminism, exhaustion,
incomplete cleanup, and incorrect results fail.

On the release-certification host, maturin, Cargo, and the Node addon share one
Blacksmith sticky-disk `target/` tree. After the manylinux maturin step the job
reclaims sticky-disk ownership for the host runner so Cargo can open its release
lock. The matrix runner redirects probe temps into the work directory, reclaims
them between cases, and fails closed when free disk drops below a fixed headroom
before the next case starts. That keeps dense L/XL publication workspaces from
competing with a second root-disk Cargo tree without dropping cases or
weakening assertions.

Retain the accepted same-SHA evidence bundle in the v0.5.0 publication
close-out record when certifying a release candidate, and record the summary on
[Release Load Matrix Results](../reference/load-matrix-results.md). Child and
construction issues close on acceptance-criteria outcomes and ordinary CI for
their changed surface; they do not wait on this matrix cascade (see
`AGENTS.md` § Issue close). The open Cypher TCK, concurrency suite, semantic
tests, and binding-release-candidate matrix remain separate mandatory gates for
release readiness; this matrix replaces none of them.
