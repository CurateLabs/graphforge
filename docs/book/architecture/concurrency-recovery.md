# Concurrency and recovery contract

Normative acknowledgement, filesystem scope, isolation tables, and anomaly
classification live in
[ADR 0018](../../adr/0018-acknowledged-durability-isolation.md). The machine-
readable coverage matrix is
[`tests/contracts/durability-isolation-matrix.json`](../../../tests/contracts/durability-isolation-matrix.json)
(`graphforge-durability-isolation/1`). This page is the architecture narrative
that reconciles ADR 0013 publication, ADR 0014 checkpoints, and ADR 0015 write
modes with that frozen public contract.

GraphForge supports concurrent, read-only operations on one facade, across
facades in one process, and across sessions opened on the same project. Results
are complete, canonically ordered Arrow data. A facade pins the generation it
opened: a long-lived reader does not follow later commits; a newly opened facade
does.

Dropping a stream or cancelling one request affects only that operation. A peer
continues to a complete result, while cooperative cancellation reports
`GF_CANCELLED`.

## Acknowledged-durable writes

Caller-visible success means the write is **acknowledged-durable** only after
participant and manifest file flushes, generation-tree platform-native namespace
durability barriers, atomic `CURRENT` replacement or creation, **and** the
project-root platform-native namespace durability barrier. Atomic `CURRENT`
replacement is the visibility linearization point for new readers;
acknowledgement against power loss additionally requires that final namespace
barrier. Journals never select authority.

POSIX supplies the namespace barrier with directory `fsync(2)`. Windows support
is limited to fixed writable local NTFS whose storage honestly honors
write-through completion: GraphForge flushes a `FILE_FLAG_WRITE_THROUGH`
staging handle and renames through that same handle with
`SetFileInformationByHandle`. Directory `FlushFileBuffers` is not claimed as a
durability barrier, and ReFS remains unsupported/unproven. See
[ADR 0020](../../adr/0020-ntfs-write-through-namespace-durability.md).

A persistent project filesystem may be mounted beneath a different container or
VM root filesystem. GraphForge admits the project parent as its storage
boundary and requires the project root and durable contents to remain on that
filesystem. Network, userspace, removable, symlink-mediated, ReFS, and unknown
project filesystems return `GF_UNSUPPORTED_FILESYSTEM` before the project root
or `CURRENT` changes. There is no best-effort durability mode.

Every durable lifecycle enters one Rust-owned parent-scoped admission before
project mutation. A deterministic sibling lock file persists across unlock and
process death; it coordinates absent-root initialization before `FORMAT`,
`generations/`, project locks, or `CURRENT` exist. Existing roots retain opened
parent/root identities. Ordinary publication, recovery, checkpoints and revert,
delta publication and compaction spill, retention cleanup, portable import, and
repository state initialization/removal all use that authority. Optimistic
staging retains identity without serializing peer stagers, then readmits the
same root before acquiring the commit writer lock. Python, Node, and CLI calls
remain thin adapters over these Rust outcomes, including
`GF_UNSUPPORTED_FILESYSTEM`.

Recovery resolves an exact valid `CURRENT` only. Journals and directory scans
are advisory cleanup input. Corrupt or ambiguous pointers fail closed as
`GF_PROJECT_CORRUPT` without electing a newest generation.

## Write modes and isolation

Project mutation has three explicit embedded modes. Every mode gives readers
pinned immutable snapshot isolation. The modes do not claim generic ACID,
serializable isolation, or SSI.

| Mode | Writer semantics | Isolation / conflict table |
| --- | --- | --- |
| `single_writer` (default) | Competing writers receive `GF_WRITER_BUSY` before staging or publication | One serial writer; no concurrent publish races |
| `queued_writer` | Bounded FIFO per facade; snapshot reads bypass the queue; cancellation removes only unstarted work | One serial writer after dequeue; queue-limit and cancel errors are structured |
| `optimistic_multi_writer` | Distinct composite transaction identities may stage concurrently; only the `CURRENT` commit point is serialized; compatible work may rebase | Closed merge/conflict matrix in [ADR 0015](../../adr/0015-embedded-write-modes.md): merge, `GF_WRITE_CONFLICT`, `GF_IDEMPOTENCY_CONFLICT`, or `GF_REBASE_EXHAUSTED` |

Optimistic merge rules are deliberately finite. Distinct creates and immutable
ledger identities merge. Changes to different properties of the same graph
object merge. Reusing an identity with changed content, changing the same
property, losing a mutation target, and delete or administrative work do not
merge. Only the composite publication API and the uniform `GraphTransaction`
lifecycle are replayed optimistically in v0.5.x; other one-shot mutation APIs
keep their established single-writer behavior even when the facade selects
optimistic mode, unless staged through that lifecycle.

Scalar construction (`add_edge`) acquires the same graph visibility /
write-admission coordinator as Cypher writes, bulk publication, and other
mutations before selecting a mutation snapshot or touching the workspace.
Endpoint validation, surrogate allocation, flush, publication, and rollback
stay inside that one coherent transaction so concurrent same-instance callers
cannot interleave partial edges or overlapping surrogates.

These modes coordinate embedded callers directly against the project directory.
GraphForge core does not include an MCP or HTTP server. A separately packaged
extension may expose one authenticated remote authority without changing the
storage engine into a distributed database.

### Write-skew witness

Optimistic mode is **not** SSI. Because different properties of one object may
merge, the following history is admitted:

1. Object `Account` starts with `credit=0`, `debit=0`.
2. T1 reads both fields, sees `debit=0`, and stages `credit=1`.
3. T2 concurrently reads both fields, sees `credit=0`, and stages `debit=1`.
4. Both publish after rebase under `optimistic_multi_writer`.
5. The committed generation can contain `credit=1` and `debit=1`, breaking an
   application invariant `credit + debit <= 1` that neither writer observed the
   other violate.

Docs therefore classify optimistic mode as optimistic snapshot / conflict
semantics. Preventing write-skew requires a separately approved SSI design
outside Milestone 6.

## Recovery and lifecycle

Recovery after a writer is killed selects either the previous complete
generation or the newly published complete generation according to the durable
publication phase. Graph, provenance, knowledge-layer, and epistemic state move together; mixed
generations are unsupported and treated as corruption.

Exact retry after acknowledgement returns the prior receipt without restaging.
Same-identity content changes return `GF_IDEMPOTENCY_CONFLICT`. Failures before
`CURRENT` replacement report `committed: false`. Failures after replacement
reread validated `CURRENT` and report `committed: true`; the generation is not
rolled back.

Import/export and other interchange surfaces that publish a generation reuse
the same publication vocabulary: stage, validate, durable generation,
linearize, acknowledge, publish, abort, and recover. See ADR 0018 and the
[repository integration guide](../../guides/repository-integration.md).

## Correctness gates versus stress observations

There are four CI surfaces for concurrency and durability contracts:

1. **Required short concurrency matrix** (`Test Suite / Concurrency Matrix`) —
   deterministic Rust, Python, and Node cases from
   `tests/contracts/concurrency-short-matrix.json`. Barriers/channels/failpoints
   coordinate interleavings. Timing sleeps, ignored tests, and probabilistic
   retries are forbidden. This job is required whenever Rust or binding surfaces
   change and must stay green for merge.
2. **Scheduled/manual stress lane** (`Concurrency Stress Gate`) — longer mixed
   workload with the published seed `2417`, resource bounds (RSS and file
   descriptors), and cleanup checks for locks/staging. Stress retries are
   diagnostic only; they cannot turn a failed required short matrix green.
   Throughput or latency figures from stress are non-blocking performance
   observations, not correctness evidence.
3. **Durability/isolation contract ledger** — `graphforge-durability-isolation/1`
   maps crash phases and anomalies to covered evidence. Repository Policy
   validates the ledger without compiling Rust.
   Persistent-media faults that process kill cannot express (torn `CURRENT` /
   manifest bytes, lost platform-native namespace durability barrier power-loss
   subsets) are modeled by the deterministic filesystem fault oracle in
   `crates/graphforge-storage/src/project_fault_oracle.rs`. Native POSIX and
   Windows subprocess failpoint matrices remain required for real API and handle
   behavior; the oracle is reusable by recovery, delta, compaction, and final
   certification. Authoritative graph delta runs (#752 / ADR 0019) publish only
   through the same `CURRENT` contract; they never recover by scanning newest
   logs. After CURRENT selects a generation, normal and checkpoint opens verify
   its inventory and complete GFDR chain, replay canonical Parquet plus typed
   records inside a private workspace, and publish no partial read view on
   corruption, unsupported versions, or resource-limit failure. Recovery-on-open
   (`open_or_initialize_project_with_recovery` / facade open) runs bounded
   inspection and idempotent cleanup when locks are free, and defers cleanup
   without blocking a valid `CURRENT` snapshot when a live writer holds them.
   Bounded snapshot retention and orphan GC (`inspect_project_reachability`,
   `preview_project_cleanup`, `execute_project_cleanup`) reuse the same verified
   reachability oracle as recovery: CURRENT, configured ancestors, and checkpoint
   roots. Live leases skip without waiting; concurrent publication returns
   `GF_WRITER_BUSY`; unknown or linked paths are quarantined and never deleted.
   Graph delta compaction (`compact_graph_delta` / `preview_graph_delta_compaction`)
   publishes a new Parquet generation through the same CURRENT path and reclaims
   subsumed inputs only after acknowledgement via that shared oracle.
4. **Seeded durability/isolation certification** (`Durability Certification Gate`)
   — independent reference model in
   `crates/graphforge-storage/src/project_certification.rs`, compared at each
   certified boundary with production `graphforge_api::GraphForge` opens,
   transactions, queries, checkpoints, reachability, cleanup, delta replay, and
   compaction. Rust, Python, Node, and CLI probes normalize the same generation,
   query-state, checkpoint-root, and recovery observations; bindings never own
   a second model or storage implementation.
   (`graphforge-durability-certification/1`, published seed `7560`). Required CI
   runs the bounded history budget; the scheduled/manual lane raises
   `GRAPHFORGE_CERT_HISTORIES` / `GRAPHFORGE_CERT_OPS`, records declared counts,
   minimized traces, commands, exact commit, runner/filesystem class,
   platform/tool versions, and artifact digests, and
   fails closed on the first untriaged invariant (no seed retries). Evidence and
   docs make no SSI, universal-filesystem, or distributed-durability claim;
   optimistic write-skew remains `allowed_documented_not_ssi`.

The deterministic fault oracle and native POSIX/Windows subprocess kill lanes
remain the process-death and persistent-media authority. Required CI aggregates
the Windows and macOS reports under the exact tested SHA and rejects a missing,
empty, wrong-platform, or mixed-seed report; the production history runner does
not model a successful API call as proof of a process crash. M6 CPU-simulation, durable walltime, and peak
RSS fallback evidence use the frozen `m6-storage-v1` / `m6_storage_io` fixture
contract documented in [Benchmarking](../../development/benchmarking.md).

The finite Rust recovery ledger remains
`tests/contracts/concurrency-recovery-matrix.json` and is validated by
Repository Policy without compiling Rust.

Local short-matrix execution after native artifacts are built:

```text
python3 scripts/ci/concurrency-short-gate.py run --output /tmp/gf-concurrency-short
```

Local durability/isolation contract validation:

```text
python3 scripts/ci/durability-isolation-gate.py validate
```

Local seeded certification (required budget):

```text
python3 scripts/ci/durability-certification-gate.py validate
python3 scripts/ci/durability-certification-gate.py run \
  --output /tmp/gf-durability-cert
```

Scheduled-lane reproduction:

```text
GRAPHFORGE_CERT_HISTORIES=64 GRAPHFORGE_CERT_OPS=32 \
  python3 scripts/ci/durability-certification-gate.py run \
  --histories 64 --ops 32 \
  --output /tmp/gf-durability-cert-scheduled
```

Local stress reproduction from an artifact uses the recorded command lines in
`reproduction.txt`, or:

```text
python3 scripts/ci/concurrency-stress-gate.py run \
  --seed 2417 \
  --iterations 24 \
  --output /tmp/gf-concurrency-stress
```
