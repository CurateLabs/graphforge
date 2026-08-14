# Concurrency and recovery contract

GraphForge supports concurrent, read-only operations on one facade, across
facades in one process, and across sessions opened on the same project. Results
are complete, canonically ordered Arrow data. A facade pins the generation it
opened: a long-lived reader does not follow later commits; a newly opened facade
does.

Dropping a stream or cancelling one request affects only that operation. A peer
continues to a complete result, while cooperative cancellation reports
`GF_CANCELLED`.

Project mutation has three explicit embedded modes. `single_writer` is the
default and rejects a competing writer with `GF_WRITER_BUSY` before it creates
staging or publication state. `queued_writer` adds a bounded, first-in-first-out
queue per facade; snapshot reads do not enter that queue, and cancellation can
remove only work that has not started. `optimistic_multi_writer` lets distinct
composite transaction identities stage concurrently, serializes only the
`CURRENT` commit point, and rebases compatible work against the winning
generation. Cross-process publication always exposes one complete generation,
never a partially staged one.

Optimistic merge rules are deliberately finite. Distinct creates and immutable
ledger identities merge. Changes to different properties of the same graph
object merge. Reusing an identity with changed content, changing the same
property, losing a mutation target, and delete or administrative work do not
merge. Stable outcomes are `GF_IDEMPOTENCY_CONFLICT`, `GF_WRITE_CONFLICT`, or
`GF_REBASE_EXHAUSTED`. Only the composite publication API is replayed
optimistically in v0.5.0; other mutation APIs keep their established
single-writer behavior even when the facade selects optimistic mode.

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

Recovery after a writer is killed selects either the previous complete
generation or the newly published complete generation according to the durable
publication phase. Graph, provenance, knowledge-layer, and epistemic state move together; mixed
generations are unsupported and treated as corruption.

## Correctness gates versus stress observations

There are two CI surfaces:

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

The finite Rust recovery ledger remains
`tests/contracts/concurrency-recovery-matrix.json` and is validated by
Repository Policy without compiling Rust.

Local short-matrix execution after native artifacts are built:

```text
python3 scripts/ci/concurrency-short-gate.py run --output /tmp/gf-concurrency-short
```

Local stress reproduction from an artifact uses the recorded command lines in
`reproduction.txt`, or:

```text
python3 scripts/ci/concurrency-stress-gate.py run \
  --seed 2417 \
  --iterations 24 \
  --output /tmp/gf-concurrency-stress
```
