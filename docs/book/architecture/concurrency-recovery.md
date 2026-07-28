# Concurrency and recovery contract

GraphForge supports concurrent, read-only operations on one facade, across
facades in one process, and across sessions opened on the same project. Results
are complete, canonically ordered Arrow data. A facade pins the generation it
opened: a long-lived reader does not follow later commits; a newly opened facade
does.

Dropping a stream or cancelling one request affects only that operation. A peer
continues to a complete result, while cooperative cancellation reports
`GF_CANCELLED`.

Project mutation is single-writer. A competing writer is rejected with
`GF_WRITER_BUSY` before it creates staging or publication state. Multi-writer
merge semantics and throughput guarantees are unsupported. Cross-process
publication exposes one complete generation, never a partially staged one.

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
