# ADR 0015: Three embedded project-write modes

**Status:** Accepted for v0.5.0

**Related:** [ADR 0018](0018-acknowledged-durability-isolation.md) freezes the
public isolation honesty rules, including the write-skew witness that shows
optimistic mode is not SSI/serializable. Semantic changes to mode isolation
require an amending ADR.

## Context

GraphForge is an embedded engine, but one project may be opened by multiple
threads, processes, or agent clients. The durable generation protocol already
provides immutable snapshots, transaction-addressed attempts, and an atomic
`CURRENT` boundary. Callers need explicit coordination choices without making
the storage engine a distributed database or adding a network server to core.

## Decision

`GraphForgeOptions` selects exactly one `ProjectWriteMode`:

- `SingleWriter` is the compatibility default. A competing cross-process writer
  receives `GF_WRITER_BUSY` before staging.
- `QueuedWriter` admits mutations through a bounded FIFO queue owned by one
  facade. Queue capacity is explicit. Snapshot reads bypass the admission queue,
  and cancellation removes only a request that has not started.
- `OptimisticMultiWriter` allows different composite transaction identities to
  stage in parallel. Committers wait for the short writer-lock promotion
  boundary, compare their pinned parent with durable `CURRENT`, and either
  publish or rebase within the configured finite retry budget.

The optimistic conflict matrix is closed:

| Concurrent work | Outcome |
| --- | --- |
| Distinct graph creates | Rebase and merge |
| Distinct immutable-ledger identities | Rebase and merge |
| Different properties on one object | Rebase and merge |
| Same property or removed target | `GF_WRITE_CONFLICT` |
| Delete or administrative work | `GF_WRITE_CONFLICT` |
| Retry budget consumed | `GF_REBASE_EXHAUSTED` |
| Same operation and same content | Exact idempotent replay |
| Same operation and changed content | `GF_IDEMPOTENCY_CONFLICT` |

Only `publish_composite_transaction` has optimistic replay semantics in
v0.5.0. Arbitrary write Cypher, bulk construction, index administration, and
other mutation APIs remain serialized. After any attempted publication, the
facade workspace, runtime catalog, and generation UUID reconcile to durable
`CURRENT`; a committed generation is never reported as rolled back.

The modes are local embedded capabilities, not transport protocols. MCP and
HTTP remain outside core. An optional extension can expose one authenticated
authority over these APIs without changing the on-disk protocol.

## Consequences

Applications keep a zero-configuration safe default and may opt into bounded
backpressure or compatible multi-writer throughput. Queue memory cannot grow
without limit, retries cannot continue without limit, and conflict outcomes are
machine-readable. Remote fleets require an extension or application-owned
authority rather than a server dependency in the engine.
