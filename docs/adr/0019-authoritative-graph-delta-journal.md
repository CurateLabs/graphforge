# ADR 0019: Authoritative durable graph delta journal

**Status:** Accepted
**Date:** 2026-08-15
**Build target:** v0.5.x (M6)
**Related:** ADR 0013 (publication protocol), ADR 0018 (acknowledgement),
ADR 0004 (derived adjacency), issues #747 / #752 / #753

## Context

Immutable complete generations already publish graph bytes through `CURRENT` and
a generation manifest (ADR 0013). Small mutations still pay full Parquet rewrite
amplification when every unchanged topology or property file is re-encoded into
the child generation.

Derived adjacency delta segments (`adjacency_delta`) remain rebuildable index
accelerators. They must not recover acknowledged graph mutations and are not
commit authority.

M6 requires an authoritative, checksummed, bounded, append-oriented mutation
journal that stays inside the immutable-generation model: one generation still
owns its bytes, `CURRENT` plus the generation manifest remain the sole commit
authority, and torn or corrupt runs fail closed.

## Decision

### Authority and layout

A committed graph generation may represent state as:

1. a verified **base** set of graph Parquet (and related) files under the
   generation-owned `graph/` tree; plus
2. zero or more immutable ordered **delta runs** under `graph/deltas/`.

Both base files and delta runs are listed in the existing `graph`/`files`
inventory participant with exact lengths and SHA-256 digests. Open and
publication validate inventory against on-disk bytes. Directory scans, newest
run filenames, or unmanifested logs never elect authority.

```text
generations/<generation-uuid>/
├── manifest.json
├── graph/
│   ├── topology/...
│   ├── properties/...
│   └── deltas/
│       ├── run_0000000000000001.gfdr
│       └── run_0000000000000002.gfdr
└── participants/graph/files.json
```

`FORMAT` remains `graphforge-project/v1`. Generations without `graph/deltas/`
entries remain the baseline readable layout. Adding delta runs does not bump
the project container format; unsupported run format versions fail closed with
`GF_UNSUPPORTED_PROJECT_FORMAT` or `GF_PROJECT_CORRUPT` as appropriate.

### Run framing

Each `.gfdr` file is one immutable run. Records are length-prefixed frames with:

| Field | Role |
| --- | --- |
| format / record version | reject unknown versions |
| run sequence | contiguous `1..=N` within the generation |
| transaction UUID | publication idempotency identity |
| operation UUID | per-mutation identity |
| op sequence | contiguous order inside the run |
| kind | create / update / delete surface enum |
| schema id | payload contract |
| payload length + bytes | bounded mutation body |
| per-record checksum | SHA-256 over the framed record |
| trailing file checksum | SHA-256 over preceding file bytes |

Acknowledged runs are durable only through the ADR 0013 / ADR 0018 publication
contract (participant and generation flushes, atomic `CURRENT` replacement, and
project-root directory flush). The journal never becomes a second commit
pointer.

### Publication and small-write rule

A small-write publication may:

- byte-copy unchanged base Parquet and prior verified delta runs from the pinned
  parent into the private child generation; and
- append exactly the new immutable run(s) required by the mutation.

It must not re-encode unchanged Parquet files solely to incorporate the
mutation. Compaction of base+deltas back into canonical Parquet is a separate
generation transaction implemented by `compact_graph_delta` /
`preview_graph_delta_compaction` (#753).

### Compaction checkpoint (#753)

Compaction is a normal project-generation transaction:

1. Pin CURRENT and select a verified contiguous run prefix
   (`1..=through_run_sequence`, or all runs).
2. Merge base + prefix under explicit memory, spill, disk, and cancellation
   budgets into a new canonical Parquet base (plus `.base_state.json`).
3. Re-encode any later suffix runs contiguously onto the child generation so
   post-snapshot deltas remain visible.
4. Verify counts/schemas/ordering/checksums and the canonical graph fingerprint
   against the pre-compaction full chain before CURRENT publication.
5. Reclaim subsumed input generations only through the shared retention
   reachability oracle (`inspect_project_reachability` /
   `preview_project_cleanup` / `execute_project_cleanup`). Delta runs stay
   protected with their generation; GC never deletes individual `.gfdr` files
   out from under a reachable generation.
6. Policy triggers (`graph_delta_compaction_status`) are caller-driven by run
   count, run bytes, or estimated replay work — there is no unbounded
   background compaction daemon.

Crash before CURRENT leaves the prior base/deltas authoritative; after
acknowledgement the compacted generation is authoritative. Named checkpoints
and live reader leases retain exact prior bytes because the parent generation
remains reachable until the shared oracle proves otherwise.

Unsupported mutation kinds are rejected **before** acknowledgement. Supported
kinds cover the create / update / delete surfaces required by current graph
mutation paths for topology rows and property set/remove operations.

### Replay, idempotency, and conflicts

Reopen reconstructs graph state only from the CURRENT-selected generation's
verified base plus its ordered, contiguous, checksum-valid runs. Replay is
idempotent on operation UUID: an exact retry returns the prior result without
duplicating the mutation. Reusing an operation UUID with a different payload is
a typed `GF_IDEMPOTENCY_CONFLICT`.

Torn, truncated, reordered, duplicated, missing, or checksum-invalid runs
referenced by the committed inventory cannot become visible: open fails closed
and never partially applies a prefix of a corrupt chain.

### Bounds

Implementations enforce finite limits on:

- runs per generation;
- bytes and records per run;
- payload size;
- open-time validation work; and
- estimated replay memory.

Exhaustion returns a structured resource-limit error without guessing.

### Separation from derived adjacency deltas

| Mechanism | Authoritative? | Purpose |
| --- | --- | --- |
| `graph/deltas/*.gfdr` | yes | recover acknowledged graph mutations |
| `indexes/adjacency/deltas/` | no | optional CSR acceleration; rebuildable |

Absence or corruption of adjacency delta segments never changes graph results.
Absence or corruption of an inventory-listed authoritative run fails open.

## Consequences

- Small acknowledged mutations avoid Parquet rewrite amplification while
  remaining one immutable generation under `CURRENT`.
- Crash cases before/after acknowledgement continue to follow ADR 0018 and the
  frozen fault oracle (#749).
- Compaction (#753) folds a verified prefix into a new Parquet base through the
  same CURRENT publication path, preserves suffix runs and leases, and reclaims
  unreachable inputs only via the shared GC oracle.
- Pre-delta v1 projects remain readable; delta-bearing generations remain v1
  containers with an explicit run-format version gate.

## Rejected alternatives

| Alternative | Reason |
|---|---|
| Side WAL outside the generation manifest | Second authority; recovery by log scan |
| Treating adjacency deltas as durable mutations | Rebuildable accelerator only |
| In-place Parquet append as commit | Breaks immutable snapshot readers |
| Recover by newest `.gfdr` mtime | Directory election protocol |

## Required verification

- Small mutation publishes without rewriting unchanged Parquet digests.
- Reopen reconstructs exact state from base + ordered runs.
- Exact retry is idempotent; conflicting identity reuse is typed.
- Torn/truncated/reordered/duplicated/missing/checksum-invalid runs fail closed.
- Crash before/after acknowledgement matches the frozen durability oracle.
- Resource limits bound replay and tiny-run accumulation.
- Legacy v1 projects without deltas remain readable.
- Compaction preserves fingerprint parity, suffix visibility, checkpoint bytes,
  crash authority, and shared-oracle cleanup.
