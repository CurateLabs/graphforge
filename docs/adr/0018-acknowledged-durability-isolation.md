# ADR 0018: Acknowledged durability and isolation contract

**Status:** Accepted
**Date:** 2026-08-15
**Build target:** v0.5.x (M6 foundations)
**Related:** ADR 0013 (publication protocol), ADR 0014 (checkpoints),
ADR 0015 (write modes), issues #747–#756, adjacent M5 interchange #738/#742/#745

## Context

ADR 0013 already freezes flush ordering and `CURRENT` authority. ADR 0015
already names three embedded write modes. Public callers still lacked one
versioned place that answers:

1. when a successful write is **acknowledged-durable**;
2. which platforms and filesystems are in scope;
3. which isolation and conflict outcomes each write mode provides; and
4. which transaction anomalies are prevented versus merely documented.

Without that contract, docs risk either understating immutable-snapshot
guarantees or claiming generic ACID / serializability that the engine does not
provide. M6 recovery, delta, transaction, and certification work must freeze
this vocabulary before changing behavior.

## Decision

### Normative surface

This ADR is the public acknowledged-durability and isolation contract for local
project writes. Machine-readable coverage lives in
[`tests/contracts/durability-isolation-matrix.json`](../../tests/contracts/durability-isolation-matrix.json)
(`graphforge-durability-isolation/1`). Narrative architecture lives in
[concurrency and recovery](../book/architecture/concurrency-recovery.md).

Semantic changes to acknowledgement, recovery authority, filesystem scope, or
isolation outcomes require a new ADR that amends or supersedes this one. Silent
doc or code drift is forbidden.

### Acknowledgement boundary

A caller-visible success means the write is **acknowledged-durable** only after
all of the following have completed on a supported filesystem:

1. every staged participant file has been written, closed, and file-flushed;
2. `manifest.json` has been written and file-flushed, and the generation tree
   (participants directories upward through the generation directory) has been
   directory-flushed;
3. for optimistic attempts, the private attempt directory has been atomically
   promoted into `generations/<generation-uuid>/` with the required parent
   directory flushes;
4. the exact new `CURRENT` bytes have been written to a sibling, file-flushed,
   atomically replaced or created, **and** the project-root directory entry has
   been flushed.

Step 4's project-root directory flush is part of acknowledgement. Atomic
`CURRENT` replacement alone is the visibility linearization point for new
readers, but acknowledgement of durability against power loss additionally
requires that root directory flush. Journals are never acknowledgement
authority.

If the process dies after `CURRENT` replacement but before the root directory
flush, reopen accepts whichever exact valid `CURRENT` the filesystem presents.
It does not infer intent from journals, directory scans, timestamps, or UUID
order.

### Platform and filesystem scope

Durable projects may be created or mutated only after fail-closed preflight of:

1. exclusive and shared advisory locks released by the OS on process exit;
2. same-directory atomic file creation and replacement;
3. file data-and-metadata flush;
4. directory-entry flush for every changed directory; and
5. stable file identity while an open handle is locked.

Supported implementations remain those named by ADR 0013: POSIX local
filesystems with `fcntl`/`flock`, same-filesystem `rename(2)`, and file plus
directory `fsync(2)`; and Windows local NTFS/ReFS with `LockFileEx`,
`FlushFileBuffers`, atomic same-volume replacement, and a flushable directory
handle.

Network, userspace, removable, cross-device, symlink-mediated, or unknown
filesystems are rejected with `GF_UNSUPPORTED_FILESYSTEM` before the project
root or `CURRENT` changes. There is no best-effort durability mode.

### Recovery authority

Recovery and reopen resolve authority exactly as ADR 0013:

- the sole commit authority is an exact, valid `CURRENT` naming an existing
  generation whose manifest digest matches;
- journals and directory scans are advisory cleanup/diagnostics only;
- malformed, missing, or digest-mismatched pointers fail closed as
  `GF_PROJECT_CORRUPT` without electing a “newest” generation.

### Reader isolation

Every opened facade pins one immutable generation. Long-lived readers do not
follow later commits. Fresh opens resolve the generation named by durable
`CURRENT`. Graph, provenance, knowledge, and epistemic participants become
visible together; mixed generations are corruption.

### Writer isolation by mode

| Mode | Reader view | Writer admission | Commit order | Conflict outcomes |
| --- | --- | --- | --- | --- |
| `single_writer` | pinned immutable snapshot | competing writers fail with `GF_WRITER_BUSY` before staging | one serial writer | no concurrent writer conflicts; busy is pre-publication |
| `queued_writer` | pinned immutable snapshot; snapshot reads bypass the queue | bounded FIFO per facade; cancel only unstarted work | one serial writer after dequeue | queue-full / cancel structured errors; no concurrent publish races |
| `optimistic_multi_writer` | pinned immutable snapshot per attempt | distinct composite transaction identities may stage concurrently | `CURRENT` commit point is serialized; compatible work may rebase | closed matrix in ADR 0015: merge, `GF_WRITE_CONFLICT`, `GF_IDEMPOTENCY_CONFLICT`, `GF_REBASE_EXHAUSTED` |

Only `publish_composite_transaction` has optimistic replay in v0.5.x. Other
mutation APIs retain single-writer behavior even when the facade selects
optimistic mode.

These modes do **not** claim generic ACID, serializable isolation, or SSI.

### Write-skew witness (optimistic is not SSI)

Optimistic mode may merge concurrent changes to **different properties of the
same object**. That admits write-skew histories that are legal under snapshot
isolation but illegal under serializability.

Minimal witness:

1. Start from one committed object `Account` with properties `credit=0` and
   `debit=0`, plus invariant “`credit + debit <= 1`” maintained only by
   application logic.
2. Transaction T1 reads both properties, observes `debit=0`, and stages
   `credit=1`.
3. Transaction T2 concurrently reads both properties, observes `credit=0`, and
   stages `debit=1`.
4. Under `optimistic_multi_writer`, the closed merge rules treat these as
   different properties, so both may publish after rebase.
5. The committed generation can contain `credit=1` and `debit=1`, violating the
   application invariant even though neither writer observed the other's write.

Therefore public docs must classify optimistic mode as optimistic snapshot /
conflict semantics, never as SSI or serializable isolation. Preventing
write-skew requires a separately approved SSI design outside M6.

### Idempotency, retry, cancellation, unknown outcome

| Situation | Required behavior |
| --- | --- |
| Exact retry of the same operation identity and content after acknowledgement | Return the prior receipt / committed generation without restaging |
| Same operation identity with changed content | `GF_IDEMPOTENCY_CONFLICT` with zero mutation |
| Cancellation before staging or before linearization | Prior generation remains authoritative; peer operations are unaffected |
| Failure before `CURRENT` replacement | `committed: false`; parent remains authoritative |
| Failure after `CURRENT` replacement (including post-linearization API errors) | Writer rereads validated `CURRENT` under the writer lock and reports `committed: true`; the generation is not rolled back |
| Crash or I/O ambiguity before acknowledgement | Reopen selects only an exact valid prior or new `CURRENT`; unknown third states are not returned to callers |

### Publication vocabulary (shared with M5 interchange)

Import, export, bulk construction, and portable project surfaces that publish a
generation MUST use this vocabulary:

- **stage** — write private participants without moving `CURRENT`;
- **validate** — domain and composite checks against pinned parent plus staged
  bytes;
- **durable generation** — flushed participants + flushed manifest + flushed
  generation tree (and optimistic promotion when applicable);
- **linearize** — atomic `CURRENT` replacement or first creation;
- **acknowledge** — linearize plus project-root directory flush;
- **publish / published** — acknowledged-durable success visible to new opens;
- **abort** — abandon staged work without moving `CURRENT`;
- **recover** — reopen/classification that never elects authority from journals
  or directory scans.

M5 issues #738, #742, and #745 consume these terms; they do not redefine them.

### Observability and privacy

Safe fields: operation, phase, transaction/generation/parent UUIDs, lock-owner
UUID, capability and record-family IDs, counts, duration, filesystem class, and
recovery classification. Forbidden: graph properties, assertion/evidence text,
vector contents, credentials, user-controlled absolute paths, hostnames, or raw
lock metadata beyond machine-owned IDs.

## Consequences

- Callers can determine acknowledged durability and isolation without reading
  implementation comments.
- M6 fault modeling (#749), recovery-on-open (#750), deltas (#752), transactions
  (#754), and certification (#756) share one vocabulary and coverage matrix.
- Optimistic throughput remains available without false serializability claims.
- Unsupported filesystems stay fail-closed.

## Rejected alternatives

| Alternative | Reason |
| --- | --- |
| Treat `CURRENT` replacement alone as acknowledgement | Omits the root directory flush required against power loss |
| Claim SSI because readers pin snapshots | Write-skew remains possible under optimistic property merge |
| Best-effort mode on network filesystems | Flush and replacement semantics are not proven |
| Let journals elect authority after crash | Turns advisory cleanup into an election protocol |
| Silently revise ADR 0013/0015 text for M6 behavior changes | Semantic change requires an amending ADR |

## Required verification

- Contract schema validation and documentation link checks via
  `scripts/ci/durability-isolation-gate.py`.
- Matrix maps crash phases and anomalies to covered evidence or later M6 owner
  issues (#749–#756).
- Public docs reference this ADR and do not claim generic ACID or SSI.
