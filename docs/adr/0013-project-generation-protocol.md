# ADR 0013: Durable v0.5 project-generation protocol

**Status:** Accepted
**Date:** 2026-07-24
**Build target:** v0.5.0

**Related:** ADR 0012 (domain ownership)

## Context

GraphForge currently rewrites fixed Parquet paths with sequential renames. A
rename failure can expose a committed prefix, the topology counter is advanced
separately from the data it describes, and Windows directory durability is not
implemented. That mechanism cannot publish graph, provenance, and knowledge
participants as one crash-safe project state.

GraphForge is pre-v1. This protocol is the first supported project format. It
does not recognize, migrate, import, or synthesize a generation-zero view of
any earlier development layout.

## Decision

### Scope and filesystem support

The protocol is for a local filesystem, one commit writer, any number of
readers, and optionally multiple private optimistic staging attempts. The
implementation must preflight these primitives before creating or mutating
a project:

1. exclusive and shared advisory file locks released by the operating system
   when a process exits;
2. same-directory atomic file creation and replacement;
3. file data-and-metadata flush;
4. directory-entry flush for every directory whose entries change; and
5. stable file identity while an open handle is locked.

The supported implementations are:

- POSIX local filesystems providing `fcntl`/`flock`, same-filesystem
  `rename(2)`, file `fsync(2)`, and directory `fsync(2)`;
- Windows local NTFS/ReFS volumes providing `LockFileEx`, `FlushFileBuffers`,
  atomic same-volume replacement (`ReplaceFileW`, with atomic first creation),
  and a flushable directory handle.

The implementation identifies the backing filesystem/volume and runs a
create-lock-flush-replace-flush probe in a private sibling under the proposed
project parent. Network, userspace, removable, cross-device, or unknown
filesystems are rejected unless they are explicitly added to the tested
allowlist. Missing semantics return `GF_UNSUPPORTED_FILESYSTEM` before the
project root or `CURRENT` is changed. There is no best-effort mode.

### On-disk layout

All machine paths use lowercase ASCII letters, digits, hyphens, and underscores.
UUIDs are lowercase canonical
hyphenated strings. SHA-256 values are 64 lowercase hexadecimal characters.
No caller-controlled name enters a path.

```text
project/
├── FORMAT
├── CURRENT
├── locks/
│   ├── writer.lock
│   └── transactions/
│       └── <transaction-uuid>.lock
├── transactions/
│   └── <transaction-uuid>.json
├── attempts/
│   └── <transaction-uuid>/<request-sha256>/
│       └── participants/
├── generations/
│   └── <generation-uuid>/
│       ├── lease.lock
│       ├── manifest.json
│       └── participants/
│           └── <capability-id>/<record-family-id>.<parquet|arrow|json>
├── cache/                  # derived, fingerprint-keyed, never authoritative
└── trash/
    └── <generation-uuid>/
```

`FORMAT` is immutable and contains exactly:

```text
graphforge-project/v1\n
```

`CURRENT` is the sole commit authority. It is canonical UTF-8 JSON followed by
one LF, with keys in this exact order and no insignificant whitespace:

```json
{"format":"graphforge-project","format_version":1,"generation_uuid":"019...","generation_manifest_sha256":"<64 lowercase hex>"}
```

`manifest.json` is canonical JSON followed by one LF. Its fixed top-level key
order is:

```text
format
format_version
generation_uuid
parent_generation_uuid
transaction_uuid
capabilities
participants
```

`capabilities` are sorted by stable capability ID. `participants` are sorted
by `(capability_id, record_family_id, relative_path)`. Every participant entry
contains, in order:

```text
capability_id
capability_version
record_family_id
record_version
relative_path
encoding
byte_length
row_count
schema_fingerprint
content_sha256
```

Paths are normalized relative paths beneath that generation's `participants/`
directory. Absolute paths, `..`, symlinks, junctions, reparse points, hard
links, or a resolved path outside the generation are invalid. `content_sha256`
is over exact persisted bytes. `generation_manifest_sha256` is the integrity
digest over the exact bytes of `manifest.json`, including its final LF.
Semantic record, schema, descriptor, projection, and result bytes follow the
[canonical fingerprint v1
contract](../book/architecture/canonical-fingerprints-v1.md); the manifest field
order and raw integrity digests above remain frozen by this ADR.

Every generation is a complete immutable project snapshot. An unchanged
participant may be copied from the pinned parent while staging, but the
published generation never depends on mutable data outside its own directory.
Derived caches may remain under root `cache/` only when they are keyed by the
canonical source fingerprint and their absence or corruption cannot change
results.

### Container creation

An absent path, or an explicitly supplied empty directory, may become a new
container. Creation performs the filesystem preflight, creates the tree, writes
and flushes `FORMAT`, and flushes every created directory bottom-up. For an
absent path, the complete private root is atomically installed from a sibling.
For an explicitly empty existing directory, initialization occurs in place
while holding an exclusive parent-scoped creation lock whose name is the
SHA-256 of the canonical target path. That lock uses the same OS-lock rules as
`writer.lock`; its file name contains no caller text. `CURRENT` does not exist
yet.

Such a container is *uninitialized*, not generation zero. Project APIs return
`GF_PROJECT_UNINITIALIZED` until the first complete generation is published.
An existing non-empty directory without the exact `FORMAT` bytes returns
`GF_UNSUPPORTED_PROJECT_FORMAT` before mutation. A different or future format
version returns the same code.

### Locks and ownership

`locks/writer.lock` is held exclusively for the complete transaction in the
default single-writer protocol, including cleanup after publication. An
optimistic attempt instead holds its transaction-scoped kernel lock while it
prepares and validates in `attempts/`; it acquires `writer.lock` only for base
comparison, promotion, publication, and cleanup. The kernel locks are
authoritative.
Metadata written after acquisition may contain an owner UUID, process ID,
hostname hash, operation, and acquisition time for diagnostics only. PID,
hostname, file age, heartbeat age, or wall-clock time never permits lock
stealing. A dead process is proved only when the operating system releases its
lock.

Only one live attempt may own a transaction UUID. Different transaction UUIDs
may stage concurrently. Transaction lock files carry no authority once their
OS lock is released and are never reclaimed using PID, age, or heartbeat
heuristics.

The default writer-lock acquisition is non-blocking and returns
`GF_WRITER_BUSY`.
Callers may supply a finite timeout measured by a monotonic clock; expiry
returns the same code without mutation. Infinite waits are not exposed by a
public API. A validated optimistic attempt is the exception inside the storage
protocol: it waits for the short commit lock, compares its pinned parent with
`CURRENT`, and returns `GF_WRITE_CONFLICT` when another commit won. This wait is
not a caller-configurable lock API and cannot expose partial state.

Each generation has `lease.lock`. A reader:

1. reads and validates `CURRENT`;
2. opens that generation's `lease.lock` without following links;
3. acquires a shared OS lock;
4. rereads `CURRENT` and verifies that the generation directory has not moved;
5. validates the manifest digest and requested participants; and
6. retains the lock and resolved generation path for the snapshot lifetime.

If step 4 observes a concurrent cleanup, the reader releases the handle and
retries from step 1. It never follows a changed `CURRENT` after pinning. A new
reader resolves the new generation; an existing reader continues to see its
pinned generation.

Cleanup must acquire the same `lease.lock` exclusively and then recheck
reachability before moving a generation. OS lock release, not a timeout, proves
that a crashed reader is gone.

### Publication state machine

The journal state names are:

```text
PREPARING -> STAGED -> VALIDATED -> DURABLE -> PUBLISHED
    |           |          |          |
    +-----------+----------+----------+----> ABORTED
```

The journal aids cleanup and diagnostics but never selects a generation.
Journal replacement itself uses write, file flush, atomic replace, and
directory flush. Its failure can leave an earlier valid journal state without
changing commit authority.

The default writer performs these ordered operations while holding
`writer.lock`. An optimistic writer performs steps 1 through 4 under its
transaction lock, then acquires `writer.lock`, resolves `CURRENT` again, and
continues only when it still names the pinned parent:

1. **Resolve parent.** Pin and fully validate `CURRENT`, if present. An absent
   `CURRENT` is allowed only for an uninitialized v1 container.
2. **PREPARING.** Create a transaction UUID and a private attempt directory;
   persist a `PREPARING` journal containing only IDs, phases, and relative
   participant names.
3. **STAGED.** Write every participant to its final path in the private
   generation. Close the format writer, flush each file, verify byte length and
   SHA-256 from a fresh read, then persist the `STAGED` journal.
4. **VALIDATED.** Run domain-local validation, build the combined parent-plus-
   staged UUID index, run composite cross-domain validation, and persist the
   `VALIDATED` journal. No validation may read through `CURRENT` again.
5. **DURABLE.** Write and flush `lease.lock`, write and flush
   `manifest.json`, reread and verify every participant and the manifest, flush
   `participants/` directories from leaves upward, flush the generation
   directory, promote an optimistic attempt by atomic rename into
   `generations/<generation-uuid>/`, flush both changed parent directories and
   `generations/`, then persist the `DURABLE` journal.
6. **Publish.** Write the exact new `CURRENT` bytes to a sibling private file,
   flush it, atomically replace `CURRENT` (or atomically create it for the first
   generation), and flush the project root.
7. **PUBLISHED.** Persist the `PUBLISHED` journal. The writer may now release
   the parent reader lease, perform conservative cleanup, and release
   `writer.lock`.

The successful atomic replacement/creation of `CURRENT` in step 6 is the sole
linearization point. Before it, every reader resolves the parent. After it,
every new reader resolves the new generation. A journal state, directory scan,
newer UUID, timestamp, or highest counter can never override `CURRENT`.

If the optimistic base comparison finds a different committed parent, the
attempt is marked `ABORTED`, its private directory is removed, and
`GF_WRITE_CONFLICT` is returned without promoting a generation. Domain-level
rebase policy belongs above this storage protocol.

The root directory flush is required to make the pointer replacement durable
against power loss. If the process stops between pointer replacement and root
flush, reopen uses whichever complete `CURRENT` the filesystem presents; it
does not infer intent from the journal.

### Validation and failure behavior

Publication is fail-closed:

- a write, flush, checksum, validation, lock, or replacement failure before the
  linearization point leaves the parent authoritative;
- failure after the linearization point cannot roll back the visible
  generation;
- after any replacement API error, the writer rereads and validates `CURRENT`
  while still holding `writer.lock`; the returned error includes
  `committed: false` or `committed: true`, never an unresolved third state;
- all error envelopes contain a stable code, transaction UUID, generation UUID,
  phase, and safe cause class, never participant values.

`CURRENT` must parse exactly, name an existing generation, and match that
generation's manifest digest. A malformed pointer, missing generation, digest
mismatch, symlink, or invalid requested participant returns
`GF_PROJECT_CORRUPT`. Recovery does not scan for a plausible replacement and
does not fall back to a previous generation.

Domain-local and cross-domain reference validation follow ADR 0012. References
to graph, provenance, or knowledge rows staged in the same transaction resolve
against the combined pinned-parent and staged indexes. A dangling or ambiguous
reference fails before `DURABLE`.

### Recovery

Open always resolves authority first:

1. verify the exact `FORMAT` bytes;
2. read and validate `CURRENT`, or return `GF_PROJECT_UNINITIALIZED` when it is
   absent;
3. acquire the selected generation's shared lease;
4. verify the manifest digest and only the capabilities requested by the API.

With the writer lock held, recovery classifies journals mechanically:

Before classifying a journal, recovery also acquires its transaction lock
without waiting. A busy transaction lock proves that the private attempt is
live, so recovery leaves that journal and attempt untouched. Because recovery
already owns `writer.lock`, that attempt cannot cross the commit boundary
during classification.

| Observed state | Classification | Action |
|---|---|---|
| `CURRENT` names the journal generation with matching digest | committed | finish/repair journal to `PUBLISHED`; retain generation |
| `CURRENT` names another valid generation | uncommitted | mark `ABORTED`; generation is cleanup-eligible |
| no `CURRENT` in an uninitialized v1 container | uncommitted | mark `ABORTED`; generation is cleanup-eligible |
| `CURRENT` is invalid or references invalid bytes | corrupt | return `GF_PROJECT_CORRUPT`; do not guess or delete evidence |
| journal is missing, truncated, or stale while `CURRENT` is valid | advisory damage | reconstruct safe cleanup facts from `CURRENT`; never change authority |

Recovery is idempotent. Repeating it produces the same selected generation and
does not rewrite participant or manifest bytes.

### Retention and garbage collection

The current generation is always reachable. The default retention set also
contains its two most recent valid ancestors, determined only from verified
`parent_generation_uuid` links. A configured larger finite retention count may
add ancestors but may not remove the current generation.

For every other generation, cleanup:

1. acquires `writer.lock`;
2. resolves and validates `CURRENT`;
3. acquires the candidate `lease.lock` exclusively without waiting;
4. recomputes reachability;
5. atomically moves the candidate into `trash/`;
6. flushes `generations/` and `trash/`; and
7. deletes the trash entry and flushes `trash/`.

A busy lease skips the candidate. A crash before the move leaves it intact; a
crash after the move makes it invisible to readers and eligible for deletion
on the next recovery. Cleanup never changes `CURRENT`. Unknown directories,
symlinks, or invalid manifests are quarantined and reported, not traversed or
deleted automatically.

### Named failpoints and required outcomes

Each failpoint fires once and terminates the writer as if the process stopped.
The names are public test vocabulary:

| Failpoint | Required authority on reopen |
|---|---|
| `project.after_format_fsync` | no installed project, or uninitialized v1 container |
| `project.after_container_dir_fsync` | uninitialized v1 container |
| `project.after_writer_lock` | parent / uninitialized |
| `project.after_journal_preparing` | parent / uninitialized |
| `project.after_participant_write` | parent / uninitialized |
| `project.after_participant_fsync` | parent / uninitialized |
| `project.after_participant_dir_fsync` | parent / uninitialized |
| `project.after_journal_staged` | parent / uninitialized |
| `project.after_domain_validation` | parent / uninitialized |
| `project.after_composite_validation` | parent / uninitialized |
| `project.after_journal_validated` | parent / uninitialized |
| `project.after_manifest_write` | parent / uninitialized |
| `project.after_manifest_fsync` | parent / uninitialized |
| `project.after_generation_dir_fsync` | parent / uninitialized |
| `project.after_optimistic_commit_lock` | parent / uninitialized |
| `project.after_optimistic_promotion` | parent / uninitialized |
| `project.after_journal_durable` | parent / uninitialized |
| `project.after_current_temp_write` | parent / uninitialized |
| `project.after_current_temp_fsync` | parent / uninitialized |
| `project.before_current_replace` | parent / uninitialized |
| `project.after_current_replace` | new generation |
| `project.after_root_fsync` | new generation |
| `project.after_journal_published` | new generation |
| `project.after_gc_move` | current unchanged; candidate absent from generations |
| `project.after_gc_delete` | current unchanged; candidate absent from generations |

The implementation may parameterize participant failpoints with an index, but
the stable base name and outcome do not change. Each pre-publication failpoint
also has an `.error` variant that makes that operation return its platform
error instead of terminating; it must return `committed: false`. The
`project.after_current_replace.error` variant must return `committed: true`.
Tests must exercise every row and every error variant for first publication and
replacement publication on each supported operating system. Tests assert
selected UUID, manifest digest, participant checksums, and row visibility; they
do not accept log inspection or timing as evidence.

### Observability and privacy

Structured events may include operation, phase, transaction/generation/parent
UUIDs, lock-owner UUID, capability and record-family IDs, counts, duration,
safe filesystem class, and recovery classification. They must not include
graph properties, assertion/evidence/reasoning text, vector contents,
credentials, user-controlled absolute paths, hostnames, or raw lock metadata.

New files inherit a project-private permission policy. A project open rejects
symlinks and reparse points at every machine-owned component, opens files
without following links where the platform supports it, and revalidates file
identity after locking to prevent path substitution.

## Consequences

- One pointer replacement publishes every project participant together.
- A failed multi-file write cannot expose a committed prefix.
- Readers have repeatable snapshots without blocking publication.
- Crashed readers and writers need no wall-clock stale-owner heuristic.
- Recovery is deterministic because it never elects a generation.
- Windows and POSIX implementations must meet the same contract or fail before
  mutation.
- Existing fixed-path `RewriteBatch` and standalone generation counters are
  transitional internals to be replaced and related knowledge-layer issues.

The cost is duplicate immutable snapshot data until later content-addressed
deduplication. Correctness and a finite recovery proof take precedence.

## Rejected alternatives

| Alternative | Reason |
|---|---|
| Sequentially rename participant files | Exposes a committed prefix after interruption. |
| Treat a topology counter as commit authority | Does not identify a complete cross-domain snapshot. |
| Pick the newest valid directory during recovery | Makes a directory scan an implicit election protocol. |
| Steal locks by PID, age, or heartbeat | PID reuse and clock behavior cannot prove the owner is dead. |
| Delete old generations after a fixed delay | Timing cannot prove that a reader released its snapshot. |
| Keep one mutable generation directory | Cannot give old and new readers stable concurrent views. |
| Accept unknown/network filesystems best-effort | Their replacement and flush semantics are not sufficient evidence. |
| Add a pre-v1 importer or generation zero | GraphForge has no backward-compatibility requirement before v1. |

## Required verification

- implements the state machine and stable error envelope.
- implements every named failpoint for first and later publication.
- repeats the crash/reopen matrix through Rust, Python, and Node package
  surfaces and proves graph/knowledge isolation.
- CI runs the platform matrix on Linux, macOS, and Windows local filesystems.
