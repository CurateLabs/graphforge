# Project format compatibility before v1.0

**Status:** Accepted  
**Decision date:** 2026-07-24  

GraphForge is pre-v1. Project storage is not backward compatible before the
v1.0 format freeze.

## Contract

- GraphForge v0.5.0 opens only a valid v0.5 project container and committed
  generation.
- Any other on-disk layout is an unsupported input.
- GraphForge does not provide an importer, migration command/API, in-place
  upgrade, read-only compatibility view, generation-zero interpretation,
  identity conversion, data-loss report, rollback workflow, or fixture-retention
  promise for unsupported layouts.
- Unsupported inputs return `GF_UNSUPPORTED_PROJECT_FORMAT` before any file is
  created, removed, renamed, rewritten, or opened as graph data.
- Detection distinguishes only a valid supported v0.5 container, a recognized
  unsupported pre-v1 container, and malformed input. It never decodes
  unrecognized graph records or guesses a format from partial files.
- Data that is not already in a valid v0.5 project must be re-created through
  the v0.5 public construction or ingest surface.

## Knowledge-layer implementation boundary

The knowledge layer starts from the v0.5 graph and project contracts on `main`.

- removes obsolete graph-embedded confidence/provenance columns,
  execution options, and schemas. It does not transform unsupported project
  layouts or fabricate knowledge records from unrecognized fields.
- resolves only versioned v0.5 committed generations. No unsupported root is a
  readable snapshot or generation zero.
- initializes the v0.5 capability manifest. It does not upgrade unrecognized
  manifests.
- guarantees atomicity and recovery for v0.5 generations only.
- exposes no migration API in Rust, Python, or Node.
- proves deterministic creation, publication, recovery, reopen, and
  binding parity for v0.5 projects only.
- the v0.5.0 publication close-out requires clean v0.5 installs and project
  creation/reopen. It does not require upgrade evidence from unsupported
  artifacts.
- named checkpoints and complete-workspace revert operate only within the v0.5
  generation protocol. They do not recognize, migrate, or import unsupported
  layouts. The frozen contract is
  [ADR 0014](../../adr/0014-workspace-checkpoints.md).

## Committed-generation resolution

`graphforge-storage` resolves a project by reading the exact `FORMAT` marker, the
canonical bounded `CURRENT` record, and the manifest of the UUID named by that
record. It never elects a generation by directory enumeration. The manifest
digest must match `CURRENT`, and machine-owned paths must remain normalized,
contained, and free of links.

The resolved generation and its lease-file handle remain owned by the opened
`GraphForge` instance. Changing `CURRENT` therefore affects only later opens;
an existing instance never follows the pointer again. Abandoned generation and
transaction directories are neither consulted nor cleaned up by the read-side
resolver.

An explicitly empty directory may be initialized as a new v0.5 project. A
non-empty directory without the exact supported marker is never initialized or
modified.

## Atomic generation publication

`graphforge-storage` stages a complete participant set beneath one private generation
UUID while holding the non-blocking OS writer lock. Participant paths are
machine-derived from bounded lowercase capability and record-family IDs.
Every request is a complete replacement snapshot: callers must explicitly
carry forward unchanged parent participants, and an omitted participant is
absent from the child generation. Publication never silently merges staged
records with the parent.
Before publication, every file is closed, flushed, reread, and checked against
its exact byte length and SHA-256; domain-owning crates then validate schemas
and values through opaque callbacks, followed by composite reference
validation against the pinned parent plus staged records.

The generation manifest and lease are flushed only after validation.
`CURRENT` is replaced once, using a same-filesystem atomic replacement, after
the participant directories and generation directory are durable. That pointer
replacement is the only commit point. A pre-publication error leaves the parent
authoritative, while an error after replacement reports `committed=true`.

Transaction journals use the frozen
`PREPARING -> STAGED -> VALIDATED -> DURABLE -> PUBLISHED` phases but never
select readable state. Replaying a published transaction UUID with identical
generation inputs is idempotent even after later publications; reusing it with
different inputs returns `GF_IDEMPOTENCY_CONFLICT`. A held writer lock returns
`GF_WRITER_BUSY` immediately.

The protocol is limited to one local filesystem with one writer and multiple
snapshot readers. Distributed transactions, object-store consensus, and
caller-managed public transactions are outside the v0.5 contract.

## Interrupted transaction recovery

Ordinary project open runs recovery as a first-class lifecycle step.
`open_or_initialize_project_with_recovery()` (and therefore
`GraphForge::new` / directory open) first resolves the exact committed
generation, then uses bounded, link-safe inspection of journals, atomic
temps, and trash. When candidate work exists it acquires `locks/writer.lock`
non-blockingly in the established writer/transaction/checkpoint order,
recovers idempotently, re-resolves `CURRENT`, and opens that pinned
generation. Successful kernel lock acquisition is the only evidence that an
earlier writer is gone. PID, owner text, timestamps, heartbeats, and file age
never permit lock stealing.

When a live writer already owns the recovery locks, open still pins the
validated `CURRENT` generation and reports recovery as deferred
(`live_writer_owns_kernel_lock`) instead of failing the snapshot open or
stealing locks. Explicit `recover_project_transactions()` retains the
`GF_WRITER_BUSY` contract for callers that require exclusive cleanup.

While holding the recovery lock, cleanup classifies canonical transaction
journals in bounded lexical order. A journal whose generation and manifest
digest match `CURRENT` is repaired to `PUBLISHED`. A non-published journal for
any other generation is marked `ABORTED`; its UUID-named private generation is
removed only after the current generation and its two verified ancestors are
retained, an exclusive generation lease is acquired when present, and
`CURRENT` is rechecked. Published historical journals remain published.
Missing journals never affect authority, while a torn or ambiguous journal
returns `GF_PROJECT_CORRUPT` with restore guidance and preserves the evidence.

Cleanup atomically moves an authorized abandoned generation into root
`trash/`, flushes both directories, then deletes and flushes the trash entry.
A crash at either cleanup boundary is idempotently completed by the next
recovery pass. Unknown, linked, noncanonical, or journal-less generation
entries are counted and preserved rather than traversed or guessed about.

Initialization of an empty or resumable uninitialized container and
read-only checkpoint opens remain semantically distinct: they do not run the
recovery cleanup pass. Safe open evidence exposes selected generation class,
repaired/aborted journal counts, removed/quarantined entries, deferral
reason, and elapsed phase without payload data or sensitive paths.

New-container interruption after `FORMAT` is durable leaves a supported
uninitialized container. A later `open_or_initialize_project()` resumes that
current-format initialization under the existing exclusive root lock. This is
not migration or historical-format inference: only the exact v0.5 `FORMAT`
contract is resumable.

The named failpoint suite terminates subprocess writers at every frozen
container, staging, validation, durability, pointer, journal, and cleanup
boundary. Each reopen asserts the selected UUID, manifest digest, participant
bytes, commit-state envelope, kernel-lock release, and repeatable recovery.
Graph-only resolution and recovery read no knowledge participant bytes, so a
corrupt or future knowledge payload cannot block a graph-only operation after
its committed manifest has been selected.

## Versioned capability manifest

Every committed v0.5 generation manifest declares a canonical, strictly
ID-ordered capability list. `graph@1` and `workspace@1` are mandatory from the
first empty generation. `workspace@1` explicitly records ontology absence and
authoritative configuration. `provenance@1` and `knowledge@1` may be added
through one complete generation publication; `epistemic` is reserved for epistemic.
Capabilities are never inferred from directories or participant files.

`project_capabilities()` resolves only `FORMAT`, `CURRENT`, and the selected
manifest. It reports unknown IDs or future versions as `unsupported_future`
without opening their participants. A capability-specific API must separately
require its supported version before opening any of its record families.

`enable_capability()` carries every verified existing participant into a
complete replacement generation, adds the requested declaration and its
registered initial participants, and publishes through the same writer lock,
journal, durability, and recovery protocol. Its generation UUID is a
deterministic UUIDv8 projection of the operation UUID and canonical request.
The same operation/request is idempotent; conflicting versions and knowledge-layer attempts
to enable `epistemic` fail before publication.

The capability declaration and participant inventory are separate manifest
fields. A supported capability may initially have zero registered participants;
that is an explicit empty ledger, not directory inference. As each owning
domain schema lands, its implementation registers canonical empty participants
for new enable operations and treats absent record families in an already
enabled capability as empty. A present but corrupt or unsupported participant
is never treated as empty.

### Graph record families under `graph@1`

`graph@1` remains the mandatory capability version. Two mutually exclusive
record families may appear:

- `snapshot` (Arrow IPC) — legacy whole-workspace envelope (1 GiB/file, 2 GiB
  total). Existing projects remain readable through hydrate.
- `files` (canonical JSON inventory) — file-backed contract. Exact workspace
  files live under the generation-owned `graph/` directory beside
  `participants/`. Open validates the inventory and either pins that tree
  (read-only) or materializes file-by-file into a private workspace. New
  publications use this path. Unsupported inventory versions return
  `GF_UNSUPPORTED_PROJECT_FORMAT`.

Portable interchange does not yet encode the generation-owned `graph/` tree and
returns a structured unsupported error for `files` generations; copy the project
directory instead. Checkpoint open/diff follows the same GraphForge hydration
path for either family.

There is no capability migration from unsupported layouts. A boolean manifest,
missing v0.5 `FORMAT`, or any other pre-v1 root returns
`GF_UNSUPPORTED_PROJECT_FORMAT` before mutation.

## Security and diagnostics

Unsupported input is untrusted. The detector validates path containment and
reads only the minimum bounded header/manifest bytes needed to return a
structured classification. Diagnostics may contain the expected format
version, detected format family/version when safely available, and sanitized
error code. They never contain graph values, user content, unrestricted paths,
or historical database records.

## Future compatibility

This decision does not define the v1.0 compatibility policy. A future format
freeze requires its own versioning, support-window, and upgrade decision. No
pre-v1 behavior becomes an implicit precedent.
