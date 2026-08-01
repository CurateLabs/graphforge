# ADR 0014: Complete-workspace checkpoints and generation-preserving revert

**Status:** Accepted  
**Date:** 2026-07-25  
**Build target:** v0.5.0  

**Contract:** [`graphforge-checkpoint-api/1`](../contracts/checkpoint-api-v1.json)  
**Related:** ADR 0012 (domain ownership), ADR 0013 (project generations),
–

## Context

v0.5.0 requires named, Git-like recovery points for the complete embedded
workspace. A checkpoint is not a DataFusion execution checkpoint, transaction
rollback, mutable branch, or pointer that can move `CURRENT` backward.

ADR 0013 already makes `CURRENT` the sole commit authority and each generation
a complete immutable snapshot. This decision adds durable names for those
snapshots, lease-pinned historical reads, deterministic diff, and revert by
publishing a new generation. GraphForge is pre-v1: no earlier project format is
recognized, migrated, or retained for compatibility.

## Decision

### Complete workspace

A generation and therefore a checkpoint contain every authoritative,
generation-managed participant:

- graph topology, properties, and `RuntimeCatalog`;
- canonical ontology bytes, effective ontology mode, and project configuration;
- capability declarations and primary workbench or embedding data;
- provenance, lineage, assertions, evidence, confidence, and run history; and
- epistemic records, statuses, supersession, hypotheses, and valid time.

Absence is explicit. A project with no adopted ontology stores an
`ontology@1` participant whose mode is `none` and whose canonical ontology
digest is null. Root YAML/JSON files are import or edit inputs only. Adoption
publishes a complete generation; session-scoped ontology loading does not alter
the project and cannot appear as adopted state.

`workspace_ontology@1` is canonical JSON plus LF with ordered fields
`contract_version`, `mode`, `source_format`, `canonical_ontology_sha256`, and
`canonical_ontology`. `mode` is `none`, `advisory`, or `strict`;
`source_format` is null, `yaml`, or `json`; `canonical_ontology` is null for
`none` and otherwise the ontology model serialized as canonical JSON, not the
original input bytes. `workspace_configuration@1` is canonical JSON plus LF
with ordered fields `contract_version`, `ontology_mode`,
`capability_configuration`, and `embedding_configuration`. Configuration maps
are ordered by their registered stable IDs and contain only versioned,
engine-recognized settings. Secrets, provider credentials, local paths, CLI
preferences, and environment overrides are never participants.

The only persistent ontology mutation is
`adopt_ontology(input, mode, idempotency_key, actor_uuid)`. It parses bounded
YAML/JSON, canonicalizes and validates the model, updates both workspace
participants, carries every other participant forward, and publishes one
generation. `clear_ontology(idempotency_key, actor_uuid)` publishes explicit
`none` participants. Ordinary `load_ontology` remains session-scoped and
cannot write these records. Inspection methods
`workspace_ontology()` and `workspace_configuration()` return the generation
UUID, mode, contract version, and canonical digest through registered Arrow
schemas; binding parity for those Rust-owned results is delivered.

Derived caches, locks, journals, credentials, temporary files, and external
source files are excluded. A cache may be reused only when its complete source
fingerprint matches the selected generation. Checkpoint registry records are
root metadata and survive revert.

### Registry and paths

The machine-owned layout extends ADR 0013:

```text
project/
├── checkpoints/
│   ├── registry.json
│   ├── registry.json.sha256
│   └── registry.txn.json
└── locks/
    └── checkpoints.lock
```

`registry.json` is canonical UTF-8 JSON plus one LF. Its top-level fields are
`format`, `format_version`, `revision`, `active`, and `tombstones`, in that
order. The format is `graphforge-checkpoints`, version `1`. `revision` is a
monotonic `UInt64` incremented by each successful registry mutation.

Active rows are sorted by `(name, checkpoint_uuid)`. Tombstones are sorted by
`(deleted_revision, checkpoint_uuid)`. Each active row has, in order:

```text
checkpoint_uuid
name
generation_uuid
generation_manifest_sha256
description
created_at
created_by
create_operation_uuid
create_request_sha256
created_revision
```

Each tombstone has the same immutable identity and target fields followed by
`deleted_at`, `deleted_by`, `delete_operation_uuid`, `delete_request_sha256`, and
`deleted_revision`. Timestamps are engine-supplied UTC microseconds.
`created_by` and `deleted_by` are nullable actor UUIDs. The file has a sibling
`registry.json.sha256` containing lowercase SHA-256 plus LF over the exact
registry bytes. A missing pair means no checkpoint has ever been created; a
partial, linked, oversized, noncanonical, or digest-mismatched pair is
`GF_CHECKPOINT_REGISTRY_CORRUPT`.

`registry.txn.json` is a private, canonical, size-bounded recovery intent. It
contains the transaction UUID, previous revision and digest (nullable for the
first create), next revision and digest, and the exact private temporary
filenames. Mutation writes and flushes both next files, atomically installs and
directory-flushes the intent, replaces `registry.json`, replaces its checksum,
directory-flushes, then removes the intent and directory-flushes again.
Recovery never guesses: with an intent it accepts only the fully validated
previous pair or completes the fully validated staged next pair; any other
combination fails closed. Without an intent, a partial or mismatched pair is
corrupt. This pair-commit protocol is what “previous-or-new” means at every
declared failpoint.

The intent is canonical JSON plus LF, at most 16 KiB, with fields in this exact
order: `transaction_uuid`, `previous_revision`, `previous_sha256`,
`next_revision`, `next_sha256`, `registry_temp`, `checksum_temp`. Temporary
names are exact root-local siblings
`.registry.<transaction-uuid>.json.next` and
`.registry.<transaction-uuid>.sha256.next`. Intent and temporary files must be
single-link regular files and are opened without following links. Recovery's
complete state table is:

| intent | installed pair | staged next checksum | result |
|---|---|---|---|
| absent | valid previous or valid next | n/a | accept installed pair |
| absent | partial or mismatched | n/a | fail corrupt |
| present | valid previous | valid staged next | retain previous; remove intent/temps |
| present | installed pair absent, no previous declared | valid staged next | retain absent previous; remove intent/temps |
| present | next registry plus previous/missing checksum | valid staged next | install next checksum; accept next |
| present | valid next | absent or valid | accept next; remove intent/temps |
| present | any other combination | any | fail corrupt |

Names are NFC-normalized UTF-8 content, never paths. They are 1–128 UTF-8 bytes
after normalization and may contain letters, numbers, spaces, `_`, `-`, and
`.`. Leading or trailing whitespace, `.` or `..`, control characters, path
separators, repeated whitespace, and normalization-changing ambiguity are
rejected with `GF_VALIDATION`. Names compare by exact normalized bytes.
Descriptions are nullable and at most 1,024 UTF-8 bytes.

Limits are 1,024 active checkpoints, 4,096 tombstones, 8 MiB for the registry,
and exactly 65 bytes for its checksum. Both reads and the canonical next bytes
before mutation enforce these byte bounds. A create beyond the active limit returns
`GF_RESOURCE_LIMIT` without mutation. The tombstone is the delete idempotency
record. Once tombstones exceed 4,096, the oldest tombstones are removed in
`(deleted_revision, checkpoint_uuid)` order. Exact delete replay is guaranteed
only while its tombstone is retained; after deterministic pruning, the same
request is treated as a delete of an unknown name and returns
`GF_CHECKPOINT_NOT_FOUND`. No second unbounded idempotency index exists. The
same bounded window applies to reuse/conflict history for the corresponding
create operation after deletion; active creates remain replayable while active.
The registry never grows implicitly from generation scans.

Registry changes hold ADR 0013's project `writer.lock` and then
`checkpoints.lock`, always in that order. They use private sibling write, file
flush, atomic replace, and directory flush. No other lock order is permitted.
An interruption exposes exactly the previous or next valid registry revision.

### Creation, deletion, and idempotency

`checkpoint` resolves and leases `CURRENT`, validates the complete selected
generation, then atomically adds one active row. It does not publish a
generation or modify participants. Its UUID is deterministic UUIDv8 from
`create_operation_uuid` and the canonical request fingerprint.

The canonical request fingerprint is
`SHA-256("graphforge-checkpoint-create-request/1" || operation_uuid_bytes ||
u32be(name_len) || normalized_name_utf8 || description_presence_byte ||
[u32be(description_len) || description_utf8] || actor_presence_byte ||
actor_uuid_bytes_if_present)`. The checkpoint UUID is the RFC 9562 UUIDv8
projection of
`SHA-256("graphforge-checkpoint-uuid/1" || create_operation_uuid_bytes ||
create_request_sha256)`: take the first 16 digest bytes, set version to 8 and
the RFC variant bits exactly as `graphforge_core::canonical::uuid_v8` does. Lengths are
unsigned 32-bit big-endian values. Presence bytes are exactly `0x00` absent and
`0x01` present; absent optional fields omit their length and payload. These
bytes are frozen test vectors, not implementation choice.

Repeating an operation UUID with identical canonical request bytes returns the
original receipt. Reuse with different bytes is
`GF_IDEMPOTENCY_CONFLICT`. Creating an existing active name with a distinct
operation is `GF_CHECKPOINT_EXISTS`, even when it targets the same generation.

`delete_checkpoint` resolves the active name, writes one tombstone, and removes
the active row in the same registry replacement. Repeating the same delete
request returns its original receipt. A distinct delete of an already deleted
or unknown name returns `GF_CHECKPOINT_NOT_FOUND`. Deletion releases only the
retention root; it never removes a generation synchronously and never
invalidates an existing open view.

Its request fingerprint is
`SHA-256("graphforge-checkpoint-delete-request/1" || operation_uuid_bytes ||
u32be(name_len) || normalized_name_utf8 || actor_presence_byte ||
actor_uuid_bytes_if_present)`, using the same exact presence-byte rules.

Mutation and recovery lock order is always project `writer.lock`, then
`checkpoints.lock`, then post-lock generation resolution/lease/validation.
The selected generation lease is held through registry publication. Deletion
does not acquire the generation lease; any independent open lease continues to
protect its generation from cleanup.

### Retention, leases, and recovery

Reachability is the union of:

1. the current generation;
2. ADR 0013's configured finite current-ancestor window;
3. every generation named by an active, fully validated checkpoint row; and
4. every generation with an acquired reader lease.

Cleanup validates the registry before classifying any generation. Corrupt
checkpoint metadata fails closed: recovery reports
`GF_CHECKPOINT_REGISTRY_CORRUPT` and deletes nothing. It never guesses names or
targets from directories.

An open checkpoint view resolves the active row, opens the exact generation
lease without following links, rereads the same registry revision, verifies
the manifest digest, and retains the shared OS lease for its lifetime. If the
row changed while pinning, resolution restarts. After pinning, deletion does
not affect the view. Cleanup requires an exclusive generation lease and a
fresh reachability calculation before moving a generation to trash.

### Read-only historical views

`open_checkpoint(name)` returns a `CheckpointView`, not a mutable
`GraphForge`. All supported graph, Cypher, analyst-verb/find, knowledge-layer, and epistemic reads execute
against its one pinned generation. Every mutation, capability enablement,
refresh/publication, checkpoint mutation, or revert attempt through the view
returns `GF_READ_ONLY_VIEW` before acquiring a writer lock.

There is no public operation that opens an arbitrary generation UUID, manifest
digest, or filesystem path. A deleted name cannot create a new view, while an
already opened view remains usable until dropped.

### Revert

`revert_to_checkpoint` validates and leases both current and checkpoint
generations under the writer lock. It stages a new complete generation whose
authoritative participants are exact logical copies of the checkpoint
participants plus one restoration transition. It then runs all domain and
cross-domain validators and publishes through ADR 0013. `CURRENT` is never
pointed backward.

The new generation:

- has the prior current generation as `parent_generation_uuid`;
- records the checkpoint generation as its restoration source;
- receives a new deterministic generation UUID and transaction UUID;
- preserves the former current and intervening generations as immutable
  history subject to ordinary retention; and
- does not create, delete, or rename checkpoint registry rows.

The restoration record belongs to the mandatory `workspace@1` capability and
record family `restoration_transition@1`. It is a normal manifest participant;
ADR 0013's manifest shape is unchanged. `graphforge-storage` verifies exact bytes and
manifest integrity, while domain owners validate their own rows:
`graphforge-ontology` validates ontology/config, `graphforge-provenance` validates provenance
and lineage, `graphforge-knowledge` validates knowledge-layer, `graphforge-api` validates epistemic and composite
cross-domain UUID references, and graph/storage validators own topology,
properties, catalog, capabilities, and workspace restoration records. A revert
cannot publish until every registered validator accepts the staged complete
snapshot.

The `restoration_transition@1` participant contains exactly one row:

| Field | Arrow type | Null |
| ------------------------------- | ----------------------------- | ---- |
| `restoration_uuid` | `FixedSizeBinary(16)` | no |
| `checkpoint_uuid` | `FixedSizeBinary(16)` | no |
| `source_generation_uuid` | `FixedSizeBinary(16)` | no |
| `source_manifest_sha256` | `FixedSizeBinary(32)` | no |
| `prior_current_generation_uuid` | `FixedSizeBinary(16)` | no |
| `restored_generation_uuid` | `FixedSizeBinary(16)` | no |
| `operation_uuid` | `FixedSizeBinary(16)` | no |
| `actor_uuid` | `FixedSizeBinary(16)` | yes |
| `reason` | `Utf8` | no |
| `restored_at` | `Timestamp(Microsecond, UTC)` | no |
| `contract_version` | `UInt32` | no |

`reason` is 1–1,024 UTF-8 bytes after trimming outer whitespace. The trimmed
bytes are the persisted bytes. Revert identity uses these exact domains and
byte layouts (domain strings have no terminator):

```text
revert_request_sha256 = SHA-256(
    "graphforge-checkpoint-revert-request/1" || operation_uuid_bytes ||
    u32be(name_len) || normalized_name_utf8 || checkpoint_uuid_bytes ||
    source_generation_uuid_bytes || source_manifest_sha256 ||
    u32be(reason_len) || trimmed_reason_utf8 || actor_presence_byte ||
    actor_uuid_bytes_if_present)

transaction_uuid = uuid_v8(SHA-256(
    "graphforge-checkpoint-revert-transaction/1" || operation_uuid_bytes))

restoration_uuid = uuid_v8(SHA-256(
    "graphforge-restoration-transition-uuid/1" || operation_uuid_bytes ||
    revert_request_sha256))

restored_generation_uuid = uuid_v8(SHA-256(
    "graphforge-checkpoint-restored-generation/1" || transaction_uuid_bytes ||
    checkpoint_uuid_bytes || source_generation_uuid_bytes ||
    source_manifest_sha256 || prior_current_generation_uuid_bytes ||
    i64be(restored_at_micros) || revert_request_sha256))
```

`u32be` is unsigned big-endian, `i64be` is signed two's-complement big-endian,
UUIDs are their 16 RFC-order bytes, SHA-256 values are their 32 raw bytes, and
actor presence is exactly `0x00` absent or `0x01` present. An absent actor omits
its UUID bytes. `uuid_v8` is exactly `graphforge_core::canonical::uuid_v8`.

On the first attempt, after acquiring `writer.lock` then `checkpoints.lock` and
resolving both generations, the engine selects `restored_at` once from its UTC
microsecond clock. Before staging participant bytes, the ADR 0013 transaction
journal's revert extension durably records the operation UUID, canonical
request digest, resolved checkpoint/source/prior-current identities and digest,
the selected `restored_at`, reason, actor, and derived UUIDs. These fields are
canonical request metadata, never participant values discovered by scanning.
Recovery and replay load this durable extension before deriving participant
bytes; they never sample the clock again. The transaction UUID depends only on
the operation UUID so the replay record remains directly addressable even if
the checkpoint name is later deleted. Identical replay returns the original
receipt. Reuse of the operation UUID with different canonical request bytes is
`GF_IDEMPOTENCY_CONFLICT`.

A failure before `CURRENT` replacement leaves the prior current authoritative.
A failure at or after replacement returns the ADR 0013 committed-state result.
Reopen resolves exactly the prior or restored complete generation, never a
mixture. Revert is not selective undo and does not erase later history.

### Diff

`diff_checkpoints(from, to, scope, page)` accepts checkpoint names plus the
reserved selector `CURRENT`. Both endpoints are independently lease-pinned
before reading. Scope is one of `summary`, `graph`, `ontology`, `configuration`,
`capabilities`, `provenance`, `knowledge`, `epistemic`, or `all`.

Summary diff emits one row per registered participant, ordered by
`(scope, capability_id, record_family_id)`, with endpoint row counts and
content/schema fingerprints. Record diff emits only changed logical records,
ordered by `(scope, record_family_id, record_uuid, change_kind)`. Identity is
the canonical public UUID defined by the owning participant; participants
without row UUIDs use their canonical record fingerprint. Values and internal
surrogate IDs are never embedded in the diff envelope.

The exact `checkpoint_summary_diff@1` and `checkpoint_record_diff@1` Arrow
schemas, sort keys, and enum spellings are frozen in the machine-readable
contract. `added` means only `to` has the identity, `removed` means only `from`
has it, and `modified` means both exist with different canonical record
fingerprints. Physical Parquet layout, row groups, chunking, dictionary
encoding, and cache state cannot affect output.

Record detail is driven by the closed `checkpoint-diff-adapter/1` registry,
never by guessing a column whose name ends in `_uuid` and never by hashing
physical participant bytes. Each authoritative participant owner registers the
exact `(capability_id, capability_version, record_family_id, record_version,
encoding)` tuple together with its canonical logical decoder; an ordered,
non-empty identity-field list; whether that identity projects to the nullable
`record_uuid`; its canonical identity-fingerprint doma; and its canonical
whole-record fingerprint domain. The adapter validates the committed schema
fingerprint before decoding. An unregistered, duplicate, version-mismatched, or
schema-mismatched participant makes record detail fail structurally; it is not
silently reduced to a summary row. Fingerprints are over owner-canonical logical
values after dictionary decoding and canonical row ordering, so physical
chunking and JSON formatting cannot affect them. The checked-in adapter
inventory and omission test cover every participant that can appear in a v0.5
complete workspace.

For the reserved `CURRENT` selector, the non-null endpoint identity field is a
synthetic UUIDv8 derived from
`SHA-256("graphforge-current-checkpoint-endpoint/1" || generation_uuid_bytes ||
generation_manifest_sha256)`. It identifies only that immutable diff endpoint;
it is not a checkpoint reference and is never accepted by `open_checkpoint`.

Default page size is 100 and the range is 1–10,000. Tokens bind API version,
method, endpoint checkpoint UUIDs and manifest digests, scope, limit, last
complete sort tuple, and integrity digest. A token with changed inputs is
`GF_PAGE_INVALID`. Because both endpoint leases live for the call only, a later
page reopens the named checkpoints; deletion or replacement yields
`GF_PAGE_SNAPSHOT_GONE`, never silent advancement. Cancellation is checked
before each participant and at least every 4,096 decoded records and returns no
partial table.

### Public operations and parity

Rust owns all semantics. The exact request fields and return schemas are frozen
in [`checkpoint-api-v1.json`](../contracts/checkpoint-api-v1.json). The public
surface is:

Every public `idempotency_key` is a canonical UUID and becomes the operation
UUID in Rust. Python accepts `uuid.UUID` or its canonical string; Node and CLI
accept only the canonical string. Empty, noncanonical, or arbitrary text keys
return `GF_VALIDATION`. `actor_uuid` is optional. Create `description` is
optional; every other request field shown by the contract is required except
pagination cursor and cancellation.

The frozen Rust request shapes are:

```rust
pub struct CheckpointRequest {
    pub name: String,
    pub description: Option<String>,
    pub idempotency_key: OperationId,
    pub actor_uuid: Option<uuid::Uuid>,
}
pub struct ListCheckpointsRequest {
    pub page: PageRequest,
}
pub struct DeleteCheckpointRequest {
    pub name: String,
    pub idempotency_key: OperationId,
    pub actor_uuid: Option<uuid::Uuid>,
}
pub struct RevertCheckpointRequest {
    pub name: String,
    pub reason: String,
    pub idempotency_key: OperationId,
    pub actor_uuid: Option<uuid::Uuid>,
}
pub enum CheckpointSelector {
    Named(String),
    Current,
}
pub enum CheckpointDiffScope {
    Summary,
    Graph,
    Ontology,
    Configuration,
    Capabilities,
    Provenance,
    Knowledge,
    Epistemic,
    All,
}
pub enum CheckpointDiffDetail {
    Summary,
    Records,
}
pub struct DiffCheckpointsRequest {
    pub from: CheckpointSelector,
    pub to: CheckpointSelector,
    pub scope: CheckpointDiffScope,
    pub detail: CheckpointDiffDetail,
    pub page: PageRequest,
}
```

```rust
impl GraphForge {
    pub fn checkpoint(&self, request: CheckpointRequest)
        -> Result<ExecutionResult, GfError>;
    pub fn list_checkpoints(&self, request: ListCheckpointsRequest)
        -> Result<ExecutionResult, GfError>;
    pub fn open_checkpoint(&self, name: &str)
        -> Result<CheckpointView, GfError>;
    pub fn revert_to_checkpoint(&mut self, request: RevertCheckpointRequest)
        -> Result<ExecutionResult, GfError>;
    pub fn delete_checkpoint(&self, request: DeleteCheckpointRequest)
        -> Result<ExecutionResult, GfError>;
    pub fn diff_checkpoints(&self, request: DiffCheckpointsRequest)
        -> Result<ExecutionResult, GfError>;
}
```

A successful revert return is also the live-facade commit boundary:
`GraphForge` has atomically replaced its resolved generation, hydrated graph
workspace, ontology/configuration, runtime catalog, and generation-scoped
caches. Reads made through that same instance after return observe the restored
generation. Returning success while the facade still exposes the pre-revert
workspace is forbidden.

Python returns `pyarrow.Table`; Node returns Arrow IPC; CLI writes the same
Arrow records through the existing output formatter. Bindings accept the same
canonical UUIDs, names, limits, cursors, cancellation, and idempotency inputs.
They do not implement registry, diff, revert, validation, or schema logic.

CLI commands are `gf checkpoint create|list|show|open|delete|diff` plus
top-level `gf revert` (`gf checkpoint revert` follows the same Rust path).
`show` returns the authoritative named metadata record. `open` accepts a read
command after `--`; it remains the query surface and does not start a mutable
shell. `revert --preview` resolves the source and current generation without
mutation. Destructive CLI revert requires `--reason`, an idempotency key, and
explicit `--yes`; omission fails closed instead of prompting. Embedded callers
remain deterministic and use the typed engine request directly.

Graph-only and Cypher reads through a view retain ADR 0012 isolation: they open
only graph participants. analyst-verb/find opens graph plus their registered primary
artifacts; knowledge-layer and epistemic methods alone open their owning participants. Being part
of one complete checkpoint does not authorize broad eager hydration.

### Structured errors

These additions are closed for contract version 1:

| Code | Meaning |
| -------------------------------- | -------------------------------------------- |
| `GF_CHECKPOINT_EXISTS` | active normalized name already exists |
| `GF_CHECKPOINT_NOT_FOUND` | active normalized name does not exist |
| `GF_CHECKPOINT_REGISTRY_CORRUPT` | registry/checksum is unsafe or inconsistent |
| `GF_READ_ONLY_VIEW` | mutation attempted through a checkpoint view |

Existing `GF_VALIDATION`, `GF_IDEMPOTENCY_CONFLICT`, `GF_WRITER_BUSY`,
`GF_RESOURCE_LIMIT`, `GF_CANCELLED`, `GF_PAGE_INVALID`,
`GF_PAGE_SNAPSHOT_GONE`, `GF_PROJECT_CORRUPT`, `GF_TRANSACTION_FAILED`, and
`GF_IO` retain their frozen meanings. Write errors carry `committed`; read
errors do not. Safe details may contain operation, phase, bounded normalized
name, UUIDs, counts, limits, and fingerprints. They never contain participant
values, reason or description text, credentials, unrestricted paths, or raw
registry bytes.

### Named proof obligations

Each implementation issue must map these edges to deterministic tests:

| Edge | Required test or failpoint |
| --------------------------------------- | ----------------------------------------------------------------------------- |
| registry first create and replacement | `checkpoint.registry.before_replace`, `checkpoint.registry.after_replace` |
| registry file or directory flush | `checkpoint.registry.after_file_fsync`, `checkpoint.registry.after_dir_fsync` |
| checksum/registry disagreement | `checkpoint_registry_corruption_fails_closed` |
| name containment and normalization | `checkpoint_names_are_content_not_paths` |
| create/delete replay and conflict | `checkpoint_idempotency_matrix` |
| active pin across cleanup | `active_checkpoint_is_retention_root` |
| delete while view is open | `deleted_checkpoint_keeps_open_lease` |
| restart resolution | `checkpoint_reopens_exact_generation` |
| read-only mutation rejection | `checkpoint_view_rejects_every_mutator` |
| revert before/after each ADR 0013 phase | `revert_publication_failpoint_matrix` |
| complete participant restoration | `revert_restores_complete_workspace` |
| no dangling cross-domain references | `revert_revalidates_composite_snapshot` |
| logical diff independent of encoding | `checkpoint_diff_ignores_physical_layout` |
| diff pagination and endpoint deletion | `checkpoint_diff_cursor_matrix` |
| cancellation | `checkpoint_diff_cancellation_has_no_partial_output` |
| Rust/Python/Node/CLI parity | `checkpoint_surface_parity` |

The crash tests terminate subprocesses; they do not simulate durability with
in-process mocks. Each failpoint is exercised for an uninitialized project and
a project with later graph, ontology, knowledge-layer, and epistemic changes. Reopen evidence
asserts registry revision and digest, selected generation and manifest digest,
participant fingerprints, restoration row, and visible logical rows.

## Consequences

- Named history is bounded, explicit, and independent of directory discovery.
- Revert preserves a linear immutable generation history.
- Historical reads are repeatable and cannot mutate authority.
- Ontology and configuration can no longer live as mutable authority outside a
  generation.
- Logical diff may be expensive, but it is bounded and cancellable.
- Registry corruption prevents cleanup until repaired; preserving reachable
  data takes precedence over availability.

## Rejected alternatives

| Alternative | Reason |
| ----------------------------------- | --------------------------------------------------------- |
| Set `CURRENT` to the old generation | Rewrites history and loses restoration provenance. |
| Use checkpoint names as directories | Makes caller text a path and complicates normalization. |
| Retain every generation | Unbounded storage growth and no explicit policy. |
| Delete a generation with its name | Invalidates active readers and races cleanup. |
| Diff Parquet bytes | Physical encoding is not a logical change. |
| Support graph-only revert | Can publish dangling knowledge/epistemic or ontology references. |
| Expose arbitrary generation open | Bypasses named retention and creates unsafe hidden roots. |
| Add transaction rollback | Different scope and lifecycle from committed history. |
| Migrate pre-v1 formats | GraphForge has no pre-v1 compatibility requirement. |
