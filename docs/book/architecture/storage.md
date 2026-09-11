# Storage Architecture

See the [permanent storage assessment](permanent-storage-assessment.md) for
source-backed ownership baselines, lossless codec experiments and regression budgets.

**Status:** v0.5.0 — Parquet project storage shipped
**Last Updated:** 2026-08-30

---

## Overview

The native directory authority, lifecycle policy, and adversarial checks are
documented in [Directory capabilities](directory-capabilities.md).

`graphforge-storage` owns project generations and participants, admission and
recovery, Parquet catalogs and write paths, derived indexes, and
portable interchange. Runtime scans use `GraphCatalog` and DataFusion
`TableProvider` implementations over the selected project's data. Query-language
semantics remain in the compiler and execution layers.

Shared logical and physical Arrow schema definitions live in `graphforge-ir::arrow_schema`; storage reexports the existing public schema names. Relational compilation consumes neutral schema data and has no production storage dependency. Execution owns path hydration through retained catalog providers.

The unused `StorageProvider`, `StorageRow`, and `ParquetProvider` stubs were removed in #1006. They had no consumers and their scan path returned `NotImplemented`; they never provided backend selection.

---

## Value contract and current dependencies

`graphforge-storage` currently depends on both `graphforge-ir` and
`graphforge-ontology` in production. Its writer and GFDR journal consume
`IrLiteral`; catalog and topology paths consume IR's runtime catalog and tagged
IDs; ontology/composition adapters supply semantic binding validation. Storage
is therefore not yet independent of compiler-owned value definitions.

[ADR 0025](../../adr/0025-storage-value-contract.md) chooses `graphforge-value`
for the shared value/ID/catalog encoding contract; extraction is pending in
#1011 and #1012. Storage retains physical schemas, I/O, and project admission.
Moving definitions must preserve existing bytes and public Arrow layouts.
`graphforge.ir_version` metadata is descriptive and does not admit a project:
container, participant and value compatibility use their own checked contracts.
See [project format compatibility](project-format-compatibility.md).

## Parquet persistence

Parquet is the graph data format used by the Rust storage implementation. It:

- Stores graph tables and opaque domain-owned participants as columnar Parquet
  files; `graphforge-storage` does not define provenance or knowledge semantics
- Carries GraphForge metadata at the file level (ontology version, IR version, query ID)
- Persists derived compiled ontology runtime tables for rapid startup; the
  CURRENT-selected workspace's canonical ontology JSON remains durable
  authority

Parquet file-level metadata:

```
graphforge.dataset_kind      = "topology_nodes"
graphforge.ontology_version  = "core-2026.05"
graphforge.writer_version    = "0.5.0"
graphforge.ir_version        = "0.1.0"
graphforge.query_id          = "01J..."
graphforge.provenance_policy = "conservative_min"
```

Project metadata and manifests use JSON. A future storage backend would need to satisfy the actual generation, admission, publication, and scan contracts; the removed `StorageProvider` stub did not establish that support.

---

## Identity and Surrogate Keys

GraphForge uses a **dual-key pattern** for all first-class objects:

| Key | Type | Purpose |
|---|---|---|
| **UUID** (`*_uuid`) | `FixedSizeBinary(16)` — UUIDv7 | Canonical stable identity. Globally unique. Immutable. Survives project merges, offline generation, and cross-analyst exchanges. |
| **Surrogate** (`*_id`) | `UInt64` | Execution-time optimization. Assigned at ingest/load time. Used for DataFusion join operations. Never exposed in public API results. |

### Why UUIDv7

UUIDv7 (RFC 9562) is time-ordered within a millisecond, globally unique without coordination, fits in Arrow `FixedSizeBinary(16)`, and supports offline generation on mobile devices or air-gapped systems. See [refactor-v0.5.md §5](refactor-v0.5.md) for the full rationale.

UUID byte order, accepted text form, content-derived UUIDv8 records, canonical
Arrow bytes, and domain-separated SHA-256 fingerprints follow the frozen
[canonical fingerprint v1 contract](canonical-fingerprints-v1.md).

### UUID→Surrogate mapping

The relational lowering layer maps `node_uuid → node_id` once at scan time. All DataFusion join operators use integer surrogates (`node_id`, `edge_id`, `src_id`, `dst_id`) for performance. Results project back to UUID columns before returning to the caller.

**Rule:** UUIDs appear in every public API result schema. Surrogates are execution-internal and must never appear in API outputs.

The v4 reverse authority makes `node_id → node_uuid` a generation-pinned,
disk-bound read rather than a graph-sized process cache. Its manifest names
typed immutable artifacts: packed UUID payloads for contiguous ordinal ranges,
UUID-sorted `(UUID, surrogate)` forward records, plus sorted surrogate
tombstones. Admission authenticates each retained file
in one streaming pass that jointly derives its whole-file digest, block fences,
structural checks, and mapping commitment. Aggregate admission counters report
the exact artifact bytes, sequential read calls, and peak bounded buffer. A
lookup accepts only
a bounded request batch, sorts and deduplicates it, applies newest tombstones,
and reads authenticated fixed-size ordinal blocks while
restoring caller order. Storage grows with retained identities rather than the
largest sparse surrogate, and anonymous buffers do not grow with graph
cardinality. Version-3 state is an explicit rebuild-required disposition; a v4
reader never interprets or mixes v3 reverse records.

Fixed-hop consumers such as projection-aware expansion retain one authenticated
generation handle and submit bounded destination-ID batches to it. They do not
reopen or reauthenticate the full authority for each chunk. The lookup restores
caller order while exposing sanitized requested/unique/found, selected-range,
logical-byte, coalesced-call, and peak-buffer evidence. Typed failure evidence
counts authentication failure without containing graph identities or paths.
The explicit migration disposition is `CanonicalTopology`: rebuild scans
canonical topology under the durable rewrite and never treats a v3 reverse run
as v4 authority. Its temporary-byte peak covers every coexisting external-sort
run, merge output, surrogate projection, and final artifact through
storage-owned lifecycle accounting, including retained staging copies and
control-record temporaries. It is not an artifact-only approximation or a
recursive observation of the active scratch tree.

Admission also streams both forward and ordinal authorities through the same
domain-separated mapping commitment and count. Strict forward UUID ordering and
that bounded reconciliation reject duplicate, missing, or cross-generation
identity mappings without constructing a graph-sized in-memory map.

The reader holds `ordinal-v4.lock` shared only during admission, then releases
it after pinning the immutable files; every v4 publisher holds that same stable
file exclusively across artifact installation and manifest replacement. Each
admitted file's stable identity, length, and
high-resolution modification time are retained as a fast change detector.
That cooperative ownership is not the cryptographic boundary: every selected
ordinal or tombstone block is verified against its manifest digest before its
bytes can produce a result. A non-cooperating mutation therefore fails closed
even if it preserves the inode and restores the original modification time.
Adjacent selected ordinal blocks may share one bounded read only when their
intervening gap and combined span fit the configured cap; every constituent
slice is still verified against its own digest. Version-3 rebuild recognition
uses the existing canonical v3 manifest and run-descriptor validator, so a
minimal version tag or a v3 document mixed with v4 fields fails closed.

### Objects requiring UUID identity

| Object | UUID column |
|---|---|
| Node (entity) | `node_uuid` |
| Edge (relationship) | `edge_uuid` |
| Document | `doc_uuid` |
| Provenance event | `provenance_uuid` |
| Analyst/User | `analyst_uuid` |
| Project | `project_uuid` |
| Workflow | `workflow_uuid` |
| Embedding | `embedding_uuid` |
| Source reference | `source_uuid` |
| Ranking output row | `rank_uuid` |
| Clustering output row | `cluster_uuid` |
| Generated artifact | `artifact_uuid` |

---

## Storage Layout

GraphForge is pre-v1 and does not support older project formats. The normative
[pre-v1 compatibility policy](project-format-compatibility.md) permits only the
v0.5 project/container contract and rejects historical inputs without mutation.

GraphForge organises a project as immutable, complete generations. `CURRENT` is
the only publication authority; graph, provenance, and knowledge participants
become visible together through one atomic pointer replacement. The normative
layout, fsync order, reader leases, recovery rules, and failpoints are frozen in
[ADR 0013](../../adr/0013-project-generation-protocol.md). The public
acknowledged-durable boundary and isolation honesty rules are frozen in
[ADR 0018](../../adr/0018-acknowledged-durability-isolation.md).

Named checkpoints, lease-pinned historical reads, logical diff, and
complete-workspace revert are defined by
[ADR 0014](../../adr/0014-workspace-checkpoints.md). A revert publishes a new
complete generation; it never moves `CURRENT` backward. Active checkpoint
references add explicit retention roots, while deletion releases only that
root and cannot invalidate an already leased reader.

```
project/
├── FORMAT
├── CURRENT             # sole committed-generation pointer
├── locks/writer.lock
├── transactions/
├── generations/
│   └── <generation-uuid>/
│       ├── lease.lock
│       ├── manifest.json
│       ├── graph/              # file-backed graph workspace (optional; with graph/files)
│       │   └── deltas/         # authoritative mutation runs (ADR 0019; not adjacency)
│       └── participants/
│           ├── graph/...       # snapshot.arrow (legacy) or files.json (inventory)
│           ├── workspace/
│           │   ├── configuration.json
│           │   └── ontology.json
│           ├── provenance/...
│           └── knowledge/...
├── cache/               # derived and source-fingerprint keyed
└── trash/
```

A minimal committed generation declares `graph@1` and `workspace@1`.
`workspace@1` contains canonical JSON records for explicit ontology absence (or
an adopted advisory/strict ontology) and authoritative registered project
configuration. Project open validates these records before opening graph
data. New publications store graph workspace files under the generation-owned
`graph/` tree with a `graph`/`files` inventory participant; legacy
`graph`/`snapshot` Arrow envelopes remain readable. Root YAML/JSON and
environment settings are inputs only and cannot override the selected
generation. Version 2 of `graph`/`files` replaces the expanded per-generation
inventory with a compact authenticated Patricia/radix root. Immutable payload and
manifest objects are addressed by SHA-256 in the project object store, so an
update writes only changed payloads and the compressed path-copy root path while
unchanged objects are reused byte-for-byte. A storage-owned root-bound state
authenticates an existing inventory once per publication sequence; individual
updates cannot substitute a caller-owned cache and do not rescan all prior
descriptors. Open resolves the bounded manifest and verifies every selected
payload before exposing the generation. Ordinary API open and restore dispatch
on the declared participant version. Expanded inventories use version 1 for
legacy raw routes and version 3 for mapped routes; compact CAS roots use
version 2 for raw routes and version 4 for mapped routes. The descriptor and
payload must agree. Immutable generations remain unchanged while a writable
facade materializes an authenticated private workspace.

### Semantic routes and portable filenames

Node labels and edge relations are semantic UTF-8 identifiers, not filesystem
components. Rust owns their translation at every node-property, edge-property,
and topology-relation path boundary. A physical route component is `r-` followed
by the 64 lowercase hexadecimal digits of SHA-256 over the exact semantic UTF-8
bytes. Its length is always 66 bytes, including for long semantic names.

The graph-owned `semantic-routes.json` control record reverses each component to
its exact semantic route. It belongs to the authenticated graph-files inventory
and travels with publication and portable export/import. Decoding verifies the
table digest, canonical serialization, unique entries, component derivation,
and resource budgets. Every physical route must resolve through the table;
unreferenced table entries are rejected. A digest collision between distinct
semantic routes is a typed refusal, never an overwrite. The table is bounded to
64 MiB and 100,000 entries; exceeding a budget fails explicitly.

No case folding or Unicode normalization changes a semantic identifier. Names
such as `CON`, `AUX`, trailing-dot names, normalization variants, and literal
`r-` prefixes retain their original meaning in public results and metadata.
Readers use authenticated layout authority, never filename-prefix guessing.
Legacy versions 1 and 2 cannot contain the reserved mapping record; mapped
versions 3 and 4 require it. Older readers reject the new versions rather than
interpreting an encoded component as a semantic name.

Legacy migration operates only in an owned private workspace. It authenticates
source identity and bytes, stages the encoded files and complete mapping in the
same durable rewrite, and recovers that rewrite before reading route authority.
Physical migration preserves graph UUIDs, ranks, values, and semantic generation
counters. An authenticated legacy publication must be translated while copying
into the private workspace, so Windows never needs to create an intermediate
raw reserved filename. Published trees and CAS objects remain immutable.

Mapping updates share the graph rewrite transaction. Writers authenticate the
current mapping under the rewrite lock before extending it, and transformations
that remove or rename physical routes rebuild the mapping from their emitted
inventory. Path containment, no-follow opens, native identity, link-count,
collision, and exact-inventory checks still apply to encoded paths.

Construction checkpoint versions 8 and 9 select mapped output explicitly. Version
9 also reclaims accepted payloads after authenticated shape and shaped payloads
after authenticated encoding; see [construction supersession](construction-supersession.md).
Versions 6
and 7 retain their original encoding when resumed; versions 7–9 use the
same compact detail codec. Mapped construction publication combines the
retained parent routes with newly emitted routes and verifies the complete
mapping before installing the version-4 manifest. Legacy parent payloads can
retain their authenticated CAS objects while their logical route paths change.

Read sessions retain one admitted route inventory with their catalog and
adjacency provider. Publishing a later generation replaces the facade's
provider; an existing lazy stream keeps its original inventory and private
index artifacts through completion.

### Immutable property snapshots

Node and edge properties use `full-snapshot-v1` fragments under
`properties/<component>/<generation>-<ordinal>.parquet` (fixed-width decimal identity) and the corresponding
`edge_properties` tree. Each row is the complete property state for one UUID;
an explicit tombstone deletes the whole row. Admission authenticates every
canonical generation and ordinal named by the committed graph-files inventory.
Readers merge all authenticated fragments by UUID and descending
`(generation, ordinal)` authority; the first row for a UUID is its complete
current map or tombstone. Unchanged UUIDs remain authoritative in older
immutable fragments, while a newer tombstone prevents a deleted UUID from
resurfacing. A write window composes repeated SET/REMOVE operations once and
publishes only its changed UUID snapshots without a full-route decode or any
prior-fragment rewrite. A PATCH/REMOVE producer performs at most one
authenticated targeted batch lookup for the window's UUID set, with the same
zero-per-record-seek scanner; sealing consumes those complete staged rows and
does not read historical fragments again.

Each fragment also carries the authenticated `graphforge-property-live-schema/1`
route summary: an exact live-UUID count for every currently present property
key. Mutation preparation updates those counts from the targeted UUIDs' old
complete maps to their new complete maps; work is bounded by the mutation
window and schema width, never total rows or fragment history. Repeated writes
in one window consume the already-staged summary. The newest fragment summary
is the logical schema authority: keys with a positive count retain their
authenticated physical type, while historical keys whose count reached zero
surface as Arrow `Null`. Physical historical schemas remain available for
decoding older UUID snapshots. Malformed counts, underflow/overflow, a live key
without an authenticated physical field, or conflicting semantic/type metadata
fail closed. Legacy routes without this summary retain their historical schema
union until rewritten by an explicit migration; targeted writes never invent
counts for untouched UUIDs.

Readers derive route authority from the committed graph-files inventory,
validate canonical fragment identity, schema/semantic metadata, strictly sorted
unique non-null UUIDs, and tombstone invariants, then perform a bounded
disk-backed newest-wins merge. SQL and direct APIs share that scanner. SQL emits
bounded Arrow batches; `LIMIT` changes emission only after authority validation.
Decoded rows enter fallible bounded scratch runs. The final emitting merge does
not begin until every authenticated fragment reaches clean EOF and all page,
Arrow, row-order, tombstone, and live-byte checks succeed. A late decoder or
resource failure therefore discards the runs and returns a typed error with zero
rows observed by direct callbacks or DataFusion—even for a projected `LIMIT 1`
plan.
Admission retains the stable root capability plus each fragment's authenticated
path, native file identity, length, digest, and schema—not one OS handle per
historical fragment. A scan opens fragments on demand without following links,
requires the admitted device/file identity, and rehashes the complete file
while streaming those exact bytes into an exclusively created, unnamed scratch
file. Identity, length, and digest must match before Parquet sees the scratch
handle; full, targeted, and SQL readers never decode the mutable source handle.
The source handle then closes. Consequently live
fragment handles are bounded by `max_open_runs` rather than total history, and
same-name replacement, transient in-place mutation (even if restored), symlink,
scratch planting, and path substitution all fail closed. Parquet page headers
are then parsed through a bounded
compact-protocol reader before Arrow allocation. Declared
compressed/uncompressed sizes must remain within the authenticated chunk/file
ranges and the configured live-byte limit. One shared budget covers Arrow
batches, decoded rows, spill buffers, and merge cursors; rolling fan-in levels
keep run references logarithmic and unlink merged inputs immediately.
Operational evidence separates raw graph-files authority authentication from
retained property-fragment authentication. Each has distinct byte totals,
64 KiB block-equivalents (`ceil(file_bytes / 64 KiB)` per file), and actual
non-empty read-call counters; block-equivalents are not read calls. Aggregate
authentication bytes, equivalents, and calls equal their authority plus
property components. `physical_blocks` is an actual-operation count: authentication
read calls plus decoder read calls. Evidence also distinguishes validation and
selected-value decoder bytes/calls, authenticated scratch bytes written, and the
largest single-snapshot coexistence peak admitted against live scratch free
space, plus range seeks and physical row-decode visits (a
row decoded by validation and selected-value passes contributes once to each
pass), shadowed rows, fragments and row groups considered/selected, and the
shared live-byte peak. External-merge evidence reports first-level encoded
spool input separately from total spill bytes, runs, and passes so amplification
is a checked ratio rather than a worst-case row-size estimate. The invariant
`per_record_seeks = 0` remains exact.

Property-only commits reserve a checked monotonic property generation under the
durable-rewrite lock. Legacy state initializes it from the maximum topology and
search generation. Portable fingerprints consume the logical overlay once per
route, so immutable and flattened projections have the same semantic identity.

Pure CAS reads open the existing `graph-objects/sha256` namespace and existing
`lifecycle.lock` with read-only capabilities. They create neither lifecycle
state nor `tmp`/`active`. The authenticated regular `lifecycle.lock` is the
cross-platform coordination authority: reads and publications hold it shared,
while collection holds it exclusively. On Unix, the same operations also lock
the retained `graph-objects` directory so replacing the still-open lifecycle
pathname cannot split cooperative coordination. Windows instead relies on the
retained lifecycle handle's delete/rename denial because directory handles do
not support byte-range locking. Post-lock identity and link validation closes
namespace substitution races on both platforms. Publication,
materialization, lease cleanup, and GC use the distinct mutable open-or-create
capability; materialization remains there because installing hard links mutates
the source inode's link state.

The node-v3 canonical shape uses bounded buckets of one to eight exact-path
entries. Maximal lowercase-hex SHA-256 prefixes are compressed into nodes;
branches have at least two distinct nibble children. Empty inventory is the
sole one-node empty-branch exception. Each entry authenticates its logical
path, byte length, role and payload SHA-256. Bucket order, unique paths and the
entire ancestral hash route are checked, including when a targeted lookup is
absent. For `F > 0` entries, the structural bound remains `2F - 1` nodes.
Node v1/v2 and mixed/future formats are refused; this node-format change does
not change the separate graph-files root version and adds no compatibility
reader or migration machinery.

Every production manifest-object read admits at most 256 KiB before allocating
the encoded buffer. Decoding admits at most eight entries, 4096 UTF-8 bytes per
path, 64 bytes per digest, and sixteen branch children. The explicit field
visitor avoids an unbounded flattened JSON intermediate and rejects the ninth
entry. A 64-KiB decoded representation charge covers the node, bounded entry or
child slots and their string contents. This is a logical charge, **not a hard
native-memory bound**: parser scratch, canonical re-encoding, allocator overhead,
read caches, and retained copy-on-write ancestors can overlap. Process RSS and
filesystem peaks must be measured separately; route-table payload limits are
independent of these manifest-node limits.

Insertion into a bucket is local; a ninth entry partitions at its first
SHA-256 divergence into at most seventeen nodes. An unsplittable ninth
full-digest collision is refused. Replacement copies only the selected path.
Deletion removes empty nodes, collapses unary branches, and coalesces a small
subtree using a probe limited to sixteen nodes and eight entries. A lower bound
from pending nonempty children stops oversized probes without scanning the
subtree. Published roots and payloads remain immutable, so retained generations
and active snapshots keep their exact authenticated inventory. The 128/256/512
entry regression ladder bounds representative successful and absent lookups and
replacements to four reads, and checks deletion work separately. These are
application I/O counters, not OS I/O or native memory measurements.

Authoritative small-write delta runs, when present, live under
`graph/deltas/` inside the same generation and are inventory-verified
([ADR 0019](../../adr/0019-authoritative-graph-delta-journal.md)). GFDR's
binary framing and JSON payload schema are a permanent, versioned exception to
the default Parquet graph-data rule; see
[ADR 0024](../../adr/0024-storage-format-exceptions.md). Compaction
and ordinary opens decode the compact base from these canonical Parquet files;
there is no duplicate JSON graph-state authority. A delta-bearing open verifies
the contiguous typed GFDR chain, materializes a contained private Parquet view,
and exposes that view only after replay succeeds within its declared limits.
Checkpoint views use the same path, so a checkpoint remains pinned to its exact
generation. Routing stems and canonical openCypher value types are retained;
routing-free or string-only prototype GFDR payloads fail with
`GF_UNSUPPORTED_PROJECT_FORMAT` rather than being guessed. Compaction
folds a verified contiguous prefix back into canonical Parquet via a new
immutable generation (`compact_graph_delta`) and reclaims unreachable inputs
only through the shared retention/GC oracle. They are
distinct from rebuildable `indexes/adjacency/deltas/` accelerators.

GFDR admits only node/edge property set and removal. All node/edge topology
upserts and deletes, including records with full-width identity metadata, fail
with `GF_UNSUPPORTED_PROJECT_FORMAT`. Canonical publication owns topology.
Admission checks precede preparation and transaction-retry shortcuts; decoding
rejects persisted topology records, and direct in-memory replay prevalidates the
complete input before changing caller state. Internal overlay writers refuse
topology before creating target authority. No backward reader or migration is
provided. See the [current publishing contract](#current-publishing-contract).

Replay materializes only UUIDs touched by property operations; unchanged UUIDs remain in prior immutable fragments. During overlay
construction the memory ceiling charges decoded runs, idempotency payloads,
typed operation values, and the overlay simultaneously. Runs are released
before materialization. Materialization then charges the retained overlay,
node endpoint/identity authority, target references, baseline and output rows,
Arrow arrays, and schema-width × row-group column metadata plus the active
Parquet writer buffer. Replay writers use the shared permanent Zstd policy, retain their justified
dictionary-off setting, and bound row groups by `max_batch_rows`. Flat
generation-zero properties enter this same authenticated, sparse-fragment
materialization path. `max_records_per_run` and `max_work_rows` independently
bound mutation and physical work. Limit failures use the typed
`GF_RESOURCE_LIMIT` code. Removing an absent key or setting an identical value
is a no-op and creates no new property fragment.

For explicit bounded composite property set/remove requests, the Rust facade
selects GFDR before mutating its private workspace. Storage prepares an owning
child graph tree but cannot publish it independently; the facade combines its
authenticated `graph/files` participant with every unchanged or updated parent
participant and stages one complete generation. Creates, deletes, Cypher,
bulk/algorithm writes, optimistic multi-writer requests, unsupported values,
and journal capacity exhaustion select canonical full-Parquet publication
before staging. Bindings and the CLI do not implement a second routing engine.

Optional capability absence is recorded in the generation manifest; it is not
inferred by scanning folders. Graph-only readers validate the mandatory
workspace control records and graph participants but never open provenance,
knowledge, or epistemic tables. Semantic table ownership remains with the
domain crates defined by
[ADR 0012](../../adr/0012-knowledge-domain-ownership.md).

#### Mutable topology rewrite recovery

Before a graph workspace becomes an immutable project participant, topology,
property, search, and index maintenance may replace several fixed-path files.
Those files advance through one authenticated durable rewrite, never through a
sequence of independently committed renames. The engine retains the admitted
project-root identity and each destination parent directory, holds the named
rewrite lock exclusively, and binds every staged/final relative path,
temporary-file identity, exact length, and SHA-256 digest in a checksummed
intent. Paths must be canonical descendants; substitution, traversal,
duplicate names, and cross-root state fail closed, while the named rewrite lock
also requires one link.

A hydrated destination may be a read-only hardlink to a published CAS payload.
After authenticating its prior identity and contents, the rewrite uses an
explicit shared-destination replacement capability. The temporary source must
remain a private single-link regular file; the replacement checks both expected
identities and preserves the old inode, bytes, attributes, and ordinary query
readers. Windows retained authentication guards still deny deletion and must
be released before replacement; their sharing protections remain unchanged.
On Windows this path alone uses `FILE_RENAME_IGNORE_READONLY_ATTRIBUTE`; it
never clears attributes shared with CAS aliases. An absent prior destination
uses no-replace installation. Ordinary filesystem replacement stays strict.

The intent is bounded to 16,384 entries and 8 MiB. Its sole generation-authority
entry is `topology/generation.json`, whose JSON is bounded to 4 KiB and must
encode the exact next topology/search pair. Data files are installed and
authenticated first, directory namespace barriers are completed, retained
root/lock identities are revalidated, and generation authority is installed
last. Only then is the intent removed durably. This makes an existing matching
destination an idempotent completed step while refusing a missing or changed
temporary instead of accepting a partial batch.

An interrupted `preparing` intent cleans up only identity-matched retained
temporaries. An interrupted `durable` intent always rolls forward from either
the exact prior or exact next generation; any other generation state is
corruption. The #931 UUID-to-surrogate index must participate through a typed
auxiliary receipt that names and authenticates one exact staged receipt entry,
so topology shards and index authority recover atomically. This internal
topology/search generation is not project publication authority: a recovered
workspace is still invisible to new project readers until the complete
generation is selected by `CURRENT`.

Unless a root is shown explicitly, graph paths in the sections below are
relative to the pinned generation's `participants/graph/`; primary workbench
paths are relative to `participants/workbench/`; derived index paths are
relative to root `cache/`.

### `embeddings/` — primary vector generations

Unlike `indexes/`, caller-, algorithm-, or provider-produced vectors are
primary workbench data and are never reconstructed or discarded as a cache.
Names do not enter paths; compatibility and generation SHA-256 digests do:

```text
embeddings/
├── aliases.json                         # display name -> compatibility digest
└── spaces/<compatibility-sha256>/
    ├── space.json                       # compatibility descriptor + refresh policy
    ├── active.json                      # checksummed active generation pointer
    └── generations/<generation-sha256>/
        ├── vectors.parquet              # node_uuid + FixedSizeList<Float32, N>
        └── manifest.json                # source fingerprint, counts, digests, state inputs
```

Builders use a collision-resistant private sibling directory, validate the
complete UUID/vector batch, write and fsync vectors, write the checksummed
manifest last, fsync the tree, then atomically replace `active.json`. The prior
active generation remains visible until that final pointer swap. Incomplete
private trees are ignored and recoverably removed on open. Alias replacement is
separate from generation publication, so an incompatible producer cannot take
over a name accidentally.

Every open recomputes `fresh`, `stale`, `substantially_stale`, `incompatible`,
or `corrupt` from the persisted descriptor/source fingerprint and current graph
metadata. The exact identity fields, mutation thresholds, forced-stale boundary,
retention, refresh coalescing, and provider privacy rules are normative in
[Embedding v1](embedding-v1.md#embedding-space-publication). Deleting a node or
removing its selected label makes it ineligible immediately; corrupt or
incompatible bytes always fail closed. Credentials, raw input text, provider
payloads, and knowledge-layer fields are never stored here.

---

## Derived Indexes

The `indexes/` folder holds **derived, rebuildable acceleration structures**. Nothing here is
canonical: every file under `indexes/` can be reconstructed from `topology/` (and, for FTS,
`properties/`) alone. An absent index is not an error — it means the accelerator has not been
built yet, and the engine falls back to building in memory on demand. See
[ADR 0004](../../adr/0004-adjacency-index.md).

### `indexes/adjacency/` — graph-native adjacency index

The adjacency index is a derived CSR (compressed sparse row) representation of the topology,
used by both the Cypher traversal path (variable-length `Expand`) and the analyst verbs
(`rank`/`cluster`/`paths`/`analyze`/`similar`). It is **optional**: absent ⇒ build in memory
on demand (today's behavior); present ⇒ load from disk. It is surrogate-keyed and never
changes results — only speed.

```
indexes/
└── adjacency/
    ├── index_manifest.parquet
    ├── WORKS_AT.out.csr.json       # versioned shard-set manifest
    ├── WORKS_AT.out.csr.shards-<digest>.d/
    │   ├── 00000000000000000000.csr
    │   └── ...
    ├── WORKS_AT.in.csr.json
    ├── OWNS.{out,in}.csr.json
    └── _all.{out,in}.csr.json      # union across relation types
```

The builder (`graphforge_storage::adjacency::build_adjacency_index`) writes one `{out, in}` pair per
relation type plus the `_all` union pair, then the manifest **last**. Relation names unusable
as file stems (path separators, `..`, the reserved `_all`) are skipped — those relations are
served by scan-build, but their rows still flow into the union index. The
manifest is stamped with the `topology_generation` counter read **before** the
edge scan. A concurrent topology mutation can therefore make the result stale,
never falsely fresh.

**`index_manifest.parquet`**

| Column | Arrow type | Notes |
|---|---|---|
| `relation_type` | `Utf8` | Relation type name, or `_all` for the union index |
| `direction` | `Utf8` | `"out"` \| `"in"` |
| `topology_generation` | `UInt64` | Counter pinned before the source scan |
| `built_at` | `Timestamp(Microseconds, UTC)` | |
| `node_count` | `UInt64` | Number of source nodes covered (CSR row count) |
| `edge_count` | `UInt64` | Number of `(edge, neighbor)` entries |

**Sharded CSR (`<REL_TYPE>.<dir>.csr.json`)** — a versioned JSON manifest names an
immutable, content-addressed shard directory. Each bounded shard is Arrow IPC with one
column and covers a contiguous local surrogate range:

| Column | Arrow type | Notes |
|---|---|---|
| `adjacency` | `LargeList<Struct { edge_id: UInt64, neighbor_id: UInt64 }>` | Row `i` holds the adjacency entries of surrogate `node_id = i`, in CSR order |

Within each shard this is the CSR structure in its idiomatic Arrow encoding — the two logical arrays cannot be
two top-level columns because a RecordBatch requires equal column lengths. The list's offsets
buffer **is** the CSR offsets array (length `node_count + 1`, `Int64`, starting at 0,
monotone), and the flattened struct child **is** the targets array (length `edge_count`):
neighbors of `node_id = i` are `targets[offsets[i]..offsets[i+1]]`. The reader authenticates
and decompresses one bounded Arrow batch, then copies its values into the shared shard cache.

Conventions:

- **Empty graph**: a zero-row batch — logical `offsets == [0]`, empty targets. The offsets
  array is never empty.
- **Node with no neighbors**: an empty list (`offsets[i] == offsets[i+1]`).
- The shard manifest records format/version, total node/edge counts, ordered boundaries,
  per-shard counts, encoded/decoded byte lengths, and SHA-256 checksums. A row may span consecutive shards when a
  high-degree vertex exceeds the configured hard edge cap; readers concatenate those
  fragments in deterministic `(key, edge_id)` order.
- Logical CSR rows cover exactly `node_id ∈ 0..node_count`; surrogates beyond `node_count`
  have no entries. Empty interior rows need no physical shard bytes.
- In-memory consumers (`graphforge_exec::AdjacencyProvider`) keep a
  `ShardedCsrIndex` on a persisted hit and materialize only the requested logical row
  from its bounded shard fragments. Only current version 2 manifests and Zstd IPC shards
  are admitted. Version 1 manifests and standalone legacy files have no compatibility reader
  or migration path.
  Scan-build fallback still materializes a hash map for oracle parity.

### Bounded CSR compression

Every production shard publisher uses Arrow IPC Zstd (the pinned Arrow default
compression level 3) with the existing full-width
`UInt64` IDs and `Int64` offsets. The standard IPC raw-buffer marker is permitted
when compression would expand a buffer; it is part of the current codec, not a
legacy file fallback. Configured shard limits remain upper bounds and are capped
at 1,048,576 rows and 1,048,576 edges. High-degree rows continue across shards;
smaller configured limits retain their existing behavior.

For admitted shard counts `N` and `E`, the seven decoded buffers total
`D = 8*(N+1) + 16*E + ceil(N/8) + 3*ceil(E/8)` bytes, including validity bitmaps.
The encoded file is capped at `D + 16,384` bytes. The manifest carries both exact
encoded length and `D`; lookup and stable-shard reuse validate these counts, bound
reads, authenticate SHA-256, then check the IPC footer/message and every buffer
before Arrow allocates decoded arrays. Preflight requires the exact fixed schema,
one batch, no dictionaries, zero null counts, matching field lengths, Zstd buffer
compression, and one exact current Zstd frame with matching content size. Invalid
schema declarations return an error before Arrow's schema converter is invoked.
Decoded offsets must start at zero, remain monotone, and end at the edge count.

Opening checks shard metadata without reading payloads. Cloned readers share one
cache. On a miss, the prior cached CSR stays alive until the replacement fully
validates. Resource accounting must include that old cache, the encoded file,
Arrow's body buffer and decoded arrays, the new CSR vectors, parser/alignment
allocations and the fixed Zstd decoder context. The pinned bulk Zstd decoder uses
the supplied bounded destination rather than a separate streaming window buffer.
`D` is a deterministic buffer-content bound, **not** a process RSS bound. Allocator
overhead and other graph/query owners remain separate. A whole logical hub row can
span many shards; only `row_chunk` bounds the returned row portion by its limit.

### Rebuild and versioning semantics

- **Source of truth.** A CSR is always reconstructable from `topology/edges/<REL_TYPE>.parquet`
  alone, deterministically.
- **Generation identity.** The adjacency manifest records the topology counter
  pinned before the source scan. A complete delta chain may advance an older
  base to the current topology counter without copying the base CSR.
- **Publication rule.** A graph mutation and its source fingerprint publish in
  the same immutable generation. `CURRENT` changes only after every participant
  is durable and validated.
- **Crash-safety invariant.** A reader sees either the prior complete graph
  generation or the new complete graph generation. A failed or interrupted
  write never exposes a counter/data mismatch or committed prefix.
- **Staleness detection.** The provider compares the manifest's topology counter
  with the current counter and validates any required bounded delta chain. A
  corrupt accelerator is never served as a hit.
- **Fallback.** On mismatch (or absent index), the provider scans the typed edge tables and
  builds the adjacency in memory — yielding identical results, only slower. A stale or missing
  index can therefore never cause incorrect output.
- **Rebuild triggers.** Lazy on first traversal when the `indexes/adjacency/` capability is
  present, or explicit via `forge.index("adjacency", ...)`. Append-only commits
  publish bounded delta segments; a full rebuild compacts them into sharded bases.
- **Determinism (R-ADJ-2).** Full rebuild streams each typed edge file once; `out` entries
  sort by `(src_id, edge_id)` and `in` entries by `(dst_id, edge_id)` — the `edge_id`
  tie-break makes shard bytes reproducible from `topology/` alone. The `_all` relation's
  indexes use the same sorts over the union of typed and exploratory edges. Cache filenames
  use portable route components; the manifest retains exact semantic relation names. The manifest's
  `built_at` is excluded from the determinism guarantee.
- **Bounded build.** Projected Parquet batches feed sorted spill runs. Bounded-fan-in merge
  passes (64 runs by default) emit rows directly into hard-capped shard sinks; they never
  reconstruct complete edge/neighbor arrays. Both edge entries and local offset rows have
  hard shard caps.
  `AdjacencyBuildMetrics` exposes source rows, spill runs/bytes, shard count, and peak
  shard entries/rows for scale evidence.
- **Build ordering.** Builders write immutable shard directories, atomically publish each
  shard-set manifest, and write `index_manifest.parquet` **last**. The public facade builds
  in a same-filesystem private directory, validates it, then swaps the complete adjacency
  directory under its visibility lock. Cancellation or failure leaves the prior directory
  active and removes unpublished spill/build state.
- **Loader semantics** (`graphforge_exec::PersistentAdjacencyProvider`).
  Freshness requires a non-empty manifest whose topology generation is current
  directly or through a complete bounded delta chain. Fresh + row present ⇒ load
  (`adjacency=hit`); stale or torn ⇒ lazy rebuild, then serve; fresh but **no
  row** for the requested relation ⇒ scan-build *without* rebuild (rebuilding
  cannot add an unknown relation — prevents a rebuild-per-query loop); a
  corrupt accelerator ⇒ always-stale scan-build; capability absent ⇒ scan-build
  (`adjacency=building`). Typed-mode `"*"` bypasses the index entirely
  (reported as `building`, never a false miss). A build or load failure never
  fails the query — only its speed.
- **Direction.** `out` and `in` CSRs are stored separately; undirected traversal unions them.
  In exploratory mode, `_exploratory.parquet` rows are routed by their `rel_type_name` column.

### `indexes/<LABEL>/tantivy/` — full-text search index

Full-text indexes (Tantivy) are also derived and rebuildable from `properties/`. See the
Find / index (`forge.find` / `forge.index`).

---

## Graph Fact Schema

### Topology layer (hot path)

Graph traversal reads only the topology layer. No property columns are read unless the query explicitly projects them.

**`topology/nodes.parquet`**

| Column | Arrow type | Notes |
|---|---|---|
| `node_uuid` | `FixedSizeBinary(16)` | UUIDv7 — canonical stable identity |
| `node_id` | `UInt64` | Local surrogate — DataFusion join key |
| `type_id` | `UInt32` | Immutable primary label used for property-file routing |
| `type_ids` | `List<UInt32>` | Authoritative complete label set; scans use membership in this column |
| `created_at` | `Timestamp(Microseconds, UTC)` | |
| `updated_at` | `Timestamp(Microseconds, UTC)` | |

The first label in a node's creation pattern is its immutable **primary label**.
The property route continues to use that primary label, encoded through the
semantic-route mapping for its physical path; adding secondary labels therefore
cannot orphan or relocate properties.
Unlabelled nodes route to `_untyped`. A v0.5 node participant must contain both
fields with the frozen schema; an earlier development schema is unsupported.

#### Filtered node lookup

Canonical node files assign `node_id` densely and monotonically, so physical
row ordinal `n - 1` contains `node_id = n`. A filtered node read proves that
layout from non-null row-group statistics plus ascending column and offset page
indexes, then supplies Parquet with an exact `RowSelection` for the requested
ordinals. Scattered destination ids therefore decode only their selected rows,
instead of every page between their minimum and maximum.

The optimization is fail-closed. Missing page indexes, deleted/gapped IDs,
malformed statistics, or unordered ranges use the row-group plus
membership-predicate reader. Exact output keys are validated after ordinal
selection; any mismatch is discarded and retried conservatively. This
accelerator fallback applies only to a valid v0.5 participant; it is not a
project-format compatibility path.

**`topology/edges/TYPENAME.parquet`** (one file per relation type)

| Column | Arrow type | Notes |
|---|---|---|
| `edge_uuid` | `FixedSizeBinary(16)` | UUIDv7 |
| `src_uuid` | `FixedSizeBinary(16)` | References `node_uuid` |
| `dst_uuid` | `FixedSizeBinary(16)` | References `node_uuid` |
| `edge_id` | `UInt64` | Local surrogate |
| `src_id` | `UInt64` | Local surrogate — DataFusion join key |
| `dst_id` | `UInt64` | Local surrogate — DataFusion join key |
| `created_at` | `Timestamp(Microseconds, UTC)` | |

Typed edge tables replace the unified `edge_facts` table. A relation is a
logical union of its legacy flat fragment and ordered immutable range
fragments. Each construction flush encodes only accepted rows; it never
decodes and rewrites prior edge topology. Ordinary catalog, traversal,
adjacency, UUID-index, mutation, projection, and delta-replay paths enumerate
the same fragments. This keeps aggregate fresh-import edge encoding O(N) and
resident topology bounded by the configured construction window while
preserving direct single-relation scans. See [refactor-v0.5.md §7](refactor-v0.5.md)
for performance analysis.

Node topology follows the same immutable layout: the first compatible write
may retain `topology/nodes.parquet`, while later appends create ordered
`topology/nodes/<first>-<last>.parquet` fragments. Counts, filtered reads,
surrogate recovery, UUID membership, semantic validation, projection, export,
label mutation, and deletion operate over the logical union. A localized
rewrite replaces only the fragment containing a changed row; untouched node
fragments retain their filesystem identity.

`topology/surrogate_tails.parquet` is a one-row control record containing the
monotonic maximum node and edge surrogates. It is staged in
the same commit as every topology append. Writer reopen reads this bounded
record rather than enumerating or decoding the accumulated topology fragments;
legacy projects without it use the bounded tail migration path once.

Bulk endpoint resolution uses the persistent authenticated
`topology/uuid-membership/` snapshot published with each immutable graph
generation. The existing `manifest.json` facet authenticates the
topology generation, record counts, lengths, and SHA-256 digests. Nodes have a
sorted fixed-width `UUID -> node_id` file; edges have a sorted UUID membership
file. Builds use bounded external sort runs and bounded-fan-in merges. Probes
sort and deduplicate the caller batch, use authenticated block fences to select
only candidate blocks, and merge-scan every selected block once. Newest runs
own tombstone and cross-kind shadowing; node results are batch-validated against
the surrogate-sorted reverse file before caller order is restored. Production
work evidence reports identity/surrogate block reads and bytes, runs considered,
and exactly zero per-record filesystem seeks while decoding zero topology rows.
Duplicate node or edge UUIDs, reuse of one UUID across the node and edge
domains, stale manifests, and missing, truncated, checksum-mismatched, or
identity/reverse-inconsistent index files fail closed.

Node ordinal resolution is a distinct, additive authority facet in the same
directory. Its `ordinal-v4-manifest.json`, `ordinal-v4-receipt.json`, and
`ordinal-v4.lock` never replace or reinterpret the v3 node-and-edge manifest.
Both facets name the same topology generation but have independent receipt-bound
manifest digests. If the ordinal facet is absent while current v3 is canonical,
discovery returns a typed rebuild requirement. A present ordinal path must pass
authenticated open and never falls back to v3 when malformed or substituted.

New mapped publications use a compact version-4 `graph/files` root, retaining
the version-2 radix representation with explicit mapped-route authority. Payloads and
compressed node-v3 radix nodes live once in the project content-addressed object
store; a generation stores only its root reference and logical totals. Updates
copy a bounded SHA-256 nibble path and split or collapse bounded buckets,
so publication never scans or recopies the entire prior file inventory. The private
workspace commit boundary records revision-identified sealed and tombstone
descriptors before mutations become visible; they are acknowledged only after
CURRENT advances. Reopen traverses the authenticated radix and hashes every
selected payload object. Post-CURRENT GC traces every remaining generation
root and defers while an optimistic attempt or CAS publication lease is live.
Legacy expanded and compact inventories remain readable. A writable facade
migrates their raw routes in its private workspace before publishing the mapped
layout under the CAS publication lease.

The authoritative write census is executable: topology node and edge shards,
node and edge properties, graph deltas, catalog records, extension-owned graph
records, and the generation/runtime-catalog/runtime-label control files must
all appear in the revision descriptor journal and resolve to the same authenticated
logical inventory. Rebuildable adjacency and UUID-membership artifacts live
under `.graphforge-cache/` and are rejected as graph authority. Parquet write
sites share `RewriteBatch` plus `commit_topology_aware`; the three control-file
writers record their descriptor before making replacement bytes visible.

Terminal buckets retain sorted exact paths even when distinct path bytes share
a SHA-256 digest, up to the eight-entry bound. A real SHA-256 collision is not a
test fixture. Tests cover exact-path replacement/deletion, bucket split/collapse,
maximum escaped path fields, oversized encodings, duplicate paths, malformed
routes, unsupported versions and authenticated old-root reads.

### Properties layer (warm path)

**`properties/<component>.parquet`** (flat first fragment) and
**`properties/<component>/<generation>-<ordinal>.parquet`** (immutable construction fragments).
The mapped component resolves to the semantic entity type; legacy raw layouts
use the entity type directly at the same route position.

| Column | Arrow type | Notes |
|---|---|---|
| `node_uuid` | `FixedSizeBinary(16)` | Join key back to `topology/nodes.parquet` |
| *(property columns)* | *(per ontology)* | e.g. `name Utf8`, `age Int64`, `email Utf8` |

Property access joins the logical node-topology shard union to the logical
property overlay on `node_uuid`. Catalog, direct, SQL, export, verification,
and import readers authenticate the legacy flat fragment and every canonical
immutable fragment, then expose one newest-authority complete snapshot per
UUID; they do not concatenate physical rows. A newest tombstone suppresses the
UUID, and the newest live-schema summary determines which property keys remain
logically present. Edge properties use the identical authority model under
`edge_properties/`. Construction and ordinary SET/REMOVE mutations append only
the changed bounded window without rewriting prior fragments. The separation
still lets topology-only traversal skip property I/O entirely.

### Provenance and knowledge participants

`provenance/` and `knowledge/` belong to the knowledge layer, but generic
storage does not own their records or schemas. Under
[ADR 0012](../../adr/0012-knowledge-domain-ownership.md):

- `graphforge-provenance` owns provenance events and lineage;
- `graphforge-knowledge` owns knowledge assertions, assertion graph references, confidence
  assessments, evidence links, algorithm runs/events, and every additive epistemic
  epistemic record;
- `graphforge-api` validates cross-domain UUID references and assembles composite
  writes; and
- `graphforge-storage` receives validated Arrow batches as opaque generation
  participants and owns only their paths, checksums, persistence, publication,
  and recovery.

The exact knowledge schemas are frozen and are generated from the two owning Rust registries in the checked
[Knowledge schema inventory](../../reference/knowledge-schema-inventory.json). The epistemic layer adds separate
append-only status, amendment, reasoning, supersession, hypothesis, selection,
and valid-time record families; it does not add mutable fields to
knowledge assertions.

The legacy pre-knowledge `PROVENANCE_EVENTS_SCHEMA`, `PROVENANCE_LINEAGE_SCHEMA`, and
graph-embedded edge `confidence`/`provenance_uuid` fields have been removed
from `graphforge-storage`. They are not the knowledge-layer contract, and no historical project
data is imported or converted.

Graph-only readers resolve the committed generation and graph-required
manifest fields without opening either participant. A future or corrupt
knowledge record blocks its owning knowledge API, never Cypher or neutral
analyst-verb/find execution.

---

## Ontology Runtime

The ontology is a **runtime-loadable knowledge schema**, not Rust structs generated into the binary. Three representations serve different purposes:

| Format | Purpose |
|---|---|
| **YAML / JSON** | Human-authored ontology definitions (Serde-based load) |
| **Arrow tables** | Compiled execution format — cheap joins during binding and planning |
| **Parquet** | Derived compiled-runtime snapshot for rapid startup or reproducible deployments; discard and recompile on ontology-checksum mismatch |

### Ontology authoring format (YAML)

```yaml
ontology_id: core
version: "2026.05"
entity_types:
  - name: Person
    abstract: false
  - name: Employee
    parent: Person
relation_types:
  - name: MANAGES
    src: Employee
    dst: Employee
    inverse: MANAGED_BY
    semantic:
      transitive: false
      symmetric: false
      functional: false
properties:
  - owner: Person
    name: name
    type: utf8
    nullable: false
constraints:
  - owner: Employee
    kind: unique_property
    expr:
      property: employee_id
```

At load time this compiles into Arrow lookup tables keyed by integer type IDs. String-heavy lookups during planning become O(1) integer comparisons.

### Ontology runtime tables

| Table | Purpose |
|---|---|
| `ontology_meta` | Identity, version, IR compatibility range, checksum |
| `entity_types` | Node classes and inheritance DAG (acyclicity enforced at load) |
| `relation_types` | Edge classes, endpoint type constraints, inverse pairs |
| `property_types` | Name, owner, value type, nullability, cardinality |
| `type_constraints` | Validation rules (unique, required, range) |
| `cardinality_rules` | Endpoint multiplicity (min/max per relation type) |
| `semantic_flags` | `transitive`, `symmetric`, `reflexive`, `functional`, `acyclic` |
| `aliases` | Human-facing and deprecated names |

Versioned migration transforms remain part of the authoritative `OntologyDoc`;
they are not a ninth compiled runtime table or Parquet snapshot file.

### Ontology versioning

Two independent version axes:

| Axis | Meaning |
|---|---|
| `ontology_version` | Meaning of types and rules — changes when the schema evolves |
| `ir_version` | Runtime/compiler contract — changes when the IR format changes |

A new ontology version does not require a new IR version, and vice versa. Persisted datasets record the `ontology_version` used to write them. Arrow schema metadata carries both versions through IPC and Parquet round-trips.

### Validation model

| Level | When | Examples |
|---|---|---|
| **Ontology-load** | On file/table load | Duplicate names, missing parents, inheritance cycles, bad inverse references |
| **Write-time** | On `CREATE`, `MERGE`, batch ingest | Unknown property, wrong value type, illegal endpoint type, cardinality overflow |
| **Query-time** | During binding/planning | Unknown labels/types/properties, illegal pattern shape, ambiguous property resolution |

---

## Serialization Systems

**Default rule (with two named exceptions):**

| System | Purpose | Format |
|---|---|---|
| **Arrow / Parquet** (`graphforge-storage`) | Graph topology/properties and generic persistence of domain-owned participants | Binary columnar (Arrow IPC / Parquet) |
| **JSON / YAML** (`graphforge-ontology`) | Ontology definitions and metadata | Text (human-readable, validatable) |

Graph data → Arrow/Parquet. Ontology definitions and metadata → JSON or YAML.
The permanent exceptions are (1) authoritative, versioned GFDR binary delta
runs with schema-qualified JSON mutation payloads, and (2) derived compiled
ontology runtime tables persisted as Parquet. The latter never supersede the
CURRENT-selected workspace's canonical ontology JSON and are
discarded/recompiled when its checksum differs. External YAML/JSON files are
authoring/import inputs, not alternate project commit pointers.
[ADR 0024](../../adr/0024-storage-format-exceptions.md)
defines both compatibility and migration boundaries. Arrow schema metadata
carries version and provenance annotations across language boundaries.

---

## Two-Mode Graph Instances

```rust
// In-memory (fast, volatile)
let forge = GraphForge::new(None)?;

// Persistent (project directory)
let forge = GraphForge::new(Some("path/to/project/"))?;
```

The storage layer is transparent to all API surfaces.

---

## References

- [Architecture Overview](overview.md) — workspace layout and provider trait
- [Architecture Refactor v0.5](refactor-v0.5.md) — UUID identity model, typed edge tables, project structure
- [Execution Model](execution-model.md) — how providers connect to DataFusion
- [ADR 0001: Rust Core](../../adr/0001-rust-core.md) — Parquet-as-primary and provider strategy


## Current publishing contract

Rust owns publication behavior. The public facade selects the operation path;
storage owns authenticated representations and atomic generation selection.
A publishing path is complete only when the selected generation and the facade's
readers agree. Table-valued data remains Arrow at the API and Parquet at rest;
versioned GFDR is the explicit property-journal exception.

Every applicable graph publisher preserves these authorities together:

- Exact node/edge UUIDs, full-width surrogates, endpoint identity, global edge
  identity uniqueness, and persistent consumed-ID high-water marks. Deletion
  cannot make an ID available to a subsequent ordinary CREATE.
- Complete typed or exploratory schemas, null/concrete property types, latest
  values and tombstones; immutable primary routes and label memberships remain
  distinct. Physical path components never substitute for semantic route names.
- Authenticated graph-file ownership, route authorities, UUID membership,
  ordinal receipts and applicable adjacency/search generations. Staging reads
  the admitted inventory rather than discovering authority from filenames.
- All graph, catalog, ontology/composition and other declared publication
  participants. A graph-only update retains unrelated participants; changing
  ontology cannot strand retained semantic bindings or reinterpret numeric IDs.
- Parent conflict, operation identity, cancellation, active reader leases and
  crash/returned-error recovery. CURRENT selects the complete old or new
  generation. A returned error after selection must reconcile actual selected
  authority; retries cannot install an unrelated private candidate.
- `permanent_parquet::writer_properties` for every verified permanent Parquet
  publisher. Per-path dictionary, row-group, streaming and memory settings remain
  local. Replay and compaction retain the accepted codec; private IPC/spill,
  portable containers and query sinks have separate contracts.

| Producer → publisher → consumer | Operation boundary and applicable proof |
| --- | --- |
| Public construction → canonical graph generation → facade/reopen | Typed/exploratory topology and properties, sharded nodes, routes, all graph identity authorities and continuation tails. Construction, CAS ownership and publishing-budget facade regressions cover exact reopening and portable interchange. |
| Ordinary Cypher/analyst mutation → MutationTransaction/GraphWriter → generation readers | Canonical topology and property mutation; complete participant publication. Public CREATE/DELETE/SET, active streams, fault recovery and next-ID tests apply. |
| Composite property mutation → GFDR preparation → verified replay/compaction | Only the four property operations are admitted. Qualified/constructed ownership fixtures cover sparse latest values, removals, nulls, route identity and shared immutable base payloads. Canonical property publication remains available when eligibility requires it. |
| Composite topology mutation → canonical GraphWriter publication → facade | Never GFDR. Qualified create and owner-routing regressions cover identities and subsequent property mutation. |
| Storage GFDR APIs → framed runs → direct replay, open, checkpoint, compaction/import | All topology operations are unsupported, including checksum-valid records, duplicate operation IDs and matching transaction retries. Refusal precedes authority changes; direct replay leaves the entire supplied state unchanged. |
| Public compaction → complete new generation → refreshed facade | Full verified property chain, same-facade subsequent mutation, exact retry, retained streams and imported continuation. Private hydration and selected permanent ownership are measured separately. |
| Ontology adoption/clear and retained semantic transformations → workspace generation → prepared readers | Same-name promotion stages graph and ontology together; disjoint metadata-only adoption reuses payloads. Unsupported clear/type-ID reinterpretation refuses before selection. Existing semantic transformation and promotion lifecycle tests own applicable retained-data proofs. |
| Graph/belief/portable projection → selected artifact → verifier/import | Exact selection and endpoint closure, schemas, routes and rebuilt applicable controls. An exported artifact is not a selected live generation; clean import must validate it before subsequent mutation. |
| Portable clean import/checkpoint restoration → complete generation → facade | Complete participant authentication and corruption refusal, exact identities/properties and continuation. Persisted unsupported topology GFDR cannot be imported as supported current state. |
| Catalog/vector/knowledge/epistemic/provenance/restore markers → declared participant → domain reader | Domain schemas, references, authentication and atomic participant ownership apply. These writers do not allocate graph topology IDs; graph ordering/tails are inapplicable to the participant payload itself. |

Private accepted construction/merge streams, decoder spools, standalone ontology
persistence without a graph publisher, external query sinks, and test/benchmark
fixtures are not permanent graph publishers. They cannot establish production
support for a topology journal operation.

The four `permanent_storage_budgets` tests
`publishing_contract_alternates_supported_paths_and_refuses_topology_journals`,
`publishing_contract_flat_ontology`, `publishing_contract_sharded_exploratory`
and `publishing_contract_sharded_ontology` use `exercise_publishing_contract`
to check flat/sharded, exploratory/ontology-promoted, two-route graphs. The flat
cases use public composite CREATE on an empty project; the sharded cases use
public construction sessions. All four exercise actual property GFDR,
compaction, canonical topology mutation, reopening,
export/full verification, clean import and subsequent mutation. It compares
UUIDs, routes, endpoints, nullable typed values and node/edge allocation continuity.
The neighboring compaction/promotion/owner fixtures add active snapshots,
interrupted publication, cancellation, retries and full-width identity coverage.
Storage journal tests additionally cover forged persisted records, direct-run
bypasses, byte-preserving refusal, corruption and exact resource thresholds.

The conformance work reuses the source-bound measurements and deterministic
budgets from the [permanent-storage assessment](permanent-storage-assessment.md),
including #1213 encoding, #1219 CAS replay, #1224 property ownership, #1231 facade
refresh and #1229 promotion. Rejection creates no graph payload or publication;
its fixture compares every retained file digest and allocation before/after.
The added preflight is a constant-work discriminant check per operation and
retains no second graph or decoded value collection. Existing replay work,
metadata, batch and memory limits remain enforced. Logical counters do not
claim process/native RSS bounds; sampled overlapping allocation is not a hard
peak bound. Source-bound integrated S20/S22 accounting remains the epic's later
measurement, not an outcome inferred from this contract test.

### Publishing-contract acceptance evidence

[The frozen four-case measurement](../../development/evidence/publishing-contract-1221.json)
records 55.58 s user CPU, 5.36 s system CPU and 162,652 KiB observed process
peak RSS. Separate syscall tracing recorded 882,417,829 bytes read and
157,666,163 bytes written; separate pathname sampling observed 21,864,448
allocated bytes at its largest sample, deduplicating shared inodes across
retained, private and portable files. These are complete lifecycle observations,
not isolated rejection costs or hard temporary-disk limits. The evidence records
OS block counters, sample gaps, overlapping validation activity and failed
superseded fixture attempts. No incomparable baseline improvement is claimed.

| Contract outcome | Direct evidence |
| --- | --- |
| Supported producers, complete authorities and encoding selection | The producer matrix above; shared-policy and per-path budgets in the permanent-storage assessment. Participant-only paths explicitly exclude topology allocation. |
| Unsupported topology refuses before authority changes | `topology_payloads_reject_before_encoding_preparation_or_publication`, `topology_cannot_bypass_published_retry_or_mutate_direct_replay_state`, `committed_checksum_valid_invalid_memberships_reject_without_authority_mutation`, and `topology_overlay_refusal_preserves_routes_and_full_width_ids`. These cover all topology variants, retry/direct-replay bypasses, authenticated persisted records and unchanged target bytes. |
| Flat/sharded and exploratory/ontology public lifecycle | The four `publishing_contract_*` cases above assert the selected topology layout after optional adoption. They preserve exact node/edge UUIDs, endpoints, nullable values, route counts and consumed node/edge IDs across deletion, reopen, CREATE, export/full verification, clean import and subsequent mutation. |
| Authentication and recovery remain enforced | Existing journal checksum/order/missing-run tests; compaction cancellation, checkpoint retention, cleanup and exact retry; portable import crash windows and pristine-target corruption refusal. Merged #1219, #1224, #1229 and #1231 supply public active-snapshot and returned-error lifecycle coverage. |
| Resource bounds remain meaningful | `streaming_resource_ladder_is_independent_of_base_rows` uses 260/516/1,028 nodes, seven-row batches, 2 MiB replay admission and at most 256 KiB logical/allocated decoder spool. It compares exact output with the non-spooling path and limits logical replay-state growth to 128 bytes. Public conformance enforces 2 MiB compaction output and 8 KiB logical replay-state ceilings. These counters exclude process/native memory. |
| Portable current-format identity correctness | `valid_identity_package_keeps_absent_primary_and_runtime_catalog_bytes`, `invalid_delta_identity_package_preserves_pristine_target_authority`, and `absent_primary_round_trip_and_topology_replay_refusal_preserve_state` distinguish valid full-width identities from unsupported topology replay. |

This ledger maps ordinary implementation criteria to their existing tests. It
adds no release-certification requirement and does not claim final capacity
completion; integrated S20/S22 evidence remains under #1194.

### Bounded manifest allocation evidence (#1204)

The fixed heterogeneous #1196 workload (4,097 nodes, 65,537 edges, four routes)
now publishes 352 exact manifest entries as 209 production objects occupying
856,064 native allocated bytes. The immediately preceding current-format
baseline used 483 objects and 1,978,368 bytes for the same entries: a
1,122,304-byte (56.7%) reduction. Attributed permanent allocation falls from
9,814,016 to 8,691,712 bytes. The historical #1196 baseline remains separately
recorded at 544 entries / 750 objects / 3,072,000 bytes; its additional 192 edge
payload references were removed by earlier work, not by bucket encoding.
The deterministic acceptance ceiling is 1,536,000 manifest bytes on native
4-KiB allocation storage.

Frozen executable source `36360cb8` preserves the exact semantic fingerprint
through real public construction, reopen/query, export, full verification and
clean import. The publishing suite additionally exercises immediate mutation,
retained streams, subsequent imported mutation, recovery, cancellation and
retry. Maximum-field and corruption tests, exact eight-to-nine split/collapse
and retained-root checks, and the 128/256/512-entry update ladder cover the
manifest boundary directly.

The source-bound resource record is
[`bounded-manifest-1204.json`](../../development/evidence/bounded-manifest-1204.json).
Its full-fixture CPU observations include the existing codec experiments and
portable lifecycle; they are not a query-speed benchmark. Application syscall
reads are 1,303,689,747 baseline versus 1,301,114,860 candidate bytes; writes are
267,812,449 versus 267,870,503 bytes. Thus the physical allocation saving does
not imply reduced write traffic in this workload. Process RSS, OS block I/O,
and separately sampled overlapping filesystem owners are reported with their
measurement limits. The candidate point-in-time whole-project census includes
21,979,136 allocated file bytes plus 1,257,472 directory bytes; no equivalent
baseline census or whole-project reduction is claimed.


### Bounded CSR allocation and cost evidence (#1205)

The fixed eight-route public lifecycle fixture uses 4,097 nodes, 65,537 edges,
random full-width identities and nullable properties. Frozen release executables
compare the uncompressed current baseline with the bounded compressed writer;
both produce the same semantic fingerprint through reopen, export, full
verification and clean import.

| Measurement | Uncompressed baseline | Bounded Zstd |
|---|---:|---:|
| Actual CSR payload bytes (18 shards) | 4,920,564 | 1,324,020 |
| Actual CSR allocated bytes | 4,972,544 | 1,363,968 |
| CSR metadata logical bytes | 10,329 | 11,431 |
| Attributed permanent allocated bytes | 15,151,104 | 11,542,528 |
| Whole-project file allocation, point census | 44,806,144 | 41,197,568 |
| Whole-lifecycle process peak RSS, KiB | 220,400 | 238,252 |
| Whole-lifecycle syscall read bytes | 2,309,281,609 | 2,098,377,656 |
| Whole-lifecycle syscall write bytes | 357,447,042 | 328,689,815 |
| Sampled overlapping workspace allocated peak | 89,038,848 | 74,625,024 |
| Cold-probe direct CSR read bytes | 35,552,246 | 9,577,462 |
| Cold-probe first-query median, seconds | 3.855 | 3.891 |

Payload and allocation both meet the deterministic 70% reduction budget. The
whole-lifecycle RSS increase is a measured cost, not a decoded-memory improvement.
The cold probe executes exact 256-row two-hop public queries in three fresh
processes, with four subsequent queries per facade. Its predeclared timing, RSS
and syscall-I/O investigation thresholds pass; this is not a latency improvement
claim or a noisy CI timing gate. Private-file cache advice does not guarantee OS
cache eviction. Syscall traffic is not physical I/O.

The workspace sampler deduplicates overlapping file owners by device/inode and
includes project, staging and portable state. It excludes directory blocks and
open-unlinked files and can miss short peaks; the largest observed sampling gaps
are approximately 62 ms and 59 ms. These are sampled workspace peaks, not hard
temporary-disk bounds. The separate point census includes retained generations.
See [`bounded-csr-1205.json`](../../development/evidence/bounded-csr-1205.json)
for frozen source/executable hashes, exact observations, commands, decoded bounds,
CPU/I/O costs and limitations, including the superseded incomplete baseline trace.

### Ordinary Cypher property ownership

Committed edge SET, map updates and REMOVE resolve the authenticated property
owner before accumulating effects. Each input batch probes only its logical
relation routes and `_exploratory`, including newest tombstones. Multiple owners
are refused; an entity without a property row keeps its logical route. Edges
created in the same statement retain the pending writer's route. Replacement
maps read existing keys in batches from the resolved owner, so omitted properties
are removed from the same authority that receives the replacement.

Typed scans and fixed expansions carry their catalog-resolved route as an
internal constant column. This uses the generation's storage binding, without
reinterpreting ontology IDs as runtime IDs. The statement retains its pinned
property inventory through SET/REMOVE staging, including declared semantic
schema metadata for a route's first property write. Immediate query success is
insufficient: ownership fixtures also reopen after qualified mutations and
exercise full portable verification, clean import and later mutation.

The `property_writes` demand diagnostics sum owner probes and replacement-key
reads across every input batch and SET item. Repeated reads are counted each
time. Decoder and authenticated-snapshot peaks describe those readers; target
counts describe retained identities. These counters neither measure the entire
publication nor bound process RSS. The representative multi-batch regression
checks exact results and explicit cumulative work ceilings.
