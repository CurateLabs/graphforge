# UUID identity authority

GraphForge answers "does this UUID exist" from the published topology Parquet.
There is no derived UUID membership index. The `topology/uuid-membership/`
directory name is historical: it now holds only the node ordinal facet below.

Issue #1902 removed the membership index (`manifest.json`,
`identities-v5-*.uuidx`, `node-surrogates-v5-*.uuidx`, `topology-receipt.json`).
It duplicated the UUID columns of the published Parquet at 17-25 B per edge
(9.28 GiB at S25) and was rewritten with every generation. Under the pre-v1
policy the format changed in place: no new generation writes those files, no
reader opens them, and a project that still carries them opens normally with the
files admitted, exported and ignored.

## Identity probe

`TopologyIdentityProbe` (`topology_identity.rs`) is the one reader. It is opened
over a pinned `TopologyFiles` list, or over a compact parent generation whose
fragments are content-addressed objects, in which case each object is
authenticated against its inventory checksum and held for the life of the probe.

- It reads each fragment footer once and caches it process-wide, keyed by the
  file's device, inode, length and mtime. A replaced fragment misses the cache.
- A probe sorts and deduplicates the caller's UUIDs, then skips every row group
  whose `node_uuid`/`edge_uuid` min/max statistics exclude all of them. Fragments
  built by an initial build are UUID-ordered, so a small batch touches a few row
  groups. Appended fragments are `node_id`/`edge_id` ordered and their UUIDs
  arrive in any order, so their ranges prune less; a probe of them reads every
  row group whose range overlaps a candidate.
- Inside a surviving row group the probe prunes again by the Parquet column
  index: a page whose min/max excludes every candidate is not decoded, and the
  reader fetches only the selected pages through the offset index. Published
  Parquet is written with a page index (20,000-row pages in 1,048,576-row row
  groups), so a small batch into a UUID-ordered fragment decodes a few pages,
  not a few row groups. A file without a page index is pruned by row group alone.
- The surviving pages decode only the UUID column (plus `node_id` for node
  lookups) and row groups run in parallel. Each decoded value is binary-searched
  against the sorted candidates. The metrics report `pages_considered` and
  `pages_read` (the difference is what pruning avoided), `identity_blocks_read`
  (row groups decoded) and `identity_bytes_read` (compressed bytes of the
  selected pages and their chunks' dictionary pages).
- Live counts are the sum of footer row counts, so no manifest carries them.

Node and edge UUIDs share one namespace. Append validation, the writer's commit
and the staged construction session all ask the same question: is this UUID a
live node, a live edge, or a deleted entity? Any yes is refused with the same
typed `IdentityConflict`.

## Deleted identities

A deleted entity is no longer a row, but its UUID is never reusable. Until
#1902 the membership index kept a tombstone for every deleted node and edge.
`topology/deleted_identities.parquet` replaces those tombstones: one
`FixedSizeBinary(16)` column of the UUIDs of every deleted node and edge, sorted
and unique. A generation that deletes writes the merged file in the same atomic
rewrite as the topology change; a generation that deletes nothing carries the
prior file forward untouched, and a graph that never deletes never has one. Its
size follows deletions, not graph size. It is an ordinary authenticated graph
file, so export, import and `gf verify` carry and check it with the rest.

Validation refuses what the commit refuses. Before #1902, `validate_bulk_*`
accepted a deleted UUID and the commit rejected it later; both now consult the
same probe.

## Projects that predate the removal

Two behaviours differ for a project written before #1902. Both are accepted
under the pre-v1 policy, which changes the format in place without a migration:

- **UUIDs deleted before the upgrade become reusable.** The index kept the
  tombstones of a project's earlier deletions and nothing reads it now.
  `deleted_identities.parquet` records only deletions made since the upgrade, so
  an entity deleted before it can be appended again under the same UUID. This
  holds for edges and nodes alike as far as the identity probe is concerned; the
  ordinal facet's own checks are unchanged. Entities deleted after the upgrade
  stay spent.
- **Search verifies nodes by repeat-check when there is no ordinal facet.**
  `NodeIdentityCheck` used the ordinal facet when a project had one and the
  membership index otherwise. With the index gone, a project without an ordinal
  facet checks only that the rows it reads do not repeat a node UUID; it no longer
  cross-checks each row's `node_id` against a separate authority. A project with
  the ordinal facet is unchanged.

The old files are left alone. Nothing reads `manifest.json`,
`topology-receipt.json`, `identities-v5-*` or `node-surrogates-v5-*` in an
upgraded project, and nothing removes them: orphan collection considers only the
canonical ordinal artifact names, so the files stay in the graph-files inventory
and travel through hydration (the two JSON controls are still copied), export,
verify and import as ordinary entries until a future cleanup drops them.

## Node ordinal facet

`ordinal-v4-manifest.json` is the additive node-only authority for bounded
`node_id -> UUID` reads. `ordinal-v4-receipt.json` binds its exact manifest
digest and topology generation. The receipt is authoritative only when its
exact bytes are selected by the pinned project generation's authenticated
`graph/files` participant; a coherent sibling receipt/manifest replacement is
not provenance. `ordinal-v4.lock` coordinates admission with
the durable writer. Forward and ordinal artifacts authenticate the same node
mapping independently. Ordinal payloads are packed by contiguous node-ID range
and carry fixed-size authenticated block fences.

Discovery and authenticated open are separate operations. When the ordinal
manifest is absent, discovery returns `RebuildRequired`. When the ordinal path
exists, discovery reports it as present without trusting its contents. Authenticated open then
requires the ordinal digest selected by the project receipt. A malformed,
substituted, or generation-mismatched ordinal facet fails closed and never
falls back to another source.

The explicit rebuild API constructs v4 only from canonical topology and returns
an aggregate `CanonicalTopology` disposition with generation, identity/range,
artifact-byte, fixed-block, buffer, temporary-run, and fsync evidence. It never
reads legacy membership files. Durable-rewrite recovery either retains the prior
authority or completes the receipt-bound v4 facet; there is no mixed-version
read state.

`peak_temporary_bytes` is the total maximum coexisting rebuild scratch, not
merely the final artifact size. Storage-owned accounting includes scan runs and
their merge outputs, the UUID-sorted and surrogate-sorted projections, and the
immutable forward/range artifacts while both projections remain live. Each
retained `stage_file` copy and staged manifest/receipt control is charged at its
actual lifecycle transition, so a source artifact and its retained copy both
appear in the peak. Accounting uses exact registered lengths rather than a
recursive scan of an active scratch directory.

Standalone graph roots have no project-generation `graph/files` inventory.
Only the mutation writer has a narrow exception: immediately before entering
the single durable-rewrite critical section it may pin the already-open sibling
manifest and artifacts when the sibling receipt's exact manifest digest and
generation match current topology authority. That exception advances an
existing facet; it cannot construct authority, is never used by public readers,
and does not authorize orphan deletion. Selected project-generation roots must
always use their externally authenticated `graph/files` authority.

The reader takes the shared ordinal lock before reading the manifest and
releases it after pinning the manifest and immutable artifacts. The durable
writer takes the project rewrite lock first and the exclusive ordinal lock
second, retaining it through data installation and the manifest switch. A
long-lived immutable read handle therefore cannot starve a writer; it advances
only by opening a newly receipt-authorized generation.

Query execution should open one authenticated handle for its pinned generation
and reuse it across bounded destination-ID chunks. Each lookup reports only
requested/unique/found counts, selected ranges, logical bytes, coalesced calls,
tombstones, and bounded-buffer charges. A typed failure can be reduced to
sanitized failure evidence, including an authentication-failure count, without
emitting UUIDs, paths, or record contents. Consumers must not reopen the index
per chunk or substitute a scan of the node Parquet.

Orphan collection requires the opaque authority resolved from a pinned project
generation before authenticating the v4 manifest and artifacts, and retains
exactly the files that manifest names. It never collects legacy membership
files. Hashing an untrusted manifest or receipt is never treated as provenance
for deciding reachability.

The ordinal facet and the deleted-identity record are persistent graph
authority, not `.graphforge-cache/` content. Construction and canonical
ordinal-facet publication are specified separately by #969.

### Incremental ordinal publication

An ordinary topology generation publishes one UUID-sorted forward delta, the
maximal contiguous ranges from its node-ID-sorted delta, and one sorted unique
tombstone delta. It never decodes or rewrites canonical topology and it rejects
zero IDs, duplicate mappings, nonmonotonic surrogate allocation, reuse, range
overlap, and tombstones that do not name retained live authority before any
transaction entry is staged.

Forward files are canonical and strictly UUID-sorted within each generation.
Their descriptors are strictly generation-ordered; they are not required to be
globally concatenation-sorted. Opening reads no artifact byte: it authenticates
the manifest and validates every descriptor and block fence, and each lookup
authenticates the ordinal or tombstone blocks it reads. Complete admission, which
every writer runs before building on the artifacts, authenticates every run and
compares the aggregate forward mapping commitment with the aggregate ordinal
mapping commitment. Historical UUID and surrogate uniqueness is proved at append by the identity
probe and the deleted-identity record, in the same topology transaction.

The manifest may record `uuid_order_matches_ordinals`: whether UUIDs ascend
strictly across every ordinal, derived by the publisher from the records it
streamed (see [ADR 0038](../../adr/0038-determinism-at-the-publication-boundary.md)).
The ordered fast path reads it instead of scanning; it is omitted when unknown.

The construction artifact remains an immutable base. Later forward artifacts
close implicit contiguous generation intervals. Two adjacent equal-width delta
intervals compact like a binary carry, so retained history stays logarithmic
without adding mutable level metadata to the descriptor. Compaction merges
forward records with the newer interval winning an identical mapping, merges
tombstones as a sorted union, concatenates adjacent ordinal ranges, and retains
nonadjacent packed ranges. The base is never rewritten by ordinary append and a
tombstone can never be removed or resurrected.

Planning consumes file handles cloned from an already authenticated v4 handle;
it never reopens descriptor names. Sorting, merge, tombstone, and ordinal I/O
uses fixed-size runs and 64 KiB artifact blocks. Aggregate work evidence reports
input rows, exact physical and sequential bytes, calls, compactions, retained
and created artifacts, peak buffers and scratch space, fsyncs, and orphan work;
it contains no graph identities.

The shared durable rewrite installs only new or compacted artifacts, then the
receipt, then the ordinal manifest as the last data participant. The topology
generation record remains the final authority switch. Recovery rolls the exact
typed receipt-bound transaction forward and reconciliation verifies the exact
expected manifest, so retry never replays a graph mutation. A retained old read
handle fails stale named-manifest revalidation after the switch; callers advance
by opening the exact newly receipt-authorized generation.

Orphan maintenance runs only from the selected authenticated v4 authority. It
removes an unreferenced single-link artifact by retained identity,
defers linked or over-budget candidates, and never treats an untrusted sibling
manifest as reachability evidence.
