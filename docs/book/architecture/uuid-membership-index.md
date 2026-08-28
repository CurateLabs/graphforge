# UUID identity authority facets

GraphForge keeps two generation-coupled authorities under
`topology/uuid-membership/`. They share a topology generation, not a manifest
schema or digest.

## UUID membership facet

`manifest.json` is the existing v3 authority for both node and edge UUID
membership and node `UUID -> node_id` resolution. Existing endpoint-resolution,
construction, and mutation consumers continue to authenticate this facet
unchanged. Its immutable runs and `topology-receipt.json` remain reachable
until the v3 manifest no longer selects them.

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
manifest is absent, discovery validates that current v3 authority is canonical
before returning `RebuildRequired`. When the ordinal path exists, discovery
reports it as present without trusting its contents. Authenticated open then
requires the ordinal digest selected by the project receipt. A malformed,
substituted, or generation-mismatched ordinal facet fails closed and never
falls back to v3.

The explicit rebuild API constructs v4 only from canonical topology and returns
an aggregate `CanonicalTopology` disposition with generation, identity/range,
artifact-byte, fixed-block, buffer, temporary-run, and fsync evidence. It never
opens a v3 reverse run as migration input. Durable-rewrite recovery either
retains the prior v3-only authority or completes the receipt-bound v4 facet;
there is no mixed-version read state.

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
per chunk or substitute the v3 membership LSM.

Orphan collection starts from the current authenticated v3 manifest and, when
the ordinal facet exists, requires the opaque authority resolved from a pinned
project generation before authenticating the v4 manifest and artifacts. It
retains the union. Hashing an untrusted manifest or receipt is never treated as
provenance for deciding reachability.

Both facets are persistent graph authority, not `.graphforge-cache/` content.
Construction and canonical v3-to-v4 publication are specified separately by
#969.

### Incremental ordinal publication

An ordinary topology generation publishes one UUID-sorted forward delta, the
maximal contiguous ranges from its node-ID-sorted delta, and one sorted unique
tombstone delta. It never decodes or rewrites canonical topology and it rejects
zero IDs, duplicate mappings, nonmonotonic surrogate allocation, reuse, range
overlap, and tombstones that do not name retained live authority before any
transaction entry is staged.

Forward files are canonical and strictly UUID-sorted within each generation.
Their descriptors are strictly generation-ordered; they are not required to be
globally concatenation-sorted. The reader authenticates every run and compares
the aggregate forward mapping commitment with the aggregate ordinal mapping
commitment. Historical UUID and surrogate uniqueness is also proved by the
coupled authenticated v3 participant in the same topology transaction.

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

Orphan maintenance runs only from the union of selected authenticated v3 and v4
authority. It removes an unreferenced single-link artifact by retained identity,
defers linked or over-budget candidates, and never treats an untrusted sibling
manifest as reachability evidence.
