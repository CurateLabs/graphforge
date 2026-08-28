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

The reader takes the shared ordinal lock before reading the manifest and
releases it after pinning the manifest and immutable artifacts. The durable
writer takes the project rewrite lock first and the exclusive ordinal lock
second, retaining it through data installation and the manifest switch. A
long-lived immutable read handle therefore cannot starve a writer; it advances
only by opening a newly receipt-authorized generation.

Orphan collection starts from the current authenticated v3 manifest and, when
the ordinal facet exists, requires the opaque authority resolved from a pinned
project generation before authenticating the v4 manifest and artifacts. It
retains the union. Hashing an untrusted manifest or receipt is never treated as
provenance for deciding reachability.

Both facets are persistent graph authority, not `.graphforge-cache/` content.
Construction and canonical v3-to-v4 publication are specified separately by
#969; append, deletion, and compaction are specified by #968.
