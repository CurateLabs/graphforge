---
title: "ADR 0059: Edge properties are read through a derived, authenticated shard index"
adr: "0059"
status: "Accepted"
date: "2026-10-12"
superseded_by: null
revisit_when: "An edge property read still scales with the route after the index exists, a per-hop edge predicate needs positional access that a UUID lookup cannot give, property fragments adopt per-block authentication that makes the index redundant, or the index's manifest outgrows a single read per statement"
---

# ADR 0059: Edge properties are read through a derived, authenticated shard index

**Status:** Accepted

**Implementation:** #1984 (second wave, slice S4b). Edge identity by dense
ordinal (slice S4a) is independent and needs no ADR: it adds no persisted
artifact.

**Related:**
- ADR 0004 / ADR 0037 (derived indexes are rebuildable and never authoritative)
- ADR 0038 (determinism at the publication boundary)
- ADR 0045 / ADR 0049 (payload authentication; a cached first-touch checksum cannot prove a whole fragment)
- ADR 0058 (initial builds run on a bulk builder; the index is built beside the adjacency CSR)
- #1983 (the property-equality index this record follows)

## Context

A traversal that returns or filters `e.timestamp` needs the properties of a few
hundred edges out of millions. Edge properties live in the property overlay:
fragments of up to 4 MiB, sorted by edge UUID, each authenticated as a whole
object. `build_edge_prop_children` resolves the hop's edge UUIDs through
`read_property_targets`, which for every fragment of the route opens the object,
checks its XXH64 over every byte, reads the footer, and decodes the UUID column
of each surviving row group. Footer-statistics pruning removes row groups, not
the open and the checksum, so the cost scales with the route, not with the
frontier. Measured at SF1 (#1982 design pass, host load 12 to 21):

- FinBench, one hop of `count(e.timestamp)` from a frontier of about 10²
  accounts: 5.5 s, against 2.4 s for the same hop without the property (+3.1 s).
- TCR9 (six one-hop `OPTIONAL MATCH`es with edge properties): 56% of its
  1.1 s in `build_edge_prop_children`; TCR6: 56% of 15 s.
- LDBC IC1: `KNOWS.creationDate` materialised into `rels` for 245,000 paths.

A read-side-only fix (cache the footer walk, prune row groups harder) keeps one
open and one whole-object checksum per fragment per statement. The route has
tens to hundreds of fragments.

## Decision

Edge properties get a derived index under `indexes/edge-properties/`, in the
same family as the adjacency CSR and the property-equality index.

### Layout

One manifest and a directory of packs per edge property route:

- `<stem>.epx`, the manifest: an XXH64 line over the JSON body, then the JSON.
  It records the format, the route, a digest of the route's source objects
  (path, length and XXH64 of every object the generation inventory names), the
  route's property columns, and for every shard its first and last edge UUID,
  row count, and a seal (pack number, offset, length and XXH64) for its UUID
  blob and for each of its column blobs.
- `<stem>.packs/<n>.epk`, a pack: consecutive blobs, up to 4 MiB, one write.
  A blob is the sorted `edge_uuid` column of a shard, or one property column of
  it, as a Zstd-compressed Arrow IPC stream of one batch with no field
  metadata. A shard holds at most 4,096 rows and at most 1 MiB per column.

The columns are separate blobs so a statement reads only the properties it
demands: a route that also stores a wide `memo` costs a `timestamp` lookup
nothing, and a damaged `memo` blob refuses only the statements that demand
`memo`. The values are the route's own Arrow values, so every type the overlay
stores is stored; nothing is re-encoded.

#### Why packs and not a file per blob

Every durable open hard-links every artifact the generation inventories into a
private workspace (#1992), at about 200 microseconds and a hundred `statx` calls
per artifact. A file per UUID list and per column was 8,339 files for the SF1
FinBench project's edge route (3.1 million rows) and 4,901 for the SF1 IC
project's (4.0 million rows), and added 1.5 to 3 s to every open of an indexed
project, more than the lookup saved on a statement that reads few properties. A
pack holds about two hundred blobs (SF1 FinBench: 8,338 blobs in 37 packs; SF1
IC: 4,900 in 23), so the route is a few dozen files. The seal of a blob, not the
name of a file, authenticates what a lookup reads, so the guarantee of the
per-file layout is kept: a lookup reads the byte range of one blob and checks
its length and XXH64.

### Authority, authentication, currency

- The index is derived and rebuildable. The fragments remain the authority.
- The manifest is first-touch (checked once per hydration, like the adjacency
  manifest); packs are self-authenticating (hydration neither reads nor
  registers their bytes, only links them) and every lookup checks the length and
  XXH64 of each blob it reads against the manifest, whichever pack holds it. A
  failure is `GF_VALIDATION`. The overlay is not substituted for a corrupted
  index, and a read never rebuilds one.
- An index is current only while its source digest equals the route's. A
  mutation appends a fragment and changes the digest, so the index goes stale
  and the overlay answers, exactly as before the index existed, until an
  explicit rebuild.
- Only routes with no tombstone and a single type per column are indexed.

### Build

- Initial bulk construction cuts the index from the fragments as they are
  encoded, writing each pack once, as it fills, as an artifact of the same
  generation (never reading the published fragments back, which would copy
  multi-object fragments to scratch and break write-once accounting). The
  manifest is written last. A pack never passes the 4 MiB property object
  bound, so the write-once accounting of the generation holds.
- `GraphForge::rebuild_property_indexes` rebuilds both the node equality index
  and this one from the current generation's newest-wins state in UUID order
  and publishes a new generation. Both directories are replaced under the
  publication lease, and a failed publication restores the prior trees.
- Bytes are a pure function of the route's content (no clock, UUID or process
  identity; no field metadata).

### Use

`read_target_edge_properties`, the one place the executor reads the edge
properties of a set of edges from a route, asks the index first and falls back to
the property overlay for a route without a current index. One-hop expansion and
variable-length expansion both resolve their edge properties through it
(`load_edge_prop_source`), so both use the index; the memory charges of the
resolved source are unchanged (#1995). Routes are independent, so a hop may read
one route from its index and another from the overlay.

The reader is a small API for later consumers. `graphforge_storage::edge_property_index::lookup_edge_properties`
takes a route, a sorted set of edge UUIDs and the property names, and returns
the rows the route holds for them as Arrow arrays; the executor's
`indexed_edge_property_batches` wraps it in the shape the overlay read returns.
It is keyed by edge UUID, not by CSR entry offset, so a per-hop predicate that
runs while a traversal still holds only `edge_id`s resolves their UUIDs first
(an ordinal-selected read of the edge file's `edge_uuid` column).

The reachability mode of variable-length expansion (S6b, #1984) is the third
consumer. Its pushed `ALL(e IN r WHERE p)` predicate does not need positional access:
for each search level it resolves the UUIDs of that level's frontier relationships,
calls the lookup once for them (in batches of at most 65,536 relationships), and
evaluates `p` over the returned columns. The lookup falls back to the overlay exactly
as above, so a stale or missing index changes cost and never the answer.

Demand pruning of the `rels` list (not materialising properties a statement
never reads) is separate work: the list still carries every property of the
relation, now read from the index.

## Alternatives considered

- **Align the values to the CSR's entry order** so a CSR row offset is the
  property offset. It removes the UUID lookup and needs no edge read at all
  for a hop that returns only a property. Rejected for this slice: the CSR is
  built per relation and direction, so every property column would be written
  per direction; the offset must be plumbed from the CSR shard decode through
  `AdjacencyProvider` into every consumer; it applies only where the persisted
  CSR is the serving structure (not the scan-built provider or a mutated
  project's delta segments); and bulk construction would have to resolve each
  property row to its edge's rank, which the over-budget plan does not hold. The
  UUID-keyed shards give the same asymptotic result (reads proportional to the
  frontier's shards) with none of that coupling. The per-hop predicate of the
  reachability mode (S6b) was the case that might have needed positional
  access; it resolves UUIDs once per search level and did not.
- **Align the values to `edge_id`.** Same rank-resolution problem at build time
  as the CSR order, and `edge_id` is not stable across mutation.
- **A read-side-only fix.** Keeps a whole-object checksum per fragment per
  statement.

## Consequences

- A project published before the index, one built by a staged append, and a
  route changed since the build answer through the overlay until
  `rebuild_property_indexes`. Nothing breaks and nothing is rebuilt silently.
- A route poisoned mid-build (a tombstone, an unsorted fragment, a conflicting
  column type, a single value over 1 MiB) is not indexed; packs written before
  the decision stay in the generation as unreferenced files.
- The index adds about the size of the indexed columns plus 16 bytes per row.
- The manifest is read whole by every lookup that touches its route: about 100
  bytes per blob (a seal and its share of the shard's bounds), so about 1 KB per
  shard: 0.8 MB for the 3.1 million rows of the SF1 FinBench edge route and 0.5 MB
  for the 4.0 million rows of the SF1 IC route.
- A lookup reads the byte range of a blob out of its pack, not the pack.
