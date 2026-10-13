---
title: "ADR 0059: Edge properties are read through a derived, authenticated shard index"
adr: "0059"
status: "Accepted"
date: "2026-10-12"
superseded_by: null
revisit_when: "An edge property read still scales with the route after the index exists, a per-hop edge predicate needs positional access that an edge-id or UUID lookup cannot give, a mutation preserves edge ids so the id join need not be rebuilt after a topology write, property fragments adopt per-block authentication that makes the index redundant, or the index's manifest outgrows a single read per statement"
---

# ADR 0059: Edge properties are read through a derived, authenticated shard index

**Status:** Accepted

**Implementation:** #1984 (second wave, slice S4b; slice S11 adds the edge-id
key described under Use). Edge identity by dense ordinal (slice S4a) is
independent and needs no ADR: it adds no persisted artifact.

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
  it, cut into Zstd-compressed Arrow IPC pages with no field metadata (see
  Pages). A shard holds at most 4,096 rows and at most 1 MiB per column.

A shard may also carry the surrogate `edge_id` of each of its rows as a further
blob (a paged non-null `UInt64` column, in the shard's UUID order) and its
smallest and largest id, and the manifest then
records `topology_digest`, the digest of the route's canonical edge topology
files that the ids were joined from. All three are optional manifest fields, so
an index without them is still `graphforge-edge-property-index/1` and reads
exactly as before.

The columns are separate blobs so a statement reads only the properties it
demands: a route that also stores a wide `memo` costs a `timestamp` lookup
nothing, and a damaged `memo` blob refuses only the statements that demand
`memo`. The values are the route's own Arrow values, so every type the overlay
stores is stored; nothing is re-encoded.

#### Pages

A blob used to be one Zstd Arrow IPC batch of the whole column, so a lookup of
three rows decoded all 4,096 of a shard's, and Zstd decode of the pack columns
was the largest share of an edge-property read (23.6% of the SF1 IC5 profile
for the `joinDate` read alone). A blob is now cut into pages of 128
rows, each one IPC record batch message compressed with Zstd, and ends with a
directory:

```text
[page 0] ... [page n-1][schema][entry 0] ... [entry n-1][schema length u32]
```

An entry is the page's byte length (`u32`), its XXH64 (`u64`) and, for the UUID
and id columns, the least and greatest key in the page (16 and 8 bytes,
big-endian). Page offsets are the running sum of the lengths and the directory
must account for every byte of the blob, so they are not stored. The schema is
written once, in the directory, not per page.

The manifest records `page_rows`, so the number of pages of a shard, and with
it the length of a directory, is known before the directory is read. A
lookup reads the directory (one read of its tail, a second only when the
schema outgrows the 256 bytes the first read allows for), chooses the pages
that can hold its keys, and reads and decodes only those; adjacent pages
coalesce into one read. UUID pages are disjoint and strictly ascending, so a
UUID names its page; ids are in UUID order and not sorted, so an id lookup reads
every page whose bounds contain the id. A property column carries no bounds and
is read at the pages of the rows the key lookup found.

The seal of a blob covers its directory, and the directory carries the XXH64
of every page, so the chain from the manifest to a byte a lookup decodes is
unbroken and a page is checked when it is read. The cost is that damage inside
a page is found by the statements that read that page, not by every statement
that touches the blob: a statement whose rows are in an undamaged page of the
same blob is answered. This narrows the old guarantee only in blast radius, not
in what is trusted; a statement never decodes a byte that no checksum covered,
and a mismatch is `GF_VALIDATION` as before.

The format name stays `graphforge-edge-property-index/1`: the layout before pages
is superseded in place, not versioned. `page_rows` is a required manifest field, so
the manifest of an index built before pages does not decode, and the reader refuses
it as it refuses every manifest it cannot use (damage, an unknown format, an invalid
page size): `GF_VALIDATION`, naming the manifest and `rebuild_property_indexes`. It
is not treated as absent or stale, so a statement that reads edge properties through
the index of such a project fails instead of silently answering from the overlay.
There is no whole-blob reader. The index is derived data and the fragments remain
the authority, so a project whose index predates pages runs
`rebuild_property_indexes` once, and the rebuilt index is paged. Nothing rewrites it
on read.

Page size trades decode work against bytes: each page is its own Zstd frame, so
smaller pages decode less per target and compress worse. IC5 touches about one
page per target (a shard lookup wants about 7 of the 32 id pages at 128 rows),
so the pages are sparse and the target is the unit that matters. Measured on the
SF1 IC project's edge route, IC5 binding 1, CPU (user plus system) under a
shared host at load 14 to 23, and the bytes of the route's index:

| rows per page | whole blob (before) | 64 | 128 | 256 | 512 |
| --- | --- | --- | --- | --- | --- |
| IC5 CPU, seconds | 45.8 | about 40.8 | 38 to 40 | about 40 to 42 | 43.1 |
| index bytes | 97.5 MB | 230 MB | 160 MB | 126 MB | 110 MB |

128 rows is the least CPU. 256 gives back about half of the growth for about 5%
more CPU, and 512 loses most of the gain. The count is `PAGE_ROWS` and is
recorded in each manifest as `page_rows`, so changing it changes no reader.

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
  registers their bytes, only links them) and every lookup checks the length
  and XXH64 of the directory of each blob it reads against the manifest, and of
  each page it decodes against the directory, whichever pack holds it. A
  failure is `GF_VALIDATION`. The overlay is not substituted for a corrupted
  index, and a read never rebuilds one.
- An index is current only while its source digest equals the route's. A
  mutation appends a fragment and changes the digest, so the index goes stale
  and the overlay answers, exactly as before the index existed, until an
  explicit rebuild.
- The ids of an index are addressable only while its `topology_digest` equals
  the digest the generation inventory records for the route's topology files
  (path, length and content XXH64 of each) and every shard carries ids. A
  mutation that writes topology changes that digest without touching the
  property fragments, so the UUID key still answers and the id key is refused
  until a rebuild. Edge ids are not stable across mutation, which is why the
  digest, not the ids, is what the index trusts.
- Only routes with no tombstone and a single type per column are indexed.

### Build

- Initial bulk construction cuts the index from the fragments as they are
  encoded, writing each pack once, as it fills, as an artifact of the same
  generation (never reading the published fragments back, which would copy
  multi-object fragments to scratch and break write-once accounting). The
  manifest is written last. A pack never passes the 4 MiB property object
  bound, so the write-once accounting of the generation holds.
- The ids are joined while the index is cut: the tap resolves each shard's UUIDs
  against the same construction's topology pages (the resolver reads only the
  UUID page statistics and the pages that can hold a candidate) and records the
  topology digest computed from the installer artifacts. A route is left
  without ids when its topology cannot be joined: a row with no edge, a UUID
  page layout whose stabbing number is too high to resolve by point reads
  (topology written by mutation is not in UUID order), or a property route
  whose name has no topology of its own (a qualified owner). Such a route keeps
  the UUID key.
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

A second entry point, `lookup_edge_properties_by_id`, takes the surrogate
`edge_id`s a traversal already holds and returns the same rows keyed by id. It
reads the manifest, the id blob of each shard whose id range holds a target, and
the demanded column blobs of the shards with a match; it never reads an
`edge_uuid`. It answers `NotAddressable` when the manifest is missing, predates
ids, or its `topology_digest` is not the route's current one, and the caller
then keys the read by UUID. One-hop expansion (`expand_single_hop_chunk`) uses
it when the statement demands, of the edge variable, only its `edge_id` and
properties: the properties of the hop's edges are fetched by the ids the CSR
entries carry, no edge file is opened, and the `edge_uuid` column is not read.
That is what makes a hop's property read cost the frontier's shards and not the
edge file. The choice is made per chunk, so a hop whose route is not
addressable reads the UUID key exactly as before. Variable-length expansion, the
reachability mode and any statement that demands another stored field of the
edge (its UUID, its timestamps, a whole-edge return) stay on the UUID key.

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
  access; it resolves UUIDs once per search level and did not. S11 later
  reached the no-edge-read result for a one-hop property read through the
  edge-id key described under Use, without the CSR coupling.
- **Align the values to `edge_id`** (make the id the only key). Same
  rank-resolution problem at build time as the CSR order, and `edge_id` is not
  stable across mutation. S11 takes the part of this that survives: the UUID
  stays the key, and the build also joins each row to its topology `edge_id`
  and records the topology digest it joined against, so a hop that holds ids
  skips the edge file. The join happens where the topology is already installed
  (bulk construction) or readable (rebuild), costs one page-pruned resolve per
  fragment, and is withdrawn, not guessed, when a row has no topology row or the
  topology's UUID pages are too entangled to resolve by point reads.
- **A read-side-only fix.** Keeps a whole-object checksum per fragment per
  statement.

## Consequences

- A project published before the index, one built by a staged append, and a
  route changed since the build answer through the overlay until
  `rebuild_property_indexes`. Nothing breaks and nothing is rebuilt silently.
- A project whose edge-property index was built before pages is not that case:
  its manifest does not decode, so a statement that reads edge properties through
  the index fails with `GF_VALIDATION` until `rebuild_property_indexes` replaces
  the index. Statements that read no edge property through the index are
  unaffected.
- A route poisoned mid-build (a tombstone, an unsorted fragment, a conflicting
  column type, a single value over 1 MiB) is not indexed; packs written before
  the decision stay in the generation as unreferenced files.
- The index adds about the size of the indexed columns plus 16 bytes per row
  before compression. Paging costs more on top, because a Zstd frame per 128
  rows compresses worse than one per 4,096: the edge-property route of the SF1
  IC project grew from 25 files and 97.5 MB to 40 files and 159.9 MB (+64%), and
  the SF1 FinBench project's from 39 files and 158.3 MB to 74 files and 300.6 MB
  (+90%), and `rebuild_property_indexes` took 33 s instead of 25 s on IC. Pages of
  256 rows would grow IC by 30%; a shared Zstd dictionary or coarser pages for
  columns that are read whole are the ways to trade the bytes back.
- The manifest is read whole by every lookup that touches its route: about 100
  bytes per blob (a seal and its share of the shard's bounds), so about 1 KB per
  shard: 0.8 MB for the 3.1 million rows of the SF1 FinBench edge route and 0.5 MB
  for the 4.0 million rows of the SF1 IC route.
- A lookup reads the directory of a blob and the pages it needs out of its pack,
  not the blob and not the pack, and decodes only those pages. Each lookup
  re-reads the directories it needs; nothing caches a directory or a decoded
  page across lookups, so a page that several lookups of one statement want is
  read and decoded once per lookup. A cache is a follow-up and needs its memory
  accounted in the query pool.
- Ids add 8 bytes per row before compression and 16 bytes per shard of bounds.
  Adding them changes no existing byte of an index without ids.
- A topology-only mutation (an edge with no properties) leaves the UUID key
  current and withdraws the id key until `rebuild_property_indexes`; the
  statement is answered by the UUID key meanwhile and returns the same rows. The
  UUID path costs the edge file's `edge_uuid` column, so a project that mutates
  topology should rebuild after a batch.
- The manifest lists every shard, so it grows with the route in both keys. Shard
  bytes read are bounded by the frontier; the manifest read is not. When the
  manifest outgrows a single read per statement the index needs a second level
  (see `revisit_when`).
- The manifest's `topology_digest` differs between two constructions of the same
  input because topology rows carry a wall-clock `created_at`; the pack bytes and
  every other manifest field are identical.
