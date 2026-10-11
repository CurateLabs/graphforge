# Resumable graph import

`GraphForge::begin_import_session` creates a Rust-owned, durable import pinned
to the current project generation. Arrow batches are encoded as Arrow IPC inside
the session; callers may then checkpoint, drop the handle, and resume by UUID.
A registered Parquet file stays where it is: see "In-place Parquet sources".

Resumable construction rows do not supply observation timestamps. Their catalog observations
use the greatest `last_seen` in the authenticated parent runtime catalog across
labels, relation types, and properties. An empty catalog uses Unix epoch zero
(`1970-01-01T00:00:00Z`). These non-null UTC values are deterministic placeholders
for missing observation time, not claims about when an import ran. Nonempty
pre-epoch catalogs retain their negative maximum; the value is never incremented.
Existing entries retain `first_seen`; observed entries increment their counts and
receive the derived `last_seen`, while unobserved entries remain unchanged. New
entries receive the derived value for both timestamps. Direct runtime writes
keep their existing clock-based observation behavior.

The recorded session clock remains in the checkpoint and shape manifest as a
recovery binding. It is not written into newly shaped catalog payloads. Encoded
topology `created_at`/`updated_at` metadata still uses the session clock; this
change establishes cross-process determinism for shaped payloads, not all encoded
metadata. Existing completed shapes remain authenticated and resumable with their
recorded payloads; they are not rewritten to change historical catalog times.

Construction shaping also has recorded partition limits. New sessions allow up
to 4,096 ranges, with a target of 16,384 sampled identities per range. The cut
retains a 256-range floor where the input has enough identities for meaningful
balance checks, and never exceeds the recorded maximum or one range per 16
identities. This is a deterministic planning policy, not a guarantee that skewed
or variable-width records fit. Recorded splitters remain the recovery authority;
worker count and host memory do not select the layout.

`GraphConstructionBudgets::max_partition_bytes` defaults to 256 MiB of accounted
materialization per load. Fixed-width families admit the routed record count
and wire representation before reserving storage; compact details include their
sorting offsets. Finish-time loads run on up to eight workers leased from the
instance's construction CPU admission, or two without one. Whatever the worker
count, a partition is dispatched only while the routed bytes of every loaded or
loading partition not yet consumed stay within two partition budgets, so at
most two budget-sized partitions are materialized at once. Boundary seals and
finish-stage segment retirement also run on leased lanes. Each lane does only
filesystem work, and the coordinator charges the evidence in partition or
segment order, so the evidence does not depend on the schedule (#1448).
Canonical encoding also leases from the instance admission (#1600). Up to eight
workers compress Parquet batches while the coordinator reads the next batch.
Read-ahead is bounded by `max_batch_bytes` of queued Arrow buffers and one more
queued job than workers. The coordinator replays each compressed write sequence
onto the existing durable writer in input order, preserving bytes, cache-release
boundaries and receipts. Worker buffers never become recovery authority.

Adjacency construction uses one admitted decoder and a two-batch channel,
an admitted sort worker alongside the coordinator, and independent CSR jobs for
each relation and direction. Spill accounting and compaction retain their serial
order. CSR results are collected in relation/direction order and the manifest is
written last. Workers finish before cancellation or an error returns; spill and
unpublished output cleanup follow the existing construction protocol.

Arrow property-row partitions load serially and charge decoded buffers,
concatenation/reordering capacity, nested child capacity, UUID keys and indexes.
Their conservative accounting can refuse a partition even when a less
conservative representation might fit. Unsupported Arrow representations are
refused rather than assigned an unproven estimate. Sealed IPC spills are
authenticated with bounded reads before Arrow can allocate from their metadata.

A fixed-width partition exceeding its recorded budget is not materialized.
Increasing the cut cannot split one node's hub group, so a large hub can exceed
the budget even at 4,096 ranges. Under [ADR 0047](../../adr/0047-over-budget-partitions-and-instance-cpu-budget.md),
a partition without a detail codec (identities and the endpoint families) is
sorted externally instead: its
load worker sorts it into checksummed runs of at most `max_partition_bytes`,
and the coordinator merges them into the same shaped bytes. The recorded
`max_external_partition_bytes` (default 64 GiB) bounds one partition's run
scratch. A partition above it is refused with "exceeds recorded external
budget" before any run is written; zero keeps the earlier refusal. Runs are
construction artifact temporaries, never resume authority, and recovery
reclaims any a crash leaves. Detail-codec partitions and Arrow property-row
partitions are still refused before their retained load is allocated. The
failed private shape remains unpublished and reopening retains the same limits.
The materialization budget is separate from source decoding, routing/writer
buffers, allocator metadata, page cache and the process memory limit; it is not
a whole-process RSS promise.

The new budget fields have stable defaults for historical checkpoints and omit
default values when serialized, except `max_external_partition_bytes`, which
is always written because its absence means zero. Opening a checkpoint with
the exact former
256-partition default through today's default retains its recorded budgets.
Likewise a checkpoint recorded before `max_external_partition_bytes` reads it
as zero and keeps its refusal when resumed with today's default;
other explicit budget mismatches remain errors. Completed shapes and their
recorded authority are not rewritten.

The resource envelope is explicit in `ImportSessionLimits`. Decoding is capped
by `batch_rows`, source bytes and files have hard limits, and source readers are
bounded by `io_concurrency`. Status reports accepted and rejected rows, bytes,
accepted and pending files, elapsed work, the peak decoded batch, and the
configured concurrency bound.

## Initial builds use the bulk builder

When `validate` runs on a session pinned to an empty project, it does not stage
anything. Sources are read in place, by row group and
in parallel; each construction batch passes the same intake normalization an
append gets, then the builder (ADR 0058) ranks nodes and edges in memory and
emits the whole encoded generation:

1. **Plan** from the source footers.
2. **Nodes** are decoded, validated, ordered (a source already in UUID order
   skips the sort), checked for duplicates and ranked: `node_id` is the rank.
3. **Edges** resolve endpoints through a node-UUID index, then order and rank:
   `edge_id` is the rank. A missing endpoint, an endpoint that is an edge, an
   edge UUID that equals a node UUID and a repeated UUID are refused.
4. **Emit** writes the runtime catalog, node and edge Parquet windows, property
   overlays, the v4 ordinal artifacts, and the CSR shards.
   Each artifact is hashed (SHA-256, XXH64) from the bytes written once.

Before native Parquet footer parsing, admission checks the footer counts and
reserves the Thrift schema-tree envelope; inferred Arrow schema and Parquet-field
allocations (including `ARROW:schema` hints) are also admitted before Arrow schema
inference. The page scan then inventories page headers, dictionary bytes, nested
shape and values that headers cannot size. Before a task decodes, it reserves its
page, validator and decoder workspace, selected row-group indexes, overlapping
Arrow arrays, and normalization/duplicate-check workspace from one
`SourceWorkspace` shared by registered sources. The reservation remains held
through the task's output callbacks and drops when the task returns, errors, or is
cancelled. Footer, schema, scan inventory and task allocations are charged before
the corresponding native parser, vector, or decoder allocates them.

`batch_rows` defines logical import batches and their operation IDs. A Parquet
logical batch may be split into smaller physical row pieces to fit the intake
window. `PhysicalBatchMap` maps those pieces back to the same logical batch and
assigns contiguous row ordinals; the pieces share the logical batch's operation ID
and duplicate-UUID set. A decoded piece is copied into buffers of exactly its
size before it is normalized, since the native reader's buffers grow by doubling
and the builder charges a batch what its buffers hold; the task's reservation
admits the reader's capacity beside that copy. Selected row groups remain in ascending source order and
their retained indexes are included in task admission. A batch that cannot fit
even as a physical piece is refused with a typed resource limit
(`GF_RESOURCE_LIMIT`) before decode allocation and counted as rejected rows. Arrow
IPC schemas and message bodies use their separate plan-time size inventory before
its checked reader opens. Normalization validates property cells without
retaining a value map per row.

`commit` then installs and publishes the encoded inventory exactly as for any
other session. The builder's receipt (`construction.bulk_build`) reports wall
time, process CPU, effective cores, logical and physical write bytes and peak
RSS per pass.

The route is a function of the session's pinned parent, never of live memory: a
session pinned to an empty project is an initial build and always runs the
builder, in memory or, when the estimate exceeds the budget, on scratch files;
nothing is recorded. Nothing is staged, so there is no durable prefix: a crash,
cancellation or error discards the attempt and the next `validate` reruns from
the sources. Appends keep the staged path described below. A session that
holds staged chunks of an initial build, made by an earlier 0.6.0-dev build, is
refused with an instruction to restart the import, and so is a session in the
earlier copied-source format. Initial builds made through
`GraphConstructionSession::append_*` (the Rust facade; the bindings'
`add_nodes`/`add_edges` publish atomically and import sessions register
sources) spool each accepted chunk as one Arrow IPC file, synced and renamed
into place, which survives a crash and resumes; sealing builds from the spool
with the same builder, in memory or, when the estimate exceeds the budget, on
scratch files, the node tables included when they do not fit. Every spooled
chunk is authenticated against the digests acknowledged at acceptance before it
is read, so a file that changed afterwards, even at the same size and still
valid Arrow IPC, fails the build. A budget below the fixed workspace is refused
before decoding and keeps the route. The route is recorded before any work and
read back on retry. `GraphForge::begin_staged_graph_construction` is the append
lifecycle and refuses an empty project. Node and edge counts are limited to
2^32 - 2.

Validation processes node sources before edge sources. Each batch is normalized
through the public bulk contract and flushed into a private graph tree. A
session-owned sorted UUID index is merged once per bounded batch; candidate
checks use binary probes against that index and the authenticated base index.
Duplicate identities
and missing endpoints are therefore checked against both the committed graph
and every earlier staged batch without loading the global UUID population.
Every completed batch advances the versioned manifest. If a process stops
between the graph flush and manifest replacement, resume recognizes the fully
present batch as an idempotent replay.

Fixed-schema topology rewrites stream prior Parquet through bounded 64K-row
batches rather than concatenating the complete accumulated table in Arrow.
Opening a writer recovers node and edge surrogate maxima from only each file's
final bounded row group. These bounds prevent resident topology state from
growing with earlier batches. The current private staging tree still recopies
prior rows while appending; append-only/shard-staged linear I/O and disk growth
is the separate #901 close gate and must not be inferred from the memory bound.

`commit` requires every source to be validated. It pins the original generation,
captures the staged graph, runtime catalog, and UUID indexes, and publishes them
through one recoverable project-generation transition. Cancellation before
publication and every validation or staging error leave `CURRENT` unchanged.
`abort` removes graph and source artifacts but preserves an observable terminal
manifest; cleanup failures are marked `quarantined`. Operators can call
`cleanup_stale_import_sessions` with an age threshold to abort abandoned
non-terminal sessions deterministically.

## In-place Parquet sources

`register_parquet` copies and writes nothing under the session. It records the
source's canonical path, native file identity (device and inode on Unix), size,
modification time and Parquet footer length and SHA-256, and refuses a file that
is not plain Parquet. Every later read re-establishes that identity first, checks
the open file and its name again before each batch and at the end of the pass, and
refuses with a typed error if the source is missing (`GF_NOT_FOUND`) or was
replaced, resized, modified or rewritten (`GF_IDENTITY_CONFLICT`).

Both construction paths read the source in place, and the read that decodes a
source is the read that digests it. A custom Parquet `ChunkReader` offers every
range the decoder asks for, including the footer, to the source's digest as it is
read. SHA-256 is sequential, so bytes that arrive in file order are hashed at once;
bytes that arrive early are held (the held runs coalesce a streamed column chunk)
until the gap before them is filled. The bulk builder starts a source's tasks in
file order (`claim_in_order` in `graphforge-storage`), where a static split would
start each worker at a far-apart position. That keeps the lead over the hashed
prefix small in the usual case, which `pending_limit` sizes as the workers times
the largest task, with a 64 MiB floor and a 1 GiB ceiling, never more than a
sixty-fourth of the memory budget (counted in the reader's planned workspace). It
does not bound the lead: a slow task holds the prefix back while the others run
ahead, and a range beyond the bound is dropped and read again. Bytes the decode never asks for (the
page index of a file that has one) are read once when the digest completes;
`source_read` reports those as `reread_bytes`, and every byte offered as
`observed_bytes`. `tasks_are_claimed_in_index_order_on_every_pass` fails if the
claim order regresses. The staged path decodes a source sequentially, so its digest
holds at most the row group being decoded: the columns of a row group are read side
by side, and all but the first wait for the one before them.

The identity pin (device, inode, size, modification time) is the change detector;
the digest is provenance. What the receipt's `sha256` guarantees is the SHA-256 of
the file content read under that pin: the bytes the decode consumed, plus any range
the bound dropped or the decode never asked for, read from the file when the digest
completes. The first complete pass records it in the session manifest and, once
every source is staged, in the import receipt (`source_provenance`); a later
complete read of the same source must produce the same digest. A rewrite that
preserves device, inode, size and modification time is not detected, and the
digest then describes whatever was read. Three consequences are deliberate:

- The pin is re-checked whenever progress is reused. A resumed staged import opens
  the source through the pin before it skips the batches already staged, so an
  edit between a stop and the resume is refused. The batches staged earlier and
  the digest of the final pass are not tied to each other beyond that pin.
- A source is pinned until it is fully consumed. The frame that marks a source
  staged also carries its complete SHA-256, so a source whose staging a stop
  interrupted is re-opened through the pin, while one that finished is not read
  again: its staged rows are exactly the bytes its digest names, and an edit or
  deletion afterwards changes nothing that is published. The same holds for
  `commit` after `validate`, which publishes artifacts already built and reads no
  source.
- An initial bulk build restarts rather than resumes. A sealed construction
  session is reused only if the digest of every in-place source it was built from
  is already recorded; otherwise it is discarded and the build runs again, so a
  stop between pinning the encoded inventory and recording the digests cannot pair
  one file's graph with another's digest.

### Sessions an earlier version began

A session written before sources stayed in place (manifest format 2) holds its own
copy of each Parquet source under `sources/` and recorded no identity or digest
for it. It resumes, validates and appends as before, reading the copy it owns:
nothing outside the session can refuse that build, `abort` removes the copy with
the session, and the receipt has no digest for it. Registering an in-place source
into such a session raises its manifest to format 3, which an earlier version
refuses.

Arrow batches passed to `append_arrow` are not an external file, so they are
still encoded into the session. `abort` removes only session-owned artifacts and
never touches a registered source.

Registered paths may not contain `..`; the source itself may not be a symlink
and must be a regular file. Schema, corrupt-file, UUID, endpoint, resource,
and generation failures are returned as structured GraphForge errors.
