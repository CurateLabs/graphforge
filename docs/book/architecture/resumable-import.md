# Resumable graph import

`GraphForge::begin_import_session` creates a Rust-owned, durable import pinned
to the current project generation. Arrow batches are copied to Arrow IPC and
Parquet files are copied into session ownership; callers may then checkpoint,
drop the handle, and resume by UUID.

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

When `validate` runs on a session pinned to an empty project and nothing is yet
staged, it does not stage anything. Sources are read in place, by row group and
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
   overlays, the UUID membership and v4 ordinal artifacts, and the CSR shards.
   Each artifact is hashed (SHA-256, XXH64) from the bytes written once.

`commit` then installs and publishes the encoded inventory exactly as for any
other session. The builder's receipt (`construction.bulk_build`) reports wall
time, process CPU, effective cores, logical and physical write bytes and peak
RSS per pass.

The first `validate` chooses the route once and records it in the manifest, so
later calls (and reruns after a crash) never re-decide it from live memory. Nothing
is staged, so there is no durable prefix: a crash, cancellation or error
discards the attempt and the next `validate` reruns from the sources. Appends,
sessions that already staged chunks, and initial builds made through
`GraphConstructionSession::append_*` keep the staged path described below. The
builder holds the ranked graph in memory; an initial build that does not fit
needs the scratch path planned in #1881. Node and edge counts are limited to
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

Registered paths may not contain `..`; the source itself may not be a symlink
and must be a regular file. Schema, corrupt-file, UUID, endpoint, resource,
and generation failures are returned as structured GraphForge errors.
