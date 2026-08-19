# Resumable graph import

`GraphForge::begin_import_session` creates a Rust-owned, durable import pinned
to the current project generation. Arrow batches are copied to Arrow IPC and
Parquet files are copied into session ownership; callers may then checkpoint,
drop the handle, and resume by UUID.

The resource envelope is explicit in `ImportSessionLimits`. Decoding is capped
by `batch_rows`, source bytes and files have hard limits, and source readers are
bounded by `io_concurrency`. Status reports accepted and rejected rows, bytes,
accepted and pending files, elapsed work, the peak decoded batch, and the
configured concurrency bound.

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
