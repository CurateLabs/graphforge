# GraphForge telemetry runtime

GraphForge owns one telemetry semantic contract in Rust. The implementation is
`graphforge-observability`, re-exported as `graphforge_api::telemetry`; Python,
Node, and `gf telemetry` only translate configuration and lifecycle calls. No
binding defines signal names, attributes, exporters, or retry policy.

## Ownership and activation

Telemetry is disabled by default. Constructing or using the default runtime
creates no provider, worker, timer, DNS lookup, socket, or record allocation.
GraphForge never reads OpenTelemetry environment variables and never installs,
replaces, or depends on a process-global tracer, meter, logger, or tracing
subscriber. An embedding host keeps full ownership of its global stack.

An enabled runtime is an explicit cloneable handle. Clones share one provider;
only the last handle performs best-effort drop cleanup. Callers should use
`force_flush` and `shutdown` explicitly. Both calls are idempotent and bounded
by `lifecycle_timeout`; exporter failure is returned only as a sanitized
lifecycle status and cannot change a graph operation.

## Version 1 semantic contract

- scope: `io.graphforge.engine`, scope version `1`
- traces: `graphforge.operation` (`ns`)
- local-job traces: `graphforge.workspace.job` (`ns`)
- metrics: `graphforge.operation.count` (`{operation}`)
- structured events: `graphforge.lifecycle` (`1`)
- attributes: only `graphforge.operation`, `graphforge.stage`,
  `graphforge.outcome`, `graphforge.failure`, and `graphforge.limit`

All values are Rust enums with finite variants. Operation code receives the
typed `Attributes` builder and cannot attach arbitrary keys. Later M10 stories
may extend this versioned registry; they must not bypass it.

`JobSnapshot` is the typed local-workspace-job envelope. It records monotonic
enqueue, start, and finish boundaries so `queue_delay + active_duration =
total_duration`. Its chronological `JobStage` list must cover active time
exactly without gaps or overlap. Each stage contains wall and wait duration,
an optional finite wait reason, a one-based attempt, normalized outcome, and
bytes or records only when exactly known. Invalid timing is rejected before it
can enter an exporter.

Every stage names only a finite Rust-owned workspace component kind and role.
The initial registry covers the CLI/API facade, discovery, Cypher parser, IR,
relational planner, executor, adjacency, storage, text/vector search, provider,
portable verification/import/export, checkpoint, recovery, publication, and
network transport. Ordered handoffs use finite from/to/kind values and exact
duration/wait/counts. Components not invoked by the job are absent. Backend
names, component instance IDs, participant IDs, catalog/ontology IDs, and
user-defined strings are not representable.

GraphForge telemetry never includes repository, project, graph, account, or
user identity; UUIDs; paths; query text or parameters; properties; credentials,
headers, or tokens; manifests; object keys; or graph/result content. Endpoint
userinfo/query/fragment data is rejected, request headers are redacted from
debug output, and diagnostics expose stable codes only.

## Modes and bounds

`disabled` is the default. `in_memory` provides deterministic snapshots for
tests and embedding validation. `otlp_http_json` sends OpenTelemetry Protocol
JSON to the standard `/v1/traces`, `/v1/metrics`, and `/v1/logs` routes.
Local-job OTLP traces use one coherent enqueue-to-finish root span, child spans
for the ordered active stages, and finite component-handoff events. Queue,
active, stage, wait, attempt, byte, and record values are numeric allowlisted
attributes. Runtime-seeded batch sequences keep valid 32-hex trace IDs and
16-hex span IDs distinct across exports; those ephemeral IDs never become
metric labels or persistent GraphForge identity.

Activation is programmatic only. Configure queue capacity, batch size,
scheduled delay, per-attempt export timeout, lifecycle timeout, retry count,
initial backoff, endpoint, and headers explicitly. Every value has a hard upper
bound; batch size cannot exceed queue capacity. A full queue drops telemetry
and increments a bounded diagnostic counter. Network failure, rejection, and
timeout remain on the owned worker. Retries are finite exponential backoff and
redirects are disabled.

Never put credentials in an endpoint or log a configuration object. Supply
collector credentials through the host's secret store as explicit headers.

## Binding projections

Python and Node export `TelemetryRuntime` with the same disabled/in-memory/OTLP
modes plus `force_flush()` and `shutdown()`. Stable failures begin with
`GF_TELEMETRY_`. The CLI command below validates the same Rust configuration and
exercises both lifecycle calls without printing header values:

```console
gf telemetry --mode disabled
gf telemetry --mode otlp_http_json --endpoint https://collector.example/
```

To inspect one slow job, explicitly enable `in_memory` or local OTLP export,
select its `graphforge.workspace.job` trace, compare queue delay with active
duration, then read the ordered stage and handoff list. The largest stage owns
the attributable wall time; its wait reason distinguishes blocked time from
compute. Usage accounting, automatic activation, hosted collection, billing,
and dashboards are deliberately outside this foundation.

### Hub clone

`gf clone OWNER/REPOSITORY --telemetry-endpoint http://127.0.0.1:4318/`
emits one `clone` job to an explicitly selected local collector. Without that
flag clone remains telemetry-disabled. Read the ordered finite stages from
identity validation through refs/manifest discovery, download, portable
verification, atomic import, reopen, and cleanup. Network-backed discovery and
download stages report network wait; `bytes` on download means bytes newly
received in this run. The separate `resumed_bytes` stage attribute means prior
partial state and is never added to current transferred bytes. Finite
`orchestration` intervals account for local time between named operations;
formatting the already-completed result and bounded exporter shutdown are
outside the workspace job.

Compare stage durations to locate the bottleneck: a delayed object response is
owned by `network_transport`/`download`, while CPU or storage verification is
owned by `portable_verify`/`portable_verification`. Component handoffs include
only components actually reached. Failed clones end once with `failed` plus a
finite failure class; raw `hub.*` text and repository, endpoint, destination,
manifest, digest, UUID, and graph content never enter telemetry.
