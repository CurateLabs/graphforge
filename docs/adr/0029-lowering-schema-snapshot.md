# ADR 0029: Compile against immutable schema and catalog data

## Status

Accepted for #1148, the first slice of #1006.

## Decision

`graphforge-ir::LoweringSnapshot` contains checked identity/name maps, semantic
composition and route assumptions, and Arrow schemas. It contains no source
paths, executable providers, or graph rows. Storage constructs the snapshot
before relational compilation using the existing catalog and schema discovery.
This uses existing dependencies: storage already consumes IR, and IR already
owns Arrow and checked semantic identities.

Relational lowering owns a copy of these facts. Schema-only explanation retains
its historical logical table sources; dataset lowering emits the existing read
descriptors and semantic contracts. Explicit write lowering enables compiling
write operations, but execution alone admits write authority. Compile mode does
not become machine authority embedded in the logical plan.

Buffered node scans combine an ordinary logical node scan with the statement's
pending rows. They no longer load stored topology during lowering. Execution
binds and reads the stored side under its existing resource context.

Schema metadata follows the existing read-source contract: only transient
property live-count metadata is omitted from logical identity. Other metadata
and physical/public schemas retain their existing semantics.

## Remaining parent work

Runtime path hydration remains in rel for this slice. Specifically,
`expr::bind_graph_read_expression` revalidates property schemas at physical
binding; `gather_path_node_labels` and `gather_path_node_props` still invoke
storage visitors at execution. Rel also imports shared storage schema constants.
The unused `StorageProvider` stubs live in `graphforge-storage/src/lib.rs`, not
in rel. Its execution binding may
validate stored schema facts and its visitors still perform runtime reads. The
next #1006 slice moves that implementation behind ordinary execution demand,
cancellation and accounting. Shared schema definitions and the unused
StorageProvider abstraction also remain pending the parent's final dependency
cleanup. This decision does not claim that rel's production storage dependency
has been removed.

## Evidence

Read-resource integration tests capture a snapshot, make the source directory
unavailable during lowering, and then independently bind complete plans to two
schema-equivalent graphs with different cardinalities. Existing fixed/variable
traversal, wildcard properties, path hydration, write visibility, read-only
explanation and incompatible-resource tests remain parity gates.

Dataset snapshot construction requires a catalog's retained authenticated
property inventory. It does not re-admit routes through discovery helpers that
hash graph data. A catalog admitted for an absent standalone write target can
supply its empty schema without creating that target; a missing inventory for
an existing target is rejected. No-catalog callers are schema-only.
