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

## Parent completion (#1006)

Relational expressions retain a neutral path-node descriptor and a recursive
expression-rewrite seam. Execution binds that descriptor to the selected read
context, revalidates its schema, and retains authenticated property providers.
The recursive rewrite preserves private quantifier and list-comprehension
positions, including empty-list and short-circuit behavior. Runtime resource
errors propagate through these nested evaluations.

Each physical query plan owns a hydration resource using the same runtime
memory pool as its execution task. Per-invocation fallible reservations cover
retained UUID indexes, label vectors, property-row locations and gathered Arrow
batches; they release on success or error. This follows ordinary DataFusion
operator accounting, not process-RSS or output-lifetime accounting. Hydration
reads remain bounded by requested UUIDs and stop once the selected rows are
found. Query-local metrics expose examined rows, gathered rows and peak gathered
entries. A transparent physical wrapper preserves child properties and
partitions and exposes only its own hydration metrics.

Whole-query stream and eager-collection owners cancel hydration on completion
or drop. Completing one partition does not cancel sibling partitions. Direct
write-expression evaluation has its own phase owner; independently planned
write prefixes and suffixes retain whole-plan cancellation guards.

Shared Arrow schemas now live in IR, with storage compatibility reexports.
Rel's storage dependency is test-only. The unused storage-provider stubs were
deleted rather than promoted into a second backend interface.

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
