# ADR 0012: Knowledge and epistemic domain ownership and schema evolution

**Status:** Accepted
**Date:** 2026-07-24
**Build target:** v0.5.0

**Related:** ADR 0005 (layering), ADR 0006 (epistemic model)

## Context

The knowledge layer replaces graph-embedded confidence/provenance with immutable domain
ledgers. Before this decision, `gf-storage` owned obsolete domain schemas and
the domain crates did not yet own the complete immutable record model. Without
a fixed dependency and schema boundary, storage or graph execution could
accidentally become the semantic owner of knowledge records.

This decision fixes ownership for the knowledge layer and its epistemic extension before production
schemas are implemented.

## Decision

### Dependency DAG

Workspace arrows point from consumer to dependency. The existing internal graph
compiler/runtime subgraph is abbreviated; these are the complete permitted
directions across the knowledge/epistemic domain boundary.

```text
graph/compiler/runtime crates ─> gf-core
gf-provenance ─────────────────> gf-core
gf-knowledge ──────────────────> gf-core

gf-api
  ├─> graph/compiler/runtime crates
  ├─> gf-provenance
  ├─> gf-knowledge
  └─> gf-storage

gf-bindings-py ─┐
gf-bindings-node├─> gf-api
gf-cli          ┘
```

The domain crates may also depend on third-party Arrow, serialization, hashing,
and error libraries. Among workspace crates they depend on `gf-core` only.
`gf-provenance` and `gf-knowledge` do not depend on each other. Cross-domain
references are UUIDs from `gf-core`; `gf-api` validates and commits them.

The following directions are mechanically forbidden:

- `gf-storage`, `gf-exec`, `gf-rel`, `gf-plan`, `gf-search`, `gf-ir`,
  `gf-cypher`, `gf-ast`, `gf-ontology`, and `gf-io` to `gf-provenance` or
  `gf-knowledge`;
- `gf-provenance` or `gf-knowledge` to `gf-storage` or any graph/compiler
  crate;
- Python, Node, or CLI directly to either domain crate;
- either domain crate to the other.

`scripts/ci/check-domain-dependencies.py` evaluates `cargo metadata --no-deps`
and fails on these directions. It also applies when `gf-knowledge` is first
added, so the rule cannot be bypassed by crate-creation order.

### Crate responsibilities

| Owner | Owns | Does not own |
|---|---|---|
| `gf-core` | shared UUID aliases/newtypes, stable error envelope/code carrier, capability/version descriptor primitives, canonical byte/fingerprint primitives | domain records, domain enums, Arrow schemas, storage paths, orchestration |
| `gf-provenance` | provenance event and lineage records, closed kind/role registries, validation, deterministic ordering, canonical record bytes/fingerprints, authoritative Arrow schemas, domain errors | graph mutation execution, files/generations, assertions/confidence/evidence, cross-domain commits |
| `gf-knowledge` | all knowledge assertion/confidence/evidence/run records and all epistemic status/amendment/reasoning/supersession/hypothesis/selection/valid-time records; their registries, validation, ordering, canonical bytes/fingerprints, Arrow schemas, domain errors | graph execution, files/generations, provenance semantics, public orchestration |
| `gf-storage` | serialized project capability manifest and committed-generation record, transaction journal/lock, resolved project generations, generic typed participant descriptors, staging, checksums, fsync/publication, recovery, retention, Parquet/IPC mechanics, path containment | provenance/knowledge records or enums, epistemic policy, cross-domain reference meaning |
| `gf-api` | public Rust facade and neutral invocation descriptors, request validation, neutral graph-mutation receipt conversion, cross-domain reference validation, composite atomic-write participant assembly, capability-specific opens, pagination/cancellation, generated cross-domain schema inventory | duplicate domain schemas, binding-specific semantics |
| `gf-exec` | neutral graph mutation/inference receipts containing operation semantics and affected UUIDs | domain record construction; opening provenance/knowledge tables |
| Python/Node/CLI | thin projection of `gf-api`, Arrow/IPC transport, lossless error mapping | record/schema reimplementation, storage access, policy |

### Table ownership

Every table has exactly one semantic/schema owner. `gf-storage` may persist a
batch only through a generic participant descriptor; it does not interpret the
batch.

| Capability | Table or record family | Owner | Milestone |
|---|---|---|---|
| provenance | `provenance/events` | `gf-provenance` | knowledge-layer |
| provenance | `provenance/lineage` | `gf-provenance` | knowledge-layer |
| project container | capability manifest, committed-generation record, participant descriptors, transaction journal | `gf-storage` (using version primitives from `gf-core`) | knowledge-layer |
| immutable knowledge | `knowledge/assertions` | `gf-knowledge` | knowledge-layer |
| immutable knowledge | `knowledge/assertion_graph_refs` | `gf-knowledge` | knowledge-layer |
| immutable knowledge | `knowledge/confidence_assessments` | `gf-knowledge` | knowledge-layer |
| immutable knowledge | `knowledge/evidence_links` | `gf-knowledge` | knowledge-layer |
| immutable knowledge | `knowledge/algorithm_runs` and `knowledge/algorithm_run_events` | `gf-knowledge` | knowledge-layer |
| epistemic | `knowledge/assertion_status_events` | `gf-knowledge` | epistemic |
| epistemic | `knowledge/assertion_amendments` | `gf-knowledge` | epistemic |
| epistemic | `knowledge/reasoning_events` | `gf-knowledge` | epistemic |
| epistemic | `knowledge/supersession_events` | `gf-knowledge` | epistemic |
| epistemic | `knowledge/hypothesis_membership_events` | `gf-knowledge` | epistemic |
| epistemic | `knowledge/hypothesis_selection_events` | `gf-knowledge` | epistemic |
| epistemic | optional `knowledge/valid_time_events` | `gf-knowledge` | epistemic |

Graph topology, graph/edge properties, ontology metadata, embeddings, and
derived indexes remain outside both domain crates. Neutral descriptors used by
algorithm dispatch are request values, not persisted knowledge tables.

### Authoritative schema registries

Each domain crate exports one registry:

```text
gf_provenance::schema_registry()
gf_knowledge::schema_registry()
```

A registry entry contains:

- stable capability ID and capability contract version;
- stable record-family ID and record contract version (`u32`);
- canonical Arrow schema including field order, type, nullability, metadata,
  timestamp unit/timezone, and dictionary policy;
- closed-enum registry versions;
- canonical sort key and record-fingerprint contract;
- reader compatibility classification;
- maximum row/value/nesting limits;
- owning crate and implementation issue.

There is no second schema definition in `gf-storage`, `gf-api`, bindings,
documentation, or tests. Those consumers obtain schemas or generated fixtures
from the owning registry.

`gf-api` assembles the two registries plus graph/workbench registries into the
generated project schema inventory owned (knowledge-layer) and extended 
(epistemic). Generated files include an inventory-format version and a deterministic
SHA-256; CI regenerates and compares them byte-for-byte.

All registry schema, record, descriptor, projection, and result fingerprints
use the versioned [canonical fingerprint
contract](../book/architecture/canonical-fingerprints-v1.md). No owner defines a
second serialization.

### Version and compatibility policy

Capability, record, enum-registry, and policy versions are independent unsigned
integers. Their meaning is:

- **Compatible reader change:** implementation-only change or addition of a
  new optional table/record family explicitly permitted by the current
  capability version. Existing canonical schemas and semantics are unchanged.
- **Migration-required change:** field add/remove/reorder, Arrow type,
  nullability, metadata, enum meaning, canonicalization, ordering, identity,
  validation, or policy semantics change. It requires a new record version and,
  when required for correctness, a new capability version plus explicit
  migration.
- **Unsupported future change:** a greater capability/record/enum version is
  never treated as absent or best-effort readable.
- **Corruption:** an unknown enum value under a known closed registry version,
  schema/fingerprint mismatch, duplicate identity, or invalid reference.

Knowledge APIs return structured compatibility/corruption errors before
projecting rows. Graph-only APIs validate only graph-required generation and
manifest fields; they do not open, parse, or validate unrequested domain
participants.

The knowledge layer freezes every knowledge record at version `1` when closes. The epistemic layer may add its
separate epistemic capability and new version-1 record families, but it must
not alter a knowledge schema, fingerprint, ordering rule, or meaning. A later change
to a knowledge record is a new version with an explicit migration, never an in-place
"compatible" edit.

### Reference-validation phases

Validation is deterministic and split into two phases:

1. **Domain-local stage validation:** the owning crate validates schema,
   values, limits, duplicates, enum versions, canonical order/fingerprint, and
   references among rows in its own staged participant.
2. **Composite pre-publication validation:** `gf-api` builds UUID indexes over
   the resolved committed generation plus every staged participant, then
   validates graph ↔ provenance ↔ knowledge references. Same-transaction
   references are valid only when their target is in that combined index.

`gf-storage` verifies participant bytes/checksums and calls opaque validation
results; it does not understand UUID roles. Publication cannot occur until both
phases pass.

On reopen, the committed-generation record and requested capability descriptor
are validated first. A requested domain then validates its own records and
cross-references against the pinned generation. Unrequested domains are not
opened. A dangling reference therefore blocks the owning domain API, never an
independent graph-only operation.

### Diagnostics and observability

The owner of a failed validation supplies the stable error code:

- `gf-provenance`: provenance schema/version/record/reference errors;
- `gf-knowledge`: knowledge/epistemic schema/version/record/reference errors;
- `gf-storage`: generation/participant/checksum/path/durability errors;
- `gf-api`: request, idempotency, cross-domain, and composite-transaction
  errors.

Safe diagnostics contain operation/generation UUID, capability/record IDs,
expected/actual versions or fingerprints, phase, row counts, and error code.
They never contain graph properties, assertion/evidence/reasoning text, vector
values, credentials, or unrestricted paths.

## Consequences

- Graph and analyst-verb/find crates cannot acquire a compile-time path to knowledge.
- Domain semantics remain testable without filesystem machinery.
- `gf-storage` can atomically publish new participant kinds without changing
  for each domain.
- Cross-domain writes require deliberate `gf-api` orchestration and a
  transaction-wide validation pass.
- epistemic extends knowledge-layer instead of rewriting it.

The cost is an explicit adaptation boundary between domain batches and generic
participants. That boundary is intentional: it prevents storage convenience
from becoming semantic ownership.

## Rejected alternatives

| Alternative | Reason |
|---|---|
| Keep knowledge schemas in `gf-storage` | Makes generic I/O own epistemic meaning and invites graph-open coupling. |
| Put all knowledge/epistemic records in `gf-provenance` | Conflates lineage with assertions, evidence, confidence, runs, and evolving belief. |
| Let `gf-knowledge` depend on `gf-provenance` | Turns a UUID reference into compile-time schema coupling and makes epistemic evolution capable of rewriting knowledge-layer provenance. |
| Validate all capabilities on every open | Violates graph/knowledge isolation and makes future/corrupt knowledge block graph-only use. |
| Duplicate schemas in bindings | Creates drift and prevents same-SHA parity from being proved. |

## Required verification

- `scripts/ci/check-domain-dependencies.py`
- `scripts/ci/test-domain-dependencies.py`
- generated inventory drift tests;
- graph-only open instrumentation and analyst-verb/find isolation;
- compile-time/public API parity tests.
