# Storage Architecture

**Status:** v0.5.0 — Parquet project storage shipped
**Last Updated:** 2026-07-27

---

## Overview

GraphForge uses a **pluggable storage provider** model. No provider is the semantic owner of the query language. All providers implement a common Rust trait and are selected at runtime based on the use case.

```rust
pub trait StorageProvider: Send + Sync {
    fn provider_name(&self) -> &'static str;
    fn table_provider(&self, table: &QualifiedTable) -> Result<Arc<dyn TableProvider>, GfError>;
    fn capabilities(&self) -> ProviderCapabilities;
}
```

---

## Provider Role: Parquet

Parquet is the sole storage provider for the Rust core. It:

- Stores graph tables and opaque domain-owned participants as columnar Parquet
  files; `gf-storage` does not define provenance or knowledge semantics
- Carries GraphForge metadata at the file level (ontology version, IR version, query ID)
- Persists the compiled ontology runtime tables for rapid startup

Parquet file-level metadata:

```
graphforge.dataset_kind      = "topology_nodes"
graphforge.ontology_version  = "core-2026.05"
graphforge.writer_version    = "0.5.0"
graphforge.ir_version        = "0.1.0"
graphforge.query_id          = "01J..."
graphforge.provenance_policy = "conservative_min"
```

The `StorageProvider` trait is designed to be extended with additional backends in a later release. No other provider is in scope for v0.5.

---

## Identity and Surrogate Keys

GraphForge uses a **dual-key pattern** for all first-class objects:

| Key | Type | Purpose |
|---|---|---|
| **UUID** (`*_uuid`) | `FixedSizeBinary(16)` — UUIDv7 | Canonical stable identity. Globally unique. Immutable. Survives project merges, offline generation, and cross-analyst exchanges. |
| **Surrogate** (`*_id`) | `UInt64` | Execution-time optimization. Assigned at ingest/load time. Used for DataFusion join operations. Never exposed in public API results. |

### Why UUIDv7

UUIDv7 (RFC 9562) is time-ordered within a millisecond, globally unique without coordination, fits in Arrow `FixedSizeBinary(16)`, and supports offline generation on mobile devices or air-gapped systems. See [refactor-v0.5.md §5](refactor-v0.5.md) for the full rationale.

UUID byte order, accepted text form, content-derived UUIDv8 records, canonical
Arrow bytes, and domain-separated SHA-256 fingerprints follow the frozen
[canonical fingerprint v1 contract](canonical-fingerprints-v1.md).

### UUID→Surrogate mapping

The relational lowering layer maps `node_uuid → node_id` once at scan time. All DataFusion join operators use integer surrogates (`node_id`, `edge_id`, `src_id`, `dst_id`) for performance. Results project back to UUID columns before returning to the caller.

**Rule:** UUIDs appear in every public API result schema. Surrogates are execution-internal and must never appear in API outputs.

### Objects requiring UUID identity

| Object | UUID column |
|---|---|
| Node (entity) | `node_uuid` |
| Edge (relationship) | `edge_uuid` |
| Document | `doc_uuid` |
| Provenance event | `provenance_uuid` |
| Analyst/User | `analyst_uuid` |
| Project | `project_uuid` |
| Workflow | `workflow_uuid` |
| Embedding | `embedding_uuid` |
| Source reference | `source_uuid` |
| Ranking output row | `rank_uuid` |
| Clustering output row | `cluster_uuid` |
| Generated artifact | `artifact_uuid` |

---

## Storage Layout

GraphForge is pre-v1 and does not support older project formats. The normative
[pre-v1 compatibility policy](project-format-compatibility.md) permits only the
v0.5 project/container contract and rejects historical inputs without mutation.

GraphForge organises a project as immutable, complete generations. `CURRENT` is
the only publication authority; graph, provenance, and knowledge participants
become visible together through one atomic pointer replacement. The normative
layout, fsync order, reader leases, recovery rules, and failpoints are frozen in
[ADR 0013](../../adr/0013-project-generation-protocol.md).

Named checkpoints, lease-pinned historical reads, logical diff, and
complete-workspace revert are defined by
[ADR 0014](../../adr/0014-workspace-checkpoints.md). A revert publishes a new
complete generation; it never moves `CURRENT` backward. Active checkpoint
references add explicit retention roots, while deletion releases only that
root and cannot invalidate an already leased reader.

```
project/
├── FORMAT
├── CURRENT             # sole committed-generation pointer
├── locks/writer.lock
├── transactions/
├── generations/
│   └── <generation-uuid>/
│       ├── lease.lock
│       ├── manifest.json
│       └── participants/
│           ├── graph/...
│           ├── workspace/
│           │   ├── configuration.json
│           │   └── ontology.json
│           ├── provenance/...
│           └── knowledge/...
├── cache/               # derived and source-fingerprint keyed
└── trash/
```

A minimal committed generation declares `graph@1` and `workspace@1`.
`workspace@1` contains canonical JSON records for explicit ontology absence (or
an adopted advisory/strict ontology) and authoritative registered project
configuration. Project open validates these records before hydrating graph
data. Root YAML/JSON and environment settings are inputs only and cannot
override the selected generation.

Optional capability absence is recorded in the generation manifest; it is not
inferred by scanning folders. Graph-only readers validate the mandatory
workspace control records and graph participants but never open provenance,
knowledge, or epistemic tables. Semantic table ownership remains with the
domain crates defined by
[ADR 0012](../../adr/0012-m20-domain-ownership.md).

Unless a root is shown explicitly, graph paths in the sections below are
relative to the pinned generation's `participants/graph/`; primary workbench
paths are relative to `participants/workbench/`; derived index paths are
relative to root `cache/`.

### `embeddings/` — primary vector generations

Unlike `indexes/`, caller-, algorithm-, or provider-produced vectors are
primary workbench data and are never reconstructed or discarded as a cache.
Names do not enter paths; compatibility and generation SHA-256 digests do:

```text
embeddings/
├── aliases.json                         # display name -> compatibility digest
└── spaces/<compatibility-sha256>/
    ├── space.json                       # compatibility descriptor + refresh policy
    ├── active.json                      # checksummed active generation pointer
    └── generations/<generation-sha256>/
        ├── vectors.parquet              # node_uuid + FixedSizeList<Float32, N>
        └── manifest.json                # source fingerprint, counts, digests, state inputs
```

Builders use a collision-resistant private sibling directory, validate the
complete UUID/vector batch, write and fsync vectors, write the checksummed
manifest last, fsync the tree, then atomically replace `active.json`. The prior
active generation remains visible until that final pointer swap. Incomplete
private trees are ignored and recoverably removed on open. Alias replacement is
separate from generation publication, so an incompatible producer cannot take
over a name accidentally.

Every open recomputes `fresh`, `stale`, `substantially_stale`, `incompatible`,
or `corrupt` from the persisted descriptor/source fingerprint and current graph
metadata. The exact identity fields, mutation thresholds, forced-stale boundary,
retention, refresh coalescing, and provider privacy rules are normative in
[Embedding v1](embedding-v1.md#embedding-space-publication). Deleting a node or
removing its selected label makes it ineligible immediately; corrupt or
incompatible bytes always fail closed. Credentials, raw input text, provider
payloads, and knowledge-layer fields are never stored here.

---

## Derived Indexes

The `indexes/` folder holds **derived, rebuildable acceleration structures**. Nothing here is
canonical: every file under `indexes/` can be reconstructed from `topology/` (and, for FTS,
`properties/`) alone. An absent index is not an error — it means the accelerator has not been
built yet, and the engine falls back to building in memory on demand. See
[ADR 0004](../../adr/0004-adjacency-index.md).

### `indexes/adjacency/` — graph-native adjacency index

The adjacency index is a derived CSR (compressed sparse row) representation of the topology,
used by both the Cypher traversal path (variable-length `Expand`) and the analyst verbs
(`rank`/`cluster`/`paths`/`analyze`/`similar`). It is **optional**: absent ⇒ build in memory
on demand (today's behavior); present ⇒ load from disk. It is surrogate-keyed and never
changes results — only speed.

```
indexes/
└── adjacency/
    ├── index_manifest.parquet
    ├── WORKS_AT.out.csr
    ├── WORKS_AT.in.csr
    ├── OWNS.out.csr
    ├── OWNS.in.csr
    ├── _all.out.csr          # union across relation types (for via=None)
    └── _all.in.csr
```

The builder (`gf_storage::adjacency::build_adjacency_index`) writes one `{out, in}` pair per
relation type plus the `_all` union pair, then the manifest **last**. Relation names unusable
as file stems (path separators, `..`, the reserved `_all`) are skipped — those relations are
served by scan-build, but their rows still flow into the union index. The
manifest is stamped with the pinned project-generation UUID and graph source
fingerprint read **before** the edge scan. A newer publication can therefore
make the result stale, never falsely fresh.

**`index_manifest.parquet`**

| Column | Arrow type | Notes |
|---|---|---|
| `relation_type` | `Utf8` | Relation type name, or `_all` for the union index |
| `direction` | `Utf8` | `"out"` \| `"in"` |
| `project_generation_uuid` | `FixedSizeBinary(16)` | Committed generation pinned by the builder |
| `graph_source_fingerprint` | `FixedSizeBinary(32)` | Canonical graph participant fingerprint |
| `built_at` | `Timestamp(Microseconds, UTC)` | |
| `node_count` | `UInt64` | Number of source nodes covered (CSR row count) |
| `edge_count` | `UInt64` | Number of `(edge, neighbor)` entries |

**CSR file (`<REL_TYPE>.<dir>.csr`)** — single-batch Arrow IPC, one column, one row per
surrogate `node_id ∈ 0..node_count`:

| Column | Arrow type | Notes |
|---|---|---|
| `adjacency` | `LargeList<Struct { edge_id: UInt64, neighbor_id: UInt64 }>` | Row `i` holds the adjacency entries of surrogate `node_id = i`, in CSR order |

This is the CSR structure in its idiomatic Arrow encoding — the two logical arrays cannot be
two top-level columns because a RecordBatch requires equal column lengths. The list's offsets
buffer **is** the CSR offsets array (length `node_count + 1`, `Int64`, starting at 0,
monotone), and the flattened struct child **is** the targets array (length `edge_count`):
neighbors of `node_id = i` are `targets[offsets[i]..offsets[i+1]]`, zero-copy on read.

Conventions:

- **Empty graph**: a zero-row batch — logical `offsets == [0]`, empty targets. The offsets
  array is never empty.
- **Node with no neighbors**: an empty list (`offsets[i] == offsets[i+1]`).
- CSR rows cover exactly `node_id ∈ 0..node_count`; surrogates beyond `node_count` simply
  have no entries.
- In-memory consumers (`gf_exec::AdjacencyProvider` — scan-build via
  `ScanBuildAdjacencyProvider` until the persistent loader) see the logical
  `offsets`/`targets` model; the list encoding is a file-format detail
  (`gf_storage::adjacency::CsrIndex` on the storage side).

### Rebuild and versioning semantics

- **Source of truth.** A CSR is always reconstructable from `topology/edges/<REL_TYPE>.parquet`
  alone, deterministically.
- **Generation identity.** The adjacency manifest records the committed
  project-generation UUID and graph source fingerprint from the reader's
  pinned snapshot. There is no absent-counter or generation-zero meaning.
- **Publication rule.** A graph mutation and its source fingerprint publish in
  the same immutable generation. `CURRENT` changes only after every participant
  is durable and validated.
- **Crash-safety invariant.** A reader sees either the prior complete graph
  generation or the new complete graph generation. A failed or interrupted
  write never exposes a counter/data mismatch or committed prefix.
- **Staleness detection.** The provider compares the manifest's source
  fingerprint with the pinned graph participant fingerprint. A corrupt
  accelerator is always stale, never fresh.
- **Fallback.** On mismatch (or absent index), the provider scans the typed edge tables and
  builds the adjacency in memory — yielding identical results, only slower. A stale or missing
  index can therefore never cause incorrect output.
- **Rebuild triggers.** Lazy on first traversal when the `indexes/adjacency/` capability is
  present, or explicit via `forge.index("adjacency", ...)`. Incremental rebuild
  (append-delta + compaction) is deferred to v0.5.1.
- **Determinism (R-ADJ-2).** Full rebuild streams each typed edge file once; `out` entries
  sort by `(src_id, edge_id)` and `in` entries by `(dst_id, edge_id)` — the `edge_id`
  tie-break makes the CSR bytes reproducible from `topology/` alone. `_all.{out,in}.csr` are
  the same sorts over the union of all typed files plus `_exploratory.parquet`. The manifest's
  `built_at` is excluded from the determinism guarantee.
- **Build ordering.** Builders write all CSR files first and `index_manifest.parquet`
  **last**, so a torn build reads as stale (absent/old manifest), never as fresh.
- **Loader semantics** (`gf_exec::PersistentAdjacencyProvider`).
  Freshness requires a non-empty manifest whose graph source fingerprint
  equals the pinned graph participant. Fresh + row present ⇒ load
  (`adjacency=hit`); stale or torn ⇒ lazy rebuild, then serve; fresh but **no
  row** for the requested relation ⇒ scan-build *without* rebuild (rebuilding
  cannot add an unknown relation — prevents a rebuild-per-query loop); a
  corrupt accelerator ⇒ always-stale scan-build; capability absent ⇒ scan-build
  (`adjacency=building`). Typed-mode `"*"` bypasses the index entirely
  (reported as `building`, never a false miss). A build or load failure never
  fails the query — only its speed.
- **Direction.** `out` and `in` CSRs are stored separately; undirected traversal unions them.
  In exploratory mode, `_exploratory.parquet` rows are routed by their `rel_type_name` column.

### `indexes/<LABEL>/tantivy/` — full-text search index

Full-text indexes (Tantivy) are also derived and rebuildable from `properties/`. See the
Find / index (`forge.find` / `forge.index`).

---

## Graph Fact Schema

### Topology layer (hot path)

Graph traversal reads only the topology layer. No property columns are read unless the query explicitly projects them.

**`topology/nodes.parquet`**

| Column | Arrow type | Notes |
|---|---|---|
| `node_uuid` | `FixedSizeBinary(16)` | UUIDv7 — canonical stable identity |
| `node_id` | `UInt64` | Local surrogate — DataFusion join key |
| `type_id` | `UInt32` | Immutable primary label used for property-file routing |
| `type_ids` | `List<UInt32>` | Authoritative complete label set; scans use membership in this column |
| `created_at` | `Timestamp(Microseconds, UTC)` | |
| `updated_at` | `Timestamp(Microseconds, UTC)` | |

The first label in a node's creation pattern is its immutable **primary label**.
`properties/<ENTITY>.parquet` continues to use that primary label as its file stem;
adding secondary labels therefore cannot orphan or relocate properties.
Unlabelled nodes route to `_untyped`. A v0.5 node participant must contain both
fields with the frozen schema; an earlier development schema is unsupported.

#### Filtered node lookup

Canonical node files assign `node_id` densely and monotonically, so physical
row ordinal `n - 1` contains `node_id = n`. A filtered node read proves that
layout from non-null row-group statistics plus ascending column and offset page
indexes, then supplies Parquet with an exact `RowSelection` for the requested
ordinals. Scattered destination ids therefore decode only their selected rows,
instead of every page between their minimum and maximum.

The optimization is fail-closed. Missing page indexes, deleted/gapped IDs,
malformed statistics, or unordered ranges use the row-group plus
membership-predicate reader. Exact output keys are validated after ordinal
selection; any mismatch is discarded and retried conservatively. This
accelerator fallback applies only to a valid v0.5 participant; it is not a
project-format compatibility path.

**`topology/edges/TYPENAME.parquet`** (one file per relation type)

| Column | Arrow type | Notes |
|---|---|---|
| `edge_uuid` | `FixedSizeBinary(16)` | UUIDv7 |
| `src_uuid` | `FixedSizeBinary(16)` | References `node_uuid` |
| `dst_uuid` | `FixedSizeBinary(16)` | References `node_uuid` |
| `edge_id` | `UInt64` | Local surrogate |
| `src_id` | `UInt64` | Local surrogate — DataFusion join key |
| `dst_id` | `UInt64` | Local surrogate — DataFusion join key |
| `created_at` | `Timestamp(Microseconds, UTC)` | |

Typed edge tables (one Parquet file per relation type) replace the unified `edge_facts` table. This enables direct scans on a single relation type without filtering, yielding significant I/O savings at 100M+ edges. See [refactor-v0.5.md §7](refactor-v0.5.md) for performance analysis.

### Properties layer (warm path)

**`properties/ENTITY_TYPE.parquet`** (one file per entity type, columns per ontology)

| Column | Arrow type | Notes |
|---|---|---|
| `node_uuid` | `FixedSizeBinary(16)` | Join key back to `topology/nodes.parquet` |
| *(property columns)* | *(per ontology)* | e.g. `name Utf8`, `age Int64`, `email Utf8` |

Property access is a join: `topology/nodes JOIN properties/PERSON ON node_uuid`. DataFusion handles this as a hash join. The separation allows graph traversal to skip property I/O entirely.

### Provenance and knowledge participants

`provenance/` and `knowledge/` belong to the knowledge layer, but generic
storage does not own their records or schemas. Under
[ADR 0012](../../adr/0012-m20-domain-ownership.md):

- `gf-provenance` owns provenance events and lineage;
- `gf-knowledge` owns knowledge assertions, assertion graph references, confidence
  assessments, evidence links, algorithm runs/events, and every additive epistemic
  epistemic record;
- `gf-api` validates cross-domain UUID references and assembles composite
  writes; and
- `gf-storage` receives validated Arrow batches as opaque generation
  participants and owns only their paths, checksums, persistence, publication,
  and recovery.

The exact knowledge schemas are frozen and are generated from the two owning Rust registries in the checked
[Knowledge schema inventory](../../reference/m20-schema-inventory.json). The epistemic layer adds separate
append-only status, amendment, reasoning, supersession, hypothesis, selection,
and valid-time record families; it does not add mutable fields to
knowledge assertions.

The legacy pre-knowledge `PROVENANCE_EVENTS_SCHEMA`, `PROVENANCE_LINEAGE_SCHEMA`, and
graph-embedded edge `confidence`/`provenance_uuid` fields have been removed
from `gf-storage`. They are not the knowledge-layer contract, and no historical project
data is imported or converted.

Graph-only readers resolve the committed generation and graph-required
manifest fields without opening either participant. A future or corrupt
knowledge record blocks its owning knowledge API, never Cypher or neutral
analyst-verb/find execution.

---

## Ontology Runtime

The ontology is a **runtime-loadable knowledge schema**, not Rust structs generated into the binary. Three representations serve different purposes:

| Format | Purpose |
|---|---|
| **YAML / JSON** | Human-authored ontology definitions (Serde-based load) |
| **Arrow tables** | Compiled execution format — cheap joins during binding and planning |
| **Parquet** | Persisted for rapid startup or reproducible deployments |

### Ontology authoring format (YAML)

```yaml
ontology_id: core
version: "2026.05"
entity_types:
  - name: Person
    abstract: false
  - name: Employee
    parent: Person
relation_types:
  - name: MANAGES
    src: Employee
    dst: Employee
    inverse: MANAGED_BY
    semantic:
      transitive: false
      symmetric: false
      functional: false
properties:
  - owner: Person
    name: name
    type: utf8
    nullable: false
constraints:
  - owner: Employee
    kind: unique_property
    expr:
      property: employee_id
```

At load time this compiles into Arrow lookup tables keyed by integer type IDs. String-heavy lookups during planning become O(1) integer comparisons.

### Ontology runtime tables

| Table | Purpose |
|---|---|
| `ontology_meta` | Identity, version, IR compatibility range, checksum |
| `entity_types` | Node classes and inheritance DAG (acyclicity enforced at load) |
| `relation_types` | Edge classes, endpoint type constraints, inverse pairs |
| `property_types` | Name, owner, value type, nullability, cardinality |
| `type_constraints` | Validation rules (unique, required, range) |
| `cardinality_rules` | Endpoint multiplicity (min/max per relation type) |
| `semantic_flags` | `transitive`, `symmetric`, `reflexive`, `functional`, `acyclic` |
| `aliases` | Human-facing and deprecated names |
| `migrations` | Versioned ontology upgrade transforms |

### Ontology versioning

Two independent version axes:

| Axis | Meaning |
|---|---|
| `ontology_version` | Meaning of types and rules — changes when the schema evolves |
| `ir_version` | Runtime/compiler contract — changes when the IR format changes |

A new ontology version does not require a new IR version, and vice versa. Persisted datasets record the `ontology_version` used to write them. Arrow schema metadata carries both versions through IPC and Parquet round-trips.

### Validation model

| Level | When | Examples |
|---|---|---|
| **Ontology-load** | On file/table load | Duplicate names, missing parents, inheritance cycles, bad inverse references |
| **Write-time** | On `CREATE`, `MERGE`, batch ingest | Unknown property, wrong value type, illegal endpoint type, cardinality overflow |
| **Query-time** | During binding/planning | Unknown labels/types/properties, illegal pattern shape, ambiguous property resolution |

---

## Serialization Systems

**Never mix these two systems:**

| System | Purpose | Format |
|---|---|---|
| **Arrow / Parquet** (`gf-storage`) | Graph topology/properties and generic persistence of domain-owned participants | Binary columnar (Arrow IPC / Parquet) |
| **JSON / YAML** (`gf-ontology`) | Ontology definitions and metadata | Text (human-readable, validatable) |

Graph data → Arrow/Parquet. Ontology/metadata → JSON or YAML. Arrow schema metadata carries version and provenance annotations across language boundaries.

---

## Two-Mode Graph Instances

```rust
// In-memory (fast, volatile)
let forge = GraphForge::new(None)?;

// Persistent (project directory)
let forge = GraphForge::new(Some("path/to/project/"))?;
```

The storage layer is transparent to all API surfaces.

---

## References

- [Architecture Overview](overview.md) — workspace layout and provider trait
- [Architecture Refactor v0.5](refactor-v0.5.md) — UUID identity model, typed edge tables, project structure
- [Execution Model](execution-model.md) — how providers connect to DataFusion
- [ADR 0001: Rust Core](../../adr/0001-rust-core.md) — Parquet-as-primary and provider strategy
