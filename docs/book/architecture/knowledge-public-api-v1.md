# Immutable knowledge ledger public API v1

**Status:** Shipped and frozen for v0.5.0
**Contract:** `graphforge-knowledge-api/1`

**Machine-readable shape:** [`knowledge-public-api-v1.json`](../../contracts/knowledge-public-api-v1.json)

This contract fixes the shipped public Rust facade and its thin Python and Node
projections. It exposes immutable
records and Arrow data; it does not put knowledge behavior into graph
execution.

## Boundary

- `graphforge-api` is the only public orchestration boundary.
- `graphforge-provenance` and `graphforge-knowledge` own records, validation, schemas, ordering,
  and canonical bytes. They are not binding dependencies.
- `graphforge-storage` receives opaque participants and owns generation publication.
- Graph reads, openCypher reads, and analyst-verb/find compute never open provenance or
  knowledge participants.
- All buffered ledger reads and write receipts are Arrow
  `graphforge_exec::ExecutionResult` values with the exact registered schema and no
  execution surrogate IDs.
- Python returns `pyarrow.Table`. Node resolves a `Promise<Buffer>` containing
  one Arrow IPC stream. Neither binding rebuilds a schema or record.

## Shared Rust types

The implementation exports these types from `graphforge-api`:

```rust
pub const KNOWLEDGE_API_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationId(pub uuid::Uuid);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteContext {
    pub operation_uuid: OperationId,
    pub actor_uuid: Option<uuid::Uuid>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRequest {
    pub limit: u32,
    pub after: Option<PageToken>,
    pub cancellation: Option<CancellationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageToken(/* opaque, private bytes */);

#[derive(Clone, Debug)]
pub struct CancellationToken(/* cloneable shared state */);

#[derive(Debug)]
pub struct RecordedAlgorithmResult {
    pub run_uuid: uuid::Uuid,
    pub result: graphforge_exec::ExecutionResult,
}
```

`PageRequest::default()` is limit `100`, no cursor, and no cancellation.
Limits are `1..=10_000`. `CancellationToken::cancel()` is idempotent.
Request structs below are exact v1 shapes, not `#[non_exhaustive]`; later
behavior uses new structs and methods rather than silently accepting fields.

UUID strings at binding boundaries follow
[graphforge-canonical/1](canonical-fingerprints-v1.md). Durable write times come
from the engine's injected UTC-microsecond clock and cannot be supplied by an
ordinary caller. Tests may replace the clock through a non-public constructor.

## Capability API

```rust
pub struct EnableCapabilityRequest {
    pub context: WriteContext,
    pub capability_id: CapabilityId,
    pub capability_version: u32,
}

impl GraphForge {
    pub fn project_capabilities(&self) -> Result<ExecutionResult, GfError>;
    pub fn enable_capability(
        &self,
        request: EnableCapabilityRequest,
    ) -> Result<ExecutionResult, GfError>;
}
```

`project_capabilities()` reads only the committed manifest. It reports declared
future capabilities without opening their participants. `enable_capability`
accepts only a registered capability/version and atomically publishes its
initial empty required participants. Repeating the same operation UUID and
canonical request returns the existing row. Reusing it for different bytes is
`GF_IDEMPOTENCY_CONFLICT`.

The result schema `project_capability@1` is ordered by `capability_id`:

| Field | Arrow type | Null |
| ------------------------- | -------------------------------------------- | ---- |
| `capability_id` | `Utf8` | no |
| `capability_version` | `UInt32` | no |
| `support` | `Utf8` (`supported` or `unsupported_future`) | no |
| `schema_inventory_sha256` | `FixedSizeBinary(32)` | yes |
| `generation_uuid` | `FixedSizeBinary(16)` | no |

The graph and workspace capabilities are created with the first generation.
`workspace@1` is mandatory control state rather than a knowledge ledger.
`provenance` and `knowledge` are knowledge-layer version `1`. `epistemic` is reserved for
`epistemic` and cannot be enabled by a knowledge-only binary.

## Provenance reads

Provenance writes are consequences of graph or ledger mutations. There is no
public method that appends an arbitrary provenance event.

```rust
pub struct ProvenanceHistoryRequest {
    pub subject_uuid: Option<uuid::Uuid>,
    pub operation_uuid: Option<OperationId>,
    pub page: PageRequest,
}

impl GraphForge {
    pub fn provenance_event(
        &self,
        provenance_uuid: uuid::Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<ExecutionResult, GfError>;

    pub fn list_provenance_history(
        &self,
        request: ProvenanceHistoryRequest,
    ) -> Result<ExecutionResult, GfError>;
}
```

`provenance_event()` returns exactly one `provenance_event@1` row or
`GF_NOT_FOUND`. `list_provenance_history()` returns `provenance_history@1`,
ordered by `(recorded_at, provenance_uuid, role, ordinal, subject_uuid)`.

`provenance_event@1`:

| Field | Arrow type | Null |
| ------------------ | ----------------------------- | ---- |
| `provenance_uuid` | `FixedSizeBinary(16)` | no |
| `operation_uuid` | `FixedSizeBinary(16)` | no |
| `event_kind` | `Utf8` | no |
| `actor_uuid` | `FixedSizeBinary(16)` | yes |
| `recorded_at` | `Timestamp(Microsecond, UTC)` | no |
| `contract_version` | `UInt32` | no |

`provenance_history@1` appends these lineage fields to the event fields:

| Field | Arrow type | Null |
| -------------------------- | --------------------- | ---- |
| `lineage_uuid` | `FixedSizeBinary(16)` | no |
| `subject_uuid` | `FixedSizeBinary(16)` | no |
| `subject_kind` | `Utf8` | no |
| `role` | `Utf8` | no |
| `ordinal` | `UInt32` | no |
| `lineage_contract_version` | `UInt32` | no |

### Graph mutation event and no-op policy

Graph execution emits a neutral UUID-only receipt. When provenance is enabled,
`graphforge-api` commits the graph snapshot, one event per aggregate mutation kind, and
its ordered input/output lineage in the same project generation.

| Successful mutation path | Provenance behavior |
| ------------------------------------------------ | --------------------------------------------------------------- |
| Cypher `CREATE` and public `add_node`/`add_edge` | `create_node` and/or `create_edge` for objects actually created |
| `MERGE` create path | `merge_create` |
| `MERGE` matched path with no create | `merge_matched_noop` |
| `SET` property assignment | `set_property`, including assignment of the current value |
| `SET n:Label` | `add_label` only for labels actually added |
| `REMOVE` existing property | `remove_property` |
| `REMOVE n:Label` | `remove_label` only for labels actually removed |
| `DELETE` | `delete` for objects actually removed |
| `DETACH DELETE` | `detach_delete` for the removed node and incident edges |
| persisted ontology-inference materialization | `ontology_inference` |

An unmatched row, `REMOVE` of an absent property, repeated label addition, or
removal of an absent label produces an empty receipt and no provenance event or
new generation. `MERGE` matched/no-create is the sole deliberate successful
no-op event because recording the match outcome is part of its semantic
contract.

## Assertions

```rust
pub struct AssertionGraphRefInput {
    pub graph_uuid: uuid::Uuid,
    pub graph_kind: GraphObjectKind,
    pub role: AssertionGraphRole,
    pub ordinal: u32,
}

pub struct CreateAssertionRequest {
    pub context: WriteContext,
    pub assertion_uuid: uuid::Uuid,
    pub claim: String,
    pub graph_refs: Vec<AssertionGraphRefInput>,
}

pub struct ListAssertionsRequest {
    pub graph_uuid: Option<uuid::Uuid>,
    pub page: PageRequest,
}

impl GraphForge {
    pub fn create_assertion(
        &self,
        request: CreateAssertionRequest,
    ) -> Result<ExecutionResult, GfError>;

    pub fn assertion(
        &self,
        assertion_uuid: uuid::Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<ExecutionResult, GfError>;

    pub fn list_assertions(
        &self,
        request: ListAssertionsRequest,
    ) -> Result<ExecutionResult, GfError>;

    pub fn assertion_graph_refs(
        &self,
        assertion_uuid: uuid::Uuid,
        page: PageRequest,
    ) -> Result<ExecutionResult, GfError>;
}
```

`create_assertion` requires caller-supplied assertion and operation UUIDs. It
publishes assertion, references, and the required provenance/lineage companion
in one project generation and returns the one `assertion@1` row. Retry compares
the complete canonical request, including ordered references.

`assertion()` returns one row or `GF_NOT_FOUND`. `list_assertions()` orders by
`(recorded_at, assertion_uuid)`. `assertion_graph_refs()` orders by
`(role, ordinal, graph_kind, graph_uuid)`.

`assertion@1`:

| Field | Arrow type | Null |
| ------------------ | ----------------------------- | ---- |
| `assertion_uuid` | `FixedSizeBinary(16)` | no |
| `claim` | `Utf8` | no |
| `provenance_uuid` | `FixedSizeBinary(16)` | no |
| `recorded_at` | `Timestamp(Microsecond, UTC)` | no |
| `contract_version` | `UInt32` | no |

`assertion_graph_ref@1`:

| Field | Arrow type | Null |
| ------------------ | --------------------- | ---- |
| `assertion_uuid` | `FixedSizeBinary(16)` | no |
| `graph_uuid` | `FixedSizeBinary(16)` | no |
| `graph_kind` | `Utf8` | no |
| `role` | `Utf8` | no |
| `ordinal` | `UInt32` | no |
| `contract_version` | `UInt32` | no |

## Confidence assessments

```rust
pub enum ConfidencePolicyRequest {
    Explicit { value: f64 },
    ConservativeMin { input_confidence_uuids: Vec<uuid::Uuid> },
}

pub struct AssessConfidenceRequest {
    pub context: WriteContext,
    pub confidence_uuid: uuid::Uuid,
    pub assertion_uuid: uuid::Uuid,
    pub policy: ConfidencePolicyRequest,
}

pub struct ListConfidenceAssessmentsRequest {
    pub assertion_uuid: Option<uuid::Uuid>,
    pub page: PageRequest,
}

impl GraphForge {
    pub fn assess_confidence(
        &self,
        request: AssessConfidenceRequest,
    ) -> Result<ExecutionResult, GfError>;

    pub fn confidence_assessment(
        &self,
        confidence_uuid: uuid::Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<ExecutionResult, GfError>;

    pub fn list_confidence_assessments(
        &self,
        request: ListConfidenceAssessmentsRequest,
    ) -> Result<ExecutionResult, GfError>;

    pub fn confidence_inputs(
        &self,
        confidence_uuid: uuid::Uuid,
        page: PageRequest,
    ) -> Result<ExecutionResult, GfError>;
}
```

The primary result is one `confidence_assessment@1` row. Assessment, normalized
input rows, and provenance companion publish atomically. The caller supplies
both durable UUIDs. `list_confidence_assessments()` orders by
`(recorded_at, confidence_uuid)`; inputs order by
`(ordinal, input_confidence_uuid)`.

`confidence_assessment@1`:

| Field | Arrow type | Null |
| ------------------ | ----------------------------- | ---- |
| `confidence_uuid` | `FixedSizeBinary(16)` | no |
| `assertion_uuid` | `FixedSizeBinary(16)` | no |
| `policy` | `Utf8` | no |
| `policy_version` | `UInt32` | no |
| `value` | `Float64` | yes |
| `provenance_uuid` | `FixedSizeBinary(16)` | no |
| `recorded_at` | `Timestamp(Microsecond, UTC)` | no |
| `contract_version` | `UInt32` | no |

`confidence_input@1`:

| Field | Arrow type | Null |
| ----------------------- | --------------------- | ---- |
| `confidence_uuid` | `FixedSizeBinary(16)` | no |
| `input_confidence_uuid` | `FixedSizeBinary(16)` | no |
| `input_value` | `Float64` | yes |
| `ordinal` | `UInt32` | no |
| `contract_version` | `UInt32` | no |

## Evidence

```rust
pub struct AttachEvidenceRequest {
    pub context: WriteContext,
    pub evidence_uuid: uuid::Uuid,
    pub assertion_uuid: uuid::Uuid,
    pub source_uuid: uuid::Uuid,
    pub source_kind: EvidenceSourceKind,
    pub role: EvidenceRole,
    pub weight: Option<f64>,
}

pub struct ListEvidenceLinksRequest {
    pub assertion_uuid: Option<uuid::Uuid>,
    pub source_uuid: Option<uuid::Uuid>,
    pub page: PageRequest,
}

pub struct EvidenceInput {
    pub evidence_uuid: uuid::Uuid,
    pub source_uuid: uuid::Uuid,
    pub source_kind: EvidenceSourceKind,
    pub role: EvidenceRole,
    pub weight: Option<f64>,
}

pub struct CreateAssertionWithEvidenceRequest {
    pub assertion: CreateAssertionRequest,
    pub evidence: Vec<EvidenceInput>,
}

impl GraphForge {
    pub fn attach_evidence(
        &self,
        request: AttachEvidenceRequest,
    ) -> Result<ExecutionResult, GfError>;

    pub fn create_assertion_with_evidence(
        &self,
        request: CreateAssertionWithEvidenceRequest,
    ) -> Result<ExecutionResult, GfError>;

    pub fn evidence_link(
        &self,
        evidence_uuid: uuid::Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<ExecutionResult, GfError>;

    pub fn list_evidence_links(
        &self,
        request: ListEvidenceLinksRequest,
    ) -> Result<ExecutionResult, GfError>;
}
```

`attach_evidence` publishes evidence and its provenance companion atomically.
`create_assertion_with_evidence` uses the assertion request's single operation
UUID and publishes assertion, references, all evidence links, and one
provenance/lineage set atomically. It returns the assertion row; evidence is
read through `list_evidence_links`. An empty evidence vector is rejected so it
cannot alias `create_assertion`.

`evidence_link()` returns one row or `GF_NOT_FOUND`.
`list_evidence_links()` orders by `(recorded_at, evidence_uuid)`.

`evidence_link@1`:

| Field | Arrow type | Null |
| ------------------ | ----------------------------- | ---- |
| `evidence_uuid` | `FixedSizeBinary(16)` | no |
| `assertion_uuid` | `FixedSizeBinary(16)` | no |
| `source_uuid` | `FixedSizeBinary(16)` | no |
| `source_kind` | `Utf8` | no |
| `role` | `Utf8` | no |
| `weight` | `Float64` | yes |
| `provenance_uuid` | `FixedSizeBinary(16)` | no |
| `recorded_at` | `Timestamp(Microsecond, UTC)` | no |
| `contract_version` | `UInt32` | no |

## Recorded algorithm invocation and run history

The existing `rank`, `cluster`, `paths`, `analyze`, and `similar` methods remain
direct graph-only methods with their existing Arrow results. `find` remains the
find/index search method. Recording is explicit:

```rust
pub struct RecordedAlgorithmRequest {
    pub context: WriteContext,
    pub run_uuid: uuid::Uuid,
    pub descriptor: InvocationDescriptor,
    pub cancellation: Option<CancellationToken>,
}

pub struct ListAlgorithmRunsRequest {
    pub algorithm: Option<AlgorithmId>,
    pub page: PageRequest,
}

impl GraphForge {
    pub fn invoke_recorded(
        &self,
        request: RecordedAlgorithmRequest,
    ) -> Result<RecordedAlgorithmResult, GfError>;

    pub fn algorithm_run(
        &self,
        run_uuid: uuid::Uuid,
        cancellation: Option<CancellationToken>,
    ) -> Result<ExecutionResult, GfError>;

    pub fn list_algorithm_runs(
        &self,
        request: ListAlgorithmRunsRequest,
    ) -> Result<ExecutionResult, GfError>;

    pub fn algorithm_run_events(
        &self,
        run_uuid: uuid::Uuid,
        page: PageRequest,
    ) -> Result<ExecutionResult, GfError>;
}
```

The run identity and `started` event publish atomically before dispatch. A
terminal event publishes in a later generation. A successful return guarantees
the terminal `completed` event is committed. A dispatch error is returned only
after `failed` or `cancelled` is committed; if that terminal publication fails,
the transaction error is primary and safe diagnostics retain the dispatch
category. Reopen reconciles a lone `started` event to one `interrupted` event.

Retry with the same run and operation UUIDs plus canonical descriptor resolves
the existing lifecycle. A completed run returns the persisted result only when
the result capability stores it; otherwise it returns
`GF_RESULT_NOT_RETAINED` and the inspection methods remain available. It never
executes the algorithm twice under the same run UUID. A different descriptor
is `GF_IDEMPOTENCY_CONFLICT`.

`algorithm_run@1`:

| Field | Arrow type | Null |
| ------------------------ | ----------------------------- | ---- |
| `run_uuid` | `FixedSizeBinary(16)` | no |
| `algorithm` | `Utf8` | no |
| `algorithm_version` | `UInt32` | no |
| `descriptor_version` | `UInt32` | no |
| `descriptor` | `Binary` | no |
| `projection_fingerprint` | `FixedSizeBinary(32)` | no |
| `provenance_uuid` | `FixedSizeBinary(16)` | no |
| `started_at` | `Timestamp(Microsecond, UTC)` | no |
| `contract_version` | `UInt32` | no |

`algorithm_run_event@1`:

| Field | Arrow type | Null |
| -------------------- | ----------------------------- | ---- |
| `event_uuid` | `FixedSizeBinary(16)` | no |
| `run_uuid` | `FixedSizeBinary(16)` | no |
| `state` | `Utf8` | no |
| `result_fingerprint` | `FixedSizeBinary(32)` | yes |
| `error_code` | `Utf8` | yes |
| `recorded_at` | `Timestamp(Microsecond, UTC)` | no |
| `provenance_uuid` | `FixedSizeBinary(16)` | no |
| `contract_version` | `UInt32` | no |

Run lists order by `(started_at, run_uuid)`. Events order by
`(recorded_at, event_uuid)`.

## Idempotency and generated identities

Every durable public ledger write requires a caller-provided operation UUID and
primary record UUID. The operation UUID is project-global across all write
methods. Its stored entry contains method ID, canonical request fingerprint,
primary UUID, transaction UUID, and committed generation UUID.

- same operation UUID, method, and fingerprint: return the existing primary
  record without a new generation;
- same operation UUID with a different method or fingerprint:
  `GF_IDEMPOTENCY_CONFLICT`, no write;
- new operation UUID with an already-used primary UUID and identical record:
  return the existing record as an idempotent alias only when that record
  family explicitly permits it;
- new operation UUID with an already-used primary UUID and different record:
  `GF_IDENTITY_CONFLICT`, no write.

Generated companion provenance, lineage, input, and lifecycle-event UUIDs use
the registered UUIDv8 projection over their domain-separated canonical bytes.
Their full fingerprint is stored and collision-checked. The caller's operation
or run UUID is part of the canonical companion record, so two intentional
otherwise-identical operations remain distinct while a retry remains
identical.

Existing convenience graph writes (`execute` with a mutation, `add_node`, and
`add_edge`) generate a fresh operation UUID and therefore are not idempotent
across separate calls. Their graph effect and automatically generated
provenance still publish atomically. Retry-sensitive clients use the
implementation issues' context-bearing write variants; those variants retain
the existing graph request/result schemas and differ only by `WriteContext`.

## Pagination, snapshots, and cancellation

Every list call returns at most `limit` rows and places an opaque
`graphforge.next_page_token` value in schema metadata when another row exists.
The token contains canonical API version, method/filter fingerprint, pinned
generation UUID, and last complete sort tuple, plus a SHA-256 integrity digest.
It contains no record values other than UUID/time sort keys and is base64url
without padding.

The next call resolves that exact immutable generation. ADR 0013's default
retention guarantees the cursor survives one intervening publication. If its
generation has been collected, the call returns `GF_PAGE_SNAPSHOT_GONE`; it
never silently advances to `CURRENT`. A token used with another method, filter,
or limit returns `GF_PAGE_INVALID`. Pagination never splits one logical row.

Cancellation is checked before opening a requested participant, between Arrow
batches, and at least every 4,096 decoded rows. A cancelled operation returns
`GF_CANCELLED` and no partial table. Cancelling a write before publication
commits nothing. Cancellation after the `CURRENT` linearization point returns
the committed success/receipt; it cannot relabel a committed write as failed.

## Composite atomicity

These are the only knowledge-layer public composites:

| Operation | One atomic generation contains |
| -------------------------------- | ------------------------------------------------------------ |
| graph mutation | graph changes + provenance event + ordered lineage |
| `enable_capability` | capability manifest + every required initial participant |
| `create_assertion` | assertion + graph refs + provenance/lineage |
| `create_assertion_with_evidence` | assertion + refs + all evidence + provenance/lineage |
| `attach_evidence` | evidence + provenance/lineage |
| `assess_confidence` | assessment + normalized inputs + provenance/lineage |
| recorded run start | run identity + `started` event + provenance/lineage |
| recorded run terminal | one terminal event + result fingerprint + provenance/lineage |

Knowledge one-shots remain atomic generations. Mixed graph + knowledge work
uses `publish_composite_transaction` or the uniform `GraphTransaction`
lifecycle. There is no arbitrary participant write, arbitrary provenance
append, or partially successful composite. Run start and terminal are
intentionally two separate generations because algorithm execution occurs
between them.

## Error envelope

Rust errors expose methods, not parsed display text:

```rust
pub trait StructuredGraphForgeError {
    fn code(&self) -> ErrorCode;
    fn category(&self) -> ErrorCategory;
    fn operation_uuid(&self) -> Option<uuid::Uuid>;
    fn committed(&self) -> Option<bool>;
    fn retryable(&self) -> bool;
    fn safe_details(&self) -> &BTreeMap<String, String>;
}
```

The closed knowledge-layer error codes are:

| Category | Codes |
| ----------- | ------------------------------------------------------------------------------------------------------- |
| request | `GF_VALIDATION`, `GF_UNKNOWN_ARGUMENT`, `GF_NOT_FOUND` |
| identity | `GF_IDEMPOTENCY_CONFLICT`, `GF_IDENTITY_CONFLICT`, `GF_FINGERPRINT_COLLISION` |
| capability | `GF_CAPABILITY_DISABLED`, `GF_UNSUPPORTED_CAPABILITY_VERSION` |
| format | `GF_UNSUPPORTED_PROJECT_FORMAT`, `GF_PROJECT_UNINITIALIZED`, `GF_PROJECT_CORRUPT`, `GF_SCHEMA_MISMATCH` |
| transaction | `GF_WRITER_BUSY`, `GF_UNSUPPORTED_FILESYSTEM`, `GF_TRANSACTION_FAILED` |
| control | `GF_CANCELLED`, `GF_RESOURCE_LIMIT` |
| pagination | `GF_PAGE_INVALID`, `GF_PAGE_SNAPSHOT_GONE` |
| run | `GF_RESULT_NOT_RETAINED` |
| I/O | `GF_IO` |

`committed` is present only for writes and is always `true` or `false`.
Display messages are non-contractual. Safe details may contain method, phase,
domain/version, UUIDs, counts, limits, and expected/actual fingerprints. They
never contain claims, source content, reasoning, graph properties, vectors,
credentials, absolute paths, or raw canonical bytes.

## Python projection

Python exports the same request concepts as keyword-only native methods. UUID
inputs accept `uuid.UUID` or canonical RFC strings. Results are
`pyarrow.Table`; no Python record or schema implementation exists.

```python
forge.project_capabilities() -> pyarrow.Table
forge.enable_capability(*, operation_uuid, capability_id, capability_version,
                        actor_uuid=None) -> pyarrow.Table
forge.provenance_event(provenance_uuid, *, cancellation=None) -> pyarrow.Table
forge.list_provenance_history(*, subject_uuid=None, operation_uuid=None,
                              limit=100, after=None, cancellation=None) -> pyarrow.Table
forge.create_assertion(*, operation_uuid, assertion_uuid, claim, graph_refs,
                       actor_uuid=None) -> pyarrow.Table
forge.assertion(assertion_uuid, *, cancellation=None) -> pyarrow.Table
forge.list_assertions(*, graph_uuid=None, limit=100, after=None,
                      cancellation=None) -> pyarrow.Table
forge.assertion_graph_refs(assertion_uuid, *, limit=100, after=None,
                           cancellation=None) -> pyarrow.Table
forge.assess_confidence(*, operation_uuid, confidence_uuid, assertion_uuid,
                        policy, actor_uuid=None) -> pyarrow.Table
forge.confidence_assessment(confidence_uuid, *, cancellation=None) -> pyarrow.Table
forge.list_confidence_assessments(*, assertion_uuid=None, limit=100,
                                  after=None, cancellation=None) -> pyarrow.Table
forge.confidence_inputs(confidence_uuid, *, limit=100, after=None,
                        cancellation=None) -> pyarrow.Table
forge.attach_evidence(*, operation_uuid, evidence_uuid, assertion_uuid,
                      source_uuid, source_kind, role, weight=None,
                      actor_uuid=None) -> pyarrow.Table
forge.create_assertion_with_evidence(*, operation_uuid, assertion_uuid, claim,
                                     graph_refs, evidence,
                                     actor_uuid=None) -> pyarrow.Table
forge.evidence_link(evidence_uuid, *, cancellation=None) -> pyarrow.Table
forge.list_evidence_links(*, assertion_uuid=None, source_uuid=None, limit=100,
                          after=None, cancellation=None) -> pyarrow.Table
forge.invoke_recorded(*, operation_uuid, run_uuid, descriptor,
                      actor_uuid=None, cancellation=None) -> RecordedAlgorithmResult
forge.algorithm_run(run_uuid, *, cancellation=None) -> pyarrow.Table
forge.list_algorithm_runs(*, algorithm=None, limit=100, after=None,
                          cancellation=None) -> pyarrow.Table
forge.algorithm_run_events(run_uuid, *, limit=100, after=None,
                           cancellation=None) -> pyarrow.Table
```

`GraphForgeError` exposes `.code`, `.category`, `.operation_uuid`,
`.committed`, `.retryable`, and read-only `.safe_details`. Specific subclasses
may exist, but code is authoritative and no caller parses `.args` or text.

## Node projection

Node uses the camelCase names in the machine-readable fixture. Every ledger I/O
method returns a Promise. Table promises resolve to an Arrow IPC `Buffer`
containing the exact Rust schema and batches. UUID inputs are canonical strings.
`AbortSignal` maps to the Rust cancellation token.

```ts
forge.projectCapabilities(): Promise<Buffer>
forge.enableCapability(request: EnableCapabilityRequest): Promise<Buffer>
forge.provenanceEvent(uuid: string, signal?: AbortSignal): Promise<Buffer>
forge.listProvenanceHistory(request?: ProvenanceHistoryRequest): Promise<Buffer>
forge.createAssertion(request: CreateAssertionRequest): Promise<Buffer>
forge.assertion(uuid: string, signal?: AbortSignal): Promise<Buffer>
forge.listAssertions(request?: ListAssertionsRequest): Promise<Buffer>
forge.assertionGraphRefs(uuid: string, page?: PageRequest): Promise<Buffer>
forge.assessConfidence(request: AssessConfidenceRequest): Promise<Buffer>
forge.confidenceAssessment(uuid: string, signal?: AbortSignal): Promise<Buffer>
forge.listConfidenceAssessments(request?: ListConfidenceAssessmentsRequest): Promise<Buffer>
forge.confidenceInputs(uuid: string, page?: PageRequest): Promise<Buffer>
forge.attachEvidence(request: AttachEvidenceRequest): Promise<Buffer>
forge.createAssertionWithEvidence(request: CreateAssertionWithEvidenceRequest): Promise<Buffer>
forge.evidenceLink(uuid: string, signal?: AbortSignal): Promise<Buffer>
forge.listEvidenceLinks(request?: ListEvidenceLinksRequest): Promise<Buffer>
forge.invokeRecorded(request: RecordedAlgorithmRequest): Promise<RecordedAlgorithmResult>
forge.algorithmRun(uuid: string, signal?: AbortSignal): Promise<Buffer>
forge.listAlgorithmRuns(request?: ListAlgorithmRunsRequest): Promise<Buffer>
forge.algorithmRunEvents(uuid: string, page?: PageRequest): Promise<Buffer>
```

Rejected Promises carry an `Error` with enumerable `code`, `category`,
`operationUuid`, `committed`, `retryable`, and `safeDetails`. TypeScript types
mirror those properties; native code sets them directly.

## CLI projection

The knowledge layer exposes read-only inspection:

```text
gf ledger capabilities
gf ledger provenance --uuid <uuid>
gf ledger assertions [--graph-uuid <uuid>]
gf ledger confidence [--assertion-uuid <uuid>]
gf ledger evidence [--assertion-uuid <uuid>] [--source-uuid <uuid>]
gf ledger runs [--run-uuid <uuid>] [--algorithm <id>]
```

Default output is Arrow IPC on stdout when piped and a bounded table when
interactive. `--format json` emits row JSON without changing order. Errors on
stderr use `{"error":{"code":...}}`; `--error-format text` is display-only.
CLI writes are out of scope for knowledge-layer.

## Removed and forbidden API

`ExecuteOptions.confidence`, `execute_with_options`, Python
`execute(..., confidence=...)`, and Node confidence execution options are
removed in. They do not warn, alias to assessments, or inject columns:
Rust no longer compiles against them; Python rejects the keyword; Node's
generated type omits it and native validation returns `GF_UNKNOWN_ARGUMENT`
for an untyped object.

No Rust, Python, Node, or CLI symbol may be named or aliased as:

```text
migrate
migrate_project
upgrade_project
import_v04
open_legacy
generation_zero
```

Opening any pre-v1 root returns `GF_UNSUPPORTED_PROJECT_FORMAT` before graph or
ledger files are opened and without mutation.

## Epistemic extension seam

The epistemic layer adds shipped methods and request types for status events, reasoning
amendments, supersession, hypotheses, snapshots, valid time, and belief
projection. It owns `epistemic@1`, optional `valid_time@1`, and separate
schema-registry entries. It may not:

- add a field to a knowledge-layer request or output schema;
- change a knowledge-layer sort, cursor, error, fingerprint, UUID, or idempotency rule;
- infer an initial assertion status;
- make a knowledge-layer or graph method open epistemic participants; or
- add knowledge selectors to the direct analyst-verb/find method signatures.

Resolved belief is materialized by the epistemic API into a neutral graph
projection before the existing algorithm descriptor dispatch.

## Required verification

- Rust compile-time API tests consume the machine-readable method/schema
  inventory and instantiate every request.
- Python signature and exception tests call a same-SHA native wheel.
- Node TypeScript and runtime tests call a same-SHA native addon and decode IPC.
- Each surface runs identical idempotency, ordering, pagination, cancellation,
  capability, corruption, and error-code vectors.
- Repository policy rejects forbidden migration and confidence-execution
  symbols. Binding and policy tests implement the boundary.
