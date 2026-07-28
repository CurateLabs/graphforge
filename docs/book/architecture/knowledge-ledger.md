# Immutable knowledge ledger

The knowledge layer is the shipped v0.5 foundation for immutable provenance and analytical
knowledge. It adds append-only domain records around graph mutations and analyst-verb
algorithm runs without changing graph execution or Arrow result schemas.

The knowledge layer deliberately does not place current belief, reasoning, supersession,
hypotheses, or valid-time interpretation in its rows. The shipped epistemic layer
implements those concerns as additive records.

## Ownership and isolation

The crate boundary is part of the storage contract:

| Owner | Responsibility |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `gf-storage` | v0.5 project manifest, immutable generations, participant checksums, atomic publication, recovery, and the single-writer lock |
| `gf-provenance` | provenance events, lineage schemas, validation, ordering, and canonical identities |
| `gf-knowledge` | knowledge assertions, graph references, confidence, evidence, and algorithm runs; additive epistemic and valid-time records |
| `gf-api` | public requests, cross-domain UUID validation, idempotency, and assembly of one atomic generation |
| Python and Node bindings | thin projections of the Rust API and its Arrow results |

Graph reads and neutral analyst-verb/find computation never open provenance or knowledge
participants. An absent, corrupt, or future-version knowledge capability cannot
change `execute`, `rank`, `cluster`, `paths`, `analyze`, `similar`, or `find`.
Knowledge is consulted only when a caller explicitly invokes a knowledge API.

## Project lifecycle

A v0.5 project root contains a versioned capability manifest and an immutable
committed-generation pointer. Each generation names every participant by
relative path, content length, and SHA-256. Readers resolve and pin one valid
committed generation when the project opens.

One writer holds an OS-backed project lock. A write:

1. resolves the current committed generation;
2. validates the operation UUID and any prior idempotent result;
3. stages complete replacement participants in a private transaction directory;
4. validates domain-local records and cross-domain UUID references;
5. syncs participant files and the transaction journal;
6. publishes an immutable generation directory;
7. atomically replaces the committed-generation pointer; and
8. syncs the project directory before releasing the lock.

Before step 7, readers continue to see the old generation. After step 7, they
see the complete new generation. Recovery removes abandoned staging state and
accepts only a generation whose journal, inventory, lengths, checksums, and
manifest linkage all validate. No partial generation is exposed.

## Capabilities

The manifest has independent versioned entries for graph, provenance,
knowledge, `epistemic@1`, and optional `valid_time@1`. Absence means “not enabled.”
An unknown greater version means “unsupported by this binary”; it is never
treated as absent.

`project_capabilities()` reads only the manifest. `enable_capability()` is an
idempotent atomic generation transition. Graph-only projects need no
provenance or knowledge files.

```python
import graphforge as g

graph_only = g.GraphForge("/data/graph-only")
assert graph_only.execute("RETURN 1 AS value").column("value").to_pylist() == [1]

try:
    graph_only.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000099",
        capability_id="knowledge",
        capability_version=2,
    )
except g.GraphForgeError as error:
    assert error.code == "GF_UNSUPPORTED_CAPABILITY_VERSION"
else:
    raise AssertionError("future capability version was accepted")
```

## Immutable records

All public record identities are UUIDs. All list methods use deterministic
sort keys and exact cursor semantics. Public tabular results are Arrow tables.

| Capability | Record families |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| provenance v1 | `events`, `lineage` |
| knowledge v1 | `assertions`, `assertion_graph_refs`, `confidence_assessments`, `confidence_inputs`, `evidence`, `algorithm_runs`, `algorithm_run_events` |
| epistemic v1 | `assertion_status_events`, `reasoning`, `assertion_supersessions`, `hypothesis_groups`, `hypothesis_membership_events`, `hypothesis_selection_events`, `algorithm_interpretation_attachments` |
| valid_time v1 | `assertion_validity_events` |

The exact field order, Arrow type, nullability, schema fingerprint, enum
registry version, sort key, row limit, owner, and implementation issue are in
the generated [Knowledge schema inventory](../../reference/m20-schema-inventory.json).
Its companion
[`m20-schema-inventory.sha256`](../../reference/m20-schema-inventory.sha256)
binds the bytes. The inventory is assembled directly from the two owning Rust
registries; `gf-storage`, bindings, and documentation do not define duplicate
schemas.

To review and accept an intentional registry change:

```bash
UPDATE_M20_SCHEMA_INVENTORY=1 \
  cargo test -p gf-api --lib m20_schema_inventory_matches_checked_contract
git diff -- docs/reference/m20-schema-inventory.*
cargo test -p gf-api --lib m20_schema_inventory_matches_checked_contract
```

The test compares generated and checked bytes exactly. A new record family,
field, type, nullability, metadata entry, version, enum registry, sort key, or
fingerprint therefore fails CI until its reviewed inventory is committed.

## Deterministic knowledge-layer contract gate

`tests/contracts/m20-contract-matrix.json` is the finite acceptance ledger.
Every row names its exact Rust, Python, or Node test IDs and the command group
that executes them. The validator rejects missing rows, stale test symbols,
ignored or skipped tests, binding omissions, an incomplete analyst-verb catalog
partition, or schema-inventory drift:

```bash
python3 scripts/ci/m20-contract-gate.py validate
python3 scripts/ci/test-m20-contract-gate.py
```

The **M20 Contract Gate** workflow is intentionally manual. It is used for
contract closure or explicit revalidation, not as another full build on every
small pull request. One dispatch runs the checked Rust, clean-wheel Python, and
fresh-addon Node command groups from the same commit. The final
`M20-Contract-Gate-<sha>` artifact contains the commit SHA, toolchain versions,
matrix and schema digests, exact commands, logs, test IDs, row outcomes, and a
SHA-256-bound report. Any missing fragment or failed command prevents report
generation.

Unsupported pre-v1 project roots have exactly one closure row per binding:
return `GF_UNSUPPORTED_PROJECT_FORMAT` before mutation. There is no migration,
import, or backward-compatibility path for unsupported layouts.

## Public API behavior

Rust is authoritative. Python returns `pyarrow.Table`; Node returns Arrow IPC
decoded as an Arrow table. The frozen request, output, pagination, and error
contracts are specified in [Immutable knowledge public API v1](m20-public-api-v1.md).

A typical persistent flow is:

```python
from graphforge import GraphForge

forge = GraphForge("/data/project")
forge.enable_capability(
    operation_uuid="018f0f4e-7b8c-7000-8000-000000000001",
    capability_id="provenance",
    capability_version=1,
)
node = forge.add_node("Person", name="Ada")
forge.enable_capability(
    operation_uuid="018f0f4e-7b8c-7000-8000-000000000002",
    capability_id="knowledge",
    capability_version=1,
)
assertion = forge.create_assertion(
    operation_uuid="018f0f4e-7b8c-7000-8000-000000000003",
    assertion_uuid="018f0f4e-7b8c-7000-8000-000000000004",
    claim="reviewed claim",
    graph_refs=[{
        "graph_uuid": node.uuid,
        "graph_kind": "node",
        "role": "subject",
        "ordinal": 0,
    }],
)
assert assertion.num_rows == 1
forge.close()

reopened = GraphForge("/data/project")
assert reopened.assertion(
    "018f0f4e-7b8c-7000-8000-000000000004"
).equals(assertion)
```

The Node binding projects the same Rust request and returns Arrow IPC:

```javascript
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "@graphforge/node";

const forge = new GraphForge("/data/project");
await forge.enableCapability({
  operationUuid: "018f0f4e-7b8c-7000-8000-000000000011",
  capabilityId: "provenance",
  capabilityVersion: 1,
});
const node = forge.addNode("Person", { name: "Ada" });
await forge.enableCapability({
  operationUuid: "018f0f4e-7b8c-7000-8000-000000000012",
  capabilityId: "knowledge",
  capabilityVersion: 1,
});
const table = tableFromIPC(
  await forge.createAssertion({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000013",
    assertionUuid: "018f0f4e-7b8c-7000-8000-000000000014",
    claim: "reviewed claim",
    graphRefs: [
      {
        graphUuid: node.uuid,
        graphKind: "node",
        role: "subject",
        ordinal: 0,
      },
    ],
  }),
);
if (table.numRows !== 1) throw new Error("assertion was not committed");
```

These shapes are executed by the same-SHA Python and Node binding acceptance
tests; neither binding contains a knowledge implementation.

The same operation UUID plus byte-equivalent request returns the original
result. Reusing it with a different request fails with
`GF_IDEMPOTENCY_CONFLICT`. Record UUID reuse with different canonical content
also fails; immutable rows are never updated in place.

## Failure contract

Failures are structured and stable:

- format and project errors reject unsupported, corrupt, or uninitialized
  roots;
- capability and schema errors reject absent, future, or mismatched contracts;
- validation and reference errors reject invalid values or dangling UUIDs;
- conflict errors reject incompatible idempotent replay or concurrent writes;
- cancellation and limits stop work without publishing a generation.

GraphForge is pre-v1. It opens only a valid v0.5 project container. Unsupported
or unrecognized roots return `GF_UNSUPPORTED_PROJECT_FORMAT` before any file is
created, deleted, renamed, or rewritten. There is no importer or in-place
upgrade path. See
[Pre-v1 project compatibility](project-format-compatibility.md).

## Security and operating limits

- Participant paths are normalized project-relative paths; absolute paths,
  traversal, symlink escapes, and reserved control paths are rejected.
- Diagnostics contain safe IDs, versions, fingerprints, row counts, and phases,
  not graph properties, claim/evidence text, vectors, credentials, or
  unrestricted paths.
- Record text, binary values, nesting, page sizes, and participant row counts
  are bounded before allocation and persistence.
- Crash recovery validates checksums and never interprets uncommitted staging
  data as a generation.
- v0.5 supports a local filesystem and one writer per project. Distributed
  transactions and multiple concurrent writers are outside knowledge-layer.

## Epistemic boundary

The epistemic layer appends status, reasoning amendments, supersession, hypothesis,
selection, optional valid-time, and algorithm-interpretation records that
reference knowledge UUIDs. It does not rewrite a knowledge row, change a knowledge schema or
fingerprint, inject knowledge fields into graph results, or make neutral
graph/analyst-verb/find paths open knowledge participants.

The exact fields and versions are generated in the
[Epistemic schema inventory](../../reference/m21-schema-inventory.json). Resolution
reconstructs mandatory transaction time first, uses `(recorded_at, event_uuid)`
for current-event ties, then applies optional valid time. See
[ADR 0006](../../adr/0006-epistemic-model.md) for the complete policy and
resolved-dispatch contract.
