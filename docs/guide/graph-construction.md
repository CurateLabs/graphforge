# Graph Construction

**Advanced:** assumes basic Python and the [first graph lesson](quickstart.md).
Use this reference to map your own records into nodes, properties, and
relationships. Start with scalar construction; bulk publication is optional.

Build graphs with the Python API or openCypher. This page is the everyday
construction path: scalar nodes and edges first, then atomic bulk
batches. Scalar `add_node` / `add_edge` return construction handles; atomic bulk
publish and Cypher/analyst paths return Apache Arrow tables (including bulk
receipts).

For deeper architecture (project generations, Rust-owned validation, binding
parity), see the [Book](../book/README.md).

---

## Scalar construction (Python)

### Create a graph

```python
from graphforge import GraphForge

forge = GraphForge()              # in-memory
# forge = GraphForge("my-graph/") # Parquet-backed on disk (directory must exist)
```

### Add nodes

`add_node` returns a `NodeHandle` with stable `.uuid` identity (UUIDv7) — there
is no numeric storage surrogate.

```python
alice = forge.add_node("Person", name="Alice", age=30, city="NYC")
bob = forge.add_node("Person", name="Bob", age=25)
print(alice.uuid)  # e.g. 018f0f4e-7b8c-7000-8000-00000000a001
```

Properties may be null, strings, numbers, booleans, or lists of supported
property values. Plain nested Python dictionaries are
not properties. Dictionary-shaped inputs are reserved for validated canonical
temporal/spatial values; an ordinary dictionary is refused.

### Add relationships

```python
forge.add_edge(alice, "KNOWS", bob, since=2020, strength="strong")
```

`add_edge` returns an `EdgeHandle` with its own `.uuid`. Endpoints are
`NodeHandle` values from the same graph (or UUID selectors where the API
accepts them).

### Query what you built

```python
table = forge.execute("""
    MATCH (a:Person)-[r:KNOWS]->(b:Person)
    RETURN a.name AS from, b.name AS to, r.since AS since
""")
print(table.to_pylist())
```

Expected output: `[{'from': 'Alice', 'to': 'Bob', 'since': 2020}]`.
This needs no pandas installation.

---

## Construction with Cypher

You can also create, update, and delete with openCypher. Prefer parameters over
string interpolation.

### CREATE

```python
forge.execute(
    "CREATE (p:Person {name: $name, age: $age})",
    {"name": "Charlie", "age": 35},
)

forge.execute("""
    CREATE (a:Person {name: 'Dana'})-[:KNOWS {since: 2021}]->(b:Person {name: 'Eve'})
""")
```

### MERGE (idempotent upsert)

Standalone new-node MERGE and `ON CREATE` / `ON MATCH` property sets are
supported. Prefer binding endpoints first, then merging the relationship —
inline multi-node MERGE patterns are rejected. See
[MERGE status](../reference/implementation-status/clauses.md#merge).

```python
forge.execute("""
    MERGE (p:Person {email: $email})
    ON CREATE SET p.name = $name, p.created = 2024
    ON MATCH  SET p.last_seen = 2024
""", {"email": "alice@example.com", "name": "Alice"})
```

### SET / REMOVE / DELETE

```python
forge.execute("""
    MATCH (p:Person {name: 'Alice'})
    SET p.age = 31, p.city = 'Boston'
""")

forge.execute("""
    MATCH (p:Person {name: 'Alice'})
    REMOVE p.temp
""")

# DETACH DELETE removes the node and its relationships
forge.execute("""
    MATCH (p:Person {name: 'Eve'})
    DETACH DELETE p
""")
```

### Batch create with UNWIND

```python
forge.execute("""
    UNWIND $people AS person
    CREATE (p:Person)
    SET p = person
""", {
    "people": [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ],
})
```

See the [Cypher Guide](cypher-guide.md) for the full clause reference.

---

## Atomic bulk construction (Python)

For many rows at once, use the Rust-owned bulk surfaces. Prefer
`publish_bulk_nodes` / `publish_bulk_edges` with a canonical Arrow table, or the
convenience helpers `add_nodes` / `add_edges`. Each call returns a canonical
receipt table (`entity_uuid`, `row_ordinal`, operation and generation identity,
…).

The `operation_uuid` is optional: when omitted, GraphForge mints a UUIDv7
operation identity in Rust and returns it in the receipt's `operation_uuid`
column. Pass that identity back explicitly to retry the same batch; reuse with
changed input fails with `GF_IDEMPOTENCY_CONFLICT` and does not mutate storage.
Each publication needs its own identity: reusing the node load's identity for
the edge load is a conflicting reuse.

If a call raises after the identity was minted, the exception carries it as
`error.operation_uuid`. On any error, retry with
`operation_uuid=error.operation_uuid` (and the same input) rather than omitting
it: the commit may have succeeded before the failure, and a retry with a new
identity would load the data twice.

```python
nodes = forge.add_nodes(
    "Person",
    [{"name": "Alice"}, {"name": "Bob"}],
)

edges = forge.add_edges(
    "KNOWS",
    [
        {
            "src_id": nodes.column("entity_uuid")[0].as_py(),
            "dst_id": nodes.column("entity_uuid")[1].as_py(),
        }
    ],
)
assert nodes.num_rows == 2 and edges.num_rows == 1
```

`add_edges` renames endpoint columns (`src` / `dst`, default `src_id` /
`dst_id`) to `source_uuid` / `target_uuid` before publication. Endpoint UUIDs
must already identify committed nodes.

Bulk property columns, here and in import sessions, may use narrower or large
Arrow types: `Int8`–`Int32`, `UInt8`–`UInt32`, `Float32`, `LargeUtf8` and
`LargeList`, also as list elements. They are widened losslessly and stored and
returned as `Int64`, `Float64`, `Utf8` and `List`, so filters and ordering
behave exactly as for data supplied in those types. `UInt64` is not accepted.

For pandas / Polars / Arrow inputs and the full receipt schema, see the
[API Reference](../reference/api.md#construction).

---

## Other bindings (brief)

Rust and Node project onto the same bulk contracts. Identity, ontology
validation, idempotency, and publication stay Rust-owned; bindings only convert
at the boundary. Both bindings accept an omitted operation identity and mint a
Rust-owned UUIDv7; Node passes it as the optional second argument,
`publishBulkNodes(batch, operationUuid?)`. An explicitly supplied identity must
still be a **UUIDv7**; a UUIDv4 is rejected with
`GF_BULK_VALIDATION(invalid_uuid)`. A thrown error carries the identity as
`error.operationUuid` (Python: `error.operation_uuid`); on any error, retry with
that identity.

Node intentionally exposes only `publishBulkNodes` / `publishBulkEdges` for
bulk construction. The Python-only `add_nodes` / `add_edges` helpers normalize
Python containers before calling those same Rust-owned publication operations;
there are no Node `addNodes` / `addEdges` stubs or JavaScript publication path.

```rust
// Rust — publish_bulk_nodes / publish_bulk_edges on graphforge_api::GraphForge
```

```js
// Node — publishBulkNodes / publishBulkEdges over Arrow IPC
import { GraphForge } from "@curatelabs/graphforge";
```

Cross-binding bulk parity is exercised by the opt-in conformance runner (not a
required PR CI Gate):

```bash
python3 scripts/ci/bulk-construction-conformance.py validate
```

---

## Best practices

1. **Use parameters** in Cypher — never interpolate untrusted strings into
   queries.
2. **Prefer `MERGE`** when re-running ETL or scripts that must be idempotent.
3. **Keep one operation identity per logical bulk load** for retries: pass
   `operation_uuid` explicitly, or capture the identity the receipt returns
   when you omitted it, and reuse exactly that identity and input on retry.
4. **Close persistent graphs** (`forge.close()`) when finished with a directory
   project. An open instance keeps OS handles on the committed generation it
   reads from, including a directory handle on `generations/<uuid>/graph`;
   `close()` releases them, which Windows requires before the project directory
   can be moved or removed.
5. **Consume Arrow results** with `to_pandas()`, `polars.from_arrow`, or
   `to_pylist()` — there are no `CypherValue` wrappers.

---

## Next steps

- [Cypher Guide](cypher-guide.md) — full openCypher reference
- [Quick Start](quickstart.md) — five-minute walkthrough
- [Tutorial](tutorial.md) — guided citation-network example
- [API Reference](../reference/api.md) — complete method signatures

### Publication and existing readers

Successful resumable construction updates the same facade's workspace, property
inventory, adjacency provider and ordinal identity authority together. Immediate
queries therefore observe the published relationships and endpoints without
requiring reopen. Publication prepares one new workspace and adopts its owner;
it does not rename directories pinned by existing readers. A lazy stream and its
runtime guard retain the workspace selected when the stream was created, so
subsequent construction cannot change that stream's identities or remove its
files. Those overlapping live workspaces remain real resource owners until the
last reader releases them.

If reader preparation fails after durable publication, the error still reports
`committed=true` and recovery by reopen or resume. The facade retains its prior
workspace until all replacement readers are ready; exact retry adopts the
already committed generation.

### Import operation timing receipts

`import-session validate` and `import-session commit` JSON receipts include
`operation_timings`. Each of the five closed rows (`begin`, `resume`, `append`,
`seal`, `publish`) contains `calls`, `errors`, and `elapsed_ns`. These are
monotonic wall-time observations around disjoint public construction calls.
These diagnostics are exposed through Rust and the CLI.
`GraphImportSession::operation_timings()` exposes the same observations after
the latest validation or commit, including returned errors. Each invocation
resets them before checking preconditions; a repeated operation cannot replay
previously reported time.

`begin` measures fresh construction creation; `resume` measures reopening and
reconciling construction authority. `append` covers construction append calls,
`seal` covers validation/sealing, and `publish` covers seal-and-publication.
Internal authentication performed within those calls belongs to that call's
wall time: the timing rows do not have the same boundaries as the durable
`application_io` attribution rows. In particular, `resume` is not the total time
of every recovery-authentication read elsewhere in sealing or publication.

Source decoding, normalization, facade opening and import-manifest checkpoints
outside those calls are excluded. The sum therefore does not equal whole-ingest
time. These values are observations, not performance limits or CPU measurements.
The certifier preserves only the closed numeric timing object; it rejects
unknown fields and invalid counters.

Timings are held only by the live handle and never written into import or
construction manifests. Durable `status` does not return old operation timings.
Successful CLI validate/commit receipts can be summed for an invocation sequence.
A failed CLI command does not emit a progress receipt, and a lost process cannot
report its missing time; neither duration is reconstructed from persisted state.
Existing durable I/O, allocation, accepted-row and recovery evidence remains
independent of these timing observations.
