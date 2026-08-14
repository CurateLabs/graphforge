# Graph Construction

Build graphs with the Python API or openCypher. This page is the everyday
construction path for v0.5.0: scalar nodes and edges first, then atomic bulk
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

Properties may be strings, numbers, booleans, or nested lists/maps that the
engine accepts as property values.

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
print(table.to_pandas())
```

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
convenience helpers `add_nodes` / `add_edges`. Always pass a stable
`operation_uuid`; the call returns a canonical receipt table
(`entity_uuid`, `row_ordinal`, generation identity, …).

Exact retry with the same operation UUID and same normalized input returns the
same ordered receipt without another generation. Reusing the UUID with different
input fails with `GF_IDEMPOTENCY_CONFLICT` and does not mutate storage.

The operation identity must be a **UUIDv7**; a UUIDv4 is rejected with
`GF_BULK_VALIDATION(invalid_uuid)`. Python 3.14 ships `uuid.uuid7()`; on earlier
versions generate one with the stdlib helper below.

```python
import os
import time
import uuid


def uuid7() -> uuid.UUID:
    """RFC 9562 UUIDv7. Use uuid.uuid7() directly on Python 3.14+."""
    stamp = int(time.time() * 1000).to_bytes(6, "big")
    raw = bytearray(stamp + os.urandom(10))
    raw[6] = (raw[6] & 0x0F) | 0x70  # version 7
    raw[8] = (raw[8] & 0x3F) | 0x80  # RFC 9562 variant
    return uuid.UUID(bytes=bytes(raw))


node_op = str(uuid7())
edge_op = str(uuid7())

nodes = forge.add_nodes(
    "Person",
    [{"name": "Alice"}, {"name": "Bob"}],
    operation_uuid=node_op,
)
edges = forge.add_edges(
    "KNOWS",
    [
        {
            "src_id": nodes.column("entity_uuid")[0].as_py(),
            "dst_id": nodes.column("entity_uuid")[1].as_py(),
        }
    ],
    operation_uuid=edge_op,
)
assert nodes.num_rows == 2 and edges.num_rows == 1
```

`add_edges` renames endpoint columns (`src` / `dst`, default `src_id` /
`dst_id`) to `source_uuid` / `target_uuid` before publication. Endpoint UUIDs
must already identify committed nodes.

For pandas / Polars / Arrow inputs and the full receipt schema, see the
[API Reference](../reference/api.md#construction).

---

## Other bindings (brief)

Rust and Node project onto the same bulk contracts. Identity, ontology
validation, idempotency, and publication stay Rust-owned; bindings only convert
at the boundary.

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
3. **Pass `operation_uuid` on every bulk call** and keep it stable for retries.
4. **Close persistent graphs** (`forge.close()`) when finished with a directory
   project.
5. **Consume Arrow results** with `to_pandas()`, `polars.from_arrow`, or
   `to_pylist()` — there are no `CypherValue` wrappers in v0.5.0.

---

## Next steps

- [Cypher Guide](cypher-guide.md) — full openCypher reference
- [Quick Start](quickstart.md) — five-minute walkthrough
- [Tutorial](tutorial.md) — guided citation-network example
- [API Reference](../reference/api.md) — complete method signatures
