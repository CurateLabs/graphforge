# GraphForge for Python

Native Python bindings for GraphForge — an embedded openCypher graph engine
with Rust-owned behavior, Arrow results, and a thin repository lifecycle CLI.

## External decision results

Provider-neutral results use the public `graphforge.DecisionBatchV1` TypedDict
contract. Pass one to `GraphForge.validate_decision_batch()`; Rust validates
correlation and values and returns a `pyarrow.Table` using `decision_result/1`.
Producer execution, thresholds, review and actions stay in caller code. See
the [decision workflow guide](../../docs/book/use-cases/decision-workflows.md).

## Install

**pip**

```bash
pip install graphforge
```

**uv** (recommended)

```bash
uv add graphforge
```

## First use

```python
from graphforge import GraphForge

forge = GraphForge()  # in-memory; pass a directory path for persistence

alice = forge.add_node("Person", name="Alice", age=30)
bob = forge.add_node("Person", name="Bob", age=25)
forge.add_edge(alice, "KNOWS", bob, since=2020)

table = forge.execute("""
    MATCH (p:Person)-[:KNOWS]->(friend:Person)
    WHERE p.age > 25
    RETURN p.name AS person, friend.name AS friend
""")
print(table.to_pandas())
```

`execute()` returns an Apache Arrow `Table`. The `graphforge` console entry
point launches the same Rust-owned repository CLI used by `gf` and
`npx @curatelabs/graphforge-cli`.

## Documentation

- [Quick start](https://docs.graphforge.sh/guide/quickstart/)
- [Installation](https://docs.graphforge.sh/guide/installation/)
- [Repository integration](https://docs.graphforge.sh/guide/repository-integration/)
- [Full documentation](https://docs.graphforge.sh/)

## Algorithm definitions

`rank()` supports fixed PageRank `iterations` and `damping`, and directed
`clustering_normalization="neighbor_edges"`. `cluster()` supports deterministic
`synchronous_iterations` with an optional Int64 `initial_label_property`.
Existing calls retain their defaults. See the API reference in the full
documentation for definitions and invocation descriptors.
