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

Existing calls retain their algorithm defaults. Optional keyword controls select
fixed rounds of PageRank (`iterations`, including zero) and its `damping` factor,
synchronous label propagation (`synchronous_iterations`, including zero), or the
neighbor-edge clustering coefficient (`clustering_normalization="neighbor_edges"`).
The default clustering normalization is `"fagiolo"`.

```python
forge.rank("Person", by="pagerank", damping=0.85, iterations=20)
forge.cluster("Person", by="label_propagation", synchronous_iterations=10,
              initial_label_property="external_id")
forge.rank("Person", by="clustering_coefficient", directed=True,
           clustering_normalization="neighbor_edges")
```

`initial_label_property` requires synchronous rounds and an Int64 property on
every selected vertex. Its values become the output labels, with ties resolved
to the smallest label. Without the property, initial labels are selected vertex
ordinals. Rank and cluster invocation descriptors accept the same controls,
including descriptors prepared from resolved belief projections. Rust owns the
algorithms and validates their semantics.
