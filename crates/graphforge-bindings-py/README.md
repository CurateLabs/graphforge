# GraphForge for Python

Native Python bindings for GraphForge — an embedded openCypher graph engine
with Rust-owned behavior, Arrow results, and a thin repository lifecycle CLI.

## Install

**pip**

```bash
pip install "graphforge==0.5.1"
```

**uv** (recommended)

```bash
uv add "graphforge==0.5.1"
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
- [Repository integration](https://docs.graphforge.sh/guides/repository-integration/)
- [Full documentation](https://docs.graphforge.sh/)
