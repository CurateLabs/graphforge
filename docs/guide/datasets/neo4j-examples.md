# Neo4j Graph Examples

> **Status:** Backlog — not shipped in v0.5.0 (see [Datasets overview](overview.md)).

Planned support for Neo4j's curated collection of example graph datasets.

## Overview

Neo4j Graph Examples provides educational and demonstration datasets that are perfect for learning Cypher and exploring graph concepts.

## Dataset Source

- **URL:** https://github.com/neo4j-graph-examples
- **License:** Apache 2.0 (most datasets)
- **Category:** Educational, Demonstrations

## Available Datasets

### Popular Examples

| Dataset | Nodes | Edges | Size | Description |
|---------|-------|-------|------|-------------|
| movie-graph | ~170 | ~250 | ~50 KB | Classic movie database |
| northwind | ~1K | ~3K | ~100 KB | Northwind business data |
| game-of-thrones | ~800 | ~3K | ~150 KB | Character relationships |
| stackoverflow | ~50K | ~200K | ~15 MB | Q&A network |
| twitter | ~2K | ~8K | ~500 KB | Twitter interactions |

## Schema Examples

### Movie Graph

**Nodes:**
- `Person` (actors, directors)
- `Movie`

**Relationships:**
- `ACTED_IN`
- `DIRECTED`
- `PRODUCED`
- `REVIEWED`

## Usage

```python
from graphforge import GraphForge
from graphforge.datasets import load_dataset

# Load the movie graph
gf = GraphForge()
load_dataset(gf, "neo4j-movie-graph")

# Find movies and actors
results = gf.execute("""
    MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
    WHERE m.released > 2000
    RETURN p.name, m.title, m.released
    ORDER BY m.released DESC
""")
```

## Example Queries

### Movie Recommendations

```cypher
MATCH (p:Person {name: 'Tom Hanks'})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(coActor:Person)
MATCH (coActor)-[:ACTED_IN]->(rec:Movie)
WHERE NOT (p)-[:ACTED_IN]->(rec)
RETURN DISTINCT rec.title AS recommendation
LIMIT 10
```

### Director Analysis

```cypher
MATCH (d:Person)-[:DIRECTED]->(m:Movie)
RETURN d.name AS director, count(m) AS moviesDirected
ORDER BY moviesDirected DESC
```

## CLI Usage

```bash
# List Neo4j example datasets
graphforge list-datasets --source neo4j-examples

# Load movie graph
graphforge load-dataset neo4j-movie-graph
```

## Learning Resources

These datasets are ideal for:
- Learning Cypher syntax
- Understanding graph modeling
- Demonstrations and presentations
- Testing queries before production

## References

- [Neo4j Graph Examples Repository](https://github.com/neo4j-graph-examples)
- [Neo4j GraphAcademy](https://graphacademy.neo4j.com/)

## Related

- [Dataset Overview](overview.md)
- [Cypher Guide](../cypher-guide.md)
- [Tutorial](../tutorial.md)
