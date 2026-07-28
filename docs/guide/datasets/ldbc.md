# LDBC Datasets

> **Status:** Backlog — not shipped in v0.5.0 (see [Datasets overview](overview.md)).

Planned support for LDBC (Linked Data Benchmark Council) benchmark datasets.

## Overview

The LDBC Social Network Benchmark (SNB) is a comprehensive benchmark for graph databases, featuring realistic social network data with complex queries.

## Dataset Source

- **URL:** https://ldbcouncil.org/resources/datasets/
- **License:** Varies by dataset
- **Category:** Benchmark, Social Network

## Available Datasets

### LDBC SNB (Social Network Benchmark)

Realistic social network data with various scale factors:

| Scale Factor | Nodes | Edges | Size | Description |
|--------------|-------|-------|------|-------------|
| SF0.001 | ~3K | ~17K | ~2 MB | Tiny (testing) |
| SF0.003 | ~9K | ~52K | ~5 MB | Small (development) |
| SF0.01 | ~30K | ~177K | ~17 MB | Medium (testing) |
| SF0.1 | ~328K | ~1.8M | ~180 MB | Standard (benchmarking) |
| SF1 | ~3.2M | ~18M | ~1.8 GB | Large (performance testing) |

## Schema

The LDBC SNB schema includes:

**Node Labels:**
- `Person` - Social network users
- `Post` - User posts
- `Comment` - Comments on posts
- `Forum` - Discussion forums
- `Organisation` - Companies and universities
- `Place` - Cities and countries
- `Tag` - Content tags
- `TagClass` - Tag categories

**Relationship Types:**
- `KNOWS` - Person to person connections
- `LIKES` - Likes on posts/comments
- `HAS_CREATOR` - Content authorship
- `REPLY_OF` - Comment replies
- `HAS_TAG` - Content tagging
- `IS_LOCATED_IN` - Location relationships
- And more...

## Usage

```python
from graphforge import GraphForge
from graphforge.datasets import load_dataset

# Load a small LDBC dataset
gf = GraphForge()
load_dataset(gf, "ldbc-snb-sf001")

# Explore the social network
results = gf.execute("""
    MATCH (p:Person)-[:KNOWS]->(friend:Person)
    RETURN p.firstName, p.lastName, count(friend) AS friendCount
    ORDER BY friendCount DESC
    LIMIT 10
""")
```

## Example Queries

### Find Popular Posts

```cypher
MATCH (post:Post)<-[:LIKES]-(liker:Person)
RETURN post.content, count(liker) AS likes
ORDER BY likes DESC
LIMIT 10
```

### Community Detection

```cypher
MATCH (p:Person)-[:KNOWS*2]-(friend:Person)
WHERE p <> friend
RETURN p.firstName, p.lastName, count(DISTINCT friend) AS secondDegreeConnections
ORDER BY secondDegreeConnections DESC
LIMIT 20
```

### Content Analysis

```cypher
MATCH (tag:Tag)<-[:HAS_TAG]-(post:Post)
RETURN tag.name, count(post) AS postCount
ORDER BY postCount DESC
LIMIT 15
```

## Benchmarking

LDBC SNB is designed for benchmarking with standardized queries:

```python
from graphforge import GraphForge
from graphforge.datasets import load_dataset
import time

gf = GraphForge()
load_dataset(gf, "ldbc-snb-sf01")

# Benchmark query execution
start = time.time()
results = gf.execute("""
    MATCH (person:Person)-[:KNOWS*1..2]-(friend:Person)
    WHERE person.id = 123
    RETURN DISTINCT friend.firstName, friend.lastName
""")
elapsed = time.time() - start

print(f"Query executed in {elapsed:.3f} seconds")
print(f"Results: {len(results)} friends found")
```

## CLI Usage

```bash
# List available LDBC datasets
graphforge list-datasets --source ldbc

# Load a specific scale factor
graphforge load-dataset ldbc-snb-sf001

# Show dataset information
graphforge dataset-info ldbc-snb-sf01
```

## Performance Notes

- **SF0.001-0.01:** Suitable for development and testing
- **SF0.1:** Standard benchmarking scale
- **SF1+:** Requires significant RAM (8GB+ recommended)
- Use a Parquet-backed project directory for large scale factors

## References

- [LDBC Official Website](https://ldbcouncil.org/)
- [SNB Specification](https://ldbcouncil.org/benchmarks/snb/)
- [Dataset Download](https://ldbcouncil.org/resources/datasets/)

## Related

- [Dataset Overview](overview.md)
