# OpenCypher Compatibility Status

**Last Updated:** 2026-07-27
**GraphForge Version:** v0.5.0

---

## Executive Summary

GraphForge v0.5.0 implements the openCypher language on a Rust core with Arrow
results and Parquet-backed projects. Authoritative TCK status lives in
[TCK Compliance](tck-compliance.md) and the CI-enforced coverage matrix.

### Design Philosophy

GraphForge prioritizes:

- ✅ **Full openCypher language** — clauses, functions, operators, patterns
- ✅ **Parquet-backed projects** with generation-based durability
- ✅ **Zero-configuration** embedded usage
- ✅ **Temporal/spatial types** — nanosecond precision, IANA timezones, extreme years
- ✅ **Arrow-first results** — `forge.execute()` and analyst verbs return Apache Arrow Tables
- ✅ Hybrid text/vector search via `forge.find()` / `forge.index()`
- ❌ Multi-database / distributed features (single-node embedded design)
- ❌ High-concurrency multi-writer workloads (one writer per project)

---

## Feature Matrix

### Reading clauses

- **MATCH** — node and relationship patterns, multi-pattern, property filters
- **WHERE** — comparisons, logical operators, NULL ternary logic
- **RETURN** — projection, aliasing, `DISTINCT`
- **WITH** — query chaining and filtering
- **ORDER BY** / **LIMIT** / **SKIP**
- **OPTIONAL MATCH**
- **UNWIND**, **UNION** / **UNION ALL**
- **EXISTS { }** / **COUNT { }** subqueries

### Writing clauses

- **CREATE**, **SET**, **REMOVE**, **DELETE**, **DETACH DELETE**
- **MERGE** (partial) — standalone new-node upsert and relationship MERGE with already-bound endpoints; multi-node construction and row-conditional map actions are rejected. See [implementation-status/clauses.md](implementation-status/clauses.md#merge).

### Aggregations

- **COUNT**, **SUM**, **AVG**, **MIN**, **MAX**, **COLLECT**
- Implicit **GROUP BY** for non-aggregated columns
- **stDev** / **percentileDisc** where covered by the corpus

### Patterns

- Directed and undirected relationships, multiple types (`|`)
- Variable-length paths (`*`, `*1..3`, named paths)
- Pattern comprehensions and list comprehensions

### Functions and types

String, math, list, aggregation, predicate, temporal, spatial, graph, and
conversion functions used by the openCypher TCK. Temporal values support
nanosecond precision and IANA timezones. See the
[Cypher guide](../guide/cypher-guide.md) for examples.

### Analyst surface (non-Cypher)

Graph algorithms and hybrid search are **not** Cypher `CALL` procedures. They
are the seven Rust-backed verbs — `rank`, `cluster`, `paths`, `analyze`,
`similar`, `find`, plus `execute` — returning Arrow Tables. See
[Algorithm Verbs](../book/architecture/algorithms.md).

---

## Out of scope

These remain outside GraphForge's embedded design:

- Enterprise auth / RBAC / multi-tenant user management
- Multi-database switching and distributed clusters
- Explicit user-managed B-tree index DDL (storage uses Parquet + derived indexes)
- High-availability replication

---

## Comparison with Neo4j

| Feature Category | GraphForge v0.5.0 | Neo4j |
|------------------|-------------------|-------|
| **Core Cypher** | ✅ openCypher TCK corpus | ✅ Full Cypher |
| **Temporal / spatial** | ✅ Supported | ✅ Supported |
| **Algorithms** | ✅ Analyst verbs → Arrow | ✅ GDS / procedures |
| **Deployment** | ✅ Embedded (`pip install`) | ⚠️ Service |
| **Persistence** | ✅ Parquet project directory | ✅ Native store |
| **Results** | ✅ Apache Arrow Tables | Driver rows |
| **Scale** | Research / notebook | Billions of nodes |
| **Multi-user** | ❌ Single writer per project | ✅ Auth / RBAC |

**Summary:** GraphForge is a lightweight, embedded alternative for single-user
analytical workflows — not a production multi-tenant database replacement.

---

## Usage recommendations

### Good fits

- Notebook-based analysis and knowledge-graph prototyping
- LLM entity/relationship storage and retrieval context
- Small to medium graphs (see [Scale Limits](scale-limits.md))
- Embedded single-user applications and teaching Cypher

### Not recommended

- Production multi-tenant web applications
- Distributed queries or HA clusters
- Workloads that need concurrent writers on one project

---

## Roadmap

See [Releases roadmap](../releases/roadmap.md). Current product line: **v0.5.0**.

---

## References

- [TCK Compliance](tck-compliance.md)
- [API Reference](api.md)
- [Cypher Guide](../guide/cypher-guide.md)
- [openCypher Specification](https://opencypher.org/resources/)
- [openCypher TCK](https://github.com/opencypher/openCypher/tree/master/tck)
