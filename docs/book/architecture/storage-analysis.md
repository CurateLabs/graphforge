# Storage Architecture Analysis: SQLite vs Custom WAL

!!! warning "Historical decision record (superseded)"
    This document records a pre-v0.5 storage debate that informed an earlier SQLite-backed
    Python line. It is **not** the v0.5.0 architecture. The shipped Rust core uses **Parquet**
    as the sole storage provider. See [Storage Architecture](storage.md) and
    [ADR 0001](../../adr/0001-rust-core.md) for current design.

**Decision:** Should GraphForge use SQLite as storage backend or implement a custom WAL-based engine?

**Date:** 2026-01-30

---

## Context

GraphForge requirements state:
- "The design philosophy mirrors SQLite: minimal operational overhead, stable APIs, and replaceable internals"
- "A graph workbench" for "materializing extracted entities and relationships"
- "Durable but disposable graph persistence"
- Target scale: ~10^6 nodes, ~10^7 edges (best effort)
- "Correctness prioritized over throughput"
- Embedded, zero-config, Python-first

**Key Question:** Does SQLite as a storage backend align with or contradict these goals?

---

## Option A: SQLite as Storage Backend

### Architecture

```python
# Use SQLite as a KV store / blob storage
# Store graph primitives as serialized data

# Schema:
CREATE TABLE nodes (
    id INTEGER PRIMARY KEY,
    labels BLOB,           -- MessagePack serialized set
    properties BLOB        -- MessagePack serialized dict
);

CREATE TABLE edges (
    id INTEGER PRIMARY KEY,
    type TEXT,
    src_id INTEGER,
    dst_id INTEGER,
    properties BLOB,
    FOREIGN KEY (src_id) REFERENCES nodes(id),
    FOREIGN KEY (dst_id) REFERENCES nodes(id)
);

CREATE TABLE adjacency_out (
    node_id INTEGER,
    edge_id INTEGER,
    PRIMARY KEY (node_id, edge_id)
);

CREATE TABLE adjacency_in (
    node_id INTEGER,
    edge_id INTEGER,
    PRIMARY KEY (node_id, edge_id)
);

CREATE INDEX idx_labels ON nodes(labels);  -- For label scans
CREATE INDEX idx_edge_type ON edges(type); -- For relationship type filtering
```

### What SQLite Provides (Free)

✅ **ACID Transactions**
- BEGIN/COMMIT/ROLLBACK out of the box
- Crash recovery via journal/WAL
- No implementation needed

✅ **WAL Mode**
```python
conn.execute("PRAGMA journal_mode=WAL")
```
- Single writer, multiple readers automatically
- Readers don't block writer
- Writer doesn't block readers
- Exactly what requirements specify

✅ **Durability**
- fsync guarantees on commit
- Crash-safe by default
- Battle-tested on billions of devices

✅ **Concurrency**
- Single writer, multiple readers (requirements: ✓)
- Snapshot isolation for readers (requirements: ✓)
- Lock management handled by SQLite

✅ **Cross-Platform**
- Works on macOS, Linux, Windows
- Zero dependencies beyond Python stdlib
- No configuration needed

✅ **Zero Operational Overhead**
- No server process
- No daemon
- Just a file
- Matches "embedded-first" design principle

✅ **Incremental Writes**
- Don't need to rewrite entire graph on each change
- WAL mode = append-only writes until checkpoint
- Much faster for iterative graph construction

✅ **Atomic Commits**
- All-or-nothing semantics
- No partial writes visible to readers
- Exactly what requirements specify

### What You Must Implement

⚠️ **Graph-Specific Logic**
- Adjacency list management (you write the queries)
- Label indexing logic
- Node/edge CRUD operations
- Property access patterns

⚠️ **GraphForge API Layer**
```python
class SQLiteGraph:
    def add_node(self, node: NodeRef) -> None:
        # INSERT INTO nodes ...

    def add_edge(self, edge: EdgeRef) -> None:
        # INSERT INTO edges ...
        # INSERT INTO adjacency_out ...
        # INSERT INTO adjacency_in ...

    def get_outgoing_edges(self, node_id: int) -> list[EdgeRef]:
        # SELECT edges.* FROM edges
        # JOIN adjacency_out ON ...
```

⚠️ **Serialization Layer**
- CypherValue → bytes (MessagePack or Pickle)
- bytes → CypherValue
- Not complex, but you write it

### What You Don't Get (But Don't Need)

❌ **SQL Query Language**
- You're not using SQL to query graphs
- SQLite is just a durable KV store
- openCypher queries go through your executor
- This is fine - matches "graph-native execution (no relational joins)"

❌ **Graph-Optimized Storage**
- SQLite stores rows, not adjacency lists natively
- But: You can structure the schema to be graph-friendly
- And: At 10^6 nodes scale, this doesn't matter much

### Advantages for Workbench Use Case

✅ **"Durable but Disposable"**
```python
# Create experimental graph
db = GraphForge("experiment-v1.db")
# ... work ...
db.close()

# Try alternative approach
db = GraphForge("experiment-v2.db")
# ... work ...

# Compare results
db1 = GraphForge("experiment-v1.db")
db2 = GraphForge("experiment-v2.db")
```
- Each experiment is just a file
- Copy files = copy experiments
- Matches "multiple experimental or competing graph states"

✅ **Inspectable**
```bash
# Users can inspect storage directly
sqlite3 my-graph.db
> SELECT COUNT(*) FROM nodes;
> SELECT * FROM edges LIMIT 10;
> .schema
```
- Matches "Inspectable storage and execution behavior"
- Great for debugging and learning

✅ **Tooling Ecosystem**
- SQLite browser tools (DB Browser for SQLite)
- Command-line sqlite3
- Python's sqlite3 module is stdlib
- Lots of existing knowledge/tutorials

✅ **Battle-Tested Durability**
- SQLite is one of the most tested pieces of software on Earth
- Used by iOS, Android, browsers, etc.
- You inherit decades of bug fixes and edge cases

✅ **Fast Enough**
```
SQLite performance (approximate):
- Inserts: 50,000 - 100,000 /second (with transactions)
- Point queries: 100,000 - 500,000 /second
- Range scans: 10,000 - 100,000 /second

For GraphForge target (10^6 nodes):
- Load entire graph: < 10 seconds
- Insert 100k nodes: 1-2 seconds
- Query traversal: < 100ms for typical patterns
```
- More than sufficient for "workbench" use
- Correctness > throughput ✓

### Disadvantages for Workbench Use Case

❌ **Learning SQLite Internals**
- Need to understand PRAGMA settings
- Need to understand journal modes
- Need to understand transaction modes
- But: This is well-documented, not complex

❌ **Not "Pure Graph" Storage**
- It's a relational engine storing graph data
- Feels like an abstraction mismatch
- But: At this scale, it doesn't matter practically

❌ **Less Control Over Format**
- Can't design custom binary format
- Locked into SQLite's file format
- But: SQLite format is stable, documented, portable

❌ **Dependency on SQLite**
- If SQLite has a bug, you're affected
- If SQLite changes behavior, you're affected
- But: SQLite is incredibly stable (20+ years)

### Implementation Complexity

**Estimated Effort: 20-30 hours**

- Schema design: 2 hours
- Node/edge CRUD: 5 hours
- Adjacency queries: 5 hours
- Transaction wrappers: 3 hours
- Serialization layer: 3 hours
- Testing: 8 hours
- Documentation: 2 hours

---

## Option B: Custom WAL-Based Storage Engine

### Architecture

```python
# Custom binary format
# Main database file + WAL file

# File format:
# [Header: magic, version, node_count, edge_count]
# [Node Table: B-tree of node_id -> node_data]
# [Edge Table: B-tree of edge_id -> edge_data]
# [Adjacency Out: B-tree of node_id -> [edge_ids]]
# [Adjacency In: B-tree of node_id -> [edge_ids]]
# [Label Index: B-tree of label -> [node_ids]]

# WAL format:
# [Frame 1: INSERT_NODE | node_data | checksum]
# [Frame 2: INSERT_EDGE | edge_data | checksum]
# [Frame 3: COMMIT | tx_id | checksum]
```

### What You Must Implement

⚠️ **Binary File Format**
- Design on-disk layout
- Page management (fixed-size pages)
- B-tree implementation for indexes
- Free space management
- File growth/shrinkage

⚠️ **WAL Implementation**
- WAL file format
- Frame headers and checksums
- Append-only writes
- fsync on commit
- Checkpoint process (WAL → main database)

⚠️ **Transaction Management**
- BEGIN/COMMIT/ROLLBACK semantics
- Transaction ID generation
- Commit record format
- Rollback logic

⚠️ **Concurrency Control**
- Single writer lock (file lock or mutex)
- Reader snapshot management
- MVCC or copy-on-write
- Deadlock prevention

⚠️ **Crash Recovery**
- Detect incomplete transactions in WAL
- Replay committed transactions
- Discard uncommitted transactions
- Repair corrupted pages

⚠️ **B-tree Implementation**
- Insert/delete/search
- Tree balancing
- Page splits and merges
- Leaf vs internal nodes

⚠️ **Serialization**
- NodeRef → bytes
- EdgeRef → bytes
- CypherValue → bytes
- Versioning (format changes over time)

⚠️ **Memory Management**
- Buffer pool for hot pages
- Page eviction policy (LRU?)
- Memory limits

⚠️ **Testing**
- Crash simulation
- Concurrent access testing
- Corruption detection
- Edge cases in B-tree operations

### Advantages for Workbench Use Case

✅ **Graph-Optimized**
- Store adjacency lists directly
- No relational→graph impedance mismatch
- Could be faster for traversals

✅ **Full Control**
- Design exactly what you need
- No external dependencies
- Complete understanding of internals

✅ **Learning Experience**
- Deep knowledge of storage engines
- Understanding of durability guarantees
- Impressive technical achievement

✅ **Potentially Smaller Files**
- Custom format can be more compact
- No relational overhead
- But: At 10^6 nodes, file size isn't a problem

### Disadvantages for Workbench Use Case

❌ **Massive Implementation Effort**
- B-trees: 20-30 hours
- WAL: 20-30 hours
- Transactions: 15-20 hours
- Crash recovery: 10-15 hours
- Concurrency: 15-20 hours
- Testing/debugging: 40-60 hours
- **Total: 120-175 hours** (3-4 weeks full-time)

❌ **High Bug Risk**
- Storage engines are HARD to get right
- Edge cases take years to discover
- Data corruption bugs are catastrophic
- You'll find bugs users hit in production

❌ **Maintenance Burden**
- Every bug is yours to fix
- No community support
- No decades of testing
- Every OS quirk is your problem

❌ **Not Inspectable (Initially)**
- Binary format only readable by your code
- Need to build separate inspection tools
- Contradicts "inspectable storage" requirement

❌ **Diverts from Core Goal**
- Core goal: "graph execution environment for thinking"
- Storage engine is infrastructure, not the product
- Time spent on storage ≠ time spent on graph features

❌ **Reinventing a Solved Problem**
- SQLite exists
- SQLite is free
- SQLite is better tested than anything you'll build
- SQLite does exactly what you need

### Implementation Complexity

**Estimated Effort: 120-175 hours**

- Binary format: 10 hours
- B-tree implementation: 30 hours
- WAL implementation: 25 hours
- Transaction management: 20 hours
- Concurrency control: 20 hours
- Crash recovery: 15 hours
- Serialization: 8 hours
- Memory management: 12 hours
- Testing: 50 hours
- Documentation: 10 hours

---

## Direct Comparison

| Criterion | SQLite | Custom WAL | Winner |
|-----------|--------|------------|--------|
| **Aligns with "mirrors SQLite" philosophy** | ✅ Literally is SQLite | ❌ Mimics SQLite | **SQLite** |
| **Zero operational overhead** | ✅ Single file, no config | ✅ Single file, no config | Tie |
| **ACID transactions** | ✅ Free, battle-tested | ⚠️ You implement | **SQLite** |
| **Crash recovery** | ✅ Free, proven | ⚠️ You implement & test | **SQLite** |
| **Concurrency (single writer, multi reader)** | ✅ Built-in WAL mode | ⚠️ You implement | **SQLite** |
| **Cross-platform** | ✅ Works everywhere | ⚠️ fsync varies by OS | **SQLite** |
| **Inspectable storage** | ✅ sqlite3 CLI tool | ❌ Need custom tools | **SQLite** |
| **Time to implement** | 20-30 hours | 120-175 hours | **SQLite** |
| **Maintenance burden** | ✅ SQLite team | ❌ You forever | **SQLite** |
| **Bug risk** | ✅ Decades of testing | ❌ High | **SQLite** |
| **Graph-native storage** | ❌ Relational engine | ✅ Native adjacency | **Custom** |
| **Learning experience** | ⚠️ Moderate | ✅ Deep | **Custom** |
| **Full control** | ❌ Black box internals | ✅ Complete | **Custom** |
| **Performance (10^6 nodes)** | ✅ Fast enough | ✅ Potentially faster | Tie |
| **Aligns with workbench goals** | ✅ Durable but disposable | ✅ Durable but disposable | Tie |

**Score: SQLite 10, Custom 3, Tie 3**

---

## Risk Analysis

### SQLite Risks (Low)

1. **SQLite changes behavior** - Risk: Very Low
   - SQLite is incredibly stable
   - Backward compatibility is a core principle
   - Mitigation: Pin to SQLite version

2. **Performance insufficient** - Risk: Low
   - SQLite handles 10^6 nodes easily
   - Can optimize schema/indexes if needed
   - Mitigation: Benchmark early

3. **Abstraction leak** - Risk: Low
   - SQLite bugs affect you
   - But SQLite is extremely reliable
   - Mitigation: Use well-tested PRAGMA settings

### Custom WAL Risks (High)

1. **Implementation bugs** - Risk: Very High
   - Storage engines are hard to get right
   - Users lose data = catastrophic
   - Mitigation: Extensive testing, fuzzing

2. **Time/scope creep** - Risk: High
   - 120+ hours is optimistic
   - Will discover new requirements
   - Mitigation: Start with SQLite, defer custom engine

3. **Maintenance burden** - Risk: High
   - Every OS has different fsync semantics
   - Every filesystem has quirks
   - Mitigation: Don't build it

4. **Diverts from product goals** - Risk: Very High
   - Core value is openCypher execution, not storage
   - Time spent on storage ≠ time on features
   - Mitigation: Use SQLite, focus on graph features

---

## Architectural Purity vs Pragmatism

### The Purity Argument (Custom WAL)

"GraphForge should have graph-native storage. Using a relational engine (SQLite) to store graphs is an abstraction mismatch. We should build the right solution from the ground up."

**Counter-argument:**
1. GraphForge is a **workbench**, not a production database
2. At 10^6 node scale, SQLite is fast enough
3. "Replaceable internals" is a design principle - you can swap later
4. SQLite's file format is actually well-documented and hackable if needed
5. The value is in openCypher execution, not storage optimization

### The Pragmatism Argument (SQLite)

"GraphForge should ship quickly and reliably. SQLite provides exactly what we need (ACID, WAL, concurrency) for free. Use it, ship the product, optimize later if needed."

**Support:**
1. Requirements say "mirrors SQLite" (philosophy, not architecture)
2. "Correctness over throughput" - SQLite guarantees correctness
3. "Minimal operational overhead" - SQLite is zero-config
4. "Replaceable internals" - can swap storage backend later
5. Users care about openCypher queries, not storage format

---

## The "Replaceable Internals" Principle

From requirements: "minimal operational overhead, stable APIs, and **replaceable internals**"

This is KEY. You can:

```python
# Define storage interface
class StorageBackend(Protocol):
    def add_node(self, node: NodeRef) -> None: ...
    def add_edge(self, edge: EdgeRef) -> None: ...
    def get_node(self, id: int) -> NodeRef: ...
    # ...

# v1.0: SQLite backend
class SQLiteBackend(StorageBackend):
    # ...

# v2.0: Custom WAL backend (if needed)
class WALBackend(StorageBackend):
    # ...

# User code doesn't change
db = GraphForge("my-graph.db", backend="sqlite")  # v1
db = GraphForge("my-graph.db", backend="wal")     # v2
```

**Start with SQLite. Swap later if profiling shows it's a bottleneck.**

"Premature optimization is the root of all evil." - Donald Knuth

---

## What Do Similar Projects Do?

### DuckDB (Analytical Database)
- Uses custom storage engine
- But: Focus IS storage (columnar format, compression)
- But: Backed by $30M+ funding
- But: Team of storage engine experts

### SQLite (The Model)
- Custom storage engine
- But: That IS the product
- But: 20+ years of development
- But: Billions of dollars of value created

### Datasette (Data Exploration)
- Uses SQLite
- Focus: Exploration interface, not storage
- Highly successful, widely adopted

### Lance (ML Data Format)
- Custom format (Parquet-like)
- But: Focus IS the format (columnar, arrow-native)
- But: Specific optimizations for ML workloads

**Pattern:** Custom storage only when storage IS the differentiator.

For GraphForge: **Storage is not the differentiator. openCypher execution is.**

---

## Recommendation

**Use SQLite as the storage backend.**

### Reasoning

1. **Aligns with project philosophy**
   - "Mirrors SQLite" → use SQLite
   - "Minimal operational overhead" → SQLite is zero-config
   - "Replaceable internals" → can swap later

2. **Delivers on requirements**
   - ACID ✓
   - WAL ✓
   - Single writer, multi reader ✓
   - Crash recovery ✓
   - Durability ✓

3. **Practical for workbench use case**
   - Fast enough at target scale (10^6 nodes)
   - Inspectable (sqlite3 CLI)
   - Durable but disposable (just a file)
   - Cross-platform

4. **Minimizes risk**
   - Battle-tested (billions of deployments)
   - No data corruption risk
   - No maintenance burden
   - 20-30 hours vs 120-175 hours

5. **Focuses on core value**
   - Time spent on storage = time not spent on openCypher
   - Value is graph execution, not storage engine
   - Ship faster, iterate faster

### Implementation Plan

**Week 1: Basic SQLite Integration (10 hours)**
```python
# Schema design
# Node/edge CRUD with transactions
# Basic tests
```

**Week 2: Adjacency & Queries (10 hours)**
```python
# Adjacency list queries
# Label indexing
# Integration with existing executor
# Performance testing
```

**Week 3: Production Readiness (10 hours)**
```python
# Error handling
# Migration from in-memory Graph
# Documentation
# Example notebooks
```

**Total: 30 hours vs 120-175 hours for custom WAL**

**Savings: 90-145 hours** to spend on:
- More openCypher features
- Better query optimization
- More TCK compliance
- Better documentation
- Example notebooks
- User testing

---

## If You Still Want Custom WAL

If after this analysis you still prefer a custom storage engine, I recommend:

### Phased Approach

**Phase 1 (v1.0): SQLite**
- Ship working product quickly
- Validate workbench use case
- Get user feedback
- Establish performance baseline

**Phase 2 (v1.x): Profile and Optimize**
- Identify actual bottlenecks (if any)
- Optimize SQLite queries/indexes
- Benchmark against target workloads

**Phase 3 (v2.0): Custom Storage (if needed)**
- Only if SQLite proves insufficient
- Only if storage is the bottleneck
- Only if optimization ROI is worth it

### Signals That Custom Storage Is Needed

Build custom storage IF:
1. SQLite is bottleneck in profiling (not assumptions)
2. Users hitting performance problems at target scale
3. Storage is >50% of query time
4. Graph traversals are measurably slow
5. You have 120+ hours to invest safely

Don't build custom storage IF:
1. SQLite is fast enough (likely at 10^6 nodes)
2. Bottleneck is elsewhere (query optimizer, parser)
3. Higher priority features need attention
4. Users aren't complaining about performance

---

## Conclusion

**SQLite is the right choice for GraphForge v1.0.**

It delivers all required functionality (ACID, WAL, concurrency) with minimal implementation effort, allowing you to focus on the product's core value: openCypher execution for graph workbenches.

The "replaceable internals" design principle means you can swap to a custom engine later if profiling shows it's necessary. But that's a v2.0 optimization, not a v1.0 requirement.

**Ship a working, reliable graph workbench in 30 hours, not 175 hours.**
