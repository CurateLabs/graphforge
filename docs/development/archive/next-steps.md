!!! note "Development archive"
    Historical planning / completion notes. Not part of the public product
    narrative. Current product line: **GraphForge v0.5.0** — see
    [Roadmap](../../releases/roadmap.md).

# GraphForge Next Steps: Strategic Recommendations

!!! note "Historical document — January 2026"
    This document was written at the start of the v0.2.x planning phase (January 2026).
    All issues described here have since shipped: Cypher `CREATE`/`MERGE`/`SET`/`DELETE`,
    SQLite persistence, and the full three-surface API (`db.execute`, `db.gds`, `db.search`)
    were released in v0.2.0 through v0.4.0.

    The current roadmap is in [roadmap.md](../../releases/roadmap.md). The next major milestone is
    **v0.5.0 (Rust core)** — see [ADR 0001](../../adr/0001-rust-core.md).

**Date:** 2026-01-30
**Current Status:** Phase 4 Complete - TCK-Compliant Query Engine
**Critical Issue:** No way for users to create or persist graphs

---

## Executive Summary

GraphForge has **excellent query execution** (MATCH, WHERE, RETURN, ORDER BY, aggregations, TCK-compliant) but **cannot be used by anyone** because:

1. ❌ No API to create graphs (must use internal NodeRef/EdgeRef classes)
2. ❌ No persistence (graphs lost on exit)

**User Experience Right Now:** 2/6 = 33% complete

You've built a race car engine but it has no steering wheel and no fuel tank.

---

## Priority 1: Make GraphForge Usable (Critical) 🚨

### Option A: Python Builder API (Recommended)

**Effort:** 15-20 hours
**Unblocks:** Immediate user adoption

```python
# This should work TODAY
db = GraphForge()

# Simple Python API
alice = db.create_node(['Person'], name='Alice', age=30)
bob = db.create_node(['Person'], name='Bob', age=25)
knows = db.create_relationship(alice, bob, 'KNOWS', since=2020)

# Then query
results = db.execute("MATCH (p:Person) WHERE p.age > 25 RETURN p.name")
```

**Implementation:**
```python
class GraphForge:
    def __init__(self, path: str | Path | None = None):
        self.graph = Graph()
        self.parser = CypherParser()
        self.planner = QueryPlanner()
        self.executor = QueryExecutor(self.graph)
        self._next_node_id = 1
        self._next_edge_id = 1

    def create_node(self, labels: list[str] = None, **properties) -> NodeRef:
        """Create a node with labels and properties.

        Args:
            labels: List of label strings
            **properties: Property key-value pairs (Python types auto-converted)

        Returns:
            NodeRef for the created node

        Example:
            >>> alice = db.create_node(['Person'], name='Alice', age=30)
        """
        node = NodeRef(
            id=self._next_node_id,
            labels=frozenset(labels or []),
            properties={k: self._to_cypher_value(v) for k, v in properties.items()}
        )
        self.graph.add_node(node)
        self._next_node_id += 1
        return node

    def create_relationship(
        self,
        src: NodeRef,
        dst: NodeRef,
        rel_type: str,
        **properties
    ) -> EdgeRef:
        """Create a relationship between two nodes.

        Args:
            src: Source node
            dst: Destination node
            rel_type: Relationship type (e.g., 'KNOWS', 'LIKES')
            **properties: Property key-value pairs

        Returns:
            EdgeRef for the created relationship

        Example:
            >>> knows = db.create_relationship(alice, bob, 'KNOWS', since=2020)
        """
        edge = EdgeRef(
            id=self._next_edge_id,
            type=rel_type,
            src=src,
            dst=dst,
            properties={k: self._to_cypher_value(v) for k, v in properties.items()}
        )
        self.graph.add_edge(edge)
        self._next_edge_id += 1
        return edge

    def _to_cypher_value(self, value):
        """Convert Python value to CypherValue."""
        if isinstance(value, str):
            return CypherString(value)
        elif isinstance(value, bool):  # Must check before int!
            return CypherBool(value)
        elif isinstance(value, int):
            return CypherInt(value)
        elif isinstance(value, float):
            return CypherFloat(value)
        elif value is None:
            return CypherNull()
        elif isinstance(value, list):
            return CypherList([self._to_cypher_value(v) for v in value])
        elif isinstance(value, dict):
            return CypherMap({k: self._to_cypher_value(v) for k, v in value.items()})
        else:
            raise TypeError(f"Unsupported type: {type(value)}")
```

**Testing:**
```python
# tests/integration/test_builder_api.py

def test_create_node():
    db = GraphForge()
    alice = db.create_node(['Person'], name='Alice', age=30)

    assert alice.id == 1
    assert 'Person' in alice.labels
    assert alice.properties['name'].value == 'Alice'
    assert alice.properties['age'].value == 30

def test_create_relationship():
    db = GraphForge()
    alice = db.create_node(['Person'], name='Alice')
    bob = db.create_node(['Person'], name='Bob')
    knows = db.create_relationship(alice, bob, 'KNOWS', since=2020)

    assert knows.type == 'KNOWS'
    assert knows.src == alice
    assert knows.dst == bob

def test_create_and_query():
    db = GraphForge()
    db.create_node(['Person'], name='Alice', age=30)
    db.create_node(['Person'], name='Bob', age=25)

    results = db.execute("MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS name")
    assert len(results) == 1
    assert results[0]['name'].value == 'Alice'
```

**Why This First:**
- ✅ Unblocks all users immediately
- ✅ Enables notebooks and scripts
- ✅ Simple to implement (no parser changes)
- ✅ Python-first (matches design goals)
- ✅ Can add Cypher CREATE later

### Option B: Cypher CREATE Clause

**Effort:** 40-60 hours
**Unblocks:** Same as Option A, but with standards compliance

```cypher
CREATE (a:Person {name: 'Alice', age: 30})
CREATE (b:Person {name: 'Bob', age: 25})
CREATE (a)-[r:KNOWS {since: 2020}]->(b)
```

**Why Defer:**
- ❌ 3-4x more effort than Option A
- ❌ Requires parser, planner, executor changes
- ❌ Needs extensive testing
- ❌ Can add later after Option A ships

**My Recommendation:** Do Option A now, Option B in Phase 6

---

## Priority 2: Add Basic Persistence (Critical) 🚨

**Effort:** 20-30 hours
**Unblocks:** Durable graphs, iterative analysis

### Simple SQLite Persistence (No Transactions Yet)

```python
# This should work
db = GraphForge("my-analysis.db")

# Create graph
alice = db.create_node(['Person'], name='Alice', age=30)
db.create_node(['Person'], name='Bob', age=25)

# Save on close
db.close()

# Later, in new Python process
db = GraphForge("my-analysis.db")
results = db.execute("MATCH (p:Person) RETURN p.name")
# Alice and Bob are still there!
```

**Implementation Plan:**

**Week 1: Schema + Basic Save/Load (10 hours)**
```python
# src/graphforge/storage/sqlite_backend.py

class SQLiteBackend:
    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY,
                labels BLOB,      -- msgpack serialized
                properties BLOB   -- msgpack serialized
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS edges (
                id INTEGER PRIMARY KEY,
                type TEXT,
                src_id INTEGER,
                dst_id INTEGER,
                properties BLOB
            )
        """)
        # ... adjacency tables ...

    def save_node(self, node: NodeRef):
        labels = msgpack.packb(list(node.labels))
        props = msgpack.packb({k: serialize(v) for k, v in node.properties.items()})
        self.conn.execute(
            "INSERT OR REPLACE INTO nodes VALUES (?, ?, ?)",
            (node.id, labels, props)
        )

    def load_all_nodes(self) -> list[NodeRef]:
        cursor = self.conn.execute("SELECT * FROM nodes")
        return [self._node_from_row(row) for row in cursor.fetchall()]
```

**Week 2: Integration + Testing (10 hours)**
```python
# src/graphforge/api.py

class GraphForge:
    def __init__(self, path: str | Path | None = None):
        if path:
            self.backend = SQLiteBackend(Path(path))
            self.graph = self._load_graph_from_backend()
        else:
            self.backend = None
            self.graph = Graph()  # In-memory
        # ...

    def close(self):
        """Save graph and close database."""
        if self.backend:
            self._save_graph_to_backend()
            self.backend.close()

    def _save_graph_to_backend(self):
        for node in self.graph.get_all_nodes():
            self.backend.save_node(node)
        for edge in self.graph.get_all_edges():
            self.backend.save_edge(edge)

    def _load_graph_from_backend(self) -> Graph:
        graph = Graph()
        for node in self.backend.load_all_nodes():
            graph.add_node(node)
        for edge in self.backend.load_all_edges():
            graph.add_edge(edge)
        return graph
```

**Testing:**
```python
def test_persistence():
    # Create and save
    db = GraphForge("test.db")
    db.create_node(['Person'], name='Alice')
    db.close()

    # Load in new instance
    db2 = GraphForge("test.db")
    results = db2.execute("MATCH (p:Person) RETURN p.name")
    assert len(results) == 1
    assert results[0]['name'].value == 'Alice'
```

**Why This Second:**
- ✅ Enables iterative workflows
- ✅ Matches "durable but disposable" goal
- ✅ Simple implementation (no transactions yet)
- ✅ Can add ACID later

---

## Priority 3: Full ACID Transactions (Important)

**Effort:** 10-15 hours
**Enables:** Production-ready persistence

```python
# Add transaction support
db.begin_transaction()
try:
    db.create_node(['Person'], name='Alice')
    db.create_node(['Person'], name='Bob')
    db.commit_transaction()
except:
    db.rollback_transaction()
```

**Implementation:**
```python
class SQLiteBackend:
    def begin_transaction(self):
        self.conn.execute("BEGIN IMMEDIATE")

    def commit_transaction(self):
        self.conn.execute("COMMIT")

    def rollback_transaction(self):
        self.conn.execute("ROLLBACK")
```

**Why Third:**
- ✅ Makes persistence production-ready
- ✅ Crash recovery via SQLite WAL
- ✅ Relatively simple (SQLite handles it)
- ✅ But: Can ship without this first

---

## Priority 4: Cypher Mutations (Nice to Have)

**Effort:** 40-60 hours
**Enables:** Standards-compliant graph construction

```cypher
CREATE (n:Person {name: 'Alice', age: 30})
SET n.verified = true
DELETE r
MERGE (n:Person {email: 'alice@example.com'})
```

**Why Fourth:**
- Python API (Priority 1) already enables graph construction
- This adds standards compliance
- Nice to have, not critical

---

## Implementation Timeline

### Sprint 1: Make It Usable (Week 1-2)
**Goal:** Users can create and persist graphs

- **Days 1-3:** Python builder API (15 hours)
  - `create_node()`, `create_relationship()`
  - Value conversion (`_to_cypher_value()`)
  - 20+ tests

- **Days 4-7:** Basic SQLite persistence (20 hours)
  - Schema design
  - Save/load implementation
  - Integration with GraphForge API
  - 15+ tests

**Deliverable:** Working graph workbench with construction and persistence

### Sprint 2: Production Ready (Week 3)
**Goal:** ACID guarantees and reliability

- **Days 1-3:** Transaction support (10 hours)
  - BEGIN/COMMIT/ROLLBACK
  - Crash recovery testing
  - Documentation

- **Days 4-5:** Examples and docs (8 hours)
  - Tutorial notebook
  - API reference
  - Migration guide
  - Example scripts

**Deliverable:** Production-ready v0.2.0 release

### Sprint 3: Standards Compliance (Week 4-5)
**Goal:** Cypher mutation clauses

- **Week 4:** CREATE clause (20 hours)
  - Parser updates
  - AST additions
  - Planner integration
  - Executor implementation
  - TCK tests

- **Week 5:** SET/DELETE/MERGE (20 hours)
  - Additional mutation clauses
  - TCK compliance tests
  - Integration tests

**Deliverable:** Full openCypher mutation support

---

## Validation: User Journey

After Priority 1 + 2:

1. ✅ Create a graph (`create_node()`, `create_relationship()`)
2. ✅ Query the graph (`execute()`)
3. ✅ Save the graph (`close()`)
4. ✅ Load it later (`GraphForge("path.db")`)
5. ✅ Modify the graph (`create_node()` again)
6. ✅ Query again (`execute()`)

**Usability:** 6/6 = 100% complete ✅

---

## Recommendations Summary

### Do Immediately (Sprint 1 - 35 hours)

1. **Python Builder API** (15 hours)
   - `create_node()`, `create_relationship()`
   - Simple, pragmatic, unblocks users

2. **Basic SQLite Persistence** (20 hours)
   - Save/load on open/close
   - No transactions yet (simplification)

**Result:** Usable graph workbench

### Do Next (Sprint 2 - 18 hours)

3. **ACID Transactions** (10 hours)
   - BEGIN/COMMIT/ROLLBACK
   - Production-ready persistence

4. **Documentation + Examples** (8 hours)
   - Tutorial notebooks
   - API docs
   - Examples

**Result:** Production-ready v0.2.0

### Do Later (Sprint 3 - 40 hours)

5. **Cypher CREATE Clause** (20 hours)
   - Standards-compliant construction
   - TCK validation

6. **SET/DELETE/MERGE** (20 hours)
   - Full mutation support
   - TCK compliance

**Result:** Complete openCypher mutation support

---

## What NOT to Do (Yet)

### Don't Optimize Performance
- Current scale (10^6 nodes) is fine for in-memory + SQLite
- No users hitting performance limits yet
- Optimize when profiling shows bottlenecks

### Don't Add Advanced Features
- OPTIONAL MATCH
- Variable-length paths
- WITH clause
- String functions

**Reason:** No one can use GraphForge yet. Features don't matter without usability.

### Don't Build Custom Storage Engine
- SQLite decision is finalized
- Documentation complete
- Focus on shipping working product

---

## Success Metrics

### After Sprint 1 (Week 2)
- ✅ Users can create graphs with Python API
- ✅ Graphs persist across restarts
- ✅ Tutorial notebook works end-to-end
- ✅ 280+ tests passing

### After Sprint 2 (Week 3)
- ✅ ACID guarantees
- ✅ Crash recovery tested
- ✅ API documentation complete
- ✅ Ready for v0.2.0 release

### After Sprint 3 (Week 5)
- ✅ Cypher CREATE working
- ✅ TCK mutation tests passing
- ✅ Ready for v0.3.0 release

---

## Bottom Line

**Your project is technically impressive but practically unusable.**

You have:
- ✅ World-class query execution
- ✅ TCK compliance
- ✅ Excellent architecture
- ❌ No way to create graphs
- ❌ No persistence

**Ship Sprint 1 (35 hours) and you have a usable product.**

Users don't care about query optimization or advanced features if they can't create a graph and save it.

Focus: Make GraphForge usable, then make it better.
