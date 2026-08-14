# OpenCypher Clause Implementation Status

Implementation status of OpenCypher query clauses in GraphForge.

**Legend:**
- ✅ **COMPLETE**: Fully implemented with comprehensive test coverage
- ⚠️ **PARTIAL**: Basic implementation, missing advanced features or edge cases
- ❌ **NOT_IMPLEMENTED**: Feature not yet implemented

---

## Summary Statistics

| Status | Count | Percentage |
|--------|-------|------------|
| ✅ Complete | 16 | 80% |
| ⚠️ Partial | 1 | 5% |
| ❌ Not Implemented | 3 | 15% |
| **TOTAL** | **20** | **100%** |

*Updated for v0.5.0 — MERGE is a documented partial Rust subset; authoritative TCK status is in [tck-compliance.md](../tck-compliance.md).*

---

## Reading Clauses

### MATCH
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:368` (`_execute_scan`)
- File: `src/graphforge/executor/executor.py:554` (`_execute_expand`)
- File: `src/graphforge/executor/executor.py:820` (`_execute_variable_expand`)
- Grammar: `src/graphforge/parser/cypher.lark:69`
- Planner: `src/graphforge/planner/planner.py`

**Features:**
- ✅ Node pattern matching by label and properties
- ✅ Relationship pattern matching with direction and type
- ✅ Variable-length paths (`-[*1..5]-`)
- ✅ Path variable binding (`path = (a)-[*]-(b)`)
- ✅ Multiple patterns in single MATCH
- ✅ Chained MATCH clauses
- ✅ Integration with WHERE clause

**Test Coverage:** Extensive (Match1-9.feature, ~160 TCK scenarios)

---

### OPTIONAL MATCH
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:483` (`_execute_optional_scan`)
- File: `src/graphforge/executor/executor.py:2732` (`_execute_optional_expand`)
- Grammar: `src/graphforge/parser/cypher.lark:72`

**Features:**
- ✅ NULL handling for unmatched patterns
- ✅ Works with node and relationship patterns
- ✅ Can be chained with MATCH
- ✅ Proper outer join semantics

**Test Coverage:** Good (TCK scenarios for optional matching)

---

## Projecting Clauses

### RETURN
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:1037` (`_execute_project`)
- Grammar: `src/graphforge/parser/cypher.lark:130`

**Features:**
- ✅ Expression projection
- ✅ Aliasing (AS keyword)
- ✅ DISTINCT support
- ✅ `RETURN *` (all variables)
- ✅ Aggregation functions
- ✅ Property access and complex expressions

**Test Coverage:** Extensive (ReturnAcceptance, aggregation scenarios)

---

### WITH
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:1072` (`_execute_with`)
- Grammar: `src/graphforge/parser/cypher.lark:66`

**Features:**
- ✅ Variable scoping and piping
- ✅ Filtering with WHERE
- ✅ Ordering with ORDER BY
- ✅ Pagination with SKIP/LIMIT
- ✅ DISTINCT support
- ✅ Aggregation in WITH
- ✅ Multiple WITH clauses (query chaining)
- ✅ WITH at query start

**Test Coverage:** Comprehensive (With1-7.feature, WithWhere1-7.feature, ~80 TCK scenarios)

**Notes:** Implemented in v0.3.0 with full spec compliance

---

### UNWIND
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:2687` (`_execute_unwind`)
- Grammar: `src/graphforge/parser/cypher.lark:78`

**Features:**
- ✅ List expansion to rows
- ✅ Works with expressions
- ✅ NULL handling
- ✅ Can be chained
- ✅ Works with CREATE, MATCH

**Test Coverage:** Good (Unwind scenarios in TCK)

---

## Filtering and Sorting Sub-clauses

### WHERE
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:1021` (`_execute_filter`)
- Grammar: `src/graphforge/parser/cypher.lark:127`

**Features:**
- ✅ Boolean expressions
- ✅ Comparison operators
- ✅ Logical operators (AND, OR, NOT)
- ✅ NULL handling (ternary logic)
- ✅ Property existence checks
- ✅ Pattern predicates
- ✅ List membership (IN)
- ✅ String matching (STARTS WITH, ENDS WITH, CONTAINS, =~)

**Test Coverage:** Extensive (MatchWhere1-6.feature, WithWhere1-7.feature)

---

### ORDER BY
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:1248` (`_execute_sort`)
- Grammar: `src/graphforge/parser/cypher.lark:136`

**Features:**
- ✅ ASC/DESC ordering
- ✅ Multiple sort keys
- ✅ NULL ordering (nulls last)
- ✅ Works with expressions
- ✅ Integration with RETURN and WITH

**Test Coverage:** Good (ReturnOrderBy1-3.feature)

---

### LIMIT
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:1201` (`_execute_limit`)
- Grammar: `src/graphforge/parser/cypher.lark:144`

**Features:**
- ✅ Result set size limiting
- ✅ Works with ORDER BY
- ✅ Works in WITH and RETURN

**Test Coverage:** Good

---

### SKIP/OFFSET
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:1205` (`_execute_skip`)
- Grammar: `src/graphforge/parser/cypher.lark:145`

**Features:**
- ✅ Skip first N rows
- ✅ Works with LIMIT for pagination
- ✅ Works in WITH and RETURN

**Test Coverage:** Good (WithSkipLimit scenarios)

---

## Writing Clauses

### CREATE
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:1921` (`_execute_create`)
- Grammar: `src/graphforge/parser/cypher.lark:75`

**Features:**
- ✅ Create nodes with labels and properties
- ✅ Create relationships with type and properties
- ✅ Create patterns with multiple elements
- ✅ Works with MATCH
- ✅ Works with UNWIND for bulk creation
- ✅ Variable binding for created elements

**Test Coverage:** Extensive (Create1-6.feature, ~78 TCK scenarios)

---

### DELETE
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:2173` (`_execute_delete`)
- Grammar: `src/graphforge/parser/cypher.lark:93`

**Features:**
- ✅ Delete nodes (requires no relationships)
- ✅ Delete relationships
- ✅ Multiple deletes in single clause
- ✅ Error on deleting node with relationships

**Test Coverage:** Good (Delete1-6.feature)

---

### DETACH DELETE
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:2173` (`_execute_delete`)
- Grammar: `src/graphforge/parser/cypher.lark:92`

**Features:**
- ✅ Delete nodes and their relationships
- ✅ Cascade deletion
- ✅ Multiple deletes

**Test Coverage:** Good (Delete scenarios with DETACH)

---

### SET
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:2096` (`_execute_set`)
- Grammar: `src/graphforge/parser/cypher.lark:81`

**Features:**
- ✅ Set node/relationship properties
- ✅ Set multiple properties
- ✅ Expression evaluation
- ✅ Works with MATCH, CREATE, MERGE

**Test Coverage:** Good

---

### REMOVE
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:2116` (`_execute_remove`)
- Grammar: `src/graphforge/parser/cypher.lark:86`

**Features:**
- ✅ Remove properties
- ✅ Remove labels
- ✅ Multiple removes in single clause

**Test Coverage:** Good (Remove1-3.feature)

---

## Reading/Writing Clauses

### MERGE
**Status:** ⚠️ PARTIAL

**Implementation (Rust authority):**
- Parser: `crates/graphforge-cypher/src/parser/clauses.rs` (`parse_merge_clause`)
- Binder / IR: `crates/graphforge-ir/src/binder.rs` (MERGE op with `on_create` / `on_match`)
- Planner node: `crates/graphforge-plan/src/lib.rs` (`GraphMergeNode`)
- Executor: `crates/graphforge-exec/src/write_driver.rs` (`run_merge_phase`, `run_relationship_merge_phase`)

**Supported forms (direct execution evidence):**
- ✅ Standalone new-node MERGE by label + properties — `MERGE (:Person {name:'Alice'})`
  - Evidence: `crates/graphforge-api/tests/e2e_baseline.rs` (`merge_node_is_idempotent_by_label_and_properties`)
- ✅ `ON CREATE SET` / `ON MATCH SET` property (and label) actions when every frontier row takes the same create-or-match branch
  - Evidence: `merge_node_runs_only_the_selected_on_action`, `merge_relationship_is_idempotent_and_runs_selected_action`
- ✅ Relationship MERGE whose endpoints are already-bound references — `MATCH (a), (b) MERGE (a)-[r:TYPE {…}]->(b)`
  - Evidence: `merge_relationship_is_idempotent_and_runs_selected_action`, `merge_relationship_preserves_all_existing_matches`, `merge_relationship_combines_pending_and_committed_matches`, `merge_relationship_resolves_row_properties_after_entity_aliasing`

**Unsupported forms (structured plan errors — not support):**
- ❌ Multi-node / relationship-construction MERGE that creates endpoints in the same pattern (for example `MERGE (a:A)-[:R]->(b:B)`), merging an already-bound node alone, or any MERGE pattern that is not exactly one standalone new node or one edge with reference-only endpoints
  - Error: `relationship and multi-node MERGE execution is not implemented yet` (`write_driver.rs` `run_merge_phase`)
- ❌ Row-conditional `ON CREATE` / `ON MATCH` map actions (`n += {…}` / `n = {…}`) when some frontier rows create and others match in the same MERGE
  - Error: `row-conditional MERGE map actions are not implemented yet` (`write_driver.rs` `run_merge_actions_masked`)
- ❌ Comma-separated / multi-pattern MERGE in one clause (parser accepts a single `PathPattern` only)
- ❌ Treating parse/bind/plan success or TCK inventory counts as end-to-end proof — logical-plan / wrapper tests are not execution support

**Test Coverage:** Direct facade tests in `e2e_baseline.rs` above. Official TCK `clauses/merge` (Merge1–9, ~75 scenarios) is inventory coverage, not a completeness claim for the unsupported shapes.

---

### CALL
**Status:** ❌ NOT_IMPLEMENTED

**Implementation:** Not implemented

**Missing Features:**
- ❌ Procedure invocation
- ❌ YIELD clause
- ❌ User-defined procedures
- ❌ Built-in procedures

**Notes:** Would require procedure registry and invocation framework. Not planned for v0.4.0.

---

## Subquery Clauses

### CALL { } (Subqueries)
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py` (`_execute_call`)
- File: `src/graphforge/planner/planner.py` (Call operator)
- File: `src/graphforge/ast/clause.py` (CallClause)
- Grammar: `src/graphforge/parser/cypher.lark:87-88`

**Features:**
- ✅ EXISTS subqueries
- ✅ COUNT subqueries
- ✅ General CALL { } syntax
- ✅ UNION and UNION ALL in subqueries
- ✅ Correlated scoping (access outer variables)
- ✅ Unit subqueries (1:1 cardinality preservation)
- ✅ Nested CALL subqueries
- ✅ Multiple CALL clauses (Cartesian product)

**Test Coverage:** Complete (13 integration tests in test_call_subqueries.py)

**Notes:** Full CALL { } subquery support implemented in v0.3.3. Supports all query types including UNION, with proper correlated scoping and unit subquery semantics.

---

## Set Operations

### UNION
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:2855` (`_execute_union`)
- Grammar: `src/graphforge/parser/cypher.lark:11`

**Features:**
- ✅ Combine query results
- ✅ Automatic deduplication
- ✅ Column alignment by position
- ✅ Works with multiple queries

**Test Coverage:** Good (Union scenarios)

---

### UNION ALL
**Status:** ✅ COMPLETE

**Implementation:**
- File: `src/graphforge/executor/executor.py:2855` (`_execute_union`)
- Grammar: `src/graphforge/parser/cypher.lark:11`

**Features:**
- ✅ Combine query results
- ✅ Keep duplicates
- ✅ Column alignment by position

**Test Coverage:** Good (Union ALL scenarios)

---

## Additional Clauses (Not in Core OpenCypher)

### FOREACH
**Status:** ❌ NOT_IMPLEMENTED

**Implementation:** Not implemented

**Notes:** Iterative update clause for lists. Not commonly used. Low priority.

---

### USE
**Status:** ❌ NOT_IMPLEMENTED

**Implementation:** Not implemented

**Notes:** Multi-database support clause. GraphForge operates on single database. Not applicable.

---

### LOAD CSV
**Status:** ❌ NOT_IMPLEMENTED

**Implementation:** Not implemented

**Notes:** CSV import clause. Alternative: GraphForge dataset system with CSV loaders. May be added in future for compatibility.

---

## Implementation Notes

### Strengths

1. **Core clauses mostly complete**: Essential reading, writing, and projecting clauses are implemented; MERGE is an explicit partial subset
2. **Advanced features**: Variable-length paths, OPTIONAL MATCH, UNION, subquery expressions
3. **Query chaining**: WITH clause with full spec compliance (v0.3.0)
4. **Pattern matching**: Comprehensive pattern support including variable-length and path binding

### Limitations

1. **MERGE**: Partial — standalone new-node and referenced-endpoint relationship forms only; multi-node construction and row-conditional map actions reject with structured plan errors
2. **CALL procedures**: No procedure system implemented
3. **CALL { } subqueries**: Only EXISTS/COUNT supported, not general syntax
4. **Import/Export**: No LOAD CSV or equivalent (use dataset system instead)

### Recommended Priority for v0.4.0+

1. **High**: Full CALL { } subquery support (general syntax, variable importing)
2. **Medium**: LOAD CSV for compatibility
3. **Low**: CALL procedure system
4. **Low**: FOREACH clause

---

## Version History

- **v0.1.0**: MATCH, CREATE, RETURN, WHERE, ORDER BY, LIMIT, SKIP
- **v0.2.0**: SET, REMOVE, DELETE, DETACH DELETE, MERGE, UNWIND
- **v0.3.0**: OPTIONAL MATCH, WITH, UNION, EXISTS/COUNT subqueries
- **v0.4.0** (in progress): TCK coverage improvements, edge case fixes

---

## References

- OpenCypher Specification: https://opencypher.org/resources/
- MERGE executor authority: `crates/graphforge-exec/src/write_driver.rs`
- MERGE parser: `crates/graphforge-cypher/src/parser/clauses.rs`
- MERGE binder: `crates/graphforge-ir/src/binder.rs`
- MERGE planner node: `crates/graphforge-plan/src/lib.rs` (`GraphMergeNode`)
- Direct MERGE execution tests: `crates/graphforge-api/tests/e2e_baseline.rs`
