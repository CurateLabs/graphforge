# OpenCypher Compatibility Matrix

Comprehensive status matrix for GraphForge's OpenCypher implementation, showing features, implementation status, and TCK test coverage.

**Last Updated:** v0.3.8 (May 2026)
**GraphForge Version:** v0.3.8
**Release Strategy:** Patch-level releases (0.3.x) until 100% feature complete
**TCK Scenarios:** 3,885 total, 3,801 passing (97.8%)

---

## Executive Summary

| Category | Total Features | Complete | Partial | Not Implemented | Coverage |
|----------|---------------|----------|---------|-----------------|----------|
| **Clauses** | 20 | 17 (85%) | 1 (5%) | 2 (10%) | Excellent |
| **Functions** | 83 | 82 (99%) | 0 (0%) | 1 (1%) | Excellent |
| **Operators** | 34 | 34 (100%) | 0 (0%) | 0 (0%) | Complete |
| **Patterns** | 8 | 7 (87%) | 0 (0%) | 1 (13%) | Excellent |
| **TOTAL** | **145** | **140 (97%)** | **1 (1%)** | **4 (3%)** | **Excellent** |

### Overall Compliance: **~99% TCK Compliant (3,801/3,885 scenarios)**

---

## Quick Reference

### ✅ Fully Supported (140 features)
- Core querying: MATCH, RETURN, WHERE, ORDER BY, LIMIT, SKIP
- Query chaining: WITH (full spec compliance)
- Writing: CREATE, MERGE, SET, REMOVE, DELETE, DETACH DELETE
- Advanced: OPTIONAL MATCH, UNION, UNWIND, variable-length paths
- Temporal: All date/time types and functions (100% complete)
- Spatial: Point and distance functions (100% complete)
- Pattern matching: All node/relationship pattern variations, pattern predicates
- Predicate functions: all(), any(), none(), single(), exists(), isEmpty()
- List operations: extract(), filter(), reduce(), slicing, negative indexing
- Operators: All comparison, logical (including XOR), arithmetic (including ^), string, list, and pattern operators
- Procedures: CALL procedures with procedure registry
- Scalar: id(), type(), labels(), properties(), keys(), coalesce(), toBoolean(), timestamp(), startNode(), endNode()
- Aggregation: count, sum, avg, min, max, collect, percentileDisc, percentileCont, stDev, stDevP (10/10)
- Numeric math: Full suite including trig, log, exp, e(), pi(), degrees(), radians() (19 functions)

### ⚠️ Partially Supported (1 feature)
- CALL { } / EXISTS subquery (simple EXISTS implemented; full correlated subquery syntax pending)

### ❌ Not Supported (4 features)
- elementId() scalar function (, planned v0.4.0)
- FOREACH clause
- LOAD CSV (use the dataset system instead)
- Pattern comprehension

---

## Detailed Feature Matrix

### Clauses (20 total: 17 complete, 1 partial, 2 not implemented)

| Clause | Status | TCK Scenarios | Implementation Files | Notes |
|--------|--------|---------------|---------------------|-------|
| **MATCH** | ✅ Complete | 195 | executor.py | All pattern types supported |
| **CREATE** | ✅ Complete | 78 | executor.py | Full pattern creation |
| **RETURN** | ✅ Complete | 129 | executor.py | With DISTINCT, aliases, aggregation |
| **WHERE** | ✅ Complete | 53 | executor.py | All predicates, NULL handling |
| **WITH** | ✅ Complete | 156 | executor.py | Full spec compliance |
| **ORDER BY** | ✅ Complete | 134 | executor.py | ASC/DESC, multiple keys, NULL ordering |
| **LIMIT** | ✅ Complete | 40 | executor.py | Result limiting |
| **SKIP** | ✅ Complete | 40 | executor.py | Pagination support |
| **MERGE** | ✅ Complete | 75 | executor.py | With ON CREATE/MATCH |
| **SET** | ✅ Complete | 53 | executor.py | Property/label updates |
| **REMOVE** | ✅ Complete | 33 | executor.py | Property/label removal |
| **DELETE** | ✅ Complete | 41 | executor.py | Node/relationship deletion |
| **DETACH DELETE** | ✅ Complete | 41 | executor.py | Cascade deletion |
| **UNWIND** | ✅ Complete | 14 | executor.py | List expansion (open scope) |
| **UNION/UNION ALL** | ✅ Complete | 12 | executor.py | Set operations |
| **OPTIONAL MATCH** | ✅ Complete | ~20 | executor.py | NULL handling, unbound-src support |
| **CALL** | ✅ Complete | 41 | executor.py | Procedure registry (v0.3.7) |
| **CALL { }** | ⚠️ Partial | 10 | executor.py | Simple EXISTS implemented; full correlated subquery pending |
| **FOREACH** | ❌ Not Implemented | 0 | N/A | Low priority |
| **LOAD CSV** | ❌ Not Implemented | 0 | N/A | Use dataset system |

---

### Functions (83 total: 82 complete, 0 partial, 1 not implemented)

#### String Functions (13 total: 13 complete) ✅ COMPLETE CATEGORY

| Function | Status | TCK Scenarios | File | Notes |
|----------|--------|---------------|------|-------|
| substring() | ✅ Complete | 4 | evaluator.py | 2 and 3 arg forms |
| trim(), ltrim(), rtrim() | ✅ Complete | 3 | evaluator.py | Whitespace trimming |
| upper(), lower() / toUpper(), toLower() | ✅ Complete | 5 | evaluator.py | Case conversion; both camelCase and UPPER/LOWER aliases |
| split() | ✅ Complete | 2 | evaluator.py | String splitting |
| replace() | ✅ Complete | 2 | evaluator.py | String replacement |
| reverse() | ✅ Complete | 2 | evaluator.py | String reversal |
| left(), right() | ✅ Complete | 2 | evaluator.py | Substring extraction |
| toString() | ✅ Complete | 5 | evaluator.py | Type conversion |
| length() (string form) | ✅ Complete | 1 | evaluator.py | String character count |

#### Numeric Functions (19 total: 19 complete) ✅ COMPLETE CATEGORY

| Function | Status | TCK Scenarios | File | Notes |
|----------|--------|---------------|------|-------|
| abs() | ✅ Complete | 1 | evaluator.py | Absolute value |
| ceil(), floor() | ✅ Complete | 1 | evaluator.py | Rounding |
| round() | ✅ Complete | 1 | evaluator.py | With precision |
| sign() | ✅ Complete | 1 | evaluator.py | Sign of number |
| toInteger(), toFloat() | ✅ Complete | 2 | evaluator.py | Type conversion |
| sqrt() | ✅ Complete | 7 | evaluator.py | Square root (v0.3.5) |
| rand() | ✅ Complete | 4 | evaluator.py | Random number (v0.3.5) |
| pow() | ✅ Complete | 9 | evaluator.py | Power function (v0.3.5) |
| e(), pi() | ✅ Complete | 2 | evaluator.py | Mathematical constants (v0.3.6) |
| exp(), log(), log10() | ✅ Complete | 4 | evaluator.py | Exponential / logarithmic (v0.3.6) |
| sin(), cos(), tan(), cot() | ✅ Complete | 6 | evaluator.py | Trigonometric (v0.3.6) |
| asin(), acos(), atan(), atan2() | ✅ Complete | 6 | evaluator.py | Inverse trig (v0.3.6) |
| degrees(), radians() | ✅ Complete | 3 | evaluator.py | Angle conversion (v0.3.6) |

#### List Functions (11 total: 11 complete) ✅ COMPLETE CATEGORY

| Function | Status | TCK Scenarios | File | Notes |
|----------|--------|---------------|------|-------|
| size() | ✅ Complete | 15 | evaluator.py | List/string length |
| head(), last() | ✅ Complete | 10 | evaluator.py | First/last element |
| tail() | ✅ Complete | 8 | evaluator.py | All but first |
| range() | ✅ Complete | 12 | evaluator.py | Integer sequence |
| reverse() | ✅ Complete | 5 | evaluator.py | List reversal |
| extract() | ✅ Complete | 15 | evaluator.py | List comprehension (v0.3.6) |
| filter() | ✅ Complete | 10 | evaluator.py | List filtering (v0.3.6) |
| reduce() | ✅ Complete | 5 | evaluator.py | List reduction (v0.3.6) |

#### Aggregation Functions (10 total: 10 complete) ✅ COMPLETE CATEGORY

| Function | Status | TCK Scenarios | File | Notes |
|----------|--------|---------------|------|-------|
| count() | ✅ Complete | 8 | executor.py | With DISTINCT |
| sum() | ✅ Complete | 4 | executor.py | Numeric sum |
| avg() | ✅ Complete | 3 | executor.py | Average |
| min(), max() | ✅ Complete | 5 | executor.py | Min/max values |
| collect() | ✅ Complete | 4 | executor.py | Collect to list |
| percentileDisc(), percentileCont() | ✅ Complete | 2 | executor.py | Percentiles (v0.3.5) |
| stDev(), stDevP() | ✅ Complete | 1 | executor.py | Standard deviation (v0.3.5) |

#### Predicate Functions (6 total: 6 complete) ✅ COMPLETE CATEGORY

| Function | Status | TCK Scenarios | File | Notes |
|----------|--------|---------------|------|-------|
| all() | ✅ Complete | 8 | evaluator.py | All elements match (v0.3.6) |
| any() | ✅ Complete | 8 | evaluator.py | Any element matches (v0.3.6) |
| none() | ✅ Complete | 4 | evaluator.py | No elements match (v0.3.6) |
| single() | ✅ Complete | 4 | evaluator.py | Exactly one matches (v0.3.6) |
| exists() | ✅ Complete | 10 | evaluator.py | Property/pattern exists (v0.3.6) |
| isEmpty() | ✅ Complete | 2 | evaluator.py | Empty list/string (v0.3.6) |

#### Scalar Functions (11 total: 10 complete, 1 not implemented)

| Function | Status | TCK Scenarios | File | Notes |
|----------|--------|---------------|------|-------|
| id() | ✅ Complete | 8 | evaluator.py | Element ID |
| type() | ✅ Complete | 6 | evaluator.py | Relationship type |
| labels() | ✅ Complete | 6 | evaluator.py | Node labels |
| properties() | ✅ Complete | 4 | evaluator.py | Property map (v0.3.7) |
| keys() | ✅ Complete | 4 | evaluator.py | Property keys (v0.3.7) |
| coalesce() | ✅ Complete | 8 | evaluator.py | First non-NULL |
| toBoolean() | ✅ Complete | 5 | evaluator.py | Boolean conversion |
| timestamp() | ✅ Complete | 2 | evaluator.py | Current epoch milliseconds (v0.3.6) |
| startNode() | ✅ Complete | 3 | evaluator.py | Start node of relationship (v0.3.7) |
| endNode() | ✅ Complete | 3 | evaluator.py | End node of relationship (v0.3.7) |
| elementId() | ❌ Not Implemented | 0 | N/A | GQL spec feature (, planned v0.4.0) |

#### Temporal Functions (11 total: 11 complete) ✅ COMPLETE CATEGORY

| Function | Status | TCK Scenarios | File | Notes |
|----------|--------|---------------|------|-------|
| date() | ✅ Complete | 15 | evaluator.py | Date creation |
| datetime() | ✅ Complete | 20 | evaluator.py | DateTime creation |
| time() | ✅ Complete | 12 | evaluator.py | Time creation |
| localtime() | ✅ Complete | 8 | evaluator.py | Local time |
| localdatetime() | ✅ Complete | 10 | evaluator.py | Local datetime |
| duration() | ✅ Complete | 12 | evaluator.py | Duration creation |
| year(), month(), day() | ✅ Complete | 8 | evaluator.py | Component accessors |
| hour(), minute(), second() | ✅ Complete | 8 | evaluator.py | Time accessors |
| truncate() | ✅ Complete | 4 | evaluator.py | Temporal truncation; compact parsing and truncate variants (v0.3.8) |

#### Spatial Functions (2 total: 2 complete) ✅ COMPLETE CATEGORY

| Function | Status | TCK Scenarios | File | Notes |
|----------|--------|---------------|------|-------|
| point() | ✅ Complete | 6 | evaluator.py | Point creation (2D/3D) |
| distance() | ✅ Complete | 4 | evaluator.py | Distance calculation |

#### Path Functions (3 total: 3 complete) ✅ COMPLETE CATEGORY

| Function | Status | TCK Scenarios | File | Notes |
|----------|--------|---------------|------|-------|
| length() | ✅ Complete | 3 | evaluator.py | Path length |
| nodes() | ✅ Complete | 2 | evaluator.py | Path nodes |
| relationships() | ✅ Complete | 2 | evaluator.py | Path relationships |

---

### Operators (34 total: 34 complete) ✅ COMPLETE CATEGORY

#### Comparison Operators (8 total: 8 complete) ✅ COMPLETE CATEGORY

| Operator | Status | TCK Scenarios | Notes |
|----------|--------|---------------|-------|
| = | ✅ Complete | ~40 | Equality with NULL handling |
| <> | ✅ Complete | ~30 | Inequality |
| <, >, <=, >= | ✅ Complete | ~50 | Ordering comparisons |
| IS NULL, IS NOT NULL | ✅ Complete | ~20 | NULL testing |

#### Logical Operators (4 total: 4 complete) ✅ COMPLETE CATEGORY

| Operator | Status | TCK Scenarios | Notes |
|----------|--------|---------------|-------|
| AND | ✅ Complete | ~40 | Ternary logic |
| OR | ✅ Complete | ~35 | Ternary logic |
| NOT | ✅ Complete | ~25 | Ternary logic |
| XOR | ✅ Complete | ~8 | Exclusive or (v0.3.6) |

#### Arithmetic Operators (6 total: 6 complete) ✅ COMPLETE CATEGORY

| Operator | Status | TCK Scenarios | Notes |
|----------|--------|---------------|-------|
| +, -, *, /, % | ✅ Complete | ~100 | Standard arithmetic |
| ^ | ✅ Complete | ~5 | Power operator (v0.3.6) |

#### String Operators (5 total: 5 complete) ✅ COMPLETE CATEGORY

| Operator | Status | TCK Scenarios | Notes |
|----------|--------|---------------|-------|
| + | ✅ Complete | ~15 | Concatenation |
| =~ | ✅ Complete | ~8 | Regex match |
| STARTS WITH, ENDS WITH, CONTAINS | ✅ Complete | ~20 | Pattern matching |

#### List Operators (5 total: 5 complete) ✅ COMPLETE CATEGORY

| Operator | Status | TCK Scenarios | Notes |
|----------|--------|---------------|-------|
| IN | ✅ Complete | ~25 | Membership test |
| [] | ✅ Complete | ~20 | Index access |
| + | ✅ Complete | ~10 | List concatenation |
| [start..end] | ✅ Complete | ~8 | List slicing (v0.3.6) |
| Negative indexing | ✅ Complete | ~5 | Negative indices (v0.3.6) |

#### Pattern Operators (5 total: 5 complete) ✅ COMPLETE CATEGORY

| Operator | Status | TCK Scenarios | Notes |
|----------|--------|---------------|-------|
| - | ✅ Complete | ~60 | Undirected relationship |
| ->, <-- | ✅ Complete | ~100 | Directed relationships |
| -[*]- | ✅ Complete | ~40 | Variable-length |
| : | ✅ Complete | ~50 | Label check |

---

### Patterns (8 total: 7 complete, 0 partial, 1 not implemented)

| Pattern Type | Status | TCK Scenarios | Notes |
|--------------|--------|---------------|-------|
| Node patterns | ✅ Complete | ~100 | All variations supported |
| Relationship patterns | ✅ Complete | ~100 | Direction, types, properties |
| Variable-length paths | ✅ Complete | ~40 | All quantifier forms; named paths (v0.3.8) |
| Path variables | ✅ Complete | ~20 | Path binding |
| OPTIONAL patterns | ✅ Complete | ~20 | NULL handling |
| Multiple patterns | ✅ Complete | ~30 | Comma-separated |
| Pattern predicates | ✅ Complete | ~15 | Full WHERE in patterns, optimizer |
| Pattern comprehension | ❌ Not Implemented | 15 | Not supported |

---

## Roadmap

### v0.3.9 — Performance (Target: June 2026)

Focus: Speed and scalability improvements for medium-to-large graphs.

- LALR(1) parser migration (faster parse times)
- Node/edge property indexing
- SQLite storage tuning (batch writes, WAL mode)
- Query plan optimisation pass
- Analytics integration hooks — –

### v0.4.0 — Graph Analytics (Target: Q3 2026)

Focus: Native social network analysis (SNA) algorithms exposed as CALL procedures.

- PageRank
- Betweenness centrality
- Weakly connected components (WCC)
- Shortest path algorithms as built-in procedures
- elementId() scalar function

---

## Version History

### v0.1.0 (Initial Release)
- Basic MATCH, CREATE, RETURN
- Core pattern matching
- String and numeric functions

### v0.2.0
- SET, REMOVE, DELETE, MERGE
- UNWIND clause
- Enhanced aggregations

### v0.3.0 (February 2026)
- OPTIONAL MATCH (full NULL handling)
- WITH clause (full spec compliance)
- UNION/UNION ALL
- Temporal functions (complete)
- Spatial functions (complete)
- EXISTS/COUNT subqueries
- TCK inventory and mapping (1,626 scenarios cataloged)
- TCK pass rate: 16.6% → 34%

### v0.3.5
- sqrt(), rand(), pow() numeric functions
- percentileDisc(), percentileCont(), stDev(), stDevP() aggregations

### v0.3.6
- e(), pi() mathematical constants
- Trigonometric functions: sin, cos, tan, cot, asin, acos, atan, atan2
- exp(), log(), log10() exponential/logarithmic functions
- degrees(), radians() angle conversion
- timestamp() current epoch milliseconds
- Predicate functions: all(), any(), none(), single(), exists(), isEmpty()
- List operations: extract(), filter(), reduce()
- List slicing [start..end] and negative indexing
- XOR logical operator
- ^ power operator

### v0.3.7
- CALL procedures with procedure registry
- startNode(), endNode() scalar functions
- keys(), properties() scalar functions
- Variable-length path improvements

### v0.3.8 (May 2026)
- Temporal edge cases: compact parsing, epochMillis, truncate variants
- Variable-length named paths
- Pattern predicate optimizer
- OPTIONAL MATCH unbound-src support
- SyntaxError validation improvements
- EXISTS subquery grammar (partial; full support pending)
- TCK pass rate: ~97.8% (3,801/3,885 scenarios)

---

## TCK Compliance Metrics

### Current Status (v0.3.8)
- **Total scenarios:** 3,885
- **Passing:** 3,801 (97.8%)
- **Failing:** 84 (2.2%)
- **Permanent xfails:** 20 (CPython implementation limitations)

### Remaining Failure Categories

1. **elementId() not implemented** — ~10 scenarios
2. **EXISTS / CALL { } subquery edge cases** — ~13 scenarios
3. **FOREACH not implemented** — ~5 scenarios
4. **TCK edge cases** — ~56 scenarios across:
   - List comprehension variable scope
   - MERGE property-copying semantics
   - Static property access on complex expressions
   - UNWIND scope leakage
   - Validation/type-coercion corner cases

---

## Strengths & Limitations

### Major Strengths ✅

1. **Near-complete OpenCypher coverage** (v0.3.8)
   - 140/145 features complete
   - 97.8% TCK pass rate

2. **Complete temporal support**
   - All date/time types
   - All temporal functions including compact forms and truncate variants
   - Duration arithmetic

3. **Complete spatial support**
   - 2D/3D points (Cartesian and geographic)
   - Distance calculations

4. **Complete function library**
   - 82/83 functions implemented
   - Full numeric suite (19 functions) including trig, log, exp
   - All predicate functions, list operations, aggregations

5. **Advanced query features**
   - OPTIONAL MATCH with proper NULL handling
   - WITH clause (full spec)
   - UNION operations
   - Variable-length paths with named path support
   - Pattern predicates with query optimizer
   - CALL procedures

### Key Limitations ❌

1. **elementId() not implemented**
   - GQL standard feature
   - Tracked as, planned for v0.4.0

2. **EXISTS subquery partial**
   - Simple EXISTS works; full correlated subquery syntax incomplete
   - Tracked as 

3. **FOREACH not implemented**
   - Low priority (no TCK scenarios currently failing hard against this)

4. **LOAD CSV not implemented**
   - Use the GraphForge dataset system (`gf.load_dataset()`) as the alternative

5. **~61 TCK edge cases remaining**
   - List comprehension scope, MERGE property copying, static property access on expressions, UNWIND scope

---

## How to Use This Matrix

### For Contributors

1. **Prioritizing work:** See the Roadmap section
2. **Finding implementation:** Use the "File" column (evaluator.py / executor.py)
3. **Understanding coverage:** Check the TCK Scenarios column
4. **Identifying gaps:** See ❌ Not Implemented rows and the remaining TCK failure categories

### For Users

1. **Feature support:** Check Status column (✅ ⚠️ ❌)
2. **Workarounds:** See Notes column for alternatives
3. **Version planning:** See Version History for feature timeline
4. **Known issues:** See Key Limitations section

### For Researchers

1. **Compliance analysis:** Use Executive Summary statistics
2. **Coverage metrics:** See TCK Compliance Metrics section
3. **Trend analysis:** Compare across Version History entries
4. **Gap analysis:** Review Remaining Failure Categories

---

## Related Documentation

- **Feature Documentation:** `docs/reference/opencypher-features/`
  - Detailed spec for each feature
- **Implementation Status:** `docs/reference/implementation-status/`
  - Per-category status with file references
- **TCK Mapping:** `docs/reference/feature-mapping/`
  - TCK test to feature mapping
- **TCK Inventory:** `docs/reference/tck-inventory.md`
  - Complete TCK scenario catalog
- **Graph Schema:** `docs/reference/feature-graph-schema.md`
  - Queryable feature knowledge graph

---

## References

- OpenCypher Specification: https://opencypher.org/resources/
- OpenCypher TCK: https://github.com/opencypher/openCypher/tree/master/tck
- GraphForge Repository: https://github.com/CurateLabs/graphforge
- GQL Standard (ISO/IEC 39075): https://www.iso.org/standard/76120.html
