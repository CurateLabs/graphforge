# OpenCypher Clause to TCK Test Mapping

Mapping of OpenCypher clauses to their corresponding TCK test scenarios.

**Source:** TCK inventory in `tck-inventory.md`

---

## Summary

| Clause | TCK Categories | Total Scenarios | Implementation Status | Coverage |
|--------|---------------|-----------------|---------------------|----------|
| MATCH | match, match-where | 195 | ✅ Complete | Excellent |
| CREATE | create | 78 | ✅ Complete | Excellent |
| MERGE | merge | 75 | ✅ Complete | Excellent |
| RETURN | return, return-orderby, return-skip-limit | 129 | ✅ Complete | Excellent |
| WHERE | match-where, with-where | 53 | ✅ Complete | Good |
| WITH | with, with-where, with-orderBy, with-skip-limit | 156 | ✅ Complete | Excellent |
| SET | set | 53 | ✅ Complete | Good |
| DELETE | delete | 41 | ✅ Complete | Good |
| REMOVE | remove | 33 | ✅ Complete | Good |
| ORDER BY | return-orderby, with-orderBy | 134 | ✅ Complete | Excellent |
| LIMIT | return-skip-limit, with-skip-limit | 40 | ✅ Complete | Good |
| SKIP | return-skip-limit, with-skip-limit | 40 | ✅ Complete | Good |
| UNION | union | 12 | ✅ Complete | Minimal |
| UNWIND | unwind | 14 | ✅ Complete | Minimal |
| OPTIONAL MATCH | match patterns | ~20 | ✅ Complete | Good |
| CALL | call | 41 | ❌ Not Implemented | N/A |
| CALL {} | existentialSubqueries | 10 | ⚠️ Partial | Minimal |
| **TOTAL** | **17 categories** | **~1180** | **15.5/17** | **Good** |

---

## Reading Clauses

### MATCH

**TCK Categories:**
- `clauses/match` (9 files, 161 scenarios)
- `clauses/match-where` (6 files, 34 scenarios)

**Total TCK Coverage:** 195 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Match1-9.feature: Comprehensive pattern matching
- MatchWhere1-6.feature: MATCH with WHERE filtering
- Covers: Node patterns, relationship patterns, variable-length paths, path variables, multiple patterns

**Coverage Assessment:** Excellent - covers all major MATCH variations

**Test Files:**
```
tests/tck/features/official/clauses/match/Match1.feature (11 scenarios)
tests/tck/features/official/clauses/match/Match2.feature (13 scenarios)
tests/tck/features/official/clauses/match/Match3.feature (30 scenarios)
tests/tck/features/official/clauses/match/Match4.feature (10 scenarios)
tests/tck/features/official/clauses/match/Match5.feature (29 scenarios)
tests/tck/features/official/clauses/match/Match6.feature (25 scenarios)
tests/tck/features/official/clauses/match/Match7.feature (31 scenarios)
tests/tck/features/official/clauses/match/Match8.feature (3 scenarios)
tests/tck/features/official/clauses/match/Match9.feature (9 scenarios)
tests/tck/features/official/clauses/match-where/MatchWhere1-6.feature (34 scenarios)
```

---

### OPTIONAL MATCH

**TCK Categories:**
- Included within `clauses/match` scenarios
- Pattern comprehension tests

**Total TCK Coverage:** ~20 scenarios (estimated)

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- OPTIONAL MATCH scenarios distributed across Match feature files
- Tests NULL handling and outer join semantics

**Coverage Assessment:** Good - covers essential OPTIONAL MATCH behavior

---

## Projecting Clauses

### RETURN

**TCK Categories:**
- `clauses/return` (8 files, 63 scenarios)
- `clauses/return-orderby` (6 files, 35 scenarios)
- `clauses/return-skip-limit` (3 files, 31 scenarios)

**Total TCK Coverage:** 129 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Return1-8.feature: Projection, aliasing, DISTINCT, expressions
- ReturnOrderBy1-3.feature: Sorting with RETURN
- ReturnSkipLimit1-3.feature: Pagination with RETURN

**Coverage Assessment:** Excellent - comprehensive coverage of RETURN variations

---

### WITH

**TCK Categories:**
- `clauses/with` (7 files, 29 scenarios)
- `clauses/with-where` (7 files, 19 scenarios)
- `clauses/with-orderBy` (4 files, 99 scenarios)
- `clauses/with-skip-limit` (3 files, 9 scenarios)

**Total TCK Coverage:** 156 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- With1-7.feature: Variable scoping, query chaining, WITH at query start
- WithWhere1-7.feature: Filtering in WITH
- WithOrderBy1-4.feature: Sorting in WITH (extensive)
- WithSkipLimit1-3.feature: Pagination in WITH

**Coverage Assessment:** Excellent - most tested clause with comprehensive edge cases

---

### UNWIND

**TCK Categories:**
- `clauses/unwind` (1 file, 14 scenarios)

**Total TCK Coverage:** 14 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Unwind1.feature: List expansion, NULL handling, chaining

**Coverage Assessment:** Minimal but adequate - covers basic UNWIND behavior

---

## Filtering and Sorting Sub-clauses

### WHERE

**TCK Categories:**
- `clauses/match-where` (6 files, 34 scenarios)
- `clauses/with-where` (7 files, 19 scenarios)

**Total TCK Coverage:** 53 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Boolean predicates, comparison operators, NULL handling
- Pattern predicates
- Property existence checks
- List membership (IN)

**Coverage Assessment:** Good - covers WHERE in both MATCH and WITH contexts

---

### ORDER BY

**TCK Categories:**
- `clauses/return-orderby` (6 files, 35 scenarios)
- `clauses/with-orderBy` (4 files, 99 scenarios)

**Total TCK Coverage:** 134 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- ASC/DESC ordering
- Multiple sort keys
- NULL ordering
- Expression ordering

**Coverage Assessment:** Excellent - comprehensive sorting scenarios

---

### LIMIT

**TCK Categories:**
- `clauses/return-skip-limit` (3 files, 31 scenarios)
- `clauses/with-skip-limit` (3 files, 9 scenarios)

**Total TCK Coverage:** 40 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Result set limiting
- Combination with ORDER BY and SKIP
- Edge cases (limit 0, negative values)

**Coverage Assessment:** Good - covers LIMIT in both RETURN and WITH

---

### SKIP

**TCK Categories:**
- `clauses/return-skip-limit` (3 files, 31 scenarios)
- `clauses/with-skip-limit` (3 files, 9 scenarios)

**Total TCK Coverage:** 40 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Pagination support
- Combination with LIMIT
- Edge cases

**Coverage Assessment:** Good - covers SKIP in both contexts

---

## Writing Clauses

### CREATE

**TCK Categories:**
- `clauses/create` (6 files, 78 scenarios)

**Total TCK Coverage:** 78 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Create1-6.feature: Node creation, relationship creation, patterns
- Property setting
- Multiple creates
- CREATE with MATCH

**Coverage Assessment:** Excellent - comprehensive CREATE scenarios

---

### DELETE

**TCK Categories:**
- `clauses/delete` (6 files, 41 scenarios)

**Total TCK Coverage:** 41 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Delete1-6.feature: Node deletion, relationship deletion
- DETACH DELETE
- Error scenarios (deleting node with relationships)

**Coverage Assessment:** Good - covers both DELETE and DETACH DELETE

---

### SET

**TCK Categories:**
- `clauses/set` (6 files, 53 scenarios)

**Total TCK Coverage:** 53 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Set1-6.feature: Property updates, label updates
- Expression evaluation in SET
- Multiple SET operations

**Coverage Assessment:** Good - comprehensive property updates

---

### REMOVE

**TCK Categories:**
- `clauses/remove` (3 files, 33 scenarios)

**Total TCK Coverage:** 33 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Remove1-3.feature: Property removal, label removal
- Multiple REMOVE operations

**Coverage Assessment:** Good - covers REMOVE variations

---

## Reading/Writing Clauses

### MERGE

**TCK Categories:**
- `clauses/merge` (9 files, 75 scenarios)

**Total TCK Coverage:** 75 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Merge1-9.feature: Match or create semantics
- ON CREATE SET
- ON MATCH SET
- Node and relationship merging

**Coverage Assessment:** Excellent - comprehensive MERGE scenarios

---

### CALL

**TCK Categories:**
- `clauses/call` (6 files, 41 scenarios)

**Total TCK Coverage:** 41 scenarios

**Implementation Status:** ❌ NOT_IMPLEMENTED

**TCK Coverage Details:**
- Call1-6.feature: Procedure invocation, YIELD clause
- Not applicable (procedures not implemented)

**Coverage Assessment:** N/A - feature not implemented

---

## Subquery Clauses

### CALL { } (Subqueries)

**TCK Categories:**
- `expressions/existentialSubqueries` (3 files, 10 scenarios)

**Total TCK Coverage:** 10 scenarios

**Implementation Status:** ⚠️ PARTIAL (EXISTS/COUNT only)

**TCK Coverage Details:**
- ExistentialSubquery1-3.feature: EXISTS() and COUNT() subqueries
- General CALL { } syntax not tested (not in TCK subset)

**Coverage Assessment:** Minimal - only covers EXISTS/COUNT subquery expressions

---

## Set Operations

### UNION

**TCK Categories:**
- `clauses/union` (3 files, 12 scenarios)

**Total TCK Coverage:** 12 scenarios

**Implementation Status:** ✅ COMPLETE

**TCK Coverage Details:**
- Union1-3.feature: UNION and UNION ALL
- Result combining
- Column alignment

**Coverage Assessment:** Minimal but adequate

---

## Coverage Gaps

### Clauses with Minimal TCK Coverage

1. **UNION**: Only 12 scenarios
   - Could benefit from more complex union scenarios
   - Edge cases with different column counts/types

2. **UNWIND**: Only 14 scenarios
   - Limited coverage of UNWIND variations
   - Could benefit from more chaining scenarios

3. **CALL { } subqueries**: Only 10 scenarios (EXISTS/COUNT)
   - General subquery syntax not in TCK yet
   - Missing: UNION in subqueries, variable importing

### Clauses with No TCK Coverage

1. **FOREACH**: Not in TCK
   - Not implemented in GraphForge

2. **LOAD CSV**: Not in TCK
   - Not implemented in GraphForge

3. **USE**: Not in TCK
   - Multi-database feature, not applicable

---

## Notes

### TCK Coverage Interpretation

- **Excellent (100+ scenarios)**: Comprehensive testing across many variations
- **Good (30-99 scenarios)**: Adequate coverage of main features
- **Minimal (<30 scenarios)**: Basic coverage, may miss edge cases

### Implementation Completeness vs TCK Coverage

GraphForge passes most TCK scenarios for implemented clauses:
- Current TCK pass rate: ~34% (1,303/3,837 total scenarios)
- For implemented clauses: Much higher pass rate
- Failures mainly due to:
  - Missing functions (predicate functions, statistical)
  - Missing operators (XOR, power, list slicing)
  - Pattern comprehension not implemented
  - Edge cases in complex queries

### Recommended Improvements

1. **Add more TCK scenarios for UNION**: Expand test coverage
2. **Add more UNWIND scenarios**: Test chaining and edge cases
3. **Implement CALL { } subqueries**: Add general subquery syntax support

---

## References

- TCK Inventory: `docs/reference/tck-inventory.md`
- Clause Implementation Status: `docs/reference/implementation-status/clauses.md`
- TCK Repository: https://github.com/opencypher/openCypher/tree/master/tck
