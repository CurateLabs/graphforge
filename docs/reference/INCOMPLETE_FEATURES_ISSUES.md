# Incomplete OpenCypher Features

Inventory of partially implemented and unimplemented OpenCypher features in GraphForge (status snapshot; not a live issue tracker).

---

## Summary by Category

| Category | Partial | Not Implemented | Total |
|----------|---------|-----------------|-------|
| **Clauses** | 2 | 3 | 5 |
| **Functions** | 0 | 19 | 19 |
| **Operators** | 0 | 4 | 4 |
| **Patterns** | 1 | 1 | 2 |
| **TOTAL** | **3** | **27** | **30** |

---

## Clauses (5 features)

### Partial Implementation (2)

| Feature | Status | TCK Scenarios | Priority |
|---------|--------|---------------|----------|
| CALL { } subqueries | ⚠️ PARTIAL | ~10 | Medium |
| MERGE | ⚠️ PARTIAL | 75 | High |

**CALL { } — Current:** EXISTS/COUNT subqueries only
**CALL { } — Needs:** General CALL { } syntax, UNION in subqueries, variable importing

**MERGE — Current:** Standalone new-node MERGE and relationship MERGE with already-bound endpoints; `ON CREATE` / `ON MATCH` property/label actions when all frontier rows share one branch. Authority: `crates/graphforge-exec/src/write_driver.rs`. Direct evidence: `crates/graphforge-api/tests/e2e_baseline.rs` (`merge_node_*`, `merge_relationship_*`).
**MERGE — Rejected (not support):** multi-node / relationship-construction MERGE (`relationship and multi-node MERGE execution is not implemented yet`); row-conditional map actions (`row-conditional MERGE map actions are not implemented yet`). Details: [clauses.md](implementation-status/clauses.md#merge).

### Not Implemented (3)

| Feature | Status | TCK Scenarios | Priority |
|---------|--------|---------------|----------|
| CALL procedures | ❌ NOT_IMPLEMENTED | 41 | Medium |
| FOREACH | ❌ NOT_IMPLEMENTED | 0 | Low |
| LOAD CSV | ❌ NOT_IMPLEMENTED | 0 | Medium |

---

## Functions (19 features)

### String Functions (2 features)

| Function | Status | TCK Scenarios | Priority |
|----------|--------|---------------|----------|
| length() | ❌ NOT_IMPLEMENTED | ~1 | Medium |
| toUpper(), toLower() (camelCase) | ❌ NOT_IMPLEMENTED | ~2 | Low |

**Notes:**
- length() conflicts with path length(), needs context-dependent resolution
- UPPER/LOWER already exist, just need camelCase aliases

### Numeric Functions (3 features)

| Function | Status | TCK Scenarios | Priority |
|----------|--------|---------------|----------|
| sqrt() | ❌ NOT_IMPLEMENTED | 0 | Medium |
| rand() | ❌ NOT_IMPLEMENTED | 0 | Low |
| pow() / ^ | ❌ NOT_IMPLEMENTED | 0 | Low |

### List Functions (3 features)

| Function | Status | TCK Scenarios | Priority |
|----------|--------|---------------|----------|
| extract() | ❌ NOT_IMPLEMENTED | ~15 | **HIGH** |
| filter() | ❌ NOT_IMPLEMENTED | ~10 | **HIGH** |
| reduce() | ❌ NOT_IMPLEMENTED | ~5 | Medium |

**Notes:** extract() and filter() are high priority with good TCK coverage

### Aggregation Functions (4 features)

| Function | Status | TCK Scenarios | Priority |
|----------|--------|---------------|----------|
| percentileDisc() | ❌ NOT_IMPLEMENTED | ~1 | Low |
| percentileCont() | ❌ NOT_IMPLEMENTED | ~1 | Low |
| stDev() | ❌ NOT_IMPLEMENTED | ~0.5 | Low |
| stDevP() | ❌ NOT_IMPLEMENTED | ~0.5 | Low |

**Notes:** Statistical aggregations for analytics use cases

### Predicate Functions (6 features) ⚠️ HIGH PRIORITY

| Function | Status | TCK Scenarios | Priority |
|----------|--------|---------------|----------|
| all() | ❌ NOT_IMPLEMENTED | ~8 | **HIGH** |
| any() | ❌ NOT_IMPLEMENTED | ~8 | **HIGH** |
| none() | ❌ NOT_IMPLEMENTED | ~4 | **HIGH** |
| single() | ❌ NOT_IMPLEMENTED | ~4 | **HIGH** |
| exists() | ❌ NOT_IMPLEMENTED | ~10 | **HIGH** |
| isEmpty() | ❌ NOT_IMPLEMENTED | ~2 | Medium |

**Notes:**
- All predicate functions are HIGH PRIORITY
- Commonly used in WHERE clauses
- ~36 TCK scenarios total
- exists() is distinct from EXISTS() subquery expression (already implemented)

### Scalar Functions (1 features)

| Function | Status | TCK Scenarios | Priority |
|----------|--------|---------------|----------|
| elementId() | ❌ NOT_IMPLEMENTED | 0 | Low |

**Notes:** GQL standard function, alternative to id()

---

## Operators (4 features)

### Logical Operators (1 features)

| Operator | Status | TCK Scenarios | Priority |
|----------|--------|---------------|----------|
| XOR | ❌ NOT_IMPLEMENTED | 0 | Low |

### Arithmetic Operators (1 features)

| Operator | Status | TCK Scenarios | Priority |
|----------|--------|---------------|----------|
| ^ (power) | ❌ NOT_IMPLEMENTED | 0 | Low |

**Notes:** Related to pow() function, should be implemented together

### List Operators (2 features)

| Operator | Status | TCK Scenarios | Priority |
|----------|--------|---------------|----------|
| [start..end] slicing | ❌ NOT_IMPLEMENTED | Unknown | Medium |
| Negative indexing | ❌ NOT_IMPLEMENTED | Unknown | Medium |

**Notes:** Python-style list operations

---

## Patterns (2 features)

### Partial Implementation (1)

| Feature | Status | TCK Scenarios | Priority |
|---------|--------|---------------|----------|
| Pattern predicates | ⚠️ PARTIAL | ~15 | Medium |

**Current:** Basic WHERE in patterns
**Needs:** Full pattern predicate support

### Not Implemented (1)

| Feature | Status | TCK Scenarios | Priority |
|---------|--------|---------------|----------|
| Pattern comprehension | ❌ NOT_IMPLEMENTED | 15 | Medium |

**Notes:** Complex feature combining pattern matching with list comprehension

---

## Patch-Level Release Strategy

**New Approach:** All features will be completed in patch releases (v0.3.x) until 100% complete.

### v0.3.1 (Target: March 2026) - Predicate Functions
**Goal:** 78% → 82% feature complete

- all() predicate function
- any() predicate function
- none() predicate function
- single() predicate function
- exists() predicate function
- isEmpty() predicate function

**Impact:** ~36 TCK scenarios, commonly used in WHERE clauses

### v0.3.2 (Target: April 2026) - List Operations
**Goal:** 82% → 85% feature complete

- extract() list function
- filter() list function
- reduce() list function

**Impact:** ~30 TCK scenarios, essential for data transformation

### v0.3.3 (Target: May 2026) - Pattern & CALL Features
**Goal:** 85% → 88% feature complete

- Complete CALL { } subquery syntax (PARTIAL → COMPLETE)
- Complete pattern predicates (PARTIAL → COMPLETE)
- Pattern comprehension

**Impact:** ~40 TCK scenarios, advanced query capabilities

### v0.3.4 (Target: June 2026) - Operators & String Functions
**Goal:** 88% → 92% feature complete

- length() string function
- toUpper/toLower camelCase variants
- XOR logical operator
- ^ (power) arithmetic operator
- List slicing [start..end]
- Negative list indexing

**Impact:** Operator completeness, string function parity

### v0.3.5 (Target: July 2026) - Math & Aggregation Functions
**Goal:** 92% → 96% feature complete

- sqrt() function
- rand() function
- pow() function
- percentileDisc() aggregation
- percentileCont() aggregation
- stDev() aggregation
- stDevP() aggregation

**Impact:** Mathematical operations complete, statistical analysis support

### v0.3.6 (Target: August 2026) - Remaining Clauses
**Goal:** 96% → 99% feature complete

- CALL procedures (with procedure registry)
- FOREACH clause
- LOAD CSV clause
- elementId() scalar function

**Impact:** Procedural capabilities, data import, GQL compliance

### v0.3.7 (Target: September 2026) - Final Polish
**Goal:** 99% → 100% feature complete

**Focus:**
- Edge case fixes from TCK
- Documentation completeness
- Performance optimization
- API refinements

**Result:** 134/134 features complete (100%)

---

## Issue Template

All issues follow this template:

### Feature Information
- Type (Clause/Function/Operator/Pattern)
- Category
- Current Status (PARTIAL/NOT_IMPLEMENTED)

### Documentation References
- Implementation Status document
- Feature Documentation document
- Compatibility Matrix

### TCK Coverage
- Scenario count
- TCK mapping document reference

### Acceptance Criteria
- **Implementation:** Specific implementation requirements
- **Testing:** Unit tests, integration tests, TCK pass rate, 90% coverage minimum
- **Documentation:** Update all relevant docs and add examples

### Notes
- Context and priority information
- Related issues
- Implementation considerations

---

## Tracking Progress

### Overall Timeline

**Start:** v0.3.0 (Feb 2026) - 78% complete (105/134 features)
**End:** v0.3.7 (Sep 2026) - 100% complete (134/134 features)
**Duration:** 7 months with 7 patch releases

### Monthly Milestones

| Month | Release | Features Added | Total Complete | Percentage |
|-------|---------|----------------|----------------|------------|
| Feb 2026 | v0.3.0 | - | 105/134 | 78% |
| Mar 2026 | v0.3.1 | 6 predicates | 111/134 | 82% |
| Apr 2026 | v0.3.2 | 3 list ops | 114/134 | 85% |
| May 2026 | v0.3.3 | 3 patterns | 117/134 | 88% |
| Jun 2026 | v0.3.4 | 6 operators | 123/134 | 92% |
| Jul 2026 | v0.3.5 | 7 math/agg | 130/134 | 96% |
| Aug 2026 | v0.3.6 | 4 clauses | 134/134 | 99% |
| Sep 2026 | v0.3.7 | Polish | 134/134 | **100%** |

### Feature Completion by Category

| Category | v0.3.0 | v0.3.7 | Change |
|----------|--------|--------|--------|
| Clauses | 16/20 (80%) | 20/20 (100%) | +4 |
| Functions | 53/72 (74%) | 72/72 (100%) | +19 |
| Operators | 30/34 (88%) | 34/34 (100%) | +4 |
| Patterns | 6/8 (75%) | 8/8 (100%) | +2 |
| **TOTAL** | **105/134 (78%)** | **134/134 (100%)** | **+29** |

---

## References

- **Documentation:** [docs/reference/](./)
- **Validation Report:** [docs/reference/VALIDATION_REPORT.md](VALIDATION_REPORT.md)
- **Compatibility Matrix:** [docs/reference/opencypher-compatibility-matrix.md](opencypher-compatibility-matrix.md)
- **the work item:** TCK Coverage v0.4.0 (parent tracking issue)
- **GitHub Milestones:** release planning notes

---

**Created:** 2026-02-17
**Last Updated:** 2026-02-17 (Updated for patch-level release strategy)
**Maintainer:** [@DecisionNerd](https://github.com/DecisionNerd)
