# TCK Baseline Analysis - GraphForge v0.3.0

**Analysis Date:** 2026-02-15
**Test Suite:** openCypher TCK Official Suite

## Executive Summary

**Current State:**
- **1,252 scenarios passing** (32.6% compliance)
- **2,585 scenarios failing** (67.4%)
- **Total:** 3,837 TCK scenarios

**Key Finding:** The actual baseline is **32.6%**, not 2.8% as documented in the v0.4.0 plan. This significantly changes the implementation strategy.

---

## Failure Analysis

### Top 5 Blockers (Accounts for 88% of failures)

| Category | Count | % of Failures | Fix Complexity |
|----------|-------|---------------|----------------|
| 1. WITH at query start | 828 | 32.0% | Medium (Parser) |
| 2. Temporal map syntax | 1,050 | 40.6% | Medium (Type constructors) |
| 3. Map property access | 64 | 2.5% | Low (Evaluator) |
| 4. CALL procedures | 50 | 1.9% | N/A (Out of scope) |
| 5. Other parser issues | 303 | 11.7% | Varies |

**Total Accounted:** 2,295 / 2,585 (88.8%)

---

## Detailed Breakdown

### 1. WITH Clause at Query Start (828 failures = 21.6% gain potential)

**Error:** `No terminal matches 'W' in the current parser context, at line 1 col 1`

**Root Cause:** Parser grammar only allows WITH after MATCH/CREATE/etc., not at query start

**Example failing queries:**
```cypher
WITH [1, 2, 3] AS list
RETURN list[0]

WITH 1 AS x
MATCH (n) WHERE n.id = x
RETURN n
```

**Fix:** Update `cypher.lark` to allow WITH as a read clause start

**Estimated TCK Impact:** +828 scenarios (21.6%)
**Effort:** 4-6 hours (parser grammar + tests)
**Risk:** Medium (affects query structure parsing)

---

### 2. Temporal Type Map Syntax (1,050 failures = 27.4% gain potential)

**Errors:**
- `DATETIME expects string, got CypherMap` (212)
- `DATE expects string, got CypherMap` (50)
- `TIME expects string, got CypherMap` (38)
- `localdatetime` related parser errors (310+266+192+150+58+44+30=1,050 total)

**Root Cause:** Temporal constructors only accept strings, not map syntax

**Example failing queries:**
```cypher
RETURN datetime({year: 1984, month: 10, day: 11}) AS d
RETURN localdatetime({year: 1816, week: 1}) AS d
RETURN date({year: 2015, month: 1, day: 1}) AS d
```

**openCypher Spec:** Temporal types support both string and map constructors

**Fix:**
1. Add map parameter support to temporal constructors in evaluator
2. Support named parameters: year, month, day, hour, minute, second, millisecond, microsecond, nanosecond, timezone
3. Support component-based constructors: year+week, year+quarter+day, etc.

**Estimated TCK Impact:** +1,050 scenarios (27.4%)
**Effort:** 12-16 hours (complex validation + tests)
**Risk:** Medium (temporal arithmetic is complex)

---

### 3. Map Property Access (64 failures = 1.7% gain potential)

**Error:** `Cannot access property on CypherMap`

**Root Cause:** Evaluator doesn't support property access syntax on map values

**Example failing queries:**
```cypher
RETURN {name: 'Alice', age: 30}.name AS name  // Should return 'Alice'
WITH {a: 1, b: 2} AS m
RETURN m.a  // Should return 1
```

**Fix:** Update property access evaluator to handle CypherMap

**Estimated TCK Impact:** +64 scenarios (1.7%)
**Effort:** 2-3 hours (evaluator + tests)
**Risk:** Low

---

### 4. Expression Category Failures (Minor impact)

| Category | Failures | Notes |
|----------|----------|-------|
| Mathematical | 0 | ✅ Fully implemented |
| String | 0 | ✅ Fully implemented |
| List | 14 | Likely list comprehension syntax |
| Aggregation | 12 | Edge cases (DISTINCT, NULL handling) |
| Null | 7 | Three-valued logic edge cases |
| Type Conversion | 1 | Minor edge case |

**Total:** 34 failures from expressions (1.3%)

**Finding:** Phase 1 (Expression Functions) in the original plan will have **near-zero TCK impact** since these are already implemented.

---

### 5. Clause Category Failures (Minor impact)

| Clause | Failures | Notes |
|--------|----------|-------|
| CALL | 50 | Stored procedures (out of scope) |
| CREATE | 8 | Edge cases |
| MERGE | 3 | Relationship patterns |
| MATCH-WHERE | 2 | Complex predicates |
| RETURN/WITH variants | 12 | Edge cases |
| Other | 8 | Various |

**Total:** 83 failures from clauses (3.2%)

**Finding:** Phases 2 & 3 (List Operations, MERGE) will have **minimal TCK impact** (~17 scenarios total).

---

## Revised TCK Impact Estimates

### Original Plan (Based on 37 scenarios = 2.8%)

| Phase | Feature | Estimated Impact |
|-------|---------|------------------|
| 1 | Expression Functions | +71 scenarios |
| 2 | List Operations | +94 scenarios |
| 3 | MERGE Enhancements | +40 scenarios |
| 4 | Optimizer | +20 scenarios |
| 5 | Bug Fixes | +351 scenarios |
| **Total** | **+576 scenarios → 50%** |

### Actual Reality (Based on 1,252 scenarios = 32.6%)

| Feature | Actual Impact | Reason |
|---------|---------------|--------|
| Expression Functions | ~0 scenarios | ✅ Already implemented |
| List Operations | ~14 scenarios | Most list ops working |
| MERGE Enhancements | ~3 scenarios | Basic MERGE works |
| **High-Impact Missing Features** | |
| WITH at query start | +828 scenarios | Major parser gap |
| Temporal map syntax | +1,050 scenarios | Major feature gap |
| Map property access | +64 scenarios | Evaluator gap |
| **Total Potential** | **+1,959 scenarios → 83.7%!** |

---

## Recommendations

### Immediate Priority Shift

**RECOMMENDED: Pivot v0.4.0 plan to focus on high-impact gaps**

**New Phase Priorities:**

1. **Phase 1A: WITH at Query Start** (6 hours)
   - Parser grammar changes
   - +828 scenarios (21.6% → 54.2%)

2. **Phase 1B: Map Property Access** (3 hours)
   - Evaluator enhancement
   - +64 scenarios (54.2% → 55.9%)

3. **Phase 2: Temporal Map Syntax** (16 hours)
   - Comprehensive temporal constructor support
   - +1,050 scenarios (55.9% → 83.3%)

4. **Phase 3: Bug Fixes & Edge Cases** (20 hours)
   - Remaining 482 failures
   - Target: +350 scenarios (83.3% → 92.4%)

**Total Effort:** 45 hours (vs. 139 hours in original plan)
**Total Impact:** 1,252 → 3,544 scenarios (32.6% → 92.4%)

---

## Original Plan Items - Revised Assessment

### Phase 1: Expression Functions ❌ NOT NEEDED
- **Reason:** Math & string functions already implemented
- **Evidence:** 0 failures in mathematical and string expression tests
- **Recommendation:** SKIP this phase

### Phase 2: List Operations ⚠️ MINIMAL IMPACT
- **Current Impact:** Only 14 TCK failures related to lists
- **Reason:** Most list operations already work
- **Recommendation:** Address only if pattern/list comprehensions are needed

### Phase 3: MERGE Enhancements ⚠️ MINIMAL IMPACT
- **Current Impact:** Only 3 TCK failures in MERGE
- **Reason:** Basic MERGE works, only edge cases remain
- **Recommendation:** Low priority, handle in bug fix phase

### Phase 4: Optimizer ✅ PROCEED AS PLANNED
- **Reason:** Performance & architecture investment, not TCK-driven
- **Recommendation:** Keep if performance matters

### Phase 5: Bug Fixes ✅ PROCEED AS PLANNED
- **Reason:** Will be needed for remaining edge cases after high-impact fixes
- **Recommendation:** Focus on actual failure categories, not hypothetical bugs

---

## Next Steps

### Decision Required

**Option A: Proceed with Original Plan**
- Implements features that are mostly done
- Lower TCK gains than expected
- 139 hours → 50% compliance (1,252 → 1,913 scenarios, +661)

**Option B: Pivot to High-Impact Plan**
- Focuses on actual gaps (WITH, temporal, maps)
- Massive TCK gains (32.6% → 92.4%)
- 45 hours → 92% compliance (1,252 → 3,544 scenarios, +2,292)

**Option C: Hybrid Approach**
- High-impact features first (25 hours → 83%)
- Then optimizer investment (38 hours)
- Then remaining bugs (20 hours)
- 83 hours → 92% compliance with optimizer

**Recommendation:** **Option C (Hybrid)** - Maximizes TCK gains while preserving architectural investment in optimizer.

---

## Validation Notes

- TCK suite run: 2026-02-15
- Command: `uv run pytest tests/tck/test_official_tck.py -v --tb=short`
- Duration: 6 minutes 26 seconds
- Environment: GraphForge v0.3.0 (commit c0c669b)
- Full output: `tck_baseline_v0.3.txt` (3,837 tests)

---

**Analysis Status:** COMPLETE
**Next Action:** Review findings with user, decide on plan revision
