# OpenCypher Function to TCK Test Mapping

Mapping of OpenCypher built-in functions to their corresponding TCK test scenarios.

**Source:** TCK inventory in `tck-inventory.md`

---

## Summary

| Function Category | TCK Category | Scenarios | Implementation | Coverage |
|------------------|--------------|-----------|----------------|----------|
| String Functions | expressions/string | 32 | 11/13 (85%) | Good |
| Numeric Functions | expressions/mathematical | 6 | 7/10 (70%) | Minimal |
| List Functions | expressions/list | 94 | 6/8 (75%) | Excellent |
| Aggregation Functions | expressions/aggregation | 27 | 5/10 (50%) | Good |
| Predicate Functions | expressions/boolean, list | ~40 | 0/6 (0%) | N/A |
| Scalar Functions | expressions/graph, typeConversion | 77 | 8/9 (89%) | Good |
| Temporal Functions | expressions/temporal | 89 | 11/11 (100%) | Excellent |
| Spatial Functions | expressions/graph (spatial) | ~10 | 2/2 (100%) | Good |
| Path Functions | expressions/path | 7 | 3/3 (100%) | Minimal |
| **TOTAL** | **9 categories** | **~380** | **53/72 (74%)** | **Good** |

---

## String Functions

### TCK Category
**`expressions/string`** (14 files, 32 scenarios)

### Coverage by Function

| Function | Implementation | TCK Scenarios | Coverage |
|----------|---------------|---------------|----------|
| substring() | ✅ Complete | ~4 | Good |
| trim(), ltrim(), rtrim() | ✅ Complete | ~3 | Good |
| toUpper(), toLower() | ✅ Complete (UPPER/LOWER) | ~3 | Good |
| split() | ✅ Complete | ~2 | Adequate |
| replace() | ✅ Complete | ~2 | Adequate |
| reverse() | ✅ Complete | ~2 | Adequate |
| left(), right() | ✅ Complete | ~2 | Adequate |
| toString() | ✅ Complete | ~5 | Good |
| length() | ❌ Not Implemented | ~1 | N/A |
| toUpper/toLower (camelCase) | ❌ Not Implemented | ~2 | N/A |

**Test Files:**
```
tests/tck/features/official/expressions/string/String1-14.feature
```

**Coverage Assessment:** Good - most string functions have TCK coverage

**Coverage Gaps:**
- length() function tests minimal
- Case sensitivity tests could be expanded
- Unicode handling not extensively tested

---

## Numeric Functions

### TCK Category
**`expressions/mathematical`** (17 files, 6 scenarios)

### Coverage by Function

| Function | Implementation | TCK Scenarios | Coverage |
|----------|---------------|---------------|----------|
| abs() | ✅ Complete | ~1 | Minimal |
| ceil(), floor() | ✅ Complete | ~1 | Minimal |
| round() | ✅ Complete | ~1 | Minimal |
| sign() | ✅ Complete | ~1 | Minimal |
| toInteger(), toFloat() | ✅ Complete | ~2 | Minimal |
| sqrt() | ❌ Not Implemented | 0 | N/A |
| rand() | ❌ Not Implemented | 0 | N/A |
| pow() / ^ | ❌ Not Implemented | 0 | N/A |

**Test Files:**
```
tests/tck/features/official/expressions/mathematical/Mathematical1-17.feature
```

**Coverage Assessment:** Minimal - very few mathematical function tests in TCK

**Notes:**
- Mathematical tests focus primarily on arithmetic operators, not functions
- Most numeric function testing happens implicitly in other scenarios
- TCK could benefit from dedicated math function test suite

---

## List Functions

### TCK Category
**`expressions/list`** (12 files, 94 scenarios)

### Coverage by Function

| Function | Implementation | TCK Scenarios | Coverage |
|----------|---------------|---------------|----------|
| size() | ✅ Complete | ~15 | Excellent |
| head(), last() | ✅ Complete | ~10 | Good |
| tail() | ✅ Complete | ~8 | Good |
| range() | ✅ Complete | ~12 | Good |
| reverse() | ✅ Complete | ~5 | Good |
| extract() | ❌ Not Implemented | ~15 | N/A |
| filter() | ❌ Not Implemented | ~10 | N/A |
| reduce() | ❌ Not Implemented | ~5 | N/A |

**Test Files:**
```
tests/tck/features/official/expressions/list/List1-12.feature
```

**Coverage Assessment:** Excellent - comprehensive list function testing

**Notes:**
- List comprehension syntax tested extensively
- extract(), filter(), reduce() have TCK tests but not implemented
- High priority for implementation (lots of TCK coverage)

---

## Aggregation Functions

### TCK Category
**`expressions/aggregation`** (8 files, 27 scenarios)

### Coverage by Function

| Function | Implementation | TCK Scenarios | Coverage |
|----------|---------------|---------------|----------|
| count() | ✅ Complete | ~8 | Good |
| sum() | ✅ Complete | ~4 | Good |
| avg() | ✅ Complete | ~3 | Good |
| min(), max() | ✅ Complete | ~5 | Good |
| collect() | ✅ Complete | ~4 | Good |
| percentileDisc(), percentileCont() | ❌ Not Implemented | ~2 | N/A |
| stDev(), stDevP() | ❌ Not Implemented | ~1 | N/A |

**Test Files:**
```
tests/tck/features/official/expressions/aggregation/Aggregation1-8.feature
```

**Coverage Assessment:** Good - essential aggregations well tested

**Notes:**
- Basic aggregations have good coverage
- Statistical functions (percentile, stdev) have minimal TCK tests
- DISTINCT in aggregations tested

---

## Predicate Functions

### TCK Categories
**`expressions/boolean`** (5 files, 36 scenarios)
**`expressions/list`** (subset for predicates)

### Coverage by Function

| Function | Implementation | TCK Scenarios | Coverage |
|----------|---------------|---------------|----------|
| all() | ❌ Not Implemented | ~8 | N/A |
| any() | ❌ Not Implemented | ~8 | N/A |
| none() | ❌ Not Implemented | ~4 | N/A |
| single() | ❌ Not Implemented | ~4 | N/A |
| exists() | ❌ Not Implemented (function form) | ~10 | N/A |
| isEmpty() | ❌ Not Implemented | ~2 | N/A |

**Test Files:**
```
tests/tck/features/official/expressions/boolean/Boolean1-5.feature (predicates subset)
tests/tck/features/official/expressions/list/ (predicate subset)
```

**Coverage Assessment:** N/A - none implemented

**Notes:**
- Predicate functions have good TCK coverage
- High priority for implementation
- EXISTS() subquery expression implemented, but not exists() property test function

---

## Scalar Functions

### TCK Categories
**`expressions/graph`** (9 files, 48 scenarios)
**`expressions/typeConversion`** (6 files, 29 scenarios)

### Coverage by Function

| Function | Implementation | TCK Scenarios | Coverage |
|----------|---------------|---------------|----------|
| id() | ✅ Complete | ~8 | Good |
| type() | ✅ Complete | ~6 | Good |
| labels() | ✅ Complete | ~6 | Good |
| properties() | ✅ Complete | ~4 | Good |
| keys() | ✅ Complete | ~4 | Good |
| coalesce() | ✅ Complete | ~8 | Good |
| toBoolean() | ✅ Complete | ~5 | Good |
| timestamp() | ✅ Complete | ~2 | Adequate |
| elementId() | ❌ Not Implemented | 0 | N/A |

**Test Files:**
```
tests/tck/features/official/expressions/graph/Graph1-9.feature
tests/tck/features/official/expressions/typeConversion/TypeConversion1-6.feature
```

**Coverage Assessment:** Good - scalar functions well tested

**Notes:**
- Graph introspection functions have solid coverage
- Type conversion functions well tested
- coalesce() NULL handling extensively tested

---

## Temporal Functions

### TCK Category
**`expressions/temporal`** (10 files, 89 scenarios)

### Coverage by Function

| Function | Implementation | TCK Scenarios | Coverage |
|----------|---------------|---------------|----------|
| date() | ✅ Complete | ~15 | Excellent |
| datetime() | ✅ Complete | ~20 | Excellent |
| time() | ✅ Complete | ~12 | Excellent |
| localtime() | ✅ Complete | ~8 | Good |
| localdatetime() | ✅ Complete | ~10 | Good |
| duration() | ✅ Complete | ~12 | Good |
| Component accessors | ✅ Complete | ~8 | Good |
| truncate() | ✅ Complete | ~4 | Good |

**Test Files:**
```
tests/tck/features/official/expressions/temporal/Temporal1-10.feature
```

**Coverage Assessment:** Excellent - comprehensive temporal testing

**Notes:**
- All temporal types tested with multiple constructors
- Arithmetic with durations tested
- Component accessors (year, month, day, etc.) tested
- Best tested function category in TCK

---

## Spatial Functions

### TCK Category
**`expressions/graph`** (subset for spatial)

### Coverage by Function

| Function | Implementation | TCK Scenarios | Coverage |
|----------|---------------|---------------|----------|
| point() | ✅ Complete | ~6 | Good |
| distance() | ✅ Complete | ~4 | Good |

**Test Files:**
```
tests/tck/features/official/expressions/graph/ (spatial subset)
```

**Coverage Assessment:** Good - adequate spatial function testing

**Notes:**
- Both 2D and 3D points tested
- Cartesian and geographic coordinate systems tested
- Distance calculations tested

---

## Path Functions

### TCK Category
**`expressions/path`** (3 files, 7 scenarios)

### Coverage by Function

| Function | Implementation | TCK Scenarios | Coverage |
|----------|---------------|---------------|----------|
| length() | ✅ Complete | ~3 | Minimal |
| nodes() | ✅ Complete | ~2 | Minimal |
| relationships() | ✅ Complete | ~2 | Minimal |

**Test Files:**
```
tests/tck/features/official/expressions/path/Path1-3.feature
```

**Coverage Assessment:** Minimal - limited path function testing

**Notes:**
- Path functions have minimal TCK coverage
- Tested in context of variable-length paths
- Could benefit from more dedicated path function tests

---

## Coverage Gaps

### Functions with Good TCK Coverage but Not Implemented

1. **Predicate Functions** (~36 TCK scenarios)
   - all(), any(), none(), single()
   - High priority for implementation

2. **List Operations** (~30 TCK scenarios)
   - extract(), filter(), reduce()
   - Medium priority

3. **Statistical Aggregations** (~3 TCK scenarios)
   - percentileDisc(), percentileCont(), stDev(), stDevP()
   - Low TCK coverage but useful functions

### Functions with Minimal/No TCK Coverage

1. **Mathematical Functions** (~2 TCK scenarios)
   - sqrt(), rand(), pow()
   - Minimal TCK tests

2. **Path Functions** (~7 TCK scenarios)
   - length(), nodes(), relationships()
   - Could use more comprehensive tests

3. **String length()** (~1 TCK scenario)
   - Conflicts with path length()
   - Needs context-dependent resolution

---

## TCK Coverage Analysis

### Best Tested Categories

1. **Temporal Functions**: 89 scenarios, excellent coverage
2. **List Functions**: 94 scenarios, excellent coverage
3. **Graph/Scalar Functions**: 77 scenarios, good coverage

### Least Tested Categories

1. **Mathematical Functions**: 6 scenarios, minimal coverage
2. **Path Functions**: 7 scenarios, minimal coverage
3. **String Functions**: 32 scenarios, adequate but could be expanded

### Implementation Priorities Based on TCK Coverage

**High Priority (Good TCK coverage, not implemented):**
1. Predicate functions: all(), any(), none(), single()
2. List operations: extract(), filter(), reduce()

**Medium Priority (Some TCK coverage, commonly used):**
3. sqrt() - mathematical function
4. Statistical aggregations

**Low Priority (Minimal TCK coverage):**
5. rand() - random number generation
6. pow() - power operator

---

## Notes

### TCK Test Distribution

Total function-related TCK scenarios: ~380 out of 1,626 total (23%)

**Distribution:**
- Temporal: 89 (23%)
- List: 94 (25%)
- Graph/Scalar: 77 (20%)
- String: 32 (8%)
- Aggregation: 27 (7%)
- TypeConversion: 29 (8%)
- Boolean/Predicate: 36 (9%)
- Path: 7 (2%)
- Mathematical: 6 (2%)

### Implementation vs TCK Pass Rate

GraphForge currently implements 53/72 functions (74%), but TCK scenarios exist for many unimplemented functions (especially predicates and list operations), lowering overall pass rate.

**Recommendation:** Prioritize implementing functions with existing TCK coverage to maximize TCK pass rate improvement.

---

## References

- TCK Inventory: `docs/reference/tck-inventory.md`
- Function Implementation Status: `docs/reference/implementation-status/functions.md`
- TCK Repository: https://github.com/opencypher/openCypher/tree/master/tck
