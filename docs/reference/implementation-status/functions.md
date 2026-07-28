# OpenCypher Function Implementation Status

Implementation status of OpenCypher built-in functions in GraphForge.

**Last Updated:** v0.3.8
**Legend:**
- ✅ **COMPLETE**: Fully implemented with comprehensive test coverage
- ⚠️ **PARTIAL**: Basic implementation, missing edge cases or variants
- ❌ **NOT_IMPLEMENTED**: Function not yet implemented

---

## Summary Statistics

| Category | Total Functions | Complete | Partial | Not Implemented |
|----------|----------------|----------|---------|-----------------|
| String | 13 | 13 (100%) | 0 (0%) | 0 (0%) |
| Numeric | 19 | 19 (100%) | 0 (0%) | 0 (0%) |
| List | 8 | 8 (100%) | 0 (0%) | 0 (0%) |
| Aggregation | 10 | 10 (100%) | 0 (0%) | 0 (0%) |
| Predicate | 6 | 6 (100%) | 0 (0%) | 0 (0%) |
| Scalar | 11 | 10 (91%) | 0 (0%) | 1 (9%) |
| Temporal | 11 | 11 (100%) | 0 (0%) | 0 (0%) |
| Spatial | 2 | 2 (100%) | 0 (0%) | 0 (0%) |
| Path | 3 | 3 (100%) | 0 (0%) | 0 (0%) |
| **TOTAL** | **83** | **82 (99%)** | **0 (0%)** | **1 (1%)** |

---

## String Functions

### substring() ✅
**Signature:** `substring(string, start [, length])`
**File:** `evaluator.py`

### trim(), ltrim(), rtrim() ✅
**Signatures:** `trim(string)`, `ltrim(string)`, `rtrim(string)`
**File:** `evaluator.py`

### toUpper(), toLower() ✅
**Signatures:** `toUpper(string)` / `upper(string)`, `toLower(string)` / `lower(string)`
**File:** `evaluator.py`
**Notes:** Both camelCase and legacy UPPER/LOWER aliases supported.

### split() ✅
**Signature:** `split(string, delimiter)`
**File:** `evaluator.py`

### replace() ✅
**Signature:** `replace(string, search, replacement)`
**File:** `evaluator.py`

### reverse() ✅
**Signature:** `reverse(string)` — also works on lists
**File:** `evaluator.py`

### left(), right() ✅
**Signatures:** `left(string, length)`, `right(string, length)`
**File:** `evaluator.py`

### toString() ✅
**Signature:** `toString(value)`
**File:** `evaluator.py`

### length() ✅
**Signature:** `length(path)` — returns relationship count; use `size()` for strings/lists
**File:** `evaluator.py`

---

## Numeric Functions

### abs() ✅
**Signature:** `abs(number)`

### ceil(), floor() ✅
**Signatures:** `ceil(number)`, `floor(number)`

### round() ✅
**Signature:** `round(number [, precision])`

### sign() ✅
**Signature:** `sign(number)`

### sqrt() ✅
**Signature:** `sqrt(number)` — returns NULL for negative input

### rand() ✅
**Signature:** `rand()` — returns random float in [0.0, 1.0)

### pow() ✅
**Signature:** `pow(base, exponent)`

### e() ✅
**Signature:** `e()` — returns Euler's number (2.718...)

### pi() ✅
**Signature:** `pi()` — returns π (3.141...)

### exp() ✅
**Signature:** `exp(number)` — e^number

### log() ✅
**Signature:** `log(number)` — natural logarithm

### log10() ✅
**Signature:** `log10(number)` — base-10 logarithm

### sin(), cos(), tan(), cot() ✅
**Signatures:** `sin(radians)`, `cos(radians)`, `tan(radians)`, `cot(radians)`

### asin(), acos(), atan(), atan2() ✅
**Signatures:** `asin(x)`, `acos(x)`, `atan(x)`, `atan2(y, x)`

### degrees(), radians() ✅
**Signatures:** `degrees(radians)`, `radians(degrees)`

### toInteger(), toFloat() ✅
**Signatures:** `toInteger(value)`, `toFloat(value)`
**File:** `evaluator.py`

---

## List Functions

### size() ✅
**Signature:** `size(list)` or `size(string)`

### head(), last() ✅
**Signatures:** `head(list)`, `last(list)`

### tail() ✅
**Signature:** `tail(list)` — returns all elements except the first

### range() ✅
**Signature:** `range(start, end [, step])`

### reverse() ✅
**Signature:** `reverse(list)` — also works on strings

### extract() ✅
**Signature:** `extract(variable IN list | expression)`
**File:** `evaluator.py`
**Notes:** Implemented as a dedicated grammar rule and AST node (not a function call).

### filter() ✅
**Signature:** `filter(variable IN list WHERE predicate)`
**File:** `evaluator.py`
**Notes:** Implemented as a dedicated grammar rule and AST node.

### reduce() ✅
**Signature:** `reduce(accumulator = initial, variable IN list | expression)`
**File:** `evaluator.py`
**Notes:** Implemented as a dedicated grammar rule and AST node.

---

## Aggregation Functions

### count() ✅
**Signature:** `count(expression)` or `count(*)`
**File:** `executor.py`

### sum() ✅
**Signature:** `sum(expression)`
**File:** `executor.py`

### avg() ✅
**Signature:** `avg(expression)`
**File:** `executor.py`

### min(), max() ✅
**Signatures:** `min(expression)`, `max(expression)`
**File:** `executor.py`
**Notes:** `min()` over mixed NULL/non-NULL values has a known edge case.

### collect() ✅
**Signature:** `collect(expression)`
**File:** `executor.py`

### percentileDisc(), percentileCont() ✅
**Signatures:** `percentileDisc(expression, percentile)`, `percentileCont(expression, percentile)`
**File:** `executor.py`
**Notes:** Percentile must be between 0.0 and 1.0. NULL values ignored.

### stDev(), stDevP() ✅
**Signatures:** `stDev(expression)`, `stDevP(expression)`
**File:** `executor.py`
**Notes:** Sample and population standard deviation. `stDev` returns NULL for a single value; `stDevP` returns 0.

---

## Predicate Functions

### all() ✅
**Signature:** `all(variable IN list WHERE predicate)`
**Notes:** Three-valued NULL logic.

### any() ✅
**Signature:** `any(variable IN list WHERE predicate)`

### none() ✅
**Signature:** `none(variable IN list WHERE predicate)`

### single() ✅
**Signature:** `single(variable IN list WHERE predicate)`

### exists() ✅
**Signature:** `exists(property)` or `exists(expression)`
**Notes:** Returns FALSE (not NULL) for missing properties.

### isEmpty() ✅
**Signature:** `isEmpty(list)`, `isEmpty(string)`, or `isEmpty(map)`
**Notes:** Returns NULL for NULL input.

---

## Scalar Functions

### id() ✅
**Signature:** `id(node_or_relationship)`
**File:** `evaluator.py`

### elementId() ❌
**Status:** NOT_IMPLEMENTED
**Notes:** GQL-spec alternative to `id()`. (v0.4.0).

### type() ✅
**Signature:** `type(relationship)`
**File:** `evaluator.py`

### labels() ✅
**Signature:** `labels(node)`
**File:** `evaluator.py`
**Notes:** Known edge case: `labels()` on relationships.

### properties() ✅
**Signature:** `properties(node_or_relationship)`
**File:** `evaluator.py`

### keys() ✅
**Signature:** `keys(node_or_relationship_or_map)`
**File:** `evaluator.py`

### startNode(), endNode() ✅
**Signatures:** `startNode(relationship)`, `endNode(relationship)`
**File:** `evaluator.py`

### coalesce() ✅
**Signature:** `coalesce(expr1, expr2, ...)`
**File:** `evaluator.py`

### toBoolean() ✅
**Signature:** `toBoolean(value)`
**File:** `evaluator.py`

### timestamp() ✅
**Signature:** `timestamp()` — returns current epoch milliseconds as integer
**File:** `evaluator.py`

---

## Temporal Functions

All temporal functions are ✅ COMPLETE with comprehensive support.

### date() ✅
**Signature:** `date()`, `date(string)`, `date({components})`

### datetime() ✅
**Signature:** `datetime()`, `datetime(string)`, `datetime({components})`, `datetime({epochMillis: n})`

### time() ✅
**Signature:** `time()`, `time(string)`, `time({components})`

### localtime() ✅
**Signature:** `localtime()`, `localtime(string)`, `localtime({components})`

### localdatetime() ✅
**Signature:** `localdatetime()`, `localdatetime(string)`, `localdatetime({components})`

### duration() ✅
**Signature:** `duration(string)`, `duration({components})`

### Temporal component accessors ✅
`year()`, `month()`, `day()`, `hour()`, `minute()`, `second()`, `.epochMillis`, `.epochSeconds`

### truncate() ✅
**Signature:** `truncate(unit, temporal [, {timezone}])`

---

## Spatial Functions

### point() ✅
**Signature:** `point({x, y [, crs]})` or `point({latitude, longitude [, crs]})`
**Notes:** Supports 2D/3D, Cartesian and Geographic coordinate systems.

### distance() ✅
**Signature:** `distance(point1, point2)`
**Notes:** Haversine for geographic coordinates; Euclidean for Cartesian.

---

## Path Functions

### length() ✅
**Signature:** `length(path)` — returns relationship count

### nodes() ✅
**Signature:** `nodes(path)` — returns list of nodes
**Notes:** `nodes(null)` handling has a known edge case.

### relationships() ✅
**Signature:** `relationships(path)` — returns list of relationships
**Notes:** `relationships(null)` handling has a known edge case.

---

## Known Gaps and Open Issues

| Function | Issue | Status |
|----------|-------|--------|
| `elementId()` | | Planned for v0.4.0 |
| `min()` over mixed null/non-null values | | v0.3.8 |
| `nodes(null)`, `relationships(null)` | | v0.3.8 |
| `labels()` on relationships | | v0.3.8 |

---

## Version History

- **v0.1.0**: Basic string, numeric, list functions
- **v0.2.0**: Type conversions, aggregations
- **v0.3.0**: Complete temporal and spatial function support; predicate functions; quantifiers
- **v0.3.5**: `sqrt()`, `rand()`, `pow()`, `stDev()`, `stDevP()`, `percentileDisc()`, `percentileCont()`
- **v0.3.6**: `e()`, `pi()`, `exp()`, `log()`, `log10()`, trig functions, `degrees()`, `radians()`, `timestamp()`
- **v0.3.7**: `startNode()`, `endNode()`, `keys()`, `properties()`; `extract()`, `filter()`, `reduce()` as grammar expressions
- **v0.3.8**: Temporal edge cases (compact parsing, `epochMillis`, truncate variants, duration arithmetic)

---

## References

- OpenCypher Specification: https://opencypher.org/resources/
- Evaluator implementation: `src/graphforge/executor/evaluator.py`
- Aggregation implementation: `src/graphforge/executor/executor.py`
