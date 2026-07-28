# OpenCypher Functions Reference

Comprehensive reference for all built-in functions in the OpenCypher specification.

## Table of Contents

- [String Functions](#string-functions)
- [Numeric Functions](#numeric-functions)
- [List Functions](#list-functions)
- [Aggregation Functions](#aggregation-functions)
- [Predicate Functions](#predicate-functions)
- [Scalar Functions](#scalar-functions)
- [Temporal Functions](#temporal-functions)
- [Spatial Functions](#spatial-functions)
- [Path Functions](#path-functions)

---

## String Functions

Functions for string manipulation and operations.

### substring()

Extract a substring from a string.

**Signature:** `substring(string, start [, length])`

**Returns:** String

**Parameters:**
- `string` - Source string
- `start` - Zero-based starting index
- `length` (optional) - Number of characters to extract

**Examples:**
```cypher
RETURN substring('hello world', 0, 5) AS result
// Returns: 'hello'

RETURN substring('hello world', 6) AS result
// Returns: 'world'

RETURN substring('hello', 1, 3) AS result
// Returns: 'ell'
```

### trim(), ltrim(), rtrim()

Remove whitespace from strings.

**Signatures:**
- `trim(string)` - Remove leading and trailing whitespace
- `ltrim(string)` - Remove leading whitespace
- `rtrim(string)` - Remove trailing whitespace

**Returns:** String

**Examples:**
```cypher
RETURN trim('  hello  ') AS result
// Returns: 'hello'

RETURN ltrim('  hello  ') AS result
// Returns: 'hello  '

RETURN rtrim('  hello  ') AS result
// Returns: '  hello'
```

### toUpper(), toLower()

Convert string case.

**Signatures:**
- `toUpper(string)` - Convert to uppercase
- `toLower(string)` - Convert to lowercase

**Returns:** String

**Examples:**
```cypher
RETURN toUpper('Hello World') AS result
// Returns: 'HELLO WORLD'

RETURN toLower('Hello World') AS result
// Returns: 'hello world'
```

### split()

Split a string into a list using a delimiter.

**Signature:** `split(string, delimiter)`

**Returns:** List of strings

**Examples:**
```cypher
RETURN split('one,two,three', ',') AS result
// Returns: ['one', 'two', 'three']

RETURN split('hello world', ' ') AS result
// Returns: ['hello', 'world']
```

### replace()

Replace all occurrences of a substring.

**Signature:** `replace(string, search, replacement)`

**Returns:** String

**Example:**
```cypher
RETURN replace('hello world', 'world', 'universe') AS result
// Returns: 'hello universe'
```

### reverse()

Reverse a string.

**Signature:** `reverse(string)`

**Returns:** String

**Example:**
```cypher
RETURN reverse('hello') AS result
// Returns: 'olleh'
```

### left(), right()

Extract characters from the start or end of a string.

**Signatures:**
- `left(string, length)` - Extract from start
- `right(string, length)` - Extract from end

**Returns:** String

**Examples:**
```cypher
RETURN left('hello', 3) AS result
// Returns: 'hel'

RETURN right('hello', 3) AS result
// Returns: 'llo'
```

### toString()

Convert a value to a string.

**Signature:** `toString(value)`

**Returns:** String

**Examples:**
```cypher
RETURN toString(123) AS result
// Returns: '123'

RETURN toString(true) AS result
// Returns: 'true'

RETURN toString(null) AS result
// Returns: null
```

---

## Numeric Functions

Functions for mathematical operations and numeric conversions.

### abs()

Return the absolute value of a number.

**Signature:** `abs(number)`

**Returns:** Number (Integer or Float)

**Examples:**
```cypher
RETURN abs(-5) AS result
// Returns: 5

RETURN abs(3.14) AS result
// Returns: 3.14

RETURN abs(-10.5) AS result
// Returns: 10.5
```

### ceil(), floor()

Round numbers up or down to the nearest integer.

**Signatures:**
- `ceil(number)` - Round up
- `floor(number)` - Round down

**Returns:** Float

**Examples:**
```cypher
RETURN ceil(3.2) AS result
// Returns: 4.0

RETURN floor(3.8) AS result
// Returns: 3.0
```

### round()

Round a number to the nearest integer or specified precision.

**Signature:** `round(number [, precision])`

**Returns:** Float

**Examples:**
```cypher
RETURN round(3.14159) AS result
// Returns: 3.0

RETURN round(3.14159, 2) AS result
// Returns: 3.14

RETURN round(3.5) AS result
// Returns: 4.0
```

### sign()

Return the sign of a number.

**Signature:** `sign(number)`

**Returns:** Integer (-1, 0, or 1)

**Examples:**
```cypher
RETURN sign(-5) AS result
// Returns: -1

RETURN sign(0) AS result
// Returns: 0

RETURN sign(10) AS result
// Returns: 1
```

### sqrt()

Return the square root of a number.

**Signature:** `sqrt(number)`

**Returns:** Float

**Example:**
```cypher
RETURN sqrt(16) AS result
// Returns: 4.0

RETURN sqrt(2) AS result
// Returns: 1.4142135623730951
```

### rand()

Generate a random float between 0 and 1.

**Signature:** `rand()`

**Returns:** Float

**Example:**
```cypher
RETURN rand() AS result
// Returns: 0.7234567891234567 (random)
```

### toInteger(), toFloat()

Convert values to numeric types.

**Signatures:**
- `toInteger(value)` - Convert to integer
- `toFloat(value)` - Convert to float

**Returns:** Integer or Float

**Examples:**
```cypher
RETURN toInteger('42') AS result
// Returns: 42

RETURN toFloat('3.14') AS result
// Returns: 3.14

RETURN toInteger(3.9) AS result
// Returns: 3
```

---

## List Functions

Functions for working with lists.

### size()

Return the number of elements in a list or the length of a string.

**Signature:** `size(list)` or `size(string)`

**Returns:** Integer

**Examples:**
```cypher
RETURN size([1, 2, 3, 4]) AS result
// Returns: 4

RETURN size('hello') AS result
// Returns: 5

RETURN size([]) AS result
// Returns: 0
```

### head(), last()

Return the first or last element of a list.

**Signatures:**
- `head(list)` - First element
- `last(list)` - Last element

**Returns:** Any (type of list elements)

**Examples:**
```cypher
RETURN head([1, 2, 3]) AS result
// Returns: 1

RETURN last([1, 2, 3]) AS result
// Returns: 3

RETURN head([]) AS result
// Returns: null
```

### tail()

Return all elements except the first.

**Signature:** `tail(list)`

**Returns:** List

**Example:**
```cypher
RETURN tail([1, 2, 3, 4]) AS result
// Returns: [2, 3, 4]

RETURN tail([1]) AS result
// Returns: []
```

### range()

Generate a list of integers.

**Signature:** `range(start, end [, step])`

**Returns:** List of integers

**Examples:**
```cypher
RETURN range(0, 10) AS result
// Returns: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

RETURN range(0, 10, 2) AS result
// Returns: [0, 2, 4, 6, 8, 10]

RETURN range(10, 0, -1) AS result
// Returns: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
```

### reverse()

Reverse the order of list elements.

**Signature:** `reverse(list)`

**Returns:** List

**Example:**
```cypher
RETURN reverse([1, 2, 3, 4]) AS result
// Returns: [4, 3, 2, 1]
```

---

## Aggregation Functions

Functions that aggregate values across multiple rows.

### count()

Count the number of values or rows.

**Signature:** `count(expression)` or `count(*)`

**Returns:** Integer

**Examples:**
```cypher
MATCH (n:Person)
RETURN count(n) AS personCount

MATCH (n:Person)
RETURN count(n.age) AS agesCount  // Excludes null values

MATCH (n)
RETURN count(*) AS totalRows
```

### sum()

Calculate the sum of numeric values.

**Signature:** `sum(expression)`

**Returns:** Number (Integer or Float)

**Example:**
```cypher
MATCH (p:Product)
RETURN sum(p.price) AS totalPrice
```

### avg()

Calculate the average of numeric values.

**Signature:** `avg(expression)`

**Returns:** Float

**Example:**
```cypher
MATCH (p:Person)
RETURN avg(p.age) AS averageAge
```

### min(), max()

Return the minimum or maximum value.

**Signatures:**
- `min(expression)` - Minimum value
- `max(expression)` - Maximum value

**Returns:** Same type as expression

**Examples:**
```cypher
MATCH (p:Person)
RETURN min(p.age) AS youngest, max(p.age) AS oldest

MATCH (p:Product)
RETURN min(p.price) AS cheapest, max(p.price) AS mostExpensive
```

### collect()

Collect values into a list.

**Signature:** `collect(expression)`

**Returns:** List

**Examples:**
```cypher
MATCH (p:Person)
RETURN collect(p.name) AS allNames

MATCH (p:Person)
WHERE p.age > 30
RETURN collect(DISTINCT p.city) AS cities
```

### percentileDisc(), percentileCont()

Calculate percentiles.

**Signatures:**
- `percentileDisc(expression, percentile)` - Discrete percentile
- `percentileCont(expression, percentile)` - Continuous percentile

**Returns:** Number

**Examples:**
```cypher
MATCH (p:Person)
RETURN percentileDisc(p.age, 0.5) AS medianAge

MATCH (p:Person)
RETURN percentileCont(p.age, 0.95) AS p95Age
```

### stDev(), stDevP()

Calculate standard deviation.

**Signatures:**
- `stDev(expression)` - Sample standard deviation
- `stDevP(expression)` - Population standard deviation

**Returns:** Float

**Examples:**
```cypher
MATCH (p:Person)
RETURN stDev(p.age) AS ageStdDev

MATCH (p:Product)
RETURN stDevP(p.price) AS priceStdDevP
```

---

## Predicate Functions

Functions that return boolean values for testing conditions.

### all()

Test if a predicate holds for all elements in a list.

**Signature:** `all(variable IN list WHERE predicate)`

**Returns:** Boolean

**Example:**
```cypher
RETURN all(x IN [2, 4, 6, 8] WHERE x % 2 = 0) AS result
// Returns: true

RETURN all(x IN [1, 2, 3] WHERE x > 2) AS result
// Returns: false
```

### any()

Test if a predicate holds for at least one element.

**Signature:** `any(variable IN list WHERE predicate)`

**Returns:** Boolean

**Example:**
```cypher
RETURN any(x IN [1, 2, 3, 4] WHERE x > 3) AS result
// Returns: true

RETURN any(x IN [1, 2, 3] WHERE x > 10) AS result
// Returns: false
```

### none()

Test if a predicate holds for no elements.

**Signature:** `none(variable IN list WHERE predicate)`

**Returns:** Boolean

**Example:**
```cypher
RETURN none(x IN [1, 2, 3] WHERE x > 10) AS result
// Returns: true
```

### single()

Test if a predicate holds for exactly one element.

**Signature:** `single(variable IN list WHERE predicate)`

**Returns:** Boolean

**Example:**
```cypher
RETURN single(x IN [1, 2, 3, 4] WHERE x = 3) AS result
// Returns: true

RETURN single(x IN [1, 2, 3, 4] WHERE x > 2) AS result
// Returns: false (matches 3 and 4)
```

### exists()

Test if a pattern exists or if a property exists.

**Signature:** `exists(pattern)` or `exists(property)`

**Returns:** Boolean

**Examples:**
```cypher
MATCH (p:Person)
WHERE exists(p.email)
RETURN p.name

MATCH (p:Person)
WHERE exists((p)-[:KNOWS]->(:Person))
RETURN p.name
```

### isEmpty()

Test if a list or string is empty.

**Signature:** `isEmpty(list)` or `isEmpty(string)`

**Returns:** Boolean

**Examples:**
```cypher
RETURN isEmpty([]) AS result
// Returns: true

RETURN isEmpty('') AS result
// Returns: true

RETURN isEmpty([1, 2]) AS result
// Returns: false
```

---

## Scalar Functions

Functions that return scalar values related to graph elements.

### id()

Return the internal ID of a node or relationship.

**Signature:** `id(element)`

**Returns:** Integer

**Example:**
```cypher
MATCH (n:Person)
RETURN id(n) AS nodeId, n.name
```

### type() (Scalar Function)

Return the type of a relationship or the type of a value.

**Signature:** `type(relationship)` or `type(value)`

**Returns:** String

**Examples:**
```cypher
MATCH (a)-[r]->(b)
RETURN type(r) AS relType

RETURN type(123) AS result
// Returns: 'INTEGER'
```

### labels()

Return the list of labels on a node.

**Signature:** `labels(node)`

**Returns:** List of strings

**Example:**
```cypher
MATCH (n)
RETURN labels(n) AS nodeLabels
```

### properties()

Return the property map of a node or relationship.

**Signature:** `properties(element)`

**Returns:** Map

**Example:**
```cypher
MATCH (n:Person)
RETURN properties(n) AS props
```

### keys()

Return the list of property keys for a node, relationship, or map.

**Signature:** `keys(element)`

**Returns:** List of strings

**Examples:**
```cypher
MATCH (n:Person)
RETURN keys(n) AS propertyKeys

RETURN keys({name: 'Alice', age: 30}) AS result
// Returns: ['name', 'age']
```

### coalesce()

Return the first non-null value from a list of expressions.

**Signature:** `coalesce(expression1, expression2, ...)`

**Returns:** Any (type of first non-null expression)

**Examples:**
```cypher
RETURN coalesce(null, 'default') AS result
// Returns: 'default'

MATCH (p:Person)
RETURN p.name, coalesce(p.nickname, p.name) AS displayName
```

### toBoolean()

Convert a value to a boolean.

**Signature:** `toBoolean(value)`

**Returns:** Boolean

**Examples:**
```cypher
RETURN toBoolean('true') AS result
// Returns: true

RETURN toBoolean('false') AS result
// Returns: false

RETURN toBoolean(1) AS result
// Returns: true

RETURN toBoolean(0) AS result
// Returns: false
```

### timestamp()

Return the current time in milliseconds since Unix epoch.

**Signature:** `timestamp()`

**Returns:** Integer

**Example:**
```cypher
RETURN timestamp() AS currentTime
// Returns: 1640000000000 (current time)
```

---

## Temporal Functions

Functions for working with dates, times, and durations.

### date()

Create or parse a date.

**Signatures:**
- `date()` - Current date
- `date(string)` - Parse date string
- `date({year, month, day})` - Construct from components

**Returns:** Date

**Examples:**
```cypher
RETURN date() AS today

RETURN date('2023-12-25') AS christmas

RETURN date({year: 2023, month: 12, day: 25}) AS christmas
```

### datetime()

Create or parse a datetime.

**Signatures:**
- `datetime()` - Current datetime
- `datetime(string)` - Parse datetime string
- `datetime({year, month, day, hour, minute, second})` - Construct from components

**Returns:** DateTime

**Examples:**
```cypher
RETURN datetime() AS now

RETURN datetime('2023-12-25T10:30:00') AS christmasMorning

RETURN datetime({year: 2023, month: 12, day: 25, hour: 10}) AS result
```

### time()

Create or parse a time with timezone.

**Signatures:**
- `time()` - Current time
- `time(string)` - Parse time string

**Returns:** Time

**Examples:**
```cypher
RETURN time() AS now

RETURN time('10:30:00+01:00') AS morning
```

### localtime()

Create or parse a time without timezone.

**Signatures:**
- `localtime()` - Current local time
- `localtime(string)` - Parse time string

**Returns:** LocalTime

**Examples:**
```cypher
RETURN localtime() AS now

RETURN localtime('10:30:00') AS morning
```

### localdatetime()

Create or parse a datetime without timezone.

**Signatures:**
- `localdatetime()` - Current local datetime
- `localdatetime(string)` - Parse datetime string

**Returns:** LocalDateTime

**Examples:**
```cypher
RETURN localdatetime() AS now

RETURN localdatetime('2023-12-25T10:30:00') AS christmasMorning
```

### duration()

Create a duration.

**Signature:** `duration(string)` or `duration({components})`

**Returns:** Duration

**Examples:**
```cypher
RETURN duration('P1Y2M10D') AS period
// 1 year, 2 months, 10 days

RETURN duration({days: 10, hours: 5, minutes: 30}) AS period
```

---

## Spatial Functions

Functions for working with geographic and cartesian coordinates.

### point()

Create a point from coordinates.

**Signature:** `point({x, y [, crs]})` or `point({latitude, longitude [, crs]})`

**Returns:** Point

**Examples:**
```cypher
RETURN point({x: 3.0, y: 4.0}) AS cartesianPoint

RETURN point({latitude: 37.7749, longitude: -122.4194}) AS sanFrancisco

RETURN point({x: 3, y: 4, z: 5, crs: 'cartesian-3d'}) AS point3d
```

### distance()

Calculate the distance between two points.

**Signature:** `distance(point1, point2)`

**Returns:** Float (distance in meters for geographic, units for cartesian)

**Example:**
```cypher
WITH point({latitude: 37.7749, longitude: -122.4194}) AS sf,
     point({latitude: 34.0522, longitude: -118.2437}) AS la
RETURN distance(sf, la) AS distanceInMeters
// Returns: ~559000 (meters)
```

---

## Path Functions

Functions for working with path objects.

### length()

Return the length of a path (number of relationships).

**Signature:** `length(path)`

**Returns:** Integer

**Example:**
```cypher
MATCH path = (a:Person)-[*1..3]-(b:Person)
RETURN length(path) AS pathLength
```

### nodes()

Return all nodes in a path.

**Signature:** `nodes(path)`

**Returns:** List of nodes

**Example:**
```cypher
MATCH path = (a:Person)-[:KNOWS*]-(b:Person)
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN nodes(path) AS nodesInPath
```

### relationships()

Return all relationships in a path.

**Signature:** `relationships(path)`

**Returns:** List of relationships

**Example:**
```cypher
MATCH path = (a:Person)-[:KNOWS*]-(b:Person)
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN relationships(path) AS relsInPath
```

---

## NULL Handling

Most functions follow these NULL handling rules:

1. **Propagation**: If any argument is NULL, the result is NULL
   ```cypher
   RETURN substring(null, 0, 5) AS result
   // Returns: null
   ```

2. **Aggregation exceptions**: Aggregation functions ignore NULL values
   ```cypher
   RETURN sum([1, 2, null, 4]) AS result
   // Returns: 7 (null ignored)
   ```

3. **coalesce() exception**: Returns first non-null value
   ```cypher
   RETURN coalesce(null, null, 'default') AS result
   // Returns: 'default'
   ```

4. **count() exception**: `count(*)` includes nulls, `count(expression)` excludes them
   ```cypher
   RETURN count(*) AS total, count(null) AS nonNullCount
   // Returns: total=1, nonNullCount=0
   ```

---

## References

- [OpenCypher Specification](https://opencypher.org/resources/)
- [Neo4j Cypher Manual - Functions](https://neo4j.com/docs/cypher-manual/current/functions/)
- [GQL Standard (ISO/IEC 39075)](https://www.iso.org/standard/76120.html)
