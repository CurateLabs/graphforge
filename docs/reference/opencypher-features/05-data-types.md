# OpenCypher Data Types

This document provides a comprehensive reference for all data types in the OpenCypher specification. The OpenCypher type system defines the values that can be stored in graph properties, used in expressions, and returned from queries.

**OpenCypher Reference**: [https://opencypher.org/](https://opencypher.org/)

**Neo4j Cypher Manual**: [https://neo4j.com/docs/cypher-manual/current/values-and-types/](https://neo4j.com/docs/cypher-manual/current/values-and-types/)

---

## Table of Contents

- [Type System Overview](#type-system-overview)
- [Primitive Types](#primitive-types)
  - [INTEGER](#integer)
  - [FLOAT](#float)
  - [STRING](#string)
  - [BOOLEAN](#boolean)
  - [NULL](#null)
- [Structural Types](#structural-types)
  - [LIST](#list)
  - [MAP](#map)
- [Composite Types](#composite-types)
  - [NODE](#node)
  - [RELATIONSHIP](#relationship)
  - [PATH](#path)
- [Temporal Types](#temporal-types)
  - [DATE](#date)
  - [TIME / ZONED TIME](#time-zoned-time)
  - [LOCAL TIME](#local-time)
  - [DATETIME / ZONED DATETIME](#datetime-zoned-datetime)
  - [LOCAL DATETIME](#local-datetime)
  - [DURATION](#duration)
- [Spatial Types](#spatial-types)
  - [POINT](#point)
- [Type Checking](#type-checking)
- [Type Coercion and Conversion](#type-coercion-and-conversion)
- [Property Types](#property-types)
- [NULL Handling and Semantics](#null-handling-and-semantics)

---

## Type System Overview

OpenCypher uses a **rich, strongly-typed value system** with automatic type inference. When writing Cypher queries, you cannot explicitly declare data types - Cypher automatically infers the type of each value based on its literal syntax or the result of an expression.

### Type Categories

**Property Types** - Can be stored as node/relationship properties:
- Primitive types: INTEGER, FLOAT, STRING, BOOLEAN
- Temporal types: DATE, TIME, DATETIME, DURATION
- Spatial types: POINT
- Homogeneous lists of property types

**Structural Types** - Used in queries and expressions:
- LIST (heterogeneous lists cannot be stored as properties)
- MAP

**Composite Types** - Graph elements:
- NODE
- RELATIONSHIP
- PATH

### Key Principles

1. **NULL is part of every type** - All Cypher types include the `null` value by default
2. **Type inference** - Types are determined automatically from literal syntax and operations
3. **Type safety** - Operations validate type compatibility at runtime
4. **Numeric coercion** - INTEGER automatically converts to FLOAT in mixed numeric operations
5. **NULL propagation** - Operations involving `null` typically return `null`

---

## Primitive Types

### INTEGER

**Description**: 64-bit signed integer values.

**Synonyms**: `INT`, `SIGNED INTEGER`

**Range**: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 (equivalent to Java `Long.MIN_VALUE` to `Long.MAX_VALUE`)

**Precision**: Exact representation of whole numbers within the 64-bit range.

**Literal Syntax**:
```cypher
// Decimal integers
42
-17
0

// No other literal formats (no hex, octal, binary in standard openCypher)
```

**Properties**:
- Can be stored as node/relationship properties
- Automatically converts to FLOAT in mixed numeric operations
- Arithmetic overflow behavior is implementation-dependent

**Examples**:

*Basic integer literals*:
```cypher
RETURN 42 AS answer
RETURN -1000 AS negative
RETURN 0 AS zero
```

*Integer arithmetic*:
```cypher
RETURN 10 + 5 AS sum        // 15
RETURN 10 * 3 AS product    // 30
RETURN 10 / 3 AS quotient   // 3 (integer division)
RETURN 10 % 3 AS remainder  // 1
```

*Integer in properties*:
```cypher
CREATE (p:Person {age: 30, followers: 1500})
RETURN p.age, p.followers
```

**NULL Handling**: Arithmetic operations with `null` return `null`:
```cypher
RETURN 5 + null  // null
RETURN null * 10 // null
```

---

### FLOAT

**Description**: 64-bit IEEE 754 double-precision floating-point values.

**Synonyms**: `FLOAT64`, `DOUBLE`

**Range**: Approximately ±1.7E+308 (equivalent to Java `Double.MIN_VALUE` to `Double.MAX_VALUE`)

**Precision**: ~15-17 decimal digits of precision. Floating-point arithmetic may introduce rounding errors.

**Literal Syntax**:
```cypher
// Decimal notation
3.14
-0.5
0.0

// Scientific notation
1.5e10    // 15,000,000,000
2.5e-3    // 0.0025
-3.7E+2   // -370.0
```

**Properties**:
- Can be stored as node/relationship properties
- Supports special values: `Infinity`, `-Infinity`, `NaN`
- Integer values automatically promote to FLOAT in mixed operations

**Examples**:

*Basic float literals*:
```cypher
RETURN 3.14159 AS pi
RETURN 2.5e-3 AS scientific
RETURN 1.0 / 3.0 AS fraction  // 0.3333...
```

*Mixed numeric operations (automatic conversion)*:
```cypher
RETURN 10 + 2.5 AS result   // 12.5 (INTEGER + FLOAT = FLOAT)
RETURN 5 * 1.5 AS product   // 7.5
RETURN 10 / 4.0 AS division // 2.5 (not integer division)
```

*Float in properties*:
```cypher
CREATE (p:Product {price: 19.99, rating: 4.7})
RETURN p.price, p.rating
```

**NULL Handling**: Operations with `null` return `null`:
```cypher
RETURN 3.14 * null  // null
```

---

### STRING

**Description**: Unicode text values of arbitrary length.

**Synonyms**: `VARCHAR`

**Size Limits**: Implementation-dependent; generally no practical limit in most systems.

**Character Encoding**: UTF-8 Unicode.

**Literal Syntax**:
```cypher
// Single quotes
'Hello, World!'
'It\'s escaped'

// Double quotes
"Hello, World!"
"Quote: \"Hello\""

// Escape sequences
'Line 1\nLine 2'        // Newline
'Tab\tseparated'        // Tab
'Unicode: \u0041'       // Unicode escape (A)
'Backslash: \\'         // Escaped backslash
```

**Properties**:
- Can be stored as node/relationship properties
- Case-sensitive by default
- Immutable values

**Examples**:

*Basic string literals*:
```cypher
RETURN 'Alice' AS name
RETURN "Hello, World!" AS greeting
RETURN 'Multi\nLine\nString' AS multiline
```

*String concatenation*:
```cypher
RETURN 'Hello' + ' ' + 'World' AS greeting  // 'Hello World'
```

*String in properties*:
```cypher
CREATE (p:Person {name: 'Alice', email: 'alice@example.com'})
RETURN p.name, p.email
```

*String functions*:
```cypher
RETURN toUpper('hello') AS upper      // 'HELLO'
RETURN toLower('WORLD') AS lower      // 'world'
RETURN trim('  spaces  ') AS trimmed  // 'spaces'
RETURN substring('Hello', 0, 4)       // 'Hell'
```

**NULL Handling**: String operations with `null` return `null`:
```cypher
RETURN 'Hello' + null  // null
```

---

### BOOLEAN

**Description**: Logical true/false values.

**Synonyms**: `BOOL`

**Values**: `true`, `false` (case-insensitive)

**Literal Syntax**:
```cypher
true
false
TRUE    // equivalent to true
FALSE   // equivalent to false
```

**Properties**:
- Can be stored as node/relationship properties
- Used in WHERE clauses and conditional expressions
- Supports three-valued logic with NULL

**Examples**:

*Basic boolean literals*:
```cypher
RETURN true AS yes
RETURN false AS no
```

*Boolean operators*:
```cypher
RETURN true AND false   // false
RETURN true OR false    // true
RETURN NOT true         // false
RETURN true XOR false   // true
```

*Boolean in properties*:
```cypher
CREATE (p:Person {name: 'Alice', active: true, verified: false})
RETURN p.name, p.active, p.verified
```

*Comparison operators (return BOOLEAN)*:
```cypher
RETURN 5 > 3            // true
RETURN 'a' = 'b'        // false
RETURN 10 >= 10         // true
```

**NULL Handling**: Three-valued logic applies:
```cypher
RETURN true AND null    // null (unknown)
RETURN false AND null   // false (definite)
RETURN true OR null     // true (definite)
RETURN false OR null    // null (unknown)
RETURN NOT null         // null
```

---

### NULL

**Description**: Represents a missing, unknown, or undefined value.

**Synonyms**: `NULL` (case-insensitive)

**Characteristics**:
- Part of every Cypher type
- `null` is NOT equal to `null` (unknown values cannot be assumed identical)
- Propagates through most operations

**Literal Syntax**:
```cypher
null
NULL    // equivalent
```

**Properties**:
- Can be stored as node/relationship properties (represents absence of value)
- Special comparison operators: `IS NULL`, `IS NOT NULL`
- Cannot use `=` or `<>` to test for NULL

**Examples**:

*NULL literal*:
```cypher
RETURN null AS nothing
```

*Testing for NULL*:
```cypher
RETURN null IS NULL         // true
RETURN 42 IS NULL           // false
RETURN null IS NOT NULL     // false
RETURN 'hello' IS NOT NULL  // true
```

*NULL propagation in arithmetic*:
```cypher
RETURN 5 + null     // null
RETURN null * 10    // null
RETURN null / 3     // null
```

*NULL propagation in comparisons*:
```cypher
RETURN null = null      // null (not true!)
RETURN null <> null     // null
RETURN 5 > null         // null
RETURN null < 10        // null
```

*NULL in properties*:
```cypher
CREATE (p:Person {name: 'Alice', age: null})  // age is explicitly null
MATCH (p:Person {name: 'Alice'})
RETURN p.age                                   // null
RETURN p.nonExistentProperty                   // null
```

**NULL Handling**: See dedicated [NULL Handling and Semantics](#null-handling-and-semantics) section below.

---

## Structural Types

### LIST

**Description**: Ordered collection of zero or more values. Lists can contain any type, including other lists (nested collections).

**Characteristics**:
- Zero-indexed (first element is at index 0)
- Heterogeneous: can mix different types in the same list
- Homogeneous lists can be stored as properties; heterogeneous lists cannot
- Supports nested lists (lists of lists)

**Literal Syntax**:
```cypher
// Empty list
[]

// Homogeneous list
[1, 2, 3, 4, 5]
['a', 'b', 'c']
[true, false, true]

// Heterogeneous list (cannot be stored as property)
[1, 'hello', 3.14, null, true]

// Nested list
[[1, 2], [3, 4], [5, 6]]
[1, [2, 3], [[4, 5], 6]]
```

**Accessing Elements**:
```cypher
// Zero-based indexing
list[0]         // First element
list[2]         // Third element
list[-1]        // Last element
list[-2]        // Second-to-last element

// Slicing (start inclusive, end exclusive)
list[0..3]      // Elements at indices 0, 1, 2
list[1..-1]     // All but first and last
list[2..]       // From index 2 to end
list[..3]       // From start to index 2 (inclusive)
```

**Properties**:
- **Homogeneous lists** can be stored as node/relationship properties
- **Heterogeneous lists** raise errors if stored as properties
- Out-of-bounds single element access returns `null`
- Out-of-bounds slice access returns truncated results

**Examples**:

*Basic list literals*:
```cypher
RETURN [1, 2, 3, 4, 5] AS numbers
RETURN ['Alice', 'Bob', 'Carol'] AS names
RETURN [1, 'hello', null, true] AS mixed
```

*List indexing*:
```cypher
WITH [10, 20, 30, 40, 50] AS list
RETURN list[0] AS first,      // 10
       list[2] AS third,      // 30
       list[-1] AS last,      // 50
       list[10] AS outOfBounds // null
```

*List slicing*:
```cypher
WITH ['a', 'b', 'c', 'd', 'e'] AS list
RETURN list[1..3] AS slice1,   // ['b', 'c']
       list[2..] AS slice2,    // ['c', 'd', 'e']
       list[..-1] AS slice3    // ['a', 'b', 'c', 'd']
```

*Nested lists*:
```cypher
WITH [[1, 2], [3, 4], [5, 6]] AS matrix
RETURN matrix[1] AS row,       // [3, 4]
       matrix[1][0] AS element // 3
```

*List in properties (homogeneous only)*:
```cypher
CREATE (p:Person {name: 'Alice', tags: ['developer', 'python', 'graph']})
RETURN p.tags

// This FAILS - heterogeneous list
CREATE (p:Person {data: [1, 'hello', true]})  // ERROR
```

*List functions*:
```cypher
RETURN size([1, 2, 3])              // 3
RETURN head([1, 2, 3])              // 1
RETURN tail([1, 2, 3])              // [2, 3]
RETURN [x IN [1, 2, 3] | x * 2]     // [2, 4, 6] (list comprehension)
```

**NULL Handling**:
```cypher
RETURN [1, null, 3] AS listWithNull     // [1, null, 3]
RETURN null IN [1, 2, null, 4]          // null (cannot determine)
RETURN 2 IN [1, null, 3]                // null (cannot determine - might be in null)
RETURN 2 IN [1, 2, 3]                   // true
RETURN 5 IN [1, 2, 3]                   // false
```

---

### MAP

**Description**: Unordered collection of key-value pairs. Keys are strings; values can be any type including nested maps and lists.

**Characteristics**:
- Keys must be string literals (cannot be expressions)
- Values can be any Cypher type
- Cannot be stored as properties directly (but nodes/relationships are map-like)
- Supports nested maps and lists as values

**Literal Syntax**:
```cypher
// Empty map
{}

// Simple map
{name: 'Alice', age: 30}

// Map with various value types
{
  name: 'Alice',
  age: 30,
  active: true,
  tags: ['dev', 'python'],
  metadata: {created: date('2024-01-01'), version: 2}
}

// Keys must be literals (quoted or unquoted identifiers)
{name: 'Alice'}           // Valid
{'name': 'Alice'}         // Valid
{"name": 'Alice'}         // Valid
```

**Accessing Values**:
```cypher
// Dot notation
map.name
map.nested.property

// Bracket notation
map['name']
map['property-with-dashes']
```

**Properties**:
- Cannot be stored directly as properties
- Nodes and relationships behave like maps for property access
- Returned through APIs as JSON objects or language-specific map types

**Examples**:

*Basic map literals*:
```cypher
RETURN {name: 'Alice', age: 30, city: 'NYC'} AS person
```

*Nested maps*:
```cypher
RETURN {
  user: {
    name: 'Alice',
    email: 'alice@example.com'
  },
  settings: {
    theme: 'dark',
    notifications: true
  }
} AS data
```

*Map with lists*:
```cypher
RETURN {
  name: 'Project Alpha',
  tags: ['important', 'active'],
  members: ['Alice', 'Bob', 'Carol']
} AS project
```

*Property access*:
```cypher
WITH {name: 'Alice', age: 30, city: 'NYC'} AS person
RETURN person.name AS name,            // 'Alice'
       person['age'] AS age,           // 30
       person.country AS missing       // null (non-existent key)
```

*Nested property access*:
```cypher
WITH {user: {name: 'Alice', email: 'alice@example.com'}} AS data
RETURN data.user.name AS name,         // 'Alice'
       data.user['email'] AS email     // 'alice@example.com'
```

*Map projection (selecting node properties)*:
```cypher
MATCH (p:Person {name: 'Alice'})
RETURN p {.name, .age, .city} AS person  // Extract specific properties as map
```

**NULL Handling**:
```cypher
WITH {name: 'Alice', age: null} AS person
RETURN person.age           // null
RETURN person.missing       // null (non-existent key)

WITH null AS map
RETURN map.name             // null (null propagation)
```

---

## Composite Types

### NODE

**Description**: Represents a node (vertex) in the graph. Nodes have an identity, zero or more labels, and a map of properties.

**Characteristics**:
- **Identity**: Internal unique identifier
- **Labels**: Zero or more label names (e.g., `:Person`, `:Company`)
- **Properties**: Map-like collection of key-value pairs

**Accessing Properties**:
```cypher
node.propertyName
node['property-name']
```

**Available Functions**:
- `id(node)` - Returns internal node ID (INTEGER)
- `labels(node)` - Returns list of label names (LIST<STRING>)
- `properties(node)` - Returns map of all properties (MAP)
- `keys(node)` - Returns list of property keys (LIST<STRING>)

**Examples**:

*Matching and returning nodes*:
```cypher
MATCH (p:Person)
RETURN p
```

*Accessing node properties*:
```cypher
MATCH (p:Person {name: 'Alice'})
RETURN p.name AS name,
       p.age AS age,
       id(p) AS nodeId,
       labels(p) AS labels
```

*Node with multiple labels*:
```cypher
CREATE (p:Person:Developer {name: 'Alice', language: 'Python'})
RETURN labels(p)  // ['Person', 'Developer']
```

*Getting all properties*:
```cypher
MATCH (p:Person {name: 'Alice'})
RETURN properties(p)  // {name: 'Alice', age: 30, city: 'NYC'}
```

**NULL Handling**:
```cypher
MATCH (p:Person)
RETURN p.nonExistent  // null (missing property)

WITH null AS node
RETURN node.name      // null (null propagation)
```

---

### RELATIONSHIP

**Description**: Represents a directed edge between two nodes. Relationships have an identity, exactly one type, and a map of properties.

**Characteristics**:
- **Identity**: Internal unique identifier
- **Type**: Exactly one relationship type (e.g., `:KNOWS`, `:WORKS_AT`)
- **Direction**: Always directed (from source to target)
- **Properties**: Map-like collection of key-value pairs

**Accessing Properties**:
```cypher
relationship.propertyName
relationship['property-name']
```

**Available Functions**:
- `id(relationship)` - Returns internal relationship ID (INTEGER)
- `type(relationship)` - Returns relationship type name (STRING)
- `properties(relationship)` - Returns map of all properties (MAP)
- `keys(relationship)` - Returns list of property keys (LIST<STRING>)
- `startNode(relationship)` - Returns source node
- `endNode(relationship)` - Returns target node

**Examples**:

*Matching and returning relationships*:
```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person)
RETURN r
```

*Accessing relationship properties*:
```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person)
RETURN type(r) AS relType,
       r.since AS since,
       id(r) AS relId,
       properties(r) AS props
```

*Relationship with properties*:
```cypher
MATCH (a:Person {name: 'Alice'})
MATCH (b:Person {name: 'Bob'})
CREATE (a)-[r:KNOWS {since: 2020, strength: 0.9}]->(b)
RETURN r.since, r.strength
```

*Getting endpoints*:
```cypher
MATCH (a)-[r:KNOWS]->(b)
RETURN startNode(r) AS source,
       endNode(r) AS target,
       type(r) AS relType
```

**NULL Handling**:
```cypher
MATCH (a)-[r:KNOWS]->(b)
RETURN r.nonExistent  // null (missing property)

WITH null AS rel
RETURN rel.since      // null (null propagation)
```

---

### PATH

**Description**: Represents a sequence of alternating nodes and relationships in the graph. Paths are the result of pattern matching and traversals.

**Characteristics**:
- **Nodes**: Ordered list of nodes in the path
- **Relationships**: Ordered list of relationships connecting the nodes
- **Length**: Number of relationships in the path

**Accessing Components**:
```cypher
nodes(path)          // Returns list of nodes
relationships(path)  // Returns list of relationships
length(path)         // Returns number of relationships (not nodes)
```

**Examples**:

*Capturing a path*:
```cypher
MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person)
RETURN p
```

*Path components*:
```cypher
MATCH p = (a:Person)-[:KNOWS*2]->(b:Person)
RETURN nodes(p) AS pathNodes,
       relationships(p) AS pathRels,
       length(p) AS pathLength  // 2 (number of relationships)
```

*Path with variable-length pattern*:
```cypher
MATCH p = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]-(b:Person {name: 'Bob'}))
RETURN length(p) AS distance,
       [node IN nodes(p) | node.name] AS names
```

*Accessing specific elements in path*:
```cypher
MATCH p = (a)-[:KNOWS*3]->(b)
WITH nodes(p) AS pathNodes, relationships(p) AS pathRels
RETURN pathNodes[0] AS start,
       pathNodes[-1] AS end,
       pathRels[1] AS secondRelationship
```

**NULL Handling**:
```cypher
// If no path matches, variable is not bound
OPTIONAL MATCH p = (a:Person)-[:KNOWS]->(b:NonExistent)
RETURN p  // null if no match
```

---

## Temporal Types

OpenCypher supports a comprehensive set of temporal types for working with dates, times, and durations. These types provide precise time modeling with timezone support.

### DATE

**Description**: Represents a calendar date without time or timezone information.

**Components**: Year, Month, Day

**Literal Syntax**:
```cypher
// ISO 8601 calendar date
date('2024-01-15')

// Constructor function
date({year: 2024, month: 1, day: 15})
```

**Format Patterns**:
- Calendar date: `YYYY-MM-DD` (e.g., `2024-01-15`)
- Week date: `YYYY-Www-D` (e.g., `2024-W03-1` for Monday of week 3)
- Ordinal date: `YYYY-DDD` (e.g., `2024-015` for 15th day of year)

**Constructor Functions**:
```cypher
date()                              // Current date
date('2024-01-15')                  // Parse from string
date({year: 2024, month: 1, day: 15})  // From components
date.truncate('day', datetime())    // Truncate datetime to date
```

**Accessible Properties**:
- `.year` - Year (e.g., 2024)
- `.month` - Month (1-12)
- `.day` - Day of month (1-31)
- `.week` - ISO week number (1-53)
- `.dayOfWeek` - Day of week (1=Monday, 7=Sunday)
- `.dayOfYear` - Day of year (1-366)
- `.quarter` - Quarter (1-4)

**Examples**:

*Creating dates*:
```cypher
RETURN date('2024-01-15') AS specificDate
RETURN date() AS today
RETURN date({year: 2024, month: 1, day: 15}) AS constructedDate
```

*Accessing date components*:
```cypher
WITH date('2024-01-15') AS d
RETURN d.year AS year,           // 2024
       d.month AS month,         // 1
       d.day AS day,             // 15
       d.dayOfWeek AS dow        // 1 (Monday)
```

*Date arithmetic*:
```cypher
WITH date('2024-01-15') AS d
RETURN d + duration({days: 7}) AS nextWeek,  // 2024-01-22
       d - duration({months: 1}) AS lastMonth // 2023-12-15
```

*Date in properties*:
```cypher
CREATE (e:Event {name: 'Conference', date: date('2024-06-15')})
RETURN e.date
```

**NULL Handling**:
```cypher
RETURN date(null)  // null
```

---

### TIME / ZONED TIME

**Description**: Represents a time of day with timezone information (offset or IANA identifier).

**Components**: Hour, Minute, Second, Nanosecond, Timezone

**Literal Syntax**:
```cypher
// With UTC offset
time('12:30:45.123+02:00')

// With IANA timezone name
time('12:30:45.123[Europe/London]')

// Constructor function
time({hour: 12, minute: 30, second: 45, timezone: '+02:00'})
```

**Format Patterns**:
- Full: `HH:MM:SS.sssssssss+ZZ:ZZ` (nanosecond precision)
- Common: `HH:MM:SS+ZZ:ZZ`
- Named timezone: `HH:MM:SS[Area/Location]`

**Constructor Functions**:
```cypher
time()                              // Current time with default timezone
time('12:30:45+02:00')             // Parse from string
time({hour: 12, minute: 30, timezone: '+02:00'})  // From components
```

**Accessible Properties**:
- `.hour` - Hour (0-23)
- `.minute` - Minute (0-59)
- `.second` - Second (0-59)
- `.millisecond` - Millisecond (0-999)
- `.microsecond` - Microsecond (0-999999)
- `.nanosecond` - Nanosecond (0-999999999)
- `.timezone` - Timezone name or offset
- `.offset` - UTC offset as string (e.g., '+02:00')
- `.offsetSeconds` - UTC offset in seconds

**Examples**:

*Creating times*:
```cypher
RETURN time('12:30:45+02:00') AS specificTime
RETURN time() AS currentTime
RETURN time({hour: 14, minute: 30, timezone: '+00:00'}) AS utcTime
```

*Accessing time components*:
```cypher
WITH time('12:30:45.123456789+02:00') AS t
RETURN t.hour AS hour,               // 12
       t.minute AS minute,           // 30
       t.second AS second,           // 45
       t.nanosecond AS nanosecond,  // 123456789
       t.offset AS offset            // '+02:00'
```

**NULL Handling**:
```cypher
RETURN time(null)  // null
```

---

### LOCAL TIME

**Description**: Represents a time of day without timezone information.

**Components**: Hour, Minute, Second, Nanosecond

**Literal Syntax**:
```cypher
// No timezone offset
localtime('12:30:45.123')

// Constructor function
localtime({hour: 12, minute: 30, second: 45})
```

**Format Patterns**:
- Full: `HH:MM:SS.sssssssss`
- Common: `HH:MM:SS`
- Short: `HH:MM`

**Constructor Functions**:
```cypher
localtime()                           // Current local time
localtime('12:30:45')                 // Parse from string
localtime({hour: 12, minute: 30})     // From components
```

**Accessible Properties**:
- `.hour`, `.minute`, `.second`, `.millisecond`, `.microsecond`, `.nanosecond`

**Examples**:

*Creating local times*:
```cypher
RETURN localtime('12:30:45') AS specificTime
RETURN localtime() AS currentLocalTime
RETURN localtime({hour: 9, minute: 0}) AS morning
```

*Use case: Comparing times across timezones*:
```cypher
// Local time allows comparison without timezone concerns
WITH localtime('09:00:00') AS openingTime
MATCH (store:Store)
WHERE store.opensAt = openingTime
RETURN store.name
```

**NULL Handling**:
```cypher
RETURN localtime(null)  // null
```

---

### DATETIME / ZONED DATETIME

**Description**: Represents a specific instant in time with date, time, and timezone information.

**Components**: Year, Month, Day, Hour, Minute, Second, Nanosecond, Timezone

**Literal Syntax**:
```cypher
// With UTC offset
datetime('2024-01-15T12:30:45.123+02:00')

// With IANA timezone
datetime('2024-01-15T12:30:45.123[Europe/London]')

// Constructor function
datetime({year: 2024, month: 1, day: 15, hour: 12, minute: 30, timezone: '+02:00'})
```

**Format Patterns**:
- Full: `YYYY-MM-DDTHH:MM:SS.sssssssss+ZZ:ZZ`
- Common: `YYYY-MM-DDTHH:MM:SS+ZZ:ZZ`
- Named timezone: `YYYY-MM-DDTHH:MM:SS[Area/Location]`

**Constructor Functions**:
```cypher
datetime()                                // Current datetime
datetime('2024-01-15T12:30:45+02:00')    // Parse from string
datetime({year: 2024, month: 1, day: 15, hour: 12, timezone: 'Europe/London'})  // From components
datetime({epochMillis: 1705324245000})   // From Unix timestamp
```

**Accessible Properties**:
- All DATE properties: `.year`, `.month`, `.day`, `.week`, `.dayOfWeek`, etc.
- All TIME properties: `.hour`, `.minute`, `.second`, `.nanosecond`, etc.
- `.timezone`, `.offset`, `.offsetSeconds`
- `.epochSeconds` - Unix timestamp in seconds
- `.epochMillis` - Unix timestamp in milliseconds

**Examples**:

*Creating datetimes*:
```cypher
RETURN datetime('2024-01-15T12:30:45+02:00') AS specificDateTime
RETURN datetime() AS now
RETURN datetime({year: 2024, month: 6, day: 15, hour: 14, minute: 30, timezone: 'UTC'}) AS utcTime
```

*Accessing datetime components*:
```cypher
WITH datetime('2024-01-15T12:30:45.123+02:00') AS dt
RETURN dt.year AS year,              // 2024
       dt.month AS month,            // 1
       dt.day AS day,                // 15
       dt.hour AS hour,              // 12
       dt.minute AS minute,          // 30
       dt.epochSeconds AS timestamp  // Unix timestamp
```

*Datetime arithmetic*:
```cypher
WITH datetime('2024-01-15T12:30:45+02:00') AS dt
RETURN dt + duration({hours: 3}) AS later,
       dt - duration({days: 7}) AS weekAgo
```

*Timezone handling*:
```cypher
// Internally stored as UTC, timezone applied for presentation
WITH datetime('2024-01-15T12:30:45+02:00') AS dt
RETURN dt.epochSeconds  // Same UTC instant regardless of timezone presentation
```

**NULL Handling**:
```cypher
RETURN datetime(null)  // null
```

---

### LOCAL DATETIME

**Description**: Represents a date and time without timezone information.

**Components**: Year, Month, Day, Hour, Minute, Second, Nanosecond

**Literal Syntax**:
```cypher
// No timezone
localdatetime('2024-01-15T12:30:45')

// Constructor function
localdatetime({year: 2024, month: 1, day: 15, hour: 12, minute: 30})
```

**Format Patterns**:
- Full: `YYYY-MM-DDTHH:MM:SS.sssssssss`
- Common: `YYYY-MM-DDTHH:MM:SS`

**Constructor Functions**:
```cypher
localdatetime()                           // Current local datetime
localdatetime('2024-01-15T12:30:45')     // Parse from string
localdatetime({year: 2024, month: 1, day: 15, hour: 12})  // From components
```

**Accessible Properties**:
- All DATE properties: `.year`, `.month`, `.day`, etc.
- All LOCAL TIME properties: `.hour`, `.minute`, `.second`, etc.

**Examples**:

*Creating local datetimes*:
```cypher
RETURN localdatetime('2024-01-15T12:30:45') AS specificLocalDateTime
RETURN localdatetime() AS now
RETURN localdatetime({year: 2024, month: 6, day: 15, hour: 9, minute: 0}) AS event
```

*Use case: Scheduled events regardless of timezone*:
```cypher
// Local datetime is useful for recurring events that should occur at the same "wall clock" time
CREATE (meeting:Meeting {
  name: 'Daily Standup',
  scheduledAt: localdatetime('2024-01-15T09:00:00')
})
```

**NULL Handling**:
```cypher
RETURN localdatetime(null)  // null
```

---

### DURATION

**Description**: Represents a temporal amount or difference between two instants. Unlike other temporal types, durations do not represent specific points in time.

**Components**: Years, Months, Weeks, Days, Hours, Minutes, Seconds, Nanoseconds

**Characteristics**:
- Can be positive or negative
- Used for date/time arithmetic
- Not anchored to a specific instant

**Literal Syntax**:
```cypher
// ISO 8601 duration format: P[nY][nM][nW][nD][T[nH][nM][nS]]
duration('P1Y2M3DT4H5M6.007S')  // 1 year, 2 months, 3 days, 4 hours, 5 minutes, 6.007 seconds

// Component-based constructor
duration({years: 1, months: 2, days: 3, hours: 4, minutes: 5, seconds: 6.007})
```

**Format Patterns**:
- Full ISO 8601: `P[nY][nM][nW][nD][T[nH][nM][nS]]`
- Date only: `P3Y6M4D` (3 years, 6 months, 4 days)
- Time only: `PT12H30M5S` (12 hours, 30 minutes, 5 seconds)
- Negative: `-P1D` (negative 1 day)

**Constructor Functions**:
```cypher
duration('P1Y2M3D')                        // Parse from ISO 8601 string
duration({years: 1, months: 2, days: 3})   // From components
duration.between(date1, date2)             // Calculate duration between two temporal values
```

**Accessible Properties**:

*First-order components* (as specified):
- `.years`, `.months`, `.weeks`, `.days`
- `.hours`, `.minutes`, `.seconds`, `.nanoseconds`

*Second-order components* (bounded):
- `.monthsOfYear` - Months component (0-11)
- `.daysOfWeek` - Days component modulo 7 (0-6)
- `.minutesOfHour` - Minutes component (0-59)
- `.secondsOfMinute` - Seconds component (0-59)

**Examples**:

*Creating durations*:
```cypher
RETURN duration('P1Y2M3D') AS period
RETURN duration('PT12H30M') AS timespan
RETURN duration({days: 7}) AS oneWeek
RETURN duration({hours: -3}) AS negativeThreeHours
```

*Duration components*:
```cypher
WITH duration('P1Y2M15DT4H30M') AS d
RETURN d.years AS years,             // 1
       d.months AS months,           // 2
       d.days AS days,               // 15
       d.hours AS hours,             // 4
       d.monthsOfYear AS monthsMod   // 2 (same as months in this case)
```

*Calculating duration between instants*:
```cypher
WITH date('2024-01-01') AS start, date('2024-12-31') AS end
RETURN duration.between(start, end) AS yearDuration
```

*Duration arithmetic*:
```cypher
WITH datetime('2024-01-15T12:00:00') AS dt
RETURN dt + duration({days: 7, hours: 3}) AS future,
       dt - duration({months: 1}) AS past
```

*Duration with dates*:
```cypher
WITH date('2024-01-01') AS start
RETURN start + duration('P1Y') AS nextYear,      // 2025-01-01
       start + duration('P1M15D') AS later       // 2024-02-16
```

**NULL Handling**:
```cypher
RETURN duration(null)  // null
RETURN duration.between(null, date())  // null
```

---

## Spatial Types

### POINT

**Description**: Represents a location in 2D or 3D space using either Cartesian (Euclidean) or Geographic (WGS-84) coordinate reference systems.

**Coordinate Reference Systems (CRS)**:

| CRS | Dimensions | Coordinates | SRID | Use Case |
|-----|-----------|-------------|------|----------|
| **Cartesian 2D** | 2D | `x`, `y` | 7203 | Euclidean plane, floor plans |
| **Cartesian 3D** | 3D | `x`, `y`, `z` | 9157 | 3D Euclidean space |
| **WGS-84 2D** | 2D | `longitude`, `latitude` | 4326 | Earth surface locations |
| **WGS-84 3D** | 3D | `longitude`, `latitude`, `height` | 4979 | Earth locations with elevation |

**Characteristics**:
- Each point has exactly one CRS
- Geographic and Cartesian points are incomparable (cannot convert implicitly)
- Coordinates are stored as 64-bit floats
- Geographic constraints: latitude ∈ [-90, 90], longitude wraps to [-180, 180]

**Literal Syntax**:
```cypher
// Cartesian 2D
point({x: 3.0, y: 4.0})

// Cartesian 3D
point({x: 1.0, y: 2.0, z: 3.0})

// Geographic 2D (WGS-84)
point({longitude: -122.4194, latitude: 37.7749})

// Geographic 3D (WGS-84 with height in meters)
point({longitude: -122.4194, latitude: 37.7749, height: 100})
```

**Constructor Functions**:
```cypher
point({x: 3, y: 4})                           // Cartesian 2D
point({x: 3, y: 4, z: 5})                     // Cartesian 3D
point({latitude: 37.7749, longitude: -122.4194})  // WGS-84 2D
point({latitude: 37.7749, longitude: -122.4194, height: 100})  // WGS-84 3D
```

**Accessible Properties**:
- **Cartesian**: `.x`, `.y`, `.z` (3D only)
- **Geographic**: `.latitude`, `.longitude`, `.height` (3D only)
- **All points**: `.crs` (CRS name as string), `.srid` (SRID identifier)

**Examples**:

*Creating points*:
```cypher
// Cartesian
RETURN point({x: 3, y: 4}) AS cartesian2D
RETURN point({x: 1, y: 2, z: 3}) AS cartesian3D

// Geographic
RETURN point({latitude: 37.7749, longitude: -122.4194}) AS sanFrancisco
RETURN point({latitude: 51.5074, longitude: -0.1278, height: 11}) AS londonWithElevation
```

*Accessing point properties*:
```cypher
WITH point({x: 3, y: 4}) AS p
RETURN p.x AS x,           // 3.0
       p.y AS y,           // 4.0
       p.crs AS crs,       // 'cartesian'
       p.srid AS srid      // 7203

WITH point({latitude: 37.7749, longitude: -122.4194}) AS p
RETURN p.latitude AS lat,      // 37.7749
       p.longitude AS lon,     // -122.4194
       p.srid AS srid          // 4326
```

*Point distance calculation*:
```cypher
WITH point({latitude: 37.7749, longitude: -122.4194}) AS sf,
     point({latitude: 34.0522, longitude: -118.2437}) AS la
RETURN distance(sf, la) AS distanceMeters  // ~559,120 meters (Haversine distance)

WITH point({x: 0, y: 0}) AS origin,
     point({x: 3, y: 4}) AS p
RETURN distance(origin, p) AS euclideanDist  // 5.0 (Pythagorean)
```

*Point in properties*:
```cypher
CREATE (loc:Location {
  name: 'Eiffel Tower',
  position: point({latitude: 48.8584, longitude: 2.2945})
})
RETURN loc.name, loc.position
```

*Spatial queries*:
```cypher
// Find locations within a distance
WITH point({latitude: 37.7749, longitude: -122.4194}) AS center
MATCH (loc:Location)
WHERE distance(center, loc.position) < 10000  // Within 10km
RETURN loc.name, distance(center, loc.position) AS dist
ORDER BY dist
```

**NULL Handling**:
```cypher
RETURN point(null)  // null
RETURN distance(point({x: 0, y: 0}), null)  // null
```

**CRS Incompatibility**:
```cypher
// Cannot compare or measure distance between different CRS types
WITH point({x: 0, y: 0}) AS cartesian,
     point({latitude: 0, longitude: 0}) AS geographic
RETURN distance(cartesian, geographic)  // ERROR or null (incompatible CRS)
```

---

## Type Checking

OpenCypher provides mechanisms to check the type of values at runtime.

### Type Predicate Expressions

**Syntax**:
```cypher
<expression> IS :: <TYPE>
<expression> IS NOT :: <TYPE>
```

**Supported Types**:
- Primitive: `INTEGER`, `FLOAT`, `STRING`, `BOOLEAN`, `NULL`
- Structural: `LIST`, `MAP`, `LIST<TYPE>`
- Composite: `NODE`, `RELATIONSHIP`, `PATH`
- Temporal: `DATE`, `TIME`, `LOCAL TIME`, `DATETIME`, `LOCAL DATETIME`, `DURATION`
- Spatial: `POINT`

**NULL Handling**:
- By default, all types include `null`
- Use `NOT NULL` suffix to exclude `null`: `INTEGER NOT NULL`

**Examples**:

*Basic type checking*:
```cypher
RETURN 42 IS :: INTEGER                    // true
RETURN 3.14 IS :: FLOAT                    // true
RETURN 'hello' IS :: STRING                // true
RETURN true IS :: BOOLEAN                  // true
RETURN [1, 2, 3] IS :: LIST                // true
RETURN {name: 'Alice'} IS :: MAP           // true
```

*NULL handling in type predicates*:
```cypher
RETURN null IS :: INTEGER                  // true (NULL is part of every type)
RETURN null IS :: INTEGER NOT NULL         // false (excludes NULL)
RETURN null IS :: STRING                   // true
RETURN null IS :: STRING NOT NULL          // false
```

*Negation*:
```cypher
RETURN 42 IS NOT :: STRING                 // true
RETURN 'hello' IS NOT :: INTEGER           // true
```

*Union types*:
```cypher
RETURN 42 IS :: INTEGER | FLOAT            // true
RETURN 'hello' IS :: INTEGER | STRING      // true
RETURN true IS :: INTEGER | STRING         // false
```

*List with inner type*:
```cypher
RETURN [1, 2, 3] IS :: LIST<INTEGER>       // true
RETURN ['a', 'b'] IS :: LIST<STRING>       // true
RETURN [1, 'a'] IS :: LIST<INTEGER>        // false (heterogeneous)
```

*Checking node/relationship types*:
```cypher
MATCH (n:Person)
WHERE n IS :: NODE
RETURN n

MATCH ()-[r]->()
WHERE r IS :: RELATIONSHIP
RETURN type(r)
```

### Type Functions

**`valueType(expression)`** - Returns string representation of the most precise type:

```cypher
RETURN valueType(42)                       // 'INTEGER'
RETURN valueType(3.14)                     // 'FLOAT'
RETURN valueType('hello')                  // 'STRING'
RETURN valueType(true)                     // 'BOOLEAN'
RETURN valueType(null)                     // 'NULL'
RETURN valueType([1, 2, 3])                // 'LIST<INTEGER NOT NULL>'
RETURN valueType(['a', 'b'])               // 'LIST<STRING NOT NULL>'
RETURN valueType([1, null])                // 'LIST<INTEGER>'
RETURN valueType({name: 'Alice'})          // 'MAP'
RETURN valueType(date('2024-01-15'))       // 'DATE'
RETURN valueType(point({x: 1, y: 2}))      // 'POINT'
```

**`type(relationship)`** - Returns relationship type name:

```cypher
MATCH (a)-[r]->(b)
RETURN type(r)  // e.g., 'KNOWS', 'WORKS_AT'
```

---

## Type Coercion and Conversion

OpenCypher performs limited automatic type coercion and provides explicit conversion functions.

### Automatic (Implicit) Coercion

**Numeric Coercion**:
- INTEGER automatically converts to FLOAT in mixed numeric operations
- Result type is always the "wider" type (FLOAT)

```cypher
RETURN 10 + 2.5       // 12.5 (INTEGER coerced to FLOAT)
RETURN 5 * 1.5        // 7.5 (FLOAT result)
RETURN 10 / 4.0       // 2.5 (FLOAT division, not integer division)
RETURN 10 / 4         // 2 (INTEGER division when both are INTEGER)
```

**No Other Implicit Coercion**:
- Strings are NOT automatically converted to numbers
- Booleans are NOT automatically converted to integers
- No automatic conversion between temporal types

```cypher
RETURN '5' + 3        // ERROR or null (no implicit string-to-number conversion)
RETURN true + 1       // ERROR or null (no implicit boolean-to-integer conversion)
```

### Explicit Conversion Functions

**To INTEGER**:
```cypher
toInteger('42')           // 42
toInteger(3.14)           // 3 (truncates)
toInteger(true)           // 1
toInteger(false)          // 0
toInteger('hello')        // null (conversion fails)
toIntegerOrNull('hello')  // null (explicit null return)
```

**To FLOAT**:
```cypher
toFloat('3.14')          // 3.14
toFloat(42)              // 42.0
toFloat('hello')         // null (conversion fails)
toFloatOrNull('hello')   // null (explicit null return)
```

**To STRING**:
```cypher
toString(42)             // '42'
toString(3.14)           // '3.14'
toString(true)           // 'true'
toString(null)           // null
toString(date('2024-01-15'))  // '2024-01-15'
```

**To BOOLEAN**:
```cypher
toBoolean('true')        // true
toBoolean('false')       // false
toBoolean(1)             // true (non-zero)
toBoolean(0)             // false
toBoolean('hello')       // null (conversion fails)
toBooleanOrNull('hello') // null (explicit null return)
```

**Examples**:

*Numeric coercion*:
```cypher
WITH 10 AS int, 2.5 AS float
RETURN int + float AS sum,           // 12.5 (FLOAT)
       int * float AS product,       // 25.0 (FLOAT)
       int / float AS quotient       // 4.0 (FLOAT)
```

*Explicit conversions*:
```cypher
WITH '42' AS strNum, '3.14' AS strFloat
RETURN toInteger(strNum) AS intVal,      // 42
       toFloat(strFloat) AS floatVal     // 3.14

WITH 42 AS num
RETURN toString(num) AS str              // '42'

WITH '123' AS str
RETURN toInteger(str) + 10 AS result     // 133
```

*Conversion failures return NULL*:
```cypher
RETURN toInteger('not a number')     // null
RETURN toFloat('invalid')            // null
RETURN toBoolean('maybe')            // null
```

---

## Property Types

**Property types** are types that can be stored as node or relationship properties. Not all Cypher types can be persisted.

### Allowed Property Types

| Type | Can Store as Property | Notes |
|------|----------------------|-------|
| INTEGER | ✅ Yes | 64-bit signed integers |
| FLOAT | ✅ Yes | 64-bit double-precision floats |
| STRING | ✅ Yes | Unicode text |
| BOOLEAN | ✅ Yes | true/false |
| NULL | ✅ Yes | Represents missing value |
| DATE | ✅ Yes | Calendar date |
| TIME | ✅ Yes | Time with timezone |
| LOCAL TIME | ✅ Yes | Time without timezone |
| DATETIME | ✅ Yes | Date and time with timezone |
| LOCAL DATETIME | ✅ Yes | Date and time without timezone |
| DURATION | ✅ Yes | Temporal amount |
| POINT | ✅ Yes | Spatial location |
| Homogeneous LIST | ✅ Yes | List of same type (e.g., `[1, 2, 3]`, `['a', 'b']`) |
| Heterogeneous LIST | ❌ No | Mixed-type lists (e.g., `[1, 'a', true]`) |
| MAP | ❌ No | Cannot store directly (but nodes/relationships are map-like) |
| NODE | ❌ No | Graph element, not a storable value |
| RELATIONSHIP | ❌ No | Graph element, not a storable value |
| PATH | ❌ No | Query result type, not storable |

### Examples

**Valid property storage**:
```cypher
CREATE (p:Person {
  name: 'Alice',                          // STRING
  age: 30,                                // INTEGER
  height: 1.75,                           // FLOAT
  active: true,                           // BOOLEAN
  birthDate: date('1994-01-15'),         // DATE
  lastLogin: datetime('2024-01-15T12:30:45+00:00'),  // DATETIME
  location: point({latitude: 37.7749, longitude: -122.4194}),  // POINT
  tags: ['developer', 'python', 'graph'] // LIST<STRING> (homogeneous)
})
```

**Invalid property storage**:
```cypher
// Heterogeneous list - ERROR
CREATE (p:Person {data: [1, 'hello', true]})  // ERROR: Unsupported property value type

// Map - ERROR
CREATE (p:Person {metadata: {key: 'value'}})  // ERROR: Cannot store MAP directly

// Node/Relationship - ERROR
CREATE (p:Person {friend: otherNode})  // ERROR: Cannot store nodes as properties
```

### Nested Structures

**Homogeneous lists can nest**:
```cypher
CREATE (m:Matrix {
  grid: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]  // LIST<LIST<INTEGER>> - Valid
})
```

**Maps cannot be properties**, but nodes/relationships act as maps:
```cypher
// Instead of storing a map, create a separate node
CREATE (p:Person {name: 'Alice'})
CREATE (a:Address {street: '123 Main St', city: 'NYC'})
CREATE (p)-[:LIVES_AT]->(a)
```

---

## NULL Handling and Semantics

`NULL` represents a **missing, unknown, or undefined value** in OpenCypher. It has special semantics that differ from most programming languages.

### Core Principles

1. **NULL ≠ NULL**: `null` is not equal to itself
2. **NULL propagation**: Most operations involving `null` return `null`
3. **Three-valued logic**: Boolean operations with `null` return `true`, `false`, or `null`
4. **Testing for NULL**: Use `IS NULL` / `IS NOT NULL`, not `=` or `<>`

### NULL in Comparisons

**Equality and inequality**:
```cypher
RETURN null = null      // null (not true!)
RETURN null <> null     // null (not false!)
RETURN 5 = null         // null
RETURN 'hello' <> null  // null
```

**Ordering comparisons**:
```cypher
RETURN null < 5         // null
RETURN null > 10        // null
RETURN null <= null     // null
RETURN null >= null     // null
```

**Testing for NULL**:
```cypher
RETURN null IS NULL             // true
RETURN 42 IS NULL               // false
RETURN null IS NOT NULL         // false
RETURN 'hello' IS NOT NULL      // true
```

### NULL in Arithmetic

All arithmetic operations with `null` return `null`:

```cypher
RETURN 5 + null         // null
RETURN null * 10        // null
RETURN null / 3         // null
RETURN 10 % null        // null
RETURN -null            // null
```

### NULL in Boolean Logic (Three-Valued Logic)

**AND operator**:
| A | B | A AND B |
|---|---|---------|
| true | true | true |
| true | false | false |
| true | null | **null** |
| false | true | false |
| false | false | false |
| false | null | **false** (definite) |
| null | true | **null** |
| null | false | **false** (definite) |
| null | null | **null** |

```cypher
RETURN true AND null    // null
RETURN false AND null   // false (definite: false AND anything = false)
RETURN null AND null    // null
```

**OR operator**:
| A | B | A OR B |
|---|---|--------|
| true | true | true |
| true | false | true |
| true | null | **true** (definite) |
| false | true | true |
| false | false | false |
| false | null | **null** |
| null | true | **true** (definite) |
| null | false | **null** |
| null | null | **null** |

```cypher
RETURN true OR null     // true (definite: true OR anything = true)
RETURN false OR null    // null
RETURN null OR null     // null
```

**NOT operator**:
```cypher
RETURN NOT null         // null
RETURN NOT true         // false
RETURN NOT false        // true
```

**XOR operator**:
```cypher
RETURN true XOR null    // null
RETURN false XOR null   // null
RETURN null XOR null    // null
```

### NULL in Collections

**IN operator**:
```cypher
RETURN 2 IN [1, 2, 3]           // true (found)
RETURN 5 IN [1, 2, 3]           // false (not found, no null)
RETURN 2 IN [1, null, 3]        // null (cannot determine - might be in null)
RETURN 5 IN [1, null, 3]        // null (cannot determine - might be in null)
RETURN null IN [1, 2, 3]        // null
RETURN null IN [1, null, 3]     // null
```

**List access**:
```cypher
WITH [1, 2, 3] AS list
RETURN list[10]                 // null (out of bounds)
RETURN list[-10]                // null (out of bounds)

WITH null AS list
RETURN list[0]                  // null (null propagation)
```

**Lists containing NULL**:
```cypher
RETURN [1, null, 3]             // [1, null, 3] (valid list)
RETURN size([1, null, 3])       // 3 (null counts as an element)
```

### NULL in Property Access

**Missing properties return NULL**:
```cypher
MATCH (p:Person {name: 'Alice'})
RETURN p.nonExistent            // null (property doesn't exist)

WITH null AS node
RETURN node.name                // null (null propagation)
```

**NULL properties vs. missing properties**:
```cypher
// Explicitly set to null
CREATE (p:Person {name: 'Alice', age: null})

// Property not set at all (implicitly null)
CREATE (p:Person {name: 'Bob'})

MATCH (p:Person)
RETURN p.age                    // null in both cases
```

### NULL in Aggregations

**Aggregations ignore NULL values** (except `count(*)`):

```cypher
WITH [1, null, 3, null, 5] AS values
UNWIND values AS val
RETURN count(val) AS cnt,       // 3 (excludes nulls)
       sum(val) AS total,       // 9 (1 + 3 + 5)
       avg(val) AS average,     // 3.0 (9 / 3)
       min(val) AS minimum,     // 1
       max(val) AS maximum      // 5

WITH [null, null, null] AS values
UNWIND values AS val
RETURN count(val) AS cnt,       // 0
       sum(val) AS total,       // null (no non-null values)
       avg(val) AS average      // null
```

**`count(*)` includes NULL**:
```cypher
WITH [1, null, 3] AS values
UNWIND values AS val
RETURN count(*) AS totalRows,   // 3 (includes null)
       count(val) AS nonNullCnt // 2 (excludes null)
```

### NULL in Pattern Matching

**OPTIONAL MATCH returns NULL for unmatched patterns**:
```cypher
MATCH (a:Person {name: 'Alice'})
OPTIONAL MATCH (a)-[:KNOWS]->(friend)
RETURN a.name, friend           // friend is null if Alice has no friends
```

**NULL in WHERE filters**:
```cypher
MATCH (p:Person)
WHERE p.age > 30                // Excludes rows where p.age is null (null > 30 = null = false)
RETURN p.name

MATCH (p:Person)
WHERE p.age > 30 OR p.age IS NULL  // Includes rows with null age
RETURN p.name
```

### Best Practices

1. **Always use `IS NULL` / `IS NOT NULL`** to test for NULL, never `= null`
2. **Be aware of three-valued logic** in boolean expressions
3. **Check for NULL before operations** if NULL inputs are possible
4. **Use `COALESCE()`** to provide default values:
   ```cypher
   RETURN coalesce(p.age, 0) AS age  // Returns 0 if p.age is null
   ```
5. **Understand NULL propagation** in arithmetic and comparisons
6. **Remember aggregations ignore NULL** (except `count(*)`)

---

## Summary

OpenCypher provides a comprehensive type system covering:

- **Primitive types**: INTEGER, FLOAT, STRING, BOOLEAN, NULL
- **Structural types**: LIST, MAP
- **Composite types**: NODE, RELATIONSHIP, PATH
- **Temporal types**: DATE, TIME, LOCAL TIME, DATETIME, LOCAL DATETIME, DURATION
- **Spatial types**: POINT (2D/3D, Cartesian/Geographic)

**Key takeaways**:
- Type inference is automatic based on literal syntax
- NULL is part of every type and has special three-valued logic semantics
- Numeric coercion (INTEGER → FLOAT) is the only implicit conversion
- Property types can be stored; structural/composite types are query-time only
- Type checking uses `IS ::` syntax and `valueType()` function
- Explicit conversions use `toInteger()`, `toFloat()`, `toString()`, `toBoolean()`

For implementation-specific details and advanced features, consult the official OpenCypher specification and Neo4j Cypher Manual.
