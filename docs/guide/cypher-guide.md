# Cypher Reference

**Advanced:** this reference assumes basic Python and database-query concepts.
For guided first use, start with [Your first graph](quickstart.md). Each Cypher
example below is a separate statement; copy one into `forge.execute()` using a
graph with the labels and properties shown.

GraphForge executes openCypher through its Rust core. Run
queries through `forge.execute(query, params=None)` — every call returns a
PyArrow `Table` (no `CypherValue` wrappers).

This guide covers everyday clauses, patterns, expressions, and functions. For
install and first-graph steps, start with [Quick Start](quickstart.md); for
programmatic construction, see [Graph Construction](graph-construction.md).

**Compliance:** See [TCK Compliance](../reference/tck-compliance.md) for the
current openCypher TCK corpus gate.

---

## Table of Contents

- [Reading Data](#reading-data)
- [Writing Data](#writing-data)
- [Updating Data](#updating-data)
- [Query Chaining](#query-chaining)
- [Patterns](#patterns)
- [Expressions and Operators](#expressions-and-operators)
- [Functions](#functions)
- [Temporal Types](#temporal-types)
- [Parameters](#parameters)
- [Saved Queries](#saved-queries)

---

## Saved Queries

A Project can keep named, reusable read-only analyses. Each definition has a
stable `query_uuid`, a unique case-sensitive name without surrounding whitespace,
an optional description, Cypher text, and parameter declarations. These are native
Project metadata, so they survive reopen and complete portable export/import.
Creating or changing this metadata requires admitted storage. Use a new project
on [supported durable storage](installation.md#durable-storage) for this example;
do not assume the memory-only quickstart removes this requirement. In particular,
an in-memory instance whose temporary directory is on an unsupported filesystem
can query the graph but fail when saving a query definition.

```python
from pathlib import Path
from uuid import uuid4
from graphforge import GraphForge

Path("saved-query-project").mkdir()
forge = GraphForge("saved-query-project")
forge.execute("CREATE (:Person {age:20}), (:Person {age:40})")
query_uuid = str(uuid4())
forge.create_saved_query({
    "query_uuid": query_uuid,
    "name": "People above an age",
    "description": "Count people meeting a supplied threshold",
    "query": "MATCH (p:Person) WHERE p.age >= $minimum RETURN count(p) AS people",
    "parameters": {"minimum": "integer"},
})
definitions = forge.saved_queries()
definition = forge.saved_query(query_uuid)
table = forge.execute_saved_query(query_uuid, {"minimum": 30})
assert table.to_pylist() == [{"people": 1}]
forge.update_saved_query({**definition, "name": "People by age threshold"})
forge.delete_saved_query(query_uuid)
forge.close()
```

Parameter declarations must exactly name the `$parameters` referenced by the
query. Every declared value is required on execution and must match its type:
`boolean`, `integer` (signed 64-bit), `float` (finite), `string`, or `uuid`.
Null, defaults, and undeclared values are refused. Python UUID parameters use
`uuid.UUID`; Node and CLI use `{"$uuid": "<canonical UUID>"}`. Literal values
embedded in query text are part of the published definition; supply private
values through execution parameters instead.

Node and CLI JSON numbers are interpreted by the saved declaration: an integral
number can supply `float`, and a floating representation can supply `integer`
only when integral and within ±9,007,199,254,740,991. Rust and Python retain their
native integer/float distinction. Lean Rust builds support current saved queries;
historical sources require the `research` feature and otherwise return
`GF_CAPABILITY_DISABLED`.

Saving validates syntax and the read-only contract without binding to the graph
or running the query. Mutation clauses and procedure `CALL` are refused. General
aggregates and tables are supported; a saved query need not return membership
UUIDs. If it does return `node_uuid` or `edge_uuid`, its text can also be used by
a Slice query selector. A saved definition and a frozen Slice remain separate
objects.

Execution pins one committed generation, uses the instance's native resource
policy, and collects at most 1,000,000 rows and 64 MiB of Arrow array data.
Exceeding either ceiling fails without a partial result. Supply a
`CancellationToken` through Python's `cancellation` argument, or pass an
`AbortSignal` as Node's fourth `executeSavedQuery(uuid, params, source, signal)`
argument.

Retained research Versions keep the definition revision they captured. Python's
`saved_query`, `saved_queries`, and `execute_saved_query` accept
`source={"kind": "version", "version_uuid": "<UUID>"}` to read that revision
and execute against its exact retained graph. Omit `source` for current state.
A missing/released Version fails; it never follows the live parent. A definition
alone does not retain graph data or promise identical results across projects.

Node exposes `createSavedQuery`, `updateSavedQuery`, `deleteSavedQuery`,
`savedQuery`, `savedQueries`, and asynchronous `executeSavedQuery`; execution
returns Arrow IPC bytes. Rust exposes the same lifecycle plus
`saved_query_at`/`saved_queries_at` with `SavedQuerySource`.

CLI definitions and parameter values use separate JSON files:

```sh
gf --project ./research saved-query save --file definition.json
gf --project ./research saved-query list
gf --project ./research saved-query show QUERY_UUID
gf --project ./research saved-query update --file definition.json
gf --project ./research saved-query run QUERY_UUID --params parameters.json > result.arrow
gf --project ./research saved-query delete QUERY_UUID
```

`list`, `show`, and `run` accept `--version-uuid UUID`. Lifecycle output is JSON;
execution emits Arrow IPC, or JSON with the global `--json` option. Saving,
listing, publishing, cloning, and importing never execute definitions. Previous
execution values and results are not saved with them.

## Reading Data

### MATCH

Find nodes and relationships matching a pattern.

```cypher
-- All nodes
MATCH (n) RETURN n

-- Nodes with label
MATCH (p:Person) RETURN p

-- Nodes with multiple labels
MATCH (e:Person:Employee) RETURN e

-- Nodes with property filter
MATCH (p:Person {name: 'Alice'}) RETURN p

-- Directed relationship
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b

-- Undirected relationship
MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a, b

-- Multiple relationship types
MATCH (a)-[r:KNOWS|LIKES]->(b) RETURN a, r, b

-- Variable-length paths
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a, b

-- Unbounded variable-length
MATCH (a)-[*]->(b) RETURN a, b

-- Named path
MATCH p = (a:Person)-[:KNOWS*]->(b:Person) RETURN p

-- Multi-hop
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)
RETURN a.name, b.name, c.name
```

### OPTIONAL MATCH

Left-outer-join semantics: returns NULL for missing matches.

```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[:KNOWS]->(friend:Person)
RETURN p.name AS person, friend.name AS friend  -- friend.name is NULL if no match

-- Multiple optional matches
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(company:Company)
OPTIONAL MATCH (p)-[:LIVES_IN]->(city:City)
RETURN p.name, company.name, city.name
```

### WHERE

Filter by any boolean expression.

```cypher
-- Comparison
MATCH (p:Person) WHERE p.age > 25 RETURN p

-- NULL check
MATCH (p:Person) WHERE p.email IS NULL RETURN p
MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p

-- Boolean operators
MATCH (p:Person) WHERE p.age > 25 AND p.city = 'NYC' RETURN p
MATCH (p:Person) WHERE p.age < 20 OR p.age > 60 RETURN p
MATCH (p:Person) WHERE NOT p.active RETURN p

-- Label predicate
MATCH (n) WHERE n:Person RETURN n

-- String predicates
MATCH (p:Person) WHERE p.name STARTS WITH 'Al' RETURN p
MATCH (p:Person) WHERE p.name ENDS WITH 'ice' RETURN p
MATCH (p:Person) WHERE p.name CONTAINS 'lic' RETURN p

-- Regex
MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN p

-- List membership
MATCH (p:Person) WHERE p.city IN ['NYC', 'Boston', 'London'] RETURN p

-- Pattern predicate (not returning path)
MATCH (p:Person) WHERE (p)-[:KNOWS]->(:Person {name: 'Alice'}) RETURN p
MATCH (p:Person) WHERE NOT (p)-[:KNOWS]->() RETURN p

-- EXISTS subquery
MATCH (p:Person)
WHERE EXISTS { MATCH (p)-[:KNOWS]->(:Person) }
RETURN p.name

-- Property existence (short form)
MATCH (p:Person) WHERE exists(p.email) RETURN p
```

### RETURN

Project variables, properties, expressions, and aggregations.

```cypher
-- Return variable
MATCH (p:Person) RETURN p

-- Return property
MATCH (p:Person) RETURN p.name, p.age

-- Alias
MATCH (p:Person) RETURN p.name AS person, p.age AS age

-- DISTINCT
MATCH (p:Person) RETURN DISTINCT p.city

-- Aggregation (implicit GROUP BY on non-aggregated columns)
MATCH (p:Person) RETURN p.city AS city, count(*) AS total

-- All properties (map)
MATCH (p:Person) RETURN p {.*}

-- Arithmetic in return
MATCH (p:Person) RETURN p.name, p.salary * 1.1 AS after_raise

-- CASE expression
MATCH (p:Person)
RETURN p.name,
       CASE
         WHEN p.age < 18 THEN 'Minor'
         WHEN p.age < 65 THEN 'Adult'
         ELSE 'Senior'
       END AS category

-- Return * (all variables in scope)
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN *
```

### ORDER BY, LIMIT, SKIP

```cypher
-- Sort ascending (default)
MATCH (p:Person) RETURN p.name ORDER BY p.age

-- Sort descending
MATCH (p:Person) RETURN p.name ORDER BY p.age DESC

-- Multiple sort keys
MATCH (p:Person) RETURN p.name ORDER BY p.city ASC, p.age DESC

-- Limit results
MATCH (p:Person) RETURN p.name ORDER BY p.age LIMIT 10

-- Pagination
MATCH (p:Person) RETURN p.name ORDER BY p.name SKIP 20 LIMIT 10
```

---

## Writing Data

### CREATE

Create nodes and relationships.

```cypher
-- Single node
CREATE (p:Person {name: 'Alice', age: 30})

-- Multiple labels
CREATE (e:Person:Employee {name: 'Charlie'})

-- Multiple nodes in one clause
CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})

-- Relationship between existing nodes
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2020}]->(b)

-- Create inline
CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})

-- CREATE ... RETURN
CREATE (p:Person {name: 'Alice'})
RETURN p.name AS name, p.node_uuid AS node_uuid
```

### MERGE

Find or create — safe for idempotent upserts within the shipped Rust subset
([implementation status](../reference/implementation-status/clauses.md#merge)).

**Supported:**

- Standalone new-node MERGE: `MERGE (p:Person {email: '…'})`
- Relationship MERGE when both endpoints are already bound: `MATCH (a), (b) MERGE (a)-[:KNOWS]->(b)`
- `ON CREATE SET` / `ON MATCH SET` property (and label) actions when every row takes the same create-or-match branch

**Not supported (structured plan errors):**

- Multi-node / relationship-construction MERGE such as `MERGE (a:A)-[:R]->(b:B)` → `relationship and multi-node MERGE execution is not implemented yet`
- Row-conditional map actions (`+=` / `=`) when some rows create and others match → `row-conditional MERGE map actions are not implemented yet`

```cypher
-- Merge node (create if not exists, match if exists)
MERGE (p:Person {email: 'alice@example.com'})

-- ON CREATE and ON MATCH callbacks
MERGE (p:Person {email: 'alice@example.com'})
ON CREATE SET p.created = 2024, p.name = 'Alice'
ON MATCH  SET p.last_seen = 2024

-- Merge relationship (both endpoints must already exist)
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
MERGE (a)-[:KNOWS]->(b)

-- Merge with RETURN
MERGE (p:Person {email: 'alice@example.com'})
RETURN p
```

### UNWIND

Expand a list into rows.

```cypher
UNWIND [1, 2, 3] AS x RETURN x * 2 AS doubled

-- Common pattern: batch-create from list
UNWIND ['Alice', 'Bob', 'Charlie'] AS name
CREATE (:Person {name: name})

-- Expand collected list
MATCH (p:Person)
WITH collect(p.name) AS names
UNWIND names AS name
RETURN name
```

---

## Updating Data

### SET

Add or update properties and labels.

```cypher
-- Set property
MATCH (p:Person {name: 'Alice'}) SET p.age = 31

-- Multiple properties
MATCH (p:Person {name: 'Alice'})
SET p.age = 31, p.city = 'Boston', p.active = true

-- Set from map
MATCH (p:Person {name: 'Alice'})
SET p += {age: 31, city: 'Boston'}

-- Overwrite all properties
MATCH (p:Person {name: 'Alice'})
SET p = {name: 'Alice', age: 31}

-- Add label
MATCH (p:Person {name: 'Alice'}) SET p:Manager

-- SET ... RETURN
MATCH (p:Person {name: 'Alice'})
SET p.age = 31
RETURN p.name AS name, p.age AS new_age
```

### REMOVE

Remove properties and labels.

```cypher
-- Remove property
MATCH (p:Person {name: 'Alice'}) REMOVE p.temp

-- Remove multiple
MATCH (p:Person {name: 'Alice'}) REMOVE p.temp, p.draft

-- Remove label
MATCH (p:Person {name: 'Alice'}) REMOVE p:Manager
```

### DELETE / DETACH DELETE

```cypher
-- Delete a relationship
MATCH (a)-[r:KNOWS]->(b) WHERE a.name = 'Alice' DELETE r

-- Delete a node (no relationships may exist)
MATCH (p:Person {name: 'Alice'}) DELETE p

-- DETACH DELETE: remove node and all its relationships
MATCH (p:Person {name: 'Alice'}) DETACH DELETE p

-- Delete everything
MATCH (n) DETACH DELETE n
```

---

## Query Chaining

### WITH

Pass results between query stages, filter aggregations, limit scope.

```cypher
-- Chain match with filter
MATCH (p:Person)-[:KNOWS]->(friend)
WITH p, count(friend) AS friends
WHERE friends > 5
RETURN p.name AS person, friends

-- Rename and continue
MATCH (p:Person)
WITH p.name AS name, p.age AS age
WHERE age > 25
RETURN name, age

-- Aggregate then match
MATCH (p:Person)
WITH p.city AS city, count(*) AS pop
WHERE pop > 100
MATCH (p2:Person {city: city})
RETURN p2.name, city, pop
ORDER BY pop DESC

-- WITH DISTINCT
MATCH (p:Person)-[:KNOWS]->(friend:Person)
WITH DISTINCT friend
RETURN friend.name AS mutual_friend
```

### UNION / UNION ALL

```cypher
-- UNION (removes duplicates)
MATCH (p:Person) RETURN p.name AS name
UNION
MATCH (c:Company) RETURN c.name AS name

-- UNION ALL (keeps duplicates)
MATCH (p:Person) RETURN p.name AS name
UNION ALL
MATCH (p2:Person) RETURN p2.name AS name
```

---

## Patterns

### Node Patterns

| Pattern                | Meaning                      |
| ---------------------- | ---------------------------- |
| `(n)`                  | Any node                     |
| `(n:Person)`           | Node with label Person       |
| `(n:Person:Employee)`  | Node with both labels        |
| `(n:Person {age: 30})` | Node with label and property |
| `(:Person)`            | Anonymous node with label    |
| `({name: 'Alice'})`    | Anonymous node with property |

### Relationship Patterns

| Pattern                      | Meaning                   |
| ---------------------------- | ------------------------- |
| `-[r]->`                     | Any directed relationship |
| `-[:KNOWS]->`                | Specific type             |
| `-[r:KNOWS {since: 2020}]->` | Type with property        |
| `-[:KNOWS\|LIKES]->`         | Multiple types (OR)       |
| `-[*]->`                     | Variable-length (any)     |
| `-[*2]->`                    | Exactly 2 hops            |
| `-[*1..3]->`                 | 1 to 3 hops               |
| `-[*..5]->`                  | Up to 5 hops              |
| `-[*3..]->`                  | 3 or more hops            |

### Path Variables

```cypher
-- Bind path to variable
MATCH p = (a:Person)-[:KNOWS*]->(b:Person)
RETURN length(p) AS hops, nodes(p) AS path_nodes

-- Shortest path
MATCH p = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]->(b:Person {name: 'Bob'}))
RETURN p
```

---

## Expressions and Operators

### Arithmetic

```cypher
RETURN 5 + 3         -- 8
RETURN 5 - 3         -- 2
RETURN 5 * 3         -- 15
RETURN 5 / 2         -- 2 (integer division)
RETURN 5.0 / 2       -- 2.5
RETURN 5 % 2         -- 1
RETURN 2 ^ 10        -- 1024.0
```

### Comparison

```cypher
RETURN 5 = 5        -- true
RETURN 5 <> 6       -- true
RETURN 5 < 6        -- true
RETURN 5 <= 5       -- true
RETURN 6 > 5        -- true
RETURN 6 >= 6       -- true
```

### Boolean

```cypher
RETURN true AND false   -- false
RETURN true OR false    -- true
RETURN NOT true         -- false
RETURN true XOR true    -- false

-- NULL propagation
RETURN null AND true    -- null
RETURN null OR true     -- true
RETURN NOT null         -- null
```

### String

```cypher
RETURN 'hello' + ' world'          -- 'hello world'
RETURN 'Alice' STARTS WITH 'Al'    -- true
RETURN 'Alice' ENDS WITH 'ice'     -- true
RETURN 'Alice' CONTAINS 'lic'      -- true
RETURN 'Alice' =~ 'A.*'            -- true (regex)
```

### List Operators

```cypher
RETURN [1, 2, 3] + [4, 5]          -- [1, 2, 3, 4, 5]
RETURN 3 IN [1, 2, 3]              -- true
RETURN [1, 2, 3][0]                -- 1
RETURN [1, 2, 3][1..3]             -- [2, 3]
```

### NULL

```cypher
-- NULL propagates through most operations
RETURN null + 5     -- null
RETURN null = null  -- null (use IS NULL instead)

-- Safe operators
MATCH (p:Person) WHERE p.age IS NULL RETURN p
MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p
```

### CASE Expression

```cypher
-- Simple form
RETURN CASE p.status
  WHEN 'active'   THEN 'Active User'
  WHEN 'inactive' THEN 'Inactive User'
  ELSE 'Unknown'
END

-- Generic form
RETURN CASE
  WHEN p.age < 18 THEN 'Minor'
  WHEN p.age < 65 THEN 'Adult'
  ELSE 'Senior'
END
```

---

## Functions

### String Functions

| Function                   | Example                    | Result          |
| -------------------------- | -------------------------- | --------------- |
| `toLower(s)`               | `toLower('HELLO')`         | `'hello'`       |
| `toUpper(s)`               | `toUpper('hello')`         | `'HELLO'`       |
| `trim(s)`                  | `trim(' hi ')`             | `'hi'`          |
| `ltrim(s)`                 | `ltrim(' hi')`             | `'hi'`          |
| `rtrim(s)`                 | `rtrim('hi ')`             | `'hi'`          |
| `replace(s, f, r)`         | `replace('aaa', 'a', 'b')` | `'bbb'`         |
| `substring(s, start, len)` | `substring('hello', 1, 3)` | `'ell'`         |
| `left(s, n)`               | `left('hello', 2)`         | `'he'`          |
| `right(s, n)`              | `right('hello', 2)`        | `'lo'`          |
| `split(s, delim)`          | `split('a,b,c', ',')`      | `['a','b','c']` |
| `reverse(s)`               | `reverse('hello')`         | `'olleh'`       |
| `size(s)`                  | `size('hello')`            | `5`             |
| `toString(x)`              | `toString(42)`             | `'42'`          |

### Math Functions

| Function                                      | Description      |
| --------------------------------------------- | ---------------- |
| `abs(n)`                                      | Absolute value   |
| `ceil(n)`                                     | Round up         |
| `floor(n)`                                    | Round down       |
| `round(n)`                                    | Round to nearest |
| `sqrt(n)`                                     | Square root      |
| `pow(base, exp)`                              | Power            |
| `exp(n)`                                      | e^n              |
| `log(n)`                                      | Natural log      |
| `log10(n)`                                    | Base-10 log      |
| `sin(n)`, `cos(n)`, `tan(n)`                  | Trig (radians)   |
| `asin(n)`, `acos(n)`, `atan(n)`, `atan2(y,x)` | Inverse trig     |
| `pi()`                                        | π (3.14159…)     |
| `e()`                                         | e (2.71828…)     |
| `rand()`                                      | Random 0.0–1.0   |
| `sign(n)`                                     | -1, 0, or 1      |

### List Functions

| Function                  | Example            | Result           |
| ------------------------- | ------------------ | ---------------- |
| `head(list)`              | `head([1,2,3])`    | `1`              |
| `tail(list)`              | `tail([1,2,3])`    | `[2,3]`          |
| `last(list)`              | `last([1,2,3])`    | `3`              |
| `size(list)`              | `size([1,2,3])`    | `3`              |
| `range(start, end)`       | `range(1,5)`       | `[1,2,3,4,5]`    |
| `range(start, end, step)` | `range(0,10,2)`    | `[0,2,4,6,8,10]` |
| `reverse(list)`           | `reverse([1,2,3])` | `[3,2,1]`        |
| `sort(list)`              | `sort([3,1,2])`    | `[1,2,3]`        |
| `keys(map)`               | `keys({a:1,b:2})`  | `['a','b']`      |

### List Comprehension and Predicates

```cypher
-- Filter a list
RETURN [x IN range(1,10) WHERE x % 2 = 0]  -- [2,4,6,8,10]

-- Transform a list
RETURN [x IN range(1,5) | x * x]           -- [1,4,9,16,25]

-- Filter + transform
RETURN [x IN range(1,10) WHERE x > 5 | x * 2]  -- [12,14,16,18,20]

-- all() — every element satisfies predicate
RETURN all(x IN [2,4,6] WHERE x % 2 = 0)   -- true

-- any() — at least one element satisfies predicate
RETURN any(x IN [1,2,3] WHERE x > 2)        -- true

-- none() — no element satisfies predicate
RETURN none(x IN [1,2,3] WHERE x > 5)       -- true

-- single() — exactly one element satisfies predicate
RETURN single(x IN [1,2,3] WHERE x = 2)     -- true

-- reduce() — fold list to single value
RETURN reduce(acc = 0, x IN [1,2,3,4,5] | acc + x)  -- 15
```

### Aggregation Functions

```cypher
-- Count rows
MATCH (p:Person) RETURN count(*) AS total

-- Count non-null
MATCH (p:Person) RETURN count(p.age) AS with_age

-- Count distinct
MATCH (p:Person) RETURN count(DISTINCT p.city) AS cities

-- Numeric aggregations
MATCH (p:Person) RETURN sum(p.salary), avg(p.age), min(p.age), max(p.age)

-- Collect into list
MATCH (p:Person) RETURN collect(p.name) AS names

-- Collect distinct
MATCH (p:Person) RETURN collect(DISTINCT p.city) AS cities

-- Standard deviation
MATCH (p:Person) RETURN stDev(p.age), stDevP(p.age)

-- Percentile
MATCH (p:Person) RETURN percentileDisc(p.age, 0.5) AS median
```

### Graph Functions

```cypher
-- Node identity (UUID, not a numeric storage ID)
MATCH (n:Person) RETURN n.node_uuid

-- Labels
RETURN labels(n)               -- list of labels
RETURN 'Person' IN labels(n)   -- true/false

-- Relationship type
RETURN type(r)

-- Properties (as map)
RETURN properties(n)

-- Keys (property names)
RETURN keys(n)

-- Path functions
MATCH p = (a)-[*]->(b)
RETURN nodes(p), relationships(p), length(p)

-- Endpoints
MATCH (a)-[r]->(b)
RETURN startNode(r), endNode(r)

-- Degree
MATCH (n:Person)
RETURN size((n)-[:KNOWS]->()) AS out_degree
```

### Predicate Functions

```cypher
-- exists() — property exists
MATCH (p:Person) WHERE exists(p.email) RETURN p

-- Empty lists and strings; unknown is separate from empty
RETURN size([]) = 0    -- true
RETURN size('') = 0    -- true
RETURN size(null) = 0  -- null

-- Null coalescing
RETURN coalesce(null, null, 'default')   -- 'default'
RETURN coalesce(p.nickname, p.name)      -- first non-null
```

### Conversion Functions

```cypher
RETURN toInteger('42')         -- 42
RETURN toInteger(3.7)          -- 3
RETURN toFloat('3.14')         -- 3.14
RETURN toString(42)            -- '42'
RETURN toBoolean('true')       -- true
RETURN toBoolean('false')      -- false
```

`toBoolean()` accepts booleans and strings (or null), not numeric values.
For a numeric coding rule, state it explicitly, for example `RETURN 0 <> 0`.

---

## Temporal Types

GraphForge implements full openCypher temporal precision including nanoseconds and IANA timezone names.

### Construction

```cypher
RETURN date('2024-01-15')
RETURN date({year: 2024, month: 1, day: 15})
RETURN date()                          -- current date

RETURN time('14:30:00.000000789')      -- with nanoseconds
RETURN time({hour: 14, minute: 30, second: 0, nanosecond: 789})

RETURN datetime('2024-01-15T14:30:00[Europe/London]')  -- IANA timezone
RETURN datetime('2024-01-15T14:30:00+01:00')           -- offset
RETURN datetime()                                       -- current datetime

RETURN localDatetime('2024-01-15T14:30:00')
RETURN duration('P1Y2M3DT4H5M6.789S')
RETURN duration({years: 1, months: 2, days: 3})
```

### Accessors

```cypher
RETURN date('2024-01-15').year         -- 2024
RETURN date('2024-01-15').month        -- 1
RETURN date('2024-01-15').day          -- 15
RETURN date('2024-01-15').weekday      -- 1 (Monday)
RETURN date('2024-01-15').week         -- 3 (ISO week)
RETURN date('2024-01-15').ordinalDay   -- 15

RETURN time('14:30:00.000000789').hour        -- 14
RETURN time('14:30:00.000000789').nanosecond  -- 789

RETURN datetime('2024-01-15T14:30:00[Europe/Stockholm]').timezone
-- 'Europe/Stockholm'

RETURN duration('P1Y2M3DT4H5M6S').years    -- 1
RETURN duration('P1Y2M3DT4H5M6S').months   -- 2
RETURN duration('PT0.000000789S').nanoseconds  -- 789
```

### Arithmetic

```cypher
RETURN date('2024-01-01') + duration('P1M')    -- 2024-02-01
RETURN date('2024-01-01') - duration('P1Y')    -- 2023-01-01
RETURN duration('P1Y') + duration('P6M')       -- P1Y6M

-- Between
RETURN duration.between(date('2020-01-01'), date('2024-01-01'))
RETURN duration.inMonths(date('2020-01-01'), date('2024-01-01'))
RETURN duration.inDays(datetime('2020-01-01T00:00'), datetime('2020-01-15T12:00'))
RETURN duration.inSeconds(time('08:00'), time('16:30'))
```

### Extreme Years

GraphForge supports years outside Python's native range (1–9999):

```cypher
RETURN localdatetime('+999999999-12-31T23:59:59').year   -- 999999999
RETURN localdatetime('-000001-01-01T00:00').year          -- -1
```

### Truncation

```cypher
RETURN date.truncate('month', date('2024-07-15'))     -- 2024-07-01
RETURN datetime.truncate('day', datetime())            -- current day at midnight
RETURN time.truncate('hour', time('14:37:00'))         -- 14:00:00
```

---

## Parameters

Bind values as parameters to avoid string interpolation:

```python
# Python
table = forge.execute(
    "MATCH (p:Person {name: $name}) WHERE p.age > $min_age RETURN p",
    {"name": "Alice", "min_age": 25},
)
```

```cypher
-- In Cypher, $name and $min_age are the parameter syntax
MATCH (p:Person {name: $name})
WHERE p.age > $min_age
RETURN p.name, p.age
```

Parameters work with all value types: strings, integers, floats, booleans, null, lists, maps.

---

## Common Patterns

### Find Friends-of-Friends

```cypher
MATCH (me:Person {name: 'Alice'})-[:KNOWS]->(friend)-[:KNOWS]->(foaf:Person)
WHERE NOT (me)-[:KNOWS]->(foaf) AND me <> foaf
RETURN DISTINCT foaf.name AS recommendation
```

### Most Connected Nodes

```cypher
MATCH (p:Person)-[:KNOWS]->(friend)
RETURN p.name AS person, count(friend) AS connections
ORDER BY connections DESC
LIMIT 10
```

### Upsert Pattern

```cypher
MERGE (p:Person {email: 'alice@example.com'})
ON CREATE SET p.name = 'Alice', p.created = 2024
ON MATCH  SET p.last_seen = 2024
RETURN p
```

### Batch Create from List

```cypher
UNWIND [
  {name: 'Alice', age: 30},
  {name: 'Bob',   age: 25},
  {name: 'Carol', age: 35}
] AS row
CREATE (:Person {name: row.name, age: row.age})
```

### Conditional Return

```cypher
MATCH (p:Person)
RETURN p.name,
       CASE WHEN p.age IS NULL THEN 'unknown'
            WHEN p.age < 18   THEN 'minor'
            ELSE 'adult'
       END AS category
```

### EXISTS Subquery

```cypher
-- Nodes that have at least one outgoing relationship
MATCH (p:Person)
WHERE EXISTS { MATCH (p)-[:KNOWS]->() }
RETURN p.name

-- Nodes that have no relationships
MATCH (p:Person)
WHERE NOT EXISTS { MATCH (p)-[]-() }
RETURN p.name AS isolated
```

---

## See Also

- [Graph Construction](graph-construction.md) — Python API for building graphs
- [Quick Start](quickstart.md) — five-minute walkthrough
- [OpenCypher Compatibility](../reference/opencypher-compatibility.md) — feature matrix
- [TCK Compliance](../reference/tck-compliance.md) — Rust engine conformance status
