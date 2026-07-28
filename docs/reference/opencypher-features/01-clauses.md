# OpenCypher Query Clauses

This document provides a comprehensive reference for all query clauses in the OpenCypher specification. Query clauses are the building blocks of Cypher queries and control how data is read, written, filtered, and returned from a graph database.

**OpenCypher Reference**: [https://opencypher.org/](https://opencypher.org/)

**Neo4j Cypher Manual**: [https://neo4j.com/docs/cypher-manual/current/clauses/](https://neo4j.com/docs/cypher-manual/current/clauses/)

---

## Table of Contents

- [Reading Clauses](#reading-clauses)
  - [MATCH](#match)
  - [OPTIONAL MATCH](#optional-match)
- [Projecting Clauses](#projecting-clauses)
  - [RETURN](#return)
  - [WITH](#with)
  - [UNWIND](#unwind)
- [Filtering and Sorting Sub-clauses](#filtering-and-sorting-sub-clauses)
  - [WHERE](#where)
  - [ORDER BY](#order-by)
  - [LIMIT](#limit)
  - [SKIP/OFFSET](#skipoffset)
- [Writing Clauses](#writing-clauses)
  - [CREATE](#create)
  - [DELETE](#delete)
  - [DETACH DELETE](#detach-delete)
  - [SET](#set)
  - [REMOVE](#remove)
- [Reading/Writing Clauses](#readingwriting-clauses)
  - [MERGE](#merge)
  - [CALL](#call)
- [Subquery Clauses](#subquery-clauses)
  - [CALL { } (Subqueries)](#call-subqueries)
- [Set Operations](#set-operations)
  - [UNION](#union)
  - [UNION ALL](#union-all)

---

## Reading Clauses

### MATCH

**Purpose**: Specify patterns to search for in the graph database. MATCH is the primary clause for retrieving data by pattern matching against graph structures.

**Syntax**:
```cypher
MATCH (node:Label {property: value})
MATCH (node1)-[:RELATIONSHIP_TYPE]->(node2)
MATCH path = (start)-[*..maxHops]->(end)
```

**Key Features**:
- Match nodes by labels and properties
- Specify relationship direction and type
- Bind entire paths to variables
- Support for variable-length paths
- Label expressions with OR (`|`) and negation (`!`)
- Can be chained with multiple MATCH clauses

**Examples**:

*Simple - Match nodes with a label*:
```cypher
MATCH (movie:Movie)
RETURN movie.title
```

*Complex - Match pattern with multiple relationships*:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)<-[:DIRECTED]-(director:Person)
WHERE movie.year > 2000
RETURN actor.name, movie.title, director.name
```

**Common Use Cases**:
- Finding nodes by label and properties
- Traversing relationships between nodes
- Path finding and pattern matching
- Graph exploration and querying

**OpenCypher Spec**: See specification for complete pattern syntax and semantics.

---

### OPTIONAL MATCH

**Purpose**: Specify patterns to search for in the database while using `null` for missing parts. Functions as Cypher's equivalent to SQL's outer join.

**Syntax**:
```cypher
OPTIONAL MATCH (node:Label)
OPTIONAL MATCH (node1)-[:RELATIONSHIP]->(node2)
```

**Key Features**:
- Returns `null` for unmatched patterns instead of eliminating rows
- Allows data to flow downstream even when patterns don't match
- `WHERE` predicates are evaluated during pattern matching, not after
- Multiple OPTIONAL MATCH clauses can be chained

**Examples**:

*Simple - Optional relationship*:
```cypher
MATCH (person:Person)
OPTIONAL MATCH (person)-[:ACTED_IN]->(movie:Movie)
RETURN person.name, movie.title
```

*Complex - Multiple optional patterns*:
```cypher
MATCH (person:Person {name: 'Tom Hanks'})
OPTIONAL MATCH (person)-[:ACTED_IN]->(movie:Movie)
OPTIONAL MATCH (person)-[:DIRECTED]->(directed:Movie)
RETURN person.name,
       collect(DISTINCT movie.title) AS acted_in,
       collect(DISTINCT directed.title) AS directed
```

**Common Use Cases**:
- Checking if relationships exist without failing the query
- Returning null for missing properties
- Conditional data retrieval
- Exploring graph structures with incomplete schemas

**Important**: When a pattern fails to match, the entire pattern returns `null`, not partial matches.

---

## Projecting Clauses

### RETURN

**Purpose**: Define what parts of a pattern (nodes, relationships, properties) to include in the query result.

**Syntax**:
```cypher
RETURN expression [AS alias] [, ...]
RETURN *
RETURN DISTINCT expression
```

**Key Features**:
- Return nodes, relationships, or specific properties
- Use dot notation for property access: `n.property`
- Column aliasing with `AS`
- `DISTINCT` removes duplicate rows
- `RETURN *` returns all matched elements
- Support for expressions, literals, predicates, and functions

**Examples**:

*Simple - Return properties with aliases*:
```cypher
MATCH (p:Person)
RETURN p.name AS name, p.bornIn AS birthplace
LIMIT 10
```

*Complex - Return with expressions and aggregations*:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
RETURN actor.name,
       count(movie) AS movie_count,
       collect(movie.title) AS movies,
       avg(movie.rating) AS avg_rating
ORDER BY movie_count DESC
LIMIT 5
```

**Common Use Cases**:
- Selecting specific data to return from queries
- Creating computed columns with expressions
- Renaming columns for clarity
- Aggregating and transforming data
- Removing duplicates with DISTINCT

**Performance Tip**: Returning specific properties is more performant than returning entire nodes or relationships.

---

### WITH

**Purpose**: Chain query parts together, piping results from one to be used as starting points or criteria in the next. Controls variable scope and enables intermediate processing.

**Syntax**:
```cypher
WITH expression [AS alias] [, ...]
WITH * [, additional_expressions]
WITH variable WHERE condition
WITH variable ORDER BY expression LIMIT/SKIP n
```

**Key Features**:
- Variable creation and binding
- Scope control - determines which variables remain available
- Expression chaining
- Aggregations and intermediate calculations
- Deduplication with `WITH DISTINCT`
- Result manipulation (ordering, pagination, filtering)

**Examples**:

*Simple - Filter intermediate results*:
```cypher
MATCH (person:Person)-[:ACTED_IN]->(movie:Movie)
WITH person, count(movie) AS movie_count
WHERE movie_count > 10
RETURN person.name, movie_count
```

*Complex - Multiple chaining with aggregation*:
```cypher
MATCH (customer:Customer)-[:PURCHASED]->(order:Order)-[:CONTAINS]->(product:Product)
WITH customer,
     sum(order.total) AS total_spent,
     collect(DISTINCT product.category) AS categories
WHERE total_spent > 1000
WITH customer, total_spent, categories, size(categories) AS category_count
ORDER BY total_spent DESC
RETURN customer.name,
       total_spent,
       category_count,
       categories
LIMIT 10
```

**Common Use Cases**:
- Aggregating data before subsequent matching
- Filtering results before passing to next query part
- Creating calculated properties for downstream use
- Controlling which matched nodes proceed through query pipeline
- Pagination and ordering intermediate results

**Note**: As of Cypher 25, WITH is no longer required as a separator between write and read clauses.

---

### UNWIND

**Purpose**: Expand a list into a sequence of rows. Transforms any list back into individual rows.

**Syntax**:
```cypher
UNWIND list_expression AS variable
```

**Key Features**:
- Converts lists to rows
- Handles nested lists with chained UNWIND
- Empty lists produce zero rows
- `null` is treated as empty (no rows)
- `null` values within lists are preserved as rows
- Non-list expressions treated as single-element lists

**Examples**:

*Simple - Expand a list of values*:
```cypher
UNWIND [1, 2, 3, 4, 5] AS number
RETURN number, number * number AS squared
```

*Complex - Nested list expansion with pattern matching*:
```cypher
WITH [[1, 2], [3, 4], [5]] AS nested_list
UNWIND nested_list AS inner_list
UNWIND inner_list AS number
MATCH (n:Node)
WHERE n.value = number
RETURN n
```

**Common Use Cases**:
- Converting parameter lists into rows for batch operations
- Creating distinct datasets by removing duplicates
- Processing nested list structures
- Expanding collected results for further processing
- Creating multiple nodes from a list of properties

**Important**: Neo4j does not guarantee row order for UNWIND. Use ORDER BY for specific ordering.

---

## Filtering and Sorting Sub-clauses

### WHERE

**Purpose**: Add constraints to patterns in MATCH/OPTIONAL MATCH clauses or filter results from WITH. Functions as a sub-clause rather than a standalone clause.

**Syntax**:
```cypher
MATCH (n) WHERE predicate
OPTIONAL MATCH (n) WHERE predicate
WITH variable WHERE condition
```

**Key Features**:
- Pattern integration - becomes part of the directly preceding MATCH clause
- Not a post-matching filter, but part of pattern matching itself
- Supports static filtering (properties, labels, relationships)
- Dynamic labels/types with `$(<expr>)` syntax
- Dynamic properties with square bracket notation `[]`
- Broader scope than WITH - can reference earlier variables

**Examples**:

*Simple - Property filtering*:
```cypher
MATCH (person:Person)
WHERE person.age > 30 AND person.country = 'USA'
RETURN person.name, person.age
```

*Complex - Multiple conditions with pattern predicates*:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
WHERE movie.year >= 2000
  AND movie.rating > 7.0
  AND EXISTS {
    MATCH (actor)-[:ACTED_IN]->(other:Movie)
    WHERE other.year < 2000
  }
RETURN actor.name,
       count(movie) AS recent_good_movies
ORDER BY recent_good_movies DESC
```

**Common Use Cases**:
- Filtering nodes by property values
- Label and relationship type constraints
- Complex boolean logic conditions
- Pattern existence checks
- Null value handling

**Performance Note**: WHERE predicates affect both query results and performance by influencing the pattern matching process.

---

### ORDER BY

**Purpose**: Determine how results from RETURN or WITH clauses are ordered. Can function as a standalone clause with SKIP/LIMIT.

**Syntax**:
```cypher
ORDER BY expression [ASC|ASCENDING|DESC|DESCENDING] [, ...]
```

**Key Features**:
- Default behavior is ascending order
- Can sort on properties, IDs, or expression results
- Mixed directional sorting (both ASC and DESC in same query)
- Null values appear last in ascending, first in descending
- No guaranteed order without ORDER BY clause

**Examples**:

*Simple - Single column sort*:
```cypher
MATCH (p:Person)
RETURN p.name, p.age
ORDER BY p.age DESC
```

*Complex - Multi-column sort with expressions*:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
WITH actor,
     count(movie) AS movie_count,
     avg(movie.rating) AS avg_rating
ORDER BY avg_rating DESC, movie_count DESC, actor.name ASC
RETURN actor.name,
       movie_count,
       round(avg_rating * 100) / 100 AS avg_rating
LIMIT 20
```

**Common Use Cases**:
- Sorting query results by properties
- Ordering by calculated values
- Multi-level sorting with multiple keys
- Finding top-N results with ORDER BY + LIMIT

**Performance Tip**: Range indexes on sorted properties can optimize ORDER BY operations by providing pre-sorted results.

---

### LIMIT

**Purpose**: Constrain the number of rows returned from a query.

**Syntax**:
```cypher
LIMIT expression
```

**Key Features**:
- Accepts expressions evaluating to positive integers
- Expressions cannot contain variables or node/relationship references
- No guaranteed result order without ORDER BY
- Does not prevent write operations (CREATE, DELETE, SET still execute)

**Examples**:

*Simple - Basic limit*:
```cypher
MATCH (n:Person)
RETURN n.name
ORDER BY n.name
LIMIT 10
```

*Complex - Dynamic limit with pagination*:
```cypher
MATCH (product:Product)
WHERE product.category = 'Electronics'
WITH product
ORDER BY product.rating DESC, product.price ASC
SKIP 20
LIMIT 10
RETURN product.name,
       product.price,
       product.rating
```

**Common Use Cases**:
- Limiting result set size
- Pagination (with SKIP)
- Top-N queries (with ORDER BY)
- Sampling data

**Important**: LIMIT does not prevent write operations. Use WITH to separate reads from writes when controlling update scope.

---

### SKIP/OFFSET

**Purpose**: Define from which row to start including rows in the output. OFFSET is a GQL-compliant synonym for SKIP.

**Syntax**:
```cypher
SKIP expression
OFFSET expression
```

**Key Features**:
- Accepts expressions evaluating to positive integers
- Cannot contain variables or node/relationship references
- Results not guaranteed without ORDER BY
- Can be used standalone or with ORDER BY and LIMIT

**Examples**:

*Simple - Skip first N rows*:
```cypher
MATCH (n:Person)
RETURN n.name
ORDER BY n.name
SKIP 5
```

*Complex - Pagination with SKIP and LIMIT*:
```cypher
// Page 3 of results (20 per page)
MATCH (movie:Movie)
WHERE movie.year >= 2000
WITH movie
ORDER BY movie.rating DESC, movie.title ASC
SKIP 40
LIMIT 20
RETURN movie.title,
       movie.year,
       movie.rating
```

**Common Use Cases**:
- Pagination in combination with LIMIT
- Skipping header rows or known results
- Implementing result windows

**Note**: Common pagination pattern is `SKIP (page - 1) * pageSize LIMIT pageSize`

---

## Writing Clauses

### CREATE

**Purpose**: Create new nodes and relationships in the graph using syntax comparable to MATCH patterns.

**Syntax**:
```cypher
CREATE (variable:Label {properties})
CREATE (node1)-[:RELATIONSHIP_TYPE {properties}]->(node2)
CREATE path = (n1)-[:REL]->(n2)-[:REL]->(n3)
```

**Key Features**:
- Create nodes with labels and properties
- Create relationships with exactly one type and direction
- Multiple labels supported (`:Label1:Label2` or `:Label1&Label2`)
- Variable reuse from MATCH clauses
- Path creation (multiple nodes and relationships simultaneously)
- Dynamic labels/types with `$(<expr>)` syntax
- Property maps for bulk property setting

**Examples**:

*Simple - Create a single node*:
```cypher
CREATE (p:Person {name: 'Alice', age: 30, country: 'USA'})
RETURN p
```

*Complex - Create pattern with multiple nodes and relationships*:
```cypher
MATCH (actor:Person {name: 'Tom Hanks'})
CREATE (movie:Movie {title: 'New Film', year: 2024})
CREATE (director:Person {name: 'Jane Smith'})
CREATE (actor)-[:ACTED_IN {role: 'Lead'}]->(movie)
CREATE (director)-[:DIRECTED]->(movie)
RETURN movie, actor, director
```

**Common Use Cases**:
- Adding new nodes to the graph
- Creating relationships between existing nodes
- Bulk data import and graph construction
- Building graph structures from application data

**Note**: CREATE always creates new entities. Use MERGE to avoid duplicates.

---

### DELETE

**Purpose**: Remove nodes, relationships, or paths from the database. For property or label removal, use REMOVE instead.

**Syntax**:
```cypher
DELETE variable [, ...]
NODETACH DELETE variable
```

**Key Features**:
- Removes nodes, relationships, or paths
- Nodes with relationships cannot be deleted (use DETACH DELETE)
- Deleted objects become inaccessible but occupy disk space reserved for future transactions
- Can delete multiple elements in one clause

**Examples**:

*Simple - Delete a relationship*:
```cypher
MATCH (person:Person)-[r:ACTED_IN]->(movie:Movie)
WHERE person.name = 'John Doe'
DELETE r
```

*Complex - Conditional deletion with multiple relationships*:
```cypher
MATCH (person:Person)
WHERE person.last_login < date('2020-01-01')
MATCH (person)-[r:FOLLOWS|LIKES]->()
DELETE r
WITH person
MATCH (person)-[r2]->()
WHERE type(r2) <> 'CREATED'
DELETE r2
```

**Common Use Cases**:
- Removing specific relationships
- Deleting nodes without relationships
- Cleaning up temporary data
- Pruning graph structures

**Important**: Attempting to DELETE a node with relationships will fail. Use DETACH DELETE instead.

---

### DETACH DELETE

**Purpose**: Remove nodes along with all connected relationships in a single operation.

**Syntax**:
```cypher
DETACH DELETE variable [, ...]
```

**Key Features**:
- Automatically deletes all relationships connected to the node
- More convenient than manually deleting relationships first
- Can delete multiple nodes in one clause
- May be restricted for users with limited security privileges

**Examples**:

*Simple - Delete node and all its relationships*:
```cypher
MATCH (person:Person {name: 'John Doe'})
DETACH DELETE person
```

*Complex - Bulk deletion with filtering*:
```cypher
MATCH (user:User)
WHERE user.status = 'inactive'
  AND user.last_login < date() - duration('P365D')
WITH user
OPTIONAL MATCH (user)-[:CREATED]->(content)
DETACH DELETE user, content
```

**Common Use Cases**:
- Removing nodes and their relationships
- Cleaning up entire subgraphs
- Deleting user accounts and associated data
- Bulk pruning of graph sections

**Performance Note**: For large-scale deletion, use CALL subqueries in transactions rather than simple DETACH DELETE.

---

### SET

**Purpose**: Update labels on nodes and properties on nodes and relationships.

**Syntax**:
```cypher
SET variable.property = value
SET variable = {map}
SET variable += {map}
SET variable:Label
SET variable:Label1:Label2
```

**Key Features**:
- Set single or multiple properties
- Dynamic property keys with square bracket notation `variable[key]`
- Map-based operations: `=` (replace), `+=` (merge)
- Label management (single or multiple)
- Dynamic labels with `$(<expr>)` syntax
- Idempotent label operations
- Setting property to `null` removes it

**Examples**:

*Simple - Update properties*:
```cypher
MATCH (person:Person {name: 'Alice'})
SET person.age = 31,
    person.city = 'New York'
RETURN person
```

*Complex - Conditional updates with map operations*:
```cypher
MATCH (user:User)
WHERE user.status = 'pending'
SET user += {
      status: 'active',
      activated_at: datetime(),
      activated_by: $admin_id
    },
    user:ActiveUser,
    user.login_count = coalesce(user.login_count, 0) + 1
RETURN user
```

**Common Use Cases**:
- Updating node and relationship properties
- Adding or modifying labels
- Bulk property updates from maps
- Dynamic property updates from parameters
- Computed property values

**Note**: `SET n = {}` removes all properties from node `n`.

---

### REMOVE

**Purpose**: Remove properties from nodes and relationships, and remove labels from nodes. For deleting nodes and relationships entirely, use DELETE.

**Syntax**:
```cypher
REMOVE variable.property
REMOVE variable:Label
REMOVE variable:Label1:Label2
REMOVE variable[dynamic_key]
```

**Key Features**:
- Remove specific properties
- Remove labels from nodes
- Dynamic property removal with computed keys
- Dynamic label removal with expressions
- Idempotent operation (no error if property/label doesn't exist)

**Examples**:

*Simple - Remove property and label*:
```cypher
MATCH (person:Person {name: 'Alice'})
REMOVE person.temporary_flag,
       person:TempLabel
RETURN person
```

*Complex - Conditional removal with dynamic keys*:
```cypher
MATCH (product:Product)
WHERE product.discontinued = true
REMOVE product:Active,
       product:Featured,
       product.featured_until,
       product.promotion_price
SET product:Discontinued,
    product.discontinued_at = datetime()
RETURN product
```

**Common Use Cases**:
- Removing temporary or obsolete properties
- Cleaning up labels
- Removing properties that should no longer exist
- Data sanitization and cleanup

**Important**: Since Neo4j doesn't support null property values, REMOVE is the mechanism for eliminating property data.

---

## Reading/Writing Clauses

### MERGE

**Purpose**: Ensure that a pattern exists in the graph. Either the pattern already exists (and is matched), or it needs to be created. Combines MATCH and CREATE functionality.

**Syntax**:
```cypher
MERGE (variable:Label {properties})
MERGE (node1)-[:RELATIONSHIP_TYPE]->(node2)
  ON CREATE SET property = value
  ON MATCH SET property = value
```

**Key Features**:
- All-or-nothing semantics (entire pattern must match or entire pattern is created)
- No partial matches
- Supports ON CREATE and ON MATCH sub-clauses for conditional actions
- Can match multiple occurrences like MATCH
- Guarantees pattern existence but not uniqueness under concurrency
- Use property uniqueness constraints for uniqueness enforcement

**Examples**:

*Simple - Merge node with unique constraint*:
```cypher
MERGE (person:Person {email: 'alice@example.com'})
ON CREATE SET person.created_at = datetime(),
              person.name = 'Alice'
ON MATCH SET person.last_seen = datetime()
RETURN person
```

*Complex - Merge pattern with relationships*:
```cypher
MERGE (user:User {id: $user_id})
ON CREATE SET user.created_at = datetime(),
              user.name = $user_name
MERGE (product:Product {sku: $product_sku})
ON CREATE SET product.name = $product_name,
              product.created_at = datetime()
MERGE (user)-[r:VIEWED]->(product)
ON CREATE SET r.first_view = datetime(),
              r.view_count = 1
ON MATCH SET r.last_view = datetime(),
             r.view_count = r.view_count + 1
RETURN user, product, r
```

**Common Use Cases**:
- Ensuring entities exist before creating relationships
- Avoiding duplicate nodes
- Creating or updating based on existence
- Idempotent data loading operations
- Tracking creation vs. update metadata

**Performance Tip**: Create schema indexes on merged properties to significantly improve performance.

---

### CALL

**Purpose**: Invoke procedures deployed in the database and return any results. Supports both built-in and user-defined procedures.

**Syntax**:
```cypher
CALL procedureName(arguments) YIELD returnColumns
CALL procedureName YIELD *
OPTIONAL CALL procedureName(arguments) YIELD columns
```

**Key Features**:
- Invoke built-in or custom procedures
- YIELD specifies which output columns to return
- `YIELD *` returns all columns (standalone calls)
- Accepts literal values, parameters, or both
- OPTIONAL CALL returns null for empty results
- VOID procedures produce side effects only (no YIELD)

**Examples**:

*Simple - Call built-in procedure*:
```cypher
CALL db.labels() YIELD label
RETURN label
ORDER BY label
```

*Complex - Call procedure with arguments and filtering*:
```cypher
MATCH (person:Person {name: 'Tom Hanks'})
CALL apoc.neighbors.tohop(person, 'ACTED_IN>', 2)
YIELD node AS coactor
WHERE coactor:Person
WITH DISTINCT coactor,
     person
MATCH (coactor)-[:ACTED_IN]->(movie:Movie)<-[:ACTED_IN]-(person)
RETURN coactor.name,
       count(DISTINCT movie) AS shared_movies
ORDER BY shared_movies DESC
LIMIT 10
```

**Common Use Cases**:
- Listing database metadata (labels, property keys, indexes)
- Executing graph algorithms
- Performing complex operations via custom procedures
- Validating configuration settings
- Data transformation and processing

**Note**: Use `SHOW PROCEDURES` to discover available procedures and their signatures.

---

## Subquery Clauses

### CALL { } (Subqueries)

**Purpose**: Execute subqueries within a defined scope, enabling database modifications, post-union processing, and efficient resource management through scoping.

**Syntax**:
```cypher
CALL (imported_variables) {
  subquery
  RETURN results
}

CALL {
  WITH variable
  subquery
  RETURN results
}

OPTIONAL CALL (variables) {
  subquery
  RETURN results
}
```

**Key Features**:
- Execute once per incoming row
- Can perform CREATE, SET, DELETE operations
- Explicit variable importing with scope clause or importing WITH
- Returning subqueries influence output row count
- Unit subqueries (no RETURN) for modifications without affecting rows
- OPTIONAL CALL for optional execution (returns null on no match)
- Post-union processing support

**Examples**:

*Simple - Unit subquery for modifications*:
```cypher
MATCH (user:User)
WHERE user.status = 'pending'
CALL (user) {
  MATCH (user)-[:INVITED_BY]->(referrer:User)
  SET referrer.referral_count = coalesce(referrer.referral_count, 0) + 1
}
SET user.status = 'active'
RETURN user
```

*Complex - Post-union processing with aggregation*:
```cypher
CALL {
  MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
  RETURN p, 'actor' AS role, count(m) AS count
  UNION
  MATCH (p:Person)-[:DIRECTED]->(m:Movie)
  RETURN p, 'director' AS role, count(m) AS count
}
WITH p,
     collect({role: role, count: count}) AS roles
RETURN p.name,
       roles,
       reduce(total = 0, r IN roles | total + r.count) AS total_credits
ORDER BY total_credits DESC
LIMIT 10
```

**Common Use Cases**:
- Separating read and write operations with clear scope
- Post-union aggregations and processing
- Conditional logic within queries
- Graph modifications without affecting row counts
- Efficient memory management for large datasets

**Performance Benefit**: Scoping ensures temporary data structures don't persist beyond their utility, reducing memory overhead.

---

## Set Operations

### UNION

**Purpose**: Combine results from multiple queries, removing duplicate rows. Column names, types, and count must match across all queries.

**Syntax**:
```cypher
query1
UNION
query2
```

**Key Features**:
- Combines results from multiple queries
- Removes duplicate rows automatically
- Requires matching column names and types
- All queries must return same number of columns

**Examples**:

*Simple - Union of two node types*:
```cypher
MATCH (p:Person)
RETURN p.name AS name, 'Person' AS type
UNION
MATCH (c:Company)
RETURN c.name AS name, 'Company' AS type
```

*Complex - Union with aggregation*:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
WHERE movie.year = 2020
RETURN actor.name AS name,
       'Actor in 2020' AS category,
       count(movie) AS count
UNION
MATCH (director:Person)-[:DIRECTED]->(movie:Movie)
WHERE movie.year = 2020
RETURN director.name AS name,
       'Director in 2020' AS category,
       count(movie) AS count
ORDER BY count DESC
```

**Common Use Cases**:
- Combining results from different node types
- Querying multiple relationship types
- Combining filtered results
- Creating unified views of heterogeneous data

**Important**: UNION removes duplicates. Use UNION ALL to retain duplicates.

---

### UNION ALL

**Purpose**: Combine results from multiple queries while retaining all duplicate rows. Faster than UNION since it doesn't perform deduplication.

**Syntax**:
```cypher
query1
UNION ALL
query2
```

**Key Features**:
- Combines results from multiple queries
- Retains all duplicate rows
- Faster than UNION (no deduplication overhead)
- Requires matching column names, types, and count

**Examples**:

*Simple - Union all with duplicates preserved*:
```cypher
MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
RETURN p.name AS name, m.title AS work
UNION ALL
MATCH (p:Person)-[:DIRECTED]->(m:Movie)
RETURN p.name AS name, m.title AS work
```

*Complex - Comprehensive activity log*:
```cypher
MATCH (user:User)-[:CREATED]->(post:Post)
RETURN user.id AS user_id,
       'created_post' AS action,
       post.id AS target_id,
       post.created_at AS timestamp
UNION ALL
MATCH (user:User)-[:LIKED]->(post:Post)
RETURN user.id AS user_id,
       'liked_post' AS action,
       post.id AS target_id,
       post.liked_at AS timestamp
UNION ALL
MATCH (user:User)-[:COMMENTED_ON]->(post:Post)
RETURN user.id AS user_id,
       'commented' AS action,
       post.id AS target_id,
       post.comment_at AS timestamp
ORDER BY timestamp DESC
LIMIT 100
```

**Common Use Cases**:
- Creating activity feeds or event logs
- Combining similar query results where duplicates are meaningful
- Performance optimization when deduplication is unnecessary
- Merging time-series or event data

**Performance Note**: Use UNION ALL instead of UNION when duplicates are acceptable or desired, as it avoids the overhead of deduplication.

---

## Additional Clauses

### FOREACH

**Purpose**: Update data within a list, whether components of a path or the result of aggregation. Performs iteration-based modifications.

**Syntax**:
```cypher
FOREACH (variable IN list |
  update_operations
)
```

**Key Features**:
- Iterates over lists to perform updates
- Cannot return values (side effects only)
- Useful for batch updates
- Can create, set, delete, or merge within loop

**Example**:
```cypher
MATCH path = (start:Node)-[*]->(end:Node)
WHERE start.id = $start_id AND end.id = $end_id
FOREACH (node IN nodes(path) |
  SET node.visited = true,
      node.visited_at = datetime()
)
```

**Common Use Cases**:
- Marking nodes in a path
- Batch property updates
- Creating multiple relationships from a list
- Processing aggregated results

---

### USE

**Purpose**: Determine which graph a query or query section executes against. For systems supporting multiple graphs.

**Syntax**:
```cypher
USE graph_name
query
```

**Note**: This clause is specific to Neo4j implementations with multiple graph support and may not be part of core OpenCypher.

---

### LOAD CSV

**Purpose**: Import data from CSV files. Facilitates external data integration.

**Syntax**:
```cypher
LOAD CSV [WITH HEADERS] FROM 'file_url' AS row
CREATE/MERGE operations
```

**Example**:
```cypher
LOAD CSV WITH HEADERS FROM 'file:///data/people.csv' AS row
CREATE (p:Person {
  name: row.name,
  age: toInteger(row.age),
  email: row.email
})
```

**Common Use Cases**:
- Initial data import
- Batch loading from external sources
- Migrating data into graph database

**Note**: This is a data import clause and may have specific implementation details in Neo4j.

---

## Query Composition

OpenCypher clauses can be composed to create powerful, expressive queries:

1. **Reading + Filtering + Returning**:
```cypher
MATCH (p:Person)
WHERE p.age > 30
RETURN p.name, p.age
ORDER BY p.age DESC
LIMIT 10
```

2. **Chaining with WITH**:
```cypher
MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
WITH p, count(m) AS movie_count
WHERE movie_count > 5
MATCH (p)-[:DIRECTED]->(directed:Movie)
RETURN p.name, movie_count, count(directed) AS directed_count
```

3. **Writing + Reading**:
```cypher
CREATE (p:Person {name: 'Alice', age: 30})
WITH p
MATCH (p)-[:KNOWS*1..2]-(friend:Person)
RETURN friend.name
```

4. **Complex Pattern Matching**:
```cypher
MATCH path = (start:Person)-[:KNOWS*1..3]->(end:Person)
WHERE start.name = 'Alice' AND end.name = 'Bob'
RETURN [node IN nodes(path) | node.name] AS path_names,
       length(path) AS hops
ORDER BY hops
LIMIT 1
```

---

## Summary

OpenCypher provides a rich set of clauses for reading, writing, filtering, and transforming graph data:

- **Reading**: MATCH, OPTIONAL MATCH
- **Writing**: CREATE, DELETE, DETACH DELETE, SET, REMOVE
- **Reading/Writing**: MERGE, CALL
- **Projecting**: RETURN, WITH, UNWIND
- **Filtering/Sorting**: WHERE, ORDER BY, LIMIT, SKIP
- **Set Operations**: UNION, UNION ALL
- **Subqueries**: CALL { }

These clauses combine to form a declarative, expressive query language for graph databases that is both powerful and readable.

---

## References

- **OpenCypher Official Site**: [https://opencypher.org/](https://opencypher.org/)
- **OpenCypher Specification**: [https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf)
- **Neo4j Cypher Manual**: [https://neo4j.com/docs/cypher-manual/current/](https://neo4j.com/docs/cypher-manual/current/)
- **Neo4j Clauses Reference**: [https://neo4j.com/docs/cypher-manual/current/clauses/](https://neo4j.com/docs/cypher-manual/current/clauses/)

---

**Document Version**: 1.0
**Last Updated**: 2026-02-16
**OpenCypher Version**: Based on OpenCypher 9 and Neo4j Cypher Manual (Current)
