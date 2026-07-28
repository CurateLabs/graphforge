# OpenCypher Pattern Matching

This document provides a comprehensive reference for all pattern matching features in the OpenCypher specification. Pattern matching is the core mechanism for navigating, describing, and extracting data from graph databases using declarative patterns.

**OpenCypher Reference**: [https://opencypher.org/](https://opencypher.org/)

**Neo4j Cypher Manual**: [https://neo4j.com/docs/cypher-manual/current/patterns/](https://neo4j.com/docs/cypher-manual/current/patterns/)

---

## Table of Contents

- [Pattern Fundamentals](#pattern-fundamentals)
- [Node Patterns](#node-patterns)
- [Relationship Patterns](#relationship-patterns)
- [Path Patterns](#path-patterns)
- [Variable-Length Patterns](#variable-length-patterns)
- [Pattern Predicates](#pattern-predicates)
- [Optional Patterns](#optional-patterns)
- [Pattern Comprehension](#pattern-comprehension)
- [Multiple Patterns](#multiple-patterns)
- [Pattern Matching Semantics](#pattern-matching-semantics)
- [Pattern Performance Considerations](#pattern-performance-considerations)

---

## Pattern Fundamentals

Graph pattern matching sits at the very core of Cypher. It uses a visual notation that mirrors whiteboard diagrams:

- **Nodes** are represented as parentheses: `()`
- **Relationships** are represented as dashes and arrows: `-->`, `<--`, or `--`

**Basic Pattern Syntax**:
```cypher
()-->()<--()
```

**Purpose**: Patterns enable declarative graph queries without explicit algorithmic specification. You describe what structure you want to match, and the database engine determines how to find it.

**Key Principle**: Patterns are used primarily in `MATCH` clauses and pattern-based subqueries (`EXISTS`, `COUNT`, `COLLECT`).

---

## Node Patterns

Node patterns match individual nodes in the graph and can specify labels, properties, and variables.

### Syntax

```
([ variable ] [ labelExpression ] [ properties ] [ WHERE clause ])
```

### Basic Node Patterns

**Empty node pattern** - Matches any node:
```cypher
MATCH (n)
RETURN n
LIMIT 10
```

**Node with variable** - Binds matched node to a variable:
```cypher
MATCH (person)
RETURN person
```

**Anonymous node** - Matches node without binding to variable:
```cypher
MATCH ()-->(n)
RETURN n
```

### Node Patterns with Labels

**Single label** - Matches nodes with specific label:
```cypher
MATCH (p:Person)
RETURN p.name
```

**Multiple labels (conjunction)** - Matches nodes with ALL specified labels:
```cypher
MATCH (e:Person:Employee)
RETURN e.name, e.employee_id
```

Alternative syntax using `&`:
```cypher
MATCH (e:Person&Employee)
RETURN e.name
```

**Label disjunction** - Matches nodes with ANY of the specified labels:
```cypher
MATCH (entity:Person|Company)
RETURN entity.name, labels(entity) AS type
```

**Label negation** - Matches nodes WITHOUT specific label:
```cypher
MATCH (n:!Person)
RETURN n
LIMIT 10
```

**Complex label expressions** - Combines operators for advanced matching:
```cypher
// Matches labeled nodes that are not Person
MATCH (n:!Person&%)
RETURN n

// Matches nodes with (A OR B) AND NOT C
MATCH (n:(A|B)&!C)
RETURN n
```

**Label Expression Operators**:
| Operator | Meaning | Precedence |
|----------|---------|-----------|
| `%` | Wildcard (any non-empty label set) | 1 |
| `()` | Grouping | 1 |
| `!` | Negation | 2 |
| `&` | Conjunction (AND) | 3 |
| `\|` | Disjunction (OR) | 4 |

### Node Patterns with Properties

**Single property match** - Matches nodes with specific property value:
```cypher
MATCH (p:Person {name: 'Alice'})
RETURN p
```

**Multiple properties** - All properties must match (conjunction):
```cypher
MATCH (p:Person {name: 'Alice', age: 30, country: 'USA'})
RETURN p
```

**Property expressions** - Use computed values:
```cypher
MATCH (m:Movie {year: date().year - 1})
RETURN m.title
```

**Empty property map** - Matches nodes without filtering by properties:
```cypher
MATCH (p:Person {})
RETURN p
```

### Node Patterns with WHERE Clause

**Inline WHERE predicate** - Filters during pattern matching:
```cypher
MATCH (p:Person WHERE p.age > 30 AND p.country = 'USA')
RETURN p.name, p.age
```

**Complex predicates** - Use functions and expressions:
```cypher
MATCH (m:Movie WHERE m.year >= 2000 AND m.rating > 7.5)
RETURN m.title, m.year, m.rating
ORDER BY m.rating DESC
```

### Common Node Pattern Use Cases

1. **Finding nodes by type**:
```cypher
MATCH (actor:Actor)
RETURN count(actor) AS total_actors
```

2. **Filtering by properties**:
```cypher
MATCH (product:Product {category: 'Electronics', in_stock: true})
RETURN product.name, product.price
ORDER BY product.price
```

3. **Complex filtering with WHERE**:
```cypher
MATCH (user:User WHERE user.created_at > datetime('2024-01-01') AND user.status = 'active')
RETURN user.email, user.created_at
```

4. **Multiple label requirements**:
```cypher
MATCH (contractor:Person:Contractor)
WHERE contractor.active = true
RETURN contractor.name, contractor.rate
```

---

## Relationship Patterns

Relationship patterns connect nodes and specify direction, type, properties, and traversal constraints.

### Syntax

```
-[ variable ][ :type ][ properties ][ WHERE clause ]->
<-[ variable ][ :type ][ properties ][ WHERE clause ]-
-[ variable ][ :type ][ properties ][ WHERE clause ]-
```

### Basic Relationship Patterns

**Directed relationship (right)** - Matches relationships pointing right:
```cypher
MATCH (a)-->(b)
RETURN a, b
```

**Directed relationship (left)** - Matches relationships pointing left:
```cypher
MATCH (a)<--(b)
RETURN a, b
```

**Undirected relationship** - Matches relationships in either direction:
```cypher
MATCH (a)--(b)
RETURN a, b
```

**Relationship with variable** - Binds relationship to variable:
```cypher
MATCH (a)-[r]->(b)
RETURN type(r), r.properties
```

### Relationship Patterns with Types

**Single type** - Matches relationships of specific type:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
RETURN actor.name, movie.title
```

**Multiple types (disjunction)** - Matches ANY of the specified types:
```cypher
MATCH (person:Person)-[:ACTED_IN|:DIRECTED]->(movie:Movie)
RETURN person.name, type(r) AS role, movie.title
```

**Type with variable**:
```cypher
MATCH (a)-[r:KNOWS]->(b)
RETURN r.since, r.relationship_type
```

### Relationship Patterns with Properties

**Single property match**:
```cypher
MATCH (a)-[r:KNOWS {since: 2020}]->(b)
RETURN a.name, b.name, r.since
```

**Multiple properties**:
```cypher
MATCH (user)-[r:PURCHASED {status: 'completed', payment_method: 'credit_card'}]->(product)
RETURN user.name, product.name, r.amount
```

**Property constraints with WHERE**:
```cypher
MATCH (a)-[r:KNOWS WHERE r.since < 2000]->(b)
RETURN a.name AS person, b.name AS friend, r.since
ORDER BY r.since
```

### Directional Pattern Variations

**Pattern 1: Left to right**:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
RETURN actor.name, movie.title
```

**Pattern 2: Right to left**:
```cypher
MATCH (movie:Movie)<-[:ACTED_IN]-(actor:Person)
RETURN actor.name, movie.title
```

**Pattern 3: Bidirectional (either direction)**:
```cypher
MATCH (person1:Person)-[:KNOWS]-(person2:Person)
WHERE person1.name = 'Alice'
RETURN person2.name
```

**Pattern 4: Chain of relationships**:
```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE a.name = 'Alice'
RETURN a.name, b.name, c.name
```

### Common Relationship Pattern Use Cases

1. **Finding related entities**:
```cypher
MATCH (actor:Person {name: 'Tom Hanks'})-[:ACTED_IN]->(movie:Movie)
RETURN movie.title, movie.year
ORDER BY movie.year DESC
```

2. **Multiple relationship types**:
```cypher
MATCH (person:Person)-[r:ACTED_IN|:DIRECTED|:PRODUCED]->(movie:Movie)
WHERE movie.year = 2020
RETURN person.name, type(r) AS role, movie.title
```

3. **Relationship property filtering**:
```cypher
MATCH (user:User)-[r:RATED]->(movie:Movie)
WHERE r.rating >= 4.0
RETURN user.name, movie.title, r.rating
ORDER BY r.rating DESC
```

4. **Bidirectional friendships**:
```cypher
MATCH (me:Person {name: 'Alice'})-[:FRIENDS]-(friend:Person)
RETURN friend.name
ORDER BY friend.name
```

---

## Path Patterns

Path patterns match sequences of nodes and relationships as cohesive units, enabling path-level operations and analysis.

### Syntax

```cypher
pathVariable = (node)-[relationship]->(node)
```

### Basic Path Patterns

**Simple path assignment**:
```cypher
MATCH path = (a:Person)-[:KNOWS]->(b:Person)
WHERE a.name = 'Alice'
RETURN path
```

**Path with multiple relationships**:
```cypher
MATCH path = (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
RETURN path, length(path) AS hops
```

**Path with mixed relationship types**:
```cypher
MATCH path = (actor:Person)-[:ACTED_IN]->(movie:Movie)<-[:DIRECTED]-(director:Person)
RETURN path, nodes(path), relationships(path)
```

### Path Functions

**Length of path** - Number of relationships in path:
```cypher
MATCH path = (a:Person)-[:KNOWS*]->(b:Person)
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN path, length(path) AS degrees_of_separation
ORDER BY length(path)
LIMIT 1
```

**Nodes in path** - Extract all nodes:
```cypher
MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.name = 'Alice'
RETURN [node IN nodes(path) | node.name] AS path_names
```

**Relationships in path** - Extract all relationships:
```cypher
MATCH path = (a)-[*]->(b)
WHERE a.name = 'Start' AND b.name = 'End'
RETURN [rel IN relationships(path) | type(rel)] AS relationship_types
```

### Path Pattern Use Cases

1. **Finding shortest path**:
```cypher
MATCH path = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]-(b:Person {name: 'Bob'}))
RETURN path, length(path) AS distance
```

2. **Analyzing path properties**:
```cypher
MATCH path = (start:City {name: 'New York'})-[:ROAD*]->(end:City {name: 'Los Angeles'})
WITH path, relationships(path) AS rels
RETURN path,
       reduce(distance = 0, r IN rels | distance + r.miles) AS total_miles
ORDER BY total_miles
LIMIT 1
```

3. **Extracting path information**:
```cypher
MATCH path = (a:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(city:City)
WHERE a.name = 'Alice'
RETURN nodes(path) AS all_nodes,
       relationships(path) AS all_relationships,
       length(path) AS path_length
```

4. **Path existence checking**:
```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
RETURN EXISTS {
  MATCH path = (a)-[:KNOWS*]-(b)
} AS are_connected
```

---

## Variable-Length Patterns

Variable-length patterns match paths of unknown or varying lengths using quantifiers and repetition syntax.

### Quantified Path Patterns

**Modern GQL-conformant syntax**:
```
((pattern)){min,max}
```

**One or more repetitions** (`+` quantifier):
```cypher
MATCH (a:Person)-[:KNOWS]->+(:Person)
WHERE a.name = 'Alice'
RETURN count(*) AS connections
```

**Zero or more repetitions** (`*` quantifier):
```cypher
MATCH (a:Person)-[:KNOWS]->*(:Person)
WHERE a.name = 'Alice'
RETURN count(*) AS all_paths
```

**Exact repetition count**:
```cypher
MATCH (a:Person) ((()-[:KNOWS]->())){3} (b:Person)
RETURN a.name, b.name
```

**Bounded repetition range**:
```cypher
MATCH (a:Person) ((()-[:KNOWS]->())){1,3} (b:Person)
WHERE a.name = 'Alice'
RETURN b.name, count(*) AS path_count
```

### Quantified Relationships

**Simplified syntax for single relationships**:
```cypher
-[:TYPE]->{min,max}
```

**Examples**:
```cypher
// 1 to 3 hops
MATCH (a:Person)-[:KNOWS]->{1,3}(b:Person)
RETURN a.name, b.name

// Exactly 2 hops
MATCH (a)-[:FOLLOWS]->{2}(b)
RETURN a, b

// At least 2 hops (unbounded)
MATCH (a)-[:PARENT_OF]->{2,}(descendant)
RETURN a.name AS ancestor, descendant.name
```

### Legacy Variable-Length Syntax

**Asterisk notation** (older syntax, still supported):
```cypher
-[*]->      // One or more hops
-[*1..3]->  // 1 to 3 hops
-[*..5]->   // Up to 5 hops
-[*2..]->   // 2 or more hops
```

**Examples with legacy syntax**:
```cypher
// Variable-length with type
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.name = 'Alice'
RETURN DISTINCT b.name

// Unbounded traversal (use with caution)
MATCH (root:Category {name: 'Electronics'})-[:SUBCATEGORY*]->(leaf:Category)
WHERE NOT EXISTS { (leaf)-[:SUBCATEGORY]->() }
RETURN leaf.name AS leaf_category

// Variable-length with relationship variable
MATCH (a:City)-[roads:ROAD*1..5]->(b:City)
WHERE a.name = 'New York' AND b.name = 'Boston'
RETURN reduce(distance = 0, r IN roads | distance + r.miles) AS total_distance
ORDER BY total_distance
LIMIT 1
```

### Group Variables

Variables declared inside quantified patterns become **group variables** (lists):

```cypher
MATCH (a:Person) ((l:Person)-[r:KNOWS]->(m:Person)){1,3} (b:Person)
WHERE a.name = 'Alice'
RETURN a.name,
       [node IN l | node.name] AS intermediate_people,
       [rel IN r | rel.since] AS relationship_years,
       b.name
```

**Key behavior**:
- `l`, `r`, `m` are lists containing all matched elements across repetitions
- Enables aggregation across path segments
- Useful for property accumulation and filtering

### Variable-Length Pattern Use Cases

1. **Social network degrees of separation**:
```cypher
MATCH (alice:Person {name: 'Alice'})-[:KNOWS*1..6]-(bob:Person {name: 'Bob'})
RETURN shortestPath((alice)-[:KNOWS*]-(bob)) AS shortest_connection
```

2. **Organizational hierarchy traversal**:
```cypher
MATCH (ceo:Employee {title: 'CEO'})-[:MANAGES*]->(report:Employee)
RETURN report.name, report.title,
       length((ceo)-[:MANAGES*]->(report)) AS levels_down
ORDER BY levels_down, report.name
```

3. **Category tree navigation**:
```cypher
MATCH (root:Category {name: 'Products'})-[:CONTAINS*0..10]->(category:Category)
RETURN category.name, category.level
```

4. **Finding all reachable nodes**:
```cypher
MATCH (start:City {name: 'San Francisco'})-[:FLIGHT*]->(reachable:City)
RETURN DISTINCT reachable.name AS destination
ORDER BY destination
```

5. **Path accumulation with group variables**:
```cypher
MATCH ((a)-[r:ROAD]->(b)){1,5}
WHERE a.name = 'Start' AND b.name = 'End'
RETURN reduce(total = 0, rel IN r | total + rel.distance) AS total_distance
ORDER BY total_distance
LIMIT 1
```

### Performance Considerations

**Inline predicates** - Filter during traversal to prevent path explosion:
```cypher
MATCH (a:Person) ((:Person)-[:KNOWS WHERE r.since > 2010]->(:Person)){1,3} (b:Person)
RETURN a.name, b.name
```

**Bounded vs. unbounded**:
- Always prefer bounded quantifiers (`{1,5}`) over unbounded (`*`, `+`)
- Unbounded traversals can cause exponential performance degradation
- Use `LIMIT` to constrain result sets

**Label filtering**:
```cypher
// Good - filters early
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE b.country = 'USA'
RETURN b.name

// Better - inline filtering
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person WHERE b.country = 'USA')
RETURN b.name
```

---

## Pattern Predicates

Pattern predicates are filtering expressions applied directly within patterns, evaluated during pattern matching rather than after.

### Inline WHERE in Patterns

**Node pattern predicates**:
```cypher
MATCH (p:Person WHERE p.age > 30 AND p.country = 'USA')
RETURN p.name, p.age
```

**Relationship pattern predicates**:
```cypher
MATCH (a:Person)-[r:KNOWS WHERE r.since < 2000]->(b:Person)
RETURN a.name, b.name, r.since
```

**Quantified pattern predicates**:
```cypher
MATCH (a:Person) (()-[r:KNOWS WHERE r.since > 2010]->()) {1,3} (b:Person)
RETURN a.name, b.name
```

### EXISTS Subqueries

**Pattern existence check**:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
WHERE movie.year >= 2000
  AND EXISTS {
    MATCH (actor)-[:ACTED_IN]->(other:Movie)
    WHERE other.year < 2000
  }
RETURN actor.name,
       count(movie) AS recent_movies
ORDER BY recent_movies DESC
```

**Negated existence** - Nodes without certain patterns:
```cypher
MATCH (person:Person)
WHERE NOT EXISTS {
  MATCH (person)-[:ACTED_IN]->(:Movie)
}
RETURN person.name AS non_actor
```

**Multiple existence checks**:
```cypher
MATCH (person:Person)
WHERE EXISTS {
  MATCH (person)-[:ACTED_IN]->(:Movie)
}
AND EXISTS {
  MATCH (person)-[:DIRECTED]->(:Movie)
}
RETURN person.name AS actor_director
```

### COUNT Subqueries

**Counting patterns**:
```cypher
MATCH (person:Person)
WHERE COUNT {
  MATCH (person)-[:ACTED_IN]->(:Movie)
} > 10
RETURN person.name, COUNT {
  MATCH (person)-[:ACTED_IN]->(:Movie)
} AS movie_count
ORDER BY movie_count DESC
```

**Conditional filtering with counts**:
```cypher
MATCH (user:User)
WHERE COUNT {
  MATCH (user)-[:PURCHASED]->(product:Product)
  WHERE product.category = 'Electronics'
} >= 3
RETURN user.name, user.email
```

### Pattern Predicate Use Cases

1. **Complex filtering during traversal**:
```cypher
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person WHERE b.age > 25 AND b.city = 'New York')
WHERE a.name = 'Alice'
RETURN DISTINCT b.name
```

2. **Existence-based filtering**:
```cypher
MATCH (product:Product)
WHERE NOT EXISTS {
  MATCH (product)-[:PURCHASED_BY]->(:Customer)
}
RETURN product.name AS never_purchased
```

3. **Relationship property constraints**:
```cypher
MATCH (user:User)-[r:RATED WHERE r.rating >= 4]->(movie:Movie)
WHERE movie.year >= 2020
RETURN user.name, movie.title, r.rating
ORDER BY r.rating DESC, movie.title
```

4. **Multi-condition pattern existence**:
```cypher
MATCH (movie:Movie)
WHERE movie.year = 2020
  AND EXISTS {
    MATCH (actor:Person {country: 'USA'})-[:ACTED_IN]->(movie)
  }
  AND EXISTS {
    MATCH (director:Person {country: 'UK'})-[:DIRECTED]->(movie)
  }
RETURN movie.title AS international_collaboration
```

---

## Optional Patterns

Optional patterns use `OPTIONAL MATCH` to match patterns that may not exist, returning `null` for missing parts instead of eliminating rows.

### Basic OPTIONAL MATCH

**Simple optional relationship**:
```cypher
MATCH (person:Person)
OPTIONAL MATCH (person)-[:ACTED_IN]->(movie:Movie)
RETURN person.name,
       movie.title,
       CASE WHEN movie IS NULL THEN 'Not an actor' ELSE 'Actor' END AS status
```

**Multiple optional patterns**:
```cypher
MATCH (person:Person {name: 'Tom Hanks'})
OPTIONAL MATCH (person)-[:ACTED_IN]->(movie:Movie)
OPTIONAL MATCH (person)-[:DIRECTED]->(directed:Movie)
RETURN person.name,
       collect(DISTINCT movie.title) AS acted_in,
       collect(DISTINCT directed.title) AS directed
```

### Optional Patterns with WHERE

**Filtering optional patterns**:
```cypher
MATCH (person:Person)
OPTIONAL MATCH (person)-[:ACTED_IN]->(movie:Movie)
WHERE movie.year > 2010
RETURN person.name,
       count(movie) AS recent_movies
```

**Note**: WHERE predicates in OPTIONAL MATCH are evaluated during pattern matching, not after. This affects which rows contain null values.

### Combining Required and Optional

**Mixed pattern matching**:
```cypher
MATCH (user:User)
WHERE user.status = 'active'
OPTIONAL MATCH (user)-[:PURCHASED]->(product:Product)
OPTIONAL MATCH (user)-[review:REVIEWED]->(product)
RETURN user.name,
       collect(DISTINCT product.name) AS purchased_products,
       collect(DISTINCT review.rating) AS reviews
ORDER BY user.name
```

**Nested optional patterns**:
```cypher
MATCH (company:Company)
OPTIONAL MATCH (company)<-[:WORKS_AT]-(employee:Employee)
OPTIONAL MATCH (employee)-[:MANAGES]->(report:Employee)
RETURN company.name,
       count(DISTINCT employee) AS employee_count,
       count(DISTINCT report) AS managed_count
```

### Optional Pattern Use Cases

1. **Outer join behavior**:
```cypher
MATCH (person:Person)
OPTIONAL MATCH (person)-[:LIVES_IN]->(city:City)
RETURN person.name,
       coalesce(city.name, 'Unknown') AS city
ORDER BY city, person.name
```

2. **Conditional data retrieval**:
```cypher
MATCH (product:Product)
OPTIONAL MATCH (product)<-[r:PURCHASED]-(customer:Customer)
WHERE r.purchased_at > datetime() - duration('P30D')
RETURN product.name,
       count(customer) AS recent_purchases
ORDER BY recent_purchases DESC
```

3. **Checking relationship existence**:
```cypher
MATCH (user:User)
OPTIONAL MATCH (user)-[:VERIFIED]->()
RETURN user.email,
       CASE WHEN COUNT {MATCH (user)-[:VERIFIED]->()} > 0
            THEN 'Verified'
            ELSE 'Unverified'
       END AS verification_status
```

4. **Exploring incomplete schemas**:
```cypher
MATCH (node)
OPTIONAL MATCH (node)-[r]->()
RETURN labels(node) AS node_type,
       collect(DISTINCT type(r)) AS outgoing_relationship_types
```

---

## Pattern Comprehension

Pattern comprehension provides list-based operations over pattern matches, similar to list comprehensions in Python.

### Syntax

```cypher
[ pattern WHERE condition | projection ]
```

### Basic Pattern Comprehension

**Simple comprehension**:
```cypher
MATCH (person:Person)
RETURN person.name,
       [(person)-[:ACTED_IN]->(movie:Movie) | movie.title] AS movies
```

**With WHERE filtering**:
```cypher
MATCH (person:Person)
RETURN person.name,
       [(person)-[:ACTED_IN]->(m:Movie) WHERE m.year > 2010 | m.title] AS recent_movies
```

**Complex projections**:
```cypher
MATCH (actor:Person)
WHERE actor.name = 'Tom Hanks'
RETURN [(actor)-[:ACTED_IN]->(m:Movie) | {title: m.title, year: m.year, rating: m.rating}] AS filmography
```

### Pattern Comprehension Use Cases

1. **Collecting related data**:
```cypher
MATCH (user:User)
RETURN user.name,
       [(user)-[:PURCHASED]->(p:Product) | p.name] AS purchase_history,
       [(user)-[:REVIEWED]->(p:Product) | {product: p.name, rating: r.score}] AS reviews
```

2. **Filtered collection**:
```cypher
MATCH (company:Company)
RETURN company.name,
       [(company)<-[:WORKS_AT]-(e:Employee) WHERE e.department = 'Engineering' | e.name] AS engineers
```

3. **Aggregating across relationships**:
```cypher
MATCH (person:Person)
RETURN person.name,
       [(person)-[r:KNOWS WHERE r.since < 2000]->(friend:Person) | friend.name] AS old_friends,
       [(person)-[r:KNOWS WHERE r.since >= 2000]->(friend:Person) | friend.name] AS new_friends
```

4. **Nested pattern comprehension**:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(:Movie)
RETURN actor.name,
       [(actor)-[:ACTED_IN]->(m:Movie) |
         {
           title: m.title,
           co_actors: [(actor)-[:ACTED_IN]->(m)<-[:ACTED_IN]-(co:Person) | co.name]
         }
       ] AS movies_with_co_actors
LIMIT 5
```

---

## Multiple Patterns

Multiple patterns can be combined in a single MATCH clause or across multiple MATCH clauses, with different semantics.

### Multiple Patterns in Single MATCH

**Comma-separated patterns** - All must match for row to be returned:
```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person),
      (b)-[:KNOWS]->(c:Person)
WHERE a.name = 'Alice'
RETURN a.name, b.name, c.name
```

**Equivalent to chained pattern**:
```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE a.name = 'Alice'
RETURN a.name, b.name, c.name
```

**Independent patterns**:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie),
      (director:Person)-[:DIRECTED]->(movie)
RETURN actor.name AS actor,
       director.name AS director,
       movie.title
```

### Multiple MATCH Clauses

**Sequential matching** - Each MATCH builds on previous:
```cypher
MATCH (person:Person {name: 'Alice'})
MATCH (person)-[:KNOWS]->(friend:Person)
MATCH (friend)-[:LIVES_IN]->(city:City)
RETURN friend.name, city.name
```

**Cartesian product** - Independent MATCH clauses:
```cypher
MATCH (actor:Person)-[:ACTED_IN]->(:Movie)
MATCH (director:Person)-[:DIRECTED]->(:Movie)
RETURN DISTINCT actor.name, director.name
LIMIT 10
```

### Multiple Pattern Use Cases

1. **Finding mutual connections**:
```cypher
MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(mutual:Person),
      (b:Person {name: 'Bob'})-[:KNOWS]->(mutual)
RETURN mutual.name AS mutual_friend
```

2. **Complex relationships**:
```cypher
MATCH (student:Person)-[:ENROLLED_IN]->(course:Course),
      (professor:Person)-[:TEACHES]->(course),
      (course)-[:PART_OF]->(program:Program)
WHERE student.name = 'Alice'
RETURN course.name, professor.name, program.name
```

3. **Independent pattern matching**:
```cypher
MATCH (popular:Movie)
WHERE COUNT {(popular)<-[:RATED]-(:User)} > 1000
MATCH (recent:Movie)
WHERE recent.year = date().year
RETURN popular.title AS popular_movie,
       recent.title AS recent_movie
```

---

## Pattern Matching Semantics

Understanding how patterns are evaluated is crucial for writing correct and efficient queries.

### Uniqueness Constraints

**Relationship uniqueness** (default): Each relationship can appear at most once in a single pattern match:
```cypher
// r1, r2, r3 are guaranteed to be different relationships
MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d)
RETURN count(*)
```

**Node repetition allowed**: The same node can appear multiple times:
```cypher
// a and c can be the same node
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE a.name = 'Alice'
RETURN a.name, b.name, c.name
```

### Pattern Binding

**Variable scope**: Variables bound in patterns are available in subsequent clauses:
```cypher
MATCH (person:Person)
WHERE person.age > 30
RETURN person.name  // person is bound and accessible
```

**Pattern reuse**: Variables from one pattern can be referenced in another:
```cypher
MATCH (a:Person {name: 'Alice'})
MATCH (a)-[:KNOWS]->(friend:Person)  // reuses 'a' from previous MATCH
RETURN friend.name
```

### Null Handling

**Property access on null**:
```cypher
MATCH (person:Person)
OPTIONAL MATCH (person)-[:LIVES_IN]->(city:City)
RETURN person.name,
       city.name  // null if city doesn't exist
```

**Null in comparisons**:
```cypher
MATCH (person:Person)
OPTIONAL MATCH (person)-[:AGED]->(age)
WHERE age.value > 30  // null ages are excluded (ternary logic)
RETURN person.name
```

### Pattern Evaluation Order

**Left-to-right evaluation**:
```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
// Evaluated as: Find a, then find b connected to a, then find c connected to b
```

**Optimization**: The query planner may reorder evaluation for performance, but results are logically equivalent.

---

## Pattern Performance Considerations

Writing efficient patterns is essential for query performance, especially on large graphs.

### Indexing

**Create indexes on frequently matched properties**:
```cypher
// Recommended for Person.name lookups
CREATE INDEX person_name FOR (p:Person) ON (p.name)

MATCH (p:Person {name: 'Alice'})  // Uses index
RETURN p
```

**Composite indexes for multiple properties**:
```cypher
CREATE INDEX movie_year_rating FOR (m:Movie) ON (m.year, m.rating)

MATCH (m:Movie {year: 2020})
WHERE m.rating > 8.0  // Uses composite index
RETURN m.title
```

### Pattern Anchoring

**Anchor patterns with specific nodes**:
```cypher
// Good - starts with specific node
MATCH (alice:Person {name: 'Alice'})-[:KNOWS*1..3]->(friend:Person)
RETURN friend.name

// Bad - starts with unbounded scan
MATCH (person:Person)-[:KNOWS*1..3]->(friend:Person)
WHERE person.name = 'Alice'
RETURN friend.name
```

### Variable-Length Bounds

**Always bound variable-length patterns**:
```cypher
// Good - bounded
MATCH (a)-[:KNOWS*1..5]->(b)
RETURN count(*)

// Dangerous - unbounded, can explode
MATCH (a)-[:KNOWS*]->(b)
RETURN count(*)
```

### Early Filtering

**Filter as early as possible**:
```cypher
// Good - inline filtering
MATCH (p:Person WHERE p.country = 'USA')-[:KNOWS]->(f:Person WHERE f.age > 30)
RETURN p.name, f.name

// Less efficient - filtering after matching
MATCH (p:Person)-[:KNOWS]->(f:Person)
WHERE p.country = 'USA' AND f.age > 30
RETURN p.name, f.name
```

### LIMIT Usage

**Use LIMIT to constrain result sets**:
```cypher
MATCH (person:Person)
RETURN person.name
ORDER BY person.name
LIMIT 100  // Prevents returning millions of rows
```

### Pattern Specificity

**Be as specific as possible**:
```cypher
// Good - specific labels and types
MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
RETURN count(*)

// Slower - generic patterns
MATCH (a)-[r]->(b)
WHERE 'Person' IN labels(a) AND 'Movie' IN labels(b) AND type(r) = 'ACTED_IN'
RETURN count(*)
```

---

## Summary

OpenCypher pattern matching provides a rich, declarative syntax for querying graph data:

**Pattern Types**:
- **Node patterns**: `()`, `(n)`, `(:Label)`, `(n:Label {prop: value})`
- **Relationship patterns**: `-[]->`, `<-[r:TYPE]-`, `-[r {prop: value}]-`
- **Path patterns**: `path = (a)-[]->(b)-[]->(c)`
- **Variable-length**: `-[*1..5]->`, `((pattern)){1,3}`, `-[:TYPE]->{2,4}`
- **Optional**: `OPTIONAL MATCH (n)-[]->(m)`
- **Pattern comprehension**: `[(n)-[]->(m) WHERE condition | m.prop]`

**Key Principles**:
- Patterns are declarative - describe structure, not algorithm
- Relationship uniqueness enforced by default
- Use indexes for performance
- Bound variable-length patterns
- Filter early with inline predicates
- Anchor patterns with specific starting nodes

**Best Practices**:
- Use specific labels and relationship types
- Create indexes on frequently queried properties
- Limit result sets with LIMIT
- Use OPTIONAL MATCH for outer join semantics
- Leverage pattern comprehension for complex projections
- Always bound variable-length patterns (`{1,5}` not `*`)

---

## References

- **OpenCypher Official Site**: [https://opencypher.org/](https://opencypher.org/)
- **OpenCypher Specification**: [https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf)
- **Neo4j Cypher Manual - Patterns**: [https://neo4j.com/docs/cypher-manual/current/patterns/](https://neo4j.com/docs/cypher-manual/current/patterns/)
- **Neo4j Cypher Manual - Clauses**: [https://neo4j.com/docs/cypher-manual/current/clauses/](https://neo4j.com/docs/cypher-manual/current/clauses/)

---

**Document Version**: 1.0
**Last Updated**: 2026-02-16
**OpenCypher Version**: Based on OpenCypher 9 and Neo4j Cypher Manual (Current)
