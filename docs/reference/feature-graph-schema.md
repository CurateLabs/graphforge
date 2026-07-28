# GraphForge Feature Mapping Knowledge Graph Schema

This document defines the graph schema for modeling OpenCypher features, implementation status, and TCK test coverage as a queryable GraphForge database.

## Overview

The feature mapping graph enables querying relationships between:
- OpenCypher specification features
- GraphForge implementation status
- TCK test scenarios
- Feature categories and dependencies

This demonstrates GraphForge's capabilities by using it to model its own feature landscape (dogfooding!).

## Node Types

### Feature

Represents an OpenCypher feature (clause, function, operator, pattern type, etc.).

**Labels:** `Feature`

**Properties:**
- `name` (string, required) - Feature name (e.g., "MATCH", "substring()", "=~")
- `category` (string, required) - Category (e.g., "clause", "function", "operator", "pattern")
- `subcategory` (string, optional) - Subcategory (e.g., "string_function", "comparison_operator")
- `description` (string, required) - Brief description of the feature
- `spec_url` (string, optional) - Link to OpenCypher specification section
- `syntax` (string, optional) - Syntax example

**Example:**
```cypher
CREATE (f:Feature {
  name: 'MATCH',
  category: 'clause',
  subcategory: 'reading',
  description: 'Pattern matching clause for querying the graph',
  spec_url: 'https://opencypher.org/resources/',
  syntax: 'MATCH (pattern) WHERE conditions RETURN results'
})
```

### TCKScenario

Represents a test scenario from the OpenCypher Technology Compatibility Kit (TCK).

**Labels:** `TCKScenario`

**Properties:**
- `name` (string, required) - Scenario name from Gherkin feature file
- `feature_file` (string, required) - Path to .feature file (e.g., "tests/tck/features/official/clauses/match/Match1.feature")
- `status` (string, required) - Test status: "passing", "failing", "skipped"
- `scenario_type` (string, required) - "Scenario" or "Scenario Outline"
- `line_number` (integer, optional) - Line number in feature file

**Example:**
```cypher
CREATE (t:TCKScenario {
  name: 'Match with simple node pattern',
  feature_file: 'tests/tck/features/official/clauses/match/Match1.feature',
  status: 'passing',
  scenario_type: 'Scenario',
  line_number: 42
})
```

### Implementation

Represents the implementation of a feature in the GraphForge codebase.

**Labels:** `Implementation`

**Properties:**
- `file_path` (string, required) - Path to source file (e.g., "src/graphforge/executor/executor.py")
- `line_number` (integer, optional) - Line number where implemented
- `status` (string, required) - Implementation status:
  - "complete" - Fully implemented with comprehensive tests
  - "partial" - Basic implementation, missing edge cases or advanced features
  - "not_implemented" - Feature not yet implemented
- `function_name` (string, optional) - Function/method name where implemented
- `notes` (string, optional) - Implementation notes, limitations, or TODOs

**Example:**
```cypher
CREATE (i:Implementation {
  file_path: 'src/graphforge/executor/executor.py',
  line_number: 234,
  status: 'complete',
  function_name: '_execute_match',
  notes: 'Full MATCH support with WHERE and pattern comprehension'
})
```

### Category

Represents a high-level grouping of features.

**Labels:** `Category`

**Properties:**
- `name` (string, required) - Category name (e.g., "Clauses", "String Functions", "Comparison Operators")
- `description` (string, required) - Category description
- `priority` (integer, optional) - Implementation priority (1=high, 3=low)

**Example:**
```cypher
CREATE (c:Category {
  name: 'Reading Clauses',
  description: 'Query clauses for reading/matching data from the graph',
  priority: 1
})
```

## Relationship Types

### TESTED_BY

Connects a Feature to TCKScenario(s) that test it.

**Pattern:** `(Feature)-[:TESTED_BY]->(TCKScenario)`

**Properties:**
- `coverage_type` (string, optional) - Type of coverage: "basic", "comprehensive", "edge_cases"

**Example:**
```cypher
MATCH (f:Feature {name: 'MATCH'}), (t:TCKScenario)
WHERE t.name CONTAINS 'simple node pattern'
CREATE (f)-[:TESTED_BY {coverage_type: 'basic'}]->(t)
```

### IMPLEMENTED_IN

Connects a Feature to its Implementation(s) in the codebase.

**Pattern:** `(Feature)-[:IMPLEMENTED_IN]->(Implementation)`

**Properties:**
- `completeness` (float, optional) - Completeness percentage (0.0-1.0)
- `since_version` (string, optional) - Version when implemented (e.g., "0.3.0")

**Example:**
```cypher
MATCH (f:Feature {name: 'MATCH'}), (i:Implementation)
WHERE i.file_path CONTAINS 'executor.py'
CREATE (f)-[:IMPLEMENTED_IN {completeness: 1.0, since_version: '0.1.0'}]->(i)
```

### BELONGS_TO_CATEGORY

Connects a Feature to its Category.

**Pattern:** `(Feature)-[:BELONGS_TO_CATEGORY]->(Category)`

**Properties:** None

**Example:**
```cypher
MATCH (f:Feature {name: 'MATCH'}), (c:Category {name: 'Reading Clauses'})
CREATE (f)-[:BELONGS_TO_CATEGORY]->(c)
```

### DEPENDS_ON

Represents feature dependencies (one feature requires another).

**Pattern:** `(Feature)-[:DEPENDS_ON]->(Feature)`

**Properties:**
- `dependency_type` (string, optional) - Type: "required", "optional", "enhances"
- `reason` (string, optional) - Why the dependency exists

**Example:**
```cypher
MATCH (with:Feature {name: 'WITH'}), (return:Feature {name: 'RETURN'})
CREATE (with)-[:DEPENDS_ON {
  dependency_type: 'required',
  reason: 'WITH requires RETURN-like projection syntax'
}]->(return)
```

### TESTS

Inverse of TESTED_BY (optional, for bidirectional traversal).

**Pattern:** `(TCKScenario)-[:TESTS]->(Feature)`

**Properties:** None

**Example:**
```cypher
MATCH (t:TCKScenario)-[:TESTS]->(f:Feature)
RETURN t.name, f.name
```

## Complete Schema Diagram

```
┌─────────────┐
│  Category   │
│             │
│ name        │
│ description │
│ priority    │
└──────▲──────┘
       │
       │ BELONGS_TO_CATEGORY
       │
┌──────┴──────────────────┐
│      Feature            │◄────────┐
│                         │         │
│ name         category   │         │ DEPENDS_ON
│ description  subcategory│         │
│ spec_url     syntax     │─────────┘
└──────┬──────────────┬───┘
       │              │
       │ IMPLEMENTED_IN│ TESTED_BY
       │              │
       ▼              ▼
┌─────────────┐  ┌──────────────┐
│Implementation│  │ TCKScenario  │
│             │  │              │
│ file_path   │  │ name         │
│ line_number │  │ feature_file │
│ status      │  │ status       │
│ function_name│  │ scenario_type│
│ notes       │  │ line_number  │
└─────────────┘  └──────────────┘
```

## Sample Data

Here are examples showing how to create a few features with relationships:

```cypher
-- Create categories
CREATE (reading:Category {
  name: 'Reading Clauses',
  description: 'Query clauses for reading data from the graph',
  priority: 1
})

CREATE (string_funcs:Category {
  name: 'String Functions',
  description: 'Functions for string manipulation',
  priority: 2
})

-- Create features
CREATE (match:Feature {
  name: 'MATCH',
  category: 'clause',
  subcategory: 'reading',
  description: 'Pattern matching clause for querying the graph',
  spec_url: 'https://opencypher.org/resources/',
  syntax: 'MATCH (pattern) [WHERE condition] RETURN ...'
})

CREATE (substring:Feature {
  name: 'substring()',
  category: 'function',
  subcategory: 'string',
  description: 'Extract substring from start index with optional length',
  syntax: 'substring(string, start [, length])'
})

CREATE (where_clause:Feature {
  name: 'WHERE',
  category: 'clause',
  subcategory: 'filtering',
  description: 'Filter results based on predicates',
  syntax: 'WHERE predicate [AND|OR predicate ...]'
})

-- Create implementations
CREATE (match_impl:Implementation {
  file_path: 'src/graphforge/executor/executor.py',
  line_number: 234,
  status: 'complete',
  function_name: '_execute_match',
  notes: 'Full MATCH support with WHERE, variable-length paths, optional patterns'
})

CREATE (substring_impl:Implementation {
  file_path: 'src/graphforge/executor/evaluator.py',
  line_number: 567,
  status: 'complete',
  function_name: 'eval_substring',
  notes: 'Full substring support with 2 and 3 argument forms'
})

-- Create TCK scenarios
CREATE (match_scenario1:TCKScenario {
  name: 'Match with simple node pattern',
  feature_file: 'tests/tck/features/official/clauses/match/Match1.feature',
  status: 'passing',
  scenario_type: 'Scenario',
  line_number: 10
})

CREATE (match_scenario2:TCKScenario {
  name: 'Match with variable-length path',
  feature_file: 'tests/tck/features/official/clauses/match/Match5.feature',
  status: 'passing',
  scenario_type: 'Scenario Outline',
  line_number: 45
})

CREATE (substring_scenario:TCKScenario {
  name: 'substring() with start and length',
  feature_file: 'tests/tck/features/official/expressions/string/String2.feature',
  status: 'passing',
  scenario_type: 'Scenario',
  line_number: 89
})

-- Create relationships
CREATE (match)-[:BELONGS_TO_CATEGORY]->(reading)
CREATE (substring)-[:BELONGS_TO_CATEGORY]->(string_funcs)
CREATE (where_clause)-[:BELONGS_TO_CATEGORY]->(reading)

CREATE (match)-[:IMPLEMENTED_IN {completeness: 1.0, since_version: '0.1.0'}]->(match_impl)
CREATE (substring)-[:IMPLEMENTED_IN {completeness: 1.0, since_version: '0.2.0'}]->(substring_impl)

CREATE (match)-[:TESTED_BY {coverage_type: 'comprehensive'}]->(match_scenario1)
CREATE (match)-[:TESTED_BY {coverage_type: 'comprehensive'}]->(match_scenario2)
CREATE (substring)-[:TESTED_BY {coverage_type: 'basic'}]->(substring_scenario)

CREATE (match)-[:DEPENDS_ON {
  dependency_type: 'enhances',
  reason: 'MATCH often used with WHERE for filtering'
}]->(where_clause)
```

## Example Queries

### Find all incomplete features with TCK coverage

Find features that have TCK tests but are not fully implemented (high priority for implementation):

```cypher
MATCH (f:Feature)-[:TESTED_BY]->(t:TCKScenario)
WHERE NOT EXISTS {
  MATCH (f)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'complete'})
}
WITH f, count(t) AS tck_count
RETURN f.name, f.category, f.subcategory, tck_count
ORDER BY tck_count DESC
LIMIT 20
```

**Use case:** Prioritize implementation work based on test coverage

### Show implementation status by category

Calculate completion statistics for each category:

```cypher
MATCH (c:Category)<-[:BELONGS_TO_CATEGORY]-(f:Feature)
OPTIONAL MATCH (f)-[:IMPLEMENTED_IN]->(i:Implementation)
WITH c, f, i
RETURN
  c.name AS category,
  count(DISTINCT f) AS total_features,
  sum(CASE WHEN i.status = 'complete' THEN 1 ELSE 0 END) AS complete,
  sum(CASE WHEN i.status = 'partial' THEN 1 ELSE 0 END) AS partial,
  sum(CASE WHEN i.status = 'not_implemented' OR i IS NULL THEN 1 ELSE 0 END) AS not_implemented,
  round(100.0 * sum(CASE WHEN i.status = 'complete' THEN 1 ELSE 0 END) / count(DISTINCT f), 1) AS completion_pct
ORDER BY completion_pct DESC
```

**Use case:** Track progress toward full OpenCypher compliance

### Find features without TCK tests (coverage gaps)

Identify implemented features that lack test coverage:

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation)
WHERE NOT EXISTS {
  MATCH (f)-[:TESTED_BY]->(:TCKScenario)
}
RETURN f.name, f.category, i.status, i.file_path
ORDER BY f.category, f.name
```

**Use case:** Identify testing gaps

### Find all TCK scenarios for a specific feature

Get all test scenarios that test pattern matching:

```cypher
MATCH (f:Feature)-[:TESTED_BY]->(t:TCKScenario)
WHERE f.category = 'pattern'
RETURN f.name, t.name, t.feature_file, t.status
ORDER BY f.name, t.feature_file
```

**Use case:** Understand test coverage for a category

### Generate priority list for v0.4.0

Find not-implemented features with high TCK coverage and category priority:

```cypher
MATCH (f:Feature)-[:TESTED_BY]->(t:TCKScenario)
MATCH (f)-[:BELONGS_TO_CATEGORY]->(c:Category)
WHERE NOT EXISTS {
  MATCH (f)-[:IMPLEMENTED_IN]->(i:Implementation)
  WHERE i.status IN ['complete', 'partial']
}
WITH f, c, count(t) AS tck_count
WHERE tck_count >= 5 AND c.priority <= 2
RETURN
  f.name,
  f.category,
  f.description,
  c.name AS category_name,
  c.priority AS category_priority,
  tck_count
ORDER BY c.priority, tck_count DESC
LIMIT 15
```

**Use case:** Plan release roadmap based on data

### Find partial implementations that need completion

Identify features marked as partial that need work:

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'partial'})
OPTIONAL MATCH (f)-[:TESTED_BY]->(t:TCKScenario)
RETURN
  f.name,
  f.category,
  i.file_path,
  i.notes,
  count(t) AS tck_scenarios
ORDER BY tck_scenarios DESC
```

**Use case:** Find incomplete work to finish

### Find feature dependencies

Show features that depend on other features:

```cypher
MATCH (f1:Feature)-[d:DEPENDS_ON]->(f2:Feature)
OPTIONAL MATCH (f2)-[:IMPLEMENTED_IN]->(i:Implementation)
RETURN
  f1.name AS feature,
  f2.name AS depends_on,
  d.dependency_type AS type,
  i.status AS dependency_status
ORDER BY f1.name
```

**Use case:** Understand implementation order requirements

### Most tested features

Find features with the most TCK coverage:

```cypher
MATCH (f:Feature)-[:TESTED_BY]->(t:TCKScenario)
WITH f, count(t) AS scenario_count
ORDER BY scenario_count DESC
LIMIT 10
RETURN f.name, f.category, scenario_count
```

**Use case:** Identify well-tested features

### Least tested categories

Find categories with poor test coverage:

```cypher
MATCH (c:Category)<-[:BELONGS_TO_CATEGORY]-(f:Feature)
OPTIONAL MATCH (f)-[:TESTED_BY]->(t:TCKScenario)
WITH c, count(DISTINCT f) AS feature_count, count(t) AS test_count
RETURN
  c.name,
  feature_count,
  test_count,
  CASE WHEN feature_count > 0
       THEN round(1.0 * test_count / feature_count, 2)
       ELSE 0.0
  END AS tests_per_feature
ORDER BY tests_per_feature ASC
LIMIT 10
```

**Use case:** Find categories needing more test coverage

## Loading the Graph

To build and load the feature mapping graph:

1. **Run the builder script:**
   ```bash
   python scripts/build_feature_graph.py
   ```

   This script:
   - Parses all feature documentation from `docs/reference/opencypher-features/`
   - Extracts implementation status from `docs/reference/implementation-status/`
   - Loads TCK inventory from `docs/reference/tck-inventory.md`
   - Creates the graph at `docs/feature-graph.db`

2. **Query the graph:**
   ```python
   from graphforge import GraphForge

   # Open the feature graph
   db = GraphForge('docs/feature-graph.db')

   # Run queries
   results = db.execute("""
       MATCH (f:Feature)-[:TESTED_BY]->(t:TCKScenario)
       WHERE f.category = 'clause'
       RETURN f.name, count(t) AS tests
       ORDER BY tests DESC
   """)

   for row in results:
       print(f"{row['f.name'].value}: {row['tests'].value} tests")
   ```

3. **Update the graph:**
   - As features are implemented, update the markdown docs
   - Re-run `build_feature_graph.py` to rebuild the graph
   - The graph stays in sync with documentation

## Schema Evolution

As GraphForge evolves, the schema may be extended with:

- **Performance metrics**: Query execution times, memory usage
- **User queries**: Common query patterns from users
- **Bug reports**: Link features to GitHub issues
- **Version history**: Track implementation status across versions
- **Deprecations**: Mark features for removal or replacement

## Benefits of the Graph Approach

1. **Queryable**: Use Cypher to analyze feature status, not manual inspection
2. **Relational**: Understand dependencies and relationships between features
3. **Maintainable**: Single source of truth synchronized with docs
4. **Dogfooding**: Demonstrates GraphForge capabilities
5. **Discoverable**: Complex queries reveal insights not obvious from flat docs
6. **Versioned**: Graph evolves with codebase

## Next Steps

- See `docs/reference/feature-graph-queries.md` for more example queries
- Run `scripts/build_feature_graph.py` to build the graph
- Explore the graph at `docs/feature-graph.db`
- Add your own queries to discover new insights
