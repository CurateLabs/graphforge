# OpenCypher Feature Graph - Example Queries

Practical Cypher queries for analyzing the OpenCypher feature knowledge graph.

**Prerequisites:** Build the graph with `python scripts/build_feature_graph.py`

**Graph Location:** `docs/feature-graph.db`

---

## Getting Started

```python
from graphforge import GraphForge

# Open the feature graph
db = GraphForge('docs/feature-graph.db')

# Run queries
results = db.execute("""
    MATCH (f:Feature)
    RETURN f.name, f.category, f.subcategory
    LIMIT 10
""")

for row in results:
    print(f"{row['f.name'].value} ({row['f.category'].value})")
```

---

## Feature Discovery Queries

### 1. List All Features by Category

Find all features organized by their category.

```cypher
MATCH (c:Category)<-[:BELONGS_TO_CATEGORY]-(f:Feature)
RETURN c.name AS category,
       collect(f.name) AS features,
       count(f) AS feature_count
ORDER BY feature_count DESC
```

**Use Case:** Get an overview of feature distribution across categories

---

### 2. Find All Complete Features

List all features that are fully implemented.

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'complete'})
RETURN f.name AS feature,
       f.category AS category,
       i.file_path AS implementation
ORDER BY category, feature
```

**Use Case:** Identify what's fully working for documentation or user guides

---

### 3. Find All Incomplete Features

List features that are partially implemented or not implemented.

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation)
WHERE i.status IN ['partial', 'not_implemented']
RETURN f.name AS feature,
       f.category AS category,
       i.status AS status
ORDER BY i.status, category, feature
```

**Use Case:** Identify work remaining for roadmap planning

---

## Implementation Status Queries

### 4. Implementation Status by Category

Calculate completion percentage for each category.

```cypher
MATCH (c:Category)<-[:BELONGS_TO_CATEGORY]-(f:Feature)
OPTIONAL MATCH (f)-[:IMPLEMENTED_IN]->(i:Implementation)
WITH c,
     count(f) AS total_features,
     sum(CASE WHEN i.status = 'complete' THEN 1 ELSE 0 END) AS complete,
     sum(CASE WHEN i.status = 'partial' THEN 1 ELSE 0 END) AS partial,
     sum(CASE WHEN i.status = 'not_implemented' OR i IS NULL THEN 1 ELSE 0 END) AS not_impl
RETURN c.name AS category,
       total_features,
       complete,
       partial,
       not_impl,
       round(100.0 * complete / total_features, 1) AS completion_pct
ORDER BY completion_pct DESC
```

**Use Case:** Track progress toward full OpenCypher compliance by category

**Expected Output:**
```
| category              | total | complete | partial | not_impl | completion_pct |
|-----------------------|-------|----------|---------|----------|----------------|
| Temporal Functions    | 11    | 11       | 0       | 0        | 100.0          |
| Spatial Functions     | 2     | 2        | 0       | 0        | 100.0          |
| Comparison Operators  | 8     | 8        | 0       | 0        | 100.0          |
| Reading Clauses       | 2     | 2        | 0       | 0        | 100.0          |
| ...                   | ...   | ...      | ...     | ...      | ...            |
```

---

### 5. Find High-Priority Implementation Gaps

Features not implemented but with good TCK coverage (proxied by category).

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'not_implemented'})
MATCH (f)-[:BELONGS_TO_CATEGORY]->(c:Category)
RETURN f.name AS feature,
       c.name AS category,
       f.category AS type
ORDER BY category, feature
LIMIT 20
```

**Use Case:** Prioritize next features to implement

---

### 6. Find Partial Implementations

Features that are partially complete and need finishing.

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'partial'})
RETURN f.name AS feature,
       f.category AS category,
       i.file_path AS file,
       i.notes AS notes
ORDER BY category, feature
```

**Use Case:** Find incomplete work to finish

---

## Category Analysis Queries

### 7. Most Complete Categories

Categories sorted by completion percentage.

```cypher
MATCH (c:Category)<-[:BELONGS_TO_CATEGORY]-(f:Feature)
OPTIONAL MATCH (f)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'complete'})
WITH c, count(DISTINCT f) AS total, count(DISTINCT i) AS complete
WHERE total > 0
RETURN c.name AS category,
       total AS features,
       complete,
       round(100.0 * complete / total, 1) AS pct
ORDER BY pct DESC, total DESC
```

**Use Case:** Identify strongest areas of OpenCypher support

---

### 8. Least Complete Categories

Categories needing the most work.

```cypher
MATCH (c:Category)<-[:BELONGS_TO_CATEGORY]-(f:Feature)
OPTIONAL MATCH (f)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'complete'})
WITH c, count(DISTINCT f) AS total, count(DISTINCT i) AS complete
WHERE total > 0
RETURN c.name AS category,
       total AS features,
       complete,
       total - complete AS remaining,
       round(100.0 * complete / total, 1) AS pct
ORDER BY pct ASC, remaining DESC
LIMIT 10
```

**Use Case:** Identify categories needing focus

---

## Feature Comparison Queries

### 9. Compare Clause vs Function Implementation

Compare implementation rates across major categories.

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation)
WITH f.category AS type,
     count(f) AS total,
     sum(CASE WHEN i.status = 'complete' THEN 1 ELSE 0 END) AS complete
RETURN type,
       total,
       complete,
       round(100.0 * complete / total, 1) AS pct
ORDER BY pct DESC
```

**Use Case:** Compare progress across feature types (clauses vs functions vs operators)

---

### 10. Find Features Without Implementations

Features that don't have any implementation records (data quality check).

```cypher
MATCH (f:Feature)
WHERE NOT EXISTS((f)-[:IMPLEMENTED_IN]->())
RETURN f.name AS feature,
       f.category AS category,
       f.subcategory AS subcategory
ORDER BY category, feature
```

**Use Case:** Data quality validation - identify missing implementation status

---

## File and Code Reference Queries

### 11. Features by Implementation File

Group features by their implementation file location.

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation)
WHERE i.file_path IS NOT NULL
WITH i.file_path AS file,
     collect(f.name) AS features,
     count(f) AS feature_count
RETURN file, feature_count, features
ORDER BY feature_count DESC
```

**Use Case:** Understand code organization and feature clustering

---

### 12. Find All Features in a Specific File

Get all features implemented in a specific file (e.g., evaluator.py).

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation)
WHERE i.file_path CONTAINS 'evaluator.py'
RETURN f.name AS feature,
       f.category AS category,
       i.status AS status,
       i.file_path AS file
ORDER BY category, feature
```

**Use Case:** Understand what a specific file implements

---

## Roadmap Planning Queries

### 13. Generate v0.5.0 Roadmap

Prioritized list of not-implemented features.

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'not_implemented'})
MATCH (f)-[:BELONGS_TO_CATEGORY]->(c:Category)
RETURN c.name AS category,
       collect(f.name) AS features,
       count(f) AS count
ORDER BY count DESC
```

**Use Case:** Plan next release features grouped by category

---

### 14. Features to Complete for Full Temporal Support

Even though temporal is 100%, check for any related features.

```cypher
MATCH (c:Category {name: 'Temporal Functions'})<-[:BELONGS_TO_CATEGORY]-(f:Feature)
OPTIONAL MATCH (f)-[:IMPLEMENTED_IN]->(i:Implementation)
RETURN f.name AS feature,
       COALESCE(i.status, 'unknown') AS status
ORDER BY status, feature
```

**Use Case:** Verify complete category coverage

---

### 15. Find "Low-Hanging Fruit" Features

Simple features that could be implemented quickly (predicates).

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'not_implemented'})
WHERE f.category = 'function' AND f.subcategory = 'predicate'
RETURN f.name AS feature,
       f.subcategory AS type
ORDER BY feature
```

**Use Case:** Find quick wins for next sprint

---

## Data Quality and Validation Queries

### 16. Validate Feature-Category Relationships

Ensure all features belong to a category.

```cypher
MATCH (f:Feature)
OPTIONAL MATCH (f)-[:BELONGS_TO_CATEGORY]->(c:Category)
WITH f, c
WHERE c IS NULL
RETURN f.name AS orphan_feature,
       f.category AS category,
       f.subcategory AS subcategory
```

**Use Case:** Data quality check - find orphaned features

---

### 17. Count Nodes by Type

Basic graph statistics.

```cypher
MATCH (n)
RETURN labels(n)[0] AS node_type,
       count(n) AS count
ORDER BY count DESC
```

**Use Case:** Graph health check

---

### 18. Count Relationships by Type

Relationship distribution.

```cypher
MATCH ()-[r]->()
RETURN type(r) AS relationship_type,
       count(r) AS count
ORDER BY count DESC
```

**Use Case:** Graph structure validation

---

## Advanced Analytical Queries

### 19. Feature Completion Trend Analysis

Identify which feature types are most/least complete.

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation)
WITH f.category AS category,
     f.subcategory AS subcategory,
     count(f) AS total,
     sum(CASE WHEN i.status = 'complete' THEN 1 ELSE 0 END) AS complete,
     sum(CASE WHEN i.status = 'partial' THEN 1 ELSE 0 END) AS partial
WHERE total >= 3
RETURN category,
       subcategory,
       total,
       complete,
       partial,
       round(100.0 * complete / total, 1) AS pct
ORDER BY pct DESC, total DESC
```

**Use Case:** Detailed completion analysis by subcategory

---

### 20. Generate Priority Matrix

Features scored by implementation complexity (assumed from category) and importance.

```cypher
MATCH (f:Feature)-[:IMPLEMENTED_IN]->(i:Implementation {status: 'not_implemented'})
MATCH (f)-[:BELONGS_TO_CATEGORY]->(c:Category)
WITH f, c,
     CASE
       WHEN c.name CONTAINS 'Predicate' THEN 'HIGH'
       WHEN c.name CONTAINS 'List' THEN 'MEDIUM'
       WHEN c.name CONTAINS 'Mathematical' THEN 'LOW'
       ELSE 'MEDIUM'
     END AS priority
RETURN priority,
       c.name AS category,
       collect(f.name) AS features,
       count(f) AS count
ORDER BY
  CASE priority
    WHEN 'HIGH' THEN 1
    WHEN 'MEDIUM' THEN 2
    ELSE 3
  END,
  count DESC
```

**Use Case:** Prioritized implementation backlog

---

## Usage Tips

### Performance

- For large graphs, add indexes on frequently queried properties
- Use `LIMIT` when exploring to avoid large result sets
- Profile queries with `PROFILE` or `EXPLAIN` (if supported)

### Querying Patterns

- Start with simple `MATCH (n:Label) RETURN n LIMIT 10` to explore
- Use `OPTIONAL MATCH` for features that may not have all relationships
- Aggregate with `collect()` to group related features

### Combining with Documentation

```python
# Get incomplete functions and look up documentation
results = db.execute("""
    MATCH (f:Feature {category: 'function'})-[:IMPLEMENTED_IN]->(i:Implementation)
    WHERE i.status = 'not_implemented'
    RETURN f.name, f.subcategory
    ORDER BY f.subcategory, f.name
""")

for row in results:
    name = row['f.name'].value
    subcategory = row['f.subcategory'].value
    print(f"{name} ({subcategory})")
    # Then look up details in docs/reference/opencypher-features/02-functions.md
```

---

## Extending the Graph

### Adding TCK Scenario Nodes

To enhance the graph with actual TCK scenario data:

```python
# Parse TCK inventory and create scenario nodes
from pathlib import Path

tck_inventory = Path('docs/reference/tck-inventory.md').read_text()

# Extract scenarios and create nodes
# Then create TESTED_BY relationships to features

db.execute("""
    CREATE (t:TCKScenario {
        name: 'Match simple node pattern',
        feature_file: 'tests/tck/features/official/clauses/match/Match1.feature',
        status: 'passing'
    })
""")

# Link to feature
db.execute("""
    MATCH (f:Feature {name: 'MATCH'}), (t:TCKScenario {name: 'Match simple node pattern'})
    CREATE (f)-[:TESTED_BY {coverage_type: 'basic'}]->(t)
""")
```

### Adding Feature Dependencies

Model dependencies between features:

```cypher
MATCH (with:Feature {name: 'WITH'}), (return:Feature {name: 'RETURN'})
CREATE (with)-[:DEPENDS_ON {
  dependency_type: 'required',
  reason: 'WITH requires RETURN-like projection syntax'
}]->(return)
```

---

## References

- Graph Schema: `docs/reference/feature-graph-schema.md`
- Compatibility Matrix: `docs/reference/opencypher-compatibility-matrix.md`
- Build Script: `scripts/build_feature_graph.py`
- GraphForge Documentation: `docs/`
