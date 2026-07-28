# Cypher Script Loading

> **Status:** Backlog — not shipped in v0.5.0 (see [Datasets overview](overview.md)).

Planned loader for `.cypher` and `.cql` script files commonly used for Neo4j example datasets and data imports. When present, CypherLoader is intended to handle schema-related statements that aren't needed for embedded database use.

## Overview

The CypherLoader is designed to:
- Parse multi-statement Cypher scripts
- Execute data manipulation statements (CREATE, MERGE, SET, etc.)
- Automatically skip schema operations that aren't required for embedded use
- Provide transparent logging of skipped statements

## Basic Usage

### Loading a Script File

```python
from graphforge import GraphForge
from graphforge.datasets.loaders import CypherLoader
from pathlib import Path

# Create a GraphForge instance
gf = GraphForge()

# Create a loader and load the script
loader = CypherLoader()
loader.load(gf, Path("data/movies.cypher"))

# Query the loaded data
results = gf.execute("MATCH (m:Movie) RETURN m.title")
```

### Example Script

```cypher
// Create constraint (automatically skipped)
CREATE CONSTRAINT movie_title IF NOT EXISTS
FOR (m:Movie) REQUIRE m.title IS UNIQUE;

// Create index (automatically skipped)
CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name);

// Create data (executed)
CREATE (TheMatrix:Movie {title:'The Matrix', released:1999});
CREATE (Keanu:Person {name:'Keanu Reeves', born:1964});

// Create relationship (executed)
MATCH (keanu:Person {name:'Keanu Reeves'}), (matrix:Movie {title:'The Matrix'})
CREATE (keanu)-[:ACTED_IN]->(matrix);
```

## Supported Statements

### ✅ Fully Supported (Executed)

All standard Cypher query statements are executed:

- **CREATE** - Node and relationship creation
- **MERGE** - Create or match patterns with ON CREATE/MATCH SET
- **MATCH** - Pattern matching
- **SET** - Property updates
- **DELETE** / **DETACH DELETE** - Node and relationship deletion
- **REMOVE** - Property and label removal
- **WITH** - Query chaining
- **WHERE** - Filtering
- **RETURN** - Result projection
- **ORDER BY**, **LIMIT**, **SKIP** - Result ordering and pagination
- **UNWIND** - List expansion

### ⏭️ Automatically Skipped (Schema Operations)

The following statements are automatically skipped because they're not needed for embedded database use:

#### CREATE CONSTRAINT / DROP CONSTRAINT

```cypher
CREATE CONSTRAINT movie_title IF NOT EXISTS
FOR (m:Movie) REQUIRE m.title IS UNIQUE;

DROP CONSTRAINT movie_title IF EXISTS;
```

**Why skipped:** GraphForge is an embedded database designed for analysis and prototyping, not production use. Schema validation can be done in Python application code if needed.

**Supported constraint types (all skipped):**
- `UNIQUE` constraints - Uniqueness validation
- `EXISTS` / `IS NOT NULL` constraints - Required properties
- `NODE KEY` constraints - Composite keys

**Impact:** None. Data loads successfully without constraints. If you need validation, implement it in your Python code.

**Related:**

#### CREATE INDEX / DROP INDEX

```cypher
CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name);
CREATE INDEX movie_released FOR (m:Movie) ON (m.released);

DROP INDEX person_name IF EXISTS;
```

**Why skipped:** GraphForge uses Parquet storage plus derived indexes. Explicit CREATE INDEX DDL is not part of the public Cypher surface.

**Supported index types (all skipped):**
- Single property indexes
- Composite indexes
- Full-text indexes
- Spatial indexes

**Impact:** Minor for research-scale graphs. Prefer LIMIT-respecting traversal and the derived adjacency index for neighborhood work.

**Related:**

#### CALL Procedures

```cypher
CALL db.labels() YIELD label;
CALL apoc.periodic.iterate(...);
CALL db.index.fulltext.createIndex(...);
```

**Why skipped:** GraphForge doesn't have a stored procedure system. Most operations that would use procedures can be done more naturally in Python.

**Common procedure patterns:**
- `CALL db.labels()` → Use Python: `gf.get_labels()`
- `CALL db.relationshipTypes()` → Use Python: `gf.get_relationship_types()`
- `CALL apoc.*` → Implement in Python or use NetworkX

**Impact:** Low. Procedures are rarely used in data loading scripts. Most CALL statements are for database administration or advanced operations that aren't needed for data imports.

**Related:**

## Script Format

### Statement Separation

Statements are separated by semicolons (`;`):

```cypher
CREATE (a:Person {name: 'Alice'});
CREATE (b:Person {name: 'Bob'});
CREATE (a)-[:KNOWS]->(b);
```

### Comments

Single-line comments are supported:

```cypher
// This is a comment
CREATE (n:Node {id: 1}); // Inline comment
```

**URLs are preserved:** The loader correctly handles URLs with `//` in property values:

```cypher
CREATE (w:Website {url: 'https://example.com'}); // This comment is removed
```

### Multi-line Statements

Statements can span multiple lines:

```cypher
CREATE (matrix:Movie {
    title: 'The Matrix',
    released: 1999,
    tagline: 'Welcome to the Real World'
});
```

### Empty Statements

Multiple semicolons and empty statements are handled gracefully:

```cypher
CREATE (a:A);;;
CREATE (b:B);;
// Result: Two nodes created, empty statements ignored
```

## Logging

The CypherLoader provides transparent logging at different levels:

### DEBUG Level

View skipped statements:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

loader = CypherLoader()
loader.load(gf, Path("movies.cypher"))
```

Output:
```
DEBUG:graphforge.datasets.loaders.cypher:Skipping unsupported statement: CREATE CONSTRAINT movie_title IF NOT EXISTS...
DEBUG:graphforge.datasets.loaders.cypher:Skipping unsupported statement: CREATE INDEX person_name IF NOT EXISTS...
```

### INFO Level

View execution summary:

```python
import logging
logging.basicConfig(level=logging.INFO)

loader = CypherLoader()
loader.load(gf, Path("movies.cypher"))
```

Output:
```
INFO:graphforge.datasets.loaders.cypher:Loading Cypher script: movies.cypher
INFO:graphforge.datasets.loaders.cypher:Loaded movies.cypher: 15 statements executed, 3 skipped
```

## Error Handling

### File Not Found

```python
loader.load(gf, Path("nonexistent.cypher"))
# Raises: FileNotFoundError: Cypher script not found: nonexistent.cypher
```

### Syntax Errors

If a statement has invalid Cypher syntax, loading stops with a detailed error:

```python
loader.load(gf, Path("invalid.cypher"))
# Raises: ValueError: Cypher execution error: No terminal matches 'X' in the current parser context
```

The error message includes:
- The line and column where the error occurred
- The problematic statement (first 100 characters)
- The expected tokens

## Best Practices

### 1. Use Comments Liberally

Document your scripts for maintainability:

```cypher
// ============================================
// Movie Graph Dataset
// Source: Neo4j Examples
// Last Updated: 2024-01-15
// ============================================

// Create movies
CREATE (TheMatrix:Movie {title:'The Matrix', released:1999});
CREATE (TheMatrixReloaded:Movie {title:'The Matrix Reloaded', released:2003});
```

### 2. Group Related Operations

Organize scripts by logical sections:

```cypher
// ======================
// Schema (auto-skipped)
// ======================
CREATE CONSTRAINT movie_title FOR (m:Movie) REQUIRE m.title IS UNIQUE;

// ======================
// Nodes
// ======================
CREATE (m1:Movie {title:'The Matrix'});
CREATE (m2:Movie {title:'The Matrix Reloaded'});

// ======================
// Relationships
// ======================
MATCH (m1:Movie {title:'The Matrix'}), (m2:Movie {title:'The Matrix Reloaded'})
CREATE (m1)-[:SEQUEL]->(m2);
```

### 3. Use Variables for Complex Patterns

Make scripts more readable:

```cypher
// Create nodes
CREATE (neo:Character {name:'Neo'});
CREATE (morpheus:Character {name:'Morpheus'});
CREATE (matrix:Movie {title:'The Matrix'});

// Create relationships using variables
MATCH (neo:Character {name:'Neo'}),
      (morpheus:Character {name:'Morpheus'}),
      (matrix:Movie {title:'The Matrix'})
CREATE (neo)-[:APPEARS_IN]->(matrix),
       (morpheus)-[:APPEARS_IN]->(matrix),
       (morpheus)-[:MENTORS]->(neo);
```

### 4. Check Logs for Skipped Statements

Enable logging to understand what's being skipped:

```python
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s: %(message)s'
)

loader = CypherLoader()
loader.load(gf, Path("script.cypher"))
# Review output to confirm expected behavior
```

## Performance Tips

### Large Scripts

For scripts with thousands of statements:

1. **Use persistence**: Store to a Parquet project directory instead of in-memory

```python
gf = GraphForge("database/")  # Parquet-backed project
loader.load(gf, Path("large_script.cypher"))
```

2. **Batch related operations**: Group CREATE statements together

```cypher
// Good: Batch creates
CREATE (a:Person {name: 'Alice'}),
       (b:Person {name: 'Bob'}),
       (c:Person {name: 'Charlie'});

// Less efficient: Individual creates
CREATE (a:Person {name: 'Alice'});
CREATE (b:Person {name: 'Bob'});
CREATE (c:Person {name: 'Charlie'});
```

3. **Use MERGE efficiently**: MERGE can be slower than CREATE

```cypher
// Use CREATE when you know nodes don't exist
CREATE (n:Person {id: 123, name: 'Alice'});

// Use MERGE when you need idempotency
MERGE (n:Person {id: 123})
ON CREATE SET n.name = 'Alice', n.created = timestamp()
ON MATCH SET n.accessed = timestamp();
```

## Comparison with Neo4j

| Feature | GraphForge CypherLoader | Neo4j Cypher Shell |
|---------|------------------------|-------------------|
| Data manipulation | ✅ Full support | ✅ Full support |
| CONSTRAINTS | ⏭️ Skipped | ✅ Enforced |
| INDEXES | ⏭️ Skipped | ✅ Created |
| CALL procedures | ⏭️ Skipped | ✅ Executed |
| Multi-statement | ✅ Supported | ✅ Supported |
| Comments | ✅ Supported | ✅ Supported |
| File size limit | ✅ No limit | ⚠️ Memory dependent |
| Error recovery | ❌ Stop on error | ⚠️ Configurable |

## Troubleshooting

### Problem: Script loads but data is missing

**Possible causes:**
1. Statements were skipped (check DEBUG logs)
2. Pattern matching failed (node variables not bound)
3. Constraints in original script prevented duplicates

**Solution:**
```python
import logging
logging.basicConfig(level=logging.DEBUG)

loader = CypherLoader()
loader.load(gf, Path("script.cypher"))

# Verify loaded data
movie_count = gf.execute("MATCH (m:Movie) RETURN count(m) AS count")
person_count = gf.execute("MATCH (p:Person) RETURN count(p) AS count")
print(f"Movies: {movie_count[0]['count'].value}")
print(f"People: {person_count[0]['count'].value}")
```

### Problem: Syntax errors for valid Neo4j scripts

**Possible causes:**
1. GraphForge doesn't support advanced syntax (e.g., label expressions)
2. Script uses Neo4j-specific extensions

**Solution:**
Check [OpenCypher Compatibility](../../reference/opencypher-compatibility.md) for supported features. Common patterns to avoid:
- Label expressions: `:A|B` (union), `!:A` (negation)
- Map projections: `node {.*}` or `node {.prop1,.prop2}`
- Advanced CALL syntax

### Problem: Performance is slow

**Possible causes:**
1. Using in-memory storage for large datasets
2. Many individual CREATE statements instead of batched creates
3. Inefficient MERGE patterns

**Solutions:**
1. Use Parquet persistence: `GraphForge("database/")`
2. Batch CREATE statements: `CREATE (a), (b), (c)`
3. Optimize MERGE: Use CREATE when appropriate

## API Reference

### CypherLoader

```python
class CypherLoader(DatasetLoader):
    """Loader for Cypher script files (.cypher, .cql)."""

    def load(self, gf: GraphForge, path: Path) -> None:
        """Load Cypher script file into GraphForge instance.

        Args:
            gf: GraphForge instance to load data into
            path: Path to .cypher or .cql file

        Raises:
            FileNotFoundError: If script file doesn't exist
            ValueError: If script contains syntax errors
        """

    def get_format(self) -> str:
        """Return the format name this loader handles.

        Returns:
            "cypher"
        """
```

### Constants

```python
CypherLoader.SKIP_PREFIXES: ClassVar[list[str]] = [
    "CREATE CONSTRAINT",
    "DROP CONSTRAINT",
    "CREATE INDEX",
    "DROP INDEX",
    "CALL",
]
```

## Related Documentation

- [Dataset Overview](overview.md) - Complete dataset loading guide
- [Neo4j Examples](neo4j-examples.md) - Neo4j example datasets
- [OpenCypher Compatibility](../../reference/opencypher-compatibility.md) - Supported Cypher features
- [API Reference](../../reference/api.md) - Complete API documentation

## GitHub Issues

- - Handle CREATE CONSTRAINT statements
- - CREATE INDEX support
- - CALL procedure support

## Examples

See `examples/` directory for complete examples:
- `examples/05_load_datasets.py` - Dataset loading examples
- Coming soon: `examples/06_cypher_scripts.py` - Cypher script loading examples
