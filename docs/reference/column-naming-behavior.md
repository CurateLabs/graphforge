# Column Naming Behavior in GraphForge

**Last Updated:** 2026-07-27
**Version:** v0.5.0

## Summary

GraphForge follows the **openCypher TCK specification** for column naming in query results. This means that simple variable references preserve their variable names as column names.

## Behavior

### Simple Variable References

When you return a simple variable without an alias, the column name is the **variable name**:

```cypher
MATCH (n:Person)
RETURN n
```

**Result:** Column name is `"n"` (not `"col_0"`)

```python
table = forge.execute("MATCH (n:Person) RETURN n")
# Arrow column name is "n"
```

### Explicit Aliases

When you provide an explicit alias, the column name is the **alias**:

```cypher
MATCH (n:Person)
RETURN n AS person
```

**Result:** Column name is `"person"`

```python
table = forge.execute("MATCH (n:Person) RETURN n AS person")
# Arrow column name is "person"
```

### Complex Expressions

Complex expressions without aliases use **`col_N` naming** (where N is the 0-based index):

```cypher
MATCH (n:Person)
RETURN n.name
```

**Result:** Column name is `"col_0"`

```cypher
MATCH (n:Person)
RETURN n.name, n.age
```

**Result:** Column names are `"col_0"` and `"col_1"`

```python
table = forge.execute("MATCH (n:Person) RETURN n.name, n.age")
# Arrow column names are "col_0" and "col_1"
```

To avoid `col_N` naming, use explicit aliases:

```cypher
MATCH (n:Person)
RETURN n.name AS name, n.age AS age
```

**Result:** Column names are `"name"` and `"age"`

## Summary Table

| Query | Column Name | Reasoning |
|-------|-------------|-----------|
| `RETURN n` | `"n"` | Simple variable - use variable name |
| `RETURN n AS person` | `"person"` | Explicit alias - use alias |
| `RETURN n.name` | `"col_0"` | Complex expression - use col_N |
| `RETURN n.name AS name` | `"name"` | Explicit alias - use alias |
| `RETURN count(*)` | `"col_0"` | Aggregation - use col_N |
| `RETURN count(*) AS total` | `"total"` | Explicit alias - use alias |

## Why This Design?

### openCypher TCK Compliance

This behavior aligns with the **openCypher specification** and matches how Neo4j and other Cypher implementations work:

```cypher
// Neo4j behavior
MATCH (n) RETURN n
// Column name: "n"
```

GraphForge aims to be compatible with openCypher TCK (Technology Compatibility Kit) tests, which expect variable names to be preserved.

### WITH Clause Correctness

The WITH clause requires variable names to be preserved through the query pipeline:

```cypher
WITH p.name AS name
RETURN name
```

In this example:
1. WITH binds `"name"` to the value of `p.name`
2. RETURN sees a simple variable `name`
3. The result column must be `"name"` (not `"col_0"`)

If we used `col_0` for all unnamed items, the WITH clause would break.

### Consistency with Neo4j

Users familiar with Neo4j expect this behavior. Maintaining compatibility makes GraphForge easier to learn and use.

## Best Practices

### Always Use Explicit Aliases

For production code, use explicit aliases to make column names clear and avoid ambiguity:

```cypher
MATCH (p:Person)-[:KNOWS]->(friend:Person)
RETURN p.name AS person_name, friend.name AS friend_name
```

This makes your code:
- **More readable** - clear what each column represents
- **More maintainable** - column names won't change if variable names change
- **More portable** - works across different Cypher implementations

### Access by Variable Name

For simple queries, access columns by variable name:

```python
results = db.execute("MATCH (n:Person) RETURN n")
for row in results:
    node = row['n']  # Use variable name
```

### Use List Access for Complex Expressions

If you don't want to use aliases, remember that `col_N` indices are 0-based:

```python
results = db.execute("MATCH (n:Person) RETURN n.name, n.age")
for row in results:
    name = row['col_0']  # First expression
    age = row['col_1']   # Second expression
```

But **explicit aliases are strongly recommended** for clarity:

```python
results = db.execute("MATCH (n:Person) RETURN n.name AS name, n.age AS age")
for row in results:
    name = row['name']  # Much clearer!
    age = row['age']
```

## Implementation Details

### Code Location

The column naming logic is implemented in `src/graphforge/executor/executor.py` in the `_execute_project` method:

```python
if return_item.alias:
    # Explicit alias provided - use it
    key = return_item.alias
elif isinstance(return_item.expression, Variable):
    # Simple variable reference - use variable name as column name
    # This preserves names from WITH clauses
    key = return_item.expression.name
else:
    # Complex expression without alias - use default column naming
    key = f"col_{i}"
```

### Testing

This behavior is tested in:
- `tests/integration/test_e2e_queries.py` - End-to-end query tests
- `tests/integration/test_with_clause.py` - WITH clause tests
- `tests/integration/test_persistence.py` - Persistence tests

All 153 integration tests verify this behavior.

## See Also

- CHANGELOG.md (in repository root) - Version history and changes
- [CHANGELOG.md](https://github.com/CurateLabs/graphforge-legecy/blob/main/CHANGELOG.md) - Version history
- [tck-compliance.md](tck-compliance.md) - openCypher TCK compliance documentation
