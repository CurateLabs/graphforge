# Feature: RETURN Aliasing

**Status**: ✅ Complete (outdated - see note below)
**Date**: 2026-01-30

> **⚠️ Note (2026-02-01):** This document describes the original v0.1.0 implementation.
> As of v0.1.3, column naming behavior has changed to align with openCypher TCK:
> - `RETURN n` now produces column "n" (not "col_0")
> - `RETURN n AS alias` produces column "alias" (unchanged)
> - `RETURN n.property` produces column "col_0" (unchanged)
> See the applicable GitHub Release notes for version-specific details.

## Overview

Implemented support for RETURN clause aliasing using the `AS` keyword. Users can now specify custom names for returned columns instead of relying on auto-generated `col_0`, `col_1`, etc.

## Syntax

```cypher
RETURN expression AS alias
```

## Examples

### Basic Variable Aliasing
```cypher
MATCH (n:Person) RETURN n AS person
```

Result keys: `{"person": NodeRef(...)}`

### Property Aliasing
```cypher
MATCH (n:Person) RETURN n.name AS name, n.age AS age
```

Result keys: `{"name": CypherString("Alice"), "age": CypherInt(30)}`

### Mixed Aliased and Non-Aliased
```cypher
MATCH (n:Person) RETURN n.name AS name, n.age
```

Result keys: `{"name": CypherString("Alice"), "col_1": CypherInt(30)}`

### Relationship Queries with Aliases
```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person)
RETURN a.name AS source, b.name AS target, r.since AS year
```

Result keys: `{"source":..., "target":..., "year":...}`

## Implementation Details

### AST Changes (src/graphforge/ast/clause.py:40-58)

Added `ReturnItem` dataclass:
```python
@dataclass
class ReturnItem:
    """A single return item with optional alias."""
    expression: Any  # Expression to evaluate
    alias: str | None = None  # Optional alias
```

Updated `ReturnClause`:
```python
@dataclass
class ReturnClause:
    """RETURN clause for projection."""
    items: list[ReturnItem]  # Changed from list[Any]
```

### Grammar Changes (src/graphforge/parser/cypher.lark:27-29)

Updated return_item rule:
```lark
return_item: expression ("AS"i IDENTIFIER)?
```

Supports both:
- `expression` (no alias)
- `expression AS identifier` (with alias)

### Parser Changes (src/graphforge/parser/parser.py:155-162)

Updated transformer:
```python
def return_item(self, items):
    """Transform return item with optional alias."""
    expression = items[0]
    alias = None
    if len(items) > 1:
        alias = self._get_token_value(items[1])
    return ReturnItem(expression=expression, alias=alias)
```

### Executor Changes (src/graphforge/executor/executor.py:161-173)

Updated _execute_project:
```python
def _execute_project(self, op: Project, input_rows: list[ExecutionContext]) -> list[dict]:
    result = []
    for ctx in input_rows:
        row = {}
        for i, return_item in enumerate(op.items):
            value = evaluate_expression(return_item.expression, ctx)
            # Use alias if provided, otherwise generate default column name
            key = return_item.alias if return_item.alias else f"col_{i}"
            row[key] = value
        result.append(row)
    return result
```

## Test Coverage

### Parser Tests (6 tests)
- `test_return_variable` - Variable without alias
- `test_return_property` - Property without alias
- `test_return_multiple_items` - Multiple items without aliases
- `test_return_with_alias` - Variable with alias
- `test_return_property_with_alias` - Property with alias
- `test_return_multiple_with_aliases` - Mixed aliases

### Integration Tests (4 tests)
- `test_return_variable_with_alias` - End-to-end variable aliasing
- `test_return_property_with_alias` - End-to-end property aliasing
- `test_return_mixed_aliases` - Mixed aliased and non-aliased
- `test_return_relationship_with_aliases` - Relationship queries with aliases

## Test Results

**Total Tests**: 220 passing (was 213, added 7 new tests)
**Coverage**: 89.49% (maintained above 85% threshold)
**Execution Time**: 1.69 seconds

## Backwards Compatibility

This feature is fully backwards compatible:
- Queries without `AS` continue to work exactly as before
- Result keys for non-aliased items remain `col_0`, `col_1`, etc.
- All existing tests pass without modification

## Known Limitations

None. Feature is complete and fully functional.

## openCypher Compliance

This implementation follows the openCypher specification for RETURN aliasing:
- Case-insensitive `AS` keyword
- Aliases are identifiers (alphanumeric + underscore)
- Aliases apply to expressions, not just variables
- Multiple items can have independent aliases

## Next Steps

With RETURN aliasing complete, the next feature to implement is ORDER BY clause.
