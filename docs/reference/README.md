# GraphForge Reference Documentation

This directory contains comprehensive reference documentation for GraphForge's OpenCypher implementation and TCK compliance.

## Directory Structure

### `opencypher-features/`
Authoritative documentation of OpenCypher features from the official specification, organized by category:

- `01-clauses.md` - Query clauses (MATCH, CREATE, WHERE, RETURN, WITH, etc.)
- `02-functions.md` - Built-in functions (string, numeric, list, aggregation, etc.)
- `03-operators.md` - Operators and expressions (comparison, logical, arithmetic, etc.)
- `04-patterns.md` - Pattern matching syntax (nodes, relationships, paths)
- `05-data-types.md` - Data types (primitives, structural, composite, temporal)

Each document includes:
- Complete feature list for that category
- Syntax and examples
- Links to OpenCypher specification
- Usage notes and edge cases

### `implementation-status/`
Implementation status reports for GraphForge, showing which features are complete, partial, or not implemented:

- `clauses.md` - Status of each query clause
- `functions.md` - Status of each built-in function
- `operators.md` - Status of each operator
- `patterns.md` - Status of pattern matching features

Each status report includes:
- Feature name
- Status: ✅ COMPLETE, ⚠️ PARTIAL, ❌ NOT_IMPLEMENTED
- File references (e.g., `src/graphforge/executor/executor.py:234`)
- Notes on partial implementations or known limitations

### `feature-mapping/`
Mappings between OpenCypher features and TCK test coverage:

- `clause-to-tck.md` - TCK scenarios that test each clause
- `function-to-tck.md` - TCK scenarios that test each function
- `tck-inventory.md` - Complete inventory of all TCK test scenarios

Shows which features have strong TCK coverage and which have gaps.

### Root Reference Documents

- `opencypher-compatibility.md` - Main compatibility document (overview, quick reference)
- `opencypher-compatibility-matrix.md` - Comprehensive matrix showing all features, status, and TCK coverage
- `feature-graph-schema.md` - Schema for the GraphForge knowledge graph
- `feature-graph-queries.md` - Example Cypher queries for analyzing feature status
- `tck-compliance.md` - TCK compliance metrics and progress tracking

## GraphForge Knowledge Graph

The feature inventory, implementation status, and TCK mapping are also available as a queryable GraphForge database:

- **Location**: `docs/feature-graph.db`
- **Builder script**: `scripts/build_feature_graph.py`
- **Schema**: See `feature-graph-schema.md`
- **Example queries**: See `feature-graph-queries.md`

This demonstrates GraphForge's capabilities by using it to model its own feature landscape.

## How to Use This Documentation

### For Contributors

1. **Implementing a new feature?**
   - Check `implementation-status/` to see current status
   - Check `feature-mapping/` to find relevant TCK tests
   - Update status after implementation

2. **Improving TCK coverage?**
   - See `tck-inventory.md` for all available scenarios
   - Check `feature-mapping/` to find under-tested features

3. **Planning a release?**
   - Query the feature graph for completion percentages
   - Check `opencypher-compatibility-matrix.md` for gaps

### For Users

1. **Want to know if a feature is supported?**
   - Check `opencypher-features/` for feature definition
   - Check `implementation-status/` for GraphForge support
   - See `opencypher-compatibility.md` for quick reference

2. **Reporting a bug?**
   - Check if the feature is complete or partial
   - Reference the implementation status in your issue

### For Researchers

1. **Analyzing OpenCypher compliance?**
   - Query the GraphForge knowledge graph
   - See `feature-graph-queries.md` for examples
   - Use `opencypher-compatibility-matrix.md` for overview

## Maintenance

This documentation is maintained alongside the codebase:

- **Update frequency**: After each feature implementation
- **Validation**: Run `scripts/build_feature_graph.py` to ensure consistency
- **Source of truth**: OpenCypher specification at https://opencypher.org/resources/

## Related Documentation

- `docs/tutorial.md` - Getting started with GraphForge
- `docs/guide/datasets/` - Dataset integration documentation
- `docs/book/use-cases/` - Use case examples
- [GitHub Releases](https://github.com/CurateLabs/graphforge/releases) - Version history and release notes
