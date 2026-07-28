# GraphForge Test Suite

This directory contains the comprehensive test suite for GraphForge.

## Structure

```
tests/
├── unit/           # Fast, isolated component tests
├── integration/    # End-to-end and component interaction tests
├── tck/            # openCypher TCK compliance tests
├── property/       # Property-based tests using Hypothesis
└── conftest.py     # Shared fixtures
```

## Running Tests

See [docs/testing-strategy.md](../docs/testing-strategy.md) for complete testing documentation.

### Quick Start

```bash
# Install dev dependencies
uv sync --all-extras

# Run all tests
pytest

# Run specific category
pytest -m unit
pytest -m integration
pytest -m tck

# Run with coverage
pytest --cov=src --cov-report=html
open htmlcov/index.html
```

### Common Commands

```bash
# Fast feedback (unit tests only)
pytest -m unit

# Stop on first failure
pytest -x

# Run last failed tests
pytest --lf

# Verbose output
pytest -vv

# Parallel execution
pytest -n auto
```

## Test Categories

### Unit Tests (`tests/unit/`)
- **Purpose:** Test individual components in isolation
- **Speed:** < 1ms per test
- **Coverage target:** >90%
- **Marker:** `@pytest.mark.unit`

### Integration Tests (`tests/integration/`)
- **Purpose:** Test component interactions and query pipeline
- **Speed:** < 100ms per test
- **Marker:** `@pytest.mark.integration`

### TCK Tests (`tests/tck/`)
- **Purpose:** Validate openCypher semantic compliance
- **Coverage:** See `tck/coverage_matrix.json`
- **Marker:** `@pytest.mark.tck`

### Property Tests (`tests/property/`)
- **Purpose:** Edge case discovery via generative testing
- **Framework:** Hypothesis
- **Marker:** `@pytest.mark.property`

## Fixtures

### Core Fixtures (tests/conftest.py)

- `tmp_db_path`: Temporary database file path
- `db`: Fresh GraphForge instance with temp storage
- `memory_db`: In-memory database instance
- `sample_graph`: Pre-populated test graph

### Using Fixtures

```python
@pytest.mark.unit
def test_example(db):
    """Test using the db fixture."""
    result = db.execute("MATCH (n) RETURN n")
    assert result is not None
```

## Coverage

View current coverage:
```bash
pytest --cov=src --cov-report=term-missing
```

Generate HTML report:
```bash
pytest --cov=src --cov-report=html
open htmlcov/index.html
```

## Quality Gates

All PRs must pass:
- All unit tests
- All integration tests
- All non-skipped TCK tests
- Coverage ≥85%
- No new warnings

## Writing Tests

### Example Unit Test

```python
import pytest

@pytest.mark.unit
def test_node_creation():
    """Test that nodes can be created with labels and properties."""
    # Arrange
    node = Node(labels={"Person"}, properties={"name": "Alice"})

    # Act
    result = node.get_property("name")

    # Assert
    assert result == "Alice"
```

### Example Integration Test

```python
import pytest

@pytest.mark.integration
def test_query_execution(db):
    """Test full query execution pipeline."""
    # Arrange
    db.execute("CREATE (n:Person {name: 'Alice'})")

    # Act
    result = db.execute("MATCH (n:Person) RETURN n.name")

    # Assert
    assert len(result) == 1
    assert result[0]["n.name"] == "Alice"
```

## CI Integration

Tests run automatically on:
- Push to any branch
- Pull request creation
- Pull request updates

See `.github/workflows/test.yml` for CI configuration.

## Resources

- [Testing Strategy](../docs/testing-strategy.md) - Complete testing documentation
- [openCypher TCK](https://github.com/opencypher/openCypher/tree/master/tck)
- [pytest documentation](https://docs.pytest.org/)
- [Hypothesis documentation](https://hypothesis.readthedocs.io/)
