# GitHub Issue Workflow for GraphForge

This document describes the standard issue-branch-PR workflow for GraphForge development.

## Standard Workflow: Issue → Branch → PR → Merge

This is the established workflow used for all feature development.

### 1. Create Feature Branch

```bash
# Start from main
git checkout main
git pull origin main

# Create feature branch
git checkout -b feature/{issue-number}-{slug}

# Examples:
# feature/27-collect-aggregation
# feature/22-case-expressions
# docs/update-contributing-guide
```

### 2. Implement Feature with Tests

Follow the **comprehensive testing approach**:

1. **Unit Tests** (parser + planner/executor)
   - Parser tests: Verify grammar and AST transformation
   - Planner tests: Verify logical plan generation (if applicable)
   - Executor/Evaluator tests: Verify logic and edge cases

2. **Integration Tests** (end-to-end)
   - Basic functionality tests
   - NULL handling tests
   - Edge case tests
   - Combination tests (with other features)
   - Multiple node/relationship tests

**Test Coverage Target:** Maintain >85% overall coverage, >80% patch coverage

### 3. Run Pre-Push Checks

Before committing, **always** run the full pre-push check suite:

```bash
make pre-push
```

This runs:
- `ruff format --check.` - Code formatting
- `ruff check.` - Linting
- `mypy crates/gf-bindings-py/python/graphforge --strict-optional --show-error-codes` - Type checking
- `pytest` - Full test suite with coverage

**All checks must pass before creating a PR.**

### 4. Commit Changes

Use descriptive commit messages with the Co-Authored-By line:

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat: implement COLLECT aggregation function

Implement COLLECT aggregation with DISTINCT support per Cypher semantics.

Features:
- COLLECT(expression) aggregates values into a list
- COLLECT(DISTINCT expression) deduplicates values
- NULL values are skipped (not included in result list)
- Empty result returns empty list

Grammar changes:
- Add COLLECT to FUNCTION_NAME terminal

Implementation:
- Add COLLECT case in _compute_aggregation()
- Handle DISTINCT flag for deduplication
- Skip NULL values per Cypher spec

Tests:
- 8 parser tests for grammar
- 12 evaluator tests for aggregation logic
- 15 integration tests for end-to-end behavior
- Total: 35 comprehensive tests

Coverage: 95.5% overall

Closes #<issue-number>

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

**Commit Message Format:**
- **Subject line**: `feat: {brief description}` (70 chars max)
- **Body**: Detailed description with:
  - Feature summary
  - Grammar/implementation changes
  - Test summary with counts
  - Coverage percentage
- **Footer**: `Closes #{issue-number}` and Co-Authored-By line

### 5. Push Branch

```bash
git push -u origin feature/{issue-number}-{slug}
```

### 6. Create Pull Request

Use the GitHub CLI to create a PR with proper labels:

```bash
gh pr create \
  --title "feat: {descriptive title matching issue}" \
  --label "enhancement" \
  --body "$(cat <<'EOF'
## Summary

{Brief description of what this PR implements}

## Features

### {Feature Category}
- {Feature detail 1}
- {Feature detail 2}

### {Special Cases}
- {Edge case handling}
- {NULL handling}

## Grammar Changes

- {Grammar modification 1}
- {Grammar modification 2}

## Implementation

### Parser (`parser.py`)
- {Parser change 1}
- {Parser change 2}

### Evaluator/Executor
- {Evaluator change 1}
- {Evaluator change 2}

## Tests

- **Parser tests**: {N} tests for {description}
- **Evaluator tests**: {N} tests for {description}
- **Integration tests**: {N} tests for {description}

**Total**: {N} comprehensive tests

### Test Coverage
- {Key test scenario 1}
- {Key test scenario 2}
- {Edge case tests}

## Examples

```cypher
// {Example 1 description}
{Cypher query}

// {Example 2 description}
{Cypher query}
```

## Coverage

- Overall: **XX.XX%**
- All pre-push checks pass ✅

Closes #{issue-number}

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

**PR Requirements:**
- Title matches issue title format: `feat: {description}`
- Label: `enhancement` (required for features)
- Body includes: `Closes #{issue-number}` for automatic linking
- Body includes comprehensive summary with examples
- All pre-push checks passed before creating PR

### 7. PR Review and Merge

- CI/CD runs automatically on PR creation
- Address any CI failures promptly
- Once approved and CI passes, PR is merged to main
- Issue is automatically closed via `Closes #N` keyword

## Branch Naming Convention

**Format:** `{type}/{issue-number}-{slug}`

**Types:**
- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation changes
- `refactor/` - Code refactoring
- `test/` - Test improvements

**Examples:**
- `feature/27-collect-aggregation`
- `fix/32-null-handling-in-merge`
- `docs/update-contributing-guide`
- `refactor/45-simplify-evaluator`

**Guidelines:**
- Use lowercase with hyphens
- Keep slug concise but descriptive
- Start with issue number for traceability

## Test Requirements

### Comprehensive Test Coverage

Every feature must include:

1. **Parser Tests** (Unit)
   - Grammar validation
   - AST node creation
   - Token transformation
   - Error cases
   - Typical count: 10-15 tests

2. **Executor/Evaluator Tests** (Unit)
   - Core logic validation
   - Type handling
   - NULL propagation
   - Error handling
   - Typical count: 15-25 tests

3. **Integration Tests** (End-to-End)
   - Basic functionality
   - NULL handling
   - Edge cases
   - Combinations with other features
   - Multiple nodes/relationships
   - Typical count: 20-30 tests

**Total test count per feature:** 45-70 comprehensive tests

### Coverage Requirements

- **Overall coverage:** >85% (enforced by CI)
- **Patch coverage:** >80% (new code must be well-tested)
- **Branch coverage:** All conditional branches tested

### Test File Locations

```
tests/
  unit/
    parser/
      test_{feature}_parser.py
    executor/
      test_{feature}_evaluator.py
    planner/
      test_{feature}_planner.py (if applicable)
  integration/
    test_{feature}.py
```

## Common Implementation Patterns

### 1. Grammar Changes (`cypher.lark`)

```lark
// Add new clause
my_clause: "KEYWORD"i expression

// Add to query structure
query: match_clause my_clause? return_clause?

// Add new operator
OPERATOR: "==" | "!="
```

### 2. Parser Transformers (`parser.py`)

```python
def my_clause(self, items):
    """Transform MY clause."""
    return MyClause(items=list(items))
```

### 3. AST Nodes (`ast/clause.py` or `ast/expression.py`)

```python
@dataclass
class MyClause:
    """MY clause AST node."""
    items: list[Expression]
```

### 4. Planner (`planner/planner.py`)

```python
if isinstance(clause, MyClause):
    operators.append(MyOperator(items=clause.items))
```

### 5. Executor (`executor/executor.py`)

```python
def _execute_my_operator(self, op: MyOperator, input_rows: list[ExecutionContext]):
    """Execute MY operator."""
    result = []
    for ctx in input_rows:
        # Operator logic here
        result.append(ctx)
    return result
```

### 6. Evaluator (`executor/evaluator.py`)

```python
# For expression evaluation
if expr.op == "MY_OP":
    # NULL handling
    if isinstance(left_val, CypherNull):
        return CypherNull()

    # Type checking
    if not isinstance(left_val, CypherString):
        raise TypeError(f"MY_OP requires string operand")

    # Operation
    return CypherBool(left_val.value.startswith(pattern))
```

## Pre-Push Checklist

Before creating a PR, verify:

- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Coverage >85% overall, >80% patch
- [ ] Code formatted with ruff
- [ ] No linting errors
- [ ] Type checking passes (mypy)
- [ ] `make pre-push` completes successfully
- [ ] Commit message includes "Closes #N"
- [ ] Commit message includes Co-Authored-By line

## Labels Reference

- `enhancement` - New feature
- `bug` - Bug fix
- `documentation` - Documentation changes
- `tests` - Test improvements
- `good first issue` - Good for newcomers
- `complex` - Requires deep understanding
- `performance` - Performance improvements
- `refactor` - Code refactoring

## Issue Structure

Well-structured issues include:

1. **Feature Description** - Brief user-facing description
2. **Use Case** - Why users need this feature
3. **Examples** - Example queries with expected results
4. **Implementation Checklist** - Step-by-step tasks
5. **Files to Modify** - Specific file paths
6. **Implementation Pattern** - Code examples and approach
7. **Testing Strategy** - Required test scenarios
8. **Acceptance Criteria** - Must-have requirements
9. **References** - Links to specs and documentation
10. **Estimated Effort** - Time estimate

### Closing issues

Close when acceptance-criteria **outcomes** are met: merged work (or an explicit
non-code disposition), deterministic evidence for the stated criteria, and green
checks for the *changed surface*. Write AC as outcomes, not as a serial
`workflow_dispatch` script (Rust surface → Binding RC → release aggregate, exact-SHA
artifact URLs, run-ID pairing). Manual SHA-bound release workflows certify
publication readiness for the v0.5.0 publication close-out issue; they are not close blockers for ordinary
implementation, construction, or gate-tracker issues. See `AGENTS.md` § Issue
close.

## Development Tips

### General Guidelines

- **Read before modifying**: Always read existing files before making changes
- **Follow patterns**: Match existing code style and patterns
- **Test thoroughly**: Write tests before implementation when possible
- **NULL handling**: Always handle NULL values per Cypher semantics
- **Type safety**: Ensure type checking passes with mypy
- **Error messages**: Provide clear, actionable error messages

### Common Pitfalls

1. **Grammar issues**: Test parser with `pytest tests/unit/parser/` first
2. **Type errors**: Run `mypy` frequently during development
3. **Coverage gaps**: Write unit tests for all code paths
4. **NULL handling**: Every operator must handle NULL correctly
5. **Formatting**: Run `ruff format.` before committing

### Debugging Tips

- Use `pytest -v -s` to see print statements
- Use `pytest -k test_name` to run specific tests
- Check `htmlcov/index.html` for coverage details
- Use `git diff` to review changes before committing

## Example: Complete Feature Implementation

Reference implementations following this workflow:

**Arithmetic operators (example):**
- 68 comprehensive tests (14 parser + 24 evaluator + 30 integration)
- Proper NULL handling throughout
- Clear error messages with type information
- Full documentation in PR description
- Examples in Cypher syntax
- 95% coverage maintained

**String matching operators (example):**
- 68 comprehensive tests (11 parser + 27 evaluator + 30 integration)
- Case-sensitive string matching per Cypher spec
- NULL propagation tested
- 95% coverage maintained

**REMOVE clause (example):**
- 38 comprehensive tests (12 parser + 10 planner + 16 integration)
- Handles both properties and labels
- Multiple REMOVE clauses supported
- 95% coverage maintained

## CI/CD Pipeline

The following checks run automatically on each PR:

1. **Formatting**: `ruff format --check`
2. **Linting**: `ruff check`
3. **Type Checking**: `mypy`
4. **Tests**: Full test suite with coverage
5. **Coverage Enforcement**: Fails if coverage <85%

All checks must pass before merge.

## Questions?

If you have questions about any issue:
1. Comment on the issue to discuss the approach
2. Tag @DecisionNerd for clarification
3. Reference related issues or OpenCypher documentation
4. Check existing PRs for implementation patterns

## Summary

**The Standard Workflow:**
1. Create feature branch from main
2. Implement with comprehensive tests (unit + integration)
3. Run `make pre-push` - **all checks must pass**
4. Commit with descriptive message and "Closes #N"
5. Push branch to origin
6. Create PR with `gh pr create --label "{appropriate-label}"`
7. Wait for CI and review
8. Merge to main

**Success Criteria:**
- ✅ 45-70 comprehensive tests
- ✅ >85% coverage overall
- ✅ >80% patch coverage
- ✅ All pre-push checks pass
- ✅ Clear commit message with issue link
- ✅ Well-documented PR with examples
