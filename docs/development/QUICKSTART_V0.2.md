# Quick Start Guide: Implementing v0.2 Features

**Last Updated:** 2026-02-02
**Target:** v0.2.0 Release (March 2026)

---

## Overview

This guide helps you start implementing GraphForge v0.2 features. Feature work items are specified below.

**Total effort:** 20-28 hours across 9 features
**Target:** ~950 TCK scenarios (~25% compliance)

---

## Prerequisites

1. ✅ Feature specifications prepared
2. ✅ Documentation complete
3. ✅ Planning committed to git
4. ⏭️ Development environment ready

---

## Implementation Order (Recommended)

### Phase 1: Quick Wins (6-8 hours)

Start here to build momentum with simple, high-impact features:

#### NOT operator (1-2h)
**Why first:** Simplest feature, validates workflow, unlocks negation logic

```bash
git checkout -b feature/not-operator

# Implement:
# 1. Grammar: Add NOT to expression rules
# 2. AST: Add NotExpression node
# 3. Parser: Add transformer
# 4. Evaluator: Implement NOT with NULL propagation
# 5. Tests: 8 unit tests, 5 integration tests

# When done:
git add .
git commit -m "feat: implement NOT logical operator

Adds NOT operator for boolean negation with proper NULL propagation.

Fixes #<issue-number>"

git push -u origin feature/not-operator
gh pr create --title "feat: implement NOT logical operator" --body "Closes #<issue-number>" --label "enhancement,v0.2"
```

#### DETACH DELETE (1-2h)
**Why second:** Small executor change, high user value

```bash
git checkout main && git pull
git checkout -b feature/detach-delete

# Implement:
# 1. Grammar: Add "DETACH DELETE" syntax
# 2. AST: Add detach flag to DeleteClause
# 3. Executor: Remove edges before deleting node
# 4. Tests: 5 integration tests

# When done: commit and PR with "Closes #<issue-number>"
```

#### REMOVE clause (2h)
**Why third:** Property/label removal, commonly needed

```bash
git checkout main && git pull
git checkout -b feature/remove-clause

# Implement: REMOVE for properties and labels
# When done: commit and PR with "Closes #<issue-number>"
```

#### String matching (2-3h)
**Why fourth:** Very common in WHERE clauses

```bash
git checkout main && git pull
git checkout -b feature/string-matching

# Implement: STARTS WITH, ENDS WITH, CONTAINS
# When done: commit and PR with "Closes #<issue-number>"
```

**🎉 After Phase 1:** 4 features complete, ~200 TCK scenarios added

---

### Phase 2: Core Features (8-10 hours)

Essential query capabilities:

#### UNWIND (2-3h)
**Good first issue**, validates workflow, enables list iteration

```bash
git checkout main && git pull
git checkout -b feature/unwind-clause

# Implement:
# 1. Grammar: UNWIND clause
# 2. AST: UnwindClause node
# 3. Operator: Unwind operator
# 4. Executor: Row expansion logic
# 5. Tests: 5 unit, 8 integration

# When done: commit and PR with "Closes #<issue-number>"
```

#### Arithmetic operators (2-3h)
**Fundamental functionality:** +, -, *, /, %

```bash
git checkout main && git pull
git checkout -b feature/arithmetic-operators

# Implement: Arithmetic in expressions
# When done: commit and PR with "Closes #<issue-number>"
```

#### COLLECT aggregation (3-4h)
**Most requested aggregation**, complements UNWIND

```bash
git checkout main && git pull
git checkout -b feature/collect-aggregation

# Implement: COLLECT aggregation function
# When done: commit and PR with "Closes #<issue-number>"
```

**🎉 After Phase 2:** 7 features complete, ~500 TCK scenarios added

---

### Phase 3: Complex Features (7-9 hours)

Advanced but valuable features:

#### MATCH-CREATE (3-4h)
**Already works!** Just needs comprehensive tests and formalization

```bash
git checkout main && git pull
git checkout -b feature/match-create

# Focus on:
# - 15+ integration tests
# - Edge cases (multiple matches, no matches)
# - Documentation

# When done: commit and PR with "Closes #<issue-number>"
```

#### CASE expressions (4-5h)
**Complex but common**, save for last

```bash
git checkout main && git pull
git checkout -b feature/case-expressions

# Implement:
# 1. Grammar: CASE WHEN ... THEN ... ELSE ... END
# 2. AST: CaseExpression node
# 3. Evaluator: Short-circuit evaluation
# 4. Tests: 10 unit, 12 integration

# When done: commit and PR with "Closes #<issue-number>"
```

**🎉 After Phase 3:** All 9 features complete! Ready for v0.2.0 release

---

## Each Issue Has

✅ **Feature description** - What and why
✅ **Use cases** - Real-world examples
✅ **Cypher syntax** - Query examples with comments
✅ **Implementation checklist** - 10+ step-by-step tasks
✅ **Files to modify** - Exact paths (e.g., `src/graphforge/parser/cypher.lark:123`)
✅ **Code patterns** - Example implementations
✅ **Testing strategy** - Unit + integration test scenarios
✅ **Acceptance criteria** - Must-have requirements
✅ **References** - Links to openCypher spec, Neo4j docs
✅ **Effort estimate** - Time estimate in hours

**Example: UNWIND**
- 10 files to modify (with paths)
- Grammar, AST, parser, operator, planner, executor
- 5 unit tests + 8 integration tests
- Code example for executor pattern
- NULL handling semantics
- Order preservation requirements

---

## Workflow Checklist

For each issue:

### 1. Start Work
```bash
git checkout main
git pull origin main
git checkout -b feature/{issue-number}-{slug}
```

### 2. Implement
- [ ] Follow checklist in GitHub issue
- [ ] Update grammar if needed
- [ ] Add/modify AST nodes
- [ ] Update parser transformers
- [ ] Implement executor/evaluator logic
- [ ] Write unit tests (aim for 5-10 per feature)
- [ ] Write integration tests (aim for 8-15 per feature)

### 3. Test
```bash
# Run tests
uv run pytest tests/

# Check coverage
uv run pytest --cov=src/graphforge tests/

# Ensure >85% coverage maintained
```

### 4. Document
- [ ] Update `README.md` feature list if needed
- [ ] Update `CHANGELOG.md` under `[Unreleased]`
- [ ] Add docstrings to new functions
- [ ] Update examples if applicable

### 5. Create PR
```bash
git add .
git commit -m "feat: implement {feature}

Brief description of what was implemented.

Fixes #{issue-number}"

git push -u origin feature/{issue-number}-{slug}

gh pr create \
  --title "feat: implement {feature}" \
  --body "Closes #{issue-number}

## Summary
- Feature summary

## Testing
- Test summary

## Checklist
- [x] Tests passing
- [x] Coverage >85%
- [x] CHANGELOG updated
- [x] Documentation updated" \
  --label "enhancement,v0.2"
```

### 6. After Merge
```bash
git checkout main
git pull origin main
git branch -d feature/{issue-number}-{slug}

# Pick next issue and repeat!
```

---

## Common Patterns

### Adding a New Clause

1. **Grammar** (`src/graphforge/parser/cypher.lark`)
   ```lark
   new_clause: "NEW" expression
   ```

2. **AST** (`src/graphforge/ast/clause.py`)
   ```python
   @dataclass
   class NewClause:
       expression: Expression
   ```

3. **Parser** (`src/graphforge/parser/parser.py`)
   ```python
   def new_clause(self, args):
       return NewClause(expression=args[0])
   ```

4. **Operator** (`src/graphforge/planner/operators.py`)
   ```python
   @dataclass
   class New:
       expression: Expression
   ```

5. **Executor** (`src/graphforge/executor/executor.py`)
   ```python
   def _execute_new(self, op: New, input_rows):
       # Implementation
       return result_rows
   ```

### Adding an Aggregation Function

1. **Evaluator** (`src/graphforge/executor/evaluator.py`)
   ```python
   def aggregate_new(values: list[CypherValue]) -> CypherValue:
       # Implementation
       return result

   AGGREGATION_FUNCTIONS = {
       'new': aggregate_new,
       # ...
   }
   ```

### Adding an Operator

1. **Grammar** - Add to expression precedence
2. **AST** - Create expression node
3. **Parser** - Add transformer
4. **Evaluator** - Implement evaluation with NULL handling

---

## Testing Guidelines

### Unit Tests

**Parser tests** (`tests/unit/parser/test_parser.py`):
```python
def test_parse_new_clause():
    query = "NEW expression"
    result = parser.parse(query)
    assert isinstance(result.clauses[0], NewClause)
```

**Evaluator tests** (`tests/unit/executor/test_evaluator.py`):
```python
def test_evaluate_new_expression():
    expr = NewExpression(...)
    ctx = ExecutionContext()
    result = evaluate_expression(expr, ctx)
    assert isinstance(result, CypherValue)
```

### Integration Tests

**End-to-end tests** (`tests/integration/test_new_feature.py`):
```python
def test_new_feature_basic(db):
    db.execute("CREATE (n:Node {value: 1})")
    result = db.execute("MATCH (n) NEW n.value RETURN n")
    assert len(result) == 1
```

**Edge cases:**
- NULL handling
- Empty results
- Type errors
- Boundary conditions

---

## Troubleshooting

### Tests Failing

1. **Check grammar** - Is syntax parsed correctly?
2. **Check AST** - Are nodes created properly?
3. **Check evaluator** - Is NULL handling correct?
4. **Check test setup** - Is database state correct?

### Coverage Dropping

1. Add tests for uncovered branches
2. Test error conditions
3. Test NULL propagation
4. Test edge cases

### CI Failing

1. Check test output in GitHub Actions
2. Run same tests locally: `uv run pytest tests/`
3. Check formatting: `uv run ruff check src/ tests/`
4. Check types: `uv run mypy src/`

---

## Resources

### Documentation
- **Issue List:** the v0.2.0 release plan
- **Workflow Guide:** `.github/ISSUE_WORKFLOW.md`
- **Compatibility Matrix:** `docs/reference/opencypher-compatibility.md`
- **Architecture:** `docs/architecture-overview.md`
- **Testing Strategy:** `docs/testing-strategy.md`

### OpenCypher References
- **Spec:** https://opencypher.org/resources/
- **Neo4j Manual:** https://neo4j.com/docs/cypher-manual/
- **TCK:** https://github.com/opencypher/openCypher/tree/master/tck

### Getting Help
- Comment on the GitHub issue
- Review similar implemented features
- Check existing integration tests for patterns
- Reference Neo4j documentation for semantics

---

## Success Metrics

Track progress:
- [ ] 9/9 issues completed
- [ ] Test coverage >85% maintained
- [ ] CHANGELOG.md updated
- [ ] README.md features list updated
- [ ] TCK compliance measured (~950 scenarios target)
- [ ] Release notes drafted
- [ ] v0.2.0 tagged and published

---

## After v0.2.0 Release

1. **Celebrate!** 🎉
2. **Measure TCK compliance** - Verify ~950 scenarios
3. **Gather user feedback**
4. **Plan v0.3.0** - OPTIONAL MATCH, paths, etc.
5. **Continue toward v1.0** - 70-75% compliance

---

**Ready to build? Start with the NOT operator!** 🚀

Choose any feature from the v0.2.0 plan and follow the workflow above.
