## Description

<!-- Provide a clear and concise description of your changes -->

## Type of Change

<!-- Mark the relevant option with an "x" -->

- [ ] 🐛 Bug fix (non-breaking change which fixes an issue)
- [ ] ✨ New feature (non-breaking change which adds functionality)
- [ ] 💥 Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] 📚 Documentation update
- [ ] 🔧 Configuration/infrastructure change
- [ ] ♻️ Refactoring (no functional changes)
- [ ] ✅ Tests (adding or updating tests)
- [ ] 🎨 Style/formatting changes

## Related Issues

<!-- Link to related issues using #issue_number -->
<!-- Example: Fixes #<issue-number>, Relates to -->

Fixes #

## Changes Made

<!-- List the specific changes made in this PR -->

-
-
-

## Testing

<!-- Describe the tests you ran to verify your changes -->

### Test Coverage

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] TCK tests affected (if applicable)
- [ ] All existing tests pass
- [ ] Coverage maintained or improved

### Test Commands Run

```bash
# Example:
pytest tests/unit/
pytest tests/integration/
pytest -m "not slow"
```

## Checklist

<!-- Mark completed items with an "x" -->

### PR Size and Quality

- [ ] This PR is **small and focused** (< 300 lines preferred, < 500 max)
- [ ] This PR addresses **one logical change** (one feature OR one bug fix)
- [ ] All issues are **fixed properly** (no bandaids, workarounds, or TODOs)
- [ ] No `# type: ignore` comments without clear explanation
- [ ] No commented-out code or skipped tests
- [ ] All CI checks pass (no temporary bypasses)

### Code Quality

- [ ] My code follows the project's style guidelines
- [ ] I have performed a self-review of my code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] My changes generate no new warnings
- [ ] I have run `ruff format.` and `ruff check.`
- [ ] I have run `mypy src/` (if type hints added/changed)
- [ ] If this changes `Cargo.lock` / crate deps, I refreshed Bazel
      (`CARGO_BAZEL_REPIN=1`, lock/fingerprint) — Dependabot cargo PRs need this too

### Testing

- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally
- [ ] I have checked that code coverage has not decreased
- [ ] I have tested on multiple Python versions (if relevant)

### Documentation

- [ ] I have updated the documentation (if needed)
- [ ] I have added docstrings to new functions/classes
- [ ] I have updated the Cypher guide (if adding Cypher features)

### Compliance

- [ ] My changes follow openCypher specification (if applicable)
- [ ] I have checked TCK compliance impact (if Cypher changes)
- [ ] No breaking changes to existing APIs (or documented in description)

## Screenshots/Examples

<!-- If applicable, add screenshots or example code to demonstrate the changes -->

```python
# Example usage (if applicable)

```

## Performance Impact

<!-- Describe any performance implications of your changes -->

- [ ] No performance impact
- [ ] Performance improved
- [ ] Performance may be affected (explain below)

**Performance notes:**

## Breaking Changes

<!-- If this is a breaking change, describe what breaks and migration steps -->

- [ ] No breaking changes

**Migration guide:**

## Additional Context

<!-- Add any other context about the PR here -->

## Reviewer Notes

<!-- Specific areas you want reviewers to focus on -->

---

**By submitting this PR, I confirm that:**

- [ ] I have read and followed the [CONTRIBUTING.md](../CONTRIBUTING.md) guidelines
- [ ] I understand that intentionally submitted contributions are licensed under Apache-2.0
- [ ] I am contributing personally, or my employer has authorized this contribution
- [ ] I am authorized to make these contributions
