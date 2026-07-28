# Codecov Integration Guide

**Date:** 2026-02-01
**Status:** ✅ Configured

## Overview

GraphForge uses [Codecov](https://codecov.io) for comprehensive code coverage tracking and reporting. Codecov automatically analyzes coverage reports from our CI pipeline and provides:

- Coverage trends over time
- Pull request coverage comments
- Component-level coverage tracking
- Branch coverage analysis

## Configuration

### GitHub Actions Integration

Coverage is uploaded to Codecov in two places:

1. **Test Job** (Unit Tests Only)
   - Runs on: Ubuntu Latest + Python 3.12
   - Flags: `unittests`
   - File: `coverage.xml`

2. **Coverage Job** (Full Coverage)
   - Runs on: Ubuntu Latest + Python 3.12
   - Flags: `full-coverage`
   - Includes: Unit + Integration tests
   - File: `coverage.xml`

### Workflow Configuration

```yaml
- name: Run tests with coverage
  run: uv run pytest tests/unit tests/integration --cov=src --cov-branch --cov-report=xml

- name: Upload coverage to Codecov
  uses: codecov/codecov-action@v5
  with:
    token: ${{ secrets.CODECOV_TOKEN }}
    file: ./coverage.xml
    flags: full-coverage
    fail_ci_if_error: false
```

### Required Secret

The workflow requires a `CODECOV_TOKEN` repository secret:

**Token:** `7d70de5b-7352-4300-8ed0-ae82ecb3b452`

**How to add:**
1. Go to GitHub repository settings
2. Navigate to: Settings > Secrets and variables > Actions
3. Click "New repository secret"
4. Name: `CODECOV_TOKEN`
5. Value: (paste token above)

## Codecov Configuration

GraphForge uses a custom `.codecov.yml` configuration file with the following settings:

### Coverage Targets

- **Project Coverage:** 85% target, 2% threshold
- **Patch Coverage:** 80% target for new code
- **Precision:** 2 decimal places
- **Range:** 70% (red) to 95% (green)

### Component Tracking

Coverage is tracked separately for each component:

- `parser` - Cypher parser and grammar
- `planner` - Query planning and optimization
- `executor` - Query execution engine
- `storage` - Graph storage and persistence
- `ast` - Abstract syntax tree
- `types` - Type system and value types

### Comment Behavior

Codecov will:
- Comment on every pull request
- Show header, diff, flags, components, and footer
- Include coverage changes for each component
- Highlight lines with decreased coverage

## Coverage Requirements

### Project-Wide

- **Minimum:** 85%
- **Threshold:** Cannot drop by more than 2%
- **Current:** ~85-90% (varies by component)

### New Code (Patches)

- **Minimum:** 80%
- **Threshold:** Cannot drop by more than 5%
- **Goal:** All new features fully tested

## Viewing Coverage Reports

### On GitHub

1. **Pull Requests:** Codecov comments appear automatically
2. **Commit Status:** Coverage status shown in commit checks
3. **Branch Badge:** See README.md for live coverage badge

### On Codecov Dashboard

Visit: https://codecov.io/gh/DecisionNerd/graphforge

**Dashboard features:**
- Coverage trends over time
- Component-level breakdown
- File-level coverage details
- Commit history and changes
- Sunburst charts for visual coverage

## Local Coverage Reports

### Generate HTML Report

```bash
# Run tests with coverage
uv run pytest tests/unit tests/integration --cov=src --cov-branch --cov-report=html

# Open in browser
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
start htmlcov/index.html  # Windows
```

### Generate Terminal Report

```bash
# Detailed terminal report
uv run pytest tests/unit tests/integration --cov=src --cov-branch --cov-report=term

# Show missing lines
uv run coverage report --show-missing
```

### Generate XML Report (for Codecov)

```bash
# XML report (used by Codecov)
uv run pytest tests/unit tests/integration --cov=src --cov-branch --cov-report=xml

# File created: coverage.xml
```

## Coverage Flags

GraphForge uses two coverage flags:

### `unittests`

- **Source:** Test job (unit tests only)
- **Purpose:** Fast feedback on core code coverage
- **When:** Every commit on all OS/Python combinations

### `full-coverage`

- **Source:** Coverage job (unit + integration)
- **Purpose:** Complete coverage including end-to-end tests
- **When:** Every commit on Ubuntu + Python 3.12

## Ignored Files

The following are excluded from coverage:

- `tests/**` - Test files
- `docs/**` - Documentation
- `scripts/**` - Utility scripts
- `examples/**` - Example code
- `**/__init__.py` - Package initialization files

## Coverage Best Practices

### Writing Tests

1. **Unit tests** for logic and edge cases
2. **Integration tests** for end-to-end workflows
3. **Property tests** for complex invariants
4. **TCK tests** for openCypher compliance

### Maintaining Coverage

1. **Before PR:** Run tests locally with coverage
2. **Check Codecov comment** on your PR
3. **Address uncovered lines** if coverage drops
4. **Aim for 100%** on new code when possible

### When Coverage Drops

**Acceptable reasons:**
- Defensive error handling (rare edge cases)
- Deprecated code paths (marked for removal)
- External library integration (hard to mock)

**Address immediately:**
- Core logic without tests
- New features missing tests
- Regression in existing coverage

## Troubleshooting

### Coverage Not Uploading

**Check:**
1. Is `CODECOV_TOKEN` secret set?
2. Did the test job complete successfully?
3. Was `coverage.xml` generated?
4. Check GitHub Actions logs for errors

**Common issues:**
- Missing token → Add `CODECOV_TOKEN` secret
- No coverage file → Ensure `--cov-report=xml` flag
- Upload failed → Check Codecov status page

### Coverage Seems Wrong

**Possible causes:**
1. **Cache issues** - Clear `.pytest_cache/`
2. **Partial runs** - Ensure all tests ran
3. **Import errors** - Check test output for failures
4. **Branch coverage** - Ensure `--cov-branch` flag used

**Fix:**
```bash
# Clear cache
rm -rf .pytest_cache/ .coverage htmlcov/

# Run clean coverage
uv run pytest tests/ --cov=src --cov-branch --cov-report=term
```

### Codecov Comment Not Appearing

**Requirements:**
- PR must be from same repository (not fork)
- Codecov app must be installed on repository
- Token must be valid
- At least one coverage upload succeeded

**Check:**
- Repository settings > Integrations > Codecov
- Codecov dashboard for upload logs
- GitHub Actions logs for upload status

## Integration Timeline

- **2026-02-01:** Codecov integration configured
- **Next PR:** First coverage comment will appear
- **Ongoing:** Coverage tracked on all PRs and commits

## Resources

- [Codecov Dashboard](https://codecov.io/gh/DecisionNerd/graphforge)
- [Codecov Documentation](https://docs.codecov.com/)
- [GitHub Actions Integration](https://docs.codecov.com/docs/github-actions)
- [Coverage.py Documentation](https://coverage.readthedocs.io/)

## Maintenance

### Updating Coverage Targets

Edit `.codecov.yml`:

```yaml
coverage:
  status:
    project:
      default:
        target: 85%  # Change this
```

### Adding New Components

Edit `.codecov.yml`:

```yaml
component_management:
  individual_components:
    - component_id: new-component
      paths:
        - src/graphforge/new-component/**
```

### Changing Comment Behavior

Edit `.codecov.yml`:

```yaml
comment:
  behavior: default  # or "once", "new", "off"
```

---

**Questions?** See the Codecov documentation or open a GitHub issue.
