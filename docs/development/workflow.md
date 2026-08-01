# Development Workflow & CI/CD Guide

This document describes GraphForge's development workflow, CI/CD pipeline, and best practices for contributors.

## Table of Contents

1. [Development Setup](#development-setup)
2. [Branch Strategy](#branch-strategy)
3. [Pull Request Workflow](#pull-request-workflow)
4. [CI/CD Pipeline](#cicd-pipeline)
5. [Code Quality Tools](#code-quality-tools)
6. [GitHub Integrations](#github-integrations)
7. [Release Process](#release-process)

---

## Development Setup

### Prerequisites

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- Git
- [pre-commit](https://pre-commit.com/) (optional but recommended)

### Initial Setup

```bash
# Clone the repository
git clone https://github.com/CurateLabs/graphforge.git
cd graphforge

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync --all-extras

# Install pre-commit hooks (recommended)
uv run pre-commit install

# Run tests to verify setup
uv run pytest
```

### Pre-commit Hooks

Pre-commit hooks run automatically before each commit to catch issues early:

```bash
# Install hooks
uv run pre-commit install

# Run hooks manually on all files
uv run pre-commit run --all-files

# Run specific hook
uv run pre-commit run ruff --all-files

# Update hook versions
uv run pre-commit autoupdate
```

**Hooks configured:**
- **ruff**: Code formatting and linting with auto-fix
- **mypy**: Type checking
- **trailing-whitespace**: Remove trailing whitespace
- **end-of-file-fixer**: Ensure files end with newline
- **check-yaml/toml/json**: Validate config files
- **bandit**: Security vulnerability scanning
- **markdownlint**: Markdown formatting

---

## Branch Strategy

GraphForge follows a **Git Flow** inspired branching strategy:

### Branch Types

| Branch | Purpose | Protected | Base | Merge Into |
|--------|---------|-----------|------|------------|
| `main` | Production-ready code | ✅ Yes | - | - |
| `rust-core` | v0.5.0 Rust core staging | ✅ Yes | `main` | `main` (after merge gates) |
| `develop` | Integration branch | ✅ Yes | `main` | `main` |
| `feature/*` | New features | ❌ No | `develop` | `develop` |
| `fix/*` | Bug fixes | ❌ No | `develop` | `develop` |
| `hotfix/*` | Urgent production fixes | ❌ No | `main` | `main` + `develop` |
| `docs/*` | Documentation updates | ❌ No | `develop` | `develop` |
| `refactor/*` | Code refactoring | ❌ No | `develop` | `develop` |

!!! note "v0.5.0 development"
    All v0.5.0 (Rust core) development targets `main` directly via feature branches.
    Feature branches use the format `feature/<issue>-<description>`. Changes merge to `main`
    only after all merge gates pass — see [roadmap.md](../releases/roadmap.md) for the gate list.

### Branch Naming Conventions

```bash
# Features
feature/with-clause-implementation
feature/optional-match-support

# Bug fixes
fix/integration-test-aliasing
fix/null-handling-in-aggregation

# Hotfixes
hotfix/security-vulnerability-cve-2026-1234

# Documentation
docs/update-api-reference
docs/add-performance-guide

# Refactoring
refactor/simplify-executor-logic
refactor/extract-pattern-matcher
```

### Creating a Feature Branch

```bash
# Update develop branch
git checkout develop
git pull origin develop

# Create feature branch
git checkout -b feature/my-new-feature

# Make changes, commit, push
git add .
git commit -m "Add my new feature"
git push -u origin feature/my-new-feature
```

### Keeping Branch Up-to-Date

```bash
# Rebase on develop to incorporate latest changes
git fetch origin
git rebase origin/develop

# Or merge if you prefer (less clean history)
git merge origin/develop
```

---

## Pull Request Workflow

### PR Best Practices

#### Keep PRs Small and Focused

**CI/CD tools work best with small, reviewable PRs.**

Large PRs (1,000+ lines, 20+ files) are:
- ❌ Difficult for humans to review thoroughly
- ❌ Overwhelming for CI/CD tools (CodeRabbit, GitHub Actions)
- ❌ More likely to introduce bugs
- ❌ Slower to get merged
- ❌ Harder to debug if issues arise

**Optimal PR size:**
- ✅ **50-300 lines** of code changed
- ✅ **1-5 files** modified
- ✅ **Single focused change** (one feature, one bug fix)
- ✅ **Reviewable in < 30 minutes**

**How to break down large changes:**

```bash
# ❌ BAD: One massive PR
feature/add-with-clause (2,000 lines, 25 files)
  - Parser changes
  - AST nodes
  - Planner logic
  - Executor implementation
  - Integration tests
  - Documentation

# ✅ GOOD: Multiple focused PRs
feature/with-clause-parser (200 lines, 3 files)
  → PR merged

feature/with-clause-planner (180 lines, 2 files)
  → PR merged

feature/with-clause-executor (220 lines, 2 files)
  → PR merged

feature/with-clause-tests (150 lines, 2 files)
  → PR merged
```

**Tips for keeping PRs small:**
1. Use feature flags for incomplete features
2. Submit infrastructure changes separately from features
3. Refactor in one PR, add features in another
4. Break features into vertical slices (parser → planner → executor)

#### Fix Problems Properly (No Bandaids)

**When you encounter an issue, fix the root cause.**

**❌ Bandaid fixes to avoid:**
```python
# Hiding errors
try:
    result = function()
except:  # Catch everything and hope
    pass

# Ignoring type issues without understanding
value = get_value()  # type: ignore

# Commenting out failing tests
# def test_feature():
#     assert feature_works()  # TODO: fix later

# Skipping CI checks
pytest -k "not test_that_fails"
```

**✅ Proper fixes:**
```python
# Handle errors explicitly
try:
    result = process_data(input)
except ValidationError as e:
    logger.error(f"Invalid input: {e}")
    return None  # Explicit null case

# Fix type issues properly
value: str | None = get_value()  # Proper type annotation
if value is None:
    return default_value

# Fix failing tests
def test_feature():
    # Fixed the underlying bug in feature()
    assert feature_works()

# All tests must pass
pytest  # No skips, no ignores
```

**When tempted to use a bandaid, ask:**
1. **Why** is this failing?
2. What's the **root cause**?
3. How can I **fix it properly**?
4. What **tests** will prevent regression?

**Common scenarios:**

| Issue | ❌ Bandaid | ✅ Proper Fix |
|-------|-----------|---------------|
| Type error | Add `# type: ignore` | Fix type annotations |
| Failing test | Comment out test | Fix the bug or update test |
| CI failure | Skip check temporarily | Fix the underlying issue |
| Import error | Wrap in try/except | Install missing dependency |
| Flaky test | Mark as `@pytest.mark.skip` | Make test deterministic |

### 1. Create Pull Request

1. Push your branch to GitHub
2. Navigate to repository and click "New Pull Request"
3. Fill out the PR template (auto-populated)
4. Add reviewers (CODEOWNERS auto-assigned)
5. Link related issues using "Fixes #<issue-number>" or "Relates to "

### 2. PR Template Sections

When creating a PR, complete these sections:

- **Description**: Clear summary of changes
- **Type of Change**: Bug fix, feature, breaking change, etc.
- **Related Issues**: Link to issues this addresses
- **Changes Made**: Bullet list of specific changes
- **Testing**: What tests were added/modified
- **Checklist**: Code quality, testing, documentation items

### 3. Automated Checks

When you open a PR, these checks run automatically:

| Check | Purpose | Required |
|-------|---------|----------|
| **Test Suite** | Run all tests on 3 OS × 4 Python versions | ✅ Yes |
| **Lint** | Ruff formatting and linting | ✅ Yes |
| **Type Check** | mypy type validation | ✅ Yes |
| **Security** | Bandit vulnerability scanning | ✅ Yes |
| **Coverage** | Code coverage report (85% threshold) | ⚠️ Warning |
| **CodeRabbit** | AI-powered code review | ℹ️ Advisory |

### 4. Code Review Process

1. **Automated Review**: CodeRabbit provides initial feedback
2. **Self-Review**: Review your own changes first
3. **Peer Review**: CODEOWNERS review and approve
4. **Address Feedback**: Make requested changes
5. **Re-request Review**: After making changes

### 5. Merging

**Requirements before merging:**
- ✅ All CI checks pass (no exceptions)
- ✅ At least 1 approval from CODEOWNERS
- ✅ No unresolved conversations
- ✅ Branch is up-to-date with base branch
- ✅ No merge conflicts
- ✅ No bandaid fixes (all issues properly resolved)
- ✅ PR is reasonably sized (< 500 lines preferred)
- ✅ All tests passing (no skipped or commented tests)
- ✅ No temporary workarounds or TODOs introduced

**Merge strategies:**
- **Squash and merge** (preferred): Clean history, single commit per PR
- **Rebase and merge**: Preserve commits, linear history
- **Merge commit**: Keep full branch history (avoid for simple PRs)

```bash
# After PR approval, merge via GitHub UI or:
git checkout develop
git merge --squash feature/my-feature
git commit -m "Add my feature (#<issue-number>)"
git push origin develop
```

---

## CI/CD Pipeline

### GitHub Actions Workflows

#### 1. Test Suite (`test.yml`)

Runs on every push to `main`/`develop` and all PRs to `main`.

**Jobs:**

**test** (12 configurations: 3 OS × 4 Python versions)
```yaml
# Runs unit tests with coverage
# Runs integration tests
# Uploads coverage to Codecov (Ubuntu 3.12 only)
```

**lint** (Ubuntu, Python 3.12)
```yaml
# ruff format --check .
# ruff check .
```

**type-check** (Ubuntu, Python 3.12)
```yaml
# mypy src/graphforge --strict-optional
```

**security** (Ubuntu, Python 3.12)
```yaml
# bandit -c pyproject.toml -r src/
```

**coverage** (Ubuntu, Python 3.12)
```yaml
# pytest --cov=src --cov-report=html
# Uploads HTML coverage report as artifact
# Soft fail if coverage < 85%
```

**Local equivalent:**
```bash
# Run full test suite locally
uv run pytest -m "not slow"

# Run with coverage
uv run pytest --cov=src --cov-report=term --cov-report=html

# Lint
uv run ruff format --check .
uv run ruff check .

# Type check
uv run mypy src/graphforge

# Security scan
uv run bandit -c pyproject.toml -r src/
```

#### 2. Publish (`publish.yaml`)

Triggered on GitHub release publication.

```yaml
# Builds package: uv build
# Publishes to PyPI: uv publish (uses trusted publishing)
```

### Status Badges

Add to README.md:

```markdown
![Tests](https://github.com/CurateLabs/graphforge/workflows/Test%20Suite/badge.svg)
![Coverage](https://codecov.io/gh/CurateLabs/graphforge/branch/main/graph/badge.svg)
![Python](https://img.shields.io/pypi/pyversions/graphforge)
![PyPI](https://img.shields.io/pypi/v/graphforge)
![License](https://img.shields.io/github/license/CurateLabs/graphforge)
```

---

## Code Quality Tools

### Ruff (Linting & Formatting)

**Configuration:** `pyproject.toml` → `[tool.ruff]`

```bash
# Format code
uv run ruff format .

# Check formatting
uv run ruff format --check .

# Lint with auto-fix
uv run ruff check --fix .

# Lint without changes
uv run ruff check .
```

**Rules enabled:**
- E/W (pycodestyle)
- F (Pyflakes)
- I (isort)
- N (pep8-naming)
- UP (pyupgrade)
- B (flake8-bugbear)
- C4 (comprehensions)
- SIM (simplify)
- RET (return)
- ARG (unused-arguments)
- PTH (use-pathlib)
- PL (Pylint)
- PERF (performance)
- RUF (ruff-specific)

### mypy (Type Checking)

**Configuration:** `pyproject.toml` → `[tool.mypy]`

```bash
# Type check entire codebase
uv run mypy src/graphforge

# Type check with verbose output
uv run mypy src/graphforge --show-error-codes --pretty

# Type check tests (looser rules)
uv run mypy tests/
```

**Settings:**
- `python_version = "3.10"`
- `check_untyped_defs = true`
- `strict_equality = true`
- Gradually enabling strict mode

### Bandit (Security Scanning)

**Configuration:** `pyproject.toml` → `[tool.bandit]`

```bash
# Run security scan
uv run bandit -c pyproject.toml -r src/

# Generate detailed report
uv run bandit -c pyproject.toml -r src/ -f json -o bandit-report.json
```

**Configured to skip:**
- B101 (assert_used) in tests and internal validation

### pytest (Testing)

**Configuration:** `pyproject.toml` → `[tool.pytest.ini_options]`

```bash
# Run all tests
uv run pytest

# Run specific test types
uv run pytest -m unit
uv run pytest -m integration
uv run pytest -m tck

# Run with coverage
uv run pytest --cov=src --cov-report=term --cov-report=html

# Run in parallel
uv run pytest -n auto

# Run with verbose output
uv run pytest -v --tb=long --showlocals
```

**Test markers:**
- `unit`: Fast, isolated unit tests
- `integration`: Integration tests (may use I/O)
- `tck`: openCypher TCK compliance tests
- `property`: Property-based tests (hypothesis)
- `slow`: Tests taking >1s

---

## GitHub Integrations

### CodeRabbit AI Code Review

**Setup:**

1. Install CodeRabbit app from [GitHub Marketplace](https://github.com/marketplace/coderabbitai)
2. Authorize for the CurateLabs/graphforge repository
3. Configuration is in `.coderabbit.yaml`

**Features:**
- **Automatic reviews** on all PRs
- **Inline suggestions** with explanations
- **Security checks** for common vulnerabilities
- **Best practices** enforcement
- **openCypher compliance** validation

**Usage:**

CodeRabbit automatically comments on PRs with:
- Code quality issues
- Potential bugs
- Performance concerns
- Security vulnerabilities
- Best practice violations

**Commands in PR comments:**
```
@coderabbitai review
@coderabbitai help
@coderabbitai explain [file:line]
```

**Configuration highlights:**
- Profile: `chill` (balanced feedback)
- Focus: openCypher correctness, type safety, performance
- Python style: PEP 8, Google docstrings
- Thresholds: Max complexity 15, min coverage 85%

### Dependabot (Dependency Updates)

**Setup:**

Dependabot is configured in `.github/dependabot.yml` and runs automatically.

**Features:**
- **Weekly updates** for Python dependencies
- **Weekly updates** for GitHub Actions
- **Grouped updates** (production vs dev dependencies)
- **Auto-assignment** to the maintainer

**Managing Dependabot PRs:**

```bash
# Dependabot PRs are labeled "dependencies"
# Review changes in PR
# Merge if tests pass

# To ignore a specific version:
@dependabot ignore this major version
@dependabot ignore this minor version
@dependabot ignore this dependency
```

### Codecov (Coverage Tracking)

**Setup:**

1. Sign up at [codecov.io](https://codecov.io/)
2. Connect GitHub repository
3. Token is auto-configured in Actions

**Features:**
- **Coverage trends** over time
- **PR comments** with coverage diff
- **Badge** for README
- **Threshold enforcement** (85%)

**Viewing reports:**
- View on codecov.io dashboard
- Download HTML report from Actions artifacts
- Local: `uv run pytest --cov=src --cov-report=html && open htmlcov/index.html`

---

## Branch Protection Rules

### Configuring Branch Protection

**For `main` branch:**

1. Go to Settings → Branches → Add rule
2. Branch name pattern: `main`
3. Enable these settings:

**Require pull request reviews:**
- ✅ Require pull request before merging
- ✅ Require 1 approval
- ✅ Dismiss stale reviews when new commits pushed
- ✅ Require review from Code Owners

**Require status checks:**
- ✅ Require status checks to pass before merging
- ✅ Require branches to be up to date
- Required checks:
  - `Test Python 3.10 on ubuntu-latest`
  - `Test Python 3.12 on ubuntu-latest`
  - `Lint and Format Check`
  - `Type Checking (mypy)`
  - `Security Scanning`

**Additional settings:**
- ✅ Require conversation resolution before merging
- ✅ Require signed commits (optional)
- ✅ Include administrators (enforce rules for admins)
- ✅ Restrict who can push (only maintainers)
- ❌ Allow force pushes (disabled)
- ❌ Allow deletions (disabled)

**For `develop` branch:**

Same as `main` but:
- Require 1 approval (can be more lenient)
- Required checks: Ubuntu tests only (faster feedback)

### Bypassing Protection (Emergency)

In rare emergencies, admins can:

1. Temporarily disable branch protection
2. Make critical fix
3. Re-enable protection immediately

**Document all bypasses** in commit message and notify team.

---

## Release Process

### Versioning Strategy

GraphForge follows [Semantic Versioning](https://semver.org/):

```
MAJOR.MINOR.PATCH
```

- **MAJOR**: Breaking changes (e.g., 1.0.0 → 2.0.0)
- **MINOR**: New features, backward compatible (e.g., 0.1.0 → 0.2.0)
- **PATCH**: Bug fixes, backward compatible (e.g., 0.1.1 → 0.1.2)

### Release Checklist

1. **Update Version**
   ```bash
   # Update version in pyproject.toml
   version = "0.2.0"
   ```

2. **Prepare GitHub Release notes**
   - Prefer notes generated from the immutable GitHub comparison.
   - Supply explicit notes only when the generated comparison needs operator
     context.

3. **Commit and Tag**
   ```bash
   git add pyproject.toml
   git commit -m "chore: bump version to 0.2.0"
   git tag -a v0.2.0 -m "Release version 0.2.0"
   git push origin develop --tags
   ```

4. **Merge to Main**
   ```bash
   # Create PR from develop to main
   git checkout main
   git merge develop
   git push origin main
   ```

5. **Create GitHub Release**
   - Go to Releases → Draft a new release
   - Tag: `v0.2.0`
   - Title: `GraphForge v0.2.0`
   - Description: Generate notes from the GitHub comparison
   - Click "Publish release"

6. **Automated Publish**
   - GitHub Action automatically builds and publishes to PyPI
   - Monitor Actions tab for completion

7. **Verify Release**
   ```bash
   # Check PyPI
   pip install --upgrade graphforge
   python -c "import graphforge; print(graphforge.__version__)"
   ```

### Hotfix Process

For critical bugs in production:

```bash
# Create hotfix branch from main
git checkout main
git checkout -b hotfix/critical-bug-fix

# Make fix, test thoroughly
# ...

# Merge to main
git checkout main
git merge hotfix/critical-bug-fix
git tag -a v0.1.2 -m "Hotfix: Critical bug"
git push origin main --tags

# Also merge to develop
git checkout develop
git merge hotfix/critical-bug-fix
git push origin develop

# Create GitHub release (triggers publish)
```

---

## Best Practices Summary

### Before Committing

- [ ] Run `uv run pytest` (all tests pass)
- [ ] Run `uv run ruff format.` (code formatted)
- [ ] Run `uv run ruff check.` (no lint errors)
- [ ] Run `uv run mypy src/graphforge` (type checks pass)
- [ ] Update tests for new features
- [ ] Update documentation if needed
- [ ] Write clear commit messages

### Commit Message Format

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Formatting, no code change
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance

**Examples:**
```
feat(executor): implement WITH clause for query chaining

Add support for WITH clause enabling multi-part queries.
Includes projection, filtering, sorting, and pagination.

Fixes #<issue-number>
```

```
fix(executor): correct column aliasing in RETURN projection

Fixed bug where RETURN aliases were ignored, causing
integration test failures.

Fixes #<issue-number>
```

### PR Best Practices

- **Small PRs**: Keep PRs focused (< 500 lines changed)
- **Clear titles**: Use conventional commit format
- **Complete template**: Fill out all PR template sections
- **Link issues**: Use "Fixes #<issue-number>" in description
- **Self-review**: Review your own code first
- **Responsive**: Address review comments promptly
- **Clean commits**: Squash fixup commits before merging

### Code Review Guidelines

**As a reviewer:**
- ✅ Check correctness and logic
- ✅ Verify tests cover changes
- ✅ Look for security issues
- ✅ Ensure documentation updated
- ✅ Check performance implications
- ✅ Be constructive and kind

**As an author:**
- ✅ Accept feedback graciously
- ✅ Explain decisions clearly
- ✅ Ask questions if unclear
- ✅ Iterate until approved
- ✅ Thank reviewers

---

## Troubleshooting

### Pre-commit Hooks Failing

```bash
# Skip hooks temporarily (not recommended)
git commit --no-verify

# Fix specific hook failure
uv run pre-commit run <hook-name> --all-files

# Update hooks to latest versions
uv run pre-commit autoupdate
```

### CI Checks Failing

```bash
# Reproduce CI failures locally
uv run pytest -m "unit and integration"
uv run ruff check .
uv run mypy src/graphforge
uv run bandit -c pyproject.toml -r src/

# Check specific OS/Python version (use Docker)
docker run -it python:3.10 bash
```

### Merge Conflicts

```bash
# Update branch with latest develop
git fetch origin
git rebase origin/develop

# Resolve conflicts
# Edit conflicted files
git add <resolved-files>
git rebase --continue
```

### Dependabot Issues

```bash
# Close unwanted Dependabot PR
@dependabot ignore this dependency

# Rebase Dependabot PR
@dependabot rebase
```

---

## Resources

- **Contributing Guide**: CONTRIBUTING.md (repository root)
- **Security Policy**:.github/SECURITY.md (repository root)
- **Issue Templates**:.github/ISSUE_TEMPLATE/ (repository root)
- **CI/CD Workflows**:.github/workflows/ (repository root)
- **Pre-commit Config**:.pre-commit-config.yaml (repository root)
- **CodeRabbit Config**:.coderabbit.yaml (repository root)

---

**Last Updated:** January 31, 2026

For questions about the development workflow, open a [discussion](https://github.com/CurateLabs/graphforge/discussions) or create an issue.
