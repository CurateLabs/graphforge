# Release Process

This document describes GraphForge's professional versioning and release process.

## Table of Contents

- [Versioning Strategy](#versioning-strategy)
- [Release Types](#release-types)
- [Release Checklist](#release-checklist)
- [Creating a Release](#creating-a-release)
- [Post-Release Tasks](#post-release-tasks)
- [Hotfix Process](#hotfix-process)
- [Pre-release Versions](#pre-release-versions)

---

## Versioning Strategy

GraphForge follows **[Semantic Versioning 2.0.0](https://semver.org/)**:

```
MAJOR.MINOR.PATCH

Example: 1.2.3
         │ │ └─── PATCH: Backwards-compatible bug fixes
         │ └───── MINOR: New features, backwards-compatible
         └─────── MAJOR: Breaking changes
```

### One product version across registries

[ADR 0017](../adr/0017-unified-release-version.md) defines one shared release
set. All public `graphforge-*` crates, the `graphforge` Python package,
`@curatelabs/graphforge` and its five native npm packages, the CLI, and agent
skills use the same exact public `MAJOR.MINOR.PATCH` version.

Bindings are thin adapters over the Rust-owned product, not independently
versioned products. A partial registry failure does not authorize a
registry-specific patch version, even temporarily. Recovery either resumes
absent artifacts from the same immutable candidate or advances the complete
shared release set to one coordinated new version. Run
`python3 scripts/set_release_version.py --check` and validate the complete
candidate record before any publication credential can write.

### Version Increments

**MAJOR version** (1.0.0 → 2.0.0) when:
- Breaking API changes
- Incompatible changes to data formats
- Removal of deprecated features
- Changes requiring user migration

**Examples:**
- Changing column naming from `col_0` to `column_0`
- Removing deprecated methods
- Changing SQLite schema in incompatible way

**MINOR version** (1.2.0 → 1.3.0) when:
- New features (backwards-compatible)
- New Cypher clauses (OPTIONAL MATCH, UNWIND)
- Performance improvements
- Deprecations (with backward compatibility)

**Examples:**
- Adding OPTIONAL MATCH support
- Adding variable-length paths
- New aggregation functions
- Enhanced error messages

**PATCH version** (1.2.3 → 1.2.4) when:
- Bug fixes (backwards-compatible)
- Documentation updates
- Internal refactoring with no API changes
- Security patches

**Examples:**
- Fixing column naming regression
- Fixing SKIP/LIMIT empty results
- Documentation typos
- Dependency security updates

### Pre-1.0.0 Versions

GraphForge is currently in **0.x.x** (pre-1.0.0):
- **0.x.0**: Minor features, may include breaking changes
- **0.x.y**: Bug fixes and patches
- **Breaking changes are allowed** before 1.0.0

**Version 1.0.0** will be released when:
- API is stable and battle-tested
- Core openCypher features complete
- TCK compliance > 50%
- Production-ready with documented migration path

---

## Release Types

### Regular Release (Minor/Patch)

**Timeline:** Every 2-4 weeks

**Criteria:**
- All tests passing
- No critical bugs
- Documentation updated
- CHANGELOG.md updated

### Major Release

**Timeline:** When breaking changes are necessary

**Additional criteria:**
- Migration guide written
- Deprecation warnings in previous release
- Community notification (if applicable)
- Extended testing period

### Hotfix Release

**Timeline:** As soon as possible

**Criteria:**
- Critical bug or security issue
- Fixes only, no new features
- Fast-tracked review process

---

## Release Checklist

### Pre-Release (1-2 days before)

- [ ] All planned features merged to `main`
- [ ] Verify Apache-2.0 metadata and distributed copies:
      `python3 scripts/license_check.py --report license-compliance-report.json`
- [ ] All CI checks passing
- [ ] All tests passing (351 unit + integration tests)
- [ ] Code coverage ≥ 81% (current baseline)
- [ ] No open critical bugs
- [ ] Dependencies up to date
- [ ] Security scan clean (bandit)

### Documentation

- [ ] Update `CHANGELOG.md` with all changes since last release
- [ ] Update version in `pyproject.toml`
- [ ] Update version references in docs if changed
- [ ] Update README.md if major changes
- [ ] Update `docs/project-status-and-roadmap.md`
- [ ] Review all documentation for accuracy

### Testing

- [ ] Run full test suite locally: `pytest tests/`
- [ ] Test on Python 3.10, 3.11, 3.12, 3.13
- [ ] After merging, when assembling **publication** evidence for the v0.5.0 publication close-out issue,
      dispatch `Binding Release Candidate` with the exact current `main` commit
      SHA; branch-head and stale SHA dispatches are rejected before the platform
      matrix starts
- [ ] Confirm its Linux, macOS, and Windows Python/Node targets and cross-built
      Node package checks produce a passing aggregate artifact
- [ ] Link that same-SHA workflow run and aggregate artifact from the release
      close-out record (the v0.5.0 publication close-out issue)—not as a close ritual for child issues
- [ ] Run the standardized XS-XL matrix with `make release-load-matrix` on the
      release-candidate SHA and retain the accepted evidence bundle in the
      publication record
- [ ] After the exact-SHA Rust surface and Binding Release Candidate runs pass,
      dispatch `M1 Release Certification Gate` with the current `main` SHA and
      those two immutable run IDs. Confirm its 144-case load job and final
      `M1-Release-Certification-<sha>` aggregate artifact for v0.5.0 publication
      readiness. This cascade is release certification; it is not a
      close criterion for child implementation or construction issues (see
      `AGENTS.md` § Issue close)
- [ ] Test installation from built package: `uv build && uv tool install dist/graphforge-*.whl`
- [ ] Manual smoke tests with examples
- [ ] Test upgrade path from previous version (if applicable)

### Release

- [ ] Confirm the release commit contains `Apache-2.0` metadata, `LICENSE`,
      `NOTICE`, `legal/THIRD_PARTY_NOTICES.md` (and packaging copies), a green
      `cargo deny check licenses`, and a passing SHA-bound license compliance report
- [ ] Create release branch: `release/vX.Y.Z`
- [ ] Update version in `pyproject.toml`
- [ ] Update `CHANGELOG.md`: Move [Unreleased] to [X.Y.Z]
- [ ] Commit: `chore(release): bump version to X.Y.Z`
- [ ] Push release branch
- [ ] Create PR to `main`
- [ ] Get PR approval
- [ ] Merge to `main`
- [ ] Create Git tag: `git tag -a vX.Y.Z -m "Release version X.Y.Z"`
- [ ] Push tag: `git push origin vX.Y.Z`
- [ ] Create GitHub Release with release notes
- [ ] Verify PyPI publish workflow succeeds
- [ ] Verify package on PyPI: https://pypi.org/project/graphforge/

### Post-Release

- [ ] Announce release (if applicable)
- [ ] Close milestone (if using milestones)
- [ ] Create next milestone
- [ ] Update project boards
- [ ] Tweet/blog post (if major release)

---

## Creating a Release

For v0.5.0 registry sequencing, stop conditions, and rollback, follow
[`publication-order.md`](publication-order.md) (authoritative for §5/§6).

### Step-by-Step Guide

#### 1. Prepare Release Branch

```bash
# Ensure main is up to date
git checkout main
git pull origin main

# Create release branch
git checkout -b release/v0.2.0
```

#### 2. Update Version

Edit `pyproject.toml`:

```toml
[project]
name = "graphforge"
version = "0.2.0"  # ← Update this
```

#### 3. Update CHANGELOG.md

Move unreleased changes to new version section:

```markdown
## [Unreleased]

<!-- Empty or new features in development -->

## [0.2.0] - 2026-02-15

### Added
- Variable-length path support (`[*1..3]`)
- OPTIONAL MATCH for left outer joins

### Changed
- Improved error messages with error codes

### Fixed
- Edge case in aggregation handling

[Unreleased]: https://github.com/CurateLabs/graphforge-legecy/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/CurateLabs/graphforge-legecy/compare/v0.1.1...v0.2.0
```

#### 4. Commit and Push

```bash
# Commit changes
git add pyproject.toml CHANGELOG.md
git commit -m "chore(release): bump version to 0.2.0"

# Push release branch
git push -u origin release/v0.2.0
```

#### 5. Create and Merge PR

```bash
# Create PR
gh pr create --title "Release v0.2.0" --body "Release version 0.2.0

See CHANGELOG.md for details."

# Wait for CI to pass and get approval

# Merge (squash not recommended for release PRs)
gh pr merge --merge
```

#### 6. Create Git Tag

```bash
# Switch to main and pull
git checkout main
git pull origin main

# Create annotated tag
git tag -a v0.2.0 -m "Release version 0.2.0

## What's Changed
- Variable-length path support
- OPTIONAL MATCH clause
- Improved error messages

See CHANGELOG.md for full details."

# Push tag
git push origin v0.2.0
```

#### 7. Create GitHub Release

```bash
# Create release using GitHub CLI
gh release create v0.2.0 \
  --title "GraphForge v0.2.0" \
  --notes-file <(cat <<'EOF'
## What's New in v0.2.0

### 🚀 New Features

- **Variable-Length Paths**: Query transitive relationships with `[*1..3]` syntax
- **OPTIONAL MATCH**: Left outer join semantics for optional patterns

### 🔧 Improvements

- Enhanced error messages with error codes
- Better TCK compliance (now at 25%)

### 🐛 Bug Fixes

- Fixed edge case in aggregation handling

### 📦 Installation

```bash
pip install graphforge==0.2.0
# or
uv add graphforge==0.2.0
```

### 📖 Documentation

See [CHANGELOG.md](https://github.com/CurateLabs/graphforge-legecy/blob/v0.2.0/CHANGELOG.md) for complete details.

### 🙏 Contributors

Thanks to all contributors who made this release possible!
EOF
)
```

Or use the GitHub web interface:
1. Go to https://github.com/CurateLabs/graphforge/releases
2. Click "Draft a new release"
3. Choose tag: `v0.2.0`
4. Release title: `GraphForge v0.2.0`
5. Copy release notes from CHANGELOG
6. Check "Set as the latest release"
7. Click "Publish release"

#### 8. Verify Publication

```bash
# Wait for publish workflow to complete
gh run list --workflow=publish.yaml --limit 1

# Check PyPI
open https://pypi.org/project/graphforge/

# Test installation
pip install graphforge==0.2.0
```

---

## Native binding parity evidence

Every pull request that changes a binding or the Rust facade runs the required Linux
`Python Binding` and `Node Binding` jobs in `test.yml`. Those jobs build the wheel and addon
from the checked-out SHA, install or load those exact artifacts, run the native non-Cypher
matrix, and upload a JSON report containing the source SHA, package version, artifact hash,
classification hash, exact test identities, outcomes, and sanitized parity diff. The policy
job independently rejects an unclassified Rust, PyO3, or napi surface change.

The release-only `publish.yaml` expands the same native sources across the supported package
targets:

| Package | Release build coverage |
|---|---|
| Python abi3 wheel | Linux manylinux, macOS, Windows (CPython 3.10+) |
| Node addon | macOS x64/arm64, Linux x64/arm64, Windows x64 |

The release matrix is the omission list: adding a supported target requires adding it to the
workflow matrix and the package manifest together. Cross-platform publication does not
replace the required Linux behavior tests; it consumes the contract already proven on the
pull-request SHA. Final release readiness additionally requires clean-install evidence for
the published artifacts under the v0.5.0 release gate (#742 section 7 / #2795). Run the harness in
[`clean-environment-verification.md`](clean-environment-verification.md) (`make clean-env-verify`
or workflow_dispatch **Clean Environment Verify**) only after public registry publication;
preflight fails closed when packages are missing.

## Post-Release Tasks

### 1. Verify Package

```bash
# Install in fresh environment
uv venv test-env
source test-env/bin/activate
uv pip install graphforge==0.2.0

# Run quick test
python -c "from graphforge import GraphForge; print(GraphForge.__name__)"
```

### 2. Update Documentation Sites

If you have external docs:
- Update version in docs deployment
- Regenerate API documentation
- Update "latest" links

### 3. Announce Release

**GitHub Discussions:**
```markdown
# GraphForge v0.2.0 Released! 🎉

We're excited to announce the release of GraphForge v0.2.0!

## Highlights
- Variable-length path queries
- OPTIONAL MATCH support
- Improved error handling

Full details: https://github.com/CurateLabs/graphforge-legecy/releases/tag/v0.2.0
```

**Twitter/Social Media (if applicable):**
```
🎉 GraphForge v0.2.0 is out!

✨ Variable-length paths: [*1..3]
🔍 OPTIONAL MATCH support
🐛 Bug fixes and improvements

📦 pip install graphforge==0.2.0

#Python #GraphDatabase #OpenCypher
```

---

## Hotfix Process

For critical bugs in production release:

### 1. Create Hotfix Branch

```bash
# Branch from the release tag
git checkout -b hotfix/v0.2.1 v0.2.0

# Or from main if it's already ahead
git checkout -b hotfix/v0.2.1 main
```

### 2. Fix the Bug

```bash
# Make minimal changes to fix the issue
git commit -m "fix: critical bug in query execution"
```

### 3. Update Version and CHANGELOG

```bash
# Update pyproject.toml to 0.2.1
# Update CHANGELOG.md with hotfix details

git commit -m "chore(release): bump version to 0.2.1"
```

### 4. Fast-Track Review

```bash
# Create PR with "hotfix" label
gh pr create --title "Hotfix v0.2.1: Fix critical bug" \
             --label hotfix

# Get expedited review and merge
gh pr merge --merge
```

### 5. Release Immediately

```bash
# Create tag and release
git tag -a v0.2.1 -m "Hotfix release v0.2.1"
git push origin v0.2.1

gh release create v0.2.1 \
  --title "GraphForge v0.2.1 (Hotfix)" \
  --notes "## Critical Bug Fix

Fixed critical bug in query execution that caused...

This is a hotfix release. All users on v0.2.0 should upgrade immediately."
```

---

## Pre-release Versions

For testing before official release:

### Alpha Releases

**Purpose:** Early testing, unstable

**Version format:** `X.Y.Z-alpha.N` (e.g., `0.3.0-alpha.1`)

```bash
# Update version in pyproject.toml
version = "0.3.0-alpha.1"

# Create tag
git tag -a v0.3.0-alpha.1 -m "Alpha release for testing"
git push origin v0.3.0-alpha.1

# Create pre-release on GitHub (check "This is a pre-release")
gh release create v0.3.0-alpha.1 --prerelease \
  --title "GraphForge v0.3.0-alpha.1" \
  --notes "⚠️ Alpha release for testing only. Not for production use."
```

### Beta Releases

**Purpose:** Feature complete, needs testing

**Version format:** `X.Y.Z-beta.N` (e.g., `0.3.0-beta.1`)

```bash
version = "0.3.0-beta.1"
```

### Release Candidates

**Purpose:** Final testing before release

**Version format:** `X.Y.Z-rc.N` (e.g., `0.3.0-rc.1`)

```bash
version = "0.3.0-rc.1"
```

---

## Automation Tools

### Version Bumping Script

Create `scripts/bump-version.py`:

```python
#!/usr/bin/env python3
"""Bump version in pyproject.toml"""
import sys
import re
from pathlib import Path

def bump_version(bump_type):
    pyproject = Path("pyproject.toml")
    content = pyproject.read_text()

    # Find current version
    match = re.search(r'version = "(\d+)\.(\d+)\.(\d+)"', content)
    if not match:
        print("Error: Could not find version in pyproject.toml")
        sys.exit(1)

    major, minor, patch = map(int, match.groups())

    # Bump version
    if bump_type == "major":
        major += 1
        minor = 0
        patch = 0
    elif bump_type == "minor":
        minor += 1
        patch = 0
    elif bump_type == "patch":
        patch += 1
    else:
        print(f"Error: Invalid bump type '{bump_type}'")
        sys.exit(1)

    new_version = f"{major}.{minor}.{patch}"

    # Replace version
    new_content = re.sub(
        r'version = "\d+\.\d+\.\d+"',
        f'version = "{new_version}"',
        content
    )

    pyproject.write_text(new_content)
    print(f"Version bumped to {new_version}")
    return new_version

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bump-version.py [major|minor|patch]")
        sys.exit(1)

    bump_version(sys.argv[1])
```

Usage:
```bash
python scripts/bump-version.py patch  # 0.1.1 → 0.1.2
python scripts/bump-version.py minor  # 0.1.1 → 0.2.0
python scripts/bump-version.py major  # 0.1.1 → 1.0.0
```

---

## Best Practices

### DO ✅

- Follow semantic versioning strictly
- Update CHANGELOG.md for every release
- Test thoroughly before releasing
- Create annotated git tags with release notes
- Use GitHub Releases for visibility
- Announce breaking changes in advance
- Keep release notes user-focused
- Test package installation after publish

### DON'T ❌

- Skip version numbers (0.1.1 → 0.1.3)
- Make breaking changes in patch releases
- Release without updating CHANGELOG
- Push to PyPI without testing
- Create release tags from non-main branches (except hotfixes)
- Reuse or delete version tags
- Release on Fridays (avoid weekend emergencies)
- Rush releases without proper testing

---

## Troubleshooting

### Release Failed to Publish to PyPI

```bash
# Check workflow logs
gh run view --log

# Manually publish if needed (requires PyPI token)
uv build
uv publish
```

### Wrong Version Published

**Cannot unpublish from PyPI!** You can only:
1. Yank the release (marks it as unsuitable)
2. Publish a new patch version with fixes

```bash
# On PyPI web interface, click "Yank" for the bad version
# Then release a new version
```

### Tag Already Exists

```bash
# Delete local tag
git tag -d v0.2.0

# Delete remote tag (careful!)
git push origin :refs/tags/v0.2.0

# Create correct tag
git tag -a v0.2.0 -m "Release version 0.2.0"
git push origin v0.2.0
```

---

## Quick Reference

### Check Current Version

```bash
grep 'version =' pyproject.toml
```

### List All Releases

```bash
gh release list
git tag -l
```

### View Release Details

```bash
gh release view v0.1.1
git show v0.1.1
```

### Compare Versions

```bash
git diff v0.1.0..v0.1.1
gh compare v0.1.0...v0.1.1
```

---

## Related Documentation

- [Semantic Versioning 2.0.0](https://semver.org/)
- [Keep a Changelog](https://keepachangelog.com/)
- [GitHub Releases Guide](https://docs.github.com/en/repositories/releasing-projects-on-github)
- [Python Packaging Guide](https://packaging.python.org/)

---

**Last Updated:** 2026-02-01
