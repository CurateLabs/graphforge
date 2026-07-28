# Quick Release Guide

This is a quick reference for creating releases. For comprehensive documentation, see [docs/RELEASE_PROCESS.md](docs/RELEASE_PROCESS.md).

## TL;DR - Release Checklist

```bash
# 1. Ensure main is clean and all tests pass
git checkout main
git pull
pytest tests/

# 2. Bump version (patch, minor, or major)
python scripts/bump_version.py minor

# 3. Edit CHANGELOG.md - add actual changes for the new version

# 4. Commit changes
git add pyproject.toml CHANGELOG.md
git commit -m "chore(release): bump version to X.Y.Z"

# 5. Push to main
git push origin main

# 6. Create and push tag
git tag -a vX.Y.Z -m "Release version X.Y.Z"
git push origin vX.Y.Z

# 7. Create GitHub Release (triggers PyPI publish automatically)
gh release create vX.Y.Z --title "GraphForge vX.Y.Z" --notes "See CHANGELOG.md"

# 8. Verify on PyPI
open https://pypi.org/project/graphforge/
```

## Semantic Versioning Quick Reference

```
MAJOR.MINOR.PATCH

Examples:
  0.1.1 → 0.1.2  (patch: bug fixes)
  0.1.1 → 0.2.0  (minor: new features, backwards-compatible)
  0.1.1 → 1.0.0  (major: breaking changes)
```

**When to bump:**

- **PATCH** (0.1.1 → 0.1.2): Bug fixes, documentation, no API changes
- **MINOR** (0.1.0 → 0.2.0): New features, backwards-compatible
- **MAJOR** (0.x.x → 1.0.0): Breaking API changes

## Pre-Release Checklist

Before running the release commands:

- [ ] All tests passing: `pytest tests/` (against the native `graphforge` wheel)
- [ ] Multi-surface coverage green: `make coverage` (Rust llvm-cov + Python wrapper + Node c8, ≥85% lines each)
- [ ] No lint errors: `ruff check .`
- [ ] Type checking passes: `mypy crates/gf-bindings-py/python/graphforge`
- [ ] Documentation updated
- [ ] All PRs merged to main
- [ ] No open critical bugs

## Post-Release Checklist

After creating the release:

- [ ] Verify GitHub Release created
- [ ] Verify PyPI publish succeeded
- [ ] Test installation: `pip install graphforge==X.Y.Z`
- [ ] Announce in GitHub Discussions (if major release)
- [ ] Update project status docs

## Common Commands

```bash
# Check current version
grep 'version =' pyproject.toml

# List all releases
gh release list
git tag -l

# View a specific release
gh release view v0.1.1

# Test package build locally
uv build
ls dist/

# Install from local build
pip install dist/graphforge-*.whl
```

## Hotfix Process

For critical bugs in production:

```bash
# 1. Create hotfix branch from main
git checkout -b hotfix/vX.Y.Z main

# 2. Fix the bug
git commit -m "fix: critical bug description"

# 3. Bump patch version
python scripts/bump_version.py patch

# 4. Update CHANGELOG with hotfix details
# (edit CHANGELOG.md)

# 5. Commit and push
git commit -am "chore(release): bump version to X.Y.Z"
git push origin hotfix/vX.Y.Z

# 6. Create PR (expedited review)
gh pr create --title "Hotfix vX.Y.Z" --label hotfix

# 7. After merge, tag and release immediately
git checkout main
git pull
git tag -a vX.Y.Z -m "Hotfix release X.Y.Z"
git push origin vX.Y.Z
gh release create vX.Y.Z --title "GraphForge vX.Y.Z (Hotfix)"
```

## Troubleshooting

**Q: Release failed to publish to PyPI**
```bash
# Check workflow logs
gh run view --log

# Manually publish if needed (requires PyPI token)
uv build
uv publish
```

**Q: Need to update CHANGELOG after release**
```bash
# Update CHANGELOG.md
# Create new patch release with updated docs
```

**Q: Wrong version published to PyPI**

Cannot unpublish! Options:
1. Yank the release on PyPI (marks as unsuitable)
2. Release new patch version with fixes

---

## Full Documentation

See [docs/RELEASE_PROCESS.md](docs/RELEASE_PROCESS.md) for:
- Detailed step-by-step instructions
- Pre-release versions (alpha, beta, rc)
- Version bumping strategies
- Best practices and anti-patterns
- Automation tools
- Troubleshooting guide
