# Release Strategy & Cadence

This document explains **when and how** to create releases for GraphForge.

## TL;DR

- **Merge to main frequently** (small PRs, continuous integration)
- **Release periodically** when enough value accumulates
- **Don't release every merge** - batch changes for meaningful releases
- **Use labels** to track what goes in next release

---

## Release Cadence

### Regular Release Schedule

GraphForge follows a **time-based + feature-based** release strategy:

**Minor Releases (0.x.0):**
- **Frequency:** Every 3-4 weeks
- **Contains:** New features, accumulated bug fixes, improvements
- **Triggered by:** Significant features complete OR scheduled date

**Patch Releases (0.x.y):**
- **Frequency:** As needed, typically 1-2 weeks after minor release
- **Contains:** Bug fixes, documentation updates, small improvements
- **Triggered by:** Critical bugs OR enough fixes accumulated

**Hotfix Releases (immediate):**
- **Frequency:** As soon as possible
- **Contains:** Critical bug fixes only
- **Triggered by:** Security issues, data loss bugs, breaking bugs

### When NOT to Release

❌ **Don't release for:**
- Single small bug fix (unless critical)
- Documentation-only changes
- Internal refactoring with no user impact
- CI/CD configuration changes
- Dependency updates (unless they fix vulnerabilities)

✅ **Instead:** Accumulate these changes and release together

---

## Decision Matrix: Should You Release?

### Release Now ✅

**If ANY of these are true:**

1. **Critical bug fix**
   - Security vulnerability
   - Data loss or corruption
   - Complete feature breakage
   - → Hotfix release (patch bump)

2. **Major feature complete**
   - Significant new capability (OPTIONAL MATCH, variable-length paths)
   - Large improvement to existing feature
   - → Minor release

3. **Scheduled release date**
   - 3-4 weeks since last release
   - Multiple changes accumulated
   - → Minor or patch release

4. **Breaking change needed**
   - API change required
   - → Major release (or 0.x.0 if pre-1.0)

### Accumulate Changes 📦

**If ALL of these are true:**

1. No critical bugs
2. No major features complete
3. Less than 2 weeks since last release
4. Changes are small fixes or improvements

**→ Merge to main, add to CHANGELOG [Unreleased], wait for more changes**

---

## PR Lifecycle

### 1. PR Created

Add label indicating release impact:

- `release:patch` - Bug fix, small improvement
- `release:minor` - New feature, significant change
- `release:major` - Breaking change
- `release:none` - Docs, CI, internal only (no release needed)

### 2. PR Merged to Main

```bash
# PR merges to main
git checkout main
git merge pr-branch

# Update CHANGELOG [Unreleased] section
# NO release created yet - just merged to main
```

**Main branch is always deployable but not always released.**

### 3. Accumulation Phase

Multiple PRs merge over days/weeks:

```
main: [PR] ── [PR] ── [PR] ── [PR]
       │           │           │           │
       patch       docs        patch       feature
```

**Unreleased section in CHANGELOG.md grows:**

```markdown
## [Unreleased]

### Added
- Variable-length path support 

### Fixed
- Fixed column naming in aggregations 
- Fixed null handling in WHERE clause 

### Changed
- Updated documentation for WITH clause 
```

### 4. Release Decision

**Release when:**
- Enough changes accumulated (5-10 items in [Unreleased])
- Major feature complete
- Scheduled date reached
- Critical bug needs immediate fix

**Trigger release:**

```bash
# Review [Unreleased] section
cat CHANGELOG.md

# Decide: minor (feature) or patch (fixes only)
python scripts/bump_version.py minor

# Complete release process (see RELEASING.md)
git commit -am "chore(release): bump version to 0.2.0"
git push origin main
git tag -a v0.2.0 -m "Release version 0.2.0"
git push origin v0.2.0
gh release create v0.2.0
```

**This triggers PyPI publish automatically.**

---

## PyPI Publishing

### Automatic Publishing

PyPI publish is **automated** via GitHub Actions:

```yaml
# .github/workflows/publish.yaml
on:
  release:
    types: [published]
```

**Workflow:**
1. Create GitHub Release → Triggers workflow
2. Workflow runs `uv build` → Creates wheel and sdist
3. Workflow runs `uv publish` → Uploads to PyPI
4. Package available at https://pypi.org/project/graphforge/

**You never manually publish to PyPI** - GitHub Actions does it.

### When PyPI Gets Updated

PyPI is updated **only when you create a GitHub Release:**

```bash
# This DOES trigger PyPI publish ✅
gh release create v0.2.0 --title "GraphForge v0.2.0"

# These DO NOT trigger PyPI publish ❌
git push origin main               # Just a commit
git tag v0.2.0                     # Just a tag
gh pr merge 123                    # Just merging a PR
```

### Version on PyPI vs Main Branch

It's normal and expected for these to differ:

```
PyPI:          0.1.1  ← Latest release (stable)
main branch:   0.1.1  ← Version in code (same until bump)
[Unreleased]:  n/a    ← Changes accumulated but not released
```

After several PRs merge:

```
PyPI:          0.1.1  ← Still latest release
main branch:   0.1.1  ← Still same version (not bumped yet)
[Unreleased]:  →      ← 5 changes ready for release
               → Feature A
               → Bug fix B
               → Bug fix C
               → Improvement D
               → Docs update E
```

When you release 0.2.0:

```
PyPI:          0.2.0  ← New release published
main branch:   0.2.0  ← Version bumped
[Unreleased]:  (empty) ← Moved to [0.2.0]
```

---

## Labels for Release Tracking

Use these labels on PRs to track what needs releasing:

### Release Impact Labels

- **`release:major`** - Breaking change, requires major version bump
- **`release:minor`** - New feature, requires minor version bump
- **`release:patch`** - Bug fix, requires patch version bump
- **`release:none`** - No release needed (docs, CI, internal)

### Release Status Labels

- **`release:pending`** - Merged to main, not yet released
- **`release:included`** - Included in a release

### Example Workflow

```bash
# 1. PR is created
gh pr create --label "release:minor"

# 2. PR is merged
# GitHub Action automatically adds "release:pending"

# 3. When release is created
# GitHub Action automatically:
#   - Removes "release:pending"
#   - Adds "release:included"
#   - Adds milestone "v0.2.0"
```

---

## Checking What's Ready to Release

### View Unreleased Changes

```bash
# Check CHANGELOG [Unreleased] section
cat CHANGELOG.md | sed -n '/## \[Unreleased\]/,/## \[/p'
```

### Count Unreleased PRs

```bash
# List PRs merged but not released
gh pr list --state merged --label "release:pending"
```

### Compare with Last Release

```bash
# See all commits since last release
git log v0.1.1..HEAD --oneline

# See diff since last release
git diff v0.1.1..HEAD --stat
```

---

## Release Decision Examples

### Example 1: Accumulate Patches

**Scenario:**
- Monday: a prior pull request fixes small bug (merged)
- Tuesday: a prior pull request updates docs (merged)
- Wednesday: a prior pull request fixes another small bug (merged)

**Decision:** ❌ **Don't release yet**
- Only 3 small changes
- No critical bugs
- No major features
- → Keep accumulating

**Action:**
```bash
# All PRs merged to main
# CHANGELOG [Unreleased] updated
# No version bump, no release
```

### Example 2: Feature Complete

**Scenario:**
- Week 1-3: Multiple PRs working on OPTIONAL MATCH
- Week 3: Final PR completes OPTIONAL MATCH feature (merged)
- [Unreleased] section has 12 changes

**Decision:** ✅ **Release now**
- Major feature complete
- Enough changes accumulated (12 items)
- Good milestone for users

**Action:**
```bash
python scripts/bump_version.py minor  # 0.1.1 → 0.2.0
# Update CHANGELOG
git commit -am "chore(release): bump version to 0.2.0"
git push origin main
git tag -a v0.2.0 -m "Release version 0.2.0"
git push origin v0.2.0
gh release create v0.2.0 --title "GraphForge v0.2.0 - OPTIONAL MATCH Support"
```

### Example 3: Critical Bug

**Scenario:**
- Friday 3pm: User reports data loss bug in production
- Friday 4pm: Bug fix a prior pull request merged

**Decision:** ✅ **Release immediately (hotfix)**
- Critical bug affecting users
- Can't wait for more changes

**Action:**
```bash
python scripts/bump_version.py patch  # 0.2.0 → 0.2.1
# Update CHANGELOG with hotfix
git commit -am "chore(release): bump version to 0.2.1 (hotfix)"
git push origin main
git tag -a v0.2.1 -m "Hotfix release 0.2.1"
git push origin v0.2.1
gh release create v0.2.1 --title "GraphForge v0.2.1 (Hotfix)"
```

### Example 4: Scheduled Release

**Scenario:**
- Last release: 3 weeks ago (0.2.0)
- [Unreleased] has 8 changes (mix of features and fixes)
- No critical issues

**Decision:** ✅ **Release on schedule**
- Scheduled release window (3-4 weeks)
- Enough changes to justify release
- Provides value to users

**Action:**
```bash
# Determine version bump based on changes
# If any features: minor, else: patch
python scripts/bump_version.py minor  # 0.2.0 → 0.3.0

# Follow normal release process
```

---

## Automation Opportunities

### GitHub Actions for Release Tracking

Create `.github/workflows/release-tracking.yaml`:

```yaml
name: Release Tracking

on:
  pull_request:
    types: [closed]

jobs:
  label-for-release:
    if: github.event.pull_request.merged == true
    runs-on: ubuntu-latest
    steps:
      - name: Add release:pending label
        if: contains(github.event.pull_request.labels.*.name, 'release:patch') ||
            contains(github.event.pull_request.labels.*.name, 'release:minor') ||
            contains(github.event.pull_request.labels.*.name, 'release:major')
        run: |
          gh pr edit ${{ github.event.pull_request.number }} \
            --add-label "release:pending"
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

### Release Reminder Script

Create `scripts/check_release_needed.py`:

```python
#!/usr/bin/env python3
"""Check if a release is needed based on unreleased changes."""

import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

def count_unreleased_changes(changelog_path: Path) -> int:
    """Count items in [Unreleased] section."""
    content = changelog_path.read_text()

    # Extract [Unreleased] section
    match = re.search(
        r'## \[Unreleased\](.*?)## \[',
        content,
        re.DOTALL
    )

    if not match:
        return 0

    unreleased = match.group(1)

    # Count bullet points
    changes = re.findall(r'^- ', unreleased, re.MULTILINE)
    return len(changes)

def main():
    changelog = Path("CHANGELOG.md")

    if not changelog.exists():
        print("❌ CHANGELOG.md not found")
        sys.exit(1)

    change_count = count_unreleased_changes(changelog)

    print(f"📦 Unreleased changes: {change_count}")

    if change_count == 0:
        print("✅ No unreleased changes. Nothing to release.")
    elif change_count < 5:
        print(f"⏳ {change_count} changes accumulated. Consider releasing when you have 5-10 changes.")
    elif change_count < 10:
        print(f"⚠️  {change_count} changes ready. Good time to consider a release!")
    else:
        print(f"🚨 {change_count} changes accumulated! Time to release.")

    sys.exit(0)

if __name__ == "__main__":
    main()
```

Usage:
```bash
# Check if release is needed
python scripts/check_release_needed.py
```

---

## Best Practices

### DO ✅

- Merge PRs frequently (continuous integration)
- Update CHANGELOG [Unreleased] with every PR
- Use release labels on PRs
- Batch fixes and features for meaningful releases
- Release every 3-4 weeks on schedule
- Hotfix critical bugs immediately
- Communicate release plans to contributors

### DON'T ❌

- Release after every single PR
- Leave main in broken state
- Forget to update CHANGELOG
- Release without testing
- Skip documentation updates
- Create releases on Fridays (avoid weekend issues)
- Batch security fixes (release immediately)

---

## Communication

### Before Release

**Announce intent in GitHub Discussions:**
```markdown
# Preparing v0.3.0 Release

Planning to release v0.3.0 this week (estimated Friday).

## What's included
- OPTIONAL MATCH support
- Variable-length paths
- 8 bug fixes

## Testing needed
- Please test branch `main` before release
- Report any critical issues

## ETA
Friday, March 1st
```

### After Release

**Announce in GitHub Discussions:**
```markdown
# GraphForge v0.3.0 Released! 🎉

We're excited to announce GraphForge v0.3.0!

## Highlights
- OPTIONAL MATCH for left outer joins
- Variable-length path queries [*1..3]
- 15% improvement in query performance

## Installation
```bash
pip install --upgrade graphforge
```

## Full Changelog
See https://github.com/CurateLabs/graphforge-legecy/releases/tag/v0.3.0
```

---

## Quick Reference

```bash
# Check what's unreleased
cat CHANGELOG.md | grep -A 20 "## \[Unreleased\]"

# Count unreleased changes
python scripts/check_release_needed.py

# See changes since last release
git log $(git describe --tags --abbrev=0)..HEAD --oneline

# List PRs not yet released
gh pr list --state merged --label "release:pending"

# When ready to release
python scripts/bump_version.py minor
# Edit CHANGELOG, commit, tag, release
```

---

**Remember:** Main branch ≠ PyPI release. It's normal for main to be ahead of PyPI. Release when it makes sense, not after every merge.
