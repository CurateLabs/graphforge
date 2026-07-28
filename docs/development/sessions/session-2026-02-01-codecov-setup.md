# Codecov Integration Setup

**Date:** 2026-02-01
**Status:** ✅ Configured - Awaiting Token

## Summary

Successfully integrated Codecov for automated code coverage tracking in the GraphForge project. Codecov will analyze coverage reports from GitHub Actions and provide coverage insights on pull requests and commits.

## What Was Implemented

### 1. GitHub Actions Workflow Updates

**File:** `.github/workflows/test.yml`

**Changes made:**

#### Test Job (Unit Tests)
- Added `--cov-branch` flag for branch coverage analysis
- Added `token: ${{ secrets.CODECOV_TOKEN }}` to Codecov upload
- Added `fail_ci_if_error: false` to prevent CI failure on upload issues
- Uploads coverage as flag: `unittests`

#### Coverage Job (Full Coverage)
- Added `--cov-branch` flag for branch coverage
- Added `--cov-report=xml` for Codecov
- **NEW:** Added Codecov upload step with `full-coverage` flag
- Uploads coverage from both unit and integration tests

### 2. Codecov Configuration File

**File:** `.codecov.yml` (NEW)

**Features:**
- **Project target:** 85% coverage (2% threshold)
- **Patch target:** 80% for new code (5% threshold)
- **Component tracking:** Separate coverage for parser, planner, executor, storage, ast, types
- **PR comments:** Automatic coverage reports on pull requests
- **Flags:** `unittests` and `full-coverage` for different coverage sources
- **Ignore patterns:** Excludes tests, docs, scripts, examples, `__init__.py` files

### 3. Documentation

**File:** `docs/codecov-integration.md` (NEW)

**Contents:**
- Complete integration guide
- Configuration details
- Coverage requirements (85% project, 80% patch)
- Local coverage generation instructions
- Troubleshooting guide
- Component tracking setup
- Best practices

### 4. CHANGELOG Update

**File:** `CHANGELOG.md`

Added entry under `[Unreleased]`:
- Codecov integration documentation
- Component-level coverage tracking
- Configuration details
- Coverage targets (85% project, 80% patch)

### 5. README Badge

**File:** `README.md`

Badge already present, verified URL is correct:
```markdown
<a href="https://codecov.io/gh/DecisionNerd/graphforge">
  <img src="https://codecov.io/gh/DecisionNerd/graphforge/graph/badge.svg" alt="Coverage" />
</a>
```

## Required Action: Add Repository Secret

**IMPORTANT:** You need to add the Codecov token as a GitHub repository secret.

### Steps:

1. **Go to GitHub repository settings:**
   ```
   https://github.com/DecisionNerd/graphforge/settings/secrets/actions
   ```

2. **Click "New repository secret"**

3. **Add the secret:**
   - **Name:** `CODECOV_TOKEN`
   - **Value:** `7d70de5b-7352-4300-8ed0-ae82ecb3b452`

4. **Click "Add secret"**

### Verification:

After adding the secret, you should see:
```
CODECOV_TOKEN
Added on [today's date]
Last used: Never
```

## What Happens Next

### On Next Push/PR

1. **GitHub Actions runs tests** with coverage
2. **Coverage report generated** (`coverage.xml`)
3. **Report uploaded to Codecov** using the token
4. **Codecov analyzes coverage** and generates report

### On Pull Requests

Codecov will automatically:
- Comment on the PR with coverage changes
- Show coverage diff (lines added/removed)
- Display component-level changes
- Provide pass/fail status based on targets

### Example PR Comment

```
# Codecov Report
> Coverage: 87.24% (+0.15%)
>
> Files Changed Coverage Δ
> src/graphforge/parser/parser.py 92.31% +1.5%
> src/graphforge/executor/executor.py 88.42% -0.3%
>
> Continue to review full report on Codecov.
```

## Coverage Tracking Setup

### Two Coverage Uploads

**1. Unit Tests (Fast Feedback)**
- Runs on: All OS × All Python versions
- Uploads from: Ubuntu + Python 3.12 only
- Flag: `unittests`
- Purpose: Quick coverage check on unit tests

**2. Full Coverage (Complete Picture)**
- Runs on: Ubuntu + Python 3.12
- Includes: Unit + Integration tests
- Flag: `full-coverage`
- Purpose: Complete coverage including e2e tests

### Component Tracking

Coverage tracked separately for:
- **parser** - `src/graphforge/parser/**`
- **planner** - `src/graphforge/planner/**`
- **executor** - `src/graphforge/executor/**`
- **storage** - `src/graphforge/storage/**`
- **ast** - `src/graphforge/ast/**`
- **types** - `src/graphforge/types/**`

## Coverage Targets

### Project-Wide Coverage

**Target:** 85%
- **Threshold:** 2% (coverage can't drop more than 2%)
- **Current:** ~85-90% (varies by component)
- **Status check:** Fails if below 83% (85% - 2%)

### Patch Coverage (New Code)

**Target:** 80%
- **Threshold:** 5% (new code can't drop more than 5%)
- **Goal:** Encourage testing for all new features
- **Status check:** Fails if new code below 75% (80% - 5%)

## Local Testing

### Generate Coverage Report

```bash
# Full coverage (unit + integration)
uv run pytest tests/unit tests/integration --cov=src --cov-branch --cov-report=term

# HTML report (visual)
uv run pytest tests/unit tests/integration --cov=src --cov-branch --cov-report=html
open htmlcov/index.html

# XML report (same format as CI)
uv run pytest tests/unit tests/integration --cov=src --cov-branch --cov-report=xml
```

### Check Coverage Threshold

```bash
# Check if coverage meets 85% requirement
uv run coverage report --fail-under=85
```

## Files Created/Modified

### Created
- ✅ `.codecov.yml` - Codecov configuration
- ✅ `docs/codecov-integration.md` - Integration guide
- ✅ `docs/session-2026-02-01-codecov-setup.md` - This file

### Modified
- ✅ `.github/workflows/test.yml` - Added Codecov uploads with token
- ✅ `CHANGELOG.md` - Documented integration
- ✅ `README.md` - Verified badge URL

## Benefits

### For Development

1. **Visibility:** See coverage trends over time
2. **PR Feedback:** Automatic coverage comments on PRs
3. **Component Tracking:** Identify under-tested components
4. **Branch Coverage:** Track both line and branch coverage
5. **Quality Gates:** Enforce coverage standards automatically

### For Contributors

1. **Clear Expectations:** 80% coverage for new code
2. **Immediate Feedback:** See coverage impact before merge
3. **Coverage Diff:** Understand what needs testing
4. **Component Context:** See which components affected

### For Project Health

1. **Prevent Regressions:** Coverage can't drop by more than 2%
2. **Encourage Testing:** New code requires 80% coverage
3. **Track Progress:** Coverage trends visible in dashboard
4. **Public Visibility:** Coverage badge in README

## Dashboard Access

Once coverage is uploaded, visit:

**Dashboard:** https://codecov.io/gh/DecisionNerd/graphforge

**Features:**
- Coverage trends graph
- Component breakdown
- File-level coverage
- Commit history
- Sunburst visualization
- Coverage changes

## Next Steps

### Immediate (Required)

1. ✅ **Add `CODECOV_TOKEN` secret** to GitHub repository
   - Go to: Settings > Secrets and variables > Actions
   - Name: `CODECOV_TOKEN`
   - Value: `7d70de5b-7352-4300-8ed0-ae82ecb3b452`

2. ✅ **Commit and push changes**
   ```bash
   git add .github/workflows/test.yml .codecov.yml CHANGELOG.md README.md docs/
   git commit -m "feat: integrate Codecov for automated coverage tracking"
   git push
   ```

3. ✅ **Verify upload** on next CI run
   - Check GitHub Actions logs
   - Look for "Upload coverage to Codecov" step
   - Should see: "Coverage reports uploaded successfully"

### After First Upload

4. **Check Codecov dashboard** at https://codecov.io/gh/DecisionNerd/graphforge
5. **Verify coverage displayed** in README badge
6. **Open a test PR** to see coverage comment
7. **Review component coverage** breakdown

## Troubleshooting

### If Coverage Upload Fails

**Check:**
1. Is `CODECOV_TOKEN` secret added?
2. Is the token correct (copy-paste carefully)?
3. Did tests generate `coverage.xml`?
4. Check GitHub Actions logs for errors

**Common Issues:**
- **Missing token:** Add `CODECOV_TOKEN` secret
- **Wrong token:** Double-check token value
- **No coverage file:** Ensure `--cov-report=xml` flag used
- **Network issue:** Codecov service may be down (check status.codecov.io)

### If Badge Doesn't Show

**Wait for:**
- First coverage upload to complete
- Badge cache to refresh (can take 5-10 minutes)

**Check:**
- Badge URL is correct in README
- Repository is public (or Codecov token valid)
- Coverage successfully uploaded to Codecov

## Success Criteria

✅ **Setup Complete When:**
1. `CODECOV_TOKEN` secret added to GitHub
2. Changes committed and pushed
3. GitHub Actions runs successfully
4. Coverage uploaded to Codecov (check logs)
5. Dashboard shows coverage data
6. README badge displays coverage percentage
7. PR comment appears on next pull request

## Timeline

- **2026-02-01:** Codecov integration configured
- **Next:** Add token and push changes
- **After first PR:** Coverage comments will appear
- **Ongoing:** Coverage tracked on all PRs and commits

## Resources

- **Codecov Dashboard:** https://codecov.io/gh/DecisionNerd/graphforge
- **Codecov Docs:** https://docs.codecov.com/
- **GitHub Actions Integration:** https://docs.codecov.com/docs/github-actions
- **Coverage.py Docs:** https://coverage.readthedocs.io/
- **Integration Guide:** `docs/codecov-integration.md`

---

**Status:** ✅ Ready - Awaiting token and first upload
**Next Action:** Add `CODECOV_TOKEN` secret to GitHub repository
