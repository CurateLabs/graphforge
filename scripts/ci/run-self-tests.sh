#!/usr/bin/env bash
# Run every scripts/ci/test-* suite. Suites written for pytest import it;
# the rest are plain scripts that exit non-zero on failure.
set -euo pipefail
cd "$(dirname "$0")/../.."

for suite in scripts/ci/test-*.py scripts/development/test-*.py; do
  echo "--- $suite"
  if grep -q '^import pytest' "$suite"; then
    uv run --no-sync pytest -q -p no:cacheprovider "$suite"
  else
    uv run --no-sync python "$suite"
  fi
done
