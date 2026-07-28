#!/usr/bin/env bash
set -euo pipefail

gate=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/require-gates.sh

"$gate" success success success skipped

if "$gate" failure success skipped 2>/dev/null; then
  echo "failed classifier must fail the aggregate gate" >&2
  exit 1
fi

if "$gate" success skipped skipped 2>/dev/null; then
  echo "skipped policy must fail the aggregate gate" >&2
  exit 1
fi

if "$gate" success success cancelled 2>/dev/null; then
  echo "cancelled applicable job must fail the aggregate gate" >&2
  exit 1
fi

echo "aggregate CI gate tests passed"
