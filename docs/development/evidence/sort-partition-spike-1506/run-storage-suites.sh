#!/usr/bin/env bash
# Storage lib suite under every GF_SHAPE_SORT_SPIKE mode, from one frozen build.
set -u
cd /home/ubuntu/gf-1506-eval
export TMPDIR=/home/ubuntu/gf-1506-tmp CARGO_TARGET_DIR=/home/ubuntu/gf-1506-target CARGO_BUILD_JOBS=8
echo "head $(git rev-parse HEAD) dirty=$(git status --porcelain | wc -l)"
cargo test -p graphforge-storage --lib --no-run 2>&1 | tail -2
for mode in baseline arrow-fixed arrow datafusion; do
  echo "== $mode start $(date -u +%FT%TZ)"
  GF_SHAPE_SORT_SPIKE=$mode cargo test -p graphforge-storage --lib 2>&1 | grep -E "^test result: .* [0-9]{3,} passed|FAILED|panicked" | tail -5
  echo "== $mode end $(date -u +%FT%TZ)"
done
