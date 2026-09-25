#!/usr/bin/env bash
# Run all measurements for #1585 comparison: all workloads × candidates × N reps.
# Usage: ./run_all.sh <gf_binary>
# TMPDIR must be on ext4. Runs are sequential; measure.sh checks quiet-host before each.
set -euo pipefail

GF_BIN="${1:?Usage: $0 <gf_binary>}"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== #1585 comparison measurements ==="
echo "Binary: $GF_BIN"
echo "Start: $(date)"
echo ""

# n=3 per cell for star; n=3 per cell for g500 (rotated)
N_STAR=3
N_G500=3

for WORKLOAD in star-9m star-20m g500-s18 g500-s20; do
  for CANDIDATE in datafusion native; do
    N=$N_STAR
    [[ "$WORKLOAD" == g500* ]] && N=$N_G500
    for i in $(seq 1 $N); do
      echo "--- ${WORKLOAD} × ${CANDIDATE} run ${i}/${N} ---"
      "$DIR/measure.sh" "$GF_BIN" "$WORKLOAD" "$CANDIDATE"
    done
  done
done

echo ""
echo "All runs complete at $(date)"
echo "Results: $DIR/runs/"
