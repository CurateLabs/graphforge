#!/usr/bin/env bash
# Concurrency probe for the real code: N independent `gf import-session validate` processes
# on N copies of the same S<scale> input, run concurrently; scaling(N) = N * T(1) / T(N).
# Same method as the F2 probes (f2-probe-b.sh) but on GraphForge's own instruction mix.
#   ./run-scaling.sh <out-dir> <gf> <generator> [scale=18] [N list="1 2 4 8 16"]
set -euo pipefail
OUT=${1:?}; GF=${2:?}; GEN=${3:?}; SCALE=${4:-18}; NS=${5:-"1 2 4 8 16"}
source "$(dirname "$0")/../measurement-common.sh"
require_quiet_helper
[[ "$SCALE" =~ ^[1-9][0-9]*$ ]]
new_run
WS="$OUT/workspace"
[[ -x "$GF" && -x "$GEN" ]]
export TMPDIR="$OUT/tmp"
mkdir -p "$TMPDIR" "$WS"
UUID=$(printf '00000000-0000-4000-8000-%012d' "$SCALE")
LOG="$OUT/scaling-s$SCALE.log"
if [ ! -f "$WS/s$SCALE/edges.parquet" ]; then
  mkdir -p "$WS/s$SCALE"
  "$GEN" --scale "$SCALE" --edge-factor 16 --seed 13907095936298285200 --nodes "$WS/s$SCALE/nodes.parquet" --edges "$WS/s$SCALE/edges.parquet"
fi
prep() { # $1 = index; a fresh project with the session begun and parquet registered (not timed)
  local p="$WS/s$SCALE/N$n-proj$1"
  "$GF" --json --project "$p" import-session begin --operation-uuid "$UUID" >/dev/null
  "$GF" --json --project "$p" import-session register-parquet --session-uuid "$UUID" --path "$WS/s$SCALE/nodes.parquet" --kind nodes >/dev/null
  "$GF" --json --project "$p" import-session register-parquet --session-uuid "$UUID" --path "$WS/s$SCALE/edges.parquet" --kind edges >/dev/null
}
echo "# scaling probe S$SCALE, $(date -u +%FT%TZ), gf=$(sha256sum "$GF" | cut -c1-16)" | tee "$LOG"
for n in $NS; do
  [[ "$n" =~ ^[1-9][0-9]*$ ]]
  for i in $(seq 1 "$n"); do prep "$i"; done
  wait_quiet
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
  echo "$(date -u +%FT%TZ) N=$n: QUIET load=$(cut -d' ' -f1-3 /proc/loadavg)" | tee -a "$LOG"
  pids=(); receipts=()
  t0=$(date +%s.%N)
  for i in $(seq 1 "$n"); do
    "$GF" --json --project "$WS/s$SCALE/N$n-proj$i" import-session validate --session-uuid "$UUID" > "$OUT/scaling-s$SCALE-N$n-p$i.json" 2>&1 &
    pids+=("$!")
    receipts+=("$OUT/scaling-s$SCALE-N$n-p$i.json")
  done
  failed=0
  for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
  [[ "$failed" = 0 ]] || { echo "validation child failed" >&2; exit 1; }
  t1=$(date +%s.%N)
  validate_receipts "${receipts[@]}"
  wall=$(python3 -c "print(round($t1-$t0,2))")
  echo "N=$n wall=$wall validated=$n/$n" | tee -a "$LOG"
done
echo "done: $LOG"
