#!/usr/bin/env bash
# Concurrency probe for the real code: N independent `gf import-session validate` processes
# on N copies of the same S<scale> input, run concurrently; scaling(N) = N * T(1) / T(N).
# Same method as the F2 probes (f2-probe-b.sh) but on GraphForge's own instruction mix.
#   ./run-scaling.sh <out-dir> <gf> <generator> [scale=18] [N list="1 2 4 8 16"]
set -euo pipefail
OUT=${1:?}; GF=${2:?}; GEN=${3:?}; SCALE=${4:-18}; NS=${5:-"1 2 4 8 16"}
QUIET=/home/ubuntu/.claude/gf-quiet-host.sh
WS=${SCALING_WS:-/home/ubuntu/gf-scaling-ws}
export TMPDIR=${TMPDIR:-/home/ubuntu/gf-tmp-measure}; mkdir -p "$TMPDIR" "$OUT" "$WS"
UUID=$(printf '00000000-0000-4000-8000-%012d' "$SCALE")
LOG="$OUT/scaling-s$SCALE.log"
if [ ! -f "$WS/s$SCALE/edges.parquet" ]; then
  mkdir -p "$WS/s$SCALE"
  "$GEN" --scale "$SCALE" --edge-factor 16 --seed 13907095936298285200 --nodes "$WS/s$SCALE/nodes.parquet" --edges "$WS/s$SCALE/edges.parquet"
fi
prep() { # $1 = index; a fresh project with the session begun and parquet registered (not timed)
  local p="$WS/s$SCALE/proj$1"; chmod -R u+w "$p" 2>/dev/null || true; rm -rf "$p"
  "$GF" --json --project "$p" import-session begin --operation-uuid "$UUID" >/dev/null
  "$GF" --json --project "$p" import-session register-parquet --session-uuid "$UUID" --path "$WS/s$SCALE/nodes.parquet" --kind nodes >/dev/null
  "$GF" --json --project "$p" import-session register-parquet --session-uuid "$UUID" --path "$WS/s$SCALE/edges.parquet" --kind edges >/dev/null
}
echo "# scaling probe S$SCALE, $(date -u +%FT%TZ), gf=$(sha256sum "$GF" | cut -c1-16)" | tee "$LOG"
for n in $NS; do
  for i in $(seq 1 "$n"); do prep "$i"; done
  until out=$("$QUIET" 2>&1) && [ "${out%%$'\n'*}" = QUIET ]; do echo "busy, waiting"; sleep 30; done
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
  echo "$(date -u +%FT%TZ) N=$n: $out load=$(cut -d' ' -f1-3 /proc/loadavg)" | tee -a "$LOG"
  t0=$(date +%s.%N)
  for i in $(seq 1 "$n"); do
    "$GF" --json --project "$WS/s$SCALE/proj$i" import-session validate --session-uuid "$UUID" > "$OUT/scaling-s$SCALE-N$n-p$i.json" 2>&1 &
  done
  wait
  t1=$(date +%s.%N)
  wall=$(python3 -c "print(round($t1-$t0,2))")
  ok=$(grep -l '"outcome":"validated"' "$OUT"/scaling-s$SCALE-N$n-p*.json | wc -l)
  cpu=$(python3 -c "
import json,glob;t=0.0
for f in glob.glob('$OUT/scaling-s$SCALE-N$n-p*.json'):
    r=json.load(open(f)); t+=sum(v['cpu_ns'] for v in r['operation_timings'].values())/1e9
print(round(t,2))")
  echo "N=$n wall=$wall validated=$ok/$n receipt_cpu_s_total=$cpu" | tee -a "$LOG"
done
echo "done: $LOG"
