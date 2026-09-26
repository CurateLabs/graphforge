#!/usr/bin/env bash
# #1448 baseline: complete-ingest core-count curve at S18, then region
# diagnostics at S18/S20/S22 on all CPUs. One frozen production gf.
#   baseline.sh BIN_DIR OUT_DIR
set -u
BIN=$1; OUT=$2; GF=$BIN/gf; GEN=$BIN/graphforge-benchmark-graph500-generator
Q=/home/ubuntu/.claude/gf-quiet-host.sh; HERE=$(cd "$(dirname "$0")" && pwd)
exec 9>"$OUT.lock"; flock -n 9 || { echo "another baseline driver holds the lock" >&2; exit 1; }
mkdir -p "$OUT/inputs" "$OUT/tmp" "$OUT/runs"; export TMPDIR=$OUT/tmp
peer_busy() { pgrep -x gf >/dev/null || pgrep -x graphforge-benc >/dev/null || pgrep -x graphforge_stor >/dev/null || pgrep -x graphforge_api- >/dev/null || pgrep -x runexec >/dev/null; }
wait_quiet() { local streak=0; while (( streak < 12 )); do if $Q >/dev/null 2>&1 && ! peer_busy; then streak=$((streak+1)); else streak=0; fi; sleep 5; done; }
{ date -u +%FT%TZ; uname -a; nproc; sha256sum "$GF" "$GEN"; } > "$OUT/host-state.txt"
for S in 18 20 22; do
  [ -f "$OUT/inputs/s$S/edges.parquet" ] || { mkdir -p "$OUT/inputs/s$S"; "$GEN" --scale $S --edge-factor 16 --seed 13907095936298285200 --nodes "$OUT/inputs/s$S/nodes.parquet" --edges "$OUT/inputs/s$S/edges.parquet" >/dev/null; }
done
(cd "$OUT/inputs" && sha256sum s*/*.parquet) > "$OUT/inputs.sha256"
run() { # NAME SCALE CORES MEMLIMIT
  local NAME=$1 S=$2 C=$3 M=$4 D=$OUT/runs/$1 P=$OUT/tmp/project-$1
  rm -rf "$D" "$P"; mkdir -p "$D"
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
  wait_quiet
  echo "$(date -u +%FT%TZ) start $NAME load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$OUT/driver.log"
  local LIMIT=(); [ "$M" != none ] && LIMIT=(--memlimit "$M")
  runexec --no-container --cores "0-$((C-1))" "${LIMIT[@]}" --output "$D/ingest.log" -- \
    bash "$HERE/ingest-one.sh" "$GF" "$P" "$OUT/inputs/s$S" "$D" > "$D/runexec.txt" 2> "$D/runexec.stderr"
  local after=BUSY; $Q >/dev/null 2>&1 && ! peer_busy && after=QUIET
  echo "$(date -u +%FT%TZ) end $NAME after=$after returnvalue=$(grep ^returnvalue= "$D/runexec.txt" | cut -d= -f2)" >> "$OUT/driver.log"
  # Untimed: reopen and count edges.
  "$GF" --json --project "$P" query --cypher "MATCH ()-[r]->() RETURN count(r)" --output "$D/edges.arrow" < /dev/null > "$D/recount.json" 2>&1
  rm -rf "$P" "$D/edges.arrow"
}
for C in 1 2 4 8 16; do run "curve-s18-c$C" 18 "$C" 4000MB; done
run regions-s18-c16 18 16 none
run regions-s20-c16 20 16 none
run regions-s22-c16 22 16 none
echo done >> "$OUT/driver.log"
