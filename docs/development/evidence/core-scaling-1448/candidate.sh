#!/usr/bin/env bash
# #1448 candidate measurements, after the baseline (baseline.sh):
#   1. A/B: three alternating pairs at S18 and at S20 (AB, BA, AB), BenchExec
#      on CPUs 0-15 with a 4,000 MB limit (the issue's adoption gate).
#   2. S22, one run each, for the before/after region profile.
#   3. The candidate's S18 core-count curve, with three runs at 1 and 8 cores
#      (the decision-bearing points of the agreed core-use criterion).
# Every timed run: caches dropped, then 60 s of sustained quiet. Untimed after
# each run: node count and scan, edge count and scan (query answer digests).
#   candidate.sh BASE_GF CAND_GF INPUTS OUT
set -u
BASE=$1; CAND=$2; IN=$3; OUT=$4
Q=/home/ubuntu/.claude/gf-quiet-host.sh; HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT/runs" "$OUT/tmp"; export TMPDIR=$OUT/tmp
exec 9>"$OUT.lock"; flock -n 9 || { echo "another candidate driver holds the lock" >&2; exit 1; }
peer_busy() { pgrep -x gf >/dev/null || pgrep -x graphforge-benc >/dev/null || pgrep -x graphforge_stor >/dev/null || pgrep -x graphforge_api- >/dev/null || pgrep -x graphforge_exec >/dev/null || pgrep -x runexec >/dev/null || pgrep -x cargo >/dev/null || pgrep -x rustc >/dev/null; }
wait_quiet() { local streak=0; while (( streak < 12 )); do if $Q >/dev/null 2>&1 && ! peer_busy; then streak=$((streak+1)); else streak=0; fi; sleep 5; done; }
{ date -u +%FT%TZ; uname -a; nproc; sha256sum "$BASE" "$CAND"; (cd "$IN" && sha256sum s18/*.parquet s20/*.parquet s22/*.parquet); } > "$OUT/host-state.txt"
q() { "$1" --json --project "$2" query --cypher "$4" --output "$3.arrow" < /dev/null > "$3.json" 2> "$3.stderr"; rm -f "$3.arrow"; }
run() { # NAME GF SCALE CORES MEMLIMIT
  local NAME=$1 GF=$2 S=$3 C=$4 M=$5 D=$OUT/runs/$1 P=$OUT/tmp/project-$1
  rm -rf "$D" "$P"; mkdir -p "$D"
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
  wait_quiet
  echo "$(date -u +%FT%TZ) start $NAME load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$OUT/driver.log"
  local LIMIT=(); [ "$M" != none ] && LIMIT=(--memlimit "$M")
  runexec --no-container --cores "0-$((C-1))" "${LIMIT[@]}" --output "$D/ingest.log" -- \
    bash "$HERE/ingest-one.sh" "$GF" "$P" "$IN/s$S" "$D" > "$D/runexec.txt" 2> "$D/runexec.stderr"
  local after=BUSY; $Q >/dev/null 2>&1 && ! peer_busy && after=QUIET
  echo "$(date -u +%FT%TZ) end $NAME after=$after returnvalue=$(grep ^returnvalue= "$D/runexec.txt" | cut -d= -f2)" >> "$OUT/driver.log"
  q "$GF" "$P" "$D/nodes" "MATCH (n) RETURN count(n)"
  q "$GF" "$P" "$D/node-scan" "MATCH (n) RETURN n.node_uuid AS id"
  q "$GF" "$P" "$D/edges" "MATCH ()-[r]->() RETURN count(r)"
  q "$GF" "$P" "$D/edge-scan" "MATCH (a)-[r]->(b) RETURN a.node_uuid AS s, b.node_uuid AS d"
  rm -rf "$P"
}
for S in 18 20; do
  run ab-s$S-r1-base "$BASE" $S 16 4000MB; run ab-s$S-r1-cand "$CAND" $S 16 4000MB
  run ab-s$S-r2-cand "$CAND" $S 16 4000MB; run ab-s$S-r2-base "$BASE" $S 16 4000MB
  run ab-s$S-r3-base "$BASE" $S 16 4000MB; run ab-s$S-r3-cand "$CAND" $S 16 4000MB
done
run regions-s22-base "$BASE" 22 16 none; run regions-s22-cand "$CAND" 22 16 none
for C in 1 2 4 8 16; do
  REPS=1; [ $C = 1 ] || [ $C = 8 ] && REPS=3
  for R in $(seq 1 $REPS); do run curve-s18-c$C-r$R "$CAND" 18 $C 4000MB; done
done
echo done >> "$OUT/driver.log"
