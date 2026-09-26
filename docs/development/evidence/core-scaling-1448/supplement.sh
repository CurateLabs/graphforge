#!/usr/bin/env bash
# #1448 supplementary S18 pair for a candidate.sh output directory: when a
# run there ends BUSY, the protocol excludes it, and this adds one more
# alternating pair (candidate first, continuing AB, BA, AB) so each arm keeps
# three accepted runs. Same run() as candidate.sh.
# Original candidate.sh header follows.
# #1448 candidate measurements, after the baseline (baseline.sh):
#   1. A/B: three alternating pairs at S18 and at S20 (AB, BA, AB), BenchExec
#      on CPUs 0-15 with a 4,000 MB limit (the issue's adoption gate).
#   2. S22, one run each, for the before/after region profile.
#   3. The candidate's S18 core-count curve, with three runs at 1 and 8 cores
#      (the decision-bearing points of the agreed core-use criterion).
# Every timed run: caches dropped, then 60 s of sustained quiet. Untimed after
# each run: node count and scan, edge count and scan (query answer digests).
#   supplement.sh BASE_GF CAND_GF INPUTS OUT [CURVE_RUN_NAME ...]
set -u
BASE=$1; CAND=$2; IN=$3; OUT=$4; shift 4
Q=/home/ubuntu/.claude/gf-quiet-host.sh; HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT/runs" "$OUT/tmp"; export TMPDIR=$OUT/tmp
exec 9>"$OUT.supplement.lock"; flock -n 9 || { echo "another candidate driver holds the lock" >&2; exit 1; }
peer_busy() { pgrep -x gf >/dev/null || pgrep -x graphforge-benc >/dev/null || pgrep -x graphforge_stor >/dev/null || pgrep -x graphforge_api- >/dev/null || pgrep -x graphforge_exec >/dev/null || pgrep -x runexec >/dev/null || pgrep -x cargo >/dev/null || pgrep -x rustc >/dev/null; }
wait_quiet() { local streak=0; while (( streak < 12 )); do if $Q >/dev/null 2>&1 && ! peer_busy; then streak=$((streak+1)); else streak=0; fi; sleep 5; done; }
{ date -u +%FT%TZ; uname -a; nproc; sha256sum "$BASE" "$CAND"; (cd "$IN" && sha256sum s18/*.parquet s20/*.parquet s22/*.parquet); } > "$OUT/host-state-supplement.txt"
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
run ab-s18-r4-cand "$CAND" 18 16 4000MB; run ab-s18-r4-base "$BASE" 18 16 4000MB
# Replacement runs for curve points excluded as BUSY, named after them.
for EXTRA in "$@"; do run "$EXTRA" "$CAND" 18 "$(echo "$EXTRA" | sed -E 's/^curve-s18-c([0-9]+)-.*/\1/')" 4000MB; done
echo supplement-done >> "$OUT/driver.log"
