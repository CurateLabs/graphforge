#!/usr/bin/env bash
# #1585 Graph500 A/B: main vs this branch, complete ingest at S18 and S20.
# No partition exceeds the budget at these scales, so the branch must publish
# the same bytes at no cost beyond noise. Three rounds per scale, alternating
# order (AB, BA, AB); caches dropped and a sustained quiet host before each
# timed ingest. After each ingest, untimed: the published CAS digest list and
# content queries.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-/home/ubuntu/gf-1585-ab}
MAIN=/home/ubuntu/gf-1448-bin-cca7a1fc/gf
BRANCH=${BRANCH:-/home/ubuntu/gf-1585-bin-fb8a3348/gf}   # e0168612 for the first A/B
INPUTS=/home/ubuntu/gf-1448-evidence/inputs
INGEST=$HERE/ingest-one.sh
Q=/home/ubuntu/.claude/gf-quiet-host.sh
mkdir -p "$OUT/runs" "$OUT/tmp"; export TMPDIR=$OUT/tmp
exec 9>"$OUT/driver.lock"; flock -n 9 || { echo "another A/B driver holds the lock" >&2; exit 1; }
peer_busy() { pgrep -x gf >/dev/null || pgrep -x graphforge-benc >/dev/null || pgrep -x graphforge_stor >/dev/null || pgrep -x graphforge_api- >/dev/null || pgrep -x runexec >/dev/null; }
wait_quiet() { local streak=0; while (( streak < 12 )); do if $Q >/dev/null 2>&1 && ! peer_busy; then streak=$((streak+1)); else streak=0; fi; sleep 5; done; }
{ date -u +%FT%TZ; uname -a; nproc; sha256sum "$MAIN" "$BRANCH"; (cd "$INPUTS" && sha256sum s18/*.parquet s20/*.parquet); } > "$OUT/host-state.txt"
q() { # q GF PROJECT DIR LABEL CYPHER
  "$1" --json --project "$2" query --cypher "$5" --output "$3/$4.arrow" < /dev/null > "$3/$4.json" 2> "$3/$4.stderr"
  rm -f "$3/$4.arrow"
}
run() { # NAME GF SCALE
  local NAME=$1 GF=$2 S=$3 D=$OUT/runs/$1 P=$OUT/tmp/project-$1
  rm -rf "$D" "$P"; mkdir -p "$D"
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
  wait_quiet
  echo "$(date -u +%FT%TZ) start $NAME load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$OUT/driver.log"
  runexec --no-container --output "$D/ingest.log" -- \
    bash "$INGEST" "$GF" "$P" "$INPUTS/s$S" "$D" > "$D/runexec.txt" 2> "$D/runexec.stderr"
  local after=BUSY; $Q >/dev/null 2>&1 && ! peer_busy && after=QUIET
  echo "$(date -u +%FT%TZ) end $NAME after=$after returnvalue=$(grep ^returnvalue= "$D/runexec.txt" | cut -d= -f2)" >> "$OUT/driver.log"
  (cd "$P/graph-objects/sha256" && find . -type f | sed 's|^\./||; s|/||' | sort) > "$D/cas-digests.txt"
  q "$GF" "$P" "$D" nodes "MATCH (n) RETURN count(n)"
  q "$GF" "$P" "$D" node-scan "MATCH (n) RETURN n.node_uuid AS id"
  q "$GF" "$P" "$D" edges "MATCH ()-[r]->() RETURN count(r)"
  q "$GF" "$P" "$D" edge-scan "MATCH (a)-[r]->(b) RETURN a.node_uuid AS s, b.node_uuid AS d"
  rm -rf "$P"
}
for S in 18 20; do
  run s$S-r1-main "$MAIN" $S;   run s$S-r1-branch "$BRANCH" $S
  run s$S-r2-branch "$BRANCH" $S; run s$S-r2-main "$MAIN" $S
  run s$S-r3-main "$MAIN" $S;   run s$S-r3-branch "$BRANCH" $S
done
echo done >> "$OUT/driver.log"
