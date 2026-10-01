#!/usr/bin/env bash
# #1688 paired A/B driver for the #1619 read-path candidates.
#
# Protocol: docs/development/cypher-read-path-inventory.md §6. One `gf query`
# per timed run under `runexec`, page cache dropped and 60 s of sustained quiet
# before each run. Pairs alternate AB, BA, AB, ... A is candidate `current` in
# the experiment build; each comparison candidate gets its own pair series.
# The build control compares A in the default build with A in the experiment
# build. A cap hit (wall time, memory) is recorded as a refusal, never retried
# with a larger cap; that candidate/query is then skipped at the same and larger
# scales. Each receipt carries `result_sha256` and the `operator_rss` labels
# that summarize.py checks against the candidate's expected plan.
#
# Required environment:
#   OUT       output root (ext4; created)
#   GF_EXP    gf built with --features read-path-experiment
#   GF_MAIN   gf default build of the same base revision (build control)
#   PROJECTS  directory holding s18/ s19/ s20/ projects
# Optional: SCALES ("18 19 20"), QUERIES (all four), FIRST_PAIR (1), PAIRS (3:
#   the last pair index), COMPARE ("stock structural"), CONTROL_SCALE (18; empty
#   skips the control), QUIET_HELPER, WALL_CAP_S (1800), MEM_CAP_BYTES (64 GiB).
# A same-sign series is extended to six pairs with FIRST_PAIR=4 PAIRS=6.
set -u
: "${OUT:?}" "${GF_EXP:?}" "${GF_MAIN:?}" "${PROJECTS:?}"
SCALES=${SCALES:-"18 19 20"}
FIRST_PAIR=${FIRST_PAIR:-1}
PAIRS=${PAIRS:-3}
COMPARE=${COMPARE:-"stock structural"}
CONTROL_SCALE=${CONTROL_SCALE:-18}
QUIET_HELPER=${QUIET_HELPER:-/home/ubuntu/.claude/gf-quiet-host.sh}
WALL_CAP_S=${WALL_CAP_S:-1800}
MEM_CAP_BYTES=${MEM_CAP_BYTES:-68719476736}

declare -A CYPHER=(
  [recount-nodes]="MATCH (n) RETURN count(n)"
  [recount-edges]="MATCH ()-[r]->() RETURN count(r)"
  [one-hop]="MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000"
  [two-hop]="MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000"
)
QUERIES=${QUERIES:-"recount-nodes recount-edges one-hop two-hop"}

mkdir -p "$OUT/runs" "$OUT/tmp" "$OUT/refused"
export TMPDIR=$OUT/tmp
exec 9>"$OUT/driver.lock"
flock -n 9 || { echo "another read-path driver holds $OUT/driver.lock" >&2; exit 1; }

# Peers' binaries the quiet helper cannot see (15-character comm names).
peer_busy() {
  pgrep -x gf >/dev/null || pgrep -x gf.real >/dev/null || pgrep -x runexec >/dev/null ||
    pgrep -x graphforge-benc >/dev/null || pgrep -x graphforge_stor >/dev/null ||
    pgrep -x graphforge_api- >/dev/null || pgrep -x cargo >/dev/null || pgrep -x rustc >/dev/null
}
quiet_now() { "$QUIET_HELPER" >/dev/null 2>&1 && ! peer_busy; }
wait_quiet() { local streak=0; while (( streak < 12 )); do if quiet_now; then streak=$((streak + 1)); else streak=0; fi; sleep 5; done; }
tree_digest() { (cd "$1" && find . -type f -printf '%P %s %T@\n' | sort | sha256sum | cut -d' ' -f1); }

{
  date -u +%FT%TZ; uname -a; nproc; free -b | head -2
  sha256sum "$GF_EXP" "$GF_MAIN"
  for s in $SCALES; do echo "s$s $(tree_digest "$PROJECTS/s$s")"; done
  echo "SCALES=$SCALES QUERIES=$QUERIES FIRST_PAIR=$FIRST_PAIR PAIRS=$PAIRS COMPARE=$COMPARE CONTROL_SCALE=$CONTROL_SCALE WALL_CAP_S=$WALL_CAP_S MEM_CAP_BYTES=$MEM_CAP_BYTES"
} > "$OUT/host-state.txt"

refused() { [[ -e "$OUT/refused/$1-$2" ]]; }   # candidate query

# run NAME BIN CANDIDATE SCALE QUERY
run() {
  local name=$1 bin=$2 candidate=$3 scale=$4 query=$5
  local dir=$OUT/runs/$name project=$PROJECTS/s$scale
  rm -rf "$dir"; mkdir -p "$dir"
  local before; before=$(tree_digest "$project")
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
  wait_quiet
  echo "$(date -u +%FT%TZ) start $name load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$OUT/driver.log"
  runexec --no-container --walltimelimit "$WALL_CAP_S" --memlimit "$MEM_CAP_BYTES" \
    --output "$dir/gf.log" -- env GF_READ_PATH_CANDIDATE="$candidate" \
    "$bin" --json --project "$project" query --format arrow-ipc \
    --cypher "${CYPHER[$query]}" --output "$dir/out.arrow" \
    < /dev/null > "$dir/runexec.txt" 2> "$dir/runexec.stderr"
  local after=BUSY; quiet_now && after=QUIET
  rm -f "$dir/out.arrow"
  local changed=no; [[ "$(tree_digest "$project")" == "$before" ]] || changed=yes
  local rv; rv=$(grep '^returnvalue=' "$dir/runexec.txt" | cut -d= -f2)
  local term; term=$(grep '^terminationreason=' "$dir/runexec.txt" | cut -d= -f2)
  printf '%s\n' "name=$name" "binary=$bin" "candidate=$candidate" "scale=$scale" \
    "query=$query" "after=$after" "project_changed=$changed" > "$dir/meta.txt"
  echo "$(date -u +%FT%TZ) end $name after=$after rv=${rv:-none} term=${term:-none} project_changed=$changed" >> "$OUT/driver.log"
  if [[ -n "$term" || "${rv:-1}" != 0 ]] && [[ "$candidate" != current ]]; then
    : > "$OUT/refused/$candidate-$query"
  fi
}

# pair SERIES I SCALE QUERY A_BIN A_LABEL X_BIN X_CANDIDATE X_LABEL
pair() {
  local series=$1 i=$2 scale=$3 query=$4 abin=$5 alabel=$6 xbin=$7 xcand=$8 xlabel=$9
  local stem="s$scale-$query-$series-p$i"
  if (( i % 2 == 1 )); then
    run "$stem-$alabel" "$abin" current "$scale" "$query"
    run "$stem-$xlabel" "$xbin" "$xcand" "$scale" "$query"
  else
    run "$stem-$xlabel" "$xbin" "$xcand" "$scale" "$query"
    run "$stem-$alabel" "$abin" current "$scale" "$query"
  fi
}

for scale in $SCALES; do
  for query in $QUERIES; do
    for candidate in $COMPARE; do
      for (( i = FIRST_PAIR; i <= PAIRS; i++ )); do
        if refused "$candidate" "$query"; then
          echo "$(date -u +%FT%TZ) skip s$scale-$query-$candidate-p$i refused" >> "$OUT/driver.log"
          break
        fi
        pair "$candidate" "$i" "$scale" "$query" "$GF_EXP" A "$GF_EXP" "$candidate" "$candidate"
      done
    done
    if [[ "$scale" == "$CONTROL_SCALE" ]]; then
      for (( i = FIRST_PAIR; i <= PAIRS; i++ )); do
        pair control "$i" "$scale" "$query" "$GF_MAIN" A0 "$GF_EXP" current A
      done
    fi
  done
done
echo "$(date -u +%FT%TZ) done" >> "$OUT/driver.log"
