#!/usr/bin/env bash
# #1506 complete-ingest pairs: one frozen test-support `gf`, GF_SHAPE_SORT_SPIKE
# selecting the partition sort. Graph500 inputs are generated once per scale
# with the ladder profile's seed; every observation starts from a fresh project
# after 60 s of sustained QUIET and a page-cache drop. Mode order rotates per
# round. Timed: `import-session validate` and `commit` (shaping + encode +
# publication). Untimed afterwards: recovery attribution, node/edge recount and
# the profile's one-/two-hop queries, whose outputs are hashed for comparison.
#   run-ingest-pairs.sh <out-dir> <bin-dir> <rounds> <scale>...
set -euo pipefail
OUT=${1:?out}; BIN=${2:?bin}; ROUNDS=${3:?rounds}; shift 3
SCALES=("$@")
MODES=(baseline arrow datafusion)
GF="$BIN/gf"; GEN="$BIN/graphforge-benchmark-graph500-generator"
mkdir -- "$OUT"; OUT=$(cd "$OUT" && pwd)
export TMPDIR="$OUT/tmp"; mkdir -p "$TMPDIR" "$OUT/workspace"
QUIET=/home/ubuntu/.claude/gf-quiet-host.sh

wait_quiet() { # 12 consecutive QUIET samples, 5 s apart
  local streak=0
  while (( streak < 12 )); do
    if "$QUIET" >/dev/null 2>&1; then streak=$((streak + 1)); else streak=0; fi
    sleep 5
  done
  echo "$(date -u +%FT%TZ) quiet-before $1 load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$OUT/quiet.log"
}

{
  echo "date: $(date -u +%FT%TZ)"; uname -a; nproc; free -g | head -2
  findmnt -T "$OUT" -o SOURCE,FSTYPE -n
  sha256sum "$GF" "$GEN"
  echo "modes: ${MODES[*]} rounds: $ROUNDS scales: ${SCALES[*]}"
} > "$OUT/host-state.txt" 2>&1

# Preflight: the selector must be compiled in and fail closed on a bad mode.
PF="$OUT/workspace/preflight"; mkdir -p "$PF"
"$GEN" --scale 10 --edge-factor 16 --seed 13907095936298285200 --nodes "$PF/nodes.parquet" --edges "$PF/edges.parquet" > /dev/null 2>&1
PU=00000000-0000-4000-8000-000000000010
for MODE in bogus datafusion; do
  PP="$PF/project-$MODE"
  "$GF" --json --project "$PP" import-session begin --operation-uuid "$PU" > /dev/null
  "$GF" --json --project "$PP" import-session register-parquet --session-uuid "$PU" --path "$PF/nodes.parquet" --kind nodes > /dev/null
  "$GF" --json --project "$PP" import-session register-parquet --session-uuid "$PU" --path "$PF/edges.parquet" --kind edges > /dev/null
  status=0
  { GF_SHAPE_SORT_SPIKE=$MODE "$GF" --json --project "$PP" import-session validate --session-uuid "$PU" \
    && GF_SHAPE_SORT_SPIKE=$MODE "$GF" --json --project "$PP" import-session commit --session-uuid "$PU"; } \
    > "$OUT/preflight-$MODE.out" 2>&1 || status=$?
  echo "preflight $MODE exit=$status" >> "$OUT/runs.log"
  if [ "$MODE" = bogus ]; then
    [ "$status" -ne 0 ] && grep -q "invalid shape sort experiment mode" "$OUT/preflight-$MODE.out" || { echo "PREFLIGHT FAILED: bogus mode accepted" >> "$OUT/runs.log"; exit 3; }
  else
    [ "$status" -eq 0 ] || { echo "PREFLIGHT FAILED: datafusion mode" >> "$OUT/runs.log"; exit 3; }
  fi
done
rm -rf -- "$PF"

for SCALE in "${SCALES[@]}"; do
  WS="$OUT/workspace/s$SCALE"; mkdir -p "$WS"
  "$GEN" --scale "$SCALE" --edge-factor 16 --seed 13907095936298285200 \
    --nodes "$WS/nodes.parquet" --edges "$WS/edges.parquet" > "$OUT/generate-s$SCALE.log" 2>&1
  sha256sum "$WS/nodes.parquet" "$WS/edges.parquet" >> "$OUT/inputs.sha256"
  UUID=$(printf '00000000-0000-4000-8000-%012d' "$SCALE")
  for ((round = 0; round < ROUNDS; round++)); do
    order=("${MODES[@]:round % 3}" "${MODES[@]:0:round % 3}")
    for MODE in "${order[@]}"; do
      NAME="s$SCALE-r$round-$MODE"; P="$WS/project-$NAME"; D="$OUT/$NAME"; mkdir -p "$D"
      "$GF" --json --project "$P" import-session begin --operation-uuid "$UUID" > "$D/begin.json"
      "$GF" --json --project "$P" import-session register-parquet --session-uuid "$UUID" --path "$WS/nodes.parquet" --kind nodes > "$D/register-nodes.json"
      "$GF" --json --project "$P" import-session register-parquet --session-uuid "$UUID" --path "$WS/edges.parquet" --kind edges > "$D/register-edges.json"
      sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches > /dev/null
      wait_quiet "$NAME"
      for STEP in validate commit; do
        GF_SHAPE_SORT_SPIKE=$MODE /usr/bin/time -v -o "$D/$STEP.time" \
          "$GF" --json --project "$P" import-session "$STEP" --session-uuid "$UUID" \
          > "$D/$STEP.json" 2> "$D/$STEP.stderr"
      done
      if "$QUIET" > /dev/null 2>&1; then echo quiet > "$D/after"; else echo busy > "$D/after"; "$QUIET" >> "$D/after" || true; fi
      "$GF" --json --project "$P" storage-attribution --recovery > "$D/recovery.json"
      "$GF" --json --project "$P" query \
        --cypher "MATCH (n) RETURN count(n)" --output "$D/node-count.arrow" \
        --cypher "MATCH ()-[r]->() RETURN count(r)" --output "$D/edge-count.arrow" > "$D/recount.json" < /dev/null
      "$GF" --json --project "$P" query \
        --cypher "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000" --output "$D/one-hop.arrow" \
        --cypher "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000" --output "$D/two-hop.arrow" > "$D/query.json" < /dev/null
      (cd "$D" && sha256sum node-count.arrow edge-count.arrow one-hop.arrow two-hop.arrow > outputs.sha256)
      # Published artifact identity across modes (ADR 0038 byte stability).
      (cd "$P" && find . -type f -name '*.parquet' -exec sha256sum {} + | awk '{print $1}' | sort) > "$D/published-parquet.sha256"
      du -sb "$P" > "$D/project-bytes.txt"
      rm -rf -- "$P"
      echo "$(date -u +%FT%TZ) done $NAME" >> "$OUT/runs.log"
    done
  done
done
echo "complete $(date -u +%FT%TZ)" >> "$OUT/runs.log"
