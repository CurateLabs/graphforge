#!/usr/bin/env bash
# Measurement script for #1585 external-partition comparison.
# Run on a quiet host: no gf/graphforge_api/graphforge_stor processes.
# Usage: ./measure.sh <gf_binary_path> <workload> <candidate>
#   workload: star-9m | star-20m | g500-s18 | g500-s20
#   candidate: datafusion | native
# Outputs per-run files to ./runs/<workload>-<candidate>-<run_num>/
# TMPDIR must be on ext4 (/home/ubuntu/gf-1585-tmp).

set -u

# Quiet-host guard: wait until no competing gf/graphforge processes.
quiet_host_guard() {
  local attempts=0
  while pgrep -x gf >/dev/null 2>&1 || \
        pgrep -x graphforge_api >/dev/null 2>&1 || \
        pgrep -x graphforge_stor >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ $((attempts % 6)) -eq 0 ]; then
      echo "  [guard] waiting for quiet host (attempt $attempts)..." >&2
    fi
    sleep 10
  done
  # Check load: wait until 1-min load < 2.0
  local load
  load=$(awk '{print $1}' /proc/loadavg)
  local load_int
  load_int=$(echo "$load" | awk -F. '{print $1}')
  if [ "$load_int" -ge 2 ]; then
    echo "  [guard] load=$load, waiting..." >&2
    attempts=0
    while [ "$(awk '{print $1}' /proc/loadavg | awk -F. '{print $1}')" -ge 2 ]; do
      sleep 10
      attempts=$((attempts + 1))
      if [ $((attempts % 6)) -eq 0 ]; then
        echo "  [guard] still waiting for load < 2.0 ($(awk '{print $1}' /proc/loadavg))..." >&2
      fi
    done
  fi
  echo "  [guard] host is quiet (load=$(awk '{print $1}' /proc/loadavg))" >&2
}

GF_BIN="${1:?Usage: $0 <gf_binary> <workload> <candidate>}"
WORKLOAD="${2:?}"
CANDIDATE="${3:?}"

EVIDENCE_DIR="$(cd "$(dirname "$0")" && pwd)"
RUNS_DIR="$EVIDENCE_DIR/runs"
mkdir -p "$RUNS_DIR"

case "$WORKLOAD" in
  star-9m)
    EDGES="/home/ubuntu/gf-hub-1509/star-9000000/edges.parquet"
    NODES="/home/ubuntu/gf-hub-1509/star-9000000/nodes.parquet"
    ;;
  star-20m)
    EDGES="/home/ubuntu/gf-1585-star-20m/edges.parquet"
    NODES="/home/ubuntu/gf-1585-star-20m/nodes.parquet"
    ;;
  g500-s18)
    EDGES="/home/ubuntu/gf-1507-evidence/workspace/s18/edges.parquet"
    NODES="/home/ubuntu/gf-1507-evidence/workspace/s18/nodes.parquet"
    ;;
  g500-s20)
    EDGES="/home/ubuntu/gf-1507-evidence/workspace/s20/edges.parquet"
    NODES="/home/ubuntu/gf-1507-evidence/workspace/s20/nodes.parquet"
    ;;
  *)
    echo "Unknown workload: $WORKLOAD" >&2; exit 1 ;;
esac

case "$CANDIDATE" in
  datafusion) SPIKE_MODE="datafusion" ;;
  native)     SPIKE_MODE="native" ;;
  *)          echo "Unknown candidate: $CANDIDATE" >&2; exit 1 ;;
esac

RUN_NUM=1
while [ -d "$RUNS_DIR/${WORKLOAD}-${CANDIDATE}-${RUN_NUM}" ]; do
  RUN_NUM=$((RUN_NUM + 1))
done
RUN_DIR="$RUNS_DIR/${WORKLOAD}-${CANDIDATE}-${RUN_NUM}"
mkdir -p "$RUN_DIR"
PROJECT="$RUN_DIR/project"
mkdir -p "$PROJECT"
SCRATCH_DIR="/home/ubuntu/gf-1585-tmp/scratch-${WORKLOAD}-${CANDIDATE}-${RUN_NUM}"
mkdir -p "$SCRATCH_DIR"

echo "=== Run ${WORKLOAD}-${CANDIDATE}-${RUN_NUM} at $(date) ===" | tee "$RUN_DIR/run.log"
echo "GF: $GF_BIN" | tee -a "$RUN_DIR/run.log"
echo "--- quiet-host guard ---" | tee -a "$RUN_DIR/run.log"
quiet_host_guard
echo "Host quiet at $(date)" | tee -a "$RUN_DIR/run.log"

export TMPDIR=/home/ubuntu/gf-1585-tmp
export GF_SHAPE_SPILL_SPIKE="$SPIKE_MODE"
export GF_SHAPE_SPILL_DIR="$SCRATCH_DIR"
export GF_SHAPE_SPILL_METRICS=1

UUID=10000000-0000-4000-8000-$(printf '%012x' $RUN_NUM)

echo "--- begin ---" | tee -a "$RUN_DIR/run.log"
"$GF_BIN" --json --project "$PROJECT" \
  import-session begin --operation-uuid "$UUID" \
  > "$RUN_DIR/begin.json" 2> "$RUN_DIR/begin.stderr" \
  && echo "begin OK" || { echo "begin FAILED"; cat "$RUN_DIR/begin.stderr"; exit 1; }

"$GF_BIN" --json --project "$PROJECT" \
  import-session register-parquet --session-uuid "$UUID" --path "$NODES" --kind nodes \
  > "$RUN_DIR/register-nodes.json" 2> "$RUN_DIR/register-nodes.stderr" \
  && echo "register-nodes OK" || { echo "register-nodes FAILED"; cat "$RUN_DIR/register-nodes.stderr"; exit 1; }

"$GF_BIN" --json --project "$PROJECT" \
  import-session register-parquet --session-uuid "$UUID" --path "$EDGES" --kind edges \
  > "$RUN_DIR/register-edges.json" 2> "$RUN_DIR/register-edges.stderr" \
  && echo "register-edges OK" || { echo "register-edges FAILED"; cat "$RUN_DIR/register-edges.stderr"; exit 1; }

echo "--- validate ---" | tee -a "$RUN_DIR/run.log"
START_NS=$(date +%s%N)
/usr/bin/time -v -o "$RUN_DIR/validate.time" \
  "$GF_BIN" --json --project "$PROJECT" \
  import-session validate --session-uuid "$UUID" \
  > "$RUN_DIR/validate.json" 2> "$RUN_DIR/validate.stderr"
VALIDATE_EXIT=$?
END_NS=$(date +%s%N)
WALL_MS=$(( (END_NS - START_NS) / 1000000 ))
echo "validate exit=$VALIDATE_EXIT wall=${WALL_MS}ms" | tee -a "$RUN_DIR/run.log"

if [ $VALIDATE_EXIT -ne 0 ]; then
  echo "VALIDATE FAILED:" | tee -a "$RUN_DIR/run.log"
  cat "$RUN_DIR/validate.stderr" | tee -a "$RUN_DIR/run.log"
  exit 1
fi

echo "--- commit ---" | tee -a "$RUN_DIR/run.log"
START_NS=$(date +%s%N)
/usr/bin/time -v -o "$RUN_DIR/commit.time" \
  "$GF_BIN" --json --project "$PROJECT" \
  import-session commit --session-uuid "$UUID" \
  > "$RUN_DIR/commit.json" 2> "$RUN_DIR/commit.stderr"
COMMIT_EXIT=$?
END_NS=$(date +%s%N)
COMMIT_WALL_MS=$(( (END_NS - START_NS) / 1000000 ))
echo "commit exit=$COMMIT_EXIT wall=${COMMIT_WALL_MS}ms" | tee -a "$RUN_DIR/run.log"

if [ $COMMIT_EXIT -ne 0 ]; then
  echo "COMMIT FAILED:" | tee -a "$RUN_DIR/run.log"
  cat "$RUN_DIR/commit.stderr" | tee -a "$RUN_DIR/run.log"
  exit 1
fi

echo "--- edge count ---" | tee -a "$RUN_DIR/run.log"
RESULT_SHA=$("$GF_BIN" --json --project "$PROJECT" \
  query --cypher "MATCH ()-[r]->() RETURN count(r)" \
  --output "$RUN_DIR/edges.arrow" </dev/null 2>"$RUN_DIR/query.stderr" \
  | python3 -c 'import json,sys;r=json.loads(sys.stdin.readline());print(r.get("scalar_u64","?"), r.get("result_sha256","no-sha"))' \
  2>/dev/null || echo "? no-sha")
echo "edges: $RESULT_SHA" | tee -a "$RUN_DIR/run.log"

RSS=$(grep "Maximum resident" "$RUN_DIR/validate.time" 2>/dev/null | awk '{print $NF}' || echo "?")
# SHAPE_SPILL metrics go to stderr; DataFusion prefix=SHAPE_SPILL, native prefix=SHAPE_SPILL_NATIVE
SPILL_LINES=$(grep "SHAPE_SPILL" "$RUN_DIR/validate.stderr" 2>/dev/null | head -5 || echo "")
echo "rss_kib=$RSS" | tee -a "$RUN_DIR/run.log"
echo "spill_metrics: $SPILL_LINES" | tee -a "$RUN_DIR/run.log"
echo "DONE: $RUN_DIR" | tee -a "$RUN_DIR/run.log"

rm -rf "$SCRATCH_DIR"
