#!/usr/bin/env bash
# F1 runner. Sequential fio jobs on a quiet host, raw output kept verbatim.
#   ./run-f1.sh <rung.json> <out-dir> [data-dir]
# Refuses to start any job unless /home/ubuntu/.claude/gf-quiet-host.sh prints QUIET
# immediately before it; drops the page cache before every job.
set -euo pipefail
RUNG=${1:?rung json}; OUT=${2:?out dir}; F1_DIR=${3:-/home/ubuntu/gf-f1-fio-data}
HERE=$(cd "$(dirname "$0")" && pwd)
QUIET=/home/ubuntu/.claude/gf-quiet-host.sh
mkdir -p "$OUT" "$F1_DIR"
export F1_DIR
eval "$("$HERE/derive-params.py" "$RUNG" --env)"
"$HERE/derive-params.py" "$RUNG" > "$OUT/derived-params.txt"
export F1_BSSPLIT_WRITES=${F1_BSSPLIT#*,}
{
  echo "# host state, $(date -u +%FT%TZ)"; uname -a; fio --version
  lscpu | grep -E "Model name|^CPU\(s\)|Thread|Core|Socket|L3"
  free -h; df -h "$F1_DIR"; findmnt -T "$F1_DIR" -no SOURCE,FSTYPE,OPTIONS || true
  lsblk -o NAME,SIZE,TYPE,MOUNTPOINT,ROTA,MODEL || true; cat /proc/mdstat
  sudo -n mdadm --detail /dev/md3 || true
  for d in nvme0n1 nvme1n1 md3; do echo "$d scheduler: $(cat /sys/block/$d/queue/scheduler 2>/dev/null)"; done
  cat /sys/block/md3/queue/read_ahead_kb 2>/dev/null | sed 's/^/md3 read_ahead_kb: /'
  echo "derived params: F1_RWMIXREAD=$F1_RWMIXREAD F1_FSYNC_EVERY=$F1_FSYNC_EVERY F1_BSSPLIT=$F1_BSSPLIT"
} > "$OUT/host-state.txt" 2>&1

wait_quiet() { # $1 = run name
  local n=0
  until out=$("$QUIET" 2>&1) && [ "${out%%$'\n'*}" = QUIET ]; do
    n=$((n+1)); echo "$(date -u +%FT%TZ) $1: host busy, waiting (#$n)"; echo "$out" | head -4; sleep 30
  done
  echo "$(date -u +%FT%TZ) $1: $out  load=$(cut -d' ' -f1-3 /proc/loadavg)" | tee -a "$OUT/quiet-host.log"
}
run() { # name jobfile [extra fio args]
  local name=$1 job=$2; shift 2
  wait_quiet "$name"
  sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
  echo "$(date -u +%FT%TZ) START $name: fio $job $* (NUMJOBS=${F1_NUMJOBS:-} FILESIZE=${F1_FILESIZE:-} JOBSIZE=${F1_JOBSIZE:-})" | tee -a "$OUT/runs.log"
  fio --output-format=normal,json --output="$OUT/$name.out" "$@" "$job" || { echo "fio exit $? for $name -- aborting" | tee -a "$OUT/runs.log"; exit 1; }
  echo "$(date -u +%FT%TZ) END   $name" | tee -a "$OUT/runs.log"
}
# 1. control
run c-control-seq-direct "$HERE/1-control-seq-direct.fio"
# 2/3. pattern at 1, 4, 16 concurrent I/O threads; 160 GiB working set per level
for n in 1 4 16; do
  export F1_NUMJOBS=$n F1_JOBSIZE=$((160/n))g F1_FILESIZE=$((40/n))g
  [ "$n" = 16 ] && export F1_FILESIZE=2560m
  run p-pattern-direct-j$n   "$HERE/2-pattern-faithful.fio"
  run b-pattern-buffered-j$n "$HERE/3-buffered-fsync.fio"
  rm -f "$F1_DIR"/pat.*
done
# 4. append into fresh files, 1 and 16 threads, 16 GiB per level
for n in 1 16; do
  export F1_NUMJOBS=$n F1_JOBSIZE=$((16/n))g F1_FILESIZE=$((4096/n))m
  run a-append-fresh-j$n "$HERE/4-append-fresh-fsync.fio"
  rm -f "$F1_DIR"/app.*
done
rm -f "$F1_DIR"/ctrl.bin
echo "done: $OUT"
