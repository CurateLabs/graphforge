#!/usr/bin/env bash
# perf stat: GraphForge `validate` between a compute-bound anchor (SHA-256) and a
# memory-bound anchor (GNU sort). Raw perf output kept verbatim.
#   ./run-perf.sh <out-dir> <gf-binary> <generator-binary> [scale=18]
# Needs passwordless sudo (perf_event_paranoid=4 on this host). Turns the NMI
# watchdog off for the runs (it pins one of Zen 2's six counters) and restores it.
set -euo pipefail
OUT=${1:?out dir}; GF=${2:?gf}; GEN=${3:?generator}; SCALE=${4:-18}
HERE=$(cd "$(dirname "$0")" && pwd)
source "$(dirname "$0")/../measurement-common.sh"
require_quiet_helper
[[ "$SCALE" =~ ^[1-9][0-9]*$ ]]
new_run
WS="$OUT/workspace"
export TMPDIR="$OUT/tmp"
mkdir -p "$TMPDIR" "$WS"
SORT_INPUT=${SORT_INPUT:-}
[[ -x "$GF" && -x "$GEN" ]]
if [[ -z "${SKIP_ANCHORS:-}" ]]; then [[ -f "$SORT_INPUT" ]]; fi

# Three passes per workload. Zen 2 has six programmable core counters and no fixed
# ones, so each pass is kept to <= 7 events to bound multiplexing.
PASS_A="cycles,instructions,stalled-cycles-frontend,cache-references,cache-misses,branches,branch-misses"
PASS_B="cycles,instructions,l2_latency.l2_cycles_waiting_on_fills,ls_refills_from_sys.ls_mabresp_lcl_dram,ls_refills_from_sys.ls_mabresp_lcl_cache,ls_refills_from_sys.ls_mabresp_lcl_l2,ls_dc_accesses"
PASS_C="cycles,instructions,de_dis_dispatch_token_stalls1.load_queue_token_stall,de_dis_dispatch_token_stalls1.store_queue_token_stall,de_dis_dispatch_token_stalls0.retire_token_stall,ls_l1_d_tlb_miss.all,ic_fetch_stall.ic_stall_any"
# nps1_die_to_dram is omitted: it needs the data-fabric PMU (dram_channel_data_controller_*), absent on this desktop Ryzen.
# l3_read_miss_latency needs the amd_l3 PMU (xi_sys_fill_latency) and llc_miss_rate needs LLC-loads; neither exists here.
METRICS=${METRICS:-l1d_miss_rate,dtlb_miss_rate,branch_misprediction_ratio}
# PASSES: which subject passes to run (default all). SKIP_ANCHORS=1 skips the anchors (for a second scale).
PASSES=${PASSES:-A A2 B C M}

{
  echo "# host state, $(date -u +%FT%TZ)"; uname -a; sudo -n perf --version
  lscpu | grep -E "Model name|^CPU\(s\)|Thread|Core|Socket|L1d|L2|L3"
  free -h; echo "perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid) nmi_watchdog=$(cat /proc/sys/kernel/nmi_watchdog)"
  echo "gf: $GF sha256=$(sha256sum "$GF" | cut -c1-16)"; "$GF" --version 2>&1 | head -1 || true
  echo "generator: $GEN sha256=$(sha256sum "$GEN" | cut -c1-16)"
  echo "openssl: $(openssl version)"; echo "sort: $(sort --version | head -1)"
  echo "PASS_A=$PASS_A"; echo "PASS_B=$PASS_B"; echo "PASS_C=$PASS_C"; echo "METRICS=$METRICS"
} > "$OUT/host-state.txt" 2>&1

pstat() { # name pass events-or-metrics -- cmd...
  local name=$1 pass=$2 spec=$3; shift 3
  wait_quiet "$name-$pass"
  local flag=-e; [ "$pass" = M ] && flag="-a -M"
  echo "$(date -u +%FT%TZ) $name-$pass: sudo perf stat $flag $spec -- $*" | tee -a "$OUT/runs.log"
  # shellcheck disable=SC2086
  # perf runs as root (perf_event_paranoid=4); the measured command is dropped back to $USER
  # with an explicit environment so its files are user-owned and TMPDIR is the ext4 path.
  sudo -n perf stat $flag "$spec" -o "$OUT/$name.pass$pass.txt" -- sudo -n -u "$USER" env HOME="$HOME" PATH="$PATH" TMPDIR="$TMPDIR" "$@" > "$OUT/$name.pass$pass.stdout" 2>&1 || { local status=$?; echo "exit $status for $name-$pass" >> "$OUT/runs.log"; return "$status"; }
}
all_passes() { local name=$1; shift; for p in $PASSES; do spec=PASS_${p%2}; [ "$p" = M ] && spec=METRICS; pstat "$name" "$p" "${!spec}" "$@"; done; }

WATCHDOG_ORIGINAL=$(sudo -n sysctl -n kernel.nmi_watchdog)
[[ "$WATCHDOG_ORIGINAL" =~ ^[01]$ ]]
trap 'sudo -n sysctl -q "kernel.nmi_watchdog=$WATCHDOG_ORIGINAL"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
sudo -n sysctl -q kernel.nmi_watchdog=0

if [ -z "${SKIP_ANCHORS:-}" ]; then
# Anchor A: compute-bound. Same command as /home/ubuntu/gf-redteam-scratch/f2-probe.sh, N=16.
all_passes anchorA-sha256 openssl speed -seconds 3 -bytes 65536 -multi 16 sha256
# Anchor B: memory-bound. Same command as f2-probe-b.sh: 16 concurrent single-threaded GNU sorts.
all_passes anchorB-sort bash -c 'pids=(); for ((i=0;i<16;i++)); do sort -n -S 1G --parallel=1 -o /dev/null "$1" & pids+=("$!"); done; status=0; for pid in "${pids[@]}"; do wait "$pid" || status=1; done; exit "$status"' _ "$SORT_INPUT"
fi

# Subject: gf import-session validate at S$SCALE, exactly the ladder profile's commands.
UUID=$(printf '00000000-0000-4000-8000-%012d' "$SCALE")
mkdir -p "$WS/s$SCALE"
"$GEN" --scale "$SCALE" --edge-factor 16 --seed 13907095936298285200 --nodes "$WS/s$SCALE/nodes.parquet" --edges "$WS/s$SCALE/edges.parquet"
ls -l "$WS/s$SCALE" >> "$OUT/host-state.txt"
prep() { # fresh project, session begun, parquet registered — not measured
  SOURCE="$WS/s$SCALE/source-$1"
  "$GF" --json --project "$SOURCE" import-session begin --operation-uuid "$UUID" > "$OUT/subject-prep.$1.json"
  "$GF" --json --project "$SOURCE" import-session register-parquet --session-uuid "$UUID" --path "$WS/s$SCALE/nodes.parquet" --kind nodes >> "$OUT/subject-prep.$1.json"
  "$GF" --json --project "$SOURCE" import-session register-parquet --session-uuid "$UUID" --path "$WS/s$SCALE/edges.parquet" --kind edges >> "$OUT/subject-prep.$1.json"
}
for pass in $PASSES; do
  prep "$pass"; sync; echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null
  spec=PASS_${pass%2}; [ "$pass" = M ] && spec=METRICS
  pstat "subject-validate-s$SCALE" "$pass" "${!spec}" "$GF" --json --project "$SOURCE" import-session validate --session-uuid "$UUID"
  validate_receipts "$OUT/subject-validate-s$SCALE.pass$pass.stdout"
  cp "$OUT/subject-validate-s$SCALE.pass$pass.stdout" "$OUT/subject-validate-s$SCALE.pass$pass.receipt.json"
done
echo "done: $OUT"
