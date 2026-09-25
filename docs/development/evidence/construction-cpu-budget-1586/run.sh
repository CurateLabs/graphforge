#!/usr/bin/env bash
# #1586 measurement driver: sustained quiet, pinned CPUs, one run per compute size.
set -u
# Exactly one driver at a time.
exec 9>/home/ubuntu/gf-1586-evidence/driver.lock
flock -n 9 || { echo "another driver holds the lock" >&2; exit 1; }
E=/home/ubuntu/gf-1586-evidence; Q=/home/ubuntu/.claude/gf-quiet-host.sh
# The guard misses plain gf, the Graph500 generator and test binaries; the
# #1585 agent measures on this host too, so wait on those names as well.
peer_busy() { pgrep -x gf >/dev/null || pgrep -x graphforge-benc >/dev/null || pgrep -x graphforge_stor >/dev/null || pgrep -x graphforge_api- >/dev/null; }
wait_quiet() { local streak=0; while (( streak < 12 )); do if $Q >/dev/null 2>&1 && ! peer_busy; then streak=$((streak+1)); else streak=0; fi; sleep 5; done; }
for T in 4 8; do
  wait_quiet
  echo "$(date -u +%FT%TZ) start T=$T load=$(cut -d' ' -f1-3 /proc/loadavg)" >> $E/driver.log
  CPUS="0-$((T-1))"
  TMPDIR=$E/tmp GF_1586_COMPUTE=$T taskset -c $CPUS $E/graphforge_api-test --exact \
    import_session::cpu_budget_report::construction_cpu_reserve_report --ignored --nocapture --test-threads=1 \
    > $E/report-t$T.log 2>&1
  echo "$(date -u +%FT%TZ) end T=$T exit=$? after=$($Q | head -1)" >> $E/driver.log
done
echo done >> $E/driver.log
