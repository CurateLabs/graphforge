#!/usr/bin/env bash
set -u
E=/home/ubuntu/gf-1506-evidence; B=$E/bin-6bf02962
$E/run-ingest-pairs.sh $E/ingest-pairs $B 3 18 20 > $E/ingest-pairs.driver.log 2>&1
echo "ingest exit $?" >> $E/window.log
# Kernel bench after sustained quiet.
streak=0; while (( streak < 12 )); do if /home/ubuntu/.claude/gf-quiet-host.sh >/dev/null 2>&1; then streak=$((streak+1)); else streak=0; fi; sleep 5; done
mkdir -p $E/kernel-scratch
TMPDIR=$E/kernel-scratch /usr/bin/time -v -o $E/kernel.time $B/sort_partition_spike $E/kernel-scratch 5 > $E/kernel.json 2> $E/kernel.stderr
echo "kernel exit $? $(date -u +%FT%TZ)" >> $E/window.log
