#!/bin/sh
set -u

evidence=/work/fly-qualification-evidence.json
ack=/work/controller-ack
rm -f "$ack" "$evidence"
export TMPDIR=/work

set +e
timeout --signal=TERM --kill-after=30s 930s \
  /usr/local/bin/graphforge-fly-filesystem-smoke \
  --work-root /work --evidence-out "$evidence" --timeout-s 900
status=$?
set -e

# Keep the auto-destroy Machine available only long enough for retrieval.
remaining=300
while [ ! -f "$ack" ] && [ "$remaining" -gt 0 ]; do
  sleep 1
  remaining=$((remaining - 1))
done
exit "$status"
