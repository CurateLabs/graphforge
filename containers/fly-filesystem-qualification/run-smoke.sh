#!/bin/sh
set -eu

evidence=/work/fly-qualification-evidence.json
ack=/work/controller-ack
temporary=/work/.fly-qualification-evidence.json.tmp
if [ "$(stat -c %d /work)" = "$(stat -c %d /)" ]; then
  echo "qualification work root is not an attached mount" >&2
  exit 3
fi
rm -f "$ack" "$evidence" "$temporary"
export TMPDIR=/work

# Budget chain: 900s probe, 930s TERM backstop plus 30s KILL grace, then a
# 300s evidence-ack wait. The controller retrieval window must cover the probe
# and grace; cleanup remains the final bounded backstop.
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
