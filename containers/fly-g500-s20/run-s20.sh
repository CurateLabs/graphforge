#!/bin/sh
set -eu

: "${GF_G500_S20_EXPECTED_SHA:?exact source SHA is required}"
: "${GF_G500_S20_IMAGE_DIGEST:?immutable image digest is required}"
: "${GF_G500_S20_REGION:?fixed Fly region is required}"
: "${GF_G500_S20_VOLUME_GB:?volume size is required}"

test "$(stat -c %d /work)" != "$(stat -c %d /)" || {
  echo "typed_failure=work_volume_missing" >&2
  exit 3
}

rm -rf /work/s20 /work/tmp
rm -f /work/s20-evidence.json /work/s20-journal.json /work/controller-ack
mkdir -p /work/tmp
export TMPDIR=/work/tmp
export GF_G500_S20_WORK_ROOT=/work/s20
export GF_G500_S20_EVIDENCE_OUT=/work/s20-evidence.json
export GF_G500_LADDER_JOURNAL_OUT=/work/s20-journal.json

started_at="$(date +%s)"
export GF_G500_S20_MEMORY_BYTES=$((4096 * 1024 * 1024))
export GF_G500_S20_DISK_BYTES=$((GF_G500_S20_VOLUME_GB * 1024 * 1024 * 1024))

# One real S20 run owns the four-hour product clock. There is no synthetic
# lower-rung admission ladder and no compilation on the Fly Machine.
set +e
timeout --signal=TERM --kill-after=30s 14400s \
  /usr/local/bin/scale-g500-ladder \
  fly_s20_full_lifecycle_evidence \
  --ignored --exact --nocapture --test-threads=1
status=$?
set -e

elapsed=$(( $(date +%s) - started_at ))
printf '{"schema":"graphforge-fly-s20-container-result/1","exit_code":%s,"elapsed_s":%s}\n' \
  "$status" "$elapsed" > /work/container-result.json.tmp
mv /work/container-result.json.tmp /work/container-result.json

remaining=300
while [ ! -f /work/controller-ack ] && [ "$remaining" -gt 0 ]; do
  sleep 1
  remaining=$((remaining - 1))
done
exit "$status"
