#!/bin/sh
set -eu

: "${GF_G500_S20_EXPECTED_SHA:?exact source SHA is required}"
case "$GF_G500_S20_EXPECTED_SHA" in
  *[!0-9a-f]*|'') echo "invalid source SHA" >&2; exit 2 ;;
esac
test "${#GF_G500_S20_EXPECTED_SHA}" -eq 40
test "$(stat -c %d /work)" != "$(stat -c %d /)" || {
  echo "/work is not an attached volume" >&2
  exit 3
}

rm -rf /work/s20 /work/tmp
rm -f /work/s20-evidence.json /work/s20-journal.json /work/controller-ack
mkdir -p /work/tmp
export TMPDIR=/work/tmp

started_at=$(date +%s)
product_timeout_s=14400

# Admission and S20 share one product clock. The second timeout receives only
# the time left after admission; it never resets the four-hour envelope.
timeout --signal=TERM --kill-after=30s 600s \
  /usr/local/bin/scale-g500-ladder \
  certification_lifecycle_journals_equivalent_round_trip_and_drills \
  --exact --test-threads=1

elapsed_s=$(($(date +%s) - started_at))
remaining_s=$((product_timeout_s - elapsed_s))
if [ "$remaining_s" -le 30 ]; then
  echo "admission consumed the S20 product envelope" >&2
  exit 124
fi

export GF_G500_S20_WORK_ROOT=/work/s20
export GF_G500_S20_EVIDENCE_OUT=/work/s20-evidence.json
export GF_G500_CERT_JOURNAL_OUT=/work/s20-journal.json

set +e
timeout --signal=TERM --kill-after=30s "${remaining_s}s" \
  /usr/local/bin/scale-g500-ladder \
  s20_integrated_full_lifecycle_evidence \
  --ignored --exact --nocapture --test-threads=1
status=$?
set -e

# Preserve the Machine briefly for evidence retrieval. The controller's
# independent four-hour deadline remains authoritative even if this loop is alive.
remaining=900
while [ ! -f /work/controller-ack ] && [ "$remaining" -gt 0 ]; do
  sleep 1
  remaining=$((remaining - 1))
done
exit "$status"
