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

# Small full-lifecycle admission first. The S20 product envelope starts only
# after this bounded proof succeeds.
timeout --signal=TERM --kill-after=30s 600s \
  /usr/local/bin/scale-g500-ladder \
  certification_lifecycle_journals_equivalent_round_trip_and_drills \
  --exact --test-threads=1

export GF_G500_S20_WORK_ROOT=/work/s20
export GF_G500_S20_EVIDENCE_OUT=/work/s20-evidence.json
export GF_G500_CERT_JOURNAL_OUT=/work/s20-journal.json

set +e
timeout --signal=TERM --kill-after=30s 14430s \
  /usr/local/bin/scale-g500-ladder \
  s20_integrated_full_lifecycle_evidence \
  --ignored --exact --nocapture --test-threads=1
status=$?
set -e

# Preserve the Machine briefly for evidence retrieval. The controller's
# independent 4h30 deadline remains authoritative even if this loop is alive.
remaining=900
while [ ! -f /work/controller-ack ] && [ "$remaining" -gt 0 ]; do
  sleep 1
  remaining=$((remaining - 1))
done
exit "$status"
