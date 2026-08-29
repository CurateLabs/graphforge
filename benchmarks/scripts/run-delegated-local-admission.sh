#!/usr/bin/env bash

set -uo pipefail

workspace=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$workspace"

if [[ ${GRAPHFORGE_SYSTEMD_SCOPE_MODE:?} == user ]]; then
  delegate=$(systemctl --user show "${GRAPHFORGE_SYSTEMD_SCOPE:?}" -p Delegate --value)
else
  delegate=$(systemctl show "${GRAPHFORGE_SYSTEMD_SCOPE:?}" -p Delegate --value)
fi
if [[ $delegate != yes ]]; then
  echo "systemd scope did not prove Delegate=yes" >&2
  exit 1
fi

python3 -m benchexec.check_cgroups
preflight_status=$?

PYTHONPATH="$workspace/harness" "$workspace/.venv/bin/reframe" \
  -C reframe/settings.py -c reframe/checks \
  -n '^LocalBenchExecAdmission$' -l | grep -F LocalBenchExecAdmission
discovery_status=$?

PYTHONPATH="$workspace/harness" "$workspace/.venv/bin/reframe" \
  -C reframe/settings.py -c reframe/checks \
  -n '^LocalBenchExecAdmission$' -r
admission_status=$?

if (( preflight_status != 0 )); then
  exit "$preflight_status"
fi
if (( discovery_status != 0 )); then
  exit "$discovery_status"
fi
exit "$admission_status"
