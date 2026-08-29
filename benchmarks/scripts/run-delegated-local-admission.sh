#!/usr/bin/env bash

set -uo pipefail

workspace=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$workspace"

if [[ $(id -u) != "${GRAPHFORGE_EXPECTED_UID:?}" ]]; then
  echo "delegated unit did not preserve the runner UID" >&2
  exit 1
fi

if [[ ${GRAPHFORGE_SYSTEMD_SCOPE_MODE:?} == user ]]; then
  delegate=$(systemctl --user show "${GRAPHFORGE_SYSTEMD_SCOPE:?}" -p Delegate --value)
  if [[ $delegate != yes ]]; then
    echo "user systemd scope did not prove Delegate=yes" >&2
    exit 1
  fi
else
  delegate=$(systemctl show "${GRAPHFORGE_SYSTEMD_SCOPE:?}" -p Delegate --value)
  if [[ $delegate != yes ]]; then
    echo "system systemd service did not prove Delegate=yes" >&2
    exit 1
  fi
  delegate_controllers=$(
    systemctl show "$GRAPHFORGE_SYSTEMD_SCOPE" -p DelegateControllers --value
  )
  delegate_subgroup=$(
    systemctl show "$GRAPHFORGE_SYSTEMD_SCOPE" -p DelegateSubgroup --value
  )
  if [[ $delegate_subgroup != init.scope ]]; then
    echo "system systemd service did not isolate its initial process" >&2
    exit 1
  fi
  for controller in cpu cpuset io memory; do
    if [[ " $delegate_controllers " != *" $controller "* ]]; then
      echo "system systemd service did not delegate $controller" >&2
      exit 1
    fi
  done
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
