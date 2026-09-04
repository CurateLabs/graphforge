#!/usr/bin/env bash
# Prepare OVHC-AGENCY (or equivalent) for local-linux-cgroups-v2 ladder admission.
# Idempotent for controller enablement; does not reboot.

set -euo pipefail

if (( EUID != 0 )); then
  echo "OVHC host setup requires root" >&2
  exit 1
fi

workspace=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
"$workspace/scripts/provision-benchexec-user-delegation.sh"

uid=${SUDO_UID:-1000}
user_service="user.slice/user-${uid}.slice/user@${uid}.service"
slice_path="/sys/fs/cgroup/${user_service}/benchexec.slice"

mkdir -p "$slice_path"
echo '+cpuset +cpu +io +memory +pids' >"/sys/fs/cgroup/${user_service}/cgroup.subtree_control"
echo '+cpuset +cpu +io +memory +pids' >"${slice_path}/cgroup.subtree_control"

echo "enabled cgroup controllers for ${user_service} and benchexec.slice"
echo "run admission as the unprivileged user inside:"
echo "  systemd-run --user --scope --slice=benchexec -p Delegate=yes -- ..."
