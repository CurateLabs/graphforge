#!/usr/bin/env bash

set -euo pipefail

if (( EUID != 0 )); then
  echo "provisioning BenchExec user delegation requires root" >&2
  exit 1
fi

workspace=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source_file="$workspace/systemd/user@.service.d/benchexec.conf"
destination=/etc/systemd/system/user@.service.d/benchexec.conf

install -D -m 0644 "$source_file" "$destination"
systemctl daemon-reload

echo "installed $destination; reboot or recreate the target user manager before admission"
