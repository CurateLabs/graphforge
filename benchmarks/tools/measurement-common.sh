#!/usr/bin/env bash
# Sourced by operator-invoked measurement runners; no measurements on source.
require_quiet_helper() {
  : "${QUIET_HELPER:?set QUIET_HELPER to an executable printing QUIET or BUSY}"
  [[ -x "$QUIET_HELPER" ]] || { echo "quiet helper is not executable" >&2; return 1; }
}
wait_quiet() {
  local report
  while true; do
    report=$("$QUIET_HELPER") || { echo "quiet helper failed" >&2; return 1; }
    case "${report%%$'\n'*}" in
      QUIET) printf '%s\n' "$report" >> "$OUT/quiet-host.log"; return 0 ;;
      BUSY) sleep 30 ;;
      *) echo "invalid quiet helper response: $report" >&2; return 1 ;;
    esac
  done
}
# Atomic mkdir refuses existing output paths, including symlinks. Retain artifacts
# on failure for diagnosis. Workspace cleanup is limited to these owned paths.
new_run() {
  mkdir -- "$OUT"
  OUT=$(cd "$OUT" && pwd)
}
validate_receipts() {
  python3 - "$@" <<'PYCODE'
import json
import sys
for name in sys.argv[1:]:
    with open(name) as stream:
        receipt = json.load(stream)
    if receipt.get("outcome") != "validated":
        raise SystemExit(f"{name}: expected validated outcome")
PYCODE
}
