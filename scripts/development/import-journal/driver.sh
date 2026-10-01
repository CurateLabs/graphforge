#!/usr/bin/env bash
set -euo pipefail
: "${GF1624_BINARY:?}" "${GF1624_PROJECT:?}" "${GF1624_OUT:?}" "${GF1624_SESSION:?}" "${GF1624_TMP:?}"
: "${GF1624_NODES:?}" "${GF1624_EDGES:?}"
NODES=$GF1624_NODES
EDGES=$GF1624_EDGES
export TMPDIR=$GF1624_TMP
[[ ! -e "$GF1624_PROJECT" ]] || { echo "measurement project already exists" >&2; exit 1; }
mkdir "$GF1624_PROJECT"
cmd() {
    local name=$1; shift
    "$GF1624_BINARY" --json --diagnostics --project "$GF1624_PROJECT" "$@" < /dev/null > "$GF1624_OUT/$name.json" 2> "$GF1624_OUT/$name.stderr"
}
cmd receipt-0-begin import-session begin --operation-uuid "$GF1624_SESSION"
cmd receipt-1-register-nodes import-session register-parquet --session-uuid "$GF1624_SESSION" --path "$NODES" --kind nodes
cmd receipt-2-register-edges import-session register-parquet --session-uuid "$GF1624_SESSION" --path "$EDGES" --kind edges
cmd receipt-3-stage-seal import-session validate --session-uuid "$GF1624_SESSION"
cmd receipt-4-commit import-session commit --session-uuid "$GF1624_SESSION"
