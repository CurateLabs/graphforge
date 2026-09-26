#!/usr/bin/env bash
# One complete ingest (the ladder profile's five import-session commands) into
# a fresh project; each receipt, with its region_diagnostics, is kept in $3.
#   ingest-one.sh GF PROJECT INPUT_DIR RUN_DIR
set -eu
GF=$1; P=$2; IN=$3; D=$4
U=00000000-0000-4000-8000-000000001448
"$GF" --json --project "$P" import-session begin --operation-uuid "$U" > "$D/receipt-0-begin.json"
"$GF" --json --project "$P" import-session register-parquet --session-uuid "$U" --path "$IN/nodes.parquet" --kind nodes > "$D/receipt-1-register-nodes.json"
"$GF" --json --project "$P" import-session register-parquet --session-uuid "$U" --path "$IN/edges.parquet" --kind edges > "$D/receipt-2-register-edges.json"
"$GF" --json --project "$P" import-session validate --session-uuid "$U" > "$D/receipt-3-validate.json"
"$GF" --json --project "$P" import-session commit --session-uuid "$U" > "$D/receipt-4-commit.json"
