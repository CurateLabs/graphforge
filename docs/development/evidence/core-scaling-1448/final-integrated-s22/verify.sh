#!/usr/bin/env bash
# Reopen a completed S22 project and retain count and full-scan answer digests.
# Usage: verify.sh GF PROJECT OUT
set -euo pipefail
GF=$1
PROJECT=$2
OUT=$3
HERE=$(cd "$(dirname "$0")" && pwd)
export TMPDIR="$HERE/tmp"
mkdir -p "$OUT" "$TMPDIR"

query() {
  local name=$1 cypher=$2
  "$GF" --json --project "$PROJECT" query --cypher "$cypher" \
    --output "$OUT/$name.arrow" < /dev/null > "$OUT/$name.json" \
    2> "$OUT/$name.stderr"
  rm -f "$OUT/$name.arrow"
}

query nodes 'MATCH (n) RETURN count(n)'
query node-scan 'MATCH (n) RETURN n.node_uuid AS id'
query edges 'MATCH ()-[r]->() RETURN count(r)'
query edge-scan 'MATCH (a)-[r]->(b) RETURN a.node_uuid AS s, b.node_uuid AS d'
