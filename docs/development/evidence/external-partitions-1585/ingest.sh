#!/usr/bin/env bash
# ingest.sh NAME GF INPUT_DIR [ENV=VALUE ...]
# The ladder profile's five import-session commands into a fresh project under
# $W/runs/NAME, then, when the commit publishes, five content queries. Keeps
# every receipt and `time -v` record; deletes the project afterwards.
set -u
NAME=$1; GF=$2; IN=$3; shift 3
W=${W:-/home/ubuntu/gf-1585-evidence}
D=$W/runs/$NAME; P=$D/project
rm -rf "$D"; mkdir -p "$D" "$W/tmp"; export TMPDIR=$W/tmp
U=00000000-0000-4000-8000-000000001585
sha256sum "$GF" | awk '{print $1}' > "$D/gf.sha256"
printf '%s\n' "$@" > "$D/env.txt"
{
"$GF" --json --project "$P" import-session begin --operation-uuid $U &&
"$GF" --json --project "$P" import-session register-parquet --session-uuid $U --path "$IN/nodes.parquet" --kind nodes &&
"$GF" --json --project "$P" import-session register-parquet --session-uuid $U --path "$IN/edges.parquet" --kind edges
} > "$D/setup.json" 2> "$D/setup.stderr" || { echo "$NAME SETUP-FAILED"; exit 1; }
env "$@" /usr/bin/time -v -o "$D/validate.time" "$GF" --json --project "$P" import-session validate --session-uuid $U > "$D/validate.json" 2> "$D/validate.stderr"
v=$?
c=1
if [ $v -eq 0 ]; then
  env "$@" /usr/bin/time -v -o "$D/commit.time" "$GF" --json --project "$P" import-session commit --session-uuid $U > "$D/commit.json" 2> "$D/commit.stderr"
  c=$?
fi
rss() { grep -h Maximum "$D/$1.time" 2>/dev/null | awk '{print $NF}'; }
if [ $c -eq 0 ]; then
  q() {  # q LABEL CYPHER
    "$GF" --json --project "$P" query --cypher "$2" --output "$D/$1.arrow" < /dev/null > "$D/$1.json" 2> "$D/$1.stderr"
    python3 -c 'import json,sys;r=json.loads(open(sys.argv[1]).readline());v=r.get("scalar_u64");print(v if v is not None else "rows=%s" % r["rows"], r["result_sha256"])' "$D/$1.json"
    rm -f "$D/$1.arrow"
  }
  nodes=$(q nodes "MATCH (n) RETURN count(n)")
  nodescan=$(q node-scan "MATCH (n) RETURN n.node_uuid AS id")
  count=$(q count "MATCH ()-[r]->() RETURN count(r)")
  hub=$(q hub "MATCH ()-[r]->(b) RETURN b.node_uuid AS id, count(r) AS n ORDER BY n DESC, id LIMIT 3")
  # Unordered on purpose: the scan follows the published layout, so equal
  # digests also mean equal storage order. (An ORDER BY over 9M rows exhausts
  # the query engine's sort pool.)
  edges=$(q edges "MATCH (a)-[r]->(b) RETURN a.node_uuid AS s, b.node_uuid AS d")
  ext=$(python3 -c 'import json,sys;c=json.load(open(sys.argv[1]))["construction"];print(c.get("external_partitions"), c.get("external_runs"), c.get("external_run_bytes"))' "$D/commit.json" 2>/dev/null || echo "? ? ?")
  echo "$NAME PUBLISHED nodes=[$nodes] node-scan=[$nodescan] count=[$count] hub=[$hub] edges=[$edges] external(partitions,runs,bytes)=[$ext] rss_kib(validate,commit)=[$(rss validate),$(rss commit)]"
else
  msg=$(grep -ho '"message":"[^"]*"' "$D/validate.json" "$D/commit.json" 2>/dev/null | head -1)
  [ -n "$msg" ] || msg=$(tail -c 400 "$D/validate.stderr" "$D/commit.stderr" 2>/dev/null | tr '\n' ' ')
  echo "$NAME REFUSED $msg rss_kib(validate,commit)=[$(rss validate),$(rss commit)]"
fi
# Scratch left behind by any outcome: run temporaries anywhere in the project.
echo "$NAME leftover_run_temps=$(find "$P" -name '.artifact-xrun-*' 2>/dev/null | wc -l) project=$(du -sh "$P" 2>/dev/null | awk '{print $1}')"
rm -rf "$P"
