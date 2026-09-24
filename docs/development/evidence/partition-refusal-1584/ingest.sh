#!/usr/bin/env bash
# ingest.sh NAME INPUT_DIR [ENV=VALUE ...]: the ladder profile's five
# import-session commands into a fresh project, then an untimed edge recount.
set -u
NAME=$1; IN=$2; shift 2
GF=/home/ubuntu/gf-1509-bin-9d4c9cba/gf
W=/home/ubuntu/gf-hub-1509; D=$W/runs/$NAME; P=$D/project
rm -rf "$D"; mkdir -p "$D" "$W/tmp"; export TMPDIR=$W/tmp
U=00000000-0000-4000-8000-000000000099
{
$GF --json --project $P import-session begin --operation-uuid $U &&
$GF --json --project $P import-session register-parquet --session-uuid $U --path $IN/nodes.parquet --kind nodes &&
$GF --json --project $P import-session register-parquet --session-uuid $U --path $IN/edges.parquet --kind edges
} > $D/setup.json 2> $D/setup.stderr || { echo "$NAME setup failed"; exit 1; }
start=$(date +%s.%N)
env "$@" /usr/bin/time -v -o $D/validate.time $GF --json --project $P import-session validate --session-uuid $U > $D/validate.json 2> $D/validate.stderr
v=$?
c=1
if [ $v -eq 0 ]; then env "$@" /usr/bin/time -v -o $D/commit.time $GF --json --project $P import-session commit --session-uuid $U > $D/commit.json 2> $D/commit.stderr; c=$?; fi
wall=$(echo "$(date +%s.%N) - $start" | bc)
if [ $c -eq 0 ]; then
  q=$($GF --json --project $P query --cypher "MATCH ()-[r]->() RETURN count(r)" --output $D/edges.arrow </dev/null | python3 -c 'import json,sys;r=json.loads(sys.stdin.readline());print(r["scalar_u64"], r["result_sha256"])')
  echo "$NAME PUBLISHED wall=${wall}s edges=$q rss_kib=$(grep Maximum $D/validate.time | awk '{print $NF}')"
else
  echo "$NAME REFUSED wall=${wall}s: $(grep -ho '"message":"[^"]*"' $D/validate.json $D/commit.json 2>/dev/null | head -1)"
fi
du -sh $P 2>/dev/null | awk '{print "project " $1}'
rm -rf "$P"
