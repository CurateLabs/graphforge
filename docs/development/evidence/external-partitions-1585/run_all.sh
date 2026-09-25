#!/usr/bin/env bash
# Every #1585 production-evidence case, serially, under one lock.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
W=${W:-/home/ubuntu/gf-1585-evidence}; export W
MAIN=/home/ubuntu/gf-1448-bin-cca7a1fc/gf          # main at cca7a1fc, production build
GF=/home/ubuntu/gf-1585-bin-fb8a3348/gf            # this branch, production build
TS=/home/ubuntu/gf-1585-bin-fb8a3348-ts/gf         # this branch, test-support build (budget overrides)
S18=/home/ubuntu/gf-1448-evidence/inputs/s18
STAR9=/home/ubuntu/gf-hub-1509/star-9000000
STAR20=/home/ubuntu/gf-1585-star-20m
mkdir -p "$W"
exec 9> "$W/driver.lock"
flock -n 9 || { echo "another driver holds $W/driver.lock"; exit 1; }
{
  date -u +%FT%TZ
  sha256sum "$MAIN" "$GF" "$TS"
  sha256sum "$S18"/*.parquet "$STAR9"/*.parquet "$STAR20"/*.parquet
} > "$W/manifest.txt"
run() { "$HERE/ingest.sh" "$@" | tee -a "$W/results.txt"; }
: > "$W/results.txt"
run g500-s18-main "$MAIN" "$S18" X=1
run g500-s18-branch "$GF" "$S18" X=1
run star9m-main "$MAIN" "$STAR9" X=1
run star9m-branch "$GF" "$STAR9" X=1
run star9m-resident "$TS" "$STAR9" GF_SHAPE_MAX_PARTITION_BYTES=536870912
run star9m-zero-bound "$TS" "$STAR9" GF_SHAPE_MAX_EXTERNAL_PARTITION_BYTES=0
run star9m-small-bound "$TS" "$STAR9" GF_SHAPE_MAX_EXTERNAL_PARTITION_BYTES=268435456
run star20m-branch "$GF" "$STAR20" X=1
run star20m-resident "$TS" "$STAR20" GF_SHAPE_MAX_PARTITION_BYTES=1073741824
echo DONE | tee -a "$W/results.txt"
