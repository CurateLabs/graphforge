#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: require-gates.sh <classifier> <policy> [optional ...]" >&2
  exit 2
fi

classifier=$1
policy=$2
shift 2

if [[ "$classifier" != "success" || "$policy" != "success" ]]; then
  echo "mandatory CI job failed: classifier=$classifier policy=$policy" >&2
  exit 1
fi

for result in "$@"; do
  if [[ "$result" != "success" && "$result" != "skipped" ]]; then
    echo "applicable CI job ended with: $result" >&2
    exit 1
  fi
done
