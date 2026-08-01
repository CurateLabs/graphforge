#!/usr/bin/env bash
set -euo pipefail

release_tag="${1:?release tag is required}"
release_version="${2:?release version is required}"
destination="${3:?destination is required}"

mkdir -p "$destination/attempts" "$destination/receipts"
assets="$(gh release view "$release_tag" --json assets --jq '.assets[].name')"
has_attempt=false
has_receipt=false
while IFS= read -r asset; do
  case "$asset" in
    "v$release_version-attempt-"*.json) has_attempt=true ;;
    "v$release_version-accepted-"*.json) has_receipt=true ;;
  esac
done <<<"$assets"

if test "$has_attempt" = true; then
  gh release download "$release_tag" \
    --pattern "v$release_version-attempt-*.json" \
    --dir "$destination/attempts"
fi

if test "$has_receipt" = true; then
  gh release download "$release_tag" \
    --pattern "v$release_version-accepted-*.json" \
    --dir "$destination/receipts"
fi
