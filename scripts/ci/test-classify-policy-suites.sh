#!/usr/bin/env bash
set -euo pipefail

classifier=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/classify-policy-suites.sh
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

git -C "$fixture" init -q
git -C "$fixture" config user.name "GraphForge CI"
git -C "$fixture" config user.email "ci@graphforge.invalid"
printf 'baseline\n' >"$fixture/README.md"
git -C "$fixture" add README.md
git -C "$fixture" commit -qm baseline
base=$(git -C "$fixture" rev-parse HEAD)

none=$'contracts=false\ncoverage_ledger=false\nbinding_parity=false\napi_bdd=false\nrelease=false\nclean_env=false\ncli_publish=false\ndomain_dependencies=false\ncrate_publish=false\nstorage=false'
contracts=$'contracts=true\ncoverage_ledger=false\nbinding_parity=false\napi_bdd=false\nrelease=false\nclean_env=false\ncli_publish=false\ndomain_dependencies=false\ncrate_publish=false\nstorage=false'
coverage_ledger=$'contracts=false\ncoverage_ledger=true\nbinding_parity=false\napi_bdd=false\nrelease=false\nclean_env=false\ncli_publish=false\ndomain_dependencies=false\ncrate_publish=false\nstorage=false'
storage=$'contracts=false\ncoverage_ledger=false\nbinding_parity=false\napi_bdd=false\nrelease=false\nclean_env=false\ncli_publish=false\ndomain_dependencies=false\ncrate_publish=false\nstorage=true'
all=$'contracts=true\ncoverage_ledger=true\nbinding_parity=true\napi_bdd=true\nrelease=true\nclean_env=true\ncli_publish=true\ndomain_dependencies=true\ncrate_publish=true\nstorage=true'

assert_classification() {
  local expected=$1
  local path=$2

  mkdir -p "$fixture/$(dirname "$path")"
  printf 'change\n' >"$fixture/$path"
  git -C "$fixture" add "$path"
  git -C "$fixture" commit -qm "change $path"

  local actual
  actual=$(cd "$fixture" && "$classifier" "$base" HEAD)
  [[ "$actual" == "$expected" ]] || {
    printf 'unexpected classification for %s\nexpected:\n%s\nactual:\n%s\n' \
      "$path" "$expected" "$actual" >&2
    exit 1
  }
  git -C "$fixture" reset --hard -q "$base"
}

assert_classification "$none" docs/guide.md
assert_classification "$contracts" tests/contracts/m20-contract-matrix.json
assert_classification "$coverage_ledger" scripts/coverage_rust_ledger.py
assert_classification "$storage" .github/workflows/docs.yml
assert_classification "$all" .github/workflows/test.yml
# Unknown paths must fail closed toward full policy validation.
assert_classification "$all" totally/unknown/path.xyz

missing=$(cd "$fixture" && "$classifier" deadbeef HEAD)
[[ "$missing" == "$all" ]] || {
  printf 'missing base must fail safe, got:\n%s\n' "$missing" >&2
  exit 1
}

echo "Repository Policy suite classifier tests passed"
