#!/usr/bin/env bash
# Emit fail-closed Repository Policy suite selectors for a PR range.
set -euo pipefail

base_sha=${1:-}
head_sha=${2:-HEAD}

contracts=false
coverage_ledger=false
binding_parity=false
api_bdd=false
release=false
clean_env=false
cli_publish=false
domain_dependencies=false
crate_publish=false
storage=false

enable_all() {
  contracts=true
  coverage_ledger=true
  binding_parity=true
  api_bdd=true
  release=true
  clean_env=true
  cli_publish=true
  domain_dependencies=true
  crate_publish=true
  storage=true
}

emit() {
  printf 'contracts=%s\n' "$contracts"
  printf 'coverage_ledger=%s\n' "$coverage_ledger"
  printf 'binding_parity=%s\n' "$binding_parity"
  printf 'api_bdd=%s\n' "$api_bdd"
  printf 'release=%s\n' "$release"
  printf 'clean_env=%s\n' "$clean_env"
  printf 'cli_publish=%s\n' "$cli_publish"
  printf 'domain_dependencies=%s\n' "$domain_dependencies"
  printf 'crate_publish=%s\n' "$crate_publish"
  printf 'storage=%s\n' "$storage"
}

if [[ -z "$base_sha" ]] || ! git cat-file -e "${base_sha}^{commit}" 2>/dev/null; then
  enable_all
  emit
  exit 0
fi

changed_files=$(mktemp)
trap 'rm -f "$changed_files"' EXIT
if ! git diff --name-only -z "${base_sha}...${head_sha}" >"$changed_files"; then
  enable_all
  emit
  exit 0
fi

while IFS= read -r -d '' path; do
  case "$path" in
    .github/workflows/test.yml | scripts/ci/classify-policy-suites.sh | \
      scripts/ci/test-classify-policy-suites.sh)
      enable_all
      ;;
    scripts/ci/m20-contract-gate.py | scripts/ci/test-m20-contract-gate.py | \
      tests/contracts/m20-contract-matrix.json | \
      scripts/ci/m21-contract-gate.py | scripts/ci/test-m21-contract-gate.py | \
      tests/contracts/m21-contract-matrix.json | \
      scripts/ci/checkpoint-recovery-gate.py | scripts/ci/test-checkpoint-recovery-gate.py | \
      tests/contracts/checkpoint-recovery-matrix.json | \
      scripts/ci/concurrency-short-gate.py | scripts/ci/test-concurrency-short-gate.py | \
      scripts/ci/concurrency-stress-gate.py | scripts/ci/test-concurrency-stress-gate.py | \
      scripts/ci/test-concurrency-recovery-gate.py | \
      tests/contracts/concurrency-short-matrix.json | \
      tests/contracts/concurrency-recovery-matrix.json | \
      scripts/ci/non-cypher-surface-gate.py | scripts/ci/test-non-cypher-surface-gate.py | \
      tests/contracts/non-cypher-rust-surface.json)
      contracts=true
      ;;
    scripts/coverage_rust_ledger.py | scripts/check-coverage-rust.sh | \
      scripts/ci/test-rust-coverage-ledger.py)
      coverage_ledger=true
      ;;
    scripts/ci/check-binding-parity-policy.py)
      binding_parity=true
      ;;
    scripts/ci/api-bdd-policy.py | scripts/ci/test-api-bdd-policy.py | \
      tests/features/api/* | tests/features/api/**/* | \
      tests/contracts/api-bdd-exclusions.json)
      api_bdd=true
      ;;
    scripts/ci/test-binding-release-candidate.py | \
      scripts/ci/test-release-load-matrix.py | scripts/ci/test-release-load-executor.py | \
      scripts/ci/test-m1-release-certification.py | \
      .github/workflows/binding-release-candidate.yml | \
      .github/workflows/m1-release-certification.yml)
      release=true
      ;;
    scripts/ci/test-clean-env-verify.py | scripts/ci/clean-env-verify.py | \
      .github/workflows/clean-env-verify.yml)
      clean_env=true
      ;;
    scripts/ci/test-publish-cli-contract.py | scripts/ci/verify-node-cli-release-package.mjs | \
      packages/cli/* | packages/cli/**/*)
      cli_publish=true
      ;;
    scripts/ci/check-domain-dependencies.py | scripts/ci/test-domain-dependencies.py | \
      docs/adr/0014-* | Cargo.toml | crates/*/Cargo.toml)
      domain_dependencies=true
      ;;
    scripts/ci/test-crate-publish-plan.py | \
      scripts/ci/test-crate-authorize-refresh-nodes.py | scripts/ci/test-publish-crates.py | \
      scripts/ci/crate-publish-plan.py | scripts/ci/publish-crates.py | \
      .github/workflows/publish.yaml)
      crate_publish=true
      ;;
    scripts/ci/test-release-publish-preflight.py | scripts/ci/test-release-candidate.py | \
      scripts/ci/test-prepare-napi-packages.py | scripts/ci/test-publish-npm-artifacts.py | \
      scripts/ci/test-amend-npm-main-artifact.py | scripts/ci/release-publish-preflight.py | \
      scripts/ci/release-candidate.py | scripts/ci/prepare-napi-packages.py | \
      scripts/ci/publish-npm-artifacts.py | scripts/ci/amend-npm-main-artifact.py | \
      .github/workflows/publish.yaml)
      release=true
      ;;
    scripts/ci/test-ci-storage-policy.py | .github/workflows/*.yml | .github/workflows/*.yaml)
      storage=true
      ;;
  esac
done <"$changed_files"

emit
