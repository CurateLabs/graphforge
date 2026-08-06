#!/usr/bin/env bash
set -euo pipefail

base_sha=${1:-}
head_sha=${2:-HEAD}

rust=false
python=false
gherkin=false
bindings=false
agent_skills=false
pulumi=false
terraform=false
bazel=false

emit() {
  printf 'rust=%s\n' "$rust"
  printf 'python=%s\n' "$python"
  printf 'gherkin=%s\n' "$gherkin"
  printf 'bindings=%s\n' "$bindings"
  printf 'agent_skills=%s\n' "$agent_skills"
  printf 'pulumi=%s\n' "$pulumi"
  printf 'terraform=%s\n' "$terraform"
  printf 'bazel=%s\n' "$bazel"
}

enable_all() {
  rust=true
  python=true
  gherkin=true
  bindings=true
  agent_skills=true
  pulumi=true
  terraform=true
  bazel=true
}

# Cargo package metadata does not affect compiled behavior. Treat a manifest
# edit as metadata-only only when both revisions exist and every changed line is
# a recognized packaging field. Dependency, feature, profile, or target edits
# still trigger Rust and binding validation.
manifest_is_metadata_only() {
  local path=$1

  git cat-file -e "${base_sha}:${path}" 2>/dev/null || return 1
  git cat-file -e "${head_sha}:${path}" 2>/dev/null || return 1

  git diff --unified=0 "${base_sha}...${head_sha}" -- "$path" |
    awk '
      /^\+\+\+|^---|^@@/ { next }
      /^[+-]/ {
        line = substr($0, 2)
        if (line ~ /^[[:space:]]*$/ || line ~ /^[[:space:]]*#/) {
          next
        }
        if (line ~ /^[[:space:]]*(authors|categories|description|documentation|homepage|keywords|license|license-file|publish|readme|repository)[[:space:]]*=/) {
          next
        }
        exit 1
      }
    '
}

# Pushes and missing PR history fail safe toward full validation.
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
    .github/workflows/test.yml | scripts/ci/classify-changes.sh | \
      scripts/ci/test-classify-changes.sh | scripts/ci/require-gates.sh | \
      scripts/ci/test-require-gates.sh | \
      .github/workflows/concurrency-stress-gate.yml | \
      scripts/ci/concurrency-short-gate.py | \
      scripts/ci/test-concurrency-short-gate.py | \
      scripts/ci/concurrency-stress-gate.py | \
      scripts/ci/test-concurrency-stress-gate.py | \
      tests/contracts/concurrency-short-matrix.json | \
      tests/contracts/concurrency-recovery-matrix.json)
      enable_all
      ;;

    Cargo.toml | crates/*/Cargo.toml)
      if ! manifest_is_metadata_only "$path"; then
        rust=true
        bindings=true
        bazel=true
      fi
      ;;

    Cargo.lock | rust-toolchain.toml | .cargo/* | fuzz/*)
      rust=true
      bindings=true
      bazel=true
      ;;

    MODULE.bazel | MODULE.bazel.lock | BUILD.bazel | .bazelrc | .bazelversion | \
      cargo-bazel-lock.json | tools/bazel/* | tools/bazel/**/* | \
      crates/BUILD.bazel | crates/*/BUILD.bazel | \
      docs/contracts/examples/BUILD.bazel | docs/reference/BUILD.bazel | \
      tests/features/BUILD.bazel | tests/tck/BUILD.bazel | \
      examples/agent_grounding/BUILD.bazel | \
      scripts/ci/cargo-bazel-drift-check.py | \
      scripts/ci/test-cargo-bazel-drift-check.py | \
      scripts/ci/assemble_bazel_binding_packages.py | \
      scripts/ci/test-assemble-bazel-binding-packages.py | \
      scripts/ci/BUILD.bazel)
      bazel=true
      ;;

    tests/features/api/* | tests/features/api/**/* | \
      crates/graphforge-api/tests/bdd/* | crates/graphforge-api/tests/bdd/**/*)
      gherkin=true
      bindings=true
      rust=true
      ;;

    tests/features/node/* | tests/features/node/**/*)
      gherkin=true
      bindings=true
      ;;

    crates/graphforge-api/* | crates/graphforge-bindings-py/* | crates/graphforge-bindings-node/*)
      bindings=true
      [[ "$path" == *.rs ]] && rust=true
      [[ "$path" == *.py ]] && python=true
      if [[ "$path" == crates/graphforge-api/src/repository.rs ]]; then
        pulumi=true
        terraform=true
      fi
      ;;

    crates/graphforge-cli/src/lib.rs)
      rust=true
      pulumi=true
      terraform=true
      ;;

    packages/cli/* | packages/cli/**/* | tests/contracts/repository-cli-*.json | \
      scripts/ci/verify-node-cli-release-package.mjs | \
      scripts/ci/test-publish-cli-contract.py)
      bindings=true
      ;;

    project-skills/*)
      rust=true
      python=true
      bindings=true
      ;;

    tests/contracts/checkpoint-recovery-matrix.json)
      rust=true
      bindings=true
      ;;

    crates/*.rs | crates/*/*.rs | crates/*/*/*.rs | crates/*/*/*/*.rs | \
      crates/*/*/*/*/*.rs)
      rust=true
      ;;

    tests/unit/* | tests/unit/**/* | tests/integration/* | \
      tests/integration/**/* | tests/parity/* | tests/parity/**/* | \
      pyproject.toml | uv.lock)
      python=true
      bindings=true
      ;;

    examples/basic_usage.py | scripts/build_feature_graph.py)
      python=true
      bindings=true
      ;;

    crates/graphforge-bindings-py/python/* | crates/graphforge-bindings-py/python/**/* | \
      scripts/*.py | scripts/**/*.py | examples/*.py)
      python=true
      ;;

    tests/features/* | tests/features/**/* | crates/graphforge-api/tests/bdd/* | \
      crates/graphforge-api/tests/bdd/**/*)
      gherkin=true
      rust=true
      ;;

    benchmarks/algorithms/* | benchmarks/algorithms/**/* | package.json | \
      pnpm-lock.yaml)
      bindings=true
      [[ "$path" == package.json || "$path" == pnpm-lock.yaml ]] && agent_skills=true
      ;;

    packages/agent-skills/* | pnpm-workspace.yaml)
      agent_skills=true
      ;;

    iac/pulumi/* | iac/pulumi/**/*)
      pulumi=true
      ;;

    iac/terraform/* | iac/terraform/**/*)
      terraform=true
      ;;

    docs/contracts/graphforge-project-config-*.schema.json | \
      docs/contracts/graphforge-resolved-config-*.schema.json | \
      docs/contracts/graphforge-infra-validation-*.schema.json | \
      docs/contracts/graphforge-deployment-spec-*.schema.json)
      bindings=true
      pulumi=true
      terraform=true
      ;;

    docs/contracts/examples/graphforge-*.json | \
      docs/contracts/examples/graphforge-*.yaml)
      rust=true
      bindings=true
      pulumi=true
      terraform=true
      ;;
  esac
done <"$changed_files"

emit
