#!/usr/bin/env bash
set -euo pipefail

classifier=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/classify-changes.sh
workflow=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/.github/workflows/test.yml
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

git -C "$fixture" init -q
git -C "$fixture" config user.name "GraphForge CI"
git -C "$fixture" config user.email "ci@graphforge.invalid"

mkdir -p "$fixture/crates/graphforge-exec/src"
printf 'baseline\n' >"$fixture/crates/graphforge-exec/src/lib.rs"
printf '[workspace.package]\nlicense = "MIT"\n' >"$fixture/Cargo.toml"
git -C "$fixture" add .
git -C "$fixture" commit -qm baseline
base=$(git -C "$fixture" rev-parse HEAD)

assert_classification() {
  local expected=$1
  local path=$2
  local message=$3

  mkdir -p "$fixture/$(dirname "$path")"
  printf '%s\n' "$message" >>"$fixture/$path"
  git -C "$fixture" add "$path"
  git -C "$fixture" commit -qm "change $path"

  actual=$(
    cd "$fixture"
    "$classifier" "$base" HEAD
  )
  if [[ "$actual" != "$expected" ]]; then
    printf 'unexpected classification for %s\nexpected:\n%s\nactual:\n%s\n' \
      "$path" "$expected" "$actual" >&2
    exit 1
  fi

  git -C "$fixture" reset --hard -q "$base"
}

none=$'rust=false\npython=false\ngherkin=false\nbindings=false\nagent_skills=false\npulumi=false\nterraform=false\nbazel=false'
rust_only=$'rust=true\npython=false\ngherkin=false\nbindings=false\nagent_skills=false\npulumi=false\nterraform=false\nbazel=false'
python_only=$'rust=false\npython=true\ngherkin=false\nbindings=false\nagent_skills=false\npulumi=false\nterraform=false\nbazel=false'
gherkin_rust=$'rust=true\npython=false\ngherkin=true\nbindings=false\nagent_skills=false\npulumi=false\nterraform=false\nbazel=false'
binding_rust=$'rust=true\npython=false\ngherkin=false\nbindings=true\nagent_skills=false\npulumi=false\nterraform=false\nbazel=false'
binding_python=$'rust=false\npython=true\ngherkin=false\nbindings=true\nagent_skills=false\npulumi=false\nterraform=false\nbazel=false'
binding_rust_python=$'rust=true\npython=true\ngherkin=false\nbindings=true\nagent_skills=false\npulumi=false\nterraform=false\nbazel=false'
binding_only=$'rust=false\npython=false\ngherkin=false\nbindings=true\nagent_skills=false\npulumi=false\nterraform=false\nbazel=false'
binding_agent_skills=$'rust=false\npython=false\ngherkin=false\nbindings=true\nagent_skills=true\npulumi=false\nterraform=false\nbazel=false'
agent_skills_only=$'rust=false\npython=false\ngherkin=false\nbindings=false\nagent_skills=true\npulumi=false\nterraform=false\nbazel=false'
pulumi_only=$'rust=false\npython=false\ngherkin=false\nbindings=false\nagent_skills=false\npulumi=true\nterraform=false\nbazel=false'
terraform_only=$'rust=false\npython=false\ngherkin=false\nbindings=false\nagent_skills=false\npulumi=false\nterraform=true\nbazel=false'
binding_iac=$'rust=false\npython=false\ngherkin=false\nbindings=true\nagent_skills=false\npulumi=true\nterraform=true\nbazel=false'
rust_binding_iac=$'rust=true\npython=false\ngherkin=false\nbindings=true\nagent_skills=false\npulumi=true\nterraform=true\nbazel=false'
rust_iac=$'rust=true\npython=false\ngherkin=false\nbindings=false\nagent_skills=false\npulumi=true\nterraform=true\nbazel=false'
all=$'rust=true\npython=true\ngherkin=true\nbindings=true\nagent_skills=true\npulumi=true\nterraform=true\nbazel=true'
bazel_only=$'rust=false\npython=false\ngherkin=false\nbindings=false\nagent_skills=false\npulumi=false\nterraform=false\nbazel=true'
rust_bindings_bazel=$'rust=true\npython=false\ngherkin=false\nbindings=true\nagent_skills=false\npulumi=false\nterraform=false\nbazel=true'

assert_classification "$rust_only" crates/graphforge-exec/src/kernel.rs core-rust
assert_classification "$binding_rust" crates/graphforge-api/src/lib.rs public-api-rust
assert_classification "$binding_python" crates/graphforge-bindings-py/tests/smoke.py python-binding
assert_classification "$binding_only" crates/graphforge-bindings-node/tests/analyze.test.mjs node-binding
assert_classification "$binding_only" packages/cli/bin/graphforge.js node-cli
assert_classification "$binding_rust_python" \
  project-skills/graphforge-bootstrap/SKILL.md project-skill-source
assert_classification "$binding_only" tests/contracts/repository-cli-parity.json cli-parity-fixture
assert_classification "$binding_only" \
  tests/contracts/repository-cli-lifecycle.json cli-lifecycle-fixture
assert_classification "$binding_only" \
  scripts/ci/verify-node-cli-release-package.mjs cli-release-verifier
assert_classification "$binding_rust" tests/contracts/checkpoint-recovery-matrix.json checkpoint-recovery-fixture
assert_classification "$gherkin_rust" tests/features/tck/features/query.feature gherkin
assert_classification "$binding_python" tests/unit/kernel_test.py python-test
assert_classification "$python_only" scripts/some_tool.py python-only
assert_classification "$binding_python" examples/basic_usage.py python-binding-example
assert_classification "$binding_python" scripts/build_feature_graph.py python-binding-script
assert_classification "$binding_agent_skills" package.json node-manifest
assert_classification "$agent_skills_only" packages/agent-skills/package.json agent-skills
assert_classification "$agent_skills_only" \
  packages/agent-skills/bin/graphforge-agent-skills.js agent-skills-nested
assert_classification "$pulumi_only" \
  iac/pulumi/typescript/src/index.ts pulumi-static-validation
assert_classification "$pulumi_only" \
  iac/pulumi/python/src/graphforge_pulumi/validation.py pulumi-python-static-validation
assert_classification "$terraform_only" \
  iac/terraform/provider/internal/validation/validation.go terraform-provider-validation
assert_classification "$terraform_only" \
  iac/terraform/modules/static-validation/main.tf terraform-module-validation
assert_classification "$binding_iac" \
  docs/contracts/graphforge-project-config-v1.schema.json shared-project-config-contract
assert_classification "$binding_iac" \
  docs/contracts/graphforge-resolved-config-v1.schema.json shared-iac-contract
assert_classification "$binding_iac" \
  docs/contracts/graphforge-infra-validation-v1.schema.json shared-infra-contract
assert_classification "$binding_iac" \
  docs/contracts/graphforge-deployment-spec-v1.schema.json shared-deployment-contract
assert_classification "$rust_binding_iac" \
  docs/contracts/examples/graphforge-infra-validation-production-v1.json shared-iac-fixture
assert_classification "$rust_binding_iac" \
  docs/contracts/examples/graphforge-deployment-spec-production-v1.json shared-deployment-fixture
assert_classification "$rust_binding_iac" \
  docs/contracts/examples/graphforge-project-config-v1.yaml shared-project-config-fixture
assert_classification "$rust_binding_iac" \
  crates/graphforge-api/src/repository.rs rust-owned-iac-contract
assert_classification "$rust_iac" crates/graphforge-cli/src/lib.rs rust-owned-iac-cli
assert_classification "$all" ".github/workflows/test.yml" workflow
assert_classification "$all" scripts/ci/require-gates.sh aggregate-gate
assert_classification "$all" scripts/ci/concurrency-short-gate.py concurrency-short-gate
assert_classification "$all" tests/contracts/concurrency-short-matrix.json concurrency-short-matrix
assert_classification "$bazel_only" MODULE.bazel bazel-module
assert_classification "$bazel_only" tools/bazel/smoke/src/lib.rs bazel-smoke
assert_classification "$bazel_only" scripts/ci/cargo-bazel-drift-check.py bazel-drift-check
assert_classification "$bazel_only" cargo-bazel-lock.json bazel-crate-universe-lock
assert_classification "$bazel_only" crates/graphforge-core/BUILD.bazel bazel-crate-build
assert_classification "$bazel_only" docs/reference/BUILD.bazel bazel-docs-reference-build
assert_classification "$bazel_only" docs/contracts/examples/BUILD.bazel bazel-docs-contracts-build
assert_classification "$bazel_only" project-skills/BUILD.bazel bazel-project-skills-build
assert_classification "$bazel_only" scripts/ci/assemble_bazel_binding_packages.py bazel-binding-packaging
assert_classification "$bazel_only" tools/bazel/bindings/BUILD.bazel bazel-bindings-handoff
assert_classification "$none" "docs/a file with spaces.md" docs-only

# Packaging-only Cargo metadata must not compile the workspace.
perl -0pi -e 's/license = "MIT"/license = "Apache-2.0"/' "$fixture/Cargo.toml"
git -C "$fixture" add Cargo.toml
git -C "$fixture" commit -qm "change license metadata"
metadata_actual=$(
  cd "$fixture"
  "$classifier" "$base" HEAD
)
[[ "$metadata_actual" == "$none" ]] || {
  printf 'license-only manifest edit must be metadata-only, got:\n%s\n' \
    "$metadata_actual" >&2
  exit 1
}
git -C "$fixture" reset --hard -q "$base"

# Behavioral Cargo manifest changes remain fail-safe.
printf 'datafusion = "50"\n' >>"$fixture/Cargo.toml"
git -C "$fixture" add Cargo.toml
git -C "$fixture" commit -qm "change dependency"
manifest_actual=$(
  cd "$fixture"
  "$classifier" "$base" HEAD
)
[[ "$manifest_actual" == "$rust_bindings_bazel" ]] || {
  printf 'dependency manifest edit must run Rust, bindings, and Bazel drift, got:\n%s\n' \
    "$manifest_actual" >&2
  exit 1
}
git -C "$fixture" reset --hard -q "$base"

missing=$(
  cd "$fixture"
  "$classifier" deadbeef HEAD
)
[[ "$missing" == "$all" ]] || {
  printf 'missing base must fail safe, got:\n%s\n' "$missing" >&2
  exit 1
}

# The PR-only workflow must classify against the pull request base. An absent
# or unusable base still fails closed in the classifier.
grep -Fq '"${{ github.event.pull_request.base.sha }}"' "$workflow"
empty=$(
  cd "$fixture"
  "$classifier" "" HEAD
)
[[ "$empty" == "$all" ]] || {
  printf 'empty base must fail safe, got:\n%s\n' "$empty" >&2
  exit 1
}

echo "changed-path classifier tests passed"
