#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
pulumi_bin="${PULUMI:-$(command -v pulumi)}"
typescript_package="$repo_root/iac/pulumi/typescript"
python_package="$repo_root/iac/pulumi/python"
test_root="$repo_root/iac/pulumi/tests"
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/graphforge-pulumi-preview.XXXXXX")"

cleanup() {
  if [[ "$temporary_root" == "${TMPDIR:-/tmp}"/graphforge-pulumi-preview.* ]]; then
    rm -rf "$temporary_root"
  fi
}
trap cleanup EXIT

test -x "$pulumi_bin"
mkdir -p "$temporary_root/backend" "$temporary_root/pulumi-home"
npm --prefix "$typescript_package" run build >/dev/null
uv venv --seed "$temporary_root/python-env" >/dev/null
uv pip install --python "$temporary_root/python-env/bin/python" "$python_package" >/dev/null

export GRAPHFORGE_REPO_ROOT="$repo_root"
export GRAPHFORGE_TEST_SECRET='GRAPHFORGE_SECRET_'"SENTINEL"
export PULUMI_BACKEND_URL="file://$temporary_root/backend"
export PULUMI_CONFIG_PASSPHRASE="graphforge-static-preview"
export PULUMI_HOME="$temporary_root/pulumi-home"
export PULUMI_PYTHON_CMD="$temporary_root/python-env/bin/python"
export PULUMI_SKIP_UPDATE_CHECK="true"

run_preview() {
  local language="$1"
  local project_dir="$temporary_root/projects/$language"
  local output="$temporary_root/$language-preview.json"
  local failure_output="$temporary_root/$language-failure-preview.json"

  mkdir -p "$project_dir"
  cp -R "$test_root/preview/$language/." "$project_dir/"
  if [[ "$language" == "typescript" ]]; then
    ln -s "$typescript_package/node_modules" "$project_dir/node_modules"
  fi
  "$pulumi_bin" --cwd "$project_dir" stack init acceptance --non-interactive >/dev/null
  if ! "$pulumi_bin" --cwd "$project_dir" preview \
    --stack acceptance \
    --json \
    --non-interactive \
    --skip-plugin-pre-install \
    --suppress-permalink \
    >"$output"; then
    "$temporary_root/python-env/bin/python" "$test_root/verify_preview.py" "$output"
    return 1
  fi
  "$temporary_root/python-env/bin/python" "$test_root/verify_preview.py" "$output"
  if GRAPHFORGE_INJECT_FORBIDDEN=1 "$pulumi_bin" --cwd "$project_dir" preview \
    --stack acceptance \
    --json \
    --non-interactive \
    --skip-plugin-pre-install \
    --suppress-permalink \
    >"$failure_output"; then
    echo "$language forbidden-field preview unexpectedly succeeded" >&2
    return 1
  fi
  "$temporary_root/python-env/bin/python" \
    "$test_root/verify_preview.py" \
    "$failure_output" \
    --expected-failure
}

run_preview typescript
run_preview python

echo "TypeScript and Python Pulumi preview acceptance passed."
