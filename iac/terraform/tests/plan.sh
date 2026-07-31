#!/bin/sh
set -eu

: "${GO:?set GO to a Go 1.25+ binary}"
: "${TERRAFORM:?set TERRAFORM to a Terraform 1.8+ binary}"
JQ=${JQ:-$(command -v jq || true)}
: "${JQ:?set JQ to a jq binary}"
if [ ! -x "$JQ" ]; then
  echo "JQ must name an executable jq binary" >&2
  exit 1
fi

terraform_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/graphforge-terraform-plan.XXXXXX")
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM

provider_version="0.5.0"
provider_platform=$("$GO" env GOOS)_$("$GO" env GOARCH)
provider_directory="$temporary_directory/providers"
mirror_directory="$temporary_directory/mirror"
mirror_provider_directory="$mirror_directory/registry.terraform.io/curatelabs/graphforge/$provider_version/$provider_platform"
mkdir -p "$provider_directory" "$mirror_provider_directory"
(
  cd "$terraform_root/provider"
  "$GO" build -buildvcs=false \
    -o "$provider_directory/terraform-provider-graphforge"
)
cp \
  "$provider_directory/terraform-provider-graphforge" \
  "$mirror_provider_directory/terraform-provider-graphforge_v$provider_version"

sed \
  -e "s|@PROVIDER_DIRECTORY@|$provider_directory|" \
  -e "s|@MIRROR_DIRECTORY@|$mirror_directory|" \
  "$terraform_root/tests/terraformrc.tmpl" \
  >"$temporary_directory/terraformrc"

fixture="$terraform_root/tests/fixture"
module="$terraform_root/modules/static-validation"
workspace="$temporary_directory/workspace"
mkdir -p "$workspace/tests" "$workspace/modules"
cp -R "$fixture" "$workspace/tests/fixture"
cp -R "$module" "$workspace/modules/static-validation"
workspace_fixture="$workspace/tests/fixture"

# Seed accepted source metadata with a sentinel. The input digest must change,
# while target-scoped validation evidence must not disclose unrelated metadata.
sentinel='GRAPHFORGE_SECRET_'"SENTINEL_DO_NOT_LEAK"
"$JQ" --arg sentinel "$sentinel" \
  '.sources = [{
    "id": "sentinel-source",
    "uri": ("https://example.invalid/" + $sentinel),
    "sha256": ("a" * 64)
  }]' \
  "$fixture/resolved.json" \
  >"$workspace_fixture/resolved.json"
if ! "$JQ" -e --arg sentinel "$sentinel" \
  '.sources[0].uri == ("https://example.invalid/" + $sentinel)' \
  "$workspace_fixture/resolved.json" >/dev/null; then
  echo "sentinel injection did not apply to the resolved fixture" >&2
  exit 1
fi

TF_CLI_CONFIG_FILE="$temporary_directory/terraformrc" \
  "$TERRAFORM" -chdir="$workspace_fixture" init -backend=false
TF_CLI_CONFIG_FILE="$temporary_directory/terraformrc" \
  "$TERRAFORM" -chdir="$workspace_fixture" validate
TF_CLI_CONFIG_FILE="$temporary_directory/terraformrc" \
  "$TERRAFORM" -chdir="$workspace_fixture" plan \
    -refresh=false \
    -input=false \
    -lock=false \
    -out="$temporary_directory/plan"
TF_CLI_CONFIG_FILE="$temporary_directory/terraformrc" \
  "$TERRAFORM" -chdir="$workspace_fixture" show -json "$temporary_directory/plan" \
  >"$temporary_directory/plan.json"

"$JQ" -e '
  ((.resource_changes // []) | length == 0) and
  (.planned_values.outputs.static_validity.value == "valid") and
  (.planned_values.outputs.planned_infrastructure.value == "validated") and
  (.planned_values.outputs.connectivity.value == "not_checked") and
  (.planned_values.outputs.readiness.value == "not_checked") and
  (.planned_values.outputs.capability_compatibility.value == "requirements_declared") and
  (.configuration.root_module.module_calls.graphforge_validation.source == "../../modules/static-validation") and
  (.configuration.root_module.module_calls.graphforge_validation.module.resources[0].address == "data.graphforge_infra_validation.this")
' "$temporary_directory/plan.json" >/dev/null

"$JQ" '
  {
    outputs: .planned_values.outputs,
    validation: (
      [
        .planned_values
        | ..
        | objects
        | select(.type? == "graphforge_infra_validation")
        | .values
      ][0]
      | del(.resolved_json)
    )
  }
' "$temporary_directory/plan.json" >"$temporary_directory/plan-evidence.json"
if grep -q "$sentinel" "$temporary_directory/plan-evidence.json"; then
  echo "secret sentinel entered Terraform plan evidence" >&2
  exit 1
fi

# Definition paths must use portable forward-slash separators.
"$JQ" '.project.ontology = "ontology\\core"' \
  "$fixture/resolved.json" >"$workspace_fixture/resolved.json"
if \
  TF_CLI_CONFIG_FILE="$temporary_directory/terraformrc" \
  "$TERRAFORM" -chdir="$workspace_fixture" plan \
    -refresh=false \
    -input=false \
    -lock=false \
    >"$temporary_directory/backslash-plan.log" 2>&1; then
  echo "Terraform accepted a definition path containing a backslash" >&2
  exit 1
fi

# An input that attempts to place a secret value in resolved JSON must fail
# closed, and the sensitive value must not enter Terraform diagnostics.
"$JQ" --arg sentinel "$sentinel" \
  '.secrets = [{"id":"service-token","source":"secret_manager","value":$sentinel}]' \
  "$fixture/resolved.json" \
  >"$workspace_fixture/resolved.json"
if cmp -s "$fixture/resolved.json" "$workspace_fixture/resolved.json" ||
  ! "$JQ" -e --arg sentinel "$sentinel" \
    '.secrets[0].value == $sentinel' \
    "$workspace_fixture/resolved.json" >/dev/null; then
  echo "secret injection did not apply to the resolved fixture" >&2
  exit 1
fi
if \
  TF_CLI_CONFIG_FILE="$temporary_directory/terraformrc" \
  "$TERRAFORM" -chdir="$workspace_fixture" plan \
    -refresh=false \
    -input=false \
    -lock=false \
    >"$temporary_directory/invalid-plan.log" 2>&1; then
  echo "Terraform accepted a resolved secret value" >&2
  exit 1
fi
if grep -q "$sentinel" "$temporary_directory/invalid-plan.log"; then
  echo "secret sentinel entered Terraform diagnostics" >&2
  exit 1
fi
