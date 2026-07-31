#!/bin/sh
set -eu

: "${TERRAFORM:?set TERRAFORM to a Terraform 1.8+ binary}"
JQ=${JQ:-$(command -v jq || true)}
: "${JQ:?set JQ to a jq binary}"

terraform_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/graphforge-deployment-spec.XXXXXX")
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM

workspace="$temporary_directory/workspace"
mkdir -p "$workspace/modules" "$workspace/example"
cp -R "$terraform_root/modules/deployment-spec" "$workspace/modules/deployment-spec"
cp -R "$terraform_root/examples/deployment-spec/." "$workspace/example/"
sed -e 's|../../modules/deployment-spec|../modules/deployment-spec|' \
  "$terraform_root/examples/deployment-spec/main.tf" >"$workspace/example/main.tf"
cp "$terraform_root/../../docs/contracts/examples/graphforge-resolved-v1.json" \
  "$workspace/example/resolved.json"

"$TERRAFORM" -chdir="$workspace/example" init -backend=false
"$TERRAFORM" -chdir="$workspace/example" validate
"$TERRAFORM" -chdir="$workspace/example" plan \
  -refresh=false -input=false -lock=false -out="$temporary_directory/plan"
"$TERRAFORM" -chdir="$workspace/example" show -json "$temporary_directory/plan" \
  >"$temporary_directory/plan.json"

"$JQ" -e '
  ((.resource_changes // []) | length == 0) and
  ([.planned_values.root_module | .. | objects | .resources? // empty | .[]] | length == 0) and
  (.planned_values.outputs.deployment_spec.value.contract == "graphforge-deployment-spec/1") and
  (.planned_values.outputs.deployment_spec.value.target_id == "production") and
  (.planned_values.outputs.deployment_spec.value.artifact.locator | endswith("@sha256:" + ("c" * 64))) and
  (.planned_values.outputs.deployment_spec.value.bindings == {"secret_ids":["service-token"],"source_ids":["example-data"]}) and
  (.planned_values.outputs.deployment_spec.value.ownership == {"data":"external","infrastructure":"caller_owned","runtime":"caller_owned","specification":"graphforge"}) and
  (.planned_values.outputs.deployment_spec.value.infrastructure == {"mutation":"none","status":"caller_owned"}) and
  (.planned_values.outputs.connectivity.value == "not_checked") and
  (.planned_values.outputs.readiness.value == "not_checked") and
  (.planned_values.outputs.capability_compatibility.value == "requirements_declared")
' "$temporary_directory/plan.json" >/dev/null
"$JQ" -e --slurpfile expected \
  "$terraform_root/../../docs/contracts/examples/graphforge-deployment-spec-production-v1.json" '
  (.planned_values.outputs.deployment_spec_json.value == (($expected[0] | tojson) + "\n")) and
  ((.planned_values.outputs.deployment_spec_json.value | utf8bytelength) == ((($expected[0] | tojson) + "\n") | utf8bytelength))
' "$temporary_directory/plan.json" >/dev/null

# Every target topology already accepted by the shared resolved-config contract
# must project unchanged; this module adds no provider-specific restrictions.
"$JQ" -c '.targets[] | {id, artifact}' \
  "$terraform_root/../../docs/contracts/examples/graphforge-resolved-v1.json" |
while IFS= read -r selected; do
  selected_id=$(printf '%s' "$selected" | "$JQ" -r '.id')
  selected_kind=$(printf '%s' "$selected" | "$JQ" -r '.artifact.kind')
  selected_sha=$(printf '%s' "$selected" | "$JQ" -r '.artifact.sha256')
  if [ "$selected_kind" = "oci_image" ]; then
    selected_locator="registry.example.com/graphforge/$selected_id@sha256:$selected_sha"
  else
    selected_locator="https://artifacts.example.invalid/graphforge/$selected_id/$selected_sha"
  fi
  "$TERRAFORM" -chdir="$workspace/example" plan \
    -refresh=false -input=false -lock=false \
    -var "target=$selected_id" -var "artifact_locator=$selected_locator" \
    -out="$temporary_directory/topology-$selected_id" >/dev/null
  "$TERRAFORM" -chdir="$workspace/example" show -json \
    "$temporary_directory/topology-$selected_id" |
    "$JQ" -e --arg id "$selected_id" '
      ((.resource_changes // []) | length == 0) and
      ([.planned_values.root_module | .. | objects | .resources? // empty | .[]] | length == 0) and
      (.planned_values.outputs.deployment_spec.value.target_id == $id)
    ' >/dev/null
done

"$TERRAFORM" -chdir="$workspace/example" apply -input=false -lock=false \
  -auto-approve "$temporary_directory/plan"
"$TERRAFORM" -chdir="$workspace/example" show -json >"$temporary_directory/state.json"
"$JQ" -e '([.values.root_module | .. | objects | .resources? // empty | .[]] | length == 0)' \
  "$temporary_directory/state.json" >/dev/null
if grep -q 'https://example.invalid/graphforge/example.parquet' "$temporary_directory/state.json"; then
  echo "source location entered Terraform state" >&2
  exit 1
fi

# A changed, still-pinned artifact is visible as output drift without creating
# provider resources or mutating infrastructure.
"$JQ" '(.targets[] | select(.id == "production") | .artifact.sha256) = ("d" * 64)' \
  "$workspace/example/resolved.json" >"$temporary_directory/drifted.json"
cp "$temporary_directory/drifted.json" "$workspace/example/resolved.json"
drift_locator='registry.example.com/graphforge/core@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'
"$TERRAFORM" -chdir="$workspace/example" plan \
  -refresh=false -input=false -lock=false \
  -var "artifact_locator=$drift_locator" -out="$temporary_directory/drift-plan"
"$TERRAFORM" -chdir="$workspace/example" show -json "$temporary_directory/drift-plan" \
  >"$temporary_directory/drift-plan.json"
"$JQ" -e '
  ((.resource_changes // []) | length == 0) and
  ([.planned_values.root_module | .. | objects | .resources? // empty | .[]] | length == 0) and
  (.output_changes.artifact_sha256.after == ("d" * 64)) and
  (.output_changes.artifact_sha256.before == ("c" * 64))
' "$temporary_directory/drift-plan.json" >/dev/null

# Every invalid fixture must fail before apply and diagnostics must not disclose
# the credential sentinel.
cp "$terraform_root/../../docs/contracts/examples/graphforge-resolved-v1.json" \
  "$workspace/example/resolved.json"
"$JQ" -c '.cases[]' "$terraform_root/tests/deployment-spec-invalid.json" |
while IFS= read -r case; do
  name=$(printf '%s' "$case" | "$JQ" -r '.name')
  locator=$(printf '%s' "$case" | "$JQ" -r '.locator')
  filter=$(printf '%s' "$case" | "$JQ" -r '.jq')
  diagnostic=$(printf '%s' "$case" | "$JQ" -r '.diagnostic')
  "$JQ" "$filter" "$terraform_root/../../docs/contracts/examples/graphforge-resolved-v1.json" \
    >"$workspace/example/resolved.json"
  if "$TERRAFORM" -chdir="$workspace/example" plan \
    -refresh=false -input=false -lock=false \
    -var "artifact_locator=$locator" >"$temporary_directory/$name.log" 2>&1; then
    echo "invalid deployment-spec fixture passed: $name" >&2
    exit 1
  fi
  if grep -q 'user:token' "$temporary_directory/$name.log"; then
    echo "credential-bearing value leaked into diagnostics: $name" >&2
    exit 1
  fi
  if ! grep -Fq "$diagnostic" "$temporary_directory/$name.log"; then
    echo "invalid fixture did not report its expected diagnostic: $name" >&2
    cat "$temporary_directory/$name.log" >&2
    exit 1
  fi
done

# Terraform jsonencode defines the cross-renderer canonical escape rule. Verify
# permitted special characters are escaped without changing their semantics.
"$JQ" '(.targets[] | select(.id == "production") | .artifact.version) = "0.5.1<>&\u2028\u2029"' \
  "$terraform_root/../../docs/contracts/examples/graphforge-resolved-v1.json" \
  >"$workspace/example/resolved.json"
"$TERRAFORM" -chdir="$workspace/example" plan \
  -refresh=false -input=false -lock=false -out="$temporary_directory/escaping-plan" >/dev/null
"$TERRAFORM" -chdir="$workspace/example" show -json "$temporary_directory/escaping-plan" \
  >"$temporary_directory/escaping-plan.json"
"$JQ" -e '
  ((.resource_changes // []) | length == 0) and
  ([.planned_values.root_module | .. | objects | .resources? // empty | .[]] | length == 0) and
  (.planned_values.outputs.deployment_spec.value.artifact.version == "0.5.1<>&\u2028\u2029") and
  (.planned_values.outputs.deployment_spec.value.resolved_config_sha256 == "37d0063465f0309553599f9f4e065cb02794c4fc019052bcc19903002a4013ff") and
  (.planned_values.outputs.deployment_spec_json.value | contains("\\u003c\\u003e\\u0026\\u2028\\u2029"))
' "$temporary_directory/escaping-plan.json" >/dev/null
"$JQ" -j '.planned_values.outputs.deployment_spec_json.value' \
  "$temporary_directory/escaping-plan.json" >"$temporary_directory/escaping-spec.json"
escaping_sha=$(/usr/bin/shasum -a 256 "$temporary_directory/escaping-spec.json" | /usr/bin/awk '{print $1}')
if [ "$escaping_sha" != "a309ea7b465c975ac2e049bf10768e819da6b6b50427e0ce986062c0a244b0b3" ]; then
  echo "canonical escaping deployment-spec bytes changed: $escaping_sha" >&2
  exit 1
fi

# Destroy owns no provider resources and cannot name repository state, data,
# namespaces, clusters, secrets, or services.
cp "$terraform_root/../../docs/contracts/examples/graphforge-resolved-v1.json" \
  "$workspace/example/resolved.json"
"$TERRAFORM" -chdir="$workspace/example" plan -destroy \
  -refresh=false -input=false -lock=false -out="$temporary_directory/destroy-plan"
"$TERRAFORM" -chdir="$workspace/example" show -json "$temporary_directory/destroy-plan" \
  >"$temporary_directory/destroy-plan.json"
"$JQ" -e '((.resource_changes // []) | length == 0)' \
  "$temporary_directory/destroy-plan.json" >/dev/null
"$JQ" -e '([.planned_values.root_module | .. | objects | .resources? // empty | .[]] | length == 0)' \
  "$temporary_directory/destroy-plan.json" >/dev/null
if grep -Eqi '\.graphforge/state|graphforge remove|https://example.invalid/graphforge/example.parquet|user:token' \
  "$temporary_directory/destroy-plan.json"; then
  echo "destroy plan claims an infrastructure or data resource" >&2
  exit 1
fi
"$TERRAFORM" -chdir="$workspace/example" apply -input=false -lock=false \
  -auto-approve "$temporary_directory/destroy-plan"
"$TERRAFORM" -chdir="$workspace/example" show -json >"$temporary_directory/destroy-state.json"
"$JQ" -e '([.values.root_module | .. | objects | .resources? // empty | .[]] | length == 0)' \
  "$temporary_directory/destroy-state.json" >/dev/null

echo "Terraform deployment-spec plan/apply/drift/destroy acceptance passed"
