output "deployment_spec" {
  description = "Provider-neutral graphforge-deployment-spec/1 object for caller-owned IaC resources."
  value       = local.deployment_spec

  precondition {
    condition     = try(local.decoded.contract, "") == "graphforge-resolved-config/1"
    error_message = "resolved_json must be graphforge-resolved-config/1."
  }
  precondition {
    condition     = length(local.selected_targets) == 1
    error_message = "resolved_json must contain exactly one target matching target."
  }
  precondition {
    condition     = local.target_contract_safe
    error_message = "selected target topology contains a value outside graphforge-resolved-config/1 bounds."
  }
  precondition {
    condition     = local.artifact_safe
    error_message = "target artifact must have a supported kind, bounded version, and lowercase SHA-256 pin."
  }
  precondition {
    condition     = local.locator_input_safe
    error_message = "artifact_locator must be a bounded credential-free remote locator, not a local path or URL with query/fragment data."
  }
  precondition {
    condition     = local.locator_safe
    error_message = "artifact_locator does not match the resolved artifact pin or is mutable; OCI locators require repository@sha256:<matching digest>."
  }
  precondition {
    condition     = local.stable_source_ids && local.stable_secret_ids
    error_message = "target source_ids and secret_ids must be bounded, unique stable IDs."
  }
  precondition {
    condition     = local.references_exist
    error_message = "target source_ids and secret_ids must reference declarations in resolved_json."
  }
  precondition {
    condition     = local.reference_contract_safe
    error_message = "resolved references must contain secret references only and bounded source URIs without inline credentials."
  }
}

output "deployment_spec_json" {
  description = "Canonical compact JSON plus LF for graphforge-deployment-spec/1."
  value       = "${jsonencode(local.deployment_spec)}\n"
}

output "artifact_sha256" {
  description = "Non-secret immutable artifact digest selected from the resolved target."
  value       = local.artifact_sha
}

output "target_id" {
  description = "Selected target ID."
  value       = var.target
}

output "infrastructure_ownership" {
  description = "Infrastructure remains owned by caller IaC state."
  value       = "caller_owned"
}

output "connectivity" {
  description = "No provider-neutral deployment specification performs connectivity checks."
  value       = "not_checked"
}

output "readiness" {
  description = "No provider-neutral deployment specification performs readiness checks."
  value       = "not_checked"
}

output "capability_compatibility" {
  description = "Capability requirements are declared but not probed."
  value       = "requirements_declared"
}
