output "contract" {
  description = "Frozen static validation receipt contract."
  value       = data.graphforge_infra_validation.this.contract
}

output "resolved_config_sha256" {
  description = "Digest of canonical resolved configuration; the configuration itself remains sensitive."
  value       = data.graphforge_infra_validation.this.resolved_config_sha256
}

output "selected_target_json" {
  description = "Selected target requirements and references, excluding secret values and data."
  value       = data.graphforge_infra_validation.this.selected_target_json
}

output "static_validity" {
  description = "Static schema and semantic validation state."
  value       = data.graphforge_infra_validation.this.static_validity
}

output "planned_infrastructure" {
  description = "Provider-neutral infrastructure intent state; no resources are provisioned."
  value       = data.graphforge_infra_validation.this.planned_infrastructure
}

output "connectivity" {
  description = "Live connectivity state, which static validation does not check."
  value       = data.graphforge_infra_validation.this.connectivity
}

output "readiness" {
  description = "Live readiness state, which static validation does not check."
  value       = data.graphforge_infra_validation.this.readiness
}

output "capability_compatibility" {
  description = "Declared capability-requirement state, distinct from live compatibility."
  value       = data.graphforge_infra_validation.this.capability_compatibility
}

output "validation_json" {
  description = "Canonical graphforge-infra-validation/1 receipt."
  value       = data.graphforge_infra_validation.this.validation_json
}
