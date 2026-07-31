module "graphforge_deployment" {
  source = "../../modules/deployment-spec"

  resolved_json    = file("${path.module}/resolved.json")
  target           = var.target
  artifact_locator = var.artifact_locator
}

variable "artifact_locator" {
  type    = string
  default = "registry.example.com/graphforge/core@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
}

variable "target" {
  type    = string
  default = "production"
}

output "deployment_spec" {
  value = module.graphforge_deployment.deployment_spec
}

output "deployment_spec_json" {
  value = module.graphforge_deployment.deployment_spec_json
}

output "artifact_sha256" {
  value = module.graphforge_deployment.artifact_sha256
}

output "infrastructure_ownership" {
  value = module.graphforge_deployment.infrastructure_ownership
}

output "connectivity" {
  value = module.graphforge_deployment.connectivity
}

output "readiness" {
  value = module.graphforge_deployment.readiness
}

output "capability_compatibility" {
  value = module.graphforge_deployment.capability_compatibility
}
