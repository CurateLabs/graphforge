terraform {
  required_version = ">= 1.8.0"

  required_providers {
    graphforge = {
      source  = "curatelabs/graphforge"
      version = "~> 0.5"
    }
  }
}

module "graphforge_validation" {
  source = "../../modules/static-validation"

  resolved_json = file("${path.module}/resolved.json")
  target        = "production"
}

output "static_validity" {
  value = module.graphforge_validation.static_validity
}

output "planned_infrastructure" {
  value = module.graphforge_validation.planned_infrastructure
}

output "connectivity" {
  value = module.graphforge_validation.connectivity
}

output "readiness" {
  value = module.graphforge_validation.readiness
}

output "capability_compatibility" {
  value = module.graphforge_validation.capability_compatibility
}
