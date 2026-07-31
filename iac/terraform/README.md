# GraphForge Terraform static validation

This subtree provides a first-party, offline Terraform surface for
`graphforge-infra-validation/1`.

- The `graphforge_infra_validation` data source accepts canonical
  `graphforge-resolved-config/1` JSON and one named target.
- The `modules/static-validation` module exposes static validity, planned
  infrastructure, connectivity, readiness, and capability compatibility as
  distinct outputs.
- The provider defines no resources and has no provider configuration,
  credentials, clients, network operations, provisioning, or destroy behavior.
- `resolved_json` is sensitive. Validation results include only the selected
  target's requirements and secret identifiers; secret values and graph/source
  data are never accepted or emitted.
- Source URIs containing inline credentials are rejected. Storage capacity,
  CPU, and memory values cannot exceed JSON's maximum safe integer
  (`9007199254740991`), preserving cross-language parity.

The canonical input should come from:

```console
graphforge config resolve --json > resolved.json
```

Then use the module during plan:

```hcl
module "graphforge_validation" {
  source = "./iac/terraform/modules/static-validation"

  resolved_json = file("resolved.json")
  target        = "production"
}
```

A successful plan reports:

- `static_validity = "valid"`
- `planned_infrastructure = "validated"` with receipt mutation `none`
- `connectivity = "not_checked"`
- `readiness = "not_checked"`
- `capability_compatibility = "requirements_declared"`

These values do not claim that a service is reachable, ready, or compatible.
Remote provisioning remains separately owned and is outside this provider.
