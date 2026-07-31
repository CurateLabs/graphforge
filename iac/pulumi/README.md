# GraphForge Pulumi portability components

The TypeScript and Python packages in this directory expose the same two local,
provider-free component resources:

- `graphforge:static:TargetValidation` validates one resolved target;
- `graphforge:deployment:DeploymentSpec` renders one portable
  `graphforge-deployment-spec/1` projection for caller-owned IaC.

The component accepts canonical `graphforge-resolved-config/1` JSON and a
target ID. It validates and projects the provider-neutral
`graphforge-infra-validation/1` receipt without creating child resources,
calling a provider, resolving secret values, or checking live connectivity or
readiness. The component registers an empty input bag so the resolved
configuration is not copied into Pulumi state. Its only registered output is
the secret-free validation receipt.

`DeploymentSpec` additionally accepts a credential-free immutable artifact
locator. It projects only the selected target's artifact pin, topology and core
requirements, source/secret reference IDs, and explicit ownership/status
boundaries. It creates no provider or child resources and does not build a
service, inspect runtime health, or read `.graphforge/state/`.

Source URIs with inline credentials are rejected. Portable numeric requirements
are capped at JSON's exact-integer limit (`9007199254740991`) so TypeScript,
Python, Rust, and Terraform validate the same bytes.

Both test suites consume `docs/contracts/examples/graphforge-resolved-v1.json`
and compare deployment output with
`docs/contracts/examples/graphforge-deployment-spec-production-v1.json`.

Run real provider-free previews against isolated local stacks with:

```sh
iac/pulumi/tests/preview_acceptance.sh
```
