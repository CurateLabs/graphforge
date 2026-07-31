# GraphForge Pulumi static validation

The TypeScript and Python packages in this directory expose the same local
`ComponentResource`: `graphforge:static:TargetValidation`.

The component accepts canonical `graphforge-resolved-config/1` JSON and a
target ID. It validates and projects the provider-neutral
`graphforge-infra-validation/1` receipt without creating child resources,
calling a provider, resolving secret values, or checking live connectivity or
readiness. The component registers an empty input bag so the resolved
configuration is not copied into Pulumi state. Its only registered output is
the secret-free validation receipt.

Source URIs with inline credentials are rejected. Portable numeric requirements
are capped at JSON's exact-integer limit (`9007199254740991`) so TypeScript,
Python, Rust, and Terraform validate the same bytes.

Both test suites consume
`docs/contracts/examples/graphforge-resolved-v1.json` directly.

Run real provider-free previews against isolated local stacks with:

```sh
iac/pulumi/tests/preview_acceptance.sh
```
