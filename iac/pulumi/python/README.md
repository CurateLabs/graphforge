# `graphforge-pulumi-static`

```python
from graphforge_pulumi import TargetValidation

validation = TargetValidation(
    "production",
    resolved_config=resolved_config,
    target_id="production",
)
pulumi.export("receipt", validation.receipt)
```

This local component performs deterministic static validation only. It creates
no provider resources and does not perform network, provisioning, mutation,
connectivity, or readiness operations.

## Portable deployment specification

```python
from graphforge_pulumi import DeploymentSpec

deployment = DeploymentSpec(
    "production",
    resolved_config=resolved_config,
    target_id="production",
    artifact_locator=(
        "registry.example/graphforge/runtime@sha256:"
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    ),
)
pulumi.export("deployment_spec", deployment.spec)
```

`DeploymentSpec` renders canonical `graphforge-deployment-spec/1` JSON for
caller-owned infrastructure. It supports the configured `python_wheel`,
`node_package`, `native_binary`, and digest-pinned `oci_image` artifact kinds.
Non-OCI locators must be credential-free HTTPS URLs; OCI locators must contain
the configured SHA-256 digest and no mutable tag.

The component registers no provider resources. It does not create a service,
transport, Kubernetes object, VM, secret, network, or storage resource; read
local GraphForge state or repository data; or claim runtime readiness. The full
resolved configuration is deliberately omitted from component inputs and IaC
state. The output contains only the selected closed target projection, bounded
source/secret IDs, artifact integrity, and explicit caller-ownership status.
