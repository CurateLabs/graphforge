# `@curatelabs/graphforge-pulumi-static`

```ts
import { TargetValidation } from "@curatelabs/graphforge-pulumi-static";
import resolvedConfig from "./graphforge-resolved.json";

const validation = new TargetValidation("production", {
  resolvedConfig,
  targetId: "production",
});

export const receipt = validation.receipt;
```

This local component performs deterministic static validation only. It creates
no provider resources and does not perform network, provisioning, mutation,
connectivity, or readiness operations.

## Portable deployment specification

Render a provider- and runtime-neutral projection for caller-owned IaC:

```ts
import { DeploymentSpec } from "@curatelabs/graphforge-pulumi-static";
import resolvedConfig from "./graphforge-resolved.json";

const deployment = new DeploymentSpec("production", {
  resolvedConfig,
  targetId: "production",
  artifactLocator:
    "registry.example/graphforge/runtime@sha256:" +
    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
});

export const deploymentSpec = deployment.spec;
export const deploymentSpecJson = deployment.canonicalJson;
```

`renderDeploymentSpec` provides the same `graphforge-deployment-spec/1`
document as a pure function; `renderDeploymentSpecJson` emits canonical JSON
with one trailing newline. The projection retains the selected artifact pin,
topology and bounded requirement/reference IDs. It does not include source
payloads, secret values, local `.graphforge/state/`, or the complete resolved
configuration.

OCI image locators must use `registry/repository@sha256:<configured digest>`;
mutable tags and mismatched digests fail closed. Wheel, npm package, and native
binary locators must be credential-free HTTPS URLs without query strings or
fragments. Local filesystem paths and whitespace/control characters are
rejected.

`DeploymentSpec` registers no child or provider resources. Its outputs report
infrastructure and runtime ownership as caller-owned, infrastructure mutation
as `none`, connectivity/readiness as `not_checked`, and capabilities as
`requirements_declared`. Consuming IaC is responsible for provisioning,
rollout, rollback, health checks, and deletion of only the resources it owns.
