# `@graphforge/pulumi-static`

```ts
import { TargetValidation } from "@graphforge/pulumi-static";
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
