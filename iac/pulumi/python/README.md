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
