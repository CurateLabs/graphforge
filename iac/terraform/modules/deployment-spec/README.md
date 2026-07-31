# GraphForge provider-neutral deployment specification

This resource-free module selects one target from canonical
`graphforge-resolved-config/1` JSON and renders deterministic
`graphforge-deployment-spec/1` JSON. Caller-owned Pulumi, Terraform, or another
IaC stack may consume that output to create infrastructure.

The module does not configure a provider or create a namespace, cluster,
service, workload, identity, secret, volume, or dataset. Consequently destroy
has no provider resource to remove and cannot invoke `graphforge remove` or
delete external data. The caller remains solely responsible for every resource
it creates from the specification.

```hcl
module "graphforge_deployment" {
  source = "../../modules/deployment-spec"

  resolved_json   = file("resolved.json")
  target          = "production"
  artifact_locator = "registry.example.com/graphforge/core@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
}
```

The resolved input is sensitive and is never returned. The canonical JSON output
ends with LF and contains only the selected target's bounded requirements and
source/secret IDs. Secret values,
source URIs, repository paths, graph state, and local data are never emitted.
OCI artifacts require a matching digest locator; mutable tags, local paths,
userinfo credentials, URL queries, and fragments fail before apply.

Canonical object keys use Terraform `jsonencode` ordering. For byte parity with
the Python and TypeScript renderers, the JSON string escapes `<`, `>`, `&`,
U+2028, and U+2029 as `\u003c`, `\u003e`, `\u0026`, `\u2028`, and
`\u2029`; arrays retain resolved-config order. The output has exactly one
trailing LF. Golden tests compare the complete bytes and derive their length
from the checked-in golden rather than freezing a separate size constant.
