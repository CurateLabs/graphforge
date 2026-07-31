# Validate infrastructure intent

GraphForge can validate a named deployment target before Pulumi or Terraform
selects any cloud provider:

```bash
gf --project-dir . config validate
gf --project-dir . config resolve --json
gf --project-dir . infra validate --target production --json
```

The commands read only `.graphforge/graphforge.yaml`. They do not open
`.graphforge/state/`, read ontology or schema documents, resolve secrets,
inspect source payloads, contact a service, or provision infrastructure.

Targets distinguish embedded, local, and separately owned external deployment
ownership. Their closed intent includes a service, worker, job,
or host role; pinned artifact version and SHA-256; process/container/host
topology; long-running or on-demand scheduling; finite replicas and resources;
storage, network, health, observability, backup requirements; digest-addressed
source references; secret IDs; and versioned capability requirements.
Source URIs cannot contain inline credentials, and portable numeric
requirements cannot exceed JSON's exact-integer limit (`9007199254740991`).

The `graphforge-infra-validation/1` receipt deliberately separates:

- `static_validity.status = valid`: the closed configuration and semantic
  combinations are valid;
- `planned_infrastructure.status = validated`: provider-neutral requirements
  form a valid plan, with `mutation = none`;
- `connectivity.status = not_checked`: no transport was contacted;
- `readiness.status = not_checked`: no live health claim was made;
- `capability_compatibility.status = requirements_declared`: requirements are
  known, but a deployed runtime has not proved compatibility.

Pulumi TypeScript and Python components and the Terraform GraphForge validation
data source/module consume the same canonical resolved JSON during
preview/plan. Their outputs contain references and checksums, never secret
values or graph/source data. A successful static result does not authorize
apply.

The portable deployment components consume a selected target and a
caller-supplied artifact locator. They render canonical
`graphforge-deployment-spec/1` JSON containing the configured artifact kind,
version, and SHA-256; execution and scheduling intent; storage, resource,
network, health, observability, and backup requirements; bounded source and
secret IDs; and declared capabilities. Locators with inline credentials fail
closed, and OCI locators must be digest-pinned rather than mutable tags.

The specification supports the core artifact kinds and topology roles already
present in resolved configuration. It does not translate them into Kubernetes,
a cloud resource, a VM, a system service, or a transport. Caller-owned Pulumi or
Terraform code chooses and owns those resources. This keeps the basics of
GraphForge core portable without turning the IaC packages into a service build.

Rendering reports infrastructure as `caller_owned`, connectivity and runtime
readiness as `not_checked`, and capability compatibility as
`requirements_declared`. Preview, plan, apply, drift, and destroy therefore
change only the component or module projection in IaC state. They do not rewrite
GraphForge generations, upload repository data, create provider resources, or
claim that a caller runtime is healthy.

IaC state never owns repository definitions or local GraphForge state. Destroy
must remove only resources recorded by the IaC engine; it must never call
`graphforge remove`, upload `.graphforge/state/`, or delete external datasets.

The ontology paths in resolved configuration are bounded repository references
only. Infrastructure validation never inspects a runtime catalog or suggests,
validates, exports, loads, adopts, or clears an ontology. It cannot invoke the
#236 Rust inspection/suggestion/validation/export surface or the #237 thin
binding parity and durable adopt/clear surface. It never contains ontology
documents, observed property values, runtime catalog IDs, or graph data.
