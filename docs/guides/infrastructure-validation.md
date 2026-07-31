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
apply. Remote provisioning belongs to #227 and remains blocked on #215's
checksum-pinned runtime artifact, transport, health, authentication, and
lifecycle contracts.

IaC state never owns repository definitions or local GraphForge state. Destroy
must remove only resources recorded by the IaC engine; it must never call
`graphforge remove`, upload `.graphforge/state/`, or delete external datasets.

The ontology paths in resolved configuration are bounded repository references
only. Infrastructure validation never inspects a runtime catalog or suggests,
validates, exports, loads, adopts, or clears an ontology. It cannot change the
committed workspace ontology authority delivered by #236 and #237, and it never
contains ontology documents, observed property values, runtime catalog IDs, or
graph data.
