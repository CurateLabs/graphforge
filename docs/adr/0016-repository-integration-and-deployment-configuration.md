# ADR 0016: Repository integration and deployment configuration boundary

**Status:** Accepted

**Date:** 2026-07-30

**Build target:** post-v0.5.0

**Contracts:** [`graphforge-project-config/1`](../contracts/graphforge-project-config-v1.schema.json), [`graphforge-resolved-config/1`](../contracts/graphforge-resolved-config-v1.schema.json), [`graphforge-infra-validation/1`](../contracts/graphforge-infra-validation-v1.schema.json), [`graphforge-deployment-spec/1`](../contracts/graphforge-deployment-spec-v1.schema.json)

**Related:** ADR 0013 (project generations), ADR 0014 (workspace checkpoints), ADR 0015 (embedded write modes), issues #219, #225, and #227

## Context

GraphForge has a durable embedded project format, native bindings, a Rust CLI,
and agent workflows, but a code repository has no canonical way to declare its
GraphForge inputs, keep generated graph data out of Git, or present one validated
deployment intent to local tools and infrastructure-as-code systems.

The contract must keep configuration, ontology, schemas, migrations, and
reproducibility recipes reviewable while excluding graph contents, imported
datasets, materialized seeds, snapshots, exports, locks, journals, and caches.
Rust must remain the behavioral authority; Python and Node must stay thin and
equivalent. Pulumi and Terraform need deterministic preview/plan input without
credentials, network access, or provisioning. Remote authorities remain peer
extensions, not a server added to core. Python-only skill installation must not
require Node.

ADR 0013 makes a validated generation selected by `CURRENT` the sole project
authority. ADR 0014 keeps root configuration outside checkpoints unless adopted
into canonical workspace participants. ADR 0015 keeps concurrency embedded and
transport-neutral. Repository integration must not create another persistence
or coordination authority.

## Options considered

1. **One `.graphforge/` namespace with selectively ignored data directories.**
   Discoverable and cohesive, but cleanup and ignore management must protect
   tracked siblings.
2. **Separate `.graphforge/` runtime and `graphforge/` definitions.** The split
   is physical, but the near-identical names are confusing to people and tools.
3. **Entirely ignored `.graphforge/` plus scattered root configuration.** Simple
   ignore behavior, but no coherent reviewed home for related definitions.
4. **Commit or deploy the live project directory.** Git cannot safely merge
   immutable generations, atomic pointers, locks, and binary participants; IaC
   would confuse deployment intent with mutable user data.
5. **Ecosystem-specific contracts with Python delegating skills to NPX.** Less
   initial packaging work, but it creates semantic drift, requires Node for
   Python users, and weakens offline operation.

## Decision

### One repository namespace

```text
.graphforge/
├── graphforge.yaml
├── ontology/
├── schemas/
├── seeds/
├── migrations/
├── imports/
├── exports/
└── state/
```

`graphforge.yaml`, ontology sources, schemas, migration definitions, and seed
recipes/manifests are tracked. Seed manifests may contain stable identities,
external locations, digests, mappings, and generator parameters, never rows.

`state/`, `imports/`, and `exports/` are ignored. Actual graph contents, source
datasets, materialized seeds, Arrow/Parquet/database files, generations,
snapshots, archives, locks, journals, caches, and trash never belong in code
Git. `init` owns an idempotent ignore block for exactly those three directories;
it never stages, untracks, commits, or ignores all of `.graphforge/`.

The live embedded project is `.graphforge/state/` and retains ADR 0013
semantics: `CURRENT`, not Git or YAML, selects authoritative data. Every
worktree gets its own default state. Explicit external roots must pass the same
containment, symlink, and format validation as repository-local roots.

### Closed portable configuration

`.graphforge/graphforge.yaml` implements the closed
`graphforge-project-config/1` schema. Unknown fields, unsupported versions,
unsafe paths, inline secrets, and unbounded collections fail before mutation.
It describes definition paths, digest-addressed external sources, named target
intent, pinned artifacts, write mode, storage, finite resources, networking,
health, observability, backup, and secret references—never secret values.

Provider-specific accounts, regions, registries, clusters, networks, identities,
and secret managers are Pulumi/Terraform inputs, not an open configuration map.

Resolution emits canonical UTF-8 JSON plus LF conforming to
`graphforge-resolved-config/1`: keys sorted lexicographically, explicit defaults,
repository-relative `/` paths, sources and targets ordered by ID, and secret
references without resolving values. Static validity, infrastructure plan,
connectivity, health, and capability compatibility remain distinct states.

`graphforge-infra-validation/1` selects one resolved target and records only
provider-neutral intent. A successful static result says `valid` and
`validated`; connectivity and readiness remain `not_checked`, while capability
compatibility remains `requirements_declared` until a deployed runtime reports
its contract. Validation performs no provisioning, provider lookup, network
request, project-state open, ontology load, or secret resolution.

### Ownership boundaries

Rust owns repository discovery, validation/resolution, path safety, ignore-file
editing, project lifecycle, portable interchange, checkpoint revert, and
destructive-operation guards. The Rust `gf` CLI is the reference behavior;
Python and Node expose thin `uvx graphforge` and `npx @curatelabs/graphforge-cli` surfaces.
Portable interchange packages one complete project generation; it does not
redefine the runtime-catalog inspection, ontology suggestion and non-mutating
validation, or explicit ontology-document export contract delivered by #236,
nor the thin binding parity and durable adopt/clear behavior delivered by #237.
Repository and IaC surfaces consume only bounded ontology references and
digests where needed; they do not infer or change ontology authority.

Skills have one checked-in source. Python and npm ship parity-checked copies and
install directly under `.agents/skills/`; Python never shells out to NPX.

GraphForge statically validates intent; a deployed runtime reports connectivity
and application readiness separately. Pulumi and Terraform own provider
configuration, preview/plan, provisioning, infrastructure readiness, drift,
rollout, rollback, and teardown. IaC state is not project state. Destroy removes
only IaC-owned resources and never invokes local project removal.

IaC consumes an immutable caller-supplied artifact declared by version and
digest; it does not build, publish, or own the runtime, and core gains no server,
transport, authentication system, or distributed deployment authority.
Provider configuration, compute, process/container orchestration, credentials,
secret values, service identities, networks, DNS, certificates, and storage
resources remain caller-owned IaC inputs or resources. Local state is never
uploaded implicitly, and data initialization is a separately authorized
external digest-addressed import. Issue #215 may provide one optional runtime,
but neither #227 nor this decision depends on it.

The first-party Pulumi components and Terraform modules materialize no provider
resources. Static validation and the portable deployment specification are
deterministic projections of the same resolved JSON. Caller-owned IaC consumes
the specification to configure its chosen local or remote process, container,
service, worker, job, or host. Rendering the specification never implies
infrastructure application, runtime connectivity, health, or capability
compatibility.

## Consequences

### Positive

- One discoverable repository convention keeps actual data out of Git.
- Rust, bindings, agents, Pulumi, and Terraform share one versioned contract.
- Preview/plan can fail closed without credentials, network access, or mutation.
- Python-only users can install skills without Node; core stays embedded.

### Negative

- Selective ignores require more care than ignoring `.graphforge/` wholesale.
- Python and npm must ship parity-checked generated schema and skill assets.
- Provider settings remain separate, so deployment needs IaC stack config.
- Deployment requires a caller-supplied immutable artifact and caller-owned IaC
  resources, so GraphForge cannot certify the infrastructure or runtime.

### Compatibility and follow-up

Existing v0.5 project roots remain valid and are not migrated automatically.
Using `.graphforge/state/` is additive; raw historical directories are not
portable imports. Schema changes are explicit and fail closed while pre-v1.

Issue #219 remains the close gate. Its native sub-issues implement lifecycle,
interchange, binding CLIs, skills, static IaC validation, and a runtime-agnostic
portable deployment specification. Direct contract and clean-environment
evidence is required before the canonical tracker closes.
