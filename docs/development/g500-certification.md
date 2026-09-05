# Billion-live-edge certification

The current #745 release outcome reuses the #900 native **OVHC-AGENCY** ladder
and its [host runbook](../../benchmarks/README.md).
Use the [streaming generator](../../benchmarks/runners/graph500-generator/README.md):
**Graph500-compliant generated input; GraphForge lifecycle measurements; no
Graph500 performance/TEPS claim.** SCALE26 emits exactly 1,073,741,824 raw
tuples, including self-loops and repeated endpoint pairs, and GraphForge stores
each as a distinct edge record. These are not unique undirected edges. The
generator never filters or replenishes tuples to reach a target-live count.

## Historical cloud certification workflow

The cloud workflow and target-live client below are retained for historical
reference. They do not authorize or define the current native host run.

Issue #745 certifies the complete persisted GraphForge lifecycle on an explicitly
provisioned Linux evidence host. The workflow is manual, protected by the
`scale-certification` GitHub environment, and never provisions infrastructure.
A maintainer must approve the provider, exact SKU, ephemeral runner label, cost,
and teardown before dispatch.

The approved SUT record is immutable for a run. Separate evidence fields record
provider, region, SKU, and the full Linux image identity with resolved version
(never a moving `latest` alias); observed fields record OS, kernel, filesystem,
memory, and local-NVMe capacity. The private provisioning record also retains
vCPU/NVMe inventory, runner version, and pinned GraphForge toolchain versions.
Neither record adds absolute host paths or mutable cloud resource identifiers.

## No-spend readiness gate

All checks before explicit cost approval are read-only: confirm the authenticated
subscription, role, provider-registration state, regional SKU restrictions,
regional vCPU quota, current on-demand price, GitHub environment policy, and
unique runner-label availability. Do not register a provider, request quota,
create resources, generate a runner registration token, or dispatch the workflow
during this gate.

The approval record names the exact provider/region/SKU/image version,
on-demand price estimate, hard total spend ceiling, five-hour infrastructure
TTL, teardown owner, and permission to register required providers and destroy
the exact ephemeral resources. Spot/low-priority capacity is not valid: an
uncontrolled eviction cannot be distinguished from the required cancellation
and interrupted-finalization drills.

The historical client profile fixes SCALE 26, seed 1, the R-MAT initiator,
undirected canonicalization, and a **target-live** stopping rule. Deterministic
attempt windows are externally sorted and merged until a complete merge proves
at least one billion unique canonical live edges. Raw attempts are never called
live edges, and evidence must reconcile attempts, self-loops, duplicates, and
unique edges.

## Dispatch and abort policy

1. Freeze the exact commit after normal PR CI is green.
2. After approval, provision a fresh Linux host with more than 128 GiB
   advertised RAM (so guest `MemTotal` still clears 128 GiB) and at least 1 TiB
   local NVMe. Format one local NVMe filesystem as XFS and mount it for the
   runner's temp and work roots. Durable managed data disks are not substitutes.
3. Configure the runner service so process `TMPDIR` and GitHub `RUNNER_TEMP`
   resolve to directories on that same filesystem. Before registration, prove
   resolved filesystem type, device identity, capacity, and a write/fsync probe;
   retain paths only in the private provisioning log, never evidence.
4. Register a repository-scoped ephemeral runner with one unique label. Do not
   reuse a runner work directory, label, or cloud disk from another attempt.
5. Configure required reviewers on the `scale-certification` environment.
6. Dispatch `Billion-edge certification` with the exact 40-character SHA,
   runner label, provider, region, SKU, and exact OS image identity. Branch
   names, moving refs, and moving image aliases are rejected.
7. Do not retry an unchanged failing tree. Preserve the partial phase journal,
   identify the first failed phase, repair the root cause, and certify a new SHA.
8. After both sanitized artifacts upload and validate, deregister the runner and
   destroy the exact VM, OS disk, NIC, public IP, and resource group created for
   the run. The independent five-hour TTL is a backstop, not a substitute for
   immediate teardown. Never retain
   the project, portable bundle, credentials, registry tokens, or spill files.

The workflow records one start timestamp before either test command. The bounded
preflight and full SCALE26 lifecycle share the 14,400-second product envelope;
the Rust watchdog includes time already spent in preflight. The job has a
270-minute outer timeout, leaving 30 minutes after the product fail-safe for
validation and artifact upload. Provisioning happens before dispatch but remains
inside the separately approved five-hour billed-resource TTL.

An in-process watchdog samples Linux
resident memory every 250 ms and allocated workspace bytes every five seconds,
sets the shared cooperative-cancellation token on the first RSS, disk, or time
breach, and records a typed failure at the affected phase. The Rust journal is
atomically replaced after each phase so cancellation or host loss retains the
last complete bounded observation. The validator rejects capacity or runtime
breaches.

## Required phases and evidence

The same public Rust-facade run performs preflight, target-live generation,
bounded ingest, CSR construction, source reopen and one/two-hop observations,
portable-v2 bundle export, full verification, atomic import, imported reopen and
matching observations, plus representative corruption, cancellation, resource
limit, and interrupted-finalization drills. Source generation, semantic package,
transport, and imported generation identities remain distinct and reconciled.

Negative drills use representative bounded data, not a second billion-edge
payload. Each records its own elapsed/RSS/disk observation and typed outcome:
corruption is rejected before import mutation; cancellation leaves no published
partial generation; a resource limit stops at the first breach; and interrupted
finalization recovers only the last acknowledged durable generation. No partial
candidate may be addressable, and the last complete atomic journal entry remains
valid.

Bounded ingest uses one public `GraphConstructionSession`: its opaque session
UUID is durably recorded before the first node chunk, recovery resumes that
exact session, nodes precede edges, and `seal_and_publish` is invoked once after
all chunks. Phase evidence records RSS, elapsed time, disk bytes, immutable
shards, accepted rows/batches, writes, and authentication reads. Adjacent
1x/2x/4x observations must show bounded resident windows and topology work that
scales with rows; continued material RSS growth with edge count is a failure.

Fingerprint reconciliation is explicit: source and imported durable generation
IDs are distinct; semantic package and transport digests are distinct;
ontology/capability authority fingerprints match; canonical source/imported
project fingerprints match; and source/imported 1-hop and 2-hop query
fingerprints match. Integrity and compatibility verification occurs before any
clean-import mutation.

Only the sanitized JSON evidence and phase journal may be downloaded and checked
in. The schema and semantic validator reject missing phases, count or authority
mismatches, query-fingerprint drift, identity collapse, unsafe paths, sensitive
fields, insufficient hosts, and envelope overruns. Project payloads, bundles,
spills, secrets, credentials, absolute host paths, and mutable runner state are
not evidence and must never be committed.

Local developer runs validate the small lifecycle and hostile fixtures only;
they are not certification evidence and cannot close #745.
