# Billion-live-edge certification

## Current native OVHC-AGENCY execution

The current #745 release outcome reuses the #900 native **OVHC-AGENCY** ladder
and its [host runbook](../../benchmarks/README.md).
Use the [streaming generator](../../benchmarks/runners/graph500-generator/README.md):
**Graph500-compliant generated input; GraphForge lifecycle measurements; no
Graph500 performance/TEPS claim.** SCALE26 emits exactly 1,073,741,824 raw
tuples, including self-loops and repeated endpoint pairs, and GraphForge stores
each as a distinct edge record. These are not unique undirected edges. The
generator never filters or replenishes tuples to reach a target-live count.

The native host runs through `local-linux-cgroups-v2`.
After the one-time native host preparation described in `benchmarks/README.md`,
run as the unprivileged user:

```bash
make -C benchmarks progressive-host-ladder-run MAXIMUM_SCALE=26 \
  WORK_ROOT="$HOME/graphforge-ladder" \
  OUTPUT_DIR="$HOME/graphforge-ladder/evidence" \
  RESERVED_HEADROOM_BYTES=80530636800
```

The command builds once and advances the existing sequential ladder, using
actual work-root free capacity, the declared reserve, and adjacent-rung measured
throughput. The first execution or projection failure stops the command. Preserve
its evidence and fix the underlying cause, including RSS/IO or recovery defects.
Successful rungs retain ordinary public lifecycle receipts and reclaim their
datasets before the next admission. Run `progressive-host-ladder-inventory`
with the same work/evidence arguments to record independent terminal cleanup.
`progressive-host-ladder-plan` previews only the next admissible rung, reads
existing binaries, and writes no immutable artifacts. Implementation closure
for #1087 does not claim the actual host outcomes retained by #900/#745.

## Historical cloud certification workflow

The following describes the earlier standalone cloud workflow. It is not a
prerequisite for the native host command above.


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

The deterministic 1x/2x/4x check retains raw, independently reopened node and
edge denominators. CAS read/write bytes keep the aggregate affine-growth check;
payload call counts keep the buffered-growth check. The CAS phase is also the
exact sum of payload installation, immutable manifest installation, and manifest
path reads. Fresh and reused objects have separate counts. File synchronization
includes actual cache-window rollovers; each completed temporary installation
attempt additionally has two namespace barriers. Early reuse performs only
authentication. A concurrent losing publisher retains its actual temporary
writes and barriers while reporting zero newly installed bytes and one reused
object. Opening a prior manifest records its actual read submissions.

Manifest control work is bounded by the native Patricia protocol, rather than
requiring every content-dependent path length to increase between rungs. This
qualification accepts exactly one publication from an empty manifest, with its
measured changed-path count equal to the canonical payload inventory `P`.
Repeated publications, prior entries, and additional tombstones cannot borrow
this bound. Each recursive branch consumes at least one of 64 digest nibbles;
a split installs at most three nodes and a collapse reads at most one extra
child per level. Thus install requests are at most `67P + 1` (including the
empty bootstrap root), and update reads at most `130P`.
The bootstrap and every changed path also require at least `P + 1` manifest
install-or-reuse requests and at least `P` existing-root reads. A coherent
omission of all metadata work therefore fails even if its reduced totals agree.

The authenticated encoding-inventory byte count `I` bounds the shared path,
digest and length values for all entries, including a hash-collision leaf.
Changing its artifact keys to manifest-entry keys adds 23 bytes; the longest
role member adds 20, for 43 bytes per entry. JSON escaping of the shared values
is identical in both encodings. The largest valid branch, with 16 digest
references and a 63-nibble compressed prefix, is 1319 bytes including its
newline; this also exceeds the leaf header. A node is therefore bounded by
`I + 43P + 1319` bytes. Native maximal-encoding tests check every depth, role,
escaped and Unicode paths, and zero/u64-maximum lengths. Manifest bytes are
bounded by this node ceiling times the request bounds, and nonempty reads by
the actual 64 KiB buffer limits. Coherent component overcounts fail even when
their aggregate totals agree. Metadata remains in the aggregate byte-growth
check and every synchronization total.

The controlled ladder varies nodes by adding isolated nodes while keeping its
edge set fixed. Its sparse CSR adjacency therefore bears the edge axis; this is
a fixture-specific rule, not a claim that arbitrary added nodes never change
adjacency storage. Hydration authenticates all objects but copies only the
private ordinal-V4 node maps; other immutable files are hardlinked. Hydration
write bytes consequently bear the node axis and remain positive and constant
on this fixture's edge axis. Read, submission, and barrier checks remain tied
to actual native work. Catalog object counts remain fixed, while decimal
lengths in their manifests can gain digits. Catalog bytes must remain positive
and below the existing normalized denominator ceiling; allocated bytes retain
the filesystem quantization checks. Source/import categories still reconcile
field-for-field with storage-owned authorities. Coherently changing those
ledgers to zero or excessive growth does not bypass the policy tests.

Completed construction retains authenticated shape/merge files for replay.
Their current allocated bytes are measured against the retained native file
inventory and checked with filesystem allocation quantization, alongside the
other retained outputs; this counter is not structurally zero. Intermediate
merge input removals subtract their actual allocations.

Ordered query admission recognizes the explicit `ordered_one_hop` and
`ordered_two_hop` leaf families alongside the existing Expand/sort paths. The
leaves report zero Arrow input batches and rows, actual candidate visits,
positive bounded identity reads, and their own RSS samples. Destination-degree
probes include empty rows and are bounded by the authoritative reopened node
count, independently of the output limit. For the fixture's unrestricted
directed one-hop query, actual result rows and emitted/candidate counts equal
`min(K, reopened live edges)`: a small graph cannot supply K distinct edge
matches. The provider ladder still requires its full K-row outcome. One-hop
emits exactly its counted candidates; two-hop retains the existing candidate-overhead ceiling. Missing
RSS, fake input work, omitted probes or reads, and materializing scans fail the
optimized-family regression checks.

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

Category authority follows one closed provenance chain. Storage derives the
per-category aggregate ledger and identity-membership roots while authenticating
native receipt identities. The ordinary GraphForge evidence preserves only
those aggregates, roots, denominators, and domain-separated commitments. The
provider result records SHA-256 of the exact ordinary evidence bytes in
`artifacts.graphforge_sha256`; trusted transport or an immutable manifest
supplies SHA-256 of that provider result independently of the evidence bundle.
`validate-g500-certification.py` first verifies this external provider-result
anchor and artifact binding, then validates the sanitized category proof. A
category map, commitment map, or authority context supplied beside the evidence
is never an independent trust anchor. Without the externally anchored provider
result, category certification fails closed.

Local developer runs validate the small lifecycle and hostile fixtures only;
they are not certification evidence and cannot close #745.
