# Move projects with portable projects

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

Portable projects are GraphForge's primary workflow for moving a project
between workspaces, machines, and registries. It packages one immutable project
generation with a canonical semantic identity. Use it for local copies,
air-gapped transfer, selective sharing, and OCI promotion.

> **Never copy, archive, synchronize, or publish a live `.graphforge/`
> directory.** It contains generation pointers, journals, locks, leases, and
> other runtime state that are not an interchange contract. Do not commit graph
> or source data, `.gfpb` bundles, `.gfproject/` packages, imports, exports, or
> materialized project state to Git. Keep those paths ignored and move them with
> portable instead.

The Rust engine owns selection, packaging, verification, import, and OCI
behavior. Python, Node, and the CLI are thin projections of the same operations
and return the same package identity, selection receipt, state classification,
progress, and typed failures.

## Copy the graph you just saved

After [Query, analyze, and save a graph](tutorial.md), transfer its three-paper
graph into a separate project. Finish the tutorial's `forge.close()` first, and
run these commands from the directory containing `citation-project`. Use the
matching `graphforge` CLI from [Installation](installation.md). The destination
must use [supported durable storage](installation.md#durable-storage).

Export the source and fully verify the resulting bundle:

```bash
graphforge --project citation-project export --current --profile complete \
  --format bundle --output ./citation-transfer.gfpb
graphforge portable verify --input ./citation-transfer.gfpb --mode full
```

The verification result must report `integrity=Verified` and
`compatibility=Supported`. The export and verification report the same
`package_digest`; its value depends on your project. The bundle is outside the
source project. To transfer between machines, move this bundle and verify it on
the receiving machine before importing.

Create a separate empty destination and import the complete package:

```bash
mkdir citation-copy
graphforge --project citation-copy import --input ./citation-transfer.gfpb \
  --idempotency-key 018f0f4e-7f4d-7c24-8f8f-8cbab5f47001
```

`--project` selects the source for export and the destination for import. It
does not select a code repository. If `citation-copy` already exists, choose a
different empty destination; do not import over your original project. The
example supplies a UUIDv7 operation key. Keep the same key and exact input when
retrying this import; use a fresh key for a different import operation.

Now open the destination in a new Python session from that same parent
directory and ask the tutorial's question:

```python
from graphforge import GraphForge

forge = GraphForge("citation-copy")
query = """
    MATCH (source:Paper {title: $title})-[:CITES]->(paper:Paper)
    RETURN paper.title AS title
    ORDER BY title
"""
print(forge.execute(query, {"title": "Survey"}).to_pylist())
forge.close()
```

Expected output:

```text
[{'title': 'Methods'}, {'title': 'Replication'}]
```

The source remains available at `citation-project`; the imported copy is a
separate durable project. You have completed the transfer. Continue below only
when you need selective packages, collaboration, or registry transport.

## Choose where to collaborate

Public projects support transparent participation with attributable authors and
committers, following Git practices. Private projects provide a non-public
space for known collaborators. The Hub or hosting application enforces that
access; Project metadata and the portable formats carry no access fields.
Published credit is public and permanent: there is no anonymity and no
redaction option.

For ordinary graph use, you do not need to publish anything. Start with
[saving and reopening a local graph](tutorial.md). When sharing is useful,
choose the intended Project and selected content, then use the supported
transfer path below. See [research workspaces](research-journey.md) for review
and contribution concepts.

## Keep the five identities separate

A portable multi-ontology workflow has five intentionally different identity
domains. Do not copy a value from one domain into another:

| Identity                 | What it names                                                             | Stable across expanded and bundle forms?   |
| ------------------------ | ------------------------------------------------------------------------- | ------------------------------------------ |
| module identity          | one authored ontology document: URI, opaque version, and canonical digest | yes                                        |
| composition identity     | the exact ordered module, bridge, and activation authority                | yes                                        |
| package identity         | the selected logical portable content                                     | yes                                        |
| runtime catalog identity | a project-local exploratory observation                                   | no; it is not portable authority           |
| evidence identity        | a TCK report, validation transcript, or benchmark attached as provenance  | only as ordinary selected evidence content |

The project generation pins authority at a publication boundary, but it is not
an ontology, composition, or package identity. Likewise, a package digest does
not authorize its ontology modules. Verification establishes package integrity
and compatibility; adoption is a separate explicit authority change.

## Choose the package you intend to share

Preview before exporting. A preview is content-free: it reports the package
class, dependency closure, omissions, redactions, estimated size, and selection
fingerprint without copying payload bytes.

Built-in profiles cover:

- `complete`, for a clean project round trip;
- `ontology-only`, for ontology plus its required bounded metadata;
- component-selective data, artifacts, or non-secret settings; and
- deterministic graph/data subsets with an explicit closure and redaction
  policy.

An ambiguous selector or missing dependency fails. GraphForge never widens a
selection silently. Selective packages do not imply graph merge or ontology
adoption; they require an explicit class-specific consumer.

Native saved-query definitions travel with `complete` exports and Project
`gf publish`/`gf clone`. They are the bounded `workspace/saved_queries` metadata
family, explicitly classified as a `settings` component. The settings profile
also selects them; a custom profile can include or omit that family explicitly.
Graph/data subsets exclude saved definitions rather than silently publishing
analyses that might describe unselected data.

Core validates included definitions and parameter declarations without executing
or binding them to a graph. Imported queries run only when requested, against
the recipient's chosen current or retained Version context. Runtime parameter
values and prior result tables do not enter the package. Treat literal values in
query text as shared content. See [saved queries](cypher-guide.md#saved-queries)
for lifecycle APIs and CLI examples.

Research interchange uses `GraphForge.research_reference`, `export_research`, and
`fork_research`. Python and Node expose the same native JSON contracts; CLI
commands are `research interchange reference|export|fork --file request.json`.
A reference explicitly selects a current Branch or an immutable Version. Export
selects a retained Version and optionally a frozen Slice plus explicit native
fields and a fresh projection Version UUID. A subset/redaction cites its source
Version but has its own immutable identity.

The authenticated `research@1` component carries selected Version commitments,
required evidence/ontology/baseline closure, genealogy, and selected acceptance
proofs. Export physically compacts selected property values; removed values and
unselected ancestor payloads are excluded. Imported lineage is historical:
import does not create live Branch heads or accept a contribution locally.
Historical expansion beyond retained content returns unavailable. Full native
verification and import reject unsupported readers and incomplete closure before
destination admission. There is no pre-v1 migration promise. A Version's size is
bounded by the portable limits and the 256 MiB participant bound, not by its
field count: native verification streams fields against the registry instead of
building a whole-graph field map.

Fork requires an independent Project UUID, author, governance declaration,
complete workspace metadata, and explicit adoption of
the selected ontology. This metadata does not enforce remote access. A Fork
records its origin while keeping destination metadata and subsequent history
independent. Its operation UUID supports exact replay after source Version
release and subsequent destination edits; a changed request is rejected.
See [research workspace semantics](../book/architecture/research-workspaces.md).

**Verifying a research package.** Full native verification of a research
package reads it once, in canonical order: the registry first, then each
Version's retained objects together under `research-retained/<version>/`. The
scan stages one Version, checks its closure against the registry, runs the
native domain and commitment checks on it, and removes it before it reads the
next one, so temporary disk is bounded by one Version and not by the package.
A bundle file, an expanded directory and a stream run the same code and give
the same verdict; only the report's `source` differs. Memory is bounded by the
scan's own limits and the owners' existing bounds, one Version at a time.

`max_research_version_bytes` bounds the retained objects of one Version that
verification stages: the participants, graph files and manifest nodes under
`research-retained/`, counted from the entry sizes before any byte is written.
The default is no limit, so verification accepts every package it accepted
before; disk is then bounded by the largest Version rather than by the whole
package. An operator who sets a limit bounds the scratch space as well: a
Version over it is refused with `LimitExceeded`, from a file and from a stream
alike. Objects that only local evidence needs are carried under
`research-retained-evidence/`; they are authenticated as they stream (their
digest must equal their path and their length the registry's) and are never
staged or decoded, so they do not count and need no disk. An evidence blob that
is also a participant, graph file or manifest node of the Version is one object
under `research-retained/` and is staged and counted like the rest. While a
Version is validated its staged objects exist once in the stage and again in
each copy the validation makes (an ephemeral object store, the expanded graph
tree built for a graph without a file manifest, and the private project that
tree is copied into; the copy count is
`PortableImportFootprint::VALIDATION_RESEARCH_COPIES`). With a limit set, budget
the stage and those copies, each up to `max_research_version_bytes`, as free
space in `scratch` (the system temporary directory when none is given), and pass
the destination's filesystem as `scratch` when you know it.

Compared with whole-package materialization, two outcomes differ:

- a package that still uses the earlier layout, `research-content/<digest>`,
  is refused as incompatible, and a Version over a configured
  `max_research_version_bytes` is refused with `LimitExceeded`; both are
  refusals, never a false `verified`;
- an evidence-only object that happens to be a Parquet file with an encoding
  outside the funded decoder profile is accepted, because no decoder reads
  evidence bytes. Earlier verification of a file refused it.

When a package has two independent defects, authentication outranks native
validation on every path: after a Version is refused, the scan keeps
authenticating the rest of the package without validating more Versions, so a
digest mismatch in a later entry is reported before an invalid commitment in an
earlier Version.

```text
graphforge --project ./citation-project portable preview --current --profile complete --strict
graphforge --project ./citation-project export --current --profile complete \
  --format bundle --output ./transfer.gfpb
```

Use `--checkpoint NAME` instead of `--current` to package a named immutable
checkpoint. For inspectable directory form, use `--format expanded` and a
`.gfproject` destination. Expanded and bundle representations have different
transport digests but verify to the same semantic package digest and selection
receipt.

For graph subsets, first pin the graph selector and its deterministic closure,
then preview the resulting ontology closure. A graph subset always carries the
exact module, bridge, activation, and schema authority needed to interpret its
selected data. An ontology-only package carries that authority without graph
data. A component-selective package is accepted only when its declared closure
is complete; strict selection never fills a missing authority dependency
implicitly.

## Verify before use

Full verification is required before import or promotion:

```text
graphforge portable verify --input ./transfer.gfpb --mode full
```

`--mode inspect` checks bounded structure only. It is useful for inventory, but
it does **not** report content as cryptographically verified. A full report
keeps these states separate:

- **completeness**: required structure and declared participants are present;
- **integrity**: every claimed byte and semantic digest matches;
- **compatibility**: versions, capabilities, schemas, and dependencies are
  supported; and
- **authenticity**: an optional explicit signer/trust policy succeeded.

A digest match proves content integrity, not publisher identity or authority.
Package identity (`package_digest`) describes selected logical content;
transport identity describes exact bundle bytes or an OCI manifest. Neither is
the source generation or the imported project's new generation.

Older portable readers must reject an M9 package as
`unsupported_future` when they encounter the required
`ontology-composition@1` capability. An M9-aware reader additionally rejects a
malformed control document, unsupported required feature, missing closure, or
digest mismatch before materializing staging. Inspect and verify never adopt,
replace, remove, activate, deactivate, or clear ontology authority.

### Verify from standard input

A canonical bundle can be verified without a file on disk, which is how a
service verifies an object it is streaming from storage:

```text
curl -sS "$OBJECT_URL" | graphforge --json portable verify --input - --mode full
```

`--input -` reads the bundle once from standard input and gives the verdict a
file gives the same bytes; only the report's `source` is `stream` instead of
`file`. Memory is bounded by the resource limits, not by the package size:

- opaque `evidence` artifacts of any size are hashed as they pass and never
  touch disk;
- a graph Parquet entry larger than `max_parquet_walk_bytes` (64 MiB) is staged
  in an anonymous file in `TMPDIR` while it streams, because the page walk needs
  random access, and the file is gone when that entry is done. At any moment the
  verifier holds at most one such entry on disk, so its temporary disk is
  bounded by the largest graph Parquet entry, not by the package;
- a research package additionally stages one Version at a time in a private
  directory under `TMPDIR`, removed before the command returns.

`--input -` requires `--mode full` (`--mode inspect` is refused with
`GF_VALIDATION`), and a bundle only: pass an expanded directory by path. To
name a file called `-`, write `./-`.

With `--json`, a verified package prints exactly one JSON document on stdout:
the report, including `package_digest`, the component inventory,
`bytes_by_kind`, and `plausibility`. A refusal, including a refused argument
list, prints one JSON error document on stderr and nothing on stdout. The exit code is `0` for a verified package
and `2` for a package that is not valid (an impostor manifest, a truncated
stream, a digest mismatch, a structural fault); any other refusal exits `1`.

A package that exceeds a resource limit (the retained control bytes, the
entry count, a declared size) is refused with the stable code
`GF_LIMIT_EXCEEDED` in `error.code` of the JSON error. Its exit code is `1`,
shared with other refusals, so a caller that must distinguish a limit from them
reads `error.code`, not the exit code.

[Portable verify qualification](../development/portable-verify-qualification.md)
records how the bounded-memory and no-disk claims are measured.

## Import a complete package

Import only into a new, empty, or pristine initialized destination, using a
caller-owned idempotency key. This alternative example uses a fresh destination,
separate from the completed copy above:

```text
mkdir selected-copy
graphforge --project ./selected-copy import --input ./transfer.gfpb \
  --idempotency-key 018f0f4e-7f4d-7c24-8f8f-8cbab5f47002
```

GraphForge fully verifies before admitting the destination, streams content
through bounded buffers, publishes one durable generation atomically, and then
reopens it. Corruption, cancellation, resource failure, or a crash leaves the
old-or-new authoritative state and no partially published project. Retrying the
same operation identity is safe. Complete import rejects selective packages
without mutating the destination.

Portable operations use finite component, manifest-byte, and copy-buffer
limits. Cancellation is observed while parsing and between streaming buffers.
Failures before `CURRENT` replacement leave the previous generation and composition
authority unchanged. After replacement, the imported generation is committed:
reopen or acknowledgement errors do not roll it back. Inspect the committed
operation outcome and retry with the same operation identity and exact input.
Private staging cleanup is reported separately; a cleanup error does not claim
that removal or its directory synchronization succeeded. Re-exporting the same pinned
selection is deterministic; host paths, runtime catalog IDs, session state, and
machine configuration cannot enter the semantic package identity.

## Clone from a Hub

`gf clone OWNER/REPOSITORY [DESTINATION]` downloads a published package,
verifies it, and imports it as a new project (see the
[CLI reference](https://github.com/CurateLabs/graphforge/blob/main/packages/cli/README.md)
for identity forms, transport rules, and every error code's class). Without
`--ref`, `--version-uuid` or `--tag`, clone follows the repository's default ref: when the
repository has research Branches, the default ref must name one and clone gets its
head Version; a repository without Branches gets the Project package. `--ref` names a Branch
by its label, the same string the Hub serves as the ref; `--tag NAME` clones the Version a
published tag names. `gf hub list OWNER/REPOSITORY` shows the refs, Branches,
Versions, Fork origin, and Proposals before you choose. Everything in progress lives in a
private staging directory beside the destination, `.DESTINATION.graphforge-clone`:
the partial download, its resume checkpoint, and the import target. The project
appears at `DESTINATION` only once it is complete and reopened, through one
atomic rename that never replaces an existing path.

Private repositories use browser/link authentication through WorkOS:
`gf auth login`, `gf auth status`, then `gf clone OWNER/REPOSITORY`.
Use `gf auth login --no-browser` to approve on another device. Auth commands
accept `--hub https://preview.example` for another Hub. Credentials live only
in the OS credential vault and are removed by `gf auth logout`, which also
revokes the provider session. Public clone remains anonymous.

Private discovery can return `404` to hide repository existence. Clone then
exchanges saved authentication for a repository-scoped read credential at its
Hub. Only the Hub-approved object origin receives that credential; authenticated
object requests never redirect. Range/resume and digest verification still apply.

Headless Hubs and protected previews also support environment credentials:
use `--hub https://preview.example --token-env HUB_READ_TOKEN`, adding
`--protection-bypass-env PREVIEW_BYPASS_SECRET` for Vercel protection. The flags
name environment variables, keeping secret values out of command arguments.
The Hub bearer and bypass travel only to initial discovery at the selected Hub.
If private content requires it, the bearer is exchanged at that Hub for a
separate object credential. Neither original secret reaches object origins or
redirects. See the CLI reference above for requirements.

### Large projects

**Disk space.** Clone needs free space on the destination's filesystem, not in
the system temporary directory: the package itself, plus the import's peak. The
import extracts the payload into a private stage and then publishes a second
copy of it into the project while the stage still exists, so a project package
needs about two more payload lengths. A research package is also validated
before it is published: its research files are copied into a validation store,
and each archived Version is rebuilt (an expanded graph through a temporary copy
of the tree, then into the Version's own project), so it may need up to four
payload lengths for the import when research content fills the package. A package that carries an ontology composition also writes
two generated controls, each about the size of its authored ontologies, into the
stage and again into the project. A package without an adjacency index spills additionally while
the import derives one, which this estimate does not cover. Less is needed for
whatever a previous run already downloaded or staged for the import; a rerun
after the import committed needs only about one more package length. Clone
checks this, and that the filesystem is admissible (`ext4`, `xfs`, `btrfs`,
APFS, or NTFS), before it
downloads anything. An import of a package you already hold makes the same two
checks before it extracts a byte. It refuses with `GF_UNSUPPORTED_FILESYSTEM`
(diagnostic `storage.unsupported_filesystem`), or with `GF_STORAGE` (diagnostic
`storage.insufficient_space`, whose message gives the free and needed bytes);
the CLI exits with status 3 for both. When the import finishes,
the staging directory and the package are removed and only the project remains.

**Interruptions and resume.** Rerun the same command. A network stall is
detected when no bytes arrive for a minute; each phase of a request has its own
bound, so a slow but steady transfer is never cut off however long it takes.
Clone retries a failed connection, a cut body, or a `408`, `429`, `502`, `503`,
or `504` response in the same invocation, up to five attempts without progress,
waiting 1, 2, 4, then 8 seconds, or longer when the Hub sends `Retry-After` (in
seconds or as an HTTP date, up to 30 seconds; a longer request is reported with its wait); a retry asks only for the missing bytes with `Range`
and the strong `ETag` the partial bytes came from. A server that ignores `Range`
sends the whole object again, which clone accepts and verifies. After the retry
bound, or a kill, power loss, or Ctrl-C at any point, a rerun of the same command
resumes the download, retries or replays the import, or finishes installing the
destination. The first Ctrl-C asks the running download, verification, or
import to stop cleanly; a second one exits immediately. Both are resumable.
Progress is printed to standard error.

**Limits.**

| Limit                       | Value                                                                                                          |
| --------------------------- | -------------------------------------------------------------------------------------------------------------- |
| One downloaded object       | 1 TiB, the same bound `gf publish` admits for a repository                                                     |
| One file inside the package | 8 GiB (the USTAR entry size field of a `.gfpb` bundle)                                                         |
| Package payload             | 1 TiB                                                                                                          |
| Files in a package          | 1,000,000 entries, but the 4 MiB tag manifest bounds a package to roughly 20,000 files at typical path lengths |

**Errors.** Codes are stable; the message adds detail.

| Code                        | Meaning                                                                                                                                                                   | What to do                                                                           |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `GF_UNSUPPORTED_FILESYSTEM` | The destination filesystem cannot hold a durable project                                                                                                                  | Clone onto a supported local volume                                                  |
| `hub.insufficient_space`    | Not enough free space beside the destination                                                                                                                              | Free space or choose another destination                                             |
| `hub.interrupted`           | The download stopped or was cancelled                                                                                                                                     | Rerun the same command                                                               |
| `hub.network`               | The Hub could not be reached, or the connection failed                                                                                                                    | Rerun later                                                                          |
| `hub.unavailable`           | The Hub answered `408`, `502`, `503`, or `504` after every retry                                                                                                          | Rerun later                                                                          |
| `hub.rate_limited`          | The Hub answered `429`; the message gives its `Retry-After` wait when sent                                                                                                | Rerun after that wait                                                                |
| `hub.not_found`             | The Hub answered `404` or `410`: no such repository or object                                                                                                             | Check the repository name and `--hub`                                                |
| `hub.access_denied`         | The Hub answered `401` or `403`                                                                                                                                           | Ask the repository owner for access                                                  |
| `hub.unexpected_status`     | The Hub answered any other unsuccessful status (named in the message)                                                                                                     | Report it to the Hub operator                                                        |
| `hub.integrity`             | The downloaded bytes do not match their digest; the partial download was removed                                                                                          | Rerun to download again                                                              |
| `hub.package.*`             | The package failed verification or import, with the import's own code (for example `hub.package.io` or `hub.package.cancelled`)                                           | `hub.package.io` and `hub.package.cancelled`: rerun; others: the package is unusable |
| `hub.destination_conflict`  | The destination already exists, or appeared while the clone ran. Clone never replaces it; it removes its own staging for that destination, including any partial download | Choose another destination, or remove the existing one and rerun                     |
| `hub.concurrent_clone`      | Another clone to the same destination is running                                                                                                                          | Wait for it                                                                          |
| `hub.limit_exceeded`        | An object exceeds the 1 TiB bound                                                                                                                                         | None; the repository cannot be cloned                                                |

## Carry TCK results as evidence

TCK results describe how an engine behaved; they do not define an ontology or
change query authority. Store a TCK report as an `evidence` or `provenance`
component. The ontology-composition component must not depend on it, and its
digest must not enter the composition fingerprint. Selecting or omitting the
evidence may change the package digest because package content changed, but the
module and composition identities remain exact and unchanged.

## Local and air-gapped transfer

For local or removable-media transfer:

1. Preview the pinned generation and review omissions/redactions.
2. Export a `.gfpb` bundle or `.gfproject/` expanded package.
3. Record the package digest and selection fingerprint separately from the
   file/media checksum.
4. Copy the completed portable package—not the live project directory.
5. Run full verification on the receiving machine before import or selective
   consumption.

The workflow needs no registry, server, cloud account, or signature service.
Apply normal access controls to the completed package; selection protects only
what was intentionally omitted or redacted.

## Promote through OCI by digest

OCI Distribution is an optional transport adapter. Publish verifies the local
package before upload; pull verifies downloaded content before publishing the
destination.

```text
graphforge portable publish-oci --package transfer.gfpb \
  --registry ghcr.io --repository example/graphforge-projects --tag candidate

graphforge portable pull-oci --registry ghcr.io \
  --repository example/graphforge-projects \
  --reference sha256:OCI_MANIFEST_DIGEST \
  --expected-digest sha256:PACKAGE_DIGEST \
  --destination received.gfpb
```

Tags are mutable discovery references. Resolve a tag, record the returned OCI
manifest digest, and promote or pull by that digest. Keep the GraphForge package
digest separate: it remains stable across expanded, bundled, and OCI forms.

Credentials are caller-owned and must come from a secure provider or process
environment. Never put credentials in a package, command transcript, receipt,
error report, or progress event. HTTPS is the default. `--insecure-http` is only
for a disposable, isolated local registry and is never a production fallback.

Optional signature attachments establish authenticity only when an explicit
signer/key policy is evaluated. Unsigned content may be integrity-valid while
authenticity is absent.

## API map

All surfaces call the same Rust authority:

| Workflow         | Rust API authority                          | CLI                    |
| ---------------- | ------------------------------------------- | ---------------------- |
| Preview          | `GraphForge::preview_portable_selection` | `portable preview`     |
| Export           | `GraphForge::export_portable`            | `export`      |
| Inspect / verify | `verify_portable`                        | `portable verify`      |
| Complete import  | `GraphForge::import_portable`            | `import`      |
| OCI publish      | `publish_portable_oci`                   | `portable publish-oci` |
| OCI pull         | `pull_portable_oci`                      | `portable pull-oci`    |

Python and Node expose equivalent preview, export, verify, import, publish, and
pull methods without wrapper-owned archive, selection, registry, or fallback
semantics. Counts and byte sizes remain lossless at the binding boundary.

For the normative layout, identity, resource, compatibility, and security
contract, see [Portable project architecture](../book/architecture/portable-project.md)
and [ADR 0021](../adr/0021-portable-project.md).

Primary `export` and `import` reuse the streaming `graphforge-portable-export/1`
and `graphforge-portable-import/1` JSON receipts. Package and transport digests
remain distinct; retired envelope digest/count fields are not compatibility aliases.
The duplicate `portable export` and `portable import` verbs are removed.

## Continue a published Branch

Against a conforming publish-capable Hub, clone a named Branch, edit it through
the native research Branch API, then publish that same ref back. The clone keeps
the original Project and Branch UUID, creation record and selected head; the next
commit records that head as its first parent. Authors and committers remain exact
native record metadata and are published in the lineage and, as the derived
people list with their roles, in the Project summary; a Fork's origin citation
carries the cited Version's credit. Reopen and inspect historical records with
`research_version`; a metadata-only ancestor is not a retained payload.

The other advertised refs remain read-only remote citations. Immutable-Version
clones stay detached. Publishing divergent work is refused as `non_fast_forward`;
a stale repository revision is still `ref_conflict`. Neither refusal moves a ref.
Merge and explicit rewrite/prune are separate operations: `gf publish --rewrite`
replaces a divergent head under the expected-revision lease, and
`gf publish --rewrite --prune` also drops now-unreferenced Versions.
[Research journey](research-journey.md) walks the full cycle. Production hosted
write availability is independent of this native protocol behavior.
