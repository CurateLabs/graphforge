# Move projects with portable project v2

Portable project v2 is GraphForge's primary workflow for moving a project
between workspaces, machines, and registries. It packages one immutable project
generation with a canonical semantic identity. Use it for local copies,
air-gapped transfer, selective sharing, and OCI promotion.

> **Never copy, archive, synchronize, or publish a live `.graphforge/`
> directory.** It contains generation pointers, journals, locks, leases, and
> other runtime state that are not an interchange contract. Do not commit graph
> or source data, `.gfpb` bundles, `.gfproject/` packages, imports, exports, or
> materialized project state to Git. Keep those paths ignored and move them with
> portable-v2 instead.

The Rust engine owns selection, packaging, verification, import, and OCI
behavior. Python, Node, and the CLI are thin projections of the same operations
and return the same package identity, selection receipt, state classification,
progress, and typed failures.

## Keep the five identities separate

A portable multi-ontology workflow has five intentionally different identity
domains. Do not copy a value from one domain into another:

| Identity | What it names | Stable across expanded and bundle forms? |
| --- | --- | --- |
| module identity | one authored ontology document: URI, opaque version, and canonical digest | yes |
| composition identity | the exact ordered module, bridge, and activation authority | yes |
| package identity | the selected logical portable content | yes |
| runtime catalog identity | a project-local exploratory observation | no; it is not portable authority |
| evidence identity | a TCK report, validation transcript, or benchmark attached as provenance | only as ordinary selected evidence content |

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

```text
graphforge portable preview --current --profile complete --strict
graphforge portable export --current --profile complete \
  --format bundle --output transfer.gfpb
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
graphforge portable verify --input transfer.gfpb --mode full
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

Older portable-v2 readers must reject an M9 package as
`unsupported_future` when they encounter the required
`ontology-composition@1` capability. An M9-aware reader additionally rejects a
malformed control document, unsupported required feature, missing closure, or
digest mismatch before materializing staging. Inspect and verify never adopt,
replace, remove, activate, deactivate, or clear ontology authority.

## Import a complete package

Import only into a new, empty, or pristine initialized destination, using a
caller-owned idempotency key:

```text
graphforge portable import --input transfer.gfpb \
  --idempotency-key 018f0f4e-7f4d-7c24-8f8f-8cbab5f47001
```

GraphForge fully verifies before admitting the destination, streams content
through bounded buffers, publishes one durable generation atomically, and then
reopens it. Corruption, cancellation, resource failure, or a crash leaves the
old-or-new authoritative state and no partially published project. Retrying the
same operation identity is safe. Complete import rejects selective packages
without mutating the destination.

Portable operations use finite component, manifest-byte, and copy-buffer
limits. Cancellation is observed while parsing and between streaming buffers.
On any failure GraphForge removes private staging residue and keeps the previous
generation and composition authority unchanged. Re-exporting the same pinned
selection is deterministic; host paths, runtime catalog IDs, session state, and
machine configuration cannot enter the semantic package identity.

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

| Workflow | Rust API authority | CLI |
| --- | --- | --- |
| Preview | `GraphForge::preview_portable_v2_selection` | `portable preview` |
| Export | `GraphForge::export_portable_v2` | `portable export` |
| Inspect / verify | `verify_portable_v2` | `portable verify` |
| Complete import | `GraphForge::import_portable_v2` | `portable import` |
| OCI publish | `publish_portable_v2_oci` | `portable publish-oci` |
| OCI pull | `pull_portable_v2_oci` | `portable pull-oci` |

Python and Node expose equivalent preview, export, verify, import, publish, and
pull methods without wrapper-owned archive, selection, registry, or fallback
semantics. Counts and byte sizes remain lossless at the binding boundary.

For the normative layout, identity, resource, compatibility, and security
contract, see [Portable project v2 architecture](../book/architecture/portable-project-v2.md)
and [ADR 0021](../adr/0021-portable-project-v2.md).
