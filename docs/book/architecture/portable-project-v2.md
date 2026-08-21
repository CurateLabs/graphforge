# Portable project v2

This document is normative. **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, and
**MAY** have their RFC 2119 meanings. ADR 0021 records the format choice.

## Identities and states

Four identities are deliberately distinct:

- `package_digest` identifies selected logical content. It is
  `sha256("graphforge-project/2\0" || JCS(manifest without package_digest))`.
- `transport_digest` is SHA-256 over the exact `.gfpb` bytes, or over the
  canonical expanded transport inventory described below. It is not package
  identity.
- `participant_id` is a stable portable component identity, globally unique
  within the package; it is never a runtime catalog ID.
- `source_generation` identifies the committed GraphForge generation from
  which selection occurred. It proves provenance, not package identity or an
  import target generation.

`integrity`, `compatibility`, and `authenticity` are separate result axes.
Digest agreement can establish integrity only. Supported versions,
capabilities, kinds, and dependency rules establish compatibility. Authenticity
is `not_evaluated`, `unsigned`, `verified`, or `failed` according to an explicit
signature/trust policy; it never defaults to verified.

## Semantic manifest

`data/graphforge-project.json` MUST validate against
[`graphforge-project-v2.schema.json`](../../contracts/graphforge-project-v2.schema.json).
JSON strings MUST be valid Unicode, object members MUST be JCS-canonical, and
numbers outside the schema are forbidden. Arrays are ordered as stated by the
schema; components use ascending UTF-8 byte order of `(kind, participant_id)`
and dependencies use ascending UTF-8 byte order with no duplicates.
Each component owns one or more file descriptors in ascending portable-path
order. Paths are globally unique, so a participant can own bounded Parquet and
index shards without fabricating per-file participant identities.
Every selection root and dependency names that globally unique participant ID;
unknown, duplicate, self, or cyclic required dependencies fail compatibility.

The closed component kinds are `ontology`, `schema`, `migration`, `settings`,
`graph-data`, `derived-artifact`, `evidence`, `provenance`, and `compatibility`.
Package classes are `complete`, `ontology-only`, `component-selective`, and
`graph-data-subset`. Selection records the requested roots, complete required
dependency closure, explicit omissions and redactions, and graph subset. It
does not imply graph merge behavior. Required omitted dependencies make the
package incompatible.

The on-wire manifest MUST contain `package_digest`. Canonicalization for digest
calculation removes only that member, without modifying any other value; the
reader recomputes it and compares in constant time before semantic use.

Duplicate JSON member names, lone Unicode surrogates, absolute paths, `.`/`..`,
backslashes, drive prefixes, NUL/control characters,
runtime-local catalog IDs, mutable timestamps, secrets, and session-only
ontology state MUST NOT occur. NFC-normalized relative paths are compared by
both exact UTF-8 bytes and Unicode default case folding; a collision fails.

## Expanded `.gfproject/` representation

The root contains exactly these tag files plus enumerated regular payload files:

```text
bagit.txt
bag-info.txt
manifest-sha256.txt
tagmanifest-sha256.txt
data/graphforge-project.json
data/components/<kind>/<participant-id>/<files...>
```

`bagit.txt` is exactly `BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n`.
`bag-info.txt` contains exactly, in this order, LF terminated:
`Bag-Software-Agent: GraphForge portable-v2\nBagging-Date: 1970-01-01\n`.
`manifest-sha256.txt` contains every `data/` regular file in ascending portable
path order as lowercase hex, two spaces, path, LF. `tagmanifest-sha256.txt`
contains `bag-info.txt`, `bagit.txt`, and `manifest-sha256.txt` in that order and
the same syntax. It MUST NOT list itself. No fetch file, hidden file, directory
entry, link, special file, or unmanifested file is allowed.

The expanded transport digest is SHA-256 over
`"graphforge-expanded/2\0"` followed, in ascending path order, by an unsigned
64-bit big-endian path length, path bytes, unsigned 64-bit big-endian file
length, and file SHA-256 bytes for every regular file including tag files except
`tagmanifest-sha256.txt`; then the canonical bytes of that tag manifest are
appended. This digest is transport evidence only.

## Deterministic `.gfpb` bundle

The bundle is an uncompressed POSIX pax interchange-format tar stream. Writers
MUST emit the same regular files as the expanded form in ascending UTF-8 path
order; directory headers are forbidden. Each entry uses a ustar header with
the whole path in `name` when its UTF-8 encoding is at most 100 bytes, otherwise
with the longest slash boundary producing `prefix` at most 155 bytes and `name`
at most 100 bytes. A path that does not fit uses exactly one preceding
local PAX header containing only `path=<UTF-8 path>\n`, encoded with the POSIX
decimal record-length rule. PAX header names are `PaxHeaders/<sha256(path)[:16]>`.

Every regular header has mode `0000644`, uid/gid `0`, uname/gname empty, mtime
`0`, decimal/ustar size encoding, typeflag `0`, and a correct checksum computed
with the checksum field treated as spaces. Numeric fields MUST use ASCII octal
with the standard trailing NUL/space and MUST NOT use base-256. Payloads are
padded with zero bytes to a 512-byte boundary. Exactly two zero blocks terminate
the archive and no trailing bytes follow. Sparse maps, GNU extensions, global
PAX headers, compression, encryption, concatenated archives, and duplicate
headers are forbidden. Readers MUST reject any non-canonical byte even if a
general tar reader accepts it.

A local PAX header uses typeflag `x` and otherwise the same mode, uid/gid,
owner-name, mtime, size/checksum encoding, and zero-padding rules. Its ustar
`name` is the specified `PaxHeaders/` value and its data length is exactly the
single PAX record length. The following regular header uses the same digest
suffix as a bounded placeholder name; the PAX `path` is authoritative.

Writers process one header and a bounded copy buffer at a time. They MUST stat,
open without following links, identity-check, stream/hash, and re-stat each
source; concurrent mutation fails. Sparse source files are read as logical
bytes and emitted densely, subject to the declared length and limits.

## Limits, cancellation, and safety

Before payload access, readers enforce configurable limits no weaker than:
10,000 components, 1,000,000 entries, 16 TiB per entry, 1 PiB declared total,
16 MiB semantic manifest, 4 MiB tag manifests, and 4 KiB paths. Arithmetic is
checked. Implementations MAY configure lower limits and return a typed limit
result. Since v2 is uncompressed, decompressed length equals bundle payload
length; a compression marker is unsupported, not auto-detected.

Cancellation is checked before each header, before each copy-buffer operation,
and before publication. Validation and import stage privately; no failure,
cancellation, unsupported semantic, or hostile entry may mutate a project.
Symlinks, hard links, devices, FIFOs, sockets, traversal, absolute paths,
non-NFC paths, case-fold collisions, duplicate normalized paths, missing or
extra entries, length/digest mismatch, unstable files, and truncated/end-marker
errors fail closed.

The billion-edge structural conformance case is a single graph-data component
whose descriptors may cover many bounded Parquet/sharded-index files totaling
over 16 GiB. Neither representation has an envelope field; all lengths are
64-bit and all I/O is incremental, so peak format memory is bounded by manifest,
entry metadata limits, and the copy buffer rather than payload size.

## Compatibility

V2 writers emit only major version 2 and this closed schema. A reader receiving
an unknown required major version, capability, component kind, or dependency
rule returns `unsupported_future` before reading component payloads or mutating
a project. Unknown optional authenticity evidence may be preserved as opaque
transport data only when it is explicitly outside the closed package.

Portable v1 is recognized and delegated to the existing bounded v1 reader;
v2 code MUST NOT reinterpret it, silently upgrade it, or emit v1. Importing v1
and exporting v2 are two explicit operations with a new v2 package digest.

## Verification API

`graphforge_api::verify_portable_v2` is the shared Rust authority used by import,
publication, CLI, and binding surfaces. It inspects either an expanded directory
or a bundle by source type, not filename suffix, and never mutates a project.
Callers supply finite limits and an optional cancellation flag.

Full mode reads and hashes every regular entry before returning
`integrity: verified`. Structure-only mode returns `integrity: not_checked` and
is never import or publication evidence. Reports keep integrity, compatibility,
and authenticity separate and identify failures only by a bounded
portable-relative entry. The implementation retains the bounded semantic/tag
records, entry index, and configured copy buffer; payloads stream incrementally
even when the declared package exceeds 16 GiB.

When `ontology-composition@1` is present, the verified report also exposes the
authenticated exact module, bridge, activation, feature, and composition
identities. Module documents remain ordinary `ontology` components and bridge
documents remain ordinary `schema` components. The versioned
`graphforge-ontology-composition` compatibility component authenticates their
closure. It deliberately does not appear in the runtime generation map:
verify, inspect, materialize, and import therefore cannot adopt or replace
workspace ontology authority. A consumer must pass the verified identities to
the explicit typed ontology lifecycle API to change authority.

See the [fixture guide](../../../tests/fixtures/portable-v2/README.md) for the
positive, negative, and structural conformance vectors.

## Complete-package exporter

### Selection planning

`graphforge_storage::preview_portable_v2_selection` resolves a selection before
any destination is created. Profiles are `Complete`, `OntologyOnly`,
`DataComponents`, `Artifacts`, `Settings`, and `Custom`. Custom selectors use
the pair `(capability_id, record_family_id)`; runtime catalog IDs, display names,
and host paths are not accepted as identity. Graph data selection always means
the whole committed graph component. Fine-grained row or subgraph selection is
handled by `preview_portable_v2_graph_subset` / `plan_graph_subset_portable_v2`
(#786): typed UUID selectors, `induced-edges` and `referential` closure, property
redaction, and `package_class: graph-data-subset` with a content-free
`selection.graph_subset` receipt.

The preview lists included and excluded stable identities, inclusion reasons,
row counts, exact committed participant byte estimates, required capabilities,
redaction reason codes, package class, and a canonical SHA-256 selection
fingerprint. It never includes setting values or source paths. Results are
canonically ordered and bounded by the portable component and byte limits.
Duplicate, missing, or ambiguous custom identities fail rather than guessing.

Schema authority is added as visible required closure for ontology, graph, and
artifact selections. Strict mode refuses that widening unless the caller
selected the authority explicitly. Portable settings use a closed recursive
JSON scan and fail closed on secret-bearing keys or absolute host paths; neither
the preview nor its typed failure echoes the rejected value.

`graphforge_storage::plan_selected_portable_v2` consumes that immutable preview
for both expanded and bundle output. It filters the authenticated runtime map,
participants, capabilities, and graph-tree placement to the same selected
closure. The durable receipt repeats the selection fingerprint, so callers can
prove that the reviewed preview is the plan the writer used.

`graphforge_storage::plan_complete_portable_v2` resolves a current generation
or checkpoint before planning and retains that generation's lease. The plan is
a bounded index of portable paths, lengths, digests, and source identities; it
never contains payload bytes and never follows `CURRENT` again. File-backed
graph shards come only from the committed graph-files inventory. Runtime files
such as `CURRENT`, leases, locks, journals, attempts, trash, caches, host paths,
and secrets are therefore not export candidates.

`export_complete_portable_v2` streams that same plan to either representation.
The copy buffer, entry count, per-entry bytes, and total declared bytes are
finite caller-visible limits. Progress contains aggregate entry and byte counts
only. Cancellation is observed before entries and copy-buffer operations.
Sources are opened as regular files and compared with their planned identity,
length, modification state, and digest after streaming; links, special files,
mutation, short reads, and digest changes fail closed.

Every complete export also carries one canonical, authenticated compatibility
control component. It maps portable participant IDs back to runtime capability
and record-family versions, encoding, schema fingerprint, row count, and graph
placement. The map is bounded and reversible, but deliberately contains no
host path, lease, credential, or secret. The shared verifier rejects duplicate
JSON members, unknown fields, non-canonical bytes, unsupported encodings, and
references that are not authenticated by the semantic manifest.

`materialize_verified_portable_v2` exposes the verified component entries to
the importer through a new private directory. It fully verifies before writing,
streams with the configured copy buffer, verifies the source again before
returning, and removes the directory on cancellation, mutation, or failure.
Expanded and bundle forms therefore yield the same component tree without
making unverified bytes available to project publication.

Expanded output is built in a unique sibling directory, synced bottom-up, and
published with a no-replace atomic rename. Bundles use a unique sibling file,
canonical header order and PAX paths, exactly two end blocks, and the same
no-replace publication. Until the final rename, neither representation exposes
the requested destination. Cancellation, I/O failure, disk exhaustion, source
mutation, or destination creation removes the private partial output and never
overwrites an existing destination. Re-running an unchanged plan produces the
same package digest and bundle bytes; the two representations intentionally
have distinct transport digests.

Portable v1 remains available through `export_portable_project`. It is a
separate explicit contract and is never emitted with v2 markers.

## Complete-package importer

`import_complete_portable_v2` accepts either local representation and invokes
the shared full verifier before admitting the destination. Authenticated
component entries are streamed into a transaction-owned materialization tree,
then streamed again into one private generation with their declared lengths and
digests rechecked. The configured copy buffer and bounded verifier indexes—not
package payload size—bound memory. Publication uses the normal generation
journal, fsync, validation, and atomic `CURRENT` transition, followed by a clean
public reopen.

The default operation accepts only the `complete` package class and only a new,
empty, or pristine initialized destination. Existing project state is never
overwritten or merged. Ontology-only, settings-only/component-selective, and
graph-subset packages require explicit class-specific consumers; complete
import returns a typed incompatibility without admitting the destination.
Cancellation and corruption remove the private materialization, while a crash
inside generation publication is handled by the normal project recovery and
transaction-idempotency protocol. Portable v1 import remains a separate,
explicit compatibility API.

## Optional OCI Distribution transport

Portable-v2 packages may be published and pulled through an OCI
Distribution/ORAS-compatible registry without changing package identity.
Local expanded/bundle export and air-gapped sharing remain fully functional
without any registry.

### Media types and digests

| Role | Media type |
| --- | --- |
| Artifact type | `application/vnd.graphforge.project.v2` |
| Config | `application/vnd.graphforge.project.v2.config+json` |
| Bundle layer | `application/vnd.graphforge.project.v2+tar` |
| Signature artifact | `application/vnd.graphforge.project.v2.signature` |
| Signature payload | `application/vnd.graphforge.project.v2.signature+json` |

The GraphForge `package_digest` remains authoritative for package equivalence.
The OCI manifest digest is transport/distribution identity only. Human tags may
be written and resolved, but they are mutable references and never substitute
for a recorded digest. Pull-by-digest is stable even when a tag later moves;
tag/digest disagreement fails closed.

The same OCI mapping is used for every package class (`complete`,
`ontology-only`, `component-selective`, `graph-data-subset`). Selective packages
are not re-wrapped into a different transport semantics.

### Publish / pull behavior

`publish_portable_v2_oci` verifies the local package fully, uploads config and
layer blobs (deduplicating by digest when the registry already has them),
writes the digest-pinned manifest, optionally attaches a signature referrer,
and only reports success after a fresh registry observation re-reads that
manifest digest. `pull_portable_v2_oci` resolves the reference, downloads
blobs, verifies the package with the shared verifier, evaluates authenticity
separately from integrity, and only then publishes the destination. Cancellation,
missing blobs, digest mismatch, incompatible media types, and auth/transport
failures never leave a successful receipt or a claimed local package.

Credentials stay caller-owned (secure providers/stdin). They are never
persisted or emitted in receipts, errors, or progress events. HTTPS is the
default; plain HTTP is an explicit `insecure_http` opt-in for disposable local
registries only.

### Signatures and authenticity

Optional signature/provenance attachments use OCI subject/referrer semantics.
Integrity and authenticity are distinct: unsigned content may verify as
integrity-valid while authenticity is `absent`, and policy mismatches or
invalid MACs fail authenticity without being reported as digest/integrity
failures. Signature verification requires an explicit signer/key policy.

### Local conformance and hosted registry path

Required conformance uses an in-process disposable registry
(`MemoryOciRegistry`) so ordinary core CI needs no network. Hosted registries
that speak OCI Distribution (for example GHCR) are reached with the HTTP
client:

```text
# Publish by digest (optional mutable tag)
registry = ghcr.io
repository = org/graphforge-packages
reference = sha256:<oci-manifest-digest>

# Pull only by the recorded OCI digest
# Tags may be resolved for discovery, then re-pinned to the digest before trust.
```

Operators retain registry credentials, retention, immutability, and visibility
policy. Offline/air-gapped users keep using `.gfpb` / `.gfproject/` copies.
Binding parity for these Rust-owned verbs is owned by the portable promotion
parity slice (#744).
