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

See the [fixture guide](../../../tests/fixtures/portable-v2/README.md) for the
positive, negative, and structural conformance vectors.

## Complete-package exporter

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
