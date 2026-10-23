# Hub publish v1 contract artifacts

This directory and the `graphforge-hub-publish` crate are the single publish
wire authority. [ADR 0053](../../../adr/0053-hub-publish-wire-contract.md)
defines the HTTP mapping, the commit order, credentials, and error statuses.
The read side (`.gf/refs`, `.gf/manifest`, objects) reuses
[discovery v1](../../discovery/v1/README.md).

## Documents

| Document | Schema | Used by |
| --- | --- | --- |
| Capabilities | `capabilities.schema.json` | `GET {repo}/.gf/publish` |
| Session request | `session-request.schema.json` | `POST {repo}/.gf/publish/sessions` |
| Session response and receipt | `session-response.schema.json` | session open and commit responses |
| Commit request | `commit-request.schema.json` | `POST {repo}/.gf/publish/sessions/{id}/commit` |
| Operation status | `operation-status.schema.json` | `GET {repo}/.gf/publish/operations/{operation_uuid}` |
| Upload status | `upload-status.schema.json` | `PUT <upload_url>` response |
| Error | `error.schema.json` | every non-OAuth error response |

The schemas describe wire shape. Rules a schema cannot express (canonical
ordering, the request commitment, the commit order, limits, and the closed
required-capability set) stay authoritative in Rust.

## `gf publish`

```bash
gf --project <dir> publish <owner/repository | https://hub/owner/repository> \
    (--ref <branch-label> | --version-uuid <uuid>) \
    [--fork-of <owner/repository | URL>] [--operation-uuid <uuid>] \
    [--protection-bypass-env <name>] [--json]
```

`--ref` publishes the head of the local research Branch with that label under
the same discovery ref; `--version-uuid` publishes one exact retained Version
into an existing repository. `--fork-of` creates a new repository for a Fork
Project and cites its origin, after checking that the origin repository
publishes the cited Version with the same identity. The repository argument
follows `gf clone`: `owner/repository` resolves against `https://graphforge.sh`,
or pass the HTTPS repository URL.

Credentials come from `GRAPHFORGE_HUB_PUBLISH_TOKEN`, a saved `gf auth login`
session for the selected Hub, or, in a terminal, the publish device flow
(instructions on standard error). Saved sessions use the operating system
credential vault and exchange for a repository-scoped publish token. Captured
binding invocations require an explicit environment token and never consult the
vault or prompt. The publish token is never written to the Project,
configuration, or output. The device flow waits at most fifteen minutes.
`--protection-bypass-env` reads a preview bypass secret from the named environment
variable for selected-Hub control and OAuth requests. Neither that secret nor
the publish bearer is sent to object reads or upload HEAD/PUT requests. Rerunning a publication of an unchanged Project replays its original
receipt without uploading; after new data is committed the same command
publishes a new snapshot. An interrupted upload resumes from the Hub's offset,
and a rerun after `hub.publish.ref_conflict` lands on the newer revision without
re-sending bytes the Hub already holds. `--json` prints the receipt.

| Failure | Code | Exit |
| --- | --- | --- |
| Missing, refused, or denied credential | `hub.publish.auth_denied` | 2 |
| Quota or entitlement refused | `hub.publish.entitlement_denied` | 2 |
| Operation reused for another publication | `GF_IDEMPOTENCY_CONFLICT` | 1 |
| Permanent item ID belongs to another publisher | `hub.publish.id_taken` | 2 |
| Repository moved since it was read; Fork target exists | `hub.publish.ref_conflict` | 2 |
| Unsupported protocol major or capability | `hub.publish.unsupported_future` | 2 |
| Bytes disagree with digest or length; Fork origin not published | `hub.publish.integrity_failure` | 2 |
| Invalid selection or request | `hub.publish.invalid_input` | 2 |
| Non-HTTPS, credentialed, or private-address URL | `hub.unsafe_location` | 2 |
| Transport failure or Hub internal error | `hub.network`, `hub.publish.internal` | 3 |

A permanent-ID refusal is HTTP 409 with `code: "id_taken"`, fixed message
`permanent item ID is already owned`, and optional `items` sorted by canonical
bare ID. Items contain `kind` (`ontology_module` or `bridge_set`) and `id`;
`owner` is included only when visible, and `publish_as_owner: true` only when
publication as the owning organization is authorized. False hints and unrelated
error-code item lists are invalid. Exact published item tuples may be reused
without transferring ownership; foreign new content refuses all claims atomically.

The CLI renders its own next steps and saves bounded native per-item refusal
receipts in the local Project, outside portable exports. A visible owner gives
a publish-as-owner option; an authorized organization hint directs publication
as that organization. Otherwise guidance names `gf ontology regenerate-id <id>`.
That future command is not implemented by this contract change. The native seam
requires an exact receipt and a retained replacement Version before recording
an old-ID/new-ID relation; a later old-Version refusal points to that Version.
Receipts contain no credentials, URLs or server message text.
Retention preserves the current refusal and all recorded replacement relations,
evicting other unresolved receipts in canonical ID order to fit 8,192 entries and
8 MiB. Regenerating an evicted ID requires a fresh exact-ID refusal. If protected
records fill capacity, the previous file remains unchanged and the CLI reports
capacity exhaustion rather than asking to repair healthy storage.

[ADR 0053](../../../adr/0053-hub-publish-wire-contract.md#client-snapshot)
records how the client builds the snapshot.

A plain `gf clone` of a research publication selects the advertised default
research Branch head, including non-`main` defaults. `--ref` and `--version-uuid`
override that choice. The clone validates lineage and imports the selected
Version package; the publication's Project package contains metadata rather
than the research graph. Invalid advertised lineage fails closed. Repositories
without lineage, or with valid lineage containing no Branches, retain Project
package cloning.

## Authenticated publication context

A Hub can offer optional `{"capability":"publish-read-context","major":1}` in
its existing `capabilities` array. After obtaining the repository publish token,
the client reads fixed `GET {repo}/.gf/publish/context` on the selected Hub through
the authenticated control transport. CI tokens, device tokens and tokens
exchanged from saved sessions use this same endpoint. The Hub rechecks current
authorization and returns a private, `no-store` snapshot with exactly three keys:

```json
{"refs_json":"<exact stored refs UTF-8 JSON>","manifest_json":"<exact stored manifest UTF-8 JSON>","lineage_json":"<exact stored lineage UTF-8 JSON>"}
```

All three values are strings from one snapshot, or all three are `null` with
HTTP 200 for authorized absence. Missing, extra or duplicate keys, mixed null
and string values, and other value types are invalid. HTTP 401/403 deny access;
422 indicates mixed or corrupt stored state. The response is bounded to 4 MiB.
An advertised endpoint failure fails the publication; it never becomes anonymous
404 or permission to create a new repository. Without optional major 1 support,
the existing public discovery path remains available.

The client derives the expected revision from the SHA-256 of the exact refs
bytes and validates repository identity, refs/manifest consistency, lineage
object byte length and SHA-256, canonical lineage digest, and package and
ref/Version bindings. It uses the returned lineage bytes directly, without
reading a Worker object URL. Control requests follow no redirects. Publish
credentials and preview bypass stay on the selected Hub control origin and are
never attached to Worker reads or upload requests. Fork origin checks retain
public source reads and never borrow the destination publish bearer, including
when source and destination share a Hub origin.

## Conformance corpus

`conformance.json` (`graphforge-hub-publish-conformance/1`) lists cases. Each
case is a sequence of steps against a fresh Hub at origin
`https://hub.example`:

- A **request** step sends `method`, `url`, `headers`, and a `json`, `form`, or
  `body_text` body, then checks `expect`: the exact `status`, listed `headers`,
  and either an exact `json` body, an `error` code (`{code, message}` with any
  message), an `oauth_error` (`{error}`), or an exact `body_text`. `capture`
  binds variables from JSON pointers into the response before matching.
- An **action** step is an operator hook outside the HTTP mapping:
  `issue_token`, `advance_clock`, `approve_device`, `deny_device`, `set_quota`,
  `corrupt_object`, `set_item_visibility`, or
  `set_organization_publish_permission`.

`${name}` in any string is replaced by a captured variable. Commitments and
manifest validators in the corpus depend on the object locations, so a Hub under
test serves origin `https://hub.example` with objects at
`{repo}/.gf/objects/{digest}`.

Regenerate every artifact from the Rust test source:

```bash
GRAPHFORGE_UPDATE_HUB_PUBLISH_ARTIFACTS=1 \
  cargo test -p graphforge-hub-publish --test contract_artifacts
```

Without that variable the same test compares the generated bytes with the
checked-in files and replays every case through `ReferenceHub`, so CI fails
when the artifacts drift or the reference Hub stops conforming.

The permanent-ID case covers first claims, exact foreign tuple reuse, hidden
and visible owners, and authorized organization hints in one stateful sequence. Permission actions are
reference-Hub operator facts, not a new identity-provider protocol.

## Explicit Version previews

`gf publish ... --preview result.png --preview result.svg` selects presentation
images for the exact immutable research Version selected by `--ref` or
`--version-uuid`. Selection is explicit: adjacent workbench files, saved results,
notebooks, and visualization intent files are never inferred. No images are
selected by default. An operation's intent and canonical manifest bind the
selected descriptors, so changing images under the same operation identity
conflicts rather than silently replacing them. Default operation identities
include validated ordered preview descriptors when present, so a new selection
of an already published Version creates a new operation and exact retries
replay it. No-preview default identities retain their legacy definition.

The optional typed `x-graphforge-previews` discovery extension has format
`graphforge-previews/1`; `previews.schema.json` describes its metadata. Each
entry names a canonical native Version UUID, encoded SHA-256 digest, media type,
byte length, width and height, and optional plain-text caption/alt text. The
Rust API is `graphforge_discovery::preview` (also reexported by
`graphforge_hub_publish`). Discovery validates metadata without image downloads;
Hub commit additionally binds the Version to native lineage and validates the
actual retained bytes. Clients must negotiate `version-previews@1` before
uploading selected images. A Hub must implement the same Rust content admission;
client checks alone are insufficient.

Images are separate digest-addressed transport objects, outside portable Project
packages. They do not change package digests or contents. `gf clone` ignores
previews and does not fetch their bytes. Native Artifacts with Core-defined
lineage remain native data; this presentation channel does not replace them or
admit arbitrary opaque package files. SavedQuery lifecycle remains separate.

The closed image profile permits up to eight images, 8 MiB each and 32 MiB total,
with width/height from 1 to 8192 and at most 16,777,216 pixels. Caption and alt
text are bounded to 1024 and 2048 UTF-8 bytes. PNG admission checks framing,
critical and ancillary CRCs, zlib checksums, and the complete decoded row stream
(including Adam7) with a 2 MiB decoder budget; it never allocates a full image.
Animation and compressed/unbounded ancillary metadata are excluded. Allowed
chunks are IHDR, PLTE, IDAT, IEND, tRNS, sRGB, gAMA, cHRM and pHYs; trailing
streams are rejected.

SVG admission uses an XML parser with DTD/entity expansion disabled, at most
100,000 nodes, depth 64 and 64 attributes per element. Before DOM allocation,
a borrowing XML tokenizer enforces the per-element quota and shallow stack;
raw opening-delimiter and equals-sign counts are each capped at 100,000 to
bound the DOM parser's eager allocation estimates even inside quoted values. Explicit integer pixel
width/height are required. The closed profile includes basic shapes, paths,
text, groups, clips and gradients. It excludes scripts, event attributes,
foreignObject, styles/CSS, animation, `use`, namespaced attributes and external
resources. Only plain local fragment references and literal `url(#fragment)`
paint/clip references are admitted; CSS escapes and comments are refused.
Unsupported valid-but-outside-profile images fail closed rather than being
rewritten. This validates bytes and resource bounds, not visualization meaning.

`previews.json` is the deterministic native validator corpus. Reproduce with
`cargo test -p graphforge-discovery --test previews`; real CLI publication tests
also check PNG/SVG retrieval, unchanged package bytes, no-upload refusals,
clone-ignore behavior, and hostile callers bypassing client validation. WASM
export/parity and host memory qualification are owned by the binding follow-up.

### Negotiated OAuth, upload chunks and digest locations

A Hub using any of the following optional fields or templates must require
`publish-negotiation@1` in `requirements`. The format remains
`graphforge-hub-publish/1`: older readers check required capabilities before
strict document decoding and reject this extension as `unsupported_future`.
Legacy documents omit these fields and retain their existing behavior.

- `authorization.client_id` is a nonempty identifier of at most 128 UTF-8 bytes,
  with no whitespace or control characters. The CLI sends the same identifier
  to device authorization and token polling; omission uses `graphforge-cli`.
- `limits.preferred_chunk_bytes` is positive and no greater than
  `max_chunk_bytes`. The CLI uses that size, including after a non-aligned resume
  offset, and sends a shorter final chunk. Without a preference it uses the
  smaller of 1 MiB and the Hub maximum. The CLI owns one upload buffer with an
  explicit 95 MiB resource budget; larger preferences fail before opening a
  session rather than being silently clamped. Request-body deadlines scale from
  the existing per-MiB budget; connection and response bounds do not change.
- A client accepting this extension echoes `publish-negotiation@1` in the
  session-open request's `requirements`. The Hub may then include a positive
  `uploads[].chunk_bytes`, at most `max_chunk_bytes`, chosen for that object's
  declared length. It overrides the capabilities preference and uses the same
  client buffer budget and timeout scaling. Omission preserves the capabilities
  preference or legacy default. Explicit JSON null is invalid. A Hub must omit
  the field for clients that do not request the extension; the CLI rejects it
  unless the capabilities document requires the extension. The ReferenceHub
  chooses the smaller of its preference and the object's declared length and
  preserves that size when the same session resumes. The client starts at the
  exact retained offset and uses full selected chunks followed by a final
  remainder; uniform storage-part semantics are specified separately.
- `object_location_template` contains exactly one `{digest}` or `{hex}` and no
  other braces. `{digest}` expands to `sha256:<64 lowercase hex>`; `{hex}` expands
  to the same 64 lowercase hex digits without the prefix. For example,
  `https://data.example/objects/sha256/{hex}`. Credential-free HTTPS and the
  existing query/fragment restrictions still apply. Commit validation uses the
  exact expanded location, not a second spelling of the digest.

The checked-in conformance corpus includes a negotiated reference Hub with a
provider-style client identifier, 8 MiB preferred chunks and `{hex}` locations,
and exercises provider OAuth, per-object session sizes and object uploads.

## Large objects and bounded commit verification

`large-object-publish@1` selects ADR 0053's asynchronous policy. It is a required
capability, so older readers reject the document before using new semantics.
The Hub also requires `publish-negotiation@1`; `preferred_chunk_bytes` is the
uniform size of every nonfinal PUT, at most 95 MiB. Per-object sizes are the
smaller of that size and object length. Offsets must be aligned, and at most
10,000 parts fit an object; `max_object_bytes` declares the practical ceiling
(625 GiB at 64 MiB parts, or a smaller configured bound).

Commit returns HTTP 202 `{ "state": "pending", "operation_uuid": "…" }`.
Poll the existing operation endpoint with fresh valid authorization until its
bound receipt is present. Refs remain unchanged during verification. The
ReferenceHub rehashes at most 64 MiB per poll, then checks the expected revision
and applies the publication atomically. Its sparse file-backed storage permits
actual generated large fixtures without a proportional RAM/disk allocation.
The ordinary conformance corpus includes asynchronous commit; an explicit
operator-scale test (explicitly opt-in under AGENTS.md’s existing operator
qualification policy, not default PR CI) traverses actual 200 GiB source bytes, 3,200 real 64 MiB
PUTs, and full stored-byte verification through the same implementation:

```sh
cargo test -p graphforge-hub-publish --release \
  actual_200_gib_object_publishes_in_uniform_64_mib_parts -- --ignored --nocapture
```

This is a protocol object fixture, not a 200 GiB native Project or a deployed
Worker performance result. It records the actual maximum request time against
the existing 300-second request budget. The buffered reference GET adapter
refuses responses larger than 95 MiB rather than allocating object-sized RAM.

The wire types, parsers and validators remain portable Rust. Native-only
`ReferenceHub` and its temporary-file dependency are excluded on `wasm32`; a
WASM validator consumes the same wire/corpus verdicts, not a filesystem-backed
reference HTTP server. `@curatelabs/graphforge-protocol-wasm` is that validator:
its tests replay the control documents of this corpus, including the request
commitment and the commit-time manifest checks (`check_commit_manifest`).

## Internal host item admission

`graphforge_hub_publish::item_admission_requirements(&manifest)` derives the
sorted canonical per-ID query keys and distinct `(version, content_digest)`
tuples. `plan_item_admission(&ItemAdmissionInput)` evaluates fresh host facts
using the same implementation as the reference Hub. The protocol-WASM package
exports both functions and generates their TypeScript declarations from Rust.
[The input schema](item-admission.schema.json) also defines requirements and
plan shapes; [the native corpus](item-admission-conformance.json) records the
same decisions and sanitized failures checked through Node and Workers.

These are internal host inputs. An HTTP caller must never provide authorization
or ownership facts. The publisher account comes from the verified **scoped
publication account**, independently of the login principal, repository owner
handle or client JSON. `StableAccountId` is an opaque nonempty UTF-8 ownership
key of at most 4096 bytes, without normalization or control characters. A
canonical display handle follows the existing 100-byte repository-owner grammar.
Renames, handle reuse and repository moves do not change stable item ownership.
The reference Hub uses a separate synthetic account namespace for conformance;
its explicit account-binding hook is not a production identity provider.

First derive requirements natively, then read exactly one claim row per
canonical ID and one explicit `published` membership boolean per queried exact
tuple. The host need not load any other item history. A missing row is an error;
`claim: {state: "unclaimed"}` is explicit database evidence of absence. Extra,
duplicate, noncanonical, incomplete and inconsistent facts fail closed. An
unclaimed row cannot advertise visibility, an organization hint or published
tuples. Claimed rows use stable account IDs. Visible owners require their
current authorized canonical handle; hidden handles, even if supplied internally,
never appear in a refusal. `publish_as_owner` is independent current evidence
that the **authenticated actor**, under a valid lease, may publish as the owning
organization. Membership alone is insufficient and the hint does not authorize
changed content under a foreign publication account.

Only this commit's manifest advertisement participates. Legacy IRIs are ignored,
known permanent IDs of the wrong advertised kind are refused, and aliases query
one canonical bare ID. The inventory remains bounded to 4096 modules and 4096
bridge sets, with at most 8192 fact rows and 8192 distinct tuple facts overall.
Strings retain the 4096-byte discovery bound. Both native typed entry points and
WASM enforce a 16 MiB serialized byte bound; JSON also enforces the incoming byte
bound and rejects duplicate members. Invalid admission/query inputs use the
existing sanitized `invalid_input` publish code; future requirements retain
`unsupported_future`. Existing discovery export error behavior is unchanged.

A new ID yields `new_claim`; its owner may append `owner_update`, including a new
content digest under the same authored version. A foreign already-published
exact tuple yields `exact_reuse`, with **no ownership or exact-membership write**.
The successful host transaction still commits its new publication, actual
repository, refs revision and manifest witness. This does not choose a source
for digest-free item resolution. Any changed foreign tuple returns the existing
HTTP 409 `id_taken` body with sorted, unique, privacy-filtered native `items` and
no partial plan. Protocol-WASM preserves these typed items in its existing
`ProtocolError`; unrelated failures omit them.

The host must obtain complete current facts, recheck scoped account grants,
organization leases, membership and entitlement, and apply the plan with claims,
objects, refs, manifests, real publication witnesses and receipt in **one atomic
mutation**. Recheck after any asynchronous verification or race; a plan is not
cached authority or a reservation. Commit failure must leave all state unchanged.
Completed idempotent replay uses the original receipt and adds no claims.
Reference tests exercise account/grant/session freshness and existing commit
rollback/replay; production entitlement and lease enforcement remain host
responsibilities.

The admission schema and decision corpus are regenerated by the native
`item_admission` integration test with
`GRAPHFORGE_UPDATE_ITEM_ADMISSION_ARTIFACTS=1`, then checked without that variable.
