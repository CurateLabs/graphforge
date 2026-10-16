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
| Repository moved since it was read; Fork target exists | `hub.publish.ref_conflict` | 2 |
| Unsupported protocol major or capability | `hub.publish.unsupported_future` | 2 |
| Bytes disagree with digest or length; Fork origin not published | `hub.publish.integrity_failure` | 2 |
| Invalid selection or request | `hub.publish.invalid_input` | 2 |
| Non-HTTPS, credentialed, or private-address URL | `hub.unsafe_location` | 2 |
| Transport failure or Hub internal error | `hub.network`, `hub.publish.internal` | 3 |

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
  or `corrupt_object`.

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
reference HTTP server.
