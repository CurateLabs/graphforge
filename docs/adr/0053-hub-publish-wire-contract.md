---
title: "ADR 0053: Hub publish wire contract"
adr: "0053"
status: "Accepted"
date: "2026-10-02"
superseded_by: null
revisit_when: "The control-plane publish session shape, data-plane upload URL policy, or ref precondition encoding needs a breaking wire change"
---

# ADR 0053: Hub publish wire contract

**Status:** Accepted. `gf publish` ships on this contract.

**Implementation:** `graphforge-hub-publish` holds the `graphforge-hub-publish/1`
documents, error classification, request commitment, and `ReferenceHub`, an
in-memory implementation of the whole mapping below. `gf publish`
(`crates/graphforge-cli/src/hub_publish.rs`) is the client; it shares HTTPS
plumbing with `gf clone` (`hub_http.rs`) and discovery derivation with the Hub
fixture generator (`hub_publication.rs`). Python and Node reach it through the
same Rust CLI entry point as `gf clone`.

**Related:** #1749, #1748, #906, ADR 0021 (portable project v2), ADR 0030 (portable OCI boundary), ADR 0038 (determinism at the publication boundary), ADR 0039 (research version publication), ADR 0045 (ingest authentication regime), ADR 0049 (published payload checksums), `graphforge-discovery` (clone/read contract).

## Context

`gf clone` consumes a Hub's discovery manifest and downloads digest-addressed objects from Hub-provided locations. Only operators with control-plane credentials can publish today. Product work needs a provider-neutral **publish** contract: the client derives the portable package and discovery descriptors locally; the Hub stores bytes and advances refs, never re-deriving semantics.

Publication must be idempotent on `(operation_uuid, request_commitment)` and must refuse changed content under the same identity with `GF_IDEMPOTENCY_CONFLICT`. Ref updates require an expected-revision precondition. Large objects upload directly to data-plane locations the Hub allocates, with resumable, length- and digest-verified transfer bounded by declared limits.

## Decision

**Rust owns the contract.** `graphforge-hub-publish` holds the publish documents, structured error codes, the request commitment, and the reference Hub. It contains no HTTP client and opens no project files. It uses `graphforge-discovery` read-only to validate the manifest a commit publishes.

**Two planes.** The control plane (capabilities, sessions, commit, credentials) carries bounded JSON. Object bytes go only to data-plane upload URLs the Hub issues.

### HTTP mapping

`{repo}` is the repository URL, `https://<hub>/<owner>/<repository>`. JSON
Schemas for every document are in
[`docs/reference/hub-publish/v1/`](../reference/hub-publish/v1/README.md).

| Request | Auth | Success | Purpose |
| --- | --- | --- | --- |
| `GET {repo}/.gf/publish` | none | `200` capabilities | Format, required capabilities, credential endpoints, object location template, limits |
| `GET {repo}/.gf/publish/operations/{operation_uuid}` | bearer | `200` operation status, `404` unknown | Classify a retry before deriving any package |
| `POST {repo}/.gf/publish/sessions` | bearer | `201` open, `200` resumed open or complete | Open (or resume, or replay) a publish session |
| `HEAD <upload_url>` | capability URL | `200`, `Upload-Offset`, `Upload-Length` | Bytes the Hub retained |
| `PUT <upload_url>` + `Content-Range: bytes a-b/len` | capability URL | `200` upload status, `Upload-Offset` | Append at the retained offset |
| `POST {repo}/.gf/publish/sessions/{id}/commit` | bearer | `200` complete | Atomically admit objects, advance refs, record the receipt |
| `GET {repo}/.gf/refs` | none | `200`, `ETag: "<revision>"` | Discovery refs (read side of `gf clone`) |
| `GET {repo}/.gf/manifest` | none | `200`, `ETag: "<validator>"` | Default ref's canonical discovery manifest |
| `GET {repo}/.gf/objects/{digest}` | none | `200`/`206`, strong `ETag`, `Range`/`If-Range` | Admitted object bytes |

**Capabilities.** `GET {repo}/.gf/publish` returns `format`
(`graphforge-hub-publish/1`), `requirements` (capabilities the client must
implement), `capabilities` (optional), `authorization`
(`device_authorization_endpoint`, `token_endpoint`, `scope`), an
`object_location_template` with one `{digest}` placeholder, and `limits`
(`max_object_bytes`, `max_objects`, `max_session_bytes`, `max_chunk_bytes`,
`max_document_bytes`). Endpoints come from this document, never from fixed
paths. A client fails `unsupported_future` on an unknown format major or
required capability before any other request.

**Operation status.** `{format, operation_uuid, repository, intent_digest,
receipt}` for an operation opened in this repository; `receipt` is `null` until
the operation commits, then the original receipt. A client calls it before
deriving any package: a different `intent_digest` is `idempotency_conflict`, and
a present receipt is the result of a retry, so a rerun of a committed
publication uploads nothing.

**Session open.** The body is `{format, operation_uuid, request_commitment,
intent_digest, repository, objects: [{digest, length, media_type}],
requirements?}` with objects
strictly ascending by canonical lowercase digest. The Hub checks, in order and
before any state change: bearer token; body size; `format` and `requirements`
(unknown → `unsupported_future`); structure; repository equals the URL; declared
counts and lengths against `limits` (`413`); the operation identity; entitlement.
An operation is one content identity, its `intent_digest`; a session is one
attempt at committing it, bound by `request_commitment`. For a known
`operation_uuid`: a different `intent_digest` is `idempotency_conflict`; a
committed operation returns its original receipt (`{state: "complete",
receipt}`); the same request returns the same session with current `received`
offsets; and a different request with the same intent supersedes the open
session, which is what a rerun after `ref_conflict` sends (it read a newer
revision). Superseding closes the old session and its upload URLs and answers
`201` with a new session; bytes the old session retained carry over by digest,
so identical bytes are never sent twice and a partial upload still resumes.
Otherwise the Hub answers `201 {state: "open", session_id, uploads: [{digest,
length, upload_url, received}]}`. Objects already stored in the same repository
start complete only if they still verify against their digest and length; a copy
corrupted at rest starts empty and must be uploaded again, and the commit
replaces it. Nothing is deduplicated across repositories.

**Uploads.** `upload_url` is a capability URL on the data plane. A client must
never send the publish token to it and must never log it. `PUT` appends exactly
at the retained offset; any other start is `416` with `Upload-Offset`. The body
is at most `max_chunk_bytes`, and the range total must equal the declared length.
When the last byte arrives the Hub verifies the digest; a mismatch discards the
bytes (`Upload-Offset: 0`) and fails `integrity_failure`. `HEAD` reports the
retained offset so an interrupted client resumes without re-sending.
`gf publish` sends chunks of at most 1 MiB (or the advertised
`max_chunk_bytes`, if smaller). Writes have no whole-request deadline; each
chunk body may take five minutes and every other phase one minute, so any uplink
of at least about 3.5 KB/s completes chunks, and a dropped connection loses at
most one chunk before the next run resumes. Reads keep `gf clone`'s
one-minute whole-request bound.

**Commit.** The body is `{manifest, refs, expected_revision}`.
`expected_revision` is required: the repository's current revision, or `null` to
create an absent repository. Under one critical section the Hub:

1. Recomputes the request commitment from the session inventory and the commit
   body. A mismatch is `idempotency_conflict`. A committed session returns its
   original receipt.
2. Verifies every inventory object is complete, and re-hashes it. A missing or
   corrupt object is `integrity_failure`.
3. Parses the manifest with `graphforge-discovery`. It must name the URL
   repository and list exactly the session inventory, each at the location the
   template gives. `refs` must include the manifest's `resolved_ref`.
4. Checks `expected_revision`. A mismatch, or `null` for an existing repository,
   is `ref_conflict`.
5. Advances every named ref to `target = immutable_version`,
   `validator = canonical manifest digest`, and computes the new revision.
6. Stores objects, manifest, refs, and receipt together.

Any failure leaves refs, objects, and receipts unchanged.

**Request commitment.** `request_commitment` is the SHA-256 of the canonical
JSON (compact, members sorted) of `{format, repository, intent_digest, objects,
manifest_validator, refs, expected_revision}`, where `manifest_validator` is the
discovery manifest's canonical digest. `PublishIntent::request_commitment`
computes it. The client derives it before opening, so the object location
template comes from the capabilities document.

**Intent digest.** The commitment binds bytes and the expected revision, which a
retry after a commit cannot reproduce (the revision has moved). `intent_digest`
is a client-defined digest of *what* the operation publishes, independent of
package bytes; the Hub stores it per operation, the commitment binds it, and the
operation status returns it. `gf publish` uses the SHA-256 of the canonical JSON
`{format: "graphforge-hub-publish-intent/1", repository, ref, version_uuid,
version_identity, project_uuid, project_content, fork_of}`, where
`project_content` is the manifest digest of the committed generation the Project
package is exported from. A component-selective package records its exporting
generation (`source_generation`), so the Project package changes exactly when
that generation does, and exports of one generation are byte-identical; the
client refuses to publish if the generation moves during the export. Its default
operation UUID is `hub_publish_operation(repository, ref, version_uuid,
project_content)`, so publishing the same Version of an unchanged Project to the
same ref again replays the original receipt, a changed Project is a new
operation, and an explicit `--operation-uuid` reused for another Version or a
changed Project fails `GF_IDEMPOTENCY_CONFLICT` before any export.

**Revision.** A repository's revision is the SHA-256 of its canonical `.gf/refs`
document. It is the `ETag` of `.gf/refs` and the value of `expected_revision`.
Publishing identical content again leaves the revision unchanged.

**Receipt.** `{format, operation_uuid, request_commitment, repository,
manifest_validator, refs, previous_revision, revision}`.

**Forks.** A Fork is a new repository identity published with
`expected_revision: null`; create-if-absent never overwrites. Its origin citation
travels in the research lineage document (#1748), uploaded as an ordinary
inventory object and referenced by the manifest. The Hub stores it verbatim and never
interprets it. Before uploading, `gf publish --fork-of` reads the origin
repository's published lineage and fails `integrity_failure` unless it lists the
cited origin Version with the same Project and identity digest.

### Client snapshot

`gf publish <owner/repo> (--ref <branch> | --version-uuid <uuid>)` derives the
whole next snapshot locally:

- **Packages.** The selected Version's research package
  (`export_research`, bundled), the Project package, the Project summary, one
  component-selective package per ontology module, and the lineage document,
  each verified after export. The Project package is every committed
  participant of the exported generation except the `research` capability: a
  `Custom` selection built from the `Complete` selection preview with research
  participants removed, so participants added later are published without a
  change here. `export_portable_v2` with `Complete` refuses a Project with
  research Branches, because its research registry carries operational heads
  and is not a single interchange archive (`portable_registry`). Research
  travels only through the research interchange packages, and `gf clone --ref`
  / `--version-uuid` fetch those Version packages, never the Project package.
  The Project package keeps the workspace research metadata and configuration
  that the Project summary is derived from (ADR 0051), so a published research
  repository's summary carries its title, licence and ontology mode. Its
  package class is `component-selective` and its summary reports
  `research_present: false`, which is exact: research presence for a
  repository is the manifest's `lineage` field (ADR 0052), not a Project
  package component.
- **Lineage.** `build_research_lineage_for_discovery` emits the selected Branch
  (named by `--ref`, matched to the local Branch label) and Version. Versions,
  Branches, and Proposals already published in the repository's lineage carry
  forward unchanged; their objects are already admitted, so they upload nothing.
- **Refs.** Every Branch ref the lineage describes, the default ref, and the
  resolved ref advance together to the new manifest, because one lineage
  document describes exactly one snapshot. `expected_revision` is the revision
  the client read, or `null` when the repository is absent. A first publication
  needs `--ref`; its ref becomes the default ref.

### Credentials

The client obtains a short-lived token with the RFC 8628 device flow: `POST`
`device_authorization_endpoint` with `client_id` and `scope`
(`publish:<owner>/<repository>`), show `user_code` and `verification_uri`, then
poll `token_endpoint` with the device-code grant, honouring
`authorization_pending` and `slow_down` (+5 s). `gf publish` bounds Hub-supplied
timings: the poll interval to 1–60 s and the whole wait to fifteen minutes, after
which it fails `auth_denied`. `expired_token`, `access_denied`, and
other OAuth errors end the flow with `auth_denied`. The token endpoint uses
RFC 6749 `{error}` bodies; every other endpoint uses `{code, message}`.

The token is held only in memory (`PublishToken` has no `Display`, `Serialize`,
or revealing `Debug`). It is never written to project files, configuration
participants, or logs. A CI job passes a token in `GRAPHFORGE_HUB_PUBLISH_TOKEN`
instead of running the device flow. `gf publish` runs the device flow only when
standard input and standard error are terminals; a captured invocation (the
Python and Node CLI shims) without that variable fails `auth_denied` before any
network request. The client sends the bearer only to the repository's own origin,
never to OAuth endpoints, upload URLs, or public reads. Every control-plane
request checks the bearer token's expiry and its scope against the URL
repository.

### Errors

Every non-OAuth error body is `{code, message}`. `message` is a fixed string
chosen by the Hub; it never echoes request input, tokens, or upload URLs. `gf
publish` classifies a failure only from a parsed `{code, message}` body and
prints its own fixed text, so a Hub message never reaches CLI output and a bare
status (for example a proxy's `409`) is a transport failure, never a publish
outcome. `GF_IDEMPOTENCY_CONFLICT` exits 1, `internal` and
transport failures (`hub.network`) exit 3, and every other code exits 2 with the
`hub.publish.*` code as the JSON `semantic_code`; an unsafe Hub, OAuth, or upload
URL is `hub.unsafe_location`.

| `code` | Default status | Other statuses | GraphForge code |
| --- | --- | --- | --- |
| `auth_denied` | 401 (missing, unknown, expired) | 403 (insufficient scope) | `hub.publish.auth_denied` |
| `entitlement_denied` | 403 | | `hub.publish.entitlement_denied` |
| `idempotency_conflict` | 409 | | `GF_IDEMPOTENCY_CONFLICT` |
| `ref_conflict` | 412 | | `hub.publish.ref_conflict` |
| `unsupported_future` | 422 | | `hub.publish.unsupported_future` |
| `integrity_failure` | 422 | | `hub.publish.integrity_failure` |
| `invalid_input` | 400 | 404 (unknown resource), 413 (over a limit), 416 (upload offset) | `hub.publish.invalid_input` |
| `internal` | 500 | | `hub.publish.internal` |

### Conformance

`docs/reference/hub-publish/v1/conformance.json` is a corpus of request/response
cases. The crate's `contract_artifacts` test regenerates it, then replays every
case through a fresh `ReferenceHub`. Another Hub can replay the same corpus
through its HTTP adapter.

## Consequences

- The CLI, Python, and Node publish through the same Rust contract that the
  reference Hub and the conformance corpus exercise.
- Discovery manifests stay the read-side authority. Publish receipts bind to the
  same digest-addressed objects and ref validators that `gf clone` verifies.
- A Hub must keep one critical section per commit and must re-verify objects
  there; it cannot advance refs ahead of verification.

## Alternatives

Embedding publish types in `graphforge-discovery` would couple read and write evolution. Keeping publish only in the CLI would forfeit shared conformance and binding parity. Letting the Hub rewrite object locations in a submitted manifest would change the manifest digest the client committed to, so locations come from the capabilities template instead. Accepting the publish token on upload URLs would send the credential to whatever data-plane host the Hub names, so upload URLs are capabilities instead. Publishing the Project package with the `DataComponents` profile dropped the workspace research metadata and configuration participants (they are `settings` components), so the derived summary of every published research repository had no title, licence or ontology mode. Exporting a `Complete` package with the research registry projected to the published Versions would put research in the Project package too, but it changes what a Project package's research component means, duplicates the Version packages, and is a storage-format decision (ADR 0044); revisit it only if a consumer needs research history from the Project package alone. Deriving the summary from the head Version's package instead would break the summary's binding to `manifest.package` (ADR 0051).
