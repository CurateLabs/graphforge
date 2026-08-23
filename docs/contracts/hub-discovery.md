# GraphForge Hub discovery protocol

GraphForge owns the semantic authority for Hub discovery. A Hub is a conforming
server. TypeScript consumers use the checked-in JSON Schemas and the fixtures in
`tests/fixtures/hub-protocol/v1`; they must not redefine validation rules.

The stable human identity is `https://graphforge.sh/{owner}/{repository}`. The
same repository exposes `/.gf/manifest` and `/.gf/refs`. Provider names, bucket
names, credentials, and object locations do not participate in repository
identity. Object bytes are fetched from each validated `location`, never from
the human HTML route.

`graphforge-hub/1` manifests reference both a `graphforge-project/2` semantic
package digest and the transport digest of its canonical portable-v2 bundle.
The bundle descriptor must use `application/vnd.graphforge.project.v2+tar`.
Discovery validation establishes repository, version, transport object,
and reader-capability metadata only. The portable-v2 verifier remains the sole
authority for package integrity, semantic compatibility, and authenticity.

Readers reject duplicate JSON keys, unknown fields, non-canonical repository
slugs, unknown required capabilities, future protocol majors, non-SHA-256
digests, and unsafe object locations before object access. Responses are bounded
to 4 MiB, 100,000 entries, and 4,096 bytes per string by default.

Canonical Rust structures serialize deterministically by declared field order;
maps use lexical key order. The conformance corpus includes current, minimal,
future, duplicate, unknown-field, unsafe-location, and integrity-failure cases.
The integrity-failure fixture is syntactically valid: consumers must compare the
descriptor size and digest with downloaded bytes and return
`integrity_failure` without publishing partial state.

Stable normalized error codes are `invalid_identity`, `malformed_response`,
`unsupported_future`, `missing_ref`, `missing_object`, `integrity_failure`,
`unsafe_location`, and `limit_exceeded`. Diagnostics and telemetry must not
contain owner/repository slugs, object locations, credentials, tokens, local
paths, manifests, or graph data.
