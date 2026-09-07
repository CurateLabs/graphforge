# ADR 0030: Portable OCI protocol boundary

Status: Accepted for implementation under #1021.

## Context

Storage currently owns OCI HTTP requests, credentials, signature policy, and
portable publish/pull orchestration. Moving only the HTTP client would leave
network authority in the durability layer. Discovery does not own upload or
portable package publication semantics. #1015 depends on this extraction and
retains the remaining non-OCI facade cleanup.

## Decision

Use a focused `graphforge-portable-oci` crate for the registry trait, HTTP and
memory implementations, wire encoding, credentials, and authenticity evaluation.
It depends on neutral portable contracts in `graphforge-core`, never storage or
API. Move the existing publish/pull workflow into API, preserving verification,
transport, authenticity, and destination publication order.

Storage owns local package verification and bounded local byte staging and
publication. Its input boundary uses `Read` rather than a registry client: this
fulfills the storage-facing trait requirement without giving storage credentials,
registry routing, or signature policy. API connects the registry's bounded bytes
to that local input. No generic transport framework is introduced.

Passive portable reports, limits, errors, and their composition metadata have one
owner in core. Preserve serialized fields, tokens, digest construction, sanitized
errors, and allocation/recovery evidence. Temporary storage re-exports of neutral
types preserve callers without reversing dependencies. OCI orchestration callers
move to API; no storage-to-API compatibility forwarding is allowed.

## Alternatives

A client-only extraction leaves authentication orchestration in storage. Moving
OCI into discovery couples unrelated discovery and upload responsibilities.
Neither provides a smaller complete boundary than the focused protocol crate.

## Consequences and verification

This changes Rust source ownership, not durable package or signature formats.
Cargo, Bazel, and crate publication inventories must include the new crate.
Existing protocol tests move with their owner; facade tests exercise real local
HTTP and durable publish/pull/verify/import/reopen behavior. Test bounded response
families, redacted credential failures, cancellation, digest failures, and
destination cleanup. Do not infer HTTP behavior from the memory backend alone.

Any verified existing boundary defect required for these tests is repaired
explicitly with a regression; do not conceal it as a mechanical move. #1015's
non-OCI facade and CLI cleanup remains outside this change.

## Rust caller migration

The API facade's publish/pull entrypoints retain their signatures and serialized
results. Rust callers previously importing `HttpOciRegistry`, `MemoryOciRegistry`,
or `PortableV2OciRegistry` from storage use `graphforge-portable-oci` (also
re-exported by API). Injected publish/pull requests are API-owned and available at
its crate root. Storage-level OCI workflow calls move to API's existing
`publish_portable_v2_oci_with_registry` and `pull_portable_v2_oci_with_registry`.
Passive package and OCI types are defined in `graphforge_core::portable`; existing
storage package-type re-exports remain while #1015 completes the facade cleanup.
No durable migration, registry re-upload, or signature regeneration is needed.

The extraction explicitly repairs three local ownership failures: a preexisting
stage is never removed after exclusive creation fails; errors after staging drop
only the owned stage; destination publication uses the existing atomic no-replace
primitive. Upload locations must resolve to the configured origin before any
credential-bearing upload. The existing 10 MiB HTTP response limit remains fixed.
