---
title: "ADR 0063: Author-declared ontology version precedence"
adr: "0063"
status: "Accepted"
date: "2026-11-06"
superseded_by: null
revisit_when: "Authors need precedence beyond a u32 ordinal, or a released descriptor contract requires negotiated ordering semantics"
---

# ADR 0063: Author-declared ontology version precedence

## Context

Core #2313 supplies native ordering for the Hub contract in #2286. Authored
versions are opaque exact labels; historical modules can contain labels such as
`v\n1`. Parsing SemVer, dotted/natural text or publication time cannot recover
an author's intended precedence. Exact identity remains `(id, version, digest)`.

## Decision

A module may declare `version_order: u32`. Present declarations enter its
existing canonical document digest. Omitted or input `null` declarations preserve
historical bytes and digests. Metadata-only changes require no graph transform.
No declaration is inferred or mandatory. Bridge ordering is outside this contract.

Rust discovery compares `DeclaredModuleVersion { module: ExactIdentity,
version_order? }` within one canonical permanent ontology URI. Identical exact
identities and declarations compare equal, including two absent declarations;
contradictory declarations for an identical exact identity fail integrity checks.
Otherwise two ordinals compare numerically, ties mean equal precedence only,
and a missing declaration means unordered. Cross-ID comparison is invalid.
Structure validation does not authenticate a declaration: callers bind it to
verified module bytes. Resolution candidates sharing a digest must agree.

Verified package summaries project the ordinal under the existing explicitly
unreleased `ontology-module-metadata@1` capability. `graphforge-item/1` gains an
optional ontology-only top-level ordinal; its snapshot and exact identity fields
are unchanged. Native fetch compares it with the verified downloaded document,
including absent/present mismatches, before producing output.

This is an intentional in-place descriptor extension, using the documented
pre-v1 absence of a compatibility guarantee in the contributor guide and the
existing closed-reader refusal boundary. It is not a claim that all public
validators are unreleased or that old readers understand the addition. Old
closed readers refuse descriptors with the new field as `malformed_response`;
unknown format majors continue to fail `unsupported_future`. Publishers must
deploy matching WASM/schema/native readers before serving declared descriptors.
No new format major or capability framework is introduced for this optional
field. Absent-field descriptors remain byte-for-byte reproducible.

## Consequences

Native and WASM share one bounded comparison implementation. Arbitrary labels
can receive deliberate precedence without changing their identity text. Historical
pairs with distinct identities and no declarations remain unordered; the Hub
owns presentation and deployed consumption under #2286. Tied ordinals never
justify deduplicating versions. Adding or changing an ordinal creates new
content and cannot silently rewrite an immutable publication.
