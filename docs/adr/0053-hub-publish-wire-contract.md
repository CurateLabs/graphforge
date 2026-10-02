---
title: "ADR 0053: Hub publish wire contract"
adr: "0053"
status: "Proposed"
date: "2026-10-02"
superseded_by: null
revisit_when: "The control-plane publish session shape, data-plane upload URL policy, or ref precondition encoding needs a breaking wire change"
---

# ADR 0053: Hub publish wire contract

**Status:** Proposed

**Implementation:** Wire contract stub in `graphforge-hub-publish`; control-plane HTTP, credential flow, CLI `gf publish`, and conformance fixtures follow in later slices.

**Related:** #1749, #1748, #906, ADR 0021 (portable project v2), ADR 0030 (portable OCI boundary), ADR 0038 (determinism at the publication boundary), ADR 0039 (research version publication), ADR 0045 (ingest authentication regime), ADR 0049 (published payload checksums), `graphforge-discovery` (clone/read contract).

## Context

`gf clone` consumes a Hub's discovery manifest and downloads digest-addressed objects from Hub-provided locations. Only operators with control-plane credentials can publish today. Product work needs a provider-neutral **publish** contract: the client derives the portable package and discovery descriptors locally; the Hub stores bytes and advances refs, never re-deriving semantics.

Publication must be idempotent on `(operation_uuid, request_commitment)` and must refuse changed content under the same identity with `GF_IDEMPOTENCY_CONFLICT`. Ref updates require an expected-revision precondition. Large objects upload directly to data-plane locations the Hub allocates, with resumable, length- and digest-verified transfer bounded by declared limits.

## Decision

**Rust owns the contract.** `graphforge-hub-publish` holds publish wire constants, structured error codes, transport traits, and an in-memory Hub for tests. It depends on neutral digest and limit types only; it does not open project files, run the portable verifier, or perform HTTP.

**Two planes.** The control plane admits a publish session (scoped credential, repository identity, object inventory, ref targets, preconditions). The data plane receives object bytes at Hub-issued locations. Control-plane responses never carry unbounded object payloads.

**Object admission.** Each inventory entry names `object_digest`, `media_type`, and declared `length`. The transport stores immutable bytes keyed by digest. Admission verifies length and digest before any ref moves. Checksum policy for graph payload bytes follows ADR 0049 where applicable.

**Idempotency.** Retries with the same operation identity and the same canonical request commitment succeed with the original receipt. A conflicting commitment under the same operation identity fails with `GF_IDEMPOTENCY_CONFLICT` and performs no ref advance.

**Ref preconditions.** Advancing `default` (Project package), an immutable Version ref, a Branch head, or creating a Fork repository identity requires an `expected_revision` (or explicit create-if-absent) precondition. Blind last-writer-wins is rejected.

**Stable errors.** Publish failures classify at least: authentication or credential denial, entitlement or quota denial, idempotency conflict (`GF_IDEMPOTENCY_CONFLICT`), ref or revision conflict, unsupported protocol or package version, and integrity or digest failure. Native GraphForge surfaces project these codes without lossy remapping.

**Wire format.** The publish protocol format identifier is `graphforge-hub-publish/1`. Session documents and receipts are versioned JSON with closed required capabilities; unknown required capabilities fail before side effects.

## Consequences

- CLI, Python, and Node publish through the same Rust contract and memory Hub used in conformance tests.
- Discovery manifests remain the read-side authority; publish receipts bind to the same digest-addressed object model.
- Full HTTP mapping, browser/device credential acquisition, and Hub server behavior are explicitly out of this stub and land in follow-up work.

## Alternatives

Embedding publish types in `graphforge-discovery` would couple read and write evolution. Keeping publish only in the CLI would forfeit shared conformance and binding parity.
