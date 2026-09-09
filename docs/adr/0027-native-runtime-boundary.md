# ADR 0027: Native GraphForge execution boundary

**Status:** Accepted
**Date:** 2026-09-09
**Decider:** Project maintainer
**Related:** [#495](https://github.com/CurateLabs/graphforge/issues/495), [#1198](https://github.com/CurateLabs/graphforge/issues/1198), [ADR 0001](0001-rust-core.md)

## Context

GraphForge targets native graph computation through its Rust API, Python data
science workflows, Node applications and servers, and CLI. Rust owns behavior;
bindings project the same engine and Arrow results.

Issue #495 proposed a second execution environment inside browsers and Web
Workers, initially with in-memory storage and a selected query/algorithm profile.
That would require a distinct memory, scheduling, concurrency, persistence and
compatibility contract. Sharing Rust source does not make browser constraints
identical to the native runtime or remove the additional testing and maintenance.
The proposal has not established a browser-only user requirement sufficient to
justify that commitment. Local or private computation does not inherently require
execution inside a browser; native applications can keep data on the user's device.

## Options considered

- Maintain a browser/Worker WASM engine with its own supported feature profile,
  enabling browser-only computation but adding another execution and validation target.
- Keep GraphForge execution native and let browser applications consume its results,
  preserving the native product focus but requiring a native host for GraphForge queries.

## Decision

Keep GraphForge execution native. Do not maintain or promise a browser/Worker
WASM GraphForge engine, binding, reduced query profile, or browser storage backend.
Close #495 as not planned and remove its milestone assignment. Leave M15 open
and available for reassignment rather than presenting WASM as scheduled work.

Browser applications may consume results produced by native GraphForge. This
requires neither a browser engine nor moving visualization into GraphForge Core.
This decision does not introduce a server, transport, or mandatory cloud service.
Rust remains the semantic owner and Python and Node remain thin native bindings.

Reconsider browser execution only with a concrete browser-only user requirement,
explicit resource and persistence guarantees, a supported API/algorithm profile,
and an agreed validation and maintenance budget. A new ADR must approve that
change before it becomes a supported target or roadmap commitment.

## Consequences

Native runtime work does not acquire browser compatibility or parity obligations.
Browser-only offline GraphForge execution and zero-backend GraphForge demos are
not supported product promises. Consumers requiring GraphForge computation need
a native runtime, local or remote according to their own deployment requirements.

This is a product support boundary, not a claim that Rust cannot compile to WASM.
It requires no runtime code removal and makes no new decision about unrelated
bindings. It supplements ADR 0001's Rust ownership rule; it does not supersede it.
