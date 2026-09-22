---
title: "ADR 0040: Frozen Slices reference retained Version context"
adr: "0040"
status: "Accepted"
date: "2026-09-22"
superseded_by: null
---

# ADR 0040: Frozen Slices reference retained Version context

## Context

A selection must be reproducible without becoming another independently mutable
research store. Branch creation and portable export need exact starting
membership and evidence/ontology context. A shared character does not imply
selection or indefinite retention of every connected story.

## Decision

Rust owns bounded Slice evaluation against one pinned current generation or an
explicit retained Version. Query and filter use the native streaming executor;
traversal consumes native topology and records canonical breadth-first
predecessors. Text search uses the existing projection, analyzer and Tantivy
backend with an ephemeral index outside the immutable generation.

Freeze requires an explicit retained Version. Its Arrow capsule records exact
active objects, required references, boundaries, typed labels and inclusion
reasons. Schema metadata commits to the Version content, original selector,
ontology participants and selected Artifact evidence disclosures. A capsule
establishes no new retention root or independent graph copy. It remains
inspectable after payload release; expanding or consuming its research content
requires the separately retained historical authority. A future Branch owner
must validate the source and establish its own selected retention dependencies.

No persistent Slice capability or registry is introduced. This avoids a second
mutable authority and keeps graph-bearing data in Arrow. Capsule fingerprints
detect inconsistent content; they are not signatures or access authorization.
Consumers must validate referenced objects against the source Version before
creating authoritative research state.

Continuations bind source, request identity, page family, selection and frozen
context. Current-head movement rejects old cursors instead of mixing snapshots.
Frozen inspection never reruns a selector, including nondeterministic queries.
Explicit revision retains exact previous membership and recomputes dependency
closure at the original or deliberately chosen historical Version.

## Consequences

A caller chooses retention lifetime explicitly. Freezing a Slice of a complete
Version does not compact that Version into a selected projection; Branch and
export owners provide those operations. A projected Version exposes its source
as outside genealogy only, and cannot silently read live ancestor bytes.

Collection, query-pool, row and final IPC limits fail without partial results.
The decoded-row limit is not a physical query-work budget. Historical Version
materialization may use private process storage; this does not claim zero-copy
reads. Independent Branch mutation and interchange packages remain separate
issues.
