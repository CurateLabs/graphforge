---
title: "ADR 0042: Contextual research decisions extend immutable knowledge"
adr: "0042"
status: "Accepted"
date: "2026-09-22"
superseded_by: null
---

# ADR 0042: Contextual research decisions extend immutable knowledge

## Context

Issue #1353 requires competing research claims and contextual canonical acceptance
without turning confidence, supported status or Branch membership into authority.
Existing assertions, evidence, provenance, status events, supersession and
hypotheses already own their respective immutable records. Branches now publish
selected research through their owning Project's CURRENT under ADR 0041.

## Decision

Extend the knowledge/epistemic schema registry with native immutable research
classification, relation and scoped decision records. Reference existing
assertion and provenance identities; do not duplicate claim text, evidence,
confidence or hypothesis membership in a parallel claim engine. Evidence remains
in the existing Source/Artifact/evidence owners. Research categories distinguish
machine extraction, analyst assertion, interpretation, hypothesis/theory and
annotation without changing the meaning of existing statusless assertions.

Canonical decisions identify Project authority, optional community, research
context and typed subject (node, relationship or assertion). Acceptance/revocation
history is independent of confidence and supported/disputed status. A Branch
challenge or suppression is scoped research history; it cannot retract or
rewrite its parent's assertion or canonical decisions. Immutable successors
record revisions and replacement without changing prior assertion bytes or
conceptual origin.

Claim relations explicitly represent alternative-to, contradicts/disputes,
refines, supersedes and supports. Existing supersession remains the owner for
assertion succession. Competing claims can coexist. Import/integration and
canonical promotion are separate decisions; source-context acceptance is
provenance and never implicitly grants destination acceptance. A later Proposal
publication may compose both explicit decisions through the same authority.

Rust derives Arrow views of current decisions and full history. Default graph
Cypher and algorithms remain unchanged; only explicit knowledge/belief projection
uses epistemic decisions. Knowledge suppression affects its scoped knowledge
view, never all graph objects referenced by a claim. Read surfaces distinguish
inherited/source decisions from decisions made in the requested destination
context and preserve creator, run, evidence, origin Branch and exact Version.

Canonical acceptance/integration decisions are current Project history, scoped
by authority and context, not frozen research state. Their data-bearing owner
records belong to the knowledge schema registry and publish as Parquet under
the existing research history capability. Complete Project or Branch restoration
must preserve this history, just as it preserves operation receipts. Frozen
claim classification, relation and local-change records remain epistemic research
content. Reads present these two layers explicitly; restored old research never
rewrites later canonical history. A source-context decision is still inspectable
provenance but cannot become a destination-context decision by inheritance.

Project operations use existing native participant publication. Branch operations
prepare the same domain changes in the private Branch facade and publish one
new Branch Version through the existing Project CURRENT/receipt protocol. There
is no second durable authority. Exact request identity, cancellation before
publication, structured conflict refusal, recovery and receipt reconciliation
remain mandatory. Python, asynchronous Node and CLI only transport the native
request and Arrow/control results.

## Alternatives

Adding canonical values to assertion supported status would be smaller but would
conflate independent decisions and silently change existing projection semantics.
A new standalone claims store would duplicate assertion/evidence ownership and
introduce another publication authority. Both alternatives are rejected.

## Consequences and proof

Selection, Bring, retention, schema validation and field-baseline owners must
carry selected research records without importing unrelated context or granting
source authority at a destination. Added families need explicit registry entries,
closed values, bounds, canonical fingerprints and fixture/inventory updates.

Acceptance requires real parent and Branch claims with competing alternatives,
separate integration/promotion, immutable revision history, graph-invariant
knowledge suppression, exact retry/failure refusal, durable reopen and native
binding parity. This ADR is a design decision; it is not completion evidence for
#1353. Proposal review workflow remains owned by #1356.
