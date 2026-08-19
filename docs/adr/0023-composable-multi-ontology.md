# ADR 0023: Composable ontology modules and semantic bridges

**Status:** Accepted  
**Date:** 2026-08-19  
**Build target:** v0.5.x (M9)  
**Related:** #833, #834; portable representation decision #835

## Context

A GraphForge project currently adopts at most one ontology and one project-wide
enforcement mode. Combining independently governed research, document,
genealogy, scientific, provenance, and evidence domains by flattening their
names would discard authorship, make collisions order-dependent, and prevent
independent upgrades.

## Decision

A project owns an ordered-by-identity inventory of immutable ontology modules.
A module identity is the tuple `(ontology_id, authored_version,
canonical_digest)`. Qualified symbols include that exact identity and a symbol
kind and local ID. Runtime catalog IDs are local execution accelerators and are
never substituted into semantic identity.

The committed project authority selects exact modules, independent bridge sets,
and per-module/per-bridge activation modes. Composition closure and its digest
are deterministic functions of those selections and required dependencies;
serialization or insertion order has no semantic precedence. Bridge sets are
versioned, provenance-bearing artifacts owned independently from their source
modules and never rewrite them.

All lifecycle changes are previewed against a committed generation and publish
atomically as a new generation. Import only stages candidates; adoption is a
separate explicit authority transition. The Rust core owns resolution,
validation, migration, diagnostics, and identity. Bindings and CLI project that
behavior without reimplementing it.

The normative types, state transitions, errors, limits, and examples are in the
[multi-ontology contract](../book/architecture/composable-multi-ontology.md).
That contract deliberately freezes portable semantic requirements but not a
portable-v2 encoding. #835 must decide whether the existing authenticated
compatibility/schema mechanism can represent them before #841 commits bytes.

## Rejected alternatives

- Matching unqualified names does not establish equivalence.
- Flattening modules, last-writer-wins, and inventory-order precedence are
  forbidden.
- Enforcement does not belong to reusable module documents.
- Import never implies adoption, and a bridge never mutates either endpoint.
- A general OWL reasoner, heterogeneous ontology federation, and a hosted
  registry are outside this decision.

## Consequences

Plans, Arrow/Parquet metadata, generation records, and reopen logic carry a
composition fingerprint rather than one ambiguous ontology version. Legacy
single-ontology projects migrate explicitly to a one-module inventory without
changing their authored ontology digest. Failures are typed, bounded, and
phase-specific, and failed previews/imports/adoptions leave authority intact.
