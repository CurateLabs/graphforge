# ADR 0026: Read plans bind resources in execution

**Status:** Accepted for implementation
**Date:** 2026-09-07
**Related:** #1136, #1008, ADR 0025

## Context

Fixed and variable expansion embed paths and ontology mode. Read scans retain
storage providers and `nodes(path)` retains a hydration directory. DataFusion
54.1 TableScan equality excludes its source, so provider ownership is a binding
problem even when scan equality already ignores location. Expansion and hydrated
expression equality do include their paths.

## Options

Keeping paths inside opaque handles would preserve the coupling. A global or
thread-local resolver would make simultaneous and retained queries ambiguous.
Removing schema discovery from lowering would expand this change into #1006.

## Decision

Read logical nodes and sources describe the query's current graph resource and
its semantic schema, never a filesystem location or provider address. The
current-graph role is deterministic across equivalent plans. Existing checked
IDs, relation names, output schemas and composition assumptions remain semantic
contracts; they are not substitutes for a pinned execution authority.

An exec-owned SessionConfig extension owns the directory, mode, catalog and
existing adjacency/ordinal resources. Physical planning resolves logical scan
descriptors and read expressions against that session before executing them.
Missing or incompatible bindings fail closed. Physical plans and owned streams
retain their bound resources; another session cannot retarget them.

Schema discovery remains in lowering. The mutation family uses the same resource
boundary with distinct write admission, as described below. Existing operator
names, Arrow schemas, checked-ID encodings and durable formats do not change.
No durable plan serialization codec is introduced by this internal resource boundary.

## Consequences

Read plans can be compared and rebound independently of their original project
location. The execution boundary must validate semantic assumptions and resolve
all read sources, including property and semantic relation providers. Tests must
cover relocated complete plans, simultaneous bindings, retained streams, missing
authority and incompatible schemas, as well as ordinary query parity and #1094
bounded traversal. The change does not authorize eager graph scans to construct
the execution context or a fallback to the working directory.

## Mutation family completion (#1008)

CREATE, DELETE, SET and REMOVE use the same deterministic current-graph role.
Their logical nodes retain checked identities, expressions, output shape and
semantic binding requirements. Paths, ontology mode and executable routing maps
belong to an explicit execution-owned write context. Read authority alone does
not grant write authority. Missing, read-only or incompatible write bindings fail
before any mutation, including literal-only statements.

The existing statement driver remains the owner of ordering, buffered visibility,
commit/rollback and write receipts, including MERGE. Physical extension planning
and the driver share the admitted session authority; neither recovers a target
from a logical node. Runtime ID/name mappings used during lowering are semantic
assumptions to validate, rather than an executable provider registry.

All logical extension equality and hashing use their semantic content. Literal
and computed property distinctions, composition fingerprints, and output modes
remain significant; filesystem placement and execution policy do not. This does
not introduce a persisted plan codec or change public Arrow/IR encodings.

Mutation literal keys normalize signed zero and make NaNs reflexive while
preserving their payload bits, recursively through lists, maps and spatial
coordinates. Computed expressions retain DataFusion's own equality/hash contract.
This policy is local to logical plans; public literal equality and serialization
are unchanged. All eleven extension nodes derive equality and hashing, with the
shared literal adapter confined to resolved mutation specifications.
