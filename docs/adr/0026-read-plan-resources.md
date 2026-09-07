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

Schema discovery remains in lowering for this slice. Mutation nodes retain their
current seam until their own #1008 family lands. Existing operator names, Arrow
schemas, checked-ID encodings and durable formats do not change. No durable plan
serialization codec is introduced by this internal resource boundary.

## Consequences

Read plans can be compared and rebound independently of their original project
location. The execution boundary must validate semantic assumptions and resolve
all read sources, including property and semantic relation providers. Tests must
cover relocated complete plans, simultaneous bindings, retained streams, missing
authority and incompatible schemas, as well as ordinary query parity and #1094
bounded traversal. The change does not authorize eager graph scans to construct
the execution context or a fallback to the working directory.
