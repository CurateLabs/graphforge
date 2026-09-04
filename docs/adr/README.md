# Architecture Decision Records

Contiguous ADR sequence for decisions that remain part of the shipped / current
GraphForge product architecture. Superseded tooling pins and roadmap-only ADRs
are not retained in this tree.

| ADR | Title | File |
| --- | --- | --- |
| 0001 | [Rust Core](0001-rust-core.md) | `0001-rust-core.md` |
| 0002 | [Recursive Descent + Pratt Parser for graphforge-cypher](0002-lr1-grammar.md) | `0002-lr1-grammar.md` |
| 0003 | [Progressive Ontology — Exploration First](0003-progressive-ontology.md) | `0003-progressive-ontology.md` |
| 0004 | [Graph-Native Adjacency Index](0004-adjacency-index.md) | `0004-adjacency-index.md` |
| 0005 | [Layered Architecture — Graph / Knowledge / Workbench](0005-layered-architecture.md) | `0005-layered-architecture.md` |
| 0006 | [Append-only epistemic interpretation](0006-epistemic-model.md) | `0006-epistemic-model.md` |
| 0007 | [Runtime Temporal Values](0007-temporal-values.md) | `0007-temporal-values.md` |
| 0008 | [Heterogeneous List Values](0008-heterogeneous-lists.md) | `0008-heterogeneous-lists.md` |
| 0009 | [Nested Heterogeneous List Values](0009-nested-heterogeneous-lists.md) | `0009-nested-heterogeneous-lists.md` |
| 0010 | [Full-range dates (proleptic-Gregorian calendar) and a wider duration model](0010-wide-date-and-duration.md) | `0010-wide-date-and-duration.md` |
| 0011 | [Dynamic Heterogeneous Value Lists](0011-dynamic-heterogeneous-values.md) | `0011-dynamic-heterogeneous-values.md` |
| 0012 | [Knowledge and epistemic domain ownership and schema evolution](0012-knowledge-domain-ownership.md) | `0012-knowledge-domain-ownership.md` |
| 0013 | [Durable v0.5 project-generation protocol](0013-project-generation-protocol.md) | `0013-project-generation-protocol.md` |
| 0014 | [Complete-workspace checkpoints and generation-preserving revert](0014-workspace-checkpoints.md) | `0014-workspace-checkpoints.md` |
| 0015 | [Three embedded project-write modes](0015-embedded-write-modes.md) | `0015-embedded-write-modes.md` |
| 0016 | [Repository integration and deployment configuration boundary](0016-repository-integration-and-deployment-configuration.md) | `0016-repository-integration-and-deployment-configuration.md` |
| 0017 | [One version across core and adapters](0017-unified-release-version.md) | `0017-unified-release-version.md` |
| 0018 | [Acknowledged durability and isolation contract](0018-acknowledged-durability-isolation.md) | `0018-acknowledged-durability-isolation.md` |
| 0019 | [Authoritative durable graph delta journal](0019-authoritative-graph-delta-journal.md) | `0019-authoritative-graph-delta-journal.md` |
| 0020 | [NTFS write-through namespace durability](0020-ntfs-write-through-namespace-durability.md) | `0020-ntfs-write-through-namespace-durability.md` |
| 0021 | [Portable project v2 package layout and identity](0021-portable-project-v2.md) | `0021-portable-project-v2.md` |
| 0022 | [Multi-ontology semantics in portable project v2](0022-portable-v2-multi-ontology-compatibility.md) | `0022-portable-v2-multi-ontology-compatibility.md` |
| 0023 | [Composable ontology modules and semantic bridges](0023-composable-multi-ontology.md) | `0023-composable-multi-ontology.md` |
| 0024 | [Storage format exceptions for GFDR and compiled ontologies](0024-storage-format-exceptions.md) | `0024-storage-format-exceptions.md` |
| 0025 | [Storage values have a compiler-independent contract](0025-storage-value-contract.md) | `0025-storage-value-contract.md` |

## Numbering

ADRs are numbered `NNNN-slug.md` starting at `0001`. Accepted ADRs are immutable;
a new ADR supersedes an old one rather than rewriting it.

## Related navigation

Published Starlight nav: **Engineering → Architecture Decision Records** (sidebar entries
mirror this table). ADR bodies stay under `docs/adr/`; the public decision log at
[`../engineering/adrs/`](../engineering/adrs/) links here and must not duplicate or
renumber bodies. Do not fork a second ADR sequence.
