# Architecture Decision Records

An **Architecture Decision Record (ADR)** captures one significant decision — the context, the
choice made, and its consequences — so the reasoning lives in the repo alongside the code.
Decisions are immutable once accepted: to change one, add a new ADR that supersedes it.

## Path coordination

DocSlime’s canonical directory is `docs/engineering/adrs/`. GraphForge’s accepted ADR bodies
live in [`../../adr/`](../../adr/) as a contiguous v0.5.0 sequence (`0001`–`0014`) after the
[#2730](https://github.com/CurateLabs/graphforge-legecy/pull/2730) cull/renumber
([#2725](https://github.com/CurateLabs/graphforge-legecy/issues/2725)). This folder is an
**index only** — do not duplicate ADR bodies or fork a second numbering sequence here.

Published Starlight nav places this log and the `docs/adr/` bodies under
**Engineering → Architecture Decision Records** (#2770 / #2771).

## Creating an ADR

```
docslime add adr <short-slug>
```

Until bodies move under this folder, add new ADR markdown under `docs/adr/` and update both
[`../../adr/README.md`](../../adr/README.md) and this decision log in the same change.

## Status values

- **Proposed** — under discussion.
- **Accepted** — decided and in effect.
- **Superseded by ADR-NNNN** — replaced by a later decision.
- **Deprecated** — no longer relevant.

## Decision log

Keeper set after #2730 (mirrors [`../../adr/README.md`](../../adr/README.md)):

| ADR | Title | Status | Path |
| --- | --- | --- | --- |
| 0001 | Rust Core | Accepted | [`../../adr/0001-rust-core.md`](../../adr/0001-rust-core.md) |
| 0002 | Recursive Descent + Pratt Parser for graphforge-cypher | Accepted | [`../../adr/0002-lr1-grammar.md`](../../adr/0002-lr1-grammar.md) |
| 0003 | Progressive Ontology — Exploration First | Accepted | [`../../adr/0003-progressive-ontology.md`](../../adr/0003-progressive-ontology.md) |
| 0004 | Graph-Native Adjacency Index | Accepted | [`../../adr/0004-adjacency-index.md`](../../adr/0004-adjacency-index.md) |
| 0005 | Layered Architecture — Graph / Knowledge / Workbench | Accepted | [`../../adr/0005-layered-architecture.md`](../../adr/0005-layered-architecture.md) |
| 0006 | Append-only epistemic interpretation | Accepted | [`../../adr/0006-epistemic-model.md`](../../adr/0006-epistemic-model.md) |
| 0007 | Runtime Temporal Values | Accepted | [`../../adr/0007-temporal-values.md`](../../adr/0007-temporal-values.md) |
| 0008 | Heterogeneous List Values | Accepted | [`../../adr/0008-heterogeneous-lists.md`](../../adr/0008-heterogeneous-lists.md) |
| 0009 | Nested Heterogeneous List Values | Accepted | [`../../adr/0009-nested-heterogeneous-lists.md`](../../adr/0009-nested-heterogeneous-lists.md) |
| 0010 | Wide date and duration | Accepted | [`../../adr/0010-wide-date-and-duration.md`](../../adr/0010-wide-date-and-duration.md) |
| 0011 | Dynamic Heterogeneous Value Lists | Accepted | [`../../adr/0011-dynamic-heterogeneous-values.md`](../../adr/0011-dynamic-heterogeneous-values.md) |
| 0012 | Knowledge and epistemic domain ownership and schema evolution | Accepted | [`../../adr/0012-m20-domain-ownership.md`](../../adr/0012-m20-domain-ownership.md) |
| 0013 | Durable v0.5 project-generation protocol | Accepted | [`../../adr/0013-project-generation-protocol.md`](../../adr/0013-project-generation-protocol.md) |
| 0014 | Complete-workspace checkpoints | Accepted | [`../../adr/0014-workspace-checkpoints.md`](../../adr/0014-workspace-checkpoints.md) |
| 0015 | Three embedded project-write modes | Accepted | [`../../adr/0015-embedded-write-modes.md`](../../adr/0015-embedded-write-modes.md) |
| 0016 | Repository integration and deployment configuration boundary | Accepted | [`../../adr/0016-repository-integration-and-deployment-configuration.md`](../../adr/0016-repository-integration-and-deployment-configuration.md) |
