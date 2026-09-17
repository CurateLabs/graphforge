# Architecture Decision Records

An **Architecture Decision Record (ADR)** captures one significant decision — the context, the
choice made, and its consequences — so the reasoning lives in the repo alongside the code.
Decisions are immutable once accepted: to change one, add a new ADR that supersedes it.

## Path coordination

DocSlime’s canonical directory is `docs/engineering/adrs/`. GraphForge’s accepted ADR bodies
live in [`../../adr/`](../../adr/) as one contiguous sequence after the
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

Exactly four, defined once in [`../../adr/README.md`](../../adr/README.md#status-vocabulary):

- **Proposed** — under discussion; not yet decided.
- **Accepted** — decided and in effect.
- **Superseded by ADR NNNN** — replaced by a later decision; the body moves to `../../adr/superseded/`.
- **Deprecated** — no longer relevant, and not replaced.

Implementation state (shipped, pending, partial) is recorded in an
`**Implementation:**` field in the ADR body, never in the status value.

## Decision log

Mirrors [`../../adr/README.md`](../../adr/README.md). This table and that one must
agree with `docs/adr/` itself; nothing enforces that yet, and
[#1390](https://github.com/CurateLabs/graphforge/issues/1390) owns the gate.

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
| 0010 | Full-range dates (proleptic-Gregorian calendar) and a wider duration model | Accepted | [`../../adr/0010-wide-date-and-duration.md`](../../adr/0010-wide-date-and-duration.md) |
| 0011 | Dynamic Heterogeneous Value Lists | Accepted | [`../../adr/0011-dynamic-heterogeneous-values.md`](../../adr/0011-dynamic-heterogeneous-values.md) |
| 0012 | Knowledge and epistemic domain ownership and schema evolution | Accepted | [`../../adr/0012-knowledge-domain-ownership.md`](../../adr/0012-knowledge-domain-ownership.md) |
| 0013 | Durable v0.5 project-generation protocol | Accepted | [`../../adr/0013-project-generation-protocol.md`](../../adr/0013-project-generation-protocol.md) |
| 0014 | Complete-workspace checkpoints and generation-preserving revert | Accepted | [`../../adr/0014-workspace-checkpoints.md`](../../adr/0014-workspace-checkpoints.md) |
| 0015 | Three embedded project-write modes | Accepted | [`../../adr/0015-embedded-write-modes.md`](../../adr/0015-embedded-write-modes.md) |
| 0016 | Repository integration and deployment configuration boundary | Accepted | [`../../adr/0016-repository-integration-and-deployment-configuration.md`](../../adr/0016-repository-integration-and-deployment-configuration.md) |
| 0018 | Acknowledged durability and isolation contract | Accepted | [`../../adr/0018-acknowledged-durability-isolation.md`](../../adr/0018-acknowledged-durability-isolation.md) |
| 0019 | Authoritative durable graph delta journal | Accepted | [`../../adr/0019-authoritative-graph-delta-journal.md`](../../adr/0019-authoritative-graph-delta-journal.md) |
| 0020 | NTFS write-through namespace durability | Accepted | [`../../adr/0020-ntfs-write-through-namespace-durability.md`](../../adr/0020-ntfs-write-through-namespace-durability.md) |
| 0021 | Portable project v2 package layout and identity | Accepted | [`../../adr/0021-portable-project-v2.md`](../../adr/0021-portable-project-v2.md) |
| 0022 | Multi-ontology semantics in portable project v2 | Accepted | [`../../adr/0022-portable-v2-multi-ontology-compatibility.md`](../../adr/0022-portable-v2-multi-ontology-compatibility.md) |
| 0023 | Composable ontology modules and semantic bridges | Accepted | [`../../adr/0023-composable-multi-ontology.md`](../../adr/0023-composable-multi-ontology.md) |
| 0024 | Storage format exceptions for GFDR and compiled ontologies | Accepted | [`../../adr/0024-storage-format-exceptions.md`](../../adr/0024-storage-format-exceptions.md) |
| 0025 | Storage values have a compiler-independent contract | Accepted | [`../../adr/0025-storage-value-contract.md`](../../adr/0025-storage-value-contract.md) |
| 0026 | Read plans bind resources in execution | Accepted | [`../../adr/0026-read-plan-resources.md`](../../adr/0026-read-plan-resources.md) |
| 0027 | Native GraphForge execution boundary | Accepted | [`../../adr/0027-native-runtime-boundary.md`](../../adr/0027-native-runtime-boundary.md) |
| 0028 | One transaction owns graph mutation effects | Accepted | [`../../adr/0028-shared-mutation-transaction.md`](../../adr/0028-shared-mutation-transaction.md) |
| 0029 | Compile against immutable schema and catalog data | Accepted | [`../../adr/0029-lowering-schema-snapshot.md`](../../adr/0029-lowering-schema-snapshot.md) |
| 0030 | Portable OCI protocol boundary | Accepted | [`../../adr/0030-portable-oci-boundary.md`](../../adr/0030-portable-oci-boundary.md) |
| 0031 | Reviewed source file size bounds | Accepted | [`../../adr/0031-source-size-policy.md`](../../adr/0031-source-size-policy.md) |
| 0032 | Research Branches share Project publication authority | Accepted | [`../../adr/0032-research-project-authority.md`](../../adr/0032-research-project-authority.md) |
| 0035 | Preserve stage diagnostics at public error boundaries | Accepted | [`../../adr/0035-structured-stage-errors.md`](../../adr/0035-structured-stage-errors.md) |
| 0036 | The GraphForge release version contract | Accepted | [`../../adr/0036-release-version-contract.md`](../../adr/0036-release-version-contract.md) |

### Superseded

Retained for history under `../../adr/superseded/`; nothing here governs.

| ADR | Title | Status | Path |
| --- | --- | --- | --- |
| 0017 | One version across core and adapters | Superseded by ADR 0036 | [`../../adr/superseded/0017-unified-release-version.md`](../../adr/superseded/0017-unified-release-version.md) |
| 0033 | Prereleases share one version with per-ecosystem spelling | Superseded by ADR 0036 | [`../../adr/superseded/0033-prerelease-version-identity.md`](../../adr/superseded/0033-prerelease-version-identity.md) |
| 0034 | One canonical release-candidate spelling, `-rc.N` | Superseded by ADR 0036 | [`../../adr/superseded/0034-canonical-release-candidate-spelling.md`](../../adr/superseded/0034-canonical-release-candidate-spelling.md) |
