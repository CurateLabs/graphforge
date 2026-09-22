# Architecture Decision Records

ADR sequence for the decisions that govern the shipped GraphForge product
architecture. The table below is the active set: every record in it currently
governs. Superseded records are not deleted — they move to
[`superseded/`](superseded/) so their reasoning and their inbound links survive,
and they are listed under [Superseded records](#superseded-records) below.
Roadmap-only ADRs are not retained in this tree.

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
| 0018 | [Acknowledged durability and isolation contract](0018-acknowledged-durability-isolation.md) | `0018-acknowledged-durability-isolation.md` |
| 0019 | [Authoritative durable graph delta journal](0019-authoritative-graph-delta-journal.md) | `0019-authoritative-graph-delta-journal.md` |
| 0020 | [NTFS write-through namespace durability](0020-ntfs-write-through-namespace-durability.md) | `0020-ntfs-write-through-namespace-durability.md` |
| 0021 | [Portable project v2 package layout and identity](0021-portable-project-v2.md) | `0021-portable-project-v2.md` |
| 0022 | [Multi-ontology semantics in portable project v2](0022-portable-v2-multi-ontology-compatibility.md) | `0022-portable-v2-multi-ontology-compatibility.md` |
| 0023 | [Composable ontology modules and semantic bridges](0023-composable-multi-ontology.md) | `0023-composable-multi-ontology.md` |
| 0024 | [Storage format exceptions for GFDR and compiled ontologies](0024-storage-format-exceptions.md) | `0024-storage-format-exceptions.md` |
| 0025 | [Storage values have a compiler-independent contract](0025-storage-value-contract.md) | `0025-storage-value-contract.md` |
| 0026 | [Read plans bind resources in execution](0026-read-plan-resources.md) | `0026-read-plan-resources.md` |
| 0027 | [Native GraphForge execution boundary](0027-native-runtime-boundary.md) | `0027-native-runtime-boundary.md` |
| 0028 | [One transaction owns graph mutation effects](0028-shared-mutation-transaction.md) | `0028-shared-mutation-transaction.md` |
| 0029 | [Compile against immutable schema and catalog data](0029-lowering-schema-snapshot.md) | `0029-lowering-schema-snapshot.md` |
| 0030 | [Portable OCI protocol boundary](0030-portable-oci-boundary.md) | `0030-portable-oci-boundary.md` |
| 0031 | [Reviewed source file size bounds](0031-source-size-policy.md) | `0031-source-size-policy.md` |
| 0032 | [Research Branches share Project publication authority](0032-research-project-authority.md) | `0032-research-project-authority.md` |
| 0035 | [Preserve stage diagnostics at public error boundaries](0035-structured-stage-errors.md) | `0035-structured-stage-errors.md` |
| 0036 | [The GraphForge release version contract](0036-release-version-contract.md) | `0036-release-version-contract.md` |
| 0037 | [Derived adjacency is published with the generation](0037-adjacency-published-with-generation.md) | `0037-adjacency-published-with-generation.md` |
| 0038 | [Determinism belongs at the publication boundary](0038-determinism-at-the-publication-boundary.md) | `0038-determinism-at-the-publication-boundary.md` |
| 0039 | [Research Versions share Project publication authority](0039-research-version-publication.md) | `0039-research-version-publication.md` |
| 0040 | [Frozen Slices reference retained Version context](0040-frozen-slice-membership.md) | `0040-frozen-slice-membership.md` |
| 0041 | [Branch state publishes through Project CURRENT](0041-branch-current-publication.md) | `0041-branch-current-publication.md` |
| 0042 | [Contextual research decisions extend immutable knowledge](0042-contextual-research-claims.md) | `0042-contextual-research-claims.md` |
| 0044 | [Ingest authentication regime — hash once on write, verify at trust boundaries](0044-ingest-authentication-regime.md) | `0044-ingest-authentication-regime.md` |
| 0043 | [Proposal acceptance shares the Project publication owner](0043-atomic-research-proposal-acceptance.md) | `0043-atomic-research-proposal-acceptance.md` |
| 0044 | [Research interchange preserves content identity separately from authority](0044-research-interchange-authority.md) | `0044-research-interchange-authority.md` |

## Superseded records

Retained under [`superseded/`](superseded/). Each names the record that replaced
it; nothing here governs.

| ADR | Title | Superseded by | File |
| --- | --- | --- | --- |
| 0017 | [One version across core and adapters](superseded/0017-unified-release-version.md) | ADR 0036 | `superseded/0017-unified-release-version.md` |
| 0033 | [Prereleases share one version with per-ecosystem spelling](superseded/0033-prerelease-version-identity.md) | ADR 0036 | `superseded/0033-prerelease-version-identity.md` |
| 0034 | [One canonical release-candidate spelling, `-rc.N`](superseded/0034-canonical-release-candidate-spelling.md) | ADR 0036 | `superseded/0034-canonical-release-candidate-spelling.md` |

## Numbering

ADRs are numbered `NNNN-slug.md` starting at `0001`. A number is used once: it
is never reassigned, and a record keeps its number when it is superseded and
moved to `superseded/`. Accepted ADRs are immutable; a new ADR supersedes an old
one rather than rewriting it.

## Status vocabulary

A record carries exactly one `**Status:**` line, immediately under its title,
whose value is one of these four and nothing else:

| Status | Meaning |
| --- | --- |
| `Proposed` | Under discussion. Not yet decided. |
| `Accepted` | Decided and in effect. |
| `Superseded by ADR NNNN` | Replaced by a later decision. The file lives under `superseded/`. |
| `Deprecated` | No longer relevant, and not replaced. |

Status records the state of the *decision*, not of the code. Where the state of
the implementation is worth recording — shipped, pending, partial, or scoped to
a milestone — it goes in an `**Implementation:**` field beneath the status, not
in the status value. `docs/engineering/adrs/README.md` carries the same four
values.

## Related navigation

Published Starlight nav: **Engineering → Architecture Decision Records** (sidebar
entries mirror the active table). ADR bodies stay under `docs/adr/`; the public
decision log at [`../engineering/adrs/`](../engineering/adrs/) links here and must
not duplicate or renumber bodies. Do not fork a second ADR sequence.

Both indexes and this directory must agree, and so do the two docs-site files
that the published build consumes. `scripts/ci/adr-index.py check` enforces all
four in the Repository Policy job; the docs-site regions are generated, so run
`python3 scripts/ci/adr-index.py generate` after adding or superseding a record
rather than editing them by hand.
