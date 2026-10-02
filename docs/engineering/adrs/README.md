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
[`../../adr/README.md`](../../adr/README.md) and this decision log in the same change, then
regenerate the two docs-site files the published build consumes:

```
python3 scripts/ci/adr-index.py generate
python3 scripts/ci/adr-index.py check
```

## Status values

Exactly four, defined once in [`../../adr/README.md`](../../adr/README.md#status-vocabulary):

- **Proposed** — under discussion; not yet decided.
- **Accepted** — decided and in effect.
- **Superseded by ADR NNNN** — replaced by a later decision; the body moves to `../../adr/superseded/`.
- **Deprecated** — no longer relevant, and not replaced.

Implementation state (shipped, pending, partial) is recorded in an
`**Implementation:**` field in the ADR body, never in the status value.

Every record also carries a `revisit_when` frontmatter trigger: the observable
condition under which the decision is re-examined (#1625). The trigger is
rendered in the tables below so the log doubles as a review list. A decision
that no code path or contract still depends on is marked **Deprecated**, not
left Accepted; superseded records name their successor in `superseded_by`.

## Decision log

Mirrors [`../../adr/README.md`](../../adr/README.md). This table and that one must
agree with `docs/adr/` itself, and `scripts/ci/adr-index.py check` enforces it in
the CI Lint job (#1390).

| ADR | Title | Status | Revisit when | Path |
| --- | --- | --- | --- | --- |
| 0001 | Rust Core | Accepted | A binding surface needs engine-owned behavior of its own, or DataFusion's extension API stops meeting graph-native execution needs | [`../../adr/0001-rust-core.md`](../../adr/0001-rust-core.md) |
| 0002 | Recursive Descent + Pratt Parser for graphforge-cypher | Accepted | The hand-written recursive-descent and Pratt parser drifts from the openCypher grammar it must match, or the differential corpus stops passing | [`../../adr/0002-lr1-grammar.md`](../../adr/0002-lr1-grammar.md) |
| 0003 | Progressive Ontology — Exploration First | Accepted | Analysts need a binder mode beyond exploratory, guided, and strict, or the exploratory fallback catch-all stops scaling for real workloads | [`../../adr/0003-progressive-ontology.md`](../../adr/0003-progressive-ontology.md) |
| 0004 | Graph-Native Adjacency Index | Accepted | Adjacency-backed and join-backed traversal results diverge in the differential correctness corpus, or incremental rebuild becomes required | [`../../adr/0004-adjacency-index.md`](../../adr/0004-adjacency-index.md) |
| 0005 | Layered Architecture — Graph / Knowledge / Workbench | Accepted | Knowledge or workbench concerns start leaking onto graph tables as unowned columns, breaking the boundary regression test | [`../../adr/0005-layered-architecture.md`](../../adr/0005-layered-architecture.md) |
| 0006 | Append-only epistemic interpretation | Accepted | Belief state needs to become mutable, or the append-only epistemic tables can no longer represent required ambiguity cases | [`../../adr/0006-epistemic-model.md`](../../adr/0006-epistemic-model.md) |
| 0007 | Runtime Temporal Values | Accepted | A temporal phase changes a rendered string and fails the passing baseline gate, or cypher_eq stops covering a new cross-type comparison | [`../../adr/0007-temporal-values.md`](../../adr/0007-temporal-values.md) |
| 0008 | Heterogeneous List Values | Accepted | Nested heterogeneous lists or maps become required beyond what the flat tagged-struct representation can express | [`../../adr/0008-heterogeneous-lists.md`](../../adr/0008-heterogeneous-lists.md) |
| 0009 | Nested Heterogeneous List Values | Accepted | The finite per-expression payload schema for nested heterogeneous lists and maps cannot represent a new TCK scenario shape | [`../../adr/0009-nested-heterogeneous-lists.md`](../../adr/0009-nested-heterogeneous-lists.md) |
| 0010 | Full-range dates (proleptic-Gregorian calendar) and a wider duration model | Accepted | Calendar correctness tests for ISO-week boundaries, proleptic year 0, or leap years fail, or the temporal corpus regresses | [`../../adr/0010-wide-date-and-duration.md`](../../adr/0010-wide-date-and-duration.md) |
| 0011 | Dynamic Heterogeneous Value Lists | Accepted | A dynamic list literal needs more than 127 elements, exceeding the Int8 tag's addressable range | [`../../adr/0011-dynamic-heterogeneous-values.md`](../../adr/0011-dynamic-heterogeneous-values.md) |
| 0012 | Knowledge and epistemic domain ownership and schema evolution | Accepted | The domain-dependency check or compile-time parity tests fail because graph or analyst-verb crates gain a path to knowledge | [`../../adr/0012-knowledge-domain-ownership.md`](../../adr/0012-knowledge-domain-ownership.md) |
| 0013 | Durable v0.5 project-generation protocol | Accepted | A supported platform's local filesystem stops meeting the atomic-rename and fsync barrier this recovery protocol assumes | [`../../adr/0013-project-generation-protocol.md`](../../adr/0013-project-generation-protocol.md) |
| 0014 | Complete-workspace checkpoints and generation-preserving revert | Accepted | Callers need graph-only revert or arbitrary generation access, which the named bounded checkpoint registry deliberately forbids | [`../../adr/0014-workspace-checkpoints.md`](../../adr/0014-workspace-checkpoints.md) |
| 0015 | Three embedded project-write modes | Accepted | Remote multi-process fleets need engine-native coordination instead of an application-owned or extension-provided authority | [`../../adr/0015-embedded-write-modes.md`](../../adr/0015-embedded-write-modes.md) |
| 0016 | Repository integration and deployment configuration boundary | Accepted | Lifecycle, interchange, or infrastructure validation needs a contract the repository snapshot and deployment spec schemas cannot express | [`../../adr/0016-repository-integration-and-deployment-configuration.md`](../../adr/0016-repository-integration-and-deployment-configuration.md) |
| 0018 | Acknowledged durability and isolation contract | Accepted | Fault modeling, recovery-on-open, deltas, transactions, or certification work needs to change an acknowledged durability or isolation outcome | [`../../adr/0018-acknowledged-durability-isolation.md`](../../adr/0018-acknowledged-durability-isolation.md) |
| 0019 | Authoritative durable graph delta journal | Accepted | Compaction or the frozen fault oracle shows the delta journal can no longer bound replay cost or tiny-run accumulation | [`../../adr/0019-authoritative-graph-delta-journal.md`](../../adr/0019-authoritative-graph-delta-journal.md) |
| 0020 | NTFS write-through namespace durability | Accepted | A drive, controller, hypervisor, or filesystem falsely acknowledges write-through completion on a certified NTFS volume | [`../../adr/0020-ntfs-write-through-namespace-durability.md`](../../adr/0020-ntfs-write-through-namespace-durability.md) |
| 0021 | Portable project v2 package layout and identity | Accepted | A package needs full-memory buffering, unmanifested files, or capabilities the closed-world v2 schema and fixture corpus forbid | [`../../adr/0021-portable-project-v2.md`](../../adr/0021-portable-project-v2.md) |
| 0022 | Multi-ontology semantics in portable project v2 | Accepted | A field needs required interpretation beyond a new feature token, or the generic manifest and component contract itself must change | [`../../adr/0022-portable-v2-multi-ontology-compatibility.md`](../../adr/0022-portable-v2-multi-ontology-compatibility.md) |
| 0023 | Composable ontology modules and semantic bridges | Accepted | A legacy single-ontology project's migration or the composition fingerprint cannot describe a new module inventory shape | [`../../adr/0023-composable-multi-ontology.md`](../../adr/0023-composable-multi-ontology.md) |
| 0024 | Storage format exceptions for GFDR and compiled ontologies | Accepted | A third storage format needs an exception beyond GFDR and compiled ontology Parquet | [`../../adr/0024-storage-format-exceptions.md`](../../adr/0024-storage-format-exceptions.md) |
| 0025 | Storage values have a compiler-independent contract | Accepted | graphforge-value cannot preserve an existing encoding during extraction, forcing a new schema or format version decision | [`../../adr/0025-storage-value-contract.md`](../../adr/0025-storage-value-contract.md) |
| 0026 | Read plans bind resources in execution | Accepted | A relocated or rebound read plan needs eager graph scans or a working-directory fallback the execution boundary forbids | [`../../adr/0026-read-plan-resources.md`](../../adr/0026-read-plan-resources.md) |
| 0027 | Native GraphForge execution boundary | Accepted | A concrete browser-only user requirement arrives with resource, persistence, API-profile guarantees, and a validation budget | [`../../adr/0027-native-runtime-boundary.md`](../../adr/0027-native-runtime-boundary.md) |
| 0028 | One transaction owns graph mutation effects | Accepted | Cypher and analyst write-back need independent writer locks or a durable transaction format beyond the shared MutationTransaction | [`../../adr/0028-shared-mutation-transaction.md`](../../adr/0028-shared-mutation-transaction.md) |
| 0029 | Compile against immutable schema and catalog data | Accepted | Relational lowering needs source paths, executable providers, or graph rows that LoweringSnapshot's compile-time immutability excludes | [`../../adr/0029-lowering-schema-snapshot.md`](../../adr/0029-lowering-schema-snapshot.md) |
| 0030 | Portable OCI protocol boundary | Accepted | graphforge-portable-oci needs a durable package or signature format change, or a second transport beyond OCI registries | [`../../adr/0030-portable-oci-boundary.md`](../../adr/0030-portable-oci-boundary.md) |
| 0031 | Reviewed source file size bounds | Accepted | A reviewed file needs an exemption beyond the accepted ones, or an existing exemption's rationale no longer fits its cohesive contract | [`../../adr/0031-source-size-policy.md`](../../adr/0031-source-size-policy.md) |
| 0032 | Research Branches share Project publication authority | Accepted | Branch publication needs independent writer locks or a per-Branch isolation level beyond the Project's shared coordination boundary | [`../../adr/0032-research-project-authority.md`](../../adr/0032-research-project-authority.md) |
| 0035 | Preserve stage diagnostics at public error boundaries | Accepted | A new binder diagnostic or nested DataFusion source starts double-prefixing or losing information at a public error boundary | [`../../adr/0035-structured-stage-errors.md`](../../adr/0035-structured-stage-errors.md) |
| 0036 | The GraphForge release version contract | Accepted | A new prerelease phase such as alpha or beta is needed, reopening the spelling-collision question this contract closed | [`../../adr/0036-release-version-contract.md`](../../adr/0036-release-version-contract.md) |
| 0037 | Derived adjacency is published with the generation | Accepted | Append constructions or parent-index tombstoning change when or how CSR adjacency is built relative to generation publication | [`../../adr/0037-adjacency-published-with-generation.md`](../../adr/0037-adjacency-published-with-generation.md) |
| 0038 | Determinism belongs at the publication boundary | Accepted | A concurrency defect reaches published bytes without being caught by the equivalence check, or intermediates need byte stability | [`../../adr/0038-determinism-at-the-publication-boundary.md`](../../adr/0038-determinism-at-the-publication-boundary.md) |
| 0039 | Research Versions share Project publication authority | Accepted | Selected-closure retention or a new registry revision changes materialization and weakens authentication or root-closure guarantees | [`../../adr/0039-research-version-publication.md`](../../adr/0039-research-version-publication.md) |
| 0040 | Frozen Slices reference retained Version context | Accepted | A Frozen Slice needs to compact its source Version into a selected projection rather than referencing retained Version context | [`../../adr/0040-frozen-slice-membership.md`](../../adr/0040-frozen-slice-membership.md) |
| 0041 | Branch state publishes through Project CURRENT | Accepted | Branch acceptance evidence, including two-Branch restoration after reopen, exposes a gap in the current-Version publication model | [`../../adr/0041-branch-current-publication.md`](../../adr/0041-branch-current-publication.md) |
| 0042 | Contextual research decisions extend immutable knowledge | Accepted | A new research claim family needs a registry entry whose closed values, bounds, or fingerprints the current schema cannot express | [`../../adr/0042-contextual-research-claims.md`](../../adr/0042-contextual-research-claims.md) |
| 0043 | Proposal acceptance shares the Project publication owner | Accepted | Acceptance tests show partial acceptance can diverge from the prepared destination, or a non-atomic acceptance path is proposed | [`../../adr/0043-atomic-research-proposal-acceptance.md`](../../adr/0043-atomic-research-proposal-acceptance.md) |
| 0044 | Research interchange preserves content identity separately from authority | Accepted | A round trip fails to stay complete, disjoint, and redacted, or a Fork loses its required independence from installed authority | [`../../adr/0044-research-interchange-authority.md`](../../adr/0044-research-interchange-authority.md) |
| 0045 | Ingest authentication regime — hash once on write, verify at trust boundaries | Accepted | Retained authentication read-backs drift materially from the measured baseline at the 67,108,864-edge rung, or a trust boundary moves | [`../../adr/0045-ingest-authentication-regime.md`](../../adr/0045-ingest-authentication-regime.md) |
| 0046 | Construction keeps its own sorting, partitioning and admission; library reuse is bounded to named hybrids | Accepted | Any decision-matrix trigger fires: partition sorting becomes a measurable share of ingest, DataFusion gains range partitioning, or the coordinator goes asynchronous | [`../../adr/0046-construction-reuse-decisions.md`](../../adr/0046-construction-reuse-decisions.md) |
| 0047 | Over-budget construction partitions succeed; one CPU budget per instance | Accepted | External partitions dominate ingest wall on a real workload, row partitions need the external path, or a second subsystem needs its own CPU admission | [`../../adr/0047-over-budget-partitions-and-instance-cpu-budget.md`](../../adr/0047-over-budget-partitions-and-instance-cpu-budget.md) |
| 0048 | Cargo with nextest is the CI build authority; Bazel is removed | Accepted | Merge-queue Rust reruns dominate CI Gate latency, a Cargo lane measures more than 1.25x the replaced Bazel lane on Rust-changing PRs, or a hermetic release build becomes a publication requirement | [`../../adr/0048-cargo-is-the-ci-build-authority.md`](../../adr/0048-cargo-is-the-ci-build-authority.md) |
| 0049 | Versioned checksums for published graph payload admission | Accepted | The same-identity adversary assumption changes, or published graph payloads move to a substrate with authoritative data checksums | [`../../adr/0049-published-payload-checksums.md`](../../adr/0049-published-payload-checksums.md) |
| 0050 | The read path keeps its adjacency operators and chooses fast paths in the lowerer | Accepted | DataFusion gains a lookup join over a TableProvider index, a query shape that only the physical rewrites caught is found after migration, or a paired timing shows lowerer selection costs more than physical selection | [`../../adr/0050-read-path-fast-path-selection.md`](../../adr/0050-read-path-fast-path-selection.md) |
| 0051 | Discovery carries a digest-addressed Project summary and exact ontology descriptors | Accepted | A summary field needs required interpretation by readers, module packages must become independent of the publishing Project (which requires a portable-v2 manifest change), or a Hub needs summary data that cannot be derived from a verified package | [`../../adr/0051-discovery-project-summary-and-ontology-descriptors.md`](../../adr/0051-discovery-project-summary-and-ontology-descriptors.md) |
| 0052 | Discovery carries a digest-addressed research lineage document | Accepted | Clone or publish paths need identities this document cannot express, or Hub moderation requires cross-owner Proposal submission semantics | [`../../adr/0052-discovery-research-lineage-document.md`](../../adr/0052-discovery-research-lineage-document.md) |
| 0053 | Hub publish wire contract | Proposed | The control-plane publish session shape, data-plane upload URL policy, or ref precondition encoding needs a breaking wire change | [`../../adr/0053-hub-publish-wire-contract.md`](../../adr/0053-hub-publish-wire-contract.md) |

### Superseded

Retained for history under `../../adr/superseded/`; nothing here governs.

| ADR | Title | Status | Revisit when | Path |
| --- | --- | --- | --- | --- |
| 0017 | One version across core and adapters | Superseded by ADR 0036 | Historical; ADR 0036 owns the release version contract | [`../../adr/superseded/0017-unified-release-version.md`](../../adr/superseded/0017-unified-release-version.md) |
| 0033 | Prereleases share one version with per-ecosystem spelling | Superseded by ADR 0036 | Historical; ADR 0036 owns the release version contract | [`../../adr/superseded/0033-prerelease-version-identity.md`](../../adr/superseded/0033-prerelease-version-identity.md) |
| 0034 | One canonical release-candidate spelling, `-rc.N` | Superseded by ADR 0036 | Historical; ADR 0036 owns the release version contract | [`../../adr/superseded/0034-canonical-release-candidate-spelling.md`](../../adr/superseded/0034-canonical-release-candidate-spelling.md) |
