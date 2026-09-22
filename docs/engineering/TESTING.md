# Testing

GraphForge proves shippable behavior with deterministic Rust and binding tests,
the openCypher TCK as the language oracle, and contract inventories for non-Cypher
surfaces. Correctness and registry honesty are non-negotiable; skips, sleeps,
retries-as-green, and weakened assertions do not satisfy gates (`AGENTS.md`,
[`../development/testing.md`](../development/testing.md)).

**Speed is a first-class engineering value alongside honesty.** Every surface
has a wall-clock target, sheds work that is not required for its objective, and
parallelizes the rest. Frequent publishing uses the **publish-track**, not a
separately named “nightly” product. Full `llvm-cov` / `make coverage-rust` is a
local (or coverage-sensitive) honesty tool — **PR CI does not run full coverage**.
The **Coverage** workflow runs the same ledger on every merge to `main` and fails
on a breached floor, so drift surfaces within one merge rather than at release
time. Repository policy keeps full `llvm-cov` out of pull-request CI, where its
cost would be paid on every review cycle; `scripts/ci/test-coverage-rust.sh`
enforces that and fails closed.

This page is the **v0.5.0 / release-prep testing strategy** that shipped on
`main`: how layers compose, what each gate proves, and what does not count as
end-to-end evidence. Command recipes and historical suite layout live in
[`../development/testing.md`](../development/testing.md). Workflow mechanics live
in [`.github/workflows/README.md`](../../.github/workflows/README.md).

Benchmark measurement authority (BenchExec vs Divan vs diagnostic-only phase
timing) is defined in [`../development/benchmarking.md`](../development/benchmarking.md)
and enforced from `config/benchmark-measurement-inventory.json`.

## Dual-track objectives (PR / publish-track / human close)

| Surface | Objective | Required when | Wall-clock target | Must keep | Shed / defer |
| --- | --- | --- | --- | --- | --- |
| `pre-push-fast` | Policy/format | Local habit | ~30s | lint/license/workflow | Full coverage |
| PR Test Suite + CI Gate | Changed-surface correctness | Every PR → `main` | ≤10m p50 / ≤12m p95 | Classifier, same-SHA Linux bindings, workspace tests, Gate | Multi-OS, load, llvm-cov, Binding RC |
| `make coverage-rust` | Honest floors | Coverage-sensitive changes / floor claims | ≤20m p50 local | Hash/runtime/ledger; real acceptance | HTML by default |
| Coverage | Floor drift detection | Every merge → `main` | ≤180m ceiling | Every floor, and the recorded baseline | Running on pull requests |
| Binding RC | Multi-OS publish bytes + offline rehearsal | publish-track and human close | ≤20m p50 warm / ≤35m cold | Retained multi-OS artifacts, same-SHA, offline rehearsal | Full PR suite re-run; cold builds when sticky hits |
| **publish-track** | Registry-honest publish certification | Whenever we publish (scheduled or on-demand) | ≤35m p50 / ≤50m cold (RC + tag + publish) | Binding RC bytes + `publish.yaml` no-rebuild | release certification, checkpoint, knowledge/epistemic, full clean-env |
| **Human release close** | Milestone / coordinated GA confidence | Human publication close | publish-track + optional gates | publish-track honesty **plus** release-certification / surface gates as documented | — |
| Unchanged-SHA reuse | Skip redundant RC | Same `main` tip + unexpired candidate | RC ~0; publish-only ≤15m | Candidate completeness checks | Rebuilding identical bytes |
| Fuzz / stress / viz | Diagnostic | Schedule/manual | N/A | Not merge or publish-track blockers | — |

**publish-track** is Binding RC → tag / release identity → `publish.yaml` on
retained bytes. release-load, checkpoint recovery, and knowledge/epistemic surface aggregates remain
**human-close / milestone** evidence — they are not registry-honesty inputs and
must not block every publish.

## Ownership

Rust owns behavior. The public facade is `graphforge-api`; Cypher runs
`graphforge-cypher → graphforge-ir → graphforge-rel → graphforge-exec`; storage and project format live in
`graphforge-storage`. Python and Node are thin bindings that project Rust semantics into
language-native APIs and Arrow/IPC — never fallback engines or parallel
implementations of graph logic.

Consequence for tests:

- Prove semantics in Rust (crate tests, facade integration, TCK BDD).
- Prove bindings by clean-install of a same-SHA wheel/addon and equality of
  results/errors against the Rust contract — not by re-implementing algorithms
  in the binding language.
- Treat logical-plan construction, wrapper smoke, and “compiled successfully”
  as necessary but **not** sufficient for shippable behavior.

### Public API BDD classifications

The shared scenarios in `tests/features/api/` use three explicit states:

- **Required:** the applicable Rust, Python, and Node runners call the real
  public surface and assert exact Arrow schema, rows, values, or structured
  error classes. Missing steps, exceptions, xfail/xpass, pending results, and
  unexpected skips fail the gate.
- **Product-excluded:** `@excluded-api-bdd` or
  `@excluded-node-api-bdd` identifies behavior that has a confirmed product
  defect. The scenario must appear in
  `tests/contracts/api-bdd-exclusions.json`, carry exactly one matching open
  `@issue-N` reference, and contributes only to the excluded total—never the
  passing total.
- **Binding-only:** runtime coercion and closed-handle scenarios execute in
  Python and Node but are reported as not applicable by the statically typed,
  non-closeable Rust facade. This classification is allowlisted by repository
  policy and is not a product-behavior exclusion.

`scripts/ci/api-bdd-policy.py` validates the corpus and writes
`target/api-bdd-policy.json` as machine-readable classification evidence.
Its policy mutation tests reject stale inventory rows, untracked exclusions,
language skip tags, xfail conversion, pending Node steps, and manufactured Rust
errors. The BDD mutation sentinels separately prove that wrong row counts,
missing columns, wrong values, wrong error classes, and `NotImplementedError`
all produce failing test processes.

This fail-closed public API model does not change the openCypher TCK. The TCK
continues to use its separately documented advisory passing-set baseline.

## Layered gates

Release readiness is a stack. Lower layers run on every applicable PR; higher
layers are SHA-bound release certification.

| Layer | When it runs | What green means |
| --- | --- | --- |
| Policy / docs | Every PR (docs path for site) | Workflows valid, license/domain policy hold; Starlight builds |
| Unit + workspace | Rust (or classified) changes | Crate logic and `cargo test --workspace` pass with Clippy `-D warnings` |
| Binding acceptance (PR) | Binding / classified changes | One same-SHA Linux Python wheel and Node addon; native contracts; short concurrency matrix |
| Language oracle | Workspace / TCK entrypoints | openCypher TCK runnable scenarios pass (currently **3897/3897**) |
| Binding release candidate | publish-track and human close; exact `main` SHA | Clean-install multi-OS natives + offline rehearsal; fail-closed aggregate; retained publish bytes |
| publish-track publication | Scheduled or on-demand publish | Binding RC retained bytes → tag → `publish.yaml` (no rebuild-on-write) |
| Surface / recovery / load certification | Human release close (optional / milestone) | Non-Cypher inventory, checkpoint recovery, XS–XL load ledger — **not** publish-track blockers |
| Human publication close | Coordinated GA / milestone | publish-track honesty **plus** documented human-close gates |

Ordinary implementation issues close on acceptance-criteria outcomes and green
checks for the **changed surface**. They do **not** require Binding RC,
publish-track, or the human-close cascade. Exact SHA pairing and downloadable
artifacts are publication evidence — see `AGENTS.md` § Issue close.

### Pull-request contract (Test Suite + CI Gate)

- A deterministic classifier enables only the Rust, Python, Gherkin, binding, or
  agent-skills jobs that own the diff. Docs-only PRs do not compile native code.
- One required **CI Gate** aggregates applicable jobs: intentionally skipped
  lanes are fine; failed or cancelled applicable jobs are not.
- PR native binding acceptance is **Linux-only** and uses Cargo’s `dev` profile.
  That is fast feedback, not multi-OS certification.
- When Rust surfaces change, Test Suite runs authoritative Bazel tests
  (`Bazel Bootstrap` → `//:ci_rust_tests`) plus Cargo fmt/clippy, and also runs
  native filesystem publication/admission tests on
  `blacksmith-4vcpu-windows-2025` and `blacksmith-12vcpu-macos-15`. Windows
  also retains the `graphforge-storage` project-root lock unit tests that Linux
  Bazel CI cannot execute. Both host-native jobs are aggregated by `CI Gate`.
- Repository policy always validates workflow syntax, the classifier, domain
  dependency directions, license compliance, and the ledgers that back later
  release gates (without running those heavy matrices on every PR).

### Binding Release Candidate

Maintainers dispatch Binding RC with an exact 40-character `main` SHA. It
clean-installs Python wheels and executes native Node addons on Linux, macOS,
and Windows, package-validates cross-built Node targets, and emits one
fail-closed aggregate. Missing targets, mixed SHAs, fallback execution, and
parity mismatches reject the candidate. It does not tag or publish.

**Windows posture:** the Windows Python lane proves user-facing use of the
installed abi3 wheel (build → clean-install → native contracts). It is **not** a
second MSVC `cargo test` of the full Rust workspace. Windows project-root lock,
filesystem admission/primitive, and publication-kill fault-oracle cross-checks
are hosted by Test Suite `Windows graphforge-storage Locks`, not Binding RC.
Do not treat “wheel contracts green” as “every Rust unit test ran under MSVC.”

### Non-Cypher surface and other publication gates

The TCK cannot substitute for construction, lifecycle, checkpoints, analyst
verbs, search, or knowledge/epistemic surfaces. The checked-in
`tests/contracts/non-cypher-rust-surface.json` inventory classifies every public
Rust receiver method (and related registry/mode rows) with linked evidence.
Manual SHA-bound workflows (Rust non-Cypher surface gate, knowledge/epistemic
contract gates, checkpoint recovery, final non-Cypher surface aggregate, load
matrix) assemble immutable publication reports. Some GitHub workflow *filenames*
and artifact names still carry historical tokens; document them by **role**, not
as product milestones.

### Documentation gate

Docs changes run `.github/workflows/docs.yml`: `pnpm docs:build` syncs
allowlisted `docs/**` into the Starlight site and fails the PR if the site does
not build. The same command imports only the pinned, checksummed public snapshot declared in
`docs-site/external-docs.json` from `graphforge-vscode/docs/published/`; mutable revisions,
missing sources, and checksum drift fail closed without requiring network access. The snapshot
is refreshed explicitly with `pnpm docs:update-extension <full-commit-sha>`. Locally, prefer
`pnpm docs:test-extension`, `pnpm docs:build`, and `pnpm docs:check-links` before
push when editing published pages. Docs green is part of merge readiness for
docs surfaces; it does not prove runtime behavior.

## Analyst UX acceptance

M11's [product requirements](analyst-ux.md) and
[workspace contract](../book/architecture/research-workspaces.md) integrate the
Core Analyst UX specification (Core §§1–20) and Branch and Slice Semantics
specification (Semantics §§1–36). The operation owners #1348–#1357 have shipped
native tests for the outcomes below. The shared-corpus composition in #1358 has
passing Rust, Python, Node and same-build CLI evidence, recorded below. The
matrix maps requirements to those results; it does not replace execution.

Use a deterministic representative corpus with two stories, a shared character,
scans/OCR and improved Artifacts, machine and analyst claims, competing
interpretations, an ontology extension, and external references. Use larger
generated selections to prove bounded behavior without treating illustrative
corpus counts as benchmark thresholds.

Evidence paths below use `API = crates/graphforge-api`,
`AP = API/src/research_proposals/tests`, `UP = API/src/research_upstream/tests`,
`RI = API/src/research_interchange`, and `ST = crates/graphforge-storage/src`.
A `path::symbol` names the exact test; API integration tests run with
`cargo test -p graphforge-api --test <file-stem> <symbol>`, while module tests run
with `cargo test -p graphforge-api --lib <symbol>`. Existing owner evidence and
new composition results are distinguished below.

| Scenario / source coverage | Required observable outcome | Native owner evidence |
| --- | --- | --- |
| Project discovery and vocabulary — Core §§1–4, 15, 17–20; Semantics §§1–2, 30–32, 36 | Metadata-only discovery, all specified entry-point categories, current/origin/Version context, and the four universal inspection questions; no Git ceremony required. | `API/tests/research_project.rs::metadata_survives_reopen_and_discovery_filters_without_graph_open`; the shared-corpus journeys add entry-stage context. Consumer presentation and human comprehension are separate evidence below. |
| Source/Artifact lineage — Core §§2–3, 9–10, 16; Semantics §18 | Scan → OCR → extraction → claim → research is traversable in both directions; preference changes retain old bytes/history and identify affected research. | `API/tests/source_artifact_lifecycle.rs::scan_to_ocr_lineage_preference_and_impact_survive_reopen`; `API/src/research_upstream/tests.rs::preference_only_source_update_is_visible_without_changing_immutable_source`; `RI/closure_tests.rs::selected_artifact_bytes_external_limits_and_ontology_survive_reopen`. |
| Evidence, claims, canonicality — Core §§7–8, 16, 18–19; Semantics §§11–13, 17 | Evidence/extraction/interpretation/hypothesis are distinguishable; contextual canonical assertions/relationships and competing claims coexist; immutable records persist. | `API/tests/research_claims.rs::supported_and_statusless_claims_require_separate_canonical_promotion`, `canonical_parent_and_branch_decisions_are_explicit_independent_and_durable`, and `bring_selected_claim_keeps_classification_without_importing_source_authority`. |
| Slice boundaries — Core §§3, 5; Semantics §§3–6, 30 | Explain inclusion, boundary references, dependency closure, expansion/contraction, and exact frozen membership; dynamic results may change, frozen results do not. | `API/tests/slices.rs::shared_character_has_separate_boundary_and_deterministic_explanations`, `frozen_membership_ignores_parent_edits_and_rejects_forged_capsules`, `source_artifact_evidence_closure_stays_separate_from_membership`, and `frozen_cursors_bind_selector_and_final_ipc_limits_cover_metadata`. |
| Branch creation and identity — Core §§3, 6; Semantics §§7–10, 21–23 | Whole Project, Slice, Branch, and historical Version creation preserves object identity, exact base, evidence/ontology context, and parent genealogy. | `API/tests/branches.rs::slice_branch_preserves_selected_identity_without_parent_graph_membership`, `required_roles_survive_branch_and_historical_branch_version_creation`, and `field_origins_and_contributions_survive_local_edits_and_slice_children`. |
| Local research — Core §§6–8, 14; Semantics §§11–13, 19–20 | Add/modify/suppress/replace/reclassify/challenge affect only selected research; Reference differs from Bring into Branch; local ontology changes do not alter parent. | `API/tests/branches.rs::two_branches_restore_independently_of_parent_and_keep_receipts_after_reopen`, `reference_does_not_expand_but_bring_preserves_selected_uuid_and_origin`, and `local_composition_is_exact_and_does_not_change_parent_or_sibling`; `API/tests/research_claims.rs::branch_challenge_revision_and_suppression_preserve_parent_and_shared_graph` and `child_inherits_frozen_suppression_while_sibling_and_parent_remain_visible`; `AP/ontology_dependencies.rs::semantic_secondary_label_and_typed_list_survive_selected_acceptance_and_reopen`. |
| Versions and retention — Core §§3, 11, 19; Semantics §§8, 14, 23, 33 | Historical graph, evidence refs/local bytes, ontology, and research state survive parent changes, GC, compaction, restore, and reopen; external evidence limitations are explicit. | `API/tests/research_versions.rs::historical_graph_ontology_and_artifact_survive_cleanup_restore_and_replay`, `released_payload_preserves_exact_replay_and_required_root_blocks_deletion`, and `selected_object_root_version_executes_after_ancestor_release_and_cleanup`; `API/tests/slices.rs::selected_history_never_falls_back_to_current_or_retains_its_ancestor`. Actual Branch/Proposal growth measurements are recorded below. |
| Comparison and upstream updates — Core §§11–12; Semantics §§14–18, 24, 28 | Relevant changes and semantic diffs are complete; previews never mutate; only selected valid updates apply; competing interpretations can be retained. | `API/tests/research_comparison.rs::independent_local_upstream_and_conflicting_fields_ignore_unrelated_parent_content`, `exact_versions_page_deterministically_while_live_continuations_fail_stale`, and `bounded_multi_page_diff_has_no_duplicate_or_missing_units`; `UP/repeated.rs::repeated_selective_updates_preserve_independent_baselines_and_original_base` and `retain_both_preserves_native_list_values_and_refuses_scalar_conflicts_atomically`; `UP/ontology.rs::typed_updates_require_reviewed_ontology_and_keep_invalid_retention_unresolved`. |
| Proposals and acceptance — Core §13; Semantics §§25–27 | Proposal pins exact Version; selected/partial acceptance respects dependencies and preserves authorship/evidence; later edits do not mutate proposal or parent; Branch continues. | `AP/two_stories.rs::two_story_shared_character_journey_preserves_partial_review_and_continued_work`; `AP/partial_review.rs::partial_review_retains_only_accepted_fields_and_deferral_can_be_reviewed_later`; `AP/nested.rs::nested_acceptance_deduplicates_per_destination_and_preserves_contribution`; `AP/historical_comparison.rs::historical_comparison_does_not_inherit_later_acceptance_after_reopen`; `AP/ontology_dependencies.rs::relationship_acceptance_requires_reviewed_ontology_and_publishes_both_atomically`; `AP/results.rs::selected_result_artifact_preserves_bytes_and_source_provenance_with_explicit_evidence_ack`. |
| Fork, sharing, and interchange — Core §§3, 14–15; Semantics §§21–23, 29, 34–36 | Fork gains independent Project governance; genealogy survives; current Branch and immutable Version references differ; round trip preserves research/evidence/ontology closure. | `RI/tests.rs::complete_research_roundtrip_preserves_version_and_historical_genealogy`, `fork_has_independent_governance_metadata_and_exact_retry`, and `disjoint_and_redacted_exports_have_distinct_content_and_stable_transport_identity`; `RI/fork/tests.rs::committed_fork_replays_after_source_version_release_without_resetting_destination`; `AP/interchange.rs::selected_accepted_lineage_roundtrips_without_private_ancestor_or_live_acceptance`; `AP/mixed_authority_tests.rs::fork_local_branch_exports_mixed_project_acceptance_genealogy`. The repaired cross-process import replay and passing composition results are recorded below. |
| Complete journey — Core §§1–20; Semantics §§1–36 | Explore → Focus → Branch → Analyze → Compare → Propose → Integrate → continue research, with every decision and lineage visible from real operations. | **Passing native evidence:** `API/tests/research_journey.rs::two_story_journey_preserves_scope_evidence_review_and_continued_research`; Python `tests/research_journey.py::ResearchJourneyTests.test_native_two_story_journey`; Node `tests/research-journey.test.mjs` test `native two-story journey preserves evidence scope review restore and interchange`; CLI `tests/research_journey.rs::two_story_cli_research_survives_partial_review_restore_and_interchange`. Python/Node/CLI paths are relative to their `crates/graphforge-bindings-py`, `crates/graphforge-bindings-node`, and `crates/graphforge-cli` crates. The existing AP two-story test covers the Proposal section, not this complete composition. |

Implementations must update the existing non-Cypher inventories and relevant
domain/schema registries. Test permission-neutral Core behavior and keep
credentials/source content out of diagnostics; policy metadata must not be
represented as enforced access. Associated-project applications validate their
own rendering and access enforcement and are not Core runtime dependencies.
Ordinary changed-surface CI gates apply; M11 introduces no publication-only
workflow requirement for individual implementation issue closure.

Implementation ownership is recorded in
[canonical #1347](https://github.com/CurateLabs/graphforge/issues/1347):
Project discovery #1348; Source/Artifact lineage #1349; Version retention #1350;
Slices #1351; Branches #1352; contextual claims #1353; semantic comparisons #1354;
upstream updates #1355; Proposals #1356; Fork/interchange #1357; and integrated
consumer/journey evidence #1358. Each implementation issue owns its direct tests;
#1358 verifies composition rather than substituting for those tests.

The #1535 foundation suite is
`cargo test -p graphforge-storage research_versions --lib`. It exercises real
Project publication/recovery, independently advancing context-head fixtures,
immutable identity conflicts, root-release blockers, permanent receipt replay,
compact Parquet object corruption, local evidence through GC/reopen, and process
faults before/after `CURRENT` replacement. These are storage-foundation tests,
not actual Branch/Proposal lifecycle or thin-binding journey certification.
The merged #1536 storage work supplies bounded selected physical retention and
labeled growing-history fixtures; #1537 supplies native Version facade/binding
integration. Actual Branch lifecycles are tested by #1352 and actual Proposal
lifecycles by #1356. Neither foundation suite substitutes for those lifecycles.

### Analyst journey comprehension

Use the [journey questions](analyst-ux.md#journey-questions-and-user-stories)
at the corresponding steps of the two-story/shared-character fixture. Preserve
the complete research model; do not test every term before first use. The
primary audience is a nontechnical analyst assisted by an agent, alongside the
direct technical notebook/API path.

| Evidence layer                  | Required observation                                                                                                                                                                                                                                                              | Owner                                                                                                                                                      |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Core behavior                   | Real outputs establish selected scope, evidence/claim distinctions, Branch isolation, Version identity, preview effects, partial acceptance, canonical decisions and replay outcomes.                                                                                             | #1348–#1357 for their operations; #1358 for composition across Rust/Python/Node/CLI.                                                                       |
| Consumer and agent presentation | At each decision, the actual interface or agent tool result exposes the relevant answer, linked to Core output. Record the consumer and package versions, prompt/actions, result and interventions. A fluent agent explanation cannot replace missing state or evidence.          | Associated application repositories; #1209 coordinates candidate first-use evidence without making applications Core dependencies.                         |
| First useful result             | Fresh supported VS Code/agent, agent-led and notebook paths produce the expected result using public instructions. Qualify Kaggle/Colab explicitly; record native/runtime constraints, memory-only state and session reset/retention guidance.                                    | #1209 and its ergonomic repairs, consuming existing clean-install infrastructure and associated-repository evidence.                                       |
| Human comprehension             | Ask an independent participant to explain the current scope, supporting evidence or intended effect at the relevant step, then compare the answer with actual state. Record misunderstanding, assistance and resolution without substituting agent self-reports for participants. | #1209 independent first-use reviewers; #1211 post-release external pilots include nontechnical analysts working with agents as well as the technical path. |

Begin scenario design and consumer checks as capabilities become available;
do not wait for #1358 to discover that a required answer cannot be rendered.
Record installation/prerequisite time separately from hands-on time to the first
useful result, commands/prompts, expected versus observed answers, errors and
undocumented help. Use #1209's existing ten-minute target and honest timing
disposition; no new timing-only CI gate is introduced. Follow-up observations
check whether the participant can continue research, not merely repeat a demo.

Automated fixtures prove behavior and presentation contracts, not human
comprehension or demand. A technically complete v0.6.0 and a usable
agent-assisted analyst experience have distinct evidence. External pilots remain
post-publication; planning and review of the existing journey can happen earlier.
Do not infer participant competence or incomprehension from technical background
alone, and never invent participants or successful hosted-environment runs.

### M11 contract regression scenarios

Quality regime: **A (contracts and deterministic fixtures)**, supplemented by
consumer journey scenarios. The exact owner regressions below exercise shipped
native behavior. #1346 defines the early
[consumer projections](../book/architecture/research-workspaces.md#consumer-interaction-contract);
#1358 composes those operations and publishes their actual results. A passing
owner regression does not, by itself, certify the complete journey.

| Scenario / owner | Given / when / then | Required evidence |
| --- | --- | --- |
| Shared authority and publication failure — #1350, #1355, #1356 | Given Branches in one Project, when update or acceptance fails before `CURRENT` replacement, prior state is unchanged; after replacement the result is committed, not rolled back. Parent change and acceptance receipt always agree. | `API/tests/branch_faults.rs::branch_mutation_faults_preserve_parent_sibling_and_exact_retry_after_reopen`; `UP/recovery.rs::both_current_boundaries_preserve_atomic_baseline_history_and_exact_replay`; `AP/recovery.rs::proposal_acceptance_is_atomic_on_both_sides_of_current_and_replays_after_reopen`; `API/tests/research_versions.rs::restoration_errors_before_and_after_current_preserve_same_facade_and_retry`; `ST/project_portable_v2_import/tests/returned_errors.rs::returned_publication_and_reopen_errors_preserve_commit_evidence_and_retry` (native storage import, not a separate facade fault test). |
| Selective baselines — #1352, #1354, #1355 | Given `x=0, y=0`, incorporate only `x=1`, then compare against upstream `x=2, y=2`: `x=1` is incorporated, not a local edit; `y` retains baseline 0. A later local `x=3` conflicts with upstream 2. Local suppression plus upstream modification requires explicit resolution, never automatic resurrection. | `UP/repeated.rs::repeated_selective_updates_preserve_independent_baselines_and_original_base`; `UP/history.rs::rejected_updates_leave_content_baselines_and_history_unchanged` and `history_pages_bind_generation_branch_and_page_size`; `API/tests/research_comparison.rs::pinned_cursor_rejects_changed_reference_retention`. The shared journey adds real x-only adoption to partial acceptance and restoration. |
| Partial acceptance across Versions — #1354, #1356 | Given accepted subsets from different Branch Versions, when research continues and is reproposed, exact accepted contributions remain distinguishable from new divergence; neither retry nor a new operation identity reapplies them. | `AP/two_stories.rs::two_story_shared_character_journey_preserves_partial_review_and_continued_work`; `AP/partial_review.rs::partial_review_retains_only_accepted_fields_and_deferral_can_be_reviewed_later`; `AP/nested.rs::nested_acceptance_deduplicates_per_destination_and_preserves_contribution`; `API/tests/research_comparison.rs::accepted_subsets_from_distinct_versions_do_not_hide_later_local_edits`; restoration/reproposal evidence below. |
| Selected retention closure — #1349, #1350, #1351, #1352, #1357 | Given a fixed Slice and increasing unrelated parent data, when the parent evolves and unrelated retention roots are released, selected graph/evidence/ontology/baselines survive cleanup while genealogy alone does not retain the complete ancestor. Expansion outside retained history reports unavailable unless separately retained. | `API/tests/branches.rs::selected_branch_releases_large_parent_after_evolution_and_cleanup`; `AP/retention.rs::fixed_parent_proposal_history_releases_obsolete_payloads_but_preserves_accepted_proof_and_replay`; `RI/closure_tests.rs::selected_artifact_bytes_external_limits_and_ontology_survive_reopen`; `AP/interchange.rs::selected_accepted_lineage_roundtrips_without_private_ancestor_or_live_acceptance`. Measurements below distinguish Branch creation, real Proposal history, and storage foundation fixtures. |
| Integration versus canonicality — #1353, #1356 | Given a canonical claim and an alternative, integrate the alternative without promotion, then explicitly promote it: two decisions are visible and integration alone preserves existing canonical choices. | `API/src/research_proposals/tests.rs::frozen_submission_survives_continued_branch_edits_and_retains_only_selected_fields` explicitly checks Integrate followed by separate Promote; `API/tests/research_claims.rs::supported_and_statusless_claims_require_separate_canonical_promotion` and `bring_selected_claim_keeps_classification_without_importing_source_authority` prevent imported status from becoming target authority. |
| Early consumer boundary — #1346 definitions; #1358 composition | Given the two-story fixture, inspect live and immutable references, shared-character boundaries, evidence limitations, and a partial review without unselected private annotations; consumers can render context and decisions from Core results. | `API/tests/slices.rs::shared_character_has_separate_boundary_and_deterministic_explanations`; `AP/two_stories.rs::two_story_shared_character_journey_preserves_partial_review_and_continued_work`; `AP/historical_comparison.rs::historical_comparison_does_not_inherit_later_acceptance_after_reopen`; `RI/reference.rs::live_and_immutable_references_preserve_base_origin_and_authorship`. Published stage outputs and full thin-surface results are recorded in the composition evidence below and the research journey guide. No application deployment or access-enforcement implementation is required. |

Durability scenarios require an admitted filesystem and actual reopen/recovery;
an unsupported filesystem's admission refusal is an environment limitation,
not a passing test or permission to skip the acceptance outcome. Reproducibility uses
a compatible reader; reject unsupported formats without mutation instead of
claiming perpetual latest-reader compatibility or adding pre-v1 migration.
Diagnostics expose bounded identity, phase, and commitment information, never
credentials, raw source content, or private annotations.

The #1536 `research_versions::tests::retention` suite covers real mapped
Parquet projection, ontology-fixture equality, local Artifact retention and
ancestor release after GC/reopen. With 32→512 parent nodes, selected graph
bytes stay at 6,331 and total retained CAS bytes at 16,320; parent payload grows
11,590→38,603 bytes. Only required route controls are copied during source
preparation. A whole-Version control still reads outside-selection rows.
Six labeled frozen-Proposal root fixtures share graph bytes; releasing four
1 KiB Artifact roots reduces CAS bytes 22,042→17,946, preserving accepted
dependencies and original receipts. Corruption, legacy paths, busy CAS and
pre/post-CURRENT crash/error tests cover safe cleanup. These remain storage
consumer fixtures, not actual Branch/Proposal acceptance proof.

### Actual Branch/Proposal composition and measurements (#1358)

`AP/restore.rs::restored_branch_reproposal_cannot_repeat_acceptance_after_cleanup_and_reopen`
executes accept A1(score 1) → A2(score 2) → B(score 73) → parent(score 99) →
restore A1 → repropose → release obsolete Proposal roots → compact → cleanup →
reopen → retry. Restored A remains 1, B remains 73, parent remains 99, the
original receipt survives, and the accepted contribution is not applied twice.
This is actual Proposal acceptance and restoration, not labeled storage roots.

`API/tests/research_claims.rs::branch_challenge_revision_and_suppression_preserve_parent_and_shared_graph`
now checks an Interpretation → Hypothesis immutable successor, its shared
conceptual identity and new Branch/Version origin, retained old category and
payload, and unchanged parent before and after reopen.

The real Branch creation test
`API/tests/branches.rs::selected_branch_releases_large_parent_after_evolution_and_cleanup`
measures fixed selection with increasing unrelated parent data. A fresh child
process opens an already-prepared durable Project and performs the native
creation. Fixture construction is excluded from the child. Its pre-create and
post-create Linux VmHWM values are observed process high-water resident memory,
including startup/open; their difference is **not** exact allocated or
materialized bytes. Other platforms may omit VmHWM while executing the same
behavior checks. No hardware-dependent latency or flat-memory threshold is
asserted.

Observed on the admitted filesystem at the process root with
`TMPDIR=/home/ubuntu/gf-test-1537` and
`CARGO_TARGET_DIR=/home/ubuntu/gf-target-1536`:

| Unrelated parent nodes | Source graph bytes | Selected graph bytes | Copied source bytes | Retained CAS bytes after cleanup | Creation µs | Pre-create HWM KiB | Post-create HWM KiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 16,064 | 7,212 | 0 | 30,724 | 1,803,472 | 70,728 | 127,212 |
| 2,048 | 146,298 | 7,212 | 0 | 30,345 | 514,061 | 70,608 | 131,324 |

Each parent additionally contains one selected Character. Identity encodings
can change byte totals between runs. Assertions require source growth, selected
graph difference below 1,024 bytes, retained-CAS growth at most 8,192 bytes,
zero source copies, actual reclamation, unavailable released parent history,
stable selection/genealogy, and exact selected content after reopen. This
measurement does not include local Artifacts or ontology modules;
`RI/closure_tests.rs::selected_artifact_bytes_external_limits_and_ontology_survive_reopen`
provides their native closure evidence separately.

For the other growth dimension,
`AP/retention.rs::fixed_parent_proposal_history_releases_obsolete_payloads_but_preserves_accepted_proof_and_replay`
uses real submit/review/release operations on a fixed parent with increasing
frozen Proposal history. It checks release blockers, reclaimed obsolete payload,
retained accepted dependencies and receipt replay. It makes no unlimited-storage
or indefinite-payload-retention claim. The #1536 numbers above remain foundation
controls, not measurements of this Proposal lifecycle.

Recorded native results for the strengthened owner tests:

- `cargo test -p graphforge-api --test branches --test research_claims -- --nocapture`:
  11 Branch tests and 10 claim tests passed, zero failures or ignores.
- `cargo test -p graphforge-api --test branches -- --nocapture` after the final
  reopened-value assertions: 11 passed, zero failures or ignores; table values
  above are from this run.
- `cargo test -p graphforge-api --lib restored_branch_reproposal_cannot_repeat_acceptance_after_cleanup_and_reopen -- --nocapture`:
  1 passed, zero failures or ignores.

**Composition evidence:** the shared corpus is
`tests/fixtures/analyst-journey-v1/corpus.json`. The real Rust/Python/Node/same-build
CLI journeys pass through focus, evidence closure, independent Branch edits,
selected upstream update, partial acceptance, continued work, restoration,
receipt replay, complete/selected interchange, Fork and cleanup/reopen.
`tests/fixtures/analyst-journey-v1/output/manifest.json` authenticates 32 captured
files: native Arrow streams and serialized control results, with two explicitly
labeled derived assertion summaries. The [journey guide](../guide/research-journey.md)
maps analyst questions to these fields and separates original accepted, restored,
projected and live references.

Final local journey results on the admitted ext4 filesystem:

- `cargo test -p graphforge-api --test research_journey -- --nocapture`: 1 passed,
  including the external-only Artifact refusal before/after import and cleanup.
  `GRAPHFORGE_JOURNEY_CAPTURE_DIR` recorded the checked-in native outputs.
- `uv run --no-sync python crates/graphforge-bindings-py/tests/research_journey.py`:
  1 passed after the native release rebuild.
- `node --test crates/graphforge-bindings-node/tests/research-journey.test.mjs crates/graphforge-bindings-node/tests/non-cypher-release-parity.test.mjs`:
  4 passed, zero skips, after the native release rebuild.
- The same-build CLI harness produced by `cargo test --workspace`,
  `research_journey-ca49e24bb51a0c1e --nocapture`: 1 passed. Its comparison retains
  exact columns, schema fields and metadata except the per-query identity.

The Rust capture additionally selects an external-only Artifact and proves
`GF_RESULT_NOT_RETAINED` without fetching replacement bytes. Thin runners share
the local scan/OCR route. Shared-journey ontology equality uses the default
ontology; the nonempty extension/physical privacy/recovery cases are the owner
regressions mapped above, not inferred from this default fixture.

This composition exposed an exact portable-import replay defect: rebuilding
omitted adjacency indexes inserted a new wall-clock time into the publication
fingerprint. Reconstruction now records the documented unknown time `0`, while
freshness remains topology-generation-based. Exact content and changed-request
checks are unchanged. `cargo test -p graphforge-storage --lib project_portable_v2_import`
passes 19 tests, including byte-identical rebuild, packaged-index preservation,
real compact import/replay, changed-generation/package refusal, valid reopened
adjacency and pre/post-publication error evidence. Ordinary changed-surface CI
and the PR merge gate still apply.

### Immutable Version facade evidence (#1537)

`crates/graphforge-api/tests/research_versions.rs` exercises real graph,
ontology and local Artifact equality after compaction, source-generation cleanup
and reopen, independent checkpoint deletion, root-blocked deletion, receipt
survival after payload release, complete Project restoration, and stale-facade
replay. Subprocess failpoints cover pre- and post-CURRENT errors on the same
facade. An empty snapshot restored over a file-backed graph is a separate
regression. A storage projection fixture refuses omitted graph/metadata instead
of inventing empty historical state. The registered contract is checked against
native Arrow fields and retention limits; malformed-request sentinel tests on
all thin surfaces reject disclosure of private values or field names.
Binding parity lives in `crates/graphforge-bindings-py/tests/research_versions.py`
and `crates/graphforge-bindings-node/tests/research-versions.test.mjs`; CLI evidence
uses the same-build binary in `crates/graphforge-cli/tests/research_versions.rs`.
These are native Version tests. The actual Branch and Proposal lifecycle tests
listed above supply their separate product evidence.

### Reproducible Slice evidence (#1351)

`crates/graphforge-api/tests/slices.rs` uses real native graph execution for
search/filter/query/direct selection agreement, a character shared across
stories, deterministic expansion/contraction, separate evidence dependencies,
page cancellation and resource refusal. Frozen membership survives parent edits
and payload release; changed selectors cannot exchange page cursors. A projected
Version fixture survives compaction, ancestor deletion, cleanup and reopen;
explicit outside expansion works only while its separately retained source
exists. Live outside objects remain present to prove there is no current fallback.

The 2,048-object case verifies bounded one-row IPC output and refusal of a smaller
active-selection ceiling. Final response size includes schema/cursor/context
metadata. The JSON contract is checked against native Arrow fields and defaults.
Python `tests/slices.py`, Node `tests/slices.test.mjs`, and same-build CLI
`tests/slices.rs` exercise the actual Rust facade, including frozen inspection,
revision and safe malformed-input diagnostics. These tests certify Slice
membership/context; the Branch retention and interchange owner tests listed
above supply retention ownership and export-packaging evidence.

## What counts as proof

| Claim | Acceptable evidence | Not enough alone |
| --- | --- | --- |
| Cypher semantics | TCK BDD / `make test-tck`; facade `execute` tests returning Arrow | Parser-only or logical-plan unit tests |
| Analyst verbs / find | `graphforge-api` surface tests + non-Cypher inventory rows | Binding wrapper that never calls Rust |
| Persistence / reopen | Facade lifecycle + kill-reopen / recovery suites | “Wrote Parquet files” without reopen readback |
| Binding parity | Same-SHA clean-install wheel/addon; Arrow/IPC and error-code equality | Import smoke or stubbed natives |
| Concurrency contract | Frozen short matrix in PR CI; stress lane is diagnostic | Stress retries used as the merge gate |
| publish-track publication | Exact SHA + same-SHA Binding RC retained bytes + `publish.yaml` no-rebuild | Green PR CI on an unrelated SHA; release-certification/checkpoint alone |
| Human release close | publish-track honesty **plus** documented release-certification / surface gates when required | Treating every human-close gate as a publish-track blocker |

Failure handling for matrix or RC failures: let safe lanes finish, census
symptoms, group by root cause, fix with earlier regression coverage, freeze a
new SHA, and rerun the full gate once — never hide flakes with skips or
weakened assertions (`AGENTS.md`).

## Strategy map

| Layer | What it verifies | Tools / entrypoints |
| --- | --- | --- |
| Unit | Crate-local logic (parse, lower, storage helpers) | `cargo test` inline + crate `tests/` |
| Integration / facade | Lifecycle, verbs, reopen, concurrency contracts | `graphforge-api` workspace tests |
| Language compliance | openCypher semantics | `cargo test -p graphforge-api --test bdd` / `make test-tck` |
| Binding / IPC | Python & Node projections match Rust semantics | pytest, Node BDD, Arrow/IPC equality |
| Contract gates | Non-Cypher public surface inventory + evidence | `scripts/ci/non-cypher-surface-gate.py`, surface-gate workflows |
| Agent skills | Offline pack/install, compatibility, schema fail-closed | `pnpm test:agent-skills`, `pnpm smoke:agent-skills` |
| Scale posture | Fixed-hop `LIMIT` materialization bounds | `make bench-fixed-hop-limit` (shape gate; see scale-limits) |
| Policy / docs | Format, lint, license, docs build | `make pre-push`, `.github/workflows/docs.yml` |

## Behavior coverage

PR CI does **not** enforce full `llvm-cov` floors, by design. Use
`make coverage-rust` locally (or when claiming floor changes). Default maintainer
loop is `make pre-push-fast`; run full `make coverage` / `make pre-push` when the
changed surface needs coverage honesty.

The floors are enforced by the **Coverage** workflow
(`.github/workflows/coverage-baseline.yml`).

It runs on every push to `main`, enforces every floor, and records the baseline.
Its patch total compares `HEAD` against the previous `main` commit rather than a
merge base, because post-merge the merge base with `origin/main` is `HEAD` itself
and would measure an empty patch.

Runs never cancel. An earlier draft cancelled in-progress runs, which on a day
with 21 merges would have left the baseline unmeasured entirely.

Coverage does not run on pull requests. That is policy, not omission, and
`scripts/ci/test-coverage-rust.sh` refuses any workflow a pull request can
trigger that invokes it. The trade is deliberate: pull-request cycles stay fast,
and the floors are enforced one merge later instead of never.

### Rust coverage evidence

`make coverage-rust` measures four explicit totals: core Rust, Python adapter
Rust, Node adapter Rust, and their merged workspace. The adapter totals come
from the functional native acceptance suites—not placeholder binding tests—and
therefore include persistence/reopen, structured lifecycle errors, parity, and
no-fallback behavior executed through the instrumented PyO3 and napi-rs
artifacts.

The run uses an isolated `CARGO_TARGET_DIR` (defaulting under its output tree),
builds each native artifact once, and verifies that the loaded artifact hash
matches the measured object.
`build/coverage-rust/ledger.json` also binds the evidence to `HEAD`, the current
`origin/main` merge base, and the LLVM toolchain. Missing, empty, malformed,
stale, wrong-artifact, or wrong-SHA evidence fails before totals are accepted.
Core has an 80% floor, every non-binding production crate has an independent
80% floor, and changed executable Rust lines have a 90% floor. The floors are
fixed values, not a ratchet: an aggregate floor set above the measured value
cannot be met while the codebase grows faster than its marginal coverage rate. Each Rust
binding adapter also retains its
independent 80% floor; neither the merged workspace percentage nor a strong
crate can average away a failed surface. Patch coverage uses executable lines
from the core LCOV report, so documentation, tests, blank lines, and non-Rust
changes do not manufacture measured production coverage. Core, per-crate, and
patch production totals exclude crate-level `tests/`, `benches/`, and
`examples/` sources plus executable lines inside `#[cfg(test)]`-gated Rust
items. The source scan is comment-, string-, and brace-aware and fails closed
when it cannot prove an item's boundary; native binding adapter totals remain
unfiltered because their functional runtime suites are the measured surface.

| Experience / Requirement | Scenario (Given/When/Then) | Test / evidence |
| ------------------------ | -------------------------- | ---- |
| FR-1 Cypher → Arrow | Given a graph, when `execute` runs, then Arrow rows match | Workspace/`graphforge-api` query tests; TCK corpus |
| FR-2 Analyst verbs | Given a graph, when a verb runs, then Arrow scores/rows return | `graphforge-api` analyst-verb/find surface tests; non-Cypher gate |
| FR-3 Project reopen | Given a published project, when reopened, then reads see published state | `cargo test -p graphforge-api --test public_lifecycle_conformance`; composite recovery suites |
| FR-4 Ontology modes | Given exploratory vs strict, when labels/violations occur, then accept or fail closed | Ontology round-trip / mode tests; agent bootstrap mode conflicts |
| FR-5 Layer isolation | Given knowledge by UUID, when Cypher runs, then graph-only baseline holds | Layer/boundary regression coverage |
| FR-6 Binding parity | Given the same op on Rust/Python/Node, when compared, then Arrow/IPC agrees | Binding RC / concurrency parity suites |
| FR-7 Fail closed formats | Given unsupported container, when opened, then no mutation | Project format compatibility tests |
| FR-8 Structured errors | Given writer-busy / capability gap, when called, then stable code | Facade + skills adapter error contracts |
| NFR-1 TCK | Given the authoritative corpus, when BDD runs, then runnable scenarios pass | `make test-tck` (3897 scenarios) |
| NFR-7 Surface inventory | Given public non-Cypher methods, when gate runs, then all classified | `tests/contracts/non-cypher-rust-surface.json` + gate script |

## Traceability contract

| Link | Evidence |
| --- | --- |
| Public behavior → architecture / ADR | [`ARCHITECTURE.md`](ARCHITECTURE.md), [`../adr/`](../adr/) |
| Behavior → BDD / scenario | TCK scenarios for Cypher; documented behavior tables for other surfaces |
| Scenario → test | Paths above; contract manifests under `tests/contracts/` |
| Public API → contract inventory | Versioned manifests under `tests/contracts/` and their gate scripts |

## Evaluation against product goals

- **Language correctness:** TCK green on the release lineage (full runnable
  denominator — not a local subset).
- **Surface completeness:** every public non-Cypher method classified with
  linked evidence; a green TCK run cannot substitute for the inventory.
- **Embedded invariants:** zero-config, local-first, portable project —
  re-proven in release close-out checklists, not only unit tests.
- **Agent usability:** skills and structured errors exercised in
  release-candidate scenarios when those gates are in scope.
- **Scale honesty:** fixed-hop LIMIT materialization shape gate; wall-clock
  reported but not treated as a cross-machine SLO
  ([`../reference/scale-limits.md`](../reference/scale-limits.md)).

## Running the tests

```bash
# Default maintainer loop (policy/format; ~30s)
make pre-push-fast

# Changed-surface validation
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
make test-tck

# Coverage-sensitive changes / floor claims (local; not PR CI)
make coverage-rust
# Full local gate when needed
make pre-push

# Non-Cypher surface (Rust)
python3 scripts/ci/non-cypher-surface-gate.py
python3 scripts/ci/test-non-cypher-surface-gate.py
cargo test -p graphforge-api \
  --test public_lifecycle_conformance \
  --test algorithm_public_surface \
  --test search_public_surface

# Agent skills
pnpm test:agent-skills
pnpm smoke:agent-skills

# Docs (when editing published pages)
pnpm docs:build
pnpm docs:check-links
```

Targeted iteration may use crate filters (`cargo test -p graphforge-cypher`). Keep native
builds isolated with `CARGO_TARGET_DIR`; limit concurrent heavy builds
(`AGENTS.md`). Literal `graphforge-api` integration test binary names above are checked-in
identifiers; they are not product milestone labels.

## Continuous integration

| Workflow surface | Role |
| --- | --- |
| `.github/workflows/test.yml` (Test Suite + CI Gate) | Classified PR/`main` policy, Rust, bindings, concurrency short matrix (not full llvm-cov) |
| `.github/workflows/binding-release-candidate.yml` | Multi-OS Binding RC for publish-track and human close (exact SHA) |
| Non-Cypher / recovery / load gate workflows | Human-close / milestone publication evidence (not publish-track blockers) |
| `.github/workflows/docs.yml` | Starlight `pnpm docs:build` |
| `.github/workflows/publish.yaml` | publish-track and human publication path (retained Binding RC bytes; no rebuild) |

Merge requires green required checks and CI Gate at the exact head SHA.
publish-track and human-close workflows certify registry publication; they are
not close rituals for ordinary implementation issues. Details:
[`.github/workflows/README.md`](../../.github/workflows/README.md).

### Mutation evidence

Line coverage proves a line executed, not that anything asserted its result.
`cargo-mutants` is the check that a test fails when the code under it is wrong.
It is a developer tool installed like `cargo-llvm-cov`, not a `Cargo.toml`
dependency, and it ships nothing:

```bash
cargo install cargo-mutants --locked
cargo mutants --file <path/to/module.rs> --package <crate> -- --tests
```

Always scope it to the module a change touches. A workspace-wide run rebuilds
and retests once per mutant and does not finish at this size; one module is
minutes. Tests that write project directories need `TMPDIR` on a filesystem the
admission policy accepts, the same as the native pre-push runs, because the
default temporary directory is refused as `filesystem_class_unproven`.

Report the score and every surviving mutant. A survivor is either killed by a
further test or justified individually; "mutation testing was impractical" is
not a disposition.

## Test data & environments

- Prefer hermetic temp project directories; no shared mutable fixtures across tests.
- Release-load and scale fixtures are generated through approved bulk publication APIs.
- TCK corpus and contract JSON manifests are checked in; do not silently shrink denominators.
- Skills smoke packs twice and requires identical SHA-256 hashes; offline `npm install` only.
