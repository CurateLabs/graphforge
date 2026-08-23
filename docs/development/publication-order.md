# Publication and recovery order (v0.5.2)

GraphForge v0.5.2 is one 27-node release: 18 crates.io crates, one PyPI
project, five native npm packages, the npm main package, CLI, and agent skills.
[ADR 0017](../adr/0017-unified-release-version.md) forbids a registry-specific
version. Existing v0.5.0 tags, records, supplements, and published packages are
immutable incident evidence; the v2 workflow rejects v0.5.0 rather than
mutating or reinterpreting those records.

This document describes the executable order in
`.github/workflows/publish.yaml`. It does not authorize a tag, GitHub Release,
or registry write.

## Tracks

| Track | Path | Required before registry write | Deferred |
| --- | --- | --- | --- |
| **publish-track** | Binding RC → tag / release identity → `publish.yaml` | Same-SHA retained candidate + offline rehearsal; no rebuild-on-write | release-load, checkpoint, knowledge/epistemic, full clean-env |
| **Human release close** | publish-track **plus** milestone evidence | Whatever the active runbook requires (may include release-certification / surface gates) | — |

release certification, checkpoint recovery, and knowledge/epistemic assemble human-close / milestone confidence.
They are **not** registry-honesty inputs for `publish.yaml` and must not block
every publish-track run. See [`../engineering/TESTING.md`](../engineering/TESTING.md)
dual-track table and wall-clock targets.

## Candidate preconditions

Before a maintainer authorizes publication (publish-track or human close):

1. The intended tag resolves to the current reviewed `main` commit and the root
   version is aligned across Cargo, PyPI, Node/native npm, CLI, and agent skills.
2. A successful same-SHA Binding Release Candidate run retains five 30-day
   artifacts: `manifest`, `python`, `npm`, `crates`, and `evidence`.
   Skip re-RC when a complete unexpired candidate for that SHA already exists.
3. The v2 manifest validates the complete file inventory and dependency graph.
   A matching checksum alone is not sufficient.
4. `evidence/offline-rehearsal.json` proves the exact partitions passed clean
   Python, Node/native, CLI, and skills consumers and crate/dependency checks
   with zero registry writes.
5. PyPI, crates.io, and npm trusted publishing are configured for
   `CurateLabs/graphforge` and `publish.yaml` (OIDC; no long-lived
   `NPM_TOKEN`). Each of the eight `@curatelabs` npm packages must list that
   workflow as a trusted publisher.
6. The maintainer explicitly enables the immutable tag and GitHub Release
   identity. Implementation CI and ordinary issue close do not run Binding RC
   or the human-close cascade.

## Publish-track orchestration

`.github/workflows/publish-track.yml` runs on a six-hour schedule and by
maintainer dispatch. It resolves the current `main` SHA (or rejects a supplied
SHA that is not current `main`), then looks for one successful Binding RC run
with exactly one unexpired retained artifact for each required group. It
downloads and validates the whole candidate before treating it as reusable.
Therefore an unexpired candidate for the same SHA skips another RC; a missing,
expired, duplicate, or invalid candidate cannot reach tag or registry-write
steps.

For ordinary scheduled runs, the workflow only dispatches the existing
`binding-release-candidate.yml` for an exact current `main` SHA when no valid
candidate is retained. It neither creates a tag nor writes a registry.

To publish a validated candidate, a maintainer dispatches `Publish Track` with:

1. `create_release: true`;
2. `confirm_registry_publish: true`; and
3. `release_tag` exactly equal to `v<root-version>`.

Both booleans are intentional release controls: creating the published GitHub
Release emits the existing release event that starts `publish.yaml`. The
orchestrator refuses an existing Release identity and directs recovery to the
explicit `publish.yaml` recovery dispatch instead. It verifies an existing tag
resolves to the candidate SHA and never moves it. `publish.yaml` remains the
only registry writer and independently revalidates the retained manifest,
partitions, rehearsal, live registry truth, and conflict evidence.

## Planner-driven execution

Every lane downloads the small manifest and only its registry partition,
revalidates those exact bytes, obtains fresh public registry observations, and
asks the pure recovery planner to authorize one node. `verified` nodes are
skipped. Only an authoritatively `absent`, dependency-ready node with an
unexpired partition receives a write action. Conflict, indeterminate state,
failed observation, pending visibility, missing artifacts, or unverified
dependencies stop the lane.

The independent work is:

- PyPI may run independently with OIDC and the Python partition.
- crates.io may run independently with its short-lived trusted-publishing token
  and the crates partition; crates remain in the checked topological order below.
- the five native npm packages run as a fail-slow parallel matrix with npm
  trusted publishing (OIDC + provenance) and the npm partition.

The npm dependency fan-in is strict:

```text
five native packages (parallel)
            ↓ fresh public verification of all five
@curatelabs/graphforge
            ↓ fresh public verification
@curatelabs/graphforge-cli
            ↓ fresh public verification
@curatelabs/graphforge-agent-skills
```

A successful upload response creates a sanitized accepted-write receipt. The
lane performs exactly one immediate public observation. It never polls, sleeps,
blindly retries, or repeats a pending write. If propagation has not completed,
the observation is `accepted_pending_visibility`; a later recovery dispatch
performs another bounded observation rather than another upload.

Immediately before a write, the lane persists an immutable attempt record on
the same GitHub Release. A successful registry response adds a separate
accepted receipt. Later preflight and reconciliation runs load both before
observing. A 404 after an accepted receipt is pending; a 404 after an attempt
without an accepted receipt is indeterminate because a cancelled or timed-out
job may have crossed the registry boundary. Neither state permits a repeat
write. Exhausted evidence requires a human decision.

## Crates.io order

The finite order is generated by `scripts/ci/crate-publish-plan.py`:

1. `graphforge-core`
2. `graphforge-discovery`
3. `graphforge-filesystem`
4. `graphforge-io`
5. `graphforge-observability`
6. `graphforge-ast`
7. `graphforge-knowledge`
8. `graphforge-ontology`
9. `graphforge-provenance`
10. `graphforge-ir`
11. `graphforge-plan`
12. `graphforge-storage`
13. `graphforge-rel`
14. `graphforge-search`
15. `graphforge-cypher`
16. `graphforge-exec`
17. `graphforge-api`
18. `graphforge-cli`

Each invocation validates the retained `.crate` checksum before `cargo publish`.
After an accepted write it observes the public version once. A verified result
may unlock the next crate; pending or unsafe truth stops the finite loop.

## Always-running reconciliation

The final job uses `if: always()` and records every lane conclusion, including
failure, cancellation, timeout, and skip. When the candidate is available it
re-observes all three registries and produces one stable 27-node summary. Job
history is operator context only; registry state and the next safe actions come
from the immutable candidate plus live registry truth.

If candidate preflight failed before a manifest was available, reconciliation
still emits all 27 node identities as `indeterminate` and identifies the
candidate blocker. The workflow is green only when all 27 nodes are publicly
`verified`. The summary is retained for 30 days and contains no credentials,
headers, cookies, tokens, or raw registry bodies.

## Recovery and stop conditions

Re-run `publish.yaml` only for the same immutable tag via `workflow_dispatch`
and provide: the exact `release_tag` (no stale default), a public
`recovery_reason`, and a reviewed `recovery_overlay_sha` (40-char commit on
`main`) whose publisher/recovery scripts may overlay the tag checkout. Do not
overlay floating `origin/main` tip. Concurrent runs for the same tag share
concurrency group `publish-<tag>` with `cancel-in-progress: false`. Registry
write jobs require the GitHub Environment `release`. The new run re-observes
the registries, skips verified nodes without downloading unrelated partitions,
and schedules only eligible absent work.

Stop and require a human decision when:

- an existing public identity has different bytes, metadata, dependency
  versions, license, ownership, or file inventory (`conflict`);
- registry truth is unavailable, stale, rate-limited, malformed, or past the
  bounded visibility window (`indeterminate`);
- retained artifacts are missing or expired;
- a credential or trusted-publisher configuration fails; or
- correction would require different bytes or a different version.

Never move a tag, replace a release asset with different bytes, weaken a check,
or advance only an adapter. For incorrect public bytes, use the registry's
yank/deprecate mechanism and prepare one coordinated later GraphForge version.

## Remaining human decisions

Publish-track automates candidate dispatch and makes the release/publish
boundary explicit. Maintainers still make these decisions:

- choose the final v0.5.1 commit after normal exact-head PR CI;
- explicitly enable both publish-track release controls for the immutable
  v0.5.1 tag and GitHub Release;
- decide any registry-specific yank/deprecation if reconciliation finds a
  conflict; and
- close the human release tracker only after the 27-node summary is complete.
