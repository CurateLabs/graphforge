# Publication order and rollback (v0.5.0)

This is the written publication order and stop/rollback policy for GraphForge
registry publication. §6 executors follow this document; do not invent a
different order at publish time.

Tracks the human release-close issue
[#192](https://github.com/CurateLabs/graphforge/issues/192) and its ordered
native publication children [#193](https://github.com/CurateLabs/graphforge/issues/193)
through [#199](https://github.com/CurateLabs/graphforge/issues/199).

Related operational docs:
[`PUBLISHING.md`](../engineering/PUBLISHING.md),
[`release-process.md`](release-process.md),
`.github/workflows/publish.yaml`.

## Preconditions (must be true before step 1)

1. §5 version freeze is complete: Cargo workspace, Python binding, Node binding,
   NPX CLI/skills, and generated metadata all report `0.5.0` (not `0.5.0-dev` /
   `0.5.0.dev0`) on the intended RC commit.
2. A successful same-SHA `Binding Release Candidate` run has retained
   `M1-Release-Candidate-<sha>` for 30 days. That bundle contains the tested
   wheels/addons, Python sdist, all npm tarballs, all 15 `.crate` archives,
   publication dry-run evidence, license reports, and
   `v0.5.0-artifacts.json`. Publication consumes this bundle; it does not
   rebuild Python/npm bytes.
3. First-party packages declare `Apache-2.0` with `LICENSE` / `NOTICE` (and
   third-party notices intact), and the public legal surface in
   [#200](https://github.com/CurateLabs/graphforge/issues/200) is complete.
4. Dry-runs are green for applicable surfaces: Python sdist/wheel packaging and
   clean install, `npm publish --dry-run` for Node + CLI + skills, and docs
   preview. All 15 Rust crates pass package inventory checks; the topological
   publish plan passes before any crates.io write.
5. Required release-certification evidence for the RC SHA is assembled per
   `AGENTS.md` / `#192` (Binding RC, load matrix, gates as required for
   **publication**, not for ordinary child-issue close).
6. The embedded write modes in
   [#211](https://github.com/CurateLabs/graphforge/issues/211) and the
   Apache-2.0 outcome in
   [#218](https://github.com/CurateLabs/graphforge/issues/218) are complete.
7. Secrets, trusted-publisher configuration, registry ownership, and explicit
   maintainer publication authorization are in place (see Human blockers).

## Ordered execution

Execute **in this order**. Do not start a later step until the prior step has
deterministic success evidence (or an explicit maintainer disposition to skip).

| Step | Action | Issue | Evidence |
| --- | --- | --- | --- |
| 1 | Annotate tag `v0.5.0` on the verified RC commit on `main` | [#193](https://github.com/CurateLabs/graphforge/issues/193) | `git rev-parse v0.5.0^{}` == RC SHA |
| 2 | Publish GitHub Release for `v0.5.0` with final CHANGELOG notes + checksum links/attachments | [#194](https://github.com/CurateLabs/graphforge/issues/194) | Release URL; notes match CHANGELOG; `Publish to PyPI, npm, and crates.io` run starts |
| 3 | `publish.yaml` builds and publishes **Python** to PyPI | [#195](https://github.com/CurateLabs/graphforge/issues/195) | Workflow green; `graphforge==0.5.0` on PyPI; checksums match RC record |
| 4 | Same workflow publishes **Node** `@graphforge/node` and platform packages to npm | [#198](https://github.com/CurateLabs/graphforge/issues/198) | npm `0.5.0`; target inventory and checksums match |
| 5 | Same workflow publishes **NPX CLI** `@graphforge/cli` to npm | [#198](https://github.com/CurateLabs/graphforge/issues/198) | npm `0.5.0`; clean-consumer handoff passed |
| 6 | Same workflow publishes **NPX skills** `@graphforge/agent-skills` to npm | [#198](https://github.com/CurateLabs/graphforge/issues/198) | npm `0.5.0`; packed offline contract passed |
| 7 | Same workflow publishes the complete 15-crate **Rust** surface to crates.io in dependency order | [#196](https://github.com/CurateLabs/graphforge/issues/196) | Workflow green; all packages at `0.5.0`; checksums match; `DecisionNerd` owns each crate |
| 8 | Deploy / confirm documentation for the release commit/tag | [#197](https://github.com/CurateLabs/graphforge/issues/197) | Pages run green; live anonymous URLs serve the RC docs set |
| 9 | Verify registry/package **metadata** (repo, license, docs, tag) | [#199](https://github.com/CurateLabs/graphforge/issues/199) | Concise surface checklist on #199 |

Notes:

- Steps 3–7 are automated by `.github/workflows/publish.yaml` once the GitHub
  Release is published. It resolves the successful, unexpired same-SHA
  `Binding Release Candidate` run, validates every recorded checksum, and
  attaches the checksum record before its first registry write. Python and npm
  publish the exact retained files. The crates.io publisher re-packages the
  immutable tagged tree only after proving each deterministic `.crate`
  checksum equals the retained record. Maintainers watch that run; they do not
  rebuild different bytes under `0.5.0` if a job fails.
- Before any registry write, the workflow requires the release tag, current
  `main` SHA, five version surfaces, dated CHANGELOG section, Apache-2.0 policy,
  and npm publishing identity to pass its fail-closed preflight.
- Docs deploy today follows `docs.yml` on `main` (GitHub Pages). Step 8 confirms
  the release SHA’s docs set is what is live (or redeploys that SHA); it does
  not authorize inventing a second docs tree under the same version label.
- Clean-environment verification after publication is tracked by
  [#167](https://github.com/CurateLabs/graphforge/issues/167), not this order.

## Crates.io dependency order

Rust crates publish in topological order of workspace path dependencies. The
Python and Node binding implementation crates remain private to Cargo because
their public artifacts ship through PyPI/npm; the Rust CLI is public:

1. `graphforge-core`
2. `graphforge-ast`
3. `graphforge-knowledge`
4. `graphforge-ontology`
5. `graphforge-provenance`
6. `graphforge-ir`
7. `graphforge-plan`
8. `graphforge-storage`
9. `graphforge-io`
10. `graphforge-rel`
11. `graphforge-search`
12. `graphforge-cypher`
13. `graphforge-exec`
14. `graphforge-api`
15. `graphforge-cli`

Generate or verify this list with:

```bash
python3 scripts/ci/crate-publish-plan.py list
python3 scripts/ci/crate-publish-plan.py check
```

`check` fails closed on non-`graphforge-*` names, missing `version` alongside
normal path dependencies (required for `cargo publish`), and cycles. Workspace
dev-dependencies remain path-only so Cargo excludes those test-only edges from
published manifests, preventing first-publication cycles.

### Crates.io publication decision (v0.5.0)

**Final maintainer decision:** GraphForge v0.5.0 publishes the complete
15-crate Rust surface above to crates.io under `graphforge-*` names. This is
tracked by [#196](https://github.com/CurateLabs/graphforge/issues/196); no
partial alternative package set is permitted.

The previous `gf-*` names were abandoned because `gf-core` and `gf-cli` are
owned by unrelated projects. On 2026-07-31 the official crates.io API returned
not-found for all 15 `graphforge-*` names. Every normal workspace path
dependency declares both `path` and version `0.5.0` so Cargo can publish the
runtime graph; path-only dev-dependencies are intentionally not published.

The evidence command remains:

```bash
python3 scripts/ci/crate-publish-plan.py check
```

The release workflow invokes `scripts/publish_crates.py` after the npm surfaces.
That publisher packages each crate, checks its SHA-256, publishes once, waits
for crates.io indexing, verifies `DecisionNerd` ownership, and resumes only
when an existing `0.5.0` checksum matches the local archive.

## Stop conditions

Stop the release train immediately if any of the following occur:

1. Any step fails (tag, GitHub Release, PyPI, npm Node, npm skills, crates.io,
   docs deploy, or metadata verification).
2. Published bytes would differ from the RC artifact record / checksums for
   `0.5.0`.
3. A registry rejects the package for license, ownership, name conflict, or
   trusted-publishing misconfiguration.
4. Legal or maintainer halt.

**Hard rule:** do not rebuild different bytes and re-publish under the same
version. Yank or deprecate per registry rules if needed; cut a new patch
version for corrected bits.

## Rollback

| Surface | Recovery |
| --- | --- |
| Annotated tag | Do **not** move `v0.5.0` to another commit. Leave the tag; cut `v0.5.1` (or later) for corrections. |
| GitHub Release | Edit notes only if text-only; do not replace attached artifacts with different checksums under the same tag. |
| PyPI | Yank the bad file(s) if policy allows; publish a new version for fixed bits. |
| npm | Deprecate or unpublish per npm policy; publish a new version for fixed bits. |
| crates.io | Yank the bad version; publish a new version for fixed bits. |
| Docs site | Redeploy last known-good commit from `main` / Pages history. |

Agents assemble evidence and automation; maintainers authorize execution and
final #192 close.

## Human blockers (checklist)

- [ ] Version freeze PR merged on the RC commit and every SHA-bound release
      certification workflow passes for that exact commit
- [ ] The same-SHA `Binding Release Candidate` run contains an unexpired
      `M1-Release-Candidate-<sha>` bundle and its release record validates
- [ ] `@graphforge` npm organization exists and the publishing maintainer can
      create public organization-scoped packages
- [ ] `NPM_TOKEN` is a granular token authorized for the `@graphforge` scope,
      can satisfy the account's 2FA policy for CI publishing, is stored as a
      repository Actions secret, and passes the workflow's `npm whoami` preflight
- [ ] After the first publish, configure trusted publishing for each npm package
      before removing the token path; npm requires an existing package and a
      supported GitHub-hosted runner
- [ ] PyPI trusted publishing OIDC is configured for
      `CurateLabs/graphforge`, workflow `publish.yaml`, with no environment name
- [x] All 15 `graphforge-*` names were available on crates.io when checked
- [x] The crates.io credential is stored in Pulumi ESC
      `curatelabs/graphforge/release` and projected to the repository's
      encrypted `CARGO_REGISTRY_TOKEN` Actions secret
- [x] Public Apache-2.0 legal and contribution docs are deployed (#200)
- [ ] Maintainer authorization to create annotated tag + GitHub Release
- [x] Repository and release documentation are publicly readable

The exact operator commands, evidence queries, and stop points are in
[`v0.5.0-release-operator-runbook.md`](v0.5.0-release-operator-runbook.md).
