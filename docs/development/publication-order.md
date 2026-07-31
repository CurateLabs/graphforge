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
2. Artifact checksum / SBOM / license record exists for that same commit
   and is retained with the #192 release evidence.
3. First-party packages declare `Apache-2.0` with `LICENSE` / `NOTICE` (and
   third-party notices intact), and the public legal surface in
   [#200](https://github.com/CurateLabs/graphforge/issues/200) is complete.
4. Dry-runs are green for applicable surfaces: Python sdist/wheel packaging and
   clean install, `npm publish --dry-run` for Node + CLI + skills, and docs
   preview. The approved crates.io no-publish disposition replaces a Cargo
   publish dry-run for v0.5.0.
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
| 2 | Publish GitHub Release for `v0.5.0` with final CHANGELOG notes + checksum links/attachments | [#194](https://github.com/CurateLabs/graphforge/issues/194) | Release URL; notes match CHANGELOG; `Publish to PyPI and npm` run starts |
| 3 | `publish.yaml` builds and publishes **Python** to PyPI | [#195](https://github.com/CurateLabs/graphforge/issues/195) | Workflow green; `graphforge==0.5.0` on PyPI; checksums match RC record |
| 4 | Same workflow publishes **Node** `@graphforge/node` and platform packages to npm | [#198](https://github.com/CurateLabs/graphforge/issues/198) | npm `0.5.0`; target inventory and checksums match |
| 5 | Same workflow publishes **NPX CLI** `@graphforge/cli` to npm | [#198](https://github.com/CurateLabs/graphforge/issues/198) | npm `0.5.0`; clean-consumer handoff passed |
| 6 | Same workflow publishes **NPX skills** `@graphforge/agent-skills` to npm | [#198](https://github.com/CurateLabs/graphforge/issues/198) | npm `0.5.0`; packed offline contract passed |
| 7 | Record the approved **no crates.io publication** disposition for v0.5.0 | [#196](https://github.com/CurateLabs/graphforge/issues/196) | Plan script output + this recorded disposition |
| 8 | Deploy / confirm documentation for the release commit/tag | [#197](https://github.com/CurateLabs/graphforge/issues/197) | Pages run green; live anonymous URLs serve the RC docs set |
| 9 | Verify registry/package **metadata** (repo, license, docs, tag) | [#199](https://github.com/CurateLabs/graphforge/issues/199) | Concise surface checklist on #199 |

Notes:

- Steps 3–6 are automated by `.github/workflows/publish.yaml` once the GitHub
  Release is published. Maintainers watch that run; they do not re-build
  different bytes under `0.5.0` if a job fails.
- Before any registry write, the workflow requires the release tag, current
  `main` SHA, five version surfaces, dated CHANGELOG section, Apache-2.0 policy,
  and npm publishing identity to pass its fail-closed preflight.
- Docs deploy today follows `docs.yml` on `main` (GitHub Pages). Step 8 confirms
  the release SHA’s docs set is what is live (or redeploys that SHA); it does
  not authorize inventing a second docs tree under the same version label.
- Clean-environment verification after publication is tracked by
  [#167](https://github.com/CurateLabs/graphforge/issues/167), not this order.

## Crates.io dependency order

Library crates publish (when unblocked) in topological order of workspace path
dependencies, excluding language-binding and CLI crates (those ship via PyPI /
npm, not as the public Rust surface):

1. `gf-core`
2. `gf-ast`
3. `gf-knowledge`
4. `gf-ontology`
5. `gf-provenance`
6. `gf-ir`
7. `gf-plan`
8. `gf-storage`
9. `gf-io`
10. `gf-rel`
11. `gf-search`
12. `gf-cypher`
13. `gf-exec`
14. `gf-api`

Generate or verify this list with:

```bash
python3 scripts/ci/crate-publish-plan.py list
python3 scripts/ci/crate-publish-plan.py check
```

`check` fails closed on known crates.io name conflicts, missing
`version` alongside path deps (required for `cargo publish`), and cycles.

### Crates.io disposition (v0.5.0)

**Final maintainer disposition:** GraphForge v0.5.0 publishes no Rust crates to
crates.io. This is the approved no-publish outcome tracked by
[#196](https://github.com/CurateLabs/graphforge/issues/196); it applies to the
complete 14-crate plan above, so no partial GraphForge crate set may be
published as `0.5.0`.

The disposition is required because **`gf-core` is already owned on crates.io
by an unrelated project**. The excluded `gf-cli` name is also foreign-owned,
and the planned library crates still lack the path-plus-`version` dependency
metadata required by `cargo publish`. Resolving those blockers would require a
separate, maintainer-approved crate naming and publication project; renaming
the crate graph during v0.5.0 release execution is outside #196.

The evidence command remains:

```bash
python3 scripts/ci/crate-publish-plan.py check
```

It intentionally fails closed while reporting the ordered plan's name and
manifest blockers. The release workflow records the same output without
running `cargo publish`. Python, Node, and agent-skills publication may proceed
once their own preconditions hold because they do not depend on crates.io.

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
- [x] Crates.io no-publish disposition is recorded for v0.5.0 (#196); no Cargo
      registry credential is required for this release
- [x] Public Apache-2.0 legal and contribution docs are deployed (#200)
- [ ] Maintainer authorization to create annotated tag + GitHub Release
- [x] Repository and release documentation are publicly readable
