# Publication order and rollback (v0.5.0)

This is the written publication order and stop/rollback policy for GraphForge
registry publication. §6 executors follow this document; do not invent a
different order at publish time.

Tracks [#2801](https://github.com/CurateLabs/graphforge-legecy/issues/2801)
(§5 prep under [#2793](https://github.com/CurateLabs/graphforge-legecy/issues/2793))
and unblocks §6 execution
([#2794](https://github.com/CurateLabs/graphforge-legecy/issues/2794) /
[#742](https://github.com/CurateLabs/graphforge-legecy/issues/742)).

Related operational docs:
[`PUBLISHING.md`](../engineering/PUBLISHING.md),
[`release-process.md`](release-process.md),
`.github/workflows/publish.yaml`.

## Preconditions (must be true before step 1)

1. §5 version freeze is complete: Cargo workspace, Python binding, Node binding,
   NPX skills, and generated metadata all report `0.5.0` (not `0.5.0-dev` /
   `0.5.0.dev0`) on the intended RC commit ([#2796](https://github.com/CurateLabs/graphforge-legecy/issues/2796)).
2. Artifact checksum / SBOM / license record exists for that same commit
   ([#2798](https://github.com/CurateLabs/graphforge-legecy/issues/2798)).
3. First-party packages declare `BUSL-1.1` with `LICENSE` / `NOTICE` (and
   third-party notices intact) ([#2799](https://github.com/CurateLabs/graphforge-legecy/issues/2799)).
4. Dry-runs are green for applicable surfaces: `cargo publish --dry-run` (see
   crates disposition below), TestPyPI/wheel clean-install, `npm publish
   --dry-run` for Node + skills, docs preview
   ([#2800](https://github.com/CurateLabs/graphforge-legecy/issues/2800)).
5. Required release-certification evidence for the RC SHA is assembled per
   `AGENTS.md` / `#742` (Binding RC, load matrix, gates as required for
   **publication**, not for ordinary child-issue close).
6. Legal activation [#2434](https://github.com/CurateLabs/graphforge-legecy/issues/2434)
   is resolved enough that maintainers authorize public registry publication.
7. Secrets and registry ownership are in place (see Human blockers).

## Ordered execution

Execute **in this order**. Do not start a later step until the prior step has
deterministic success evidence (or an explicit maintainer disposition to skip).

| Step | Action | Issue | Evidence |
| --- | --- | --- | --- |
| 1 | Annotate tag `v0.5.0` on the verified RC commit on `main` | [#2802](https://github.com/CurateLabs/graphforge-legecy/issues/2802) | `git rev-parse v0.5.0` == RC SHA |
| 2 | Publish GitHub Release for `v0.5.0` with final CHANGELOG notes + checksum links/attachments | [#2803](https://github.com/CurateLabs/graphforge-legecy/issues/2803) | Release URL; notes match CHANGELOG |
| 3 | `publish.yaml` (triggered by the published release) builds and publishes **Python** to PyPI | [#2805](https://github.com/CurateLabs/graphforge-legecy/issues/2805) | Workflow green; `graphforge==0.5.0` on PyPI; checksums match RC record |
| 4 | Same workflow publishes **Node** `@graphforge/node` to npm | [#2806](https://github.com/CurateLabs/graphforge-legecy/issues/2806) | Workflow green; npm `0.5.0`; checksums match |
| 5 | Same workflow publishes **NPX skills** `@graphforge/agent-skills` to npm | [#2806](https://github.com/CurateLabs/graphforge-legecy/issues/2806) | Workflow green; npm `0.5.0`; checksums match |
| 6 | Publish applicable **Rust crates** to crates.io in dependency order (see disposition) | [#2804](https://github.com/CurateLabs/graphforge-legecy/issues/2804) | Plan script + publish evidence, or recorded skip disposition |
| 7 | Deploy / confirm **versioned docs** for the release commit/tag | [#2807](https://github.com/CurateLabs/graphforge-legecy/issues/2807) | Live docs URL(s) for v0.5.0 set |
| 8 | Verify registry/package **metadata** (repo, license, docs, tag) | [#2808](https://github.com/CurateLabs/graphforge-legecy/issues/2808) | Checklist evidence on #2808 |

Notes:

- Steps 3–5 are automated by `.github/workflows/publish.yaml` once the GitHub
  Release is published. Maintainers watch that run; they do not re-build
  different bytes under `0.5.0` if a job fails.
- Docs deploy today follows `docs.yml` on `main` (GitHub Pages). Step 7 confirms
  the release SHA’s docs set is what is live (or redeploys that SHA); it does
  not authorize inventing a second docs tree under the same version label.
- Clean-environment verification after publication is **§7**
  ([#2795](https://github.com/CurateLabs/graphforge-legecy/issues/2795)), not this order.

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

As of the prep of this document, **`gf-core` and `gf-cli` crate names are
already occupied on crates.io by unrelated projects**. GraphForge must not
attempt to overwrite or squat those names. Until maintainers choose and land a
rename (or an alternate publish set) and crate ownership is confirmed:

- Treat step 6 / [#2804](https://github.com/CurateLabs/graphforge-legecy/issues/2804)
  as **blocked** (human disposition required).
- Do **not** partial-publish a subset under conflicting names.
- Binding and skills publication (steps 3–5) may still proceed once other
  preconditions hold; they do not depend on crates.io.

Record the disposition on #2804 before closing that child.

## Stop conditions

Stop the release train immediately if any of the following occur:

1. Any step fails (tag, GitHub Release, PyPI, npm Node, npm skills, crates.io,
   docs deploy, or metadata verification).
2. Published bytes would differ from the RC artifact record / checksums for
   `0.5.0`.
3. A registry rejects the package for license, ownership, name conflict, or
   trusted-publishing misconfiguration.
4. Legal / counsel halt (#2434) or maintainer halt.

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
final `#742` close.

## Human blockers (checklist)

- [ ] Version freeze PR merged on the RC commit (#2796)
- [ ] `@graphforge` npm organization exists and the publishing maintainer can
      create public organization-scoped packages
- [ ] `NPM_TOKEN` is a granular token authorized for the `@graphforge` scope,
      can bypass 2FA for CI publishing, and is available to `publish.yaml`
- [ ] After the first publish, configure trusted publishing for each npm package
      before removing the token path; npm requires an existing package and a
      supported GitHub-hosted runner (the repository is currently private, so
      npm provenance is not available)
- [ ] PyPI trusted publishing OIDC configured for this repo / environment
- [ ] `CARGO_REGISTRY_TOKEN` (or crates.io trusted publishing) once crates.io
      names are resolved
- [ ] crates.io crate rename / ownership disposition for conflicting names
- [ ] Maintainer authorization to create annotated tag + GitHub Release
- [ ] Legal activation sufficient for public registries (#2434)
- [ ] Repo visibility / registry org access as required for the publish path
