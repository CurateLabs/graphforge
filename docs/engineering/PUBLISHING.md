# Publishing

A change that passes [`TESTING.md`](TESTING.md) becomes versioned artifacts only through
the documented release path. GraphForge publishes library artifacts and docs — not a hosted
multi-tenant service. Operational detail:
[`../development/publication-order.md`](../development/publication-order.md)
(authoritative §5/§6 order + rollback),
[`../development/release-process.md`](../development/release-process.md),
[`../development/release-workflows.md`](../development/release-workflows.md), and
`.github/workflows/publish.yaml`.

## Artifacts and destinations

| Artifact | Destination | Versioned by | Owner |
| --- | --- | --- | --- |
| Rust crates (`graphforge-*`, including `graphforge-cli`) | crates.io | Same release version | Maintainers |
| Python package (wheels/sdist) | PyPI | Same release version | Maintainers |
| Node binding package | npm | Same release version | Maintainers |
| Lifecycle CLI package | npm (`@curatelabs/graphforge-cli`) | Same release version | Maintainers |
| Agent skills package | npm (`npx` skills) | Same release line | Maintainers |
| Source archive / GitHub Release | GitHub | Annotated tag | Maintainers |
| Documentation site | Astro Starlight (`docs-site/`; CI via `docs.yml`) | Commit / release | Maintainers |

## Suggested versioning and change history

- The project already uses **Semantic Versioning** and Keep a Changelog (`CHANGELOG.md`).
- Pre-1.0 (`0.x`) may include breaking changes; v0.5 documents explicit lack of pre-v1
  project-format compatibility.
- Patch vs minor conventions and checklists:
  [`../development/release-process.md`](../development/release-process.md).
- Commit messages follow Conventional Commit–style scopes used in the repo history; do not
  add new enforcement without maintainer agreement.
- The coordinated **v0.5.1** publication (GitHub milestone **M1**, tracked on
  [#192](https://github.com/CurateLabs/graphforge/issues/192)) is the current
  release close-out target. Partial v0.5.0 registry records remain immutable
  historical evidence and must not be overwritten.

## Build and continuous delivery

```bash
# Local validation before release candidate freeze
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
make pre-push

# Docs site (Starlight)
pnpm docs:build

# Publication tooling — authoritative order:
# docs/development/publication-order.md
python3 scripts/ci/crate-publish-plan.py check
# Cargo: package the complete 15-crate graph in dependency order.
make publish-dry-run-cargo
# Python: maturin / TestPyPI clean-install checks
# Node / CLI / skills: npm publish --dry-run
```

The release-event workflow runs `release-publish-preflight.py`, validates the
complete partitioned candidate and offline rehearsal, and obtains fresh public
registry truth before any registry write. The release tag must resolve to the
reviewed `main` SHA; Cargo, Python, Node, CLI, and skills versions must match
the tag; and the dated CHANGELOG section must be cut. `npm whoami` runs only in
an npm write step so registry credentials remain isolated.

Required TESTING.md gates (TCK, contract gates applicable to the release, binding RC evidence)
must be green on the **same SHA** that is tagged for publication.

## Environments and promotion

| From | To | Required evidence / approval |
| --- | --- | --- |
| PR branch | `main` | Focused PR, green CI Gate, clean review threads |
| `main` SHA | Release candidate | Complete v0.5.1 candidate manifest and offline rehearsal (tracker [#192](https://github.com/CurateLabs/graphforge/issues/192)) |
| Release candidate | Registries + GitHub Release | Dry-runs, checksums/SBOM where configured, human release execution |
| Published artifacts | Clean-install verification | Fresh pip/npm/Cargo consumers use only public registries |
| `main` docs | Public docs site | Green `docs.yml` / Starlight build for the deployed commit |

## Deployment verification

- **Docs:** `pnpm docs:build` / docs workflow green; published URLs resolve to current Guide +
  Book + allowlisted lifecycle pages.
- **Packages:** clean-environment quickstart / smoke from public registries only
  ([`../development/clean-environment-verification.md`](../development/clean-environment-verification.md),
  tracker [#167](https://github.com/CurateLabs/graphforge/issues/167)).
  Fail closed when the requested version is unpublished — never check off against missing
  artifacts.
- **Skills:** packed artifact hashes and offline compatibility check
  ([`../agent-skills.md`](../agent-skills.md)); post-publish NPX bootstrap is a clean-env lane.
- Versions and checksums match the release record; do not rebuild different bytes under the
  same version if a step fails — stop and recover per the release plan.

## Rollback and recovery

Authoritative stop/rollback table:
[`../development/publication-order.md`](../development/publication-order.md).

- **Registries:** yank or follow registry-specific yank/deprecate procedures; never overwrite
  an already-published version with different bits.
- **GitHub Release / tag:** do not move an annotated release tag to a different commit; cut a
  new patch version if needed.
- **Docs site:** redeploy last known-good commit from `main` / hosting history.
- Authority: maintainers explicitly authorize the immutable tag, GitHub
  Release, registry writes, and final closure of the v0.5.1 publication
  tracker ([#192](https://github.com/CurateLabs/graphforge/issues/192)).

## Official references

- [Semantic Versioning 2.0.0](https://semver.org/)
- [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)
- [Keep a Changelog](https://keepachangelog.com/)
- [`../development/publication-order.md`](../development/publication-order.md)
- [`../development/release-process.md`](../development/release-process.md)
- [`../development/release-workflows.md`](../development/release-workflows.md)
- `.github/workflows/publish.yaml`
