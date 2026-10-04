# Publishing

A change that passes [`TESTING.md`](TESTING.md) becomes versioned artifacts only through
one workflow, `.github/workflows/publish.yaml`. GraphForge publishes library artifacts and
docs — not a hosted multi-tenant service. The steps for cutting a release are in
`RELEASING.md` at the repository root.

Pushing a `v<version>` tag builds every artifact from that commit, smoke-tests it, and — after
approval of the `release` environment — publishes to crates.io, PyPI, and npm, creates the
GitHub Release, then installs from the public registries and runs a smoke test. A manual run
or a pull request that touches the release path is a dry run that uploads nothing. Each registry
step skips a version that is already published, so a failed tag run is re-run as-is.

## Artifacts and destinations

| Artifact | Destination | Versioned by | Owner |
| --- | --- | --- | --- |
| Rust crates (`graphforge-*`, including `graphforge-cli`) | crates.io | Same release version | Maintainers |
| Python package (wheels/sdist) | PyPI | Same release version | Maintainers |
| Node binding package | npm | Same release version | Maintainers |
| Lifecycle CLI package | npm (`@curatelabs/graphforge-cli`) | Same release version | Maintainers |
| Agent skills package | npm (`npx` skills) | Same release line | Maintainers |
| Source archive / GitHub Release | GitHub | Tag `v<version>` | Maintainers |
| Documentation site | Astro Starlight (`docs-site/`; CI via `docs.yml`) | Commit / release | Maintainers |

## Versioning and release history

- The project uses **Semantic Versioning**; release history lives in immutable
  GitHub Releases and their generated or explicitly supplied notes.
- Pre-1.0 (`0.x`) may include breaking changes; v0.5 documents explicit lack of pre-v1
  project-format compatibility.
- Commit messages follow Conventional Commit–style scopes used in the repo history; do not
  add new enforcement without maintainer agreement.
- The tag must be `v<workspace version>`. A prerelease (`X.Y.Z-rc.N`) is published to npm
  under the `next` dist-tag; a release under `latest`.

## Build and continuous delivery

```bash
# Local validation before tagging
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
make check

# Docs site (Starlight)
pnpm docs:build

# Publication tooling
python3 scripts/set_release_version.py --check
make publish-dry-run   # package every crate in publish order without uploading
```

The publish job runs only for a tag push and needs the wheels, sdist, npm tarballs, and crate
packaging jobs from the same run. The tag must equal `v<workspace version>`, and Cargo, Python,
Node, CLI, and skills versions must agree (`set_release_version.py --check`). npm and crates.io
writes use trusted publishing (GitHub Actions OIDC; npm with provenance); PyPI uses
`uv publish`. A crate name that has never been published is uploaded with the repository
secret `CARGO_REGISTRY_TOKEN`, because crates.io Trusted Publishing cannot create a crate.

Required TESTING.md gates (TCK, applicable contract gates) are enforced by `CI Gate` on the PR
that lands the release commit on `main`; the tag run does not repeat them.

## Environments and promotion

| From | To | Required evidence / approval |
| --- | --- | --- |
| PR branch | `main` | Focused PR, green CI Gate, clean review threads |
| `main` commit | Dry run | Manual `publish.yaml` run: everything built, packed, and smoke-tested; nothing uploaded |
| `v<version>` tag | Registries and GitHub Release | Approval of the `release` environment; `publish.yaml` publishes |
| Published artifacts | Verified | The `verify-published` job installs from PyPI and npm and runs a smoke test |
| `main` docs | Public docs site | Green `docs.yml` / Starlight build for the deployed commit |

## Deployment verification

- **Docs:** `pnpm docs:build` / docs workflow green; published URLs resolve to current Guide +
  Book + allowlisted lifecycle pages.
- **Packages:** the `verify-published` job installs `graphforge` from PyPI and
  `@curatelabs/graphforge` and `-cli` from npm and runs smoke tests; it does not install the
  crates or the agent-skills package. Confirm those on the registries by hand.
- **Skills:** packed artifact hashes and offline compatibility check
  ([`../agent-skills.md`](../agent-skills.md)).
- Do not rebuild different bytes under the same version if a step fails; the registry steps skip
  a version already published.

## Rollback and recovery

- **Partial failure:** re-run the failed tag run; versions already on a registry are skipped.
  Build artifacts are kept for 7 days; after that, re-run all jobs.
- **Registries:** yank or follow registry-specific yank/deprecate procedures; never overwrite
  an already-published version with different bits.
- **GitHub Release / tag:** do not move a release tag to a different commit; cut a new version
  if needed.
- **Docs site:** redeploy last known-good commit from `main` / hosting history.

## Official references

- [Semantic Versioning 2.0.0](https://semver.org/)
- [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)
- `.github/workflows/publish.yaml`
