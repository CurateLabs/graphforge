# Releasing

One workflow, `.github/workflows/publish.yaml`, builds, publishes, and checks a
release. GraphForge ships one version across 20 crates (crates.io), the
`graphforge` wheels and sdist (PyPI), and 8 npm packages (five native addons,
`@curatelabs/graphforge`, `-cli`, `-agent-skills`).

## What the workflow does

| Trigger | Result |
| --- | --- |
| Push of a `v*` tag | Builds everything, smoke-tests it, waits for approval of the `release` environment, publishes, creates the GitHub Release, then installs from PyPI and npm and runs a smoke test. |
| Manual run (`workflow_dispatch`) or a pull request touching the release path | Dry run: builds, packs, and smoke-tests everything. Nothing is uploaded. |

Build jobs: 3 wheels (Linux x86_64 manylinux 2_17, macOS arm64, Windows
amd64), the sdist, 5 Node addons, the 8 npm tarballs, and a dry-run package of
every crate. The publish job needs all of them and runs crates.io, then PyPI,
then npm, then the GitHub Release. Every registry step skips a version that is
already published. A tag run fails before any upload if the tag is not
`v<workspace version>`.

## Prerequisites

Registry and repository settings are outside this repository; the workflow
requires them but nothing here proves they are set.

- crates.io, PyPI, and each of the eight npm packages name this repository,
  workflow file `publish.yaml`, and environment `release` in their trusted
  publishing settings. Renaming the file or the environment breaks publishing
  until those settings change.
- The `release` environment has the reviewers who approve a publish. The publish
  job pauses for that approval only if reviewers are configured.
- Trusted Publishing on crates.io cannot create a crate. A crate name that has
  never been published is uploaded with the repository secret
  `CARGO_REGISTRY_TOKEN` (passed to the script as `CARGO_REGISTRY_TOKEN_NEW_CRATES`).
  It must hold a valid crates.io token before a release that adds new crates.
  After a crate's first release, add this repository as its trusted publisher
  on crates.io so later releases use OIDC.
  Five names had never been published when this workflow was written:
  `graphforge-discovery`, `graphforge-filesystem`, `graphforge-observability`,
  `graphforge-portable-oci`, and `graphforge-value`. New crates are rate limited to one per ten minutes;
  the script sleeps until the time crates.io names (at most two hours in total).

## Cut a release

1. Set the version and merge it to `main` through the normal PR flow:
   `python3 scripts/set_release_version.py X.Y.Z`, then
   `make release-version-check`. The command updates Cargo, the lockfile,
   Python, and the Node, CLI, and skills packages together.
2. Optional local check of crate packaging: `make publish-dry-run`.
3. Dry run the merged commit: `gh workflow run publish.yaml --ref main`. Wait
   for it to pass.
4. Tag the merged commit and push the tag. The tag must equal `vX.Y.Z`:

   ```bash
   git switch main && git pull --ff-only origin main
   git tag -a vX.Y.Z -m "GraphForge vX.Y.Z"
   git push origin vX.Y.Z
   ```

5. When the run reaches the publish job, review and approve the `release`
   environment deployment.

## Prerelease

A prerelease version is `X.Y.Z-rc.N` (lowercase `rc`, a dot, no leading zero);
`set_release_version.py` refuses other spellings. Tag it `vX.Y.Z-rc.N`. PyPI
receives the PEP 440 form `X.Y.ZrcN` of the same version. npm publishes it
under the `next` dist-tag and a release under `latest`; the script derives the
tag from the version and refuses a contradicting one. The GitHub Release is
marked as a prerelease.

## After a partial failure

Re-run the failed tag run from the Actions UI; do not move or re-push the tag.
Versions already on a registry are skipped, so only the missing ones upload.
Artifacts from the build jobs are kept for 7 days. After that, use "Re-run all
jobs" so the workflow rebuilds from the same tag; PyPI files are skipped by
file name, so rebuilt wheels are not re-sent. If a published version is
wrong, yank or deprecate it on the registry and release a later version. Never
move a tag.

## Verify

The last job, `verify-published`, waits up to 15 minutes for PyPI and npm to
serve the version, installs `graphforge` from PyPI and the npm packages
`@curatelabs/graphforge` and `@curatelabs/graphforge-cli` from npm, and runs
smoke tests. It does not install the crates or the agent-skills package. Also
confirm by hand that the 20 crates show the version on crates.io and that the
GitHub Release exists.
