# Quick Release Guide

GraphForge publishes one coordinated version across Rust crates, PyPI, npm
packages, the CLI, and agent skills. The authoritative order and recovery rules
are in
[`docs/development/publication-order.md`](docs/development/publication-order.md).

## Preconditions

- The release version is aligned across every public package surface.
- Normal PR CI is green on the intended `main` commit.
- The retained partitioned candidate and offline rehearsal are complete.
- PyPI, crates.io, and npm trusted publishing are configured for
  `publish.yaml` (OIDC; no long-lived `NPM_TOKEN`).
- A maintainer has explicitly authorized the immutable tag, GitHub Release, and
  registry writes.

## Release

```bash
# Verify the coordinated version and current branch.
python3 scripts/set_release_version.py --check
git switch main
git pull --ff-only origin main

# Create the immutable tag and GitHub Release with GitHub-generated notes.
git tag -a vX.Y.Z -m "GraphForge vX.Y.Z"
git push origin vX.Y.Z
gh release create vX.Y.Z \
  --title "GraphForge vX.Y.Z" \
  --generate-notes
```

Publishing resumes from the immutable candidate and live registry truth. Never
move a release tag, replace accepted bytes, or advance only one adapter. If a
published version is wrong, use the registry's yank/deprecation mechanism and
prepare one coordinated later GraphForge version.

## Verification

- Confirm the final reconciliation reports all 24 public nodes as verified.
- Exercise clean consumers from crates.io, PyPI, and npm.
- Confirm the GitHub Release notes and public documentation resolve.
- Close the human release tracker only after the public state is complete.

No repository-maintained change history file is part of the release contract.
