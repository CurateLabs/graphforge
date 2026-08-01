# Release candidate manifest

GraphForge publication consumes one immutable, partitioned candidate. The
candidate manifest is the authority for release identity, package inventory,
dependency order, exact bytes, and retained-artifact availability. A matching
checksum proves byte identity; it does **not** prove that a package contains its
required runtime, metadata, and legal files.

This page does not authorize publication or replace the operator stop conditions
in [`publication-order.md`](publication-order.md).

## Canonical contract

`graphforge-release-candidate-v2` has one root `version` and no per-node version
field. The public node set is fixed:

- 15 `graphforge-*` crates on crates.io;
- `graphforge` on PyPI (three tested wheels and one source distribution);
- five native npm packages and `@curatelabs/graphforge`;
- `@curatelabs/graphforge-cli` and
  `@curatelabs/graphforge-agent-skills`.

Every archive records its byte length, SHA-256, SHA-256/SHA-512 SRI integrities,
package identity, required files, member count, and an inventory digest. Validation reopens the
exact archive and compares those facts. It rejects missing Python import/native
surfaces, Node entrypoints or types, native addons, CLI/skills entrypoints, crate
sources, legal files, or exact-version first-party dependency metadata—even when
the recorded checksum matches the incomplete archive.

The dependency graph includes crate-to-crate publication prerequisites, all five
native npm packages before the npm main package, main before CLI, and CLI before
agent skills. It must be complete, refer only to declared nodes, and be acyclic.

## Artifact groups and retention

Candidate bytes are routed into four non-overlapping groups:

| Group | Contents |
| --- | --- |
| `python` | Three tested wheels and one sdist |
| `npm` | Five native packages, main package, CLI, and agent skills |
| `crates` | All 15 `.crate` archives |
| `evidence` | Five tested Node addons plus dry-run and legal reports |

The small manifest lives beside those partitions. Each group declares its
retention period and expiry. Missing, expired, overlapping, unrecorded, or
wrongly routed files fail closed. Later recovery may download only a needed
partition, but it may never rebuild or substitute candidate bytes.

## Publication states

The manifest names the release state vocabulary without deriving state from a
workflow job result: `not_attempted`, `absent`, `accepted_pending_visibility`,
`verified`, `conflict`, `indeterminate`, and `failed`. Registry observation and
recovery planning define how those states are reached; the candidate only fixes
their meanings and the bytes being observed.

## Build and validate offline

After the binding workflow has assembled the four directories, it creates the
manifest and immediately validates the complete candidate before any registry
write:

```bash
python3 scripts/record_release_artifacts.py \
  --version "$RELEASE_VERSION" \
  --dist-dir candidate/release-artifacts \
  --out "candidate/v${RELEASE_VERSION}-artifacts.json" \
  --recorded-at "$RECORDED_AT"

python3 scripts/ci/release-candidate.py validate \
  --record "candidate/v${RELEASE_VERSION}-artifacts.json" \
  --artifacts-dir candidate/release-artifacts \
  --expected-sha "$RELEASE_SHA" \
  --version "$RELEASE_VERSION"
```

The recorder produces stable JSON for the same version, SHA, timestamp, notes,
and exact partitions. The validator uses only local bytes; it performs no
registry access, tag creation, release creation, or publication.

`clean-env-verify.py` continues to accept historical
`graphforge-release-record-v1` documents while also reading the v2 artifact list.
Historical v0.5.0 records remain immutable.
