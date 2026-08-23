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

- 16 `graphforge-*` crates on crates.io;
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
| `crates` | All 16 `.crate` archives |
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

## Registry truth contracts

Registry observations are fresh, bounded evidence about one manifest node. A
GitHub Actions job conclusion is never a package state.

| Registry | Authoritative public observation | Verified identity and metadata | Digest comparison |
| --- | --- | --- | --- |
| PyPI | `GET https://pypi.org/pypi/{name}/{version}/json` | project name/version, Apache-2.0, and the exact wheel/sdist filename set | every `urls[].digests.sha256` equals the candidate |
| npm | `GET https://registry.npmjs.org/{encoded-name}/{version}` | package name/version, Apache-2.0, and exact first-party dependency versions | a supported `dist.integrity` SHA-256 or SHA-512 token equals the candidate |
| crates.io | `GET https://crates.io/api/v1/crates/{name}/{version}` plus `/owners` | crate name/version, Apache-2.0, not yanked, and `DecisionNerd` ownership | `version.checksum` equals the candidate SHA-256 |

The adapters classify results consistently:

- an authoritative 404 with no accepted-write receipt is `absent`;
- an existing identity with different bytes, metadata, dependencies, owner, or
  file set is `conflict`;
- authorization failure or another deterministic invalid operation is `failed`;
- rate limiting, service failure, timeout, malformed evidence, or stale evidence
  is `indeterminate`;
- exact public identity, bytes, and required metadata is `verified`.

An accepted registry write is not yet public verification. Its sanitized receipt
records the node, root version, acceptance time, a visibility deadline no more
than 15 minutes later, and an observation count. One invocation performs one
authoritative observation—there is no internal retry loop or sleep. An absent or
incomplete public response before the deadline becomes
`accepted_pending_visibility`; four observations or the deadline exhaust the
bound and become `indeterminate`. Verification-only continuation can never emit
another write action.

## Recovery planning

`scripts/ci/release_registry.py` creates a machine-readable plan from only:

1. the immutable candidate manifest;
2. fresh normalized registry observations; and
3. current retained-group availability.

Only `absent` nodes whose dependencies are already `verified` and whose exact
artifact group is available can receive a `publish` action. `not_attempted`
nodes receive an observation action, accepted-but-not-visible nodes receive a
visibility-verification action, and `verified` nodes are skipped without an
artifact download. Conflict, indeterminate, failed, stale, dependency-blocked,
or expired-artifact nodes fail closed.

The graph preserves crate topological order and npm native → main → CLI → skills
gates. A registry-scoped recovery plan does not download or schedule unrelated
surfaces. Extra workflow-job history is ignored, and normalized output contains
no response bodies, credentials, authorization headers, cookies, or tokens.

Fixture-driven commands are safe for offline planning:

```bash
python3 scripts/ci/release_registry.py observe \
  --manifest candidate/vX.Y.Z-artifacts.json \
  --node npm:@curatelabs/graphforge \
  --response npm-response.json \
  --observed-at 2030-01-01T12:00:00Z \
  --out npm-observation.json

python3 scripts/ci/release_registry.py plan \
  --manifest candidate/vX.Y.Z-artifacts.json \
  --observations registry-observations.json \
  --availability artifact-availability.json \
  --planned-at 2030-01-01T12:01:00Z \
  --out recovery-plan.json
```

`observe --live` performs the same public, read-only registry requests. Neither
command has a registry-write path.

## Build and validate offline

After the binding workflow has assembled the four directories, it creates a
temporary manifest and runs a clean-consumer rehearsal before any registry
write. The rehearsal installs the compatible exact wheel without an index,
imports the native Python module, validates the full eight-package npm
inventory, then installs only the host-compatible native tarball with
main/CLI/skills offline, loads the Node native binding through the main
package, executes the CLI and agent-skills entrypoints, and validates all
16 crate archives and their exact dependency graph. Only a passing report is
added to the evidence partition;
the temporary manifest is then removed and the final manifest is recorded over
the now-complete inventory.

The rehearsal is a local byte/runtime proof, not a publication or release
certification workflow. It performs zero registry writes and cannot create a
tag. A missing runtime entrypoint or any version divergence fails before the
final candidate exists.

The final validation remains fully offline:

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

## Sequential recovery proof

`scripts/ci/release_rehearsal.py` also produces a stable reconciliation report
for all 25 public nodes. Its sequential simulator accepts only actions emitted
by the pure recovery planner, applies one supplied live observation at a time,
and re-plans from the updated registry truth. This proves dependency order and
partial recovery before workflow parallelism is introduced.

The report records job outcomes such as `cancelled`, `timed_out`, and `skipped`
for operator context, but those labels never determine package state. For
example, a timed-out job whose package is publicly verified is skipped, while
a cancelled job whose package remains authoritatively absent is eligible for a
future write. Accepted-but-not-visible packages receive only a bounded
visibility check; conflict, indeterminate state, and expired artifacts remain
blocked. Every scenario lists every node, its registry state, disposition, and
sanitized job outcome without credentials or raw registry bodies.

## Partitioned orchestration

The Binding Release Candidate retains the manifest and its `python`, `npm`,
`crates`, and `evidence` groups as five separately downloadable 30-day
artifacts. Publication jobs download the manifest plus only their own registry
group and run `release_action.py validate-partition` before observing or
writing. The five native npm packages are the only parallel publication
matrix; fresh verification fans into main, CLI, and skills. PyPI and crates.io
remain independent, credential-isolated lanes.

Before a write the lane persists a sanitized immutable attempt record; after a
successful registry response it persists an accepted receipt and performs one
public observation. Later recovery runs load both, so cancellation, timeout,
or propagation delay cannot become permission for a second write. There is no polling loop. The `always()`
reconciliation job then observes all 27 nodes and combines those states with
job conclusions for operator context. See
[`publication-order.md`](publication-order.md) for the recovery and human stop
decisions.
