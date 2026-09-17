# ADR 0036: The GraphForge release version contract

**Status:** Accepted

**Date:** 2026-09-17

**Build target:** v0.6.0-rc.1 and later

**Supersedes:** [ADR 0017](superseded/0017-unified-release-version.md) (one
version across core and adapters),
[ADR 0033](superseded/0033-prerelease-version-identity.md) (prereleases share
one version with per-ecosystem spelling) and
[ADR 0034](superseded/0034-canonical-release-candidate-spelling.md) (one
canonical release-candidate spelling, `-rc.N`), in full

**Related:** [ADR 0001](0001-rust-core.md) (Rust core); issues #288 (resumable
publication), #291 (incomplete npm v0.5.0 artifact), #292, #858 (coordinated
prerelease version contract), #1359 (Binding RC repair), #1378 (prerelease
support across the publish surfaces)

## Context

The GraphForge release version contract was decided in three records. ADR 0017
established that one release carries one version everywhere. ADR 0033 replaced
one sentence of ADR 0017 so that a release may be a prerelease with a derived
per-ecosystem spelling. ADR 0034 replaced one clause of ADR 0033 so that the
only admissible prerelease is a canonical release candidate.

Each of the two later records was correct and each left the earlier one
partially wrong. A reader who started at ADR 0017 and stopped there got the
wrong answer about prereleases; a reader who went on to ADR 0033 and stopped
there got the wrong answer about which prerelease spellings are legal. The
contract is one contract and is now recorded once.

**This ADR changes no decision.** It restates, in full and in one place, what
ADRs 0017, 0033 and 0034 together already decided. The three records are
superseded and retained under [`superseded/`](superseded/); their context,
options and consequences remain the record of how each part was reasoned
through.

### Why the contract has the shape it has

GraphForge is one product. Rust owns behavior; the Python and Node packages are
thin adapters over that Rust implementation. The public Rust crates, language
packages, native npm packages, command-line package and agent-skills package are
assembled and released together. Registry immutability makes partial publication
expensive, but it does not change the product boundary, and operational
convenience cannot turn thin adapters into independently versioned products.
The v0.5.0 publication stopped after PyPI, five native npm packages and an
incomplete npm main package had become immutable; a proposed recovery would have
advanced only npm to v0.5.1, so that the same GraphForge version would mean
different behavior and provenance depending on the registry.

Two mechanical facts about registries shape the rest.

**PEP 440 forces a different spelling of the same version.** Python normalizes
`0.6.0-rc.1` to `0.6.0rc1`. Maturin and pip write that normalized form into
wheel filenames, sdist roots and distribution metadata, and the PyPI API returns
it. cargo and npm both carry `0.6.0-rc.1` unchanged. So a prerelease is one
version with two spellings, whether or not the project wants it to be. This is
the same shape of problem as the development suffixes `-dev`, `.dev0` and
`-dev.0`, which are three spellings of one development version.

**SemVer prerelease spellings are not injective into PEP 440.** Normalization
is many-to-one:

| Root version (cargo/npm) | PyPI projection |
| --- | --- |
| `0.6.0-rc.1` | `0.6.0rc1` |
| `0.6.0-rc1` | `0.6.0rc1` |
| `0.6.0rc1` | `0.6.0rc1` |
| `0.6.0-RC.1` | `0.6.0rc1` |
| `0.6.0-rc.01` | `0.6.0rc1` |

Every row projects to one PyPI version, but `0.6.0-rc.1` and `0.6.0-rc1` are
*different versions* to cargo and to npm: different crates.io releases,
different npm tarballs, different resolutions and different SemVer precedence.
Admitting more than one root spelling therefore lets two genuinely different
SemVer versions claim one Python identity, and the failure is silent. Every
cross-surface check compares a Python artifact against the derived Python
spelling, and both roots derive the same one, so a tree whose cargo and npm
surfaces carried `0.6.0-rc1` while its Python surface carried the wheel built
for `0.6.0-rc.1` would pass validation, publish, and become immutable on three
registries under two logical versions.

**npm alone fails open.** cargo and PyPI exclude prereleases from default
resolution, so a published candidate does not reach a consumer who did not ask
for one. npm assigns the `latest` dist-tag whenever none is given, regardless of
whether the version is a prerelease.

## Options considered

These were weighed across the three superseded records; the full deliberation is
in them.

1. **One logical version for the complete first-party release set, admitting a
   single canonical release-candidate spelling with mechanically derived
   per-ecosystem projections.** Chosen. Recovery advances the whole set
   together, and the map from accepted root versions to PyPI projections is
   injective by construction.
2. **Independently version each registry surface.** Registry-local patches
   become easy, but version equality no longer communicates compatibility or
   provenance. Rejected.
3. **Keep core aligned but version the CLI and skills packages independently.**
   These packages participate in one tested release graph and declare exact
   compatibility with the bindings; a second version policy adds another
   recovery and support matrix. Rejected.
4. **Reuse a partially published version after replacing bad bytes.** Public
   registries do not generally permit this, and replacing immutable release
   identity would invalidate recorded checksums and user trust. Rejected.
5. **Keep releases three-component and treat candidate-ness as workflow state.**
   Closest to the original machinery, but it cannot put a candidate in front of
   consumers, which is the point of publishing one. Rejected.
6. **Publish a candidate under a distinct version on npm only.** Reintroduces
   exactly the registry divergence this contract exists to prevent. Rejected.
7. **Admit any SemVer prerelease identifier, requiring only that the root
   spelling round-trips through PEP 440 unchanged.** Rejects `rc1` and `RC.1`
   but not `beta.2` versus `b.2`, which collide the same way. It replaces a
   simple rule with a subtler one and still needs a per-phase table. Rejected.
8. **Admit any SemVer identifier and detect collisions by comparing a root
   version stored in release evidence.** Adds a second version record whose
   agreement with the surfaces must itself be checked, which is the divergence
   this contract forbids, restated one level up. Rejected.
9. **Rely on review rather than a gate.** The failure mode is a spelling
   difference that no automated gate reports. Review is not a gate. Rejected.

## Decision

### Shared release set

Every first-party artifact in one GraphForge release carries the same logical
version:

- all 18 public `graphforge-*` crates on crates.io;
- `graphforge` on PyPI;
- `@curatelabs/graphforge` and its five native npm platform packages;
- `@curatelabs/graphforge-cli`;
- `@curatelabs/graphforge-agent-skills`.

The release version is declared once at the root of the release candidate.
Package records and publication or recovery plans derive from it and cannot
override it. One release version is spelled in each ecosystem's canonical form.
The spellings are mechanically derived from the single root version and are
never chosen per package.

### Admissible release versions

A GraphForge release version is `MAJOR.MINOR.PATCH`, or a release candidate of
one. A release version may carry a release-candidate identifier, and no other
prerelease identifier. The canonical root spelling of a candidate is exactly
`MAJOR.MINOR.PATCH-rc.N`: lowercase `rc`, a single dot separator, and a decimal
candidate counter `N` with no leading zero. The Git tag is
`vMAJOR.MINOR.PATCH` or `vMAJOR.MINOR.PATCH-rc.N`. Any other spelling of a
prerelease — differing in case, in separator, in leading zeroes, or in phase —
is not a GraphForge release version, and release tooling refuses it before any
registry credential is used.

`0.6.0-rc.1` and `0.6.0-rc.10` are release versions. `0.6.0-rc1`, `0.6.0rc1`,
`0.6.0-RC.1`, `0.6.0-rc.01`, `1.0.0-beta.2` and `1.0.0-alpha.3` are not.

Development builds are not releases and keep their ecosystem-specific
development syntax: `-dev`, `.dev0` and `-dev.0`.

### Canonical spellings

| Surface | Spelling of the same version |
| --- | --- |
| crates.io | `0.6.0-rc.1` |
| npm, all eight packages | `0.6.0-rc.1` |
| PyPI | `0.6.0rc1` |

The Python spelling is whatever PEP 440 normalization produces from the root
version. It is computed, not written by hand, and it is not a second version.
Release tooling that compares a Python artifact against the root version
compares against the normalized spelling.

### Why one candidate spelling rather than a validated set

The guarantee this buys is stated once: **the map from accepted root versions to
PyPI projections is injective.** `0.6.0rcN` names exactly one root version,
`0.6.0-rc.N`, for every `N`. A release-path check that compares a Python
artifact against the derived spelling therefore also establishes the root
version, which is what every such check has always been assumed to do. No option
that admits a set of spellings can state that in one line, and a guarantee that
cannot be stated in one line is not one that a future change will preserve.

### A candidate counter is a number, not a label

`N` is a decimal integer with no leading zero, so that candidate identities
order the same way in SemVer precedence and in PEP 440, and `rc.10` follows
`rc.9` rather than sorting between `rc.1` and `rc.2` under any string
comparison. Build metadata is never repurposed as a counter. A failed candidate
advances to the next `N`; a published candidate's identity is immutable and is
never reused.

### A prerelease is never the default install

A prerelease must not become the default version a consumer receives without
asking. cargo and PyPI enforce this themselves. On npm it is enforced
explicitly: every prerelease publishes under a non-default dist-tag, and only a
final release takes `latest`. The dist-tag is derived from the version, not
supplied per invocation, and an unclassifiable version refuses to publish rather
than defaulting.

This applies to all eight npm packages in the release set. A prerelease main
package with a `latest` native package, or the reverse, is a divergence of the
same kind this contract forbids.

### Recovery rule

Independent publication and resumability mean that already verified nodes may
skip work; they do not permit independent version selection. After a partial
publication failure, maintainers may:

1. resume missing artifacts at the same version only when their immutable
   candidate bytes remain valid and the registry version is absent; or
2. issue a coordinated new version for the entire shared release set.

They may not advance only one registry, adapter, CLI, skills package, native
package or crate. Temporary divergence is still divergence. A recovery plan
containing more than one release version fails before every registry write.

Historical partial artifacts remain accurately documented. Tags, GitHub
Releases, registry files and checksum records are never moved, replaced, or
misrepresented as a successful unified release.

### Enforcement

`scripts/set_release_version.py` is the single authority on the release version.
It parses the root version structurally, admits only `MAJOR.MINOR.PATCH` and the
canonical `MAJOR.MINOR.PATCH-rc.N`, and derives the PEP 440 spelling with
`packaging`, additionally verifying that the projection names the same candidate
it came from.

Version tooling validates the Cargo workspace, Cargo lockfile, Python metadata,
Node binding, CLI, skills package and skills compatibility metadata as one set,
applying the canonical spelling for each surface. Candidate validation verifies
that every recorded Python, npm and crates.io artifact carries the root version
in that surface's spelling, and that the candidate holds the exact expected
package inventory. Publication preflight admits only `vMAJOR.MINOR.PATCH` and
`vMAJOR.MINOR.PATCH-rc.N` as release tags, re-parses them through the same
authority so that no noncanonical tag reaches the surface comparison, and
verifies the tag, the repository version surfaces and the candidate record
before credentials can write. Candidate-manifest, registry-observation,
rehearsal and clean-environment verification all resolve the spelling through
that authority rather than re-deriving it.

A release whose surfaces disagree on the logical version fails before every
registry write. Workflow job state never authorizes a version override.

## Consequences

### Positive

- One version identifies one GraphForge product across every installation path,
  and adapter compatibility and Rust-core provenance remain understandable.
- Release notes, support reports, SBOMs and checksum records share one key.
- Recovery cannot quietly turn operational failure into ecosystem drift.
- A release candidate can be installed by consumers who ask for one, on every
  supported path, without fragmenting product identity.
- One release, one root spelling, one projection per registry, checkable in one
  place. The silent two-version outcome described above is unreachable.
- The Python spelling is an explicit, derived, tested value rather than an
  assumption that the same literal string appears in every filename.
- The npm default-install hazard is closed by policy rather than by remembering
  a flag, and multi-digit candidates order correctly by construction.

### Negative

- A defect in one immutable registry artifact can require a coordinated patch
  release across packages whose bytes did not otherwise change, and more
  packages may be published during recovery than a registry-local policy would
  require.
- Candidate construction and recovery planning need fail-closed version
  validation.
- Release tooling must carry two spellings of one version and know which surface
  wants which. Code that compares a version to an artifact must select the right
  one, and getting it wrong is a silent mismatch rather than a loud failure.
- Compatibility ranges expressed in npm semver do not match prerelease versions
  by default, so any range that must admit a candidate needs to say so.
- A prerelease consumes a version identity that cannot be reused, so a candidate
  and its final release are distinct immutable registry facts.
- Publishing an alpha or beta phase now requires a new decision rather than just
  a version string. That is deliberate: each additional phase reintroduces the
  collision question (`beta.2` and `b.2` project alike) and should be decided
  with the injectivity argument in front of the decider.
- A maintainer who types `0.6.0-rc1` gets a refusal rather than a release. The
  diagnostic names the canonical form.

### Compatibility and follow-up

No published GraphForge release carries a prerelease identifier, so nothing
existing is invalidated by the candidate rules. The incomplete v0.5.0 npm
package and other partial v0.5.0 artifacts remain historical registry facts.

Issue #288 owns the manifest, observation, recovery and orchestration changes
that apply the shared-release-set and recovery rules without replaying already
verified work. Issue #1378 owns prerelease support across the publish surfaces
and the audit of every release-path assumption that a single literal version
string appears in a crate, a tarball and a wheel filename alike. Issue #858
remains the owner of the coordinated prerelease contract across preparation,
Binding RC, publication, recovery, registry observation and closeout evidence.
