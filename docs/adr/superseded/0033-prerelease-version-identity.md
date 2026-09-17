---
title: "ADR 0033: Prereleases share one version with per-ecosystem spelling"
adr: "0033"
status: "Superseded by ADR 0036"
date: "2026-09-17"
superseded_by: "0036"
---

# ADR 0033: Prereleases share one version with per-ecosystem spelling

> **Superseded.** This record is retained for history. The GraphForge
> release version contract is stated in full in
> [ADR 0036: The GraphForge release version contract](../0036-release-version-contract.md),
> which consolidates ADRs 0017, 0033 and 0034 without changing any decision
> they made.

**Build target:** v0.6.0-rc.1 and later

**Supersedes:** Partial update to ADR 0017 (the `MAJOR.MINOR.PATCH` restriction
on public releases)

**Related:** ADR 0017 (one version across core and adapters), issue #1378
(prerelease support across the publish surfaces), issue #1359 (Binding RC
repair)

## Context

ADR 0017 established that one GraphForge release carries one version across
every registry, so that version equality communicates compatibility and
provenance. That invariant is not in question and this ADR does not weaken it.

ADR 0017 also wrote the invariant in a specific form:

> Ecosystem-specific development syntax may differ (`-dev`, `.dev0`, or
> `-dev.0`), but a public release normalizes to the same exact
> `MAJOR.MINOR.PATCH` value everywhere.

That sentence permits per-ecosystem spelling for development builds and forbids
it for anything public. It admits exactly two version shapes: a three-component
release, and a development build of one.

The maintainer has decided that v0.6.0-rc.1 is published to crates.io, PyPI and
npm. A release candidate is public, so under the sentence above it must be
three-component, which a release candidate is not. The decision and the recorded
wording cannot both stand.

The restriction also cannot be avoided by tagging alone. Publication derives the
version from the tag by stripping the leading `v`, asserts the tag equals `v`
plus that version, and then requires the in-tree workspace version to match. Tag
and workspace version are one fact expressed twice.

Three further facts shaped this decision.

**PEP 440 forces a different spelling of the same version.** Python normalizes
`0.6.0-rc.1` to `0.6.0rc1`. Maturin and pip write that normalized form into
wheel filenames, sdist roots and distribution metadata, and the PyPI API returns
it. cargo and npm both carry `0.6.0-rc.1` unchanged. So a prerelease is one
version with two spellings, whether or not the project wants it to be.

**This is the same problem ADR 0017 already solved once.** The development
suffixes it sanctions, `-dev`, `.dev0` and `-dev.0`, are three spellings of one
development version. Prereleases need the same treatment, not a new mechanism.

**npm alone fails open.** cargo and PyPI exclude prereleases from default
resolution, so a published candidate does not reach a consumer who did not ask
for one. npm assigns the `latest` dist-tag whenever none is given, regardless of
whether the version is a prerelease. Publishing a candidate without an explicit
tag would make it the default install.

## Options considered

1. **Extend the shared-version rule to permit a prerelease identifier, with a
   defined per-ecosystem spelling.** Preserves the ADR 0017 invariant, since one
   logical version still covers the whole set. Requires the release tooling to
   carry two spellings of one version.
2. **Keep releases three-component and treat candidate-ness as workflow state.**
   Build and certify at `0.6.0`, publish nothing, and let the tag carry the
   candidate identity. This is closest to what the machinery does today, but it
   cannot put a candidate in front of consumers, which is the point of
   publishing one.
3. **Publish a candidate under a distinct version on npm only**, where
   prerelease consumption is conventional. Reintroduces exactly the registry
   divergence ADR 0017 exists to prevent.
4. **Give each ecosystem its own version.** Rejected by ADR 0017 and not
   revisited here.

## Decision

### Prereleases are permitted, and remain one version

A GraphForge release version may carry a Semantic Versioning prerelease
identifier. Every first-party artifact in that release carries the same logical
version, exactly as ADR 0017 requires for a final release. The shared release
set, the recovery rule and the prohibition on advancing one surface alone are
unchanged.

The ADR 0017 sentence quoted above is replaced by the following.

> The release version is declared once at the root of the release candidate.
> Package records and publication or recovery plans derive from it and cannot
> override it. One release version is spelled in each ecosystem's canonical
> form. The spellings are mechanically derived from the single root version and
> are never chosen per package.

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

This generalizes rather than replaces the development-suffix allowance in
ADR 0017. Development builds keep `-dev`, `.dev0` and `-dev.0`.

### A prerelease is never the default install

A prerelease must not become the default version a consumer receives without
asking. cargo and PyPI enforce this themselves. On npm it is enforced
explicitly: every prerelease publishes under a non-default dist-tag, and only a
final release takes `latest`. The dist-tag is derived from the version, not
supplied per invocation, and an unclassifiable version refuses to publish rather
than defaulting.

This applies to all eight npm packages in the release set. A prerelease main
package with a `latest` native package, or the reverse, is a divergence of the
same kind ADR 0017 forbids.

### Enforcement

Version tooling validates the Cargo workspace, Cargo lockfile, Python metadata,
Node binding, CLI, skills package and skills compatibility metadata as one set,
applying the canonical spelling for each. Candidate validation verifies every
recorded Python, npm and crates.io artifact carries the root version in that
surface's spelling. Publication preflight accepts a prerelease tag and verifies
it against the repository version surfaces before credentials can write.

A release whose surfaces disagree on the logical version still fails before
every registry write, unchanged from ADR 0017.

## Consequences

### Positive

- A release candidate can be installed by consumers who ask for one, on every
  supported path, without fragmenting product identity.
- The ADR 0017 invariant survives intact: one release, one logical version.
- The Python spelling becomes an explicit, derived, tested value rather than an
  assumption that the same literal string appears in every filename.
- The npm default-install hazard is closed by policy rather than by remembering
  a flag.

### Negative

- Release tooling must carry two spellings of one version and know which surface
  wants which. Code that compares a version to an artifact must select the right
  one, and getting it wrong is a silent mismatch rather than a loud failure.
- Compatibility ranges expressed in npm semver do not match prerelease versions
  by default, so any range that must admit a candidate needs to say so.
- A prerelease consumes a version identity that cannot be reused, so a candidate
  and its final release are distinct immutable registry facts.

### Compatibility and follow-up

Issue #1378 owns the implementation and the audit of every release-path
assumption that a single literal version string appears in a crate, a tarball
and a wheel filename alike. Five of the eight blockers it records trace to PEP
440 normalization, which is the reason the canonical-spelling rule above is
stated as a derivation rather than a convention.

ADR 0017 remains the authority for everything else it decides, including the
shared release set, the recovery rule and the prohibition on advancing a single
surface.
