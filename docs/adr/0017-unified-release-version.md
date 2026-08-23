# ADR 0017: One version across core and adapters

**Status:** Accepted

**Date:** 2026-08-01

**Build target:** v0.5.1 and later

**Related:** ADR 0001 (Rust core), issue #288 (resumable publication), issue
#291 (incomplete npm v0.5.0 artifact), and issue #292

## Context

GraphForge is one product. Rust owns behavior; the Python and Node packages are
thin adapters over that Rust implementation. The public Rust crates, language
packages, native npm packages, command-line package, and agent-skills package
are assembled and released together.

The v0.5.0 publication stopped after PyPI, five native npm packages, and an
incomplete npm main package had become immutable. One proposed recovery would
have advanced only npm to v0.5.1 while leaving Rust and Python at v0.5.0. That
would make the same GraphForge version mean different behavior and provenance
depending on the registry. Users and support tooling could no longer infer
which Rust core an adapter exposes from its public version.

Registry immutability makes partial publication expensive, but it does not
change the product boundary. Operational convenience cannot turn thin adapters
into independently versioned products.

## Options considered

1. **One version for the complete first-party release set.** Recovery advances
   the whole set together. This may publish byte-identical packages again at a
   new version, but preserves an unambiguous product identity.
2. **Independently version each registry surface.** This makes registry-local
   patches easy, but version equality no longer communicates compatibility or
   provenance.
3. **Keep core aligned but version CLI and skills independently.** These
   packages still participate in one tested release graph and declare exact
   compatibility with the bindings. A second version policy would add another
   recovery and support matrix.
4. **Reuse a partially published version after replacing bad bytes.** Public
   registries do not generally permit this, and replacing immutable release
   identity would invalidate recorded checksums and user trust.

## Decision

### Shared release set

Every first-party artifact in one GraphForge release uses the same exact
Semantic Version:

- all 18 public `graphforge-*` crates on crates.io;
- `graphforge` on PyPI;
- `@curatelabs/graphforge` and its five native npm platform packages;
- `@curatelabs/graphforge-cli`;
- `@curatelabs/graphforge-agent-skills`.

The release version is declared once at the root of the release candidate.
Package records and publication or recovery plans derive from it and cannot
override it. Ecosystem-specific development syntax may differ (`-dev`,
`.dev0`, or `-dev.0`), but a public release normalizes to the same exact
`MAJOR.MINOR.PATCH` value everywhere.

### Recovery rule

Independent publication and resumability mean that already verified nodes may
skip work; they do not permit independent version selection. After a partial
publication failure, maintainers may:

1. resume missing artifacts at the same version only when their immutable
   candidate bytes remain valid and the registry version is absent; or
2. issue a coordinated new version for the entire shared release set.

They may not advance only one registry, adapter, CLI, skills package, native
package, or crate. Temporary divergence is still divergence. A recovery plan
containing more than one release version fails before every registry write.

Historical partial artifacts remain accurately documented. Tags, GitHub
Releases, registry files, and checksum records are never moved, replaced, or
misrepresented as a successful unified release.

### Enforcement

Repository version tooling validates the Cargo workspace, Cargo lockfile,
Python metadata, Node binding, CLI, skills package, and skills compatibility
metadata as one set. Candidate validation verifies every recorded Python, npm,
and crates.io artifact carries the candidate's root version and exact expected
package inventory. Publication preflight verifies the tag, repository version
surfaces and candidate record before credentials can write.

Release-orchestration work in #288 extends this invariant into the canonical
manifest and recovery planner. Workflow job state never authorizes a version
override.

## Consequences

### Positive

- One version identifies one GraphForge product across every installation path.
- Adapter compatibility and Rust-core provenance remain understandable.
- Release notes, support reports, SBOMs, and checksum records share one key.
- Recovery cannot quietly turn operational failure into ecosystem drift.

### Negative

- A defect in one immutable registry artifact can require a coordinated patch
  release across packages whose bytes did not otherwise change.
- More packages may be published during recovery than a registry-local policy
  would require.
- Candidate construction and recovery planning need stronger fail-closed
  version validation.

### Compatibility and follow-up

The incomplete v0.5.0 npm package and other partial v0.5.0 artifacts remain
historical registry facts. release-certification targets a coordinated v0.5.1 release. Issue #288
owns the manifest, observation, recovery, and orchestration changes needed to
apply this decision without replaying already verified work.
