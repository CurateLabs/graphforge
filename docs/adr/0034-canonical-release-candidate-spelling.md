# ADR 0034: One canonical release-candidate spelling, `-rc.N`

**Status:** Accepted

**Date:** 2026-09-17

**Build target:** v0.6.0-rc.1 and later

**Supersedes:** Partial update to ADR 0033 (the unrestricted clause "A
GraphForge release version may carry a Semantic Versioning prerelease
identifier", under "Prereleases are permitted, and remain one version")

**Related:** ADR 0017 (one version across core and adapters), ADR 0033
(prereleases share one version with per-ecosystem spelling), issue #858
(coordinated prerelease version contract), issue #1378 (prerelease support
across the publish surfaces)

## Context

ADR 0033 permitted a release version to carry a prerelease identifier, and
required that the per-ecosystem spellings of that version be mechanically
derived from a single root version rather than chosen per package. It named
`0.6.0-rc.1` in every example and in its canonical-spelling table, but it stated
the permission itself without restricting the identifier's form:

> A GraphForge release version may carry a Semantic Versioning prerelease
> identifier.

Read literally, that admits any SemVer-legal identifier — `rc.1`, `rc1`,
`RC.1`, `rc.01`, `beta.2` — as the root version of a GraphForge release. The
release tooling written against it did exactly that: it accepted any identifier
that `packaging.version.Version` could parse into a plain prerelease of the
base.

That is unsound, for a reason ADR 0033 did not consider.

**SemVer prerelease spellings are not injective into PEP 440.** Normalization
is many-to-one:

| Root version (cargo/npm) | PyPI projection |
| --- | --- |
| `0.6.0-rc.1` | `0.6.0rc1` |
| `0.6.0-rc1` | `0.6.0rc1` |
| `0.6.0rc1` | `0.6.0rc1` |
| `0.6.0-RC.1` | `0.6.0rc1` |
| `0.6.0-rc.01` | `0.6.0rc1` |

Every row projects to one PyPI version. But `0.6.0-rc.1` and `0.6.0-rc1` are
*different versions* to cargo and to npm: different crates.io releases,
different npm tarballs, different resolutions, and different SemVer precedence.

So permitting more than one spelling per release lets two genuinely different
SemVer versions claim one Python identity. The failure is silent rather than
loud. Every cross-surface check in the release path compares a Python artifact
against the derived Python spelling, and both roots derive the same one, so a
tree whose cargo and npm surfaces carried `0.6.0-rc1` while its Python surface
carried the wheel built for `0.6.0-rc.1` would pass validation, publish, and
become immutable on three registries under two logical versions. That is the
precise outcome the ADR 0017 one-version invariant exists to prevent, arriving
through the mechanism ADR 0033 introduced to protect it.

Issue #858 — open, milestone M12, on the critical path for the coordinated
v0.6.0 release, and predating ADR 0033 — already specified the contract this
requires. It calls for lowercase `rc`, a dot-separated numeric candidate
identifier and no leading zero in `N`; it lists `0.6.0-rc1`, `0.6.0rc1`,
`0.6.0-RC.1` and `0.6.0-rc.01` as spellings that must be rejected; and it names
"Supporting multiple canonical spellings" as an explicit non-goal. ADR 0033 did
not cite it.

## Options considered

1. **Admit one canonical identifier form, `rc.N`.** Restores injectivity by
   construction: the accepted root spellings and their PyPI projections are in
   bijection, so a Python artifact identifies exactly one root version. Costs
   the ability to publish `alpha`/`beta` phases without a further decision.
2. **Admit any SemVer identifier, and additionally require that the root
   spelling round-trips through PEP 440 unchanged.** Rejects `rc1` and `RC.1`
   but not `beta.2` versus `b.2`, which collide the same way. It replaces a
   simple rule with a subtler one and still needs a per-phase table.
3. **Admit any SemVer identifier, and detect collisions by comparing the root
   version stored in release evidence.** Adds a second version record whose
   agreement with the surfaces must itself be checked, which is the divergence
   ADR 0017 forbids, restated one level up.
4. **Keep ADR 0033 as written and rely on review.** The failure mode is a
   spelling difference that no automated gate reports. Review is not a gate.

## Decision

### The only prerelease a GraphForge release may carry is a release candidate

The clause quoted above is replaced by the following.

> A GraphForge release version may carry a release-candidate identifier, and no
> other prerelease identifier. The canonical root spelling is exactly
> `MAJOR.MINOR.PATCH-rc.N`: lowercase `rc`, a single dot separator, and a
> decimal candidate counter `N` with no leading zero. The Git tag is
> `vMAJOR.MINOR.PATCH-rc.N`. Any other spelling of a prerelease — differing in
> case, in separator, in leading zeroes, or in phase — is not a GraphForge
> release version, and release tooling refuses it before any registry
> credential is used.

`0.6.0-rc.1` and `0.6.0-rc.10` are release versions. `0.6.0-rc1`, `0.6.0rc1`,
`0.6.0-RC.1`, `0.6.0-rc.01`, `1.0.0-beta.2` and `1.0.0-alpha.3` are not.

Everything else ADR 0033 decides stands unchanged: the derivation rule, the
canonical-spelling table, the PyPI projection as a projection and not a second
version, the non-default dist-tag rule for npm, and the enforcement section.
Development builds keep `-dev`, `.dev0` and `-dev.0` under ADR 0017.

### Why one spelling rather than a validated set

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

### Enforcement

`scripts/set_release_version.py` is the single authority. It parses the root
version structurally, admits only the canonical form, and derives the PEP 440
spelling with `packaging`, additionally verifying that the projection names the
same candidate it came from. Publication preflight admits only
`vMAJOR.MINOR.PATCH` and `vMAJOR.MINOR.PATCH-rc.N` as release tags and then
re-parses through the same authority, so no noncanonical tag reaches the
surface comparison. Candidate-manifest, registry-observation, rehearsal and
clean-environment verification all resolve the spelling through that authority
rather than re-deriving it.

## Consequences

### Positive

- One release, one root spelling, one projection per registry, checkable in one
  place. The silent two-version outcome described above is unreachable.
- The contract issue #858 specified is the contract the tooling enforces, and
  the two no longer have to be reconciled at release time.
- Multi-digit candidates order correctly by construction, on every surface.

### Negative

- Publishing an alpha or beta phase now requires a new decision rather than
  just a version string. That is deliberate: each additional phase reintroduces
  the collision question (`beta.2` and `b.2` project alike) and should be
  decided with the injectivity argument in front of the decider.
- A maintainer who types `0.6.0-rc1` gets a refusal rather than a release. The
  diagnostic names the canonical form.

### Compatibility and follow-up

No published GraphForge release carries a prerelease identifier, so nothing
existing is invalidated. Issue #858 remains the owner of the coordinated
prerelease contract across preparation, Binding RC, publication, recovery,
registry observation and closeout evidence; this ADR records the identity rule
those surfaces enforce. ADR 0033 remains the authority for the derivation rule
and the per-ecosystem spelling table, and ADR 0017 for the shared release set,
the recovery rule and the prohibition on advancing one surface alone.
