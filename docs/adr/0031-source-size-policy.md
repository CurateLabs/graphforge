---
title: "ADR 0031: Reviewed source file size bounds"
adr: "0031"
status: "Accepted"
date: "2026-09-16"
superseded_by: null
---

# ADR 0031: Reviewed source file size bounds

## Context

[Issue #1016](https://github.com/CurateLabs/graphforge/issues/1016) requires
oversized modules to be decomposed into cohesive owners without changing public
interfaces, behavior, storage formats, error semantics, resource accounting, or
observability. The initial inventory contained 35 source files above 3,000 lines,
including embedded tests. The extraction sequence establishes smaller owners;
an enforceable bound must keep those outcomes reviewable as the code grows.

The choices considered were retaining advisory size guidance and adopting the
approved strict bound with specific, reviewed exemptions. Advisory guidance
allows case-by-case judgment but does not prevent oversized files from returning.
Strict enforcement gives reviewers a reproducible limit and requires an explicit
architectural decision for a larger cohesive owner. It adds maintenance work when
a file reaches its bound, including its direct tests.

## Decision

Every Git-tracked file recursively beneath `crates/<crate>/src/` has a default
maximum of **3,000 physical lines**, regardless of extension. Count comments,
blank lines, and embedded tests. A newline terminates one physical line; count
the final unterminated line as well. An empty file has zero lines. CRLF does not
count as two lines. Extracted child modules and separate test files obey the same
bound; relocating an oversized body into another oversized file is insufficient.

An exemption must identify one exact source path, a finite `max_lines`, an
accepted ADR reference, and a rationale explaining cohesion and reviewability.
It cannot authorize growth in neighboring files or an entire directory. Remove
an exemption from the policy when its source fits the default bound. This ADR
accepts only the two exemptions below. A changed exemption decision requires a
new ADR; accepted records remain historical decision evidence.

### Adjacency

- **Path:** `crates/graphforge-storage/src/adjacency.rs`
- **Measured size at acceptance:** 3,017 physical lines.
- **Maximum:** 3,500 physical lines.

The spill builder and codec already have separate owners. The remaining
current-format compressed sparse row (CSR) representation, authenticated reads,
freshness inspection, and direct tests implement one bounded derived-index
contract. Reviewing these together makes the representation's validation and
freshness rules visible alongside the operations that depend on them. The
3,500-line cap permits this cohesive owner to remain intact while preventing
unbounded growth. Additional construction or codec responsibilities belong with
their existing owners and do not gain an exemption here.

### Project generation

- **Path:** `crates/graphforge-storage/src/project_generation.rs`
- **Measured size at acceptance:** 3,014 physical lines.
- **Maximum:** 3,500 physical lines.

Committed-generation resolution, initialization, manifest authentication,
generation leases, and corruption tests share the single `CURRENT` authority.
Keeping them together lets reviewers follow how a selected generation is
authenticated and kept alive, including refusal of corrupt state. This is a
bounded authority contract, with a finite 3,500-line cap. It grants no exemption
to publication coordination, recovery, or other lifecycle owners.

## Enforcement and evidence

The [Python checker](../../scripts/source_size_policy.py) reads the
[JSON policy](../../config/source-size-policy.json), enumerates tracked source files
deterministically, and reports each path, measured size, permitted bound, and
applicable ADR. It fails on exceeded bounds, malformed or duplicate configuration,
invalid exact paths or finite bounds, missing or untracked sources, missing or
unaccepted ADR references, and obsolete exemptions.

The checker and its regression tests run in the existing Repository Policy CI
job and `make pre-push-fast`. The full pre-push policy cache includes source
contents, tracked membership, policy, and ADR inputs so a cached result cannot
hide a newly tracked file or changed exemption. The sole required PR status
remains `github-status/CI Gate`, evaluated at the exact PR head.

Strict enforcement lands after the extraction sequence, without temporary
blanket exemptions. The [architecture inventory](../book/architecture/source-modules.md)
records owners and merged evidence. Keep #1016 open until every oversized file
has a merged extraction or accepted exemption and the checker passes on merged
`main`; release-only certification is not part of that completion criterion.

## Consequences

File size becomes a mechanically checked review constraint. Extractions should
keep cohesive production code and direct tests together, preserve public paths
through explicit reexports, and retain shared state and resource ownership.
Physical line counts are deliberately simple and reproducible; they do not
replace behavioral tests or architectural review. Comments and tests remain part
of the review burden and therefore count toward the same limit.

The two exemptions preserve specific cohesive contracts with visible finite
bounds. Future growth beyond a bound requires further decomposition or a new
accepted decision before it can pass repository policy.
