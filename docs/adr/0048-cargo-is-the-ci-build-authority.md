---
title: "ADR 0048: Cargo with nextest is the CI build authority; Bazel is removed"
adr: "0048"
status: "Accepted"
date: "2026-09-30"
superseded_by: null
revisit_when: "Merge-queue Rust reruns dominate CI Gate latency, a Cargo lane measures more than 1.25x the replaced Bazel lane on Rust-changing PRs, or a hermetic release build becomes a publication requirement"
---

# ADR 0048: Cargo with nextest is the CI build authority; Bazel is removed

**Status:** Accepted

**Implementation:** #1644 (CI Gate Rust lane), #1645 (Binding RC Linux
wheel and addon), #1646 (Bazel removal).

**Build target:** v0.6.0

**Related:** #1618 (this decision and its measurements), #1 (Bazel as compile
authority), #4 (CI Gate cutover to Bazel), #5 (Blacksmith cache measurement),
#1398 (release version hand-maintained in `MODULE.bazel`), #1643 (a failure
found while measuring), ADR 0036 (release version contract).

## Context

Two build systems describe the same tree. Cargo manifests and `Cargo.lock` own
dependencies, fmt, clippy, fuzz, coverage, packaging, and publication. Bazel
(`rules_rust` with crate-universe) owns the CI Gate Rust compile and test lane
(`//:ci_rust_tests` in the `Bazel Bootstrap` job) and the Linux wheel and Node
addon in Binding RC. Keeping them in step takes 46 `BUILD.bazel` files,
`MODULE.bazel` and its lock, a generated `cargo-bazel-lock.json`, a
Cargo/Bazel drift check, a parity inventory, and a fourth hand-maintained
release version (#1398).

Bazel was adopted for hermetic incremental CI with a remote cache. #1618 asked
for that benefit to be measured rather than assumed. A first pass reported a
5× warm advantage for Bazel. That figure came from sampling: its "warm" runs
were changes that touched no Rust, or merge-queue runs over a tree the PR had
already tested. It also compared against `cargo test`, which runs test binaries
one after another. The corrected measurement is on #1618.

## Measurements

All times are on `blacksmith-4vcpu-ubuntu-2404`, the Bazel lane's runner
class. The results tables and run ids are on #1618. The Cargo side comes from
`.github/workflows/build-lane-measurement.yml`, which is dispatch-only and has
a `runner` input of `cargo-test` or `nextest`.

- **Bazel, Rust-changing PRs** (50 runs): job wall p50 22.4 min. The
  authoritative test step p50 is 1114 s. Only 8 of the 50 replayed their
  tests from cache.
- **Bazel, non-Rust changes** (39 runs): job wall p50 about 4.4 min. The test
  step replays in about 25 s. A fixed lifecycle-producer step of about 165 s
  makes up most of that time.
- **Bazel, Rust-changing merge groups** (33 runs): 15 replayed a tree already
  tested on the PR (wall p50 3.7 min). The other 18 reran (wall p50 25.4 min).
- **Cargo + nextest, warm `target/` volume** (9 runs): test step p50 996 s
  (843–1235 s), compile included. Paired with the Bazel lane on four
  Rust-changing PR SHAs, nextest took a median of 964 s against 1045 s and was
  faster in 3 of 4 pairs. Run-to-run variation is about ±150 s, so the
  difference is within noise.
- **`cargo test`, warm**: 1388–1754 s on the same SHAs, 1.4–1.8× both of the
  above.
- **sccache**: it received zero compile requests in three attempts on these
  runners, so it is not a usable cache mechanism here.
- **Maintenance**: over the trailing 90 days (2026-07-01 → 2026-09-29), 98 of
  441 Rust-touching commits on `main` (22%) also edited a Bazel build
  description.

## Decision

1. **Cargo is the only build description.** The CI Gate Rust lane runs
   `cargo nextest run --workspace --locked` on a sticky `target/` volume. The
   two custom-harness targets (`graphforge-api` `bdd`,
   `graphforge-observability` `disabled_allocations`) and the doctests run
   under `cargo test --workspace --locked`, which keeps workspace feature
   unification.
2. **The lane runs only when Rust inputs change**, as classified by
   `scripts/ci/classify-changes.sh`. Changes that touch no Rust skip it.
3. **Correctness settings carry over.** Bazel's `--config=correctness` forced
   debug assertions and overflow checks on in an optimized build. The Cargo
   dev and test profiles have both on by default. Optimization level is not a
   correctness property and is not carried over.
4. **Bazel is removed**: `BUILD.bazel`, `MODULE.bazel`, its lock,
   `cargo-bazel-lock.json`, `.bazelrc`, `.bazelversion`, `tools/bazel/`, the
   drift, parity, ledger, and cache-perf tooling, the `bazel-bootstrap` and
   `bazel-diagnostics` jobs, and the Bazel native builder in Binding RC.
   Binding RC builds the Linux wheel with maturin and the Linux Node addon with
   napi, as it already does on macOS and Windows.
5. **The gate does not change.** `CI Gate` remains the sole required status.

## Options considered

- **(b) Bazel kept only for a hermetic release build.** This keeps both build
  descriptions and the whole 22% edit tax, but gives up Bazel's replay benefit
  on the PR gate. No publication requirement today needs hermeticity beyond
  `--locked` Cargo builds.
- **(c) Bazel kept as-is.** Its one measured advantage is replaying an
  already-tested tree in the merge queue: about 16 min saved on roughly half
  of Rust merge groups. The cost is two build descriptions on every change,
  and #1618 requires that a Rust change touch exactly one build description.
  Generating `BUILD.bazel` from Cargo would meet that requirement, but it adds
  a generator to keep in step instead of removing the second description.

## Consequences

- Adding a file, test, crate, feature, or dependency edits Cargo only. The
  drift check, the parity inventory, and the `MODULE.bazel` release version
  (#1398) go away.
- Rust-changing PRs keep roughly the same gate latency, and non-Rust changes
  get faster.
- Rust merge groups that Bazel would have replayed now rerun, costing about
  16 min each for roughly half of Rust merges.
- nextest runs test binaries concurrently. That exposed one intermittent
  product failure in 14 runs (#1643). Concurrent execution is the intended
  gate condition, so the fix belongs in the product (#1643), not in the lane.
- Contributors need `cargo-nextest` to reproduce the gate locally. Plain
  `cargo test --workspace` stays valid, but slower.

## Revisit when

- Merge-queue Rust reruns become the dominant share of CI Gate latency. The
  remedy to measure first is skipping a merge group whose tree was already
  tested, not reinstating a second build description.
- A Cargo lane measures more than 1.25× the replaced Bazel lane on
  Rust-changing PRs, using the dispatch harness on the same runner class.
- A publication requirement needs a hermetic, remotely cached release build.

## Evidence

- Measurement method: `.github/workflows/build-lane-measurement.yml`, with the
  `runner` and `cache` inputs. Results and run ids are on #1618.
- CI history classification: the last 150 `test.yml` runs, split by whether
  the diff touches `*.rs`, `Cargo.toml`, `Cargo.lock`, or `BUILD.bazel`.
