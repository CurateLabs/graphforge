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

**Implementation:** #1648 (this record), #1644 (CI Gate Rust lane), #1645 (Binding RC Linux
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
a dispatch-only harness, `.github/workflows/build-lane-measurement.yml`, with a
`runner` input of `cargo-test` or `nextest`. It was retired after the comparison
(#1663); its method and input digests are in
[testing.md](../development/testing.md#build-lane-measurement-method-adr-0048).

Three different quantities appear below, and they are not interchangeable:

- the **test step**: compile plus test execution, the part the two build
  systems actually do differently;
- the **Bazel job wall**: the test step plus about 4 min of steps a Cargo lane
  would also run, mostly the lifecycle producer (p50 about 165 s) and setup;
- **CI Gate or merge latency**: set by the slowest required job of the run.
  Nothing here measures it.

Bazel, from CI history (the last 150 `test.yml` runs):

- **Rust-changing PRs** (50 runs): test step p50 1114 s, job wall p50
  22.4 min. Only 8 of the 50 replayed their tests from cache.
- **Non-Rust changes** (39 runs): the test step replays in about 25 s, and the
  job wall p50 is about 4.4 min.
- **Rust-changing merge groups** (33 runs): 15 replayed a tree already tested
  on the PR (job wall p50 3.7 min). The other 18 reran (job wall p50
  25.4 min).

Cargo, from the dispatch harness (test step only):

- **nextest, warm `target/` volume** (9 runs on the final harness): p50 996 s
  (843–1235 s). One of the nine, `fc09f6a1`, had one failing test (#1643);
  it still executed the whole suite and its 926 s is included. Paired with the
  Bazel test step on three Rust-changing PR SHAs whose two lanes passed,
  nextest took a median of 1028 s against 1075 s and was faster in 2 of 3
  pairs. A fourth pair, `f6d88ab9`, is excluded from this successful-lane
  comparison: its Bazel authoritative test step failed after 1014 s, while
  nextest passed in 899 s. Failed or cancelled historical runs are retained
  in the issue census rather than treated as completed green timings.
  These different-SHA samples do not establish a repeatability or noise bound,
  and the small sample does not establish a general performance difference.
  This is a small-sample lane comparison, not evidence of a CI Gate speedup.
- **nextest, empty `target/`** (2 runs): 1412 s and 954 s.
- **`cargo test`, warm**: 1388–1754 s on the same PR SHAs, 1.4–1.8× both of
  the above.
- **Excluded samples**: fifteen earlier nextest runs on superseded harness
  commits. Ten, on `5bbf944e`, failed before running any test because nextest
  could not list a custom-harness target. Five, on `59e91659`, ran the suite
  but rebuilt the custom-harness targets under per-package features, adding
  1–3 min. Two of those five were the first run on an empty cache volume.
  #1618 lists every run with its harness commit.
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
- Projected, not measured: on Rust-changing PRs the Rust lane's test step
  stays about the same, and the lane's job wall stays near Bazel's once the
  shared steps are added back. Non-Rust changes skip the lane. Whether CI Gate
  latency changes depends on which job is slowest, which #1644 will observe.
- Projected: Rust merge groups that Bazel would have replayed now rerun. At the
  measured test-step p50 of 996 s, that adds about 16 min to the lane for
  roughly half of Rust merge groups.
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
  Rust-changing PRs, reproducing the recorded method on the same runner class.
- A publication requirement needs a hermetic, remotely cached release build.

## Evidence

- Measurement method, harness digests, and cohort digest:
  [testing.md](../development/testing.md#build-lane-measurement-method-adr-0048).
  The harness itself was retired in #1663 and remains in Git history. Results
  and run ids are on #1618.
- CI history classification: the last 150 `test.yml` runs, split by whether
  the diff touches `*.rs`, `Cargo.toml`, `Cargo.lock`, or `BUILD.bazel`.
