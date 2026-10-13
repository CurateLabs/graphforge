# GraphForge vendoring notice

This directory is a vendored copy of `datafusion-physical-plan` 54.1.0 from
crates.io (the registry crate `datafusion-physical-plan-54.1.0`, every file
except cargo's `.cargo-ok` marker). It is third-party code under the Apache
License 2.0; `LICENSE.txt` and `NOTICE.txt` are the upstream files, unchanged.

The root `Cargo.toml` selects it with `[patch.crates-io]` and lists it under
the workspace `exclude`, so fmt, clippy and the workspace test run never lint or
test it. Cargo matches each `exclude` entry as a literal path (a `vendor/*` glob
is not recognised), so each vendored crate is named there.

## The one applied change

Apache DataFusion pull request
[#24891](https://github.com/apache/datafusion/pull/24891), upstream commit
`09a2aff72d88c16499ae3266a943a4cd9841b618`, ported to the 54.1.0 spill-pool
API. It changes only `src/spill/spill_pool.rs`; every other file is identical
to the 54.1.0 crate. `git diff` of that file against the first commit that added
this directory is the whole delta. The port keeps the upstream semantics and
adapts only names and docs to the 54.1.0 API (one cloneable `SpillPoolWriter`
and one `channel` function instead of `SpillPoolSink`, `spsc_channel` and
`mpsc_channel`). The upstream tests that need 55-only I/O hooks are not ported;
the tests added in the same file cover the fix on the 54.1.0 API.

To run the vendored crate's spill tests, use a checkout whose root is not nested
inside another cargo workspace:
`cargo test --locked --manifest-path vendor/datafusion-physical-plan/Cargo.toml --lib spill`.
The regression test that CI runs is in `graphforge-exec`
(`session::spill_pool_regression_tests`).

## Why

[apache/datafusion#24883](https://github.com/apache/datafusion/issues/24883):
`RepartitionExec` deadlocks under a memory limit when its spill pool holds two
open files. The reader parks on a file that never receives another batch while
the writer has already moved on to a second file and nobody wakes the reader.
The fix is on upstream `main` and in no release (not 54.1.0, not 55.2.0).
GraphForge cannot take it by upgrading, so it carries the fix here.

## Removal

Delete this directory, the `[patch.crates-io]` entry and the
`vendor/datafusion-physical-plan` workspace exclude in the root `Cargo.toml` when GraphForge adopts a DataFusion
version that contains #24891. Re-verify with `cargo tree -i
datafusion-physical-plan` that a single registry copy resolves.

## Scope

`[patch]` applies to builds from this repository: the Python wheel, the Node
addon and the CLI. Crates published to crates.io resolve stock DataFusion,
because published manifests do not carry `[patch]` sections.
