# Agent environment notes

Practical caveats for running GraphForge in agent sandboxes and shared hosts.
Workflow and architecture rules live in [AGENTS.md](../../AGENTS.md); this page
only records environment behaviour that is easy to misread as a code bug.

## Dependency sync

The native `graphforge` wheel is installed outside the deps-only uv project, so
plain `uv sync --all-extras` uninstalls it. `make install` uses the preserving
form; use the same flag for direct syncs or rebuild afterwards:

```bash
uv sync --all-extras --inexact
pnpm install
```

## Rebuild native bindings after changing Rust

Dependency sync does not build. After pulling or editing Rust, rebuild before
running Python or Node tests. The editable rebuild is fast when `target/` is warm.

```bash
uv run maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml
pnpm --filter @curatelabs/graphforge build
```

## Filesystem admission

Durable, disk-backed projects require an `ext4`, `xfs`, or `btrfs` volume at
the process root. On hosts that do not satisfy this, `GraphForge(path)`,
`gf --project <dir> ...`, and any durable write fail with
`GF_UNSUPPORTED_FILESYSTEM`. This is an environment limitation, not a code bug.
Observed causes:

| Host layout                                       | Reported cause                                           |
| ------------------------------------------------- | -------------------------------------------------------- |
| Root on `overlay` (typical cloud agent VM)        | `filesystem_class_unproven`                              |
| Loopback `ext4` mounted below an unsupported root | `ancestor_cross_volume`                                  |
| `chroot` onto a loopback `ext4`                   | `device_identity_unknown` (device probe)                 |
| Supported root but `TMPDIR` on `tmpfs`            | tests that create projects under `TMPDIR` fail admission |

Consequences:

- **In-memory works everywhere.** `GraphForge()` with no path runs the real
  engine (Cypher parse → IR → plan → rel → exec → Arrow) entirely in memory.
- **Point `TMPDIR` at a supported volume** before running CLI or binding tests
  on a host whose `/tmp` is `tmpfs`, for example `TMPDIR=$HOME/tmp`.
- **Durable-project tests fail on unsupported roots.** The Python unit suite
  and the Node binding suite each contain tests that open a durable project.
  On an overlay root those tests fail, and `node --test` over the full binding
  suite can hang because a failed durable test in `provider-workflow.test.mjs`
  leaves its in-process mock-server `Worker` open. Run that file in isolation
  with `--test-timeout` when diagnosing. Counts change as suites grow; do not
  treat a remembered pass/fail number as a contract.

## Bazel

`bazelisk` must be on `PATH` (Bazel version pinned by `.bazelversion`).
`make pre-push-fast` checks for it and runs the Cargo/Bazel drift check;
`make bazel-test` runs the authoritative `//:ci_rust_tests` suite locally.
See [bazel.md](bazel.md).
