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
- **Run durable tests through the shared environment launcher.** It selects
  `../.<checkout-name>-test-tmp` beside the checkout, checks Linux filesystem class
  and ancestor volume identity, and exercises write/link permissions before
  launching the command. `GF_TEST_TMPDIR` explicitly overrides this location;
  ambient `TMPDIR` does not. Rust still performs the authoritative admission.
- **Durable-project tests fail on unsupported roots.** The Python unit suite
  and the Node binding suite each contain tests that open a durable project.
  On an overlay root those tests fail, and `node --test` over the full binding
  suite can hang because a failed durable test in `provider-workflow.test.mjs`
  leaves its in-process mock-server `Worker` open. Run that file in isolation
  with `--test-timeout` when diagnosing. Counts change as suites grow; do not
  treat a remembered pass/fail number as a contract.

`make cargo-test`, `make test`, `make test-unit`, `make test-tck`,
`make bazel-test`, and the `make pre-push` entrypoints use the launcher.
Linux PR binding and Rust test lanes use the same setup. For direct commands:

```bash
python3 scripts/test_environment.py -- cargo test -p graphforge-api --lib
python3 scripts/test_environment.py -- uv run pytest tests/unit
python3 scripts/test_environment.py -- pnpm --filter @curatelabs/graphforge test
python3 scripts/test_environment.py -- bazelisk test --config=correctness //:ci_rust_tests
```

The launcher exports `TMPDIR`, `TMP`, and `TEMP`. Its Bazel adapter also passes
test environment flags, a native test root, and sandbox write permission.
The default root stays outside the checkout so temporary Git fixtures cannot
accidentally discover the enclosing repository or use its shared build cache.
This setup is shared by Cargo and Bazel; it does not depend on the build-system
decision tracked in [#1618](https://github.com/CurateLabs/graphforge/issues/1618).
Build/cache comparisons should use this same setup for both lanes and record
the temporary root's filesystem along with the runner and cache state.

### Runtime hydration workspaces

Rust creates `graphforge-graph-workspace-*` directories directly inside an initialized
project container. These private mutable trees stay outside immutable generations
and on the same volume as the hard-linked graph objects. Ordinary durable opens,
commits, and reopens therefore do not require ambient `TMPDIR` on the project
volume. The container must permit creating these private workspaces, including
when a compact generation is opened for reading.

Authenticated property reads similarly allocate `.gf-property-scratch-*` beside
their source trees or in the project container, preserving their same-volume
snapshot checks. These per-query directories are removed when the read ends.

Each workspace has a unique owner; the final retained reader releases and removes
it. Opening another facade never sweeps another reader's workspace. A killed
process can leave a workspace behind, just as with temporary directories elsewhere;
automatic crash-orphan reclamation is not provided. These directories are not
commit authority. In-memory instances keep their process-owned ephemeral roots
and continue to work on tmpfs.

## Bazel

`bazelisk` must be on `PATH` (Bazel version pinned by `.bazelversion`).
`make pre-push-fast` checks for it and runs the Cargo/Bazel drift check;
`make bazel-test` runs the authoritative `//:ci_rust_tests` suite locally.
See [bazel.md](bazel.md).
