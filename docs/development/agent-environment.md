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
uv run maturin develop --release --features hub-test-support -m crates/graphforge-bindings-py/Cargo.toml
pnpm --filter @curatelabs/graphforge exec napi build --platform --release --features hub-test-support
```

The full binding suites use `hub-test-support` to inject the native
`ReferenceHub` transport for collaboration tests. Activation and teardown are
explicit and local to the calling thread; no server is started. The CLI parser,
workflow operations, URL validation and native binding entry points still run.
The feature is absent from distribution builds; omit it when building a release
artifact. The ordinary Rust test lanes continue to test the production HTTPS
transport and public-address policy without this feature.

## Filesystem admission

Disk-backed projects probe the project parent, independently of the process
root's filesystem. Qualified classes are Linux ext4/xfs/btrfs, macOS local APFS
and Windows fixed writable local NTFS. Unproven classes warn by default with
`GF_UNVERIFIED_FILESYSTEM`; `GRAPHFORGE_FILESYSTEM_DURABILITY=strict` preserves
`GF_UNSUPPORTED_FILESYSTEM` refusal. Invalid policy values return `GF_VALIDATION`.
Every mode retains primitive, link, namespace and takeover checks. In-memory
projects do not warn. See [ADR 0064](../adr/0064-unverified-filesystem-admission.md).

| Layout | Default outcome after successful primitive checks |
| --- | --- |
| Project parent on overlayfs | `filesystem_class_unproven` warning; Linux file/directory mount IDs may agree despite different devices |
| Project parent on tmpfs | `filesystem_class_unproven` warning; contents are lost at reboot |
| Qualified project volume below an overlay process root | Qualified when its own parent and namespace pass |
| Device qualification unavailable | `device_identity_unknown` warning |
| Broken locking/flush/replace or substituted namespace | Refusal, including in warning mode |

Run ordinary durable tests through the shared environment launcher. It selects
`../.<checkout-name>-test-tmp` beside the checkout and checks its filesystem and
write/link permissions; `GF_TEST_TMPDIR` overrides that location. Ambient
`TMPDIR` does not. Keeping ordinary tests on a qualified volume also isolates
tests of warning counts; explicit tmpfs fixtures test unverified admission.

### Split-device overlay reproduction

Docker's same-device overlay does not prove file/directory identity on Colab.
In a throwaway `unshare -rm` namespace, mount separate tmpfs lower and upper
volumes, create `upper` and `work` in the upper volume, then mount:

```bash
mount -t overlay overlay -o lowerdir="$lower",upperdir="$upper/upper",workdir="$upper/work",xino=off,userxattr "$merged"
GF_FILESYSTEM_TEST_ROOT="$merged" GF_FILESYSTEM_TEST_CLASS=overlayfs \
  cargo nextest run -p graphforge-api --test filesystem_durability_warning \
  -E 'test(unverified_open_write_close_reopen_and_authenticated_receipts)'
```

Create a regular file under the merged directory and compare its `stat -c %d`
with the directory's: they must differ for this proof. Run from the original
checkout with fixtures on the explicit overlay, keeping compilation outside
it. The test writes, closes, reopens and verifies package receipts. This requires
unprivileged namespaces and overlayfs support; it is a local reproduction
method, not a skipped CI lane. A real connected Colab run remains separate
acceptance evidence.

`make cargo-test`, `make test`, `make test-unit`, `make test-tck`,
`make test-rust`, `make test-python`, `make test-node`, and `make check`
use the launcher.
Linux PR binding and Rust test lanes use the same setup. For direct commands:

```bash
python3 scripts/test_environment.py -- cargo test -p graphforge-api --lib
python3 scripts/test_environment.py -- uv run pytest tests/unit
python3 scripts/test_environment.py -- pnpm --filter @curatelabs/graphforge test
```

The launcher exports `TMPDIR`, `TMP`, and `TEMP`.
The default root stays outside the checkout so temporary Git fixtures cannot
accidentally discover the enclosing repository or use its shared build cache.
Build/cache comparisons should use this same setup and record the temporary
root's filesystem along with the runner and cache state.

### Runtime hydration workspaces

Rust creates `graphforge-graph-workspace-*` directories directly inside an initialized
project container. These private mutable trees stay outside immutable generations
and on the same volume as the hard-linked graph objects. Ordinary durable opens,
commits, and reopens therefore do not require ambient `TMPDIR` on the project
volume. The container must permit creating these private workspaces, including
when a compact generation is opened for reading.

An authenticated property read checks each fragment object (at most 4 MiB) against
the manifest in memory and decodes those bytes. Larger legacy fragments use a
budgeted in-memory block-checksum index: complete-file authentication builds the
index, and each requested block is authenticated into owned bytes before decoding.
Queries create no scratch and write nothing on either path. Replay and mutation
reads, which bound their memory separately, keep a scratch copy beside the source
tree or in the project container; it is removed when that read ends.

Each workspace has a unique owner; the final retained reader releases and removes
it. The owner holds an exclusive advisory lock on a sibling
`.gf-workspace-lock-<token>` file for as long as the workspace exists (the
protocol of the query spill scratch, `query_spill.rs`). A killed process cannot
remove its workspace, but the operating system releases its lock, so the next
open of the same project removes every `graphforge-graph-workspace-<token>`
whose lock it can take, and never one whose lock a live reader holds. A
workspace without a lock file, left by a build before this protocol, gives no
signal that its owner is gone and is never reclaimed automatically; remove it
by hand once no process has the project open. These directories are not
commit authority. In-memory instances keep their process-owned ephemeral roots
and continue to work on tmpfs.

## Rust test gate

Cargo with nextest is the CI compile/test authority
([ADR 0048](../adr/0048-cargo-is-the-ci-build-authority.md)). The CI Gate Rust
lane is the `rust-tests` job in `.github/workflows/test.yml`; these are its
commands, run from the repository root:

```bash
python3 scripts/test_environment.py -- \
  cargo nextest run --workspace --locked --no-fail-fast \
  -E 'not ((package(graphforge-api) and binary(bdd)) or (package(graphforge-observability) and binary(disabled_allocations)))'
python3 scripts/test_environment.py -- \
  cargo test --workspace --locked --test bdd --test disabled_allocations
python3 scripts/test_environment.py -- cargo test --workspace --locked --doc
python3 scripts/test_environment.py -- \
  cargo nextest run --locked --no-fail-fast \
  -p graphforge-exec --features differential-testing --test differential_traversal
```

The `-E` filterset is required: `bdd` (cucumber) and `disabled_allocations`
are custom-harness targets that cannot answer nextest's `--list` protocol, so
they run under `cargo test` instead. The last command runs a test target whose
required feature is outside its package's default set, which the workspace run
skips; any test target with a `required-features` outside the default set must
be run with an explicit `--features` flag as shown above. Install nextest with
`cargo install --locked cargo-nextest` (CI pins the version in the job). The
lane uses the dev/test profile, which keeps debug assertions and overflow
checks on; do not substitute a release profile.

Cargo discovers each crate's `tests/*.rs` files itself, so adding a Rust test
file needs no build-description edit; at most a `[[test]]` entry in the crate's
`Cargo.toml` when the target needs a custom harness or features.
