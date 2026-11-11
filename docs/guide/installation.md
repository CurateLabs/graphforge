# Install GraphForge

**Advanced · Assumes basic Python, database, and terminal skills.**
For guided execution with a helper, start with the [Basic agent lesson](work-with-an-agent.md).

These docs describe GraphForge v0.6.0, the current release.
[GitHub releases](https://github.com/CurateLabs/graphforge/releases) lists public
artifacts. Select the runtime for your task before installing.

## Choose a runtime for your task

| Task                                                                                            | Runtime                                                                                       | Executable                                                       |
| ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| First graph, Basic mixed-methods project, inquiry records, local notebook, ordinary save/reopen | Python package `graphforge`; [Python setup](#python)                                          | The activated environment's `python`; `gf` for CLI tasks         |
| Public Hub browse → clone → reopen → query, and research continuation                           | The same Python package; [Hub lesson](portable-projects.md#public-hub-launch-path)            | The activated environment's `gf` and `python`                    |
| Hosted notebook session storage                                                                 | The same Python package in the notebook; [hosted notebooks](use-a-notebook.md#hosted-notebooks) | The notebook kernel                                              |
| Node application using ordinary graph queries                                                   | `@curatelabs/graphforge`; [Node setup](#node-and-typescript)                                  | Your Node process; `@curatelabs/graphforge-cli` for the CLI      |
| Publishing to graphforge.sh or another Hub                                                      | Any `gf` above, or a [native CLI archive](#native-cli-release-assets)                         | `gf`; [publishing prerequisites](portable-projects.md#publish-after-local-evaluation) |

The Python package, the npm CLI and the native archives all run the same Rust
CLI. Each can save a login session with `gf auth login` and reuse it for
`gf publish` and `gf push`, or fall back to terminal device approval.

These commands install the latest release. A future pre-release is available
with `python -m pip install --pre graphforge` or
`npm install @curatelabs/graphforge@next`; do not mix it into an environment
used for a lesson. The site maintains one current documentation set. Before
v1.0.0, GraphForge does not guarantee backward compatibility, legacy APIs, or
project migration. Keep original source data and reproducible construction code
when changing versions.

## Status and limits

Read this before you choose durable storage or build an integration. See also
[Who GraphForge is for](audience-and-fit.md).

- **Pre-v1.** APIs and project formats can change without notice before
  v1.0.0. There is no backward-compatibility, legacy-API, or migration
  guarantee; a project written by another version may be refused with
  `GF_UNSUPPORTED_PROJECT_FORMAT`. GraphForge opens only supported
  current-format projects ([compatibility contract](../book/architecture/project-format-compatibility.md)).
  GraphForge warns when data comes from a different Core version
  ([details](portable-projects.md#data-from-another-core-version)); it does not
  convert that data. Keep original source data and construction code so you can
  rebuild.
- **Current-format interchange is separate.** [Portable export, verify, and
  import](portable-projects.md) is a supported workflow for current-format
  projects. It is not a migration path from older versions.
- **Prebuilt packages.** The release workflow builds the Python wheel for
  Linux x86-64 (glibc 2.17 or newer), macOS on Apple silicon, and Windows
  x86-64. It builds Node packages for macOS (x86-64 and arm64), Linux
  (x86-64 and arm64, glibc), and Windows x86-64. Other platforms need a
  [source build](#build-from-source).
- **Durable storage** probes the [project filesystem](#durable-storage).
  Unproven filesystem classes open with `GF_UNVERIFIED_FILESYSTEM` by default;
  durability is not guaranteed. Strict mode retains refusal. In-memory projects
  (`GraphForge()`) run the full engine without a filesystem warning.
- **Prerequisites.** Python 3.10+ or Node.js 20+; a Rust toolchain only for a
  source build or `cargo install`.
- **Crates.io builds lack two DataFusion fixes.** `cargo install graphforge-cli`
  and other crates.io builds compile against stock DataFusion 54.1. The Python
  wheel, the Node packages and the native `gf` archives carry two fixes that
  crates.io builds do not: a hang when a query spills under memory pressure
  ([apache/datafusion#24883](https://github.com/apache/datafusion/issues/24883))
  and a planning error when a join's keys read fields of one struct
  ([#2084](https://github.com/CurateLabs/graphforge/issues/2084)). Prefer the
  wheel or npm package unless you need the Rust crates. Do not expect
  workspace and registry builds to behave identically.
- **Measured scale is workload-specific.** No universal size or latency promise
  exists. The 1,000,000 edges/s ingest target is **not met**. See
  [claims and evidence](../reference/scale-limits.md#claims-and-their-evidence).
- **Unaudited benchmarks.** GDC suite results are engineering observations, not
  audited results, and include failed and incomplete suites.

## Python

Use Python 3.10 or newer in an isolated environment:

```bash
python3 -m venv .venv
```

On Windows, use `py -3 -m venv .venv` instead. If environment creation fails,
install your Python distribution's venv and pip support before continuing.

Activate it using the command for your shell (`source .venv/bin/activate` on
POSIX shells; `.venv\Scripts\Activate.ps1` in PowerShell), then install:

```bash
python -m pip install graphforge
```

Check which version you got:

```bash
python -c "import graphforge; print(graphforge.__version__)"
```

The engine prints the installed version, `0.6.0` for this release. The package
also installs the `gf` CLI (and its `graphforge` alias); `gf --version` reports
the same version.

Native wheels include the Rust engine; pip installs PyArrow automatically as a
runtime dependency. pandas and Polars are optional and are not needed for the
first graph.

Continue with [Your first graph](quickstart.md), [an agent](work-with-an-agent.md),
or [a notebook](use-a-notebook.md).

## Node and TypeScript

Use Node.js 20 or newer and a platform with a matching native package:

```bash
npm install @curatelabs/graphforge apache-arrow@21
```

`npm ls @curatelabs/graphforge` shows which version you got. `apache-arrow`
provides result decoding for the
[Node integration example](integrate-graphforge.md#node-and-typescript).
GraphForge declares it as an optional peer dependency with range `^21.2.0`;
`@21` keeps npm from selecting a later major that the peer range rejects.

The separate CLI package can be invoked without a global install:

```bash
npx --yes @curatelabs/graphforge-cli --help
```

Install it with `npm install --global @curatelabs/graphforge-cli` to put `gf`
on your path. The Python package and the npm CLI both provide `gf` and
`graphforge`. All launch the Rust CLI. Repository initialization and
project skills are optional next steps in [repository integration](repository-integration.md).

## Native CLI release assets

The [v0.6.0 GitHub Release](https://github.com/CurateLabs/graphforge/releases/tag/v0.6.0)
attaches `gf` archives for macOS arm64/x86_64, Linux arm64/x86_64 (glibc 2.17+),
and Windows x86_64: `gf-<version>-<Rust target>.tar.gz` (or `.zip` for Windows)
with a matching `.sha256` file. Check the archive against that checksum, extract
it, and place `gf` (`gf.exe` on Windows) in your executable path. For example,
on Linux x86_64:

```bash
sha256sum --check "gf-0.6.0-x86_64-unknown-linux-gnu.tar.gz.sha256"
tar -xzf "gf-0.6.0-x86_64-unknown-linux-gnu.tar.gz"
./gf --version
./gf auth login
```

For a later release, use the version in that release's asset names. The
archives are built from the same source as the Python and npm packages and
carry the same DataFusion fixes.

## Durable storage

Start with `GraphForge()` if you only need a temporary graph. It runs the full
engine in memory and loses its state when closed or when the process exits.

`GraphForge(path)` opens a project, creating the final directory if its parent
exists. The qualified filesystem classes are Linux ext4/xfs/btrfs, macOS local
APFS, and Windows fixed writable local NTFS. A project volume may be mounted
beneath a different process root.

Unproven classes, including overlayfs and tmpfs, open with one structured
`GF_UNVERIFIED_FILESYSTEM` warning per open after the publication primitive
probe passes. Writes do not emit additional warnings. Durability is **unverified**:
successful probing does not prove survival of power loss. Set
`GRAPHFORGE_FILESYSTEM_DURABILITY=strict` to refuse unproven filesystems with
`GF_UNSUPPORTED_FILESYSTEM`; `warn` is the default. Other values return
`GF_VALIDATION`. Broken locks, flushes or replacement operations, read-only
volumes, links, namespace changes and lock takeover still refuse in both modes.
Network and FUSE mounts may fail those primitive checks; this policy does not
promise they work.

Python emits `graphforge.FilesystemDurabilityWarning` (a `UserWarning`), Node
emits a `FilesystemDurabilityWarning` event with the code and JSON `detail`,
and CLI text commands warn on stderr (`--json` includes structured warnings).
The last writer's qualification is authenticated in each generation; `status`,
package verification, export and publish receipts report it. Older unstamped
generations remain unchanged. See [ADR 0064](../adr/0064-unverified-filesystem-admission.md).

Use [the persistence tutorial](tutorial.md) to create, close, and reopen a
project. Use [portable export/import](portable-projects.md) for movement;
do not copy live project storage or add it to Git. Package verification does
not provide a migration path from unsupported older versions.

[Kaggle and Colab](use-a-notebook.md#hosted-notebooks) are supported with this
warning when the native package and primitive checks work. Their VM disks and
tmpfs are session storage: tmpfs loses data on reboot and hosted runtimes lose
data when recycled. Export to persistent storage before the session ends.
For detailed diagnostics, see [concurrency and recovery](../book/architecture/concurrency-recovery.md)
and [agent environment notes](../development/agent-environment.md).

## Build from source

Build from source for an unsupported platform or to work on GraphForge itself.
It requires Git, [uv](https://docs.astral.sh/uv/getting-started/installation/),
Python 3.10+, the Rust toolchain pinned by the checkout's `rust-toolchain.toml`,
and a native build toolchain. See [contributor setup](../development/contributing.md)
for build prerequisites. Durable examples also require [supported storage](#durable-storage).

In a POSIX shell, build the release tag outside any other Git checkout:

```bash
git clone --branch v0.6.0 https://github.com/CurateLabs/graphforge.git graphforge-source
cd graphforge-source
uv sync --locked --inexact
export CARGO_TARGET_DIR="$PWD/target"
uv run --no-sync maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml
cargo build --locked --release -p graphforge-cli --bin gf
. .venv/bin/activate
python -c "import sys, graphforge; print(sys.executable); print(graphforge.__version__)"
./target/release/gf --version
```

Both version commands report the checked-out version. Put `target/release/gf`
(`target/release/gf.exe` on Windows) on your path to use it as `gf`. The
source-built Python binding and CLI use the checkout's bundled DataFusion fixes;
`cargo install` from crates.io does not (see [status and limits](#status-and-limits)).
For Node development use the contributor build instructions.

## If setup fails

Record your OS/architecture, Python or Node version, selected package version,
and the complete installation error. For storage failures, also record the
filesystem and whether you used a path or memory-only instance. Then follow
[Get help and report a problem](../community/support.md).
