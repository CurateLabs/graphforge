# Install GraphForge

**Advanced · Assumes basic Python, database, and terminal skills.**
For guided execution with a helper, start with the [Basic agent lesson](work-with-an-agent.md).

These docs describe the v0.6.0 product line. The pinned **published** candidate
is `0.6.0-rc.7` (PyPI spelling `0.6.0rc7`); final v0.6.0 is not yet published.
[GitHub releases](https://github.com/CurateLabs/graphforge/releases) lists public
artifacts. Select the runtime for your task before installing.

## Choose a runtime for your task

| Task                                                                                                       | Runtime                                                                                           | Executable                                                                                              |
| ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| First graph, Basic mixed-methods project, inquiry records, local notebook, ordinary save/reopen            | Published Python `graphforge==0.6.0rc7`; [Python setup](#python)                                  | The activated environment's `python`; `graphforge` for CLI tasks                                        |
| Public Hub browse → clone → reopen → query                                                                 | The same published Python package; [Hub lesson](portable-projects.md#public-hub-launch-path)      | The activated environment's `graphforge` and `python`                                                   |
| Node application using ordinary graph queries                                                              | Published `@curatelabs/graphforge@0.6.0-rc.7`; [Node setup](#node-and-typescript)                 | Your Node process; the explicitly pinned npm CLI when needed                                            |
| Retained research with composed ontologies, composition reimport, or the advanced Hub continuation example | [Pinned repaired source build](#build-from-source), separate from registry rc.7                   | Its activated Python environment and absolute `GF_EVALUATION_CLI` path                                  |
| Human device approval or saved-session publishing                                                          | The same standalone source-built CLI, an authorized repository and a conforming write-capable Hub | `"$GF_EVALUATION_CLI"`; [publishing prerequisites](portable-projects.md#publish-after-local-evaluation) |

Published rc.7 predates the retained-query and composition-reimport repairs.
The repaired source below still reports `0.6.0-rc.7`; **the source revision**
distinguishes it from registry rc.7. A version string alone is insufficient.
Keep these environments separate. The registry Python/npm launchers use a
captured publish interface. They can save a login session, but cannot reuse
it for publishing or initiate publish device approval. An explicitly provisioned
scoped token is supported. The pinned standalone source CLI supports both
saved-session publishing and publish device approval.

Use the exact versions below rather than `--pre`, `@next`, or an unversioned
install: those can select different code. Instructions for newer development
CLI syntax live in its reference and do not change the runtime selected here.

The site maintains one current documentation set. Before v1.0.0, GraphForge
does not guarantee backward compatibility, legacy APIs, or project migration.
Keep original source data and reproducible construction code when changing
versions. Production publish-back and final public-package verification remain
separate release work; installing a candidate does not establish either result.

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
- **Durable storage** needs an [admitted local filesystem](#durable-storage)
  (Linux: ext4, xfs, or btrfs). Otherwise it fails with
  `GF_UNSUPPORTED_FILESYSTEM`. In-memory projects (`GraphForge()`) run the full
  engine on any platform with a native package.
- **Prerequisites.** Python 3.10+ or Node.js 20+; a Rust toolchain only for a
  source build or `cargo install`.
- **Crates.io builds lack two DataFusion fixes.** `cargo install graphforge-cli`
  and other crates.io builds compile against stock DataFusion 54.1. The Python
  wheel and the Node packages (including their `gf` CLI) carry two fixes that
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
python -m pip install "graphforge==0.6.0rc7"
```

Check which version you got:

```bash
python -c "import graphforge; print(graphforge.__version__)"
```

The engine prints `0.6.0-rc.7`, the canonical spelling of PyPI's `0.6.0rc7`.

Native wheels include the Rust engine; pip installs PyArrow automatically as a
runtime dependency. pandas and Polars are optional and are not needed for the
first graph.

Continue with [Your first graph](quickstart.md), [an agent](work-with-an-agent.md),
or [a notebook](use-a-notebook.md).

## Node and TypeScript

Use Node.js 20 or newer and a platform with a matching native package:

```bash
npm install @curatelabs/graphforge@0.6.0-rc.7 apache-arrow@21.2.0
```

`npm ls @curatelabs/graphforge` shows which version you got. `apache-arrow`
provides result decoding for the
[Node integration example](integrate-graphforge.md#node-and-typescript).

The separate CLI package can be invoked without a global install:

```bash
npx --yes @curatelabs/graphforge-cli@0.6.0-rc.7 --help
```

The Python package provides `graphforge`; the npm CLI provides both
`graphforge` and `gf`. All launch the Rust CLI. Repository initialization and
project skills are optional next steps in [repository integration](repository-integration.md).

## Native CLI release assets

The Publish workflow builds `gf` archives for macOS arm64/x86_64, Linux
arm64/x86_64 (glibc 2.17+), and Windows x86_64, alongside the package artifacts.
A release containing this support attaches `gf-<version>-<Rust target>.tar.gz`
(or `.zip` for Windows) and a matching `.sha256` file to its GitHub Release.
Check the archive against that checksum, extract it, and place `gf` (`gf.exe`
on Windows) in your executable path. For example, on Linux x86_64:

```bash
sha256sum --check "gf-<version>-x86_64-unknown-linux-gnu.tar.gz.sha256"
tar -xzf "gf-<version>-x86_64-unknown-linux-gnu.tar.gz"
./gf --version
./gf auth login
```

Replace `<version>` with the downloaded release version. Older releases,
including rc.7, have no native assets. Until a containing release is published,
use the [pinned standalone source CLI](#build-from-source) for the walkthrough.

The [launcher repair on development source](https://github.com/CurateLabs/graphforge/commit/3f79e4c29a9e7d0c7003065b24a3675032344fee)
also lets PyPI/npm console launchers inherit native streams and reuse saved
sessions; PyPI adds `gf` alongside `graphforge`. These changes await a
containing release. They are absent from registry rc.7 and the older pinned
source used below, so keep using its standalone CLI in that walkthrough.

## Durable storage

Start with `GraphForge()` if you only need a temporary graph. It runs the full
engine in memory and loses its state when closed or when the process exits.

`GraphForge(path)` opens a durable project in an existing directory. Native
filesystem admission must succeed before writing:

| Platform | Durable storage                                                                      |
| -------- | ------------------------------------------------------------------------------------ |
| Linux    | Local ext4, xfs, or btrfs; the project and process-root ancestry must pass admission |
| macOS    | An admitted local APFS volume                                                        |
| Windows  | An admitted fixed local writable NTFS volume                                         |

Network filesystems, container overlay roots, and other unproven filesystems
are not a durable-storage workaround. On Linux, mounting ext4 below an
unsupported root does not necessarily satisfy admission. A refusal reports
`GF_UNSUPPORTED_FILESYSTEM`; move to a supported environment rather than
bypassing the check. Native-package availability and filesystem admission are
separate requirements.

Use [the persistence tutorial](tutorial.md) to create, close, and reopen a
project. Use [portable export/import](portable-projects.md) for movement;
do not copy live project storage or add it to Git. Package verification does
not provide a migration path from unsupported older versions.

[Kaggle and Colab](use-a-notebook.md#hosted-notebooks) require their own
qualification. A local notebook example does not establish hosted persistence.
For detailed diagnostics, see [concurrency and recovery](../book/architecture/concurrency-recovery.md)
and [agent environment notes](../development/agent-environment.md).

## Build from source

Use this separate **developer environment** for the repaired research workflow.
It requires Git, [uv](https://docs.astral.sh/uv/getting-started/installation/),
Python 3.10+, the Rust toolchain pinned by the checkout's `rust-toolchain.toml`,
and a native build toolchain. See [contributor setup](../development/contributing.md)
for build prerequisites. Durable examples also require [supported storage](#durable-storage).

The immutable source is
[`aaf0448293915796cecf69bb8109c0b96c6a427d`](https://github.com/CurateLabs/graphforge/commit/aaf0448293915796cecf69bb8109c0b96c6a427d),
which contains the retained-query and composition-reimport repairs. The
[historical packed-candidate qualification](https://github.com/CurateLabs/graphforge/issues/1209#issuecomment-6657797848)
identifies tested package bytes and platforms at this source. A local source
build is a different installation route; that record does not qualify your build.
This route uses Git source and does not require temporary Actions artifacts.

In a POSIX shell, from a new working directory:

```bash
git clone --no-checkout https://github.com/CurateLabs/graphforge.git graphforge-source
cd graphforge-source
git checkout --detach aaf0448293915796cecf69bb8109c0b96c6a427d
uv sync --locked --inexact
export CARGO_TARGET_DIR="$PWD/target"
uv run --no-sync maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml
cargo build --locked --release -p graphforge-cli --bin gf
. .venv/bin/activate
export GF_EVALUATION_CLI="$PWD/target/release/gf"
git rev-parse HEAD
python -c "import sys, graphforge; print(sys.executable); print(graphforge.__version__)"
"$GF_EVALUATION_CLI" --version
```

The revision must be the full hash above; both version commands report rc.7.
Keep this shell and its environment when following the advanced examples. In a
new shell, reactivate this checkout's `.venv` and set `GF_EVALUATION_CLI` to the
same absolute executable path. On Windows use the equivalent environment
activation, `$env:GF_EVALUATION_CLI`, and `target/release/gf.exe`.

The source-built Python binding and standalone CLI use the same checkout and
its bundled DataFusion fixes. Do not replace the executable with `cargo install`
from crates.io or a registry npm launcher mid-lesson. For Node development use
the contributor build instructions at the same revision. Newer source syntax,
such as remembered publish destinations, requires a separately selected build;
it is not part of this pinned walkthrough.

## If setup fails

Record your OS/architecture, Python or Node version, selected package version,
and the complete installation error. For storage failures, also record the
filesystem and whether you used a path or memory-only instance. Then follow
[Get help and report a problem](../community/support.md).
