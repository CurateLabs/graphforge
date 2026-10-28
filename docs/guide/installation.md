# Install GraphForge

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

These docs describe **v0.6.0**. Until it is published, install the latest
pre-release: the latest release is older and does not implement everything
described here. [GitHub releases](https://github.com/CurateLabs/graphforge/releases)
lists what is available; a [source build](#build-from-source) also works.

The currently published candidate is `0.6.0-rc.6` (PyPI spelling `0.6.0rc6`).
For a repeatable evaluation, pin it instead of following `--pre` or `@next`:

```bash
python -m pip install "graphforge==0.6.0rc6"
npm install @curatelabs/graphforge@0.6.0-rc.6 apache-arrow
npx --yes @curatelabs/graphforge-cli@0.6.0-rc.6 --version
```

Use the Python or Node setup below first. Publication is not full launch
qualification: [#1209's current results](https://github.com/CurateLabs/graphforge/issues/1209#issuecomment-6460385114)
cover local Linux x86-64 consumers, while the
[production Hub journey](portable-projects.md#public-hub-launch-path) is blocked.
Later source builds with the same version label are distinct artifacts; record
the source revision as well as the version. The selected launch candidate must
be rechecked before announcement.

The site maintains one current documentation set. Before v1.0.0, GraphForge
does not guarantee backward compatibility, legacy APIs, or project migration.
Use matching engine, bindings, CLI, and skills versions. Keep original source
data and reproducible construction code when changing versions.

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
python -m pip install --pre graphforge # latest pre-release
```

```bash
python -m pip install graphforge # latest release
```

Check which version you got:

```bash
python -c "import graphforge; print(graphforge.__version__)"
```

Native wheels include the Rust engine; pip installs PyArrow automatically as a
runtime dependency. pandas and Polars are optional and are not needed for the
first graph.

Continue with [Your first graph](quickstart.md), [an agent](work-with-an-agent.md),
or [a notebook](use-a-notebook.md).

## Node and TypeScript

Use Node.js 20 or newer and a platform with a matching native package:

```bash
npm install @curatelabs/graphforge@next apache-arrow # latest pre-release
```

```bash
npm install @curatelabs/graphforge apache-arrow # latest release
```

`npm ls @curatelabs/graphforge` shows which version you got. `apache-arrow`
provides result decoding for the
[Node integration example](integrate-graphforge.md#node-and-typescript).

The separate CLI package can be invoked without a global install:

```bash
npx @curatelabs/graphforge-cli@next --help # latest pre-release
```

```bash
npx @curatelabs/graphforge-cli --help # latest release
```

The Python package provides `graphforge`; the npm CLI provides both
`graphforge` and `gf`. All launch the Rust CLI. Repository initialization and
project skills are optional next steps in [repository integration](repository-integration.md).

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

For engine development or evaluation before a matching artifact is published,
use the Rust toolchain pinned by `rust-toolchain.toml`, Python 3.10+, `uv`, and
maturin. Build from the source revision you intend to evaluate:

```bash
git clone https://github.com/CurateLabs/graphforge.git
cd graphforge
uv sync --inexact
uv run maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml
uv run --no-sync python -c "import graphforge; print(graphforge.__version__)"
```

A development build can still carry the repository's pre-bump version. Record
its source revision when evaluating it; it is not a published v0.6.0 artifact.
See [contributing](../development/contributing.md) for the Node build and
validation environment.

## If setup fails

Record your OS/architecture, Python or Node version, selected package version,
and the complete installation error. For storage failures, also record the
filesystem and whether you used a path or memory-only instance. Then follow
[Get help and report a problem](../community/support.md).
