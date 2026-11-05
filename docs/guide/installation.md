# Install GraphForge

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

These docs describe **v0.6.0**. Until it is published, install the latest
pre-release: the latest release is older and does not implement everything
described here. [GitHub releases](https://github.com/CurateLabs/graphforge/releases)
lists what is available; a [source build](#build-from-source) also works.

The currently published candidate is `0.6.0-rc.7` (PyPI spelling `0.6.0rc7`).
For a repeatable evaluation, pin it instead of following `--pre` or `@next`:

```bash
python -m pip install "graphforge==0.6.0rc7"
npm install @curatelabs/graphforge@0.6.0-rc.7 apache-arrow@21.2.0
npx --yes @curatelabs/graphforge-cli@0.6.0-rc.7 --version
```

The public `opendata/openalex` fixture is reachable through both clone forms;
see the [Hub journey](portable-projects.md#public-hub-launch-path) for its useful
question, expected output and producing-version warning. The
[published rc.7 consumer results](https://github.com/CurateLabs/graphforge/issues/1209#issuecomment-6587486441)
cover ordinary queries, durable reopen, local kernels and installed skills.

Published rc.7 predates the fixes for ontology-composed retained-Version queries
and reimport of previously imported composition. For that research lifecycle,
use the repaired packed candidate below. Both carry the label `0.6.0-rc.7`;
the source revision and artifact bytes distinguish them. The registry packages
have not been replaced. #1095 owns final public-package substitution and recheck.

The site maintains one current documentation set. Before v1.0.0, GraphForge
does not guarantee backward compatibility, legacy APIs, or project migration.
Use matching engine, bindings, CLI, and skills versions. Keep original source
data and reproducible construction code when changing versions.

## Evaluate the repaired packed candidate

The selected source is
[`aaf044829`](https://github.com/CurateLabs/graphforge/commit/aaf0448293915796cecf69bb8109c0b96c6a427d).
Download its wheel and npm artifacts from the existing
[Publish dry run](https://github.com/CurateLabs/graphforge/actions/runs/41960987276).
The [first-use qualification record](https://github.com/CurateLabs/graphforge/issues/1209#issuecomment-6657797848)
identifies the tested artifact hashes, platforms and results. Check those identities
before installing; a version string alone does not distinguish this build from
published rc.7.

This prepublication route requires GitHub CLI access to Actions artifacts.
Artifacts expire after seven days; if unavailable, use the later candidate
recorded by [release execution](https://github.com/CurateLabs/graphforge/issues/1095).
It is not a new registry release. The following commands use Linux x86-64 and
a POSIX shell, with Node.js 20+, Python 3.10+ and an admitted durable volume:

```bash
mkdir graphforge-evaluation
cd graphforge-evaluation
python3 -m venv .venv
. .venv/bin/activate
gh run download 41960987276 --repo CurateLabs/graphforge --name npm-packages --dir packages
gh run download 41960987276 --repo CurateLabs/graphforge --name wheel-linux-x86_64 --dir wheels
npm init --yes
npm install ./packages/curatelabs-graphforge-0.6.0-rc.7.tgz ./packages/curatelabs-graphforge-cli-0.6.0-rc.7.tgz ./packages/curatelabs-graphforge-linux-x64-gnu-0.6.0-rc.7.tgz apache-arrow@21.2.0
python -m pip install wheels/*.whl
./node_modules/.bin/gf --version
```

Invoke `./node_modules/.bin/gf` throughout the evaluation so an older `gf` in
`PATH` cannot be selected. Use this environment's Python for the query examples.
On macOS or Windows choose the matching wheel artifact and native npm tarball
from the same run; retain the same source and package identities. The available
platforms are listed below. These packages contain the native engine; no workspace
checkout or local binding rebuild is needed.

Rust evaluators can reproduce the external consumer from the qualification record:
it assembles the ordinary publishable `.crate` archives at the same source, then
builds a fresh consumer using only their pristine extracted contents for GraphForge
dependencies. That is packed-candidate proof, not a registry release. Keep its
recorded dependency overrides and stock DataFusion distinction; a workspace build
or public rc.7 dependency does not substitute for that consumer.

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
