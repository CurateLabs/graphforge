# Install GraphForge

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

These instructions target **v0.6.0**. Check the
[GitHub releases](https://github.com/CurateLabs/graphforge/releases) for the
available artifact before installing. The final-version commands below require
v0.6.0 to have been published. While it is being prepared, use an explicitly
published candidate or a source build; do not substitute an older package and
assume it implements these docs.

The site maintains one current documentation set. Before v1.0.0, GraphForge
does not guarantee backward compatibility, legacy APIs, or project migration.
Use matching engine, bindings, CLI, and skills versions. Keep original source
data and reproducible construction code when changing versions.

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
python -m pip install graphforge==0.6.0
python -c "import graphforge; print(graphforge.__version__)"
```

The imported version must match the release you selected. Native wheels include
the Rust engine; pip installs PyArrow automatically as a runtime dependency. pandas
and Polars are optional and are not needed for the first graph.

For an available release candidate, use its exact PyPI version. Canonical
`0.6.0-rc.N` is spelled `0.6.0rcN` on PyPI, with `N` replaced by the published
candidate number. Verify that candidate exists before choosing it.

Continue with [Your first graph](quickstart.md), [an agent](work-with-an-agent.md),
or [a notebook](use-a-notebook.md).

## Node and TypeScript

Use Node.js 20 or newer and a platform with a matching native package:

```bash
npm install @curatelabs/graphforge@0.6.0 apache-arrow
npm ls @curatelabs/graphforge
```

`apache-arrow` provides result decoding for the
[Node integration example](integrate-graphforge.md#node-and-typescript).
For an available candidate, use the exact npm version `0.6.0-rc.N`.

The separate CLI package can be invoked without a global install:

```bash
npx @curatelabs/graphforge-cli@0.6.0 --help
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
filesystem and whether you used a path or memory-only instance. Ask in
[Discussions](https://github.com/CurateLabs/graphforge/discussions) or open a
[minimal bug report](https://github.com/CurateLabs/graphforge/issues/new/choose).
