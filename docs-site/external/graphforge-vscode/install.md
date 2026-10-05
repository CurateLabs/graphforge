# Install and setup

**Advanced · Assumes VS Code and basic Python or Node environment setup.**

These instructions target an explicitly selected v0.6.0 engine. The pinned
extension snapshot's automatic package-install choices target an earlier engine;
do not use those choices to prepare a v0.6.0 environment. Install the selected
engine manually below, then select its interpreter or package path. A successful
runtime check does not establish that the packaged extension/engine combination
is qualified; that remains tracked in
[#1209](https://github.com/CurateLabs/graphforge/issues/1209).

## Requirements

- VS Code `^1.96.0`
- One GraphForge engine runtime — Node **or** Python (see below); you don't need both.

## Quick start

1. Install **GraphForge** from the Marketplace (or Open VSX).
2. Open a folder. Run **`GraphForge: Check Environment`** from the Command Palette.
3. Check the runtime and version it reports. Use the manual setup below for
   v0.6.0, then rerun the check before opening a project.

## Choosing a runtime: Node vs. Python

`graphforge.runtime` (setting, default `auto`) controls which engine backs Cypher execution
and the analyst verbs:

- **Node (`@curatelabs/graphforge`)** — the default for Node-ish and ambiguous workspaces. Fast,
  in-process, no subprocess. Also the only runtime backing the more advanced surfaces:
  checkpoints, embedding spaces, indexing, invocation descriptors, composite transactions, and
  knowledge-ledger writes.
- **Python (`graphforge` on PyPI)** — a first-class alternative for Cypher execute and the
  analyst verbs, communicating with a small bundled subprocess over newline-delimited JSON /
  Arrow IPC. No engine semantics are reimplemented in the extension.

In `auto`, **Node is the global default** — except when the workspace looks like a **Python
project** and not primarily a Node project, in which case `auto` prefers Python even if
`@curatelabs/graphforge` is also available:

- **Python signals:** `pyproject.toml`, `requirements.txt`, `uv.lock`, `.python-version`,
  `Pipfile`, `environment.yml`, `setup.py`, a notebook-dominant workspace root, or an explicitly
  selected VS Code Python interpreter.
- **Node signals:** a `package.json` at the workspace root.
- **If both are present:** Python wins only on a strong signal (`pyproject.toml`/`uv.lock`
  present, or a Python `graphforge` environment already usable); otherwise the workspace is
  treated as ambiguous and Node stays the default.
- Set `graphforge.runtime` to `node` or `python` explicitly to bypass detection entirely — an
  explicit preference never falls back to the other runtime.

Run **`GraphForge: Check Environment`** any time to see both runtimes' status, which one is
active, and the next step to fix whichever is missing.

## Setting up the Node binding

`@curatelabs/graphforge` is an optional peer dependency. Install it in your
workspace using the exact version selected in the
[engine installation guide](https://docs.graphforge.sh/guide/installation/):

```bash
npm install @curatelabs/graphforge@0.6.0
```

This final-version command requires v0.6.0 to be published; for a candidate use
its exact npm version. Set `graphforge.runtime` to `node`. Run
**GraphForge: Setup Native Binding** and browse to the installed package folder,
or set `graphforge.nativeModulePath` to its absolute path. Avoid the automatic
registry-install choice until its configured version matches your selected engine.

## Setting up the Python binding

Prepare the selected environment using the
[Python installation instructions](https://docs.graphforge.sh/guide/installation/).
With that environment active, identify the interpreter and version:

```bash
python -c "import sys, graphforge; print(sys.executable); print(graphforge.__version__)"
```

Set `graphforge.runtime` to `python` and `graphforge.pythonInterpreterPath` to the
printed absolute interpreter path. Alternatively use **GraphForge: Setup Python
Binding → Select interpreter…**. This selects an existing environment; it does
not install another package. `pyarrow` is also required and is declared as a
GraphForge Python dependency.

The extension's automatic installer uses `uv` and its own configured package
version. That installer policy does not prevent manually preparing a Python
environment with pip. For these v0.6.0 instructions, select the environment you
prepared rather than using the older automatic install choice.

Rerun **GraphForge: Check Environment**. If runtime loading fails, inspect the
reported error and confirm the selected path and package version. Do not infer
compatibility from the fact that the extension itself installed successfully.

## Project detection

A folder is a GraphForge project only when it contains a `FORMAT` file whose exact contents
are `graphforge-project/v1\n` (including the trailing newline) — never inferred from Parquet
files alone.

No project yet? Run **`GraphForge: Initialize Project Here`** — it picks the current workspace
folder or one you choose, confirms once, and only ever succeeds on an empty or
already-initializing directory.

## Neither runtime available yet?

Commands and views still register. Query/open paths fail closed with a status-bar message and
an error offering both **Setup Native Binding** and **Setup Python Binding** — never a silent
no-op or an opaque exception.
