# Install and setup

## Requirements

- VS Code `^1.96.0`
- One GraphForge engine runtime — Node **or** Python (see below); you don't need both.

## Quick start

1. Install **GraphForge** from the Marketplace (or Open VSX).
2. Open a folder. Run **`GraphForge: Check Environment`** from the Command Palette.
3. Follow the single next step it reports — it always names exactly one command to run next,
   whether that's setting up a runtime or opening/initializing a project.

## Choosing a runtime: Node vs. Python

`graphforge.runtime` (setting, default `auto`) controls which engine backs Cypher execution
and the analyst verbs:

- **Node (`@graphforge/node`)** — the default for Node-ish and ambiguous workspaces. Fast,
  in-process, no subprocess. Also the only runtime backing the more advanced surfaces:
  checkpoints, embedding spaces, indexing, invocation descriptors, composite transactions, and
  knowledge-ledger writes.
- **Python (`graphforge` on PyPI)** — a first-class alternative for Cypher execute and the
  analyst verbs, communicating with a small bundled subprocess over newline-delimited JSON /
  Arrow IPC. No engine semantics are reimplemented in the extension.

In `auto`, **Node is the global default** — except when the workspace looks like a **Python
project** and not primarily a Node project, in which case `auto` prefers Python even if
`@graphforge/node` is also available:

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

`@graphforge/node` is an optional peer dependency. Either:

- Run **`GraphForge: Setup Native Binding`** — one QuickPick offering: link a detected sibling
  build, browse to a built package folder (sets `graphforge.nativeModulePath`), or install it
  via `npm install @graphforge/node` once it's published; or
- Set `graphforge.nativeModulePath` yourself to an absolute path.

## Setting up the Python binding

**Package manager policy: [`uv`](https://docs.astral.sh/uv/) only — never `pip`.** If `uv`
isn't installed, [install it first](https://docs.astral.sh/uv/getting-started/installation/);
GraphForge will not fall back to `pip install`.

Run **`GraphForge: Setup Python Binding`** — a single QuickPick with up to three choices:

1. **Use detected interpreter** — checked in order: an explicit
   `graphforge.pythonInterpreterPath`, the interpreter selected in the
   [Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python),
   a workspace `.venv`/`venv`/`env` folder, then `python3`/`python` on `PATH`.
2. **Select interpreter…** — browse for a specific interpreter; sets
   `graphforge.pythonInterpreterPath`.
3. **Install via uv** — runs `uv add graphforge` in a uv-managed project (`pyproject.toml` /
   `uv.lock` present), otherwise `uv pip install --python <interpreter> graphforge`, only after
   you explicitly confirm. If `uv` isn't installed, the command stops and tells you to install
   it — it never falls back to `pip`.

Or from a terminal directly:

```bash
# in a uv-managed project (has pyproject.toml / uv.lock)
uv add graphforge

# targeting an arbitrary interpreter/venv instead
uv pip install --python /path/to/python graphforge
```

Requires the [`pyarrow`](https://pypi.org/project/pyarrow/) package alongside `graphforge`
(installed automatically as a `graphforge` dependency in most setups).

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
