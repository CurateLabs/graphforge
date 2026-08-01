# Installation

GraphForge **v0.5.0** ships as thin native bindings over a Rust core
(Python via maturin; Node via N-API). Install from the published registries
for normal use, or build from source on `main` when developing the engine or
bindings.

Prefer an editor workflow? The optional [GraphForge VS Code extension](vscode-extension/install.md)
detects Node- and Python-first workspaces, helps configure the appropriate native binding, and
uses the same Rust-owned engine described below.

> **Name collision:** PyPI currently lists an unrelated pure-Python package
> named `graphforge` at **0.4.0** (~279 KB). CurateLabs GraphForge
> **0.5.0** is the native engine (Rust `.so` / `.node`). Until CurateLabs
> publishes 0.5.0, prefer source/dev installs from this repository rather than
> assuming PyPI/npm already ship the native wheels.

---

## Python package (current — v0.5.0)

**Requirements:** Python 3.10 or newer (3.10–3.14 tested in CI).

```bash
# pip
pip install graphforge

# uv (recommended)
uv add graphforge
```

### Verify

```python
import graphforge
print(graphforge.__version__)   # 0.5.0…
```

### Optional dependencies

```bash
# Polars convenience wrapper around Arrow results
pip install "graphforge[polars]"
```

Results are Apache Arrow tables. Convert with `table.to_pandas()`,
`pl.from_arrow(table)`, or `table.to_pylist()` as needed. Graph algorithms
run in the native Rust engine; NetworkX and igraph are development-only
parity oracles, not runtime backends.

Python installs typically also pull **PyArrow** as a separate runtime
dependency (not bundled inside the `graphforge` wheel). See
[Install footprint](#install-footprint) for approximate sizes.

---

## Node package (`@curatelabs/graphforge`)

**Requirements:** a current Node.js LTS (CI covers the binding’s supported
targets).

```bash
npm install @curatelabs/graphforge
# or
pnpm add @curatelabs/graphforge
```

```js
import { GraphForge } from "@curatelabs/graphforge";
```

Until the CurateLabs package is published to npm, install from a local
release build or path dependency in this repository.

---

## Install footprint

Approximate **download** and **on-disk** sizes for operators sizing CI images,
laptops, and air-gapped mirrors. These are **not** [scale-limits](../reference/scale-limits.md)
query/bench results.

| Surface | Packed / download | Installed / unpacked |
|---|---|---|
| Python `graphforge` | ~42 MB wheel | ~121 MB (mostly `_graphforge_rs.abi3.so`) |
| Node `@curatelabs/graphforge` | ~44 MB npm pack | ~128 MB (mostly `.node` ≈ 121 MB) |
| PyArrow (Python dep, e.g. 21.0.0) | ~31 MB wheel | ~108 MB |

**Caveats**

- Measured on **local macOS arm64 (darwin-arm64)** release builds of unpublished
  **0.5.0-dev**. Other OS/arch combinations will differ.
- Almost all of the GraphForge footprint is the **Rust native binary**; the
  thin Python/JS wrappers are negligible by comparison.
- **PyArrow is separate** — it is not inside the `graphforge` wheel. Budget it
  only for Python environments that install that dependency.
- Do not confuse these sizes with PyPI `graphforge` **0.4.0** (unrelated
  pure-Python package, ~279 KB).

---

## Install from source (`main`)

Building from source requires a Python environment and the Rust toolchain.

### Requirements

- Python 3.10 or newer
- Rust stable toolchain (`rustup`)
- [uv](https://github.com/astral-sh/uv)
- [maturin](https://www.maturin.rs/) (Python/Rust build bridge)

### Setup

```bash
# 1. Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable

# 2. Clone
git clone https://github.com/CurateLabs/graphforge.git
cd graphforge

# 3. Install Python dev dependencies
uv sync --dev

# 4. Build and install the Rust extension in development mode
maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml

# 5. Verify
python -c "import graphforge; print(graphforge.__version__)"
```

### Run checks

```bash
# Rust: unit + integration tests across all crates
cargo test --workspace

# Rust: lint
cargo clippy --workspace -- -D warnings

# Full pre-push suite
make pre-push
```

---

## Next steps

- [VS Code extension](vscode-extension/install.md) — configure GraphForge inside your editor
- [Quick Start](quickstart.md) — build your first graph in five minutes
- [Tutorial](tutorial.md) — step-by-step guided walkthrough
- [Architecture Overview](../book/architecture/overview.md) — Rust core design
