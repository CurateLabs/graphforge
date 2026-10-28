# Visualization examples (issue #298)

Comparable, executable paths from a GraphForge public-API result to interactive
visualizations for:

| Runtime | Library | Entry point |
| --- | --- | --- |
| Python | Plotly | `python/plotly_example.py` |
| Python | Jaal | `python/jaal_example.py` |
| Python | PyVis | `python/pyvis_example.py` |
| Node.js | Plotly.js | `node/plotly_example.mjs` |
| Node.js | Cytoscape.js | `node/cytoscape_example.mjs` |
| Node.js | Sigma.js | `node/sigma_example.mjs` |

Reader-facing guide: [`docs/guide/visualization.md`](../../docs/guide/visualization.md).

## Dataset

Zachary's Karate Club via Mark Newman's `karate.zip` (34 nodes / 78 undirected
edges). Provenance, license/citation, and SHA-256 identities live in
[`dataset/MANIFEST.json`](dataset/MANIFEST.json). Raw archives are downloaded into
`.cache/` (gitignored) by `dataset/fetch.py`.

## Setup

```bash
# Python (from repo root, with GraphForge installed)
python -m pip install -r examples/visualization/requirements.txt
python examples/visualization/dataset/fetch.py

# Node (from repo root; explicitly select the published candidate)
npm install --prefix examples/visualization --no-save --package-lock=false @curatelabs/graphforge@0.6.0-rc.6 apache-arrow@21.2.0
export GRAPHFORGE_NODE_PATH="$PWD/examples/visualization/node_modules/@curatelabs/graphforge/index.js"
```

The environment assignment above uses a POSIX shell; other shells must set the
same variable to the installed package's absolute `index.js` path. This overrides
the example manifest's historical engine/Arrow pins and the loader's local
binding preference. For development, select your rebuilt native binding instead.
Use the exact selected version when qualifying another candidate. Run the
commands below from the repository root; setup does not change directories.

## Run

```bash
# Shared projection JSON
python examples/visualization/shared/projection.py
node examples/visualization/shared/projection.mjs

# Library adapters (write artifacts under examples/visualization/output/)
python examples/visualization/python/plotly_example.py
python examples/visualization/python/pyvis_example.py
python examples/visualization/python/jaal_example.py
node examples/visualization/node/plotly_example.mjs
node examples/visualization/node/cytoscape_example.mjs
node examples/visualization/node/sigma_example.mjs
```

## Tests

Not wired into required CI or release gates:

```bash
python -m pytest examples/visualization/tests/test_python_examples.py -q
node --test examples/visualization/tests/test_node_examples.mjs
```

## Boundary

Adapters transform GraphForge public `execute()` results only. No Core, storage,
or binding visualization behavior is added by this suite.
