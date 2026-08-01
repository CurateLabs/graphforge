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

# Node (uses in-repo binding when present; else @curatelabs/graphforge@0.5.1)
cd examples/visualization
npm install
```

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
