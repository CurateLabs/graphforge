# Visualization examples

GraphForge returns Arrow tables from its public Python and Node APIs. This guide
shows how to take **one shared real-data projection** and render it with common
ecosystem libraries in both Python and Node.js—without adding visualization
behavior to GraphForge Core.

Runnable sources live under
[`examples/visualization/`](https://github.com/CurateLabs/graphforge/tree/main/examples/visualization).

## Comparison

| Runtime | Library | Artifact | Layout seed | Notes |
| --- | --- | --- | --- | --- |
| Python | [Plotly](https://plotly.com/python/) | `plotly_karate.html` + JSON | Deterministic circular layout using seed `42` | Fully offline-friendly HTML via CDN Plotly.js |
| Python | [Jaal](https://github.com/imohitmayank/jaal) | `jaal_karate_payload.json` (+ Dash app) | **Not supported** by Jaal `create()`/`plot()` | Interactive view needs a Dash server (`--serve`) |
| Python | [PyVis](https://pyvis.readthedocs.io/) | `pyvis_karate.html` | `layout.randomSeed = 42` | Writes standalone HTML |
| Node.js | [Plotly.js](https://plotly.com/javascript/) | `plotly_js_karate.html` + JSON | Same circular layout / seed `42` as Python Plotly | CDN Plotly.js; figure JSON built without a Node Plotly package |
| Node.js | [Cytoscape.js](https://js.cytoscape.org/) | elements JSON + HTML | cose has no portable seed matching the contract | Closest honest path: cose, `animate: false` |
| Node.js | [Sigma.js](https://www.sigmajs.org/) | graphology JSON + HTML | Seeded circular coordinates (`42`) | Browser page loads graphology + sigma via import map |

Awkward or limited options stay in the comparison with documented compromises.

## Dataset provenance

| Field | Value |
| --- | --- |
| Dataset | Zachary's Karate Club |
| Source | [Mark Newman network data](https://public.websites.umich.edu/~mejn/netdata/karate.zip) |
| Version identity | SHA-256 of `karate.zip` recorded in `examples/visualization/dataset/MANIFEST.json` |
| Nodes / edges | 34 / 78 |
| Directed | No (undirected friendships) |
| Citation | W. W. Zachary, *Journal of Anthropological Research* 33, 452–473 (1977) |

Raw archives and extracts are **not** committed. Fetch and verify:

```bash
python examples/visualization/dataset/fetch.py
```

## Shared projection contract

All examples:

1. Download and checksum-verify Newman's `karate.zip`.
2. Parse `karate.gml` edges.
3. Load nodes/edges through GraphForge's public API (`add_node` / `add_edge` or
   Node `addNode` / `addEdge`).
4. Obtain the projection with public Cypher `execute()` queries defined in
   `examples/visualization/shared/contract.json`.
5. Transform that projection into the library's documented input shape.

Projection identity: `karate-member-friend-v1`

- Node label: `Member` with `club_id` (1–34) and display `label` (`M{id}`)
- Relationship: undirected `FRIEND` (stored once per unordered pair)
- Style defaults: node color `#2E86AB`, edge color `#A0AEC0`, layout seed `42`

Adapters **must not** open GraphForge storage internals or extend Core for
rendering.

## Setup

### Python

```bash
python -m pip install graphforge
```

```bash
python -m pip install -r examples/visualization/requirements.txt
python examples/visualization/dataset/fetch.py
```

Pinned example libraries (install-time versions from the requirements file):
Plotly, Jaal, PyVis, pandas, pyarrow.

### Node.js

```bash
cd examples/visualization
npm install
# Prefer a built in-repo binding during development:
# export GRAPHFORGE_NODE_PATH=/absolute/path/to/crates/graphforge-bindings-node/index.js
```

Node examples depend on `apache-arrow` for IPC decoding and load Plotly.js,
Cytoscape.js, and Sigma.js in the generated HTML from public CDNs (no
visualization dependency is added to GraphForge packages).

## Commands and expected output

```bash
# Shared projection
python examples/visualization/shared/projection.py
# -> examples/visualization/output/projection.json

python examples/visualization/python/plotly_example.py
# -> output/plotly_karate.html, output/plotly_karate.json

python examples/visualization/python/pyvis_example.py
# -> output/pyvis_karate.html

python examples/visualization/python/jaal_example.py
# -> output/jaal_karate_payload.json
# Local interactive Dash UI: add --serve

node examples/visualization/node/plotly_example.mjs
# -> output/plotly_js_karate.html, output/plotly_js_karate.json

node examples/visualization/node/cytoscape_example.mjs
# -> output/cytoscape_karate_elements.json, output/cytoscape_karate.html

node examples/visualization/node/sigma_example.mjs
# -> output/sigma_karate_graph.json, output/sigma_karate.html
```

Open the HTML files in a browser locally. Example tests validate artifact
construction without launching a browser:

```bash
python -m pytest examples/visualization/tests/test_python_examples.py -q
node --test examples/visualization/tests/test_node_examples.mjs
```

These checks are part of the example suite. They are **not** required CI,
scheduled CI, or release gates.

## Limitations (honest comparison)

- **Jaal** only becomes interactive through Dash; headless CI constructs the app
  via `Jaal.create()` and writes a JSON payload. Layout seed is unsupported.
- **Cytoscape.js `cose`** does not provide a stable, documented seed equivalent
  to the shared contract; the HTML records the requested seed and uses
  non-animated cose.
- **Plotly / Plotly.js** have no built-in force-directed seed for graphs; both
  examples use the same deterministic circular layout derived from seed `42`.
- These examples demonstrate integration paths. They are not library
  recommendations or scalability proofs.

## Related

- [Analytics Integration](analytics-integration.md) — Arrow → pandas / NetworkX
- [Graph Construction](graph-construction.md) — public construction API
- [Network Analysis use case](../book/use-cases/network-analysis.md)
