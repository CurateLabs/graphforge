# Visualization limits methodology (#299)

This document freezes the size ladder, stopping condition, timeout, and
resource-observation method **before** a recorded CI-class run.

## Dependency on #298

The harness consumes the shared visualization projection modules landed with
[#298](https://github.com/CurateLabs/graphforge/issues/298) / [#312](https://github.com/CurateLabs/graphforge/pull/312):

- `examples/visualization/shared/contract.json` — projection id
  `karate-member-friend-v1`, layout seed `42`, undirected `FRIEND`
- `examples/visualization/shared/projection.py` — GraphForge public-API projection
- `examples/visualization/dataset/fetch.py` — verified karate GML fetch

`harness/contract.py` loads those modules directly for karate-sized steps.
Offline unit tests may use committed `fixtures/` when dataset fetch is
unavailable; ladder steps above 34 nodes download SNAP `facebook_combined`
on demand.

## Datasets

| Ladder steps | Dataset | Provenance |
| --- | --- | --- |
| ≤ 34 nodes | Zachary karate club | Academic citation dataset (Zachary 1977); edge list under `fixtures/` |
| > 34 nodes | SNAP `facebook_combined` | Downloaded on demand from https://snap.stanford.edu/data/facebook_combined.txt.gz into `.cache/` (not committed) |

Raw SNAP bytes are kept out of git history; the result record stores the SHA-256
of the downloaded edge list.

## Size ladder

Defined in [`size_ladder.json`](size_ladder.json):

- Seed: `29901`
- Selection: deterministic BFS from the lexicographically lowest node id, with
  seeded neighbor rotation
- Steps: 10 → 20 → 34 → 100 → 250 → 500 → 1000 → 2000 → 4039 nodes (capped by
  available nodes in the chosen dataset)

## Stopping condition

For each visualization option independently:

1. Advance through ladder steps in order.
2. Stop advancing that option after the first `failure`, `timeout`, or
   `resource_limit`.
3. Other options continue.

## Timeouts and resources

- Per option × step soft timeout: **120 seconds** (`signal.setitimer` on Unix).
- Peak RSS observation limit: **6144 MB** (record `resource_limit` if exceeded).
- Peak RSS via `resource.getrusage(RUSAGE_SELF).ru_maxrss` (Linux KB / macOS bytes,
  normalized to MB in the result record).

## Measured phases (separated)

| Field | Meaning |
| --- | --- |
| `graphforge_projection_seconds` | Public-API load + query of the shared projection |
| `viz_prep_seconds` | Transform projection → library input structures |
| `renderer_init_seconds` | Library object / headless first-ready construction |
| `payload_bytes` | UTF-8 size of the emitted artifact or probe JSON |
| `peak_rss_mb` | Process peak RSS after the step (best-effort) |

## Equivalence compromises

| Option | Compromise |
| --- | --- |
| Plotly (Python) | No native graph layout; deterministic circular coordinates from the shared seed |
| Plotly.js | Same circular-layout figure JSON as Python; no DOM/`Plotly.newPlot` in the probe |
| Jaal | Measures `Jaal.create()` dashboard construction; does not serve HTTP or open a browser |
| PyVis | Barnes-Hut physics are engine-internal; shared seed is recorded but not injected into vis.js |
| Cytoscape.js | Headless element + position construction; no DOM layout tick |
| Sigma.js | graphology export only; WebGL/`Sigma` renderer is not started (understates GPU cost) |

Poor performance, configuration difficulty, and failures are retained in the
result record rather than omitted.

## CI policy

- Routine unit tests cover schema, deterministic sampling, and adapter
  construction on a tiny subgraph only.
- The full ladder is **workflow_dispatch-only**
  (`.github/workflows/visualization-limits-stress.yml`).
- It is **not** a PR, push, scheduled, required, or release check.
- No visualization functionality is added to GraphForge Core.
