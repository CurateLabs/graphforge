# Visualization limits report (#299)

> **Disclaimer:** Numbers below are comparative observations from a stated
> environment. They are **not** universal rankings, production capacity
> guarantees, or hardware-independent benchmarks. GraphForge projection time is
> reported separately from visualization preparation and renderer
> initialization.

## Status

- Methodology frozen in [`METHODOLOGY.md`](METHODOLOGY.md) before recorded runs.
- Depends on the #298 shared projection contract (provisional bridge in
  `harness/contract.py` until #298 merges `examples/visualization/shared`).
- Checked-in `results/results-latest.json` currently reflects a **local**
  dry-run (`--no-graphforge --max-nodes 34`) that exercises all five options on
  the karate ladder. Replace/augment with the `workflow_dispatch` artifact from
  `Visualization Limits Stress` for CI-class evidence on the full ladder.

## Options in scope

All five options remain in the comparison, including awkward or failing cases:

1. Plotly (Python)
2. Jaal (Python / Dash)
3. PyVis (Python)
4. Cytoscape.js (Node, headless)
5. Sigma.js (Node / graphology headless)

## Local dry-run (karate ladder, no GraphForge binding)

Environment: local developer machine; GraphForge projection bypassed
(`graphforge_projection_seconds = 0`). Largest step: `karate_full` (34 nodes /
78 edges). All five options succeeded at every karate step.

| option | largest success | first failure/timeout | notes |
| --- | --- | --- | --- |
| plotly | `karate_full` (34 / 78) | none on karate ladder | Circular layout compromise |
| jaal | `karate_full` (34 / 78) | none on karate ladder | `Jaal.create()` only; no HTTP server |
| pyvis | `karate_full` (34 / 78) | none on karate ladder | HTML artifact ~largest payload on this ladder |
| cytoscape | `karate_full` (34 / 78) | none on karate ladder | Headless; no DOM layout |
| sigma | `karate_full` (34 / 78) | none on karate ladder | graphology only; no WebGL |

Raw rows: [`results/results-latest.json`](results/results-latest.json).

## CI-class full ladder

Preferred path (standard hosted Ubuntu runner, GraphForge wheel built in-job):

```bash
gh workflow run visualization-limits-stress.yml --ref <this-branch-or-sha>
```

Download the `visualization-limits-stress-<sha>` artifact, copy
`results-latest.json` here, and refresh the comparison table. Steps above 34
nodes download SNAP `facebook_combined` on demand (checksummed in the record).

## How to read a limit

When an option stops earlier than another:

1. Read the exact `step_id`, `node_count`, `edge_count`, and `status`.
2. Read `divergence_notes` for implementation inequivalence.
3. Read the environment manifest in the JSON result (`environment`).
4. Do **not** treat the ordering as a universal library recommendation.

## Separating GraphForge from visualization

- `graphforge_projection_seconds` — public API work only.
- `viz_prep_seconds` + `renderer_init_seconds` — ecosystem library work only.
- Do not sum them into a single “library score” without stating both parts.
