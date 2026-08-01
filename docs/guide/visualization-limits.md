# Visualization limits comparison

> Status: maintainer evidence harness for issue #299. Not a product feature and
> not a CI/release gate.

GraphForge does not ship a visualization engine. Ecosystem libraries can render
graphs produced through the public Python and Node APIs. Issue #298 documents
comparable examples; issue #299 records an apples-to-apples stress pass over the
same projection contract.

## What is measured

Maintainers can run an **opt-in** harness that walks a deterministic size ladder
for Plotly, Jaal, PyVis, Cytoscape.js, and Sigma.js, recording:

- GraphForge projection time (public API only)
- Visualization input preparation time
- Renderer / headless first-ready time
- Peak process memory (best-effort)
- Output payload size
- Success, failure, timeout, or resource-limit outcomes

Hosted-runner numbers are comparative observations for that environment, not
universal benchmarks.

## Where to look

- Harness + methodology: [`examples/visualization/stress/`](../../examples/visualization/stress/)
- Pre-run methodology freeze: [`METHODOLOGY.md`](../../examples/visualization/stress/METHODOLOGY.md)
- Honest report template: [`REPORT.md`](../../examples/visualization/stress/REPORT.md)
- Dispatch-only workflow: `.github/workflows/visualization-limits-stress.yml`

## What this is not

- Not wired into pull-request, push, scheduled, required, or release CI
- Not a GraphForge Core visualization subsystem
- Not a recommendation ranking of front-end libraries
