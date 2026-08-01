# Visualization limits stress harness (#299)

Apples-to-apples stress harness over the shared #298 projection contract for:

| Runtime | Option |
| --- | --- |
| Python | Plotly, Jaal, PyVis |
| Node.js | Cytoscape.js, Sigma.js |

## Not a CI/release gate

This harness is **opt-in**. It is never invoked by pull-request, push, scheduled,
required, or release workflows. Maintainers collect evidence with a local run or
the `workflow_dispatch`-only Action `Visualization Limits Stress`.

## Quick start (local)

```bash
# Python viz deps (isolated)
python3 -m venv .venv-stress
source .venv-stress/bin/activate
pip install -r examples/visualization/stress/requirements-stress.txt
# Optionally: install a local GraphForge wheel so projection time is measured
# through the public API. Without it, use --no-graphforge.

# Node headless probes
(cd examples/visualization/stress/node && npm install --no-fund --no-audit)

# Tiny dry-run (no GraphForge, karate-sized ladder only)
python examples/visualization/stress/harness/run.py --no-graphforge --max-nodes 34

# Full ladder (downloads SNAP facebook_combined into .cache/)
python examples/visualization/stress/harness/run.py
```

Results land in `results/results-latest.json` and `results/REPORT.generated.md`.

## Unit tests (safe for routine CI)

```bash
pytest examples/visualization/stress/tests -q
```

These exercise the result schema, deterministic subgraph selection, and each
adapter's construction path on a 12-node projection. They do **not** run the
full stress matrix.

## Documents

- [`METHODOLOGY.md`](METHODOLOGY.md) — ladder, timeouts, resource method (pre-run)
- [`REPORT.md`](REPORT.md) — honest comparison narrative
- [`size_ladder.json`](size_ladder.json) — machine-readable ladder + stopping rules

## Privacy / license

Karate fixture: cite Zachary (1977). SNAP facebook edges: research-use SNAP
terms; downloaded on demand, checksummed in the result record, not committed.
No personal data beyond the public academic/SNAP graphs is retained.
