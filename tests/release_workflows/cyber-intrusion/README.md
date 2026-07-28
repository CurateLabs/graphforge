# Cyber intrusion release workflow

This deterministic synthetic investigation proves that strict ontology errors,
derived-state freshness, graph correction, and competing explanations compose
through public GraphForge APIs. It is release evidence, not a security-product
claim or real-world attribution.

Run from the repository root after committing the scenario:

```bash
python3 tests/release_workflows/cyber-intrusion/run.py \
  --evidence-dir target/release-workflow-evidence
```

The bounded runner invokes an opt-in Rust example, builds a native Python wheel
from the same clean commit, installs it in an isolated Python 3.13 environment,
and rejects stale SHA, wheel, native-module, path, or version provenance. The
workflow is intentionally absent from required PR checks and the aggregate CI
gate.
