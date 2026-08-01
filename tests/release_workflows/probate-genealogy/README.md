# Probate and genealogy release workflow

This executable, synthetic scenario asks which parentage interpretation is the
current working view after conflicting and late records. It is research
workflow evidence, not legal advice and not an adjudication of identity or
inheritance.

Run from the repository root:

```bash
python3 tests/release_workflows/probate-genealogy/run.py \
  --evidence-dir target/release-workflow-evidence
```

The bounded command validates the bundle and step mapping, runs the opt-in
Rust-owned `graphforge-api` example, builds and clean-installs the Python wheel, repeats the logical
selection/clear/reopen path through the binding, and writes SHA-bound evidence.
It has a 15-minute timeout and is intentionally not a required pull-request or
aggregate CI check.

The incomplete synthetic birth record is retained in advisory mode and named
as `PG-WARN-MISSING-RECORDED-ON`; the warning is research-process evidence, not
a claim that GraphForge inferred a fact. Transaction time records when the
researcher entered evidence. Valid time records the separately interpreted
historical interval. Neither is silently substituted for the other.
