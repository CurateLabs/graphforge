# Knowledge-evolution release workflow

This deterministic synthetic workflow freezes one strict graph and ontology,
then evolves only evidence and epistemic records. Confidence never chooses a
hypothesis. Working interpretations change only through explicit selection
events, and neutral query/search/M18/M19 results must remain identical. Belief
resolution is invoked only through the explicit public projection surface.

Run from the repository root:

```bash
python3 tests/release_workflows/knowledge-evolution/run.py \
  --commit-sha $(git rev-parse HEAD)
```

The bounded command validates the bundle, executes the Rust-owned workflow,
clean-installs a same-SHA native Python wheel for representative cutoff and
reopen replay, and writes SHA-bound evidence below
`target/release-workflows/knowledge-evolution/`. Its 15-minute execution is
local/release-candidate evidence and is not a required PR or aggregate CI gate.
