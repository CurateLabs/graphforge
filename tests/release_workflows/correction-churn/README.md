# Correction-churn release workflow

This deterministic synthetic workflow proves that repeated curation mistakes are
corrected through public compensating operations rather than hidden undo or
mutable audit history. It exercises a graph correction, an ontology/data
validation correction, and append-only M20/M21 corrections across separate
committed generations. The prior graph view is pinned by a public checkpoint;
assertion supersession, reasoning amendment, and hypothesis-member removal stay
inspectable after reopen.

Run from the repository root:

```bash
python3 tests/release_workflows/correction-churn/run.py \
  --commit-sha $(git rev-parse HEAD)
```

The command validates the bundle, runs the Rust-owned workflow, builds and
clean-installs the same-SHA native Python wheel, repeats representative
correction/reopen behavior, and writes SHA-bound evidence under
`target/release-workflows/correction-churn/`. It has a 15-minute bound and is
intentionally absent from required PR checks and the aggregate CI gate.
