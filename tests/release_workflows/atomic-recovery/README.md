# Atomic-recovery release workflow

This deterministic synthetic workflow proves that one analytical step can
publish graph topology/property mutations together with provenance, assertions,
evidence, and epistemic participants as a single project generation. Controlled
failpoint interruption before and after the `CURRENT` publish edge recovers
exactly the previous or new complete generation. Invalid ontology or
cross-reference input rejects before staging. Exact retry is idempotent;
conflicting identity reuse returns a structured error. No orphan participant,
staging file, or live lock remains.

Run from the repository root:

```bash
python3 tests/release_workflows/atomic-recovery/run.py \
  --commit-sha $(git rev-parse HEAD)
```

The bounded command validates the bundle, executes the Rust-owned composite
failpoint matrix, clean-installs a same-commit native Python wheel for
representative reopen parity, and writes evidence under
`target/release-workflows/atomic-recovery/`. It has a 15-minute bound and is
intentionally absent from required PR checks and the aggregate CI gate.

Depends on the public composite publication surface from #2581.
