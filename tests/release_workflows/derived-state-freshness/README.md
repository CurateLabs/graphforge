# Derived-state freshness workflow

This deterministic M22 release workflow proves that public freshness contracts never
silently serve stale text, adjacency, caller-vector, or analytical results after
independent source mutations. It uses one immutable strict ontology throughout; phase
manifests record the same ontology digest so schema drift cannot explain the outcome.

Run from a clean checkout at the exact commit under test:

```bash
python3 tests/release_workflows/derived-state-freshness/run.py \
  --commit-sha "$(git rev-parse HEAD)"
```

The bounded command runs lightweight fixture checks, the exact same-SHA private
adjacency-barrier regression test, the authoritative public Rust example, and isolated
same-SHA Python and Node bindings. It writes aggregate evidence under
`target/release-workflows/derived-state-freshness/`. The central registry wraps that
child record in the shared SHA-bound evidence envelope. This is an opt-in developer or
release-candidate workflow, not a required PR check or part of the aggregate CI Gate.

The fixture is synthetic, contains no network or provider calls, and intentionally
excludes throughput benchmarking and provider-managed embedding refresh workers.
