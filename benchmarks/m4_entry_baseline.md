# M4 Entry Baseline Methodology

Companion to [docs/development/m4-entry-baseline.md](../docs/development/m4-entry-baseline.md)
and GitHub issue **#334**.

## Reproduction

```bash
# Contract + evidence-schema unit tests
make m4-entry-matrix-check

# Required short CI matrix (structural gates; prints timing observations)
cargo test -p graphforge-api --test m4_entry_baseline -- --nocapture

# Manual large evidence emitter (ignored test)
make bench-m4-entry
```

## Accepted entry posture

- **Supported runtime:** fixed two-worker Tokio facade; DataFusion default partitions as observed.
- **Deferred runtime matrix:** `1` / `2` / `4` / `8` / `automatic` → owned by **#337** with parity assertions named in `tests/contracts/m4-entry-matrix.json`.
- **CI gates:** schema, row counts, determinism / fingerprint stability, fixed-hop demand integrity (no eager `RoundRobinBatch` side effect).
- **Not CI gates:** absolute wall-clock thresholds.
- **8M/128M:** discovery-only; public path gated by **#338**.

## Workload classes

See the versioned contract `workloads` array. Short CI runs all six classes on
the `synthetic-small` fixture through the public Rust facade.

## Linking from later M4 work

Implementation issues cite this methodology + the versioned contract as the
before/after evidence source. Do not invent per-algorithm disconnected scripts
when the shared harness can record the required structural counters.
