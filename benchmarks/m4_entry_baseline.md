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

- **Supported runtime:** Explicit `ExecutionResourcePolicy` default remains fixed
  two-worker / two-partition; callers may request `1` / `2` / `4` / `8` /
  `automatic` (#337).
- **Thread-parity matrix:** every cell in `deferred_runtime_configurations` is
  policy-supported; hosts may still record a cell `unavailable` when the request
  exceeds the machine-relative concurrency budget (no fabricated results).
- **CI gates:** schema, row counts, determinism / fingerprint stability, fixed-hop demand integrity (no eager `RoundRobinBatch` side effect).
- **Not CI gates:** absolute wall-clock thresholds.
- **8M/128M:** discovery-class measured fixture; public persistence beyond the legacy 2 GiB snapshot envelope is accepted via file-backed `graph`/`files` (#338 oversize evidence). Full densified 8M/128M public-facade reruns remain optional scale-host evidence under local resource stops (#345).
- **Exit ledger:** [`docs/development/m4-exit-evidence.md`](../docs/development/m4-exit-evidence.md).

## Workload classes

See the versioned contract `workloads` array. Short CI runs all six classes on
the `synthetic-small` fixture through the public Rust facade.

## Linking from later M4 work

Implementation issues cite this methodology + the versioned contract as the
before/after evidence source. Do not invent per-algorithm disconnected scripts
when the shared harness can record the required structural counters.
