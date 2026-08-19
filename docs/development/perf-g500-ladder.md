# Bounded billion-edge scale ladder (#736)

This is the M5 ([#735](https://github.com/DecisionNerd/graphforge/issues/735))
**root gate**: a versioned, **bounded-memory** Graph500-parameter (`edgefactor=16`
undirected Kronecker/R-MAT) scale ladder that measures the *first real
bottleneck* on the published Rust facade before any billion-edge certification
([#745](https://github.com/DecisionNerd/graphforge/issues/745)).

It is a thin in-tree **reference client** under the
[Scale Evaluation](../reference/scale-evaluation.md) contract — **not**
Official-track and **not** TEPS. It does **not** itself claim one billion live
edges.

| Claim | Status |
|---|---|
| Official Graph500 submission | No |
| `track: official` in evidence JSON | Must not be set (`track` is `null`) |
| Pinned `github.com/graph500/graph500` generator | No — bounded bench-local Kronecker in the test file |
| Graph500 BFS kernel / harmonic-mean TEPS | Non-goal (`teps` is `null`) |
| One-billion-live-edge product certification | No — that is #745 |
| Engineering green (generate → ingest → reopen → GSI → `LIMIT 1000`) | Yes, on `GraphForge::publish_bulk_*` + `execute` |

## What is new versus the #710 SCALE-20 client

[perf-g500-scale20.md](perf-g500-scale20.md) retains **every** raw tuple in
memory (a `Vec` of all `2^scale * 16` attempts) and sorts/deduplicates in RAM.
That is fine at SCALE-20 but allocates ~8.6 GiB of raw tuples at SCALE-26.

This ladder replaces that with **external sort + spill + k-way merge**:

1. Generate Kronecker attempts into a fixed `buffer_edges` buffer.
2. When full, sort the buffer and **spill a run file** to disk (no in-buffer
   dedup, so every duplicate is counted at merge).
3. K-way merge the sorted runs, emitting each unique undirected pair once.

Peak resident edges never exceed `buffer_edges`, **independent of total edge
count**. Live edges stream straight into `publish_bulk_edges` during the merge,
so ingest is bounded too.

## Counts always reconcile

Every attempted rung proves:

```
raw_attempts == live_unique_edges + self_loops_rejected + duplicates_rejected
```

`raw_attempts` (`2^scale * 16`) can therefore **never** be reported as live
persisted edges. SCALE-26 produces `1,073,741,824` raw attempts; the live
persisted count after self-loop and duplicate policy may fall **below** one
billion, and the evidence records both distinctly.

## The versioned profile

The ladder, seed, initiator, policy, envelope, metrics, and exact invocation
live in a single committed profile,
[`crates/graphforge-api/tests/fixtures/scale_g500_ladder.v1.json`](../../crates/graphforge-api/tests/fixtures/scale_g500_ladder.v1.json)
(`schema: graphforge-billion-edge-ladder/1`). The runner reads it; do not
hard-code rungs elsewhere.

| Field | Value |
|---|---|
| Rungs | S10 (CI), S20, S22, S24, S25, S26 (provisioned) |
| Seed / initiator | `1` / `A,B,C,D = 0.57, 0.19, 0.19, 0.05` |
| Policy | undirected, drop self-loops, drop duplicates, canonical `(lo,hi)` |
| Host envelope | **128 GiB** peak RSS, **1 TiB** local NVMe, **24 h** |

The **S10** rung runs in normal CI and deliberately sets `buffer_edges` below
its own edge count, so the spill/merge path is exercised on every run. S20–S26
are `#[ignore]` and opt-in.

## First-fail ladder

`run_ladder` walks the provisioned rungs in increasing scale and, after each
phase (`generate`, `ingest`, `reopen`, `query`), compares peak RSS / disk /
elapsed time against the envelope. On the **first** violation it records the
failing phase and `error_class` (`oom` | `disk_exhaustion` | `timeout`) and
stops — no larger rung is attempted and no SCALE-26 pass is claimed.

> RSS fidelity: peak RSS is read from `/proc/self/status` `VmHWM` on Linux
> (a true high-water mark) and falls back to sampled `ps` RSS otherwise (an
> *instantaneous lower bound*, not a peak). Evidence records `rss_source`
> (`vmhwm` | `ps_sampled`) so a `ps_sampled` value is read as a floor. Run
> provisioned certification rungs on **Linux**.

> Ingest-phase attribution: `publish_bulk_nodes` / `publish_bulk_edges` build an
> in-memory set of existing identities per call (`bulk_construction.rs`), so RSS
> during the `ingest` phase grows with the persisted graph, not just the
> generator buffer. Per-phase `rss_peak_bytes` is recorded on each step, and an
> `oom` with `first_failing_phase: "ingest"` reflects that upstream
> bulk-publication cost — **not** a generator-memory regression. Reducing that
> cost is upstream storage work (a non-goal here), tracked toward #745.

## Commands

Always-on CI (SCALE-10 smoke + all reconciliation / determinism / bounded /
first-fail unit tests):

```bash
cargo test -p graphforge-api --test scale_g500_ladder
```

Provisioned full ladder (long; isolate the target dir; Linux scale-host):

```bash
CARGO_TARGET_DIR=/tmp/cargo-g500-ladder make bench-g500-ladder
# override the evidence path:
GF_G500_LADDER_EVIDENCE_OUT=build/g500-ladder-evidence.json make bench-g500-ladder
```

Default evidence path: `docs/development/g500-ladder-evidence.json`.

## Evidence JSON

One object per attempted rung (schema
`graphforge-billion-edge-ladder-evidence/1`), carrying:

- `counts`: `raw_attempts`, `self_loops_rejected`, `duplicates_rejected`,
  `live_unique_edges`, and the reconciliation identity.
- `persisted`: reopened `node_count` / `edge_count` (must equal
  `live_unique_edges`).
- `first_failing_phase`, `error_class`, `pass`, `reconciles`.
- `input_fingerprint` (deterministic SHA-256 of the sorted live edge set),
  `rss_peak_bytes`, `disk_used_bytes`, `wall_time_s`, per-phase `steps`.
- `machine_envelope` (128 GiB / 1 TiB / 24 h), `sut`, `generator`.
- `track` and `teps` are always `null`.

Wall-clock and RSS numbers are hardware-specific observations, never CI
millisecond gates.

## CI placement

Per the [Scale Evaluation](../reference/scale-evaluation.md) contract, large
Graph500 runs **must not** be wired into normal GitHub Actions. Only the S10 CI
rung runs in `test.yml`. The provisioned ladder is Make-only until an approved
dedicated runner / evidence job exists (tracked with #745 certification).
