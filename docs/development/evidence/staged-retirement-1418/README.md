# Staged-input retirement at shaping boundaries (#1418)

Measured before/after on the same host, same Graph500 inputs (edge factor 16,
seed 1418), same #1393 `peak-probe` driver and composition instrumentation
(`transient_composition.rs`). "Before" is current `main` at `ab1a713e`;
"after" is this change. The probe drives the five `gf import-session`
commands under `--allocation-diagnostics` with the owner union chained across
processes, exactly as the certify runner does, and records the composition at
the instant the high-water mark is raised.

## Result

| scale | edges | rung peak (B/edge) before → after | `staged_chunk_run` + `staged_chunk_parquet` at the peak instant, before → after | staged residency (share of allocation transitions) before → after |
|---|---:|---:|---:|---:|
| S17 | 2,097,152 | 499.6 → 493.4 | 186.38 → **0.00** B/edge (390.9 MB → 0) | 97% → 86% |
| S18 | 4,194,304 | 499.2 → 495.2 | 186.38 → **0.00** B/edge (781.7 MB → 0) | 97% → 86% |

The staged copy of already-consumed input no longer exists at the instant
that sets the transient peak: every full sealing group retires its staged
chunks before the run reaches the phases that now set the peak. The residual
86% "residency" is the shortened per-group lifecycle (each chunk exists only
until its group seals), not whole-run residency.

The total rung peak moves by under 1% at these scales: with the staged block
gone from the shaping phase, the peak instant shifts to the later
encode/publish phases whose components (`shaped_output`, `shaped_partition_run`,
`staged_domain_run`, encoded workspace) are per-edge-constant and now set the
mark. Per the #1393 accounting rules, the staged-block saving and the
later-phase floor must not be added together; the honest rung-peak statement
at S17/S18 is the ~0.5–5 MB measured deltas above.

## Why the peak instant matters more at admission scale

The sealing cadence keeps at most one boundary group of staged input live:
`open_spills × 255 KiB` (the #1442 bytes-per-fsync budget, one fsync per open
spill per seal). `open_spills` saturates at `partition_count = 4096` for
inputs above ~71 M identity records, so the live staged ceiling saturates at
~4.3 GB while staged totals scale linearly:

| rung | staged total (measured 186.38 B/edge) | live staged ceiling | staged at peak |
|---|---:|---:|---:|
| S22 (67.1M edges) | 12.5 GB | 4.3 GB | ~64 B/edge |
| S26 (1.07B edges) | 200 GB | 4.3 GB | ~4 B/edge |

The S22/S26 rows are projections from the measured cadence and the recorded
per-edge constants, not run outputs; the measured scales above are S17/S18.

## Method

```text
# before: worktree at main ab1a713e; after: this branch
cargo build --release -p graphforge-cli
cargo build --release -p graphforge-benchmark-graph500-generator \
    --manifest-path benchmarks/Cargo.toml
cp docs/development/evidence/transient-peak-1393/peak-probe.rs \
   crates/graphforge-storage/examples/peak-probe.rs
cargo build --release -p graphforge-storage --example peak-probe

graphforge-benchmark-graph500-generator --scale 17 --edge-factor 16 --seed 1418 \
    --nodes nodes-s17.parquet --edges edges-s17.parquet
peak-probe <gf> <project-dir> <nodes> <edges> <operation-uuid>   # TMPDIR on the project volume
```

Host: single ext4 volume, TMPDIR on the same volume as the project (the
certifier's chained-union contract requires `StorageAllocationOperation`
project paths to resolve on one device). Raw probe receipts:
`s17-before-allocation.json`, `s17-after-allocation.json`,
`s18-before-allocation.json`, `s18-after-allocation.json` — each is the
winning-peak composition record (`contract: graphforge-storage-allocation/1`
family); `unclassified` is 0 in all four, and the composition sums to the
recorded peak.

## Correctness evidence in the same change

- Crash mutation tests kill the run at `shape.after_group_seal` (marker
  installed, unlinks pending) and `shape.after_group_retire` (unlinks done):
  the reopened session resumes behind the boundary, completes, publishes the
  full graph, and its `storage_current` ledger equals a clean run's.
- A forward-lying or internally inconsistent progress chain is refused: the
  head boundary is proven against the chunk receipts' identity-record count,
  and every covered boundary is proven by the chain digest.
- The transient-composition invariant (`composition sums to the peak exactly,
  `Unclassified` stays 0`) is asserted by
  `peak_composition_accounts_for_every_byte_of_the_peak_at_two_scales`.
