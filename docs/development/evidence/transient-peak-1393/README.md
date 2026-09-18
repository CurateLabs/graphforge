# Transient peak composition (#1393)

Phase one of #1393: measure what the 36.76 GB transient peak is made of before
proposing to reduce any of it. Nothing here is a reduction.

## 1. The ladder peak decomposes exactly, at four scales

The rung metric `transient_peak_storage_bytes` is a high-water mark over the
**identity union of every file the lifecycle has open**, taken as the maximum
over the `gf` processes the certifier observes
(`benchmarks/runners/certify/src/lib.rs`, `LifecycleStorageSession`). That union
is wider than "the graph": it includes the benchmark's own generated input
Parquet and the import session's registered copy of it.

Splitting the recorded rungs in `docs/development/evidence/integrated-storage-1194/`
into the owners that are resident at the moment of the peak gives, per edge:

| rung | edges | peak B/edge | generated input | registered copy | construction root | residual |
|---|---:|---:|---:|---:|---:|---:|
| S18 | 4,194,304 | 546.80 | 49.02 | 49.02 | 448.34 | 0.424 |
| S19 | 8,388,608 | 546.79 | 49.02 | 49.02 | 448.34 | 0.418 |
| S20 | 16,777,216 | 546.78 | 49.02 | 49.02 | 448.34 | 0.415 |
| S22 | 67,108,864 | 547.78 | 49.02 | 49.02 | 449.33 | 0.411 |

`generated input` is `retained_owners["generated-inputs"]`, `registered copy` is
`retained_owners["source-project-import"]` — the `register-parquet` copy under
`import-sessions/<id>/sources/` — and `construction root` is
`storage_attribution.construction.transient_peak_allocated_bytes`. The residual
is 0.08% of the peak at every rung and is project control state (`FORMAT`,
`CURRENT`, locks, transaction and session manifests).

Two conclusions follow, and both are load-bearing:

1. **The peak is linear in edge count**, 546.8 to 547.8 bytes per edge across a
   sixteen-fold range — 0.18% drift. The S26 projection's linearity assumption
   holds over the measured range. It has still never been checked past 67
   million edges.
2. **82% of the peak is the construction root** and nothing else moves. The
   published graph, the portable package and the clean-import project do not
   exist yet when the peak is reached, so they contribute nothing to it.

## 2. Measuring the construction root

The union high-water mark was a bare scalar: `StorageAllocationLifecycle` keyed
its owners by `sha256(path)` and kept no record of what the bytes were. The
retained `ArtifactCategory` inventory cannot answer it either — the whole
construction root is one `ConstructionStaging` row, and at S22 the categories
account for 2.79 GB against a 36.76 GB peak.

`crates/graphforge-storage/src/transient_composition.rs` adds the missing axis: a
total, disjoint classification of every path a lifecycle run can allocate.
`StorageAllocationLifecycle` now records the composition **at the instant the
mark is raised**, so it sums to the peak by construction rather than explaining a
fraction of it, and refuses a continuation whose composition does not sum
(`validate_composition`).

## 3. Measured composition of the peak

Method: the Graph500 generator produces the same `nodes.parquet` / `edges.parquet`
the ladder uses (same seed, same edge factor; the S18 input is byte-identical to
the recorded rung's `generated-inputs`). The five `gf import-session` commands
run under `--allocation-diagnostics`, with the owner union chained across
processes exactly as `benchmarks/runners/certify` chains it. `peak-probe.rs` in
this directory is that driver.

**The measurement reproduces the ladder.** At S18 this run's peak is
2,293,440,512 bytes against the recorded rung's 2,293,444,608 — 4,096 bytes
apart, 0.0002%. So the composition below is the composition of the number that
gates admission, not of a proxy for it.

**The peak is reached inside `validate`**, not `commit`. `validate` decodes,
stages, seals, shapes and encodes; `commit` only publishes. The peak instant is
in the middle of the shaping merge.

Composition at the instant of the peak, bytes per edge:

| component | S16 (1.05M) | S17 (2.10M) | S18 (4.19M) | S18 share | residency |
|---|---:|---:|---:|---:|---:|
| `merge_tree_level` | 146.64 | 193.02 | **261.96** | 47.9% | 37% |
| `staged_chunk_run` | 137.31 | 137.31 | **137.31** | 25.1% | 94% |
| `staged_chunk_parquet` | 49.07 | 49.07 | **49.07** | 9.0% | 94% |
| `source_parquet` | 49.02 | 49.02 | **49.02** | 9.0% | 100% |
| `registered_source_copy` | 49.02 | 49.02 | **49.02** | 9.0% | 100% |
| `merge_source_copy` | 67.31 | 68.94 | **0** | 0% | 34% |
| `shaped_output` | 0 | 0 | **0** | 0% | 23% |
| `encoded_workspace` | 0 | 0 | **0** | 0% | 6% |
| `construction_control` | 0.51 | 0.50 | **0.42** | 0.1% | 100% |
| everything else | 0.02 | 0.01 | **0.02** | 0.0% | 100% |
| **total** | **498.91** | **546.88** | **546.80** | 100% | |
| recorded rung total | — | — | 546.80 | | |

Residency is the share of allocation transitions during which the component held
bytes; the peak is set at transition 6,169 of 8,023 at S18.

Four things this settles.

**The peak is the shaping merge, and nothing else.** At the instant of the peak
the resident set is the whole staged input (186.4 B/edge), the merge tree at its
deepest (262.0 B/edge), and the two source copies (98.0 B/edge).
`shaped_output`, `encoded_workspace` and the content-addressed staging copy are
all **zero at the peak** — they are built after the merge has collapsed. Their
own high-water marks are 76.64, 38.77 and 0 B/edge, every one of them below the
merge tree's. Reducing them does not move the admission number.

**The merge tree is the only component that grows with scale.** Every other
component is per-edge constant to two decimal places across a fourfold range —
137.31, 49.07, 49.02, 49.02 at all three scales. The merge tree grows with merge
depth: 146.64 at 16 edge chunks, 193.02 at 32, 261.96 at 64. Fan-in is 32, so
S18 (64 chunks) and S22 (1,024 chunks) are both two-level, which is why the S18
and S22 peaks per edge agree to 0.18%.

**Therefore the S22 composition is S18's, plus about 1 B/edge of merge tree.**
The rung's construction-root peak per edge is 448.34 at S18 and 449.33 at S22;
the non-merge components are scale-invariant, so the +0.99 belongs to the merge
tree, putting it at ~262.9 B/edge, 48.0% of the S22 peak.

**S16 is below a structural threshold and must not be used for projection.** At
16 edge chunks the merge tree is one level, so S16 reads 498.91 B/edge against
546.8 at every deeper scale. That is a step, not a trend.

## 4. Does the peak scale predictably?

Yes, over the measured range, and the S26 projection's linear assumption holds:

- across the four recorded rungs, 546.80 / 546.79 / 546.78 / 547.78 bytes per
  edge over a sixteen-fold range — 0.18% drift;
- across this run's three scales, 546.88 and 546.80 at the two that are above
  the one-level merge threshold.

Two caveats that the projection cannot see. The merge tree is a step function of
`ceil(log32(chunks))`; S22 through S26 are all in the two-to-three level band,
and a third level would add roughly another 130 B/edge. And nothing has been
measured past 67 million edges, so linearity beyond that remains an assumption.

## 5. Classification

| bucket | components | S22 B/edge | share |
|---|---|---:|---:|
| Immovable | `source_parquet`, `registered_source_copy` | 98.04 | 17.9% |
| Removed by the write-path redesign | `merge_tree_level` (and `merge_source_copy`, which peaks earlier) | 262.95 | 48.0% |
| Reducible | `staged_chunk_run`, `staged_chunk_parquet` | 186.38 | 34.0% |
| Control | `construction_control` and the rest | 0.44 | 0.1% |

**Immovable.** `registered_source_copy` is settled policy: the session owns its
bytes from registration, hardlinking would not preserve that, and reflink is
unavailable on ext4. `source_parquet` is the operator's own input file, which
must be on disk to be read; GraphForge did not create it and cannot remove it.
Together they are exactly two copies of the input, 49.02 B/edge each, identical
at every scale.

**Removed by the redesign.** The design's range partitioning makes
`concat(shard_0..shard_P)` the globally sorted sequence, so `merge-*-l*-g*.run`,
`merge-rows-*.parquet`, `merge-unified-*` and `merge-*-source-*` have no
producer left. That is the 262.95 B/edge that sits at the top of the peak, and
it is measured, not assumed. **Contingent on shards not spilling**: the design
itself flags (§8.2) that a hard memory budget forces a bounded per-shard
external merge, which would put some of these bytes back.

**Reducible.** The staged input is resident for 94% of the run because nothing is
retired until the whole shape completes. Under per-shard retirement the live
staged set becomes one partition's worth rather than all of it. Separately,
`staged_chunk_run` is 2.8x `staged_chunk_parquet` for the same rows — the
fixed-width runs are uncompressed. Neither saving is quantified here and neither
is needed for the target below.

## 6. Target and margin

Removing the merge tree leaves a peak somewhere between two measured bounds:

- **lower**, 284.86 B/edge — the recorded peak instant with the merge tree
  subtracted, which is what would still be resident at that moment;
- **upper**, 400.27 B/edge — that same set plus the full independent high-water
  marks of `shaped_output` and `encoded_workspace`, assuming pessimistically that
  they become simultaneously resident once the merge tree stops dominating.

Take the upper bound as the target, because the peak after the merge tree is
gone has not been measured and a target should not depend on an optimistic
assumption about which components overlap.

| | B/edge | projected S26 peak | margin | margin as % of peak |
|---|---:|---:|---:|---:|
| today | 547.78 | 588.5 GB | 9.5 GB | 1.6% |
| **target** | **400.3** | **430.0 GB** | **168.0 GB** | **39%** |
| if overlap is favourable | 284.9 | 306.0 GB | 292.0 GB | 95% |

Projection uses the ladder's own `_project` on S20 and S22 with every peak
scaled by the same factor; 739.3 GB available, 141.3 GB reserve, 598.0 GB
envelope. The target leaves 28% of the envelope free instead of 1.6%.

Nothing in this document raises the reserve, frees host disk, or reclassifies
bytes out of the peak. The target is reached by deleting the merge tree, which
the write-path redesign already deletes for unrelated reasons.
