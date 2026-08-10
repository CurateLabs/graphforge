# M4 Exit Evidence Reconciliation (#345)

Final accepted M4 tree: `2b1bfda8145c7f9fe125a75faf695792fc6bbfcb`
(`perf(exec): parallelize Node2Vec walk generation (#344) (#493)`).

This page is the exit ledger for epic **#335**. It performs no new optimization.
GPU acceleration remains intentionally out of scope; CPU-only embedded operation
satisfies M4 closure.

## Child disposition (live blocked-by of #345)

| Issue | Disposition | Exact-head PR |
|---|---|---|
| #334 | Merged — entry contract / harness | [#485](https://github.com/CurateLabs/graphforge/pull/485) |
| #337 | Merged — embedded resource policy | [#486](https://github.com/CurateLabs/graphforge/pull/486) |
| #338 | Merged — file-backed generations | [#487](https://github.com/CurateLabs/graphforge/pull/487) |
| #336 | Merged — streamed adjacency past Arrow 134M | [#488](https://github.com/CurateLabs/graphforge/pull/488) |
| #339 | Merged — streaming partitioned Parquet | [#489](https://github.com/CurateLabs/graphforge/pull/489) |
| #340 | Merged — CSR-native adjacency | [#490](https://github.com/CurateLabs/graphforge/pull/490) |
| #341 | Merged — bounded Arrow shaping | [#491](https://github.com/CurateLabs/graphforge/pull/491) |
| #342 | Merged — parallel exact cosine KNN | [#494](https://github.com/CurateLabs/graphforge/pull/494) |
| #343 | Merged — parallel PageRank | [#492](https://github.com/CurateLabs/graphforge/pull/492) |
| #344 | Merged — parallel Node2Vec walks | [#493](https://github.com/CurateLabs/graphforge/pull/493) |

Optional milestone side track **#398** (GSI profiler) is not on the #335 close
ledger and does not block M4.

## Exit evidence artifacts

| Artifact | Schema / role |
|---|---|
| [`m4-exit-evidence.json`](m4-exit-evidence.json) | `graphforge-m4-entry-evidence/1` rerun on the final tree (short + thread-parity matrix) |
| [`file-backed-oversize-evidence.json`](file-backed-oversize-evidence.json) | `graphforge-file-backed-oversize-evidence/1` public reopen past legacy 2 GiB envelope |
| [`m4-entry-baseline.md`](m4-entry-baseline.md) | Entry contract narrative (updated for shipped M4) |
| [`execution-resource-policy.md`](execution-resource-policy.md) | #337 policy + #342/#343/#344 crossovers |
| [`../reference/scale-limits.md`](../reference/scale-limits.md) | Persistence / adjacency / CSR claim table |

## Final-tree reproduction

```bash
make m4-entry-matrix-check
cargo test -p graphforge-api --test m4_entry_baseline -- --nocapture --test-threads=1
cargo test -p graphforge-api --test file_backed_graph_generation -- --test-threads=1
GF_M4_ENTRY_EVIDENCE_OUT=docs/development/m4-exit-evidence.json \
  cargo test -p graphforge-api --test m4_entry_baseline \
  large_manual_matrix_emits_hardware_dataset_evidence -- --ignored --nocapture
GF_FILE_BACKED_OVERSIZE_EVIDENCE_OUT=docs/development/file-backed-oversize-evidence.json \
  cargo test -p graphforge-api --test file_backed_graph_generation \
  oversize_file_backed_generation_exceeds_legacy_snapshot_envelope -- --ignored --nocapture
```

## Structural outcomes (not wall-clock)

- **File-backed persistence (#338):** public `GraphForge::new` reopens past the
  legacy 1 GiB/file · 2 GiB snapshot envelope (sparse oversize evidence). No
  universal GiB product ceiling. Full **8M-node / 128M-edge** densified
  public-facade reruns remain an optional scale-host measurement under local
  resource stops — not mislabeled as CI-accepted product max.
- **Adjacency (#336):** streamed projected Parquet build removes the
  134,217,727-edge Arrow concat ceiling. Full **>200M-edge** RSS evidence stays
  scale-host / scheduled (accepted blocker on this 4 vCPU / ~15 GiB agent).
- **Streaming Parquet (#339):** query path uses `GraphForgeParquetExec` with
  execution-time I/O and bounded batches.
- **CSR-native (#340):** fresh index hits report zero base-CSR HashMap expansion
  via structural counters.
- **Arrow shaping (#341):** analyst outputs are Arrow batches only (no retained
  `rows: Vec<Vec<…>>`).
- **CPU kernels (#342–#344):** exact cosine KNN, PageRank, and Node2Vec walk
  generation use the instance-owned private compute pool above documented
  crossovers; one-thread fingerprints are preserved. Thread cells that exceed
  the machine-relative concurrency budget are recorded `unavailable` (on this
  host `threads-8` is unavailable).

## Honest scale dispositions

| Claim | Exit disposition |
|---|---|
| Legacy 1 GiB/file · 2 GiB snapshot | Historical envelope only; still readable; not raised |
| Public reopen >2 GiB validated bytes | Proven via oversize file-backed evidence |
| 8M/128M (~15 GiB) densified public facade | Optional measured evidence / accepted blocker on agent hosts — not a universal product claim |
| >200M-edge adjacency index | Accepted pending scale-host evidence; 134M Arrow boundary no longer governs the public build path |
| GPU / accelerator | Out of scope for M4; no shipped capability or claim |
| Universal graph-size / SLO / cross-machine timing | Explicitly rejected |

## Exact-head CI

Each child PR above carried required Test Suite + CI Gate at its merge SHA.
This exit reconciliation does not rerun unchanged historical trees solely to
attach duplicate badges to the squash commits.

## Closing #335

After this issue merges and closes, #335 has no remaining required open child
other than itself (optional #398 excluded). Post this ledger on #335 and close
the epic when the live parent/blocked-by graph matches.
