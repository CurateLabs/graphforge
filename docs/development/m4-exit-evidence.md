# M4 Exit Evidence Reconciliation (#345)

> **Status: current.** Frozen against exact main tip
> `53b369598ba748144b531a50dbed34de36bef0f2`
> (`test(storage): prove densified 8M/128M file-backed public reopen (#338) (#763)`),
> which includes the recovered #336 >200M adjacency evidence (#762) and the
> densified #338 public-facade evidence (#763) after the post-#498/#499–#588
> expansion.

This ledger replaces the superseded `2b1bfda…` exit artifact. GPU acceleration
remains intentionally out of scope; CPU-only embedded operation remains the
required runtime model. M5 (#735) is separate forward work and does not waive
or replace this gate.

## Child disposition (live blocked-by of #345)

| Issue | Disposition | Exact-head PR |
|---|---|---|
| #334 | Merged — entry contract / harness | [#485](https://github.com/CurateLabs/graphforge/pull/485) |
| #337 | Merged — embedded resource policy | [#486](https://github.com/CurateLabs/graphforge/pull/486) |
| #338 | Merged — file-backed generations + densified 8M/128M public reopen evidence | [#487](https://github.com/CurateLabs/graphforge/pull/487), evidence [#763](https://github.com/CurateLabs/graphforge/pull/763) |
| #336 | Merged — streamed adjacency past Arrow 134M + >200M public build evidence | [#488](https://github.com/CurateLabs/graphforge/pull/488), evidence [#762](https://github.com/CurateLabs/graphforge/pull/762) |
| #339 | Merged — streaming partitioned Parquet | [#489](https://github.com/CurateLabs/graphforge/pull/489) |
| #340 | Merged — CSR-native adjacency | [#490](https://github.com/CurateLabs/graphforge/pull/490) |
| #341 | Merged — bounded Arrow shaping | [#491](https://github.com/CurateLabs/graphforge/pull/491) |
| #342 | Merged — parallel exact cosine KNN | [#494](https://github.com/CurateLabs/graphforge/pull/494) |
| #343 | Merged — parallel PageRank | [#492](https://github.com/CurateLabs/graphforge/pull/492) |
| #344 | Merged — parallel Node2Vec walks | [#493](https://github.com/CurateLabs/graphforge/pull/493) |
| #498/#499–#588 | Merged — later algorithm scale/polish batch; per-algorithm disposition notes under `docs/development/m4-disposition-*.md` | individual child PRs on those issues |

Optional milestone side track **#398** (GSI profiler) is not on the #335 close
ledger and does not block M4.

## Exit evidence artifacts

| Artifact | Schema / role |
|---|---|
| [`m4-exit-evidence.json`](m4-exit-evidence.json) | `graphforge-m4-entry-evidence/1` final-tree #334 matrix rerun at the frozen SHA |
| [`adjacency-200m-evidence.json`](adjacency-200m-evidence.json) | `graphforge-adjacency-200m-evidence/1` densified >200M public `index_adjacency` |
| [`file-backed-128m-evidence.json`](file-backed-128m-evidence.json) | `graphforge-file-backed-128m-evidence/1` densified 8M/128M public reopen |
| [`file-backed-oversize-evidence.json`](file-backed-oversize-evidence.json) | `graphforge-file-backed-oversize-evidence/1` sparse >2 GiB envelope reopen |
| [`m4-entry-baseline.md`](m4-entry-baseline.md) | Entry contract narrative |
| [`execution-resource-policy.md`](execution-resource-policy.md) | #337 policy + CPU-kernel crossovers |
| [`../reference/scale-limits.md`](../reference/scale-limits.md) | Persistence / adjacency / CSR claim table |

## Final-tree reproduction

```bash
git rev-parse HEAD   # expect 53b369598ba748144b531a50dbed34de36bef0f2 on the accepted tip
make m4-entry-matrix-check
cargo test -p graphforge-api --test m4_entry_baseline -- --nocapture --test-threads=1
cargo test -p graphforge-api --test file_backed_graph_generation -- --test-threads=1
GF_M4_ENTRY_EVIDENCE_OUT=docs/development/m4-exit-evidence.json \
  make bench-m4-entry
make bench-adjacency-200m
make bench-file-backed-128m
```

Short CI surfaces on this tip:

- `make m4-entry-matrix-check` — OK
- `cargo test -p graphforge-api --test m4_entry_baseline` — 6 passed, 1 ignored (large manual)

## Structural outcomes (not wall-clock)

- **File-backed persistence (#338):** public `GraphForge::new` reopens past the
  legacy 1 GiB/file · 2 GiB snapshot envelope (sparse oversize evidence) and the
  densified **8M-node / 128M-edge** class
  ([`file-backed-128m-evidence.json`](file-backed-128m-evidence.json)).
  No universal GiB product ceiling.
- **Adjacency (#336):** streamed projected Parquet build removes the
  134,217,727-edge Arrow concat ceiling structurally. Densified **>200M-edge**
  public `GraphForge::index_adjacency` succeeded with recorded RSS/spill/chunk
  configuration ([`adjacency-200m-evidence.json`](adjacency-200m-evidence.json)).
- **Streaming Parquet (#339):** query path uses `GraphForgeParquetExec` with
  execution-time I/O and bounded batches.
- **CSR-native (#340):** fresh index hits report zero base-CSR HashMap expansion
  via structural counters.
- **Arrow shaping (#341):** analyst outputs are Arrow batches only (no retained
  `rows: Vec<Vec<…>>`).
- **CPU kernels:** exact cosine KNN, PageRank, and Node2Vec walk generation from
  #342–#344 remain on the close ledger. The later #498/#499–#588 algorithm batch
  is dispositioned per-issue under `m4-disposition-*.md` on this same tree.
  Thread cells that exceed the machine-relative concurrency budget remain
  recorded as `unavailable`, never fabricated.

## Honest scale dispositions

| Claim | Exit disposition |
|---|---|
| Legacy 1 GiB/file · 2 GiB snapshot | Historical envelope only; still readable; not raised |
| Public reopen >2 GiB validated bytes | Proven via oversize file-backed evidence |
| 8M/128M densified public facade | Proven via [`file-backed-128m-evidence.json`](file-backed-128m-evidence.json) (#338 / #763) |
| >200M-edge adjacency index | Proven via [`adjacency-200m-evidence.json`](adjacency-200m-evidence.json) (#336 / #762) |
| GPU / accelerator | Out of scope for M4; no shipped capability or claim |
| Universal graph-size / SLO / cross-machine timing | Explicitly rejected |

## Exact-head CI

Each child PR carried required Test Suite + CI Gate at its merge SHA. This exit
reconciliation does not rerun unchanged historical trees solely to attach
duplicate badges to squash commits. Final freeze SHA:
`53b369598ba748144b531a50dbed34de36bef0f2`.

## Close gate

With #336 and #338 outcomes attached and this ledger refreshed on the post-#498
tree, #345 may close. #335 then has no open implementation child other than the
canonical tracker itself and may close M4 when its own AC are satisfied.

M5 (#735) remains the separate forward-looking billion-live-edge and hardened-
interchange program.
