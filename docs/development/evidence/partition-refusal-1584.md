# When construction refuses an over-budget partition (#1584)

Evidence for [ADR 0047](../../adr/0047-over-budget-partitions-and-instance-cpu-budget.md).
It answers two questions. How large does the largest construction partition get,
on Graph500 and on graphs with real hubs? What happens when it exceeds the
recorded `max_partition_bytes` budget, which defaults to 256 MiB
(268,435,456 bytes)?

Raw records, scripts and hashes are in [`partition-refusal-1584/`](partition-refusal-1584/).
Measured on OVHC-AGENCY on 2026-09-24.

## Why a hub fills one partition

Construction routes endpoint records by node UUID with a node-only splitter
plan (#1439). Every edge contributes two 33-byte endpoint records, one for each
end. So all of one node's records land in the same partition, and no splitter
set can divide them. The node plan's range count is
`clamp(ceil(nodes / 16,384), 256, 4,096)`
(`IdentitySampler::with_target`, `graph_construction/partition.rs`), where
4,096 is the recorded maximum.

That gives a model for the largest node-keyed partition:

```
bytes ≈ 33 × (hub degree + 2 × edges / ranges)
```

The hub term is the largest node's total degree. The second term is an even
share of every other endpoint record.

## Graph500 hub growth, measured

`degrees.py` counts every node's exact in-, out- and total degree by streaming
the generator's edge Parquet. Inputs are the ladder profile's: edge factor 16,
seed `13907095936298285200`. Records: `degrees.jsonl`; input hashes:
`inputs.sha256`.

| Scale | Edges | Largest total degree | Largest out / in | Growth per step |
| --- | ---: | ---: | --- | ---: |
| S18 | 4,194,304 | 59,962 | 29,799 / 30,163 | — |
| S20 | 16,777,216 | 137,940 | 69,267 / 68,673 | 1.52× |
| S22 | 67,108,864 | 320,548 | 159,938 / 160,610 | 1.52× |
| S24 | 268,435,456 | 739,650 | 369,103 / 370,547 | 1.52× |
| S26 | 1,073,741,824 | 1,709,763 | 854,732 / 855,031 | 1.52× |

Which node is the largest hub changes with scale, because the generator
scrambles vertex numbers per scale. Growth matches the generator's Kronecker
parameters: the largest node's expected out-degree grows
by `2 × (A + B) = 2 × 0.76 = 1.52` per step.

## The model against measured partitions

The #1509 instrumented runs recorded the largest fixed-width partition at S18
and S20 (`construction-reuse-integrated-1509/calibration-s18.jsonl` and
`main/instrumented.jsonl`).

| Scale | Ranges | Hub term | Share term | Model | Measured largest partition | Measured / model |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| S18 | 256 | 59,962 | 32,768 | 3,060,090 B | 2,888,820 B | 0.944 |
| S20 | 256 | 137,940 | 131,072 | 8,877,396 B | 8,449,320 B | 0.952 |

The model overstates by 5–6%. The sampled cut gives the hub's range a little
less than an even share, and that error errs on the safe side.

## Graph500 projection

The hub term is measured through S26. The S28–S30 rows extend the S26 hub by
1.52× per step and are **model outputs, not measurements**.

| Scale | Ranges | Hub term | Share term | Model largest partition | Share of 256 MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| S22 | 256 | 320,548 | 524,288 | 27.9 MB | 10% |
| S24 | 1,024 | 739,650 | 524,288 | 41.7 MB | 16% |
| S26 | 4,096 | 1,709,763 | 524,288 | 73.7 MB | 27% |
| S28 | 4,096 | 3,950,236 (projected) | 2,097,152 | 199.6 MB | 74% |
| S29 | 4,096 | 6,004,359 (projected) | 4,194,304 | 336.6 MB | 125% |
| S30 | 4,096 | 9,126,626 (projected) | 8,388,608 | 578.0 MB | 215% |

Graph500 through S26, the certification target, stays well under the budget.
Beyond it the 4,096-range maximum stops the share term shrinking. From S30 an
even share alone exceeds the budget, with no hub at all (276.8 MB).

## Real hubs: a star graph

Graph500's hubs grow slowly. Real graphs do not follow that curve. A class node
in a knowledge graph, linked to every entity of its type, is one hub whose
degree equals its population. `star.py` writes the extreme case: `L` leaf nodes,
each with one edge to a single hub. It uses the Graph500 generator's UUID layout
and schema, so only the degree distribution differs.

`ingest.sh` runs the ladder profile's five `import-session` commands into a
fresh project with the frozen #1509 binary, `gf` SHA-256 `bc324b03…d601c`. That
binary is a test-support build of the code #1580 landed. With no `GF_SHAPE_*`
selector set, it runs the production construction path. After a successful
commit, the script reopens the project and counts edges. Summary lines:
`star-runs/results.txt`. Per-run receipts, `time -v` output and external-sort
metrics are also under `star-runs/`.

| Case | Hub degree | Outcome |
| --- | ---: | --- |
| Production, 4M leaves | 4,000,000 | Publishes; reopened edge count 4,000,000 |
| Production, 9M leaves | 9,000,000 | **Refused:** `partition materialization requires 297536415 bytes, exceeds recorded budget 268435456` |
| #1507 hybrid, pool = budget (256 MiB) | 9,000,000 | **Fails:** `Resources exhausted: Failed to allocate additional 264.0 KB for ExternalSorterMerge[0] with 36.6 MB already allocated ... greedy(used: 255.8 MB, pool_size: 256.0 MB)` |
| #1507 hybrid, 64 MiB pool | 9,000,000 | Publishes; 11 spill runs, 298.8 MB spilled, peak pool 66,985,422 of 67,108,864 B |
| #1507 hybrid, 8 MiB pool | 9,000,000 | Publishes; 117 spill runs, 848.5 MB spilled, peak pool 8,381,720 of 8,388,608 B |

The refusal's byte count fits the model. Divided by 33 bytes, 297,536,415 bytes
is 9,016,255 records: the hub's 9,000,000 plus 16,255 leaf records from its
range. The external-sort metrics report the same record count.

Both publishing hybrid runs return the same reopened edge count, 9,000,000, with
the same `result_sha256` (`a5803634…`). With the default budget, the refusal
threshold is a hub of about 8.1 million edges at any total graph size. That is
roughly the edge count of the S19 Graph500 rung.

### The hybrid's pool needs headroom

The #1507 adapter sizes DataFusion's pool to `max_partition_bytes` by default.
With the pool equal to the budget and a partition just over it, the sort filled
the pool before spilling. Its merge reservation then could not grow, and
DataFusion refused. Pools of 64 MiB and 8 MiB left the merge room and
published. The #1507 and #1509 spikes never saw this: they only ran pools of
1 MiB or less against partitions of at most 8.4 MB. Any production adapter must
size its pool with measured headroom below the budget (ADR 0047, #1585).

Wall times here are indicative only. They are single runs without the quiet-host
protocol, and the host was generating and counting the S24 and S26 inputs at
the same time.

## Reproduce

```bash
# Graph500 degrees (inputs from the generator with the ladder seed)
uv run --no-sync python degrees.py <scale-dir>/edges.parquet
# Star inputs, then an ingest (TMPDIR on ext4)
uv run --no-sync python star.py 9000000 star-9000000
./ingest.sh star9m-production star-9000000 X=1
./ingest.sh star9m-hybrid-pool67108864 star-9000000 \
  GF_SHAPE_SPILL_SPIKE=datafusion GF_SHAPE_SPILL_POOL_BYTES=67108864 GF_SHAPE_SPILL_METRICS=1
```

`ingest.sh` hardcodes this host's binary and work paths; edit them for another
host.

## Changelog

| Date | Change |
| --- | --- |
| 2026-09-24 | Measurements and model. `degrees.py` and `star.py` were cleaned for lint after they ran. The cleaned `degrees.py` reproduces the S18 and S20 records exactly, and the cleaned `star.py` reproduces the 4M star's Parquet byte for byte. |
