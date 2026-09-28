# Staged routing admission lanes (#1606)

This experiment compares the exact integrated `main` tree at `fc09f6a193e50ab6d10488b90206cfee2bf188b5` with one candidate binary. The base and candidate binaries and Graph500 S18/S20 input shards are identified in `source.json` and `source.sha256`. The full command receipts, BenchExec reports, host state and query digests are in `candidate-runs.tar.gz`; its SHA-256 is recorded in `source.sha256`. The human-readable aggregate is `candidate/summary.txt`.

**Disposition: no-go.** Across three alternating pairs, the S18 median ingest wall improved from 23.72 s to 22.07 s (6.9%), and S20 improved from 83.81 s to 78.27 s (6.6%); both miss the 10% adoption gate. The S18/eight-core route CPU/wall median was 1.2277, below 2.0. Reopened node/edge counts and full-scan digests matched across all A/B runs, and peak memory stayed below 4,000 MiB (maximum 2,950 MiB). No routing production change is adopted.

The measurement driver runs three alternating 16-core, 4,000 MiB S18 pairs and three S20 pairs, then the candidate S18 1/2/4/8/16-core curve, with three samples at 1 and 8 cores. It drops page cache and waits for 60 seconds of quiet before every ingest; after each run it requires the host to be quiet and reopens the project for node/edge counts and full scans. `summarize.py` validates the full run set and answer digests, then prints every paired whole-ingest result, region inclusive/residual wall and CPU, resource pressure, memory and the adoption decision.

The route region is `import_command/validate/seal/shaping/shape_routing`. Its parent capture measures process CPU across the coordinator and admitted workers, while wall spans worker joins. The coordinator still authenticates each Parquet receipt and runs row-group routing before launching fixed-width family work; it folds family evidence and completes every admitted task before sealing, flushing the directory batch, installing progress or retiring staged inputs.

For one receipt, node routing has two independent fixed families: identity plus node details. Edge routing has three: identity, edge details and endpoints. At eight usable CPUs, the instance policy admits three background construction lanes plus the caller, but this receipt has at most three useful tasks (two lanes and the identity coordinator). Node receipts lease at most one background lane; edge receipts request two and use at most two. No receipt is queued ahead.

The additional explicit routing buffers are bounded by three simultaneous 1 MiB `BufReader`s and three 64 KiB `PartitionRun`s (3,342,336 bytes total, excluding allocator overhead and existing partition-writer buffers). The existing cache-release policy assigns each of four stream windows at most 256 MiB, for the same 1 GiB aggregate cap. Up to three source descriptors can be open concurrently. Destination spill descriptors remain owned by the same four partitioners and are bounded by their populated partitions (at most `2 * joint_partitions + 2 * node_partitions`); routing adds no spill file, exchange file or next-receipt descriptor. BenchExec records observed process peak memory and I/O pressure for every run.

The idealized whole-ingest ceiling is computed from baseline S18/S20 route wall divided by complete-ingest wall, treating routing as if it vanished entirely: `1 / (1 - route_wall / ingest_wall)`. The ceilings were 1.216x at S18 and 1.199x at S20; they are bounds, not predictions of family-lane speedup. The candidate missed both performance thresholds despite passing correctness and memory checks. Do not extend this issue into another mechanism.

Reproduce on the quiet ext4 host with the captured binaries and inputs:

```sh
docs/development/evidence/routing-lanes-1606/measure.sh \
  /path/to/frozen-base-gf /path/to/candidate-gf \
  /home/ubuntu/gf-1448-evidence/inputs \
  docs/development/evidence/routing-lanes-1606/candidate

python3 docs/development/evidence/routing-lanes-1606/summarize.py \
  docs/development/evidence/routing-lanes-1606/candidate
```

To validate the archived run set first, extract it from the repository root with:

```sh
tar -xzf docs/development/evidence/routing-lanes-1606/candidate-runs.tar.gz
python3 docs/development/evidence/routing-lanes-1606/summarize.py \
  docs/development/evidence/routing-lanes-1606/candidate
```
