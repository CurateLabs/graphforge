# External processing of over-budget partitions (#1585)

Evidence for decision 1 of [ADR 0047](../../adr/0047-over-budget-partitions-and-instance-cpu-budget.md).
It covers the production implementation that #1585's comparison selected
([external-partition-comparison-1585.md](external-partition-comparison-1585.md)).
A partition without a detail codec whose materialization would exceed
`max_partition_bytes` is sorted into checksummed runs and merged, instead of
refusing the ingest.

It answers four questions:

1. Does an ingest that main refuses now publish at the default budgets?
2. Does it publish the same graph the resident path publishes?
3. Is Graph500, where no partition exceeds the budget, unchanged?
4. Does the change cost anything on Graph500?

Raw receipts, `time -v` and `runexec` records, scripts and hashes are in
[`external-partitions-1585/`](external-partitions-1585/). Measured on
OVHC-AGENCY on 2026-09-25.

## Binaries and inputs

| Label | Build | `gf` SHA-256 |
| --- | --- | --- |
| main | production, main at `cca7a1fc` | `b07bce4d…09e8da` |
| branch | production, this branch at `fb8a3348` | `f5dda609…6e623a` |
| branch-ts | `test-support` build of `fb8a3348`, which reads the `GF_SHAPE_MAX_*` budget overrides | `000d7c4d…552ae0` |
| branch (first A/B) | production, this branch at `e0168612`, before the fix below | `a167e8e6…84d505` |

The production branch binary contains neither override string. The
`test-support` build is used only to record a non-default budget: a larger
resident budget for the resident comparison, and a zero or small external bound
for the refusal controls. The PR head adds one commit after `fb8a3348`, a
refactor that reuses an existing helper for the same computation.

Inputs are the #1448 Graph500 S18 and S20 files (edge factor 16, the ladder
seed) and the #1584 star graphs (`star.py L`: `L` leaves, each with one edge to
a single hub). The 9M star's hashes match `partition-refusal-1584/star-inputs.sha256`.
The 20M star was generated with `star.py 20000000` for #1585's comparison.
Every input hash is in `manifest.txt` and `ab*/host-state.txt`.

## Answers are compared by query digest

Each query receipt carries `result_sha256`, which is stable across fresh
projects. Published file bytes are not a cross-run signal. Two ingests of S18
by the **same** binary share only 53 of 329 content-addressed objects in
`graph-objects/sha256`, and at S20 only 161 of 1,359 (`ab*/summary.txt`). The
rest carry per-run identity. Main versus branch differs by exactly that same
number of objects, so the object store cannot distinguish them either way.

Two checks replace it:

- **Query answers.** Node count, a node scan, edge count and an edge scan, plus
  the hub's degree for the stars. The scans are unordered on purpose: they
  follow the published layout, so equal digests over every row also mean equal
  storage order. An `ORDER BY` over 9M rows exhausts the query engine's sort
  pool, which is a separate defect (#1591).
- **Shaped and encoded bytes.** The determinism suite's
  `same_input_twice_produces_identical_digests` prints the digests of 5 shaped
  and 28 encoded artifacts (`DETERMINISM_SAME_INPUT`). Its output at this
  branch and at its base `c6d17945` is byte-identical (`cmp`, 4,836 bytes
  each). That fixture has no over-budget partition.

## Stars and refusal controls

`run_all.sh` runs each case through `ingest.sh`: the ladder profile's five
`import-session` commands into a fresh project, then five queries. External
counts come from the commit receipt's `construction` object. RSS is the
validate step's maximum resident set from `time -v`; validate is where shaping
runs. Summary lines: `results.txt`.

| Case | Binary | Budget | Outcome | External partitions / runs / bytes | Validate RSS |
| --- | --- | --- | --- | --- | ---: |
| g500-s18-main | main | default | Published | not reported | 249,744 KiB |
| g500-s18-branch | branch | default | Published | 0 / 0 / 0 | 242,672 KiB |
| star9m-main | main | default | **Refused**: `partition materialization requires 297536415 bytes, exceeds recorded budget 268435456` | — | 207,916 KiB |
| star9m-branch | branch | default | Published | 1 / 2 / 297,536,415 | 444,216 KiB |
| star9m-resident | branch-ts | resident 512 MiB | Published | 0 / 0 / 0 | 491,960 KiB |
| star9m-zero-bound | branch-ts | external 0 | **Refused**: `partition materialization requires 297536415 bytes, exceeds recorded budget 268435456` | — | 187,856 KiB |
| star9m-small-bound | branch-ts | external 256 MiB | **Refused**: `partition requires 297536415 bytes of external scratch, exceeds recorded external budget 268435456` | — | 194,136 KiB |
| star20m-branch | branch | default | Published | 1 / 3 / 660,536,415 | 445,176 KiB |
| star20m-resident | branch-ts | resident 1 GiB | Published | 0 / 0 / 0 | 845,272 KiB |

Query digests (`result_sha256`, first 12 hex digits; counts in parentheses):

| Graph | Cases | nodes | node scan | edges | hub | edge scan |
| --- | --- | --- | --- | --- | --- | --- |
| Graph500 S18 | main, branch | `3943ce07cba5` (262,144) | `a4759b63cc96` | `2cb98d649303` (4,194,304) | `bdefb318470e` | `82a56ff970f9` |
| 9M star | external, resident | `cd65e79200ab` (9,000,001) | `51265e3a6ec4` | `a5803634ea2e` (9,000,000) | `c8f2b956fa05` | `b0b42cc50c21` |
| 20M star | external, resident | `bf901a86f9ee` (20,000,001) | `fa5d67d58d82` | `e441de2e222b` (20,000,000) | `a34145f266e8` | `f4d24e1e4444` |

Each pair is identical on all five queries. The edge scan covers every edge,
so it includes the whole hub neighbourhood. The 9M edge-count digest also
equals the one #1584 recorded for the #1507 hybrid runs (`a5803634…`).

No case left a run temporary (`.artifact-xrun-*`) in its project, whether it
published or refused.

## Graph500 A/B

`ab.sh` ingests S18 and S20 three times with each of main and the branch, in
the order AB, BA, AB. It drops caches before each timed ingest and waits for a
sustained quiet host: the quiet-host guard plus no `gf`, generator, test-binary
or `runexec` process for 60 seconds. `runexec` records wall and CPU time. After
each timed ingest, untimed, it records the CAS digest list and the four
Graph500 queries. Every run's `driver.log` line reads `after=QUIET`.
`summarize_ab.py` produces `ab*/summary.txt`.

**First A/B (`ab/`, branch `e0168612`).** Answers were identical in all twelve
runs, but the branch was slower in all six pairs:

| Scale | Main median wall | Branch median wall | Per-pair branch − main |
| --- | ---: | ---: | --- |
| S18 | 31.44 s | 31.67 s | +0.05, +0.24, +0.46 s |
| S20 | 127.58 s | 128.20 s | +0.44, +1.07, +1.80 s |

The cause was in the branch. Before each codec-free partition load, the resident
fit check opened every sealed segment to sum its length, even when the routed
record count was known, and then used that count. `fb8a3348` decides from the
routed count without I/O.

**Second A/B (`ab2/`, branch `fb8a3348`).**

| Scale | Main walls | Branch walls | Main median | Branch median | Per-pair branch − main |
| --- | --- | --- | ---: | ---: | --- |
| S18 | 31.39, 31.61, 31.62 s | 31.22, 31.45, 31.72 s | 31.61 s | 31.45 s | −0.17, −0.16, +0.09 s |
| S20 | 127.96, 126.70, 126.24 s | 126.59, 127.25, 126.78 s | 126.70 s | 126.78 s | −1.37, +0.54, +0.54 s |

The per-pair differences change sign and lie inside each binary's own spread.
No cost is distinguishable at three runs per arm. Answers are again identical
in all twelve runs.

## What this shows

1. **Main refuses, the branch publishes.** The 9M star is refused on main at
   the default budgets. On the branch it publishes 9,000,000 edges at the same
   budgets. The 20M star publishes too.
2. **Same graph.** For both stars, the external path and a resident path given
   enough budget return the same node count, node scan, edge count, hub degree
   and edge scan.
3. **Graph500 unchanged.** At S18 and S20 the branch reports no external
   partition and returns main's answers. The determinism fixture's shaped and
   encoded bytes are identical.
4. **No measurable cost** on Graph500 after the fix, at S18 or S20.
5. **The scratch bound.** Run bytes equal the refused materialization,
   297,536,415 bytes for 9M. That is 9,016,255 records of 33 bytes: the hub's
   9,000,000 plus its range's share. Runs are at most `max_partition_bytes`, so
   9M needs 2 runs and 20M needs 3. An external bound below a partition's
   scratch refuses before any run is written. A zero bound restores main's
   refusal.
6. **Memory stays bounded.** External validate RSS is 444 MB at 9M and 445 MB
   at 20M. The resident path needs 845 MB at 20M because it materializes the
   whole 660 MB partition. On the external path, memory is set by the run size,
   not the hub.

Star wall times were not measured under the quiet-host protocol and are not
reported.

## Tests

Deterministic coverage in `graphforge-storage`:

- `graph_construction::external_partition::tests` covers:
  - output: the external output equals the resident output, and the merge
    emits sorted records;
  - bounds: a zero bound refuses, and a partition over the external bound
    refuses;
  - integrity: a mutated run fails its checksum, a truncated run fails its
    length check, and a routed-count mismatch refuses;
  - cleanup: a failed run write (standing in for a full disk) leaves no run, a
    stopped sort leaves no run, a merge stopped by its consumer removes every
    run, and `Drop` removes every run;
  - recorded budget: a legacy checkpoint resumes under its refusal, and an
    external bound below the resident budget is invalid;
  - recovery: it recognizes run temporaries and nothing else.

  Five mutations of the implementation each fail at least one of these tests:
  checksum, `Drop`, the stop poll, the external bound and the length check.
- `graph_construction::tests::determinism::external_partitions_publish_the_unconstrained_answer`
  runs a hub fixture in a fresh process. It checks the zero-bound refusal and
  compares the external path with an unconstrained control fingerprint.
- `graph_construction::tests::determinism::an_interrupted_external_partition_resumes_to_the_same_answer`
  ends the process at `shape.external_run.after_write`, with a run on disk, and
  at `shape.partition_output.after_install`. Each resume publishes the control
  fingerprint, and no run temporary survives. This test found that recovery did
  not reclaim run temporaries; that is fixed.
- `graph_construction::tests::determinism::a_cancelled_external_partition_leaves_no_run_and_retries`
  cancels the shape at the first poll that finds a run on disk and asserts that
  it fired. Nothing is published and no run remains. A retry publishes the
  control fingerprint.

## Reproduce

```bash
# TMPDIR must be on ext4; the scripts name the frozen binaries and inputs.
docs/development/evidence/external-partitions-1585/run_all.sh
OUT=/path/ab2 docs/development/evidence/external-partitions-1585/ab.sh
python3 docs/development/evidence/external-partitions-1585/summarize_ab.py /path/ab2
```
