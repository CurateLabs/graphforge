# Provenance of the pattern-faithful fio parameters

Every fio parameter in `2-pattern-faithful.fio`, `3-buffered-fsync.fio` and
`4-append-fresh-fsync.fio` is computed by `derive-params.py` from one file:

    /home/ubuntu/graphforge-ladder/s18-s22-93c041df-evidence/s20-rung.json
    (rung S20, commit 93c041df9a788b1603844114ccd7223cef6082ab, live_edges = 16,777,216)

That file is local to the GraphForge host. The fields used are copied verbatim
into `rung-application-io-extract.json` beside this document, and quoted below.
`clean-f80f69fe-evidence/s20-rung.json` (commit f80f69fe, a later run) carries
byte-identical `application_io` counters, so the choice of evidence set does not
affect any parameter.

## Fields read (`storage_attribution.construction.application_io.totals`)

| field | value |
|---|---:|
| `read_bytes` | 20,134,730,473 |
| `read_calls` | 384,923 |
| `write_bytes` | 11,870,432,160 |
| `write_calls` | 272,323 |
| `fsync_calls` | 41,070 |
| `object_count` | 272 |

## Parameters derived from them

| fio parameter | derivation | value |
|---|---|---:|
| `rwmixread` | `read_calls / (read_calls + write_calls)` = 384,923 / 657,246 | 58.6 % → **59** |
| (check) read share by bytes | `read_bytes / (read_bytes + write_bytes)` | 62.9 % |
| `fsync` (writes per fsync) | `write_calls / fsync_calls` = 272,323 / 41,070 | 6.63 → **7** |
| (check) bytes per fsync | `write_bytes / fsync_calls` | 282 KiB (S22: 300 KiB, S24: 299 KiB) |
| mean read call | `read_bytes / read_calls` | 51.1 KiB |
| mean write call | `write_bytes / write_calls` | 42.6 KiB |

fio's `rwmixread` is a per-I/O percentage, so the call-count share (59) is the
right input; with the block-size split below it yields ≈ 63 % of bytes as reads,
matching the byte share. `fsync=7` rounds 6.63 up; 7 × 42.6 KiB = 298 KiB per
fsync, which is the S22/S24 value (300 KiB) rather than the S20 value (282 KiB).

## Block-size split (`application_io.phases.<phase>`)

`bssplit` takes one bucket per construction phase: the phase's mean call size
(`read_bytes / read_calls`, rounded to 4 KiB for O_DIRECT alignment) weighted by
its share of calls. Phases with zero calls or a weight that rounds to 0 % are
omitted (`hydration_verification` 1,291 reads, `publication_preauthentication` 3
reads); their weight goes to the largest bucket.

Reads (weight = `read_calls` / 384,923):

| phase | `read_bytes` | `read_calls` | mean | bucket |
|---|---:|---:|---:|---|
| `shape_consume_reauthentication` | 12,265,167,561 | 195,524 | 61.3 KiB | 60k / 52 % |
| `encode_write_postwrite_authentication` | 4,395,299,283 | 162,835 | 26.4 KiB | 28k / 42 % |
| `recovery_reauthentication` | 2,710,915,685 | 13,345 | 198.4 KiB | 200k / 3 % |
| `cas_install_read_write` | 679,176,194 | 11,925 | 55.6 KiB | 56k / 3 % |

Writes (weight = `write_calls` / 272,323):

| phase | `write_bytes` | `write_calls` | mean | bucket |
|---|---:|---:|---:|---|
| `shape_consume_reauthentication` | 7,225,487,178 | 211,379 | 33.4 KiB | 32k / 78 % |
| `encode_write_postwrite_authentication` | 798,035,338 | 40,994 | 19.0 KiB | 20k / 15 % |
| `cas_install_read_write` | 678,497,696 | 11,289 | 58.7 KiB | 60k / 4 % |
| `append_merge` | 3,126,373,984 | 8,016 | 380.9 KiB | 380k / 3 % |

(Byte counts are copied from the extract; means and weights are as printed by
`derive-params.py`.)

Resulting `bssplit=60k/52/28k/42/200k/3/56k/3,32k/78/20k/15/60k/4/380k/3`.

The fsyncs themselves are attributed 37,088 to the `fsync_synchronization`
phase, 3,360 to `cas_install_read_write`, 606 to `encode_write_postwrite_authentication`
and 16 to `hydration_verification`; the job uses the rung total (41,070).

## What the evidence does not record, and what the jobs assume instead

- **Offset pattern.** `application_io` records calls and bytes, not offsets.
  GraphForge writes each object once, appending, and streams it back, so the
  jobs use `rw=readwrite` (sequential within each file). Random offsets would
  be *less* favourable to the device; if the sequential jobs are slow, random
  ones would not be faster.
- **Concurrency.** Not recorded. Jobs run at `numjobs` = 1, 4 and 16 and all
  three are reported.
- **Extent allocation.** Jobs 2 and 3 overwrite pre-laid files, so their fsyncs
  do not commit fresh extents; GraphForge's do. Job 4 (append into files that
  do not exist yet, `create_on_open=1`, `fallocate=none`) isolates that cost.
- **Working set.** 160 GiB per concurrency level for jobs 2/3 on a host with
  125 GiB RAM (102 GiB in buff/cache at the start), page cache dropped
  (`echo 3 > drop_caches`) before every job. Job 4 uses 16 GiB because it is a
  write-only job and dirty pages cannot accumulate past the next fsync.
