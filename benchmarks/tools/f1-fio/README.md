# F1 — device ceiling vs. ingest access pattern (issue #1387, plan §2.2)

Historical `fio` probes on md RAID1 (`/dev/md3`, ext4) that investigated whether the
~360 MB/s the ladder harness achieves is the device's capacity or GraphForge's
access pattern. Run `./run-f1.sh` on a quiet host; it refuses otherwise.

| job | what it answers |
|---|---|
| `1-control-seq-direct.fio` | known-positive device ceiling: sequential, `--direct=1`, page cache bypassed |
| `2-pattern-faithful.fio` | the S20 ingest pattern: 63/37 read/write by bytes, ~51 KiB reads, ~43 KiB writes, one fsync per ~6.6 writes |
| `3-buffered-fsync.fio` | the same pattern through the page cache (`--direct=0`), closest to what GraphForge experiences |

Parameter derivation for job 2 is in `derive-params.py`, which reads the rung
JSON and prints the numbers the job file uses. Every parameter is traceable to
a named field in `storage_attribution.construction.application_io`.

## Preservation and future invocation

Imported from the historical measurement branch through `e6ddeb84`; see
[the operator contract and provenance](../perf-stat-validate/README.md).
Supply `QUIET_HELPER`, a new output directory and an existing dedicated data
parent directory. The runner creates its own unique child under that parent;
cleanup touches only that child. Host cache dropping requires explicit operator
invocation. No new measurements were made for #1539.

Historical `append-fresh` outputs used `time_based=1` with a 16 GiB aggregate
working set. Recorded write totals of 34,359,836,672 and 49,994,334,208 bytes
therefore include overwrites after the first pass. They are **not pure fresh
extent allocation measurements**. The preserved provenance and labels must be
read with that qualification. Future job 4 invocations use one bounded pass
(`time_based=0`, `loops=1`) over fresh files. This changes the experiment and
must not be presented as a rerun of the historical time-based result.
