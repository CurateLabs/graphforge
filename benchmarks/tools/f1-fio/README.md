# F1 — device ceiling vs. ingest access pattern (issue #1387, plan §2.2)

Three `fio` jobs on the md RAID1 (`/dev/md3`, ext4) that settle whether the
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
