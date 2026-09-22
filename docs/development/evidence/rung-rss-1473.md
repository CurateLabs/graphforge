# The g500-ladder rung peak is page cache, not engine memory (#1473)

Two complete, passed S18 rungs on the same source (`4f0604dd`), binaries, host,
and work root, with a cgroup-v2 sampler attached to the delegated benchexec
slice (diagnostic: `benchmarks/diagnostics/rss_1473_cgroup.py`, sanitized
numbers in [rung-rss-1473.json](rung-rss-1473.json)).

## What the two RSS figures are

| Quantity | Run 1 | Run 2 | Authority |
|---|---:|---:|---|
| Rung-level peak (BenchExec `memory` column) | 759.0 MB | 754.8 MB | cgroup memory usage of the tool scope |
| Tool scope `memory.peak` (kernel) | — | 761.0 MB | kernel high-water |
| — of which `file` (page cache) at peak | — | 568.0 MB (74.6%) | sampled at the peak |
| — of which `anon` at peak | — | 166.6 MB (21.9%) | sampled at the peak |
| Process peak (certify phase VmHWM max) | 246.4 MB | 239.8 MB | `/proc/<pid>/status` VmHWM |

The rung-level figure and the process figure are different resources. At the
tool scope's peak, page cache outnumbers anonymous memory 3.41:1. The cgroup
peak reproduces within 0.6% across the two runs while tracking the rung's
32.3 GB of physical I/O, and the process VmHWM stays 3.2x below it — the
engine holds a bounded working set while the kernel caches bytes moved.

## Verdict

The hypothesis in #1473 is confirmed. The rung-level peak RSS that motivated
the issue is cgroup memory usage, dominated by page cache; it grows with bytes
moved and no engine change can reduce it short of moving fewer bytes, which is
#1194's mandate. The per-phase VmHWM observations show the bounded-memory
property with ingest RSS per edge falling as scale grows. Since the #1278
repair, ladder admission consumes the process VmHWM authority and the separate
BenchExec kill ceiling accommodates the page cache, so no gate or engine
change is indicated by this measurement; this document and the committed
diagnostic are the record.

## Method notes

- Each ladder attempt ran inside `systemd-run --user --scope -p Delegate=yes`
  under a shared `benchexec.slice`; BenchExec (with pystemd) runs the tool in
  its own child scope, which the sampler records per child before systemd
  destroys the inactive unit.
- `memory.stat` in cgroup v2 is hierarchical; child-scope figures separate the
  tool's cache from the launcher's. `memory.peak` is kernel-authoritative and
  survives unit teardown; it was also captured by sampling.
- Raw sample streams stay local; the committed JSON carries byte counters,
  digests, and derived ratios only.
