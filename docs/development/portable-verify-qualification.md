# Portable verify qualification

How `gf portable verify --input -` is shown to verify a package of any size in
bounded memory, and what temporary disk it holds (#1777). This page records the
method and the identity of the inputs. The measurements themselves are attached
to the issue or pull request that produced them, not committed here.

## What is claimed, and where it is proven

| Property | Proven | By |
|---|---|---|
| Opaque `evidence` of any size is hashed as it streams and writes nothing | in-process | storage test |
| A graph Parquet entry over `max_parquet_walk_bytes` (64 MiB) is staged once, whole, in an anonymous file in `TMPDIR`, and removed; at most one such entry is on disk at a time; a research package stages one Version at a time | in-process | storage test |
| Memory is bounded by the limits, not by the package size | process | CLI test, `ulimit -v` and `ru_maxrss` over the whole run |
| No single file exceeds 64 MiB while verifying opaque evidence | process | CLI test, `ulimit -f` (SIGXFSZ) |
| Exit status, verdict, report and refusal codes of the real binary | process | CLI test |

Write volume is proven in-process because a process's own I/O counters cannot be
captured reliably from outside at exit: `/proc/<pid>/io` is unreadable once the
process has exited, even for an unreaped zombie, and a sampler misses whatever is
written after its last sample. In-process, the scan and its spill writes run
synchronously on the calling thread, so `wchar` in `/proc/thread-self/io` read
before and after the call is exact.

## In-process: writes

`crates/graphforge-storage/src/project_portable/plausibility_tests.rs`, Linux
only, calls `verify_portable_stream`, the call the CLI makes, with a scratch
directory, and reads `wchar` on the calling thread around it.

- **Opaque evidence, 5 GiB.** One `evidence` artifact of 5 GiB of zero bytes,
  generated as a stream by a `Read` and never held (SHA-256
  `7f06c62352aebd8125b2a1841e2b9e1ffcbed602f381c3dcb3200200e383d1d5`; reproduce
  with `head -c $((5 * 1024 ** 3)) /dev/zero | sha256sum`). Asserted: the whole
  stream was consumed; `integrity` `verified`; `bytes_by_kind.evidence` equal to
  5 GiB and the sum of `bytes_by_kind` equal to `payload_bytes`; writes under
  1 MiB; the scratch directory empty.
- **Graph entry just over a lowered threshold.** A `graph-data` component holding
  a small plain-encoded Parquet file of `n` bytes, with `max_parquet_walk_bytes`
  set to `n - 1`. Asserted: `integrity` `verified`; the page walk read the file
  (`files_with_footer` is 1); writes at least the threshold and at most `n`; the
  scratch directory empty afterwards. The same package under the default
  threshold writes less than the threshold.

## Process: memory, the per-file cap, exit and verdict

`crates/graphforge-cli/tests/portable_stream.rs`, Linux only. Each run spawns
`sh -c 'ulimit -v <KiB>; ulimit -f <512-byte blocks>; exec gf portable verify --input - --json'`
with `TMPDIR` set to a fresh empty directory. A thread feeds a generated stream
(one entry at a time; no copy on disk) to its standard input.

Right after spawn, while the child is blocked on the still-empty stdin, the test
reads `Max address space` and `Max file size` from `/proc/<pid>/limits`, retrying
briefly until the process is `gf` rather than the shell that set them, and
requires the read to succeed. They are asserted equal to those requested;
`ulimit -f` counts 512-byte blocks in `dash`, so the effective value is checked
rather than assumed. The feeder then runs freely.

Peak memory is the child's whole-life peak: after the child is reaped, the test
reads `ru_maxrss` from `getrusage(RUSAGE_CHILDREN)`. `exec` makes the shell the
verifier, so there is one child, and nextest runs each test in its own process,
so it is the only child the process reaps. Unlike a sample at end of input, it
includes the allocations of finishing the bundle, semantic validation and
serializing the report.

One deadline (120 s) covers feeding and completion of every run. On expiry the
child is killed and reaped, the feeder is joined (its write fails with `EPIPE`),
and the test fails with the bytes fed and the child's stderr, so a verifier that
stops reading or hangs after end of input cannot stall the lane.

- **5 GiB opaque evidence**, same package as above plus its rebuilt controls,
  under a 1 GiB address space and a 64 MiB file size. A verifier that buffered
  the input could not allocate it; one that spooled it to a single file would be
  killed by SIGXFSZ. Asserted: normal exit `0`; `integrity` `verified`;
  `bytes_by_kind.evidence` equal to 5 GiB; the sum of `bytes_by_kind` equal to
  `payload_bytes`; `package_digest` equal to the digest the generator computed;
  peak `ru_maxrss` below 768 MiB (printed as `ru_maxrss_bytes`); `TMPDIR` empty.
- **A 70 MiB graph Parquet entry** (64 rows of 1,150,000 bytes), under a 128 MiB
  file size so that one staged entry fits. Asserted: exit `0`, `verified`, the
  report's `graph-data` bytes, `files_with_footer` 1, the same memory bound,
  `TMPDIR` empty.
- **Refusals.** A stream cut at 60% (inside the artifact) is refused as
  `truncated payload`, `GF_VALIDATION`, exit `2`. An impostor manifest and a
  corrupted artifact are refused with `GF_VALIDATION`, exit `2`. A manifest over
  the retained control limit is refused with `GF_LIMIT_EXCEEDED` in the JSON error
  and the CLI's general exit code. A refused argument list under `--json` is one
  JSON error document on stderr with exit `2`.

The packages are the fixture ontology-only package
(`tests/fixtures/portable/ontology-only.manifest.json`) plus one component, with
the manifest, `package_digest`, inventories and tag manifest rebuilt to describe
it.

## Mutation checks

- Read the whole input with `read_to_end` before verifying in
  `crates/graphforge-cli/src/portable_cli.rs`: the CLI 5 GiB run must fail under
  the address-space limit.
- Make `reaches_a_decoder` in
  `crates/graphforge-storage/src/project_portable/authenticated_entries.rs`
  return `true` for `evidence`: the in-process 5 GiB test must fail on its write
  assertion.

Restore both afterwards.

## Real run

The same command, fed by an object store stream in a container whose disk is
smaller than the object, is the issue's 100 GiB acceptance. Its stream digest,
`gf` commit, container limits and result are attached to #1777.

### Reproducing it on one host

`--url` refuses loopback and private addresses, so a local run feeds the same
stream verifier through `--input -`. The generator is
`crates/graphforge-cli/examples/generate_large_portable_package.rs`. It streams a
valid graph bundle of at least `--size-bytes` to a file or standard output,
deterministically from `--seed`, holding one Parquet file at a time: a graph
tree of 46 MB graph Parquet files plus one of 74 MB, over the 64 MiB walk
threshold, so the staged-entry path runs. It prints `package_digest` to standard
error. The package builder is `crates/graphforge-cli/tests/support/portable_package.rs`,
shared with the stdin tests.

```bash
cargo build --release -p graphforge-cli --bin gf --example generate_large_portable_package
generate_large_portable_package --size-bytes 107374182400 --seed 1 --output package.gfpb

# File mode: the reference verdict.
/usr/bin/time -v gf --json portable verify --input package.gfpb --mode full > file.json

# Stream mode: 1 GiB of memory, 2 GiB of scratch, one cgroup holding only gf.
truncate -s 2G scratch.img && mkfs.ext4 -q scratch.img
sudo mount -o loop scratch.img tmp && sudo chown "$USER" tmp
cat package.gfpb | sudo systemd-run --scope --quiet -p MemoryMax=1G -p MemorySwapMax=0 \
  --uid="$USER" --setenv=TMPDIR="$PWD/tmp" \
  /usr/bin/time -v gf --json portable verify --input - --mode full > stream.json

# Same verdict: only `source` differs.
diff <(jq -S 'del(.source)' file.json) <(jq -S 'del(.source)' stream.json)
```

Sample `df` on the scratch volume while the stream run lasts for its peak usage,
and read `memory.peak` from the scope's cgroup before it exits. Attach both
reports, the peaks, the exit codes and wall times, and the `gf` commit to #1777.
