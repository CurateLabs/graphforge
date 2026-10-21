# Portable verify qualification

How `gf portable verify --input -` is shown to verify a package of any size in
bounded memory, and what temporary disk it holds (#1777). This page records the
method and the identity of the inputs. The measurements themselves are attached
to the issue or pull request that produced them, not committed here.

## What is claimed

- Memory is bounded by the resource limits, not by the package size.
- Opaque `evidence` artifacts are hashed as they stream and never touch disk.
- A graph Parquet entry larger than `max_parquet_walk_bytes` (64 MiB) is staged
  in an anonymous (unlinked) file in `TMPDIR` while it streams, so the page walk
  can seek in it. At any moment at most one such entry is on disk; temporary
  disk is bounded by the largest graph Parquet entry. A research package stages
  one Version at a time.

## Method

`crates/graphforge-cli/tests/portable_stream.rs`, Linux only, runs in the normal
test lane. Each run spawns
`sh -c 'ulimit -v <KiB>; ulimit -f <512-byte blocks>; exec gf portable verify --input - --json'`
with `TMPDIR` set to a fresh empty directory. A thread feeds a generated stream
to its standard input. The test samples the child every 50 ms and keeps:

- the maximum of `VmHWM` in `/proc/<pid>/status` (printed as `peak_rss_bytes`);
- `wchar` in `/proc/<pid>/io`, every byte the process passed to `write(2)` to a
  file or pipe (printed as `written_bytes`);
- the most bytes allocated at once to the open file descriptors under
  `/proc/<pid>/fd` whose targets read `(deleted)` (printed as `unlinked_bytes`).
  This is what listing `TMPDIR` after exit cannot see, because the scratch file
  has no name;
- `Max address space` and `Max file size` from `/proc/<pid>/limits`, asserted to
  equal the limits the run was given. `ulimit -f` counts 512-byte blocks in
  `dash`, so the effective value is checked rather than assumed.

The bundle is generated one entry at a time; no copy exists on disk. Both
packages are the fixture ontology-only package
(`tests/fixtures/portable/ontology-only.manifest.json`) plus one component, with
the manifest, `package_digest`, inventories and tag manifest rebuilt to describe
it.

### Opaque evidence, 5 GiB

One `evidence` artifact of 5 GiB of zero bytes (SHA-256
`7f06c62352aebd8125b2a1841e2b9e1ffcbed602f381c3dcb3200200e383d1d5`; reproduce
with `head -c $((5 * 1024 ** 3)) /dev/zero | sha256sum`), run under a 1 GiB
address space and a 64 MiB file size. Asserted: exit `0`; `integrity` `verified`;
`bytes_by_kind.evidence` equal to 5 GiB; the sum of `bytes_by_kind` equal to
`payload_bytes`; `package_digest` equal to the digest the generator computed;
`written_bytes` and `unlinked_bytes` each under 1 MiB; `TMPDIR` empty; peak
resident memory below 768 MiB.

### Graph Parquet entry over the walk threshold

One `graph-data` component holding a plain-encoded, uncompressed Parquet file
of 64 rows of 1,150,000 bytes (about 70 MiB, over the 64 MiB threshold), run
under a 1 GiB address space and a 128 MiB file size. The feeder pauses for
400 ms halfway through the entry so the sampler sees the open scratch file.
Asserted: exit `0`; `integrity` `verified`; `written_bytes` at least the entry
and at most the entry plus 1 MiB; `unlinked_bytes` above a quarter of the entry
and at most the entry plus 1 MiB; `TMPDIR` empty after exit.

### Refusals

A stream cut at 60% (inside the artifact) is refused as `truncated payload`,
`GF_VALIDATION`, exit `2`. An impostor manifest and a corrupted artifact are
refused with `GF_VALIDATION`, exit `2`. A manifest over the retained control
limit is refused with `GF_LIMIT_EXCEEDED` in the JSON error and the CLI's general
exit code. A refused argument list under `--json` is one JSON error document on
stderr with exit `2`.

## Mutation checks

- Read the whole input with `read_to_end` before verifying in
  `crates/graphforge-cli/src/portable_cli.rs`: the 5 GiB run must fail under the
  address-space limit.
- Make `reaches_a_decoder` in
  `crates/graphforge-storage/src/project_portable/authenticated_entries.rs`
  return `true` for `evidence`: the 5 GiB run must fail on `written_bytes`.

Restore both afterwards.

## Real run

The same command, fed by an object store stream in a container whose disk is
smaller than the object, is the issue's 100 GiB acceptance. Its stream digest,
`gf` commit, container limits and result are attached to #1777.
