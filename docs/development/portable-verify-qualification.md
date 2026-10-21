# Portable verify qualification

How `gf portable verify --input -` is shown to verify a package of any size in
bounded memory with no temporary disk (#1777). This page records the method and
the identity of the inputs. The measurements themselves are attached to the
issue or pull request that produced them, not committed here.

## Scaled run

`crates/graphforge-cli/tests/portable_stream.rs`, Linux only, runs in the
normal test lane:

- The package is the fixture ontology-only package
  (`tests/fixtures/portable/ontology-only.manifest.json`) plus one `evidence`
  component holding a single `evidence` artifact of 5 GiB of zero bytes
  (SHA-256
  `7f06c62352aebd8125b2a1841e2b9e1ffcbed602f381c3dcb3200200e383d1d5`;
  reproduce with `head -c $((5 * 1024 ** 3)) /dev/zero | sha256sum`). The
  manifest, `package_digest`, inventories and tag manifest are rebuilt to
  describe it. The bundle is generated as a stream, one entry at a time; no
  copy exists on disk.
- A thread feeds that stream to the standard input of
  `sh -c 'ulimit -v 1048576; ulimit -f 65536; exec gf portable verify --input - --json'`
  with `TMPDIR` set to a fresh empty directory. The address-space limit (1 GiB)
  and the file-size limit (64 MiB) make a verifier that buffers the input or
  spools it to disk fail rather than merely measure badly.
- The test samples `VmHWM` of `/proc/<pid>/status` every 50 ms, keeps the
  maximum, and prints it as `peak_rss_bytes`.
- It asserts exit `0`, `integrity` `verified`, `bytes_by_kind.evidence` equal to
  5 GiB, the sum of `bytes_by_kind` equal to `payload_bytes`, `package_digest`
  equal to the digest the generator computed, `TMPDIR` still empty, and peak
  resident memory below 768 MiB.
- A second run cuts the stream at 60% (inside the artifact) and expects the
  stable `truncated payload` refusal, `GF_VALIDATION`, exit `2`. A third and
  fourth feed an impostor manifest and a corrupted artifact, refused with
  `GF_VALIDATION`, exit `2`. A fifth feeds a manifest larger than the retained
  control limit and expects `GF_LIMIT_EXCEEDED` in the JSON error with the
  CLI's general exit code.

## Mutation check

Read the whole input with `read_to_end` before verifying in
`crates/graphforge-cli/src/portable_cli.rs`. The scaled run must fail under the
address-space limit. Restore the stream call afterwards.

## Real run

The same command, fed by an object store stream in a container whose disk is
smaller than the object, is the issue's 100 GiB acceptance. Its stream digest,
`gf` commit, container limits and result are attached to #1777.
