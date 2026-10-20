---
title: "ADR 0021: Portable project v2 package layout and identity"
adr: "0021"
status: "Accepted"
date: "2026-08-19"
superseded_by: null
revisit_when: "A package needs full-memory buffering, unmanifested files, or capabilities the closed-world v2 schema and fixture corpus forbid; or the 2026-10-20 outer-compression amendment's trigger fires"
---

# ADR 0021: Portable project v2 package layout and identity

**Implementation:** Shipped across storage, API, CLI and both bindings
(#784, #827, #829, #849, #850). The legacy fully buffered envelope is
superseded and deleted (#1932). Primary `gf export`/`gf import` use the streaming
package and its canonical `/2` result contracts; no legacy reader, converter,
alias or deprecation path remains. Retired bytes fail with
`GF_UNSUPPORTED_PROJECT_FORMAT` before destination mutation. The manifest media
type remains an independently tracked wire-contract concern.
**Build target:** v0.5.x (M5 portable project v2)  
**Related:** #783, parent #740; consumers #741, #742, #784–#787, #744

## Context

Portable v1 is a fully buffered proprietary envelope. It cannot express a
billion-edge file-backed project without its 16 GiB envelope ceiling and does
not give independent implementations a standard inspectable representation or
a representation-independent identity.

The logical package must have an expanded form for inspection and a single,
incrementally writable bundle form. Identity must describe selected project
meaning, not host paths, archive metadata, or a particular transport.

## Options considered

1. **BagIt 1.0 profile plus a canonical bundle.** RFC 8493 supplies a simple,
   content-addressed expanded layout and payload/tag manifests. A narrow
   GraphForge profile can close BagIt's intentionally extensible surface while
   retaining standard tooling.
2. **OCI image layout.** OCI gives digest-addressed blobs and useful registry
   transport, but an image-layout directory is not an inspectable project
   package, permits descriptor graphs and annotations beyond this semantic
   model, and does not define deterministic layer bytes. OCI promotion remains
   an optional later mapping of the canonical bundle.
3. **Raw tar or ZIP.** Both are widely implemented, but neither supplies the
   semantic inventory, dependency closure, payload manifests, or closed-world
   profile. ZIP also has multiple filename/timestamp/extra-field encodings.
4. **New proprietary framing.** It could be streamable, but would duplicate
   mature archive parsing and security work and create another ecosystem-only
   envelope.

## Decision

Adopt the closed BagIt-compatible expanded profile and deterministic PAX/ustar
bundle defined by the normative
[portable project v2 specification](../book/architecture/portable-project-v2.md).
The expanded extension is `.gfproject/`; the uncompressed bundle extension is
`.gfpb` with media type `application/vnd.graphforge.project.v2+tar`. Compression
is deliberately absent in v2 so that bytes are reproducible and readers can
enforce limits while streaming.

The semantic manifest is closed JSON with media type
`application/vnd.graphforge.project.manifest.v2+json`, canonicalized using RFC
8785 JCS. Package identity is SHA-256 over a domain-separated preimage containing
the canonical semantic manifest after its self-referential `package_digest`
field is omitted. Component descriptors bind every logical payload by portable
path, byte length, SHA-256, kind, stable identity, dependencies, and selection
state. Expanded and bundled representations therefore have the same package
digest and inventory while retaining distinct transport digests.

## Consequences

- Export, verification, import, bindings, and optional OCI promotion consume one
  schema and fixture corpus rather than inventing format rules.
- Full-memory buffering is forbidden. Counts and per-entry lengths are bounded;
  payload bytes are hashed and copied incrementally.
- BagIt's general extensions are narrowed: only enumerated regular files are
  allowed, all payload and tag files are manifested, and extra files fail.
- Authenticity is not inferred from integrity. Signatures and trust policy are
  optional evidence over the package digest and belong to later issues.
- The legacy envelope is neither emitted nor readable. Unsupported formats and
  future semantics fail before project mutation.

## Amendment 2026-10-20: no outer compression (#1770)

**Decision:** v2 bundles stay uncompressed. No optional zstd transport
representation is specified. Issue #1770 requirement 6 asked whether an outer
zstd transport representation (its own transport digest, an unchanged package
digest, bounded streaming decode, `Range` on the stored compressed object)
would materially reduce bytes on the wire or in storage. Measured on realistic
large exports, it does not at any level a publisher can reasonably run.

**Decision rule, fixed before measuring:** recommend the representation only if
it saves at least 15% of bytes on the realistic large exports with a decode
throughput of at least 500 MB/s on one core. Otherwise record "no outer
compression" with the numbers.

**Result.** Savings are the fraction of bundle bytes removed by streaming
`zstd --single-thread`. Throughput is indicative only: the host was never
quiet (load average 8-17 from other agents' builds, so `require_quiet_host`
refused). Ratios do not depend on host load.

| Input class | Bundle bytes | zstd-1 | zstd-3 | zstd-9 | zstd-19 | zstd-3 `--long=27` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| LDBC SNB Interactive SF1 (3,181,724 nodes and 17,256,038 edges per LDBC's published bulk-load totals; string, date and list properties) | 2,074,354,688 | 7.35% | 11.66% | 16.18% | 17.67% | 13.05% |
| Graph500 SCALE 20 (1.05M nodes, 16.8M edges, no properties) | 765,299,200 | 11.95% | 13.94% | 14.55% | 15.34% | 14.54% |
| Research package fixture `tests/fixtures/research-v6` | 119,296 | 87.85% | 88.24% | 89.13% | 89.64% | 88.32% |
| OpenAlex Hub fixture (metadata only) | 21,504 | 83.59% | 83.90% | 84.74% | 84.97% | 83.95% |

The two fixtures are not evidence for large packages: 37-52% of their bytes
are 512-byte tar block padding and the rest is JSON and a few small Parquet
files. They are listed for completeness and carry no weight in the decision.

Throughput on the two large inputs (MB/s of bundle bytes, one core, indicative):

| Input | Level | Compress | Decompress | Decode peak RSS |
| --- | --- | ---: | ---: | ---: |
| SF1 | 1 / 3 / 9 / 19 | 485 / 180 / 55 / 2 | 3,143 / 2,074 / 1,321 / 1,392 | 6-14 MiB |
| SF1 | 3 `--long=27` | 156 | 2,593 | 133 MiB |
| SCALE 20 | 1 / 3 / 9 / 19 | 1,367 / 781 / 157 / 3 | 5,887 / 5,466 / 4,252 / 3,189 | 5-13 MiB |
| SCALE 20 | 3 `--long=27` | 264 | 2,943 | 133 MiB |

SCALE 20 at levels 12 and 15 saves 14.52% and 14.53%; only level 19 passes
15%. Level 19 with 8 threads took 62 s for the 765 MB bundle (about 12 MB/s)
on the loaded host.

**Why the savings are small.** Per-member `zstd -3` over each bundle's files,
grouped by component kind and extension:

| Bundle | Member kind | Share of bundle | Saved |
| --- | --- | ---: | ---: |
| SF1 | graph Parquet (`.parquet`) | 62.1% | 11.1% |
| SF1 | adjacency CSR (`.csr`) | 21.9% | 2.4% |
| SF1 | UUID index, edge keys and edge queues (`.uuidx`, `.epk`, `.eqs`) | 15.9% | 15.7-29.5% |
| SF1 | control JSON and tag manifests | 0.1% | 16-84% |
| SCALE 20 | adjacency CSR (`.csr`) | 56.0% | 0.0% |
| SCALE 20 | graph Parquet (`.parquet`) | 38.5% | 17.8% |
| SCALE 20 | UUID index (`.uuidx`, mostly zero extents) | 5.5% | 97.8% |
| SCALE 20 | control JSON, tag manifests | 0.0% | 16-85% |

The payloads are already zstd-compressed (the CSR files are written with zstd,
and the shared permanent Parquet writer uses zstd level 1), so an outer layer
recompresses compressed bytes. The Parquet saving is consistent with a higher
zstd level beating level 1 on the same pages; that attribution was not
measured, and raising the writer level is a product change outside this
amendment.
Control JSON and tag manifests compress well but are under 0.2% of a large
bundle. SCALE 20's total saving is 6.9 points from Parquet plus 5.4 points from
the sparse UUID index; the CSR files, over half the bundle, save nothing.
Tar overhead is 0.06% (SF1) and 0.04% (SCALE 20), so the 512-byte block
padding is not the lever either. Evidence (Source and Artifact) payloads were
absent from every large input, so their compressibility was not measured.

**Applying the rule.** Level 3, the level whose encode cost is small, saves
11.66% on SF1 and 13.94% on SCALE 20: below 15% on both. Level 9 saves 16.18% on
SF1 but 14.55% on SCALE 20, and levels 12 and 15, measured only on SCALE 20, save
14.52% and 14.53% there: all below 15% on SCALE 20. Level 19 is the only level
that clears 15% on both (17.67% and 15.34%), with decode well above 500 MB/s.
Level 19 meets the pre-stated rule. This decision overrides it because the rule
bounded decode throughput and did not bound encode cost, and level 19 encodes
at 2-3 MB/s single-threaded: roughly 10-15 hours for a 100 GiB package (the
size cap of #1770), and about 12 MB/s with 8 threads on this host. The SCALE 20
margin at level 19 is 0.34 points, on a synthetic input without properties.
The numbers above let the override be revisited.

**What the decision keeps.** One transport digest per representation, a
package digest that does not depend on transport, and a decoder with no
window or buffer beyond the tar reader's own. A bounded-memory zstd decode
would have had to forbid `--long` windows (133 MiB decode RSS against 5-14 MiB
at default windows). `Range` on an outer-compressed object resumes the
compressed stream only; it gives no random access into the bundle.

**Method.** One host (AMD Ryzen 7 3800X, 16 threads, zstd 1.5.7) and one
`gf` built from main `d65a1cc9f8ba`. Run from a checkout of that commit; each
input gets its own directory and project path.

```bash
export CARGO_BUILD_JOBS=3 CARGO_INCREMENTAL=0
CARGO_TARGET_DIR=$PWD/../gf-target cargo build --locked --release -p graphforge-cli
CARGO_TARGET_DIR=$PWD/../gf-target-bench cargo build --locked --release \
  --manifest-path benchmarks/Cargo.toml \
  --bin graphforge-benchmark-gdc-scorecard --bin graphforge-benchmark-graph500-generator
GF=$PWD/../gf-target/release/gf
BENCH=$PWD/../gf-target-bench/release
W=$PWD/../gf-1770-work; mkdir -p $W

# load_project PROJECT PARQUET_DIR: stage every nodes/*.parquet, then every
# edges/*.parquet (nodes first), validate, commit.
load_project() {
  mkdir -p "$1"
  S=$($GF --json --project "$1" import-session begin --operation-uuid "$(uuidgen)" \
      | python3 -I -c 'import sys,json;print(json.load(sys.stdin)["session_uuid"])')
  for kind in nodes edges; do
    for f in $(ls "$2/$kind"/*.parquet | sort); do
      $GF --json --project "$1" import-session register-parquet \
        --session-uuid "$S" --path "$f" --kind "$kind" >/dev/null || return 1
    done
  done
  $GF --json --project "$1" import-session validate --session-uuid "$S" >/dev/null || return 1
  $GF --json --project "$1" import-session commit   --session-uuid "$S"   # expect "outcome":"committed"
}

# SF1: pinned archive -> converter -> project -> bundle
mkdir -p $W/dl $W/sf1-src
curl -fL -o $W/dl/sf1.tar.zst \
  https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf1-CsvComposite-LongDateFormatter.tar.zst
echo "2e3dfb9e3695592161a62ce893b9619c2566775512b7c1995c5a627277c2fb29  $W/dl/sf1.tar.zst" | sha256sum -c -
zstd -dc $W/dl/sf1.tar.zst | tar -x -C $W/sf1-src
$BENCH/graphforge-benchmark-gdc-scorecard convert --input-root $W/sf1-src \
  --mapping benchmarks/profiles/gdc/snb-interactive-load-mapping.json --output-dir $W/sf1-conv
load_project $W/sf1-project $W/sf1-conv
$GF --json --project $W/sf1-project export --current --output $W/sf1.gfpb

# Graph500 SCALE 20: generator -> project -> bundle
mkdir -p $W/g500-gen/nodes $W/g500-gen/edges
$BENCH/graphforge-benchmark-graph500-generator --scale 20 --edge-factor 16 \
  --seed 13907095936298285200 \
  --nodes $W/g500-gen/nodes/nodes.parquet --edges $W/g500-gen/edges/edges.parquet
load_project $W/g500-project $W/g500-gen
$GF --json --project $W/g500-project export --current --output $W/g500-s20.gfpb

# Fixtures, measured in place at these repository paths
#   tests/fixtures/research-v6/package.gfpb
#   tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb

# Every level in the table, on each of the four bundles (BUNDLE = path)
for L in 1 3 9 19; do
  /usr/bin/time -f '%e s %M KiB' zstd -q --single-thread -$L -c BUNDLE > c.zst; wc -c < c.zst
  /usr/bin/time -f '%e s %M KiB' zstd -q -d --single-thread -c c.zst > /dev/null
done
/usr/bin/time -f '%e s %M KiB' zstd -q --single-thread -3 --long=27 -c BUNDLE > c.zst; wc -c < c.zst
/usr/bin/time -f '%e s %M KiB' zstd -q -d --single-thread --long=27 -c c.zst > /dev/null
# SCALE 20 only (sizes only; run without --single-thread, which does not change the ratio)
zstd -q -12 -c g500-s20.gfpb | wc -c
zstd -q -15 -c g500-s20.gfpb | wc -c
/usr/bin/time -f '%e s %M KiB' zstd -q -19 -T8 -c g500-s20.gfpb | wc -c   # 8-thread level 19
```

Saved fraction is `1 - compressed/bundle_bytes`; throughput is bundle bytes over
wall time. Per-kind compressibility, run on SF1 and SCALE 20:

```bash
python3 -I - BUNDLE <<'PY'
import collections, os, subprocess, sys, tarfile
agg = collections.defaultdict(lambda: [0, 0, 0])   # key -> [files, raw, zstd-3]
with tarfile.open(sys.argv[1], "r:") as tf:
    for m in tf:
        if not m.isfile():
            continue
        parts = m.name.split("/")
        kind = (parts[2] if parts[:2] == ["data", "components"] and len(parts) > 2
                else "data-control" if parts[0] == "data" else "bagit-tagfiles")
        key = kind + (os.path.splitext(m.name)[1] or "(none)")
        packed = subprocess.run(["zstd", "-q", "-3", "--single-thread", "-c"],
                                input=tf.extractfile(m).read(), capture_output=True,
                                check=True).stdout
        a = agg[key]; a[0] += 1; a[1] += m.size; a[2] += len(packed)
total = os.path.getsize(sys.argv[1])
for key, (n, raw, z) in sorted(agg.items(), key=lambda kv: -kv[1][1]):
    print(key, n, raw, z, f"saved={100 * (1 - z / raw):.2f}% share={100 * raw / total:.2f}%")
PY
```

(`tf.extractfile(m).read()` holds one member in memory; the largest member in
these bundles is far smaller than the host's RAM.)

| Input | Bundle sha256 (transport digest) | Package digest |
| --- | --- | --- |
| SF1 | `2fc848027e92c6f6a0cac539290ca2308558e9545d97ced10cd52ac48f34d03b` | `sha256:69a3dd3214b2895302958bfa6196dab7e21bed4ba6488b302a3f562b6dbdb78e` |
| SCALE 20 | `486ad50e17a3dc68b49d04d54eea81997aebe9d1afa00e2b5d7c0b9a4548e98c` | `sha256:961828bf725389143cb82980d750eae6e76aa6a5c540aac5d754abc705db234c` |
| research-v6 fixture | `a5a3c102bdad3a27e1212e615a8609eefc09821bd04e3c4bd37854655e85668b` | `sha256:b06489087e61a3cae78a440dbcf360e399c26cb04401f8c5363a81536f442ea3` |
| OpenAlex fixture | `52cbffc43ab2bd081aefa297970a4f060fb52b8a8ab601b77e2bca9ca6dda54d` | `sha256:492188fb9c3ac96b06b5373f571b47b6c6fc703c12800da1d02585001203fb35` |

Raw per-run output is on issue #1770, not in the repository. The SF1 archive sha256 is `2e3dfb9e3695592161a62ce893b9619c2566775512b7c1995c5a627277c2fb29` (it matches the pin in `benchmarks/profiles/gdc/snb-interactive-scorecard-identity.json`).

**revisit_when:** the payload encodings change (a different Parquet or CSR
codec or level, uncompressed payloads, or bundles that carry evidence), or a
measurement on a deployed Hub shows at least 15% savings at an encode
throughput of at least 100 MB/s and a decode throughput of at least 500 MB/s on
a realistic export of 100 GiB or more.

## Required verification

The machine-readable schema and golden corpus under
`tests/fixtures/portable-v2/` are normative. Their validator must cover every
package class, expanded/bundle equivalence, JCS bytes, bundle ordering and
headers, hostile paths and entries, truncation, closed-world inventory,
limits, v1 disposition, and future-version/capability rejection. A structural
billion-edge vector proves streaming bounds without committing the dataset.

## References

- [RFC 8493: The BagIt File Packaging Format (V1.0)](https://www.rfc-editor.org/rfc/rfc8493)
- [RFC 8785: JSON Canonicalization Scheme](https://www.rfc-editor.org/rfc/rfc8785)
- [OCI Image Layout](https://github.com/opencontainers/image-spec/blob/main/image-layout.md)
- [POSIX pax archive format](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/pax.html)
