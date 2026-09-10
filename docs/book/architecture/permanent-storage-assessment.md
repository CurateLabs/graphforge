# Permanent storage assessment (#1196)

This assessment measures one authoritative published graph. Construction inputs,
private staging, the portable package and the clean import are separate owners.
It uses `GraphForge::storage_attribution`, including physical-object deduplication,
and executes exact UUID, endpoint, label, relationship and nullable-property
queries after reopen and after export → full verification → clean import.

## Reproduction

The fixture is `crates/graphforge-api/tests/permanent_storage_budgets.rs`.
The integrated runtime source is `83a153f164faa2f7dc276e593719ec71e7e0bc9e`,
based on merged lifecycle main `0539c8eed0be6c5de9b5a7ef83b265e5f3dc0f79`.
All five tests passed on OVHC-AGENCY, Linux 7.0.0-30, process-root ext4.
Rust is 1.96.0 (`ac68faa20`), Arrow/Parquet 58.4.0.
[Raw aggregate evidence](../../development/evidence/permanent-storage-1196.json)
records each numerator, denominator and semantic fingerprint. The standalone
serial test binary took 449.10 seconds; `/usr/bin/time -v` measured 268,780 KiB
maximum RSS, 397.56 user seconds and 31.65 system seconds. These include the
entire construction/query/export/import assessment, not codec-only memory or CPU.
The binary was built with `cargo test ... --no-run` before timing, so compilation
is excluded. The initial pre-lifecycle baseline reproduced the same permanent
bytes and exact semantic fingerprints.
Run on an admitted native filesystem with 4096-byte allocation blocks:

```sh
CARGO_TARGET_DIR=/path/to/isolated-target TMPDIR=/path/on/native-root \
  cargo test -p graphforge-api --test permanent_storage_budgets -- --nocapture --test-threads=1
```

All fixtures contain 65,537 edges. Sequential IDs use an explicit domain byte
and increasing integer; random IDs use all 128 bits from a deterministic SHA-256
prefix, without a UUID version mask. Construction chunks contain 1,024 records,
merge fan-in is two, and the maximum run contains 4,096 records. Routes alternate
by row, including parallel edges and self loops. Properties include nullable
integers, repeated categorical strings and distinct strings. The heterogeneous
case alternates chunks that omit a property field entirely. These definitions
cross chunk, merge and route boundaries and preserve external identities.

## Measured permanent ownership

Bytes are raw bytes, not MB. Adjacency is absent except in the explicitly indexed
case. The indexed publication currently changes manifest representation; its
smaller total despite adding CSR is therefore not a claim that indexes are free.

| Fixture | Nodes | Routes | Permanent allocated | Physical logical | Parquet bytes | Zstd level 1 candidate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Sequential, no properties | 4,097 | 1 | 8,388,608 | 7,813,955 | 5,262,627 | 973,925 |
| Random, no properties | 8,193 | 1 | 8,929,280 | 8,266,084 | 5,318,519 | 4,015,814 |
| Random, properties, CSR built | 4,097 | 8 | 20,840,448 | 17,962,542 | 10,231,116 | 7,845,372 |
| Random, heterogeneous properties | 4,097 | 4 | 14,929,920 | 11,219,825 | 8,357,272 | 6,398,839 |

The eight-route case before CSR is 21,446,656 allocated bytes. Its indexed
categories are topology nodes 135,168; topology edges 7,307,264; properties
5,488,640; UUID/surrogates 2,523,136; adjacency 5,050,368; manifests/catalog
335,872. The executable report emits every category's logical bytes, physical
logical bytes, allocated bytes, references and unique objects for every fixture.
No graph values or UUIDs are included in that aggregate report. The indexed
case has 37 adjacency references and 37 physical objects, establishing that its
18 CSR shard files are physically distinct. The codec experiment sums shard-file
bytes; the separate attribution report remains the authority for permanent allocation.

## Authority and access contracts

| Representation and source | Meaning and consumer | Retention requirement |
| --- | --- | --- |
| Node/edge Parquet, `graph_construction_encoding.rs` | Canonical external UUIDs, endpoint UUIDs, runtime label/type IDs, surrogates and property routing; scan and hydration readers | Preserve exact values, schema and nulls. Runtime catalog IDs are not ontology IDs. |
| Property Parquet, same encoder | Typed values and routing/tombstone metadata; selective property hydration | Similar identity columns provide joins and are not proven redundant. |
| Membership v3, `uuid_membership.rs` | UUID16 + kind1 + reserved7 + surrogate8; sorted membership and conflict probes, including tombstones | SHA, ordering, full-width surrogate and kind remain authoritative for bounded reopen/probes. |
| Ordinal v4, `ordinal_identity_v4.rs` | UUID→ordinal records (24 bytes), ordinal→UUID (16 bytes), tombstones | Forward and reverse probes serve different access directions; do not delete either based on repeated UUID bytes. |
| CSR shard v1, `adjacency.rs` | Arrow IPC lists of full-width u64 edge/neighbor IDs and offsets; per-relation and union in/out traversal | Whole bounded shard is authenticated and decoded on cache miss. Union and relation indexes serve distinct queries. |
| Patricia node v2, `graph_manifest.rs` | SHA-addressed path→full file metadata; authenticated lookup and copy-on-write publication | Every entry digest, length, kind and path remains protected; active generations retain old nodes. |

## Candidate decisions

Experiments rewrite identical Arrow batches and assert exact decoded equality.
They do not publish a candidate production format. Normalized uncompressed
Parquet matches measured source bytes in these fixtures, but codec comparisons
are explicitly against the normalized baseline. Elapsed encode/decode times are
single-run exploratory observations, not performance acceptance thresholds.

| Candidate | Quantified evidence and bounded follow-up | Tradeoffs and compatibility |
| --- | --- | --- |
| Parquet Zstd level 1 (#1202) | Random fixtures save 23–25% of Parquet bytes; sequential fixture saves 81.5%. Production follow-up must reproduce these fixture reductions and exact public round trips. | Lower storage/read I/O; additional encode/decode CPU. Keep bounded row groups, existing checksums and reader codec support. Level 3 is slightly worse for random fixtures. |
| Remove membership reserved padding (#1203) | 69,634 actual records: 2,228,288→1,740,850 bytes, saving exactly 487,438 raw bytes; 73,730 records save 516,110. | This is a 25-byte candidate, not a narrower identifier. Requires versioned readers/writers and mixed legacy/new runs, full u64/kind/tombstone boundaries, recovery and bounded binary search. Physical savings depend on file rounding and migration overlap. |
| Bounded manifest buckets (#1204) | Heterogeneous fixture: 750 node objects / 3,072,000 allocated bytes versus 238 candidate objects / estimated 974,848 bytes at 4 KiB. All 544 file entries resolve with SHA-authenticated lookup. | Fewer small-file reads/allocations; a lookup decodes up to eight entries. Production must bound serialized bytes as well as entry count, preserve legacy nodes, split/update/delete correctly, reject corruption and preserve active snapshots. Estimate excludes root metadata, history and migration overlap. |
| CSR IPC compression (#1205) | 18 shards: 4,920,564 bytes uncompressed, 2,320,564 LZ4, 1,324,020 Zstd. Estimated allocated bytes: 4,972,544→1,363,968 with Zstd. Largest decoded batch arrays: 3,322,008 bytes (not process RSS). Exact schemas and values match. | Cold lookup gains lower read I/O but incurs decompression CPU and temporary buffers; warm decoded cache behavior remains similar. Preserve full u64 fields and shard boundaries; budget decode memory and support legacy IPC before changing writes. |
| Delete repeated identity or directional indexes | No change: distinct access and recovery contracts remain in use. | No evidence that removal preserves bounded membership, reverse hydration or relation/union traversal. |

## Regression ceilings

These are fixture-specific ceilings, not universal bytes-per-edge promises.
Let N/E be fixture rows and R its routes. The test allows at most
`ceil(N/1024)*R` node fragments and `ceil(E/1024)*R` edge fragments, an intentionally
conservative upper bound from construction's chunk/route partitioning.
Topology logical ceilings allow 48 bytes/node and 96 bytes/edge plus 2 KiB per
possible fragment; this covers the fixed-width identity/surrogate columns and
these fixtures' Parquet framing/dictionaries. Property rows allow 64 bytes plus
the same fragment framing allowance, covering their fixed integer and bounded
string payloads. These allowances must be reconsidered if fixture schemas change.

UUID bytes allow 32 per identity, 64 per node for forward/reverse authorities,
and 128 KiB for manifests. At most 32 UUID objects allows bounded run and ordinal
fragments for these configurations. CSR allows 80 bytes/edge plus 16 bytes/node
and 8 KiB per route/union: u64 edge/neighbor pairs occur in in/out relation and
union paths, with room for offsets and framing. Manifests allow 2 KiB per payload
object plus 64 KiB controls and at most twice the payload count plus 32 objects.
These ceilings detect representation amplification while allowing format compaction.

Allocated bytes are separately bounded by physical logical bytes plus 4096 bytes
per unique object. The 4-KiB rounding assumption belongs to the measured host;
it is not a portable guarantee for arbitrary allocation units. Each category is
checked independently, so added CSR cannot hide growing topology or manifests.
Final production repairs also require recovery, snapshot, corruption and resource
budgets appropriate to their changed surface. Larger-scale confirmation reuses
#900's admitted ladder; this assessment does not authorize S24/S26 execution.
