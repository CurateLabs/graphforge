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

The maintainer explicitly waived backward compatibility on 2026-09-10 because
GraphForge is pre-v1. The selected repairs do not require legacy codecs or
migration machinery. Current-format authentication, active snapshots, exact
semantics, recovery and bounded resource use remain required.

Experiments rewrite identical Arrow batches and assert exact decoded equality.
They do not publish a candidate production format. Normalized uncompressed
Parquet matches measured source bytes in these fixtures, but codec comparisons
are explicitly against the normalized baseline. Elapsed encode/decode times are
single-run exploratory observations, not performance acceptance thresholds.

| Candidate | Quantified evidence and bounded follow-up | Tradeoffs and compatibility |
| --- | --- | --- |
| Parquet Zstd level 1 (#1202) | Random fixtures save 23–25% of Parquet bytes; sequential fixture saves 81.5%. Production follow-up must reproduce these fixture reductions and exact public round trips. | Lower storage/read I/O; additional encode/decode CPU. Keep bounded row groups, existing checksums and reader codec support. Level 3 is slightly worse for random fixtures. |
| Remove membership reserved padding (#1203) | 69,634 actual records: 2,228,288→1,740,850 bytes, saving exactly 487,438 raw bytes; 73,730 records save 516,110. | This is a 25-byte candidate, not a narrower identifier. Requires consistent current-format readers/writers, full u64/kind/tombstone boundaries, recovery and bounded binary search. Physical savings depend on file rounding. |
| Bounded manifest buckets (#1204) | Heterogeneous fixture: 750 node objects / 3,072,000 allocated bytes versus 238 candidate objects / estimated 974,848 bytes at 4 KiB. All 544 file entries resolve with SHA-authenticated lookup. | Fewer small-file reads/allocations; a lookup decodes up to eight entries. Production must bound serialized bytes as well as entry count, split/update/delete correctly, reject corruption and preserve active snapshots. Estimate excludes root metadata and history. |
| CSR IPC compression (#1205) | 18 shards: 4,920,564 bytes uncompressed, 2,320,564 LZ4, 1,324,020 Zstd. Estimated allocated bytes: 4,972,544→1,363,968 with Zstd. Largest decoded batch arrays: 3,322,008 bytes (not process RSS). Exact schemas and values match. | Cold lookup gains lower read I/O but incurs decompression CPU and temporary buffers; warm decoded cache behavior remains similar. Preserve full u64 fields and shard boundaries; budget decode memory before changing writes. |
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

## Construction Parquet repair (#1202)

New permanent construction payloads use Zstd level 1, including the runtime
catalog produced during shaping and copied into the publication. Row-group,
dictionary, schema, hashing, cache-release and durability behavior stay as before.
Accepted input and private merge Parquet are outside this permanent-output policy.
This repair does not yet change mutation or delta replay writers; their separate
resource admission requires the publishing-policy follow-up requested under #1194.

[Repair measurements](../../development/evidence/permanent-parquet-1202.json)
record source `87a5b19f` on the same host and toolchain as the assessment. All five
facade tests passed, including exact reopen/query/export/full-verify/clean-import
oracles and every production column's codec. Random-ID Parquet payloads remain
below 80% of their same-layout uncompressed control.

| Fixture | Baseline permanent allocation | New permanent allocation | New Parquet payload | New Parquet allocation |
| --- | ---: | ---: | ---: | ---: |
| Sequential | 8,388,608 | 4,112,384 | 973,925 | 1,097,728 |
| Random | 8,929,280 | 7,585,792 | 4,015,814 | 4,145,152 |
| Eight property routes, CSR built | 20,840,448 | 18,644,992 | 7,845,372 | 10,743,808 |
| Heterogeneous properties | 14,929,920 | 13,086,720 | 6,398,839 | 7,467,008 |

The serial assessment took 393.65 seconds and peaked at 270,208 KiB RSS, versus
449.10 seconds and 268,780 KiB before the repair. These are exploratory whole-test
measurements, not isolated codec costs or proof of a timing improvement. Raw
results include per-fixture encode/decode times and process filesystem I/O;
process I/O includes queries and portable copies and is not a publication-only
byte counter.

The paired lifecycle test also passes at 1x/2x/4x. Compression moves the maximum
allocation into shaping: the reclaimed and retained controls can now tie at that
phase. The test verifies both measured phase peaks when they tie, no increase
against the compressed retained control, strict reduction against the frozen
pre-compression control, and the original strict retained-allocation reduction.
Compression does not retroactively remove a historical shaping peak.

## Delta and compaction ownership repair (#1219)

Construction publishes mapped CAS roots. Delta preparation and compaction had
assumed generation-owned graph directories, so direct construction → composite
property mutation failed before replay. Current production emits both ownership
forms. The repair resolves the declared authority, authenticates CAS payloads,
and links immutable inputs into a private workspace. A delta seals its new run
into the shared manifest. Compaction seals changed files and removes delta-run
entries while retaining unchanged objects, capabilities and other participants.
Publication retains its CAS lease through CURRENT and releases it before cleanup.

Runtime catalog persistence now replaces the private alias through the existing
durable rewrite protocol. Catalog and label-encoding marker commit together.
Composite deltas include observed runtime names in their candidate; introducing
a property can legitimately change the catalog digest. Node/property payloads
remain byte-identical when appending a delta. The catalog writer retains its
previous Parquet defaults; shared encoding policy remains #1213.

[Measured ownership evidence](../../development/evidence/cas-delta-ownership-1219.json)
uses runtime source `36bc94505baef9125ad254c4b707aac2383ea090`. Two compiled API
regressions pass public construction, composite and direct storage publication,
reopen/query, cancellation before publication, active query snapshots,
idempotent retries, compaction, export, full verify and clean import. Node-only
fixtures isolate ownership from the multi-relationship replay repair in #1218.
The existing delta/compaction integration suites also pass (26 tests), alongside
10 unit/crash tests, 19 composite tests and 30 bulk-construction tests. A focused
storage test installs real CAS catalog/marker objects and verifies that durable
persistence replaces both private aliases while preserving the original objects;
the Windows and macOS native CI lanes run this exact test.

| Nodes | Base logical bytes | Reused payload bytes | Private controls + run allocated |
| ---: | ---: | ---: | ---: |
| 33 | 14,018 | 11,460 | 24,576 |
| 4,097 | 545,130 | 379,901 | 188,416 |

Both allocation fixtures assert identical CAS/private file identities for node
and property payloads, unchanged parent inventories, a run no larger than 4 KiB,
and private control/run allocation no larger than 256 KiB. They use seven-row
batches and the existing 2 MiB logical replay limit. Mutable membership controls
still require private copies, so their allocation grows with the fixture.
These are scoped preparation budgets, not total temporary-disk or process-memory
bounds. Fingerprinting still materializes a replay view; compaction now publishes
its existing private replay output without making a second complete staging copy.

The untraced serial binary took 5.38 seconds (4.10 user, 0.98 system), with
142,932 KiB peak RSS. Kernel filesystem accounting reported 54,064 input and
28,672 output blocks of 512 bytes. A separate syscall trace counted 60,252,083
returned read bytes and 10,211,682 returned write bytes, including startup,
authentication, both fixtures, queries and the portable round trip. These are
observations, not portable CPU/I/O thresholds; cache-dependent physical I/O and
syscall byte counts are different quantities. Compilation is excluded. Reproduce
with the prebuilt `permanent_storage_budgets` binary and
`cas_ --nocapture --test-threads=1`, using native-root `TMPDIR`; run `/usr/bin/time -v`
separately from `strace -f -e trace=read,pread64,readv,preadv,write,pwrite64,writev,pwritev,copy_file_range,sendfile,fsync,fdatasync`.

## Workspace publication ownership (#1222)

Source `47505945dcdcf4711aa6710ac192b2616e20558a` fixes two CAS ownership
assumptions in workspace operations: publication now selects a generation tree
only for a generation-owned graph participant, and composition preflight reads
the manifest-selected CAS runtime catalog. CAS publication retains its lease
through CURRENT. Catalog decoding uses the existing authenticated file reader
with a fixed 1 MiB authentication buffer and a retained read lease; it does not
allocate a whole encoded-catalog copy or materialize the graph.

The public fixture constructs 33 nodes and 129 exploratory edges, adopts a
URI-identified Advisory ontology with disjoint new names, retries adoption,
clears and re-adopts before any qualified bindings exist, and reopens. It then
uses public composition preview/publication to establish bindings, constructs
33 new qualified nodes and 129 edges, updates graph directedness, and proves
exact query results through export, full verification and clean import.
The original 18 graph files (20,842 bytes) retain their inventory entries and
inode identities across adoption; no original graph payload is re-encoded.
The test independently binds the stored relation route to its qualified symbol.

| Measurement | Result |
| --- | ---: |
| Prebuilt public test elapsed / user / system | 2.03 / 1.12 / 0.61 s |
| Peak process RSS | 129,120 KiB |
| Syscall read / write bytes | 10,162,546 / 1,396,254 |
| Kernel input / output blocks (512 bytes) | 14,360 / 8,048 |
| Successful fsync calls in separate trace | 2,259 |

The separate trace takes 11.65 s with instrumentation. Process I/O includes
startup, verification, import and test output; kernel I/O is cache-dependent.
These measurements do not establish portable CPU/RSS or temporary-disk peak
budgets. Encoding-policy resource assessment remains #1213. The focused public
test, 53 ontology-related API tests, production workspace Clippy, fast pre-push
and gate-registry checks pass. Commands and raw measurements are in
[`workspace-cas-ownership-1222.json`](../../development/evidence/workspace-cas-ownership-1222.json).

Same-name ontology promotion changes graph and ordinal authorities after
workspace publication, and clearing authority for retained typed data can
leave semantic bindings without their composition. Both are recorded under
#1221 with public reproducers. This ownership-only repair does not claim those
complete graph-contract failures are resolved.

## Composite property ownership (#1224)

Composite property operations now resolve physical owners during the existing
validation scan. Nodes use their immutable primary identity, as ordinary Cypher
mutation does; runtime nodes use `_untyped`, and qualified nodes use the retained
semantic binding. Edges use the authenticated physical relation route (including
logical relation names in exploratory storage). Same-request creates resolve the
same qualified identities. Both canonical staging and GFDR encoding consume this
operation plan, including optimistic conflict baselines. Qualified properties
must resolve against their declared owner before publication; a missing or wrong
owner never silently selects `_untyped`.

A first write to an absent qualified property route uses canonical staging:
GFDR carries values but cannot establish the route's semantic schema authority.
The property inventory seeds that authority from authenticated property bindings,
without decorating an existing malformed fragment. Canonical CAS replacement
reuses the existing compaction object-difference helper and holds its publication
lease through CURRENT. Later supported property updates use GFDR. Hydrated
property and semantic validation share the authenticated materialized inventory;
replay advances property/search counters so subsequent ordinary mutations cannot
reuse a fragment generation. Optimistic CAS publication authenticates the final
installed directory after promotion, before CURRENT.

Five public fixtures at source `df3a707d` cover declared-property refusal,
qualified create-then-set for nodes and edges, edge removal through portable
import, qualified node GFDR/compaction at 33 and 4,097 nodes, and a mixed graph
with 33 exploratory plus 33 qualified nodes and 129 edges of each kind through
canonical optimistic publication. Exact values, active snapshots, reopen,
export, full verification, clean import and later removals are checked. The
mixed canonical fixture has no GFDR run and explicitly checks compaction refusal
without changing CURRENT; actual mixed-edge GFDR compaction remains #1218's
integration test. These are distinct proofs, not interchangeable substitutes.

The first property write changes 4,393 Parquet bytes on both node-only sizes,
and 4,494 bytes on the mixed graph. Each fixture enforces a 64 KiB changed-Parquet
budget and exact reuse of unchanged topology payload inventory entries. No
encoding defaults change in this repair. The owner maps retain only requested
identities and same-request creates, and use existing binding/catalog authority.
The existing validation snapshot still reads complete topology batches and
retains full identity sets; this repair adds no second topology scan, but the
whole transaction is **not** request-sized. Hydration, canonical staging,
manifest capture, portable verification and import also retain their existing
I/O and temporary-workspace costs. The changed-Parquet ceiling is not a bound on
those costs, native codec memory, peak RSS or total temporary disk. Their
cross-path admission and encoding-policy budgets remain #1213.

The five prebuilt fixtures take 12.51 s elapsed (8.70 s user, 2.62 s system),
with peak RSS 139,224 KiB and kernel input/output of 126,224/53,288 blocks of
512 bytes on the admitted native ext4 host. These are source-bound observations,
not portable resource ceilings; the separate syscall trace includes startup,
query, verification, import and test output. Raw evidence and commands are in
[`composite-property-ownership-1224.json`](../../development/evidence/composite-property-ownership-1224.json).

Validation includes 734 API unit tests, 45 publication tests, the unequal-route
property generation regression, production workspace Clippy and fast pre-push.
The storage-wide run passed 1,074 tests with two existing ignored tests; its one
failure is an unchanged test hard-coding `/tmp`, which is tmpfs on this host and
fails filesystem admission before its hostile-file assertions. Required Bazel
PR CI remains the merge authority. Composite topology creation on an already
constructed CAS parent still exposes the UUID authority defect tracked in
#1221; no authentication check was relaxed to admit that operation.

## Exploratory construction and replay layout (#1218)

Source `13699caebd45a97e2da2cbb4575a91be1fe9f9e9` coalesces logical
relationship groups into their physical route inside each existing bounded
construction ID window. Previously two exploratory logical types produced
individually sorted fragments with overlapping ID ranges in `_exploratory`;
replay correctly rejected their concatenation. The encoder now sorts only the
selected window's indexes and preserves each row's relationship name. It does
not introduce a full-graph sort or a compatibility reader.

Replay retains the authenticated source schema for both node and edge output,
including qualified route/composition metadata. Exploratory rows retain the
physical `rel_type_name` column; typed rows retain their physical route. Resource
charges include variable relationship names. Full-width IDs, UUIDs, endpoints,
replacement identity, duplicate rejection and strict cross-fragment ordering
remain enforced. Encoding defaults are unchanged here; replay compression is
still the policy repair in #1213.

Four public fixtures cover retained-parent construction with 66 nodes and 4,097
random-ID edges across two logical relationships, property delta publication,
actual compaction, active snapshots, subsequent mutation, exact query, reopen,
export, full verification and clean import. The mixed fixture contains 33
exploratory and 33 qualified nodes, with 129 edges in each domain. It establishes
qualified property authority, publishes a second update, asserts exactly one
GFDR run, compacts, and checks both physical edge schemas and the exact value
123 through reopen and portable import. A separate long-route fixture admits
small input chunks but rejects the oversized coalesced Arrow batch before
publication, preserving CURRENT and the exact parent graph.

The retained-parent fixture has two first-generation edge fragments and three
additional child fragments, each at most 1,024 rows. The reader check uses
seven-row batches and verifies strictly increasing IDs across fragments.
Deterministic ceilings and observed counters are:

| Counter | Ceiling | Parent | Child |
| --- | ---: | ---: | ---: |
| Peak Arrow batch bytes | 131,072 | 103,396 | 103,396 |
| Accounted live bytes | 4,194,304 | 2,805,218 | 2,805,232 |
| Temporary allocated peak bytes | 2,097,152 | 1,224,704 | 1,302,528 |
| Encode read bytes | 1,572,864 | 927,188 | 1,042,455 |
| Encode write bytes | 524,288 | 302,218 | 363,917 |
| Total construction read bytes | 16,777,216 | 5,827,068 | 7,319,877 |
| Canonical output bytes | 262,144 | 160,479 | 189,915 |
| Staged plus retained logical bytes | 655,360 | 479,256 | 476,148 |

Batch rows stay at most 1,024, run bytes at most 4,096, merge fan-in at most two,
and prior topology payload decoding is zero. These counters describe the
construction subsystem. Retained logical groups and the selected physical
Arrow batch coexist inside the bounded window; the Arrow counter alone is not
a whole-process RSS measurement. The full public fixture process takes 14.86 s
elapsed (10.57 s user, 2.77 s system), peaks at 146,584 KiB RSS, and records
148,784 input / 84,416 output kernel blocks of 512 bytes. Startup, query, replay,
portable verification and import are included. Raw syscall I/O and commands are
in [`exploratory-replay-1218.json`](../../development/evidence/exploratory-replay-1218.json).

Validation: four public regressions, 99 construction tests, 23 replay-focused
tests, ten delta/compaction tests, production workspace Clippy, fast pre-push,
formatting and gate-registry checks pass. Independent review verifies the
retained #1224 property-generation repair and real mixed GFDR coverage. #1221
still owns unsupported topology-journal authority and constructed-parent
composite topology mutation; this repair does not broaden that support.

## Permanent Parquet publishing policy (#1213)

The production audit follows publication ownership, not temporary filenames.
There are fourteen storage constructor sites and two API constructor sites.
`permanent_parquet::writer_properties` selects Zstd level 1, Parquet V1,
page statistics and offset indexes, 1 MiB page/dictionary targets, 20,000 page
rows, 1,024-value write batches and 64-byte statistics/index truncation requests.
These are explicit pinned Parquet 58 defaults, with the codec changed where
necessary. Statistics truncation is a request: an unincrementable maximum can
retain its original value. A footer identifies Zstd but does not encode its
compression level; the shared builder establishes level 1.

| Permanent producer | Publishing path | Retained resource/lifecycle choices |
| --- | --- | --- |
| `graph_construction_encoding::write_parquet` | Resumable topology, properties and controls | Existing bounded construction windows, cache advice and publication leases |
| `graph_construction::write_parquet_with_properties` | Privately shaped runtime catalog, then published unchanged | Permanent caller explicitly supplies policy; accepted construction batches remain private |
| `writer::stream_replay_nodes` | Canonical replay and compaction nodes | Dictionaries off, row groups at most `max_batch_rows`; bounded decoder strategy below |
| `writer::stream_replay_edges` | Canonical replay and compaction edge routes | Dictionaries off, same row-group bound, authenticated physical route/schema |
| `writer::open_replay_property_fragment` | Immutable property/tombstone fragments | Dictionaries off, chunk flushes, same row-group bound, route/generation metadata |
| `staging::restage_append` | Mutation replacement later owned by `RewriteBatch` | 65,536-row groups; private file ownership and atomic replacement |
| `staging::stage_parquet_temp` | Mutation and catalog replacement | Same row-group bound and publication lifecycle |
| `staging::stage_parquet_batches_temp` | Streaming mutation replacement | Same row-group bound; separate reader/writer lifetime |
| `graph_projection::write_parquet` | Belief and portable projected graph payloads/catalog | Existing collection/sort behavior; no new whole-process memory claim |
| `semantic_bindings::rewrite_legacy_route` | Supported current-format route composition | Existing 8,192-row reader batches and semantic metadata |
| `semantic_bindings::materialize_semantic_migration` | Supported current-format multi-ontology composition | Existing file/row/input-byte limits and cancellation checkpoints |
| `vector_store::write_vector_snapshot` | Vector-search participant | Dimension, vector, cell and encoded-file limits; complete Arrow batch remains |
| `project_checkpoints` restoration encoder | Restoration-transition participant | `graphforge-restoration-transition/1` creator marker and restoration lifecycle |
| `runtime_entity_labels::persist_runtime_catalog` | Bulk/composite runtime catalog publication | Existing private staging and catalog authority |
| API `knowledge::write_parquet` | Assertion/evidence/confidence/reasoning participants | Existing serialized-Vec ownership and atomic participant publication |
| API `provenance::write_parquet` | Provenance participant | Existing serialized-Vec ownership and atomic participant publication |

Separate writer implementations remain. The policy does not own files,
leases, authentication, durability, recovery, cancellation or commit authority.
No permanent writer has an uncompressed-policy exception. Current semantic
composition remains included despite old function names containing “legacy” or
“migration”; this change adds no compatibility or migration machinery.

Private accepted construction chunks, shape/merge streams, the bounded replay
IPC stream and test fixtures are excluded. The user-selected Parquet result
sink in `graphforge-io` is an external result artifact, not a graph generation.
Standalone ontology persistence has eight Parquet tables but no verified graph
publication caller; it remains a separate public API, not a test fixture. The
source audit found no production `SerializedFileWriter` constructor.

### Baseline and paired experiments

The baseline source is `60ffdca983ed1ad4b2acd4edfe2605f3e85d6e7e`.
[`permanent-parquet-1213-baseline.json`](../../development/evidence/permanent-parquet-1213-baseline.json)
records actual public construction, mutation, compaction, reopen, query, export,
full verification and clean import. At 1,025 nodes and 4,097 random edges with
heterogeneous properties, construction published 353,548 Parquet bytes; mutation
published 377,487; compaction increased that to 537,467. The respective column
codec counts were 99/0, 92/10 and 52/21 Zstd/uncompressed. This demonstrates the
production regression rather than inferring it from a writer helper.

That baseline process took 12.48 s elapsed, 11.89 s user and 0.84 s system,
with 156,284 KiB peak RSS. Its separate syscall run read 179,612,114 bytes and
wrote 42,717,623 bytes, including file-copy syscalls, startup, queries and portable
operations. A third run sampled a peak of 8,749,056 allocated bytes across the
workspace, deduplicating hard links. Sampling excludes unlinked open files and
can miss short-lived peaks; it includes retained generations and portable
artifacts, so it is neither an exact temporary-only peak nor an admission limit.

Paired encodes retain each path's schema, dictionary and row-group settings,
changing only the codec. Cases include a materialized one-row fragment, 257
wide/nullable/heterogeneous rows with a 256 KiB string, and 65,537 random UUID /
full-width integer rows crossing both replay and staging row-group boundaries.
The independent pair-process run took 1.31 s elapsed, 1.27 s user and 0.04 s
system, with 58,604 KiB peak RSS; its syscall run read 22,263,859 and wrote
12,610,549 bytes. These are codec experiments, not whole-publication admission.
Small fragments can grow with Zstd; the one-row dictionaries-on case grew from
10,017 to 10,722 bytes. Wide replay-profile output fell from 502,549 to 26,794
bytes; the narrow random replay profile fell from 1,576,490 to 1,119,732 bytes.

The codec tradeoff is measurable even where storage improves. In the paired
wide replay case, encoding elapsed time rose from 4.711 to 6.419 ms and decoding
from 2.272 to 2.889 ms. For 65,537 narrow random rows, encoding rose from 36.107
to 44.281 ms and decoding from 3.198 to 6.286 ms. These are single-run elapsed
measurements, not CPU guarantees or benchmark distributions. The raw evidence
retains per-profile results and whole-process user/system CPU observations;
Zstd is selected for durable storage despite these measured codec costs.

Deterministic codec-test ceilings are 2 MiB per output, 8 MiB combined temporary
allocation per pair, and 8 MiB of `ArrowWriter::memory_size()`. The latter omits
native codec allocation and completed metadata, so it is deliberately not used
as a replay memory proof. Timing/RSS/kernel-I/O observations are host-dependent;
row, page, byte and explicit reservation ceilings provide repeatable regressions.

### Replay resource composition

The old estimate omitted native codecs, physical nested leaves, decoder page
buffers and retained page indexes. Pinned Parquet 58/Zstd 1.5.7 source inspection
and safe context-size measurements establish separate reservations for schemas,
writer structures, completed metadata/indexes, active encoded chunks and native
contexts. Decoder admission inspects authenticated raw page headers before
allocating the decoder, includes compressed and decompressed buffers, and bounds
returned values across pages and row-group boundaries. Variable/repeated leaves
conservatively reserve the contributing row groups; this can refuse small reads
from very large groups. These are allocation-component/logical estimates, not a
process-RSS or allocator-fragmentation guarantee.

Dictionaries remain disabled for replay. Below both page thresholds, no page is
compressed during writes; row-group close consumes leaf writers sequentially.
Only one compressor becomes active alongside the other dormant codec pairs.
Larger groups reserve all active codec contexts. Completed compressed chunks
remain charged until flush. Property decoder and encoder phases are separate,
while metadata from previously flushed property chunks remains resident.

The 2 MiB, seven-row topology replay fixture remains supported. When direct
node decoding plus encoding does not fit but each separate phase does, replay
selects a private uncompressed Arrow IPC stream **before** encoding. One
self-deleting stream is capped at 64 MiB per replay invocation; cumulative writes
are checked before reaching disk. Both phases are admitted independently.
The normal direct strategy incurs no stream I/O. This is private staging, not
an uncompressed permanent-output fallback.

At 260 and 516 nodes the fixture uses direct replay. At 1,028 nodes the private
stream is 207,496 bytes / 208,896 allocated bytes, below its deterministic
256 KiB fixture ceiling. Successful replay writes and reads that stream once;
its permanent node Parquet file is byte-for-byte identical to the direct
strategy with the same seven-row bound. Separate tests cover exact byte-limit
acceptance, one-byte-below rejection, schema/full-width values, row order,
row-count authority, cleanup and unchanged source bytes. Reader admission also
covers seven one-row large-string pages, repeated values and batches spanning
several tiny row groups, with rejection immediately below the computed bound.

### Integrated result and regression gates

Implementation source `c16397d2cd7aeb01411f5cbb51c433154d3c3ef9` was measured
with the same prebuilt public fixture after other builds/tests finished.
[`permanent-parquet-1213.json`](../../development/evidence/permanent-parquet-1213.json)
retains actual column-codec/encoding counts, output inventories and process observations.

| Published stage | Baseline Parquet bytes / allocated bytes | Shared policy bytes / allocated bytes |
| --- | ---: | ---: |
| Construction | 353,548 / 389,120 | 353,548 / 389,120 |
| Mutation | 377,487 / 413,696 | 371,901 / 409,600 |
| Compaction | 537,467 / 569,344 | 303,718 / 335,872 |

Every published column uses Zstd (99 construction, 102 mutation and 73
compaction columns). Changed compaction files retain dictionaries-off encoding
and at most 8,192 rows per group. The public fixture enforces logical/allocated
ceilings of 400/448 KiB for construction, 416/480 KiB for mutation and 352/400 KiB
for compaction, in addition to exact values after reopen and portable round trips.

The final process took 12.99 s elapsed, 12.41 s user and 0.83 s system, with
159,492 KiB peak RSS, versus baseline 12.48/11.89/0.84 s and 156,284 KiB. Separate
syscall observations fell from 179,612,114 to 163,149,583 read bytes and from
42,717,623 to 40,187,215 write bytes; both had 3,109 successful fsync calls and
no traced syscall errors. OS filesystem output fell from 90,752 to 85,904
512-byte blocks; input remained 50,640 blocks. Sampled unique-inode workspace
allocation fell from 8,749,056 to 7,798,784 bytes. These single-host observations
include startup, queries, authentication, portable operations and test output;
the candidate additionally inspects pre-compaction inventory and prints file
digests. They are not CPU, RSS or temporary-disk guarantees. The separately
bounded low-memory IPC stream is measured directly, since pathname sampling
cannot see its unlinked file on Unix.

Validation covers all 20 public publishing fixtures; 734 API unit tests; three
public retained-data semantic-composition certification tests; ten compaction
and 16 journal integration tests; and 20 replay-focused unit tests. The API
fixtures inspect published knowledge, epistemic, provenance, vector, projection
and restoration output, including the restoration `created_by` marker. Existing
recovery, cancellation, active-snapshot, authentication and exact retry tests
remain active. Targeted native Bazel certification/journal/compaction tests,
workspace Clippy, formatting, fast pre-push and gate-registry checks passed.
The full local storage aggregate passed 1,081 tests with two existing ignores;
one unchanged test hardcodes `/tmp`, where this host's tmpfs fails filesystem
admission. Required native Bazel CI remains the merge gate.

## CAS UUID mutation ownership (#1228)

The integrated publishing-contract census reproduced ordinary and qualified
CREATE failures on a constructed CAS parent. Hydration shared the mutable v5
UUID manifest and receipt, but mutation planning correctly required private
control-file ownership. Those two controls now use the existing authenticated,
bounded private-copy path. Immutable identity and surrogate runs remain shared;
retained reads accept extra links only while readonly, with unchanged named-file
identity, manifest digest/generation and full-run/per-block authentication.
Private construction ownership guards remain single-link. No encoding changes,
compatibility format or generic writer abstraction are introduced.

Public regressions exercise flat 33-node and sharded 4,097-node exploratory
parents with 129 edges. Nine successive CREATEs, DELETE and another CREATE
preserve prior UUID/surrogate mappings and never reuse the deleted surrogate.
Qualified node/edge creation and property changes retain the constructed parent
and semantic owners. Both paths check exact query values, reopen, export, full
portable verification and clean import. An active pre-mutation stream retains
its exact ordered values; all original CAS inode identities and digests remain
unchanged. Adversarial tests reject writable shared runs, readonly-to-writable
transitions, mutated blocks and extra manifest aliases. Unix replacement fails
retained identity checks; Windows retained handles prevent replacement itself.

Eight subprocess cases cover process exit and returned errors before private
intent, after durable intent, before CURRENT and after CURRENT. A returned error
after durable UUID intent is authenticated and rolled forward before normal
publication; a process exit there leaves the selected parent intact. Tests
assert these distinct outcomes, reopen exact selected data, perform another
mutation and complete a portable round trip. Existing storage tests continue to
cover cancellation and unfinished private artifact cleanup.

The public hydration fixtures cap the two mutable controls at 2 KiB logical and
8 KiB allocated, all private hydration writes at 192 KiB, and the existing copy
buffer at 64 KiB. Immutable UUID payloads must have the original readonly CAS
inode: zero payload copying or reencoding. The buffer is reused serially across
files; control file size changes copy I/O and disk use, not this buffer size.
These are fixture/component bounds, not a whole-process RSS guarantee. Existing
v4 ordinal private artifacts remain included in total hydration writes; they
are not attributed to this repair. The control-specific unit test checks exact
read/write bytes, calls and file/directory synchronization counts.

Source `ad7a867bc836bf3c7381f3a1f70a71914237b349` and raw observations are in
[`cas-uuid-ownership-1228.json`](../../development/evidence/cas-uuid-ownership-1228.json).

| Constructed nodes / edges | New control bytes / allocated | Shared immutable UUID bytes | All private hydration writes |
|---|---:|---:|---:|
| 33 / 129 | 1,429 / 4,096 | 3,810 | 4,093 |
| 4,097 / 129 | 1,440 / 4,096 | 202,946 | 166,775 |

The complete canonical fixture took 18.12 s elapsed, 15.91 s user and 2.30 s
system, with 153,872 KiB peak RSS. Separate syscall tracing recorded 297,801,224
read bytes, 38,792,187 write bytes and 8,052 successful fsync calls, with no
traced errors. OS filesystem input/output were 39,592/101,216 512-byte blocks.
A separate 1,317-sample scan observed 21,532,672 bytes of unique-inode workspace
allocation (maximum sample interval 24.2 ms). This includes retained generations,
private workspaces and portable artifacts; unlinked files and shorter peaks can
be missed. These whole-fixture observations are not control-copy-only costs or
process admission limits. The baseline fails its first CREATE, so there is no
valid baseline runtime comparison and no performance improvement claim.

Validation passed all 24 public publishing tests, 54 membership and 38
object-store tests, the final writable-transition regression, and all 734 API
unit cases (733 in the aggregate and the exact-control assertion after its
focused update). Workspace Clippy, formatting, fast pre-push and gate-registry
checks passed. Independent review corrected the Windows replacement test;
required exact-head native CI remains the platform/merge authority.

This ownership repair does not make same-name adoption atomic (#1229), define
bound-composition removal (#1230), refresh a facade after compaction (#1231), or
complete the canonical publishing-contract gate (#1221).
