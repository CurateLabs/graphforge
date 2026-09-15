# Integrated storage and query evidence for #1194

The epic implemented permanent-storage reductions, one permanent Parquet encoding policy, query execution improvements, and the lifecycle repairs needed to preserve exact public behavior. The final measurement source is `710c6c64f4718c0664d08bdd3aafe21de4a29eba`, after the consumed-root retirement (#1268), recovery authentication repair (#1269), growth-envelope repair (#1272) and packed construction records (#1274). The final S20/S22 lifecycle passes, but the existing capacity projection returns refuse; final capacity/S26 remains the explicit open outcome.

## Implemented improvements and measured costs

These are separate representative workloads. Their savings must not be added or their speedups multiplied. The linked evidence retains source/executable identities, command results, budgets and unsuccessful observations.

| Concern | Implemented outcome and evidence | Measured tradeoff or boundary |
| --- | --- | --- |
| Permanent inventory and topology/property Parquet | [#1196 census](evidence/permanent-storage-1196.json) and [#1202 repair](evidence/permanent-parquet-1202.json) cover topology, properties, identities, manifests, catalog and derived adjacency. Sequential permanent allocation fell from 8,388,608 to 4,112,384 bytes; random from 8,929,280 to 7,585,792. | Four heterogeneous/route fixtures and current-format exact lifecycle tests supplement the property-free host ladder. Whole-test CPU/RSS observations are not isolated codec costs. |
| Membership representation | [#1203 packed membership](evidence/packed-membership-1203.json) removes reserved padding while preserving full-width identity, kind and tombstone information. | Physical savings depend on allocation rounding; exact current-format readers, recovery and corruption refusal remain required. |
| Manifest allocation | [#1204 bounded buckets](evidence/bounded-manifest-1204.json): the same 352-entry fixture uses 209 objects / 856,064 allocated bytes instead of 483 / 1,978,368; permanent allocation falls from 9,814,016 to 8,691,712. | Bounded authenticated bucket lookup replaces separate tiny objects. The earlier 544-entry assessment is a different fixture. |
| CSR representation | [#1205 bounded CSR](evidence/bounded-csr-1205.json): payload 4,920,564→1,324,020 bytes, allocation 4,972,544→1,363,968; cold CSR reads 35,552,246→9,577,462 bytes. | Whole-fixture RSS rises from 220,400 to 238,252 KiB; observed query time 3.855→3.891 s does not establish a latency gain. Decode windows remain bounded. |
| Every verified permanent publisher | [#1213 shared policy](evidence/permanent-parquet-1213.json) applies Zstd level 1 across the verified publishing paths; compaction Parquet allocation 569,344→335,872 bytes on its fixture. | Preserve dictionaries-off compaction, its 8,192-row groups, and justified local streaming/memory settings. Whole-process user CPU 11.89→12.41 s and RSS 156,284→159,492 KiB rise; syscall read/write and sampled temporary allocation fall. |
| Input predicate placement | [#1241](evidence/input-predicates-1241.json): fixed-path candidates 1,113,889→272, key rows 8,932,490→80,898; warm query median 3.850→0.254 s. | Observed CPU 31→2.1 s and RSS 153,640→101,544 KiB improve on this workload; physical filesystem input blocks are unchanged. |
| Property projection | [#1247](evidence/property-projection-1247.json): narrow property query median 290.6→27.8 ms; CPU 1.55→0.22 s, RSS 136,192→76,692 KiB. | Full-width median 293.6→309.2 ms is slower within its preselected envelope. Physical input blocks are unchanged; avoiding overlay materialization is not permission to skip authentication. |
| Qualified count row marker | [#1249 final implementation](evidence/count-row-marker-1249.json): count-all median 14.45→10.93 ms; affected counts avoid about 923.9 KB read and 308.8 KB write syscalls over five queries. | The explicit identity-count RSS control failed its original cap by 232 KiB. Fixed follow-up observations and the bounded disposition remain in the evidence; do not claim all final RSS caps passed. Nullable count, SUM and selective-property controls preserve their semantics. |

The [five-candidate assessment](evidence/query-maintenance-assessment-1207.json) also measured optimizer statistics, Parquet layout, property Bloom filters and fragmentation/maintenance. Statistics forwarding changed plans but missed the preselected CPU threshold across seven workloads and was reverted. A 128-row-group layout reduced sparse second-pass compressed column reads from 8,094,549 to 1,012,462 bytes, excluding mandatory validation, but doubled repeated-text allocation and increased dense second-pass reads; the global change was rejected. The 16 KiB page target was byte-identical on 30/30 files. Plain strings saved 45,056 bytes for random text but breached the RSS cap and grew repeated-text allocation. Bloom filters had no false negatives in the tested present-value probes but no admitted public consumer that could skip mandatory validation, so demonstrated public read savings were zero. Existing compaction folded runs and cleanup reclaimed retained generations; approximately 14 ms warm query latency stayed flat, and compaction missed its read-I/O budget in all three cases. These quantified decisions do not substitute for the implemented opportunities above.

## Capacity follow-up: consumed shaping roots (#1268)

[The frozen comparison](evidence/consumed-shaping-roots-1268.json) measures the same 8,192-node, 32,768/65,536/131,072-edge shaping fixture before and after retiring consumed identity and endpoint roots. Endpoint retirement precedes the final online carry insertion, after the final input run is durable and readers are closed. Original accepted chunks remain incomplete-shape recovery authority. The separately merged #1269 repair authenticates surviving derived payloads before recovery removes their controls.

| Edges | Baseline peak B | Candidate peak B | Reduction | Baseline retained B | Candidate retained B |
| --- | --- | --- | --- | --- | --- |
| 32,768 | 20,873,216 | 18,513,920 | 11.30% | 11,407,360 | 6,950,912 |
| 65,536 | 40,435,712 | 35,979,264 | 11.02% | 21,958,656 | 13,307,904 |
| 131,072 | 79,560,704 | 70,909,952 | 10.87% | 43,061,248 | 26,021,888 |

Three frozen runs reproduced each allocation value. Whole-fixture CPU was 4.55–4.78 seconds before and 4.73–4.89 after; maximum RSS was 49,872 and 49,976 KiB respectively. Both candidate diagnostic envelopes passed (5.24 seconds and 53,968 KiB), with a small observed CPU cost. Logical shape reads and merge writes were identical, as were GNU time filesystem inputs (1,693,576 blocks) and outputs (895,616 blocks). These are process observations, not hard memory limits or isolated algorithm timing. Receipt-accounted construction allocation excludes separately reported JSON control allocation; physical identities are counted once. This fixture ends after shaping and does not establish whole-lifecycle capacity or S26 admission.

The peak floor was selected from baseline identity-root bytes before implementation; the final deterministic test additionally retains the measured saving from deferring the final carry. Failed diagnostic publication, superseded first-candidate results, corrected test setup/oracles and the independently repaired corruption cause remain documented in the evidence. Public construction and four publishing-contract fixtures separately cover exact mutation/query/reopen/export/full verification/clean import/subsequent mutation. The integrated S20/S22 results below measure the merged follow-up; the September 11 receipts remain preserved as a historical comparison.

The first #1268 CI run exposed an aggregate-peak derivative false positive. [#1272 evidence](evidence/lifecycle-peak-envelope-1272.md) preserves that failed run and the stronger corrected growth envelope; the focused gate repair merged separately before this change was refreshed.

## Capacity follow-up: packed construction records (#1274)

[The frozen comparison](evidence/packed-construction-records-1274.json) uses the post-#1268 retirement implementation as its baseline. Current construction format 10 packs endpoint records from 48 to 33 bytes, resolved endpoints from 32 to 25, and shaped identities from 32 to 26. UUIDs, roles, retained markers and 64-bit surrogates remain exact. The permanent membership record remains 25 bytes. Earlier private construction formats and omitted old counters are refused; their readers and backfills are removed. Historical evidence retains its original format and measurements.

| Edges (8,192 nodes) | Baseline shaping peak B | Packed peak B | Logical shape reads B, before → after | Merge writes B, before → after |
| --- | --- | --- | --- | --- |
| 32,768 | 18,513,920 | 15,319,040 | 54,544,436 → 45,926,452 | 37,339,136 → 29,736,960 |
| 65,536 | 35,979,264 | 29,638,656 | 124,245,316 → 104,027,460 | 89,112,576 → 70,828,032 |
| 131,072 | 70,909,952 | 58,277,888 | 282,652,710 → 235,958,310 | 208,519,168 → 165,593,088 |

All three observations reproduced these exact values. Allocated shaping peaks fall 17.26–17.81%, logical shape reads 15.80–16.52%, and merge writes 20.36–20.59%. Retained construction allocation falls 6,950,912/13,307,904/26,021,888 → 6,246,400/11,948,032/23,351,296 bytes. Each deterministic peak ceiling was selected before implementation as the frozen baseline minus 14 bytes per edge; logical reads and writes must also fall at every scale. The maximum is measured across allocation transitions, not estimated by adding independently retained streams.

Across each full 1x/2x/4x invocation, baseline CPU observations were 4.73/4.89/4.79 seconds; packed observations were 4.43/4.76/4.63. Observed RSS maxima were 49,976 → 50,468 KiB, a 492 KiB increase. All candidate runs pass the preselected 5.21-second CPU and 54,072-KiB RSS diagnostic ceilings. Kernel process-accounted filesystem input blocks fall 1,693,576 → 1,509,928, and output blocks 895,616 → 747,776 in each run. These blocks are distinct from logical byte counters; RSS excludes page-cache/cgroup ownership and is not a hard memory bound. Observations were sequential, not randomized, and CPU ranges overlap; no causal latency improvement is claimed.

Both measured executables use Rust 1.96.0, the same lockfile and optimized release profile, and were copied and hashed before timing without overlapping builds. Candidate source is `5165ad4299bb70c97c64af5972b3efec54ca1abb`; executable SHA-256 is `ad3a8d19e86078b43e801cba6b77dcf625d39fffcb7c6a9a5d5e35c26afe4066`. The earlier debug executable and failed freeze-helper launch are explicitly excluded, with failed/superseded evidence retained in the comparison ledger.

Current-format tests cover exact packed bytes, full-width UUIDs and surrogates, every partial-record tail, invalid roles/kinds/markers, unsupported checkpoints before cleanup, canonical parent reuse, corruption, cancellation and crash recovery. The frozen storage suite passed 1,105 tests with two existing ignored tests; its only failure is the known #1192 hardcoded `/tmp` filesystem-class fixture on this host. Workspace Clippy, formatting, `make pre-push-fast` and `make gate-registry-check` passed. `cargo test -p graphforge-api --lib resumable_construction::` passed all five public lifecycle tests; `cargo test -p graphforge-api --test permanent_storage_budgets publishing_contract_` passed all four publishing-path contracts. Exact-head CI is recorded in #1274 before merge.

This paired fixture proves a material construction improvement. The merged host integration below separately measures the admitted S18/S19/S20/S22 ladder on the same host and reserve. No S24/S25/S26 workload or new certification workflow is introduced.

## Public publishing and correctness

The [current publishing contract](../book/architecture/storage.md#current-publishing-contract) and [#1221 evidence](evidence/publishing-contract-1221.json) cover public construction, ordinary mutation, property replay, compaction, ontology publication, projections, portable clean import and participant writers. All verified permanent Parquet publishers use the shared policy; replay and compaction retain it. Separate lifecycle implementations remain.

The four `permanent_storage_budgets` publishing-contract fixtures exercise flat/sharded and exploratory/ontology-promoted graphs through real construction or CREATE, property mutation, compaction, canonical topology mutation, reopen, export, full verification, clean import and subsequent mutation. Exact UUIDs, endpoints, routes, nullable properties and allocation continuation are compared. Adjacent recovery, cancellation, retained-stream, promotion and ownership regressions cover their applicable boundaries. Unsupported topology journals fail closed; no legacy reader or compatibility fallback was added.

#1257 fixes publication on the same facade: the selected workspace, property inventory, adjacency provider and ordinal resolver rotate together at stable paths. The strengthened public construction regression checks exact initial/child/replayed/reopened relationship tuples and counts, cancellation/retry, three real preparation-failure boundaries and retained lazy streams. #1249 adds exact 129-row property mutation/snapshot/reopen/export/verify/import/subsequent-mutation proof; #1253 preserves typed corruption refusal. Logical plans or wrapper tests are not the evidence for these claims.

## Final admitted host run

The existing `progressive_host_run` controller uses `--maximum-scale 22`, the unchanged `local-linux-cgroups-v2` profile and generator, and the original 141,258,578,535-byte reserve on OVHC-AGENCY. The native root is ext4, with 941,723,856,896 total bytes. Executables were built once from merged source, copied outside Cargo and SHA-256 frozen before any subprocess measurement; no build or other resource campaign overlapped this run. S22 may start only after S20 passes and the controller admits its measured headroom. No S24/S26 run or new certification workflow is used.

The executed build was `make -C benchmarks progressive-host-ladder-binaries HOST_TARGET_DIR=/home/ubuntu/code/graphforge-target-1195`, with `TMPDIR=/home/ubuntu/graphforge-native-tmp-1195` and `CARGO_BUILD_JOBS=4`. The API, execution, relational, IR and storage release packages were invalidated before building to avoid stale shared-target dependency metadata. The [summary](evidence/integrated-storage-1194.json) records the source tree, Rust version, lockfile digests and all three frozen executable hashes.

From `benchmarks`, the existing controller command was:

```bash
TMPDIR=/home/ubuntu/graphforge-native-tmp-1195 \
PYTHONPATH=harness uv run --locked python -m graphforge_bench.progressive_host_run \
  --maximum-scale 22 \
  --output-dir /home/ubuntu/graphforge-ladder/1194-710c6c64-evidence \
  --work-root /home/ubuntu/graphforge-ladder/1194-710c6c64-work \
  --gf /home/ubuntu/graphforge-ladder/1194-710c6c64-bin/gf \
  --certify /home/ubuntu/graphforge-ladder/1194-710c6c64-bin/graphforge-benchmark-certify \
  --generator /home/ubuntu/graphforge-ladder/1194-710c6c64-bin/graphforge-benchmark-graph500-generator \
  --benchexec-python /usr/bin/python3 \
  --reserved-headroom-bytes 141258578535
```

| Rung | Live edges | Wall s | CPU s | Process VmHWM B | Cgroup peak B | Retained union B | Lifecycle peak B |
| --- | --- | --- | --- | --- | --- | --- | --- |
| S18 | 4,194,304 | 87.884 | 79.439 | 183,107,584 | 819,089,408 | 1,065,082,880 | 2,293,444,608 |
| S19 | 8,388,608 | 175.969 | 160.435 | 189,415,424 | 1,576,611,840 | 2,159,181,824 | 4,586,803,200 |
| S20 | 16,777,216 | 359.113 | 327.439 | 199,135,232 | 3,117,494,272 | 4,372,508,672 | 9,173,499,904 |
| S22 | 67,108,864 | 1682.601 | 1551.141 | 262,049,792 | 12,473,958,400 | 17,813,299,200 | 36,760,567,808 |

| Rung | Logical reads B | Logical writes B | BenchExec reads B | BenchExec writes B |
| --- | --- | --- | --- | --- |
| S18 | 9,511,424,160 | 4,330,719,595 | 50,825,838,592 | 16,199,606,272 |
| S19 | 19,072,019,733 | 8,675,804,711 | 102,905,856,000 | 32,509,587,456 |
| S20 | 38,239,170,038 | 17,378,779,070 | 208,139,395,072 | 65,235,488,768 |
| S22 | 162,115,326,212 | 78,265,631,161 | 885,782,102,016 | 371,891,003,392 |

All four rungs passed all ten ordinary lifecycle phases. [Sanitized receipts](evidence/integrated-storage-1194/) and the [derived summary](evidence/integrated-storage-1194.json) retain exact byte values, identities and hashes. The shared native rung reader validated schemas, artifact hashes, phase success and equality with recomputed ordinary-receipt summaries. Frozen executable hashes were unchanged after the run.

Process `VmHWM` is distinct from BenchExec cgroup memory, which includes charged page cache. Logical application bytes are distinct from process/BenchExec read and write observations. Accounted Arrow/live-state budgets are not native RSS limits. Sampled peaks are observations, not hard bounds.

#1256 records disjoint public construction-call wall times for begin, resume, append, seal and publication. These successful receipt observations exclude source decoding, normalization, facade opening and import checkpoints outside those calls. Resume is not all recovery-authentication I/O, and the sum is not whole-ingest duration or CPU time. Returned errors are recorded by the live Rust handle; failed CLI commands and lost processes cannot reconstruct unreported durations. Timing observations never enter durable manifests. The existing receipts do not provide per-operation CPU or native-memory peaks; those measurements remain whole-process observations rather than inferred phase values.

| Rung | Operation | Calls | Wall s | Returned errors |
| --- | --- | --- | --- | --- |
| S20 | begin | 1 | 0.000947 | 0 |
| S20 | append | 272 | 16.411405 | 0 |
| S20 | seal | 1 | 168.962114 | 0 |
| S20 | resume | 1 | 0.821904 | 0 |
| S20 | publish | 1 | 7.857278 | 0 |
| S22 | begin | 1 | 0.000933 | 0 |
| S22 | append | 1,088 | 67.591800 | 0 |
| S22 | seal | 1 | 899.920035 | 0 |
| S22 | resume | 1 | 3.405283 | 0 |
| S22 | publish | 1 | 31.407244 | 0 |

| Rung | Whole-ingest wall s | Reopen wall s | Export wall s | Full verify wall s | Clean import wall s |
| --- | --- | --- | --- | --- | --- |
| S20 | 217.267 | 10.688 | 6.909 | 0.923 | 9.339 |
| S22 | 1104.101 | 42.453 | 27.688 | 3.867 | 37.633 |

## Physical ownership and historical comparison

| Rung | Allocation | Historical B | Final B | Reduction |
| --- | --- | --- | --- | --- |
| S20 | Selected project permanent | 1,852,637,184 | 681,910,272 | 63.19% |
| S20 | Retained lifecycle | 18,242,637,824 | 4,372,508,672 | 76.03% |
| S20 | Simultaneous lifecycle peak | 20,090,777,600 | 9,173,499,904 | 54.34% |
| S22 | Selected project permanent | 7,578,365,952 | 2,808,389,632 | 62.94% |
| S22 | Retained lifecycle | 73,638,645,760 | 17,813,299,200 | 75.81% |
| S22 | Simultaneous lifecycle peak | 81,197,961,216 | 36,760,567,808 | 54.73% |

| Rung | Allocation | Historical B/node | Final B/node | Historical B/edge | Final B/edge |
| --- | --- | --- | --- | --- | --- |
| S20 | Selected project permanent | 1766.812 | 650.320 | 110.426 | 40.645 |
| S20 | Retained lifecycle | 17397.535 | 4169.949 | 1087.346 | 260.622 |
| S20 | Simultaneous lifecycle peak | 19160.059 | 8748.531 | 1197.504 | 546.783 |
| S22 | Selected project permanent | 1806.823 | 669.572 | 112.926 | 41.848 |
| S22 | Retained lifecycle | 17556.821 | 4247.021 | 1097.301 | 265.439 |
| S22 | Simultaneous lifecycle peak | 19359.103 | 8764.402 | 1209.944 | 547.775 |

At S22, retained/current-project amplification is 9.717→6.343, while peak/current-project amplification is 10.714→13.090. Permanent output shrinks faster than the remaining peak, so the second ratio can rise despite the substantial absolute peak reduction. Observed whole-lifecycle wall time is 1954.185→1682.601 seconds and CPU is 1747.748→1551.141 seconds; these are single-run observations.

The [older 3868e3c7 baseline receipts](evidence/integrated-storage-1194/historical-3868e3c7/) and final run use identical generator/workload/host profiles. Both sides pass the shared native reader's schema, artifact-hash, identity and recomputed-summary checks. The interim 9312a470 run is retained honestly: it completed through S22 but lacked the operation timings and preceded the same-facade reader repair. A single run per source does not establish variance or isolate each repair's contribution. Historical logical I/O instrumentation differs, including recovery attribution; changes in those totals cannot be assigned solely to a new algorithmic cost.

Retained lifecycle allocation is a unique physical-file union. Component snapshots and named owners may refer to those same files, so their totals must not be added to the union. The final owner census is not a timestamped decomposition of an earlier simultaneous peak. Construction staging's retained allocation, its peak and the whole construction owner are different quantities.

| S22 retained owner | Historical B | Final B |
| --- | --- | --- |
| source-project-import | 3,289,366,528 | 3,289,370,624 |
| generated-inputs | 3,289,358,336 | 3,289,362,432 |
| source-project-construction | 44,344,586,240 | 2,823,782,400 |
| source-project-published | 7,578,365,952 | 2,808,389,632 |
| imported-project-published | 7,578,365,952 | 2,808,377,344 |
| portable-package | 7,558,496,256 | 2,793,910,272 |
| query-results | 98,304 | 98,304 |
| imported-project-transactions | 4,096 | 4,096 |
| source-project-transactions | 4,096 | 4,096 |

Construction staging peak is 44,315,299,840→30,154,256,384 bytes. The retained construction owner and the earlier staging maximum are not interchangeable: retiring intermediates removes later coexistence but cannot erase an already incurred shaping peak. The final lifecycle peak remains larger than final retained allocation by 18,947,268,608 bytes. The receipts do not provide a timestamped per-owner decomposition of that maximum.

| S22 selected-source category | Allocated B | Physical logical B |
| --- | --- | --- |
| adjacency | 0 | 0 |
| catalog_and_manifests | 1,339,392 | 367,277 |
| clean_imported_project | 0 | 0 |
| construction_staging | 0 | 0 |
| other | 0 | 0 |
| portable_package | 0 | 0 |
| properties | 0 | 0 |
| topology_edges | 1,263,366,144 | 1,261,294,928 |
| topology_nodes | 16,777,216 | 16,636,135 |
| uuid_and_surrogates | 1,514,545,152 | 1,514,517,330 |

The host graph is property-free and does not retain a built CSR; the separate property-bearing and indexed fixtures above prove those categories. Identity authorities remain a material permanent cost because forward/reverse lookups, exact external UUIDs and ordinal access have different consumers. Generated input, the import-owned copy, the portable package and source/imported publications remain distinct measured lifecycle owners. These retained artifacts are not proof of interchangeable authority or permission to remove a consumer's data.

| S22 construction I/O phase | Read B | Write B | Read calls | Write calls | Fsync calls |
| --- | --- | --- | --- | --- | --- |
| append_merge | 0 | 12,505,495,936 | 0 | 32,064 | 0 |
| cas_install_read_write | 2,799,486,209 | 2,796,149,993 | 49,173 | 46,160 | 13,341 |
| encode_write_postwrite_authentication | 23,278,760,707 | 6,081,760,278 | 652,669 | 173,475 | 2,388 |
| fsync_synchronization | 0 | 0 | 0 | 0 | 47,818 |
| hydration_verification | 2,960,596,381 | 168,145,111 | 45,397 | 2,568 | 19 |
| publication_preauthentication | 367,883 | 0 | 4 | 0 | 0 |
| recovery_reauthentication | 60,106,116,855 | 0 | 103,245 | 0 | 0 |
| seal_authentication | 0 | 0 | 0 | 0 | 0 |
| shape_consume_reauthentication | 72,969,998,177 | 56,714,079,843 | 1,407,101 | 73,789 | 0 |

From S20 to S22, live edges grow 4×, retained bytes 4.074×, simultaneous peak 4.007×, logical reads 4.240× and writes 4.504×. The source-bound deterministic 1x/2x/4x tests assert the configured work-window and phase ceilings; these host observations do not turn sampled process peaks into hard bounds. The construction constant-factor defect is materially reduced in absolute retained and simultaneous peak allocation, while shaping and authenticated recovery remain substantial costs and final capacity remains unresolved.

## Integrated capacity follow-up comparison

The [September 11 baseline](evidence/integrated-storage-1194/historical-24ca688c/integrated-storage-1194.json) and its complete receipts remain unchanged. Both runs use identical generator, workload and host-profile identities, verified with the native rung reader. These single observations measure the combined merged follow-up; they do not isolate each repair or establish CPU variance. Negative reductions indicate increases.

| Rung | Measure | 24ca688c B | 710c6c64 B | Reduction |
| --- | --- | --- | --- | --- |
| S20 | Selected permanent | 681,910,272 | 681,910,272 | 0.00% |
| S20 | Retained lifecycle | 4,372,516,864 | 4,372,508,672 | 0.00% |
| S20 | Lifecycle peak | 11,897,737,216 | 9,173,499,904 | 22.90% |
| S20 | Logical reads | 45,635,825,144 | 38,239,170,038 | 16.21% |
| S20 | Logical writes | 20,524,507,070 | 17,378,779,070 | 15.33% |
| S20 | BenchExec reads | 222,907,678,720 | 208,139,395,072 | 6.63% |
| S20 | BenchExec writes | 74,677,432,320 | 65,235,488,768 | 12.64% |
| S22 | Selected permanent | 2,808,385,536 | 2,808,389,632 | <0.01% increase |
| S22 | Retained lifecycle | 17,813,295,104 | 17,813,299,200 | <0.01% increase |
| S22 | Lifecycle peak | 47,657,119,744 | 36,760,567,808 | 22.86% |
| S22 | Logical reads | 193,069,289,732 | 162,115,326,212 | 16.03% |
| S22 | Logical writes | 92,215,886,265 | 78,265,631,161 | 15.13% |
| S22 | BenchExec reads | 947,578,322,944 | 885,782,102,016 | 6.52% |
| S22 | BenchExec writes | 414,307,926,016 | 371,891,003,392 | 10.24% |

| Rung | CPU s before → after | Wall s before → after | Process VmHWM B before → after | Cgroup peak B before → after |
| --- | --- | --- | --- | --- |
| S20 | 327.278 → 327.439 | 384.686 → 359.113 | 200,232,960 → 199,135,232 | 3,118,157,824 → 3,117,494,272 |
| S22 | 1551.682 → 1551.141 | 1775.591 → 1682.601 | 263,090,176 → 262,049,792 | 12,474,519,552 → 12,473,958,400 |

Projected S26 demand falls 762,866,827,264 → 588,524,115,286 bytes, a 174,342,711,978-byte reduction. Holding the old available-capacity sample fixed at 633,354,096,640 bytes and preserving the reserve gives a remaining projected deficit of 96,428,597,181 bytes. This fixed-capacity comparison separates product demand from cleanup.

The [owned-build reclamation ledger](evidence/integrated-storage-1194/owned-build-reclamation.json) records 624,598,843,392 → 699,686,653,952 available bytes, an observed increase of 75,087,810,560 bytes. Only the isolated regenerable development cache and explicitly identified unmeasured debug copies were removed after executable freezing; measured release binaries, raw logs, evidence and source worktrees were preserved. Fourteen registered worktrees and active process references were checked. This is a filesystem available-byte delta, not a sum of potentially overlapping objects; other filesystem activity can affect it. The 47,912,370,176-byte filesystem reserve and the separate 141,258,578,535-byte admission reserve are unchanged.

The summary retains the pre-cleanup freeze capacity. Final native qualification instead measures actual available capacity after all accepted rung workspaces are reclaimed. Its projected deficit is 30,096,125,885 bytes and spare capacity after the declared reserve is 0 bytes. Neither projection is an S26 execution.

## RSS admission diagnosis (#1278)

The [query RSS diagnosis](evidence/query-rss-1278.md) preserves the unchanged full
S24 refusal and exercises the public lifecycle plus 72 independently checked
query observations at S16/S17/S18. Allocation traces confirm that adjacency
rebuild merge readers consume 1 MiB per concurrently open run, alongside retained
accumulators and CSR serialization state. The 16/64-reader S20/S22 working-set
increase is a concrete repair target; it is an extrapolation from the smaller
traces, not a new S20/S22 measurement or a repaired admission pass. The historical
RSS refusal and #1278 remain open.

The [reader-buffer repair evidence](evidence/query-rss-1278-repair.md) records a
verified 1-MiB aggregate merge-reader budget, flat live-reader allocation across
S16/S17/S18, and all 135 successful small comparison commands. The new native
S18/S19/S20/S22 prefix passes with frozen executables containing both verified
storage repairs. S20–S22 process RSS rises 182,755,328 → 193,495,040 B (5.8766%);
full S24 admission passes all nine unchanged checks. No S24 workload ran. #1278
remains open, and PR #1281 is blocked by a separate retention-test CI failure.

## Acceptance ledger and remaining capacity

| #901 acceptance criterion | Direct evidence and outcome |
| --- | --- |
| One authoritative construction chunk budget (#979) | `million_edge_sink_uses_sixteen_durable_chunks_and_replays_stably` in `crates/graphforge-api/tests/scale_g500_ladder.rs`; real accepted chunks and replay. Final host receipts retain configured/submitted batch counts. |
| Linear phase rows/bytes with deterministic ceilings | `construction_application_reads_reconcile_and_scale_at_one_two_four` in `resumable_construction.rs` checks 4,096/8,192/16,384-node phase reconciliation. `equivalent_full_lifecycle_1x_2x_4x_has_bounded_metric_policies` exercises the actual complete lifecycle. |
| Resident topology bounded by work windows | `tiny_construction_ladder_resumes_and_scales_bounded_work_linearly` asserts configured batches/run/catalog windows and at most 64 MiB accounted live state, with a separate linear merge-disk bound. Process memory is observed separately in the final host table. |
| Physical owner reconciliation (#951) | `construction_lifecycle_multilevel_allocation_baseline` in `construction_lifecycle_tests.rs`, native lifecycle owner receipts and independent tiny union/owner inventories. The final table distinguishes retained union and historical simultaneous peak. |
| No complete prior-topology scan per batch | `journal_is_constant_control_state_and_seal_reopens_every_artifact`, `canonical_encoder_outputs_feed_ordinary_readers_index_and_adjacency`, `construction_append_publishes_current_complete_ordinal_authority`, and `reopen_recovers_surrogate_tails_without_full_topology_reads` assert direct scan/row boundaries. |
| Authenticated immediate/recovery consumers | #971/#1195 and `shape_inventory_and_evidence_commit_recover_without_double_counting`; #1257 installs current generation readers together. Immediate authority and durable recovery authentication remain distinct. |
| Interrupted resume, cancellation, prior authority and corruption refusal | `construction_session_reenters_across_processes`; the storage `supersession_crashes_reconcile_removed_allocations_and_replay`, `supersession_returned_errors_preserve_prior_authority_and_retry`, `supersession_corrupt_successors_and_replaced_predecessors_fail_closed`, `supersession_all_published_replay_paths_refuse_corrupt_public_payload`, and `supersession_cancellation_during_authentication_and_removal_is_recoverable` tests. |
| Real public construction/query/reopen/export/verify/import | #1221's four publishing paths, #1249's property/snapshot lifecycle, #1257's exact same-facade construction tuples and three failure/retry boundaries, and the final ordinary CLI host lifecycle. No wrapper-only or plan-only substitution. |
| Named-host S20, then admitted S22 with full measurements | Passed S18/S19/S20/S22 on merged source, in that order with native admission; exact receipts and observations above. Successful operation receipts record append/seal/resume wall times with the documented scopes; physical allocation, process memory, CPU and both logical/BenchExec I/O remain separately reported. |
| Required checks and merged work | Implementation PRs #1270, #1271, #1273 and #1275 passed exact-head CI Gate before squash merge; #1269, #1268, #1272 and #1274 are verified closed. Earlier linked implementation evidence records its exact commands and CI. #1276 remains the close gate for this focused evidence PR’s required exact-head CI, merge and verified closure. |

The epic's permanent encoding, representation, publishing-path correctness and five query-opportunity concerns are fulfilled by the merged evidence above. Final storage headroom/capacity remains a distinct outcome; no assessment-only or compression-only completion is claimed.

The existing [native S20/S22 qualification](evidence/integrated-storage-1194/s20-s22-storage-qualification.json) returns **refuse**: projected S26 lifecycle peak 588,524,115,286 bytes, available capacity 699,686,567,936 bytes and reserve 141,258,578,535 bytes. The deficit against available capacity after reserve is 30,096,125,885 bytes. Here the schema's `volume_bytes` field is measured **available** capacity, not the filesystem's total size. No S26 workload was executed.

At this available-capacity sample, preserving the reserve requires projected demand at or below 558,427,989,401 bytes: another 30,096,125,885 bytes of available capacity or a 5.11% reduction in projected demand would remove this planning deficit. Neither is demonstrated here. The final close gate still requires the existing ladder’s admitted S24/S25 observations and actual S26 evidence for at least one billion live persisted edges, the complete public lifecycle, reconciled identities and resource envelopes. Those rungs were not authorized or executed in this follow-up.

The native S20/S22 qualification is a capacity projection, not S26 execution or publication certification. Final capacity/S26 remains open in #1194/#900/#745: a lower-rung projection does not establish the final execution outcome. #901 is already closed; #1276 tracks this authorized follow-up evidence through required CI and merge.


## Lifecycle runtime investigation (#1279)

The [reproducible baseline](evidence/lifecycle-runtime-1279-baseline.json) is
produced by the native rung reader and the existing projection implementation:

```bash
PYTHONPATH=benchmarks/harness .venv/bin/python -m graphforge_bench.lifecycle_runtime \
  --evidence docs/development/evidence/integrated-storage-1194
```

Both receipts must pass schema, artifact-hash and recomputed-summary validation,
and their source, executable, generator, host and measurement-tool identities
must agree. The report uses the historical S22 disk sample, not current free
space. S24 projects to 6,975 seconds and passes time and work-rate headroom;
process RSS growth still refuses admission. S25 (14,031 seconds) and S26
(28,143 seconds) are diagnostic extrapolations only. Actual adjacent-rung
observations remain required. The 14,400-second limit and 20% time headroom
are unchanged.

At S20/S22, respectively, 23.213/101.776 seconds of ingest lie outside the
recorded construction calls. Another 9.489/35.009 seconds of whole-lifecycle
wall time lie outside summed phase durations. These residuals include
unmeasured work and timing precision; they are not isolated CPU costs.
Construction I/O, physical I/O, process RSS and cgroup memory retain separate
scopes in the report.

### External baseline attribution

[Host profiling observations](evidence/lifecycle-runtime-1279-profile.json)
retain executable and raw-artifact hashes. Frozen `710c6c64` executables ran
the unmodified ordinary S18 profile on OVHC-AGENCY, including exact source and
imported counts, canonical queries, full verification and clean import. These
direct diagnostic executions are not BenchExec ladder receipts and do not
admit any successor. All four diagnostic executions matched source/imported
query-result hashes.

| Probed baseline scope | Calls | Inclusive wall seconds |
| --- | ---: | ---: |
| Seal and prepare | 1 | 43.565 |
| Fixed-record merge groups | 16 | 11.411 |
| Parquet row merge groups | 4 | 15.838 |
| Canonical encoder | 1 | 6.315 |
| Explicit construction artifact authentication | 68 | 0.221 |
| Construction open, including recovery | 2 | 0.230 |
| Encoding inventory authentication | 3 | 0.527 |

The 198 entry/return events pair without unmatched events. Parent scopes
include children: **do not sum this table**. The seal-and-prepare residual
after subtracting observed child probes is 9.602 seconds; it includes remaining
shaping/control work and unprobed children. Authentication inside readers,
writers and merge groups remains inside those scopes. These observations do
not assert isolated CPU cost or attribute every lifecycle instruction.

Whole-lifecycle CPU sampling found SHA-256 compression at 15.60% of samples
inside `gf`, Zstd's double-fast compression routine at 4.62%, and construction
record encoding at 2.26%. The separate syscall trace observed 7,279 sync calls
inside import validation (2.802 seconds) and 893 inside commit (0.185 seconds).
Sync latency overlaps the enclosing commands and differs from kernel CPU time.
The 1×/2×/4× shaping fixture separately observed 9,590 sync calls, taking 1.109
seconds under tracing. Tracing changed that fixture's wall time from 6.03 to
13.14 seconds; traced times are not throughput measurements.

Profiles used `perf record -F 199 -e cpu-clock --call-graph dwarf`, timestamp-only
`perf probe --no-demangle` entry/return events on the frozen executable, and
`strace -f -ttt -T -e trace=fsync,fdatasync,execve`. The fixture syscall summary
also traces reads, writes and cache advice with `strace -f -c -w`. No graph
arguments were attached to probes. The temporary `gf1279` probes were removed
after collection. Raw traces remain on the host; checked-in evidence contains
only sanitized observations and hashes.

No cache drop was used. Sibling compilation overlapped portions of diagnostic
captures; our build was paused during entry/return capture. These are scoped
work observations, not a statistical overhead estimate or a controlled
before/after speedup. These diagnostic captures are not admission evidence.
The subsequent [integrated native prefix](evidence/query-rss-1278-repair.md)
contains both repairs and passes full S24 admission without executing S24.

### Verified repair: share decoded row sources

The measured Parquet row-merge path retained a `RecordBatch` clone for every
selected row and constructed one Arrow source descriptor per row per column.
The repair retains each decoded batch once per output window and stores
`(source_index, row_index)` selections. A per-cursor batch generation prevents
confusing successive decoder batches; flushing an output window clears cached
indices and retained sources. Output row/byte limits and all selected columns
remain unchanged. No authentication, merge pass, fsync or verification is
removed, and no public API or storage format changes.

The deterministic regression reduces 4,096 selections from one batch to one
retained source. Additional tests exercise interleaved inputs, sliced nullable
strings, decoder refills and output-window resets. The production fan-in test
measures actual fixed-run merges at 31/32/33 and 1,023/1,024/1,025 inputs, plus
the S20/S22 chunk counts. Its one-record-per-input model reads/writes 544
records for 272 inputs and 3,264 for 1,088 inputs: crossing the level boundary
adds real work. This is a model of that accumulator, not a claim that every
construction artifact family has identical pass counts.

[Three alternating fixture comparisons](evidence/lifecycle-runtime-1279-comparison.json)
use frozen optimized binaries with the same Rust toolchain and lockfile. All
observed compilers were stopped before the six measurements; there was no
manual cache drop. Median total CPU falls 4.69 → 3.90 seconds (16.84%); median
wall time falls 6.20 → 5.28 seconds (14.84%). All six 1×/2×/4× observations
preserve exact counts, logical shape reads, merge writes, merge passes and
storage peaks. This is representative shaping evidence, not a statistical
significance claim, whole-lifecycle speedup or higher-rung admission.

The repair reduces avoidable descriptor work but retains the bounded external
merge algorithm's additional passes. Do not apply the fixture speedup as a
multiplier to S25/S26 projections. Until a fresh comparable prefix is accepted,
the preserved S20/S22 admission results and the RSS refusal remain authoritative.

### Ordinary lifecycle correctness comparison

The [S18 baseline/candidate observations](evidence/lifecycle-runtime-1279-lifecycle.json)
run all ten ordinary lifecycle phases successfully using frozen release
executables. Both source and clean-imported projects return exactly 262,144
nodes and 4,194,304 relationships. All four count/query result hashes agree
between projects and executables. Reopen, export, full verification and clean
import pass. Every construction counter, including logical I/O, reader calls,
publication work, synchronization and storage peaks, is identical.

Baseline/candidate seal wall time is 41.243/35.371 seconds; ingest is
53.042/47.063 seconds; whole lifecycle is 88.65/82.55 seconds. Whole-process
user plus system CPU is 79.84/74.15 seconds. These nested observations must not
be added. This single sequential pair is diagnostic: competing processes
appeared in 20 one-second baseline samples (19 `gf-symbolized`, one `cargo`),
and no competing builder or
symbolized diagnostic appeared in candidate samples. No manual cache drop or
profiler was attached to either lifecycle process. The unchanged 4-GiB cgroup
limit and 14,400-second timeout applied to both. These are correctness and
diagnostic observations, not accepted ladder receipts or a controlled lifecycle
speedup claim.

Local validation passes the 20 runtime/progressive-admission tests, the 14
selected-row matching storage tests, and the real fan-in-boundary regression.
The initial full storage suite reports 1,108 passed, one failed and two existing
ignored tests. The failure is the existing `wave9_participant_inventory_rejects_links_and_special_files`
test tracked by #1192: it explicitly uses `/tmp` (tmpfs on this host), bypassing
the admitted ext4 `TMPDIR`. The unchanged frozen baseline reproduces the same
filesystem-admission failure. Running the full frozen suite with an ext4-backed
`/tmp` in a private mount namespace passes **1,109 tests, zero failed, two existing
ignored**. This includes the existing corruption, cancellation, recovery and
publication regressions. The host mounts and production admission checks are
unchanged; no assertion or test was disabled. The command, environment and both
log hashes are retained in the lifecycle summary. Workspace
Clippy, formatting, `make pre-push-fast`, and `make gate-registry-check` pass;
the full pre-push workflow is recorded separately and must not be inferred green
from these targeted checks.

Reproduction commands (the frozen binary variables resolve to the hashes in
the linked evidence; `TMPDIR` resolves to admitted ext4 storage):

```bash
PYTHONPATH=benchmarks/harness .venv/bin/python -m graphforge_bench.lifecycle_runtime --evidence docs/development/evidence/integrated-storage-1194
# Run from benchmarks/:
PYTHONPATH=harness ../.venv/bin/python -m unittest tests.test_lifecycle_runtime tests.test_progressive_qualification -q
# Run each frozen baseline/candidate storage executable three times, alternating:
/usr/bin/time -v "$FROZEN_STORAGE_TESTS" --exact graph_construction::tests::lifecycle_budget::consumed_shape_roots_have_bounded_multilevel_peak --nocapture --test-threads=1
CARGO_TARGET_DIR="$ISOLATED_TARGET" cargo test -p graphforge-storage --release selected_rows
CARGO_TARGET_DIR="$ISOLATED_TARGET" cargo test -p graphforge-storage --release fixed_merge_work_is_exact_across_production_fan_in_boundaries
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
make pre-push-fast
make pre-push
make gate-registry-check
```

The ordinary S18 command and its cgroup/timeout wrapper are retained in the
lifecycle comparison summary. Profile summaries retain raw-artifact hashes;
raw files remain on the designated host.
