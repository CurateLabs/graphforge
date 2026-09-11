# Integrated storage and query evidence for #1194

The epic implemented permanent-storage reductions, one permanent Parquet encoding policy, query execution improvements, and the lifecycle repairs needed to preserve exact public behavior. The final measurement source is `24ca688c516a86a68de9cffaab2d5a9215291256`, after the construction reader repair (#1257) and operation timing repair (#1256). The final S20/S22 lifecycle passes, but the existing capacity projection returns refuse; final capacity/S26 remains the explicit open outcome.

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

## Public publishing and correctness

The [current publishing contract](../book/architecture/storage.md#current-publishing-contract) and [#1221 evidence](evidence/publishing-contract-1221.json) cover public construction, ordinary mutation, property replay, compaction, ontology publication, projections, portable clean import and participant writers. All verified permanent Parquet publishers use the shared policy; replay and compaction retain it. Separate lifecycle implementations remain.

The four `permanent_storage_budgets` publishing-contract fixtures exercise flat/sharded and exploratory/ontology-promoted graphs through real construction or CREATE, property mutation, compaction, canonical topology mutation, reopen, export, full verification, clean import and subsequent mutation. Exact UUIDs, endpoints, routes, nullable properties and allocation continuation are compared. Adjacent recovery, cancellation, retained-stream, promotion and ownership regressions cover their applicable boundaries. Unsupported topology journals fail closed; no legacy reader or compatibility fallback was added.

#1257 fixes publication on the same facade: the selected workspace, property inventory, adjacency provider and ordinal resolver rotate together at stable paths. The strengthened public construction regression checks exact initial/child/replayed/reopened relationship tuples and counts, cancellation/retry, three real preparation-failure boundaries and retained lazy streams. #1249 adds exact 129-row property mutation/snapshot/reopen/export/verify/import/subsequent-mutation proof; #1253 preserves typed corruption refusal. Logical plans or wrapper tests are not the evidence for these claims.

## Final admitted host run

The existing `progressive_host_run` controller uses `--maximum-scale 22`, the unchanged `local-linux-cgroups-v2` profile and generator, and the original 141,258,578,535-byte reserve on OVHC-AGENCY. The native root is ext4, with 941,723,856,896 total bytes. Executables were built once from merged source, copied outside Cargo and SHA-256 frozen before any subprocess measurement; no build or other resource campaign overlapped this run. S22 may start only after S20 passes and the controller admits its measured headroom. No S24/S26 run or new certification workflow is used.

The executed build was `make -C benchmarks progressive-host-ladder-binaries HOST_TARGET_DIR=/home/ubuntu/code/graphforge-target-1195`, with `TMPDIR=/home/ubuntu/graphforge-native-tmp-1195` and `CARGO_BUILD_JOBS=4`. The API, execution, relational, IR and storage release packages were invalidated before building to avoid stale shared-target dependency metadata. The [summary](evidence/integrated-storage-1194.json) records the source tree, Rust version, lockfile digests and all three frozen executable hashes.

From `benchmarks`, the existing controller command was:

```bash
PYTHONPATH=harness uv run --locked python -m graphforge_bench.progressive_host_run \
  --maximum-scale 22 \
  --output-dir /home/ubuntu/graphforge-ladder/1194-24ca688c-evidence \
  --work-root /home/ubuntu/graphforge-ladder/1194-24ca688c-work \
  --gf /home/ubuntu/graphforge-ladder/1194-24ca688c-bin/gf \
  --certify /home/ubuntu/graphforge-ladder/1194-24ca688c-bin/graphforge-benchmark-certify \
  --generator /home/ubuntu/graphforge-ladder/1194-24ca688c-bin/graphforge-benchmark-graph500-generator \
  --benchexec-python /usr/bin/python3 \
  --reserved-headroom-bytes 141258578535
```

| Rung | Live edges | Wall s | CPU s | Process VmHWM B | Cgroup peak B | Retained union B | Lifecycle peak B |
| --- | --- | --- | --- | --- | --- | --- | --- |
| S18 | 4,194,304 | 96.245 | 80.258 | 182,611,968 | 819,265,536 | 1,065,082,880 | 2,974,519,296 |
| S19 | 8,388,608 | 191.478 | 161.155 | 188,862,464 | 1,577,000,960 | 2,159,190,016 | 5,948,940,288 |
| S20 | 16,777,216 | 384.686 | 327.278 | 200,232,960 | 3,118,157,824 | 4,372,516,864 | 11,897,737,216 |
| S22 | 67,108,864 | 1775.591 | 1551.682 | 263,090,176 | 12,474,519,552 | 17,813,295,104 | 47,657,119,744 |

| Rung | Logical reads B | Logical writes B | BenchExec reads B | BenchExec writes B |
| --- | --- | --- | --- | --- |
| S18 | 11,360,063,649 | 5,117,151,595 | 54,517,145,600 | 18,559,918,080 |
| S19 | 22,770,347,285 | 10,248,668,711 | 110,290,894,848 | 37,230,034,944 |
| S20 | 45,635,825,144 | 20,524,507,070 | 222,907,678,720 | 74,677,432,320 |
| S22 | 193,069,289,732 | 92,215,886,265 | 947,578,322,944 | 414,307,926,016 |

All four rungs passed all ten ordinary lifecycle phases. [Sanitized receipts](evidence/integrated-storage-1194/) and the [derived summary](evidence/integrated-storage-1194.json) retain exact byte values, identities and hashes. The shared native rung reader validated schemas, artifact hashes, phase success and equality with recomputed ordinary-receipt summaries. Frozen executable hashes were unchanged after the run.

Process `VmHWM` is distinct from BenchExec cgroup memory, which includes charged page cache. Logical application bytes are distinct from process/BenchExec read and write observations. Accounted Arrow/live-state budgets are not native RSS limits. Sampled peaks are observations, not hard bounds.

#1256 records disjoint public construction-call wall times for begin, resume, append, seal and publication. These successful receipt observations exclude source decoding, normalization, facade opening and import checkpoints outside those calls. Resume is not all recovery-authentication I/O, and the sum is not whole-ingest duration or CPU time. Returned errors are recorded by the live Rust handle; failed CLI commands and lost processes cannot reconstruct unreported durations. Timing observations never enter durable manifests. The existing receipts do not provide per-operation CPU or native-memory peaks; those measurements remain whole-process observations rather than inferred phase values.

| Rung | Operation | Calls | Wall s | Returned errors |
| --- | --- | --- | --- | --- |
| S20 | begin | 1 | 0.000955 | 0 |
| S20 | append | 272 | 16.737041 | 0 |
| S20 | seal | 1 | 177.088074 | 0 |
| S20 | resume | 1 | 0.975587 | 0 |
| S20 | publish | 1 | 9.436728 | 0 |
| S22 | begin | 1 | 0.000920 | 0 |
| S22 | append | 1,088 | 68.519419 | 0 |
| S22 | seal | 1 | 923.237336 | 0 |
| S22 | resume | 1 | 3.811642 | 0 |
| S22 | publish | 1 | 38.586566 | 0 |

| Rung | Whole-ingest wall s | Reopen wall s | Export wall s | Full verify wall s | Clean import wall s |
| --- | --- | --- | --- | --- | --- |
| S20 | 228.672 | 12.622 | 7.901 | 0.973 | 10.841 |
| S22 | 1139.389 | 51.299 | 31.842 | 4.474 | 43.675 |

## Physical ownership and historical comparison

| Rung | Allocation | Historical B | Final B | Reduction |
| --- | --- | --- | --- | --- |
| S20 | Selected project permanent | 1,852,637,184 | 681,910,272 | 63.19% |
| S20 | Retained lifecycle | 18,242,637,824 | 4,372,516,864 | 76.03% |
| S20 | Simultaneous lifecycle peak | 20,090,777,600 | 11,897,737,216 | 40.78% |
| S22 | Selected project permanent | 7,578,365,952 | 2,808,385,536 | 62.94% |
| S22 | Retained lifecycle | 73,638,645,760 | 17,813,295,104 | 75.81% |
| S22 | Simultaneous lifecycle peak | 81,197,961,216 | 47,657,119,744 | 41.31% |

| Rung | Allocation | Historical B/node | Final B/node | Historical B/edge | Final B/edge |
| --- | --- | --- | --- | --- | --- |
| S20 | Selected project permanent | 1766.812 | 650.320 | 110.426 | 40.645 |
| S20 | Retained lifecycle | 17397.535 | 4169.957 | 1087.346 | 260.622 |
| S20 | Simultaneous lifecycle peak | 19160.059 | 11346.566 | 1197.504 | 709.160 |
| S22 | Selected project permanent | 1806.823 | 669.571 | 112.926 | 41.848 |
| S22 | Retained lifecycle | 17556.821 | 4247.021 | 1097.301 | 265.439 |
| S22 | Simultaneous lifecycle peak | 19359.103 | 11362.343 | 1209.944 | 710.146 |

At S22, retained/current-project amplification is 9.717→6.343, while peak/current-project amplification is 10.714→16.970. Permanent output shrinks faster than the remaining peak, so the second ratio can rise despite the substantial absolute peak reduction. Observed whole-lifecycle wall time is 1954.185→1775.591 seconds and CPU is 1747.748→1551.682 seconds; these are single-run observations.

The [older 3868e3c7 baseline receipts](evidence/integrated-storage-1194/historical-3868e3c7/) and final run use identical generator/workload/host profiles. Both sides pass the shared native reader's schema, artifact-hash, identity and recomputed-summary checks. The interim 9312a470 run is retained honestly: it completed through S22 but lacked the operation timings and preceded the same-facade reader repair. A single run per source does not establish variance or isolate each repair's contribution. Historical logical I/O instrumentation differs, including recovery attribution; changes in those totals cannot be assigned solely to a new algorithmic cost.

Retained lifecycle allocation is a unique physical-file union. Component snapshots and named owners may refer to those same files, so their totals must not be added to the union. The final owner census is not a timestamped decomposition of an earlier simultaneous peak. Construction staging's retained allocation, its peak and the whole construction owner are different quantities.

| S22 retained owner | Historical B | Final B |
| --- | --- | --- |
| source-project-import | 3,289,366,528 | 3,289,370,624 |
| generated-inputs | 3,289,358,336 | 3,289,362,432 |
| source-project-construction | 44,344,586,240 | 2,823,782,400 |
| source-project-published | 7,578,365,952 | 2,808,385,536 |
| imported-project-published | 7,578,365,952 | 2,808,377,344 |
| portable-package | 7,558,496,256 | 2,793,910,272 |
| query-results | 98,304 | 98,304 |
| imported-project-transactions | 4,096 | 4,096 |
| source-project-transactions | 4,096 | 4,096 |

Construction staging peak is 44,315,299,840→41,050,935,296 bytes. The retained construction owner and the earlier staging maximum are not interchangeable: retiring intermediates removes later coexistence but cannot erase an already incurred shaping peak. The final lifecycle peak remains larger than final retained allocation by 29,843,824,640 bytes. The receipts do not provide a timestamped per-owner decomposition of that maximum.

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
| uuid_and_surrogates | 1,514,541,056 | 1,514,517,330 |

The host graph is property-free and does not retain a built CSR; the separate property-bearing and indexed fixtures above prove those categories. Identity authorities remain a material permanent cost because forward/reverse lookups, exact external UUIDs and ordinal access have different consumers. Generated input, the import-owned copy, the portable package and source/imported publications remain distinct measured lifecycle owners. These retained artifacts are not proof of interchangeable authority or permission to remove a consumer's data.

| S22 construction I/O phase | Read B | Write B | Read calls | Write calls | Fsync calls |
| --- | --- | --- | --- | --- | --- |
| append_merge | 0 | 14,518,761,856 | 0 | 34,112 | 0 |
| cas_install_read_write | 2,799,486,209 | 2,796,149,993 | 49,173 | 46,160 | 13,341 |
| encode_write_postwrite_authentication | 25,501,741,827 | 6,081,760,278 | 654,795 | 173,882 | 2,388 |
| fsync_synchronization | 0 | 0 | 0 | 0 | 51,301 |
| hydration_verification | 2,960,596,381 | 168,145,111 | 45,397 | 2,568 | 19 |
| publication_preauthentication | 367,883 | 0 | 4 | 0 | 0 |
| recovery_reauthentication | 74,945,564,407 | 0 | 116,501 | 0 | 0 |
| seal_authentication | 0 | 0 | 0 | 0 | 0 |
| shape_consume_reauthentication | 86,861,533,025 | 68,651,069,027 | 1,421,617 | 87,238 | 0 |

From S20 to S22, live edges grow 4×, retained bytes 4.074×, simultaneous peak 4.006×, logical reads 4.231× and writes 4.493×. The source-bound deterministic 1x/2x/4x tests assert the configured work-window and phase ceilings; these host observations do not turn sampled process peaks into hard bounds. The construction constant-factor defect is materially reduced in absolute retained and simultaneous peak allocation, while shaping and authenticated recovery remain substantial costs and final capacity remains unresolved.

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
| Required checks and merged work | Implementation PRs #1258 and #1259 passed exact-head CI Gate before squash merge; their issues are verified closed. Earlier linked implementation evidence records its exact commands and CI. Completion also requires this focused evidence PR to pass the exact-head CI Gate and merge; #901 records that outcome. |

The epic's permanent encoding, representation, publishing-path correctness and five query-opportunity concerns are fulfilled by the merged evidence above. Final storage headroom/capacity remains a distinct outcome; no assessment-only or compression-only completion is claimed.

The existing [native S20/S22 qualification](evidence/integrated-storage-1194/s20-s22-storage-qualification.json) returns **refuse**: projected S26 lifecycle peak 762,866,827,264 bytes, available capacity 633,354,096,640 bytes and reserve 141,258,578,535 bytes. The deficit against available capacity after reserve is 270,771,309,159 bytes. Here the schema's `volume_bytes` field is measured **available** capacity, not the filesystem's total size. No S26 workload was executed.

The native S20/S22 qualification is a capacity projection, not S26 execution or publication certification. Final capacity/S26 remains open in #1194/#900/#745 because the projection is refused and S26 is unproved. #901 closes only after its construction/lower-rung outcomes and this focused evidence PR pass their required checks and merge.
