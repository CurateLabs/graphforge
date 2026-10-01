# Cypher read-path operator inventory and comparison protocol (#1687 / #1619)

**Status:** recorded before any #1619 candidate is measured (#1688) or decided (#1689).  
**Baseline revision:** `666c8c0cb` (`origin/main` when this branch was created). Line citations are to the commit that adds this document; it differs from the baseline in one doc comment in `crates/graphforge-exec/src/expand_exec.rs`, which moves that file's later lines up by one.  
**Parent issue:** [#1619](https://github.com/CurateLabs/graphforge/issues/1619). This is the read-path sibling of #1504; §6 reuses its protocol rather than restating it.

#1688 must cite this document. It may add detail, but it must not add speedup thresholds, caps or exclusion rules after seeing results. Any protocol change needs a §7 changelog entry that says why.

## 1. Baseline

| Crate | Workspace pin | Locked version |
| --- | --- | --- |
| `datafusion` / `datafusion-datasource` | `54` | `54.1.0` |
| `arrow` / `arrow-data` / `parquet` | `58` | `58.4.0` |

Upstream reference: [DataFusion 54.1.0](https://docs.rs/datafusion/54.1.0/datafusion/). It covers `ExecutionPlan`, `PhysicalOptimizerRule`, `ExtensionPlanner`, `MemoryConsumer`, `HashJoinExec`, `SortExec` top-K and `RecursiveQueryExec`.

## 2. How a Cypher read executes today

There is a single executor, and it is DataFusion. The AST goes through `graphforge-ir` and is lowered by `graphforge-rel` into a DataFusion `LogicalPlan`. That plan uses stock builder nodes, plus `Extension` nodes defined in `graphforge-plan`. `graphforge-exec` then plans and runs it:

- **Session.** A DataFusion `SessionContext` is built with `SessionStateBuilder` (`crates/graphforge-exec/src/session.rs:707`). It has:
  - query planner `GraphForgeQueryPlanner` (`session.rs:277`), wrapping DataFusion's default physical planner with `GraphForgeExtensionPlanner` (`session.rs:171`);
  - logical rules `graphforge_rel::input_predicates::optimizer_rules()`, which are the DataFusion defaults plus `FixedExpandInputPredicates`;
  - physical rules `FixedHopDemandRule` and `SortRunCoalesceRule`.
- **Execution.** `execute_plan_with_params` (`session.rs:1282`) and `execute_plan_stream` (`session.rs:1418`) run the physical plan through DataFusion (`execute_stream` at `session.rs:1427`).
- **Memory.** A DataFusion `RuntimeEnv` pool bounds execution memory (`session.rs:357`), with a 512 MiB default (`session.rs:338`; API default at `crates/graphforge-api/src/resource_policy.rs:188`). Spill goes only to a configured directory, and is capped only when a limit is given (`session.rs:358`–`:363`). Otherwise the disk manager is disabled. The API turns spill on by default: a durable project spills into its scratch directory, capped at 8 GiB (`crates/graphforge-api/src/runtime_ownership.rs:50`–`:57`, `crates/graphforge-storage/src/query_spill.rs:43`).

Scope: `graphforge-exec` has 93,018 source lines at the baseline. 69,329 of them are the analyst `algorithm_*` modules, which bypass Cypher and are a #1619 non-goal. The read-path operators, rewrites, demand machinery and adjacency provider come to about 12k lines, including in-file tests. The write driver is also a non-goal.

## 3. Operator inventory

"Pool" means the operator registers a `MemoryConsumer` with the session memory pool. Only two read-path operators do so: `SortRunCoalesceExec` (`crates/graphforge-exec/src/sort_runs.rs:167`) and path hydration (`crates/graphforge-exec/src/path_hydration.rs:200`).

| Operator class | Current implementation | Stock DataFusion candidate | Invariant that makes it custom | Pool |
| --- | --- | --- | --- | --- |
| Node scan | Stock `TableScan` over `TopologyNodeTable` (`crates/graphforge-storage/src/catalog/providers.rs:50`), executed by custom `GraphForgeParquetExec` (`crates/graphforge-storage/src/parquet_scan.rs:245`). Multi-file tables are merged serially by `OrderedPartitionStreamExec` (`parquet_scan.rs:551`). | `DataSourceExec` over Parquet | Scans a fixed, authenticated fragment list in canonical fragment order. Topology normalisation. `is_complete_node_id_scan` (`parquet_scan.rs:98`) relies on it to prove the frontier is complete. | No |
| Edge scan | Stock `TableScan` over `TypedEdgeTable` / `UnionEdgeTable` (`providers.rs:128`, `:191`), executed by `GraphForgeParquetExec` | `DataSourceExec` | Same as node scan | No |
| Property read | `PropertyTable` / `EdgePropertyTable` (`providers.rs:445`, `:411`), executed by `PropertyOverlayExec` (`crates/graphforge-storage/src/property_scan.rs:144`). Joined to scans with a stock left join (`crates/graphforge-rel/src/lowerer/scans.rs:169`). | `DataSourceExec` plus stock join | Authenticated newest-wins overlay reads | No |
| Filter, project, WITH | Stock (`crates/graphforge-rel/src/lowerer.rs:1488`, `:1500`, `lower_with_op` `:1128`) | — (already stock) | — | Stock |
| One hop, project-backed | `ExpandNode` (`crates/graphforge-plan/src/lib.rs:467`), lowered at `crates/graphforge-rel/src/lowerer/traversal.rs:486`, planned by `plan_expand_extension` (`session.rs:131`) into `ExpandExec` (`crates/graphforge-exec/src/expand_exec.rs:1314`). Chosen whenever a provider target exists, the relationship is not already bound, and its type is known (`traversal.rs:432`–`:441`). | Inner `HashJoinExec` from node to edge to node. This is already implemented as the relational lowering (`traversal.rs:633`, `:665`). It is used whenever the provider path declines (no target, a bound relationship, or an unknown type), and when a `differential-testing` build sets `relational_reference` (`traversal.rs:220`, `:255`). | Uses CSR adjacency (`AdjacencyProvider`, a `SessionConfig` extension at `session.rs:122`) instead of joining the whole edge table. Resumable within a high-degree row. `with_fetch` and demand cancellation. Generation-pinned ordinal-to-UUID identity. Narrows output to the demanded columns. | No |
| One hop, undirected | Provider path: `ExpandExec` reads the merged adjacency view and removes duplicates within each source row by edge id (`expand_exec.rs:1636`–`:1637`). Join path: stock union of Out and In legs, with self-loops filtered from the In leg instead of a `Distinct` (`traversal.rs:298`–`:401`, filter at `:353`). Both paths emit a self-loop once. | Union of two hash joins, which is the current join path | Same as one hop | No |
| Fixed multi-hop | One `ExpandExec` per hop, chained | Chained hash joins | Same as one hop | No |
| Variable-length | `VarLenExpandNode` (`lib.rs:251`, lowered at `traversal.rs:110`) planned into `VarLenExpandExec` (`expand_exec.rs:167`). Collects its whole input (`expand_exec.rs:230`) and then runs BFS. Optionally wrapped by pass-through `OntologyInferExec` (`expand_exec.rs:291`). | `RecursiveQueryExec` (recursive CTE) | Relationship isomorphism (no repeated edge per path), edge-list output, `*min..max` bounds. DataFusion 54's recursive CTE applies neither per-path uniqueness nor list output (inferred from its documented semantics; not tested here). | No |
| OPTIONAL MATCH | `OptionalMatchNode` (`lib.rs:635`, lowered at `crates/graphforge-rel/src/lowerer/nested_queries.rs:465`) planned into `OptionalMatchExec` (`crates/graphforge-exec/src/row_exec.rs:98`). Collects both sides (`row_exec.rs:155`–`:156`). | Left `HashJoinExec` plus projection | Cypher null-shaping keyed on shared variables. It is not known whether a stock left join matches it on every TCK scenario; untested. | No |
| EXISTS, pattern comprehension | Stock left semi, left anti and left joins (EXISTS at `nested_queries.rs:46`, `:100`; pattern comprehension at `:180`) | — (already stock) | — | Stock |
| UNWIND | `UnwindNode` (`lib.rs:1617`, lowered at `lowerer.rs:821` and `nested_queries.rs:354`) planned into `UnwindExec` (`row_exec.rs:370`). Collects its input (`row_exec.rs:426`). | `UnnestExec` | Cypher list semantics over heterogeneous list values; untested against stock `unnest` | No |
| Aggregate | Stock `AggregateExec` (`lowerer.rs:1577`) | — (already stock) | — | Stock |
| Edge count `count(r)` | `EdgeCountExec` (`crates/graphforge-exec/src/edge_count.rs:277`), substituted by rewrite R1 (§4) | Stock `AggregateExec` over the expand or join | O(1) answer from the adjacency edge-entry count, valid only when the frontier is complete | No |
| ORDER BY | Stock `SortExec` (`lowerer.rs:1717`), fed by custom `SortRunCoalesceExec` (`sort_runs.rs:128`) | `SortExec` alone | Works around DataFusion's external-sort merge-reservation shortfall (#1591) | Yes |
| ORDER BY dest uuid LIMIT k, one hop | `OrderedOneHopExec` (`crates/graphforge-exec/src/ordered_one_hop.rs:214`), substituted by rewrite R2 | `SortExec` top-K over the expand or join | When the session's identity authority confirms node-ordinal order equals UUID order (`crates/graphforge-storage/src/ordinal_identity_v4.rs:825`, checked per session), destinations can be walked in order and stopped at k without materialising every path | No |
| ORDER BY dest uuid LIMIT k, two hops | `OrderedTwoHopPathCountExec` (`crates/graphforge-exec/src/ordered_two_hop.rs:230`), substituted by rewrite R3 | `SortExec` top-K over two expands or joins | Same ordinal invariant, with path multiplicity counted per destination | No |
| LIMIT, SKIP | Stock (`lowerer.rs:1743`, `:1753`). A terminal fixed-hop limit is wrapped by `DemandGuardExec` (`crates/graphforge-exec/src/demand.rs:1601`). | `GlobalLimitExec` alone | Lets a terminal limit cancel traversal work early | No |
| Cartesian product | Stock `cross_join` (`lowerer.rs:712`) | — (already stock) | — | Stock |
| Relationship uniqueness | Stock filter using the `cypher_relationship_disjoint` UDF (`lowerer.rs:792`; UDF at `crates/graphforge-rel/src/expr.rs:2111`) | — (already stock) | — | Stock |
| UNION | Stock `union` (`nested_queries.rs:11`) | — (already stock) | — | Stock |
| CALL procedure | Fixture rows joined in with a stock join (`lowerer.rs:1011`) | — (already stock) | — | Stock |
| CALL { subquery } | Rejected at bind time (`crates/graphforge-ir/src/binder.rs:352`) | — | Not supported | — |
| shortestPath | Only recognised as a function name (`crates/graphforge-ir/src/binder/expressions.rs:1114`). No planner or executor path was found. | — | Not supported (inferred) | — |
| Diagnostics | `ProbeExec` (`demand.rs:1485`), `RssProbeExec` (`demand.rs:1038`) | — | Evidence capture only. Never used to bound anything. | No |

Two logical nodes are defined but never planned: `PathUniqueNode` (`lib.rs:698`) and `GraphMergeNode` (`lib.rs:840`). `GraphForgeExtensionPlanner` has no branch for either (`session.rs:171`–`:270`). MERGE runs through the write driver, which is out of scope.

## 4. Rewrite rules

| Rule | Location | What it gates on | When the gate fails |
| --- | --- | --- | --- |
| `FixedExpandInputPredicates` (logical) | `crates/graphforge-rel/src/input_predicates.rs:100`, inserted after `push_down_filter` | Total, deterministic predicates over qualified, unchanged input fields | The predicate stays above the expand. The result is correct, only slower. |
| `FixedHopDemandRule` (physical) | `crates/graphforge-exec/src/demand.rs:869` | Column-demand narrowing (`rewrite_materialization`), then R3, R2 and R1 in that order (`demand.rs:881`–`:883`), then demand batch goals and `DemandGuardExec` for a terminal limit | Each rewrite returns the plan unchanged |
| R1 edge count | `try_rewrite_edge_count` (`edge_count.rs:40`), `detect_edge_count` (`:184`) | After peeling transport operators: a global, non-DISTINCT, unfiltered, unordered `count` (Single mode, or a matched Partial/Final pair) over one `ExpandExec`, with only coalesce, repartition and projection nodes between them (`edge_count.rs:90`–`:101`). The count argument must be a non-null literal, the matched edge identity column, or the row marker (`edge_count.rs:167`–`:180`). Direction must be Out, and `has_complete_frontier` (`edge_count.rs:105`) must hold: no fetch, and the source traces through column-only projections to `is_complete_node_id_scan`. | **Silent.** The generic `ExpandExec` plus stock aggregate runs instead (`edge_count.rs:43`–`:44`). |
| R2 ordered one hop | `try_rewrite_ordered_one_hop` (`ordered_one_hop.rs:37`), `detect_ordered_one_hop` (`:104`) | A single-column `node_uuid` projection over an ascending, fetch-bearing `SortExec` (optionally under `SortPreservingMergeExec`) over exactly one `ExpandExec`, which must be destination-identity-only with direction Out. Needs an ordinal identity session with `uuid_order_matches_ordinals`, and a complete frontier. | **Silent.** The generic expand plus stock sort top-K runs instead. |
| R3 ordered two hop | `try_rewrite_ordered_two_hop` (`ordered_two_hop.rs:41`), `detect_ordered_two_hop` (`:108`) | As R2, but over two stacked expands with the same relation type and direction Out. The first expand must be the **direct** child of the second (`ordered_two_hop.rs:153`); no operator may sit between them. The first expand is intermediate-topology-only and the second destination-identity-only. An optional `cypher_relationship_disjoint` filter is accepted. | **Silent.** The generic expands plus stock sort top-K run instead. |
| `SortRunCoalesceRule` (physical) | `crates/graphforge-exec/src/sort_runs.rs:72` | A `SortExec` without a fetch whose input is not already coalesced | The sort runs unmodified |

R1–R3 recognise a DataFusion **physical** plan shape. Any change to session configuration, partitioning or operator properties can alter that shape: #1466 raised `target_partitions`, and DataFusion inserted a round-robin exchange. When that happens, the fast path disappears without an error. `regression1513_fast_paths_survive_multi_file_node_tables` (`crates/graphforge-api/tests/fixed_hop_limit.rs:451`) guards the known shapes: multi-file node tables at `target_partitions` of 1, the node file count, and the widest value. It asserts that `EdgeCountExec`, `OrderedOneHopExec` and `OrderedTwoHopPathCountExec` appear in `explain` output, and that `operator_rss` records `edge_count`, `ordered_one_hop` and `ordered_two_hop` (labels at `demand.rs:952`–`:967`). It cannot guard shapes nobody has thought of.

The peel helpers pass through coalesce, repartition and column projection. A repartition above the node scan is therefore traced through by `has_complete_frontier` (`edge_count.rs:114`–`:118`) and does **not** defeat R1–R3. An operator between the two expands does defeat R3: it falls back silently.

## 5. Candidate matrix

#1619 prototypes only the fixed-hop and edge-count paths:

| Candidate | Fixed hop | Edge count | ORDER BY … LIMIT one/two hop | What it tests |
| --- | --- | --- | --- | --- |
| **A. Current** | `ExpandExec` | R1 to `EdgeCountExec` | R2/R3 to ordered operators | Baseline |
| **B. Stock** | Relational hash-join lowering (`traversal.rs:220`, `:633`, `:665`) | Stock `AggregateExec` | Stock `SortExec` top-K | Whether DataFusion's stock operators are enough, and what the custom ones buy |
| **C. Structural hybrid** | `ExpandExec`, registered with the memory pool | Chosen by the lowerer from the Graph IR as a logical node | Chosen by the lowerer as logical nodes | Whether choosing fast paths at logical time removes the silent-fallback class while keeping A's cost |

Operators that are not prototyped (variable-length, OPTIONAL MATCH, UNWIND, `SortRunCoalesceExec`, the scans) keep their §3 stock candidate as a recorded option. #1689 may mark them **not evaluated**, with a revisit trigger. It must not mark them retained or rejected without their own evidence. An inapplicability claim needs specific, independently reviewed proof, as #1505 §4 requires.

## 6. Comparison protocol

This protocol is recorded before #1688 runs any measurement.

**Reused from #1505.** The following sections of [`construction-reuse-inventory-protocol-1505.md`](construction-reuse-inventory-protocol-1505.md) apply unchanged:
- §5.2 resource envelopes: equal and recorded CPUs and memory; reservation bound, RSS and page cache reported separately;
- §5.3 repetitions and uncertainty: alternating matched pairs, at least three accepted observations per mode, median and observed range, no universal speedup threshold;
- §5.4 wall time and CPU seconds, with [`benchmarking.md`](benchmarking.md) as the authority;
- §5.6 maintenance rubric;
- §5.7 adopt / retain / hybrid vocabulary.

The construction workloads and correctness gates (§5.1, §5.5) do not apply. The read-path replacements follow.

### 6.1 Workloads

- **Inputs.** Graph500 at S18, S19 and S20, edge factor 16, seed `13907095936298285200`. They are generated with `graphforge-benchmark-graph500-generator` as pinned in `benchmarks/profiles/graph500/s18-local.json`, `s19-local.json` and `s20-provider.json`.
- **One project per scale.** Each scale is ingested once, and every candidate queries that same project read-only, so the inputs are identical by construction. Identify each project by its ingest receipt's result digest, not by file hashes, which differ between identical ingests.
- **Queries.** The four ladder queries from those profiles (`s18-local.json:124`–`:150`), each run as its own `gf query` invocation:
  - `MATCH (n) RETURN count(n)`
  - `MATCH ()-[r]->() RETURN count(r)`: rewrite R1 under A
  - `MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000`: R2
  - `MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000`: R3
- **Session configuration.** The API defaults as `gf query --project` resolves them: the default `target_partitions`, the 512 MiB memory budget, and spill enabled into the project scratch directory with its 8 GiB cap (§2). Record the resolved values with each run, and the bytes spilled where the run reports them.

### 6.2 Path verification

Before a run is timed, the plan must be shown to be the one the candidate intends. Record `explain` output for every candidate and query. For the three rewritten queries it must contain:

| Query | A | B | C |
| --- | --- | --- | --- |
| `count(r)` | `EdgeCountExec` | stock `AggregateExec`; no `ExpandExec` or `EdgeCountExec` | the lowerer-chosen edge-count node |
| One hop, ordered | `OrderedOneHopExec` | stock `SortExec` with a fetch; no `ExpandExec` or ordered node | the lowerer-chosen ordered node |
| Two hops, ordered | `OrderedTwoHopPathCountExec` | as for one hop | the lowerer-chosen ordered node |

`count(n)` uses none of these nodes under any candidate; it is timed as a control. Receipt `operator_rss` uses lower-case labels (`edge_count`, `ordered_one_hop`, `ordered_two_hop`, `expand`) and can corroborate `explain`, but it is not a substitute for it.

A run whose plan doesn't match is invalid, not slow. The mismatch is itself a result.

### 6.3 Correctness gates

- **Query results.** `count` values are equal across candidates. The ordered queries return byte-identical `id` columns: the same values in the same order.
- **Errors.** Where A and a candidate both fail, their structured errors (GraphForge code and kind) are identical. Where only one of them fails, the gate fails unless the failure is a recorded refusal (§6.4).
- **TCK.** For each candidate, run the full corpus (`cargo test -p graphforge-api --test bdd`) with its own `BDD_TIMING_DIR`, and without `TCK_ONLY`, which disables the whole-corpus gate. Collect the keys with outcome `"passed"` from `report.json`. The key set must equal `main`'s at the same base revision exactly, and the runner's built-in baseline gate must report no regressions.
- **Fast-path survival.** `regression1513_fast_paths_survive_multi_file_node_tables` passes under C. So does a variant that inserts an operator the R1–R3 peel helpers do not pass through: for R3, any operator between the two expands (`ordered_two_hop.rs:153`). The variant is valid only after it has first been shown to make A fall back. A repartition above the node scan does not qualify, because A already traces through it (§4).

### 6.4 Caps and refusals

- **Time.** 30 minutes of wall time per `gf query` invocation. That is more than 40× the S20 query phase measured in #1388.
- **Memory.** The session keeps the API defaults from §6.1: a 512 MiB pool and an 8 GiB spill cap. The process runs under a 64 GiB `runexec` memory limit on a 125 GiB host.
- **A cap hit is a result.** Record it as a refusal (timeout, pool `ResourcesExhausted`, spill cap exhausted, or memory-limit kill) with the cap and the scale. It is never dropped, retried with a larger cap, or averaged in. A candidate that is refused at a scale isn't measured at larger scales for that query. Record it as refused at those scales too.

### 6.5 Pairs and the same-sign rule

- **Host.** OVHC-AGENCY: 16 cores, 125 GiB, root on ext4. `TMPDIR` must be on ext4; `/tmp` is tmpfs. Run under `runexec --no-container` for wall time, CPU time and peak memory.
- **Pairs.** A pair is one A run and one comparison-candidate run on the same query and scale, back to back. Order alternates AB, BA, AB and so on. A–B and A–C are separate pair series.
- **Quiet host.** Before each run there must be 60 s of sustained quiet: 12 checks 5 s apart with no `cargo`, `rustc`, `gf`, `runexec` or test process from any agent, Claude or otherwise. Drop the page cache before each run. A run whose after-check reports busy is kept in the raw output and excluded from the medians.
- **Build control.** A under the experiment build must be compared with A under the default build at S18, with at least three pairs. This checks that the experiment feature adds no cost to A.
- **Reporting.** Report the delta and its sign (candidate − A) for every pair, plus the median and range of the deltas. Headline medians alone are not enough.
- **Same-sign rule.** Under the null hypothesis the sign of each pair is a coin flip, so n same-sign pairs have two-sided probability 2 × 0.5ⁿ.
  - Three accepted pairs (probability 0.25) is the minimum for reporting. Three same-sign pairs is **suggestive**, not established.
  - Before calling a same-sign difference **real**, run to six accepted pairs (probability 0.031). If the signs are still all the same, treat the difference as real and audit for the work that was added or removed. Don't describe it as within noise.
  - If the signs are mixed at six pairs, report no distinguishable difference at that n, with the range.
- **Metrics per run:** wall time, CPU seconds, peak RSS, and, for C, the peak pool reservation charged by `ExpandExec`.
- **Where results go.** Raw output (driver log, `runexec` output, explain output, TCK reports) is attached to #1619. The digests of the inputs and binaries used go in #1688's final comment, and are cited from the ADR.

### 6.5a Method (#1688)

The candidate is selected per session by `GF_READ_PATH_CANDIDATE` (`current`, `stock`, `structural`) in a `gf` built with `--features read-path-experiment`. `GF_READ_PATH_INJECT=between-expands` adds the §6.3 fast-path survival variant.

- **Driver.** `benchmarks/tools/read-path-candidates/ab.sh` runs the pairs. `summarize.py` produces the per-pair table and the same-sign verdicts. Besides the quiet helper, the driver treats any `cargo`, `rustc`, `gf`, `runexec` or test process as busy.
- **Plan path.** `gf` has no `explain` command, so verification uses two sources:
  - The `read_path_explain` example in `graphforge-api` prints each candidate's plan for the four queries, once per scale.
  - Every timed run's receipt lists its `operator_rss` labels. `summarize.py` rejects any run whose labels contradict the candidate. B's only label on the ordered queries is the stock `sort`.
- **Results.** Every receipt's `result_sha256` must equal A's for that scale and query. `scalar_u64` carries the counts.
- **Refusals.** One refusal of a candidate on a query ends that series. The candidate is not run on that query again at the same or larger scales.
- **Project state.** A digest of each project's file list, sizes and modification times is taken before and after every run. A run that changes the project is rejected.
- **Exploratory projects.** In exploratory projects with an `_untyped` node property table, a LEFT join sits between the first node scan and the expand. Candidate A's `has_complete_frontier` does not trace through it, so A refuses the fast path where C takes it. This was found in the #1688 review. On the Graph500 projects used here, A's fast paths fire (receipt labels `edge_count`, `ordered_one_hop`).

### 6.6 Maintenance measures specific to the read path

In addition to #1505 §5.6, #1688 reports for each candidate:
- the number of **silent fallback sites**: detection paths that return the plan unchanged without an error or a recorded signal;
- the lines of shape-matching code (`peel_*`, `detect_*`) removed or added;
- whether a new DataFusion transport operator or partitioning change could remove the candidate's fast path without failing a test.

## 7. Changelog

- 2026-09-30: recorded (#1687).
- 2026-10-01: the experiment, its driver and summarizer were retired by #1696 after [ADR 0050](../adr/0050-read-path-fast-path-selection.md). They remain in git history at `646c032cd`. The §6.5 paired timing runs were stopped by maintainer decision before any were accepted.
- 2026-10-01 (#1688, before any timed run): added §6.5a. It records how plan paths are verified without `gf explain`, the result digests, the project-state check, and that one refusal ends a series. None of these replaces an earlier rule.
