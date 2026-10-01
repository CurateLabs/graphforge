---
title: "ADR 0050: The read path keeps its adjacency operators and chooses fast paths in the lowerer"
adr: "0050"
status: "Accepted"
date: "2026-10-01"
superseded_by: null
revisit_when: "DataFusion gains a lookup join over a TableProvider index, a query shape that only the physical rewrites caught is found after migration, or a paired timing shows lowerer selection costs more than physical selection"
---

# ADR 0050: The read path keeps its adjacency operators and chooses fast paths in the lowerer

**Status:** Accepted

**Implementation:** Not migrated by this record. Follow-up: #1696 (migrate fast-path selection to the lowerer, account `ExpandExec` in the memory pool, retire the experiment).

**Build target:** v0.6.0

**Related:** #1619 (decision), #1687 (inventory and protocol), #1688 (candidates), ADR 0046 (construction reuse), #1388 (bounded query cost), #1513 (silent fast-path fallback).

## Context

GraphForge runs Cypher reads on DataFusion: a `SessionContext`, DataFusion's physical planner with a GraphForge extension planner, and a DataFusion memory pool. It does not keep a second executor. The custom parts sit inside DataFusion, as inventoried in [cypher-read-path-inventory.md](../development/cypher-read-path-inventory.md):

- **Adjacency operators.** `ExpandExec` traverses the CSR adjacency index. `EdgeCountExec`, `OrderedOneHopExec` and `OrderedTwoHopPathCountExec` answer the count and ordered-limit queries.
- **Physical rewrites.** `FixedHopDemandRule` substitutes those three operators when it recognises the physical plan shape (R1–R3). When the shape differs, it keeps the generic plan without an error. That is the #1513 bug class.

#1688 ran three candidates for the fixed-hop and edge-count paths through the whole engine:
- **A, current:** physical rewrites over `ExpandExec`.
- **B, stock:** DataFusion hash joins, aggregate and top-K sort.
- **C, structural:** the lowerer picks the fast operator from the Graph IR. Physical-plan shape cannot change that choice, and a failed session precondition keeps the generic plan under a visible `FastPathFallbackExec`.

## Evidence

- **Answers.** The default build, A, B and C each pass 3,898 of 3,898 TCK scenarios and 118 of 118 API scenarios, with identical passed-key sets.
- **Plans on the Graph500 S18, S19 and S20 projects:**
  - A and C both run `EdgeCountExec`, `OrderedOneHopExec` and `OrderedTwoHopPathCountExec`.
  - B runs eight `HashJoinExec` over complete node and edge scans.
  - No `FastPathFallbackExec` appears.
- **Robustness.** An operator injected between the two hops makes A's ordered two-hop rewrite fall back silently. C keeps its operator, because its choice does not read the physical plan. The test was checked by mutation.
- **Coverage.** In exploratory projects with `_untyped` node properties, a LEFT join sits between the first scan and the expand. A's frontier proof does not trace through it, so A refuses the fast path. C takes it.
- **Memory.** With `ExpandExec` charged to the pool under C, the whole TCK still passes. A known positive shows the pool refuses an oversized hop that runs unaccounted under A.
- **Timing.** **Not measured.** The protocol's paired S18–S20 runs were stopped by maintainer decision on 2026-10-01, after three S18 count runs, and none are used here. Single unpaired smoke runs on S18 took 1.0 s (A) against 1.9 s (B) for `count(r)`, and 1.1 s against 1.4 s for the ordered one-hop. They are anecdote, not evidence.

## Decision

| Mechanism | Decision | Evidence | Confidence | Revisit when |
| --- | --- | --- | --- | --- |
| `ExpandExec` and the three fast operators | **Retain** | B's stock plans join the complete edge table, so their cost grows with the graph; the fast operators read only the adjacency rows the result needs (#1388). Identical answers. | Medium (structural; no paired timing) | DataFusion gains a lookup join that reads a TableProvider index per probe row |
| Physical fast-path rewrites R1–R3 | **Replace** with lowerer selection (C) | Same operators run; the #1513 class is gone for these shapes; covers a case A misses; identical TCK set | High for correctness; performance parity inferred from identical operators, not measured | A shape only R1–R3 caught turns up during migration |
| `ExpandExec` memory-pool accounting | **Adopt** | TCK passes with it on; known-positive refusal | Medium | Pool refusals appear on workloads that ran before |
| Stock B for fixed hops | **Reject** as the production path; keep the relational lowering as the `differential-testing` oracle | Above | Medium | As for the first row |
| `VarLenExpandExec`, `OptionalMatchExec`, `UnwindExec`, `SortRunCoalesceExec`, scan and overlay operators | **Not evaluated** | No prototype in #1688 | — | Any becomes a measured bottleneck or a correctness defect |

### Fast-path classes

- **Eliminated:** a physical-shape change (partitioning, transport operators, operator properties) silently removing the edge-count, ordered one-hop or ordered two-hop fast path.
- **Kept, now visible:** a session precondition failure keeps the generic plan. Examples are a missing ordinal identity authority, or ordinal order that differs from UUID order. `FastPathFallbackExec` names the reason in `explain` output.

### To delete when the migration lands

- `try_rewrite_edge_count`, `try_rewrite_ordered_one_hop`, `try_rewrite_ordered_two_hop`, and their `detect_*` and `peel_*` helpers.
- The call to them in `FixedHopDemandRule`.
- The `read-path-experiment` feature, the candidate switch, the injection rule, `benchmarks/tools/read-path-candidates/`, the `read_path_explain` example, and the `read_path_candidates` CI lane. The structural selection and pool accounting become unconditional.

## Staged migration and rollback

1. Make lowerer selection and `ExpandExec` accounting the default, and keep R1–R3 behind it for one change. Run `fixed_hop_limit` and the TCK, and log any statement where R1–R3 fire after the lowerer declined. Each such statement is a shape to add to the lowerer or to accept.
2. Delete R1–R3 and the experiment.
3. `FastPathFallbackExec` is not transparent to terminal demand. The migration must pass a `LIMIT` through it, so a fallback costs no more than today's generic plan.

**Rollback:** re-enable R1–R3 and drop the `FastPathNode` wrap. Both are single-commit changes until step 2.

## Not claimed

- Any speedup or cost parity backed by measurement. No paired timing was taken.
- Anything about B beyond the plans it produced, or about scales above S20.
- Anything about the operators marked "not evaluated", or about construction (ADR 0046).
