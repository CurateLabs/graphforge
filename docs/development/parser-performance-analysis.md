# Parser Performance Analysis

## Background

During v0.3.8 TCK compliance work, the `test_generate_the_movie_graph` test
(Create4.feature, Scenario 1) was observed to take 9+ minutes in CI. This document
records the root-cause analysis, the interim fix applied, and the path to a proper solution.

## Root Cause: xearley on Long Productions

GraphForge uses [Lark](https://github.com/lark-parser/lark) for parsing openCypher.
The grammar is configured with `parser="earley"`, which Lark automatically upgrades to
**xearley** (extended Earley) when the grammar contains dynamic or regex terminals.

Earley parsers are general-purpose but have O(n²) or worse complexity on grammars with
long repetitions. The movie graph query in the TCK is a single statement with 971
consecutive `CREATE` clauses — no semicolons separating them:

```cypher
CREATE (theMatrix:Movie {title: 'The Matrix', ...})
CREATE (keanu:Person {name: 'Keanu Reeves', ...})
...  -- 971 CREATE clauses total
CREATE (keanu)-[:ACTED_IN {roles: ['Neo']}]->(theMatrix),
       ...
```

### Profiler output (xearley on Create4 Scenario 1)

```
4,816,353,715 function calls in 1,656 seconds

ncalls  tottime  cumtime  function
31108   422s     1650s    earley.predict_and_complete
1.37B   536s     1085s    grammar.__eq__       ← O(n²) item comparisons
830M    307s      408s    lexer.__eq__
2.23B   245s      245s    isinstance
```

**The parser makes 1.37 billion equality comparisons** for a ~50KB query. The grammar
rule `create_clause+` forces the Earley algorithm to maintain an ever-growing set of
candidate parse states, each of which must be compared with all existing states.

## Benchmarks

| Query size | xearley (before) | Batch-5 (interim) | LALR (target) |
|------------|-----------------|-------------------|---------------|
| 20 CREATEs | 1.2s | ~0.2s | <1ms |
| 50 CREATEs | 5.5s | ~0.5s | <1ms |
| 171 nodes (movie graph) | ~590s (10 min) | ~26s | <1s |
| 731 nodes (school graph) | ~2700s (est.) | ~72s | <3s |

Earley per-call overhead is also high (~60-185ms per call) even for tiny queries,
because xearley re-processes the grammar table on every parse invocation.

## Interim Fix (v0.3.8)

Added `_split_create_sequence()` in `parser.py` that detects pure CREATE-only queries
and splits them into batches of 5 before passing each batch to the Earley parser. This
avoids the O(n²) blowup by keeping each batch small.

**Tradeoffs:**
- Movie graph: 10 min → 26s (23x speedup)
- School graph: estimated 45min → 72s (38x speedup)
- Overhead: regex pre-scan on every query (negligible)
- Fragility: only helps CREATE-only sequences; doesn't help MATCH/RETURN-heavy queries

This is a band-aid, not the proper fix.

## Proper Fix: Migrate to LALR(1) — the work item

LALR(1) is O(n) with near-zero per-call overhead (<1ms). It requires a non-ambiguous
grammar, which means removing the reduce/reduce conflicts in our current grammar.

The conflicts arise because the same suffixes appear in multiple rules:

```lark
// Current (causes conflicts):
read_query: match_clause where_clause? return_clause ...
update_query: match_clause where_clause? (set_clause | remove_clause)* ...
// Both start with match_clause where_clause? — LALR can't distinguish
```

**Solution:** Flatten to a single `clause*` production and validate ordering in the planner:

```lark
// Flat grammar (LALR-compatible):
query: clause+
clause: match_clause
      | optional_match_clause
      | create_clause
      | merge_clause
      | with_clause
      | where_clause
      | return_clause
      | set_clause
      | remove_clause
      | delete_clause
      | unwind_clause
      | call_clause
      | order_by_clause
      | skip_clause
      | limit_clause
```

The planner already has access to the full clause list and can enforce semantic ordering
rules (e.g., RETURN required after MATCH, CREATE without MATCH is a write-only query).
This is consistent with the architecture principle: "Parser produces AST, planner validates
semantics."

### Files to change

1. `src/graphforge/parser/cypher.lark` — flatten `query` to `clause+`, remove
   `read_query`/`write_query`/`update_query`/etc.
2. `src/graphforge/parser/parser.py` — change `parser="earley"` to `parser="lalr"`,
   remove `_split_create_sequence` and related constants
3. `src/graphforge/planner/planner.py` — add validation for illegal clause orders if
   needed (most already enforced implicitly by planner dispatch)

## Performance Tracking

The CI now runs `scripts/tck_perf_report.py` after each TCK run, reporting:
- Tests exceeding a 5-second threshold
- Tracked use-case tests: `test_generate_the_movie_graph`, `test_many_create_clauses`

This makes performance regressions visible before they become 9-minute CI runs.

## Storage Layer Fix (also v0.3.8)

A secondary performance issue was found in `storage/memory.py`:
`_update_statistics_after_add_edge` performed an O(n) set comprehension on every edge
insertion to compute `unique_sources`. This was O(n²) total for n edges.

**Fix:** Added `_unique_sources_by_type: dict[str, set]` to track unique source nodes
incrementally (O(1) per insert). Also moved all statistics tracking from immutable
Pydantic `model_copy()` calls (expensive object creation) to mutable Python dicts,
building the `GraphStatistics` Pydantic object lazily only in `get_statistics()`.

Impact: ~2,000 `model_copy()` calls eliminated for a 1,000-node graph, plus O(n²) → O(n)
for edge statistics.
