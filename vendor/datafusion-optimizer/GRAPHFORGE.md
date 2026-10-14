# GraphForge vendoring notice

This directory is a vendored copy of `datafusion-optimizer` 54.1.0 from
crates.io (the registry crate `datafusion-optimizer-54.1.0`, every file except
cargo's `.cargo-ok` marker). It is third-party code under the Apache License
2.0; `LICENSE.txt` and `NOTICE.txt` are the upstream files, unchanged.

The root `Cargo.toml` selects it with `[patch.crates-io]` and lists it under
the workspace `exclude`, so fmt, clippy and the workspace test run never lint or
test it. Cargo matches each `exclude` entry as a literal path (a `vendor/*` glob
is not recognised), so each vendored crate is named there.

## The one applied change

Apache DataFusion pull request
[#22903](https://github.com/apache/datafusion/pull/22903), merge commit
`69c3a78a4162694a77e7da7047b8f50d86e4b058` (in 55.0.0), adapted to the 54.1.0
optimizer driver. It changes only `src/optimizer.rs`, in the `Rewriter` that
applies one rule to a plan, and only in how a node is finished once its inputs
have been rewritten. `git diff` of that file against the first commit that added
this directory is the whole delta, apart from one stale snapshot in
`tests/optimizer_integration.rs` (below).

`Rewriter::f_down` records the output schemas of the node's inputs before the
rule rewrites its subtree. `Rewriter::f_up` compares them with the schemas the
inputs have afterwards; if any input's schema changed, it calls
`LogicalPlan::recompute_schema()` on the node, which rebuilds the node's cached
output schema from its inputs. A node whose inputs kept their schemas is
untouched, so a rule that rewrites nothing, or rewrites without changing a
schema, behaves exactly as before.

Upstream makes the same repair in `rewrite_plan_in_place`, the in-place
traversal that 55.0.0 added for plans without subqueries. 54.1.0 has no in-place
traversal: every rule goes through `Rewriter`, a `TreeNodeRewriter`, so the
repair lives there. The `input_schemas` stack is pushed in `f_down` and popped in
`f_up`; the tree traversal runs `f_up` for every node it entered, including when
`f_down` jumps over the children, so the stack stays balanced.

Test changes in the vendored crate, mirroring upstream's:

- `optimizer::tests::rewriting_a_child_recomputes_the_parent_schema` (with the
  helper rule `KeepFirstColumnOverScan`) covers both `TopDown` and `BottomUp`.
  Upstream's test, `in_place_rewrite_recomputes_parent_schema_when_child_schema_changes`,
  calls `rewrite_plan_in_place`, which 54.1.0 does not have.
- `tests/optimizer_integration.rs::intersect` has its snapshot updated to the
  plan that DataFusion 55.x produces for the same SQL; upstream's pull request
  makes the identical snapshot edit. The plan loses the outer `Aggregate` over
  the inner `LeftSemi Join`.
- `union_of_filtered_scans_keeps_unqualified_output_labels` guards the hazard
  that upstream's pull request also added a regression for (the `UNION` output
  labels after `unions_to_filter` while schemas are recomputed). It passes with
  and without the change on 54.1.0, so it is a guard, not the regression test.

To run the vendored crate's tests, use a checkout whose root is not nested
inside another cargo workspace:
`cargo test --locked --manifest-path vendor/datafusion-optimizer/Cargo.toml`.
The regression test that CI runs is in `graphforge-rel`
(`tests/struct_field_join_keys.rs`), at plan level through the real optimizer
rules; the end-to-end read, count and `CREATE` forms are in `graphforge-api`
(`tests/unwind_inline_map_anchors.rs`).

## Why

[apache/datafusion#22895](https://github.com/apache/datafusion/issues/22895)
(closed, fixed by #22903): when a join key is a field of a struct column, for
example `get_field(col, 'a') = t.id`, `extract_leaf_expressions` puts an
extraction `Projection` between two joins. `push_down_leaf_projections` then
replaces that `Projection` with the lower join carrying the extraction, which has
a different column order and count. `LogicalPlan::map_children` keeps the
enclosing join's cached schema, so the upper join keeps describing columns its
input no longer produces. `optimize_projections` splits the columns the plan
needs at the wrong offset, prunes a column the query reads, and the plan fails
with `No field named ...`. Stock DataFusion 54.1.0 fails the same way on a plan
with no GraphForge node.

GraphForge hits it with `UNWIND [{a: 1, b: 2}] AS e MATCH (a:N {id: e.a}),
(b:N {id: e.b}) RETURN a.name, b.name`, which lowers to two inner joins keyed by
`get_field(e, 'a')` and `get_field(e, 'b')` over one input. The same stale join
makes `RETURN count(*)` and `CREATE (a)-[:E]->(b)` fail on some runs.

The repair is a DataFusion fix and is in DataFusion 55.0.0 and later (55.2.0
included). GraphForge has not adopted DataFusion 55, so it carries the fix on
54.1.0 here instead of a workaround of its own in a GraphForge optimizer rule.

## Removal

Delete this directory, the `[patch.crates-io]` entry and the
`vendor/datafusion-optimizer` workspace exclude in the root `Cargo.toml` when
GraphForge adopts DataFusion 55.0.0 or later. Re-verify with `cargo tree -i
datafusion-optimizer` that a single registry copy resolves, and keep
`tests/struct_field_join_keys.rs` and `tests/unwind_inline_map_anchors.rs`: they
then prove the registry crate behaves.

## Scope

`[patch]` applies to builds from this repository: the Python wheel, the Node
addon and the CLI. Crates published to crates.io resolve stock DataFusion,
because published manifests do not carry `[patch]` sections.
