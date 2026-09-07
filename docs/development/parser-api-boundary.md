# Parser and explanation ownership in v0.6.0

`graphforge-cypher` parses query text into the syntax-faithful AST. Its production
Cargo and Bazel dependencies are only `graphforge-ast` and `graphforge-core`.
Parsing takes no project path, ontology, runtime catalog, IR, or DataFusion value.
The lexer, parser, parse errors, AST, tokens, and spans remain available there.

`graphforge-api::GraphForge` owns compiler orchestration. Both `explain(query)`
and `explain_stage(query, stage)` use one implementation. AST inspection stops
before binding. Later stages bind once against a private copy of the instance's
runtime catalog, with its ontology, mode, registered procedures, and persisted
default composition. Execution, streaming, and explanation project that same
composition into generation storage identities without publishing the projection.
Logical and
physical planning use that same snapshot and the existing generation visibility
guard. Explanation renders plans without executing writes or publishing catalog
entries. Full explanation retains its four sections and its existing diagnostic
text when a logical renderer cannot represent an operator supported physically.
A selected logical stage returns that stage's structured error directly.

## Rust source migration

| Previous import or call | v0.6.0 owner |
|---|---|
| `graphforge_cypher::parse(query)` | Unchanged |
| `graphforge_cypher::explain(query)` | `graph.explain(query)` on a `graphforge_api::GraphForge` instance |
| `graphforge_cypher::explain_stage(query, stage)` | `graph.explain_stage(query, stage)` |
| `graphforge_cypher::ExplainStage` | `graphforge_api::ExplainStage` (also defined in core) |
| `graphforge_cypher::{Binder, RuntimeCatalog}` | `graphforge_ir::{Binder, RuntimeCatalog}` for compiler tooling |
| `graphforge_cypher::{OntologyMode, GfError}` | `graphforge_api::{OntologyMode, GfError}` (also defined in core) |

The old parser explanation functions and downstream re-exports are removed;
there is no second free-function orchestrator. Use `GraphForge::new(None)` when
an isolated in-memory explanation context is appropriate. Parser benchmarks and
fuzz binding tools import IR explicitly as development/tool dependencies, which
does not pull IR or rel into a production parser dependency tree.

This is an intentional v0.6.0 Rust source API migration. Binder rejection now uses
the shared #1018 conversion through every facade entry point: ordinary binder
failures are `GF_PARSE` with all diagnostics and spans; the existing typed-UUID
validation exception remains `GF_VALIDATION`. The old logical-stage `GF_PLAN`
binder classification and successful `bind_errors` JSON are replaced by the
same fallible result as execution. Parser display text remains unchanged.

`BoundAst` remains deferred because binding produces GraphIR directly. Selecting
`PhysicalPlan` reuses the renderer already used by the full facade explanation;
it no longer refers to the old parser's unimplemented physical-stage stub. No
new compiler stage, persisted format, binding fallback, or query behavior is
introduced.

## Verification

`cargo tree -p graphforge-cypher --edges normal` shows the production dependency
boundary. The parser unit tests and frozen corpus preserve syntax and spans;
front-end benchmarks still cover lex, parse, bind, and their combined path.
Facade tests compare every selected stage with the full explanation, execute the
same query against real data, compare binder codes and complete diagnostics
across execution/streaming/explanation, and preserve strict ontology and procedure
context. Persisted-composition tests verify qualified symbols, binding receipts,
streamed rows, and unchanged catalog, semantic bindings, and generation. Read-only
write-query tests check catalog, generation, and file bytes.
The API BDD explanation step calls the actual facade rather than the parser.
