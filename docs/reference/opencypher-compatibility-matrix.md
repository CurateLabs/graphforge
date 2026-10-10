# openCypher compatibility matrix

This matrix describes the Rust engine on the v0.6.0 development line. Python
and Node use that engine. Support is scoped to the forms below; a passing
workload fixture does not establish complete language or TCK conformance.

The workload contracts are executed through `GraphForge::execute` and
`GraphForge::execute_with_params` in
[`cypher_workload_constructs.rs`](../../crates/graphforge-api/tests/cypher_workload_constructs.rs).
The parser, binder, and relational lowering live in `graphforge-cypher`,
`graphforge-ir`, and `graphforge-rel` respectively. See the
[language reference](opencypher-compatibility.md) for the broader surface.

## Workload forms

| Construct | Current behavior | Contract |
| --- | --- | --- |
| Stored date property components, such as `n.creationDate.year` | Executes directly in filters and grouped projections | Literal year and count results |
| Literal `duration({days: 1})` | Executes | Date arithmetic result |
| `duration` maps whose entries depend on parameters or expressions | `GF_NOT_IMPLEMENTED`: dynamic duration constructor maps | Specific error payload |
| `OPTIONAL MATCH` predicate using a `WITH` value | Executes | Matching row and null extension |
| Node-list concatenation followed by `UNWIND` and a pattern | Executes for proved whole-node lists | Endpoint rows |
| `collect`ed node lists, renamed aliases, `WITH DISTINCT`, and `UNWIND` | Executes for proved whole-node lists | Duplicates, null extension, mixed labels, and properties after recollection |
| `UNWIND` alias already present in the input scope | `GF_PARSE` with `VariableAlreadyBound` diagnostic | Existing scalar and node aliases rejected |
| `UNWIND $rows` with row-dependent matches across distinct label schemas in one scope | `GF_NOT_IMPLEMENTED`: parameter rows matched across different label schemas | Specific error payload, including duplicated row aliases |
| Parameter-row endpoint matches separated by `WITH` | Executes with independent owner projections | Both returned node properties |
| `ALL` over scalar lists | Executes | Literal Boolean result |
| `ALL` indexing a variable-length relationship list directly after `MATCH` | `GF_NOT_IMPLEMENTED`: indexed variable-length relationship predicates | Specific error payload |
| Indexed relationship `ALL` after a scalar `WITH` projection | Executes | Literal true and false results, forwarding, wildcard, and renamed alias |
| Path-node list comprehension filtered in the same `WITH` | Executes | Literal node count |
| Relationship-type alternation on a variable-length hop | `GF_NOT_IMPLEMENTED`: variable-length relationship type alternation | Both directions rejected specifically |
| `startNode` / `endNode` of an already bound fixed-hop relationship | Executes | Both endpoint properties |
| `startNode` / `endNode` of an unwound path relationship | `GF_NOT_IMPLEMENTED`: endpoints of unbound relationship values | Specific error payload |
| `id(...)` | `GF_NOT_IMPLEMENTED`: Cypher identity function | Specific error payload; use declared identifier properties for workloads |
| `reduce(...)` | `GF_NOT_IMPLEMENTED`: reduce expressions | Scalar and path-list forms rejected specifically |
| `COUNT { ... }` | `GF_NOT_IMPLEMENTED`: COUNT subqueries | Specific error payload |
| `CALL { ... }` | `GF_NOT_IMPLEMENTED`: CALL subqueries | Uncorrelated, correlated, and union bodies rejected specifically |
| `shortestPath` / `allShortestPaths` | `GF_NOT_IMPLEMENTED`: shortest-path expressions | Unbounded and bounded named-pattern forms rejected specifically |

Malformed syntax retains a parse error. Unsupported-feature diagnostics are
emitted for recognized constructs, rather than treating every parser or binder
failure as an unsupported feature. An unproved scalar, map, relationship, or
heterogeneous list element does not become a node merely because a later
pattern uses its name.

## Benchmark query provenance

The runnable BI texts in
[`queries.rs`](../../benchmarks/runners/gdc-snb-bi/src/queries.rs) identify their
upstream LDBC query and explain remaining rewrites. BI1 and BI13 use stored
property components directly; BI8 collects whole nodes and unwinds them before
matching, following upstream forms. Rewrites for unsupported constructs remain
explicit. The runners' deterministic public-facade fixtures pin their results.

The upstream reference revision is
[`47dd38b40844ecdb0e42e5a610c369535304786d`](https://github.com/ldbc/ldbc_snb_bi/tree/47dd38b40844ecdb0e42e5a610c369535304786d/neo4j/queries).

## Validation and remaining work

Run the workload contracts with the Rust CI lane narrowed to the API crate:

```bash
make test-rust ARGS="-p graphforge-api --test cypher_workload_constructs"
```

Run the BI runner's query fixtures after changing its query texts. The repository
TCK harness and its current reports establish TCK results; this page does not
publish a pass rate or promise complete categories without a current run.
Supporting a refused construct requires its own semantic implementation and
positive public-facade coverage before changing this matrix.
