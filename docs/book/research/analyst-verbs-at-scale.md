# Research notes: analyst verbs at notebook scale

This Advanced note describes how to evaluate native graph algorithms. Start
with the runnable [network guide](../use-cases/network-analysis.md); a beginner
research project does not require benchmarking.

## Supported workflow

`forge.rank()` and `forge.cluster()` return Arrow tables and support explicit
`write_property`. `paths`, `analyze`, and `similar` provide other read-only
algorithm results. Use the [algorithm catalog](../architecture/algorithms.md)
for names, options, graph projections, and result schemas. Python and Node call
the Rust engine; NetworkX and igraph are not fallback execution engines.

## Measure an actual workload

Select a dataset you can reproduce and record its source, content identity,
node and edge definitions, direction, and preprocessing. The proposed
`graphforge.datasets` catalogue is not required or available as a built-in
loader; use [graph construction](../../guide/graph-construction.md).

Measure import, project reopen, the selected algorithm, and any write-back
separately. Record engine version, hardware, memory, storage, input size, and
algorithm options. Distinguish first execution from reuse of prepared state.
Report failures and resource use alongside duration. Earlier unattributed
sub-second examples are not current performance guarantees.

Use [scale guidance](../../reference/scale-limits.md) and the
[measurement method](../../reference/scale-evaluation.md) for the repository's
workload-specific evidence. Keep results on the owning issue or artifact.

## Check correctness and interpretation separately

Compare an algorithm with a reference implementation only after matching
projection, direction, parameters, and normalization. Public UUIDs or a chosen
stable external key must identify the same nodes in both outputs.

Matching implementations support the algorithmic result. They do not validate
sampling, establish that a community represents a real social group, or turn a
centrality score into a quality measure. Inspect source evidence and exceptions
before drawing a substantive conclusion.
