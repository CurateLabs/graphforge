# Research notes: network analysis

This Advanced note supports [Network analysis in notebooks](../use-cases/network-analysis.md).
For a first mixed-methods assignment, start with
[Your first research project](../../guide/first-research-project.md).

## Define the network before measuring it

Specify the unit represented by each node, the meaning and direction of each
relationship, the collection period, and the inclusion rule. Distinguish an
absent recorded relationship from evidence that no relationship exists. Record
whether repeated edges are meaningful or duplicate imports.

The worked guide uses a synthetic graph and the native construction API.
`graphforge.datasets` is a proposed catalogue, not an installed loader. For a
real dataset, retain its exact source, license, preparation code, and schema;
use [graph construction](../../guide/graph-construction.md) for import.

## Interpret results within their scope

- Degree counts depend on whether you count edges or distinct neighbors and
  whether incoming, outgoing, or undirected relationships are included.
- Use `count(relationship)` after an `OPTIONAL MATCH` to preserve zero-degree
  nodes without counting the unmatched row as an edge.
- Community labels describe an algorithmic partition. They do not identify
  social groups, interview themes, or shared beliefs without further evidence.
- A path demonstrates recorded connectivity, not causation or transmission.
- Centrality does not by itself measure expertise, credibility, or influence.

Connect a numerical pattern back to source material and inspect exceptions.
Do not infer a population relationship from a convenience sample merely because
its graph query has a deterministic answer.

## Reproduce the analysis

Record selected labels, relationship types, direction, algorithm options, and
source scope. Public result identities are UUIDs; do not depend on internal
numeric storage IDs. Python results are Arrow tables. See
[analytics integration](../../guide/analytics-integration.md) for explicit
pandas/NetworkX/igraph conversion, and the
[algorithm catalog](../architecture/algorithms.md) for native verbs.

Use [save and reopen](../../guide/tutorial.md) for a durable working graph and
[portable projects](../../guide/portable-projects.md) to share it. A saved score
is an observation about the graph used to compute it, not an automatically
updated property of later graph states.
