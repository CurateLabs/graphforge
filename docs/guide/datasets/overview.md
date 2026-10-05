# Choose data for a graph

**Advanced:** assumes basic Python, tabular data, and graph queries. For a
guided introduction, start with [Your first graph](../quickstart.md).

Start here after [Your first graph](../quickstart.md) when you want data for your
own question. GraphForge v0.6.0 does not ship a `graphforge.datasets` catalog,
`GraphForge.from_dataset()`, or dataset-download CLI commands. Download and
inspect source files separately, then construct the graph through the
[public APIs](../graph-construction.md).

## Choose data that can answer the question

Before loading, write down what one record represents, what a relationship
means, which people or documents are included, and what is missing. An edge list
usually contains only connections; people with no recorded connections may be
absent. A missing edge is not automatically evidence that no relationship exists.

Keep the source URL, citation, download date, file version or checksum, license,
and any selection or cleaning steps. Counts from an example network describe
that captured network, not a representative sample of a wider population.
Interview excerpts and coding decisions need their own source links; a network
file does not supply that qualitative context.

## External sources

| Source                                    | What to inspect                                                                   |
| ----------------------------------------- | --------------------------------------------------------------------------------- |
| [SNAP](snap.md)                           | Network definitions, collection period, direction, and the dataset's own citation |
| [NetworkRepository](networkrepository.md) | File format, node and edge meanings, and the original study                       |
| [Neo4j examples](neo4j-examples.md)       | Teaching datasets and queries; check syntax and dependencies before reuse         |
| [LDBC benchmarks](ldbc.md)                | Specialist engine-evaluation workloads, not a first research-project dataset      |

For a small runnable real-network example, the repository's
[visualization examples](../visualization.md) fetch a specific Karate Club
network and build it through public APIs. They require a source checkout and
additional packages.

## Bring your own files

For a small spreadsheet, export CSV and read it with Python's `csv.DictReader`.
Decide which columns identify records, which become properties, and which link
records. Preserve identifiers as strings when leading zeros matter. Handle
blank cells and repeated rows explicitly before creating nodes.

Use `add_node` and `add_edge` for small inputs as shown in
[Graph construction](../graph-construction.md). Keep a mapping from your source
identifiers to returned node handles so relationships connect the intended
records. Bulk APIs are optional for larger imports; they do not decide what the
data means or whether your sample is complete.

Check loaded counts and a few source-linked rows before analysis. Keep original
files and the construction script. For persistence, follow
[save and reopen](../tutorial.md); for a bounded conclusion and later reuse,
continue with [Record an inquiry](../record-an-inquiry.md).
