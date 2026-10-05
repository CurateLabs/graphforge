# Use a SNAP dataset

**Advanced:** assumes basic Python, tabular data, and graph queries. For a
guided introduction, start with [Your first graph](../quickstart.md).

The [Stanford Large Network Dataset Collection](https://snap.stanford.edu/data/)
provides networks such as citations, communication, and social connections.
This page helps you assess an external source after the
[basic graph lesson](../quickstart.md). GraphForge v0.6.0 has no SNAP catalog
loader or `from_dataset()` method.

## Choose the individual dataset

Read the source page for the dataset you intend to use. Record what a node and
edge mean, the collection period, whether edges are directed, file layout,
known omissions, citation, and applicable use terms. Do not assume a common
license or common format across the collection.

A citation network can answer questions about links among its captured papers.
It cannot establish that papers outside the collection have no citations.
Social-network connections do not by themselves measure trust or explain
participants' experiences.

## Load and check

Download the source archive separately and inspect its files before writing a
parser. Preserve source identifiers; account for comment/header rows, repeated
edges, edge direction, and nodes absent from an edge list.

Construct the graph using [the public Python APIs](../graph-construction.md).
The [data preparation guide](overview.md#bring-your-own-files) describes the
mapping from source rows to node handles and relationships. Compare the loaded
counts and selected rows with the source, and record any deliberate filtering.
No automatic download, caching, or schema inference is promised here.

For a first class project, choose a small dataset whose collection and meaning
you can explain. Keep the original data and your analysis script; use
[Record an inquiry](../record-an-inquiry.md) to retain a scoped conclusion.
