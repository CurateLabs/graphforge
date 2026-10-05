# Use NetworkRepository data

**Advanced:** assumes basic Python, tabular data, and graph queries. For a
guided introduction, start with [Your first graph](../quickstart.md).

[NetworkRepository](https://networkrepository.com/) is an external collection
of network datasets. Start with [Your first graph](../quickstart.md) before
adapting one. GraphForge v0.6.0 does not ship a NetworkRepository downloader,
GraphML loader, or catalog convenience API.

## Inspect the source before analysis

Choose an individual dataset page and follow its original-study citation.
Check the actual download format instead of assuming every file is GraphML.
Record node and edge meanings, direction, weights, collection period, source
version, and applicable use terms.

A co-purchase link, a reported friendship, and a shared appearance in a chapter
are different observations. A community-detection result does not by itself
explain motivations, social identity, or causation. Read the study and any
qualitative material alongside the network.

## Construct the graph

Parse the downloaded format with an appropriate external tool, then use
[Graph construction](../graph-construction.md) to create nodes and relationships
through GraphForge's public API. The [data preparation guide](overview.md)
covers source identifiers, missing records, and checks before analysis.
There are no verified catalog load-time or automatic-cache guarantees here.

For a runnable small example, use the [visualization guide](../visualization.md).
It uses Zachary's Karate Club from
[Mark Newman's network data collection](https://public.websites.umich.edu/~mejn/netdata/),
with an explicit source and checksum. That example does not qualify every
NetworkRepository dataset or replace reading the original research.

Retain source files and preprocessing decisions so another reader can reproduce
your graph. Use [save and reopen](../tutorial.md) for persistence.
