# SNAP Datasets

> **Status:** Backlog — not shipped in v0.5.0 (see [Datasets overview](overview.md)).

Planned support for SNAP (Stanford Network Analysis Project) datasets for network analysis and research.

## Overview

SNAP provides an extensive collection of real-world network datasets from Stanford University, widely used in research and education. This page documents the planned curated SNAP catalog.

## Dataset Source

- **URL:** https://snap.stanford.edu/data/
- **License:** Public Domain (for datasets we support)
- **Category:** Research, Academic
- **Format:** Edge-list (CSV/TSV), automatically downloaded and cached

## Available Datasets

| Dataset | Nodes | Edges | Size | Category | Description |
|---------|-------|-------|------|----------|-------------|
| `snap-ego-facebook` | 4,039 | 88,234 | 0.5 MB | Social | Facebook social circles (ego networks) |
| `snap-email-enron` | 36,692 | 183,831 | 2.5 MB | Communication | Enron email communication network |
| `snap-ca-astroph` | 18,772 | 198,110 | 1.8 MB | Collaboration | Astrophysics collaboration network |
| `snap-web-google` | 875,713 | 5,105,039 | 75 MB | Web | Google web graph from 2002 |
| `snap-twitter-combined` | 81,306 | 1,768,149 | 25 MB | Social | Twitter social circles (combined ego networks) |

### Coming Soon

Additional SNAP datasets are planned for future releases. See the work item for the roadmap to support 100+ SNAP datasets.

## Quick Start

### Load a Dataset

```python
from graphforge import GraphForge

# Method 1: Load during initialization
gf = GraphForge.from_dataset("snap-ego-facebook")

# Method 2: Load into existing instance
gf = GraphForge()
from graphforge.datasets import load_dataset
load_dataset(gf, "snap-ego-facebook")

# Start querying immediately
results = gf.execute("""
    MATCH (n)-[r]->(m)
    RETURN count(DISTINCT n) AS users, count(r) AS connections
""")

print(f"Users: {results[0]['users'].value:,}")
print(f"Connections: {results[0]['connections'].value:,}")
```

### Browse Available Datasets

```python
from graphforge.datasets import list_datasets

# List all SNAP datasets
snap_datasets = list_datasets(source="snap")

for ds in snap_datasets:
    print(f"{ds.name}")
    print(f"  {ds.description}")
    print(f"  Nodes: {ds.nodes:,}, Edges: {ds.edges:,}")
    print(f"  Category: {ds.category}, Size: {ds.size_mb} MB")
    print()
```

### Filter by Category

```python
from graphforge.datasets import list_datasets

# Get only social network datasets
social_nets = list_datasets(source="snap", category="social")

# Get small datasets (< 10 MB)
small_datasets = list_datasets(source="snap", max_size_mb=10.0)
```

## Example Queries

### Social Network Analysis

```python
gf = GraphForge.from_dataset("snap-ego-facebook")

# Find most connected users
results = gf.execute("""
    MATCH (n)-[r]->()
    RETURN n.id AS user, count(r) AS connections
    ORDER BY connections DESC
    LIMIT 10
""")

for row in results:
    print(f"User {row['user'].value}: {row['connections'].value} connections")
```

### Network Statistics

```python
gf = GraphForge.from_dataset("snap-email-enron")

# Calculate basic network metrics
results = gf.execute("""
    MATCH (n)-[r]-(m)
    WITH count(DISTINCT n) AS nodes, count(r)/2 AS edges
    RETURN
        nodes,
        edges,
        edges * 1.0 / nodes AS avgDegree
""")

print(f"Nodes: {results[0]['nodes'].value:,}")
print(f"Edges: {results[0]['edges'].value:,}")
print(f"Average Degree: {results[0]['avgDegree'].value:.2f}")
```

### Collaboration Patterns

```python
gf = GraphForge.from_dataset("snap-ca-astroph")

# Find researchers with many collaborators
results = gf.execute("""
    MATCH (researcher)-[:CONNECTED_TO]->(colleague)
    WITH researcher, count(DISTINCT colleague) AS collaborators
    WHERE collaborators > 10
    RETURN researcher.id AS researcher, collaborators
    ORDER BY collaborators DESC
    LIMIT 20
""")
```

### Web Graph Analysis

```python
gf = GraphForge.from_dataset("snap-web-google")

# Find most linked-to pages
results = gf.execute("""
    MATCH ()-[r]->(page)
    RETURN page.id AS page, count(r) AS inlinks
    ORDER BY inlinks DESC
    LIMIT 10
""")
```

## Dataset Details

### Edge List Format

SNAP datasets use a simple edge-list format:

```
# Comment lines (ignored)
source_id target_id [weight]
```

The CSV loader automatically:
- Detects delimiter (tab, comma, or space)
- Handles gzip compression
- Skips comment lines
- Creates nodes with `Node` label and `id` property
- Creates `CONNECTED_TO` relationships
- Supports optional weights

### Download and Caching

Datasets are automatically downloaded and cached:

1. **First load**: Downloads from SNAP website (~5-30 seconds depending on size)
2. **Subsequent loads**: Uses cached file (instant)
3. **Cache location**: `~/.graphforge/datasets/`
4. **Cache expiry**: 30 days (refreshes automatically)

Clear cache manually:
```python
from graphforge.datasets import clear_cache

clear_cache("snap-ego-facebook")  # Clear specific dataset
clear_cache()                       # Clear all cached datasets
```

## Performance Tips

### Large Datasets

For large datasets like `snap-web-google`:

```python
# Use persistent storage to avoid memory issues
gf = GraphForge("analysis.db")
load_dataset(gf, "snap-web-google")

# Queries run against the Parquet-backed project
results = gf.execute("MATCH (n) RETURN count(n) AS count")
```

### Incremental Analysis

```python
# Load dataset once
gf = GraphForge("facebook.db")
if not gf.execute("MATCH (n) RETURN count(n) AS c")[0]['c'].value:
    load_dataset(gf, "snap-ego-facebook")

# Run multiple analyses without reloading
analysis1 = gf.execute("MATCH (n) RETURN count(n)")
analysis2 = gf.execute("MATCH ()-[r]->() RETURN count(r)")
```

## References

- [SNAP Website](https://snap.stanford.edu/data/)
- [SNAP Data Description](https://snap.stanford.edu/data/index.html)
- GraphForge the work item - Expand SNAP support

## Related

- [Dataset Overview](overview.md) - Complete dataset guide
- [Loading Datasets](../quickstart.md#load-a-real-world-dataset) - Quick start guide
- [API Reference](../../reference/api.md) - Dataset API documentation
