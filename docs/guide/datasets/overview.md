# Dataset Integration

> **Status:** Backlog — **not shipped** in v0.5.0. Canonical open-dataset catalogs
> and loaders (`graphforge.datasets`, SNAP / LDBC / NetworkRepository convenience
> APIs) are a planned extension, not part of the core product. Build graphs with
> the [construction APIs](../graph-construction.md) or Cypher today. Pages below
> keep catalog and loading reference material for when that extension lands.

This reference describes the planned dataset catalogs and loading patterns for
popular public graph repositories (experimentation, benchmarking, and learning).

## Overview

When the extension ships, the dataset system is intended to:
- Load pre-configured graph datasets with a single command
- Explore standard benchmark datasets
- Test queries on realistic data
- Compare performance with other graph databases
- Learn Cypher with meaningful examples

## Planned catalog sources

#### [SNAP (Stanford Network Analysis Project)](snap.md)
Real-world network datasets from Stanford, covering social networks, web graphs, citation networks, collaboration networks, and communication networks.

**Use cases:** Research, network analysis, graph algorithm development, academic projects

**Example datasets:**
- `snap-ego-facebook` - Facebook social circles (4K nodes, 88K edges)
- `snap-email-enron` - Enron email network (37K nodes, 184K edges)
- `snap-ca-astroph` - Astrophysics collaboration (19K nodes, 198K edges)
- `snap-web-google` - Google web graph (876K nodes, 5.1M edges)
- `snap-twitter-combined` - Twitter social circles (81K nodes, 1.8M edges)

#### [LDBC full suite](ldbc.md)
Official Graph Data Council / LDBC portfolio: SNB (Interactive + BI),
Graphalytics, FinBench, and SPB — **spec** for datasets, workloads, and
validation. Generators/drivers run in an **external scale harness**, not
GraphForge core CI.

**Use cases:** Standard DBMS and analytics workload completeness; SF → GSI
crosswalk after load

#### [NetworkRepository](networkrepository.md)
Large collection of diverse network datasets.

**Use cases:** Network analysis, algorithm testing, research

#### Scale size ladder (Graph500 × GSI)
Synthetic scale work uses two Graph500 tracks on the
[Graph Scale Index](../../reference/graph-scale-index.md) — not a separate
dataset catalog page:

1. **Official Graph500** — standard parameters (typically ef=16) for GSI size
   notches / community comparability; progressive / first-fail in the harness.
2. **Graph500-derived SCALE×density matrix** — parameterized `edgefactor` to
   hit GSI density tiers at feasible (usually XS) SCALEs; **not** official
   Graph500 submissions.

Both tracks execute in the external harness; this repo is spec only.

#### [WDC Hyperlink Graphs](wdc-hyperlink-graph.md) (retired from scale harness)
Formerly considered for a T0–T6 web-graph ladder. **Not used** for GraphForge
scale testing; kept only to redirect readers to Graph500 × GSI and LDBC.


## Quick Start

### Loading a Dataset

```python
from graphforge import GraphForge
from graphforge.datasets import load_dataset

# Create a GraphForge instance
gf = GraphForge()

# Load a dataset by name
load_dataset(gf, "snap-ego-facebook")

# Query the loaded data
results = gf.execute("MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 10")
```

### Using the Convenience Method

```python
from graphforge import GraphForge

# Load dataset during initialization
gf = GraphForge.from_dataset("snap-ego-facebook")

# Start querying immediately
results = gf.execute("MATCH (n) RETURN count(n) as node_count")
```

### Listing Available Datasets

```python
from graphforge.datasets import list_datasets

# Get all available datasets
datasets = list_datasets()

for ds in datasets:
    print(f"{ds.name}: {ds.description}")
    print(f"  Source: {ds.source}")
    print(f"  Nodes: {ds.nodes:,}, Edges: {ds.edges:,}")
    print(f"  Size: {ds.size_mb:.1f} MB")
    print()
```

### Filtering by Source

```python
from graphforge.datasets import list_datasets

# Get only SNAP datasets
snap_datasets = list_datasets(source="snap")

# Get only small datasets (< 10 MB)
small_datasets = [ds for ds in list_datasets() if ds.size_mb < 10]
```

## CLI Usage

GraphForge provides command-line tools for working with datasets:

```bash
# List all available datasets
graphforge list-datasets

# Show detailed information about a dataset
graphforge dataset-info snap-ego-facebook

# Load a dataset
graphforge load-dataset snap-ego-facebook

# List datasets by source
graphforge list-datasets --source snap

# Clear dataset cache
graphforge clear-cache
```

## Dataset Caching

Datasets are automatically cached locally after the first download to improve load times:

- **Cache location:** `~/.graphforge/datasets/`
- **Cache behavior:** Downloaded once, reused on subsequent loads
- **Cache management:** Use `graphforge clear-cache` to remove cached datasets

## Dataset Metadata

Each dataset includes comprehensive metadata:

```python
from graphforge.datasets import get_dataset_info

info = get_dataset_info("snap-ego-facebook")

print(f"Name: {info.name}")
print(f"Description: {info.description}")
print(f"Source: {info.source}")
print(f"URL: {info.url}")
print(f"Nodes: {info.nodes:,}")
print(f"Edges: {info.edges:,}")
print(f"Labels: {', '.join(info.labels)}")
print(f"Relationship Types: {', '.join(info.relationship_types)}")
print(f"Size: {info.size_mb:.1f} MB")
print(f"License: {info.license}")
print(f"Category: {info.category}")
```

## Jupyter Notebook Integration

Datasets work seamlessly in Jupyter notebooks:

```python
# In a Jupyter notebook cell
from graphforge import GraphForge
from graphforge.datasets import load_dataset

gf = GraphForge()
load_dataset(gf, "snap-ego-facebook")

# Explore the data
gf.execute("MATCH (n) RETURN count(n) AS node_count")
```


## Contributing Datasets

To add a new dataset source or specific dataset:

1. Create a loader in `src/graphforge/datasets/`
2. Register the dataset in the registry
3. Add tests in `tests/integration/test_datasets.py`
4. Update documentation

See the [development guide](../../development/workflow.md) for details.

## Troubleshooting

### Download Failures

If a dataset download fails:
- Check your internet connection
- Try clearing the cache: `graphforge clear-cache`
- Manually download from the source URL

### Memory Issues

Large datasets may require significant memory:
- Start with smaller scale factors (e.g., LDBC SF0.001)
- Use a machine with sufficient RAM
- Consider a Parquet-backed project directory for large datasets

### Import Errors

If dataset import fails:
- Check the dataset format compatibility
- Verify the source URL is accessible
- Report issues on GitHub

## Related Documentation

- [Graph Scale Index (GSI)](../../reference/graph-scale-index.md) — size axis; Official Graph500 + Derived density matrix; harness contract
- [Cypher Script Loading](cypher-script-loading.md) - Planned .cypher / .cql script loading
- [SNAP Datasets](snap.md) - Planned SNAP catalog
- [LDBC full suite](ldbc.md) - Spec for SNB / Graphalytics / FinBench / SPB
- [NetworkRepository Datasets](networkrepository.md) - Planned
- [WDC Hyperlink Graphs](wdc-hyperlink-graph.md) - Retired from scale harness
- [API Reference](../../reference/api.md)

## License Information

Each dataset has its own license. Always check the dataset metadata for licensing information before using in production or research.

## Next Steps

- Explore [SNAP](snap.md) for research and network analysis datasets
- Read the [LDBC full suite](ldbc.md) **spec** (execution in external harness)
- Check [NetworkRepository](networkrepository.md) for diverse networks (coming soon)
- See [GSI × Graph500](../../reference/graph-scale-index.md#graph500-on-the-gsi-axis) for Official + Derived track **specs**
