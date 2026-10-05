# GraphForge Reference Documentation

Use reference pages to look up an exact command, supported query form, or
technical limit. For a guided first exercise, start with
[Your first research project](../guide/first-research-project.md) or
[Your first graph](../guide/quickstart.md).

## Current product reference

| What you need                                    | Page                                                    |
| ------------------------------------------------ | ------------------------------------------------------- |
| Method arguments and return values               | [API reference](api.md)                                 |
| Supported query language and its limits          | [openCypher compatibility](opencypher-compatibility.md) |
| Query examples with explanations                 | [Cypher guide](../guide/cypher-guide.md)                |
| Names of columns returned by a query             | [Column naming](column-naming-behavior.md)              |
| Workload and resource considerations             | [Scale limits](scale-limits.md)                         |
| Conformance method and authoritative test status | [TCK compliance](tck-compliance.md)                     |

The API reference is the published site's reference landing page. These docs
target v0.6.0; [Installation](../guide/installation.md) explains package
availability.

## Specialist material

The remaining files support implementers, compatibility researchers, and
release operators. They are not prerequisites for a student research project.

- `opencypher-features/` describes language constructs.
- `implementation-status/` and `feature-mapping/` contain feature inventories and
  test mappings. Some files retain dated status snapshots; the current
  conformance authority is identified in [TCK compliance](tck-compliance.md).
- [Graph Scale Index](graph-scale-index.md) and
  [scale evaluation](scale-evaluation.md) define profiling and benchmark methods.
- `discovery/` and `hub-publish/` contain protocol contracts for integration
  developers.
- The JSON schema inventories describe exact machine-readable contracts.

The optional [`scripts/build_feature_graph.py`](../../scripts/build_feature_graph.py)
builder turns the feature inventories into a local graph. Its default output
is `docs/feature-graph.db`, a generated project directory, not a bundled product
dataset. The graph reflects its input documents and is not independent proof
of current feature support. See [its schema](feature-graph-schema.md) and
[example queries](feature-graph-queries.md) when maintaining that inventory.

Dated release tracking, validation reports, and failure histograms record their
stated historical measurements. They do not establish the current release's
capabilities or readiness.
