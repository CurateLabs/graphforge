# Agent grounding with a tool graph

**Advanced:** basic Python, database queries, and terminal use.

This optional guide is for developers building an agent application. A student
using an existing coding agent should start with
[Work with an agent](../../guide/work-with-an-agent.md) and
[Your first research project](../../guide/first-research-project.md).
Building a tool registry is not a prerequisite for using GraphForge.

A graph can describe tools, capabilities, and dependencies for retrieval.
It does not itself run a model, grant permission to execute a tool, or establish
that an agent's answer is correct. This example uses the public Python API after
[Installation](../../guide/installation.md), with no model account or network call.

## Describe tools and capabilities

```python
from graphforge import GraphForge

forge = GraphForge()
capability = forge.add_node("Capability", name="search documents")
search = forge.add_node(
    "Tool", name="search_documents", description="Search document titles and text",
    deprecated=False,
)
read = forge.add_node(
    "Tool", name="read_document", description="Read one document by its source key",
    deprecated=False,
)
forge.add_edge(search, "CAN_DO", capability)
forge.add_edge(search, "NEXT_TOOL", read)
```

These are ordinary graph labels and relationships. Calling a node `Capability`
or linking it with `CAN_DO` does not install a native ontology or enforce its
meaning. Your application owns the registry's accuracy and the actual tools.

## Query for tool selection

```python
candidates = forge.execute("""
    MATCH (tool:Tool)-[:CAN_DO]->(capability:Capability)
    WHERE capability.name = $capability AND tool.deprecated = false
    RETURN tool.name AS name, tool.description AS description
    ORDER BY name
""", {"capability": "search documents"})
print(candidates.to_pylist())
```

Expected output:

```text
[{'name': 'search_documents', 'description': 'Search document titles and text'}]
```

This query finds a declared capability match. It does not establish that the
current user has permission or that required inputs are present. The executing
application must check those conditions at the point of use.

## Inspect a declared next step

```python
next_steps = forge.execute("""
    MATCH (tool:Tool {name: $name})-[:NEXT_TOOL]->(next:Tool)
    WHERE next.deprecated = false
    RETURN next.name AS name
""", {"name": "search_documents"})
print(next_steps.to_pylist())
```

Expected output: `[{'name': 'read_document'}]`. A declared sequence is not an
executed plan. Check the prior operation's actual result before passing a
source key to the next tool. See [tool recall](agent-tool-recall.md) for explicit
input requirements.

## Hybrid search for tool selection

Text search provides another candidate-retrieval path:

```python
forge.index("Tool", properties=["name", "description"])
hits = forge.find("Search document titles", label="Tool", limit=5)
print(hits.select(["name", "score", "matched_on"]).to_pylist())
```

Text matching does not guarantee synonym, paraphrase, or typo recall. Hybrid
search additionally needs a suitable embedding space and query vector; small
synthetic vectors test mechanics, not semantic quality. Inspect actual hits on
your own task and apply the same permission and input checks to every candidate.
See the [API reference](../../reference/api.md) and
[embedding publication contract](../architecture/embedding-v1.md#embedding-space-publication).

```python
forge.close()
```

This registry is memory-only. Use [save and reopen](../../guide/tutorial.md) for
persistence, then [portable projects](../../guide/portable-projects.md) for
transfer. There is no single-file persistence or live-directory Git workflow.

## Run the native notebook

For developers working from a source checkout, the
[e-commerce agent notebook](https://github.com/CurateLabs/graphforge/blob/main/examples/agent_grounding/ecommerce_agent.ipynb)
provides a larger native example. After building a matching wheel into `dist/`,
run its notebook check on supported durable storage:

```bash
wheel="$(find dist -maxdepth 1 -name '*.whl' -print -quit)"
uv run --isolated --no-project \
  --with "$wheel" \
  --with nbclient==0.11.0 \
  --with ipykernel==7.3.0 \
  python scripts/ci/run-native-notebook.py
```

The runner uses synthetic data and a deterministic loopback provider fixture.
It compares two independent temporary projects and kernels. Those checks prove
specified native behavior; they do not demonstrate better agent reasoning,
production authorization, or a student's understanding.

## Resources

- [Tool recall](agent-tool-recall.md)
- [LLM extraction and review](llm-workflows.md)
- [GraphForge documentation](../../index.md)
- [Cypher guide](../../guide/cypher-guide.md)
- [API reference](../../reference/api.md)
