# Agent tool recall

**Advanced:** basic Python, database queries, and terminal use.

This optional developer guide retrieves tools by their declared capability and
checks which declared inputs are available. It assumes Python and
[basic graph use](../../guide/quickstart.md); use the matching
[installation](../../guide/installation.md). It does not install or qualify a
particular agent framework.

If you want an agent to help with a research assignment, use
[Work with an agent](../../guide/work-with-an-agent.md) and
[Your first research project](../../guide/first-research-project.md) instead.

## Build a small registry

```python
from graphforge import GraphForge

forge = GraphForge()
capability = forge.add_node("Capability", name="read source")
source_key = forge.add_node("Input", name="source_key")
reader = forge.add_node("Tool", name="read_document", deprecated=False)
forge.add_edge(reader, "CAN_DO", capability)
forge.add_edge(reader, "REQUIRES", source_key)
```

For a larger registry, give tools stable keys and use `MERGE` for repeat
imports. Share reference nodes deliberately: two distinct `Input` nodes with
the same display name are not automatically the same object.

## Retrieve candidates and inspect requirements

```python
candidates = forge.execute("""
    MATCH (tool:Tool)-[:CAN_DO]->(capability:Capability)
    WHERE capability.name = $capability AND tool.deprecated = false
    RETURN tool.name AS name
    ORDER BY name
""", {"capability": "read source"}).to_pylist()

available_inputs = {"source_key": "note-1"}
for candidate in candidates:
    requirements = forge.execute("""
        MATCH (tool:Tool {name: $name})-[:REQUIRES]->(input:Input)
        RETURN input.name AS name
        ORDER BY name
    """, {"name": candidate["name"]}).to_pylist()
    missing = [row["name"] for row in requirements if row["name"] not in available_inputs]
    print({"tool": candidate["name"], "missing_inputs": missing})
```

Expected output: `{'tool': 'read_document', 'missing_inputs': []}`.

Try `available_inputs = {}` and rerun the loop. It reports
`{'tool': 'read_document', 'missing_inputs': ['source_key']}`. This checks only
the presence of declared input names, not their types, validity, permission, or
freshness. Your application owns those checks and the actual tool invocation.
The example executes no tool.

## Keep selection separate from execution

A `REQUIRES_PERMISSION` edge can describe intended access. It cannot authenticate
a user or enforce permission. Check authorization at the tool boundary even
when a graph query returned that tool. Likewise, `DEPENDS_ON`, `PRODUCES`, and
`SUPERSEDES` can describe application relationships, but do not run a workflow
or automatically retire an implementation.

For candidate recall from names and descriptions, see
[agent grounding](agent-grounding.md#hybrid-search-for-tool-selection). Preserve
actual candidate identities and reasons for selection. Search scores and graph
reachability are not confidence that the tool will solve the user's task.

For a proposed tool chain, inspect required inputs, the result of each preceding
operation, and any explicit human review requirement before executing the next
step. An LLM-generated plan is a proposal, not evidence of completed work.

## Evaluate on your own workload

Measure whether the right tools are retrieved for representative requests,
including ambiguous requests, missing inputs, deprecated tools, and disallowed
actions. Report misses and incorrect selections, not just lookup latency.
Do not infer quality or a universal graph-size limit from a tiny registry.
See [scale guidance](../../reference/scale-limits.md) for workload-specific
measurement and [decision workflows](decision-workflows.md) for optional typed
external choices without automatic action authority.

```python
forge.close()
```

The example is memory-only. Follow [save and reopen](../../guide/tutorial.md)
to retain it and [portable projects](../../guide/portable-projects.md) to move
it. Keep application code, tool implementation, and access enforcement separate
from the stored registry.

## See also

- [Agent grounding](agent-grounding.md)
- [LLM-powered workflows](llm-workflows.md)
- [Cypher guide](../../guide/cypher-guide.md)
- [API reference](../../reference/api.md)
