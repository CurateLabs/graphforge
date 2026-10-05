# Research notes: tool and capability graphs

This Advanced note supports [agent grounding](../use-cases/agent-grounding.md)
and [tool recall](../use-cases/agent-tool-recall.md). It is for application
developers. A student using an existing agent should start with
[Work with an agent](../../guide/work-with-an-agent.md).

## What the graph represents

Tools, declared capabilities, inputs, outputs, dependencies, and deprecation can
be stored as ordinary graph records. Cypher retrieves declared relationships;
`forge.find` retrieves candidates from names/descriptions or a configured vector
space. Neither executes tools nor enforces hosted permissions.

Share reference nodes under stable keys so a dependency refers to the intended
input or capability. A node labeled `Role` or an edge named `MAY_USE` records
application metadata; actual authorization remains at the execution boundary.

## What requires evaluation

Test capability lookup, missing inputs, deprecated tools, ambiguous intent, and
unauthorized actions on the intended registry. Measure candidate recall and
incorrect selections separately from latency. Do not infer agent reasoning
quality from a successful graph query or from the absence of a network hop.

Native examples are linked from the [worked guide](../use-cases/agent-grounding.md#run-the-native-notebook).
Its synthetic notebook checks exercise bounded behavior. They do not prove
real-agent quality, a branded integration, or universal sub-millisecond lookup.
For durable registries and transfer, use [save and reopen](../../guide/tutorial.md)
and [portable projects](../../guide/portable-projects.md).
