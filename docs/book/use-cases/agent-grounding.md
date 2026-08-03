# AI Agent Grounding with Ontologies

## Overview

**Grounded agents** use knowledge graphs and ontologies to structure domain knowledge, map capabilities to tools, and enable semantic reasoning. This approach helps LLM-based agents make better decisions by grounding their understanding in structured knowledge rather than relying solely on prompt context.

GraphForge **v0.5.0** is a strong fit for agent grounding: embedded Rust core, Arrow results, Parquet projects, Python bindings, and zero-configuration local deployment.

## Why Knowledge Graphs for Agent Grounding?

### The Problem

Modern LLM agents face several challenges:
- **Context limitations**: Cannot fit all domain knowledge in prompts
- **Tool selection**: Choosing the right tool from dozens or hundreds of options
- **Semantic understanding**: Knowing which tools work with which domain concepts
- **Reasoning**: Following relationships between concepts, tools, and actions

### The Solution: Ontological Grounding

A knowledge graph provides:
- **Structured domain model**: Classes, properties, and relationships
- **Tool annotations**: Capabilities mapped to domain concepts
- **Semantic queries**: Find tools by intent, not just keywords
- **Reasoning paths**: Navigate concept hierarchies and relationships

## GraphForge vs. Neo4j for Agent Development

| Feature | GraphForge | Neo4j |
|---------|-----------|-------|
| **Deployment** | Embedded in Python process | Requires server setup |
| **Setup** | `pip install graphforge` | Install server, configure ports, manage service |
| **Integration** | Native Python API | HTTP API or driver |
| **Agent workflow** | Direct in-process access | Network calls add latency |
| **Development** | Zero config, instant start | Configuration, connection strings |
| **Portability** | Runs anywhere Python runs | Requires server infrastructure |
| **Query language** | Full openCypher (same as Neo4j) | openCypher |
| **Best for** | AI/ML development, research, prototyping | Production deployments, massive scale |

**Bottom line**: GraphForge eliminates deployment complexity for agent development while providing full openCypher compatibility.

## Architecture

```
┌─────────────────────────────────────┐
│   LLM Agent (GPT-4, Claude, etc.)   │
│                                     │
│  "Find tools to check inventory"   │
└───────────────┬─────────────────────┘
                │
                ├─ Semantic Query
                │
┌───────────────▼─────────────────────┐
│        GraphForge                   │
│                                     │
│  ┌──────────────────────────────┐  │
│  │     Domain Ontology          │  │
│  │                              │  │
│  │  Product ─IS_A─> Item        │  │
│  │  Inventory ─TRACKS─> Product │  │
│  │                              │  │
│  └──────────────────────────────┘  │
│                                     │
│  ┌──────────────────────────────┐  │
│  │     Tool Definitions         │  │
│  │                              │  │
│  │  check_inventory()           │  │
│  │    ─OPERATES_ON─> Inventory  │  │
│  │    ─REQUIRES─> product_id    │  │
│  │                              │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
                │
                ├─ Matched Tools
                │
┌───────────────▼─────────────────────┐
│   Agent Execution Layer             │
│                                     │
│   Execute: check_inventory("123")  │
└─────────────────────────────────────┘
```

## Implementation Guide

### Step 1: Define Domain Ontology

Create a structured model of your domain:

```python
from graphforge import GraphForge

gf = GraphForge()

# Define class hierarchy
gf.execute("""
    CREATE (:Class {name: 'Entity', description: 'Base class for all domain entities'})
    CREATE (:Class {name: 'Product', description: 'Physical or digital product'})
    CREATE (:Class {name: 'Inventory', description: 'Stock tracking system'})
    CREATE (:Class {name: 'Order', description: 'Customer purchase order'})
""")

# Define IS_A relationships
gf.execute("""
    MATCH (product:Class {name: 'Product'}), (entity:Class {name: 'Entity'})
    CREATE (product)-[:IS_A]->(entity)
""")

# Define domain properties
gf.execute("""
    CREATE (:Property {name: 'price', type: 'float', class: 'Product'})
    CREATE (:Property {name: 'quantity', type: 'int', class: 'Inventory'})
    CREATE (:Property {name: 'status', type: 'string', class: 'Order'})
""")
```

### Step 2: Annotate Tools with Capabilities

Map tools to domain concepts:

```python
# Define a tool
gf.execute("""
    CREATE (t:Tool {
        name: 'check_inventory',
        description: 'Check current stock levels for a product',
        returns: 'int',
        endpoint: 'api.inventory.check'
    })
""")

# Define tool parameters
gf.execute("""
    MATCH (t:Tool {name: 'check_inventory'})
    CREATE (t)-[:HAS_PARAMETER]->(:Parameter {
        name: 'product_id',
        type: 'string',
        required: true,
        description: 'Unique identifier for the product'
    })
""")

# Link tool to domain concepts
gf.execute("""
    MATCH (t:Tool {name: 'check_inventory'}),
          (inv:Class {name: 'Inventory'}),
          (prod:Class {name: 'Product'})
    CREATE (t)-[:OPERATES_ON]->(inv)
    CREATE (t)-[:REQUIRES]->(prod)
""")

# Annotate tool capabilities
gf.execute("""
    MATCH (t:Tool {name: 'check_inventory'})
    CREATE (t)-[:CAN_DO]->(:Capability {name: 'query_stock'})
    CREATE (t)-[:CAN_DO]->(:Capability {name: 'verify_availability'})
""")
```

### Step 3: Query for Tool Selection

The agent queries the ontology to find relevant tools:

```python
def find_tools_for_intent(gf, intent_description):
    """Find tools matching agent intent using semantic queries."""

    # Example: "I need to check product availability"
    query = """
    MATCH (t:Tool)-[:CAN_DO]->(c:Capability)
    WHERE c.name CONTAINS 'availability' OR c.name CONTAINS 'stock'
    RETURN t.name AS tool,
           t.description AS description,
           collect(c.name) AS capabilities
    """

    results = gf.to_dicts(query)
    return results

tools = find_tools_for_intent(gf, "check product availability")
# Returns: [{'tool': 'check_inventory', 'description': '...', 'capabilities': [...]}]
```

### Step 4: Navigate Concept Hierarchies

Find tools that work with a class or its superclasses:

```python
def find_tools_for_entity(gf, entity_class):
    """Find all tools that work with an entity or its parent classes."""

    query = """
    MATCH (c:Class {name: $entity_class})-[:IS_A*0..]->(parent:Class)
    WITH parent
    MATCH (t:Tool)-[:OPERATES_ON]->(parent)
    RETURN DISTINCT t.name AS tool,
           t.description AS description,
           parent.name AS operates_on
    """

    return gf.to_dicts(query, {'entity_class': entity_class})

# Find all tools that work with Products
tools = find_tools_for_entity(gf, 'Product')
```

### Step 5: Get Tool Metadata

Retrieve complete tool signature for execution:

```python
def get_tool_metadata(gf, tool_name):
    """Get complete tool definition including parameters."""

    query = """
    MATCH (t:Tool {name: $tool_name})
    OPTIONAL MATCH (t)-[:HAS_PARAMETER]->(p:Parameter)
    OPTIONAL MATCH (t)-[:OPERATES_ON]->(c:Class)
    OPTIONAL MATCH (t)-[:CAN_DO]->(cap:Capability)
    RETURN t.name AS name,
           t.description AS description,
           t.endpoint AS endpoint,
           collect(DISTINCT {
               name: p.name,
               type: p.type,
               required: p.required,
               description: p.description
           }) AS parameters,
           collect(DISTINCT c.name) AS operates_on,
           collect(DISTINCT cap.name) AS capabilities
    """

    result = gf.to_dicts(query, {'tool_name': tool_name})
    return result[0] if result else None
```

## Integration with Agent Frameworks

### LangChain / LangGraph Integration

```python
from langchain_core.tools import StructuredTool
from langgraph.prebuilt import ToolNode

# Load tools from GraphForge ontology
def load_tools_from_ontology(gf):
    tools_data = gf.to_dicts("""
        MATCH (t:Tool)
        RETURN t.name AS name, t.description AS description, t.endpoint AS endpoint
    """)

    tools = []
    for t in tools_data:
        # Create LangChain tool from ontology definition
        tool = StructuredTool.from_function(
            name=t['name'],
            description=t['description'],
            func=lambda **kwargs, ep=t['endpoint']: call_api(ep, kwargs),  # Your API caller
        )
        tools.append(tool)

    return tools

# Use in a LangGraph agent node
tools = load_tools_from_ontology(gf)
tool_node = ToolNode(tools)
```

### LlamaIndex Integration

```python
from llama_index.core.tools import FunctionTool
from llama_index.core.objects import ObjectRetriever

# Create tools from ontology
def create_llama_tools(gf):
    tools_data = gf.to_dicts("""
        MATCH (t:Tool)
        OPTIONAL MATCH (t)-[:HAS_PARAMETER]->(p:Parameter)
        RETURN t.name AS name,
               t.description AS description,
               collect({name: p.name, type: p.type, required: p.required}) AS params
    """)

    tools = []
    for t in tools_data:
        # Create function signature from ontology
        tool = FunctionTool.from_defaults(
            fn=lambda **kwargs, name=t['name']: execute_tool(name, kwargs),
            name=t['name'],
            description=t['description']
        )
        tools.append(tool)

    return tools

# For dynamic retrieval — subclass ObjectRetriever to use GraphForge as backend
class GraphForgeToolRetriever(ObjectRetriever):
    def __init__(self, gf):
        self._gf = gf

    def retrieve(self, query_str: str) -> list[FunctionTool]:
        tools_data = self._gf.to_dicts("""
            MATCH (t:Tool)-[:CAN_DO]->(c:Capability)
            WHERE c.name CONTAINS $q OR t.description CONTAINS $q
            WITH DISTINCT t WHERE t.deprecated = false
            RETURN t.name AS name, t.description AS description, t.endpoint AS endpoint
            ORDER BY t.latency_ms ASC LIMIT 5
        """, {'q': query_str})
        return [
            FunctionTool.from_defaults(
                fn=lambda **kwargs, ep=t['endpoint']: call_api(ep, kwargs),
                name=t['name'], description=t['description']
            )
            for t in tools_data
        ]
```

### Custom Agent Implementation

```python
class OntologyGroundedAgent:
    """Agent that uses GraphForge ontology for tool grounding."""

    def __init__(self, gf: GraphForge, llm):
        self.gf = gf
        self.llm = llm

    def execute(self, user_query: str):
        """Execute user query with ontology-grounded tool selection."""

        # Step 1: Extract intent from user query
        intent = self.llm.extract_intent(user_query)

        # Step 2: Query ontology for relevant tools
        tools = self.find_tools_for_intent(intent)

        # Step 3: LLM selects best tool and parameters
        selected_tool, params = self.llm.select_tool(user_query, tools)

        # Step 4: Execute tool
        result = self.execute_tool(selected_tool, params)

        return result

    def find_tools_for_intent(self, intent):
        """Query ontology for tools matching intent."""
        query = """
        MATCH (t:Tool)-[:CAN_DO]->(c:Capability)
        WHERE c.name CONTAINS $intent OR t.description CONTAINS $intent
        RETURN t.name AS name,
               t.description AS description,
               collect(c.name) AS capabilities
        """
        return self.gf.to_dicts(query, {'intent': intent})
```

## Advanced Patterns

### Multi-Step Reasoning

Chain tools using relationship traversal:

```python
# Find tools that can be composed
query = """
MATCH (t1:Tool)-[:PRODUCES]->(concept:Class)<-[:REQUIRES]-(t2:Tool)
WHERE t1.name = 'search_products'
RETURN t1.name AS first_tool,
       concept.name AS intermediate,
       t2.name AS next_tool
"""

# Result: search_products -> Product -> check_inventory
# Agent can chain: search_products() |> check_inventory()
```

### Contextual Tool Selection

Select tools based on current conversation context:

```python
def select_contextual_tools(gf, conversation_entities):
    """Find tools relevant to entities mentioned in conversation."""

    query = """
    MATCH (c:Class)<-[:IS_A*0..]-(entity)
    WHERE entity.name IN $entities
    WITH c, entity
    MATCH (t:Tool)-[:OPERATES_ON]->(c)
    RETURN DISTINCT t.name AS tool,
           t.description AS description,
           entity.name AS relevant_to
    ORDER BY tool
    """

    return gf.to_dicts(query, {'entities': conversation_entities})
```

### Permission and Access Control

Model tool permissions in the ontology:

```python
# Define user roles and permissions
gf.execute("""
    CREATE (:Role {name: 'customer', level: 1})
    CREATE (:Role {name: 'admin', level: 10})
""")
gf.execute("""
    MATCH (t:Tool {name: 'check_inventory'}), (r:Role {name: 'customer'})
    CREATE (r)-[:CAN_USE]->(t)
""")
gf.execute("""
    MATCH (t:Tool {name: 'update_inventory'}), (r:Role {name: 'admin'})
    CREATE (r)-[:CAN_USE]->(t)
""")

# Filter tools by user role
def get_authorized_tools(gf, user_role):
    return gf.to_dicts("""
        MATCH (r:Role {name: $role})-[:CAN_USE]->(t:Tool)
        RETURN t.name AS tool, t.description AS description
    """, {'role': user_role})
```

## Benefits of GraphForge for Agent Grounding

### 1. Embedded Architecture
- **No server overhead**: GraphForge runs in your Python process
- **Zero latency**: Direct in-memory queries, no network round-trips
- **Simple deployment**: No ports, no services, no configuration

### 2. Python-Native Integration
- **Seamless**: Import GraphForge like any Python library
- **Type safety**: Python objects in, Python objects out
- **Debugging**: Use standard Python debuggers and tools

### 3. Development Velocity
- **Instant setup**: `pip install graphforge` and you're running
- **Rapid iteration**: No server restarts or connection management
- **Portable**: Works in notebooks, scripts, containers, serverless

### 4. openCypher Compatibility
- **Standard queries**: Same Cypher syntax as Neo4j
- **Transferable skills**: Knowledge applies across graph databases
- **Rich expressiveness**: Full pattern matching, aggregations, paths

### 5. Research-Friendly
- **Inspectable**: Print graph state, examine queries interactively
- **Lightweight**: Perfect for experiments and prototypes
- **Reproducible**: Single-file persistence, easy to version control

## Example Use Cases

### E-commerce Agent
- Ontology: Products, Orders, Inventory, Customers
- Tools: search(), check_stock(), place_order(), track_shipment()
- Queries: Find tools to complete purchase flow

### Customer Support Agent
- Ontology: Issues, Products, Solutions, Procedures
- Tools: search_kb(), create_ticket(), escalate(), get_status()
- Queries: Find resolution tools for issue type

### Data Analysis Agent
- Ontology: Datasets, Metrics, Visualizations, Transformations
- Tools: load_data(), compute_metric(), plot(), export()
- Queries: Find analysis pipeline for metric

### DevOps Agent
- Ontology: Services, Deployments, Monitors, Alerts
- Tools: deploy(), rollback(), check_health(), scale()
- Queries: Find remediation tools for alert type

## Getting Started

### Installation

**pip**

```bash
pip install graphforge
```

**uv** (recommended)

```bash
uv add graphforge
```

### Quick Example

```python
from graphforge import GraphForge

# Create ontology
gf = GraphForge()

# Define domain
gf.execute("""
    CREATE (:Class {name: 'Product'}),
           (:Class {name: 'Inventory'})
""")

# Add tool (CREATE and MATCH must be separate execute() calls)
gf.execute("CREATE (:Tool {name: 'check_stock', description: 'Check product stock'})")
gf.execute("""
    MATCH (t:Tool {name: 'check_stock'}), (i:Class {name: 'Inventory'})
    CREATE (t)-[:OPERATES_ON]->(i)
""")

# Query for tool
results = gf.to_dicts("""
    MATCH (t:Tool)-[:OPERATES_ON]->(c:Class {name: 'Inventory'})
    RETURN t.name AS tool, t.description AS description
""")

print(results)
# [{'tool': 'check_stock', 'description': 'Check product stock'}]
```

## Hybrid Search for Tool Selection

Cypher `WHERE` clauses do exact or range matching. For natural-language tool selection — where an
agent might describe an intent in any phrasing — `GraphForge.find()` composes native BM25 text
retrieval and exact UUID-keyed vector search over graph properties.

### Index Tool Descriptions for Text Search

```python
# After loading the tool graph, build a deterministic text index.
forge.index("Tool", properties=["name", "description"])

text_results = forge.find("product search", label="Tool", limit=5)
assert {"node_uuid", "score", "matched_on"}.issubset(text_results.column_names)
```

### Publish Complete Caller Embeddings

```python
# tool_nodes contains the UUID NodeHandles returned by add_node().
forge.publish_caller_embeddings(
    "tool-intent",
    [
        {"node": tool_nodes["search_products"], "vector": [1.0, 0.0]},
        {"node": tool_nodes["check_inventory"], "vector": [0.8, 0.2]},
        {"node": tool_nodes["place_order"], "vector": [0.0, 1.0]},
    ],
    dimensions=2,
    source_projection={"label": "Tool", "recipe": "agent-grounding-v1"},
)

freshness = forge.inspect_embedding_space_freshness("tool-intent")
assert freshness["state"] == "fresh"
```

### Agent Tool Selection

```python
def select_tools(forge, user_intent: str, intent_vector: list[float], limit: int = 5):
    """Return one stable Arrow table of hybrid-ranked tools."""
    return forge.find(
        user_intent,
        vector=intent_vector,
        label="Tool",
        space="tool-intent",
        limit=limit,
    )


tools = select_tools(forge, "find an available product", [1.0, 0.0])
for hit in tools.to_pylist():
    print(hit["node_uuid"].hex(), hit["score"], hit["matched_on"])
```

### Why Hybrid Over Pure-Cypher

| Approach | Handles phrasing variation | Requires exact keywords | Semantic proximity |
|----------|--------------------------|------------------------|--------------------|
| `WHERE t.name = $name` | No | Yes | No |
| `WHERE t.name CONTAINS $term` | Partial | Yes | No |
| `forge.find(intent, label="Tool")` | Yes (BM25) | No | No |
| `forge.find(intent, vector=vec, ...)` | Yes (BM25 + cosine) | No | Yes |

For ontology-grounded agents, hybrid retrieval narrows the candidate tool list before Cypher
fetches additional structured metadata. Results stay in the canonical Arrow/UUID contract.

### Run the Native Notebook

The [e-commerce agent notebook](https://github.com/CurateLabs/graphforge/blob/main/examples/agent_grounding/ecommerce_agent.ipynb)
is the executable version of this workflow. After building a wheel into `dist/`, run the same
two-clean-run gate used by CI:

```bash
wheel="$(find dist -maxdepth 1 -name '*.whl' -print -quit)"
uv run --isolated --no-project \
  --with "$wheel" \
  --with nbclient==0.11.0 \
  --with ipykernel==7.3.0 \
  python scripts/ci/run-native-notebook.py
```

The gate uses synthetic data, permits only its deterministic loopback provider fixture, and
compares bounded evidence from two independent temporary projects and kernels.

---

## Next Steps

1. **Execute the example**: Run `examples/agent_grounding/ecommerce_agent.ipynb` through the
   native notebook gate above
2. **Design your ontology**: Model your domain classes and relationships
3. **Annotate your tools**: Map tools to domain concepts
4. **Build query patterns**: Create semantic queries for tool selection
5. **Integrate with LLM**: Connect to your agent framework

## Resources

- [GraphForge Documentation](../../index.md)
- [openCypher Tutorial](../../guide/tutorial.md)
- [API Reference](../../reference/api.md)
- [Example: E-commerce Agent](https://github.com/CurateLabs/graphforge/blob/main/examples/agent_grounding/ecommerce_agent.ipynb)

## Conclusion

GraphForge provides the ideal foundation for AI agent grounding:
- **Simple**: Embedded architecture eliminates deployment complexity
- **Powerful**: Full openCypher query expressiveness
- **Fast**: Direct in-process access, no network latency
- **Flexible**: Python-native integration with any agent framework

Start building grounded agents today with zero infrastructure overhead.
