# AI Agent Tool Recall

How to use GraphForge as the tool registry for LLM agents with large, structured tool libraries.

---

## The Problem: Tool Selection at Scale

When an LLM agent has 10 tools, you can describe all of them in the system prompt. At 100 tools, the prompt is bloated and selection accuracy drops. At 1000+ tools, it is simply impossible — description alone exceeds a typical context window.

Even with retrieval, flat lists and dense embeddings have structural blind spots:

- **Synonyms and indirection**: "inventory", "stock", "supply" are semantically related but lexically distinct. An embedding retrieval may miss tools connected only via indirect entity relationships.
- **Dependencies**: `place_order` requires a `product_id` that must come from `search_products` first. Flat lists cannot express this.
- **Permissions**: In multi-role systems, which tools is *this* user allowed to call?
- **Deprecation**: `update_inventory_v1` was superseded by `update_inventory_v2`. The agent should never select the old one.

A knowledge graph captures all of this structure explicitly.

---

## Graph Schema for Tool Libraries

### Node Types

```python
# In Cypher node creation:
# (:Tool)           — a callable function or API endpoint
# (:Parameter)      — a named input to a Tool
# (:OutputType)     — the return type of a Tool
# (:Capability)     — an abstract action (e.g. "query_stock")
# (:Domain)         — a subject area (e.g. "inventory_management")
# (:DataModel)      — a data entity the tool reads or writes
# (:Role)           — a permission level
```

### Relationship Types

| Edge | Direction | Semantics |
|------|-----------|-----------|
| `HAS_PARAMETER` | Tool → Parameter | Input declaration |
| `RETURNS` | Tool → OutputType | Return type |
| `CAN_DO` | Tool → Capability | What the tool achieves |
| `BELONGS_TO_DOMAIN` | Tool → Domain | Subject area |
| `REQUIRES` | Tool → DataModel | Must have this data before calling |
| `PRODUCES` | Tool → DataModel | Outputs data of this type |
| `REQUIRES_PERMISSION` | Tool → Role | Minimum permission level |
| `DEPENDS_ON` | Tool → Tool | Runtime dependency |
| `SUPERSEDES` | Tool → Tool | Newer tool replacing older |
| `SIMILAR_TO` | Tool → Tool | Functional substitute |

---

## Building the Tool Graph

### Define Tools

Shared reference nodes (DataModel, Capability, Role, Domain) should be created once and
linked via `MATCH` — not inline with `CREATE` per tool — so chain queries like
`PRODUCES → DataModel ← REQUIRES` correctly match across tools.

```python
from graphforge import GraphForge

db = GraphForge()  # or GraphForge("tools/") for persistence

# Create shared reference nodes first
db.execute("""
    CREATE (:DataModel {name: 'Product'})
    CREATE (:DataModel {name: 'PaymentMethod'})
    CREATE (:Capability {name: 'query_stock', category: 'read'})
    CREATE (:Capability {name: 'verify_availability', category: 'read'})
    CREATE (:Capability {name: 'discover_products', category: 'read'})
    CREATE (:Capability {name: 'purchase', category: 'write'})
    CREATE (:Domain {name: 'inventory_management'})
    CREATE (:Role {name: 'customer', level: 1})
""")

db.execute("""
    CREATE (:Tool {
        name: 'check_inventory',
        description: 'Check current stock levels for a product',
        endpoint: 'api.inventory.check',
        version: '2.1',
        cost_per_call: 1,
        latency_ms: 45,
        deprecated: false
    })
""")
db.execute("""
    MATCH (t:Tool {name: 'check_inventory'}),
          (p:Parameter) WHERE false  WITH t
    CREATE (t)-[:HAS_PARAMETER]->(:Parameter {
        name: 'product_id', type: 'string', required: true,
        description: 'Unique identifier for the product'
    })
    CREATE (t)-[:RETURNS]->(:OutputType {name: 'StockLevel', type: 'int'})
""")
db.execute("""
    MATCH (t:Tool {name: 'check_inventory'}),
          (qs:Capability {name: 'query_stock'}),
          (va:Capability {name: 'verify_availability'}),
          (d:Domain {name: 'inventory_management'}),
          (dm:DataModel {name: 'Product'}),
          (r:Role {name: 'customer'})
    CREATE (t)-[:CAN_DO]->(qs)
    CREATE (t)-[:CAN_DO]->(va)
    CREATE (t)-[:BELONGS_TO_DOMAIN]->(d)
    CREATE (t)-[:REQUIRES]->(dm)
    CREATE (t)-[:REQUIRES_PERMISSION]->(r)
""")

db.execute("""
    CREATE (:Tool {
        name: 'search_products',
        description: 'Search for products by name, category, or attribute',
        endpoint: 'api.catalog.search',
        version: '3.0',
        cost_per_call: 2,
        latency_ms: 120,
        deprecated: false
    })
""")
db.execute("""
    MATCH (t:Tool {name: 'search_products'}),
          (cap:Capability {name: 'discover_products'}),
          (dm:DataModel {name: 'Product'}),
          (r:Role {name: 'customer'})
    CREATE (t)-[:HAS_PARAMETER]->(:Parameter {name: 'query', type: 'string', required: true})
    CREATE (t)-[:RETURNS]->(:OutputType {name: 'ProductList', type: 'list'})
    CREATE (t)-[:CAN_DO]->(cap)
    CREATE (t)-[:PRODUCES]->(dm)
    CREATE (t)-[:REQUIRES_PERMISSION]->(r)
""")

db.execute("""
    CREATE (:Tool {
        name: 'place_order',
        description: 'Place an order for a product',
        endpoint: 'api.orders.create',
        version: '1.5',
        cost_per_call: 5,
        latency_ms: 200,
        deprecated: false
    })
""")
db.execute("""
    MATCH (t:Tool {name: 'place_order'}),
          (cap:Capability {name: 'purchase'}),
          (prod:DataModel {name: 'Product'}),
          (pay:DataModel {name: 'PaymentMethod'}),
          (dep:Tool {name: 'check_inventory'}),
          (r:Role {name: 'customer'})
    CREATE (t)-[:HAS_PARAMETER]->(:Parameter {name: 'product_id', type: 'string', required: true})
    CREATE (t)-[:HAS_PARAMETER]->(:Parameter {name: 'quantity', type: 'int', required: true})
    CREATE (t)-[:CAN_DO]->(cap)
    CREATE (t)-[:REQUIRES]->(prod)
    CREATE (t)-[:REQUIRES]->(pay)
    CREATE (t)-[:DEPENDS_ON {type: 'required'}]->(dep)
    CREATE (t)-[:REQUIRES_PERMISSION]->(r)
""")
```

### Register Tool Versions and Supersessions

```python
db.execute("""
    MATCH (old:Tool {name: 'update_inventory_v1'})
    MATCH (new:Tool {name: 'update_inventory_v2'})
    CREATE (new)-[:SUPERSEDES {
        migration_guide: 'Replace quantity_delta with absolute_quantity',
        breaking_changes: 'Parameter renamed'
    }]->(old)
    SET old.deprecated = true, old.replacement_tool = 'update_inventory_v2'
""")
```

---

## Querying the Tool Graph

### Find Tools by Intent

```python
results = db.to_dicts("""
    MATCH (t:Tool)-[r:CAN_DO]->(cap:Capability)
    WHERE cap.name CONTAINS 'stock' OR cap.description CONTAINS 'inventory'
    WITH t, max(1.0) AS confidence
    WHERE t.deprecated = false
    RETURN t.name AS tool, t.description AS description, t.endpoint AS endpoint
    ORDER BY t.latency_ms ASC
    LIMIT 5
""")

for row in results:
    print(row['tool'], '—', row['description'])
# check_inventory — Check current stock levels for a product
# verify_stock — Verify stock is sufficient for order fulfillment
```

### Find Executable Tools (Given Available Data)

Only return tools whose requirements are already satisfied:

```python
available_data = ['Product', 'Session']  # what the agent already has

results = db.to_dicts("""
    MATCH (t:Tool)
    WHERE t.deprecated = false
    AND NOT EXISTS {
        MATCH (t)-[:REQUIRES]->(dm:DataModel)
        WHERE NOT dm.name IN $available
    }
    RETURN t.name AS tool, t.description AS description
    ORDER BY t.cost_per_call ASC
""", {'available': available_data})
```

### Discover Tool Chains

Find what tools can follow a given tool (data flow reasoning):

```python
results = db.to_dicts("""
    MATCH (t1:Tool {name: 'search_products'})-[:PRODUCES]->(dm:DataModel)<-[:REQUIRES]-(t2:Tool)
    WHERE t2.deprecated = false
    RETURN t2.name AS next_tool, t2.description AS description, dm.name AS via
    ORDER BY t2.cost_per_call ASC
""")

for row in results:
    print(f"After search_products → {row['next_tool']} (via {row['via']})")
# After search_products → check_inventory (via Product)
# After search_products → place_order (via Product)
```

### Permission-Based Filtering

```python
user_role = 'customer'

results = db.to_dicts("""
    MATCH (t:Tool)-[:REQUIRES_PERMISSION]->(r:Role)
    WHERE r.name = $role AND t.deprecated = false
    OPTIONAL MATCH (t)-[:CAN_DO]->(cap:Capability)
    RETURN t.name AS tool, collect(cap.name) AS capabilities
    ORDER BY t.name
""", {'role': user_role})
```

### Find Substitutes (Jaccard Similarity)

Identify tools with overlapping capabilities as alternatives:

```python
# Pre-compute target_total once to avoid re-scanning it once per candidate (25x faster at 1000 tools)
results = db.to_dicts("""
    MATCH (target:Tool {name: 'check_inventory'})-[:CAN_DO]->(tc:Capability)
    WITH count(tc) AS target_total
    MATCH (target:Tool {name: 'check_inventory'})-[:CAN_DO]->(shared:Capability)<-[:CAN_DO]-(alt:Tool)
    WHERE alt.name <> 'check_inventory' AND alt.deprecated = false
    WITH alt, count(shared) AS shared_caps, target_total
    MATCH (alt)-[:CAN_DO]->(ac:Capability)
    WITH alt, shared_caps, target_total, count(ac) AS alt_total
    WITH alt, toFloat(shared_caps) / (target_total + alt_total - shared_caps) AS jaccard
    RETURN alt.name AS substitute, jaccard AS similarity
    ORDER BY jaccard DESC
    LIMIT 5
""")
```

---

## ToolRegistry Class

Wrap the graph queries in a class for your agent:

```python
from graphforge import GraphForge


class ToolRegistry:
    def __init__(self, db_path: str | None = None, db: GraphForge | None = None):
        self.db = db if db else (GraphForge(db_path) if db_path else GraphForge())

    def find_by_capability(self, capability: str, limit: int = 5) -> list[dict]:
        return self.db.to_dicts("""
            MATCH (t:Tool)-[:CAN_DO]->(cap:Capability)
            WHERE toLower(cap.name) CONTAINS toLower($cap)
               OR toLower(cap.description) CONTAINS toLower($cap)
               OR toLower(t.description) CONTAINS toLower($cap)
            WITH DISTINCT t
            WHERE t.deprecated = false
            RETURN t.name AS name, t.description AS description,
                   t.endpoint AS endpoint, t.latency_ms AS latency_ms
            ORDER BY t.latency_ms ASC
            LIMIT $limit
        """, {'cap': capability, 'limit': limit})

    def get_prerequisites(self, tool_name: str) -> list[str]:
        rows = self.db.to_dicts("""
            MATCH (t:Tool {name: $name})-[:REQUIRES]->(dm:DataModel)
            RETURN dm.name AS data_model
        """, {'name': tool_name})
        return [r['data_model'] for r in rows]

    def next_tools(self, tool_name: str) -> list[dict]:
        return self.db.to_dicts("""
            MATCH (t:Tool {name: $name})-[:PRODUCES]->(dm:DataModel)<-[:REQUIRES]-(next:Tool)
            WHERE next.deprecated = false
            RETURN next.name AS name, next.description AS description, dm.name AS via
        """, {'name': tool_name})

    def authorized_tools(self, role: str) -> list[str]:
        rows = self.db.to_dicts("""
            MATCH (t:Tool)-[:REQUIRES_PERMISSION]->(r:Role {name: $role})
            WHERE t.deprecated = false
            RETURN t.name AS name
            ORDER BY t.name
        """, {'role': role})
        return [r['name'] for r in rows]

    def substitutes(self, tool_name: str, top_k: int = 3) -> list[dict]:
        # Pre-compute target_total once — avoids re-scanning it per candidate (25x faster at scale)
        return self.db.to_dicts("""
            MATCH (target:Tool {name: $name})-[:CAN_DO]->(tc:Capability)
            WITH count(tc) AS target_total, $name AS tname
            MATCH (target:Tool {name: tname})-[:CAN_DO]->(shared:Capability)<-[:CAN_DO]-(alt:Tool)
            WHERE alt.name <> tname AND alt.deprecated = false
            WITH alt, count(shared) AS shared_caps, target_total
            MATCH (alt)-[:CAN_DO]->(ac:Capability)
            WITH alt, shared_caps, target_total, count(ac) AS alt_total
            WITH alt, toFloat(shared_caps) / (target_total + alt_total - shared_caps) AS jaccard
            RETURN alt.name AS name, jaccard AS similarity
            ORDER BY jaccard DESC
            LIMIT $k
        """, {'name': tool_name, 'k': top_k})
```

---

## Integration with LLM Agents

### Pattern: Retrieval Then Graph Reranking

For large tool libraries (100+ tools), combine embedding retrieval with graph reranking:

```python
from graphforge import GraphForge

registry = ToolRegistry("tools/")

def select_tools(agent_intent: str, user_role: str, available_data: list[str]) -> list[dict]:
    # Stage 1: candidate retrieval by capability keywords
    candidates = registry.find_by_capability(agent_intent, limit=20)

    # Stage 2: filter by permission
    authorized = set(registry.authorized_tools(user_role))
    candidates = [t for t in candidates if t['name'] in authorized]

    # Stage 3: filter by data preconditions
    executable = []
    for tool in candidates:
        prereqs = registry.get_prerequisites(tool['name'])
        if all(p in available_data for p in prereqs):
            executable.append(tool)

    # Stage 4: rank by latency (cheapest first)
    return executable[:5]
```

### Pattern: Multi-Step Planning

Build a tool chain for a goal:

```python
def plan_workflow(start_data: list[str], goal_capability: str, db: GraphForge) -> list[str]:
    """BFS over the tool graph to find a sequence reaching goal_capability."""
    rows = db.to_dicts("""
        MATCH path = (start:DataModel)<-[:REQUIRES*1..4]-(t:Tool)-[:CAN_DO]->(cap:Capability)
        WHERE start.name IN $available
          AND cap.name = $goal
          AND all(step IN nodes(path) WHERE NOT (step:Tool AND step.deprecated))
        RETURN [node IN nodes(path) WHERE node:Tool | node.name] AS tool_chain
        ORDER BY length(path) ASC
        LIMIT 1
    """, {'available': start_data, 'goal': goal_capability})

    return rows[0]['tool_chain'] if rows else []
```

### Pattern: Explain Tool Selection

Return a reasoning path alongside the selected tool:

```python
def explain_selection(intent: str, selected_tool: str, db: GraphForge) -> str:
    rows = db.to_dicts("""
        MATCH (t:Tool {name: $tool})-[:CAN_DO]->(cap:Capability)
        RETURN collect(cap.name) AS capabilities
    """, {'tool': selected_tool})

    caps = rows[0]['capabilities']  # already a plain list via to_dicts()
    return (
        f"Selected '{selected_tool}' because it can: {', '.join(caps)}. "
        f"Intent '{intent}' matched capability description."
    )
```

---

## Complete Example: E-Commerce Agent

```python
from graphforge import GraphForge

# --- Build tool graph ---
db = GraphForge("ecommerce-tools/")

# Create shared nodes first, then link tools via MATCH
db.execute("""
    CREATE (:DataModel {name: 'Product'})
    CREATE (:Capability {name: 'find_products', category: 'read'})
    CREATE (:Capability {name: 'query_stock',   category: 'read'})
    CREATE (:Capability {name: 'purchase',      category: 'write'})
""")

db.execute("""
    CREATE (search:Tool {name: 'search_products', description: 'Search product catalog',
                         endpoint: 'catalog/search', version: '3.0', deprecated: false,
                         cost_per_call: 2, latency_ms: 120})
    CREATE (check:Tool  {name: 'check_inventory',  description: 'Check stock levels',
                         endpoint: 'inventory/check', version: '2.1', deprecated: false,
                         cost_per_call: 1, latency_ms: 45})
    CREATE (order:Tool  {name: 'place_order',       description: 'Place a customer order',
                         endpoint: 'orders/create', version: '1.5', deprecated: false,
                         cost_per_call: 5, latency_ms: 200})
    CREATE (order)-[:DEPENDS_ON {type: 'required'}]->(check)
""")

db.execute("""
    MATCH (search:Tool {name: 'search_products'}), (check:Tool {name: 'check_inventory'}),
          (order:Tool {name: 'place_order'}),
          (fp:Capability {name: 'find_products'}), (qs:Capability {name: 'query_stock'}),
          (pur:Capability {name: 'purchase'}), (prod:DataModel {name: 'Product'})
    CREATE (search)-[:CAN_DO]->(fp)
    CREATE (check)-[:CAN_DO]->(qs)
    CREATE (order)-[:CAN_DO]->(pur)
    CREATE (search)-[:PRODUCES]->(prod)
    CREATE (check)-[:REQUIRES]->(prod)
    CREATE (order)-[:REQUIRES]->(prod)
""")

registry = ToolRegistry(db=db)  # pass existing GraphForge instance

# --- Agent loop ---
available_data = []
intent = "I want to buy a laptop"

# 1. Find tools for intent
candidates = registry.find_by_capability("find_products")
print("Candidates:", [t['name'] for t in candidates])
# ['search_products']

# 2. Agent calls search_products → gets Product back
available_data.append('Product')

# 3. What can we do now?
follow_ups = registry.next_tools('search_products')
print("Next:", [t['name'] for t in follow_ups])
# ['check_inventory', 'place_order']

# 4. check_inventory first (place_order depends on it)
# 5. place_order to complete purchase
```

---

## Why GraphForge vs. Alternatives

| | GraphForge | Neo4j | Vector DB only |
|---|---|---|---|
| **Deployment** | `pip install`, embedded | Server (Docker/cloud) | Cloud API |
| **Latency** | Sub-millisecond (in-process) | 5–50 ms (network) | 10–100 ms (network) |
| **Queryability** | Full openCypher | Full Cypher | Similarity only |
| **Explainability** | Full query trace | Full query trace | Opaque similarity score |
| **Dependencies** | No external services | Neo4j server | External service |
| **Cost** | Free | Enterprise license | Per-query pricing |

GraphForge is the right choice when you need:
- Zero deployment overhead (notebook, script, serverless function)
- Explainable, deterministic tool selection
- Python-native integration with LangChain, LlamaIndex, or custom agents
- Graphs up to ~10M nodes (typical tool libraries are tens of thousands)

For production multi-tenant deployments with > 10M nodes or concurrent writes, use Neo4j or Memgraph with the same openCypher queries.

---

## Performance Notes

- GraphForge handles tool libraries of **10,000+ tools** without configuration changes
- Label-indexed queries (`:Tool`, `:Capability`) scan only relevant nodes
- For repeated queries within a session, cache results in Python dicts — GraphForge is fast but querying once and caching is faster still
- Persist with `GraphForge("tools/")` to avoid rebuilding the graph on every agent startup

---

## See Also

- [Agent Grounding](agent-grounding.md) — ground agents in domain ontologies with defined vocabularies
- [LLM-Powered Workflows](llm-workflows.md) — store and query LLM extractions in GraphForge
- [Cypher Reference](../../guide/cypher-guide.md) — full query language documentation
- [API Reference](../../reference/api.md) — Python API
