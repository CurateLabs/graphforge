# The Exploratory Analyst

GraphForge is designed for analysts who build knowledge from the ground up. This guide describes the **Exploratory Analyst** persona and the journey GraphForge supports from raw, uncertain data to structured, actionable knowledge.

---

## Who Is the Exploratory Analyst?

The Exploratory Analyst works with incomplete information. They do not know what they will find before they begin. They create graphs incrementally, rename concepts as understanding evolves, and progressively discover the structure of a domain rather than defining it in advance.

### Representative roles

| Role | What they investigate |
|---|---|
| Intelligence analyst | Networks of actors, connections, and influence |
| OSINT investigator | Open-source information graphs, entity relationships |
| Investigative journalist | Corporate structures, financial flows, social networks |
| Genealogist | Family trees, historical records, identity resolution |
| Academic researcher | Citation networks, collaboration graphs, concept maps |
| Due diligence analyst | Corporate ownership, key people, risk indicators |
| Fraud investigator | Transaction networks, identity fraud, synthetic identities |
| Cybersecurity analyst | Attack graphs, actor attribution, infrastructure mapping |
| Entity resolution engineer | Deduplication across heterogeneous sources |

---

## The Exploratory Journey

Exploratory analysis has a characteristic arc. GraphForge is designed to support the entire journey without requiring structure before it has been discovered.

### Phase 1: Ingestion — raw and messy

The analyst begins with source data: documents, spreadsheets, open databases, scraped pages. They do not yet know what entity types exist.

```python
forge = GraphForge.new("investigation-alpha/")
# No ontology required. GraphForge starts in exploratory mode.

# Ingest whatever you have
alice = forge.add_node(labels=["Person"], props={"name": "Alice", "source": "doc_001"})
acme = forge.add_node(labels=["Organization"], props={"name": "Acme Corp"})
doc = forge.add_node(labels=["Document"], props={"title": "Contract 2024"})

# Use whatever relationship type makes sense right now
forge.add_edge(alice, acme, type="WORKS_AT", confidence=0.9)
forge.add_edge(alice, doc, type="MENTIONED_IN")

# Unknown entity types are fine too
unknown_entity = forge.add_node(
    labels=["UnknownEntity"],
    props={"raw": "mysterious_string_from_data"}
)
```

GraphForge accepts all of this without complaint. The RuntimeCatalog records every label, relation type, and property observed.

### Phase 2: Exploration — pattern discovery

As the graph grows, the analyst runs queries to discover patterns. GraphForge works with whatever labels and types have been ingested, even without a formal ontology.

```python
# Who is connected to Alice?
result = forge.execute("""
    MATCH (a:Person {name: 'Alice'})-[r]->(b)
    RETURN b.name, type(r)
""")

# What entity types have I seen so far?
catalog = forge.runtime_catalog()
print(catalog.entity_types())    # ["Person", "Organization", "Document", "UnknownEntity"]
print(catalog.relation_types())  # ["WORKS_AT", "MENTIONED_IN"]

# What properties have I seen on Person nodes?
print(catalog.properties_for("Person"))  # ["name", "source"]
```

### Phase 3: Refinement — structure emerges

After exploring the data, the analyst understands the domain better. They start renaming and normalising.

```python
# I realise "UnknownEntity" should be "Location"
forge.execute("""
    MATCH (n:UnknownEntity)
    SET n:Location
    REMOVE n:UnknownEntity
""")

# I want to see what an ontology would look like for this graph
suggested_ontology = forge.suggest_ontology()
print(suggested_ontology)
# entity_types: [Person, Organization, Document, Location]
# relation_types: [WORKS_AT, MENTIONED_IN]
# properties: {Person: [name, source], Organization: [name]}
```

### Phase 4: Formalisation — optional structure

If the analyst wants to enforce constraints or share a validated graph, they can graduate to an ontology.

```python
# Export a draft ontology
forge.export_ontology("ontology.yaml")

# Edit ontology.yaml to add constraints, types, cardinality...
# Then switch to advisory mode
forge.set_ontology_mode("advisory")
forge.load_ontology("ontology.yaml")

# Now the system will warn on schema violations but still accept data
```

For production graphs, strict mode enforces all constraints:

```python
forge.set_ontology_mode("strict")
# Now unknown labels will raise BindError
```

---

## GraphForge Exploratory Mode Features

When no ontology is present (or `ontology_mode: exploratory` is set in `graphforge.yaml`):

- **No ontology required** — start immediately with `forge.add_node()` / `forge.add_edge()`
- **Arbitrary labels** — any string is a valid label
- **Arbitrary relation types** — any string is a valid relation type
- **Arbitrary properties** — any key-value pair is valid
- **RuntimeCatalog** — tracks all observed labels, types, and properties
- **Query support** — full Cypher query support over exploratory data
- **Analysis verbs** — `forge.rank()`, `forge.cluster()`, `forge.find()` all work
- **No validation errors** — the system never rejects data in exploratory mode

---

## Design Philosophy

GraphForge is a **Knowledge Analysis Workbench** — not a graph database. The product positioning is:

```
Graph Database → Graph Analytics Engine → Knowledge Analysis Workbench
```

GraphForge optimizes for analyst workflows, not storage workflows. Just as a physical workbench does not demand you know the final shape of an object before you pick up a tool, GraphForge does not require you to define your schema before you begin analysing data.

The appropriate workflow is:

```
Observe → Collect → Explore → Understand → (Optionally) Formalise
```

Not:

```
Define Schema → Import Data → Query
```

The ontology is a **destination**, not a prerequisite.

---

## The Knowledge Journey: Exploration → Understanding → Formalization

### Stage 1: Exploration

```
Messy Data → Documents → Entities → Relationships
```

No ontology. No schema. No structure assumptions. The analyst ingests whatever they have and builds a graph incrementally. The RuntimeCatalog records everything observed.

### Stage 2: Understanding

```
Emerging Patterns → Candidate Types → Candidate Relationships
```

The analyst queries the graph, discovers patterns, and the RuntimeCatalog evolves. `forge.suggest_ontology()` can propose structure based on observations.

### Stage 3: Formalization

```
Runtime Catalog → Ontology → Repeatable Workflow
```

The analyst hardens discovered structure into a formal ontology. Workflows become repeatable. The graph can be validated and shared with confidence.

---

## Progressive Path

| Stage | Mode | What changes |
|---|---|---|
| Raw ingestion | `exploratory` | Accept everything; no schema; RuntimeCatalog tracks observations |
| Pattern analysis | `exploratory` | Query freely; discover structure; use suggest_ontology() |
| Draft ontology | `advisory` | Ontology loaded; violations are warnings; RuntimeCatalog tracks drift |
| Validated graph | `strict` | Ontology enforced; violations produce errors; typed edge tables cover all types |

Moving between stages is always the analyst's choice. GraphForge never forces the transition.

---

## References

- [ADR 0003: Progressive Ontology](../adr/0003-progressive-ontology.md)
- [Storage Architecture](../book/architecture/storage.md)
- [Architecture Refactor v0.5](../book/architecture/refactor-v0.5.md)
