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

```rust
// The Rust facade returns a frozen, deterministically ordered product view.
let catalog = forge.inspect_runtime_catalog()?;
for entry in catalog.entries {
    println!("{:?}: {} ({})", entry.kind, entry.name, entry.observation_count);
}
```

The snapshot deliberately excludes mutable catalog handles, runtime IDs, and
first/last-seen timestamps. The same Rust-owned contract is available as
`forge.inspect_runtime_catalog()` in Python and
`forge.inspectRuntimeCatalog()` in Node.

### Phase 3: Refinement — structure emerges

After exploring the data, the analyst understands the domain better. They start renaming and normalising.

```rust
use gf_api::OntologySuggestionOptions;

let suggestion = forge.suggest_ontology(OntologySuggestionOptions {
    ontology_id: "analyst-draft".into(),
    version: "0.1.0".into(),
})?;
assert!(suggestion.draft);
assert!(forge.validate_ontology(&suggestion.document).valid);
```

Suggestion is deliberately conservative. Observed labels become concrete entity
types, and properties with a known entity owner become nullable UTF-8 properties.
No constraints, inheritance, cardinality, semantic flags, or property value
types are guessed. Observed relationship names are reported in
`omitted_relation_types` because the runtime catalog does not retain endpoint
evidence sufficient to create a valid relation declaration.

Python uses the same Rust implementation:

```python
suggestion = forge.suggest_ontology("analyst-draft", "0.1.0")
assert suggestion["draft"]
assert forge.validate_ontology(suggestion["document"])["valid"]
forge.export_ontology(
    "suggested",
    "ontology.yaml",
    "yaml",
    document=suggestion["document"],
)
```

The Node names are `suggestOntology`, `validateOntology`, and `exportOntology`;
the result fields use the normal JavaScript camel-case convention.

### Phase 4: Formalisation — optional structure

If the analyst wants to enforce constraints or share a validated graph, they can graduate to an ontology.

```rust
use gf_api::{OntologyExportFormat, OntologyExportSource};

forge.export_ontology(
    OntologyExportSource::Suggested(suggestion.document),
    std::path::Path::new("ontology.yaml"),
    OntologyExportFormat::Yaml,
)?;

// Edit and review the draft, then choose authority explicitly:
forge.load_ontology("ontology.yaml")?; // session-scoped
// or forge.adopt_ontology(...)?;      // durable project authority
```

`Loaded` and `Adopted` are also explicit export sources. Export validates and
canonicalizes entity, relation, property, and constraint declaration order
before serializing and atomically replacing the destination. Authored migration
order is preserved because it breaks ties between equal-length migration routes.
This applies to caller-supplied `Suggested` documents as well as loaded and
adopted documents. Export never changes the live mode, loaded ontology, project
configuration, or durable generation.

Session load and project adoption are intentionally separate in every binding:

```python
forge.load_ontology("ontology.yaml")  # only this live facade

forge.adopt_ontology(
    "ontology.yaml",
    "strict",
    operation_uuid="018f5f0d-65dd-7a88-b6ef-0123456789ab",
)
assert forge.workspace_ontology()["mode"] == "strict"

forge.clear_ontology(
    operation_uuid="018f5f0d-65dd-7a88-b6ef-0123456789ac",
)
```

Reopening a project discards a session load, but observes adopted ontology or
explicit durable absence from the committed workspace generation. Retrying an
adopt or clear with the same operation UUID is idempotent. There is deliberately
no standalone ontology-mode setter: authority and enforcement mode change
together through load, adopt, or clear.

---

## GraphForge Exploratory Mode Features

When the committed workspace records explicit ontology absence (at project
initialization or after `clear_ontology`):

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
