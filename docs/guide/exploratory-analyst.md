# The Exploratory Analyst

**Advanced:** this guide assumes basic Python and graph queries. Use it when
you want to turn evolving labels and properties into an explicit schema.
For a first guided project, start with [Your first graph](quickstart.md).

Start without a declared ontology: add records, inspect the labels and
properties you actually used, and decide whether explicit constraints would
help future work. An **ontology** declares the types and rules you want the
engine to check. It is optional for ordinary graph use.

This example builds a person–organization connection, inspects the observed
structure, and exports a draft ontology for review. The draft describes the
graph; it does not validate whether the source claim is true.

### Phase 1: Ingestion — raw and messy

The analyst begins with source data: documents, spreadsheets, open databases, scraped pages. They do not yet know what entity types exist.

```python
from graphforge import GraphForge

forge = GraphForge()  # in-memory; retain/export deliberately before session reset
# No ontology required. GraphForge starts in exploratory mode.

# Ingest whatever you have
alice = forge.add_node("Person", name="Alice", source="doc_001")
acme = forge.add_node("Organization", name="Acme Corp")
doc = forge.add_node("Document", title="Contract 2024")

# Use whatever relationship type makes sense right now
forge.add_edge(alice, "WORKS_AT", acme, confidence=0.9)
forge.add_edge(alice, "MENTIONED_IN", doc)

# Unknown entity types are fine too
unknown_entity = forge.add_node(
    "UnknownEntity",
    raw="mysterious_string_from_data",
)
```

GraphForge accepts these valid inputs without a declared ontology. The runtime
catalog is an inventory of the labels, relationship types, and properties seen
in the graph; it does not decide whether the observations are reliable.

### Phase 2: Exploration — pattern discovery

As the graph grows, the analyst runs queries to discover patterns. GraphForge works with whatever labels and types have been ingested, even without a formal ontology.

```python
catalog = forge.inspect_runtime_catalog()
print(catalog)
```

The snapshot deliberately excludes mutable catalog handles, runtime IDs, and
first/last-seen timestamps. The same Rust-owned contract is available as
`forge.inspect_runtime_catalog()` in Python and
`forge.inspectRuntimeCatalog()` in Node.

### Phase 3: Refinement — structure emerges

After exploring the data, the analyst understands the domain better. They start renaming and normalising.

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

The previous block writes a draft to `ontology.yaml`. Review that file before
loading or adopting it. A session load applies to the current open instance;
adoption records the schema as the durable project's authority.

Loaded and adopted ontologies are also explicit export sources. Export validates and
canonicalizes entity, relation, property, and constraint declaration order
before serializing and atomically replacing the destination. Authored migration
order is preserved because it breaks ties between equal-length migration routes.
This applies to caller-supplied `Suggested` documents as well as loaded and
adopted documents. Export never changes the live mode, loaded ontology, project
configuration, or durable generation.

Session load and project adoption are intentionally separate in every binding:

The following is a separate durable-project example, requiring an admitted
filesystem. Opening this directory does not transfer the earlier in-memory
graph into it. The reopen guarantees below apply to this durable project;
session-only notebook work must be retained or exported deliberately.

```python
from pathlib import Path

forge.close()
Path("investigation-alpha").mkdir()  # New empty directory; choose another if it exists.
forge = GraphForge("investigation-alpha/")
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
forge.close()
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
- **User-defined labels and relationship types** — no ontology declaration needed
- **User-defined properties** — use supported names and value types; input validation still applies
- **RuntimeCatalog** — tracks all observed labels, types, and properties
- **Query support** — full Cypher query support over exploratory data
- **Analysis verbs** — `forge.rank()`, `forge.cluster()`, `forge.find()` all work

---

## Progressive Path

| Stage            | Mode          | What changes                                                                          |
| ---------------- | ------------- | ------------------------------------------------------------------------------------- |
| Raw ingestion    | `exploratory` | Accept valid records without a declared ontology; runtime catalog tracks observations |
| Pattern analysis | `exploratory` | Query freely; discover structure; use suggest_ontology()                              |
| Draft ontology   | `advisory`    | Ontology loaded; violations are warnings; RuntimeCatalog tracks drift                 |
| Validated graph  | `strict`      | Ontology enforced; violations produce errors; typed edge tables cover all types       |

Moving between stages is always the analyst's choice. GraphForge never forces the transition.

---

## References

- [ADR 0003: Progressive Ontology](../adr/0003-progressive-ontology.md)
- [Storage Architecture](../book/architecture/storage.md)
- [Architecture overview](../book/architecture/overview.md)
