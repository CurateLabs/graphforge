# ADR 0003: Progressive Ontology — Exploration First

**Status:** Accepted  
**Date:** 2026-05-31  
**Supersedes:** Implicit ontology-required assumption in ADR 0001

---

## Context

GraphForge is a **knowledge analysis workbench** for discovery-oriented analysts. Its primary workloads — OSINT, intelligence analysis, genealogy, entity resolution, investigative journalism, fraud investigation, due diligence, and knowledge construction — all share a common characteristic:

> **The analyst begins with uncertainty and discovers structure over time.**

The analyst does not know what entity types exist before the analysis begins. They encounter raw data, extract entities, observe relationships, and progressively understand the domain. Imposing an ontology before analysis begins is not just inconvenient — it inverts the natural workflow and blocks the primary use case.

At the same time, GraphForge must support **curated knowledge bases** and **validated production graphs** where strict schema enforcement is required for correctness, compliance, and reproducibility.

The existing `requirements.md` already mandates:

> "Data models MUST be optional. MUST NOT be required to create or query graphs. MUST NOT prevent insertion of incomplete or uncertain data by default."

Without an explicit progressive model, several defaults would force ontology-first operation:
- The binder would reject unknown labels/types/properties
- Typed edge tables would require ontology-driven file layout before any write
- The `GraphPlan` IR envelope would treat `ontology_version` as always required
- Project layout would assume `ontology.yaml` is always present

This ADR resolves that tension by formalising **three progressive ontology modes**.

---

## Decision

**Ontology is progressive, not required.**

GraphForge supports three ontology modes that determine how the system responds to labels, relation types, and properties that are not (or not yet) defined in a formal ontology.

### Mode 1: `exploratory`

**No ontology required. All labels and types accepted. No rejections.**

- Analysts may create arbitrary labels, relation types, and properties
- The system accepts heterogeneous and messy data without complaint
- A **RuntimeCatalog** auto-assigns integer IDs for all observed labels/types/properties
- The RuntimeCatalog is persisted to `topology/runtime_catalog.parquet`
- Typed edge tables are not used in exploratory mode — all edges go to `topology/edges/_exploratory.parquet`
- The IR envelope carries `ontology_version: None`
- This is the **default mode when no `ontology.yaml` is present**

### Mode 2: `advisory`

**Ontology present, violations are warnings — not errors.**

- Analysts have defined (or partially defined) an ontology
- Known labels/types/properties are resolved via the OntologyHandle as normal
- Unknown labels/types/properties produce a **warning** and are added to the RuntimeCatalog
- The RuntimeCatalog tracks schema drift: new types, renamed types, property mismatches
- Typed edge tables are used for known relation types; unknown types fall back to `_exploratory.parquet`
- The system may suggest ontology improvements but never blocks data ingestion or queries
- This is the **default mode when `ontology.yaml` is present** and `ontology_mode` is not explicitly set

### Mode 3: `strict`

**Ontology required. Violations produce errors. No unknown types accepted.**

- Unknown labels produce `BindError::UnknownLabel`
- Unknown relation types produce `BindError::UnknownRelationType`
- Unknown properties produce `BindError::UnknownProperty`
- All edge types must be declared in the ontology; typed edge tables cover all edges
- The IR envelope carries a required `ontology_version`
- Appropriate for production deployments, validated knowledge graphs, compliance workflows
- Must be **explicitly enabled** via `ontology_mode: strict` in `graphforge.yaml`

### Default mode logic

| Condition | Default mode |
|---|---|
| No `ontology.yaml`, no `ontology_mode` in manifest | `exploratory` |
| `ontology.yaml` present, no `ontology_mode` in manifest | `advisory` |
| `ontology_mode: strict` in manifest | `strict` |
| `ontology_mode: exploratory` in manifest | `exploratory` (override) |
| `ontology_mode: advisory` in manifest | `advisory` (override) |

---

## RuntimeCatalog

The RuntimeCatalog is an auto-growing registry of observed entity types, relation types, and property names that the system has encountered but that may not be formally defined in an ontology.

```
RuntimeCatalog
├── entity_types      label → RuntimeTypeId (UInt32, auto-assigned)
├── relation_types    name → RuntimeTypeId
├── property_names    name → RuntimePropId
├── statistics        observation counts, first-seen, last-seen timestamps
└── inference_hints   suggested ontology entries based on observed patterns
```

**Key properties:**
- Always present (even in strict mode — records what was observed)
- Persisted to `topology/runtime_catalog.parquet`
- Exportable to `ontology.yaml` via `forge.export_ontology()` (future feature)
- Survives project merges (UUIDs for RuntimeCatalog entries follow the same UUID identity model)
- RuntimeTypeIds are **local** integers (not globally stable like UUID identity) — suitable for query planning within a session, not for cross-project references

**RuntimeCatalog in the binder (exploratory/advisory modes):**

```
bind(label: &str) → TypeId:
1. Check OntologyHandle (if present): return TypeId if found
2. Check RuntimeCatalog: return existing RuntimeTypeId if seen before
3. New label: RuntimeCatalog::intern(label) → new RuntimeTypeId
4. Record: observation count += 1
5. Return RuntimeTypeId for use in GraphOp
```

---

## Storage consequences

### Exploratory mode storage

Without a predefined ontology, the typed edge table layout cannot be pre-determined. Exploratory mode uses a unified fallback:

```
topology/edges/_exploratory.parquet   — all edges, includes rel_type_name: Utf8 column
```

As the RuntimeCatalog grows, the system can optionally reorganise into typed edge files via a maintenance operation (not blocking the write path).

### Advisory mode storage

Known relation types use typed edge tables as normal. Unknown relation types append to `_exploratory.parquet`. When the ontology is later extended to cover a previously-unknown type, a migration operation can promote rows from `_exploratory.parquet` into the typed file.

### Strict mode storage

All edge types are pre-declared; typed edge tables cover all edges; `_exploratory.parquet` is empty.

---

## `graphforge.yaml` manifest changes

The project manifest gains an `ontology_mode` field:

```yaml
# Exploratory project (no ontology)
project_uuid: "0195f3a2-..."
name: "OSINT Investigation Alpha"
version: "1"
ontology_mode: exploratory   # default when no ontology.yaml present
ontology: null

# Advisory project (ontology present, non-enforcing)
project_uuid: "0195f3a2-..."
name: "Entity Graph v2"
version: "1"
ontology_mode: advisory      # default when ontology.yaml present
ontology: "./ontology.yaml"

# Strict project (fully validated)
project_uuid: "0195f3a2-..."
name: "Production Knowledge Base"
version: "1"
ontology_mode: strict        # must be explicitly declared
ontology: "./ontology.yaml"
```

---

## IR envelope changes

`GraphPlan.ontology_version` changes from required to optional:

```rust
GraphPlan {
    ir_version: IrVersion,
    dialect: String,
    ontology_version: Option<OntologyVersion>,  // None in exploratory mode
    ontology_mode: OntologyMode,                // new field
    feature_flags: Vec<String>,
    ops: Vec<GraphOp>,
    exprs: ExprArena,
}
```

---

## Exploratory Analyst Persona

The **Exploratory Analyst** is a first-class design target for GraphForge.

Characteristics:
- Does not know the ontology ahead of time
- Works with messy, incomplete, or heterogeneous data
- Progressively discovers entity types and relationships during analysis
- Frequently renames concepts during investigation
- Creates temporary entity types and relationship types
- Builds graphs incrementally from multiple data sources
- Begins with maximum uncertainty; refines toward structure

Representative users:
- Intelligence analyst
- OSINT investigator
- Journalist
- Genealogist
- Academic researcher
- Due diligence analyst
- Fraud investigator
- Cybersecurity analyst
- Entity resolution engineer

See `docs/guide/exploratory-analyst.md` for the full persona and journey documentation.

---

## Consequences

**Positive:**
- GraphForge remains a workbench for discovery-oriented analysis
- Onboarding friction is eliminated — no ontology required to start
- Exploratory and structured workflows coexist without compromising either
- RuntimeCatalog enables gradual schema discovery and promotes iterative refinement

**Negative / risks:**
- Binder implementation is more complex — must handle three modes
- Storage provider must handle the `_exploratory.parquet` fallback
- RuntimeCatalog adds a persistent artifact to manage
- Advisory mode warnings need a UX surface (CLI, API, Python binding)

**Mitigations:**
- Mode defaults are sensible: analysts doing exploration get exploration; structured projects get structure
- RuntimeCatalog is optional for callers who only use strict mode
- The fallback `_exploratory.parquet` is a simple single-file catch-all with a `rel_type_name: Utf8` column

---

## References

- `docs/development/requirements.md` — "Data models MUST be optional"
- `docs/book/architecture/refactor-v0.5.md` — storage architecture, IR design
- `docs/book/architecture/storage.md` — typed edge tables, topology/properties layout
- `docs/guide/exploratory-analyst.md` — persona and journey documentation
- — project-centric directory structure
- — architecture milestone issue sweep
