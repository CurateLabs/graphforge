# GraphForge Roadmap

**Last Updated:** 2026-08-07
**Current release line:** v0.5.2 (see [installation](../guide/installation.md))

---

## How to read this page

This roadmap describes **product surfaces and versions**, not internal delivery
milestones. Where a GitHub milestone name appears elsewhere in operator docs
(for example **release-certification** for the coordinated v0.5.x publication), it is paired with
plain-language purpose there; this page stays version- and outcome-oriented.

---

## v0.5.2 — Coordinated release (current)

GraphForge **v0.5.2** is the current coordinated release across Rust crates,
PyPI, npm bindings/CLI/skills, and public docs. Install from the published
packages (see [installation](../guide/installation.md)).

- GitHub Release: https://github.com/CurateLabs/graphforge/releases/tag/v0.5.2
- PyPI: https://pypi.org/project/graphforge/0.5.2/
- npm: `@curatelabs/graphforge`, `@curatelabs/graphforge-cli`,
  `@curatelabs/graphforge-agent-skills` at `0.5.2`
- Docs: https://docs.graphforge.sh/

Capability surface matches the shipped v0.5 engine: openCypher, seven
analyst-intent verbs, Parquet projects, and thin Python/Node bindings.

Mobile bindings (Swift/Kotlin/UniFFI) are **not** part of the current core
package set.

See [Publishing](../engineering/PUBLISHING.md) for artifact destinations and
[release process](../development/release-process.md) for operator sequence.

### Prior coordinated line

**v0.5.1** was the first complete coordinated registry publication after the
incomplete v0.5.0 surface. Prefer v0.5.2 for new installs.

---

## v0.5.0 — Rust core (shipped on `main`)

GraphForge v0.5.0 is an embedded openCypher engine with a Rust core, Apache Arrow
results, and Parquet-backed projects. The public surface is Cypher plus seven
analyst-intent verbs:

```python
forge.execute(cypher)              # openCypher → Arrow Table
forge.rank(label, by=...)          # centrality, structural scoring → Arrow Table + score
forge.cluster(label, by=...)       # community detection, components → Arrow Table + community_id
forge.paths(source, target, by=…)  # shortest paths, flow, reachability → Arrow Table
forge.analyze(label, by=…)         # spanning trees, DAG, coloring, matching, embeddings → Arrow Table
forge.similar(label, by=…)         # pairwise node similarity → Arrow Table
forge.find(query, …)               # text/vector/hybrid search → Arrow Table + score + matched_on
```

Full algorithm catalog: [Algorithm Verbs](../book/architecture/algorithms.md)

Architecture reference: [Architecture Overview](../book/architecture/overview.md),
[Refactor notes](../book/architecture/refactor-v0.5.md)

**Architecture decisions:** [ADR 0001](../adr/0001-rust-core.md), [ADR 0002](../adr/0002-lr1-grammar.md)

### v0.5 capability surface

Status vocabulary (same as architecture docs): **Shipped** = implemented and tested on
`main`; **Partially built** = some paths real, others stubbed; **Designed** = specified, not
yet a complete public capability; **Deferred** = intentionally after the current release line.

Shipped capabilities (present tense — this is the product):

| Capability | Status | What it is |
| --- | --- | --- |
| Compiler + ontology + Graph IR + relational lowering | Shipped | openCypher path into DataFusion |
| Execution + adjacency index | Shipped | Parquet-backed execution; derived CSR adjacency under `indexes/adjacency/` ([ADR 0004](../adr/0004-adjacency-index.md)) |
| Python + Node bindings | Shipped | Thin bindings over `graphforge-api`; Arrow / IPC results |
| Conformance hardening | Shipped | openCypher TCK, fuzzing, and semantic gates |
| Analyst verbs | Shipped | `rank` / `cluster` / `similar` / `paths` / `analyze` → Arrow |
| Find / index | Shipped | `find` + `index` — text, vector, hybrid |
| Knowledge layer | Shipped | Immutable assertions, confidence, evidence, provenance, neutral algorithm runs |
| Epistemic model | Shipped | Append-only status, reasoning, supersession, hypotheses, optional valid time, resolved algorithm dispatch |

Layer boundary notes: [refactor-v0.5 §8](../book/architecture/refactor-v0.5.md#8-v05-capability-layers).


### Merge gates (required before merging to `main`)

- Parser parity — RD+Pratt corpus + syntax goldens pass
- openCypher TCK subset — agreed compliance threshold met
- Ontology round-trips — load/validate/migrate stable
- Arrow/IPC round-trips — data contract stable across shipped Rust, Python, and Node surfaces
- Parquet provider — core semantics verified
- Python + Node bindings — packaging and smoke tests pass
- Observability — `explain`, query IDs, provenance IDs, structured errors
- All seven analyst verbs — Arrow Tables, write-back, via/directed filters
- `forge.find()` / `forge.index()` — lazy indexing, text + vector + hybrid

---

## Language Binding Matrix

| Language | Mechanism | Result | Status |
| ----------------- | -------------- | ------------------------------------------ | --------- |
| Python | PyO3 + maturin | `pyarrow.Table` | Shipped (v0.5) |
| Node / TypeScript | napi-rs | Arrow IPC `Buffer` | Shipped (v0.5) |
| Rust | Native crate | `ExecutionResult` | Shipped (semantic owner) |
| Swift | UniFFI | Arrow IPC `Data` → `GraphForgeResult` | Planned (after v0.5.1 core publication) |
| Kotlin / JVM | UniFFI | Arrow IPC `ByteArray` → `GraphForgeResult` | Planned (after v0.5.1 core publication) |


---

## Version Numbering

GraphForge follows [Semantic Versioning](https://semver.org/) and is **pre-v1.0**.
The `0.x` series signals that the API is still maturing.

- **Patch** (`0.5.y`): Bug fixes, small improvements, no intentional API breaks
- **Minor** (`0.x.0`): New features; backwards-compatible where practical

**v0.5.1** is the current coordinated release. A `v1.0` release will happen when
the API is stable enough to commit to long-term compatibility.
