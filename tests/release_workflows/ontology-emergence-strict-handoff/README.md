# Ontology emergence and strict-target handoff

This synthetic workflow keeps three state/project tuples explicit:

1. the persistent `source` starts and reopens with no ontology in exploratory mode;
2. an analyst-approved partial ontology applies only to one live `source` session via
   `load_ontology()`, promoting that session to advisory without rewriting project truth;
3. a separate persistent `target` adopts the formal ontology in strict mode before it
   receives any curated finding.

The workflow never calls ontology discovery “truth,” never edits a project
manifest or generation behind GraphForge, never copies source storage into the
target, and never claims a pre-v1 migration. The handoff contract carries
`source_graph_uuid` and `approval_record_uuid` as explicit public mapping fields.

Bulk ingestion uses the shipped public atomic Arrow surfaces
`publish_bulk_nodes` / `publish_bulk_edges` (and the Python/Node projections of
those methods). Legacy placeholder `add_nodes` / `add_edges` overloads are
classified and deliberately unused.

## Bounded local command

```bash
python3 tests/release_workflows/ontology-emergence-strict-handoff/run.py \
  --commit-sha "$(git rev-parse HEAD)"
```

Structural validation only:

```bash
python3 tests/release_workflows/ontology-emergence-strict-handoff/run.py \
  --commit-sha "$(git rev-parse HEAD)" --validate-only
```

This remains opt-in release-candidate evidence and must not join the required PR
`CI Gate`.
