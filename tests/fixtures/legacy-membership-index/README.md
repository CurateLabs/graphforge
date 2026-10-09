# Legacy membership index fixture

A real durable Project written before #1902 by the binary that produced the UUID
membership index (`topology/uuid-membership/manifest.json`,
`identities-v5-*.uuidx`, `node-surrogates-v5-*.uuidx`, `topology-receipt.json`).
`crates/graphforge-api/tests/legacy_membership_index.rs` copies it to a private
directory and proves that such a project still opens, appends, refuses
duplicates, exports, verifies and imports once nothing reads the index.

Contents: 14 `Person` nodes and 14 `KNOWS` edges from two `import-session`
commits. The first builds eight nodes and a ring of eight edges (bulk builder);
the second appends six nodes and a ring of six edges (staged path, so the index
carries a delta run). UUIDs are v7-shaped and derived from the node or edge
index: `(kind << 100) | (0x7 << 76) | (0x2 << 62) | index` with kind 1 for
nodes and 2 for edges; the base uses indexes 0-7 and the append 100-105.

Empty directories are not checked in; the Project recreates them. Git does not
preserve the producer's read-only CAS permissions, so the test restores them on
the copied `graph-objects/sha256/` payloads.

## Regenerate

Build `gf` from the parent of the #1902 commit (the index is gone after it) and
run, with `TMPDIR` on ext4:

```bash
mkdir base append
python3 tiny_data.py base 0 8 0        # nodes.parquet and edges.parquet
python3 tiny_data.py append 100 6 100
gf --project project import-session begin --operation-uuid 00000000-0000-4000-8000-000000001001
gf --project project import-session register-parquet --session-uuid 00000000-0000-4000-8000-000000001001 --path base/nodes.parquet --kind nodes
gf --project project import-session register-parquet --session-uuid 00000000-0000-4000-8000-000000001001 --path base/edges.parquet --kind edges
gf --project project import-session validate --session-uuid 00000000-0000-4000-8000-000000001001
gf --project project import-session commit --session-uuid 00000000-0000-4000-8000-000000001001
# repeat with session 00000000-0000-4000-8000-000000001002 and the append directory
```

`tiny_data.py` writes `node_uuid`/`label` and `edge_uuid`/`rel_type`/`source_uuid`/
`target_uuid` columns with the UUID formula above and a ring of edges over its
nodes. Delete `.graphforge-construction`, `.graphforge-query-spill` and
`import-sessions` from the result.
