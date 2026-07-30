# Repository integration

GraphForge uses one `.graphforge/` directory. Definitions in `graphforge.yaml`,
`ontology/`, `schemas/`, `seeds/`, and `migrations/` are ordinary reviewable Git
content. Runtime state, imports, and exports are data and must not be committed.

```bash
gf --project-dir . init
gf --project-dir . config validate
gf --project-dir . config resolve --json
gf --project-dir . sync --json
gf --project-dir . remove --yes
```

Commands discover the nearest Git worktree when `--project-dir` is omitted.
`init` preserves existing `.gitignore` content while managing only the three
data exclusions selected by ADR 0016. It refuses to proceed if any of those data
paths are already tracked. `sync` validates only declared definition paths and
digest-addressed sources; it never scans or ingests the repository implicitly.
`remove` requires `--yes` and deletes only `.graphforge/state/`, leaving tracked
definitions, project-local skills, imports, exports, external datasets, and
credentials alone.

The stable machine interface is selected with global `--json`. Configuration
resolution always emits canonical compact JSON and never resolves secret values.
