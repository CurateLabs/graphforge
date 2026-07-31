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

## Portable export and import

Portable interchange moves one complete, immutable project generation without
copying GraphForge's live project layout. Select the current committed
generation explicitly, or select the generation pinned by a named checkpoint:

```bash
gf --project-dir . export --current --output .graphforge/exports/current.gfportable
gf --project-dir . export --checkpoint before-change \
  --output .graphforge/exports/before-change.gfportable
```

`--current` is resolved when export starts. `--checkpoint NAME` resolves that
checkpoint's pinned generation, even when a checkpoint is literally named
`current`. The two selectors are mutually exclusive. Export verifies the
selected manifest and every participant, then writes a versioned envelope with
the capability inventory, participant metadata, sizes, and integrity hashes in
canonical order. Identical selected state produces identical portable content.
An existing output path is rejected, preventing accidental replacement of a
previous export.

An envelope contains only the selected generation. It never contains `CURRENT`,
checkpoint registries, writer or lease locks, transaction journals, caches,
temporary files, trash, or another generation discovered beside the selected
one. It is therefore not a raw archive of `.graphforge/state/`.

Import accepts a portable envelope only into a new, empty, or pristine
initialized project container. The pristine case is what makes import usable
immediately after `gf init`; any graph mutation or extra project artifact makes
the target ineligible:

```bash
gf --project-dir . import \
  --input .graphforge/imports/incoming.gfportable \
  --idempotency-key 4f6a9b78-887d-4b8e-872b-a8b59059f777
```

Before any project mutation, import validates the envelope format and version,
bounded sizes and counts, canonical participant identities, source filesystem
type, every integrity hash, and every required capability version. Any failure
leaves the target without a newly published `CURRENT`. A successful import
stages and verifies all participants before atomically publishing the complete
generation; import does not merge into or overwrite an existing project.

`.graphforge/imports/` and `.graphforge/exports/` are convenience transfer
areas, not durable project authority. Both are managed Git ignores, so envelopes
placed there remain outside the code repository. Keep tracked schemas,
ontology, migrations, and seed recipes separate from these data files.
