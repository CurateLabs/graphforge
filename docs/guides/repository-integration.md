# Repository integration

GraphForge uses one `.graphforge/` directory. Definitions in `graphforge.yaml`,
`ontology/`, `schemas/`, `seeds/`, and `migrations/` are ordinary reviewable Git
content. Runtime state, imports, and exports are data and must not be committed.

Run the same native lifecycle contract from either published package:

```bash
uvx graphforge init
npx @graphforge/cli init
```

Both entry points forward arguments to the Rust CLI and preserve its exact
stdout, stderr, structured JSON, and exit status. They do not contain Python or
JavaScript fallback implementations.

`init` installs the compatible project-local GraphForge skills into
`.agents/skills/` unless `--no-skills` is supplied. The wheel and npm package
carry byte-identical offline assets generated from the repository's single
`project-skills/` source. GraphForge owns only the skill files recorded in its
versioned managed manifest: unrelated skills and user edits are preserved, and
`skills status` reports conflicts before `skills update` or `skills remove`
can change them.

```bash
gf --project-dir . skills install
gf --project-dir . skills status --json
gf --project-dir . skills update
gf --project-dir . skills remove
```

The installed skills are tracked repository guidance, not graph data. Do not
blanket-ignore `.agents/skills/`; review and commit the GraphForge skill
directories and their managed provenance manifest when the team wants the same
agent experience across clones.

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
Repository lifecycle receipts identify files and directories relative to the
discovered repository root, using `/` separators. They never expose a checkout,
home-directory, or other machine-private absolute path.

Successful Arrow-backed commands use the versioned
`graphforge-cli-result/1` JSON envelope when `--json` is selected. The envelope
is schema-first: `columns` declares each name, Arrow data type, and nullability
before `metadata` and positional `rows`. UUID and binary values use canonical
portable text encodings. The default output remains Arrow IPC.

JSON failures use a stable `error` object with ordered `code`, `message`, and
bounded safe `details`. Details may identify the operation or a
repository-relative path, but never include credentials, raw data, unrestricted
paths, descriptions, or revert reasons. Parse failures and runtime failures
retain their established nonzero exit codes.

## Checkpoint inspection and revert

Checkpoint metadata inspection and checkpoint queries are separate:

```bash
gf --project-dir . checkpoint show before-change
gf --project-dir . checkpoint open before-change -- \
  "MATCH (n) RETURN n"
```

`checkpoint show` resolves and verifies the authoritative active checkpoint
record, then returns the same one-row metadata schema used by `checkpoint
list`. `checkpoint open` remains the read-only query surface and never creates
a mutable shell. Global `--json` selects the schema-first JSON result for
checkpoint commands; without it, the native result is Arrow IPC.

Revert is fail-closed. Preview resolves the checkpoint and current generation
without publishing anything and does not require mutation identity:

```bash
gf --project-dir . --json revert before-change --preview
```

An actual revert requires `--reason`, `--idempotency-key`, and explicit
non-interactive confirmation with `--yes`:

```bash
gf --project-dir . revert before-change \
  --reason "restore known state" \
  --idempotency-key 4f6a9b78-887d-4b8e-872b-a8b59059f777 \
  --yes
```

Omitting `--yes` refuses the mutation. Successful and idempotently replayed
receipts identify the prior current generation so automation can relate the
previewed state to the published result.

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

This whole-project interchange surface is distinct from ontology-document
inspection, suggestion, validation, and YAML/JSON export. The Rust-owned
ontology lifecycle is tracked by #236, with thin Python and Node parity tracked
by #237. Repository `export` never substitutes for ontology export, and
ontology export never packages graph data or a project generation.
