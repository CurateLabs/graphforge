---
name: graphforge-bootstrap
description: Initialize or inspect GraphForge repository integration without putting graph data in Git.
---

# Bootstrap GraphForge in a repository

Use this skill when a user wants to initialize GraphForge repository support,
inspect its configuration, or validate the local setup.

## Procedure

1. Find the repository root and read the closest `AGENTS.md`.
2. Run `graphforge config validate --project-dir <root>` before changing an
   existing setup.
3. For a new setup, run `graphforge init --project-dir <root>`. Do not pass
   `--no-skills` unless the user explicitly does not want project skills.
4. Review `.graphforge/graphforge.yaml` and the managed `.gitignore` entries.
5. Run `graphforge config resolve --json --project-dir <root>` and report the
   result without exposing secret values.

## Repository boundary

Track GraphForge definitions such as configuration, ontology, schemas,
migration definitions, and seed recipes. Never stage graph data, imported
datasets, materialized seeds, snapshots, or generated imports/exports. Runtime
state belongs under `.graphforge/state/`; import and export staging belongs
under `.graphforge/imports/` and `.graphforge/exports/`.

Reject symlinked project paths and stop if a requested operation would escape
the repository. Do not infer or ingest files by scanning the repository.
