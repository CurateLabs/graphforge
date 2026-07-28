# Release workflow registry

The release-candidate workflow suite is release-candidate evidence, not a required pull-request
CI gate. Each approved scenario owns its required scenario set, coverage matrix,
fixture, behavior, expected results, and child evidence. The central scaffolding
only registers and orchestrates those bundles.

## Register a bundle

Create an approved scenario issue and a bundle at
`tests/release_workflows/<scenario-id>/`. A bundle contains `scenario.yaml`,
`generator.yaml`, `workflow.feature`, `run.py`, `README.md`, and the required
`expected/` contracts. Add the common `workflow-scenario-v1` metadata block to
the manifest, then add one row to `registry-v1.json`. Step IDs must be unique and
must appear in exactly the same order in the feature and manifest.

The generator SHA-256 is over the checked-in `generator.yaml` bytes. Update it
in both metadata locations when the generator changes. Paths are repository
relative and may not be absolute or contain `..`. Select an ontology class from
`ontology-complexity-v1.json`; do not infer complexity from the domain name.
Identical coverage signatures are rejected. A near-duplicate needs a distinct
checked-in release-risk rationale before registration.

## Validate and execute

Static validation never starts a domain workflow:

```bash
python3 scripts/ci/release-workflows.py validate
```

Native execution is explicit and intended for a prepared release-candidate
machine. Selection is always reduced to registry order, independent of CLI
argument order:

```bash
python3 scripts/ci/release-workflows.py run \
  --scenario sna-intelligence --scenario probate-genealogy \
  --commit-sha "$(git rev-parse HEAD)" \
  --output target/release-workflow-evidence/envelope.json

python3 scripts/ci/release-workflows.py run --all \
  --commit-sha "$(git rev-parse HEAD)" \
  --output target/release-workflow-evidence/envelope.json
```

The aggregate command stops on the first child failure and preserves an
attributable, sanitized failure. It does not print fixture data or parameter
collections.

## Inspect evidence

The `evidence-envelope-v1` record binds the selected registry-ordered scenarios,
environment, bounded command, outcomes, artifact paths, and hashes to one exact
40-character commit SHA. Validate an envelope and every child hash with:

```bash
python3 scripts/ci/release-workflows.py validate-evidence \
  target/release-workflow-evidence/envelope.json \
  --commit-sha "$(git rev-parse HEAD)"
```

This envelope is infrastructure evidence only. Parent retains ownership
of global coverage interpretation and the final closure artifact.

## Evolve the contract

Schema changes are additive within a version. Breaking changes require new
filenames and identifiers (`workflow-registry-v2`, for example), a documented
migration of every row and manifest, and simultaneous runner support. Never
silently reinterpret an older registry or evidence envelope.
