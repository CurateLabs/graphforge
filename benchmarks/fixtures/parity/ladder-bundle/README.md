# Completed #900 ladder bundle ingestion (#959)

This directory holds **read-only** sanitized rung bundles produced by issue #900.
Do not generate synthetic ladder evidence here and do not rerun S18–S26 solely for parity (#959).

## Expected layout

Each completed rung from the Fly progressive ladder should be copied as:

```text
ladder-bundle/
  manifest.json                 # immutable identities for commit, image, profiles, region
  s18-rung.json                 # graphforge-progressive-qualification-rung-evidence/1
  s19-rung.json
  ...
  teardown-inventory.json       # graphforge-progressive-provider-teardown-inventory/1
```

`manifest.json` must record the exact merged commit SHA, OCI digest, generator identity,
BenchExec version, and maximum authorized scale. Rung files must validate against
`benchmarks/schemas/progressive-qualification-rung-evidence.json`.

## Parity usage

Once bundles are present, `graphforge_bench.scale_parity.compare_ladder_bundle` compares
each rung against preserved legacy migration fixtures under `fixtures/parity/legacy/` and
declared accepted differences in `fixtures/parity/accepted-differences.json`.

Until #900 completes, parity work uses only tiny/local shadow fixtures in
`fixtures/parity/legacy/tiny-pass.json` and `fixtures/parity/new/tiny-pass.json`.

After #900 completes, validate and ingest the sanitized bundle read-only:

```bash
make -C benchmarks ingest-ladder-bundle SOURCE=/path/to/#900-output VALIDATE_ONLY=1
make -C benchmarks ingest-ladder-bundle SOURCE=/path/to/#900-output
make -C benchmarks parity-gate
```
