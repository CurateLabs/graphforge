# Completed #900 ladder bundle ingestion (#952 / #900)

This directory holds **read-only** sanitized rung bundles produced by issue #900.
Do not generate synthetic ladder evidence here and do not rerun S18–S26 solely for parity.
Ingestion is optional engineering evidence for parent **#952**; it is **not** a #959 closure prerequisite.

## Current bundle

The native OVHC-AGENCY host ladder (`progressive_host_run`, `local-linux-cgroups-v2`) at
merged commit `114b1964874419b31d13820bfbdf72fe57286544`, rungs S18, S19, S20, S22, S24,
S25 and S26, copied by `make -C benchmarks ingest-ladder-bundle`:

```text
ladder-bundle/
  s<N>-plan.json                # graphforge-progressive-host-run-plan/1
  s<N>-projection.json          # admission decision for rung N (S20 onward)
  s<N>-benchexec.json           # BenchExec authority evidence
  s<N>-graphforge.json          # graphforge-public-certification/1
  s<N>-rung.json                # graphforge-progressive-qualification-rung-evidence/1
  s<N>-result.json              # graphforge-progressive-host-run-result/1
  work-root-inventory.json      # graphforge-host-work-root-inventory/2, empty after teardown
```

Rung files must validate against
`benchmarks/schemas/progressive-qualification-rung-evidence.json`. Replace the whole
directory when a new complete ladder is ingested.

## Parity usage

Once bundles are present, `graphforge_bench.scale_parity.compare_ladder_bundle` compares
each rung against preserved legacy migration fixtures under `fixtures/parity/legacy/` and
declared accepted differences in `fixtures/parity/accepted-differences.json`.

**#959 bounded migration** uses only tiny/local shadow fixtures in
`fixtures/parity/legacy/tiny-pass.json` and `fixtures/parity/new/tiny-pass.json`.

After #900 completes, validate and ingest the sanitized bundle read-only for #952:

```bash
make -C benchmarks ingest-ladder-bundle SOURCE=/path/to/#900-output VALIDATE_ONLY=1
make -C benchmarks ingest-ladder-bundle SOURCE=/path/to/#900-output
make -C benchmarks parity-gate
```
