# Scale orchestration parity index (#959)

Maps retired legacy Graph500 scale orchestration to the isolated `benchmarks/` harness.
Retirement is complete in-tree; the parity gate remains blocked on ingested #900 ladder
bundles until Fly execution evidence is checked in (see `fixtures/parity/ladder-bundle/README.md`).

## Coverage map

| Legacy entrypoint (retired) | Harness equivalent | Parity status |
|---|---|---|
| `make bench-g500-ladder` | `make -C benchmarks progressive-qualification-run` | tiny shadow OK; ladder bundle pending #900 |
| `make bench-g500-scale20` | `profiles/graph500/s20-*.json` + progressive qualification | tiny shadow OK; ladder bundle pending #900 |
| `make g500-ladder-qualification` | progressive qualification schemas + controller | retired; harness authoritative |
| `cargo test -p graphforge-api --test scale_g500_ladder` (S10 CI) | `benchmarks/scripts/test-tiny-lifecycle-certification.py` | bounded correctness retained in product CI |
| `cargo test … certification_target_live…` (retired) | `qualification-operator GATE=progressive-ladder` | blocked on #900 Fly execution |
| `scripts/ci/validate-g500-certification.py` | `graphforge_bench.scale_parity` + progressive schemas | historical fixture validates |
| `docs/development/perf-g500-ladder.md` | `benchmarks/README.md` | historical reference retained |
| `.github/workflows/g500-certification.yml` | `.github/workflows/progressive-ladder.yml` | retired; progressive-ladder handoff wired |

## Accepted semantic differences

Declared in `fixtures/parity/accepted-differences.json`:

- **generator_seed** — legacy seed `1` vs harness seed `13907095936298285200`
- **generator_identity** — in-tree Kronecker vs `graphforge-benchmark-graph500-generator`
- **execution_surface** — Rust API vs `gf` CLI + public certification runners
- **phase_model** — legacy CSR, split queries, negative drills vs ten-phase contract
- **resource_authority** — local `/proc` sampling vs BenchExec process-tree metrics

## Comparator commands

```bash
# Parity gate status (#959 criteria tracker; tiny must pass, full retirement blocked until #900)
cd benchmarks
make parity-gate
PYTHONPATH=harness uv run --locked python -m unittest tests.test_scale_parity tests.test_parity_gate

# Historical legacy certification fixture readability
PYTHONPATH=harness uv run --locked python -c "
from pathlib import Path
from graphforge_bench.scale_parity import validate_historical_legacy_cert, workspace_root
fixture = workspace_root() / 'fixtures/parity/legacy/cert-s20-minimal.json'
validate_historical_legacy_cert(fixture, expected_sha='a' * 40)
print('legacy cert fixture readable')
"
```

## Retirement gate (#959 acceptance)

Legacy orchestration is retired in-tree. Full gate green requires:

1. Parity matrix reports no unexplained gaps on tiny fixtures **and** ingested #900 ladder bundles.
2. Coverage map rows move from `pending` to `accepted` once ladder bundles pass comparison.
3. Bounded correctness tests remain in product CI (`scale_g500_ladder` S10 or equivalent).
