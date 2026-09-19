# Scale orchestration parity index (#959)

Maps retired legacy Graph500 scale orchestration to the isolated `benchmarks/` harness.
Retirement is complete in-tree. **#959 closes on bounded tiny/shadow parity**; ingested
#900 ladder bundles are optional read-only evidence for parent **#952**, not a closure
prerequisite for this epic.

## Coverage map

| Legacy entrypoint (retired) | Harness equivalent | Parity status |
|---|---|---|
| `make bench-g500-ladder` | `make -C benchmarks progressive-qualification-run` | tiny shadow OK; full ladder is #952/#900 track |
| `make bench-g500-scale20` | `profiles/graph500/s20-*.json` + progressive qualification | tiny shadow OK; full ladder is #952/#900 track |
| `make g500-ladder-qualification` | progressive qualification schemas + controller | retired; harness authoritative for bounded migration |
| `cargo test -p graphforge-api --test scale_g500_ladder` (S10 CI) | `benchmarks/scripts/test-tiny-lifecycle-certification.py` | bounded correctness retained in product CI |
| `cargo test … certification_target_live…` (retired) | `qualification-operator GATE=progressive-ladder` | native host execution belongs to #952/#900 |
| `scripts/ci/validate-g500-certification.py` | `graphforge_bench.scale_parity` + progressive schemas | historical lifecycle fixture remains readable |
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
# Parity gate status (#959 criteria tracker; tiny must pass; full ladder is informational)
cd benchmarks
make parity-gate
PYTHONPATH=harness uv run --locked python -m unittest tests.test_scale_parity tests.test_parity_gate

# Historical legacy certification fixture readability
PYTHONPATH=harness uv run --locked python -c "
from pathlib import Path
from graphforge_bench.scale_parity import read_historical_legacy_cert, workspace_root
fixture = workspace_root() / 'fixtures/parity/legacy/cert-s20-minimal.json'
historical = read_historical_legacy_cert(fixture, expected_sha='a' * 40)
assert historical.status == 'passed' and len(historical.phases) == 10
print('legacy cert fixture readable')
"
```

The historical reader checks the preserved fixture and lifecycle mapping only.
It does not authenticate a benchmark result or satisfy full-ladder certification.
`validate_historical_legacy_cert` separately requires an externally authenticated
provider-result anchor before validating certification evidence.

## Retirement gate (#959 acceptance)

Legacy orchestration is retired in-tree. **#959 migration completion** requires:

1. Parity matrix reports no unexplained gaps on **tiny shadow fixtures**.
2. Legacy orchestration remains retired with coverage map and preserved migration fixtures.
3. Bounded correctness tests remain in product CI (`scale_g500_ladder` S10 or equivalent).
4. GDC measurement boundaries are enforced via inventory and fail-closed policy tests;
   **live GDC suite execution is not required** for this epic.

Optional ingested #900 ladder bundles and `full_ladder_evidence_complete` are tracked
for parent **#952** and must not block #959 closure.
