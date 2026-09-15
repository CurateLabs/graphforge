# Lifecycle peak envelope correction (#1272)

The complete [#1271 CI run](https://github.com/CurateLabs/graphforge/actions/runs/34869384852) failed its aggregate growth check after all three ordinary SCALE6/7/8 lifecycles passed. Rust, binding, durability and concurrency lanes passed. The failed run is not relabeled green. [Preserved aggregate receipts](../../../benchmarks/tests/fixtures/lifecycle-peak-crossover-1272.json) retain the exact head and allocations.

| Work (nodes + edges) | Baseline peak B | Candidate peak B | Candidate peak/work |
| --- | --- | --- | --- |
| 1,088 | 1,101,824 | 1,093,632 | 1005.18 |
| 2,176 | 1,515,520 | 1,343,488 | 617.41 |
| 4,352 | 2,842,624 | 2,449,408 | 562.82 |

Baseline comes from [successful #1270 CI](https://github.com/CurateLabs/graphforge/actions/runs/34864684237), before consumed-root retirement. Every candidate peak is lower. Its adjacent derivatives, however, are 229.65 and 508.24 bytes/work, a factor of 2.213. The producer computes the maximum across validated operation peaks; that maximum can change its winning operation. These receipts do not establish which operation won.

For this fixed tiny workload, the corrected gate requires strict positive peak growth and nonincreasing adjacent peak/work. The maximum of affine operation costs with nonnegative fixed overhead obeys that envelope even when its derivative changes. This is a fixture-specific enforced bound, not a universal claim across discrete merge thresholds. The upper-growth constraint is stricter than the prior factor-two normalized allowance. Owner EOF derivatives, retained-allocation derivatives, existing ceilings, exact raw physical-owner reconciliation, schemas, row counts and ratio integrity remain checked.

The regression suite includes the affine crossover `max(120000 + 5000*w, 80000*w)` for `w = 1, 2, 4`: peaks 125000/160000/320000 are valid but fail the old derivative rule. Conversely, 60000/120000/300000 satisfies both old peak checks but fails the new adjacent normalized bound. Flat, inflated, missing and malformed evidence refusals remain tested.

Test-only baseline execution produced the expected two crossover errors and one missing-refusal failure. After correction, `python3 -m unittest discover -s benchmarks/tests -p test_lifecycle_growth.py` passes all eight tests. The initial broad invocation without `PYTHONPATH` failed imports; the authoritative broader command is `PYTHONPATH=harness uv run --locked python -m unittest discover -s tests -p 'test_*.py'` from `benchmarks`.
