# Coverage reporting

**Status:** CI main-branch enforcement only (no PR CI upload)

## Current policy

GraphForge does **not** upload coverage to Codecov (or any external coverage
service). The authoritative Rust compile/test path under CI Gate is the
`rust-tests` nextest lane (ADR 0048), which does not produce Codecov-compatible
reports.

Coverage floors are enforced by the **Coverage** workflow
(`.github/workflows/coverage-baseline.yml`) on every push to `main` via
`scripts/coverage-rust.sh` (Rust llvm-cov plus Python/Node wrapper thresholds).
PR CI does not run full `llvm-cov`. Run `make coverage-rust` locally when
claiming floor changes.

## Secrets

Do **not** commit Codecov upload tokens. If a repository secret `CODECOV_TOKEN`
exists from a prior integration, rotate or delete it in GitHub settings and on
the Codecov side — treat any token that ever appeared in this repository’s git
history as compromised.
