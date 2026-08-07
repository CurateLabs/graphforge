# Coverage reporting

**Status:** Local pre-push only (no CI upload)

## Current policy

GraphForge does **not** upload coverage to Codecov (or any external coverage
service) from CI. Post–Bazel cutover, the authoritative Rust compile/test path
under CI Gate is `bazelisk test //:ci_rust_tests`, which does not produce
Codecov-compatible reports.

Coverage floors remain enforced locally by `make pre-push` via
`scripts/pre_push_validation.py` (Rust llvm-cov plus Python/Node wrapper
thresholds). PR CI does not re-run full `llvm-cov`.

## Secrets

Do **not** commit Codecov upload tokens. If a repository secret `CODECOV_TOKEN`
exists from a prior integration, rotate or delete it in GitHub settings and on
the Codecov side — treat any token that ever appeared in this repository’s git
history as compromised.

## Future work

A scheduled or nightly Bazel/`llvm-cov` coverage job (with optional external
publication) may be added later as a separate issue. Until then, treat
`make pre-push` as the coverage gate.
