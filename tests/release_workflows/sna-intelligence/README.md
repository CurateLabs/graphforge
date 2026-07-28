# Social-network intelligence release workflow

This deterministic synthetic scenario asks whether a cross-community signal is
compatible with influence, coordination, or coincidence. It deliberately does
not make objective attribution. The analyst preserves the imported `DIRECTS`
source record, adds neutral `ASSOCIATED_WITH`, supersedes the affected assertion,
and explicitly selects coordination only as a working interpretation. The
pre-correction checkpoint and every immutable knowledge record remain readable.

Run it from a clean checkout of the release-candidate SHA:

```bash
python3 tests/release_workflows/sna-intelligence/run.py \
  --commit-sha "$(git rev-parse HEAD)"
```

The command validates the bundle and stable step mapping, runs the authoritative
Rust workflow in an isolated Cargo target directory, builds a native wheel from
the checked-out SHA, installs it into a fresh environment, runs the representative
Python construction/query/search/algorithm/reopen replay, and writes SHA-bound
evidence to `target/release-workflows/sna-intelligence/evidence.json`. It fails
closed on a stale SHA, missing fixture component, step drift, missing correction
history, implicit confidence-based selection, binding failure, source-tree or
pure-Python fallback, wheel/native-extension hash drift, or reopen drift.

This is an opt-in release-candidate workflow. It is not registered as a required
pull-request check and must not be added to the aggregate `CI Gate`.

The `.yaml` manifests use JSON syntax intentionally: JSON is valid YAML, while
the bundle validator can remain dependency-free and deterministic.
