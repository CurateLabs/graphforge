# Release artifact record

This page is the §5 checklist home for **checksums, SBOM/provenance, licenses, and
contents** of v0.5.0 release-candidate artifacts
([M1 #192](https://github.com/CurateLabs/graphforge/issues/192)).

It does **not** replace the authoritative publication order / stop conditions in
[`publication-order.md`](publication-order.md).

## Same-tagged-commit rule

Every first-party publishable artifact for version `0.5.0` must be built from one
verified commit (the eventual `v0.5.0` tag target) or have an explicit reproducible
link to that commit recorded in the artifact JSON. Do not mix bytes from different
commits under the same version.

## How to record

1. Freeze the RC SHA and surface versions (`scripts/set_release_version.py` / #192).
2. Build or collect artifacts for that SHA into a directory (wheels, sdists,
   npm tarballs, all 15 `.crate` archives, and SBOM/provenance if emitted).
3. Run:

```bash
python3 scripts/record_release_artifacts.py \
  --version 0.5.0 \
  --dist-dir path/to/artifacts \
  --out docs/releases/records/v0.5.0-artifacts.json \
  --notes "RC sha=<40-char> built via <workflow/run>"
```

4. Attach the JSON (or its checksums table) to the GitHub Release (#194).
5. Post-release clean-env verification (#167) matches `sha256` values from this record.

The generated document uses the same `graphforge-release-record-v1` schema
consumed by `clean-env-verify.py`. Validate it before attaching:

```bash
python3 scripts/ci/clean-env-verify.py validate-release-record \
  docs/releases/records/v0.5.0-artifacts.json
```

Template-only (no files yet):

```bash
python3 scripts/record_release_artifacts.py \
  --version 0.5.0 \
  --dist-dir target/release-artifacts \
  --allow-empty \
  --out docs/releases/records/v0.5.0-artifacts.template.json
```

## License / third-party pointers

- First-party: `Apache-2.0`, shipped `LICENSE` + `NOTICE` (`make package-license-verify` / #218).
- Third-party inventory: [`legal/THIRD_PARTY_NOTICES.md`](../../legal/THIRD_PARTY_NOTICES.md) (#218).

## SBOM / provenance

When the release process emits SBOM or provenance files, place them in the same
`--dist-dir` so `record_release_artifacts.py` classifies them (`sbom` /
`provenance`). If none are configured for a surface, the record’s
`sbom_provenance.configured` stays false — that is an explicit disposition, not a
silent skip.
