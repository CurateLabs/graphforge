# Clean-environment verification

Post-publication proof for **#742 section 7** / tracker **#2795**: from new clean
environments, using only public registries, install and exercise GraphForge
release artifacts.

This is **not** Binding RC evidence (local same-SHA wheels/addons) and **not** a
substitute for section 6 publication (#2794). If public `0.5.0` packages are missing,
verification must **fail closed** — do not check off children against unpublished
artifacts.

## Child issues and evidence lanes

| Lane | Child | Outcome |
| --- | --- | --- |
| `pip` | #2809 | `pip install graphforge==<version>` + documented quickstart E2E |
| `npm` | #2810 | Install `@graphforge/node@<version>` + smoke execute/Arrow decode |
| `skills` | #2811 | Install `@graphforge/agent-skills@<version>` + offline `compatibility --json` |
| `cargo` | #2812 | `cargo add <crate>@<version>` + `cargo check` (default crate: `gf-api`) |
| `reopen` | #2813 | Create/close/reopen project; Arrow rows survive reopen |
| `urls` | #2814 | Published docs + package/registry URLs resolve |
| `checksums` | #2815 | Registry digests match `graphforge-release-record-v1` from #2798 + #2803 |

Close each child only with commands + outcomes (or an explicit disposition). The
tracker (#2795) blocks #742 until children complete; it does **not** auto-close
#742.

## Upstream blockers

| Child | Wait on |
| --- | --- |
| #2809, #2813 | PyPI publish (#2805) under #2794 |
| #2810, #2811 | npm publish (#2806) under #2794 |
| #2812 | crates.io publish (#2804) under #2794 |
| #2814 | docs deploy (#2807) + registry metadata (#2808) |
| #2815 | release record (#2798) + GitHub Release checksums (#2803) |

## Harness

```bash
# Fail closed if public artifacts are missing (expected before section 6 completes)
python3 scripts/ci/clean-env-verify.py preflight --version 0.5.0

# Unit tests (no install success claims; includes live unpublished preflight)
python3 scripts/ci/test-clean-env-verify.py
make clean-env-verify-check

# After publication — full local run (writes evidence JSON)
make clean-env-verify VERSION=0.5.0 \
  RELEASE_RECORD=path/to/release-record.json \
  OUTPUT=build/clean-env-evidence.json

# Or per lane
python3 scripts/ci/clean-env-verify.py run \
  --version 0.5.0 --lane pip --lane reopen \
  --output build/clean-env-pip.json
```

CI: workflow_dispatch **Clean Environment Verify**
(`.github/workflows/clean-env-verify.yml`). Inputs: `version`, `lanes`, optional
`release_record_path`, `crate`. Upload the evidence artifact to the matching
child issues.

## Release record schema

`checksums` requires a JSON document:

```json
{
  "schema": "graphforge-release-record-v1",
  "version": "0.5.0",
  "tag": "v0.5.0",
  "commit_sha": "<40-hex>",
  "artifacts": [
    {
      "surface": "pypi",
      "name": "graphforge",
      "version": "0.5.0",
      "filename": "graphforge-0.5.0-….whl",
      "sha256": "<64-hex>"
    }
  ]
}
```

Surfaces: `pypi`, `npm`, `crates`, `github`. Produced under #2798 and attached to
the GitHub Release (#2803). Validate with:

```bash
python3 scripts/ci/clean-env-verify.py validate-release-record path/to/record.json
```

## Acceptance evidence shape

Evidence JSON uses schema `graphforge-clean-env-evidence-v1` with per-lane
`ok`, `commands`, `notes`/`error`, and the child `issue` number. Validate:

```bash
python3 scripts/ci/clean-env-verify.py validate-evidence build/clean-env-evidence.json --require-ok
```

## Related docs

- [`PUBLISHING.md`](../engineering/PUBLISHING.md) — promotion to clean-install verification
- [`release-process.md`](release-process.md) — release checklist
- Umbrella: [#742](https://github.com/CurateLabs/graphforge-legecy/issues/742) section 7
- Tracker: [#2795](https://github.com/CurateLabs/graphforge-legecy/issues/2795)
