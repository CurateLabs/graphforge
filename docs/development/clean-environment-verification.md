# Clean-environment verification

Post-publication proof for release tracker **#192** / follow-up tracker **#167**: from new clean
environments, using only public registries, install and exercise GraphForge
release artifacts.

This is **not** Binding RC evidence (local same-SHA wheels/addons) and **not** a
substitute for section 6 publication (#2794). If public `0.5.2` packages are missing,
verification must **fail closed** — do not check off children against unpublished
artifacts.

## Child issues and evidence lanes

| Lane | Child | Outcome |
| --- | --- | --- |
| `pip` | #180 | `pip install graphforge==<version>` + documented quickstart E2E |
| `npm` / `cli` | #183 | Install `@curatelabs/graphforge@<version>` and `@curatelabs/graphforge-cli@<version>` + smoke execution |
| `skills` | #182 | Install `@curatelabs/graphforge-agent-skills@<version>` + offline `compatibility --json` |
| `cargo` | #185 | Add all 16 `graphforge-*` crates at `<version>` and compile a clean consumer |
| `reopen` | #184 | Create/close/reopen project; Arrow rows survive reopen |
| `urls` | #186 | Published docs, licensing, and package/registry URLs resolve (human HTML pages optional when CDNs block bots) |
| `checksums` | #187 | Registry digests match `graphforge-release-record-v1` |

Close each child only with commands + outcomes (or an explicit disposition). The
tracker (#167) is intentionally post-release and does not block or auto-close #192.

## Upstream blockers

| Child | Wait on |
| --- | --- |
| #180, #184 | PyPI publish (#195) |
| #182, #183 | npm publish (#198) |
| #185 | crates.io publish (#196) |
| #186 | docs deploy (#197) + registry metadata (#199) |
| #187 | release record + published package checksums |

## Harness

```bash
# Fail closed if public artifacts are missing (expected before section 6 completes)
python3 scripts/ci/clean-env-verify.py preflight --version 0.5.2

# Unit tests (no install success claims; includes live unpublished preflight)
python3 scripts/ci/test-clean-env-verify.py
make clean-env-verify-check

# After publication — full local run (writes evidence JSON)
make clean-env-verify VERSION=0.5.2 \
  RELEASE_RECORD=path/to/release-record.json \
  OUTPUT=build/clean-env-evidence.json

# Or per lane
python3 scripts/ci/clean-env-verify.py run \
  --version 0.5.2 --lane pip --lane reopen \
  --output build/clean-env-pip.json
```

CI: workflow_dispatch **Clean Environment Verify**
(`.github/workflows/clean-env-verify.yml`). Inputs: `version`, `lanes`, and optional
`release_record_path`. Upload the evidence artifact to the matching child issues.

The current GraphForge release publishes 16 Rust packages under `graphforge-*`.
The harness probes all 16 by default and the `cargo` lane creates a clean consumer, adds
the exact release version of every crate, and runs `cargo check`.

## Release record schema

`checksums` requires a JSON document:

```json
{
  "schema": "graphforge-release-record-v1",
  "version": "0.5.2",
  "tag": "v0.5.2",
  "commit_sha": "<40-hex>",
  "artifacts": [
    {
      "surface": "pypi",
      "name": "graphforge",
      "version": "0.5.2",
      "filename": "graphforge-0.5.2-….whl",
      "sha256": "<64-hex>"
    }
  ]
}
```

Surfaces: `pypi`, `npm`, `crates`, and `github`. Produced for #192 and attached to
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
- Release tracker: [#192](https://github.com/CurateLabs/graphforge/issues/192)
- Post-release tracker: [#167](https://github.com/CurateLabs/graphforge/issues/167)
