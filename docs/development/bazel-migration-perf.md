# Bazel migration Blacksmith cache + performance gates (#5)

Implements sequence step 8 of [#1](https://github.com/CurateLabs/graphforge/issues/1)
via child issue [#5](https://github.com/CurateLabs/graphforge/issues/5).

Companion artifacts:

- Baseline (#12): [bazel-migration-baseline.md](bazel-migration-baseline.md)
- Parity (#6): [bazel-migration-parity.md](bazel-migration-parity.md)
- Machine-readable sample: [bazel-migration-evidence/perf-sample.json](bazel-migration-evidence/perf-sample.json)
- Harness: `scripts/ci/bazel-cache-perf.py`

## Blacksmith Bazel Build Caching (enabled)

Blacksmith injects repository Bazel caching after an organization administrator
enables **Bazel Build Caching** for this repository. GraphForge must **not** set
`--remote_cache` in `.bazelrc` or workflows.

Enablement is confirmed: identical-SHA warm observation reported remote cache
hits, and ≥10 cold/warm pairs are checked in under
[perf-sample.json](bazel-migration-evidence/perf-sample.json). Re-check steps if
hits regress:

1. Open [Blacksmith Settings → Features](https://app.blacksmith.sh/settings?tab=features).
2. Under **Caching**, confirm **Bazel Build Caching** for `CurateLabs/graphforge`.
3. Confirm the [Cache page](https://app.blacksmith.sh/cache) shows a Bazel tab.
4. Do **not** add a competing `--remote_cache`.
5. Docs: [Blacksmith Bazel Build Caching](https://docs.blacksmith.sh/blacksmith-caching/bazel-build-caching).

## In-repo harness + evidence

| Piece | Location |
| --- | --- |
| No competing `--remote_cache` | `.bazelrc`, workflows; enforced by `bazel-cache-perf.py --mode policy` |
| Cache-unavailable cold correctness | `--mode cold-correctness` (CLI `--noremote_cache` + fresh `--output_base`) |
| Warm observation harness | `--mode observe-warm` (prime + warm across distinct `--output_base`s) |
| Pair collector (≥10 cold/warm) | `--mode collect-pairs` (CI runs when hits observed + evidence incomplete) |
| Affected-input isolation probe | `--mode affected-inputs` |
| Gate evaluator (≥10 pairs, #1 thresholds) | `--mode evaluate` |
| CI wiring | Required: `Bazel Bootstrap` (policy + harness unit tests). Diagnostics: `Bazel Diagnostics` (observe/collect; **not** in `CI Gate` `needs`) |
| Checked-in sample status | `perf-sample.json` → `complete` (10 pairs; **one-shot Bazel-migration evidence**) |

`perf-sample.json` is **one-shot Bazel-migration close evidence**, not a live PR regression
gate. Required bootstrap runs `--mode policy` and harness unit tests only.
`Bazel Diagnostics` may still observe/collect and roll up `evaluate` for
dashboards; failures there do not fail `CI Gate`.

## Measurement plan

### Representative Bazel surface

Matches the Blacksmith `Bazel Bootstrap` compile/test path (not full TCK BDD):

- Test: `//:bazel_test_graph_smoke`
- Build: `//:bazel_smoke`, `//:first_party_libs`, `//:cli_bins`,
  `//:resource_inputs`, `//:release_bins`
- Bindings: `//:binding_cdylibs`

### Cold protocol

- **Bazel cold (correctness):** CLI-only empty `--remote_cache` / `--disk_cache`
  (never checked in as repo defaults). Must succeed without repository changes
  and report zero remote cache hits.
- **Bazel cold (perf):** clean local output base / empty or evicted Blacksmith
  repository cache (admin can clear from the Cache page). Sticky local disks do
  not count as warm remote-cache hits.
- **Cargo cold:** empty `target/` **without** sticky-disk hydrate. Sticky-disk
  warm Cargo starts from the #12 sample are **not** cold.

### Warm protocol

1. Populate cache with a successful representative Bazel run at SHA `S`.
2. Re-run the same targets at the same SHA on a Blacksmith runner into a
   **fresh** `--output_base` (same-base re-runs are satisfied locally and hide
   remote hits).
3. Bazel process summary must show `remote cache hit` counts &gt; 0.
4. Record wall seconds, process counts, and (when available) Blacksmith Cache
   dashboard storage / hit-rate links.

### Paired sample (≥10)

Each pair records cold + warm Bazel walls for the representative surface at one
immutable SHA, plus optional `cargo_cold_wall_seconds` and
`compute_proxy_seconds` (sum of Bazel job walls comparable to the #12 proxy).

Append pairs into `perf-sample.json`, set:

- `observations.remote_cache_hits_on_identical_sha = true`
- `observations.cache_unavailable_cold_correct = true` (from CI/harness)
- `observations.affected_inputs_isolation = true` (from harness)
- `status = "complete"` only when gates pass
- `blacksmith_dashboard_links` to Cache/Bazel job URLs
- exact SHA in closure notes

### Thresholds (#1 / baseline)

Against [bazel-migration-baseline.md](bazel-migration-baseline.md):

| Gate | Requirement |
| --- | --- |
| Warm PR build/test p50 | ≥ 30% faster than Cargo primary job set p50 (**625s** = Rust Tests 327 + Python 177 + Node 121) |
| Total build compute proxy | ≥ 25% lower than Cargo six-job sum p50 (**923s**) |
| Cold p50 regression | ≤ 10% vs cold Cargo walls recorded in the paired sample |
| Sample size | ≥ 10 pairs |
| Remote hits | Present on repeated identical-SHA builds |
| Affected inputs | Source change reruns only actions with changed declared inputs |
| Cache unavailable | Cold build remains correct |

Maintainer-approved waiver may be checked in under `waiver` (prefer pass).

## Local commands

```bash
python3 scripts/ci/bazel-cache-perf.py --mode policy
python3 scripts/ci/test-bazel-cache-perf.py

# Cache-unavailable correctness (does not require Blacksmith admin)
python3 scripts/ci/bazel-cache-perf.py --mode cold-correctness

# Warm observation (hits require org-admin enablement on Blacksmith runners)
mkdir -p dist
python3 scripts/ci/bazel-cache-perf.py --mode observe-warm --write dist/warm-observation.json

# Collect ≥10 cold/warm pairs (Blacksmith runners; long-running)
python3 scripts/ci/bazel-cache-perf.py --mode collect-pairs --pairs 10 \
  --write dist/perf-sample-collected.json

# Affected-input isolation probe
python3 scripts/ci/bazel-cache-perf.py --mode affected-inputs --write dist/affected-inputs.json

# Strict close gate (fails while pending_org_admin)
python3 scripts/ci/bazel-cache-perf.py --mode evaluate \
  --evidence docs/development/bazel-migration-evidence/perf-sample.json

# CI readiness evaluate
python3 scripts/ci/bazel-cache-perf.py --mode evaluate --allow-pending \
  --evidence docs/development/bazel-migration-evidence/perf-sample.json
```

## Security

- No secrets, tokens, OIDC material, or publish credentials in cacheable Bazel
  actions (unchanged release/publish boundary).
- Cross-branch cache reuse is safe only via Bazel action keys / declared inputs.
- Do not upload sensitive fixtures into remote cache payloads.

## Issue close rule

Close [#5](https://github.com/CurateLabs/graphforge/issues/5) only when:

1. Org-admin enablement is done and remote hits are observed on identical-SHA builds.
2. `perf-sample.json` has ≥10 pairs and `evaluate` passes **without** `--allow-pending`.
3. Cache-unavailable cold correctness and affected-input isolation are proven.
4. Exact SHA + Blacksmith dashboard links are in closure notes.

[#5](https://github.com/CurateLabs/graphforge/issues/5) is closed with complete
evidence. Cutover: [bazel-migration-cutover.md](bazel-migration-cutover.md) / [#4](https://github.com/CurateLabs/graphforge/issues/4).
