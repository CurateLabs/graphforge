# Release Load Matrix Results

**Last updated:** 2026-07-28 (v0.5.0)

Accepted same-SHA results for the standardized release load matrix land here.
This page is the durable public record for case-level and summary evidence. It is
not a benchmark leaderboard and does not replace the
[Scale Limits](scale-limits.md) guidance or the fixed-hop LIMIT benches.

For how the matrix is defined and run, see
[Standardized Release Load Matrix](../development/release-load-matrix.md).
For what each size/density class proves against published limits, see
[Scale Limits → Release load matrix coverage](scale-limits.md#release-load-matrix-coverage).

## Evidence (v0.5.0)

Host-native evidence is green on `main` tip
`ec81fd8f35eda9cd59247865c79e296875d3ebf9` (merge of #2779 binding-parity
fix). Issue #2765 closed on outcome-based criteria (infra + local 144/144 +
this page); a downloadable `Release-Load-Matrix-<sha>` CI artifact is **not**
required for that close.

| Field | Value |
|-------|-------|
| SHA | `ec81fd8f35eda9cd59247865c79e296875d3ebf9` |
| Date (UTC) | 2026-07-28 |
| Evidence source | Local host-native run (worktree `graphforge-worktrees/load-matrix-main`) |
| Evidence path | `build/release-load-evidence.json` (`schema`: `graphforge-load-evidence/1`, `status`: `passed`) |
| CI run URL | Optional — Release Certification Gate artifact upload not required for #2765 close |
| Artifact name | Optional — `Release-Load-Matrix-<sha>` when a publication run uploads one |
| Pass summary | 144/144 cases `passed` (48 Rust / 48 Python / 48 Node); same SHA; attempt=1 only; no retries, skips, parity diffs, or sanitized errors |

### Case-level summary

| Size | Density | Topologies exercised | Outcome |
|------|---------|----------------------|---------|
| XS | sparse / dense | disconnected, path; clustered, cyclic | 36/36 passed |
| S | sparse / dense | hub, path; clustered, cyclic | 36/36 passed |
| M | sparse / dense | disconnected, hub; clustered, cyclic | 36/36 passed |
| L | sparse / dense | clustered; cyclic | 18/18 passed |
| XL | sparse / dense | hub; clustered | 18/18 passed |

Languages: Rust, Python, Node (48 cases each).

## How to update this page

1. Prefer a same-SHA green host-native or CI matrix run with case-level evidence.
2. Optionally record a CI run URL and `Release-Load-Matrix-<sha>` when a gate upload
   exists (useful for release publication; not an ordinary issue-close blocker).
3. Record SHA, UTC date, evidence source/path, and a short pass summary
   (accepted case count, binding languages, any diagnostic notes that are not
   graph content or secrets).
4. Optionally attach a compact case-level rollup from the bundle metadata
   (outcomes and fingerprints only).

Published scale limits and LIMIT benches remain the authoritative scale
narrative outside this matrix.

## Related

- [Scale Limits](scale-limits.md)
- [Standardized Release Load Matrix](../development/release-load-matrix.md)
- [Release Process](../development/release-process.md)
