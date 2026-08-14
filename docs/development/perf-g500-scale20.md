# Official-parameter SCALE-20 engineering green (#710)

This is an in-tree **thin reference client** for Graph500 **parameters**
(SCALE=20, `edgefactor=16`, undirected Kronecker/R-MAT) on the **published**
Rust facade. It is **not** Official-track and **not** TEPS.

| Claim | Status |
|---|---|
| Official Graph500 submission | No |
| `track: official` in evidence JSON | Must not be set |
| Pinned `github.com/graph500/graph500` generator | No — bench-local Kronecker in the test file |
| Graph500 BFS kernel / harmonic-mean TEPS | Non-goal (`paths(by="bfs")` is a different algorithm) |
| Engineering green (ingest → reopen → GSI → `LIMIT 1000`) | Yes, on `GraphForge::publish_bulk_*` + `execute` |

The SCALE-6 smoke is required CI. SCALE-20 is ignored and opt-in via Make.
Do not wire SCALE-20 into GitHub Actions.

## What engineering green means here

Per [Scale Evaluation](../reference/scale-evaluation.md):

1. Generate SCALE / ef=16 Kronecker edges; drop self-loops; canonicalize unique undirected pairs.
2. Ingest through `GraphForge::new` + `publish_bulk_nodes` / `publish_bulk_edges` (UUIDv7 identities).
3. Drop the instance and reopen with a second `GraphForge::new(Some(path))`.
4. Recount live V/E and grade GSI (`GU` from known undirected intent) until #398 ships a profiler.
5. Run both one-hop and two-hop `MATCH … LIMIT 1000` queries; each must finish with ≤1000 rows.

Optional extra (not an Official required step): `rank(by="degree")` at
`compute_threads=1` and `4` with fingerprint parity. Do **not** copy in-process
exec-kernel speedups onto this facade evidence.

## Commands

SCALE-6 CI smoke (also runs under Bazel `//crates/graphforge-api:scale_g500_scale20`):

```bash
cargo test -p graphforge-api --test scale_g500_scale20
```

SCALE-20 (long; isolate the target directory; at most two heavy builds):

```bash
CARGO_TARGET_DIR=/tmp/cargo-g500-scale20 make bench-g500-scale20
GF_G500_SCALE20_EVIDENCE_OUT=build/g500-scale20-evidence.json make bench-g500-scale20
```

Default evidence path: `build/g500-scale20-evidence.json`.

## Evidence JSON

Wrapper schema `graphforge-official-parameter-scale20/1` carries the field names
from [Scale Evaluation](../reference/scale-evaluation.md#evidence-artifact-schema)
(`schema_version: "1"`). Differences from an Official-track artifact:

- `track` is JSON `null` (not `"official"`).
- `generator.name` is `graphforge-kronecker-rmat`; `source` is this test file.
- `teps` is JSON `null`.
- A `steps` array records generate / ingest / reopen / LIMIT / optional rank.

Wall-clock numbers are hardware-specific observations, never CI millisecond gates.
If ingest OOMs or times out at 16M edges, record `error_class` `oom` / `timeout`
and do not call the run green.

## GSI expectation

After unique-undirected filtering, SCALE 20 (`V = 2^20`) grades `GU-06-MD-*`
(typically `D00`). SCALE 6 (`V = 64`) grades `GU-01-XS-*`. Density uses the
documented undirected formula `2|E| / (|V| × (|V| − 1))`.

## Non-goals

- Adding a Graph500 generator to product crates
- Pairwise neighborhood kernels at `V = 2^20`
- CI wall-clock thresholds
- Claiming ComputePool × from an in-process CSR kernel as facade proof
