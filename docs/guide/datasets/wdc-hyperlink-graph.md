# WDC Hyperlink Graphs (not used for scale harness)

**Last updated:** 2026-08-05

> **Status: retired from GraphForge scale testing.**
>
> Web Data Commons (WDC) Hyperlink Graphs are **not** the GraphForge
> scale-testing track. Size escalation uses **Graph500 notches on
> [GSI](../../reference/graph-scale-index.md)**; workload completeness uses the
> **[LDBC full suite](ldbc.md)**. Both execute in an **external scale harness**,
> not GraphForge core CI.
>
> This page remains only so contributors do not rediscover WDC as a competing
> ladder. Do **not** present WDC T0–T6 as the execution ladder in new work.

## What changed

| Former framing | Current framing |
|---|---|
| WDC T0→T6 first-fail ladder on GSI | **Removed** as primary scale track |
| Controlled R2 mirror + `GF_WDC_*` for scale runners | **Not recommended** for scale harness |
| Optional M4 WDC validation track | Retargeted to **Graph500 + LDBC** (still non-blocking for M4) |

## What WDC is (background only)

[Web Data Commons Hyperlink Graphs](https://webdatacommons.org/hyperlinkgraph/)
are public Common Crawl–derived web graphs (2012/2014) at Page / Host / PLD
aggregation, published as Index/Arc and/or WebGraph. They remain useful research
corpora if you need real web topology — but GraphForge’s documented scale
harness path does **not** use them.

## Reference fetch scripts (legacy)

Thin helpers may still exist under `scripts/datasets/`
(`fetch_wdc_hyperlink.py`, `sync_wdc_mirror.py`) and Makefile targets
`fetch-wdc-hyperlink` / `sync-wdc-mirror`. They are **not** the recommended scale
path. Prefer Graph500 generation and LDBC Datagen in the external harness.

If you use the helpers for ad-hoc research downloads, treat env vars
(`GF_WDC_CACHE`, `GF_WDC_MIRROR_BASE`, `GF_WDC_SOURCE`, …) as legacy — do not
wire them into new scale runbooks.

## Where to go instead

| Need | Document |
|---|---|
| Size axis + Graph500 SCALE notches | [Graph Scale Index](../../reference/graph-scale-index.md) |
| Progressive / first-fail on size | [Official Graph500 first-fail policy](../../reference/graph-scale-index.md#progressive--first-fail-policy-official-graph500--gsi) |
| SNB / Graphalytics / FinBench / SPB | [LDBC full suite](ldbc.md) |
| Product envelopes | [Scale limits](../../reference/scale-limits.md) |
| Harness boundary | [External scale harness contract](../../reference/graph-scale-index.md#external-scale-harness-contract) |

## Related

- [Dataset overview](overview.md)
- [LDBC full suite](ldbc.md)
- [Graph Scale Index](../../reference/graph-scale-index.md)
