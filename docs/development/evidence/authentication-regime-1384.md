# Ingest authentication regime on the integrated tree (#1384)

Measured 2026-09-22 on OVHC-AGENCY at `ed273d2b` (contains the #1384 redesign
through PR #1552), stock release, quiet host, native ladder S18 → S19 → S20
under a delegated cgroup scope. Evidence archived with verified receipts:
[`ladder/ed273d2b2558c4137a398e4980471ca3bfcac613/`](ladder/ed273d2b2558c4137a398e4980471ca3bfcac613/).
The decision of record is [ADR 0045](../adr/0045-ingest-authentication-regime.md).

## Throughput (rung wall, edges per second)

| Rung | Edges | Wall s | Edges/s | Device read | Device write | Process peak |
|---|---:|---:|---:|---:|---:|---:|
| S18 | 4,194,304 | 45.0 | 92,340 | 19.36 GB | 12.92 GB | 250 MB |
| S19 | 8,388,608 | 90.3 | 92,851 | 39.44 GB | 26.08 GB | 275 MB |
| S20 | 16,777,216 | 181.7 | 92,349 | 80.03 GB | 52.67 GB | 333 MB |
| S22 (b6ffb088 baseline) | 67,108,864 | 725.7 | 92,470 | 326.90 GB | 233.29 GB | 494.7 MB |

Throughput is flat in scale. The 1M edges/s floor (#1387) is a separate
workstream; this issue's claim is the authentication regime, not the floor.

## Ingest application I/O by phase (read GB / write GB)

| Phase | S18 | S20 | S22 (baseline) |
|---|---|---|---|
| shaping region (legacy name `shape_consume_reauthentication`) | 2.82 / 1.78 | 11.26 / 7.11 | 45.03 / 28.42 |
| encode region (legacy name `encode_write_postwrite_authentication`) | 0.87 / 0.26 | 3.48 / 1.11 | 13.91 / 4.60 |
| recovery reauthentication (resume boundary) | 0.26 / 0 | 1.11 / 0 | 4.60 / 0 |
| CAS install (copy + digest naming) | 0.26 / 0.21 | 1.11 / 0.89 | 4.61 / 3.70 |
| hydration verification (open time) | 0.02 / 0.01 | 0.08 / 0.04 | 0.34 / 0.17 |
| append merge | 0 / 0.78 | 0 / 3.13 | 0 / 12.51 |
| **total** | **4.23 / 3.04** | **17.04 / 12.28** | **68.50 / 49.40** |

## Where the 310 GB went

The pre-redesign tree read 310.1 GB per 67,108,864-edge ingest — 4,579 B per
edge, 17.3× the retained 265 B/edge — with 99.1% of ingest reads existing to
re-hash bytes the process had just written. On the integrated tree:

- Ingest application reads are **68.5 GB at S22** (0.29× the old figure),
  dominated by the shaping and encode regions' real data movement, not
  authentication.
- Remaining authentication read-backs are **4.94 GB ≈ 74 B/edge ≈ 28% of the
  retained size**: resume-boundary reauthentication (68.6 B/edge — the #1269
  class, deliberately retained and regression-tested) plus open-time CAS
  hydration (5.1 B/edge, charged at read, not ingest). #1552 removed the CSR
  shard write read-back (0.906 GB at S22, 13.5 B/edge); with it the figure
  was 5.85 GB.
- Every removed pass is listed with its disposition in ADR 0045; every
  surviving boundary names the failure it uniquely catches and its measured
  cost.

## Reproducibility

Per-phase ingest application-I/O counters are **byte-identical** to the
`b6ffb088` baseline at S18 and S20, and every rung passes its correctness,
digest-reconciliation and result-digest checks. Removing read-back passes
changed no digest and no counter — exactly the determinism constraint.

## Write-volume attribution

Device writes run a consistent **4.25-4.72× application writes** across S18,
S19, S20 and S22 (12.92/3.04, 26.08/6.11, 52.67/12.28, 233.29/49.40 GB), owned
by the shaping region's spill/merge write-back pattern and fsync writeback.
Publication is separately decomposed and is not a write sink
([publication-attribution-1481.md](publication-attribution-1481.md): publish
is ~8% of complete-ingest wall at S18); the transient peak is attributed by
the lifecycle owner-union receipt (#1415, #1481). No unexplained write volume
remains at this issue's bar; the residual multiplier is the shaping
write-amplification work the floor workstream attacks next (#1448,
#1506-#1508).

## Acceptance-criteria map

See `authentication-regime-1384.json` for the per-criterion map; the ADR is
AC11, this document is AC9, and the storage suite's
`completed_shape_boundary_refuses_same_inode_payload_corruption` and
`shaping_recovery_refuses_same_inode_payload_corruption` are AC2's regression
proof.
