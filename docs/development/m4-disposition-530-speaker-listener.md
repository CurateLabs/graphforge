# M4 disposition: cluster(by="speaker_listener") (#530)

Disposition: serial speaker-listener sweeps.

`speaker_listener` advances a deterministic random stream while each listener
samples neighbor memories and mutates its own memory. Parallel listener updates
would consume randomness and expose memory snapshots differently, changing label
selection and final community IDs.

| Evidence item | Disposition |
|---|---|
| Path / threads | Serial for 1/2/4/8/automatic; no private-pool or process-global Rayon work is introduced. |
| Work units | Normalized topology, seeded listener order, memory sampling/update, canonical label extraction, output. |
| Determinism | Existing `graphforge-exec` tests cover repeatability, memory thresholds, limits, cancellation, empty/isolate cases, and Rust registration. |
| Resource shape | Uses selected Rust adjacency and bounded node/community output; no parallel-only graph copy is added. |

No GPU, distributed, approximate, or foreign-engine fallback is implied, and
hardware timing/RSS observations remain non-gating evidence only.
