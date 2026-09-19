# Bounded parallel import normalization (#1472)

Original decoded batches are admitted to synchronous windows on the existing instance-owned compute pool. The window admits at most four batches and an estimated memory weight no larger than one quarter of the configured memory budget, capped at 256 MiB. A single oversized batch retains baseline single-batch behavior. The estimate includes retained Arrow and conservative row/property/nested-value scratch; it is not allocator enforcement. One decoded lookahead may coexist with the admitted window. Whole-process memory is measured separately.

Each job keeps its original source sequence and batch index. It normalizes the complete original batch, so generated UUID row ordinals and within-chunk duplicate detection are unchanged. Ordered individual Results let the coordinator append successful earlier batches before reporting a later failure. Decoder failures drain the earlier pending prefix inside existing source cache cleanup. Appends, manifests, checkpoint ownership and publication remain serial. Work is joined before return; there are no detached tasks or global Rayon-pool submissions. One-thread resource policy stays inline.

This experiment isolates parallel normalization. It does not overlap append with normalization or claim that input/append pipelining was adopted. The existing normalization region encloses only synchronous pool work, so its process CPU measures active normalization workers without concurrent append. Calling-thread scheduler counters describe the coordinator, not the workers.

Correctness tests compare normalized Arrow under 1/2/4 workers, generated and explicit UUIDv7 identities, properties, empty batches, duplicate refusals, error precedence, cancellation, bounded admission and oversized singletons. Real durable imports compare four retained shaped-run content digests and actual encoded payload bytes under 1/4 workers, publish and reopen, and query the resulting nodes/edges. The fixture pins the recorded session clock before staging and preserves recovery binding; it does not fix #1416. The established nonce-bearing ordinal receipt control remains outside ADR0038's payload boundary. A failing later batch preserves exactly two earlier durable batches across reopen and resume.

Measurement was predeclared before implementation: Graph500 S18 (262144 nodes,4194304 edges, edgefactor16, seed13907095936298285200), identical input bytes, fresh projects, equal16logicalCPU/4GiBcgroup limits, and three alternating matched baseline/candidate pairs. Qualification requires a quiet host before, during and after each run. Require normalization processCPU/wall>1, median validatewall reduction≥3%, and positive reduction in every pair. Completeingest wall/CPU/cgrouppeak are reported alongside validate and normalization regions. Cgrouppeak includes cache and is notRSS. Raw invalid or contended attempts remain excluded with their reason.

## Results and decision

The final baseline is current-main production code `43413cf9`; measured candidate is `5d900a34`, including the closed diagnostics-contract correction described below. The baseline was built from `8f695ad2`, whose only differences from `43413cf9` are three `cfg(test)` files excluded from the CLI binary. Frozen executable hashes and the source diff are retained. Measurements qualify these binaries, not a moving `main` branch.

The four fixed-width shaped-run SHA-256 receipts are also identical across all six full S18 observations, independently of the smaller recorded-clock payload fixture.

All six observations published successfully with 4,456,448 accepted rows and zero rejected rows, exit code zero, equal persisted construction budgets and clean before/during/after contention checks. No observation was excluded.

| Median measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Validate wall | 25.777 s | 24.674 s |
| Normalization wall | 2.296 s | 0.924 s |
| Normalization process CPU | 2.340 s | 2.660 s |
| Normalization effective cores | 1.018  | 2.879  |
| Complete ingest wall | 28.609 s | 27.455 s |
| Complete ingest CPU | 25.206 s | 25.880 s |
| Complete ingest cgroup peak | 762.090 MiB | 819.594 MiB |

Validate improved 4.28%, 5.71%, and 3.40% in the three matched pairs. The median reduction is **4.28%**, and every candidate normalization region exceeded 2.8 effective cores. The predeclared gate passes. Complete-ingest wall fell 4.03%, while process CPU increased 2.67% and cgroup peak memory increased about 58 MiB. Parallel normalization trades some CPU and retained memory for reduced elapsed time. These three pairs establish the stated experiment gate, not broad statistical significance or a throughput-floor qualification.

The candidate uses 23 normalization windows versus 68 baseline batches at S18. Byte admission allows three ordinary edge batches in a window under the default 128 MiB estimate budget; the task-count ceiling remains four. Individual input batches are never split or merged for validation or replay.

**Adopt this bounded normalization change.** It preserves the tested durability and payload contracts and meets the measured benefit rule. Input/append overlap and shaping decomposition remain separate work. No claim is made about #1387's 1M-edge/s floor or the full S18–S22 release ladder.

## Verification and provenance

With an isolated Cargo target and ext4 `TMPDIR`:

- `cargo test --release -p graphforge-api --lib`: 753 passed, no failures, two existing ignored tests before the counter correction. After correction, `cargo test --release -p graphforge-api --lib import_session -- --nocapture`: 27 passed, including the new closed work-counter contract regression.
- `cargo test --release -p graphforge-exec --lib ordered_map_keeps_serial_policy_inline_and_parallel_results_ordered`: passed.
- `cargo clippy --release -p graphforge-api -p graphforge-exec --lib -- -D warnings`: passed.
- `taskset -c 0 target/release/deps/graphforge_api-24430acb605070f2 import_session --nocapture` after rebuilding final fixtures: 26 passed on a single logical CPU.
- `make pre-push-fast`, `make gate-registry-check`, final formatting and source-size policy: passed.

The initial PR CI passed the authoritative Rust tests but failed the actual tiny lifecycle producer: new `admitted_bytes` and `batches` work counters violated the closed receipt contract, whose supported units are rows/bytes/nodes/edges. The correction emits supported `rows` only, counting successfully normalized work, including work later discarded after an earlier error; it is not a durable-append count. The schema and certifier remain unchanged. The corrected frozen binary passes `benchmarks/scripts/test-tiny-lifecycle-certification.py` with both ordinary and `--growth` modes (S6/S7/S8, three-observation growth oracle), plus schema validation of every final S18 validate/commit receipt. Both modes used the frozen `--gf`, `--certify`, and `--generator` binaries and the ext4 `--workspace-root` under the final evidence directory.

The initial timing series is retained in JSON under `initial_series`, explicitly disqualified as lifecycle certification evidence by that counter defect. The final six-run series above requalifies the corrected binary against current main using the same predeclared gate; the series are not pooled.

Initial fixture development exposed invalid UUIDs and an attempt to read correctly retired shaped payloads; both were corrected before qualification. A default `/tmp` invocation encountered the host's unsupported tmpfs admission; durable tests use the root ext4 volume. These are test-development failures, not excluded performance observations.

The [JSON evidence](parallel-normalization-1472.json) records every sample, raw-output hashes, binary/input/method hashes, persisted budgets and exact ratios. Raw scripts, binaries, project checkpoints and test logs remain in `/home/ubuntu/gf-1472-final-evidence/`; earlier development and test logs remain in `/home/ubuntu/gf-1472-evidence/`.
