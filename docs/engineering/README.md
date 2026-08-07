# Engineering

Engineering follows public behavior through design, pre-release evidence, continuous
delivery, and production learning.

```mermaid
flowchart LR
    Issue["Bounded issue"] --> Arch["ARCHITECTURE.md"]
    Arch --> Test["TESTING.md"]
    Test --> Pub["PUBLISHING.md"]
    Pub --> Obs["OBSERVABILITY.md"]
    Obs -. "validated feedback" .-> Issue
```

## Lifecycle

| Document | Responsibility |
| --- | --- |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Problem model, responsibility boundaries, and components that satisfy requirements |
| [`TESTING.md`](TESTING.md) | How tests and CI prove the system before release |
| [`PUBLISHING.md`](PUBLISHING.md) | How verified artifacts are versioned, promoted, and rolled back |
| [`OBSERVABILITY.md`](OBSERVABILITY.md) | How CI/release/user signals feed discovery |
| [`adrs/`](adrs/) | ADR index (bodies in [`../adr/`](../adr/) `0001`–`0014`) |

## Supporting documentation

| Document | Description |
| --- | --- |
| [`../book/architecture/`](../book/architecture/overview.md) | Deep architecture notes, pipeline, storage, embedding contracts |
| [`../development/`](../development/contributing.md) | Contributor workflow, testing detail, release process |
| [`../development/bazel.md`](../development/bazel.md) | M2 / #1 Bazel developer guide (install, extend, cache, CI/release) |
| [`../development/bazel-migration-ac-evidence.md`](../development/bazel-migration-ac-evidence.md) | M2 / #1 close-readiness AC → child evidence map |
| [`../development/bazel-migration-orchestration.md`](../development/bazel-migration-orchestration.md) | M2 / #1 Bazel migration sub-agent roles, contracts, and handoffs |
| [`../development/bazel-migration-ledger.md`](../development/bazel-migration-ledger.md) | M2 / #1 Cargo target + CI command migration ledger (freeze) |
| [`../development/bazel-migration-baseline.md`](../development/bazel-migration-baseline.md) | M2 / #1 accepted Blacksmith/Cargo CI performance baseline |
| [`../development/bazel-bootstrap.md`](../development/bazel-bootstrap.md) | M2 / #11 Bazelisk/Bzlmod bootstrap and Cargo drift check |
| [`../development/bazel-migration-parity.md`](../development/bazel-migration-parity.md) | M2 / #6 same-SHA Cargo/Bazel parity |
| [`../development/bazel-migration-perf.md`](../development/bazel-migration-perf.md) | M2 / #5 Blacksmith cache + performance gates |
| [`../development/bazel-migration-cutover.md`](../development/bazel-migration-cutover.md) | M2 / #4 CI Gate cutover + Cargo rollback |
| [`../contracts/`](https://github.com/CurateLabs/graphforge/tree/main/docs/contracts) | Frozen public API / fingerprint JSON contracts |
| [`../reference/`](../reference/api.md) | Compatibility, TCK, scale limits, column naming |
| Root `AGENTS.md` | Agent workflow and validation gates |

## Decision records

Create the next Architecture Decision Record with:

```sh
docslime add adr <short-slug>
```

Until bodies live under `engineering/adrs/`, add new ADR markdown under `docs/adr/` and update
both [`../adr/README.md`](../adr/README.md) and [`adrs/README.md`](adrs/README.md) in the same
change. Do not fork a second numbering sequence.
