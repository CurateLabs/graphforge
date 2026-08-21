# Billion-live-edge certification

Issue #745 certifies the complete persisted GraphForge lifecycle on an explicitly
provisioned Linux evidence host. The workflow is manual, protected by the
`scale-certification` GitHub environment, and never provisions infrastructure.
A maintainer must approve the provider, exact SKU, ephemeral runner label, cost,
and teardown before dispatch.

The committed profile fixes SCALE 26, seed 1, the Graph500/R-MAT initiator,
undirected canonicalization, and a **target-live** stopping rule. Deterministic
attempt windows are externally sorted and merged until a complete merge proves
at least one billion unique canonical live edges. Raw attempts are never called
live edges, and evidence must reconcile attempts, self-loops, duplicates, and
unique edges.

## Dispatch and abort policy

1. Freeze the exact commit after normal PR CI is green.
2. Provision a fresh Linux host with at least 128 GiB RAM and 1 TiB local NVMe
   formatted as ext4, XFS, or Btrfs. Register it with one unique runner label.
3. Configure required reviewers on the `scale-certification` environment.
4. Dispatch `Billion-edge certification` with the exact 40-character SHA,
   runner label, provider, and SKU. Branch names and moving refs are rejected.
5. Do not retry an unchanged failing tree. Preserve the partial phase journal,
   identify the first failed phase, repair the root cause, and certify a new SHA.
6. Deregister and destroy the host after artifacts are uploaded. Never retain
   the project, portable bundle, credentials, registry tokens, or spill files.

The workflow has a four-hour outer timeout. An in-process watchdog samples Linux
resident memory every 250 ms and allocated workspace bytes every five seconds,
sets the shared cooperative-cancellation token on the first RSS, disk, or time
breach, and records a typed failure at the affected phase. The Rust journal is
atomically replaced after each phase so cancellation or host loss retains the
last complete bounded observation. The validator rejects capacity or runtime
breaches.

## Required phases and evidence

The same public Rust-facade run performs preflight, target-live generation,
bounded ingest, CSR construction, source reopen and one/two-hop observations,
portable-v2 bundle export, full verification, atomic import, imported reopen and
matching observations, plus representative corruption, cancellation, resource
limit, and interrupted-finalization drills. Source generation, semantic package,
transport, and imported generation identities remain distinct and reconciled.

Only the sanitized JSON evidence and phase journal may be downloaded and checked
in. The schema and semantic validator reject missing phases, count or authority
mismatches, query-fingerprint drift, identity collapse, unsafe paths, sensitive
fields, insufficient hosts, and envelope overruns. Project payloads, bundles,
spills, secrets, credentials, absolute host paths, and mutable runner state are
not evidence and must never be committed.

Local developer runs validate the small lifecycle and hostile fixtures only;
they are not certification evidence and cannot close #745.
